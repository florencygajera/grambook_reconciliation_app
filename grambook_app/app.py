"""
Grambook Reconciliation - Production-grade Flask Backend
Run: python app.py
Then open http://localhost:5000
"""

from __future__ import annotations

import csv
import io
import os
import re
import shutil
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any

import pandas as pd
from flask import Flask, jsonify, request, send_file, send_from_directory
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

try:
    from PIL import Image
except ImportError:
    Image = None

try:
    import pytesseract
except ImportError:
    pytesseract = None

try:
    import winreg
except ImportError:
    winreg = None

try:
    from rapidfuzz import fuzz
except ImportError:
    fuzz = None

app = Flask(__name__, static_folder="static")


class ReconciliationError(Exception):
    pass


INDIC_DIGIT_MAP = str.maketrans(
    {
        "\u0966": "0", "\u0967": "1", "\u0968": "2", "\u0969": "3",
        "\u096A": "4", "\u096B": "5", "\u096C": "6", "\u096D": "7",
        "\u096E": "8", "\u096F": "9",
        "\u0AE6": "0", "\u0AE7": "1", "\u0AE8": "2", "\u0AE9": "3",
        "\u0AEA": "4", "\u0AEB": "5", "\u0AEC": "6", "\u0AED": "7",
        "\u0AEE": "8", "\u0AEF": "9",
    }
)


@dataclass
class ParsedDataset:
    rows: list[dict[str, str]]
    columns: list[str]
    column_meta: list[dict[str, Any]]
    normalized_map: dict[str, str]
    header_row_index: int
    header_row_span: int
    dropped_columns: list[str]
    source_format: str
    parser_notes: list[str]
    # Tracks which 0-based original xlsx column each parsed column maps to.
    # Used by highlight_excel_fast to find the correct cell in the original file.
    kept_indices: list[int] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────────
# Tesseract setup
# ──────────────────────────────────────────────────────────────────────────────

def _iter_registry_tesseract_paths() -> list[str]:
    if winreg is None:
        return []
    roots = [
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall"),
        (winreg.HKEY_CURRENT_USER,  r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
    ]
    found: list[str] = []
    for root, subkey in roots:
        try:
            with winreg.OpenKey(root, subkey) as key:
                i = 0
                while True:
                    try:
                        child_name = winreg.EnumKey(key, i)
                        i += 1
                        with winreg.OpenKey(key, child_name) as child:
                            try:
                                display_name = winreg.QueryValueEx(child, "DisplayName")[0]
                            except OSError:
                                continue
                            if "tesseract" not in str(display_name).lower():
                                continue
                            for value_name in ("InstallLocation", "UninstallString"):
                                try:
                                    raw = str(winreg.QueryValueEx(child, value_name)[0]).strip().strip('"')
                                except OSError:
                                    continue
                                if not raw:
                                    continue
                                candidate = (raw if raw.lower().endswith(".exe")
                                             else os.path.join(raw, "tesseract.exe"))
                                if os.path.basename(candidate).lower() == "tesseract-uninstall.exe":
                                    candidate = os.path.join(os.path.dirname(candidate), "tesseract.exe")
                                if os.path.isdir(candidate):
                                    candidate = os.path.join(candidate, "tesseract.exe")
                                found.append(candidate)
                    except OSError:
                        break
        except OSError:
            continue
    return found


def _configure_tesseract() -> None:
    if pytesseract is None:
        return
    candidates: list[str] = []
    env_cmd = os.getenv("TESSERACT_CMD", "").strip()
    if env_cmd:
        candidates.append(env_cmd)
    path_cmd = shutil.which("tesseract")
    if path_cmd:
        candidates.append(path_cmd)
    local_app_data = os.getenv("LOCALAPPDATA", "").strip()
    if local_app_data:
        candidates.append(os.path.join(local_app_data, "Programs", "Tesseract-OCR", "tesseract.exe"))
    candidates.extend([
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    ])
    candidates.extend(_iter_registry_tesseract_paths())
    for cmd in candidates:
        if cmd and os.path.isfile(cmd):
            pytesseract.pytesseract.tesseract_cmd = cmd
            tessdata_dir = os.path.join(os.path.dirname(cmd), "tessdata")
            if os.path.isdir(tessdata_dir) and not os.getenv("TESSDATA_PREFIX"):
                os.environ["TESSDATA_PREFIX"] = tessdata_dir
            break


_configure_tesseract()


# ──────────────────────────────────────────────────────────────────────────────
# Text normalisation helpers
# ──────────────────────────────────────────────────────────────────────────────

def canonical_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(INDIC_DIGIT_MAP)
    text = re.sub(r"\s+", " ", text).strip()
    return text


EXCEL_ILLEGAL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")


def excel_safe_text(value: Any) -> str:
    return EXCEL_ILLEGAL_CHARS_RE.sub("", canonical_text(value))


def _to_decimal_if_simple_number(value: str) -> Decimal | None:
    text = canonical_text(value).replace(",", "")
    if not text:
        return None
    if not re.fullmatch(r"[+-]?\d+(\.\d+)?", text):
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _is_zero_or_blank(value: Any) -> bool:
    """
    FIX — Zero/blank equivalence:
    Returns True for None, '', '0', '0.0', '0.00' and any zero-valued number.
    These must all be treated as equivalent during reconciliation so that a cell
    containing 0.0 in one file does not raise a false mismatch against an empty
    cell in the other file.
    """
    text = canonical_text(value)
    if not text:
        return True
    dec = _to_decimal_if_simple_number(text)
    return dec is not None and dec == 0


def values_equivalent_for_compare(left: Any, right: Any) -> bool:
    # Both blank/zero → match (no false mismatch for 0.0 vs "")
    if _is_zero_or_blank(left) and _is_zero_or_blank(right):
        return True

    l_txt = canonical_text(left)
    r_txt = canonical_text(right)
    if l_txt == r_txt:
        return True

    # Numeric formatting variants (330 == 330.0 == 330.00)
    if "." in l_txt or "." in r_txt:
        l_dec = _to_decimal_if_simple_number(l_txt)
        r_dec = _to_decimal_if_simple_number(r_txt)
        if l_dec is not None and r_dec is not None and l_dec == r_dec:
            return True

    return False


def normalize_column_key(name: str) -> str:
    return re.sub(r"[\s_\-]+", "", canonical_text(name).lower())


def is_numeric_like(text: str) -> bool:
    t = canonical_text(text)
    if not t:
        return False
    return bool(re.fullmatch(r"[+-]?\d+(\.\d+)?", t.replace(",", "")))


# ──────────────────────────────────────────────────────────────────────────────
# File parsing — CSV / XLS / XLSX → raw string matrix
# ──────────────────────────────────────────────────────────────────────────────

def _decode_csv_bytes(file_bytes: bytes) -> io.StringIO:
    for enc in ["utf-8-sig", "utf-8", "utf-16", "cp1252", "latin1"]:
        try:
            return io.StringIO(file_bytes.decode(enc))
        except UnicodeDecodeError:
            continue
    raise ReconciliationError("CSV encoding is unsupported or file is corrupt.")


def _normalize_row_length(matrix: list[list[str]], max_cols: int | None = None) -> list[list[str]]:
    if not matrix:
        return matrix
    if max_cols is None:
        max_cols = max(len(r) for r in matrix)
    return [r + [""] * (max_cols - len(r)) if len(r) < max_cols else r[:max_cols] for r in matrix]


def _parse_csv_matrix(file_bytes: bytes) -> list[list[str]]:
    sio = _decode_csv_bytes(file_bytes)
    sample = sio.read(4096)
    sio.seek(0)
    dialect = csv.excel
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        pass
    matrix = [[canonical_text(cell) for cell in row] for row in csv.reader(sio, dialect)]
    return _normalize_row_length(matrix)


def _parse_xls_matrix(file_bytes: bytes) -> list[list[str]]:
    try:
        import xlrd
    except ImportError as e:
        raise ReconciliationError(".xls file detected but xlrd is missing. Install xlrd==2.0.1") from e
    try:
        wb = xlrd.open_workbook(file_contents=file_bytes)
        sh = wb.sheet_by_index(0)
    except Exception as e:
        raise ReconciliationError(f"Unable to read .xls workbook: {e}") from e
    matrix = [[canonical_text(sh.cell_value(r, c)) for c in range(sh.ncols)] for r in range(sh.nrows)]
    matrix = _normalize_row_length(matrix, sh.ncols)
    for (rlo, rhi, clo, chi) in getattr(sh, "merged_cells", []):
        top_left = matrix[rlo][clo] if rlo < len(matrix) and clo < len(matrix[rlo]) else ""
        for rr in range(rlo, rhi):
            for cc in range(clo, chi):
                matrix[rr][cc] = top_left
    return matrix


def _ocr_text_from_image(raw_image_bytes: bytes | None) -> str:
    if not raw_image_bytes or Image is None or pytesseract is None:
        return ""
    try:
        with Image.open(io.BytesIO(raw_image_bytes)) as img:
            for lang in ("guj+hin+eng", "hin+eng", "eng"):
                try:
                    text = canonical_text(pytesseract.image_to_string(img, lang=lang))
                    if text:
                        return text
                except Exception:
                    continue
    except Exception:
        pass
    return ""


def _parse_xlsx_matrix(file_bytes: bytes) -> tuple[list[list[str]], list[str]]:
    notes: list[str] = []
    try:
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True)
        ws = wb.worksheets[0]
    except Exception as e:
        raise ReconciliationError(f"Unable to read .xlsx workbook: {e}") from e

    max_row = ws.max_row or 0
    max_col = ws.max_column or 0
    matrix = [["" for _ in range(max_col)] for _ in range(max_row)]

    for r in range(1, max_row + 1):
        for c in range(1, max_col + 1):
            matrix[r - 1][c - 1] = canonical_text(ws.cell(row=r, column=c).value)

    # Forward-fill merged cells so category labels span all their sub-columns.
    # This is essential for multi-header files like the Managana Register where
    # "ઘરવેરો" sits in a merged cell above "બાકી", "ચાલુ", "કુલ".
    for mrange in ws.merged_cells.ranges:
        top_left = matrix[mrange.min_row - 1][mrange.min_col - 1]
        for rr in range(mrange.min_row - 1, mrange.max_row):
            for cc in range(mrange.min_col - 1, mrange.max_col):
                matrix[rr][cc] = top_left

    images = getattr(ws, "_images", [])
    if images:
        if Image is None or pytesseract is None:
            notes.append("Image(s) detected but OCR dependencies are unavailable.")
        else:
            for img in images:
                anchor = getattr(img, "anchor", None)
                marker = getattr(anchor, "_from", None)
                if marker is None:
                    continue
                rr, cc = marker.row, marker.col
                if rr < 0 or cc < 0:
                    continue
                while rr >= len(matrix):
                    matrix.append([""] * max_col)
                while cc >= len(matrix[rr]):
                    matrix[rr].append("")
                if matrix[rr][cc]:
                    continue
                try:
                    raw = img._data()
                except Exception:
                    raw = None
                ocr_text = _ocr_text_from_image(raw)
                if ocr_text:
                    matrix[rr][cc] = ocr_text

    return _normalize_row_length(matrix), notes


def parse_matrix_from_upload(file_storage) -> tuple[list[list[str]], str, list[str]]:
    filename = (file_storage.filename or "").lower().strip()
    file_bytes = file_storage.read()
    if not file_bytes:
        raise ReconciliationError("Uploaded file is empty.")
    if filename.endswith(".csv"):
        return _parse_csv_matrix(file_bytes), "csv", []
    if filename.endswith(".xlsx"):
        matrix, notes = _parse_xlsx_matrix(file_bytes)
        return matrix, "xlsx", notes
    if filename.endswith(".xls"):
        return _parse_xls_matrix(file_bytes), "xls", []
    raise ReconciliationError("Unsupported file format. Upload .csv, .xls, or .xlsx")


# ──────────────────────────────────────────────────────────────────────────────
# Header detection
# ──────────────────────────────────────────────────────────────────────────────

def _row_header_score(row: list[str]) -> float:
    non_empty = [canonical_text(x) for x in row if canonical_text(x)]
    if len(non_empty) < 2:
        return -1.0
    text_cells = sum(1 for x in non_empty if not is_numeric_like(x))
    text_ratio = text_cells / max(1, len(non_empty))
    avg_len = sum(len(x) for x in non_empty) / len(non_empty)
    long_penalty = sum(1 for x in non_empty if len(x) > 80)
    return len(non_empty) * 0.25 + text_ratio * 2.5 - (avg_len / 120) - long_penalty * 1.2


def _is_sub_header_row(row: list[str]) -> bool:
    """
    FIX — Multi-header detection:
    Detects that a row is a repeating sub-header row (e.g. બાકી ચાલુ કુલ cycling
    across many columns). Such a row should NOT be the header_start; the row above
    it (containing the category labels) is the real start, with span = 2.
    """
    non_empty = [x for x in row if x]
    if len(non_empty) < 4:
        return False
    unique_vals = set(non_empty)
    # High repetition = same sub-labels cycling across column groups
    repetition_ratio = 1.0 - len(unique_vals) / len(non_empty)
    if repetition_ratio < 0.40:
        return False
    # All values must be short text labels, not numeric
    text_ratio = sum(1 for x in non_empty if not is_numeric_like(x)) / len(non_empty)
    return text_ratio >= 0.90


def detect_header_start(
    matrix: list[list[str]], manual_header_row: int | None = None
) -> tuple[int, int | None]:
    """
    Returns (header_start_0indexed, forced_span_or_None).
    forced_span is set to 2 when a multi-header layout is detected automatically
    so that build_headers combines category + sub-label rows correctly.
    """
    if not matrix:
        raise ReconciliationError("File has no rows.")

    if manual_header_row is not None:
        idx = manual_header_row - 1
        if idx < 0 or idx >= len(matrix):
            raise ReconciliationError("Manual header row is out of range.")
        return idx, None

    max_scan = min(60, len(matrix))
    scored = []
    for i in range(max_scan):
        score = _row_header_score(matrix[i])
        if score < 0:
            continue
        non_empty = [x for x in matrix[i] if x]
        if len(non_empty) < 2:
            continue
        text_ratio = sum(1 for x in non_empty if not is_numeric_like(x)) / len(non_empty)
        if text_ratio < 0.55:
            continue
        if any(len(x) > 140 for x in non_empty):
            continue
        scored.append((i, score))

    if not scored:
        raise ReconciliationError("Could not detect a valid header row. Please use manual header override.")

    scored.sort(key=lambda x: x[1], reverse=True)
    best_idx = scored[0][0]

    # If the best row looks like a repeating sub-header (e.g. બાકી/ચાલુ/કુલ),
    # check the row immediately before it: if that row contains category labels,
    # step back to it and force span = 2 so both rows form the combined header.
    if _is_sub_header_row(matrix[best_idx]) and best_idx > 0:
        prev = matrix[best_idx - 1]
        prev_non_empty = [x for x in prev if x]
        if (prev_non_empty and
                sum(1 for x in prev_non_empty if not is_numeric_like(x)) / len(prev_non_empty) >= 0.9):
            return best_idx - 1, 2

    return best_idx, None


def _forward_fill_header_cells(row: list[str]) -> list[str]:
    out, last = [], ""
    for cell in row:
        c = canonical_text(cell)
        if not c and last:
            c = last
        if c:
            last = c
        out.append(c)
    return out


def _is_unnamed_header(value: str) -> bool:
    v = value.strip().lower()
    return not v or v.startswith("unnamed")


def _dedupe_in_order(values: list[str]) -> list[str]:
    out: list[str] = []
    for v in values:
        if not v:
            continue
        if not out or out[-1] != v:
            out.append(v)
    return out


def _build_column_meta(
    matrix: list[list[str]],
    header_start: int,
    header_span: int,
    kept_indices: list[int],
    headers: list[str],
) -> list[dict[str, Any]]:
    max_cols = max(len(r) for r in matrix) if matrix else 0
    header_rows: list[list[str]] = []
    for i in range(header_start, min(header_start + header_span, len(matrix))):
        row = (matrix[i] if i < len(matrix) else []) + [""] * max_cols
        header_rows.append(_forward_fill_header_cells(row[:max_cols]))

    out: list[dict[str, Any]] = []
    for out_idx, original_col_idx in enumerate(kept_indices):
        header_value = headers[out_idx] if out_idx < len(headers) else f"column_{out_idx}"
        parts = _dedupe_in_order([
            canonical_text(hr[original_col_idx]) for hr in header_rows
            if original_col_idx < len(hr)
        ])
        if not parts:
            parts = [header_value]
        group = parts[0] if len(parts) > 1 else "Other"
        out.append({
            "column": header_value,
            "group": group,
            "parts": parts,
            "hierarchy": " > ".join(parts),
            "hierarchy_short": " > ".join(parts[1:]) if len(parts) > 1 else header_value,
        })
    return out


def build_headers(
    matrix: list[list[str]],
    header_start: int,
    manual_header_span: int | None = None,
    forced_span: int | None = None,
) -> tuple[list[str], dict[str, str], list[int], int]:
    """
    FIX — Multi-header column naming:
    When span ≥ 2, each column name is built by joining ALL non-empty, non-duplicate
    label parts from every header row (category row + sub-header row), producing
    names like "ઘરવેરો બાકી", "ઘરવેરો ચાલુ", "ઘરવેરો કુલ" instead of just
    "ઘરવેરો", "ઘરવેરો_2", "ઘરવેરો_3".
    """
    if not matrix:
        raise ReconciliationError("Empty matrix.")

    max_cols = max(len(r) for r in matrix)
    local_rows: list[list[str]] = []
    for i in range(header_start, min(header_start + 5, len(matrix))):
        row = (matrix[i] if i < len(matrix) else []) + [""] * max_cols
        local_rows.append(_forward_fill_header_cells(row[:max_cols]))

    # Determine span: forced detection result > manual override > auto-score
    if forced_span is not None and manual_header_span is None:
        chosen_span = forced_span
    else:
        spans = ([manual_header_span] if manual_header_span in (1, 2, 3, 4, 5)
                 else [5, 4, 3, 2, 1])
        chosen_span = 1
        best_score = -1.0

        for span in spans:
            if header_start + span > len(matrix):
                continue
            combined = []
            for c in range(max_cols):
                parts = _dedupe_in_order([local_rows[r][c] for r in range(span) if local_rows[r][c]])
                combined.append(" ".join(parts).strip())

            non_empty = sum(1 for h in combined if h)
            score = non_empty - sum(1 for h in combined if len(h) > 90) * 1.5

            multi_header_bonus = 0
            for row_idx in range(span):
                row_text = " ".join(local_rows[row_idx]).lower()
                if any(k in row_text for k in ["baki", "test", "type", "category",
                                                "status", "remark", "sr no", "serial",
                                                "બાકી", "ચાલુ", "કુલ"]):
                    multi_header_bonus += 2
                if any(k in row_text for k in ["no", "number", "id", "code", "name",
                                                "ક્રમ", "નંબર"]):
                    multi_header_bonus += 1
            score += multi_header_bonus

            if score > best_score:
                best_score = score
                chosen_span = span

    # Build raw header names: join category label + sub-label for each column
    header_rows = local_rows[:chosen_span]
    raw_headers: list[str] = []
    for c in range(max_cols):
        parts = _dedupe_in_order([header_rows[r][c] for r in range(chosen_span)
                                  if header_rows[r][c]])
        raw_headers.append(canonical_text(" ".join(parts)))

    # Deduplicate display headers
    display_headers: list[str] = []
    normalized_map: dict[str, str] = {}
    drop_indices: list[int] = []

    for idx, h in enumerate(raw_headers):
        if _is_unnamed_header(h):
            if h.lower().startswith("unnamed"):
                drop_indices.append(idx)
                continue
            h = f"column_{idx}"

        if h in display_headers:
            suffix = 2
            while f"{h}_{suffix}" in display_headers:
                suffix += 1
            h = f"{h}_{suffix}"

        display_headers.append(h)

    for h in display_headers:
        nk = normalize_column_key(h)
        if nk not in normalized_map:
            normalized_map[nk] = h
        else:
            suffix = 2
            while f"{nk}_{suffix}" in normalized_map:
                suffix += 1
            normalized_map[f"{nk}_{suffix}"] = h

    return display_headers, normalized_map, drop_indices, chosen_span


def _align_row(row: list[str], target_cols: int) -> list[str]:
    r = list(row)
    if len(r) < target_cols:
        r += [""] * (target_cols - len(r))
    elif len(r) > target_cols:
        r = r[:target_cols]
    first_non_empty = next((i for i, v in enumerate(r) if canonical_text(v)), None)
    if first_non_empty is not None and first_non_empty > 0:
        trailing_empty = all(not canonical_text(x) for x in r[-first_non_empty:])
        if trailing_empty:
            r = r[first_non_empty:] + [""] * first_non_empty
    return r


def dataframe_from_matrix(
    matrix: list[list[str]],
    source_format: str,
    manual_header_row: int | None = None,
    manual_header_span: int | None = None,
) -> ParsedDataset:
    if not matrix:
        raise ReconciliationError("No data rows found in file.")

    max_cols = max(len(r) for r in matrix)
    matrix = _normalize_row_length(matrix, max_cols)

    header_start, forced_span = detect_header_start(matrix, manual_header_row=manual_header_row)
    headers, normalized_map, drop_indices, header_span = build_headers(
        matrix, header_start,
        manual_header_span=manual_header_span,
        forced_span=forced_span,
    )

    kept_indices = [i for i in range(max_cols) if i not in set(drop_indices)]
    if not kept_indices:
        raise ReconciliationError("No usable columns detected after header processing.")

    data_rows = matrix[header_start + header_span:]
    aligned_rows = [
        [_align_row(row, max_cols)[i] for i in kept_indices]
        for row in data_rows
    ]

    if len(headers) != len(kept_indices):
        headers = [f"column_{i}" for i in range(len(kept_indices))]
        normalized_map = {normalize_column_key(h): h for h in headers}

    column_meta = _build_column_meta(
        matrix=matrix,
        header_start=header_start,
        header_span=header_span,
        kept_indices=kept_indices,
        headers=headers,
    )

    df = pd.DataFrame(aligned_rows, columns=headers, dtype="string")
    for col in df.columns:
        s = df[col].fillna("").astype("string").str.replace(r"\s+", " ", regex=True).str.strip()
        df[col] = s.map(canonical_text)

    if not df.empty:
        df = df[df.ne("").any(axis=1)].drop_duplicates()

    rows = df.fillna("").to_dict(orient="records")
    parser_notes: list[str] = []
    if source_format == "xls":
        parser_notes.append("Legacy .xls format parsed via xlrd.")

    return ParsedDataset(
        rows=rows,
        columns=list(df.columns),
        column_meta=column_meta,
        normalized_map=normalized_map,
        header_row_index=header_start,
        header_row_span=header_span,
        dropped_columns=[str(i) for i in drop_indices],
        source_format=source_format,
        parser_notes=parser_notes,
        kept_indices=kept_indices,
    )


def parse_uploaded_dataset(
    file_storage,
    manual_header_row: int | None = None,
    manual_header_span: int | None = None,
) -> ParsedDataset:
    matrix, source_format, notes = parse_matrix_from_upload(file_storage)
    parsed = dataframe_from_matrix(
        matrix,
        source_format=source_format,
        manual_header_row=manual_header_row,
        manual_header_span=manual_header_span,
    )
    parsed.parser_notes.extend(notes)
    return parsed


# ──────────────────────────────────────────────────────────────────────────────
# Reconciliation
# ──────────────────────────────────────────────────────────────────────────────

def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if fuzz is not None:
        return float(fuzz.ratio(a, b)) / 100.0
    return SequenceMatcher(None, a, b).ratio()


def map_columns_smart(
    admin_cols: list[str], suv_cols: list[str], admin_key: str, suv_key: str
) -> list[tuple[str, str, float]]:
    admin_candidates = [c for c in admin_cols if c != admin_key]
    suv_candidates = [c for c in suv_cols if c != suv_key]
    suv_norm = {normalize_column_key(c): c for c in suv_candidates}
    used_suv: set[str] = set()
    mapped: list[tuple[str, str, float]] = []

    for ac in admin_candidates:
        sc = suv_norm.get(normalize_column_key(ac))
        if sc and sc not in used_suv:
            mapped.append((ac, sc, 1.0))
            used_suv.add(sc)

    for ac in admin_candidates:
        if any(x[0] == ac for x in mapped):
            continue
        best_sc, best_score = None, 0.0
        na = normalize_column_key(ac)
        for sc in suv_candidates:
            if sc in used_suv:
                continue
            score = _similarity(na, normalize_column_key(sc))
            if score > best_score:
                best_score = score
                best_sc = sc
        if best_sc and best_score >= 0.72:
            mapped.append((ac, best_sc, best_score))
            used_suv.add(best_sc)

    return mapped


def _build_key_index(rows: list[dict[str, str]], key_col: str):
    idx: dict[str, list[tuple[int, dict]]] = defaultdict(list)
    missing = 0
    for i, row in enumerate(rows):
        key = canonical_text(row.get(key_col, ""))
        if not key:
            missing += 1
            continue
        idx[key].append((i, row))
    return idx, missing


def reconcile(
    admin_rows: list[dict[str, str]],
    admin_cols: list[str],
    suv_rows: list[dict[str, str]],
    suv_cols: list[str],
    admin_key: str,
    suv_key: str,
) -> dict[str, Any]:
    admin_idx, admin_missing_keys = _build_key_index(admin_rows, admin_key)
    suv_idx, suv_missing_keys = _build_key_index(suv_rows, suv_key)

    col_pairs = map_columns_smart(admin_cols, suv_cols, admin_key, suv_key)
    pair_lookup = {a: s for a, s, _ in col_pairs}

    admin_keys = set(admin_idx)
    suv_keys = set(suv_idx)
    common = admin_keys & suv_keys
    only_admin_keys = admin_keys - suv_keys
    only_suv_keys = suv_keys - admin_keys

    discrepancies = []
    matching_records = 0
    duplicate_key_conflicts = []

    for key in sorted(common):
        a_rows = admin_idx[key]
        s_rows = suv_idx[key]

        if len(a_rows) != 1 or len(s_rows) != 1:
            duplicate_key_conflicts.append({
                "key": key,
                "admin_count": len(a_rows),
                "suvidha_count": len(s_rows),
            })

        a_index, a = a_rows[0]
        s_index, s = s_rows[0]
        diffs: dict[str, Any] = {}

        for ac, sc, confidence in col_pairs:
            av = canonical_text(a.get(ac, ""))
            sv = canonical_text(s.get(sc, ""))
            if not values_equivalent_for_compare(av, sv):
                diffs[ac] = {
                    "admin": av,
                    "suvidha": sv,
                    "suv_col": sc,
                    "confidence": round(confidence, 4),
                    "col_index": admin_cols.index(ac),
                }

        if diffs:
            discrepancies.append({
                "key": key,
                "row_index": a_index,
                "admin_row": a,
                "suv_row": s,
                "diffs": diffs,
            })
        else:
            matching_records += 1

    only_admin_rows = [row for key in sorted(only_admin_keys) for _, row in admin_idx[key]]
    only_suv_rows = [row for key in sorted(only_suv_keys) for _, row in suv_idx[key]]

    return {
        "discrepancies": discrepancies,
        "only_admin_rows": only_admin_rows,
        "only_suv_rows": only_suv_rows,
        "col_pairs": [
            {"admin_col": a, "suv_col": s, "confidence": round(c, 4)}
            for a, s, c in col_pairs
        ],
        "admin_key": admin_key,
        "suv_key": suv_key,
        "admin_cols": admin_cols,
        "suv_cols": suv_cols,
        "meta": {
            "compared_keys": len(common),
            "duplicate_key_conflicts": duplicate_key_conflicts,
            "admin_missing_keys": admin_missing_keys,
            "suvidha_missing_keys": suv_missing_keys,
            "unmapped_admin_cols": [
                c for c in admin_cols if c != admin_key and c not in pair_lookup
            ],
        },
        "stats": {
            "total": len(admin_keys | suv_keys),
            "matched": matching_records,
            "disc": len(discrepancies),
            "only_a": len(only_admin_rows),
            "only_s": len(only_suv_rows),
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# Excel output
# ──────────────────────────────────────────────────────────────────────────────

def _border() -> Border:
    s = Side(style="thin", color="D9D9D9")
    return Border(left=s, right=s, top=s, bottom=s)


def _style_cell(
    cell, *, fill_hex: str | None = None, bold: bool = False,
    color: str = "1F2937", align: str = "left", wrap: bool = False,
) -> None:
    if fill_hex:
        cell.fill = PatternFill("solid", start_color=fill_hex)
    cell.font = Font(bold=bold, color=color, name="Calibri", size=10)
    cell.alignment = Alignment(horizontal=align, vertical="center", wrap_text=wrap)
    cell.border = _border()


def highlight_excel_fast(
    original_file_bytes: bytes,
    result: dict,
    parsed_admin: ParsedDataset,
) -> io.BytesIO:
    """
    FIX — Round-trip format preservation:
    Opens the original uploaded file and highlights only the discrepant cells in
    red, leaving the complete multi-header structure, merges, and formatting intact.

    Row mapping:
      The parsed DataFrame's row index 0 corresponds to the first DATA row in the
      original xlsx, which sits at Excel row:
        header_row_index  (0-based blank/leading rows before header)
        + header_row_span (number of header rows)
        + 1               (convert to 1-based Excel addressing)
      So:  excel_row = df_row_index + header_row_index + header_row_span + 1

    Column mapping:
      col_index in diffs = position within the parsed admin_cols list.
      kept_indices[col_index] = 0-based original xlsx column index.
      Add 1 for 1-based Excel column addressing.
    """
    wb = load_workbook(io.BytesIO(original_file_bytes), read_only=False)
    ws = wb.active

    # FFFF0000 = fully opaque red (openpyxl requires 8-char ARGB hex)
    red_font  = Font(color="FFFF0000", bold=True)
    red_fill  = PatternFill("solid", start_color="FFFFE0E0")

    # First data row in xlsx (1-based)
    data_row_offset = parsed_admin.header_row_index + parsed_admin.header_row_span + 1

    # Map: parsed column list position → 1-based xlsx column number
    kept = parsed_admin.kept_indices
    parsed_col_to_xlsx_col = {
        parsed_idx: (orig_0based_idx + 1)
        for parsed_idx, orig_0based_idx in enumerate(kept)
    }

    for disc in result["discrepancies"]:
        excel_row = disc["row_index"] + data_row_offset
        for col_name, diff in disc["diffs"].items():
            xlsx_col = parsed_col_to_xlsx_col.get(diff["col_index"])
            if xlsx_col is None:
                continue
            cell = ws.cell(row=excel_row, column=xlsx_col)
            cell.font = red_font
            cell.fill = red_fill

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def _write_sheet_table(ws, headers: list[str], rows: list[dict[str, Any]], header_fill: str = "1F4E78") -> None:
    for c, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=c, value=excel_safe_text(h))
        _style_cell(cell, fill_hex=header_fill, bold=True, color="FFFFFF", align="center", wrap=True)
    for r_idx, row in enumerate(rows, 2):
        for c_idx, h in enumerate(headers, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=excel_safe_text(row.get(h, "")))
            _style_cell(cell)
    for idx, header in enumerate(headers, 1):
        values = [str(header)] + [str(r.get(header, "")) for r in rows[:2000]]
        ws.column_dimensions[get_column_letter(idx)].width = min(max(len(v) for v in values) + 3, 45)


def build_xlsx(result: dict) -> io.BytesIO:
    wb = Workbook()
    ws = wb.active
    ws.title = "Reconciliation"

    ws["A1"] = excel_safe_text("Grambook Reconciliation Report")
    ws["A1"].font = Font(bold=True, size=14)
    ws["A2"] = excel_safe_text(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    ws["A2"].font = Font(italic=True, size=9)
    ws["A4"] = excel_safe_text("Summary")
    ws["A4"].font = Font(bold=True, size=12)

    stats = result.get("stats", {})
    row = 5
    for label, key in [
        ("Total Records", "total"), ("Matched", "matched"),
        ("Discrepancies", "disc"), ("Only in Admin", "only_a"),
        ("Only in Suvidha", "only_s"),
    ]:
        ws[f"A{row}"] = excel_safe_text(label)
        ws[f"B{row}"] = stats.get(key, 0)
        row += 1

    discrepancies = result.get("discrepancies", [])
    admin_cols = result.get("admin_cols", [])

    if discrepancies and admin_cols:
        row += 2
        ws[f"A{row}"] = excel_safe_text("Discrepancies")
        ws[f"A{row}"].font = Font(bold=True, size=11)
        row += 1
        for col_idx, col in enumerate(admin_cols, 1):
            cell = ws.cell(row=row, column=col_idx, value=excel_safe_text(col))
            cell.fill = PatternFill(start_color="FFE6E6", end_color="FFE6E6", fill_type="solid")
            cell.font = Font(bold=True)
        for disc in discrepancies:
            row += 1
            for col_idx, col in enumerate(admin_cols, 1):
                cell = ws.cell(row=row, column=col_idx,
                               value=excel_safe_text(disc.get("admin_row", {}).get(col, "")))
                if col in disc.get("diffs", {}):
                    cell.fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
                    cell.font = Font(color="FFFFFF", bold=True)

    for col in range(1, len(admin_cols) + 1):
        ws.column_dimensions[get_column_letter(col)].width = 20

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


# ──────────────────────────────────────────────────────────────────────────────
# Flask routes
# ──────────────────────────────────────────────────────────────────────────────

def _parse_optional_int(form_value: str | None) -> int | None:
    if form_value is None:
        return None
    val = str(form_value).strip()
    if not val:
        return None
    if not re.fullmatch(r"\d+", val):
        raise ReconciliationError("Header override values must be positive integers.")
    out = int(val)
    if out <= 0:
        raise ReconciliationError("Header override values must be greater than 0.")
    return out


def _resolve_key_column(selected_key: str, available_columns: list[str]) -> str:
    selected_clean = canonical_text(selected_key)
    if selected_clean in available_columns:
        return selected_clean
    target_norm = normalize_column_key(selected_clean)
    for col in available_columns:
        if normalize_column_key(col) == target_norm:
            return col
    raise ReconciliationError(f"Key column '{selected_key}' not found in detected columns.")


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/columns", methods=["POST"])
def get_columns():
    admin_file = request.files.get("admin_file")
    suv_file = request.files.get("suvidha_file")
    if not admin_file or not suv_file:
        return jsonify({"error": "Both files are required."}), 400
    try:
        admin = parse_uploaded_dataset(admin_file,
                                       _parse_optional_int(request.form.get("admin_header_row")),
                                       _parse_optional_int(request.form.get("admin_header_span")))
        suv = parse_uploaded_dataset(suv_file,
                                     _parse_optional_int(request.form.get("suv_header_row")),
                                     _parse_optional_int(request.form.get("suv_header_span")))
        return jsonify({
            "admin_cols": admin.columns,
            "admin_col_meta": admin.column_meta,
            "suv_cols": suv.columns,
            "suv_col_meta": suv.column_meta,
            "preview": {
                "admin": {
                    "detected_header_row": admin.header_row_index + 1,
                    "detected_header_span": admin.header_row_span,
                    "notes": admin.parser_notes,
                },
                "suvidha": {
                    "detected_header_row": suv.header_row_index + 1,
                    "detected_header_span": suv.header_row_span,
                    "notes": suv.parser_notes,
                },
            },
        })
    except ReconciliationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to parse files: {e}"}), 500


@app.route("/api/header-preview", methods=["POST"])
def header_preview():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "File is required."}), 400
    try:
        parsed = parse_uploaded_dataset(
            file,
            _parse_optional_int(request.form.get("header_row")),
            _parse_optional_int(request.form.get("header_span")),
        )
        return jsonify({
            "columns": parsed.columns,
            "col_meta": parsed.column_meta,
            "header_row": parsed.header_row_index + 1,
            "header_span": parsed.header_row_span,
            "sample_rows": parsed.rows[:10],
            "notes": parsed.parser_notes,
        })
    except ReconciliationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Preview failed: {e}"}), 500


@app.route("/api/reconcile", methods=["POST"])
def run_reconcile():
    admin_file = request.files.get("admin_file")
    suv_file = request.files.get("suvidha_file")
    admin_key_raw = request.form.get("admin_key", "").strip()
    suv_key_raw = request.form.get("suv_key", "").strip()

    if not admin_file or not suv_file:
        return jsonify({"error": "Both files are required."}), 400
    if not admin_key_raw or not suv_key_raw:
        return jsonify({"error": "Key columns must be selected."}), 400

    try:
        admin = parse_uploaded_dataset(admin_file,
                                       _parse_optional_int(request.form.get("admin_header_row")),
                                       _parse_optional_int(request.form.get("admin_header_span")))
        suv = parse_uploaded_dataset(suv_file,
                                     _parse_optional_int(request.form.get("suv_header_row")),
                                     _parse_optional_int(request.form.get("suv_header_span")))

        admin_key = _resolve_key_column(admin_key_raw, admin.columns)
        suv_key = _resolve_key_column(suv_key_raw, suv.columns)

        result = reconcile(admin.rows, admin.columns, suv.rows, suv.columns, admin_key, suv_key)
        result["ingestion"] = {
            "admin": {
                "detected_header_row": admin.header_row_index + 1,
                "detected_header_span": admin.header_row_span,
                "notes": admin.parser_notes,
            },
            "suvidha": {
                "detected_header_row": suv.header_row_index + 1,
                "detected_header_span": suv.header_row_span,
                "notes": suv.parser_notes,
            },
        }
        return jsonify(result)

    except ReconciliationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Reconciliation failed: {e}"}), 500


@app.route("/api/download", methods=["POST"])
def download():
    admin_file = request.files.get("admin_file")
    suv_file = request.files.get("suvidha_file")
    admin_key_raw = request.form.get("admin_key", "").strip()
    suv_key_raw = request.form.get("suv_key", "").strip()

    if not admin_file or not suv_file or not admin_key_raw or not suv_key_raw:
        return jsonify({"error": "Both files and key columns are required."}), 400

    try:
        # Read raw bytes before file_storage is consumed by parse_uploaded_dataset
        admin_bytes = admin_file.read()
        admin_file.seek(0)
        suv_file.seek(0)

        admin = parse_uploaded_dataset(admin_file,
                                       _parse_optional_int(request.form.get("admin_header_row")),
                                       _parse_optional_int(request.form.get("admin_header_span")))
        suv = parse_uploaded_dataset(suv_file,
                                     _parse_optional_int(request.form.get("suv_header_row")),
                                     _parse_optional_int(request.form.get("suv_header_span")))

        admin_key = _resolve_key_column(admin_key_raw, admin.columns)
        suv_key = _resolve_key_column(suv_key_raw, suv.columns)

        result = reconcile(admin.rows, admin.columns, suv.rows, suv.columns, admin_key, suv_key)

        # Return the original file with discrepant cells highlighted in red.
        # This preserves the multi-header structure exactly as it was uploaded.
        buf = highlight_excel_fast(admin_bytes, result, admin)

    except ReconciliationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        import traceback
        print(f"Download error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": f"Failed to generate Excel report: {e}"}), 500

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(
        buf,
        as_attachment=True,
        download_name=f"grambook_reconciliation_{ts}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    os.makedirs("static", exist_ok=True)
    print("\nGrambook Reconciliation Tool")
    print("http://localhost:5000\n")
    app.run(debug=True, port=5000)