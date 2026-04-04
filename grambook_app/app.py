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

# FIX (High): Set a 50 MB upload cap to prevent memory exhaustion / DoS.
# Adjust the value to whatever your server can safely handle.
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB


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
    # Tracks which 0-based original column each parsed column maps to.
    kept_indices: list[int] = field(default_factory=list)
    # Tracks which original Excel row (1-based) each parsed row came from.
    excel_row_numbers: list[int] = field(default_factory=list)
    # Maps dataframe row index to actual Excel row number.
    row_position_map: dict[int, int] = field(default_factory=dict)
    # Explicit mapping between dataframe columns and Excel coordinates.
    column_position_map: dict[str, dict[str, int]] = field(default_factory=dict)


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

# FIX (Critical): ZERO_WIDTH_RE must be defined BEFORE canonical_text() so that
# any future module-level call to canonical_text() cannot hit a NameError.
EXCEL_ILLEGAL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF]")


def canonical_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"[\r\n]+", " ", text)
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(INDIC_DIGIT_MAP)
    text = ZERO_WIDTH_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def excel_safe_text(value: Any) -> str:
    return EXCEL_ILLEGAL_CHARS_RE.sub("", canonical_text(value))


def canonical_numeric_text(value: Any) -> str | None:
    text = canonical_text(value)
    if not text:
        return None
    text = text.replace(",", "")
    try:
        num = float(text)
    except (TypeError, ValueError):
        return None
    if not (num == num and num not in (float("inf"), float("-inf"))):
        return None
    if abs(num - round(num)) < 1e-9:
        return str(int(round(num)))
    try:
        dec = Decimal(text)
        normalized = format(dec.normalize(), "f").rstrip("0").rstrip(".")
        return normalized or "0"
    except (InvalidOperation, ValueError):
        return text


def canonical_compare_value(value: Any) -> str:
    numeric = canonical_numeric_text(value)
    if numeric is not None:
        return numeric
    return canonical_text(value)


# FIX (Critical): Only ONE definition of normalize_key_value is kept.
# The original had two definitions — the first (typed) at ~line 215 and a second
# bare def at ~line 747. Python silently uses whichever is defined last, making
# the first one dead code. Keeping only the correct, typed version here.
def normalize_key_value(value: Any) -> str:
    return canonical_text(value).strip().lower()


def _is_zero_or_blank(value: Any) -> bool:
    """
    Returns True for None, '', '0', '0.0', '0.00' and any zero-valued number.
    These are treated as equivalent during reconciliation so that a cell
    containing 0.0 in one file does not raise a false mismatch against an empty
    cell in the other file.
    """
    text = canonical_text(value)
    if not text:
        return True
    return canonical_numeric_text(text) == "0"


def values_equivalent_for_compare(left: Any, right: Any) -> bool:
    # Both blank/zero → match (no false mismatch for 0.0 vs "")
    if _is_zero_or_blank(left) and _is_zero_or_blank(right):
        return True
    return canonical_compare_value(left) == canonical_compare_value(right)


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
    Detects that a row is a repeating sub-header row (e.g. બાકી ચાલુ કુલ cycling
    across many columns). Such a row should NOT be the header_start; the row above
    it (containing the category labels) is the real start, with span = 2.
    """
    non_empty = [x for x in row if x]
    if len(non_empty) < 4:
        return False
    unique_vals = set(non_empty)
    repetition_ratio = 1.0 - len(unique_vals) / len(non_empty)
    if repetition_ratio < 0.40:
        return False
    text_ratio = sum(1 for x in non_empty if not is_numeric_like(x)) / len(non_empty)
    return text_ratio >= 0.90


def detect_header_start(
    matrix: list[list[str]], manual_header_row: int | None = None
) -> tuple[int, int | None]:
    """
    Returns (header_start_0indexed, forced_span_or_None).
    forced_span is set to 2 when a multi-header layout is detected automatically.
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
    When span ≥ 2, each column name is built by joining ALL non-empty, non-duplicate
    label parts from every header row (category row + sub-header row).
    """
    if not matrix:
        raise ReconciliationError("Empty matrix.")

    max_cols = max(len(r) for r in matrix)
    local_rows: list[list[str]] = []
    for i in range(header_start, min(header_start + 5, len(matrix))):
        row = (matrix[i] if i < len(matrix) else []) + [""] * max_cols
        local_rows.append(_forward_fill_header_cells(row[:max_cols]))

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

    header_rows = local_rows[:chosen_span]
    raw_headers: list[str] = []
    for c in range(max_cols):
        parts = _dedupe_in_order([header_rows[r][c] for r in range(chosen_span)
                                  if header_rows[r][c]])
        raw_headers.append(canonical_text(" ".join(parts)))

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
    aligned_rows: list[list[str]] = []
    excel_row_numbers: list[int] = []
    row_position_map: dict[int, int] = {}

    for rel_idx, row in enumerate(data_rows):
        aligned = _align_row(row, max_cols)
        row_values = [canonical_text(aligned[i]) for i in kept_indices]
        if not any(row_values):
            continue
        df_row_index = len(aligned_rows)
        aligned_rows.append(row_values)
        actual_excel_row = header_start + header_span + rel_idx + 1
        excel_row_numbers.append(actual_excel_row)
        row_position_map[df_row_index] = actual_excel_row

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

    rows = [dict(zip(headers, vals)) for vals in aligned_rows]
    column_position_map = {
        headers[i]: {"df_index": i, "excel_col": kept_indices[i] + 1}
        for i in range(len(headers))
    }

    parser_notes: list[str] = []
    if source_format == "xls":
        parser_notes.append("Legacy .xls format parsed via xlrd.")
    parser_notes.append(f"Row mapping generated for {len(excel_row_numbers)} data rows.")

    return ParsedDataset(
        rows=rows,
        columns=headers,
        column_meta=column_meta,
        normalized_map=normalized_map,
        header_row_index=header_start,
        header_row_span=header_span,
        dropped_columns=[str(i) for i in drop_indices],
        source_format=source_format,
        parser_notes=parser_notes,
        kept_indices=kept_indices,
        excel_row_numbers=excel_row_numbers,
        row_position_map=row_position_map,
        column_position_map=column_position_map,
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


def _build_key_index(
    rows: list[dict[str, str]],
    key_col: str,
    row_position_map: dict[int, int],
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    idx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing = 0
    for i, row in enumerate(rows):
        key = normalize_key_value(row.get(key_col, ""))
        if not key:
            missing += 1
            continue
        excel_row = row_position_map.get(i, -1)
        idx[key].append(
            {
                "df_row_index": i,
                "excel_row": excel_row,
                "row": row,
                "display_key": canonical_text(row.get(key_col, "")),
            }
        )
    return idx, missing


def reconcile(
    admin: ParsedDataset,
    suv: ParsedDataset,
    admin_key: str,
    suv_key: str,
) -> dict[str, Any]:
    admin_idx, admin_missing_keys = _build_key_index(admin.rows, admin_key, admin.row_position_map)
    suv_idx, suv_missing_keys = _build_key_index(suv.rows, suv_key, suv.row_position_map)

    col_pairs = map_columns_smart(admin.columns, suv.columns, admin_key, suv_key)
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

        a_item = a_rows[0]
        s_item = s_rows[0]
        a = a_item["row"]
        s = s_item["row"]
        diffs: dict[str, Any] = {}

        for ac, sc, confidence in col_pairs:
            av = canonical_text(a.get(ac, ""))
            sv = canonical_text(s.get(sc, ""))
            if not values_equivalent_for_compare(av, sv):
                position = admin.column_position_map.get(
                    ac,
                    {"df_index": admin.columns.index(ac), "excel_col": admin.columns.index(ac) + 1},
                )
                diffs[ac] = {
                    "admin": av,
                    "suvidha": sv,
                    "suv_col": sc,
                    "confidence": round(confidence, 4),
                    "df_index": position["df_index"],
                    "excel_col": position["excel_col"],
                }

        if diffs:
            discrepancies.append({
                "key": a_item["display_key"] or key,
                "normalized_key": key,
                "row_index": a_item["df_row_index"],
                "admin_excel_row": a_item["excel_row"],
                "suvidha_excel_row": s_item["excel_row"],
                "admin_row": a,
                "suv_row": s,
                "diffs": diffs,
            })
        else:
            matching_records += 1

    only_admin_rows = [item["row"] for key in sorted(only_admin_keys) for item in admin_idx[key]]
    only_suv_rows = [item["row"] for key in sorted(only_suv_keys) for item in suv_idx[key]]

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
        "admin_cols": admin.columns,
        "suv_cols": suv.columns,
        "admin_column_position_map": admin.column_position_map,
        "meta": {
            "compared_keys": len(common),
            "duplicate_key_conflicts": duplicate_key_conflicts,
            "admin_missing_keys": admin_missing_keys,
            "suvidha_missing_keys": suv_missing_keys,
            "unmapped_admin_cols": [
                c for c in admin.columns if c != admin_key and c not in pair_lookup
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


_REPORT_EXCLUDE_SUBSTRINGS: list[str] = [
    "ક્રમ",
    "નંબર",
]
_REPORT_EXCLUDE_NORM_KEYS: set[str] = {
    "no", "sr", "srno", "sno", "serialno", "number", "slno", "seq",
}


def _is_report_excluded_column(col_name: str) -> bool:
    text = canonical_text(col_name)
    for sub in _REPORT_EXCLUDE_SUBSTRINGS:
        if sub in text:
            return True
    nk = normalize_column_key(col_name)
    return nk in _REPORT_EXCLUDE_NORM_KEYS


def generate_discrepancy_report(
    admin: ParsedDataset,
    result: dict,
) -> io.BytesIO:
    """
    Build the discrepancy Excel report — FAST.

    Layout
    ------
    • Col 1   : "Source" label  (Admin / Suvidha)
    • Col 2+  : all admin columns (serial/no columns excluded)

    For every mismatched record pair:
      - Only the DIFFERING columns carry values in both the Admin and Suvidha rows.
      - All other columns are intentionally left BLANK so the reader's eye goes
        straight to what changed.
      - Diff cells are highlighted red-bold; blank cells have no fill/border so
        the file stays small and generates instantly.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Discrepancies"

    # ── styles (defined once, reused — not rebuilt per cell) ─────────────────
    HEADER_FILL      = "1F4E78"
    ADMIN_SRC_FILL   = "FF8A65"
    SUVIDHA_SRC_FILL = "4DD0A4"
    SEP_FILL         = "F0F0F0"

    hdr_font   = Font(bold=True, color="FFFFFF", name="Calibri", size=10)
    hdr_align  = Alignment(horizontal="center", vertical="center", wrap_text=True)
    src_align  = Alignment(horizontal="center", vertical="center")
    diff_font  = Font(bold=True, color="C0392B", name="Calibri", size=10)
    diff_align = Alignment(horizontal="left", vertical="center")
    thin_side  = Side(style="thin", color="D9D9D9")
    diff_border = Border(left=thin_side, right=thin_side,
                         top=thin_side, bottom=thin_side)
    hdr_fill        = PatternFill("solid", start_color=HEADER_FILL)
    adm_src_fill    = PatternFill("solid", start_color=ADMIN_SRC_FILL)
    suv_src_fill    = PatternFill("solid", start_color=SUVIDHA_SRC_FILL)
    adm_diff_fill   = PatternFill("solid", start_color="FDECEA")
    suv_diff_fill   = PatternFill("solid", start_color="E8F8F3")
    sep_fill_obj    = PatternFill("solid", start_color=SEP_FILL)

    # ── column list (exclude serial/number columns) ───────────────────────────
    filtered_cols: list[str] = [
        c for c in admin.columns if not _is_report_excluded_column(c)
    ] or admin.columns

    SOURCE_COL = 1
    # col_to_excel: admin column name → Excel column number (1-based, offset by Source col)
    col_to_excel: dict[str, int] = {c: i + 2 for i, c in enumerate(filtered_cols)}

    # ── header row ────────────────────────────────────────────────────────────
    src_hdr = ws.cell(row=1, column=SOURCE_COL, value="Source")
    src_hdr.font   = hdr_font
    src_hdr.fill   = hdr_fill
    src_hdr.alignment = hdr_align
    ws.column_dimensions[get_column_letter(SOURCE_COL)].width = 10

    for col_name, excel_col in col_to_excel.items():
        cell = ws.cell(row=1, column=excel_col, value=excel_safe_text(col_name))
        cell.font      = hdr_font
        cell.fill      = hdr_fill
        cell.alignment = hdr_align
        ws.column_dimensions[get_column_letter(excel_col)].width = min(
            max(len(excel_safe_text(col_name)) + 4, 12), 40
        )

    # ── discrepancy rows ──────────────────────────────────────────────────────
    current_row = 2

    for disc in result.get("discrepancies", []):
        diffs:    dict[str, Any] = disc.get("diffs", {})
        diff_set: set[str]       = set(diffs.keys())

        # ── Admin row ─────────────────────────────────────────────────────────
        src_a = ws.cell(row=current_row, column=SOURCE_COL, value="Admin")
        src_a.font      = Font(bold=True, color="FFFFFF", name="Calibri", size=10)
        src_a.fill      = adm_src_fill
        src_a.alignment = src_align

        for col_name, excel_col in col_to_excel.items():
            if col_name not in diff_set:
                # Leave blank — do NOT write value, do NOT apply fill/border
                continue
            val  = diffs[col_name].get("admin", "")
            cell = ws.cell(row=current_row, column=excel_col,
                           value=excel_safe_text(val))
            cell.font      = diff_font
            cell.fill      = adm_diff_fill
            cell.border    = diff_border
            cell.alignment = diff_align
        current_row += 1

        # ── Suvidha row ───────────────────────────────────────────────────────
        src_s = ws.cell(row=current_row, column=SOURCE_COL, value="Suvidha")
        src_s.font      = Font(bold=True, color="FFFFFF", name="Calibri", size=10)
        src_s.fill      = suv_src_fill
        src_s.alignment = src_align

        for col_name, excel_col in col_to_excel.items():
            if col_name not in diff_set:
                # Leave blank — do NOT write value, do NOT apply fill/border
                continue
            val  = diffs[col_name].get("suvidha", "")
            cell = ws.cell(row=current_row, column=excel_col,
                           value=excel_safe_text(val))
            cell.font      = diff_font
            cell.fill      = suv_diff_fill
            cell.border    = diff_border
            cell.alignment = diff_align
        current_row += 1

        # ── thin separator (only Source col coloured, rest plain) ─────────────
        sep = ws.cell(row=current_row, column=SOURCE_COL, value="")
        sep.fill = sep_fill_obj
        ws.row_dimensions[current_row].height = 3
        current_row += 1

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

        result = reconcile(admin, suv, admin_key, suv_key)
        result["ingestion"] = {
            "admin": {
                "detected_header_row": admin.header_row_index + 1,
                "detected_header_span": admin.header_row_span,
                "notes": admin.parser_notes,
                "row_map_size": len(admin.excel_row_numbers),
                "row_position_map": admin.row_position_map,
                "column_position_map": admin.column_position_map,
            },
            "suvidha": {
                "detected_header_row": suv.header_row_index + 1,
                "detected_header_span": suv.header_row_span,
                "notes": suv.parser_notes,
                "row_map_size": len(suv.excel_row_numbers),
                "row_position_map": suv.row_position_map,
                "column_position_map": suv.column_position_map,
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
        admin = parse_uploaded_dataset(admin_file,
                                       _parse_optional_int(request.form.get("admin_header_row")),
                                       _parse_optional_int(request.form.get("admin_header_span")))
        suv = parse_uploaded_dataset(suv_file,
                                     _parse_optional_int(request.form.get("suv_header_row")),
                                     _parse_optional_int(request.form.get("suv_header_span")))

        admin_key = _resolve_key_column(admin_key_raw, admin.columns)
        suv_key = _resolve_key_column(suv_key_raw, suv.columns)

        result = reconcile(admin, suv, admin_key, suv_key)
        buf = generate_discrepancy_report(admin, result)

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