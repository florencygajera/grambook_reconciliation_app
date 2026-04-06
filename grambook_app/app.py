"""
Grambook Reconciliation - Production-grade Flask Backend
Run: python app.py
Then open http://localhost:5000
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import shutil
import traceback
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

# ── Optional dependency imports (graceful degradation) ───────────────────────

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

try:
    import xlrd
except ImportError:
    xlrd = None

# ── App setup ─────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB


class ReconciliationError(Exception):
    """Raised for user-facing data / configuration problems."""


# ── Indic digit normalisation map ─────────────────────────────────────────────

INDIC_DIGIT_MAP = str.maketrans(
    {
        # Devanagari digits
        "\u0966": "0",
        "\u0967": "1",
        "\u0968": "2",
        "\u0969": "3",
        "\u096a": "4",
        "\u096b": "5",
        "\u096c": "6",
        "\u096d": "7",
        "\u096e": "8",
        "\u096f": "9",
        # Gujarati digits
        "\u0ae6": "0",
        "\u0ae7": "1",
        "\u0ae8": "2",
        "\u0ae9": "3",
        "\u0aea": "4",
        "\u0aeb": "5",
        "\u0aec": "6",
        "\u0aed": "7",
        "\u0aee": "8",
        "\u0aef": "9",
    }
)


# ── Data model ────────────────────────────────────────────────────────────────


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
    kept_indices: list[int] = field(default_factory=list)
    excel_row_numbers: list[int] = field(default_factory=list)
    row_position_map: dict[int, int] = field(default_factory=dict)
    column_position_map: dict[str, dict[str, int]] = field(default_factory=dict)


# ── Tesseract setup ───────────────────────────────────────────────────────────


def _iter_registry_tesseract_paths() -> list[str]:
    """Return Tesseract executable paths found via Windows registry."""
    if winreg is None:
        return []
    roots = [
        (
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall",
        ),
        (
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall",
        ),
        (
            winreg.HKEY_CURRENT_USER,
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall",
        ),
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
                                display_name = winreg.QueryValueEx(
                                    child, "DisplayName"
                                )[0]
                            except OSError:
                                continue
                            if "tesseract" not in str(display_name).lower():
                                continue
                            for value_name in ("InstallLocation", "UninstallString"):
                                try:
                                    raw = (
                                        str(winreg.QueryValueEx(child, value_name)[0])
                                        .strip()
                                        .strip('"')
                                    )
                                except OSError:
                                    continue
                                if not raw:
                                    continue
                                candidate = (
                                    raw
                                    if raw.lower().endswith(".exe")
                                    else os.path.join(raw, "tesseract.exe")
                                )
                                if (
                                    os.path.basename(candidate).lower()
                                    == "tesseract-uninstall.exe"
                                ):
                                    candidate = os.path.join(
                                        os.path.dirname(candidate), "tesseract.exe"
                                    )
                                if os.path.isdir(candidate):
                                    candidate = os.path.join(candidate, "tesseract.exe")
                                found.append(candidate)
                    except OSError:
                        break
        except OSError:
            continue
    return found


def _configure_tesseract() -> None:
    """Locate and configure tesseract command path."""
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
        candidates.append(
            os.path.join(local_app_data, "Programs", "Tesseract-OCR", "tesseract.exe")
        )
    candidates.extend(
        [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        ]
    )
    candidates.extend(_iter_registry_tesseract_paths())
    for cmd in candidates:
        if cmd and os.path.isfile(cmd):
            pytesseract.pytesseract.tesseract_cmd = cmd
            tessdata_dir = os.path.join(os.path.dirname(cmd), "tessdata")
            if os.path.isdir(tessdata_dir) and not os.getenv("TESSDATA_PREFIX"):
                os.environ["TESSDATA_PREFIX"] = tessdata_dir
            break


_configure_tesseract()


# ── Text normalisation helpers ────────────────────────────────────────────────

EXCEL_ILLEGAL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF]")


def canonical_text(value: Any) -> str:
    """Normalize any value to a clean unicode string."""
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
    """
    Return a canonical numeric string if *value* is numeric, else None.
    Strips commas, trailing zeros, and handles Decimal precision safely.
    """
    text = canonical_text(value)
    if not text:
        return None
    text = text.replace(",", "")
    try:
        num = float(text)
    except (TypeError, ValueError):
        return None
    # Reject NaN / Inf
    if num != num or num in (float("inf"), float("-inf")):
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


def normalize_key_value(value: Any) -> str:
    """Normalize a key column value for fuzzy matching."""
    text = canonical_text(value).lower()
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = text.replace("_", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _is_zero_or_blank(value: Any) -> bool:
    text = canonical_text(value)
    if not text:
        return True
    return canonical_numeric_text(text) == "0"


def values_equivalent_for_compare(left: Any, right: Any) -> bool:
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


# ── File parsing — CSV / XLS / XLSX → raw string matrix ──────────────────────


def _decode_csv_bytes(file_bytes: bytes) -> io.StringIO:
    for enc in ["utf-8-sig", "utf-8", "utf-16", "cp1252", "latin1"]:
        try:
            return io.StringIO(file_bytes.decode(enc))
        except UnicodeDecodeError:
            continue
    raise ReconciliationError("CSV encoding is unsupported or the file is corrupt.")


def _normalize_row_length(
    matrix: list[list[str]], max_cols: int | None = None
) -> list[list[str]]:
    if not matrix:
        return matrix
    if max_cols is None:
        max_cols = max(len(r) for r in matrix)
    return [
        r + [""] * (max_cols - len(r)) if len(r) < max_cols else r[:max_cols]
        for r in matrix
    ]


def _parse_csv_matrix(file_bytes: bytes) -> list[list[str]]:
    sio = _decode_csv_bytes(file_bytes)
    sample = sio.read(4096)
    sio.seek(0)
    dialect = csv.excel
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        pass
    matrix = [
        [canonical_text(cell) for cell in row] for row in csv.reader(sio, dialect)
    ]
    return _normalize_row_length(matrix)


def _parse_xls_matrix(file_bytes: bytes) -> list[list[str]]:
    # FIX: xlrd is now imported at module level; check here instead of re-importing
    if xlrd is None:
        raise ReconciliationError(
            ".xls file detected but xlrd is not installed. Run: pip install xlrd==2.0.1"
        )
    try:
        wb = xlrd.open_workbook(file_contents=file_bytes)
        sh = wb.sheet_by_index(0)
    except Exception as e:
        raise ReconciliationError(f"Unable to read .xls workbook: {e}") from e

    matrix = [
        [canonical_text(sh.cell_value(r, c)) for c in range(sh.ncols)]
        for r in range(sh.nrows)
    ]
    matrix = _normalize_row_length(matrix, sh.ncols)
    for rlo, rhi, clo, chi in getattr(sh, "merged_cells", []):
        top_left = (
            matrix[rlo][clo] if rlo < len(matrix) and clo < len(matrix[rlo]) else ""
        )
        for rr in range(rlo, rhi):
            for cc in range(clo, chi):
                if rr < len(matrix) and cc < len(matrix[rr]):
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

    # FIX: Guard against empty sheets with no data
    if max_row == 0 or max_col == 0:
        raise ReconciliationError("The uploaded .xlsx sheet appears to be empty.")

    matrix = [["" for _ in range(max_col)] for _ in range(max_row)]

    for r in range(1, max_row + 1):
        for c in range(1, max_col + 1):
            matrix[r - 1][c - 1] = canonical_text(ws.cell(row=r, column=c).value)

    for mrange in ws.merged_cells.ranges:
        top_left = matrix[mrange.min_row - 1][mrange.min_col - 1]
        for rr in range(mrange.min_row - 1, mrange.max_row):
            for cc in range(mrange.min_col - 1, mrange.max_col):
                if rr < len(matrix) and cc < len(matrix[rr]):
                    matrix[rr][cc] = top_left

    images = getattr(ws, "_images", [])
    if images:
        if Image is None or pytesseract is None:
            notes.append(
                "Image(s) detected but OCR dependencies (Pillow / pytesseract) are unavailable."
            )
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


def parse_matrix_from_upload(
    file_storage,
) -> tuple[list[list[str]], str, list[str]]:
    """Read an uploaded FileStorage and return (matrix, format_name, notes)."""
    filename = (file_storage.filename or "").lower().strip()
    file_bytes = file_storage.read()
    if not file_bytes:
        raise ReconciliationError("Uploaded file is empty.")

    # FIX: Also detect format by magic bytes when extension is ambiguous
    if filename.endswith(".csv"):
        return _parse_csv_matrix(file_bytes), "csv", []
    if filename.endswith(".xlsx"):
        matrix, notes = _parse_xlsx_matrix(file_bytes)
        return matrix, "xlsx", notes
    if filename.endswith(".xls"):
        return _parse_xls_matrix(file_bytes), "xls", []

    # Fallback: sniff by magic bytes (PK = xlsx/zip, \xD0\xCF = xls)
    if file_bytes[:2] == b"PK":
        matrix, notes = _parse_xlsx_matrix(file_bytes)
        return matrix, "xlsx", notes
    if file_bytes[:2] == b"\xd0\xcf":
        return _parse_xls_matrix(file_bytes), "xls", []

    raise ReconciliationError(
        "Unsupported file format. Please upload a .csv, .xls, or .xlsx file."
    )


# ── Header detection ──────────────────────────────────────────────────────────


def _row_header_score(row: list[str]) -> float:
    non_empty = [canonical_text(x) for x in row if canonical_text(x)]
    if len(non_empty) < 2:
        return -1.0
    text_cells = sum(1 for x in non_empty if not is_numeric_like(x))
    text_ratio = text_cells / max(1, len(non_empty))
    avg_len = sum(len(x) for x in non_empty) / len(non_empty)
    long_penalty = sum(1 for x in non_empty if len(x) > 80)
    return (
        len(non_empty) * 0.25 + text_ratio * 2.5 - (avg_len / 120) - long_penalty * 1.2
    )


def _is_sub_header_row(row: list[str]) -> bool:
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
    if not matrix:
        raise ReconciliationError("File has no rows.")

    if manual_header_row is not None:
        idx = manual_header_row - 1
        if idx < 0 or idx >= len(matrix):
            raise ReconciliationError(
                f"Manual header row {manual_header_row} is out of range "
                f"(file has {len(matrix)} rows)."
            )
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
        text_ratio = sum(1 for x in non_empty if not is_numeric_like(x)) / len(
            non_empty
        )
        if text_ratio < 0.55:
            continue
        if any(len(x) > 140 for x in non_empty):
            continue
        scored.append((i, score))

    if not scored:
        raise ReconciliationError(
            "Could not detect a valid header row. "
            "Please use the manual header row override."
        )

    scored.sort(key=lambda x: x[1], reverse=True)
    best_idx = scored[0][0]

    if _is_sub_header_row(matrix[best_idx]) and best_idx > 0:
        prev = matrix[best_idx - 1]
        prev_non_empty = [x for x in prev if x]
        if (
            prev_non_empty
            and sum(1 for x in prev_non_empty if not is_numeric_like(x))
            / len(prev_non_empty)
            >= 0.9
        ):
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
        header_value = (
            headers[out_idx] if out_idx < len(headers) else f"column_{out_idx}"
        )
        parts = _dedupe_in_order(
            [
                canonical_text(hr[original_col_idx])
                for hr in header_rows
                if original_col_idx < len(hr)
            ]
        )
        if not parts:
            parts = [header_value]
        group = parts[0] if len(parts) > 1 else "Other"
        out.append(
            {
                "column": header_value,
                "group": group,
                "parts": parts,
                "hierarchy": " > ".join(parts),
                "hierarchy_short": " > ".join(parts[1:])
                if len(parts) > 1
                else header_value,
            }
        )
    return out


def build_headers(
    matrix: list[list[str]],
    header_start: int,
    manual_header_span: int | None = None,
    forced_span: int | None = None,
) -> tuple[list[str], dict[str, str], list[int], int]:
    if not matrix:
        raise ReconciliationError("Empty matrix — cannot build headers.")

    max_cols = max(len(r) for r in matrix)
    local_rows: list[list[str]] = []
    for i in range(header_start, min(header_start + 5, len(matrix))):
        row = (matrix[i] if i < len(matrix) else []) + [""] * max_cols
        local_rows.append(_forward_fill_header_cells(row[:max_cols]))

    # FIX: Ensure local_rows is never empty before span selection
    if not local_rows:
        raise ReconciliationError(
            "Header row is at the very end of the file — no data rows follow."
        )

    if forced_span is not None and manual_header_span is None:
        chosen_span = forced_span
    else:
        spans = (
            [manual_header_span]
            if manual_header_span in (1, 2, 3, 4, 5)
            else [5, 4, 3, 2, 1]
        )
        chosen_span = 1
        best_score = -1.0

        for span in spans:
            if header_start + span > len(matrix):
                continue
            # FIX: Don't access local_rows[r] beyond its actual length
            if span > len(local_rows):
                continue
            combined = []
            for c in range(max_cols):
                parts = _dedupe_in_order(
                    [
                        local_rows[r][c]
                        for r in range(span)
                        if r < len(local_rows) and local_rows[r][c]
                    ]
                )
                combined.append(" ".join(parts).strip())

            non_empty = sum(1 for h in combined if h)
            score = non_empty - sum(1 for h in combined if len(h) > 90) * 1.5

            multi_header_bonus = 0
            for row_idx in range(span):
                if row_idx >= len(local_rows):
                    break
                row_text = " ".join(local_rows[row_idx]).lower()
                if any(
                    k in row_text
                    for k in [
                        "baki",
                        "test",
                        "type",
                        "category",
                        "status",
                        "remark",
                        "sr no",
                        "serial",
                        "બાકી",
                        "ચાલુ",
                        "કુલ",
                    ]
                ):
                    multi_header_bonus += 2
                if any(
                    k in row_text
                    for k in [
                        "no",
                        "number",
                        "id",
                        "code",
                        "name",
                        "ક્રમ",
                        "નંબર",
                    ]
                ):
                    multi_header_bonus += 1
            score += multi_header_bonus

            if score > best_score:
                best_score = score
                chosen_span = span

    header_rows = local_rows[:chosen_span]
    raw_headers: list[str] = []
    for c in range(max_cols):
        parts = _dedupe_in_order(
            [header_rows[r][c] for r in range(len(header_rows)) if header_rows[r][c]]
        )
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

    header_start, forced_span = detect_header_start(
        matrix, manual_header_row=manual_header_row
    )
    headers, normalized_map, drop_indices, header_span = build_headers(
        matrix,
        header_start,
        manual_header_span=manual_header_span,
        forced_span=forced_span,
    )

    kept_indices = [i for i in range(max_cols) if i not in set(drop_indices)]
    if not kept_indices:
        raise ReconciliationError("No usable columns detected after header processing.")

    data_rows = matrix[header_start + header_span :]
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

    # FIX: Realign headers / kept_indices length mismatch gracefully
    if len(headers) != len(kept_indices):
        logger.warning(
            "Header count (%d) != kept column count (%d). "
            "Falling back to generic column names.",
            len(headers),
            len(kept_indices),
        )
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
    column_position_map: dict[str, dict[str, int]] = {
        headers[i]: {"df_index": i, "excel_col": kept_indices[i] + 1}
        for i in range(len(headers))
    }

    parser_notes: list[str] = []
    if source_format == "xls":
        parser_notes.append("Legacy .xls format parsed via xlrd.")
    parser_notes.append(
        f"Row mapping generated for {len(excel_row_numbers)} data rows."
    )

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


# ── Reconciliation ────────────────────────────────────────────────────────────


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if fuzz is not None:
        return float(fuzz.ratio(a, b)) / 100.0
    return SequenceMatcher(None, a, b).ratio()


def map_columns_smart(
    admin_cols: list[str],
    suv_cols: list[str],
    admin_key: str,
    suv_key: str,
) -> list[tuple[str, str, float]]:
    """Map admin columns to Suvidha columns by name similarity."""
    admin_candidates = [c for c in admin_cols if c != admin_key]
    suv_candidates = [c for c in suv_cols if c != suv_key]
    suv_norm = {normalize_column_key(c): c for c in suv_candidates}
    used_suv: set[str] = set()
    mapped: list[tuple[str, str, float]] = []

    # Pass 1 — exact normalized match
    for ac in admin_candidates:
        sc = suv_norm.get(normalize_column_key(ac))
        if sc and sc not in used_suv:
            mapped.append((ac, sc, 1.0))
            used_suv.add(sc)

    # Pass 2 — fuzzy match (>= 85 % similarity)
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
        if best_sc and best_score >= 0.85:
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
    """Core reconciliation logic."""

    def _decimal_from_value(value: Any) -> Decimal | None:
        num = canonical_numeric_text(value)
        if num is None:
            return None
        try:
            return Decimal(num)
        except (InvalidOperation, ValueError):
            return None

    def _decimal_to_text(value: Decimal) -> str:
        text = format(value.normalize(), "f").rstrip("0").rstrip(".")
        return "0" if not text or text == "-0" else text

    def _detect_numeric_columns(
        rows: list[dict[str, str]], columns: list[str], key_col: str
    ) -> set[str]:
        hints = (
            "amount",
            "tax",
            "total",
            "balance",
            "baki",
            "paid",
            "due",
            "fee",
            "debit",
            "credit",
        )
        numeric_cols: set[str] = set()
        for col in columns:
            if col == key_col:
                continue
            nk = normalize_column_key(col)
            if any(h in nk for h in hints):
                numeric_cols.add(col)
                continue
            sample = [
                canonical_text(r.get(col, ""))
                for r in rows
                if canonical_text(r.get(col, ""))
            ]
            if not sample:
                continue
            numeric_count = sum(1 for v in sample if _decimal_from_value(v) is not None)
            if (numeric_count / len(sample)) >= 0.8:
                numeric_cols.add(col)
        return numeric_cols

    def _aggregate_group(
        entries: list[dict[str, Any]],
        columns: list[str],
        numeric_columns: set[str],
    ) -> dict[str, Any]:
        agg_row: dict[str, str] = {}
        numeric_totals: dict[str, Decimal] = {}
        for col in columns:
            values = [e["row"].get(col, "") for e in entries]
            if col in numeric_columns:
                total = Decimal("0")
                found = False
                for value in values:
                    dec = _decimal_from_value(value)
                    if dec is not None:
                        total += dec
                        found = True
                if found:
                    agg_row[col] = _decimal_to_text(total)
                    numeric_totals[col] = total
                else:
                    agg_row[col] = ""
            else:
                chosen = ""
                for value in values:
                    txt = canonical_text(value)
                    if txt:
                        chosen = txt
                        break
                agg_row[col] = chosen
        return {
            "row": agg_row,
            "numeric_totals": numeric_totals,
            "display_key": next(
                (e["display_key"] for e in entries if e.get("display_key")), ""
            ),
            "df_row_index": entries[0]["df_row_index"] if entries else -1,
            "excel_rows": [
                e["excel_row"] for e in entries if e.get("excel_row", -1) > 0
            ],
            "count": len(entries),
        }

    admin_idx, admin_missing_keys = _build_key_index(
        admin.rows, admin_key, admin.row_position_map
    )
    suv_idx, suv_missing_keys = _build_key_index(
        suv.rows, suv_key, suv.row_position_map
    )

    col_pairs = map_columns_smart(admin.columns, suv.columns, admin_key, suv_key)
    pair_lookup = {a: s for a, s, _ in col_pairs}

    admin_numeric_cols = _detect_numeric_columns(admin.rows, admin.columns, admin_key)
    suv_numeric_cols = _detect_numeric_columns(suv.rows, suv.columns, suv_key)
    identifier_columns = [
        c
        for c in admin.columns
        if c == admin_key
        or normalize_column_key(c)
        in ("name", "naam", "id", "code", "account", "acc", "member")
    ]

    admin_keys = set(admin_idx)
    suv_keys = set(suv_idx)
    common = admin_keys & suv_keys
    only_admin_keys = admin_keys - suv_keys
    only_suv_keys = suv_keys - admin_keys

    discrepancies: list[dict[str, Any]] = []
    mismatch_details: list[dict[str, Any]] = []
    matching_records = 0
    duplicate_key_conflicts: list[dict[str, Any]] = []

    for key in sorted(common):
        a_rows = admin_idx[key]
        s_rows = suv_idx[key]

        if len(a_rows) != 1 or len(s_rows) != 1:
            duplicate_key_conflicts.append(
                {
                    "key": key,
                    "admin_count": len(a_rows),
                    "suvidha_count": len(s_rows),
                }
            )

        a_agg = _aggregate_group(a_rows, admin.columns, admin_numeric_cols)
        s_agg = _aggregate_group(s_rows, suv.columns, suv_numeric_cols)
        a = a_agg["row"]
        s = s_agg["row"]
        diffs: dict[str, Any] = {}

        for ac, sc, confidence in col_pairs:
            is_numeric = ac in admin_numeric_cols or sc in suv_numeric_cols
            # FIX: Use .get() with a safe default instead of dict.index() which doesn't exist
            position = admin.column_position_map.get(
                ac,
                {
                    "df_index": admin.columns.index(ac) if ac in admin.columns else 0,
                    "excel_col": (admin.columns.index(ac) + 1)
                    if ac in admin.columns
                    else 1,
                },
            )
            if is_numeric:
                a_total = a_agg["numeric_totals"].get(ac, Decimal("0"))
                s_total = s_agg["numeric_totals"].get(sc, Decimal("0"))
                if a_total != s_total:
                    diffs[ac] = {
                        "admin": _decimal_to_text(a_total),
                        "suvidha": _decimal_to_text(s_total),
                        "difference": _decimal_to_text(a_total - s_total),
                        "is_numeric": True,
                        "suv_col": sc,
                        "confidence": round(confidence, 4),
                        "df_index": position["df_index"],
                        "excel_col": position["excel_col"],
                    }
            else:
                av = canonical_text(a.get(ac, ""))
                sv = canonical_text(s.get(sc, ""))
                if not values_equivalent_for_compare(av, sv):
                    diffs[ac] = {
                        "admin": av,
                        "suvidha": sv,
                        "difference": "",
                        "is_numeric": False,
                        "suv_col": sc,
                        "confidence": round(confidence, 4),
                        "df_index": position["df_index"],
                        "excel_col": position["excel_col"],
                    }

        if diffs:
            display_key = a_agg["display_key"] or s_agg["display_key"] or key
            discrepancies.append(
                {
                    "id": display_key,
                    "normalized_id": key,
                    "admin_excel_rows": a_agg["excel_rows"],
                    "suvidha_excel_rows": s_agg["excel_rows"],
                    "admin_count": a_agg["count"],
                    "suvidha_count": s_agg["count"],
                    "changed_columns": sorted(diffs.keys()),
                    "mismatch_count": len(diffs),
                }
            )
            for admin_col, diff in diffs.items():
                mismatch_details.append(
                    {
                        "id": display_key,
                        "normalized_id": key,
                        "column": admin_col,
                        "suvidha_column": diff.get("suv_col", admin_col),
                        "admin_value": diff.get("admin", ""),
                        "suvidha_value": diff.get("suvidha", ""),
                        "difference": diff.get("difference", ""),
                        "is_numeric": bool(diff.get("is_numeric")),
                        "admin_excel_rows": a_agg["excel_rows"],
                        "suvidha_excel_rows": s_agg["excel_rows"],
                    }
                )
        else:
            matching_records += 1

    only_admin_rows = [
        item["row"] for k in sorted(only_admin_keys) for item in admin_idx[k]
    ]
    only_suv_rows = [item["row"] for k in sorted(only_suv_keys) for item in suv_idx[k]]
    mismatch_details.sort(
        key=lambda item: (
            canonical_text(item.get("normalized_id", "")),
            canonical_text(item.get("column", "")),
        )
    )

    return {
        "discrepancies": discrepancies,
        "mismatch_details": mismatch_details,
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
            "identifier_columns": identifier_columns,
            "admin_numeric_columns": sorted(admin_numeric_cols),
            "suvidha_numeric_columns": sorted(suv_numeric_cols),
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


# ── Excel report generation ───────────────────────────────────────────────────


def _border() -> Border:
    s = Side(style="thin", color="D9D9D9")
    return Border(left=s, right=s, top=s, bottom=s)


def _style_cell(
    cell,
    *,
    fill_hex: str | None = None,
    bold: bool = False,
    color: str = "1F2937",
    align: str = "left",
    wrap: bool = False,
) -> None:
    if fill_hex:
        cell.fill = PatternFill("solid", start_color=fill_hex)
    cell.font = Font(bold=bold, color=color, name="Calibri", size=10)
    cell.alignment = Alignment(horizontal=align, vertical="center", wrap_text=wrap)
    cell.border = _border()


def _pick_column(columns: list[str], hints: tuple[str, ...]) -> str:
    """Return the first column whose normalized key contains any hint fragment."""
    for col in columns:
        nk = normalize_column_key(col)
        if any(h in nk for h in hints):
            return col
    return ""


# FIX: Mojibake column-name constants replaced with proper Unicode strings.
# Original code had garbled byte sequences like "????????????" which arose from
# accidental double-encoding of Gujarati text (??????????? / ?????? etc.).
_NAME_HINTS = ("name", "naam", "holder", "owner", "નામ", "ધારક")
_NUMBER_HINTS = ("number", "nambar", "no", "account", "નંબર", "ખાતા")
_KRAM_HINTS = ("kram", "serial", "sr", "seq", "ક્રમ", "અ.નં")

# FIX: Removed unused _REPORT_EXCLUDE_* and _IDENTIFIER_NORM_FRAGMENTS constants
# that were never consumed by generate_discrepancy_report (the function that
# replaced the old, broken one).


def generate_discrepancy_report(
    admin: ParsedDataset,
    result: dict,
) -> io.BytesIO:
    """
    Generate a styled .xlsx discrepancy report.

    Sheets:
      1. Summary       — overall stats
      2. Discrepancies — side-by-side Admin vs Suvidha rows, highlighted diffs
      3. Only in Admin — rows missing from Suvidha
      4. Only in Suvidha — rows missing from Admin
    """
    wb = Workbook()

    # ── Colour palette ────────────────────────────────────────────────────────
    C_HDR_BG = "1A1814"  # dark header bg
    C_HDR_FG = "FFFFFF"
    C_ADM_BG = "FDF1ED"  # admin row tint
    C_ADM_DIFF = "F5C4B3"  # admin diff cell
    C_ADM_FG = "B5451B"
    C_SUV_BG = "EDF5F1"  # suvidha row tint
    C_SUV_DIFF = "B8E3D0"  # suvidha diff cell
    C_SUV_FG = "1A6B4A"
    C_OK_BG = "EDF5F1"
    C_OK_FG = "1A6B4A"
    C_WARN_BG = "FDF1ED"
    C_WARN_FG = "B5451B"
    C_NEUTRAL = "F8F7F4"

    name_col = _pick_column(admin.columns, _NAME_HINTS)
    number_col = _pick_column(admin.columns, _NUMBER_HINTS)
    kram_col = _pick_column(admin.columns, _KRAM_HINTS)

    # ── Sheet 1: Summary ──────────────────────────────────────────────────────
    ws_sum = wb.active
    ws_sum.title = "Summary"
    ws_sum.column_dimensions["A"].width = 30
    ws_sum.column_dimensions["B"].width = 18

    stats = result.get("stats", {})
    meta = result.get("meta", {})

    summary_rows = [
        ("Grambook Reconciliation Report", ""),
        ("Generated", datetime.now().strftime("%d %b %Y, %H:%M")),
        ("", ""),
        ("Metric", "Count"),
        ("Total unique records", stats.get("total", 0)),
        ("Matching records", stats.get("matched", 0)),
        ("Discrepancies", stats.get("disc", 0)),
        ("Only in Admin", stats.get("only_a", 0)),
        ("Only in Suvidha", stats.get("only_s", 0)),
        ("", ""),
        ("Admin key column", result.get("admin_key", "")),
        ("Suvidha key column", result.get("suv_key", "")),
        ("Columns compared", len(result.get("col_pairs", []))),
        ("Duplicate key conflicts", len(meta.get("duplicate_key_conflicts", []))),
        ("Admin rows missing key", meta.get("admin_missing_keys", 0)),
        ("Suvidha rows missing key", meta.get("suvidha_missing_keys", 0)),
    ]

    for r_idx, (label, value) in enumerate(summary_rows, start=1):
        ca = ws_sum.cell(row=r_idx, column=1, value=excel_safe_text(label))
        cb = ws_sum.cell(row=r_idx, column=2, value=value)
        if r_idx == 1:
            _style_cell(ca, fill_hex=C_HDR_BG, bold=True, color=C_HDR_FG, wrap=True)
            _style_cell(cb, fill_hex=C_HDR_BG, bold=True, color=C_HDR_FG)
            ws_sum.merge_cells(f"A1:B1")
        elif label == "Metric":
            _style_cell(ca, fill_hex="E5E2DB", bold=True)
            _style_cell(cb, fill_hex="E5E2DB", bold=True, align="center")
        elif label in ("Matching records",):
            _style_cell(ca, fill_hex=C_OK_BG, color=C_OK_FG)
            _style_cell(cb, fill_hex=C_OK_BG, color=C_OK_FG, align="center")
        elif label in ("Discrepancies", "Only in Admin", "Only in Suvidha"):
            _style_cell(ca, fill_hex=C_WARN_BG, color=C_WARN_FG)
            _style_cell(cb, fill_hex=C_WARN_BG, color=C_WARN_FG, align="center")
        elif label:
            _style_cell(ca, fill_hex=C_NEUTRAL)
            _style_cell(cb, fill_hex=C_NEUTRAL, align="center")

    # ── Sheet 2: Discrepancies ────────────────────────────────────────────────
    discrepancies = result.get("discrepancies", [])
    ws_disc = wb.create_sheet("Discrepancies")

    disc_col_headers = [
        "ID",
        "Normalized ID",
        "Mismatch Count",
        "Changed Columns",
        "Admin Excel Rows",
        "Suvidha Excel Rows",
    ]
    for ci, hdr in enumerate(disc_col_headers, start=1):
        c = ws_disc.cell(row=1, column=ci, value=excel_safe_text(hdr))
        _style_cell(c, fill_hex=C_HDR_BG, bold=True, color=C_HDR_FG)

    if discrepancies:
        for ri, disc in enumerate(discrepancies, start=2):
            ws_disc.cell(row=ri, column=1, value=excel_safe_text(disc.get("id", "")))
            ws_disc.cell(
                row=ri, column=2, value=excel_safe_text(disc.get("normalized_id", ""))
            )
            ws_disc.cell(row=ri, column=3, value=disc.get("mismatch_count", 0))
            ws_disc.cell(
                row=ri,
                column=4,
                value=excel_safe_text(", ".join(disc.get("changed_columns", []))),
            )
            ws_disc.cell(
                row=ri,
                column=5,
                value=", ".join(str(x) for x in disc.get("admin_excel_rows", [])),
            )
            ws_disc.cell(
                row=ri,
                column=6,
                value=", ".join(str(x) for x in disc.get("suvidha_excel_rows", [])),
            )
            for ci in range(1, len(disc_col_headers) + 1):
                _style_cell(ws_disc.cell(row=ri, column=ci), fill_hex=C_NEUTRAL)
    else:
        ws_disc.cell(row=2, column=1, value="No discrepancies found.")
        _style_cell(ws_disc.cell(row=2, column=1), fill_hex=C_OK_BG, color=C_OK_FG)

    # Auto-width for discrepancies sheet
    for col_cells in ws_disc.columns:
        length = max((len(str(c.value or "")) for c in col_cells), default=8)
        ws_disc.column_dimensions[get_column_letter(col_cells[0].column)].width = min(
            length + 4, 40
        )

    # ── Sheet 3: Only in Admin ────────────────────────────────────────────────
    ws_md = wb.create_sheet("Mismatch Details")
    mismatch_details = result.get("mismatch_details", [])
    detail_headers = [
        "ID",
        "Normalized ID",
        "Column",
        "Suvidha Column",
        "Admin Value",
        "Suvidha Value",
        "Difference",
        "Admin Excel Rows",
        "Suvidha Excel Rows",
    ]
    for ci, hdr in enumerate(detail_headers, start=1):
        c = ws_md.cell(row=1, column=ci, value=hdr)
        _style_cell(c, fill_hex=C_HDR_BG, bold=True, color=C_HDR_FG)

    if mismatch_details:
        for ri, item in enumerate(mismatch_details, start=2):
            ws_md.cell(row=ri, column=1, value=excel_safe_text(item.get("id", "")))
            ws_md.cell(
                row=ri, column=2, value=excel_safe_text(item.get("normalized_id", ""))
            )
            ws_md.cell(row=ri, column=3, value=excel_safe_text(item.get("column", "")))
            ws_md.cell(
                row=ri,
                column=4,
                value=excel_safe_text(item.get("suvidha_column", "")),
            )
            ws_md.cell(
                row=ri, column=5, value=excel_safe_text(item.get("admin_value", ""))
            )
            ws_md.cell(
                row=ri, column=6, value=excel_safe_text(item.get("suvidha_value", ""))
            )
            ws_md.cell(
                row=ri, column=7, value=excel_safe_text(item.get("difference", ""))
            )
            ws_md.cell(
                row=ri,
                column=8,
                value=", ".join(str(x) for x in item.get("admin_excel_rows", [])),
            )
            ws_md.cell(
                row=ri,
                column=9,
                value=", ".join(str(x) for x in item.get("suvidha_excel_rows", [])),
            )
            for ci in range(1, len(detail_headers) + 1):
                _style_cell(ws_md.cell(row=ri, column=ci), fill_hex=C_NEUTRAL)
    else:
        ws_md.cell(row=2, column=1, value="No mismatches found.")
        _style_cell(ws_md.cell(row=2, column=1), fill_hex=C_OK_BG, color=C_OK_FG)

    for col_cells in ws_md.columns:
        length = max((len(str(c.value or "")) for c in col_cells), default=8)
        ws_md.column_dimensions[get_column_letter(col_cells[0].column)].width = min(
            length + 4, 42
        )

    only_admin_rows = result.get("only_admin_rows", [])
    ws_oa = wb.create_sheet("Only in Admin")
    if only_admin_rows:
        oa_cols = admin.columns
        for ci, hdr in enumerate(oa_cols, start=1):
            c = ws_oa.cell(row=1, column=ci, value=excel_safe_text(hdr))
            _style_cell(c, fill_hex=C_HDR_BG, bold=True, color=C_HDR_FG)
        for ri, row in enumerate(only_admin_rows, start=2):
            for ci, col in enumerate(oa_cols, start=1):
                c = ws_oa.cell(
                    row=ri, column=ci, value=excel_safe_text(row.get(col, ""))
                )
                _style_cell(c, fill_hex=C_ADM_BG)
        for col_cells in ws_oa.columns:
            length = max((len(str(c.value or "")) for c in col_cells), default=8)
            ws_oa.column_dimensions[get_column_letter(col_cells[0].column)].width = min(
                length + 4, 40
            )
    else:
        ws_oa.cell(row=1, column=1, value="No records found only in Admin.")

    # ── Sheet 4: Only in Suvidha ──────────────────────────────────────────────
    only_suv_rows = result.get("only_suv_rows", [])
    ws_os = wb.create_sheet("Only in Suvidha")
    if only_suv_rows:
        # FIX: Use suv_cols (not admin.columns) for suvidha-only rows
        os_cols = result.get("suv_cols", admin.columns)
        for ci, hdr in enumerate(os_cols, start=1):
            c = ws_os.cell(row=1, column=ci, value=excel_safe_text(hdr))
            _style_cell(c, fill_hex=C_HDR_BG, bold=True, color=C_HDR_FG)
        for ri, row in enumerate(only_suv_rows, start=2):
            for ci, col in enumerate(os_cols, start=1):
                c = ws_os.cell(
                    row=ri, column=ci, value=excel_safe_text(row.get(col, ""))
                )
                _style_cell(c, fill_hex=C_SUV_BG)
        for col_cells in ws_os.columns:
            length = max((len(str(c.value or "")) for c in col_cells), default=8)
            ws_os.column_dimensions[get_column_letter(col_cells[0].column)].width = min(
                length + 4, 40
            )
    else:
        ws_os.cell(row=1, column=1, value="No records found only in Suvidha.")

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


# ── Flask request helpers ─────────────────────────────────────────────────────


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
    raise ReconciliationError(
        f"Key column '{selected_key}' was not found in the detected columns. "
        f"Available: {available_columns}"
    )


def _parse_header_params(
    form, admin_prefix: str = "admin", suv_prefix: str = "suv"
) -> tuple[int | None, int | None, int | None, int | None]:
    """Extract and validate all four header override parameters from form data."""
    return (
        _parse_optional_int(form.get(f"{admin_prefix}_header_row")),
        _parse_optional_int(form.get(f"{admin_prefix}_header_span")),
        _parse_optional_int(form.get(f"{suv_prefix}_header_row")),
        _parse_optional_int(form.get(f"{suv_prefix}_header_span")),
    )


# ── Flask routes ──────────────────────────────────────────────────────────────


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
        a_hr, a_hs, s_hr, s_hs = _parse_header_params(request.form)
        admin = parse_uploaded_dataset(admin_file, a_hr, a_hs)
        suv = parse_uploaded_dataset(suv_file, s_hr, s_hs)
        return jsonify(
            {
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
            }
        )
    except ReconciliationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error("get_columns: %s\n%s", e, traceback.format_exc())
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
        return jsonify(
            {
                "columns": parsed.columns,
                "col_meta": parsed.column_meta,
                "header_row": parsed.header_row_index + 1,
                "header_span": parsed.header_row_span,
                "sample_rows": parsed.rows[:10],
                "notes": parsed.parser_notes,
            }
        )
    except ReconciliationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error("header_preview: %s\n%s", e, traceback.format_exc())
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
        a_hr, a_hs, s_hr, s_hs = _parse_header_params(request.form)
        admin = parse_uploaded_dataset(admin_file, a_hr, a_hs)
        suv = parse_uploaded_dataset(suv_file, s_hr, s_hs)

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
        logger.error("run_reconcile: %s\n%s", e, traceback.format_exc())
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
        a_hr, a_hs, s_hr, s_hs = _parse_header_params(request.form)
        admin = parse_uploaded_dataset(admin_file, a_hr, a_hs)
        suv = parse_uploaded_dataset(suv_file, s_hr, s_hs)

        admin_key = _resolve_key_column(admin_key_raw, admin.columns)
        suv_key = _resolve_key_column(suv_key_raw, suv.columns)

        result = reconcile(admin, suv, admin_key, suv_key)
        buf = generate_discrepancy_report(admin, result)

    except ReconciliationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error("download: %s\n%s", e, traceback.format_exc())
        return jsonify({"error": f"Failed to generate Excel report: {e}"}), 500

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(
        buf,
        as_attachment=True,
        download_name=f"grambook_reconciliation_{ts}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    os.makedirs("static", exist_ok=True)
    print("\nGrambook Reconciliation Tool")
    print("http://localhost:5000\n")
    app.run(debug=True, port=5000)
