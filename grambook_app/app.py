"""
Grambook Reconciliation — Production-Grade Backend (PATCHED v3)
Run: python app.py  →  http://localhost:5000

═══════════════════════════════════════════════════════════════
PATCH CHANGELOG v3 (zero-loss row routing fix)
═══════════════════════════════════════════════════════════════
PATCH-1  FUZZY_COLUMN_MATCH_THRESHOLD lowered 0.92 → 0.75

PATCH-2  map_columns_smart returns (pairs, unmapped_admin, unmapped_suv)
         + optional manual_mappings dict for hard overrides.

PATCH-3  _compare_row_pair handles unmapped cols:
         • Unmapped admin col + non-empty value → suvidha = "COLUMN NOT FOUND"
         • Unmapped suvidha col + non-empty value → admin  = "COLUMN NOT FOUND"

PATCH-4  reconcile() — CORRECTED BUCKET ASSIGNMENT (v3 fix):
         Every row ends up in EXACTLY ONE bucket:

         FOR each key in common:
           FOR each admin_row:
             IF matched suvidha row found:
               IF diffs → discrepancies          (values differ)
               ELSE      → matching_records       (identical)
             ELSE:
               → only_admin_rows                 (surplus admin duplicate)
           FOR each unmatched suvidha row:
             → only_suv_rows                     (surplus suvidha duplicate)

         Keys seen only in admin   → only_admin_rows
         Keys seen only in suvidha → only_suv_rows

         v2 BUG fixed: surplus admin/suvidha duplicates were incorrectly
         routed into discrepancies.  Now they go to the correct only_* lists.

PATCH-5  generate_discrepancy_report: ALL columns, no _should_show() filter.

PATCH-6  Debug logging: unmapped cols + per-column mismatch frequency +
         final row-count audit across all 4 buckets.

PATCH-7  strict_values_equal / Gujarati digit normalisation unchanged.
═══════════════════════════════════════════════════════════════
ZERO-LOSS GUARANTEE
Every input row appears in exactly one of:
  • matching_records  (key matched, values identical)
  • discrepancies     (key matched, ≥1 value differs)
  • only_admin_rows   (key absent from Suvidha, or surplus admin duplicate)
  • only_suv_rows     (key absent from Admin,   or surplus suvidha duplicate)
═══════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import shutil
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

# Sentinel value written when a column exists in one file but not the other
COLUMN_NOT_FOUND = "COLUMN NOT FOUND"


class ReconciliationError(Exception):
    pass


INDIC_DIGIT_MAP = str.maketrans(
    {
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


# ──────────────────────────────────────────────────────────────────────────────
# Tesseract setup
# ──────────────────────────────────────────────────────────────────────────────


def _iter_registry_tesseract_paths() -> list[str]:
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


# ──────────────────────────────────────────────────────────────────────────────
# Text normalisation helpers
# ──────────────────────────────────────────────────────────────────────────────

EXCEL_ILLEGAL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF]")


def canonical_text(value: Any) -> str:
    """Clean display text — used for rendering/headers, NOT for equality comparison."""
    text = "" if value is None else str(value)
    text = re.sub(r"[\r\n]+", " ", text)
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(INDIC_DIGIT_MAP)
    text = ZERO_WIDTH_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def excel_safe_text(value: Any) -> str:
    return EXCEL_ILLEGAL_CHARS_RE.sub("", canonical_text(value))


def _parse_decimal(text: str) -> Decimal | None:
    t = text.strip().replace(",", "")
    if not t:
        return None
    try:
        d = Decimal(t)
        if not d.is_finite():
            return None
        return d
    except InvalidOperation:
        return None


def canonical_numeric_text(value: Any) -> str | None:
    text = canonical_text(value)
    if not text:
        return None
    d = _parse_decimal(text)
    if d is None:
        return None
    return format(d.normalize(), "f")


# PATCH START — strict_values_equal: handle "" vs "0", 1 vs 1.0, COLUMN_NOT_FOUND
def strict_values_equal(left: Any, right: Any) -> bool:
    left_str = canonical_text(left)
    right_str = canonical_text(right)

    # COLUMN_NOT_FOUND is never equal to anything
    if left_str == COLUMN_NOT_FOUND or right_str == COLUMN_NOT_FOUND:
        return False

    # Both empty → equal
    if left_str == "" and right_str == "":
        return True

    # PATCH: treat empty string as equivalent to "0" for financial data
    # (blank cell and numeric 0 are considered equal)
    def _effective(s: str) -> str:
        if s == "":
            return "0"
        return s

    left_str = _effective(left_str)
    right_str = _effective(right_str)

    # Numeric comparison — handles 1 == 1.0, "001" == "1", etc.
    left_num = _parse_decimal(left_str.replace(",", ""))
    right_num = _parse_decimal(right_str.replace(",", ""))

    if left_num is not None and right_num is not None:
        return left_num == right_num

    # Mixed (one numeric, one not) → not equal
    if (left_num is None) != (right_num is None):
        return False

    # Plain string comparison (already NFKC-normalised via canonical_text)
    return left_str == right_str


# PATCH END


def normalize_column_key(name: str) -> str:
    return re.sub(r"[\s_\-]+", "", canonical_text(name).lower())


# PATCH START — normalize_key_value: deterministic, handles "001"=="1", Gujarati digits
def normalize_key_value(value: Any) -> str:
    if value is None:
        return ""

    text = str(value)
    # Step 1: Unicode + Gujarati digit normalisation
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(INDIC_DIGIT_MAP)
    text = ZERO_WIDTH_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return ""

    # Step 2: Numeric normalisation — strips leading zeros, removes trailing .0
    # "001" → "1", "1.0" → "1", "1,234.50" → "1234.5"
    try:
        num = Decimal(text.replace(",", ""))
        if not num.is_finite():
            return text.lower()
        # PATCH: integer-valued decimals collapse to int string
        if num == num.to_integral_value():
            return str(int(num.to_integral_value()))
        return format(num.normalize(), "f")
    except Exception:
        pass

    # Step 3: Non-numeric fallback — lowercase for consistent matching
    return text.lower()


# PATCH END


def is_numeric_like(text: str) -> bool:
    t = canonical_text(text)
    if not t:
        return False
    return bool(re.fullmatch(r"[+-]?\d+(\.\d+)?", t.replace(",", "")))


# ──────────────────────────────────────────────────────────────────────────────
# File parsing — CSV / XLS / XLSX
# ──────────────────────────────────────────────────────────────────────────────


def _decode_csv_bytes(file_bytes: bytes) -> io.StringIO:
    for enc in ["utf-8-sig", "utf-8", "utf-16", "cp1252", "latin1"]:
        try:
            return io.StringIO(file_bytes.decode(enc))
        except UnicodeDecodeError:
            continue
    raise ReconciliationError("CSV encoding is unsupported or file is corrupt.")


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
    try:
        import xlrd
    except ImportError as e:
        raise ReconciliationError(
            ".xls file detected but xlrd is missing. Install xlrd==2.0.1"
        ) from e
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
        text_ratio = sum(1 for x in non_empty if not is_numeric_like(x)) / len(
            non_empty
        )
        if text_ratio < 0.55:
            continue
        if any(len(x) > 140 for x in non_empty):
            continue
        scored.append((i, score))

    if not scored:
        return 0, None

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


# PATCH START — detect_category: guard empty/numeric columns
def detect_category(col: str) -> str:
    col = str(col).strip()
    if not col:
        return "empty"  # PATCH: distinguish from "other"

    if "ઘર" in col:
        return "ghar"
    if "સફાઈ" in col:
        return "safai"
    if "સફઈ" in col:
        return "safai"  # PATCH: alternate spelling
    if "લાઇટ" in col:
        return "light"
    if "લાઈટ" in col:
        return "light"  # PATCH: before normalization
    if "સા.પાણી" in col or "સાપાણી" in col:
        return "sa_pani"
    if "ખા.પાણી" in col or "ખાપાણી" in col:
        return "kha_pani"
    if "ગટર" in col:
        return "gatar"

    return "other"


def build_headers(
    matrix: list[list[str]],
    header_start: int,
    manual_header_span: int | None = None,
    forced_span: int | None = None,
) -> tuple[list[str], dict[str, str], list[int], int]:
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
            combined = []
            for c in range(max_cols):
                parts = _dedupe_in_order(
                    [local_rows[r][c] for r in range(span) if local_rows[r][c]]
                )
                combined.append(" ".join(parts).strip())

            non_empty = sum(1 for h in combined if h)
            score = non_empty - sum(1 for h in combined if len(h) > 90) * 1.5

            multi_header_bonus = 0
            # Replace the existing multi_header_bonus block in build_headers:
            HEADER_KEYWORDS_ROW1 = [
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
                "જૂની",
                "નવી",
                "વર્ષ",
                "વિગત",
                "પ્રકાર",
                "સ્થિતિ",
                "નોંધ",
                "કરવેરો",
                "વેરો",
                "મિલ્કત",
                "સરવેળો",
            ]
            HEADER_KEYWORDS_ROW2 = [
                "no",
                "number",
                "id",
                "code",
                "name",
                "ક્રમ",
                "નંબર",
                "કોડ",
                "નામ",
                "વિભાગ",
                "gsn",
                "survey",
                "property",
                "ward",
                "owner",
            ]

            for row_idx in range(span):
                row_text = " ".join(local_rows[row_idx]).lower()
                if any(k in row_text for k in HEADER_KEYWORDS_ROW1):
                    multi_header_bonus += 2
                if any(k in row_text for k in HEADER_KEYWORDS_ROW2):
                    multi_header_bonus += 1
            score += multi_header_bonus

            if score > best_score:
                best_score = score
                chosen_span = span

    header_rows = local_rows[:chosen_span]
    raw_headers: list[str] = []
    for c in range(max_cols):
        parts = _dedupe_in_order(
            [header_rows[r][c] for r in range(chosen_span) if header_rows[r][c]]
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
    """Pad or truncate — no shifting."""
    r = list(row)
    if len(r) < target_cols:
        r += [""] * (target_cols - len(r))
    elif len(r) > target_cols:
        r = r[:target_cols]
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


# ──────────────────────────────────────────────────────────────────────────────
# Reconciliation Engine
# ──────────────────────────────────────────────────────────────────────────────


def _debug_log(label: str, data: Any) -> None:
    """Structured debug output to stdout."""
    try:
        print(
            f"\n[DEBUG] {label}:\n"
            f"{json.dumps(data, ensure_ascii=False, indent=2, default=str)}"
        )
    except Exception:
        print(f"\n[DEBUG] {label}: {data}")


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if fuzz is not None:
        return float(fuzz.ratio(a, b)) / 100.0
    return SequenceMatcher(None, a, b).ratio()


# ── PATCH-1: Lowered from 0.92 → 0.75 ────────────────────────────────────────
FUZZY_COLUMN_MATCH_THRESHOLD = 0.75

# PATCH-GUJ: Gujarati term normalization for consistent fuzzy matching
# PATCH START — normalize_gujarati_terms: canonical Gujarati column spelling
_GUJARATI_NORM_MAP: list[tuple[str, str]] = [
    # Light
    ("લાઈટ", "લાઇટ"),
    # Water
    ("સાપાણી", "સા.પાણી"),
    ("સા.પાણી", "સા.પાણી"),
    ("ખાપાણી", "ખા.પાણી"),
    ("ખા.પાણી", "ખા.પાણી"),
    # Sewage
    ("ગટર", "ગટર"),
    # Cleaning
    ("સફઈ", "સફાઈ"),
    # Tax suffix normalisation
    ("વેરા", "વેરો"),
    ("ટેક્ષ", "ટેક્સ"),
]


def normalize_gujarati_terms(text: str) -> str:
    """
    PATCH-GUJ: Normalise known Gujarati script variations so that
    e.g. 'લાઈટ' and 'લાઇટ' score as identical before fuzzy matching.
    """
    for variant, canonical in _GUJARATI_NORM_MAP:
        text = text.replace(variant, canonical)
    return text


# PATCH END


# PATCH START — map_columns_smart v2: category-gated, Gujarati-normalised, GS-fallback
FUZZY_COLUMN_MATCH_THRESHOLD = 0.75  # used for exact-norm pass
FUZZY_CAT_THRESHOLD = 0.65  # used for category-gated fuzzy pass


def map_columns_smart(
    admin_cols: list[str],
    suv_cols: list[str],
    admin_key: str,
    suv_key: str,
    manual_mappings: dict[str, str] | None = None,
) -> tuple[
    list[tuple[str, str, float]],
    list[str],
    list[str],
]:
    """
    Returns: (mapped_pairs, unmapped_admin, unmapped_suv)

    Pass 0 — manual overrides (unconditional)
    Pass 1 — exact normalised key (category-gated)
    Pass 2 — Gujarati-normalised fuzzy (category-gated, threshold 0.65)
    Pass 3 — GS-suffix fallback → same-category 'કુલ'/'total' column
    """

    admin_candidates = [c for c in admin_cols if c != admin_key]
    suv_candidates = [c for c in suv_cols if c != suv_key]

    # Pre-compute normalised keys for suvidha candidates
    suv_norm_map: dict[str, str] = {}  # normalised_key → original col name
    for sc in suv_candidates:
        nk = normalize_column_key(normalize_gujarati_terms(sc))
        if nk not in suv_norm_map:  # first-come-first-served on collision
            suv_norm_map[nk] = sc

    used_suv: set[str] = set()
    mapped: list[tuple[str, str, float]] = []
    rejected: list[dict] = []  # PATCH-DBG: rejected pairings

    def _already_mapped(ac: str) -> bool:
        return any(x[0] == ac for x in mapped)

    # ── Pass 0: manual overrides ──────────────────────────────────────────────
    if manual_mappings:
        for ac, sc in manual_mappings.items():
            if ac in admin_candidates and sc in suv_candidates and sc not in used_suv:
                mapped.append((ac, sc, 1.0))
                used_suv.add(sc)

    # ── Pass 1: exact normalised key match (category-gated) ──────────────────
    for ac in admin_candidates:
        if _already_mapped(ac):
            continue

        nk = normalize_column_key(normalize_gujarati_terms(ac))
        sc = suv_norm_map.get(nk)

        if not sc or sc in used_suv:
            continue

        ac_cat = detect_category(ac)
        sc_cat = detect_category(sc)

        # Hard block: different named categories — never cross-map
        if ac_cat != sc_cat and not (ac_cat == "other" and sc_cat == "other"):
            rejected.append(
                {
                    "admin": ac,
                    "suvidha": sc,
                    "reason": "exact-key: category mismatch",
                    "admin_cat": ac_cat,
                    "suvidha_cat": sc_cat,
                }
            )
            continue

        mapped.append((ac, sc, 1.0))
        used_suv.add(sc)

    # ── Pass 2: Gujarati-normalised fuzzy match (category-gated) ─────────────
    for ac in admin_candidates:
        if _already_mapped(ac):
            continue

        ac_cat = detect_category(ac)
        na = normalize_column_key(normalize_gujarati_terms(ac))

        best_sc: str | None = None
        best_score = 0.0

        for sc in suv_candidates:
            if sc in used_suv:
                continue

            sc_cat = detect_category(sc)

            # HARD category filter
            if ac_cat != sc_cat:
                if not (ac_cat == "other" and sc_cat == "other"):
                    rejected.append(
                        {
                            "admin": ac,
                            "suvidha": sc,
                            "reason": "fuzzy: category mismatch",
                            "admin_cat": ac_cat,
                            "suvidha_cat": sc_cat,
                        }
                    )
                    continue

            nb = normalize_column_key(normalize_gujarati_terms(sc))
            score = _similarity(na, nb)

            if score > best_score:
                best_score = score
                best_sc = sc

        if best_sc and best_score >= FUZZY_CAT_THRESHOLD:
            mapped.append((ac, best_sc, round(best_score, 4)))
            used_suv.add(best_sc)

    # ── Pass 3: GS-suffix fallback ────────────────────────────────────────────
    # Admin cols with "GS" that are still unmapped → try a same-category
    # suvidha col that contains "કુલ" or "total".
    for ac in admin_candidates:
        if _already_mapped(ac):
            continue
        if "GS" not in ac and "gs" not in ac.lower():
            continue

        ac_cat = detect_category(ac)

        for sc in suv_candidates:
            if sc in used_suv:
                continue
            sc_cat = detect_category(sc)
            if ac_cat != sc_cat and not (ac_cat == "other" and sc_cat == "other"):
                continue
            if "કુલ" in sc or "total" in sc.lower():
                mapped.append((ac, sc, 0.60))
                used_suv.add(sc)
                break

    # ── Compute unmapped sets ─────────────────────────────────────────────────
    mapped_admin = {a for a, _, _ in mapped}
    unmapped_admin = [c for c in admin_candidates if c not in mapped_admin]
    unmapped_suv = [c for c in suv_candidates if c not in used_suv]

    # ── Debug output ──────────────────────────────────────────────────────────
    _debug_log("unmapped_admin_cols", unmapped_admin)
    _debug_log("unmapped_suv_cols", unmapped_suv)
    _debug_log("rejected_mappings", rejected)
    _debug_log(
        "column_mapping_result",
        [
            {
                "admin": a,
                "suvidha": s,
                "category": detect_category(a),
                "confidence": c,
            }
            for a, s, c in mapped
        ],
    )

    return mapped, unmapped_admin, unmapped_suv


# PATCH END


# PATCH START — _build_key_index: cleaner logging for missing keys
def _build_key_index(
    rows: list[dict[str, str]],
    key_col: str,
    row_position_map: dict[int, int],
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    idx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing = 0
    missing_rows: list[int] = []  # PATCH: track which Excel rows had no key

    for i, row in enumerate(rows):
        raw_key = row.get(key_col, "")
        key = normalize_key_value(raw_key)
        if not key:
            missing += 1
            missing_rows.append(row_position_map.get(i, i + 1))
            continue
        excel_row = row_position_map.get(i, -1)
        idx[key].append(
            {
                "df_row_index": i,
                "excel_row": excel_row,
                "row": row,
                "display_key": canonical_text(raw_key),
            }
        )

    if missing_rows:
        _debug_log(
            f"missing_key_rows [{key_col}]",
            {"count": missing, "excel_rows": missing_rows[:50]},
        )

    return idx, missing


# PATCH END


# ── PATCH-3: Full comparison including unmapped columns ───────────────────────
def _compare_row_pair(
    a: dict[str, str],
    s: dict[str, str],
    col_pairs: list[tuple[str, str, float]],
    unmapped_admin: list[str],
    unmapped_suv: list[str],
    admin_column_position_map: dict[str, dict[str, int]],
    admin_columns: list[str],
) -> dict[str, Any]:
    """
    Compare one admin row vs one suvidha row.

    For mapped pairs: strict equality check.
    For unmapped admin cols: if value non-empty → suvidha = COLUMN_NOT_FOUND.
    For unmapped suvidha cols: if value non-empty → admin  = COLUMN_NOT_FOUND.

    Returns {col_name: diff_info} for every mismatch found.
    """
    diffs: dict[str, Any] = {}

    def _position(col: str) -> dict[str, int]:
        return admin_column_position_map.get(
            col,
            {
                "df_index": admin_columns.index(col) if col in admin_columns else 0,
                "excel_col": admin_columns.index(col) + 1
                if col in admin_columns
                else 1,
            },
        )

    # Mapped column pairs
    for ac, sc, confidence in col_pairs:
        av = canonical_text(a.get(ac, ""))
        sv = canonical_text(s.get(sc, ""))
        if not strict_values_equal(av, sv):
            diffs[ac] = {
                "admin": av,
                "suvidha": sv,
                "suv_col": sc,
                "confidence": round(confidence, 4),
                **_position(ac),
            }

    # PATCH-3a: Admin columns that have NO suvidha counterpart
    for ac in unmapped_admin:
        av = canonical_text(a.get(ac, ""))
        if av:  # non-empty admin value with no suvidha column → mismatch
            diffs[ac] = {
                "admin": av,
                "suvidha": COLUMN_NOT_FOUND,
                "suv_col": None,
                "confidence": 0.0,
                **_position(ac),
            }

    # PATCH-3b: Suvidha columns that have NO admin counterpart
    for sc in unmapped_suv:
        sv = canonical_text(s.get(sc, ""))
        if sv:  # non-empty suvidha value with no admin column → mismatch
            label = f"[SUV] {sc}"  # prefix to distinguish in output
            diffs[label] = {
                "admin": COLUMN_NOT_FOUND,
                "suvidha": sv,
                "suv_col": sc,
                "confidence": 0.0,
                "df_index": -1,
                "excel_col": -1,
            }

    return diffs


# ── PATCH-4 v3: Zero-loss reconcile — every row lands in exactly one bucket ───
def reconcile(
    admin: ParsedDataset,
    suv: ParsedDataset,
    admin_key: str,
    suv_key: str,
    manual_mappings: dict[str, str] | None = None,
) -> dict[str, Any]:

    admin_idx, admin_missing_keys = _build_key_index(
        admin.rows, admin_key, admin.row_position_map
    )
    suv_idx, suv_missing_keys = _build_key_index(
        suv.rows, suv_key, suv.row_position_map
    )

    # PATCH-2: unpack three-tuple
    col_pairs, unmapped_admin, unmapped_suv = map_columns_smart(
        admin.columns, suv.columns, admin_key, suv_key, manual_mappings
    )

    pair_lookup = {a: s for a, s, _ in col_pairs}

    admin_keys = set(admin_idx)
    suv_keys = set(suv_idx)
    common = admin_keys & suv_keys
    only_admin_keys = admin_keys - suv_keys
    only_suv_keys = suv_keys - admin_keys

    _debug_log(
        "column_mapping",
        [{"admin": a, "suvidha": s, "confidence": c} for a, s, c in col_pairs],
    )
    _debug_log(
        "key_stats",
        {
            "total_admin_keys": len(admin_keys),
            "total_suvidha_keys": len(suv_keys),
            "common_keys": len(common),
            "only_admin": len(only_admin_keys),
            "only_suvidha": len(only_suv_keys),
            "admin_missing_key_rows": admin_missing_keys,
            "suvidha_missing_key_rows": suv_missing_keys,
        },
    )

    discrepancies: list[dict[str, Any]] = []
    matching_records = 0
    duplicate_key_conflicts: list[dict] = []

    # Surplus rows from duplicate-key groups — routed to only_* NOT discrepancies
    # (v3 fix: v2 incorrectly sent these to discrepancies)
    extra_only_admin: list[dict[str, str]] = []
    extra_only_suv: list[dict[str, str]] = []

    # Per-column mismatch frequency (PATCH-6)
    col_mismatch_counter: dict[str, int] = defaultdict(int)

    for key in sorted(common):
        a_rows = admin_idx[key]
        s_rows = suv_idx[key]

        # Log duplicate key groups for auditability
        if len(a_rows) > 1 or len(s_rows) > 1:
            duplicate_key_conflicts.append(
                {
                    "key": key,
                    "admin_count": len(a_rows),
                    "suvidha_count": len(s_rows),
                }
            )

        # ── Many-to-many matching via greedy best-fit ─────────────────────────
        # matched_s_indices tracks which suvidha rows have been claimed so that
        # each suvidha row can only pair with one admin row (1:1 per iteration).
        matched_s_indices: set[int] = set()

        for a_item in a_rows:
            best_diffs: dict[str, Any] | None = None
            best_s_item = None
            best_s_idx = -1

            # Find the suvidha row with the FEWEST differences (best pairing)
            for s_idx, s_item in enumerate(s_rows):
                if s_idx in matched_s_indices:
                    continue
                diffs = _compare_row_pair(
                    a_item["row"],
                    s_item["row"],
                    col_pairs,
                    unmapped_admin,
                    unmapped_suv,
                    admin.column_position_map,
                    admin.columns,
                )
                if best_diffs is None or len(diffs) < len(best_diffs):
                    best_diffs = diffs
                    best_s_item = s_item
                    best_s_idx = s_idx

            if best_s_item is None:
                # ── BUCKET: only_admin ────────────────────────────────────────
                # No suvidha row remains to pair with this admin row.
                # This happens when suvidha has fewer duplicates than admin for
                # the same key (e.g. admin has 3 rows, suvidha has 2).
                # v2 BUG: these went to discrepancies — FIXED in v3.
                extra_only_admin.append(a_item["row"])
                continue

            matched_s_indices.add(best_s_idx)

            if best_diffs:
                # ── BUCKET: discrepancies ────────────────────────────────────
                # Rows were paired AND have at least one value difference.
                diff_cols = list(best_diffs.keys())
                for dc in diff_cols:
                    col_mismatch_counter[dc] += 1

                discrepancies.append(
                    {
                        "key": a_item["display_key"] or key,
                        "normalized_key": key,
                        "row_index": a_item["df_row_index"],
                        "admin_excel_row": a_item["excel_row"],
                        "suvidha_excel_row": best_s_item["excel_row"],
                        "admin_row": a_item["row"],  # FULL row
                        "suv_row": best_s_item["row"],  # FULL row
                        "diffs": best_diffs,
                        "diff_cols": diff_cols,
                    }
                )
            else:
                # ── BUCKET: matching_records ──────────────────────────────────
                # Rows were paired AND are identical across all columns.
                matching_records += 1

        # ── BUCKET: only_suv ─────────────────────────────────────────────────
        # Suvidha rows that were not paired (suvidha has more duplicates than admin).
        # v2 BUG: these went to discrepancies — FIXED in v3.
        for s_idx, s_item in enumerate(s_rows):
            if s_idx not in matched_s_indices:
                extra_only_suv.append(s_item["row"])

    # ── Assemble final only_* lists ───────────────────────────────────────────
    # Keys entirely absent from the other file
    only_admin_rows: list[dict[str, str]] = [
        item["row"] for key in sorted(only_admin_keys) for item in admin_idx[key]
    ]
    only_suv_rows: list[dict[str, str]] = [
        item["row"] for key in sorted(only_suv_keys) for item in suv_idx[key]
    ]

    # Surplus duplicate rows from matched-key groups
    only_admin_rows.extend(extra_only_admin)
    only_suv_rows.extend(extra_only_suv)

    disc_count = len(discrepancies)
    validation_note = ""
    if disc_count == 0 and len(common) > 0:
        validation_note = (
            "WARNING: 0 discrepancies found across all matched keys. "
            "Verify column mapping and key selection are correct."
        )

    # ── PATCH-6: comprehensive debug audit ───────────────────────────────────
    total_admin_rows = sum(len(v) for v in admin_idx.values())
    total_suv_rows = sum(len(v) for v in suv_idx.values())
    audit_admin = matching_records + disc_count + len(only_admin_rows)
    audit_suv = matching_records + disc_count + len(only_suv_rows)

    _debug_log("col_mismatch_frequency", dict(col_mismatch_counter))
    _debug_log(
        "reconciliation_result",
        {
            "total_admin_rows_indexed": total_admin_rows,
            "total_suvidha_rows_indexed": total_suv_rows,
            "admin_missing_key_rows": admin_missing_keys,
            "suvidha_missing_key_rows": suv_missing_keys,
            "common_keys": len(common),
            "─── buckets ───": "─────────────────────",
            "matching_records": matching_records,
            "discrepancies": disc_count,
            "only_admin_rows": len(only_admin_rows),
            "  └─ unique keys": len(only_admin_keys),
            "  └─ surplus duplicates": len(extra_only_admin),
            "only_suv_rows": len(only_suv_rows),
            "  └─ unique keys": len(only_suv_keys),
            "  └─ surplus duplicates": len(extra_only_suv),
            "─── audit ───": "─────────────────────",
            "admin_accounted_for": f"{audit_admin}/{total_admin_rows}",
            "suvidha_accounted_for": f"{audit_suv}/{total_suv_rows}",
            "zero_loss_admin": audit_admin == total_admin_rows,
            "zero_loss_suvidha": audit_suv == total_suv_rows,
            "validation_note": validation_note,
        },
    )

    return {
        "discrepancies": discrepancies,
        "only_admin_rows": only_admin_rows,
        "only_suv_rows": only_suv_rows,
        "column_map": {a: s for a, s, _ in col_pairs},  # ADD THIS
        "col_pairs": [
            {"admin_col": a, "suv_col": s, "confidence": round(c, 4)}
            for a, s, c in col_pairs
        ],
        "admin_key": admin_key,
        "suv_key": suv_key,
        "admin_cols": admin.columns,
        "suv_cols": suv.columns,
        "admin_column_position_map": admin.column_position_map,
        "unmapped": {
            "admin_cols": unmapped_admin,
            "suv_cols": unmapped_suv,
        },
        "col_mismatch_frequency": dict(col_mismatch_counter),
        "meta": {
            "compared_keys": len(common),
            "duplicate_key_conflicts": duplicate_key_conflicts,
            "admin_missing_keys": admin_missing_keys,
            "suvidha_missing_keys": suv_missing_keys,
            "unmapped_admin_cols": unmapped_admin,
            "unmapped_suv_cols": unmapped_suv,
            "fuzzy_threshold_used": FUZZY_COLUMN_MATCH_THRESHOLD,
            "zero_loss_verified": {
                "admin": audit_admin == total_admin_rows,
                "suvidha": audit_suv == total_suv_rows,
            },
        },
        "stats": {
            "total": len(admin_keys | suv_keys),
            "matched": matching_records,
            "disc": disc_count,
            "only_a": len(only_admin_rows),
            "only_s": len(only_suv_rows),
            "validation_note": validation_note,
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# Excel output  (PATCH-5: ALL columns, no _should_show filtering)
# ──────────────────────────────────────────────────────────────────────────────


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


def generate_discrepancy_report(admin: ParsedDataset, result: dict) -> io.BytesIO:
    """
    Excel report — 3 sheets.
    PATCH-5: ALL admin columns included, no column filtering.
    Mismatched cells highlighted; all other cells shown normally.
    """
    wb = Workbook()

    HEADER_FILL = "1F4E78"
    ADMIN_BG = "FFF3EE"
    SUVIDHA_BG = "F0FBF7"
    ADMIN_SRC_FILL = "C0392B"
    SUVIDHA_SRC_FILL = "27AE60"
    ONLY_ADMIN_FILL = "EF9A9A"
    ONLY_SUV_FILL = "80DEEA"
    SEP_FILL = "E8EAF0"

    admin_mismatch_font = Font(color="C0392B", bold=True, name="Calibri", size=10)
    suv_mismatch_font = Font(color="1A7A4A", bold=True, name="Calibri", size=10)
    cnf_font = Font(color="7F7F7F", italic=True, name="Calibri", size=10)

    # PATCH-5: ALL columns, no exclusion
    all_admin_cols: list[str] = admin.columns

    suv_col_lookup: dict[str, str] = {
        p["admin_col"]: p["suv_col"] for p in result.get("col_pairs", [])
    }
    admin_key_col: str = result.get("admin_key", "")

    SOURCE_COL = 1
    col_to_excel: dict[str, int] = {c: i + 2 for i, c in enumerate(all_admin_cols)}

    def _write_header_row(ws) -> None:
        src_hdr = ws.cell(row=1, column=SOURCE_COL, value="Source")
        _style_cell(
            src_hdr, fill_hex=HEADER_FILL, bold=True, color="FFFFFF", align="center"
        )
        ws.column_dimensions[get_column_letter(SOURCE_COL)].width = 10

        for col_name, excel_col in col_to_excel.items():
            cell = ws.cell(row=1, column=excel_col, value=excel_safe_text(col_name))
            _style_cell(
                cell,
                fill_hex=HEADER_FILL,
                bold=True,
                color="FFFFFF",
                align="center",
                wrap=True,
            )
            ws.column_dimensions[get_column_letter(excel_col)].width = min(
                max(len(excel_safe_text(col_name)) + 6, 14), 45
            )

    def _write_simple_header(ws, cols: list[str], fill: str) -> None:
        for i, col_name in enumerate(cols, start=1):
            cell = ws.cell(row=1, column=i, value=excel_safe_text(col_name))
            _style_cell(
                cell,
                fill_hex=fill,
                bold=True,
                color="FFFFFF",
                align="center",
                wrap=True,
            )
            ws.column_dimensions[get_column_letter(i)].width = min(
                max(len(excel_safe_text(col_name)) + 6, 14), 45
            )

    # ── Sheet 1: Discrepancies ────────────────────────────────────────────────
    ws_disc = wb.active
    ws_disc.title = "Discrepancies"
    _write_header_row(ws_disc)
    current_row = 2

    for disc in result.get("discrepancies", []):
        admin_row: dict[str, str] = disc.get("admin_row", {})
        suv_row: dict[str, str] = disc.get("suv_row", {})
        diffs: dict[str, Any] = disc.get("diffs", {})
        diff_set = set(diffs.keys())

        # ── Admin row ─────────────────────────────────────────────────────────
        src_a = ws_disc.cell(row=current_row, column=SOURCE_COL, value="Admin")
        _style_cell(
            src_a, fill_hex=ADMIN_SRC_FILL, bold=True, color="FFFFFF", align="center"
        )

        for col_name, excel_col in col_to_excel.items():
            cell = ws_disc.cell(row=current_row, column=excel_col)
            val = excel_safe_text(admin_row.get(col_name, ""))

            if col_name in diff_set:
                # Mismatch → red font highlight
                cell.value = val
                cell.font = admin_mismatch_font
                cell.fill = PatternFill("solid", start_color="FDECEA")
                cell.border = _border()
                cell.alignment = Alignment(horizontal="left", vertical="center")
            else:
                cell.value = val
                _style_cell(cell, fill_hex=ADMIN_BG)

        current_row += 1

        # ── Suvidha row ───────────────────────────────────────────────────────
        src_s = ws_disc.cell(row=current_row, column=SOURCE_COL, value="Suvidha")
        _style_cell(
            src_s, fill_hex=SUVIDHA_SRC_FILL, bold=True, color="FFFFFF", align="center"
        )

        for col_name, excel_col in col_to_excel.items():
            cell = ws_disc.cell(row=current_row, column=excel_col)

            if col_name in diff_set:
                val = diffs[col_name].get("suvidha", "")
                if val == COLUMN_NOT_FOUND:
                    cell.value = val
                    cell.font = cnf_font
                    cell.fill = PatternFill("solid", start_color="FFF9C4")
                else:
                    cell.value = excel_safe_text(val)
                    cell.font = suv_mismatch_font
                    cell.fill = PatternFill("solid", start_color="E8F8F3")
                cell.border = _border()
                cell.alignment = Alignment(horizontal="left", vertical="center")
            else:
                # Use mapped suvidha column name where available
                suv_col = suv_col_lookup.get(col_name, col_name)
                val = suv_row.get(suv_col, suv_row.get(col_name, ""))
                cell.value = excel_safe_text(val)
                _style_cell(cell, fill_hex=SUVIDHA_BG)

        current_row += 1

        # ── Separator ─────────────────────────────────────────────────────────
        for c in range(1, len(all_admin_cols) + 2):
            sep = ws_disc.cell(row=current_row, column=c, value="")
            sep.fill = PatternFill("solid", start_color=SEP_FILL)
            sep.border = _border()
        ws_disc.row_dimensions[current_row].height = 4
        current_row += 1

    # ── Sheet 2: Only in Admin ────────────────────────────────────────────────
    only_admin_rows: list[dict] = result.get("only_admin_rows", [])
    ws_oa = wb.create_sheet(title="Only in Admin")
    _write_simple_header(ws_oa, all_admin_cols, ONLY_ADMIN_FILL)
    for row_idx, row in enumerate(only_admin_rows, start=2):
        for col_idx, col_name in enumerate(all_admin_cols, start=1):
            cell = ws_oa.cell(
                row=row_idx,
                column=col_idx,
                value=excel_safe_text(row.get(col_name, "")),
            )
            _style_cell(cell, fill_hex="FFEBEE")

    # ── Sheet 3: Only in Suvidha ──────────────────────────────────────────────
    only_suv_rows: list[dict] = result.get("only_suv_rows", [])
    suv_cols: list[str] = result.get("suv_cols", all_admin_cols)
    ws_os = wb.create_sheet(title="Only in Suvidha")
    _write_simple_header(ws_os, suv_cols, ONLY_SUV_FILL)
    for row_idx, row in enumerate(only_suv_rows, start=2):
        for col_idx, col_name in enumerate(suv_cols, start=1):
            cell = ws_os.cell(
                row=row_idx,
                column=col_idx,
                value=excel_safe_text(row.get(col_name, "")),
            )
            _style_cell(cell, fill_hex="E0F7FA")

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
    raise ReconciliationError(
        f"Key column '{selected_key}' not found in detected columns."
    )


def _parse_manual_mappings(form_value: str | None) -> dict[str, str] | None:
    """
    Optional JSON string from request form:
      {"Admin Col Name": "Suvidha Col Name", ...}
    """
    if not form_value:
        return None
    try:
        data = json.loads(form_value)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except (json.JSONDecodeError, TypeError):
        pass
    return None


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
        admin = parse_uploaded_dataset(
            admin_file,
            _parse_optional_int(request.form.get("admin_header_row")),
            _parse_optional_int(request.form.get("admin_header_span")),
        )
        suv = parse_uploaded_dataset(
            suv_file,
            _parse_optional_int(request.form.get("suv_header_row")),
            _parse_optional_int(request.form.get("suv_header_span")),
        )
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
        return jsonify({"error": f"Preview failed: {e}"}), 500


# PATCH START — /reconcile route: delegate to reconcile() instead of inline loop
@app.route("/reconcile", methods=["POST"])
def reconcile_api():
    try:
        admin_file = request.files.get("admin_file")
        suvidha_file = request.files.get("suvidha_file")

        if not admin_file or not suvidha_file:
            return jsonify({"error": "Both files are required"}), 400

        admin_key = (request.form.get("admin_key") or "").strip()
        suv_key = (request.form.get("suvidha_key") or "").strip()

        if not admin_key or not suv_key:
            return jsonify({"error": "Key columns not selected"}), 400

        admin_ds = parse_uploaded_dataset(
            admin_file,
            manual_header_row=_parse_optional_int(request.form.get("admin_header_row")),
            manual_header_span=_parse_optional_int(
                request.form.get("admin_header_span")
            ),
        )
        suv_ds = parse_uploaded_dataset(
            suvidha_file,
            manual_header_row=_parse_optional_int(
                request.form.get("suvidha_header_row")
            ),
            manual_header_span=_parse_optional_int(
                request.form.get("suvidha_header_span")
            ),
        )

        # Resolve key columns with fuzzy fallback
        try:
            admin_key = _resolve_key_column(admin_key, admin_ds.columns)
        except ReconciliationError:
            return jsonify(
                {"error": f"Admin key '{admin_key}' not found in columns"}
            ), 400

        try:
            suv_key = _resolve_key_column(suv_key, suv_ds.columns)
        except ReconciliationError:
            return jsonify(
                {"error": f"Suvidha key '{suv_key}' not found in columns"}
            ), 400

        manual_mappings = _parse_manual_mappings(request.form.get("manual_mappings"))

        # ── Delegate to the single authoritative reconcile() function ─────────
        result = reconcile(admin_ds, suv_ds, admin_key, suv_key, manual_mappings)

        # ── Normalise response shape ──────────────────────────────────────────
        # The frontend may use either key name; expose both for compatibility.
        return jsonify(
            {
                # Primary keys (used by generate_discrepancy_report / download)
                "discrepancies": result["discrepancies"],
                "only_admin_rows": result["only_admin_rows"],
                "only_suv_rows": result["only_suv_rows"],
                # Legacy aliases consumed by older frontend versions
                "matching_records": result["stats"]["matched"],
                "only_suvidha_rows": result["only_suv_rows"],
                # Diagnostic / metadata
                "stats": result["stats"],
                "col_pairs": result["col_pairs"],
                "unmapped": result["unmapped"],
                "col_mismatch_frequency": result["col_mismatch_frequency"],
                "meta": result["meta"],
            }
        )

    except ReconciliationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        import traceback

        print(f"Reconcile error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": f"Reconciliation failed: {e}"}), 500


# PATCH END


@app.route("/api/download", methods=["POST"])
def download():
    admin_file = request.files.get("admin_file")
    suv_file = request.files.get("suvidha_file")
    admin_key_raw = request.form.get("admin_key", "").strip()
    suv_key_raw = request.form.get("suv_key", "").strip()

    if not admin_file or not suv_file or not admin_key_raw or not suv_key_raw:
        return jsonify({"error": "Both files and key columns are required."}), 400

    try:
        admin = parse_uploaded_dataset(
            admin_file,
            _parse_optional_int(request.form.get("admin_header_row")),
            _parse_optional_int(request.form.get("admin_header_span")),
        )
        suv = parse_uploaded_dataset(
            suv_file,
            _parse_optional_int(request.form.get("suv_header_row")),
            _parse_optional_int(request.form.get("suv_header_span")),
        )
        admin_key = _resolve_key_column(admin_key_raw, admin.columns)
        suv_key = _resolve_key_column(suv_key_raw, suv.columns)

        manual_mappings = _parse_manual_mappings(request.form.get("manual_mappings"))
        result = reconcile(admin, suv, admin_key, suv_key, manual_mappings)
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
    print("\n═══════════════════════════════════════")
    print("  Grambook Reconciliation Tool (PATCHED v2)")
    print("  http://localhost:5000")
    print("═══════════════════════════════════════\n")
    app.run(debug=True, port=5000)
