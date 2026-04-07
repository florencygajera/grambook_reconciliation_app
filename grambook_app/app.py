"""
Grambook Reconciliation — Production-Grade Backend (PATCHED v3)
Run: python app.py  →  http://localhost:5000

═══════════════════════════════════════════════════════════════
PATCH CHANGELOG v3 (zero-loss row routing fix)
═══════════════════════════════════════════════════════════════
PATCH-1  FUZZY_COLUMN_MATCH_THRESHOLD lowered 0.92 → 0.70

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


def strict_values_equal(left: Any, right: Any) -> bool:
    """
    Strict equality:
      • "" == ""     → True
      • "" vs  0     → False
      • 100 vs 100.0 → True  (Decimal normalisation)
      • "abc" vs "ABC" → False (case-sensitive)
    """
    left_str = canonical_text(left)
    right_str = canonical_text(right)

    if left_str == "" and right_str == "":
        return True
    if left_str == "" or right_str == "":
        return False

    left_num = _parse_decimal(left_str.replace(",", ""))
    right_num = _parse_decimal(right_str.replace(",", ""))

    if left_num is not None and right_num is not None:
        return left_num == right_num
    if (left_num is None) != (right_num is None):
        return False

    return left_str == right_str


def normalize_column_key(name: str) -> str:
    return re.sub(r"[\s_\-]+", "", canonical_text(name).lower())


def normalize_key_value(value: Any) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(INDIC_DIGIT_MAP)
    text = ZERO_WIDTH_RE.sub("", text)
    text = re.sub(r"\s+", "", text).strip().lower()
    if not text:
        return ""

    # Canonicalize numeric-looking keys so "4.0" and "4" map to same key.
    d = _parse_decimal(text)
    if d is not None:
        if d == 0:
            return "0"
        return format(d.normalize(), "f")
    return text


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
            for row_idx in range(span):
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
                    for k in ["no", "number", "id", "code", "name", "ક્રમ", "નંબર"]
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


def get_category(col: str) -> str:
    col = canonical_text(col).lower()
    if "\u0a98\u0ab0\u0ab5\u0ac7\u0ab0\u0acb" in col:  # ઘરવેરો
        return "gharvero"
    if "\u0ab8\u0abe\u0aaa\u0abe\u0aa3\u0ac0\u0aaf\u0acb" in col:  # સાપાણીયો
        return "sapaniyo"
    return "other"


def _categories_compatible(admin_col: str, suv_col: str) -> bool:
    return get_category(admin_col) == get_category(suv_col)


# ── PATCH-1: Lowered from 0.92 → 0.75 ────────────────────────────────────────
FUZZY_COLUMN_MATCH_THRESHOLD = 0.75

MANUAL_MAP: dict[str, str] = {
    "ઘરવેરો બાકી": "ઘરવેરો GS",
    "ઘરવેરો ચાલુ": "ઘરવેરો GS_2",
    "ઘરવેરો કુલ": "ઘરવેરો GS_3",
    "સાપાણીયો બાકી": "સાપાણીયો GS",
    "સાપાણીયો ચાલુ": "સાપાણીયો GS_2",
    "સાપાણીયો કુલ": "સાપાણીયો GS_3",
}


def map_columns_smart(
    admin_cols: list[str],
    suv_cols: list[str],
    admin_key: str,
    suv_key: str,
    manual_mappings: dict[str, str] | None = None,
) -> tuple[
    list[tuple[str, str, float]],  # mapped pairs (admin_col, suv_col, confidence)
    list[str],  # unmapped admin cols
    list[str],  # unmapped suv cols
]:
    """
    PATCH-2: Returns three values now:
      • mapped_pairs  — list of (admin_col, suv_col, confidence)
      • unmapped_admin — admin cols with no suvidha counterpart
      • unmapped_suv   — suvidha cols with no admin counterpart

    manual_mappings: dict {admin_col_name: suv_col_name} for hard overrides.
    Threshold lowered to 0.70 to catch transliterated Gujarati/English headers.
    """
    admin_candidates = [c for c in admin_cols if c != admin_key]
    suv_candidates = [c for c in suv_cols if c != suv_key]

    suv_norm = {normalize_column_key(c): c for c in suv_candidates}
    admin_norm = {normalize_column_key(c): c for c in admin_candidates}
    used_suv: set[str] = set()
    mapped: list[tuple[str, str, float]] = []

    # Canonical aliases to improve Gujarati <-> English fallback matching.
    aliases = {
        "gsn": {"gsn", "નંબર", "number", "no", "ક્રમ"},
        "number": {"number", "no", "નંબર", "ક્રમ", "gsn"},
        "baki": {"baki", "બાકી"},
        "chalu": {"chalu", "ચાલુ"},
        "kul": {"kul", "કુલ", "total"},
        "gharvero": {"gharvero", "ઘરવેરો"},
        "sapaniyo": {"sapaniyo", "સાપાણીયો"},
    }

    def _expand_alias_tokens(col_name: str) -> set[str]:
        text = canonical_text(col_name).lower()
        text = re.sub(r"[^\w\u0A80-\u0AFF]+", " ", text)
        toks = {t for t in text.split() if t}
        expanded = set(toks)
        for t in list(toks):
            if t in aliases:
                expanded.update(aliases[t])
            for root, group in aliases.items():
                if t in group:
                    expanded.update(group)
                    expanded.add(root)
        return expanded

    def _alias_score(a_col: str, s_col: str) -> float:
        a_toks = _expand_alias_tokens(a_col)
        s_toks = _expand_alias_tokens(s_col)
        if not a_toks or not s_toks:
            return 0.0
        inter = len(a_toks & s_toks)
        union = len(a_toks | s_toks)
        return inter / union if union else 0.0

    # Pass 0: manual overrides
    if manual_mappings:
        applied_manual: list[dict[str, str]] = []
        for ac_raw, sc_raw in manual_mappings.items():
            ac = admin_norm.get(normalize_column_key(ac_raw))
            sc = suv_norm.get(normalize_column_key(sc_raw))
            # Manual mapping MUST override fuzzy/category constraints.
            if ac and sc and sc not in used_suv:
                mapped.append((ac, sc, 1.0))
                used_suv.add(sc)
                applied_manual.append({"admin_col": ac, "suvidha_col": sc})
        _debug_log("manual_mapping_applied", applied_manual)

    # Pass 1: exact normalised key match
    for ac in admin_candidates:
        if any(x[0] == ac for x in mapped):
            continue
        sc = suv_norm.get(normalize_column_key(ac))
        if sc and sc not in used_suv:
            mapped.append((ac, sc, 1.0))
            used_suv.add(sc)

    # Pass 2: alias-aware normalized fallback before fuzzy.
    for ac in admin_candidates:
        if any(x[0] == ac for x in mapped):
            continue
        best_sc, best_alias = None, 0.0
        for sc in suv_candidates:
            if sc in used_suv:
                continue
            alias_score = _alias_score(ac, sc)
            if alias_score > best_alias:
                best_alias = alias_score
                best_sc = sc
        if best_sc and best_alias >= 0.34:
            mapped.append((ac, best_sc, round(best_alias, 4)))
            used_suv.add(best_sc)

    # Pass 3: fuzzy match at relaxed threshold (PATCH-1)
    for ac in admin_candidates:
        if any(x[0] == ac for x in mapped):
            continue
        best_sc, best_score = None, 0.0
        na = normalize_column_key(ac)
        for sc in suv_candidates:
            if sc in used_suv:
                continue
            # Keep category guard for tax-family columns, but do not block "other".
            ca = get_category(ac)
            cs = get_category(sc)
            if ca != "other" and cs != "other" and ca != cs:
                continue
            score = _similarity(na, normalize_column_key(sc))
            if score > best_score:
                best_score = score
                best_sc = sc
        if best_sc and best_score >= FUZZY_COLUMN_MATCH_THRESHOLD:
            mapped.append((ac, best_sc, round(best_score, 4)))
            used_suv.add(best_sc)

    mapped_admin_set = {a for a, _, _ in mapped}
    mapped_suv_set = used_suv

    unmapped_admin = [c for c in admin_candidates if c not in mapped_admin_set]
    unmapped_suv = [c for c in suv_candidates if c not in mapped_suv_set]

    # PATCH-6: log unmapped columns
    _debug_log("unmapped_admin_cols", unmapped_admin)
    _debug_log("unmapped_suv_cols", unmapped_suv)

    return mapped, unmapped_admin, unmapped_suv


def _build_key_index(
    rows: list[dict[str, str]],
    key_col: str,
    row_position_map: dict[int, int],
) -> tuple[dict[str, list[dict[str, Any]]], int, list[dict[str, Any]]]:
    idx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing = 0
    missing_rows: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        raw_key = row.get(key_col)
        key = normalize_key_value(raw_key)
        excel_row = row_position_map.get(i, -1)
        item = {
            "df_row_index": i,
            "excel_row": excel_row,
            "row": row,
            "display_key": canonical_text(raw_key),
        }
        if not key:
            missing += 1
            missing_rows.append(item)
            continue
        idx[key].append(item)
    return idx, missing, missing_rows


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

    Compare mapped columns and include unmapped columns as COLUMN_NOT_FOUND
    only when the source side has a non-empty value.
    """
    diffs: dict[str, Any] = {}

    def _position(col: str) -> dict[str, int]:
        return admin_column_position_map.get(
            col,
            {
                "df_index": admin_columns.index(col) if col in admin_columns else -1,
                "excel_col": admin_columns.index(col) + 1
                if col in admin_columns
                else -1,
            },
        )

    s_norm_lookup = {normalize_column_key(k): k for k in s.keys()}

    def _resolve_suv_col(mapped_col: str) -> str | None:
        if mapped_col in s:
            return mapped_col
        return s_norm_lookup.get(normalize_column_key(mapped_col))

    for ac, sc, confidence in col_pairs:
        if ac not in a:
            continue
        resolved_sc = _resolve_suv_col(sc)
        if not resolved_sc:
            continue
        av = canonical_text(a.get(ac, ""))
        sv = canonical_text(s.get(resolved_sc, ""))
        if not strict_values_equal(av, sv):
            diffs[ac] = {
                "admin": av,
                "suvidha": sv,
                "suv_col": resolved_sc,
                "confidence": round(confidence, 4),
                **_position(ac),
            }

    for ac in unmapped_admin:
        av = canonical_text(a.get(ac, ""))
        if av:
            diffs[ac] = {
                "admin": av,
                "suvidha": COLUMN_NOT_FOUND,
                "suv_col": None,
                "confidence": 0.0,
                **_position(ac),
            }

    for sc in unmapped_suv:
        sv = canonical_text(s.get(sc, ""))
        if sv:
            diffs[f"[SUV] {sc}"] = {
                "admin": COLUMN_NOT_FOUND,
                "suvidha": sv,
                "suv_col": sc,
                "admin_col": None,
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

    admin_idx, admin_missing_keys, admin_missing_rows = _build_key_index(
        admin.rows, admin_key, admin.row_position_map
    )
    suv_idx, suv_missing_keys, suv_missing_rows = _build_key_index(
        suv.rows, suv_key, suv.row_position_map
    )

    col_pairs, unmapped_admin, unmapped_suv = map_columns_smart(
        admin.columns, suv.columns, admin_key, suv_key, manual_mappings
    )
    pair_lookup = {a: s for a, s, _ in col_pairs}

    # Always align key columns in output rows.
    pair_lookup[admin_key] = suv_key

    # Deterministic fallback mapping for missed columns:
    # exact normalized-name match, then safe fuzzy match.
    used_suv_cols = set(pair_lookup.values())
    suv_candidate_cols = [c for c in suv.columns if c != suv_key]
    for admin_col in admin.columns:
        if admin_col == admin_key or admin_col in pair_lookup:
            continue
        admin_norm = normalize_column_key(admin_col)
        exact_match = None
        for suv_col in suv_candidate_cols:
            if suv_col in used_suv_cols:
                continue
            if normalize_column_key(suv_col) == admin_norm:
                exact_match = suv_col
                break
        if exact_match:
            pair_lookup[admin_col] = exact_match
            used_suv_cols.add(exact_match)

    for admin_col in admin.columns:
        if admin_col == admin_key or admin_col in pair_lookup:
            continue
        best_col = None
        best_score = 0.0
        admin_norm = normalize_column_key(admin_col)
        for suv_col in suv_candidate_cols:
            if suv_col in used_suv_cols:
                continue
            score = _similarity(admin_norm, normalize_column_key(suv_col))
            if score > best_score:
                best_score = score
                best_col = suv_col
        if best_col and best_score >= FUZZY_COLUMN_MATCH_THRESHOLD:
            pair_lookup[admin_col] = best_col
            used_suv_cols.add(best_col)

    unmapped_admin = [
        c for c in admin.columns if c not in pair_lookup and c != admin_key
    ]
    unmapped_suv = [
        c for c in suv.columns if c not in used_suv_cols and c != suv_key
    ]
    if not pair_lookup:
        # Safe fallback: direct same-name column mapping only.
        for col in admin.columns:
            if col == admin_key:
                continue
            if col in suv.columns and col != suv_key:
                pair_lookup[col] = col
        if pair_lookup:
            col_pairs = [(a, s, 1.0) for a, s in pair_lookup.items()]
            unmapped_admin = [c for c in admin.columns if c not in pair_lookup and c != admin_key]
            unmapped_suv = [c for c in suv.columns if c not in set(pair_lookup.values()) and c != suv_key]

    _debug_log("column_mapping", pair_lookup)
    _debug_log("unmapped_admin_cols", unmapped_admin)
    _debug_log("unmapped_suv_cols", unmapped_suv)

    admin_keys = set(admin_idx)
    suv_keys = set(suv_idx)
    common_keys = admin_keys & suv_keys
    only_admin_keys = admin_keys - suv_keys
    only_suv_keys = suv_keys - admin_keys

    _debug_log(
        "key_stats",
        {
            "total_admin_keys": len(admin_keys),
            "total_suvidha_keys": len(suv_keys),
            "common_keys": len(common_keys),
            "only_admin_keys": len(only_admin_keys),
            "only_suvidha_keys": len(only_suv_keys),
            "admin_missing_key_rows": admin_missing_keys,
            "suvidha_missing_key_rows": suv_missing_keys,
        },
    )

    def align_rows(
        admin_row: dict[str, Any],
        suv_row: dict[str, Any],
        admin_columns: list[str],
        lookup: dict[str, str],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        aligned_admin: dict[str, Any] = {}
        aligned_suv: dict[str, Any] = {}

        row_norm_lookup: dict[str, str] = {}
        for suv_col_name in suv_row.keys():
            nk = normalize_column_key(suv_col_name)
            if nk and nk not in row_norm_lookup:
                row_norm_lookup[nk] = suv_col_name

        def _resolve_suv_value(admin_col_name: str) -> Any:
            mapped = lookup.get(admin_col_name)
            if mapped and mapped in suv_row:
                return suv_row.get(mapped, "")
            if admin_col_name in suv_row:
                return suv_row.get(admin_col_name, "")

            nk = normalize_column_key(admin_col_name)
            direct = row_norm_lookup.get(nk)
            if direct:
                return suv_row.get(direct, "")

            # Runtime fuzzy fallback prevents blank Suvidha cells in output.
            best_col = None
            best_score = 0.0
            for suv_col_name in suv_row.keys():
                score = _similarity(nk, normalize_column_key(suv_col_name))
                if score > best_score:
                    best_score = score
                    best_col = suv_col_name
            if best_col and best_score >= FUZZY_COLUMN_MATCH_THRESHOLD:
                return suv_row.get(best_col, "")
            return ""

        for col in admin_columns:
            aligned_admin[col] = admin_row.get(col, "")
            aligned_suv[col] = _resolve_suv_value(col)
        return aligned_admin, aligned_suv

    discrepancies: list[dict[str, Any]] = []
    matching_records = 0
    duplicate_key_conflicts: list[dict[str, Any]] = []
    col_mismatch_counter: dict[str, int] = defaultdict(int)

    only_admin_rows: list[dict[str, Any]] = [x["row"] for x in admin_missing_rows]
    only_suv_rows: list[dict[str, Any]] = [x["row"] for x in suv_missing_rows]

    for key in sorted(only_admin_keys):
        only_admin_rows.extend(item["row"] for item in admin_idx[key])
    for key in sorted(only_suv_keys):
        only_suv_rows.extend(item["row"] for item in suv_idx[key])

    for key in sorted(common_keys):
        a_rows = admin_idx[key]
        s_rows = suv_idx[key]

        if len(a_rows) > 1 or len(s_rows) > 1:
            duplicate_key_conflicts.append(
                {
                    "key": key,
                    "admin_count": len(a_rows),
                    "suvidha_count": len(s_rows),
                }
            )

        unmatched_s: set[int] = set(range(len(s_rows)))

        for a_item in a_rows:
            if not unmatched_s:
                only_admin_rows.append(a_item["row"])
                continue

            best_idx = -1
            best_diffs: dict[str, Any] | None = None
            best_score: tuple[int, int, int] | None = None

            for s_idx in sorted(unmatched_s):
                s_item = s_rows[s_idx]
                diffs = _compare_row_pair(
                    a_item["row"],
                    s_item["row"],
                    col_pairs,
                    unmapped_admin,
                    unmapped_suv,
                    admin.column_position_map,
                    admin.columns,
                )
                score = (
                    len(diffs),
                    s_item.get("excel_row", 10**9)
                    if s_item.get("excel_row", -1) >= 0
                    else 10**9,
                    s_idx,
                )
                if best_score is None or score < best_score:
                    best_score = score
                    best_idx = s_idx
                    best_diffs = diffs

            matched_s_item = s_rows[best_idx]
            unmatched_s.remove(best_idx)
            row_diffs = best_diffs or {}

            diff_cols = [c for c in admin.columns if c in row_diffs]
            diff_cols.extend([c for c in row_diffs.keys() if c not in diff_cols])

            aligned_admin, aligned_suv = align_rows(
                a_item["row"], matched_s_item["row"], admin.columns, pair_lookup
            )

            if diff_cols:
                for dc in diff_cols:
                    col_mismatch_counter[dc] += 1
                discrepancies.append(
                    {
                        "key": a_item["display_key"] or key,
                        "normalized_key": key,
                        "row_index": a_item["df_row_index"],
                        "admin_excel_row": a_item["excel_row"],
                        "suvidha_excel_row": matched_s_item["excel_row"],
                        "admin_row": aligned_admin,
                        "suv_row": aligned_suv,
                        "diffs": row_diffs,
                        "diff_cols": diff_cols,
                    }
                )
            else:
                matching_records += 1

        for s_idx in sorted(unmatched_s):
            only_suv_rows.append(s_rows[s_idx]["row"])

    disc_count = len(discrepancies)

    total_admin_rows = len(admin.rows)
    total_suv_rows = len(suv.rows)
    audit_admin = matching_records + disc_count + len(only_admin_rows)
    audit_suv = matching_records + disc_count + len(only_suv_rows)

    validation_note = ""
    if audit_admin != total_admin_rows or audit_suv != total_suv_rows:
        validation_note = "WARNING: Row accounting mismatch detected."

    _debug_log("col_mismatch_frequency", dict(col_mismatch_counter))
    _debug_log(
        "reconciliation_result",
        {
            "total_admin_rows": total_admin_rows,
            "total_suvidha_rows": total_suv_rows,
            "matching_records": matching_records,
            "discrepancies": disc_count,
            "only_admin_rows": len(only_admin_rows),
            "only_suv_rows": len(only_suv_rows),
            "admin_accounted_for": f"{audit_admin}/{total_admin_rows}",
            "suvidha_accounted_for": f"{audit_suv}/{total_suv_rows}",
            "zero_loss_admin": audit_admin == total_admin_rows,
            "zero_loss_suvidha": audit_suv == total_suv_rows,
            "validation_note": validation_note,
        },
    )

    total_records = max(total_admin_rows, total_suv_rows)

    output_col_pairs = [
        {"admin_col": a, "suv_col": s, "confidence": 1.0}
        for a, s in pair_lookup.items()
    ]

    return {
        "discrepancies": discrepancies,
        "only_admin_rows": only_admin_rows,
        "only_suv_rows": only_suv_rows,
        "col_pairs": output_col_pairs,
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
            "compared_keys": len(common_keys),
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
            "total": total_records,
            "matched": matching_records,
            "disc": disc_count,
            "only_a": len(only_admin_rows),
            "only_s": len(only_suv_rows),
            "validation_note": validation_note,
        },
    }

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

    # Include SUV-only diff keys as dedicated output columns.
    suv_only_diff_cols: list[str] = []
    for disc in result.get("discrepancies", []):
        for diff_key in (disc.get("diffs", {}) or {}).keys():
            if isinstance(diff_key, str) and diff_key.startswith("[SUV] "):
                label = diff_key.replace("[SUV] ", "SUV::", 1)
                if label not in suv_only_diff_cols:
                    suv_only_diff_cols.append(label)

    SOURCE_COL = 1
    report_cols = all_admin_cols + suv_only_diff_cols
    col_to_excel: dict[str, int] = {c: i + 2 for i, c in enumerate(report_cols)}

    def _resolve_suv_value_for_report(
        suv_row: dict[str, Any],
        admin_col: str,
        mapped_suv_col: str | None,
    ) -> Any:
        # 1) direct aligned key (preferred)
        if admin_col in suv_row and suv_row.get(admin_col) not in ("", None):
            return suv_row.get(admin_col, "")
        # 2) mapped Suvidha key from backend col_pairs
        if (
            mapped_suv_col
            and mapped_suv_col in suv_row
            and suv_row.get(mapped_suv_col) not in ("", None)
        ):
            return suv_row.get(mapped_suv_col, "")
        # 3) normalized key fallback
        target_norm = normalize_column_key(admin_col)
        mapped_norm = normalize_column_key(mapped_suv_col or "")
        for suv_col_name, val in (suv_row or {}).items():
            nk = normalize_column_key(suv_col_name)
            if nk == target_norm or (mapped_norm and nk == mapped_norm):
                if val not in ("", None):
                    return val
        # 4) fuzzy fallback as last resort
        best_key = None
        best_score = 0.0
        for suv_col_name in (suv_row or {}).keys():
            score = _similarity(target_norm, normalize_column_key(suv_col_name))
            if score > best_score:
                best_score = score
                best_key = suv_col_name
        if best_key and best_score >= FUZZY_COLUMN_MATCH_THRESHOLD:
            return suv_row.get(best_key, "")
        return ""

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
            if col_name.startswith("SUV::"):
                raw_suv_col = col_name.replace("SUV::", "", 1)
                suv_diff_key = f"[SUV] {raw_suv_col}"
                if suv_diff_key in diffs:
                    val = COLUMN_NOT_FOUND
                    cell.value = val
                    cell.font = cnf_font
                    cell.fill = PatternFill("solid", start_color="FFF9C4")
                    cell.border = _border()
                    cell.alignment = Alignment(horizontal="left", vertical="center")
                    continue
                else:
                    val = ""
            else:
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

            if col_name.startswith("SUV::"):
                raw_suv_col = col_name.replace("SUV::", "", 1)
                suv_diff_key = f"[SUV] {raw_suv_col}"
                if suv_diff_key in diffs:
                    val = diffs[suv_diff_key].get("suvidha", "")
                    cell.value = excel_safe_text(val)
                    cell.font = suv_mismatch_font
                    cell.fill = PatternFill("solid", start_color="E8F8F3")
                    cell.border = _border()
                    cell.alignment = Alignment(horizontal="left", vertical="center")
                else:
                    cell.value = ""
                    _style_cell(cell, fill_hex=SUVIDHA_BG)
                continue

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
                # Suvidha rows are already aligned to admin columns in reconcile().
                # Keep safe fallback to mapped/source column names for backward compatibility.
                suv_col = suv_col_lookup.get(col_name, col_name)
                val = _resolve_suv_value_for_report(suv_row, col_name, suv_col)
                cell.value = excel_safe_text(val)
                _style_cell(cell, fill_hex=SUVIDHA_BG)

        current_row += 1

        # ── Separator ─────────────────────────────────────────────────────────
        for c in range(1, len(report_cols) + 2):
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
    merged = dict(MANUAL_MAP)
    if not form_value:
        return merged if merged else None
    try:
        data = json.loads(form_value)
        if isinstance(data, dict):
            merged.update({str(k): str(v) for k, v in data.items()})
            return merged
    except (json.JSONDecodeError, TypeError):
        pass
    return merged if merged else None


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
        admin_key = (
            _resolve_key_column("Number", admin.columns)
            if any(normalize_column_key(c) == normalize_column_key("Number") for c in admin.columns)
            else _resolve_key_column(admin_key_raw, admin.columns)
        )
        suv_key = (
            _resolve_key_column("GSN", suv.columns)
            if any(normalize_column_key(c) == normalize_column_key("GSN") for c in suv.columns)
            else _resolve_key_column(suv_key_raw, suv.columns)
        )

        # Optional manual column overrides from frontend
        manual_mappings = _parse_manual_mappings(request.form.get("manual_mappings"))

        result = reconcile(admin, suv, admin_key, suv_key, manual_mappings)
        result["ingestion"] = {
            "admin": {
                "detected_header_row": admin.header_row_index + 1,
                "detected_header_span": admin.header_row_span,
                "notes": admin.parser_notes,
                "row_map_size": len(admin.excel_row_numbers),
            },
            "suvidha": {
                "detected_header_row": suv.header_row_index + 1,
                "detected_header_span": suv.header_row_span,
                "notes": suv.parser_notes,
                "row_map_size": len(suv.excel_row_numbers),
            },
        }
        return jsonify(result)

    except ReconciliationError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        import traceback

        print(f"Reconcile error: {e}\n{traceback.format_exc()}")
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
        admin_key = (
            _resolve_key_column("Number", admin.columns)
            if any(normalize_column_key(c) == normalize_column_key("Number") for c in admin.columns)
            else _resolve_key_column(admin_key_raw, admin.columns)
        )
        suv_key = (
            _resolve_key_column("GSN", suv.columns)
            if any(normalize_column_key(c) == normalize_column_key("GSN") for c in suv.columns)
            else _resolve_key_column(suv_key_raw, suv.columns)
        )

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

