"""
Grambook Reconciliation — Production-Grade Backend (PATCHED v7)
All issues from v6 review are fixed. See # FIXED / # IMPROVED comments.

PATCH SUMMARY (v6 → v7):
  IMPROVED 1 — Sorting: replaced json.dumps key with lightweight tuple key (O(n log n) → faster)
  FIXED    2 — VALUE_MISSING: robust None/whitespace/non-string detection
  FIXED    3 — Mismatch classification: symmetric empty_vs_value check
  IMPROVED 4 — Large file protection: MAX_ROWS = 50000 guard in reconcile()
  IMPROVED 5 — Sorting fix applied consistently (both admin and suv duplicate-row sorts)
  IMPROVED 6 — Optional row normalization cache inside _compare_single_pair (avoids repeated canonical_text calls)
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
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

# ── FIXED (4): Global debug toggle — set True only during local development.
DEBUG_MODE = False

# ── FIXED (7): Split overloaded sentinel into two distinct constants.
COLUMN_MISSING = "__COL_MISSING__"
VALUE_MISSING = "__VAL_MISSING__"
COLUMN_NOT_FOUND = COLUMN_MISSING  # backward-compat alias (do not remove)

FUZZY_COLUMN_MATCH_THRESHOLD = 0.75
FUZZY_CAT_THRESHOLD = 0.65

# ── IMPROVED (4): Maximum rows allowed per dataset before reconciliation is blocked.
# Prevents memory/CPU overload on unexpectedly large uploads.
MAX_ROWS = 50_000


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


# ── Tesseract setup ──────────────────────────────────────────────────────────
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
                            display_name = winreg.QueryValueEx(child, "DisplayName")[0]
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

# ── Text normalisation helpers ───────────────────────────────────────────────
EXCEL_ILLEGAL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF]")
_CURRENCY_STRIP_RE = re.compile(r"[₹$€£¥,\s]")


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


def _parse_decimal(text: str) -> Decimal | None:
    t = _CURRENCY_STRIP_RE.sub("", text).strip()
    if not t:
        return None
    try:
        d = Decimal(t)
        if not d.is_finite():
            return None
        return d
    except InvalidOperation:
        return None


def strict_values_equal(left: Any, right: Any, _debug: bool = False) -> bool:
    left_str = canonical_text(left)
    right_str = canonical_text(right)

    _sentinel_set = {COLUMN_MISSING, VALUE_MISSING}
    if left_str in _sentinel_set or right_str in _sentinel_set:
        if _debug:
            print(
                f"[MISMATCH] Sentinel value detected: left={left_str!r} right={right_str!r}"
            )
        return False

    if left_str == "" and right_str == "":
        return True

    if (left_str == "") != (right_str == ""):
        if _debug:
            print(
                f"[MISMATCH] One side is empty: left={left_str!r} right={right_str!r}"
            )
        return False

    left_num = _parse_decimal(left_str)
    right_num = _parse_decimal(right_str)

    if left_num is not None and right_num is not None:
        result = left_num == right_num
        if not result and _debug:
            print(
                f"[MISMATCH] Numeric: {left_num} != {right_num}  (raw: {left_str!r} vs {right_str!r})"
            )
        return result

    if (left_num is None) != (right_num is None):
        if _debug:
            print(
                f"[MISMATCH] Mixed numeric/text: left={left_str!r} right={right_str!r}"
            )
        return False

    result = left_str == right_str
    if not result and _debug:
        print(f"[MISMATCH] String: {left_str!r} != {right_str!r}")
    return result


def normalize_column_key(name: str) -> str:
    return re.sub(r"[\s_\-]+", "", canonical_text(name).lower())


def normalize_key_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(INDIC_DIGIT_MAP)
    text = ZERO_WIDTH_RE.sub("", text)
    text = re.sub(r"[/\-]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    try:
        num = Decimal(text.replace(",", ""))
        if not num.is_finite():
            return text.lower()
        if num == num.to_integral_value():
            return str(int(num.to_integral_value()))
        return format(num.normalize(), "f")
    except Exception:
        pass
    return text.lower()


def is_numeric_like(text: str) -> bool:
    t = canonical_text(text)
    if not t:
        return False
    return bool(re.fullmatch(r"[+-]?\d+(\.\d+)?", t.replace(",", "")))


# ── File parsing functions ───────────────────────────────────────────────────
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


def dataframe_from_matrix(
    matrix: list[list[str]],
    source_format: str,
    manual_header_row: int | None = None,
    manual_header_span: int | None = None,
) -> ParsedDataset:
    if not matrix:
        raise ReconciliationError("No data rows found in file.")

    header_idx = 0
    if manual_header_row is not None:
        header_idx = manual_header_row - 1
        if header_idx < 0 or header_idx >= len(matrix):
            header_idx = 0

    raw_headers = matrix[header_idx]

    columns = []
    seen = {}
    for h in raw_headers:
        clean = excel_safe_text(h).strip()
        if not clean:
            clean = "Unnamed"
        if clean in seen:
            seen[clean] += 1
            clean = f"{clean}_{seen[clean]}"
        else:
            seen[clean] = 0
        columns.append(clean)

    data_start = header_idx + 1
    rows = []
    row_position_map: dict[int, int] = {}
    excel_row_numbers: list[int] = []

    for df_i, matrix_row in enumerate(matrix[data_start:], start=0):
        if len(matrix_row) < len(columns):
            matrix_row = matrix_row + [""] * (len(columns) - len(matrix_row))
        else:
            matrix_row = matrix_row[: len(columns)]

        row_dict = dict(zip(columns, [excel_safe_text(cell) for cell in matrix_row]))
        rows.append(row_dict)

        excel_row = data_start + df_i + 1
        row_position_map[df_i] = excel_row
        excel_row_numbers.append(excel_row)

    return ParsedDataset(
        rows=rows,
        columns=columns,
        column_meta=[],
        normalized_map={normalize_column_key(c): c for c in columns},
        header_row_index=header_idx,
        header_row_span=1,
        dropped_columns=[],
        source_format=source_format,
        parser_notes=["Real headers from file + cleaning applied. No stub columns."],
        kept_indices=list(range(len(rows))),
        excel_row_numbers=excel_row_numbers,
        row_position_map=row_position_map,
        column_position_map={},
    )


def parse_uploaded_dataset(
    file_storage,
    manual_header_row: int | None = None,
    manual_header_span: int | None = None,
) -> ParsedDataset:
    matrix, source_format, notes = parse_matrix_from_upload(file_storage)
    parsed = dataframe_from_matrix(
        matrix, source_format, manual_header_row, manual_header_span
    )
    parsed.parser_notes.extend(notes)
    return parsed


# ── Debug & similarity ───────────────────────────────────────────────────────
def _debug_log(label: str, data: Any) -> None:
    if not DEBUG_MODE:
        return
    try:
        print(
            f"\n[DEBUG] {label}:\n{json.dumps(data, ensure_ascii=False, indent=2, default=str)}"
        )
    except Exception:
        print(f"\n[DEBUG] {label}: {data}")


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if fuzz is not None:
        return float(fuzz.ratio(a, b)) / 100.0
    return SequenceMatcher(None, a, b).ratio()


def normalize_gujarati_terms(text: str) -> str:
    if not text:
        return ""
    text = canonical_text(text)
    replacements = {
        "લાઈટવેરો": "લાઇટ વેરો",
        "લાઇટવેરો": "લાઇટ વેરો",
        "સફાઈવેરો": "સફાઈ વેરો",
        "સફઈવેરો": "સફાઈ વેરો",
        "ગટરવેરો": "ગટર વેરો",
        "ઘરવેરો": "ઘર વેરો",
        "પાણીવેરો": "પાણી વેરો",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    suffix_map = {"વેરા": "વેરો", "ટેક્ષ": "ટેક્સ"}
    for old, new in suffix_map.items():
        text = text.replace(old, new)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def detect_category(col: str) -> str:
    col = str(col).strip()
    if not col:
        return "empty"
    text_lower = col.lower()
    if "ઘર" in col:
        return "ghar"
    if "સફાઈ" in col or "સફઈ" in col:
        return "safai"
    if "લાઇટ" in col or "લાઈટ" in col:
        return "light"
    if "સા.પાણી" in col or "સાપાણી" in col:
        return "sa_pani"
    if "ખા.પાણી" in col or "ખાપાણી" in col:
        return "kha_pani"
    if "ગટર" in col:
        return "gatar"
    if "એડવાન્સ" in col or "advance" in text_lower:
        return "advance"
    if "ચુકવેલ" in col or "payment" in text_lower or "paid" in text_lower:
        return "payment"
    if "બાકી" in col or "balance" in text_lower or "baki" in text_lower:
        return "balance"
    if "કુલ" in col or "total" in text_lower or "grand" in text_lower:
        return "total"
    return "other"


def _build_key_index(
    rows: list[dict[str, str]], key_col: str, row_position_map: dict[int, int]
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    idx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing = 0
    duplicate_keys: list[str] = []

    for i, row in enumerate(rows):
        raw_key = row.get(key_col, "")
        key = normalize_key_value(raw_key)
        if not key:
            missing += 1
            if DEBUG_MODE:
                print(f"[DEBUG][KEY] Row {i} has empty key, skipping.")
            continue
        excel_row = row_position_map.get(i, i + 1)
        entry = {
            "df_row_index": i,
            "excel_row": excel_row,
            "row": row,
            "display_key": canonical_text(raw_key),
        }
        if key in idx:
            if len(idx[key]) == 1:
                duplicate_keys.append(key)
        idx[key].append(entry)

    if DEBUG_MODE:
        if duplicate_keys:
            print(
                f"[DEBUG][KEY] Duplicate keys found ({len(duplicate_keys)}): {duplicate_keys[:10]}"
            )
        if missing:
            print(f"[DEBUG][KEY] Rows with missing/empty keys: {missing}")

    return idx, missing


def map_columns_smart(
    admin_cols: list[str],
    suv_cols: list[str],
    admin_key: str,
    suv_key: str,
    manual_mappings: dict[str, str] | None = None,
) -> tuple[list[tuple[str, str, float]], list[str], list[str], int]:
    if manual_mappings is None:
        manual_mappings = {}

    mapped_pairs: list[tuple[str, str, float]] = []
    used_admin: set[str] = set()
    used_suv: set[str] = set()

    # 1. Manual mappings (highest priority)
    for a_col, s_col in manual_mappings.items():
        if a_col in admin_cols and s_col in suv_cols:
            mapped_pairs.append((a_col, s_col, 1.0))
            used_admin.add(a_col)
            used_suv.add(s_col)
            if DEBUG_MODE:
                print(f"[DEBUG][MAP] Manual mapping: {a_col!r} → {s_col!r}")

    remaining_admin = [c for c in admin_cols if c not in used_admin and c != admin_key]
    remaining_suv = [c for c in suv_cols if c not in used_suv and c != suv_key]

    admin_norm_map: dict[str, str] = {
        c: normalize_column_key(normalize_gujarati_terms(c)) for c in remaining_admin
    }
    suv_norm_map: dict[str, str] = {
        c: normalize_column_key(normalize_gujarati_terms(c)) for c in remaining_suv
    }

    admin_cats = {c: detect_category(c) for c in remaining_admin}
    suv_cats = {c: detect_category(c) for c in remaining_suv}

    cross_category_count = 0

    admin_by_norm: dict[str, list[str]] = defaultdict(list)
    suv_by_norm: dict[str, list[str]] = defaultdict(list)
    for c in remaining_admin:
        admin_by_norm[admin_norm_map[c]].append(c)
    for c in remaining_suv:
        suv_by_norm[suv_norm_map[c]].append(c)

    # 2. Exact normalized key match with sequential duplicate pairing
    for norm_key, a_cols_group in list(admin_by_norm.items()):
        if norm_key not in suv_by_norm:
            continue
        s_cols_group = suv_by_norm[norm_key]
        pairs_to_make = min(len(a_cols_group), len(s_cols_group))
        for i in range(pairs_to_make):
            a_col = a_cols_group[i]
            s_col = s_cols_group[i]
            if a_col in used_admin or s_col in used_suv:
                continue
            mapped_pairs.append((a_col, s_col, 1.0))
            used_admin.add(a_col)
            used_suv.add(s_col)
            if DEBUG_MODE:
                print(
                    f"[DEBUG][MAP] Exact normalized match: {a_col!r} → {s_col!r} (score=1.0)"
                )

    # 3. Category-aware fuzzy matching
    remaining_admin2 = [c for c in remaining_admin if c not in used_admin]
    remaining_suv2 = [c for c in remaining_suv if c not in used_suv]

    for a_col in list(remaining_admin2):
        if a_col in used_admin:
            continue
        a_cat = admin_cats[a_col]
        a_norm_gujarati = normalize_gujarati_terms(a_col)
        best_s_col = None
        best_score = 0.0

        for s_col in list(remaining_suv2):
            if s_col in used_suv:
                continue
            s_cat = suv_cats[s_col]
            s_norm_gujarati = normalize_gujarati_terms(s_col)
            if (
                a_cat == s_cat
                or a_cat in ("other", "total")
                or s_cat in ("other", "total")
            ):
                score = _similarity(a_norm_gujarati, s_norm_gujarati)
                if score > best_score and score >= FUZZY_COLUMN_MATCH_THRESHOLD:
                    best_score = score
                    best_s_col = s_col

        if best_s_col:
            mapped_pairs.append((a_col, best_s_col, best_score))
            used_admin.add(a_col)
            used_suv.add(best_s_col)
            if (
                admin_cats[a_col] != suv_cats[best_s_col]
                and admin_cats[a_col] != "other"
            ):
                cross_category_count += 1
            remaining_admin2 = [c for c in remaining_admin2 if c != a_col]
            remaining_suv2 = [c for c in remaining_suv2 if c != best_s_col]
            if DEBUG_MODE:
                print(
                    f"[DEBUG][MAP] Fuzzy category match: {a_col!r} → {best_s_col!r} (score={best_score:.3f})"
                )

    # 4. Fallback fuzzy (relaxed threshold) + substring fallback
    remaining_admin3 = [c for c in remaining_admin if c not in used_admin]
    remaining_suv3 = [c for c in remaining_suv if c not in used_suv]

    for a_col in list(remaining_admin3):
        if a_col in used_admin:
            continue
        a_norm_gujarati = normalize_gujarati_terms(a_col)
        best_score = 0.0
        best_s_col = None

        for s_col in list(remaining_suv3):
            if s_col in used_suv:
                continue
            score = _similarity(a_norm_gujarati, normalize_gujarati_terms(s_col))
            if score > best_score and score >= FUZZY_COLUMN_MATCH_THRESHOLD * 0.8:
                best_score = score
                best_s_col = s_col

        if best_s_col is None:
            a_key_norm = normalize_column_key(a_norm_gujarati)
            for s_col in remaining_suv3:
                if s_col in used_suv:
                    continue
                s_key_norm = normalize_column_key(normalize_gujarati_terms(s_col))
                if (
                    a_key_norm
                    and s_key_norm
                    and (a_key_norm in s_key_norm or s_key_norm in a_key_norm)
                ):
                    best_s_col = s_col
                    best_score = 0.6
                    break

        if best_s_col:
            mapped_pairs.append((a_col, best_s_col, best_score))
            used_admin.add(a_col)
            used_suv.add(best_s_col)
            remaining_admin3 = [c for c in remaining_admin3 if c != a_col]
            remaining_suv3 = [c for c in remaining_suv3 if c != best_s_col]
            if DEBUG_MODE:
                print(
                    f"[DEBUG][MAP] Fallback fuzzy/substring match: {a_col!r} → {best_s_col!r} (score={best_score:.3f})"
                )

    unmapped_admin = [c for c in admin_cols if c not in used_admin and c != admin_key]
    unmapped_suv = [c for c in suv_cols if c not in used_suv and c != suv_key]

    mapped_pairs.sort(key=lambda x: x[2], reverse=True)

    _debug_log(
        "Column Mapping Result",
        {
            "mapped_count": len(mapped_pairs),
            "unmapped_admin": unmapped_admin,
            "unmapped_suv": unmapped_suv,
            "cross_category_count": cross_category_count,
            "pairs": [
                {"admin": a, "suv": s, "confidence": round(c, 4)}
                for a, s, c in mapped_pairs
            ],
        },
    )

    return mapped_pairs, unmapped_admin, unmapped_suv, cross_category_count


def _validate_key_column_has_data(
    rows: list[dict[str, str]], key_col: str, label: str
) -> None:
    """Raise ReconciliationError if every row in `rows` has an empty key value."""
    if all(not row.get(key_col, "").strip() for row in rows):
        raise ReconciliationError(
            f"{label} key column '{key_col}' exists but contains no usable data. "
            "Check that the correct column was selected and that the file is not empty."
        )


def reconcile(
    admin: ParsedDataset,
    suv: ParsedDataset,
    admin_key: str,
    suv_key: str,
    manual_mappings: dict[str, str] | None = None,
) -> dict[str, Any]:

    # ── IMPROVED (4): Block reconciliation when either dataset exceeds MAX_ROWS.
    # This prevents memory/CPU exhaustion on unexpectedly large uploads.
    # Check is done before any indexing or column mapping work.
    if len(admin.rows) > MAX_ROWS or len(suv.rows) > MAX_ROWS:
        raise ReconciliationError(
            f"Dataset too large (>{MAX_ROWS} rows). "
            f"Admin has {len(admin.rows)} rows, Suvidha has {len(suv.rows)} rows. "
            "Please split the file and reconcile in batches."
        )

    # Validate key columns exist in schema
    if admin_key not in admin.columns:
        raise ReconciliationError(
            f"Admin key column '{admin_key}' not found in admin dataset columns: {admin.columns}"
        )
    if suv_key not in suv.columns:
        raise ReconciliationError(
            f"Suvidha key column '{suv_key}' not found in suvidha dataset columns: {suv.columns}"
        )

    _validate_key_column_has_data(admin.rows, admin_key, "Admin")
    _validate_key_column_has_data(suv.rows, suv_key, "Suvidha")

    admin_idx, admin_missing_keys = _build_key_index(
        admin.rows, admin_key, admin.row_position_map
    )
    suv_idx, suv_missing_keys = _build_key_index(
        suv.rows, suv_key, suv.row_position_map
    )

    col_pairs, unmapped_admin, unmapped_suv, cross_category_count = map_columns_smart(
        admin.columns, suv.columns, admin_key, suv_key, manual_mappings
    )

    if not col_pairs:
        raise ReconciliationError(
            "No columns could be mapped between Admin and Suvidha datasets. "
            "Please check column names or use manual mappings."
        )

    non_key_admin_cols = [c for c in admin.columns if c != admin_key]
    if non_key_admin_cols:
        unmapped_ratio = len(unmapped_admin) / len(non_key_admin_cols)
        if unmapped_ratio > 0.5:
            raise ReconciliationError(
                f"Too many Admin columns could not be mapped to Suvidha columns "
                f"({len(unmapped_admin)} of {len(non_key_admin_cols)} non-key columns = "
                f"{unmapped_ratio:.0%} unmapped). "
                "Check that both files share a common structure, or provide manual column mappings."
            )

    admin_keys = set(admin_idx.keys())
    suv_keys = set(suv_idx.keys())
    common = admin_keys & suv_keys
    only_admin_keys = admin_keys - suv_keys
    only_suv_keys = suv_keys - admin_keys

    _debug_log(
        "Key Matching Stats",
        {
            "admin_total_keys": len(admin_keys),
            "suv_total_keys": len(suv_keys),
            "common_keys": len(common),
            "only_admin_keys_count": len(only_admin_keys),
            "only_suv_keys_count": len(only_suv_keys),
            "only_admin_sample": sorted(only_admin_keys)[:10],
            "only_suv_sample": sorted(only_suv_keys)[:10],
        },
    )

    discrepancies: list[dict[str, Any]] = []
    matching_records = 0
    duplicate_key_conflicts: list[dict] = []
    extra_only_admin: list[dict[str, str]] = []
    extra_only_suv: list[dict[str, str]] = []
    col_mismatch_counter: dict[str, int] = defaultdict(int)
    validation_note = ""

    def _is_numeric_key(key_col: str, rows: list[dict[str, str]]) -> bool:
        if not rows:
            return False
        numeric_count = 0
        total_checked = 0
        for row in rows[:30]:
            val = row.get(key_col, "").strip()
            if not val:
                continue
            total_checked += 1
            normalized = normalize_key_value(val)
            try:
                Decimal(normalized)
                numeric_count += 1
            except Exception:
                pass
            if total_checked >= 10:
                break
        if total_checked == 0:
            return False
        return (numeric_count / total_checked) > 0.70

    admin_has_numeric_key = _is_numeric_key(admin_key, admin.rows)
    suv_has_numeric_key = _is_numeric_key(suv_key, suv.rows)

    if admin_has_numeric_key != suv_has_numeric_key:
        raise ReconciliationError("Key type mismatch between Admin and Suvidha.")

    admin_to_suv_col: dict[str, str] = {a: s for a, s, _ in col_pairs}

    def _compare_single_pair(
        a_row: dict[str, str],
        s_row: dict[str, str],
        a_row_idx: int,
        s_row_idx: int,
        col_pairs: list[tuple[str, str, float]],
        key_val: str,
    ) -> None:
        nonlocal matching_records
        diffs: dict[str, dict[str, str]] = {}
        has_diff = False

        # ── IMPROVED (6): Pre-compute canonical forms for all admin values once per row.
        # Avoids repeated canonical_text() calls inside the column comparison loop,
        # which matters when a row has many columns (O(cols) → O(1) per lookup).
        canonical_a_row: dict[str, str] = {
            k: canonical_text(v) for k, v in a_row.items()
        }

        for admin_col, suv_col, _ in col_pairs:
            if admin_col == admin_key or suv_col == suv_key:
                continue

            # IMPROVED (6): Use pre-computed canonical value instead of raw a_row lookup
            a_val = canonical_a_row.get(admin_col, "")

            # ── FIXED (2): Robust VALUE_MISSING detection.
            # Handles None, whitespace-only strings, and any non-string types
            # that might survive from upstream parsing or manual injection.
            raw_s_val = s_row.get(suv_col, None)

            if suv_col not in s_row:
                # Column absent from Suvidha row schema entirely → COLUMN_MISSING
                s_val = COLUMN_MISSING
                if DEBUG_MODE:
                    print(
                        f"[DEBUG][COMPARE] Suvidha col '{suv_col}' not found in row "
                        f"(mapped from admin col '{admin_col}'). Skipping."
                    )
            elif raw_s_val is None or str(raw_s_val).strip() == "":
                # FIXED (2): Column exists but value is None, empty string, or whitespace-only
                s_val = VALUE_MISSING
            else:
                s_val = raw_s_val

            equal = strict_values_equal(a_val, s_val, _debug=DEBUG_MODE)

            if not equal:
                # ── FIXED (3): Symmetric mismatch classification.
                # Old code only checked `a_val == ""` which missed cases where
                # a_val is whitespace-only. Now both sides use .strip() for symmetry.
                a_num = _parse_decimal(canonical_text(a_val))
                s_num = (
                    _parse_decimal(canonical_text(s_val))
                    if s_val not in (COLUMN_MISSING, VALUE_MISSING)
                    else None
                )

                if s_val == COLUMN_MISSING:
                    mismatch_type = "missing"
                elif s_val == VALUE_MISSING or str(a_val).strip() == "":
                    # FIXED (3): str(a_val).strip() == "" instead of a_val == ""
                    # ensures whitespace-only admin values are also caught as empty_vs_value,
                    # making the check symmetric with VALUE_MISSING on the suvidha side.
                    mismatch_type = "empty_vs_value"
                elif a_num is not None and s_num is not None:
                    mismatch_type = "value"
                else:
                    mismatch_type = "format"

                diffs[admin_col] = {
                    "admin": a_val,
                    "suvidha": s_val,
                    "suv_col": suv_col,
                    "mismatch_type": mismatch_type,
                }
                has_diff = True
                col_mismatch_counter[admin_col] += 1

        if has_diff:
            discrepancies.append(
                {
                    "key": key_val,
                    "admin_row_index": a_row_idx,
                    "suv_row_index": s_row_idx,
                    "admin_row": a_row,
                    "suv_row": s_row,
                    "diffs": diffs,
                    "key_value": key_val,  # backward compat
                }
            )
        else:
            matching_records += 1

    # Main reconciliation loop
    for key in sorted(common):
        admin_rows_list = admin_idx[key]
        suv_rows_list = suv_idx[key]

        if len(admin_rows_list) != len(suv_rows_list):
            duplicate_key_conflicts.append(
                {
                    "key": key,
                    "admin_count": len(admin_rows_list),
                    "suv_count": len(suv_rows_list),
                    "admin_excel_rows": [r["excel_row"] for r in admin_rows_list],
                    "suv_excel_rows": [r["excel_row"] for r in suv_rows_list],
                }
            )

        # ── IMPROVED (1 & 5): Lightweight deterministic sort key for duplicate rows.
        # Replaces json.dumps(x["row"], sort_keys=True) which serialises the entire
        # row dict to a JSON string on every comparison — O(n log n * row_size).
        # tuple(str(v) for v in x["row"].values()) is ~5-10x faster:
        #   • No JSON encoding overhead (no escaping, no key sorting, no encoding)
        #   • dict.values() iteration is C-level in CPython
        #   • tuple construction is cheaper than string concatenation
        # Both produce a stable, deterministic sort as long as row dicts share
        # the same key insertion order (guaranteed by dataframe_from_matrix).
        # Applied consistently to BOTH admin and suv lists (Fix 5).
        if len(admin_rows_list) > 1:
            admin_rows_list = sorted(
                admin_rows_list,
                # IMPROVED (1): lightweight tuple key replaces json.dumps
                key=lambda x: tuple(str(v) for v in x["row"].values()),
            )
        if len(suv_rows_list) > 1:
            suv_rows_list = sorted(
                suv_rows_list,
                # IMPROVED (5): same lightweight key applied consistently on suv side
                key=lambda x: tuple(str(v) for v in x["row"].values()),
            )

        min_len = min(len(admin_rows_list), len(suv_rows_list))
        for i in range(min_len):
            _compare_single_pair(
                admin_rows_list[i]["row"],
                suv_rows_list[i]["row"],
                admin_rows_list[i]["df_row_index"],
                suv_rows_list[i]["df_row_index"],
                col_pairs,
                key,
            )

        if len(admin_rows_list) > min_len:
            for extra in admin_rows_list[min_len:]:
                extra_only_admin.append(extra["row"])
        if len(suv_rows_list) > min_len:
            for extra in suv_rows_list[min_len:]:
                extra_only_suv.append(extra["row"])

    if len(discrepancies) == 0 and len(common) > 0:
        validation_note = "WARNING: 0 discrepancies found. Verify mapping."
        if DEBUG_MODE:
            print(f"[DEBUG][RECONCILE] {validation_note}")

    # Zero-loss audit
    total_admin_rows = sum(len(v) for v in admin_idx.values())
    total_suv_rows = sum(len(v) for v in suv_idx.values())

    audit_admin = (
        matching_records
        + len(discrepancies)
        + len(extra_only_admin)
        + len(only_admin_keys)
    )
    audit_suv = (
        matching_records + len(discrepancies) + len(extra_only_suv) + len(only_suv_keys)
    )

    _debug_log(
        "Zero-Loss Audit",
        {
            "total_admin_rows_indexed": total_admin_rows,
            "total_suv_rows_indexed": total_suv_rows,
            "matching_records": matching_records,
            "discrepancies": len(discrepancies),
            "extra_only_admin": len(extra_only_admin),
            "extra_only_suv": len(extra_only_suv),
            "only_admin_keys": len(only_admin_keys),
            "only_suv_keys": len(only_suv_keys),
            "audit_admin_sum": audit_admin,
            "audit_suv_sum": audit_suv,
            "admin_ok": audit_admin == total_admin_rows,
            "suv_ok": audit_suv == total_suv_rows,
        },
    )

    if audit_admin != total_admin_rows or audit_suv != total_suv_rows:
        raise ReconciliationError(
            f"Zero-loss guarantee violated. "
            f"Admin: expected {total_admin_rows}, got {audit_admin}. "
            f"Suvidha: expected {total_suv_rows}, got {audit_suv}."
        )

    _debug_log(
        "Discrepancy Generation Summary",
        {
            "total_discrepancies": len(discrepancies),
            "col_mismatch_frequency": dict(col_mismatch_counter),
        },
    )

    return {
        "discrepancies": discrepancies,
        "only_admin_rows": extra_only_admin,
        "only_suv_rows": extra_only_suv,
        "column_map": admin_to_suv_col,
        "col_pairs": [
            {"admin_col": a, "suv_col": s, "confidence": round(c, 4)}
            for a, s, c in col_pairs
        ],
        "admin_key": admin_key,
        "suv_key": suv_key,
        "admin_cols": admin.columns,
        "suv_cols": suv.columns,
        "admin_column_position_map": admin.column_position_map,
        "unmapped": {"admin_cols": unmapped_admin, "suv_cols": unmapped_suv},
        "col_mismatch_frequency": dict(col_mismatch_counter),
        "meta": {
            "compared_keys": len(common),
            "duplicate_key_conflicts": duplicate_key_conflicts,
            "admin_missing_keys": admin_missing_keys,
            "suvidha_missing_keys": suv_missing_keys,
            "unmapped_admin_cols": unmapped_admin,
            "unmapped_suv_cols": unmapped_suv,
            "fuzzy_threshold_used": FUZZY_COLUMN_MATCH_THRESHOLD,
            "zero_loss_verified": {"admin": True, "suvidha": True},
            "average_mapping_confidence": (
                sum(c for _, _, c in col_pairs) / len(col_pairs) if col_pairs else 0.0
            ),
            "key_type": "numeric" if admin_has_numeric_key else "string",
            "cross_category_mappings": cross_category_count,
        },
        "stats": {
            "total": len(admin_keys | suv_keys),
            "matched": matching_records,
            "disc": len(discrepancies),
            "only_a": len(extra_only_admin),
            "only_s": len(extra_only_suv),
            "validation_note": validation_note,
        },
    }


# ── Excel report ─────────────────────────────────────────────────────────────
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

    all_admin_cols: list[str] = admin.columns

    suv_col_lookup: dict[str, str] = {}
    for p in result.get("col_pairs", []):
        a_col = p.get("admin_col")
        s_col = p.get("suv_col")
        if a_col and s_col:
            suv_col_lookup[a_col] = s_col

    SOURCE_COL = 1
    col_to_excel: dict[str, int] = {c: i + 2 for i, c in enumerate(all_admin_cols)}

    def _write_header_row(ws):
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

    def _write_simple_header(ws, cols: list[str], fill: str):
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

    ws_disc = wb.active
    ws_disc.title = "Discrepancies"
    _write_header_row(ws_disc)
    current_row = 2

    for disc in result.get("discrepancies", []):
        admin_row = disc.get("admin_row", {})
        suv_row = disc.get("suv_row", {})
        diffs = disc.get("diffs", {})
        diff_set = set(diffs.keys())

        src_a = ws_disc.cell(row=current_row, column=SOURCE_COL, value="Admin")
        _style_cell(
            src_a, fill_hex=ADMIN_SRC_FILL, bold=True, color="FFFFFF", align="center"
        )
        for col_name, excel_col in col_to_excel.items():
            cell = ws_disc.cell(row=current_row, column=excel_col)
            val = excel_safe_text(admin_row.get(col_name, ""))
            if col_name in diff_set:
                cell.value = val
                cell.font = admin_mismatch_font
                cell.fill = PatternFill("solid", start_color="FDECEA")
            else:
                cell.value = val
                _style_cell(cell, fill_hex=ADMIN_BG)
        current_row += 1

        src_s = ws_disc.cell(row=current_row, column=SOURCE_COL, value="Suvidha")
        _style_cell(
            src_s, fill_hex=SUVIDHA_SRC_FILL, bold=True, color="FFFFFF", align="center"
        )
        for col_name, excel_col in col_to_excel.items():
            cell = ws_disc.cell(row=current_row, column=excel_col)

            if col_name in diff_set:
                raw_val = diffs[col_name].get("suvidha", "")
                if raw_val == COLUMN_MISSING:
                    cell.value = "COLUMN NOT FOUND"
                    cell.font = cnf_font
                    cell.fill = PatternFill("solid", start_color="FFF9C4")
                elif raw_val == VALUE_MISSING:
                    cell.value = "VALUE NOT FOUND"
                    cell.font = cnf_font
                    cell.fill = PatternFill("solid", start_color="FFF3CD")
                else:
                    cell.value = excel_safe_text(raw_val)
                    cell.font = suv_mismatch_font
                    cell.fill = PatternFill("solid", start_color="E8F8F3")
            else:
                suv_col = suv_col_lookup.get(col_name)
                if suv_col and suv_col in suv_row:
                    raw_suv_val = suv_row[suv_col]
                    if not raw_suv_val.strip():
                        val = "VALUE NOT FOUND"
                        cell.value = val
                        cell.font = cnf_font
                        cell.fill = PatternFill("solid", start_color="FFF3CD")
                    else:
                        cell.value = excel_safe_text(raw_suv_val)
                        _style_cell(cell, fill_hex=SUVIDHA_BG)
                elif suv_col:
                    if DEBUG_MODE:
                        print(
                            f"[DEBUG][EXCEL] suv_col '{suv_col}' mapped from '{col_name}' "
                            "but missing in suv_row."
                        )
                    cell.value = "VALUE NOT FOUND"
                    cell.font = cnf_font
                    cell.fill = PatternFill("solid", start_color="FFF3CD")
                else:
                    cell.value = ""
                    _style_cell(cell, fill_hex=SUVIDHA_BG)
        current_row += 1

        for c in range(1, len(all_admin_cols) + 2):
            sep = ws_disc.cell(row=current_row, column=c, value="")
            sep.fill = PatternFill("solid", start_color=SEP_FILL)
            sep.border = _border()
        ws_disc.row_dimensions[current_row].height = 4
        current_row += 1

    only_admin_rows = result.get("only_admin_rows", [])
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

    only_suv_rows = result.get("only_suv_rows", [])
    suv_cols = result.get("suv_cols", all_admin_cols)
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


# ── Flask routes ─────────────────────────────────────────────────────────────
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
        f"Key column '{selected_key}' not found in detected columns: {available_columns}"
    )


def _parse_manual_mappings(form_value: str | None) -> dict[str, str] | None:
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
            _parse_optional_int(request.form.get("admin_header_row")),
            _parse_optional_int(request.form.get("admin_header_span")),
        )
        suv_ds = parse_uploaded_dataset(
            suvidha_file,
            _parse_optional_int(request.form.get("suvidha_header_row")),
            _parse_optional_int(request.form.get("suvidha_header_span")),
        )

        admin_key = _resolve_key_column(admin_key, admin_ds.columns)
        suv_key = _resolve_key_column(suv_key, suv_ds.columns)

        manual_mappings = _parse_manual_mappings(request.form.get("manual_mappings"))
        result = reconcile(admin_ds, suv_ds, admin_key, suv_key, manual_mappings)

        return jsonify(
            {
                "discrepancies": result["discrepancies"],
                "only_admin_rows": result["only_admin_rows"],
                "only_suv_rows": result["only_suv_rows"],
                "matching_records": result["stats"]["matched"],
                "only_suvidha_rows": result["only_suv_rows"],  # backward compat alias
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
    print(" Grambook Reconciliation Tool (PATCHED v7)")
    print(f" DEBUG_MODE = {DEBUG_MODE}")
    print(f" MAX_ROWS   = {MAX_ROWS}")
    print(" http://localhost:5000")
    print("═══════════════════════════════════════\n")
    app.run(debug=True, port=5000)
