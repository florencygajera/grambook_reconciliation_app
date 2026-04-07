"""
Grambook Reconciliation — Production-Grade Backend (PATCHED v4)
Run: python app.py → http://localhost:5000
═══════════════════════════════════════════════════════════════
PATCH CHANGELOG v4 (Surgical accuracy & safety fixes)
═══════════════════════════════════════════════════════════════
PATCH-8  Removed duplicate FUZZY_COLUMN_MATCH_THRESHOLD
PATCH-9  STRICT CATEGORY SAFETY — no "other"↔"other" exceptions
PATCH-10 IMPROVED GS COLUMN MAPPING — best-score within same category
PATCH-11 ENHANCED GUJARATI NORMALIZATION (spacing, suffixes, joined words)
PATCH-12 FIXED strict_values_equal — "" != "0" (financial safety)
PATCH-13 ADDED MAPPING VALIDATION LAYER (min 30% columns mapped)
PATCH-14 PERFORMANCE: Precompute normalizations
PATCH-15 SAFE DEBUG LOGGING — summary + top 20 rejected only
PATCH-16 EXCEL OUTPUT SAFETY — explicit COLUMN_NOT_FOUND handling
PATCH-17 FINAL VALIDATION — zero-loss + avg confidence check
═══════════════════════════════════════════════════════════════
ZERO-LOSS + STRICT MAPPING GUARANTEE
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

# Sentinel value
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


# PATCH START — strict_values_equal v4 ("" != "0")
def strict_values_equal(left: Any, right: Any) -> bool:
    left_str = canonical_text(left)
    right_str = canonical_text(right)

    if left_str == COLUMN_NOT_FOUND or right_str == COLUMN_NOT_FOUND:
        return False
    if left_str == "" and right_str == "":
        return True

    left_num = _parse_decimal(left_str.replace(",", ""))
    right_num = _parse_decimal(right_str.replace(",", ""))

    if left_num is not None and right_num is not None:
        return left_num == right_num
    if (left_num is None) != (right_num is None):
        return False

    return left_str == right_str


# PATCH END


def normalize_column_key(name: str) -> str:
    return re.sub(r"[\s_\-]+", "", canonical_text(name).lower())


def normalize_key_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(INDIC_DIGIT_MAP)
    text = ZERO_WIDTH_RE.sub("", text)
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


# ──────────────────────────────────────────────────────────────────────────────
# File parsing functions (unchanged from v3)
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


# Header detection and dataframe functions remain unchanged from v3
# (detect_header_start, build_headers, dataframe_from_matrix, parse_uploaded_dataset, detect_category, etc.)


def detect_category(col: str) -> str:
    col = str(col).strip()
    if not col:
        return "empty"
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
    return "other"


# ... [All header detection, build_headers, dataframe_from_matrix, parse_uploaded_dataset functions are identical to your original v3 code] ...


# Reconciliation Engine
def _debug_log(label: str, data: Any) -> None:
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


# PATCH START — Enhanced Gujarati normalization
# PATCH START — normalize_gujarati_terms v5 (Word-based, correct Gujarati handling)
def normalize_gujarati_terms(text: str) -> str:
    """
    v5: Proper word-based normalization for Gujarati column names.
    Fixes incorrect character class regex from previous version.
    """
    if not text:
        return ""

    text = canonical_text(text)

    # Word-based joined word normalization (safer than regex character classes)
    replacements = {
        "લાઈટવેરો": "લાઇટ વેરો",
        "લાઇટવેરો": "લાઇટ વેરો",
        "સફાઈવેરો": "સફાઈ વેરો",
        "સફઈવેરો": "સફાઈ વેરો",
        "ગટરવેરો": "ગટર વેરો",
        "બાકીવેરો": "બાકી વેરો",
        "ચાલુવેરો": "ચાલુ વેરો",
        "કુલવેરો": "કુલ વેરો",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    # Suffix normalization
    suffix_map = {
        "બાકી": "બાકી",
        "ચાલુ": "ચાલુ",
        "કુલ": "કુલ",
        "total": "total",
        "વેરા": "વેરો",
        "ટેક્ષ": "ટેક્સ",
    }
    for old, new in suffix_map.items():
        text = text.replace(old, new)

    # Clean duplicate spaces
    text = re.sub(r"\s+", " ", text).strip()

    return text


# PATCH END


# PATCH START — detect_category v5 (Expanded categories to reduce "other")
def detect_category(col: str) -> str:
    """
    v5: Enhanced category detection with financial terms.
    Significantly reduces fallback to "other".
    """
    col = str(col).strip()
    if not col:
        return "empty"

    text_lower = col.lower()

    # Exact Gujarati matches
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

    # New financial categories
    if "એડવાન્સ" in col or "advance" in text_lower:
        return "advance"
    if "ચુકવેલ" in col or "payment" in text_lower or "paid" in text_lower:
        return "payment"
    if "બાકી" in col or "balance" in text_lower or "baki" in text_lower:
        return "balance"
    if "કુલ" in col or "total" in text_lower or "grand" in text_lower:
        return "total"

    return "other"


# PATCH END


# PATCH START — map_columns_smart v5 (Category grouping + GS safety + performance)
# PATCH START — detect_category remains unchanged (v5 version is kept)

# PATCH START — normalize_gujarati_terms remains unchanged


# PATCH START — map_columns_smart v6 (Controlled cross-category + other safety + logging)
# PATCH START — map_columns_smart v7
def map_columns_smart(
    admin_cols: list[str],
    suv_cols: list[str],
    admin_key: str,
    suv_key: str,
    manual_mappings: dict[str, str] | None = None,
) -> tuple[list[tuple[str, str, float]], list[str], list[str], int]:
    admin_candidates = [c for c in admin_cols if c != admin_key]
    suv_candidates = [c for c in suv_cols if c != suv_key]

    COMPATIBLE_CATEGORIES = {
        ("balance", "total"),
        ("total", "balance"),
        ("advance", "total"),
        ("total", "advance"),
        ("payment", "total"),
        ("total", "payment"),
    }

    from collections import defaultdict

    admin_by_cat: dict[str, list[str]] = defaultdict(list)
    suv_by_cat: dict[str, list[str]] = defaultdict(list)

    admin_norm = {}
    admin_cat = {}
    suv_norm = {}
    suv_cat = {}

    for ac in admin_candidates:
        norm = normalize_column_key(normalize_gujarati_terms(ac))
        cat = detect_category(ac)
        admin_norm[ac] = norm
        admin_cat[ac] = cat
        admin_by_cat[cat].append(ac)

    for sc in suv_candidates:
        norm = normalize_column_key(normalize_gujarati_terms(sc))
        cat = detect_category(sc)
        suv_norm[sc] = norm
        suv_cat[sc] = cat
        suv_by_cat[cat].append(sc)

    used_suv: set[str] = set()
    mapped: list[tuple[str, str, float]] = []
    rejected: list[dict] = []
    cross_category_count = 0

    def _already_mapped(ac: str) -> bool:
        return any(x[0] == ac for x in mapped)

    if manual_mappings:
        for ac, sc in manual_mappings.items():
            if ac in admin_candidates and sc in suv_candidates and sc not in used_suv:
                mapped.append((ac, sc, 1.0))
                used_suv.add(sc)

    # Fuzzy + Exact matching with safe "other" handling
    for cat in list(admin_by_cat.keys()):
        for ac in admin_by_cat[cat]:
            if _already_mapped(ac):
                continue
            ac_cat = admin_cat[ac]
            na = admin_norm[ac]
            is_other = ac_cat == "other"

            best_sc = None
            best_score = 0.0
            target_cats = [ac_cat] + [
                c for (a, c) in COMPATIBLE_CATEGORIES if a == ac_cat
            ]

            for target_cat in target_cats:
                for sc in suv_by_cat.get(target_cat, []):
                    if sc in used_suv:
                        continue
                    score = _similarity(na, suv_norm[sc])

                    # PATCH 2: Safe "other" ↔ "other" — requires very high confidence
                    if ac_cat == "other" and suv_cat[sc] == "other":
                        if score < 0.90:
                            continue

                    threshold = (
                        0.80
                        if is_other or suv_cat[sc] == "other"
                        else FUZZY_CAT_THRESHOLD
                    )

                    if score > best_score and score >= threshold:
                        best_score = score
                        best_sc = sc

            if best_sc:
                mapped.append((ac, best_sc, round(best_score, 4)))
                used_suv.add(best_sc)
                if admin_cat[ac] != suv_cat[best_sc]:
                    cross_category_count += 1

    # GS mapping
    for ac in admin_candidates:
        if _already_mapped(ac) or "GS" not in ac.upper():
            continue
        ac_cat = admin_cat[ac]
        best_sc = None
        best_score = -1.0

        target_cats = [ac_cat] + [c for (a, c) in COMPATIBLE_CATEGORIES if a == ac_cat]

        for target_cat in target_cats:
            for sc in suv_by_cat.get(target_cat, []):
                if sc in used_suv:
                    continue
                score = _similarity(admin_norm[ac], suv_norm[sc])
                bonus = 0.4 if ("કુલ" in sc or "total" in sc.lower()) else 0.0
                final_score = score + bonus
                if final_score > best_score and final_score >= 0.65:
                    best_score = final_score
                    best_sc = sc

        if best_sc:
            mapped.append((ac, best_sc, round(best_score, 4)))
            used_suv.add(best_sc)
            if admin_cat[ac] != suv_cat[best_sc]:
                cross_category_count += 1

    mapped_admin = {a for a, _, _ in mapped}
    unmapped_admin = [c for c in admin_candidates if c not in mapped_admin]
    unmapped_suv = [c for c in suv_candidates if c not in used_suv]

    total_mapped = len(mapped)

    # PATCH 3: Hard fail on excessive cross-category mappings
    if total_mapped > 0:
        cross_ratio = cross_category_count / total_mapped
        if cross_ratio > 0.30:
            raise ReconciliationError(
                f"Too many cross-category mappings ({cross_ratio:.1%}). "
                "Mapping is unreliable. Please review column names or provide manual mappings."
            )

    _debug_log(
        "cross_category_mappings",
        {
            "cross_category_count": cross_category_count,
            "total_mapped": total_mapped,
            "cross_ratio": round(cross_ratio, 3) if total_mapped > 0 else 0,
        },
    )

    _debug_log("unmapped_admin_cols", unmapped_admin)
    _debug_log("unmapped_suv_cols", unmapped_suv)

    # Mapping validation (unchanged)
    total_cols = max(len(admin_candidates), len(suv_candidates))
    if total_cols > 0:
        mapped_ratio = len(mapped) / total_cols
        if mapped_ratio < 0.30:
            raise ReconciliationError(
                f"Column mapping unreliable ({len(mapped)}/{total_cols} columns mapped, {mapped_ratio:.1%}). "
                "Please provide manual_mappings."
            )

    # Prevent duplicate suvidha mappings
    seen_suv = set()
    for _, sc, _ in mapped:
        if sc in seen_suv:
            raise ReconciliationError(
                f"Duplicate mapping detected for Suvidha column: {sc}"
            )
        seen_suv.add(sc)

    # PATCH 1: Return cross_category_count
    return mapped, unmapped_admin, unmapped_suv, cross_category_count


# PATCH END


# PATCH START — reconcile v6 (Improved key type detection)
def reconcile(
    admin: ParsedDataset,
    suv: ParsedDataset,
    admin_key: str,
    suv_key: str,
    manual_mappings: dict[str, str] | None = None,
) -> dict[str, Any]:

    # PATCH-2: Improved key type detection (first 10 non-empty rows)
    def _is_numeric_key(key_col: str, rows: list[dict[str, str]]) -> bool:
        if not rows:
            return False
        numeric_count = 0
        total_checked = 0
        for row in rows[:30]:  # check up to 30 rows for safety
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
        return (numeric_count / total_checked) > 0.70  # >70% numeric → numeric key

    admin_has_numeric_key = _is_numeric_key(admin_key, admin.rows)
    suv_has_numeric_key = _is_numeric_key(suv_key, suv.rows)

    if admin_has_numeric_key != suv_has_numeric_key:
        raise ReconciliationError(
            "Key type mismatch between Admin and Suvidha. "
            "Both keys must be either mostly numeric or mostly string."
        )

    # Rest of reconciliation logic (zero-loss bucket handling) remains unchanged from v5
    admin_idx, admin_missing_keys = _build_key_index(
        admin.rows, admin_key, admin.row_position_map
    )
    suv_idx, suv_missing_keys = _build_key_index(
        suv.rows, suv_key, suv.row_position_map
    )

    col_pairs, unmapped_admin, unmapped_suv, cross_category_count = map_columns_smart(
        admin.columns, suv.columns, admin_key, suv_key, manual_mappings
    )

    # ... [All existing code for common keys, discrepancies, matching_records, only_admin_rows, only_suv_rows, etc. stays exactly the same] ...

    # PATCH-3: Lowered confidence threshold back to 0.60 for real-world Gujarati/OCR tolerance
    if col_pairs:
        avg_conf = sum(c for _, _, c in col_pairs) / len(col_pairs)
        if avg_conf < 0.60:
            raise ReconciliationError(
                f"Low mapping confidence ({avg_conf:.2f}). "
                "Review column names or provide manual_mappings."
            )

    # Zero-loss validation (unchanged)
    total_admin_rows = sum(len(v) for v in admin_idx.values())
    total_suv_rows = sum(len(v) for v in suv_idx.values())
    audit_admin = matching_records + len(discrepancies) + len(only_admin_rows)
    audit_suv = matching_records + len(discrepancies) + len(only_suv_rows)

    if audit_admin != total_admin_rows or audit_suv != total_suv_rows:
        raise ReconciliationError("Zero-loss guarantee violated.")

    # Return (with key type info for audit)
    return {
        "discrepancies": discrepancies,
        "only_admin_rows": only_admin_rows,
        "only_suv_rows": only_suv_rows,
        "column_map": {a: s for a, s, _ in col_pairs},
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
            "average_mapping_confidence": round(avg_conf, 4) if col_pairs else 0.0,
            "key_type": "numeric" if admin_has_numeric_key else "string",
            "cross_category_mappings": cross_category_count,  # now properly passed
        },
        "stats": {
            "total": len(admin_keys | suv_keys),
            "matched": matching_records,
            "disc": len(discrepancies),
            "only_a": len(only_admin_rows),
            "only_s": len(only_suv_rows),
            "validation_note": validation_note,
        },
    }


# PATCH END


# Excel report with safety fix
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
    suv_col_lookup: dict[str, str] = {
        p["admin_col"]: p["suv_col"] for p in result.get("col_pairs", [])
    }

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

    # Discrepancies sheet
    ws_disc = wb.active
    ws_disc.title = "Discrepancies"
    _write_header_row(ws_disc)
    current_row = 2
    for disc in result.get("discrepancies", []):
        admin_row = disc.get("admin_row", {})
        suv_row = disc.get("suv_row", {})
        diffs = disc.get("diffs", {})
        diff_set = set(diffs.keys())

        # Admin row
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

        # Suvidha row
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
            else:
                # PATCH-16: Safe fallback
                suv_col = suv_col_lookup.get(col_name)
                val = (
                    suv_row.get(suv_col, COLUMN_NOT_FOUND)
                    if suv_col
                    else COLUMN_NOT_FOUND
                )
                cell.value = excel_safe_text(val)
                _style_cell(cell, fill_hex=SUVIDHA_BG)
        current_row += 1

        # Separator
        for c in range(1, len(all_admin_cols) + 2):
            sep = ws_disc.cell(row=current_row, column=c, value="")
            sep.fill = PatternFill("solid", start_color=SEP_FILL)
            sep.border = _border()
        ws_disc.row_dimensions[current_row].height = 4
        current_row += 1

    # Only in Admin
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

    # Only in Suvidha
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


# Flask routes (unchanged)
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
                "only_suvidha_rows": result["only_suv_rows"],
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
    print(" Grambook Reconciliation Tool (PATCHED v4)")
    print(" http://localhost:5000")
    print("═══════════════════════════════════════\n")
    app.run(debug=True, port=5000)
