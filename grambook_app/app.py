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
    """Conservative key normalization for audit-safe matching."""
    return canonical_text(value)


def normalize_column_key(name: str) -> str:
    return re.sub(r"[\s_\-]+", "", canonical_text(name).lower())


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


def detect_header_start(
    matrix: list[list[str]], manual_header_row: int | None = None
) -> int:
    """
    Return the 0-based index of the header row.

    FIX (Bug 4): The original function returned a tuple[int, int | None] where
    the second element (forced_span) was *always* None, making the corresponding
    branch in build_headers permanently unreachable.  The return type has been
    simplified to just int; build_headers no longer accepts a forced_span arg.
    """
    if not matrix:
        raise ReconciliationError("File has no rows.")

    if manual_header_row is not None:
        idx = manual_header_row - 1
        if idx < 0 or idx >= len(matrix):
            raise ReconciliationError(
                f"Manual header row {manual_header_row} is out of range "
                f"(file has {len(matrix)} rows)."
            )
        return idx

    return 0


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
) -> tuple[list[str], dict[str, str], list[int], int]:
    """
    FIX (Bug 4): Removed the unused `forced_span` parameter. The caller
    (dataframe_from_matrix) no longer unpacks a second value from
    detect_header_start, so forced_span was always None and the
    `elif forced_span` branch was permanently dead code.
    """
    if not matrix:
        raise ReconciliationError("Empty matrix — cannot build headers.")

    max_cols = max(len(r) for r in matrix)
    local_rows: list[list[str]] = []
    for i in range(header_start, min(header_start + 5, len(matrix))):
        row = (matrix[i] if i < len(matrix) else []) + [""] * max_cols
        local_rows.append(_forward_fill_header_cells(row[:max_cols]))

    if not local_rows:
        raise ReconciliationError(
            "Header row is at the very end of the file — no data rows follow."
        )

    chosen_span = manual_header_span if manual_header_span in (1, 2, 3, 4, 5) else 1

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

    # FIX (Bug 4): detect_header_start now returns a plain int (not a tuple).
    header_start = detect_header_start(matrix, manual_header_row=manual_header_row)
    headers, normalized_map, drop_indices, header_span = build_headers(
        matrix,
        header_start,
        manual_header_span=manual_header_span,
    )

    kept_indices = [i for i in range(max_cols) if i not in set(drop_indices)]
    if not kept_indices:
        raise ReconciliationError("No usable columns detected after header processing.")

    data_rows = matrix[header_start + header_span :]

    # Safety fallback: if auto span consumed all rows as "header",
    # retry with a single header row.
    if (
        manual_header_span is None
        and not data_rows
        and (header_start + 1) < len(matrix)
        and header_span > 1
    ):
        headers, normalized_map, drop_indices, header_span = build_headers(
            matrix, header_start, manual_header_span=1
        )
        kept_indices = [i for i in range(max_cols) if i not in set(drop_indices)]
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

    # Pass 2 — stricter fuzzy match (>= 92 % with clear winner margin)
    for ac in admin_candidates:
        if any(x[0] == ac for x in mapped):
            continue
        best_sc, best_score = None, 0.0
        second_best = 0.0
        na = normalize_column_key(ac)
        for sc in suv_candidates:
            if sc in used_suv:
                continue
            score = _similarity(na, normalize_column_key(sc))
            if score > best_score:
                second_best = best_score
                best_score = score
                best_sc = sc
            elif score > second_best:
                second_best = score
        if best_sc and best_score >= 0.92 and (best_score - second_best) >= 0.03:
            mapped.append((ac, best_sc, best_score))
            used_suv.add(best_sc)

    # Pass 3 — safe positional fallback for remaining columns
    remaining_admin = [ac for ac in admin_candidates if all(m[0] != ac for m in mapped)]
    remaining_suv = [sc for sc in suv_candidates if sc not in used_suv]
    for ac, sc in zip(remaining_admin, remaining_suv):
        mapped.append((ac, sc, 0.5))
        used_suv.add(sc)

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
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Core reconciliation logic (mismatch-only, row-to-row, no aggregation)."""

    def _decimal_from_value(value: Any) -> Decimal | None:
        num = canonical_numeric_text(value)
        if num is None:
            return None
        try:
            return Decimal(num)
        except (InvalidOperation, ValueError):
            return None

    def _decimal_to_text(value: Decimal) -> str:
        text = format(value.normalize(), "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return "0" if not text or text == "-0" else text

    def _values_different(left: Any, right: Any) -> tuple[bool, str, str, str, bool]:
        left_txt = canonical_text(left)
        right_txt = canonical_text(right)
        left_dec = _decimal_from_value(left_txt) if left_txt else None
        right_dec = _decimal_from_value(right_txt) if right_txt else None

        # Numeric compare only when both are numeric. Blank is never equal to zero.
        if left_dec is not None and right_dec is not None:
            if left_dec == right_dec:
                return (
                    False,
                    _decimal_to_text(left_dec),
                    _decimal_to_text(right_dec),
                    "",
                    True,
                )
            return (
                True,
                _decimal_to_text(left_dec),
                _decimal_to_text(right_dec),
                _decimal_to_text(left_dec - right_dec),
                True,
            )

        if left_txt == right_txt:
            return False, left_txt, right_txt, "", False
        return True, left_txt, right_txt, "", False

    admin_idx, admin_missing_keys = _build_key_index(
        admin.rows, admin_key, admin.row_position_map
    )
    suv_idx, suv_missing_keys = _build_key_index(
        suv.rows, suv_key, suv.row_position_map
    )

    col_pairs = map_columns_smart(admin.columns, suv.columns, admin_key, suv_key)
    pair_lookup = {a: s for a, s, _ in col_pairs}

    priority_tokens = ("tax", "amount", "baki", "balance")
    focused_pairs = [
        (a, s, c)
        for a, s, c in col_pairs
        if any(
            t in normalize_column_key(a) or t in normalize_column_key(s)
            for t in priority_tokens
        )
    ]
    if focused_pairs:
        focused_admin_cols = {a for a, _, _ in focused_pairs}
        compare_pairs = focused_pairs + [
            pair for pair in col_pairs if pair[0] not in focused_admin_cols
        ]
    else:
        compare_pairs = col_pairs

    admin_keys = set(admin_idx)
    suv_keys = set(suv_idx)
    only_admin_rows: list[dict[str, str]] = []
    only_suv_rows: list[dict[str, str]] = []
    discrepancies: list[dict[str, Any]] = []
    mismatch_details: list[dict[str, Any]] = []
    matching_records = 0
    duplicate_key_conflicts: list[dict[str, Any]] = []
    strict_mismatch_count = 0
    hidden_mismatches = 0
    hidden_mismatch_samples: list[dict[str, Any]] = []

    matched_key_pairs: list[tuple[str, str, float, str]] = []
    used_suv_keys: set[str] = set()

    # Exact key matches first (fast path).
    for a_key in admin_idx.keys():
        if a_key in suv_idx:
            matched_key_pairs.append((a_key, a_key, 1.0, "exact"))
            used_suv_keys.add(a_key)

    # Fuzzy key fallback for remaining keys (>= 90%).
    remaining_suv_keys = [k for k in suv_idx.keys() if k not in used_suv_keys]
    for a_key in admin_idx.keys():
        if a_key in suv_idx:
            continue
        best_key = ""
        best_score = 0.0
        second_best = 0.0
        for s_key in remaining_suv_keys:
            score = _similarity(a_key, s_key)
            if score > best_score:
                second_best = best_score
                best_score = score
                best_key = s_key
            elif score > second_best:
                second_best = score
        if best_key and best_score >= 0.90 and (best_score - second_best) >= 0.03:
            matched_key_pairs.append((a_key, best_key, best_score, "fuzzy"))
            used_suv_keys.add(best_key)
            remaining_suv_keys = [k for k in remaining_suv_keys if k != best_key]

    def _record_mismatch(
        a_item: dict[str, Any],
        s_item: dict[str, Any],
        normalized_key: str,
        key_confidence: float,
        key_match_type: str,
    ) -> bool:
        """
        Compare a matched pair of rows and record any column-level mismatches.

        FIX (Bug 2): This function now ONLY accepts two non-None row items.
        The original code also called it with one side as None (for unmatched /
        extra rows), which caused every admin column to compare against an empty
        string and be counted as a discrepancy.  Unmatched rows are already
        captured in only_admin_rows / only_suv_rows; they must NOT flow through
        this function.
        """
        nonlocal strict_mismatch_count, hidden_mismatches
        admin_row = a_item["row"]
        suv_row = s_item["row"]
        mismatch_map: dict[str, Any] = {}
        id_display = (
            a_item.get("display_key") or s_item.get("display_key") or normalized_key
        )

        for ac, sc, col_conf in compare_pairs:
            left_val = admin_row.get(ac, "")
            right_val = suv_row.get(sc, "")
            left_raw = canonical_text(left_val)
            right_raw = canonical_text(right_val)
            strict_changed = left_raw != right_raw
            if strict_changed:
                strict_mismatch_count += 1
            changed, left_fmt, right_fmt, diff_text, is_numeric = _values_different(
                left_val, right_val
            )
            if strict_changed and not changed:
                hidden_mismatches += 1
                if len(hidden_mismatch_samples) < 25:
                    hidden_mismatch_samples.append(
                        {
                            "id": id_display,
                            "column": ac,
                            "suvidha_column": sc,
                            "admin_raw": left_raw,
                            "suvidha_raw": right_raw,
                            "reason": "raw_unequal_but_treated_equal",
                        }
                    )
            if not changed:
                continue
            mismatch_map[ac] = {
                "admin": left_fmt,
                "suvidha": right_fmt,
                "difference": diff_text,
                "is_numeric": is_numeric,
                "suv_col": sc,
                "column_confidence": round(col_conf, 4),
            }

        if not mismatch_map:
            return False

        admin_excel_rows = (
            [a_item["excel_row"]] if a_item.get("excel_row", -1) > 0 else []
        )
        suv_excel_rows = (
            [s_item["excel_row"]] if s_item.get("excel_row", -1) > 0 else []
        )
        discrepancies.append(
            {
                "id": id_display,
                "normalized_id": normalized_key,
                "admin_excel_rows": admin_excel_rows,
                "suvidha_excel_rows": suv_excel_rows,
                "admin_count": 1,
                "suvidha_count": 1,
                "changed_columns": list(mismatch_map.keys()),
                "mismatch_count": len(mismatch_map),
                "key_match_type": key_match_type,
                "key_confidence": round(key_confidence, 4),
                "mismatches": mismatch_map,
            }
        )
        for admin_col, diff in mismatch_map.items():
            mismatch_details.append(
                {
                    "id": id_display,
                    "normalized_id": normalized_key,
                    "column": admin_col,
                    "suvidha_column": diff.get("suv_col", admin_col),
                    "admin_value": diff.get("admin", ""),
                    "suvidha_value": diff.get("suvidha", ""),
                    "difference": diff.get("difference", ""),
                    "is_numeric": bool(diff.get("is_numeric")),
                    "admin_excel_rows": admin_excel_rows,
                    "suvidha_excel_rows": suv_excel_rows,
                    "key_match_type": key_match_type,
                    "key_confidence": round(key_confidence, 4),
                }
            )
        return True

    processed_admin_keys: set[str] = set()
    processed_suv_keys: set[str] = set()

    for a_key, s_key, key_conf, match_type in matched_key_pairs:
        processed_admin_keys.add(a_key)
        processed_suv_keys.add(s_key)
        a_rows = admin_idx.get(a_key, [])
        s_rows = suv_idx.get(s_key, [])

        if len(a_rows) != 1 or len(s_rows) != 1:
            duplicate_key_conflicts.append(
                {
                    "admin_key": a_key,
                    "suvidha_key": s_key,
                    "admin_count": len(a_rows),
                    "suvidha_count": len(s_rows),
                    "match_type": match_type,
                    "key_confidence": round(key_conf, 4),
                }
            )

        pair_count = min(len(a_rows), len(s_rows))
        for i in range(pair_count):
            changed = _record_mismatch(
                a_rows[i], s_rows[i], a_key, key_conf, match_type
            )
            if not changed:
                matching_records += 1

        # FIX (Bug 2): Extra rows from duplicate keys are tracked in
        # only_admin_rows / only_suv_rows only.  The original code also
        # called _record_mismatch(extra, None, ...) here, causing every
        # non-empty admin field to be flagged as a mismatch against "".
        for extra in a_rows[pair_count:]:
            only_admin_rows.append(extra["row"])
        for extra in s_rows[pair_count:]:
            only_suv_rows.append(extra["row"])

    # FIX (Bug 2): Unmatched rows go to only_admin_rows / only_suv_rows.
    # The original code additionally called _record_mismatch(item, None, ...)
    # which inflated discrepancy counts and polluted the Excel report.
    for a_key, a_rows in admin_idx.items():
        if a_key in processed_admin_keys:
            continue
        for item in a_rows:
            only_admin_rows.append(item["row"])

    for s_key, s_rows in suv_idx.items():
        if s_key in processed_suv_keys:
            continue
        for item in s_rows:
            only_suv_rows.append(item["row"])

    debug_info = {
        "total_keys": len(admin_keys | suv_keys),
        "matched_keys": len(matched_key_pairs),
        "discrepancy_count": len(discrepancies),
        "strict_mismatch_count": strict_mismatch_count,
        "hidden_mismatches": hidden_mismatches,
        "hidden_mismatch_samples": hidden_mismatch_samples,
    }
    logger.info(
        "Reconcile stats: total_keys=%d matched_keys=%d discrepancy_count=%d "
        "strict_mismatch_count=%d hidden_mismatches=%d",
        debug_info["total_keys"],
        debug_info["matched_keys"],
        debug_info["discrepancy_count"],
        debug_info["strict_mismatch_count"],
        debug_info["hidden_mismatches"],
    )
    if hidden_mismatch_samples:
        logger.info("Hidden mismatch samples: %s", hidden_mismatch_samples[:5])

    out = {
        "discrepancies": discrepancies,
        "mismatch_details": mismatch_details,
        "only_admin_rows": only_admin_rows,
        "only_suv_rows": only_suv_rows,
        "col_pairs": [
            {"admin_col": a, "suv_col": s, "confidence": round(c, 4)}
            for a, s, c in compare_pairs
        ],
        "admin_key": admin_key,
        "suv_key": suv_key,
        "admin_cols": [a for a, _, _ in compare_pairs],
        "suv_cols": [s for _, s, _ in compare_pairs],
        "admin_column_position_map": admin.column_position_map,
        "meta": {
            "compared_keys": len(matched_key_pairs),
            "duplicate_key_conflicts": duplicate_key_conflicts,
            "admin_missing_keys": admin_missing_keys,
            "suvidha_missing_keys": suv_missing_keys,
            "unmapped_admin_cols": [
                c for c in admin.columns if c != admin_key and c not in pair_lookup
            ],
            "key_pairs_compared": len(matched_key_pairs),
        },
        "stats": {
            "total": len(admin_keys | suv_keys),
            "matched": matching_records,
            "disc": len(discrepancies),
            "only_a": len(only_admin_rows),
            "only_s": len(only_suv_rows),
        },
    }
    if debug_mode:
        out["debug"] = debug_info
    return out


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


def generate_discrepancy_report(result: dict) -> io.BytesIO:
    """
    Generate mismatch-only Excel report.
    Output sheet: "Mismatched Records"

    FIX (Bug 1): Removed the unused `admin: ParsedDataset` parameter.
    The original signature was generate_discrepancy_report(admin, result) but
    `admin` was never referenced inside the function body.  All callers have
    been updated to pass only `result`.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Mismatched Records"

    C_HDR_BG = "1A1814"
    C_HDR_FG = "FFFFFF"
    C_ADMIN_DIFF = "F5C4B3"
    C_SUV_DIFF = "B8E3D0"
    C_NEUTRAL = "F8F7F4"
    C_ADMIN_FG = "B5451B"
    C_SUV_FG = "1A6B4A"

    col_pairs = result.get("col_pairs", [])
    compare_cols = [p.get("admin_col", "") for p in col_pairs if p.get("admin_col")]
    headers = ["Source", "ID"] + compare_cols
    for ci, hdr in enumerate(headers, start=1):
        c = ws.cell(row=1, column=ci, value=excel_safe_text(hdr))
        _style_cell(c, fill_hex=C_HDR_BG, bold=True, color=C_HDR_FG)

    discrepancies = result.get("discrepancies", [])
    row_no = 2
    for disc in discrepancies:
        mismatch_map = disc.get("mismatches", {})
        if not mismatch_map:
            continue

        admin_row_no = row_no
        suv_row_no = row_no + 1
        ws.cell(row=admin_row_no, column=1, value="Admin")
        ws.cell(row=admin_row_no, column=2, value=excel_safe_text(disc.get("id", "")))
        ws.cell(row=suv_row_no, column=1, value="Suvidha")
        ws.cell(row=suv_row_no, column=2, value=excel_safe_text(disc.get("id", "")))
        _style_cell(
            ws.cell(row=admin_row_no, column=1),
            fill_hex=C_ADMIN_DIFF,
            color=C_ADMIN_FG,
            bold=True,
        )
        _style_cell(
            ws.cell(row=admin_row_no, column=2),
            fill_hex=C_ADMIN_DIFF,
            color=C_ADMIN_FG,
            bold=True,
        )
        _style_cell(
            ws.cell(row=suv_row_no, column=1),
            fill_hex=C_SUV_DIFF,
            color=C_SUV_FG,
            bold=True,
        )
        _style_cell(
            ws.cell(row=suv_row_no, column=2),
            fill_hex=C_SUV_DIFF,
            color=C_SUV_FG,
            bold=True,
        )

        for ci, col in enumerate(compare_cols, start=3):
            diff = mismatch_map.get(col)
            admin_cell = ws.cell(row=admin_row_no, column=ci, value="")
            suv_cell = ws.cell(row=suv_row_no, column=ci, value="")
            if diff:
                admin_cell.value = excel_safe_text(diff.get("admin", ""))
                suv_cell.value = excel_safe_text(diff.get("suvidha", ""))
                _style_cell(
                    admin_cell, fill_hex=C_ADMIN_DIFF, color=C_ADMIN_FG, bold=True
                )
                _style_cell(suv_cell, fill_hex=C_SUV_DIFF, color=C_SUV_FG, bold=True)
            else:
                _style_cell(admin_cell, fill_hex=C_NEUTRAL)
                _style_cell(suv_cell, fill_hex=C_NEUTRAL)

        row_no += 2

    if row_no == 2:
        ws.cell(row=2, column=1, value="No mismatched records found.")
        _style_cell(ws.cell(row=2, column=1), fill_hex=C_NEUTRAL)

    for col_cells in ws.columns:
        length = max((len(str(c.value or "")) for c in col_cells), default=8)
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(
            length + 4, 42
        )

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
        debug_mode = str(request.form.get("debug", "")).strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        admin = parse_uploaded_dataset(admin_file, a_hr, a_hs)
        suv = parse_uploaded_dataset(suv_file, s_hr, s_hs)

        admin_key = _resolve_key_column(admin_key_raw, admin.columns)
        suv_key = _resolve_key_column(suv_key_raw, suv.columns)

        result = reconcile(admin, suv, admin_key, suv_key, debug_mode=debug_mode)
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
        # FIX (Bug 1): generate_discrepancy_report no longer takes `admin`.
        buf = generate_discrepancy_report(result)

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
