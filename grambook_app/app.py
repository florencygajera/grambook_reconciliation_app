from __future__ import annotations

import csv
import io
import math
import hashlib
import json
import logging
import os
import re
import shutil
import secrets
import tempfile
import time
import warnings
import unicodedata
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Any

from flask import Flask, after_this_request, jsonify, request, send_file, send_from_directory, session
from openpyxl.cell import WriteOnlyCell
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

try:
    import xlrd
except Exception:  # pragma: no cover
    xlrd = None

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="/static")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024
_secret_key = os.environ.get("GRAMBOOK_SECRET_KEY")
if not _secret_key:
    if os.environ.get("FLASK_ENV") == "production":
        raise RuntimeError(
            "GRAMBOOK_SECRET_KEY environment variable must be set in production."
        )
    _secret_key = "grambook-development-secret-key"
    warnings.warn(
        "GRAMBOOK_SECRET_KEY is not set. Set it via environment variable before deploying.",
        stacklevel=1,
    )
app.config["SECRET_KEY"] = _secret_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("grambook")

RESULT_CACHE: dict[str, dict[str, Any]] = {}
RESULT_CACHE_ORDER: list[str] = []
RESULT_CACHE_LIMIT = 16
RESULT_CACHE_TTL_SECONDS = int(os.environ.get("GRAMBOOK_RESULT_CACHE_TTL_SECONDS", "3600"))
RESULT_CACHE_LOCK = threading.RLock()
RESULT_CACHE_DIR = BASE_DIR / ".grambook_cache"
RESULT_CACHE_DIR.mkdir(exist_ok=True)
CACHE_SESSION_KEY = "grambook_session_id"
CSRF_SESSION_KEY = "grambook_csrf_token"
CSRF_RATE_LIMIT_KEY = "grambook_csrf_rate_limit"
CSRF_RATE_LIMIT_WINDOW_SECONDS = 60
CSRF_RATE_LIMIT_MAX_REQUESTS = 20
LAST_DISK_PRUNE_AT = 0.0
MAX_COLUMN_INDEX = 1000
MAX_ID_LENGTH = 256

ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF]")
CONTROL_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
SPACE_RE = re.compile(r"\s+")
DIGITS_ONLY_RE = re.compile(r"^\d+$")

DIGIT_TRANSLATION = str.maketrans(
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


class ReconciliationError(Exception):
    pass


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        if value.is_integer():
            return str(int(value))
        text = f"{value:.15f}".rstrip("0").rstrip(".")
        return text or "0"
    text = str(value)
    text = CONTROL_RE.sub("", text)
    text = ZERO_WIDTH_RE.sub("", text)
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(DIGIT_TRANSLATION)
    return text.strip()


def _normalize_gujarati_terms(text: str) -> str:
    # Normalize common Gujarati variants before comparison.
    out = text
    out = out.replace("\u00a0", " ")
    out = out.replace("_", " ")
    out = out.replace("ـ", " ")
    out = re.sub(r"ઘર\s*વેરો", "ઘરવેરા", out)
    out = re.sub(r"વેરો\b", "વેરા", out)
    out = re.sub(r"વેરા\s*gs\b", "વેરા gs", out, flags=re.IGNORECASE)
    out = re.sub(r"\b(\w+)\s*[_-](\d+)\b", r"\1 \2", out)
    out = re.sub(r"\s*\(\s*(\d+)\s*\)\s*$", r" \1", out)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _normalize_for_compare(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    text = _normalize_gujarati_terms(text)
    return SPACE_RE.sub(" ", text).casefold().strip()


def _file_bytes(upload) -> tuple[bytes, str]:
    if upload is None:
        raise ReconciliationError("Both files are required.")
    try:
        upload.stream.seek(0)
    except Exception as _seek_err:
        logger.debug(
            "Stream seek failed for '%s': %s — reading from current position",
            upload.filename or "file",
            _seek_err,
        )
    data = upload.read()
    if not data:
        raise ReconciliationError(
            f"Uploaded file '{upload.filename or 'file'}' is empty."
        )
    return data, (upload.filename or "").lower()


def _validate_upload_bytes(file_bytes: bytes, filename: str) -> None:
    if filename.endswith(".xlsx"):
        if not file_bytes.startswith(b"PK\x03\x04"):
            raise ReconciliationError(
                "The .xlsx file is malformed or does not look like a workbook."
            )
        return
    if filename.endswith(".xls"):
        if not file_bytes.startswith(b"\xD0\xCF\x11\xE0"):
            raise ReconciliationError(
                "The .xls file is malformed or does not look like a workbook."
            )
        return
    if filename.endswith(".csv"):
        _decode_csv_bytes(file_bytes)
        return
    raise ReconciliationError("Unsupported file format. Upload .csv, .xls, or .xlsx")


def _decode_csv_bytes(file_bytes: bytes) -> io.StringIO:
    for encoding in ("utf-8-sig", "utf-8", "utf-16", "cp1252", "latin1"):
        try:
            return io.StringIO(file_bytes.decode(encoding))
        except UnicodeDecodeError:
            continue
    raise ReconciliationError("CSV encoding is unsupported or the file is corrupt.")


def _normalize_matrix_width(matrix: list[list[str]]) -> list[list[str]]:
    if not matrix:
        return matrix
    width = max((len(row) for row in matrix), default=0)
    out: list[list[str]] = []
    for row in matrix:
        row = list(row)
        if len(row) < width:
            row.extend([""] * (width - len(row)))
        else:
            row = row[:width]
        out.append(row)
    return out


def _prune_blank_rows(matrix: list[list[str]]) -> tuple[list[list[str]], list[int]]:
    kept: list[list[str]] = []
    row_numbers: list[int] = []
    for idx, row in enumerate(matrix, start=1):
        if any(_normalize_for_compare(cell) for cell in row):
            kept.append(row)
            row_numbers.append(idx)
    return kept, row_numbers


def _parse_csv_matrix(file_bytes: bytes) -> tuple[list[list[str]], list[int]]:
    sio = _decode_csv_bytes(file_bytes)
    sample = sio.read(4096)
    sio.seek(0)
    dialect = csv.excel
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        pass
    matrix = [[_clean_text(cell) for cell in row] for row in csv.reader(sio, dialect)]
    matrix = _normalize_matrix_width(matrix)
    return _prune_blank_rows(matrix)


def _parse_xls_matrix(file_bytes: bytes) -> tuple[list[list[str]], list[int]]:
    if xlrd is None:
        raise ReconciliationError(".xls support requires xlrd==2.0.1")
    try:
        wb = xlrd.open_workbook(file_contents=file_bytes)
        sh = wb.sheet_by_index(0)
    except Exception as exc:
        raise ReconciliationError(f"Unable to read .xls workbook: {exc}") from exc
    matrix = [
        [_clean_text(sh.cell_value(r, c)) for c in range(sh.ncols)]
        for r in range(sh.nrows)
    ]
    matrix = _normalize_matrix_width(matrix)
    return _prune_blank_rows(matrix)


def _parse_xlsx_matrix(file_bytes: bytes) -> tuple[list[list[str]], list[int]]:
    try:
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=False)
        ws = wb.worksheets[0]
    except Exception as exc:
        raise ReconciliationError(f"Unable to read .xlsx workbook: {exc}") from exc
    merged_map: dict[str, str] = {}
    merged_cells = getattr(ws, "merged_cells", None)
    if merged_cells is not None:
        for merged_range in merged_cells.ranges:
            min_col, min_row, max_col, max_row = merged_range.bounds
            top_left = ws.cell(min_row, min_col).value
            cleaned = _clean_text(top_left)
            for row in range(min_row, max_row + 1):
                for col in range(min_col, max_col + 1):
                    merged_map[f"{row}:{col}"] = cleaned
    matrix: list[list[str]] = []
    row_numbers: list[int] = []
    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        display_row: list[str] = []
        for col_idx, cell in enumerate(row, start=1):
            if cell is None:
                cell = merged_map.get(f"{row_idx}:{col_idx}", "")
            display_row.append(_clean_text(cell))
        if any(_normalize_for_compare(cell) for cell in display_row):
            matrix.append(display_row)
            row_numbers.append(row_idx)
    matrix = _normalize_matrix_width(matrix)
    wb.close()
    return matrix, row_numbers


def _parse_matrix_from_bytes(
    file_bytes: bytes, filename: str
) -> tuple[list[list[str]], list[int], str]:
    if filename.endswith(".csv"):
        matrix, rows = _parse_csv_matrix(file_bytes)
        return matrix, rows, "csv"
    if filename.endswith(".xls"):
        matrix, rows = _parse_xls_matrix(file_bytes)
        return matrix, rows, "xls"
    if filename.endswith(".xlsx"):
        matrix, rows = _parse_xlsx_matrix(file_bytes)
        return matrix, rows, "xlsx"
    raise ReconciliationError("Unsupported file format. Upload .csv, .xls, or .xlsx")


def _parse_column_index(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if not DIGITS_ONLY_RE.fullmatch(text):
        raise ReconciliationError("Column index must be a non-negative integer.")
    index = int(text)
    if index > MAX_COLUMN_INDEX:
        raise ReconciliationError(
            f"Column index is too large. Maximum supported value is {MAX_COLUMN_INDEX}."
        )
    return index


def _request_payload() -> dict[str, Any]:
    if request.is_json:
        data = request.get_json(silent=True) or {}
        return data if isinstance(data, dict) else {}
    return request.form.to_dict(flat=True)


def _csrf_check() -> None:
    token = request.headers.get("X-Grambook-CSRF", "")
    if token != _csrf_token():
        raise ReconciliationError(
            "Security token mismatch. Refresh the page and try again."
        )


def _csrf_token() -> str:
    token = session.get(CSRF_SESSION_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        session[CSRF_SESSION_KEY] = token
    return token


def _csrf_rate_limit_check() -> None:
    now = time.time()
    state = session.get(CSRF_RATE_LIMIT_KEY) or {}
    window_start = float(state.get("window_start", 0.0) or 0.0)
    request_count = int(state.get("count", 0) or 0)
    if now - window_start >= CSRF_RATE_LIMIT_WINDOW_SECONDS:
        window_start = now
        request_count = 0
    if request_count >= CSRF_RATE_LIMIT_MAX_REQUESTS:
        raise ReconciliationError(
            "Too many CSRF token requests. Please wait a moment and refresh the page."
        )
    session[CSRF_RATE_LIMIT_KEY] = {
        "window_start": window_start,
        "count": request_count + 1,
    }


def _session_scope_id() -> str:
    sid = session.get(CACHE_SESSION_KEY)
    if not sid:
        sid = secrets.token_urlsafe(16)
        session[CACHE_SESSION_KEY] = sid
    return sid


def _request_fingerprint(
    admin_bytes: bytes,
    admin_name: str,
    suv_bytes: bytes,
    suv_name: str,
    *parts: Any,
) -> str:
    payload = {
        "admin": hashlib.sha256(admin_bytes).hexdigest(),
        "suv": hashlib.sha256(suv_bytes).hexdigest(),
        "admin_name": admin_name,
        "suv_name": suv_name,
        "parts": [str(part) for part in parts],
        "session_id": _session_scope_id(),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _wrap_cache_result(result: dict[str, Any], cached_at: float | None = None) -> dict[str, Any]:
    return {
        "cached_at": cached_at if cached_at is not None else time.time(),
        "result": result,
    }


def _cache_entry_age(entry: dict[str, Any], fallback_cached_at: float | None = None) -> float | None:
    if not isinstance(entry, dict):
        return None
    cached_at = entry.get("cached_at")
    if isinstance(cached_at, (int, float)):
        return float(cached_at)
    return fallback_cached_at


def _cache_result(cache_key: str, result: dict[str, Any]) -> None:
    entry = _wrap_cache_result(result)
    tmp_path = None
    with RESULT_CACHE_LOCK:
        RESULT_CACHE[cache_key] = entry
        RESULT_CACHE_ORDER[:] = [k for k in RESULT_CACHE_ORDER if k != cache_key]
        RESULT_CACHE_ORDER.append(cache_key)
        while len(RESULT_CACHE_ORDER) > RESULT_CACHE_LIMIT:
            old_key = RESULT_CACHE_ORDER.pop(0)
            RESULT_CACHE.pop(old_key, None)
    try:
        cache_path = RESULT_CACHE_DIR / f"{cache_key}.json"
        tmp_path = cache_path.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(entry, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        tmp_path.replace(cache_path)
    except Exception:
        logger.debug("Failed to persist cache entry %s", cache_key)
        try:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
    _prune_disk_cache()


def _prune_disk_cache() -> None:
    global LAST_DISK_PRUNE_AT
    now = time.time()
    with RESULT_CACHE_LOCK:
        last_prune_at = LAST_DISK_PRUNE_AT
        if now - last_prune_at < 3600:
            return
        LAST_DISK_PRUNE_AT = now
    cutoff = now - RESULT_CACHE_TTL_SECONDS
    for path in RESULT_CACHE_DIR.glob("*.json"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
        except Exception:
            continue
    for path in RESULT_CACHE_DIR.glob("*.tmp"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
        except Exception:
            continue


def _cache_path(cache_key: str) -> Path:
    return RESULT_CACHE_DIR / f"{cache_key}.json"


def _lookup_cached_result(cache_key: str) -> dict[str, Any] | None:
    with RESULT_CACHE_LOCK:
        result = RESULT_CACHE.get(cache_key)
        if result is not None:
            RESULT_CACHE_ORDER[:] = [k for k in RESULT_CACHE_ORDER if k != cache_key]
            RESULT_CACHE_ORDER.append(cache_key)
            cached_at = _cache_entry_age(result)
            if cached_at is not None and time.time() - cached_at > RESULT_CACHE_TTL_SECONDS:
                RESULT_CACHE.pop(cache_key, None)
                return None
            if "result" in result and "cached_at" in result:
                return result["result"]
            return result

    cache_path = _cache_path(cache_key)
    if not cache_path.exists():
        return None
    try:
        file_mtime = cache_path.stat().st_mtime
        cached_entry = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(cached_entry, dict):
        return None
    cached_at = _cache_entry_age(cached_entry, file_mtime)
    if cached_at is not None and time.time() - cached_at > RESULT_CACHE_TTL_SECONDS:
        try:
            cache_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None
    entry = (
        cached_entry
        if "result" in cached_entry and "cached_at" in cached_entry
        else _wrap_cache_result(cached_entry, cached_at)
    )
    with RESULT_CACHE_LOCK:
        RESULT_CACHE[cache_key] = entry
        RESULT_CACHE_ORDER[:] = [k for k in RESULT_CACHE_ORDER if k != cache_key]
        RESULT_CACHE_ORDER.append(cache_key)
        while len(RESULT_CACHE_ORDER) > RESULT_CACHE_LIMIT:
            old_key = RESULT_CACHE_ORDER.pop(0)
            RESULT_CACHE.pop(old_key, None)
    return entry["result"]


def normalize_key(value: Any) -> str:
    if value is None:
        return ""
    v = _clean_text(value)
    if not v:
        return ""
    if DIGITS_ONLY_RE.match(v):
        if set(v) == {"0"}:
            return v[:MAX_ID_LENGTH]
        normalized = v.lstrip("0")
        if not normalized:
            normalized = "0"
        return normalized[:MAX_ID_LENGTH]
    return v[:MAX_ID_LENGTH]


def normalize_value(value: Any) -> str:
    return _normalize_for_compare(value)


def _excel_styles() -> dict[str, Any]:
    thin = Side(style="thin", color="D9D9D9")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    return {
        "border": border,
        "header_fill": PatternFill("solid", start_color="1F4E78"),
        "admin_fill": PatternFill("solid", start_color="FFF4F0"),
        "suv_fill": PatternFill("solid", start_color="F1FBF6"),
        "mismatch_fill_admin": PatternFill("solid", start_color="FDE2DB"),
        "mismatch_fill_suv": PatternFill("solid", start_color="DDF3E7"),
        "sep_fill": PatternFill("solid", start_color="EFF2F7"),
        "header_font": Font(bold=True, color="FFFFFF", size=10),
        "bold_font": Font(bold=True, color="1F2937", size=10),
        "admin_font": Font(bold=True, color="B5451B", size=10),
        "suv_font": Font(bold=True, color="1A6B4A", size=10),
    }


def _build_discrepancy_report_buffer(
    result: dict[str, Any],
) -> tempfile.SpooledTemporaryFile:
    styles = _excel_styles()
    wb = Workbook(write_only=True)

    def _wo_cell(ws, value, *, fill=None, font=None, align="left", wrap=False, border=None):
        cell = WriteOnlyCell(ws, value=value)
        if fill is not None:
            cell.fill = fill
        if font is not None:
            cell.font = font
        cell.alignment = Alignment(horizontal=align, vertical="center", wrap_text=wrap)
        if border is not None:
            cell.border = border
        return cell

    def _row_list(item: dict[str, Any], side: str) -> list[Any]:
        value = item.get(side)
        if isinstance(value, list):
            return list(value)
        return []

    def _diff_columns(item: dict[str, Any], width: int) -> set[int]:
        diffs = item.get("diff_columns")
        if isinstance(diffs, list):
            return {int(idx) for idx in diffs if isinstance(idx, int) and 0 <= idx < width}
        return set()

    items = list(result.get("mismatches", []))
    ws = wb.create_sheet("Mismatches")
    if not items:
        ws.append(
            [
                _wo_cell(
                    ws,
                    "No mismatches found",
                    fill=styles["header_fill"],
                    font=styles["header_font"],
                    border=styles["border"],
                )
            ]
        )
        buf = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024, mode="w+b")
        wb.save(buf)
        buf.seek(0)
        return buf

    max_width = 0
    for item in items:
        max_width = max(
            max_width,
            len(_row_list(item, "admin")),
            len(_row_list(item, "suvidha")),
        )
    headers = [f"Column {i + 1}" for i in range(max_width)]

    ws.append(
        [
            _wo_cell(
                ws,
                "TYPE",
                fill=styles["header_fill"],
                font=styles["header_font"],
                align="center",
                wrap=True,
                border=styles["border"],
            ),
            *[
                _wo_cell(
                    ws,
                    col,
                    fill=styles["header_fill"],
                    font=styles["header_font"],
                    align="center",
                    wrap=True,
                    border=styles["border"],
                )
                for col in headers
            ],
        ]
    )

    for item in items:
        admin_row = _row_list(item, "admin")
        suv_row = _row_list(item, "suvidha")
        width = max(len(admin_row), len(suv_row), max_width)
        diff_cols = _diff_columns(item, width)

        for source_label, source_row, fill, diff_fill, font in [
            (
                "ADMIN",
                admin_row,
                styles["admin_fill"],
                styles["mismatch_fill_admin"],
                styles["admin_font"],
            ),
            (
                "SUVIDHA",
                suv_row,
                styles["suv_fill"],
                styles["mismatch_fill_suv"],
                styles["suv_font"],
            ),
        ]:
            row_cells = [
                _wo_cell(
                    ws,
                    source_label,
                    fill=fill,
                    font=font,
                    border=styles["border"],
                )
            ]
            for idx in range(max_width):
                value = source_row[idx] if idx < len(source_row) else ""
                row_cells.append(
                    _wo_cell(
                        ws,
                        value,
                        fill=diff_fill if idx in diff_cols else None,
                        font=styles["bold_font"] if idx in diff_cols else None,
                        border=styles["border"],
                        wrap=True,
                    )
                )
            ws.append(row_cells)

        ws.append(
            [
                _wo_cell(ws, "", fill=styles["sep_fill"], border=styles["border"])
                for _ in range(max_width + 1)
            ]
        )

    buf = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024, mode="w+b")
    try:
        wb.save(buf)
        buf.seek(0)
        return buf
    except Exception:
        buf.close()
        raise


def reconcile_raw(
    admin_rows: list[list[str]],
    admin_row_numbers: list[int],
    suv_rows: list[list[str]],
    suv_row_numbers: list[int],
    id_column_index: int,
) -> dict[str, Any]:
    id_index = max(0, int(id_column_index or 0))

    def _row_key(row: list[str]) -> str:
        if id_index >= len(row):
            return ""
        return normalize_key(_clean_text(row[id_index]))

    admin_map: dict[str, dict[str, Any]] = {}
    suv_map: dict[str, dict[str, Any]] = {}
    duplicates: list[dict[str, Any]] = []

    for idx, row in enumerate(admin_rows):
        source_row_number = (
            admin_row_numbers[idx] if idx < len(admin_row_numbers) else idx + 1
        )
        key = _row_key(row)
        if not key:
            continue
        if key in admin_map:
            previous_row_number = admin_map[key]["row_number"]
            logger.warning(
                "Duplicate key '%s' in Admin at row %s - earlier row %s discarded",
                key,
                source_row_number,
                previous_row_number,
            )
            duplicates.append(
                {
                    "source": "admin",
                    "id": key,
                    "row_number": source_row_number,
                    "replaced_row_number": previous_row_number,
                }
            )
        admin_map[key] = {"row": list(row), "row_number": source_row_number}

    for idx, row in enumerate(suv_rows):
        source_row_number = (
            suv_row_numbers[idx] if idx < len(suv_row_numbers) else idx + 1
        )
        key = _row_key(row)
        if not key:
            continue
        if key in suv_map:
            previous_row_number = suv_map[key]["row_number"]
            logger.warning(
                "Duplicate key '%s' in Suvidha at row %s - earlier row %s discarded",
                key,
                source_row_number,
                previous_row_number,
            )
            duplicates.append(
                {
                    "source": "suvidha",
                    "id": key,
                    "row_number": source_row_number,
                    "replaced_row_number": previous_row_number,
                }
            )
        suv_map[key] = {"row": list(row), "row_number": source_row_number}

    all_keys = sorted(set(admin_map.keys()) | set(suv_map.keys()))
    mismatches: list[dict[str, Any]] = []
    admin_only: list[dict[str, Any]] = []
    suvidha_only: list[dict[str, Any]] = []

    for key in all_keys:
        a = admin_map.get(key)
        b = suv_map.get(key)
        if a and b:
            a_row = a["row"]
            b_row = b["row"]
            diff_cols: list[int] = []
            max_len = max(len(a_row), len(b_row))
            for i in range(max_len):
                if normalize_value(a_row[i] if i < len(a_row) else "") != normalize_value(
                    b_row[i] if i < len(b_row) else ""
                ):
                    diff_cols.append(i)
            if diff_cols:
                mismatches.append(
                    {
                        "id": key,
                        "diff_columns": diff_cols,
                        "admin": a_row,
                        "suvidha": b_row,
                        "admin_row_number": a["row_number"],
                        "suvidha_row_number": b["row_number"],
                    }
                )
        elif a:
            admin_only.append(
                {"id": key, "row": a["row"], "row_number": a["row_number"]}
            )
        elif b:
            suvidha_only.append(
                {"id": key, "row": b["row"], "row_number": b["row_number"]}
            )

    column_count = max(
        max((len(row) for row in admin_rows), default=0),
        max((len(row) for row in suv_rows), default=0),
    )
    return {
        "mismatches": mismatches,
        "admin_only": admin_only,
        "suvidha_only": suvidha_only,
        "stats": {
            "total": len(all_keys),
            "mismatched": len(mismatches),
            "only_a": len(admin_only),
            "only_s": len(suvidha_only),
            "matched": len(all_keys)
            - len(mismatches)
            - len(admin_only)
            - len(suvidha_only),
        },
        "meta": {
            "id_column_index": id_index,
            "comparison_mode": "column_position",
            "column_count": column_count,
        },
        "column_count": column_count,
        "duplicates": duplicates,
    }


def generate_discrepancy_report(result: dict[str, Any]) -> Path:
    buf = _build_discrepancy_report_buffer(result)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    try:
        buf.seek(0)
        shutil.copyfileobj(buf, tmp)
        tmp.flush()
    finally:
        try:
            buf.close()
        except Exception:
            pass
        tmp.close()
    return Path(tmp.name)


@app.route("/")
def index():
    response = send_from_directory(STATIC_DIR, "index.html")
    response.cache_control.no_cache = True
    response.cache_control.no_store = True
    response.cache_control.must_revalidate = True
    response.headers["Pragma"] = "no-cache"
    response.expires = 0
    return response


@app.route("/api/csrf", methods=["GET"])
def api_csrf():
    try:
        _csrf_rate_limit_check()
        return jsonify({"token": _csrf_token()})
    except ReconciliationError as exc:
        return jsonify({"error": str(exc)}), 429


@app.route("/reconcile", methods=["POST"])
@app.route("/api/reconcile", methods=["POST"])
def api_reconcile():
    try:
        _csrf_check()
        payload = _request_payload()
        admin_upload = request.files.get("admin_file")
        suv_upload = request.files.get("suvidha_file")
        if not admin_upload or not suv_upload:
            return jsonify({"error": "Both files are required."}), 400
        id_column_index = _parse_column_index(payload.get("id_column_index")) or 0
        admin_bytes, admin_name = _file_bytes(admin_upload)
        suv_bytes, suv_name = _file_bytes(suv_upload)
        _validate_upload_bytes(admin_bytes, admin_name)
        _validate_upload_bytes(suv_bytes, suv_name)
        cache_key = _request_fingerprint(
            admin_bytes, admin_name, suv_bytes, suv_name, id_column_index
        )
        admin_rows, admin_row_numbers, _ = _parse_matrix_from_bytes(
            admin_bytes, admin_name
        )
        suv_rows, suv_row_numbers, _ = _parse_matrix_from_bytes(suv_bytes, suv_name)
        result = reconcile_raw(
            admin_rows,
            admin_row_numbers,
            suv_rows,
            suv_row_numbers,
            id_column_index,
        )
        _cache_result(cache_key, result)
        response = dict(result)
        response["cache_key"] = cache_key
        return jsonify(response)
    except ReconciliationError as exc:
        logger.warning("Reconcile rejected: %s", exc)
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:  # pragma: no cover
        logger.exception("Reconcile failed")
        return jsonify({"error": f"Reconciliation failed: {exc}"}), 500


@app.route("/api/download", methods=["POST"])
def api_download():
    try:
        _csrf_check()
        payload = _request_payload()
        cache_key = _clean_text(payload.get("cache_key"))
        result = _lookup_cached_result(cache_key) if cache_key else None

        if result is None:
            admin_upload = request.files.get("admin_file")
            suv_upload = request.files.get("suvidha_file")
            if not admin_upload or not suv_upload:
                return jsonify(
                    {"error": "Either cache_key or both files are required."}
                ), 400
            id_column_index = _parse_column_index(payload.get("id_column_index")) or 0
            admin_bytes, admin_name = _file_bytes(admin_upload)
            suv_bytes, suv_name = _file_bytes(suv_upload)
            _validate_upload_bytes(admin_bytes, admin_name)
            _validate_upload_bytes(suv_bytes, suv_name)
            cache_key = _request_fingerprint(
                admin_bytes, admin_name, suv_bytes, suv_name, id_column_index
            )
            admin_rows, admin_row_numbers, _ = _parse_matrix_from_bytes(
                admin_bytes, admin_name
            )
            suv_rows, suv_row_numbers, _ = _parse_matrix_from_bytes(suv_bytes, suv_name)
            result = reconcile_raw(
                admin_rows,
                admin_row_numbers,
                suv_rows,
                suv_row_numbers,
                id_column_index,
            )
            _cache_result(cache_key, result)

        tmp_path = generate_discrepancy_report(result)
        @after_this_request
        def _cleanup_download(response):
            response.call_on_close(lambda: tmp_path.unlink(missing_ok=True))
            return response

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return send_file(
            tmp_path,
            as_attachment=True,
            download_name=f"grambook_reconciliation_{timestamp}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except ReconciliationError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:  # pragma: no cover
        logger.exception("Download failed")
        return jsonify({"error": f"Failed to generate Excel report: {exc}"}), 500


@app.errorhandler(413)
def request_too_large(_exc):
    return jsonify(
        {"error": "Uploaded file is too large. Maximum allowed size is 50 MB."}
    ), 413


if __name__ == "__main__":
    print("Grambook Reconciliation Tool")
    print(f"Static dir: {STATIC_DIR}")
    app.run(debug=True, host="127.0.0.1", port=5000)

