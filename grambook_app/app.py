"""
Grambook Reconciliation - Flask Backend
Run: python app.py
Then open http://localhost:5000
"""

import io
import os
import re
import shutil
import unicodedata
from datetime import datetime

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

app = Flask(__name__, static_folder="static")


def _iter_registry_tesseract_paths():
    if winreg is None:
        return []

    roots = [
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall"),
        (winreg.HKEY_CURRENT_USER, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
    ]
    found = []
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
                                candidate = raw if raw.lower().endswith(".exe") else os.path.join(raw, "tesseract.exe")
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


def _configure_tesseract():
    """
    Configure pytesseract executable path on Windows/non-standard installs.
    Priority:
    1. TESSERACT_CMD env var
    2. PATH (shutil.which)
    3. Common Windows install paths
    """
    if pytesseract is None:
        return

    candidates = []
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


def _border():
    s = Side(style="thin")
    return Border(left=s, right=s, top=s, bottom=s)


def _style(cell, fill_hex=None, bold=False, color="000000", align="left", font_size=9, wrap=False):
    if fill_hex:
        cell.fill = PatternFill("solid", start_color=fill_hex)
    cell.font = Font(bold=bold, color=color, name="Arial", size=font_size)
    cell.alignment = Alignment(horizontal=align, vertical="center", wrap_text=wrap)
    cell.border = _border()


def _header_row(ws, row_n, values, fill_hex, font_color="FFFFFF", height=28):
    ws.row_dimensions[row_n].height = height
    for col_n, val in enumerate(values, 1):
        cell = ws.cell(row=row_n, column=col_n, value=val)
        cell.fill = PatternFill("solid", start_color=fill_hex)
        cell.font = Font(bold=True, color=font_color, name="Arial", size=10)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = _border()


def _auto_col_widths(ws, headers, data_rows):
    for idx, h in enumerate(headers, 1):
        col_vals = [str(h)] + [str(r.get(h, "")) for r in data_rows]
        width = min(max(len(v) for v in col_vals) + 4, 38)
        ws.column_dimensions[get_column_letter(idx)].width = width


def normalize(s):
    return s.lower().replace(" ", "").replace("_", "").replace("-", "")


INDIC_DIGIT_MAP = str.maketrans(
    {
        # Devanagari digits U+0966..U+096F
        "\u0966": "0",
        "\u0967": "1",
        "\u0968": "2",
        "\u0969": "3",
        "\u096A": "4",
        "\u096B": "5",
        "\u096C": "6",
        "\u096D": "7",
        "\u096E": "8",
        "\u096F": "9",
        # Gujarati digits U+0AE6..U+0AEF
        "\u0AE6": "0",
        "\u0AE7": "1",
        "\u0AE8": "2",
        "\u0AE9": "3",
        "\u0AEA": "4",
        "\u0AEB": "5",
        "\u0AEC": "6",
        "\u0AED": "7",
        "\u0AEE": "8",
        "\u0AEF": "9",
    }
)


def canonical_text(value):
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(INDIC_DIGIT_MAP)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _ocr_text_from_image(raw_image_bytes):
    if not raw_image_bytes or Image is None or pytesseract is None:
        return ""

    try:
        with Image.open(io.BytesIO(raw_image_bytes)) as img:
            # Try Gujarati/Hindi/English first, then fallback to English only.
            for lang in ("guj+hin+eng", "hin+eng", "eng"):
                try:
                    text = pytesseract.image_to_string(img, lang=lang)
                    text = canonical_text(text)
                    if text:
                        return text
                except Exception:
                    continue
    except Exception:
        return ""

    return ""


def _inject_ocr_text_from_xlsx_images(file_bytes, df):
    """
    OCR fallback for xlsx files containing embedded images instead of typed text.
    We put OCR text into the image anchor cell if that cell is currently blank.
    """
    if Image is None or pytesseract is None or df.empty:
        return df

    try:
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True)
        ws = wb.worksheets[0]
        images = getattr(ws, "_images", [])
        if not images:
            return df

        for img in images:
            anchor = getattr(img, "anchor", None)
            marker = getattr(anchor, "_from", None)
            if marker is None:
                continue

            # openpyxl image anchors are zero-based at sheet level.
            # DataFrame excludes the header row, so sheet row index needs -1.
            row_idx = marker.row - 1
            col_idx = marker.col
            if row_idx < 0 or col_idx < 0 or row_idx >= len(df.index):
                continue

            if col_idx >= len(df.columns):
                col_name = f"OCR_COL_{col_idx + 1}"
                if col_name not in df.columns:
                    df[col_name] = ""
            else:
                col_name = df.columns[col_idx]

            if canonical_text(df.at[row_idx, col_name]):
                continue

            raw = None
            try:
                raw = img._data()
            except Exception:
                raw = None

            ocr_text = _ocr_text_from_image(raw)
            if ocr_text:
                df.at[row_idx, col_name] = ocr_text

    except Exception:
        # Keep standard flow working even if OCR parsing fails.
        return df

    return df


def _parse_dataframe_from_upload(file_storage):
    """
    Read bytes once and parse from in-memory streams.
    This keeps OCR fallback and normal dataframe parsing compatible.
    """
    fname = (file_storage.filename or "").lower()
    file_bytes = file_storage.read()
    if not file_bytes:
        raise Exception("Uploaded file is empty.")

    try:
        if fname.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_bytes), dtype=str).fillna("")

        elif fname.endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl", dtype=str).fillna("")
            df = _inject_ocr_text_from_xlsx_images(file_bytes, df)

        elif fname.endswith(".xls"):
            try:
                df = pd.read_excel(io.BytesIO(file_bytes), engine="xlrd", dtype=str).fillna("")
            except ImportError:
                raise Exception(".xls file detected but xlrd is not installed. Run: pip install xlrd==2.0.1")

        else:
            raise Exception("Unsupported file format. Use .csv, .xls, or .xlsx")

    except Exception as e:
        raise Exception(f"File parsing failed: {str(e)}")

    return df


def read_file(file_storage):
    df = _parse_dataframe_from_upload(file_storage)

    # 🔥 STEP 1: Remove fully empty rows
    df = df.dropna(how="all")

    # 🔥 STEP 2: Detect actual header row (VERY IMPORTANT)
    header_row_index = None

    for i in range(min(20, len(df))):
        row = df.iloc[i].astype(str).str.strip()

        # Count meaningful cells
        non_empty = row[row != ""]

        # Heuristic rules for header:
        # 1. Has multiple columns
        # 2. Mostly TEXT (not numbers)
        # 3. No long sentences (avoid title rows)

        text_cells = sum(not v.replace('.', '', 1).isdigit() for v in non_empty)
        
        if (
            len(non_empty) >= 4 and          # enough columns
            text_cells >= len(non_empty) * 0.6 and  # mostly text
            all(len(v) < 50 for v in non_empty)     # not long sentences
        ):
            header_row_index = i
            break

    if header_row_index is None:
        raise Exception("❌ Could not detect proper header row.")

    # 🔥 STEP 3: Set header
    df.columns = df.iloc[header_row_index]
    df = df[header_row_index + 1:]

    # 🔥 STEP 4: Clean column names
    new_cols = []
    for i, col in enumerate(df.columns):
        col = canonical_text(col)

        if not col or col.lower().startswith("unnamed"):
            col = f"column_{i}"

        new_cols.append(col)

    # Ensure column names are unique to avoid pandas warnings
    seen = {}
    unique_cols = []
    for col in new_cols:
        if col in seen:
            seen[col] += 1
            unique_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            unique_cols.append(col)

    df.columns = unique_cols

    # 🔥 STEP 5: Clean data
    df = df.apply(lambda col: col.map(canonical_text))

    # 🔥 STEP 6: Drop garbage rows again
    df = df[df.apply(lambda row: any(str(v).strip() for v in row), axis=1)]

    return df.to_dict(orient="records"), list(df.columns)

def reconcile(admin_rows, admin_cols, suv_rows, suv_cols, admin_key, suv_key):
    admin_idx = {}
    for r in admin_rows:
        k = canonical_text(r.get(admin_key, ""))
        if k:
            admin_idx[k] = r

    suv_idx = {}
    for r in suv_rows:
        k = canonical_text(r.get(suv_key, ""))
        if k:
            suv_idx[k] = r

    all_admin = set(admin_idx)
    all_suv = set(suv_idx)
    common = all_admin & all_suv
    only_a = all_admin - all_suv
    only_s = all_suv - all_admin

    suv_norm = {normalize(c): c for c in suv_cols if c != suv_key}
    col_pairs = []
    for ac in admin_cols:
        if ac == admin_key:
            continue
        sc = suv_norm.get(normalize(ac))
        if sc:
            col_pairs.append((ac, sc))

    discrepancies = []
    for k in sorted(common):
        a = admin_idx[k]
        s = suv_idx[k]
        diffs = {}
        for ac, sc in col_pairs:
            av = canonical_text(a.get(ac, ""))
            sv = canonical_text(s.get(sc, ""))
            if av != sv:
                diffs[ac] = {"admin": av, "suvidha": sv, "suv_col": sc}
        if diffs:
            discrepancies.append({"key": k, "admin_row": a, "suv_row": s, "diffs": diffs})

    only_admin_rows = [admin_idx[k] for k in sorted(only_a)]
    only_suvidha_rows = [suv_idx[k] for k in sorted(only_s)]

    total = len(all_admin | all_suv)
    matched = len(common) - len(discrepancies)

    return {
        "discrepancies": discrepancies,
        "only_admin_rows": only_admin_rows,
        "only_suv_rows": only_suvidha_rows,
        "col_pairs": col_pairs,
        "admin_key": admin_key,
        "suv_key": suv_key,
        "admin_cols": admin_cols,
        "suv_cols": suv_cols,
        "stats": {
            "total": total,
            "matched": matched,
            "disc": len(discrepancies),
            "only_a": len(only_admin_rows),
            "only_s": len(only_suvidha_rows),
        },
    }


def build_xlsx(result):
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary"
    
    # Simple test data
    ws['A1'] = "Grambook Reconciliation Report"
    ws['A2'] = f"Generated: {datetime.now()}"
    ws['A4'] = "Summary Statistics:"
    ws['A5'] = f"Total records: {result['stats']['total']}"
    ws['A6'] = f"Matched: {result['stats']['matched']}"
    ws['A7'] = f"Discrepancies: {result['stats']['disc']}"
    
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


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
        _, admin_cols = read_file(admin_file)
        _, suv_cols = read_file(suv_file)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"admin_cols": admin_cols, "suv_cols": suv_cols})


@app.route("/api/reconcile", methods=["POST"])
def run_reconcile():
    admin_file = request.files.get("admin_file")
    suv_file = request.files.get("suvidha_file")
    admin_key = request.form.get("admin_key", "").strip()
    suv_key = request.form.get("suv_key", "").strip()

    if not admin_file or not suv_file:
        return jsonify({"error": "Both files are required."}), 400
    if not admin_key or not suv_key:
        return jsonify({"error": "Key columns must be selected."}), 400

    try:
        admin_rows, admin_cols = read_file(admin_file)
        suv_rows, suv_cols = read_file(suv_file)
    except Exception as e:
        return jsonify({"error": f"File read error: {e}"}), 500

    admin_key = canonical_text(admin_key)
    suv_key = canonical_text(suv_key)

    if admin_key not in admin_cols:
        return jsonify({"error": f"'{admin_key}' not found in Admin file."}), 400
    if suv_key not in suv_cols:
        return jsonify({"error": f"'{suv_key}' not found in Suvidha file."}), 400

    result = reconcile(admin_rows, admin_cols, suv_rows, suv_cols, admin_key, suv_key)

    for d in result["discrepancies"]:
        d["diffs"] = {k: {"admin": v["admin"], "suvidha": v["suvidha"]} for k, v in d["diffs"].items()}

    return jsonify(result)


@app.route("/api/download", methods=["POST"])
def download():
    admin_file = request.files.get("admin_file")
    suv_file = request.files.get("suvidha_file")
    admin_key = canonical_text(request.form.get("admin_key", "").strip())
    suv_key = canonical_text(request.form.get("suv_key", "").strip())

    if not admin_file or not suv_file or not admin_key or not suv_key:
        return jsonify({"error": "Missing data."}), 400

    try:
        admin_rows, admin_cols = read_file(admin_file)
        suv_rows, suv_cols = read_file(suv_file)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if admin_key not in admin_cols:
        return jsonify({"error": f"'{admin_key}' not found in Admin file."}), 400
    if suv_key not in suv_cols:
        return jsonify({"error": f"'{suv_key}' not found in Suvidha file."}), 400

    try:
        result = reconcile(admin_rows, admin_cols, suv_rows, suv_cols, admin_key, suv_key)
        buf = build_xlsx(result)
    except Exception as e:
        return jsonify({"error": f"Failed to generate Excel file: {str(e)}"}), 500

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
