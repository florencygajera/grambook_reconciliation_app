# Grambook Reconciliation Tool

A full-stack web app: Flask backend + browser frontend.
Upload Admin & Suvidha exports, pick the linking ID column.

**Download format (NEW):**
- ONLY discrepancy records
- 2 rows per discrepancy: "Admin - KEY" / "Suvidha - KEY"  
- ONLY mismatched columns filled (others BLANK)
- Mismatches: red bold font
- Preserves Gujarati text, headers, normalization

## Setup & Run

```bash
pip install -r requirements.txt
python app.py
```

Open: `http://localhost:5000`

## Production Notes

- Set `GRAMBOOK_SECRET_KEY` to the same value on every worker/process so CSRF tokens stay stable in multi-worker deployments.
- The browser UI now truncates very large JSON result sets for safety. Use the Excel download for the full discrepancy report.

## OCR Support (Gujarati/Hindi/English)

The backend now supports OCR fallback for `.xlsx` files that contain embedded image content.

1. Install Tesseract OCR on your machine.
2. Ensure language packs are installed: Gujarati (`guj`), Hindi (`hin`), English (`eng`).
3. Keep Tesseract available in system PATH.

If OCR dependencies are missing, the app still works with normal typed Excel/CSV data.

## API Endpoints

- `GET /` - UI
- `POST /api/columns` - Return column names from both files
- `POST /api/reconcile` - Return reconciliation JSON
- `POST /api/download` - Download reconciliation `.xlsx`

POST inputs (`multipart/form-data`):
- `admin_file`
- `suvidha_file`
- `admin_key` (not needed for `/api/columns`)
- `suv_key` (not needed for `/api/columns`)
