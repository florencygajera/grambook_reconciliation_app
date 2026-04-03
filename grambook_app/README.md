# Grambook Reconciliation Tool

A full-stack web app: Flask backend + browser frontend.
Upload Admin & Suvidha exports, pick the linking ID column, and get a discrepancy report downloadable as `.xlsx`.

## Setup & Run

```bash
pip install -r requirements.txt
python app.py
```

Open: `http://localhost:5000`

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
