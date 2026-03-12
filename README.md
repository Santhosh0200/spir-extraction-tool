# SPIR Extraction Tool — Enterprise Edition (v9.0)

> Automated extraction platform for Spare Parts & Interchangeability Records (SPIR).
> Supports 5 SPIR formats, concurrent users, large files (50 MB+), and background processing.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Browser (existing UI — unchanged)                                  │
│  static/index.html                                                  │
└───────────────┬─────────────────────────────────────────────────────┘
                │  HTTP  POST /extract
                │         GET /status/{job_id}
                │         GET /download/{file_id}
                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  FastAPI Application  (app/main.py + app/routes.py)                 │
│                                                                     │
│  ┌─────────────────┐   file ≤ 5 MB   ┌───────────────────────────┐ │
│  │  POST /extract  │ ───────────────► │  Inline pipeline          │ │
│  │                 │                  │  (synchronous, <5 s)      │ │
│  │                 │   file > 5 MB   └───────────────────────────┘ │
│  │                 │ ───────────────► Enqueue → return job_id      │ │
│  └────────┬────────┘                                               │ │
│           │  GET /status/{job_id}                                  │ │
│           │  GET /download/{file_id}                               │ │
└───────────┼─────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Redis                                                              │
│  • Celery task queue (broker DB 0)                                  │
│  • Celery result backend (DB 1)                                     │
│  • Job progress store  (spir:progress:{job_id})                     │
│  • Generated XLSX store (spir:result:{file_id})                     │
└───────────────┬─────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Celery Worker  (app/worker.py)                                     │
│  Task: spir.process_file                                            │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  Extraction Pipeline                                         │  │
│  │                                                              │  │
│  │  1. validate_file()        extraction/spir_detector.py       │  │
│  │  2. detect_format()        extraction/spir_engine.py         │  │
│  │  3. ColumnMapper.scan()    extraction/column_mapper.py       │  │
│  │  4. extract_spir()         extraction/spir_engine.py         │  │
│  │     ├── extract_format1()  annexure sheets                   │  │
│  │     ├── extract_format2()  single tag                        │  │
│  │     ├── extract_format3()  multi-tag, all flags              │  │
│  │     ├── extract_format4()  matrix + continuation             │  │
│  │     └── extract_format5()  flag + multi-continuation         │  │
│  │  5. analyse_duplicates()   services/duplicate_checker.py     │  │
│  │  6. build_xlsx()           services/excel_builder.py         │  │
│  │  7. store_result()         Redis                             │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
spir_enterprise/
│
├── app/
│   ├── __init__.py
│   ├── main.py          ← FastAPI factory, middleware, lifecycle
│   ├── routes.py        ← All API endpoints
│   ├── worker.py        ← Celery tasks + Redis helpers
│   └── config.py        ← Pydantic settings (reads .env)
│
├── extraction/
│   ├── __init__.py
│   ├── spir_engine.py   ← Core parser (FORMAT1-5) — battle-tested, untouched
│   ├── spir_detector.py ← File validation + format detection wrapper
│   ├── column_mapper.py ← Dynamic column detection engine
│   └── annexure_parser.py ← Standalone annexure/continuation sheet parser
│
├── services/
│   ├── __init__.py
│   ├── excel_builder.py    ← Builds styled output XLSX
│   └── duplicate_checker.py ← Post-extraction duplicate analysis
│
├── models/
│   ├── __init__.py
│   └── spir_schema.py   ← Pydantic models (request/response contracts)
│
├── static/
│   └── index.html       ← Existing UI (unchanged layout + styling)
│
├── tests/
│   ├── __init__.py
│   └── test_extraction.py ← Full test suite
│
├── logs/                ← Structured JSON logs (mounted volume in Docker)
├── .env.example
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/extract` | Upload SPIR file → synchronous or background extraction |
| `GET`  | `/status/{job_id}` | Poll background job status |
| `GET`  | `/download/{file_id}` | Download generated XLSX (valid 1 hour) |
| `POST` | `/inspect` | Structural analysis of workbook (debug) |
| `GET`  | `/health` | System health check (Redis + Celery) |
| `GET`  | `/api/docs` | Interactive Swagger UI |
| `GET`  | `/api/redoc` | ReDoc documentation |

### POST /extract — Response

```json
{
  "job_id":         "a1b2c3d4",
  "status":         "done",
  "background":     false,
  "format":         "FORMAT3",
  "spir_no":        "VEN-4460-KAHS-5-43-1002",
  "equipment":      "EPIC FOR DUKHAN ...",
  "manufacturer":   "Arabian Industries Manufacturing LLC",
  "spir_type":      "Normal Operating Spares",
  "total_tags":     2,
  "spare_items":    11,
  "total_rows":     18,
  "dup1_count":     0,
  "sap_count":      2,
  "annexure_stats": {},
  "preview_cols":   ["SPIR NO", "TAG NO", ...],
  "preview_rows":   [[...], ...],
  "file_id":        "uuid-for-download",
  "filename":       "VEN-4460-KAHS-5-43-1002_SPIR_Extraction.xlsx"
}
```

For large files (>5 MB), `status` = `"queued"` and only `job_id` is returned.
Poll `/status/{job_id}` until `status == "done"`.

---

## Dynamic Column Detection

`extraction/column_mapper.py` implements a keyword-based column scanner
that finds columns without relying on fixed indices.

```python
from extraction.column_mapper import map_sheet

mapper = map_sheet(ws, header_rows=[5, 6, 7])

sap_col  = mapper.get("SAP_NUMBER")    # → 26  (or None)
desc_col = mapper.get("DESCRIPTION")  # → 9
print(mapper.coverage())               # → 0.82  (82% of known fields found)
print(mapper.report())                 # → full field→column mapping dict
```

The engine scans rows 4–8 (configurable), tries each keyword for a field
in priority order, and returns the first matching column index.
A confidence score (0.7–1.0) is attached to each match.

---

## Quick Start

### Local (no Docker)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start Redis
docker run -d -p 6379:6379 redis:7-alpine

# 3. Start the API
uvicorn app.main:app --reload --port 8000

# 4. (Optional) Start Celery worker for large files
celery -A app.worker.celery_app worker --loglevel=info

# 5. Open browser
open http://localhost:8000
```

### Docker Compose (recommended)

```bash
# Start full stack (API + 2 workers + Redis)
docker compose up --build

# Scale workers
docker compose up --scale worker=4

# With monitoring (Flower at http://localhost:5555)
docker compose --profile monitoring up --build
```

### Run Tests

```bash
pytest tests/ -v
pytest tests/ -v --tb=short    # less verbose on failures
pytest tests/test_extraction.py::TestColumnMapper -v   # single class
```

---

## Supported SPIR Formats

| Format | Description | Detection |
|--------|-------------|-----------|
| FORMAT 1 | Multi-Annexure (.xlsx) | `annexure` sheets present |
| FORMAT 2 | Single-Sheet, 1 Tag (.xlsm) | Single tag col, no annexure |
| FORMAT 3 | Single-Sheet, Multi-Tag (.xlsm) | Multiple tag cols, flag per row |
| FORMAT 4 | Matrix SPIR + Continuation (.xlsx) | Exactly 1 continuation sheet |
| FORMAT 5 | Flag SPIR + Multi-Continuation (.xlsm) | 2+ continuation sheets |

---

## Configuration

All settings are in `.env` (copy from `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_FILE_SIZE_MB` | `100` | Max upload size |
| `RESULT_TTL_SECONDS` | `3600` | How long XLSX is kept in Redis |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `WORKERS` | `4` | Uvicorn worker processes |
| `DEBUG` | `false` | Enable debug mode |

---

## Key Design Decisions

**Why was the core engine kept as-is?**
The `extraction/spir_engine.py` module is 2000+ lines of battle-tested
extraction logic that handles all 5 formats correctly. Rewriting it would
introduce regression risk. The enterprise refactor wraps it in a
production-quality shell without touching its internals.

**Why Redis for file storage?**
Generated XLSX files are transient (1-hour TTL). Redis binary storage avoids
disk management, works across multiple API workers, and is already required
for Celery. Files >5 MB stored in Redis still fit comfortably within the
512 MB Redis limit at typical SPIR file sizes.

**Why the 5 MB background threshold?**
SPIR files typically extract in 1-4 seconds under 5 MB. Above that, openpyxl's
read time becomes noticeable and can block the HTTP event loop. The threshold
is tunable via the `BG_THRESHOLD_MB` constant in `app/routes.py`.
