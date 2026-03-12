"""
app/worker.py
──────────────
Celery worker configuration and task definitions.

The single task `process_spir_file` handles the full extraction pipeline
for large files that would block the HTTP request cycle.

Architecture:
  FastAPI /extract endpoint
      │
      ├─ file ≤ threshold  →  extract inline  →  return result immediately
      └─ file > threshold  →  enqueue task   →  return job_id for polling

  Celery worker (separate process) picks up the task from Redis,
  runs the full pipeline, stores the result back in Redis.

  Client polls GET /status/{job_id} until status == 'done'.
"""
from __future__ import annotations
import io
import json
import logging
import traceback
import uuid

from celery import Celery
import openpyxl
import redis

from app.config import get_settings
from extraction.adaptive_extractor import spir_dispatcher
from extraction.spir_engine import OUTPUT_COLS, CI
from services.excel_builder import build_xlsx
from services.duplicate_checker import analyse_duplicates

log = logging.getLogger(__name__)
cfg = get_settings()

# ── Celery app ────────────────────────────────────────────────────────────────

celery_app = Celery(
    "spir_worker",
    broker  = cfg.celery_broker,
    backend = cfg.celery_backend,
)

celery_app.conf.update(
    task_serializer         = "json",
    result_serializer       = "json",
    accept_content          = ["json"],
    result_expires          = cfg.result_ttl_seconds,
    task_track_started      = True,
    task_acks_late          = True,          # re-queue on worker crash
    worker_prefetch_multiplier = 1,          # one task at a time per worker
    task_soft_time_limit    = 300,           # 5 min soft limit
    task_time_limit         = 360,           # 6 min hard kill
)

# ── Redis helper for storing binary results ───────────────────────────────────

def _redis_client() -> redis.Redis:
    return redis.from_url(cfg.redis_url, decode_responses=False)


def store_result(file_id: str, xlsx_bytes: bytes, filename: str) -> None:
    """Store generated XLSX bytes in Redis with TTL."""
    r = _redis_client()
    key = f"spir:result:{file_id}"
    payload = json.dumps({"filename": filename}).encode() + b"\x00" + xlsx_bytes
    r.setex(key, cfg.result_ttl_seconds, payload)


def retrieve_result(file_id: str) -> tuple[bytes, str] | None:
    """
    Retrieve (xlsx_bytes, filename) from Redis.
    Returns None if expired or not found.
    """
    r = _redis_client()
    raw = r.get(f"spir:result:{file_id}")
    if raw is None:
        return None
    sep = raw.index(b"\x00")
    meta = json.loads(raw[:sep])
    xlsx = raw[sep + 1:]
    return xlsx, meta["filename"]


def set_job_progress(job_id: str, status: str, progress: int, message: str = "") -> None:
    """Write job progress to Redis (for polling)."""
    r = _redis_client()
    r.setex(
        f"spir:progress:{job_id}",
        cfg.result_ttl_seconds,
        json.dumps({"status": status, "progress": progress, "message": message}),
    )


def get_job_progress(job_id: str) -> dict | None:
    r = _redis_client()
    raw = r.get(f"spir:progress:{job_id}")
    return json.loads(raw) if raw else None


# ── The extraction pipeline (shared by inline and background paths) ───────────

def run_extraction_pipeline(file_bytes: bytes, original_filename: str) -> dict:
    """
    Full extraction pipeline.  Returns the same dict structure as the
    old Flask /extract route so the API layer stays backward-compatible.

    Raises on any extraction error — callers decide how to handle.
    """
    wb     = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    result = spir_dispatcher(wb)
    xlsx   = build_xlsx(result["rows"], result["spir_no"])
    dups   = analyse_duplicates(result["rows"])

    file_id  = str(uuid.uuid4())
    fname    = (result["spir_no"] or "SPIR").replace(" ", "") + "_SPIR_Extraction.xlsx"

    # Store generated file in Redis
    store_result(file_id, xlsx, fname)

    return {
        "format":         result["format"],
        "spir_no":        result["spir_no"],
        "equipment":      result["equipment"],
        "manufacturer":   result["manufacturer"],
        "supplier":       result["supplier"],
        "spir_type":      result["spir_type"],
        "eqpt_qty":       result["eqpt_qty"],
        "spare_items":    result["spare_items"],
        "total_tags":     result["total_tags"],
        "annexure_count": result["annexure_count"],
        "annexure_stats": result["annexure_stats"],
        "dup1_count":     dups["dup1_count"],
        "sap_count":      dups["sap_count"],
        "total_rows":     len(result["rows"]),
        "preview_cols":   OUTPUT_COLS,
        "preview_rows":   result["rows"][:cfg.preview_row_count],
        "file_id":        file_id,
        "filename":       fname,
    }


# ── Celery task ───────────────────────────────────────────────────────────────

@celery_app.task(bind=True, name="spir.process_file")
def process_spir_file(self, file_bytes_hex: str, original_filename: str) -> dict:
    """
    Background task: receive file bytes (hex-encoded for JSON serialisation),
    run the full pipeline, store results, return summary dict.

    Progress is also written to Redis so the polling endpoint can report it.
    """
    job_id = self.request.id

    try:
        set_job_progress(job_id, "processing", 10, "Loading workbook…")
        file_bytes = bytes.fromhex(file_bytes_hex)

        set_job_progress(job_id, "processing", 40, "Detecting format and extracting…")
        result_dict = run_extraction_pipeline(file_bytes, original_filename)

        set_job_progress(job_id, "done", 100, "Complete")
        return result_dict

    except Exception as exc:
        tb = traceback.format_exc()
        log.error("Background task failed: %s\n%s", exc, tb)
        set_job_progress(job_id, "failed", 0, str(exc))
        raise          # Celery records the failure in its backend
