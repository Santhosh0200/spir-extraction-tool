"""
app/routes.py
──────────────
All FastAPI route handlers.

Endpoints:
  POST /extract              — upload + extract (inline for small, background for large)
  GET  /status/{job_id}      — poll background job status
  GET  /download/{file_id}   — stream the generated XLSX
  GET  /inspect              — workbook sheet summary (diagnostics / debug)
  GET  /health               — liveness + dependency check

Routing rules:
  • Files ≤  BG_THRESHOLD_MB  →  processed inline (synchronous, <5 s typical)
  • Files >  BG_THRESHOLD_MB  →  dispatched to Celery (async, poll /status)
"""
from __future__ import annotations
import io
import json
import logging
import traceback

import redis
import openpyxl
from fastapi import APIRouter, File, UploadFile, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse

from app.config import get_settings, Settings
from app.worker import (
    run_extraction_pipeline,
    process_spir_file,
    retrieve_result,
    get_job_progress,
    set_job_progress,
    celery_app,
)
from extraction.spir_detector import validate_file, detect, ValidationError
from extraction.annexure_parser import summarise_workbook
from models.spir_schema import (
    ExtractResponse, JobStatusResponse, HealthResponse, ErrorResponse
)

log = logging.getLogger(__name__)
router = APIRouter()

# Files larger than this are dispatched to Celery instead of processed inline
BG_THRESHOLD_MB = 5


def _settings() -> Settings:
    return get_settings()


# ── POST /extract ─────────────────────────────────────────────────────────────

@router.post(
    "/extract",
    response_model      = ExtractResponse,
    summary             = "Upload and extract a SPIR file",
    response_model_exclude_none = True,
)
async def extract(
    file: UploadFile = File(..., description="SPIR Excel file (.xlsx / .xlsm)"),
    cfg:  Settings   = Depends(_settings),
):
    """
    Upload a SPIR Excel file and receive extracted data.

    For files under 5 MB the response contains the full extraction result
    immediately (status='done').

    For larger files the file is queued for background processing and the
    response contains only a job_id (status='queued').  Poll GET /status/{job_id}
    to retrieve the result when ready.
    """
    raw = await file.read()

    # ── Validate ──────────────────────────────────────────────────────────────
    try:
        validate_file(file.filename or "upload.xlsx", raw, max_mb=cfg.max_file_size_mb)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    size_mb = len(raw) / (1024 * 1024)
    log.info("Received file '%s'  %.2f MB", file.filename, size_mb)

    # ── Decide: inline vs background ──────────────────────────────────────────
    if size_mb > BG_THRESHOLD_MB:
        return _dispatch_background(raw, file.filename or "upload.xlsx")

    return _extract_inline(raw, file.filename or "upload.xlsx")


def _extract_inline(raw: bytes, filename: str) -> ExtractResponse:
    """Run extraction synchronously and return a complete ExtractResponse."""
    try:
        d = run_extraction_pipeline(raw, filename)
        import uuid
        job_id = str(uuid.uuid4())
        return ExtractResponse(
            job_id          = job_id,
            status          = "done",
            background      = False,
            **{k: d[k] for k in d if k != "preview_rows"},
            preview_rows    = [[_jsonify(v) for v in row] for row in d["preview_rows"]],
        )
    except Exception as exc:
        tb = traceback.format_exc()
        log.error("Inline extraction failed: %s\n%s", exc, tb)
        raise HTTPException(
            status_code = 500,
            detail      = {"error": str(exc), "trace": tb},
        )


def _dispatch_background(raw: bytes, filename: str) -> ExtractResponse:
    """Enqueue the file for background processing and return a queued response."""
    try:
        task = process_spir_file.delay(raw.hex(), filename)
        set_job_progress(task.id, "queued", 0, "Queued for processing")
        log.info("Job %s queued for '%s'", task.id, filename)
        return ExtractResponse(
            job_id     = task.id,
            status     = "queued",
            background = True,
        )
    except Exception as exc:
        log.error("Failed to enqueue task: %s", exc)
        raise HTTPException(status_code=503, detail="Background queue unavailable. Try again.")


def _jsonify(v):
    """Make a cell value JSON-safe."""
    if v is None:
        return None
    if isinstance(v, (int, float, bool, str)):
        return v
    return str(v)


# ── GET /status/{job_id} ──────────────────────────────────────────────────────

@router.get(
    "/status/{job_id}",
    response_model      = JobStatusResponse,
    summary             = "Poll background job status",
    response_model_exclude_none = True,
)
async def job_status(job_id: str):
    """
    Poll the status of a background extraction job.

    Returns one of:
      status='queued'     — waiting in the queue
      status='processing' — worker is actively extracting
      status='done'       — extraction complete; result embedded in response
      status='failed'     — extraction failed; error message in response
    """
    # Check Redis progress store first (fast path)
    prog = get_job_progress(job_id)

    if prog is None:
        # Try Celery backend
        task = celery_app.AsyncResult(job_id)
        if task.state == "PENDING":
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        prog = {
            "status":   task.state.lower(),
            "progress": 100 if task.state == "SUCCESS" else 0,
            "message":  "",
        }

    status   = prog.get("status", "unknown")
    progress = prog.get("progress", 0)
    message  = prog.get("message", "")

    if status == "done":
        # Fetch the full result from Celery backend
        task = celery_app.AsyncResult(job_id)
        if task.state == "SUCCESS":
            d = task.result
            result = ExtractResponse(
                job_id          = job_id,
                status          = "done",
                background      = True,
                **{k: d[k] for k in d if k != "preview_rows"},
                preview_rows    = [[_jsonify(v) for v in row] for row in d["preview_rows"]],
            )
            return JobStatusResponse(
                job_id   = job_id,
                status   = "done",
                progress = 100,
                message  = "Complete",
                result   = result,
            )

    if status == "failed":
        task = celery_app.AsyncResult(job_id)
        return JobStatusResponse(
            job_id   = job_id,
            status   = "failed",
            progress = 0,
            message  = message,
            error    = str(task.result) if task.result else message,
        )

    return JobStatusResponse(
        job_id   = job_id,
        status   = status,
        progress = progress,
        message  = message,
    )


# ── GET /download/{file_id} ───────────────────────────────────────────────────

@router.get(
    "/download/{file_id}",
    summary = "Download generated XLSX file",
)
async def download(file_id: str):
    """
    Download the generated XLSX extraction output.
    Files are retained in Redis for 1 hour after generation.
    """
    result = retrieve_result(file_id)
    if result is None:
        raise HTTPException(
            status_code = 404,
            detail      = "File not found or expired. Re-upload and extract again.",
        )

    xlsx_bytes, filename = result
    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers    = {"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── GET /inspect ──────────────────────────────────────────────────────────────

@router.post(
    "/inspect",
    summary = "Inspect workbook structure without full extraction",
)
async def inspect(
    file: UploadFile = File(...),
    cfg:  Settings   = Depends(_settings),
):
    """
    Upload a SPIR file and receive a structural summary:
      - Detected format
      - Sheet names and roles
      - Dynamic column mapping coverage per sheet

    Useful for debugging unknown or malformed SPIR files.
    """
    raw = await file.read()
    try:
        validate_file(file.filename or "upload.xlsx", raw, max_mb=cfg.max_file_size_mb)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    try:
        det_info = detect(raw)
        wb       = openpyxl.load_workbook(io.BytesIO(raw), data_only=True)
        sheet_summary = summarise_workbook(wb)
        wb.close()
        return {
            "filename":      file.filename,
            "size_mb":       round(len(raw) / 1024 / 1024, 3),
            "detected":      det_info,
            "sheets":        sheet_summary,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /health ───────────────────────────────────────────────────────────────

@router.get(
    "/health",
    response_model = HealthResponse,
    summary        = "System health check",
)
async def health(cfg: Settings = Depends(_settings)):
    """
    Liveness + dependency check.
    Returns status of Redis and Celery workers.
    """
    from app.config import get_settings
    settings = get_settings()

    # Check Redis
    redis_status = "ok"
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=2)
        r.ping()
    except Exception:
        redis_status = "unavailable"

    # Check Celery workers
    worker_status = "ok"
    try:
        inspect = celery_app.control.inspect(timeout=2.0)
        active  = inspect.active()
        if active is None:
            worker_status = "no workers responding"
    except Exception:
        worker_status = "unavailable"

    overall = "healthy" if redis_status == "ok" else "degraded"

    return HealthResponse(
        status  = overall,
        version = settings.app_version,
        redis   = redis_status,
        workers = worker_status,
    )
