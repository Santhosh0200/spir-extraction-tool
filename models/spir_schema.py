"""
models/spir_schema.py
Pydantic models that define every data contract in the system:
  - API request / response shapes
  - Internal extraction result shape
  - Celery task status shape
"""
from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, Field


# ── Extraction result produced by the core engine ─────────────────────────────

class ExtractionResult(BaseModel):
    """What extract_spir() returns — the canonical internal data object."""
    format:          str
    spir_no:         str
    equipment:       str
    manufacturer:    str
    supplier:        str
    spir_type:       Optional[str]
    eqpt_qty:        int
    spare_items:     int
    total_tags:      int
    annexure_count:  int
    annexure_stats:  dict[str, int]    # sheet_name → tag count
    rows:            list[list[Any]]   # each row = OUTPUT_COLS-length list


# ── API response models ────────────────────────────────────────────────────────

class ExtractResponse(BaseModel):
    """
    Returned immediately after a file is submitted for extraction.
    If background=False (default), processing is already done when this arrives.
    If background=True, poll /status/{job_id} until status == 'done'.
    """
    job_id:          str
    status:          str              # 'done' | 'queued' | 'processing'
    background:      bool = False

    # Populated when status == 'done'
    format:          Optional[str]  = None
    spir_no:         Optional[str]  = None
    equipment:       Optional[str]  = None
    manufacturer:    Optional[str]  = None
    supplier:        Optional[str]  = None
    spir_type:       Optional[str]  = None
    eqpt_qty:        Optional[int]  = None
    spare_items:     Optional[int]  = None
    total_tags:      Optional[int]  = None
    annexure_count:  Optional[int]  = None
    annexure_stats:  Optional[dict[str, int]] = None
    dup1_count:      Optional[int]  = None
    sap_count:       Optional[int]  = None
    total_rows:      Optional[int]  = None
    preview_cols:    Optional[list[str]]       = None
    preview_rows:    Optional[list[list[Any]]] = None
    file_id:         Optional[str]  = None
    filename:        Optional[str]  = None


class JobStatusResponse(BaseModel):
    """Returned by GET /status/{job_id} for polling background jobs."""
    job_id:    str
    status:    str              # 'queued' | 'processing' | 'done' | 'failed'
    progress:  int  = 0        # 0-100
    message:   str  = ""
    result:    Optional[ExtractResponse] = None
    error:     Optional[str]   = None


class HealthResponse(BaseModel):
    status:  str
    version: str
    redis:   str    # 'ok' | 'unavailable'
    workers: str    # 'ok' | 'unavailable'


class ErrorResponse(BaseModel):
    error:   str
    detail:  Optional[str] = None
    trace:   Optional[str] = None
