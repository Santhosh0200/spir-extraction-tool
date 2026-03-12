"""
app/main.py
────────────
FastAPI application factory.

Creates and configures the FastAPI app:
  • Structured JSON logging (structlog)
  • CORS middleware
  • Request ID middleware (every request gets a UUID for tracing)
  • Static file serving (serves the existing HTML UI at '/')
  • Router mounting
  • Startup / shutdown lifecycle hooks

Run locally:
    uvicorn app.main:app --reload --port 8000

Production:
    uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
"""
from __future__ import annotations
import logging
import os
import time
import uuid
from pathlib import Path

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.routes import router

# ── Logging setup ─────────────────────────────────────────────────────────────

def _configure_logging(level: str = "INFO") -> None:
    """Configure structlog for structured JSON output."""
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        context_class    = dict,
        logger_factory   = structlog.stdlib.LoggerFactory(),
        wrapper_class    = structlog.stdlib.BoundLogger,
        cache_logger_on_first_use = True,
    )
    logging.basicConfig(
        format  = "%(message)s",
        level   = getattr(logging, level.upper(), logging.INFO),
    )


# ── Application factory ───────────────────────────────────────────────────────

def create_app() -> FastAPI:
    cfg = get_settings()
    _configure_logging(cfg.log_level)

    log = structlog.get_logger(__name__)

    app = FastAPI(
        title       = cfg.app_name,
        version     = cfg.app_version,
        description = (
            "Enterprise-grade SPIR Extraction API. "
            "Supports FORMAT 1–5 SPIR Excel files with automatic format detection, "
            "dynamic column mapping, annexure parsing, and duplicate detection."
        ),
        docs_url    = "/api/docs",
        redoc_url   = "/api/redoc",
        openapi_url = "/api/openapi.json",
    )

    # ── CORS ──────────────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins     = cfg.allowed_origins,
        allow_credentials = True,
        allow_methods     = ["*"],
        allow_headers     = ["*"],
    )

    # ── Request ID + timing middleware ────────────────────────────────────────
    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        request_id = str(uuid.uuid4())[:8]
        request.state.request_id = request_id
        start = time.perf_counter()

        response = await call_next(request)

        elapsed = (time.perf_counter() - start) * 1000
        response.headers["X-Request-ID"]    = request_id
        response.headers["X-Response-Time"] = f"{elapsed:.1f}ms"

        log.info(
            "http_request",
            method     = request.method,
            path       = request.url.path,
            status     = response.status_code,
            elapsed_ms = round(elapsed, 1),
            request_id = request_id,
        )
        return response

    # ── Global exception handler ──────────────────────────────────────────────
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        import traceback
        log.error(
            "unhandled_exception",
            path  = request.url.path,
            error = str(exc),
            trace = traceback.format_exc(),
        )
        return JSONResponse(
            status_code = 500,
            content     = {"error": "Internal server error", "detail": str(exc)},
        )

    # ── Static files + UI ─────────────────────────────────────────────────────
    static_dir = Path(__file__).parent.parent / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

        @app.get("/", include_in_schema=False)
        async def serve_ui():
            index = static_dir / "index.html"
            if index.exists():
                return FileResponse(str(index))
            return JSONResponse({"message": "SPIR Extraction API", "docs": "/api/docs"})
    else:
        @app.get("/", include_in_schema=False)
        async def root():
            return JSONResponse({
                "message": "SPIR Extraction API",
                "version": cfg.app_version,
                "docs":    "/api/docs",
            })

    # ── API routes ────────────────────────────────────────────────────────────
    app.include_router(router, tags=["SPIR Extraction"])

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    @app.on_event("startup")
    async def startup():
        os.makedirs(cfg.upload_dir, exist_ok=True)
        log.info(
            "startup",
            app     = cfg.app_name,
            version = cfg.app_version,
            host    = cfg.host,
            port    = cfg.port,
        )

    @app.on_event("shutdown")
    async def shutdown():
        log.info("shutdown", app=cfg.app_name)

    return app


# ── Module-level app instance (used by uvicorn) ───────────────────────────────
app = create_app()
