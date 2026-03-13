"""
app/config.py
Centralised configuration — all tunables in one place.
Values are read from environment variables with safe defaults.
"""
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Application ───────────────────────────────────────────────────────────
    app_name: str       = "SPIR Extraction Tool"
    app_version: str    = "9.0.0"
    debug: bool         = False
    log_level: str      = "INFO"

    # ── Server ────────────────────────────────────────────────────────────────
    host: str           = "0.0.0.0"
    port: int           = 8000
    workers: int        = 4           # uvicorn worker count for production

    # ── File Handling ─────────────────────────────────────────────────────────
    max_file_size_mb: int  = 100      # hard upload limit
    upload_dir: str        = "/tmp/spir_uploads"
    result_ttl_seconds: int = 3600    # how long to keep results in Redis

    # ── Redis / Celery ────────────────────────────────────────────────────────
    import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379")

    celery_broker: str = redis_url
    celery_backend: str = redis_url

    class Config:
        env_file = ".env"


def get_settings():
    return Settings()

def get_settings():
    return Settings()
    # ── CORS ──────────────────────────────────────────────────────────────────
    allowed_origins: list[str] = ["*"]   # tighten in production

    # ── Preview ───────────────────────────────────────────────────────────────
    preview_row_count: int = 12

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """Return a cached singleton — called once per process."""
    return Settings()
