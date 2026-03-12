# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps for openpyxl (lxml, et_xmlfile)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 2: production image ─────────────────────────────────────────────────
FROM python:3.11-slim AS production

# Non-root user for security
RUN useradd -m -u 1001 spir
WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY --chown=spir:spir . /app/

# Runtime directories
RUN mkdir -p /tmp/spir_uploads /app/logs \
 && chown -R spir:spir /tmp/spir_uploads /app/logs

USER spir

EXPOSE 8000

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

# Default command: run FastAPI with uvicorn
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "4", \
     "--access-log"]


# ── Stage 3: Celery worker image (same base, different CMD) ───────────────────
FROM production AS worker

CMD ["celery", "-A", "app.worker.celery_app", "worker", \
     "--loglevel=info", \
     "--concurrency=2", \
     "--queues=celery"]
