# app/main.py
# ============================================================
# LEAN-BRIDGE FASTAPI BRIDGE — MAIN APPLICATION
# Entry point. Assembles all routers, middleware, and startup
# events into the FastAPI application instance.
#
# Start with:
#   uvicorn app.main:app --host 0.0.0.0 --port 8000
# Production (behind nginx):
#   uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 1
# ============================================================

import logging
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers import webhooks, admin, health
from app.middleware.logging import RequestLoggingMiddleware

# ── Logging Setup ─────────────────────────────────────────────

settings = get_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # In production add: logging.FileHandler("logs/leanbridge.log")
    ]
)

logger = logging.getLogger(__name__)


# ── Lifespan (startup / shutdown) ────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Code here runs ONCE at startup before accepting requests,
    and again at shutdown.
    """
    logger.info("=" * 60)
    logger.info("LEAN-BRIDGE FASTAPI BRIDGE STARTING")
    logger.info("Environment: %s", settings.environment)
    logger.info("=" * 60)

    # Verify Supabase connection at startup
    try:
        from app.services.supabase_service import get_supabase
        supabase = get_supabase()
        supabase.table("system_flags").select("key").limit(1).execute()
        logger.info("✓ Supabase connection verified")
    except Exception as e:
        logger.error("✗ Supabase connection FAILED: %s", str(e))
        logger.error("Bridge will start but signal processing will fail until DB is available")

    # Verify Redis connection at startup
    try:
        import redis
        r = redis.from_url(settings.redis_url, socket_connect_timeout=3)
        r.ping()
        logger.info("✓ Redis connection verified")
    except Exception as e:
        logger.error("✗ Redis connection FAILED: %s", str(e))
        logger.error("Celery tasks cannot be dispatched until Redis is available")

    logger.info("Bridge ready. Accepting signals on /webhook/signal")

    yield  # Application runs here

    logger.info("Lean-Bridge FastAPI bridge shutting down.")


# ── FastAPI App ───────────────────────────────────────────────

app = FastAPI(
    title="Lean-Bridge Copy Trading Bridge",
    description=(
        "VPS-side signal intake bridge for the Lean-Bridge copy trading platform. "
        "Receives HMAC-signed trade signals from the Mentor MT5 EA, validates them, "
        "and dispatches Celery tasks to the worker fleet."
    ),
    version="2.0.0",
    lifespan=lifespan,
    # Disable docs in production — no need to expose API structure
    docs_url="/docs" if settings.environment != "production" else None,
    redoc_url=None,
)


# ── Middleware ────────────────────────────────────────────────

# Request logging — every request gets logged with timing
app.add_middleware(RequestLoggingMiddleware)

# CORS — the bridge is VPS-only, not called from browsers directly
# Only allow localhost in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost"] if settings.environment == "production" else ["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Global Exception Handler ──────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Catch-all for unhandled exceptions.
    Logs the error and returns a clean 500 response.
    Never exposes internal error details in production.
    """
    logger.error("Unhandled exception on %s: %s", request.url.path, str(exc), exc_info=True)

    detail = str(exc) if settings.environment != "production" else "Internal server error"

    return JSONResponse(
        status_code=500,
        content={"error": "internal_server_error", "detail": detail}
    )


# ── Routers ───────────────────────────────────────────────────

app.include_router(webhooks.router)
app.include_router(admin.router)
app.include_router(health.router)


# ── Root ──────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def root():
    return {
        "service": "lean-bridge-fastapi",
        "version": "2.0.0",
        "status":  "online"
    }
