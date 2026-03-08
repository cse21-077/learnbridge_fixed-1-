# app/routers/health.py
# ============================================================
# LEAN-BRIDGE — HEALTH CHECK ENDPOINT
# GET /health — returns status of all platform components.
# Used by uptime monitors and the super admin dashboard.
# ============================================================

import logging
import redis
import psutil
from fastapi import APIRouter
from app.models.payloads import HealthResponse, HealthComponent
from app.config import get_settings

logger   = logging.getLogger(__name__)
router   = APIRouter(tags=["Health"])
settings = get_settings()

VERSION = "2.0.0"


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Platform health check — all components"
)
async def health_check() -> HealthResponse:
    """
    Returns the health status of every platform component.
    Used by uptime monitors (UptimeRobot, Hetzner monitoring).
    Also displayed in super admin dashboard.

    Response codes:
    - 200: All components OK or degraded (platform still operational)
    - 503: Critical component down (FastAPI won't return this — proxy level)
    """
    components = {}
    overall    = "ok"

    # ── Redis ─────────────────────────────────────────────────
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=2)
        r.ping()
        components["redis"] = HealthComponent(status="ok")
    except Exception as e:
        components["redis"] = HealthComponent(status="down", detail=str(e))
        overall = "degraded"
        logger.error("Health check: Redis DOWN — %s", str(e))

    # ── Supabase ──────────────────────────────────────────────
    try:
        from app.services.supabase_service import get_supabase
        supabase = get_supabase()
        # Lightweight query — just check connectivity
        supabase.table("system_flags").select("key").limit(1).execute()
        components["supabase"] = HealthComponent(status="ok")
    except Exception as e:
        components["supabase"] = HealthComponent(status="down", detail=str(e))
        overall = "degraded"
        logger.error("Health check: Supabase DOWN — %s", str(e))

    # ── Celery Workers ────────────────────────────────────────
    try:
        from app.services.celery_app import celery_app
        inspect = celery_app.control.inspect(timeout=3)
        active  = inspect.active()

        if active is None:
            components["celery_workers"] = HealthComponent(
                status="degraded",
                detail="No workers responded — may be starting up"
            )
            overall = "degraded"
        else:
            worker_count = len(active)
            components["celery_workers"] = HealthComponent(
                status="ok",
                detail=f"{worker_count} worker(s) active"
            )
    except Exception as e:
        components["celery_workers"] = HealthComponent(
            status="down",
            detail=str(e)
        )
        overall = "degraded"

    # ── VPS System Resources ──────────────────────────────────
    try:
        cpu_pct  = psutil.cpu_percent(interval=0.1)
        ram      = psutil.virtual_memory()
        disk     = psutil.disk_usage("C:\\") if settings.environment == "production" \
                   else psutil.disk_usage("/")

        ram_used_gb  = ram.used / (1024 ** 3)
        ram_total_gb = ram.total / (1024 ** 3)
        disk_used_gb = disk.used / (1024 ** 3)

        # Warn if resources are critically high
        status = "ok"
        detail = (
            f"CPU: {cpu_pct:.1f}% | "
            f"RAM: {ram_used_gb:.1f}/{ram_total_gb:.1f}GB ({ram.percent:.0f}%) | "
            f"Disk: {disk_used_gb:.0f}GB used ({disk.percent:.0f}%)"
        )

        if ram.percent > 90 or disk.percent > 85 or cpu_pct > 90:
            status = "degraded"
            overall = "degraded"

        components["vps_resources"] = HealthComponent(status=status, detail=detail)

    except Exception as e:
        components["vps_resources"] = HealthComponent(
            status="degraded",
            detail=f"Could not read system resources: {str(e)}"
        )

    # ── Mentor Heartbeat ──────────────────────────────────────
    try:
        from app.services.supabase_service import get_supabase
        supabase = get_supabase()
        result   = (
            supabase.table("mentor_heartbeat")
            .select("status, last_seen")
            .limit(1)
            .execute()
        )

        if result.data:
            hb     = result.data[0]
            status = hb.get("status", "UNKNOWN")
            detail = f"Status: {status} | Last seen: {hb.get('last_seen', 'never')}"
            components["mentor_heartbeat"] = HealthComponent(
                status="ok" if status == "ONLINE" else "degraded",
                detail=detail
            )
        else:
            components["mentor_heartbeat"] = HealthComponent(
                status="degraded",
                detail="No heartbeat record found"
            )
    except Exception as e:
        components["mentor_heartbeat"] = HealthComponent(
            status="degraded",
            detail=str(e)
        )

    return HealthResponse(
        status=overall,
        version=VERSION,
        environment=settings.environment,
        components=components
    )
