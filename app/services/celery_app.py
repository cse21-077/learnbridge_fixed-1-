# app/services/celery_app.py
# ============================================================
# LEAN-BRIDGE — CELERY APPLICATION (OPTIMIZED v2.0)
# Task queue configuration, worker pre-warming, and task definitions.
# OPTIMIZATION: execute_trade_batch now uses broadcast endpoint
# ============================================================

import logging
import os
from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from app.services.supabase_service import get_supabase
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# ── Celery App ────────────────────────────────────────────────

celery_app = Celery(
    "leanbridge",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.services.celery_app"]
)

celery_app.conf.update(
    # Serialisation
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",

    # Timezone
    timezone="UTC",
    enable_utc=True,

    # Reliability
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,

    # Retries
    task_max_retries=settings.max_retries,
    task_default_retry_delay=settings.retry_countdown_seconds,

    # Results expire after 24 hours
    result_expires=86400,

    # Beat schedule
    beat_schedule={
        "check-subscription-expiry": {
            "task":     "app.services.celery_app.check_subscription_expiry",
            "schedule": 86400,
            "options":  {"expires": 3600}
        },
        "check-heartbeat-status": {
            "task":     "app.services.celery_app.check_heartbeat_status",
            "schedule": 60,
            "options":  {"expires": 30}
        }
    }
)


# ── Worker Pre-Warming ────────────────────────────────────────

_worker_terminal_paths: dict[str, str] = {}
_worker_id: int = 0


@worker_process_init.connect
def init_worker_state(**kwargs):
    """Runs once when each Celery worker process starts."""
    global _worker_id, _worker_terminal_paths

    _worker_id = int(os.environ.get("WORKER_INDEX", 0))
    logger.info("Worker %d initialised. Pre-loading terminal path cache...", _worker_id)

    try:
        from app.services.supabase_service import get_supabase
        supabase = get_supabase()

        result = (
            supabase.table("student_accounts")
            .select("user_id, folder_path, mt5_login")
            .eq("is_active", True)
            .not_.is_("folder_path", "null")
            .execute()
        )

        if result.data:
            _worker_terminal_paths = {
                row["user_id"]: row["folder_path"]
                for row in result.data
            }
            logger.info("Worker %d: cached %d terminal paths", _worker_id, len(_worker_terminal_paths))

    except Exception as e:
        logger.error("Worker %d: failed to pre-load terminal paths: %s", _worker_id, str(e))


@worker_process_shutdown.connect
def cleanup_worker(**kwargs):
    """Clean shutdown."""
    logger.info("Worker %d shutting down cleanly.", _worker_id)


def get_terminal_path(user_id: str) -> str | None:
    """Return cached terminal folder path for a student."""
    if user_id in _worker_terminal_paths:
        return _worker_terminal_paths[user_id]

    try:
        from app.services.supabase_service import get_supabase
        supabase = get_supabase()
        result = (
            supabase.table("student_accounts")
            .select("folder_path")
            .eq("user_id", user_id)
            .single()
            .execute()
        )
        if result.data and result.data.get("folder_path"):
            path = result.data["folder_path"]
            _worker_terminal_paths[user_id] = path
            return path
    except Exception as e:
        logger.error("Failed to fetch terminal path for %s: %s", user_id, str(e))

    return None


# ── Celery Tasks (OPTIMIZED) ──────────────────────────────────

@celery_app.task(
    bind=True,
    name="execute_trade_batch",
    max_retries=settings.max_retries,
    default_retry_delay=settings.retry_countdown_seconds,
    acks_late=True
)
def execute_trade_batch(self, students: list[dict], signal: dict) -> dict:
    """
    OPTIMIZED: Single broadcast instead of per-student loop.
    Reduces 53 HTTP requests → 1 broadcast request.
    """
    from app.services.trade_executor import execute_batch_broadcast, execute_for_student
    from app.config import get_settings
    
    settings = get_settings()
    
    # Check if broadcast is enabled (can be toggled via env)
    use_broadcast = os.environ.get("USE_BROADCAST", "true").lower() == "true"
    
    if use_broadcast:
        logger.info("📡 Using BROADCAST mode for %d students", len(students))
        try:
            result = execute_batch_broadcast(students, signal)
            
            return {
                "success": result.get("successes", 0),
                "failed": result.get("failures", 0),
                "skipped": 0,
                "total": result.get("total", 0),
                "mode": "broadcast"
            }
        except Exception as e:
            logger.error("Broadcast failed, falling back to per-student: %s", str(e))
            # Fall through to legacy mode
    
    # Legacy mode (fallback or if broadcast disabled)
    logger.info("Using LEGACY per-student mode for %d students", len(students))
    results = {"success": 0, "failed": 0, "skipped": 0, "retried": 0, "mode": "legacy"}
    
    for student in students:
        try:
            outcome = execute_for_student(student, signal)
            results[outcome] = results.get(outcome, 0) + 1
        except Exception as e:
            logger.error("Unhandled error for student %s: %s", student.get("user_id"), str(e))
            results["failed"] += 1
            
            if self.request.retries >= self.max_retries:
                _write_dead_letter(self.request.id, student, signal, str(e))
    
    logger.info("Batch complete. Success: %d, Failed: %d, Skipped: %d", 
                results["success"], results["failed"], results["skipped"])
    return results


@celery_app.task(
    bind=True,
    name="execute_close_batch",
    max_retries=settings.max_retries,
    default_retry_delay=settings.retry_countdown_seconds,
    acks_late=True
)
def execute_close_batch(self, students: list[dict], close_data: dict) -> dict:
    """
    Close all open positions for a batch of students.
    """
    from app.services.trade_executor import close_for_student

    results = {"closed": 0, "failed": 0, "skipped": 0}

    for student in students:
        try:
            outcome = close_for_student(student, close_data)
            results[outcome] = results.get(outcome, 0) + 1
        except Exception as e:
            logger.error("Close error for student %s: %s", student.get("user_id"), str(e))
            results["failed"] += 1

    return results


@celery_app.task(name="check_subscription_expiry")
def check_subscription_expiry() -> dict:
    """Celery Beat task — runs daily at 00:00 UTC."""
    from app.services.supabase_service import get_supabase

    supabase = get_supabase()

    try:
        result = supabase.rpc("process_subscription_expiry").execute()
        data = result.data[0] if result.data else {}
        expired_count = data.get("expired_count", 0)

        logger.info("Subscription expiry check complete. Expired: %d", expired_count)
        return {"expired_count": expired_count}

    except Exception as e:
        logger.error("Subscription expiry check failed: %s", str(e))
        return {"error": str(e)}


@celery_app.task(name="check_heartbeat_status")
def check_heartbeat_status() -> dict:
    """Celery Beat task — runs every 60 seconds."""
    from app.services.supabase_service import get_supabase, mark_heartbeat_missed
    from datetime import datetime, timezone

    supabase = get_supabase()
    settings = get_settings()

    try:
        result = (
            supabase.table("mentor_heartbeat")
            .select("trader_id, last_seen, status")
            .execute()
        )

        updated = []
        now = datetime.now(timezone.utc)

        for row in (result.data or []):
            last_seen = datetime.fromisoformat(row["last_seen"].replace("Z", "+00:00"))
            seconds_since = (now - last_seen).total_seconds()

            if seconds_since >= settings.heartbeat_warning_seconds:
                updated_row = mark_heartbeat_missed(row["trader_id"])
                updated.append(updated_row)

        return {"checked": len(result.data or []), "updated": len(updated)}

    except Exception as e:
        logger.error("Heartbeat check failed: %s", str(e))
        return {"error": str(e)}


# ── Dead Letter Queue ─────────────────────────────────────────

def _write_dead_letter(task_id: str, student: dict, signal: dict, error: str) -> None:
    """Write a failed task to dead_letter_queue for admin review."""
    supabase = get_supabase()

    try:
        supabase.table("dead_letter_queue").insert({
            "task_id":        task_id,
            "user_id":        student.get("user_id"),
            "signal_payload": signal,
            "error":          error,
            "retry_count":    settings.max_retries,
        }).execute()
        logger.warning("Task %s written to dead_letter_queue for student %s", 
                       task_id, student.get("user_id"))
    except Exception as e:
        logger.error("Failed to write dead letter: %s", str(e))


# ── Batch Utility ─────────────────────────────────────────────

def chunk_list(lst: list, size: int) -> list[list]:
    """Split a list into chunks of given size."""
    return [lst[i:i + size] for i in range(0, len(lst), size)]