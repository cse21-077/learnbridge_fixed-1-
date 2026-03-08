# app/services/celery_app.py
# ============================================================
# LEAN-BRIDGE — CELERY APPLICATION
# Task queue configuration, worker pre-warming, and task definitions.
# Workers use prefork pool (true OS processes) — required for MT5
# Python API which is not thread-safe.
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
    task_acks_late=True,          # Ack only after task completes — prevents lost tasks
    task_reject_on_worker_lost=True,  # Re-queue if worker dies mid-task
    worker_prefetch_multiplier=1, # Each worker takes one task at a time — fair distribution

    # Retries
    task_max_retries=settings.max_retries,
    task_default_retry_delay=settings.retry_countdown_seconds,

    # Results expire after 24 hours — we log to Supabase anyway
    result_expires=86400,

    # Beat schedule — subscription expiry check daily at midnight UTC
    beat_schedule={
        "check-subscription-expiry": {
            "task":     "app.services.celery_app.check_subscription_expiry",
            "schedule": 86400,  # Every 24 hours
            "options":  {"expires": 3600}
        },
        "check-heartbeat-status": {
            "task":     "app.services.celery_app.check_heartbeat_status",
            "schedule": 60,     # Every 60 seconds
            "options":  {"expires": 30}
        }
    }
)


# ── Worker Pre-Warming ────────────────────────────────────────
# MT5 terminal connections are expensive to initialise (~300ms each).
# Pre-warming loads terminal paths at worker startup so the worker
# knows which terminals it owns. Actual MT5 init still happens per
# trade but folder mapping is cached.

# Global worker state — set once at startup
_worker_terminal_paths: dict[str, str] = {}
_worker_id: int = 0


@worker_process_init.connect
def init_worker_state(**kwargs):
    """
    Runs once when each Celery worker process starts.
    Assigns this worker its shard of student terminals.
    """
    global _worker_id, _worker_terminal_paths

    # Worker index from environment (set by Supervisor/start script)
    _worker_id = int(os.environ.get("WORKER_INDEX", 0))

    logger.info(
        "Worker %d initialised. Pre-loading terminal path cache...",
        _worker_id
    )

    # Load terminal paths from Supabase for this worker's student shard
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
            logger.info(
                "Worker %d: cached %d terminal paths",
                _worker_id, len(_worker_terminal_paths)
            )

    except Exception as e:
        logger.error("Worker %d: failed to pre-load terminal paths: %s", _worker_id, str(e))
        # Non-fatal — workers will fetch paths per-task as fallback


@worker_process_shutdown.connect
def cleanup_worker(**kwargs):
    """
    Clean shutdown — log worker going offline.
    """
    logger.info("Worker %d shutting down cleanly.", _worker_id)


def get_terminal_path(user_id: str) -> str | None:
    """
    Return cached terminal folder path for a student.
    Falls back to Supabase query if not in cache.
    """
    if user_id in _worker_terminal_paths:
        return _worker_terminal_paths[user_id]

    # Cache miss — query Supabase
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
            _worker_terminal_paths[user_id] = path  # populate cache
            return path
    except Exception as e:
        logger.error("Failed to fetch terminal path for %s: %s", user_id, str(e))

    return None


# ── Celery Tasks ──────────────────────────────────────────────

@celery_app.task(
    bind=True,
    name="execute_trade_batch",
    max_retries=settings.max_retries,
    default_retry_delay=settings.retry_countdown_seconds,
    acks_late=True
)
def execute_trade_batch(self, students: list[dict], signal: dict) -> dict:
    """
    Core trade execution task.
    Each task handles one batch of up to BATCH_SIZE students.
    Runs in parallel across all available workers.

    Args:
        students: List of student dicts (account + risk profile data)
        signal:   Trade signal dict from Mentor EA

    Returns:
        Summary dict with success/fail/skip counts
    """
    from app.services.trade_executor import execute_for_student

    results = {"success": 0, "failed": 0, "skipped": 0, "retried": 0}

    for student in students:
        try:
            outcome = execute_for_student(student, signal)
            results[outcome] = results.get(outcome, 0) + 1
        except Exception as e:
            logger.error(
                "Unhandled error for student %s: %s",
                student.get("user_id"), str(e)
            )
            results["failed"] += 1

            # Write to dead_letter_queue if this was the final retry
            if self.request.retries >= self.max_retries:
                _write_dead_letter(self.request.id, student, signal, str(e))

    logger.info(
        "Batch complete. Success: %d, Failed: %d, Skipped: %d",
        results["success"], results["failed"], results["skipped"]
    )
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
    Close all open positions for a batch of students
    when the Mentor closes their trade.
    """
    from app.services.trade_executor import close_for_student

    results = {"closed": 0, "failed": 0, "skipped": 0}

    for student in students:
        try:
            outcome = close_for_student(student, close_data)
            results[outcome] = results.get(outcome, 0) + 1
        except Exception as e:
            logger.error(
                "Close error for student %s: %s",
                student.get("user_id"), str(e)
            )
            results["failed"] += 1

    return results


@celery_app.task(name="check_subscription_expiry")
def check_subscription_expiry() -> dict:
    """
    Celery Beat task — runs daily at 00:00 UTC.
    Calls the Supabase function that marks overdue subscriptions
    and deactivates their trading accounts.
    """
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
    """
    Celery Beat task — runs every 60 seconds.
    Checks each trader's last heartbeat timestamp and
    updates their status to WARNING or OFFLINE if stale.
    """
    from app.services.supabase_service import get_supabase, mark_heartbeat_missed
    from datetime import datetime, timezone

    supabase = get_supabase()
    settings = get_settings()

    try:
        # Get all traders with their last heartbeat
        result = (
            supabase.table("mentor_heartbeat")
            .select("trader_id, last_seen, status")
            .execute()
        )

        updated = []
        now = datetime.now(timezone.utc)

        for row in (result.data or []):
            last_seen = datetime.fromisoformat(
                row["last_seen"].replace("Z", "+00:00")
            )
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
    """
    Write a failed task to dead_letter_queue for admin review.
    Called when all retries are exhausted.
    """
    
    supabase = get_supabase()

    try:
        supabase.table("dead_letter_queue").insert({
            "task_id":        task_id,
            "user_id":        student.get("user_id"),
            "signal_payload": signal,
            "error":          error,
            "retry_count":    settings.max_retries,
        }).execute()
        logger.warning(
            "Task %s written to dead_letter_queue for student %s",
            task_id, student.get("user_id")
        )
    except Exception as e:
        logger.error("Failed to write dead letter: %s", str(e))


# ── Batch Utility ─────────────────────────────────────────────

def chunk_list(lst: list, size: int) -> list[list]:
    """Split a list into chunks of given size."""
    return [lst[i:i + size] for i in range(0, len(lst), size)]
