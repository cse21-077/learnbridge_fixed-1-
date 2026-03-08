# app/routers/admin.py
# ============================================================
# LEAN-BRIDGE — ADMIN ENDPOINTS
# Super admin only. All routes require super_admin JWT.
# These use the service_role key server-side.
# ============================================================

import logging
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
from app.services.supabase_service import (
    get_supabase,
    set_system_flag,
    get_system_flag,
    log_system_event
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["Admin"])


# ── Auth Helper ───────────────────────────────────────────────

def verify_super_admin(authorization: str = Header(...)) -> str:
    """
    Verify the JWT is from a super_admin user.
    In production: decode JWT and check role claim.
    Returns user_id on success, raises 403 on failure.
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = authorization.replace("Bearer ", "")

    try:
        supabase = get_supabase()
        user_response = supabase.auth.get_user(token)
        user = user_response.user

        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")

        # Check role in users table
        role_result = (
            supabase.table("users")
            .select("role")
            .eq("id", user.id)
            .single()
            .execute()
        )

        if not role_result.data or role_result.data.get("role") != "super_admin":
            raise HTTPException(status_code=403, detail="Super admin access required")

        return user.id

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Auth verification error: %s", str(e))
        raise HTTPException(status_code=401, detail="Token verification failed")


# ── POST /admin/emergency-close ───────────────────────────────

class EmergencyCloseRequest(BaseModel):
    confirm: bool  # Must be True to execute — prevents accidental triggers
    reason:  Optional[str] = None


@router.post(
    "/emergency-close",
    summary="EMERGENCY: Close all positions across all student accounts"
)
async def emergency_close_all(
    body: EmergencyCloseRequest,
    authorization: str = Header(...)
) -> dict:
    """
    Nuclear option — closes ALL open positions on ALL student accounts.
    Sets emergency_close_all flag → workers read this before every trade.
    Also dispatches immediate close tasks to all active students.

    Requires:
    - Valid super_admin JWT
    - body.confirm = true (prevents accidental triggers)
    """
    actor_id = verify_super_admin(authorization)

    if not body.confirm:
        raise HTTPException(
            status_code=400,
            detail="Set confirm=true to execute emergency close. This cannot be undone."
        )

    logger.critical(
        "EMERGENCY CLOSE ALL triggered by super_admin %s. Reason: %s",
        actor_id, body.reason or "No reason provided"
    )

    # Set the kill switch flag — workers read this before every trade
    set_system_flag("emergency_close_all", "true", actor_id=actor_id)

    # Also dispatch close tasks immediately for all active students
    from app.services.supabase_service import get_all_active_students
    from app.services.celery_app import execute_close_batch, chunk_list
    from app.config import get_settings

    settings = get_settings()

    try:
        students = get_all_active_students()
        batches  = chunk_list(students, settings.batch_size)

        for batch in batches:
            execute_close_batch.delay(batch, {"emergency": True, "symbol": "ALL"})

        log_system_event(
            event_type="emergency_close_all",
            actor_id=actor_id,
            payload={"reason": body.reason, "students_affected": len(students)},
            severity="critical"
        )

        return {
            "status":            "emergency_close_dispatched",
            "students_affected": len(students),
            "batches_dispatched": len(batches),
            "warning":           "emergency_close_all flag is now TRUE. New signals are blocked."
        }

    except Exception as e:
        logger.error("Emergency close failed: %s", str(e))
        raise HTTPException(status_code=500, detail=f"Emergency close failed: {str(e)}")


# ── POST /admin/resume ────────────────────────────────────────

@router.post("/resume", summary="Lift emergency close — resume normal trading")
async def resume_trading(authorization: str = Header(...)) -> dict:
    """
    Clears the emergency_close_all flag.
    New signals will be processed again after this.
    """
    actor_id = verify_super_admin(authorization)

    set_system_flag("emergency_close_all", "false", actor_id=actor_id)
    set_system_flag("platform_paused", "false", actor_id=actor_id)

    log_system_event(
        event_type="trading_resumed",
        actor_id=actor_id,
        payload={},
        severity="info"
    )

    logger.info("Trading resumed by super_admin %s", actor_id)

    return {"status": "trading_resumed", "emergency_close_all": "false"}


# ── POST /admin/pause ─────────────────────────────────────────

@router.post("/pause", summary="Pause all new signal processing without closing positions")
async def pause_platform(authorization: str = Header(...)) -> dict:
    """
    Soft pause — stops new signals being processed.
    Does NOT close existing open positions.
    Use emergency-close for that.
    """
    actor_id = verify_super_admin(authorization)

    set_system_flag("platform_paused", "true", actor_id=actor_id)

    log_system_event(
        event_type="platform_paused",
        actor_id=actor_id,
        payload={},
        severity="warning"
    )

    return {"status": "platform_paused"}


# ── GET /admin/status ─────────────────────────────────────────

@router.get("/status", summary="Current system flags and platform state")
async def get_platform_status(authorization: str = Header(...)) -> dict:
    """Returns current state of all system flags."""
    verify_super_admin(authorization)

    return {
        "emergency_close_all": get_system_flag("emergency_close_all"),
        "platform_paused":     get_system_flag("platform_paused")
    }


# ── POST /admin/requeue-dlq ───────────────────────────────────

class RequeueRequest(BaseModel):
    task_ids: list[str]  # List of dead_letter_queue IDs to requeue


@router.post("/requeue-dlq", summary="Requeue failed tasks from dead letter queue")
async def requeue_dead_letters(
    body: RequeueRequest,
    authorization: str = Header(...)
) -> dict:
    """
    Takes tasks from dead_letter_queue and re-dispatches them to Celery.
    Super admin reviews failed tasks and decides which to retry.
    """
    actor_id = verify_super_admin(authorization)
    supabase  = get_supabase()

    requeued  = []
    failed    = []

    for task_id in body.task_ids:
        try:
            # Fetch the original task
            result = (
                supabase.table("dead_letter_queue")
                .select("*")
                .eq("id", task_id)
                .eq("resolved", False)
                .single()
                .execute()
            )

            if not result.data:
                failed.append({"task_id": task_id, "reason": "not_found_or_already_resolved"})
                continue

            dlq_row = result.data

            # Re-dispatch to Celery
            from app.services.celery_app import execute_trade_batch

            # Wrap in single-student batch
            execute_trade_batch.delay(
                [{"user_id": dlq_row["user_id"]}],
                dlq_row["signal_payload"]
            )

            # Mark as resolved
            supabase.table("dead_letter_queue").update({
                "resolved":    True,
                "resolved_by": actor_id,
                "resolved_at": "now()"
            }).eq("id", task_id).execute()

            requeued.append(task_id)

        except Exception as e:
            logger.error("Failed to requeue task %s: %s", task_id, str(e))
            failed.append({"task_id": task_id, "reason": str(e)})

    log_system_event(
        event_type="dlq_requeue",
        actor_id=actor_id,
        payload={"requeued": requeued, "failed": failed},
        severity="info"
    )

    return {
        "requeued_count": len(requeued),
        "failed_count":   len(failed),
        "requeued":       requeued,
        "failed":         failed
    }
