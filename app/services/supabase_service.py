# app/services/supabase_service.py
# ============================================================
# LEAN-BRIDGE — SUPABASE DATABASE SERVICE
# All Supabase reads/writes go through this module.
# Uses service_role key — bypasses RLS intentionally.
# Workers and the bridge need full access; RLS is for the PWA.
# ============================================================

import logging
from datetime import datetime, timezone
from typing import Optional
from supabase import create_client, Client
from app.config import get_settings
from functools import lru_cache

logger = logging.getLogger(__name__)


@lru_cache()
def get_supabase() -> Client:
    """
    Returns cached Supabase client singleton.
    Uses service_role key — full database access, bypasses RLS.
    This client is for the VPS bridge and workers ONLY.
    Never send the service_role key to the browser.
    """
    settings = get_settings()
    return create_client(settings.supabase_url, settings.supabase_service_key)


# ── Student Queries ───────────────────────────────────────────

def get_active_students(trader_id: str) -> list[dict]:
    """
    Fetch all students eligible to receive a copy trade signal.
    Conditions:
      - is_active = true (account is live)
      - pause_copy = false (student hasn't paused)
      - payment_status = active (subscription current)
      - is_ghost = false (included — ghost accounts DO copy trades)
    Returns list of student dicts with account + risk profile data.
    """
    supabase = get_supabase()

    try:
        # Join student_accounts + risk_profiles + payment_status
        result = supabase.rpc("get_active_students_for_signal", {
            "p_trader_id": trader_id
        }).execute()

        students = result.data or []
        logger.info(
            "Fetched %d active students for trader %s",
            len(students), trader_id
        )
        return students

    except Exception as e:
        logger.error("Failed to fetch active students: %s", str(e))
        raise


def get_all_active_students() -> list[dict]:
    """
    Fetch ALL active students across ALL traders.
    Uses separate queries since risk_profiles and payment_status
    link to users, not directly to student_accounts.
    """
    supabase = get_supabase()

    try:
        # Get all active student accounts
        result = (
            supabase.table("student_accounts")
            .select("*")
            .eq("is_active", True)
            .execute()
        )
        students = result.data or []

        # Enrich each student with risk profile and payment status
        enriched = []
        for student in students:
            user_id = student.get("user_id")

            # Get risk profile
            risk = supabase.table("risk_profiles") \
                .select("aggressiveness, max_lot, max_drawdown, min_balance, pause_copy, advanced_options, overleverage_max_lot") \
                .eq("user_id", user_id) \
                .single() \
                .execute()

            # Get payment status
            payment = supabase.table("payment_status") \
                .select("subscription_status") \
                .eq("user_id", user_id) \
                .single() \
                .execute()

            risk_data = risk.data or {}
            payment_data = payment.data or {}

            # Skip paused or non-active subscriptions
            if risk_data.get("pause_copy", False):
                continue
            if payment_data.get("subscription_status") != "active":
                continue

            student["risk_profiles"] = risk_data
            student["payment_status"] = payment_data
            enriched.append(student)

        logger.info("Fetched %d active students", len(enriched))
        return enriched

    except Exception as e:
        logger.error("Failed to fetch all active students: %s", str(e))
        raise



# ── Heartbeat Updates ─────────────────────────────────────────

def update_heartbeat(trader_id: str, status: str = "ONLINE") -> None:
    """
    Upsert the mentor_heartbeat row for this trader.
    Called every time a heartbeat webhook arrives.
    """
    supabase = get_supabase()

    try:
        supabase.table("mentor_heartbeat").upsert({
            "trader_id":           trader_id,
            "last_seen":           datetime.now(timezone.utc).isoformat(),
            "status":              status,
            "consecutive_misses":  0,
            "updated_at":          datetime.now(timezone.utc).isoformat()
        }, on_conflict="trader_id").execute()

        logger.debug("Heartbeat updated for trader %s → %s", trader_id, status)

    except Exception as e:
        logger.error("Failed to update heartbeat for trader %s: %s", trader_id, str(e))
        # Non-fatal — don't raise, heartbeat failure shouldn't block signal processing


def mark_heartbeat_missed(trader_id: str) -> dict:
    """
    Increment consecutive_misses and update status.
    Called by Celery Beat when no heartbeat received within window.
    Returns updated heartbeat row.
    """
    supabase = get_supabase()

    try:
        # Read current state
        result = (
            supabase.table("mentor_heartbeat")
            .select("consecutive_misses")
            .eq("trader_id", trader_id)
            .single()
            .execute()
        )

        misses = (result.data.get("consecutive_misses", 0) if result.data else 0) + 1
        settings = get_settings()

        # Determine new status based on miss count
        # Celery Beat runs every 60s, so misses map to seconds
        seconds_missed = misses * 60
        if seconds_missed >= settings.heartbeat_offline_seconds:
            new_status = "OFFLINE"
        elif seconds_missed >= settings.heartbeat_warning_seconds:
            new_status = "WARNING"
        else:
            new_status = "ONLINE"

        supabase.table("mentor_heartbeat").upsert({
            "trader_id":          trader_id,
            "status":             new_status,
            "consecutive_misses": misses,
            "updated_at":         datetime.now(timezone.utc).isoformat()
        }, on_conflict="trader_id").execute()

        logger.warning(
            "Heartbeat missed for trader %s. Misses: %d, Status: %s",
            trader_id, misses, new_status
        )
        return {"trader_id": trader_id, "status": new_status, "consecutive_misses": misses}

    except Exception as e:
        logger.error("Failed to mark heartbeat missed: %s", str(e))
        return {}


# ── System Flags ──────────────────────────────────────────────

def get_system_flag(key: str) -> str:
    """
    Read a system flag from the system_flags table.
    Workers check 'emergency_close_all' before every trade.
    """
    supabase = get_supabase()

    try:
        result = (
            supabase.table("system_flags")
            .select("value")
            .eq("key", key)
            .single()
            .execute()
        )
        return result.data.get("value", "false") if result.data else "false"

    except Exception as e:
        logger.error("Failed to read system flag %s: %s", key, str(e))
        return "false"


def set_system_flag(key: str, value: str, actor_id: Optional[str] = None) -> None:
    """
    Update a system flag. Used by super admin emergency close.
    """
    supabase = get_supabase()

    try:
        supabase.table("system_flags").upsert({
            "key":        key,
            "value":      value,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "updated_by": actor_id
        }, on_conflict="key").execute()

        # Audit log
        supabase.table("system_events").insert({
            "event_type": f"system_flag_{key}_set",
            "actor_id":   actor_id,
            "payload":    {"key": key, "value": value},
            "severity":   "warning" if key == "emergency_close_all" and value == "true" else "info"
        }).execute()

        logger.info("System flag '%s' set to '%s' by %s", key, value, actor_id)

    except Exception as e:
        logger.error("Failed to set system flag %s: %s", key, str(e))
        raise


# ── Audit Logging ─────────────────────────────────────────────

def log_system_event(
    event_type: str,
    actor_id:   Optional[str] = None,
    payload:    Optional[dict] = None,
    severity:   str = "info"
) -> None:
    """
    Write to system_events audit log.
    Non-fatal — never raises.
    """
    supabase = get_supabase()

    try:
        supabase.table("system_events").insert({
            "event_type": event_type,
            "actor_id":   actor_id,
            "payload":    payload or {},
            "severity":   severity
        }).execute()
    except Exception as e:
        logger.error("Failed to write system event %s: %s", event_type, str(e))
