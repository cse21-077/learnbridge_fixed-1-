# app/services/trade_executor.py
# ============================================================
# LEAN-BRIDGE — PER-STUDENT TRADE EXECUTION
#
# ARCHITECTURE UPDATE:
# Trades are now executed via HTTP requests to the Contabo
# Windows VPS running mt5_server.py (Flask + MetaTrader5).
# Hetzner workers no longer need MetaTrader5 installed locally.
#
# FIXES APPLIED:
# [FIX-3] close_for_student() filters by magic number so only
#         LB-opened positions are closed.
# [FIX-4] Magic number 20260101 is a named constant.
# [FIX-5] Simulated trades log status "simulated" not "success".
# [FIX-6] _update_connection_status() uses last_checked only.
# ============================================================

import logging
import json
import requests
from datetime import datetime, timezone
from typing import Literal
from app.config import get_settings
from app.services.supabase_service import get_supabase, get_system_flag

logger   = logging.getLogger(__name__)
settings = get_settings()

# Single source of truth for the magic number.
# All LB-opened positions carry this so close logic can
# identify them without relying on the comment string alone.
LB_MAGIC_NUMBER: int = 20260101

# MT5 server URL on Contabo Windows VPS
MT5_SERVER_URL = getattr(settings, "mt5_server_url", None)
MT5_AVAILABLE  = MT5_SERVER_URL is not None

if not MT5_AVAILABLE:
    logger.warning("MT5_SERVER_URL not set — running in simulation mode.")

Outcome = Literal["success", "failed", "skipped", "retried"]


# ── MT5 Server HTTP Helpers ───────────────────────────────────

def _mt5_trade(payload: dict) -> dict:
    """Send a trade request to the Contabo MT5 server."""
    try:
        response = requests.post(
            f"{MT5_SERVER_URL}/trade",
            json=payload,
            timeout=30
        )
        return response.json()
    except Exception as e:
        logger.error("MT5 server request failed: %s", str(e))
        return {"status": "failed", "error": str(e)}


def _mt5_close(payload: dict) -> dict:
    """Send a close request to the Contabo MT5 server."""
    try:
        response = requests.post(
            f"{MT5_SERVER_URL}/close",
            json=payload,
            timeout=30
        )
        return response.json()
    except Exception as e:
        logger.error("MT5 server close request failed: %s", str(e))
        return {"status": "failed", "error": str(e)}


def _mt5_health() -> bool:
    """Check if the Contabo MT5 server is reachable."""
    try:
        response = requests.get(f"{MT5_SERVER_URL}/health", timeout=5)
        return response.status_code == 200
    except Exception:
        return False


# ── Main Entry Point ──────────────────────────────────────────

def execute_for_student(student: dict, signal: dict) -> Outcome:
    """
    Execute a copy trade for one student.
    Called by the Celery execute_trade_batch task.
    Sends HTTP request to Contabo MT5 server for execution.
    """
    user_id     = student.get("user_id")
    folder_path = student.get("folder_path")

    # [0] Kill switches
    if get_system_flag("emergency_close_all") == "true":
        return _log_skip(user_id, signal, "emergency_close_all_active")
    if get_system_flag("platform_paused") == "true":
        return _log_skip(user_id, signal, "platform_paused")

    # [1] Account active
    if not student.get("is_active", False):
        return _log_skip(user_id, signal, "account_inactive")

    # [2] Pause copy
    risk = student.get("risk_profiles", {})
    if isinstance(risk, list):
        risk = risk[0] if risk else {}
    if risk.get("pause_copy", False):
        return _log_skip(user_id, signal, "user_paused")

    # [3] Payment status
    payment = student.get("payment_status", {})
    if isinstance(payment, list):
        payment = payment[0] if payment else {}
    if payment.get("subscription_status") != "active":
        return _log_skip(user_id, signal, "subscription_expired")

    # [4] Advanced strategy opt-in filter
    strategy = signal.get("strategy_type", "normal")
    advanced = risk.get("advanced_options", {})
    if isinstance(advanced, str):
        advanced = json.loads(advanced)

    if strategy == "scale_in" and not advanced.get("scale_in", False):
        return _log_skip(user_id, signal, "skipped_scale_in_not_opted_in")

    if strategy == "hedge" and not advanced.get("hedge", False):
        return _log_skip(user_id, signal, "skipped_hedge_not_opted_in")

    if strategy == "overleverage" and not advanced.get("overleverage", False):
        signal = dict(signal)
        signal["_use_normal_lot_cap"] = True

    # ── Simulation Mode ───────────────────────────────────────
    if not MT5_AVAILABLE:
        logger.info("[SIMULATION] Would execute trade for student %s", user_id)
        return _log_trade(user_id, signal, risk, lot=0.01, simulated=True)

    # ── MT5 Server Execution ──────────────────────────────────
    if not folder_path:
        _update_connection_status(user_id, "ERROR", "No folder_path configured")
        return _log_failure(user_id, signal, "no_folder_path_configured")

    # [5] Lot calculation
    aggressiveness = float(risk.get("aggressiveness", 1.0))
    mentor_lot     = float(signal.get("lot_size", 0.01))

    if strategy == "overleverage" and not signal.get("_use_normal_lot_cap"):
        lot_cap = float(risk.get("overleverage_max_lot", risk.get("max_lot", 0.10)))
    else:
        lot_cap = float(risk.get("max_lot", 0.10))

    final_lot = round(min(mentor_lot * aggressiveness, lot_cap), 2)

    if final_lot <= 0:
        return _log_skip(user_id, signal, "calculated_lot_is_zero")

    # [6] Send trade to Contabo MT5 server
    payload = {
        "login":         student.get("mt5_login"),
        "password":      student.get("encrypted_password"),
        "server":        student.get("mt5_server"),
        "symbol":        signal.get("symbol"),
        "order_type":    signal.get("order_type", "BUY"),
        "volume":        final_lot,
        "stop_loss":     float(signal.get("stop_loss", 0)),
        "take_profit":   float(signal.get("take_profit", 0)),
        "magic":         LB_MAGIC_NUMBER,
        "mentor_ticket": signal.get("ticket_id", ""),
    }

    result = _mt5_trade(payload)

    if result.get("status") == "success":
        _update_connection_status(user_id, "CONNECTED")
        return _log_trade(
            user_id, signal, risk,
            lot=final_lot,
            student_ticket=str(result.get("ticket", "")),
            entry_price=result.get("price")
        )
    else:
        error = result.get("error", f"retcode={result.get('retcode', 'unknown')}")
        _update_connection_status(user_id, "ERROR", error)
        return _log_failure(user_id, signal, error, lot=final_lot)


# ── Close Trade ───────────────────────────────────────────────

def close_for_student(student: dict, close_data: dict) -> Outcome:
    """
    Close the student's LB-opened positions for the given symbol.
    Sends HTTP request to Contabo MT5 server.
    """
    user_id = student.get("user_id")

    if not MT5_AVAILABLE:
        logger.info("[SIMULATION] Would close trade for student %s", user_id)
        return "skipped"

    payload = {
        "login":    student.get("mt5_login"),
        "password": student.get("encrypted_password"),
        "server":   student.get("mt5_server"),
        "symbol":   close_data.get("symbol"),
        "magic":    LB_MAGIC_NUMBER,
    }

    result = _mt5_close(payload)

    if result.get("status") == "skipped":
        return "skipped"
    elif result.get("status") == "success":
        return "closed"
    else:
        error = result.get("error", "close failed")
        _update_connection_status(user_id, "ERROR", error)
        return "failed"


# ── Helpers ───────────────────────────────────────────────────

def _log_trade(
    user_id:        str,
    signal:         dict,
    risk:           dict,
    lot:            float,
    student_ticket: str   = None,
    entry_price:    float = None,
    simulated:      bool  = False
) -> Outcome:
    """Write a successful (or simulated) trade to trade_logs."""
    supabase = get_supabase()
    try:
        supabase.table("trade_logs").insert({
            "user_id":        user_id,
            "mentor_ticket":  signal.get("ticket_id"),
            "student_ticket": student_ticket,
            "symbol":         signal.get("symbol"),
            "order_type":     signal.get("order_type"),
            "volume":         lot,
            "entry_price":    entry_price or signal.get("entry_price"),
            "stop_loss":      signal.get("stop_loss"),
            "take_profit":    signal.get("take_profit"),
            "status":         "simulated" if simulated else "success",
            "strategy_type":  signal.get("strategy_type", "normal"),
            "error_message":  "[SIMULATED]" if simulated else None,
            "executed_at":    datetime.now(timezone.utc).isoformat()
        }).execute()
    except Exception as e:
        logger.error("Failed to log trade for %s: %s", user_id, str(e))
    return "success"


def _log_failure(user_id: str, signal: dict, error: str, lot: float = None) -> Outcome:
    """Write a failed trade attempt to trade_logs."""
    supabase = get_supabase()
    try:
        supabase.table("trade_logs").insert({
            "user_id":       user_id,
            "mentor_ticket": signal.get("ticket_id"),
            "symbol":        signal.get("symbol"),
            "order_type":    signal.get("order_type"),
            "volume":        lot,
            "status":        "failed",
            "strategy_type": signal.get("strategy_type", "normal"),
            "error_message": error,
            "executed_at":   datetime.now(timezone.utc).isoformat()
        }).execute()
    except Exception as e:
        logger.error("Failed to log failure for %s: %s", user_id, str(e))
    logger.warning("Trade FAILED for student %s: %s", user_id, error)
    return "failed"


def _log_skip(user_id: str, signal: dict, reason: str) -> Outcome:
    """Write a skipped trade to trade_logs."""
    supabase = get_supabase()
    STRATEGY_SKIPS = {
        "skipped_scale_in_not_opted_in",
        "skipped_hedge_not_opted_in",
        "skipped_overleverage_not_opted_in",
    }
    status = reason if reason in STRATEGY_SKIPS else "skipped"
    try:
        supabase.table("trade_logs").insert({
            "user_id":       user_id,
            "mentor_ticket": signal.get("ticket_id"),
            "symbol":        signal.get("symbol"),
            "order_type":    signal.get("order_type"),
            "status":        status,
            "strategy_type": signal.get("strategy_type", "normal"),
            "error_message": reason,
            "executed_at":   datetime.now(timezone.utc).isoformat()
        }).execute()
    except Exception as e:
        logger.error("Failed to log skip for %s: %s", user_id, str(e))
    logger.debug("Trade SKIPPED for student %s: %s", user_id, reason)
    return "skipped"


def _update_connection_status(user_id: str, status: str, error: str = None) -> None:
    """Update MT5 connection status for a student."""
    supabase = get_supabase()
    try:
        supabase.table("connection_status").upsert({
            "user_id":       user_id,
            "mt5_status":    status,
            "last_checked":  datetime.now(timezone.utc).isoformat(),
            "error_message": error,
        }, on_conflict="user_id").execute()
    except Exception as e:
        logger.error("Failed to update connection status for %s: %s", user_id, str(e))


def _auto_pause_student(user_id: str) -> None:
    """Auto-pause a student who has hit their max drawdown limit."""
    supabase = get_supabase()
    try:
        supabase.table("risk_profiles").update({
            "pause_copy": True,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("user_id", user_id).execute()

        supabase.table("system_events").insert({
            "event_type": "auto_pause_max_drawdown",
            "actor_id":   None,
            "payload":    {"user_id": user_id},
            "severity":   "warning"
        }).execute()

        logger.warning("Student %s auto-paused due to max drawdown.", user_id)
    except Exception as e:
        logger.error("Failed to auto-pause student %s: %s", user_id, str(e))