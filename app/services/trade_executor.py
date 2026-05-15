# app/services/trade_executor.py
# ============================================================
# LEAN-BRIDGE — PER-STUDENT TRADE EXECUTION (EA Architecture v3.0)
#
# ARCHITECTURE UPDATE:
#   OLD: /trade/broadcast → Python workers → mt5.initialize() → IPC timeout
#   NEW: /signal → file-based → EA inside MT5 → zero IPC
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

LB_MAGIC_NUMBER: int = 20260101

MT5_SERVER_URL = getattr(settings, "mt5_server_url", None)
MT5_AVAILABLE  = MT5_SERVER_URL is not None

if not MT5_AVAILABLE:
    logger.warning("MT5_SERVER_URL not set — running in simulation mode.")

Outcome = Literal["success", "failed", "skipped", "retried"]

BROADCAST_TIMEOUT = 5


# ── MT5 Server HTTP Helpers (NEW EA ARCHITECTURE) ─────────────

def _mt5_signal(payload: dict) -> dict:
    """
    Send signal to new EA-based manager at /signal.
    Fire-and-forget: manager writes files, EA executes asynchronously.
    """
    try:
        response = requests.post(
            f"{MT5_SERVER_URL}/signal",
            json=payload,
            timeout=BROADCAST_TIMEOUT
        )
        return response.json()
    except Exception as e:
        logger.error("MT5 signal request failed: %s", str(e))
        return {"status": "failed", "error": str(e)}


# ── DEPRECATED: Old endpoints kept for emergency fallback ─────

def _mt5_broadcast(payload: dict) -> dict:
    """Legacy broadcast endpoint — kept for emergency fallback only."""
    try:
        response = requests.post(
            f"{MT5_SERVER_URL}/trade/broadcast",
            json=payload,
            timeout=BROADCAST_TIMEOUT
        )
        return response.json()
    except Exception as e:
        logger.error("MT5 broadcast request failed: %s", str(e))
        return {"status": "failed", "error": str(e)}


def _mt5_trade(payload: dict) -> dict:
    """Legacy single-trade endpoint — kept for emergency fallback."""
    try:
        response = requests.post(
            f"{MT5_SERVER_URL}/trade",
            json=payload,
            timeout=5
        )
        return response.json()
    except Exception as e:
        logger.error("MT5 server request failed: %s", str(e))
        return {"status": "failed", "error": str(e)}


# ── Batch Signal Dispatch (NEW — EA Architecture) ─────────────

def execute_batch_broadcast(students: list[dict], signal: dict) -> dict:
    """
    Dispatch signal to ALL students in one request.
    Manager writes individual files; EA slaves execute inside MT5.
    """
    if not MT5_AVAILABLE:
        logger.info("[SIMULATION] Would signal %d students", len(students))
        return {"status": "simulated", "student_count": len(students)}
    
    trader_id = signal.get("trader_id", "")
    if not trader_id and students:
        trader_id = students[0].get("trader_id", "")
    
    payload = {
        "action":        "OPEN",
        "symbol":        signal.get("symbol"),
        "order_type":    signal.get("order_type", "BUY"),
        "volume":        signal.get("lot_size", 0.01),
        "mentor_ticket": signal.get("ticket_id", ""),
        "trader_id":     trader_id,
        "stop_loss":     signal.get("stop_loss", 0),
        "take_profit":   signal.get("take_profit", 0),
    }
    
    logger.info("📡 Signalling %d students (trader=%s)", len(students), trader_id)
    result = _mt5_signal(payload)
    
    if result.get("status") == "written":
        files_written = result.get("files_written", 0)
        files_failed = result.get("files_failed", 0)
        logger.info("✅ Signals written: %d/%d (failed: %d)", 
                    files_written, len(students), files_failed)
        
        for student in students:
            user_id = student.get("user_id")
            risk = student.get("risk_profiles", {})
            if isinstance(risk, list):
                risk = risk[0] if risk else {}
            
            _log_trade(
                user_id=user_id,
                signal=signal,
                risk=risk,
                lot=float(signal.get("lot_size", 0.01)),
                student_ticket="queued",
                entry_price=None,
            )
    else:
        error = result.get("error", "Signal dispatch failed")
        logger.error("❌ Signal dispatch failed: %s", error)
        for student in students:
            _log_failure(student.get("user_id"), signal, error)
    
    return result


# ── Main Entry Point (Legacy — kept for compatibility) ────────

def execute_for_student(student: dict, signal: dict) -> Outcome:
    """
    DEPRECATED: Single-student execution.
    New architecture uses execute_batch_broadcast() exclusively.
    Kept for emergency fallback only.
    """
    user_id     = student.get("user_id")
    folder_path = student.get("folder_path")

    if get_system_flag("emergency_close_all") == "true":
        return _log_skip(user_id, signal, "emergency_close_all_active")
    if get_system_flag("platform_paused") == "true":
        return _log_skip(user_id, signal, "platform_paused")

    if not student.get("is_active", False):
        return _log_skip(user_id, signal, "account_inactive")

    risk = student.get("risk_profiles", {})
    if isinstance(risk, list):
        risk = risk[0] if risk else {}
    if risk.get("pause_copy", False):
        return _log_skip(user_id, signal, "user_paused")

    payment = student.get("payment_status", {})
    if isinstance(payment, list):
        payment = payment[0] if payment else {}
    if payment.get("subscription_status") != "active":
        return _log_skip(user_id, signal, "subscription_expired")

    strategy = signal.get("strategy_type", "normal")
    advanced = risk.get("advanced_options", {})
    if isinstance(advanced, str):
        advanced = json.loads(advanced)

    if strategy == "scale_in" and not advanced.get("scale_in", False):
        return _log_skip(user_id, signal, "skipped_scale_in_not_opted_in")

    if strategy == "hedge" and not advanced.get("hedge", False):
        return _log_skip(user_id, signal, "skipped_hedge_not_opted_in")

    if not MT5_AVAILABLE:
        logger.info("[SIMULATION] Would execute trade for student %s", user_id)
        return _log_trade(user_id, signal, risk, lot=0.01, simulated=True)

    if not folder_path:
        _update_connection_status(user_id, "ERROR", "No folder_path configured")
        return _log_failure(user_id, signal, "no_folder_path_configured")

    aggressiveness = float(risk.get("aggressiveness", 1.0))
    mentor_lot     = float(signal.get("lot_size", 0.01))
    lot_cap = float(risk.get("max_lot", 0.10))
    final_lot = round(min(mentor_lot * aggressiveness, lot_cap), 2)

    if final_lot <= 0:
        return _log_skip(user_id, signal, "calculated_lot_is_zero")

    payload = {
        "login":         student.get("mt5_login"),
        "password":      student.get("encrypted_password"),
        "server":        student.get("mt5_server"),
        "symbol":        signal.get("symbol"),
        "order_type":    signal.get("order_type", "BUY"),
        "volume":        final_lot,
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


# ── Close Trade (NEW — EA Architecture) ───────────────────────

def close_for_student(student: dict, close_data: dict) -> Outcome:
    """
    Dispatch close signal. With EA architecture, one call per trader_id
    closes all matching students. Per-student calls are redundant but safe.
    """
    user_id = student.get("user_id")

    if not MT5_AVAILABLE:
        return "skipped"

    trader_id = student.get("trader_id", "")

    payload = {
        "action":        "CLOSE_SYMBOL",
        "symbol":        close_data.get("symbol"),
        "order_type":    "SELL",
        "volume":        0,
        "mentor_ticket": close_data.get("mentor_ticket", ""),
        "trader_id":     trader_id,
    }

    result = _mt5_signal(payload)

    if result.get("status") == "written":
        return "success"
    else:
        error = result.get("error", "Close dispatch failed")
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
            "status":         "simulated" if simulated else "queued",
            "strategy_type":  signal.get("strategy_type", "normal"),
            "error_message":  "[SIMULATED]" if simulated else None,
            "executed_at":    datetime.now(timezone.utc).isoformat()
        }).execute()
    except Exception as e:
        logger.error("Failed to log trade for %s: %s", user_id, str(e))
    return "success"


def _log_failure(user_id: str, signal: dict, error: str, lot: float = None) -> Outcome:
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
