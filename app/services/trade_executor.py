# app/services/trade_executor.py
# ============================================================
# LEAN-BRIDGE — PER-STUDENT TRADE EXECUTION
#
# FIXES APPLIED:
# [FIX-3] close_for_student() was closing ALL positions on a
#         symbol including ones not opened by LB. Now filters
#         by magic number (20260101) so only LB-opened
#         positions are closed. This also correctly handles
#         scale-in where a student may have multiple LB
#         positions on the same symbol.
# [FIX-4] Magic number 20260101 is now a named constant so it
#         is shared between open and close logic — no mismatch.
# [FIX-5] _log_trade() had duplicate "success" in ternary —
#         simulated trades now log status "simulated" so they
#         are distinguishable from real fills in the dashboard.
# [FIX-6] _update_connection_status() had duplicate timestamp
#         keys (last_checked + updated_at both set). Removed
#         the redundant updated_at — schema uses last_checked.
# ============================================================

import logging
import json
from datetime import datetime, timezone
from typing import Literal
from app.config import get_settings
from app.services.supabase_service import get_supabase, get_system_flag

logger   = logging.getLogger(__name__)
settings = get_settings()

# [FIX-4] Single source of truth for the magic number.
# All LB-opened positions carry this so close logic can
# identify them without relying on the comment string alone.
LB_MAGIC_NUMBER: int = 20260101

# MT5 is only available on the Windows VPS.
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    mt5 = None
    logger.warning("MetaTrader5 not available — running in simulation mode.")

Outcome = Literal["success", "failed", "skipped", "retried"]


# ── Main Entry Point ──────────────────────────────────────────

def execute_for_student(student: dict, signal: dict) -> Outcome:
    """
    Execute a copy trade for one student.
    Called by the Celery execute_trade_batch task.
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
        # Graceful degradation — still trades, but with normal lot cap
        signal = dict(signal)
        signal["_use_normal_lot_cap"] = True

    # ── MT5 Execution ─────────────────────────────────────────

    if not folder_path:
        _update_connection_status(user_id, "ERROR", "No folder_path configured")
        return _log_failure(user_id, signal, "no_folder_path_configured")

    if not MT5_AVAILABLE:
        logger.info("[SIMULATION] Would execute trade for student %s", user_id)
        return _log_trade(user_id, signal, risk, lot=0.01, simulated=True)

    if not mt5.initialize(path=f"{folder_path}\\terminal.exe"):
        error = f"mt5.initialize() failed: {mt5.last_error()}"
        _update_connection_status(user_id, "ERROR", error)
        return _log_failure(user_id, signal, error)

    try:
        terminal_info = mt5.terminal_info()
        account_info  = mt5.account_info()

        if terminal_info is None or account_info is None:
            error = "Terminal or account info unavailable"
            _update_connection_status(user_id, "DISCONNECTED", error)
            return _log_failure(user_id, signal, error)

        _update_connection_status(user_id, "CONNECTED")

        balance = account_info.balance
        equity  = account_info.equity

        # [5] Min balance
        min_balance = float(risk.get("min_balance", 100.0))
        if balance < min_balance:
            return _log_skip(
                user_id, signal,
                f"below_min_balance (balance={balance:.2f}, min={min_balance:.2f})"
            )

        # [6] Max drawdown
        max_drawdown = float(risk.get("max_drawdown", 20.0))
        if balance > 0:
            drawdown_pct = ((balance - equity) / balance) * 100
            if drawdown_pct >= max_drawdown:
                _auto_pause_student(user_id)
                return _log_skip(
                    user_id, signal,
                    f"max_drawdown_hit (drawdown={drawdown_pct:.2f}%, max={max_drawdown:.2f}%)"
                )

        # [7] Lot calculation
        aggressiveness = float(risk.get("aggressiveness", 1.0))
        mentor_lot     = float(signal.get("lot_size", 0.01))

        if strategy == "overleverage" and not signal.get("_use_normal_lot_cap"):
            lot_cap = float(risk.get("overleverage_max_lot", risk.get("max_lot", 0.10)))
        else:
            lot_cap = float(risk.get("max_lot", 0.10))

        final_lot = round(min(mentor_lot * aggressiveness, lot_cap), 2)

        if final_lot <= 0:
            return _log_skip(user_id, signal, "calculated_lot_is_zero")

        # [8] Build and send order
        symbol     = signal.get("symbol")
        order_type = signal.get("order_type", "BUY")

        if not mt5.symbol_select(symbol, True):
            return _log_failure(user_id, signal, f"Symbol {symbol} not available")

        tick = mt5.symbol_info_tick(symbol)
        order_request = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       symbol,
            "volume":       final_lot,
            "type":         mt5.ORDER_TYPE_BUY if order_type == "BUY" else mt5.ORDER_TYPE_SELL,
            "price":        tick.ask if order_type == "BUY" else tick.bid,
            "sl":           float(signal.get("stop_loss", 0)),
            "tp":           float(signal.get("take_profit", 0)),
            "deviation":    settings.mt5_slippage_deviation,
            "magic":        LB_MAGIC_NUMBER,
            "comment":      f"LB:{signal.get('ticket_id', '')}",
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        result = mt5.order_send(order_request)

        # [9] Log result
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            error_msg = f"order_send failed: retcode={getattr(result, 'retcode', 'None')}"
            return _log_failure(user_id, signal, error_msg, lot=final_lot)

        return _log_trade(
            user_id, signal, risk,
            lot=final_lot,
            student_ticket=str(result.order),
            entry_price=result.price
        )

    finally:
        mt5.shutdown()


# ── Close Trade ───────────────────────────────────────────────

def close_for_student(student: dict, close_data: dict) -> Outcome:
    """
    Close the student's LB-opened positions for the given symbol.

    [FIX-3] Filters positions by LB_MAGIC_NUMBER so we never
    accidentally close positions the student opened manually or
    via another EA. Also handles scale-in correctly — all LB
    positions on the symbol are closed, not just the first one.
    """
    user_id     = student.get("user_id")
    folder_path = student.get("folder_path")

    if not folder_path or not MT5_AVAILABLE:
        return "skipped"

    if not mt5.initialize(path=f"{folder_path}\\terminal.exe"):
        return "failed"

    try:
        symbol = close_data.get("symbol")
        if not symbol:
            return "skipped"

        # [FIX-3] Fetch only positions on this symbol, then filter
        # to LB_MAGIC_NUMBER so we never close foreign positions.
        all_positions = mt5.positions_get(symbol=symbol)
        if not all_positions:
            return "skipped"

        lb_positions = [p for p in all_positions if p.magic == LB_MAGIC_NUMBER]
        if not lb_positions:
            logger.info(
                "No LB positions found for student %s on %s — skipping close.",
                user_id, symbol
            )
            return "skipped"

        any_failed = False
        for position in lb_positions:
            close_type = (
                mt5.ORDER_TYPE_SELL if position.type == mt5.POSITION_TYPE_BUY
                else mt5.ORDER_TYPE_BUY
            )
            tick = mt5.symbol_info_tick(symbol)
            close_request = {
                "action":       mt5.TRADE_ACTION_DEAL,
                "symbol":       symbol,
                "volume":       position.volume,
                "type":         close_type,
                "position":     position.ticket,
                "price":        tick.bid if position.type == mt5.POSITION_TYPE_BUY else tick.ask,
                "deviation":    settings.mt5_slippage_deviation,
                "magic":        LB_MAGIC_NUMBER,
                "comment":      f"LB_CLOSE:{close_data.get('mentor_ticket', '')}",
                "type_time":    mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }

            result = mt5.order_send(close_request)
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                logger.info(
                    "Closed LB position for student %s, ticket %d",
                    user_id, position.ticket
                )
            else:
                any_failed = True
                logger.error(
                    "Close failed for student %s ticket %d: retcode=%s",
                    user_id, position.ticket, getattr(result, "retcode", "None")
                )

        return "failed" if any_failed else "closed"

    finally:
        mt5.shutdown()


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
            # [FIX-5] Distinguish simulated from real fills
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
    # Known advanced-strategy skip reasons map to their own status values
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
        # [FIX-6] Removed duplicate updated_at key — schema uses last_checked
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
