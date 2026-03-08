# app/routers/webhooks.py
# ============================================================
# LEAN-BRIDGE — WEBHOOK ENDPOINTS
# All inbound signals from the Mentor EA hit these endpoints.
# Every request is HMAC-verified and timestamp-checked before
# any processing occurs.
# ============================================================

import logging
import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Request, Depends
from app.models.payloads import (
    SignalPayload, ClosePayload, HeartbeatPayload,
    SignalResponse, CloseResponse, HeartbeatResponse
)
from app.services.security import validate_webhook_request
from app.services.supabase_service import (
    get_all_active_students,
    update_heartbeat,
    log_system_event
)
from app.services.celery_app import (
    execute_trade_batch,
    execute_close_batch,
    chunk_list
)
from app.config import get_settings

logger     = logging.getLogger(__name__)
router     = APIRouter(prefix="/webhook", tags=["Webhooks"])
settings   = get_settings()


# ── POST /webhook/signal ──────────────────────────────────────

@router.post(
    "/signal",
    response_model=SignalResponse,
    summary="Receive trade signal from Mentor EA",
    description=(
        "Validates HMAC signature and timestamp, fetches all active students, "
        "splits into batches, and dispatches one Celery task per batch. "
        "Returns immediately after dispatching — does not wait for execution."
    )
)
async def handle_signal(payload: SignalPayload, request: Request) -> SignalResponse:
    """
    Main signal handler. The most critical endpoint in the system.
    Every Mentor trade open hits this endpoint.

    Flow:
    1. Validate HMAC + timestamp
    2. Fetch active students
    3. Chunk into batches
    4. Dispatch Celery tasks
    5. Return 200 immediately
    """
    signal_id = str(uuid.uuid4())

    logger.info(
        "Signal received | id=%s | symbol=%s | type=%s | lot=%.2f | strategy=%s",
        signal_id,
        payload.data.symbol,
        payload.data.order_type.value,
        payload.data.lot_size,
        payload.data.strategy_type.value
    )

    # Step 1: Verify HMAC and timestamp
    validate_webhook_request(payload.model_dump(), payload.signature)

    # Step 2: Fetch all eligible students
    try:
        students = get_all_active_students()
    except Exception as e:
        logger.error("Failed to fetch students for signal %s: %s", signal_id, str(e))
        raise HTTPException(status_code=503, detail="Database unavailable")

    if not students:
        logger.warning("Signal %s received but no active students found.", signal_id)
        return SignalResponse(
            status="dispatched_no_students",
            student_count=0,
            batches=0,
            signal_id=signal_id
        )

    # Step 3: Build signal dict for Celery (JSON-serialisable)
    signal_dict = {
        "ticket_id":     payload.data.ticket_id,
        "symbol":        payload.data.symbol,
        "order_type":    payload.data.order_type.value,
        "lot_size":      payload.data.lot_size,
        "entry_price":   payload.data.entry_price,
        "stop_loss":     payload.data.stop_loss,
        "take_profit":   payload.data.take_profit,
        "strategy_type": payload.data.strategy_type.value,
        "signal_id":     signal_id,
        "received_at":   datetime.now(timezone.utc).isoformat()
    }

    # Step 4: Chunk and dispatch to Celery
    batches = chunk_list(students, settings.batch_size)

    for batch in batches:
        execute_trade_batch.delay(batch, signal_dict)

    logger.info(
        "Signal %s dispatched | %d students | %d batches",
        signal_id, len(students), len(batches)
    )

    # Step 5: Audit log (non-blocking)
    log_system_event(
        event_type="signal_dispatched",
        payload={
            "signal_id":     signal_id,
            "ticket_id":     payload.data.ticket_id,
            "symbol":        payload.data.symbol,
            "strategy_type": payload.data.strategy_type.value,
            "student_count": len(students),
            "batches":       len(batches)
        },
        severity="info"
    )

    return SignalResponse(
        status="dispatched",
        student_count=len(students),
        batches=len(batches),
        signal_id=signal_id
    )


# ── POST /webhook/close ───────────────────────────────────────

@router.post(
    "/close",
    response_model=CloseResponse,
    summary="Mentor closed a trade — close all student positions"
)
async def handle_close(payload: ClosePayload, request: Request) -> CloseResponse:
    """
    Triggered when Mentor closes a position.
    Dispatches close tasks to all students who have that position open.
    """
    logger.info(
        "Close signal received | ticket=%s | symbol=%s",
        payload.mentor_ticket, payload.symbol
    )

    # Verify HMAC + timestamp
    validate_webhook_request(payload.model_dump(), payload.signature)

    try:
        students = get_all_active_students()
    except Exception as e:
        raise HTTPException(status_code=503, detail="Database unavailable")

    if not students:
        return CloseResponse(
            status="no_students",
            student_count=0,
            mentor_ticket=payload.mentor_ticket
        )

    close_data = {
        "mentor_ticket": payload.mentor_ticket,
        "symbol":        payload.symbol
    }

    batches = chunk_list(students, settings.batch_size)
    for batch in batches:
        execute_close_batch.delay(batch, close_data)

    logger.info(
        "Close dispatched for ticket %s | %d students | %d batches",
        payload.mentor_ticket, len(students), len(batches)
    )

    return CloseResponse(
        status="dispatched",
        student_count=len(students),
        mentor_ticket=payload.mentor_ticket
    )


# ── POST /webhook/heartbeat ───────────────────────────────────

@router.post(
    "/heartbeat",
    response_model=HeartbeatResponse,
    summary="60-second keepalive from Mentor EA"
)
async def handle_heartbeat(payload: HeartbeatPayload, request: Request) -> HeartbeatResponse:
    """
    Receives the Mentor EA's 60-second heartbeat.
    Updates mentor_heartbeat table → Celery Beat monitors it.
    If no heartbeat arrives for 90s → WARNING.
    If no heartbeat for 180s → OFFLINE, new signals paused.
    """
    # Verify HMAC + timestamp
    validate_webhook_request(payload.model_dump(), payload.signature)

    update_heartbeat(trader_id=payload.trader_id, status="ONLINE")

    logger.debug("Heartbeat received from trader %s", payload.trader_id)

    return HeartbeatResponse(
        status="ok",
        trader_id=payload.trader_id,
        server_time=datetime.now(timezone.utc).isoformat()
    )
