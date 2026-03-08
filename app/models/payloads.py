# app/models/payloads.py
# ============================================================
# LEAN-BRIDGE — REQUEST & RESPONSE MODELS
# Pydantic v2 models for all webhook payloads
# ============================================================

from pydantic import BaseModel, Field, field_validator
from typing import Literal, Optional
from enum import Enum


# ── Enums ────────────────────────────────────────────────────

class OrderType(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"


class StrategyType(str, Enum):
    NORMAL       = "normal"
    SCALE_IN     = "scale_in"
    HEDGE        = "hedge"
    OVERLEVERAGE = "overleverage"


class WebhookEvent(str, Enum):
    TRADE_OPEN  = "trade_open"
    TRADE_CLOSE = "trade_close"
    HEARTBEAT   = "heartbeat"


# ── Trade Signal Data ─────────────────────────────────────────

class TradeData(BaseModel):
    symbol:        str   = Field(..., min_length=1, max_length=100)
    order_type:    OrderType
    lot_size:      float = Field(..., gt=0, le=100.0)
    entry_price:   float = Field(..., gt=0)
    stop_loss:     float = Field(..., ge=0)
    take_profit:   float = Field(..., ge=0)
    ticket_id:     str   = Field(..., min_length=1, max_length=50)
    strategy_type: StrategyType = StrategyType.NORMAL

    @field_validator("lot_size")
    @classmethod
    def validate_lot_size(cls, v: float) -> float:
        # Round to 2 decimal places — MT5 requirement
        return round(v, 2)


# ── Inbound Webhook Payloads ──────────────────────────────────

class SignalPayload(BaseModel):
    """
    POST /webhook/signal
    Sent by Mentor EA on every trade open.
    """
    signature:  str          = Field(..., min_length=64, max_length=64)
    timestamp:  int          = Field(..., gt=0)
    event:      Literal["trade_open"]
    data:       TradeData


class ClosePayload(BaseModel):
    """
    POST /webhook/close
    Sent by Mentor EA when they close a position.
    """
    signature:      str   = Field(..., min_length=64, max_length=64)
    timestamp:      int   = Field(..., gt=0)
    event:          Literal["trade_close"]
    mentor_ticket:  str   = Field(..., min_length=1)
    symbol:         str   = Field(..., min_length=1)


class HeartbeatPayload(BaseModel):
    """
    POST /webhook/heartbeat
    Sent by Mentor EA every 60 seconds.
    """
    signature:  str   = Field(..., min_length=64, max_length=64)
    timestamp:  int   = Field(..., gt=0)
    event:      Literal["heartbeat"]
    trader_id:  str   = Field(..., min_length=1)


# ── Outbound Response Models ──────────────────────────────────

class SignalResponse(BaseModel):
    status:         str
    student_count:  int
    batches:        int
    signal_id:      str


class CloseResponse(BaseModel):
    status:         str
    student_count:  int
    mentor_ticket:  str


class HeartbeatResponse(BaseModel):
    status:         str
    trader_id:      str
    server_time:    str


class HealthComponent(BaseModel):
    status:   Literal["ok", "degraded", "down"]
    detail:   Optional[str] = None


class HealthResponse(BaseModel):
    status:     Literal["ok", "degraded", "down"]
    version:    str
    environment: str
    components: dict[str, HealthComponent]


class ErrorResponse(BaseModel):
    error:    str
    detail:   Optional[str] = None
    code:     int
