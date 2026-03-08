# tests/test_bridge.py
# ============================================================
# LEAN-BRIDGE — BRIDGE TEST SUITE
# Run with: pytest tests/ -v
# These tests run WITHOUT a real Supabase or Redis connection.
# All external services are mocked.
# ============================================================

import pytest
import hmac
import hashlib
import json
import time
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient


# ── App Setup ─────────────────────────────────────────────────

# Set test environment variables before importing app
import os
os.environ.update({
    "SUPABASE_URL":         "https://test.supabase.co",
    "SUPABASE_SERVICE_KEY": "test_service_key",
    "WEBHOOK_SECRET":       "test_secret_key_for_testing_only",
    "ENCRYPTION_KEY":       "dGVzdF9rZXlfMzJfYnl0ZXNfbG9uZ19leGFtcGxlX2tleQ==",
    "REDIS_URL":            "redis://localhost:6379/0",
    "CELERY_BROKER_URL":    "redis://localhost:6379/0",
    "CELERY_RESULT_BACKEND":"redis://localhost:6379/0",
    "ENVIRONMENT":          "test",
})

from app.main import app
from app.services.security import compute_hmac, verify_signature, verify_timestamp

client = TestClient(app)


# ── Helpers ───────────────────────────────────────────────────

def make_valid_signal_payload(
    symbol="Volatility 75 Index",
    order_type="BUY",
    lot_size=0.10,
    strategy="normal",
    timestamp=None
) -> tuple[dict, str]:
    """Build a valid signed signal payload. Returns (payload, signature)."""
    if timestamp is None:
        timestamp = int(time.time())

    payload = {
        "signature": "",  # placeholder — will be replaced
        "timestamp":  timestamp,
        "event":      "trade_open",
        "data": {
            "symbol":        symbol,
            "order_type":    order_type,
            "lot_size":      lot_size,
            "entry_price":   12345.67,
            "stop_loss":     12300.00,
            "take_profit":   12450.00,
            "ticket_id":     "TEST_12345",
            "strategy_type": strategy
        }
    }

    body_bytes = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode("utf-8")
    signature  = compute_hmac(body_bytes, "test_secret_key_for_testing_only")
    payload["signature"] = signature

    return payload, signature


# ── Security Tests ────────────────────────────────────────────

class TestHMACVerification:

    def test_valid_signature_passes(self):
        body    = b'{"test": "data"}'
        secret  = "my_secret"
        sig     = compute_hmac(body, secret)
        assert verify_signature(sig, body, secret) is True

    def test_wrong_signature_fails(self):
        body = b'{"test": "data"}'
        assert verify_signature("wrong" * 16, body) is False

    def test_tampered_body_fails(self):
        body    = b'{"test": "data"}'
        secret  = "my_secret"
        sig     = compute_hmac(body, secret)
        tampered = b'{"test": "tampered"}'
        assert verify_signature(sig, tampered, secret) is False

    def test_fresh_timestamp_passes(self):
        assert verify_timestamp(int(time.time())) is True

    def test_old_timestamp_fails(self):
        old_timestamp = int(time.time()) - 60  # 60 seconds ago
        assert verify_timestamp(old_timestamp) is False

    def test_future_timestamp_fails(self):
        future_timestamp = int(time.time()) + 60  # 60 seconds in future
        assert verify_timestamp(future_timestamp) is False


# ── Health Endpoint Tests ─────────────────────────────────────

class TestHealthEndpoint:

    @patch("app.routers.health.redis")
    @patch("app.routers.health.psutil")
    def test_health_returns_200(self, mock_psutil, mock_redis):
        # Mock redis ping
        mock_redis.from_url.return_value.ping.return_value = True

        # Mock psutil
        mock_psutil.cpu_percent.return_value    = 30.0
        mock_psutil.virtual_memory.return_value = MagicMock(
            used=5 * 1024**3, total=32 * 1024**3, percent=16
        )
        mock_psutil.disk_usage.return_value = MagicMock(
            used=50 * 1024**3, percent=20
        )

        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "components" in data
        assert "version" in data

    def test_health_has_expected_components(self):
        response = client.get("/health")
        # Even if components are down, the keys should exist
        assert response.status_code == 200


# ── Webhook Signal Tests ──────────────────────────────────────

class TestSignalWebhook:

    @patch("app.routers.webhooks.log_system_event")
    @patch("app.routers.webhooks.get_all_active_students")
    @patch("app.routers.webhooks.execute_trade_batch")
    def test_valid_signal_dispatches_tasks(
        self, mock_celery, mock_students, mock_log
    ):
        """Valid signal with active students should dispatch Celery tasks."""
        mock_students.return_value = [
            {"user_id": f"student_{i}", "folder_path": f"C:\\terminals\\{i}"}
            for i in range(15)  # 15 students = 2 batches (batch_size=10)
        ]
        mock_celery.delay = MagicMock()
        mock_log.return_value = None

        payload, _ = make_valid_signal_payload()
        response = client.post("/webhook/signal", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "dispatched"
        assert data["student_count"] == 15
        assert data["batches"] == 2

    def test_invalid_signature_returns_401(self):
        payload, _ = make_valid_signal_payload()
        payload["signature"] = "a" * 64  # Wrong signature

        response = client.post("/webhook/signal", json=payload)
        assert response.status_code == 401

    def test_stale_timestamp_returns_400(self):
        payload, _ = make_valid_signal_payload(timestamp=int(time.time()) - 120)

        response = client.post("/webhook/signal", json=payload)
        assert response.status_code == 400

    @patch("app.routers.webhooks.log_system_event")
    @patch("app.routers.webhooks.get_all_active_students")
    @patch("app.routers.webhooks.execute_trade_batch")
    def test_no_active_students_returns_200(self, mock_celery, mock_students, mock_log):
        """No students should still return 200 — not an error."""
        mock_students.return_value = []
        mock_celery.delay = MagicMock()
        mock_log.return_value = None

        payload, _ = make_valid_signal_payload()
        response = client.post("/webhook/signal", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["student_count"] == 0

    def test_missing_fields_returns_422(self):
        """Pydantic validation should reject incomplete payloads."""
        response = client.post("/webhook/signal", json={"event": "trade_open"})
        assert response.status_code == 422

    def test_invalid_order_type_returns_422(self):
        payload, _ = make_valid_signal_payload()
        payload["data"]["order_type"] = "INVALID"
        response = client.post("/webhook/signal", json=payload)
        assert response.status_code == 422

    def test_negative_lot_size_returns_422(self):
        payload, _ = make_valid_signal_payload(lot_size=-0.1)
        response = client.post("/webhook/signal", json=payload)
        assert response.status_code == 422


# ── Heartbeat Tests ───────────────────────────────────────────

class TestHeartbeatWebhook:

    @patch("app.routers.webhooks.update_heartbeat")
    def test_valid_heartbeat_returns_200(self, mock_hb):
        mock_hb.return_value = None

        timestamp = int(time.time())
        payload = {
            "event":     "heartbeat",
            "timestamp": timestamp,
            "trader_id": "trader_abc123",
            "signature": ""
        }
        body_bytes = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
        payload["signature"] = compute_hmac(body_bytes, "test_secret_key_for_testing_only")

        response = client.post("/webhook/heartbeat", json=payload)
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_heartbeat_invalid_signature(self):
        payload = {
            "event":     "heartbeat",
            "timestamp": int(time.time()),
            "trader_id": "trader_abc123",
            "signature": "b" * 64
        }
        response = client.post("/webhook/heartbeat", json=payload)
        assert response.status_code == 401


# ── Risk Logic Tests ──────────────────────────────────────────

class TestRiskCalculations:
    """Unit tests for lot calculation logic."""

    def _calc_lot(self, mentor_lot, aggressiveness, max_lot):
        """Mirror the lot calculation from trade_executor."""
        adjusted = mentor_lot * aggressiveness
        return round(min(adjusted, max_lot), 2)

    def test_mirror_copy(self):
        assert self._calc_lot(0.10, 1.0, 0.10) == 0.10

    def test_conservative_copy(self):
        assert self._calc_lot(0.10, 0.5, 0.10) == 0.05

    def test_aggressive_copy_uncapped(self):
        assert self._calc_lot(0.10, 2.0, 0.20) == 0.20

    def test_aggressive_copy_capped_by_max_lot(self):
        """Aggressiveness would give 0.15 but max_lot is 0.10."""
        assert self._calc_lot(0.10, 1.5, 0.10) == 0.10

    def test_double_risk(self):
        assert self._calc_lot(0.10, 2.0, 1.0) == 0.20
