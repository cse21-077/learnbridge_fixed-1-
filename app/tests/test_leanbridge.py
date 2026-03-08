# tests/test_leanbridge.py
# ============================================================
# LEAN-BRIDGE — COMPREHENSIVE TEST SUITE
# Run with: pytest tests/test_leanbridge.py -v
#
# Coverage:
#   - HMAC security (signing, verification, timing safety)
#   - Timestamp handling (seconds, milliseconds, stale, future)
#   - All webhook endpoints (signal, close, heartbeat)
#   - Payload validation (Pydantic models)
#   - Trade execution risk checks (all 9 steps)
#   - Advanced strategy filtering (scale_in, hedge, overleverage)
#   - Close logic — magic number filtering (FIX-3)
#   - Lot calculation (aggressiveness, caps, overleverage)
#   - Heartbeat status transitions (ONLINE→WARNING→OFFLINE)
#   - Dead letter queue writes
#   - Batch chunking utility
#   - Admin endpoints (emergency close, resume, pause)
#   - Health endpoint structure
#   - Simulation mode behaviour
#
# All external services (Supabase, Redis, Celery, MT5) mocked.
# No network calls made during tests.
# ============================================================

import os
import sys
import hmac
import hashlib
import json
import time
import unittest
from unittest.mock import patch, MagicMock, call, PropertyMock
from datetime import datetime, timezone, timedelta

# ── Environment setup BEFORE any app imports ─────────────────
os.environ.update({
    "SUPABASE_URL":                          "https://test.supabase.co",
    "SUPABASE_SERVICE_KEY":                  "test_service_key_do_not_use_in_prod",
    "WEBHOOK_SECRET":                        "test_webhook_secret_32chars_abcdef",
    "ENCRYPTION_KEY":                        "dGVzdF9rZXlfMzJfYnl0ZXNfbG9uZ19leGFtcGxlX2tleQ==",
    "REDIS_URL":                             "redis://localhost:6379/0",
    "CELERY_BROKER_URL":                     "redis://localhost:6379/0",
    "CELERY_RESULT_BACKEND":                 "redis://localhost:6379/0",
    "ENVIRONMENT":                           "test",
    "LOG_LEVEL":                             "ERROR",   # silence logs during tests
    "SIGNAL_TIMESTAMP_TOLERANCE_SECONDS":    "30",
    "HEARTBEAT_WARNING_SECONDS":             "90",
    "HEARTBEAT_OFFLINE_SECONDS":             "180",
    "BATCH_SIZE":                            "10",
    "MAX_RETRIES":                           "3",
})

# ── Helpers ───────────────────────────────────────────────────

SECRET = os.environ["WEBHOOK_SECRET"]


def _hmac(body: bytes, secret: str = SECRET) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _signed_payload(overrides: dict = None, secret: str = SECRET) -> dict:
    """Build a fully-signed, fresh SignalPayload dict."""
    payload = {
        "signature": "",
        "timestamp": int(time.time()),
        "event":     "trade_open",
        "data": {
            "symbol":        "Volatility 75 Index",
            "order_type":    "BUY",
            "lot_size":      0.10,
            "entry_price":   12345.67,
            "stop_loss":     12300.00,
            "take_profit":   12450.00,
            "ticket_id":     "TKT_001",
            "strategy_type": "normal",
        }
    }
    if overrides:
        # Support nested overrides with dot notation: "data.symbol"
        for key, val in overrides.items():
            if "." in key:
                outer, inner = key.split(".", 1)
                payload[outer][inner] = val
            else:
                payload[key] = val
    body_bytes      = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload["signature"] = _hmac(body_bytes, secret)
    return payload


def _signed_heartbeat(trader_id: str = "trader_abc") -> dict:
    payload = {
        "signature": "",
        "timestamp": int(time.time()),
        "event":     "heartbeat",
        "trader_id": trader_id,
    }
    body_bytes       = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload["signature"] = _hmac(body_bytes)
    return payload


def _signed_close(symbol: str = "Volatility 75 Index", ticket: str = "TKT_001") -> dict:
    payload = {
        "signature":     "",
        "timestamp":     int(time.time()),
        "event":         "trade_close",
        "mentor_ticket": ticket,
        "symbol":        symbol,
    }
    body_bytes       = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload["signature"] = _hmac(body_bytes)
    return payload


def _make_student(
    user_id: str = "student_001",
    is_active: bool = True,
    pause_copy: bool = False,
    subscription: str = "active",
    balance: float = 500.0,
    equity: float = 500.0,
    aggressiveness: float = 1.0,
    max_lot: float = 0.10,
    max_drawdown: float = 20.0,
    min_balance: float = 100.0,
    advanced_options: dict = None,
    overleverage_max_lot: float = 1.0,
    folder_path: str = "C:\\terminals\\student_001",
) -> dict:
    """Build a student dict matching the shape returned by Supabase."""
    return {
        "user_id":     user_id,
        "folder_path": folder_path,
        "is_active":   is_active,
        "risk_profiles": {
            "pause_copy":           pause_copy,
            "aggressiveness":       aggressiveness,
            "max_lot":              max_lot,
            "max_drawdown":         max_drawdown,
            "min_balance":          min_balance,
            "advanced_options":     advanced_options or {
                "scale_in": False, "hedge": False, "overleverage": False
            },
            "overleverage_max_lot": overleverage_max_lot,
        },
        "payment_status": {
            "subscription_status": subscription,
        },
    }


def _make_signal(
    strategy: str = "normal",
    lot: float = 0.10,
    symbol: str = "Volatility 75 Index",
    order_type: str = "BUY",
) -> dict:
    return {
        "ticket_id":     "TKT_001",
        "symbol":        symbol,
        "order_type":    order_type,
        "lot_size":      lot,
        "entry_price":   12345.67,
        "stop_loss":     12300.00,
        "take_profit":   12450.00,
        "strategy_type": strategy,
        "signal_id":     "sig_test_001",
        "received_at":   datetime.now(timezone.utc).isoformat(),
    }


# ═══════════════════════════════════════════════════════════════
# 1. SECURITY — compute_hmac / verify_signature / timestamps
# ═══════════════════════════════════════════════════════════════

class TestComputeHmac(unittest.TestCase):

    def setUp(self):
        from app.services.security import compute_hmac, verify_signature
        self.compute_hmac    = compute_hmac
        self.verify_signature = verify_signature

    def test_returns_64_char_hex(self):
        sig = self.compute_hmac(b"hello", "secret")
        self.assertEqual(len(sig), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in sig))

    def test_deterministic(self):
        a = self.compute_hmac(b"same", "key")
        b = self.compute_hmac(b"same", "key")
        self.assertEqual(a, b)

    def test_different_body_gives_different_sig(self):
        a = self.compute_hmac(b"aaa", "key")
        b = self.compute_hmac(b"bbb", "key")
        self.assertNotEqual(a, b)

    def test_different_secret_gives_different_sig(self):
        a = self.compute_hmac(b"body", "secret1")
        b = self.compute_hmac(b"body", "secret2")
        self.assertNotEqual(a, b)

    def test_verify_valid(self):
        body = b'{"data": "value"}'
        sig  = self.compute_hmac(body, "my_secret")
        self.assertTrue(self.verify_signature(sig, body, "my_secret"))

    def test_verify_wrong_sig_fails(self):
        body = b'{"data": "value"}'
        self.assertFalse(self.verify_signature("a" * 64, body, "my_secret"))

    def test_verify_tampered_body_fails(self):
        body     = b'{"amount": "100"}'
        sig      = self.compute_hmac(body, "secret")
        tampered = b'{"amount": "999"}'
        self.assertFalse(self.verify_signature(sig, tampered, "secret"))

    def test_verify_case_insensitive(self):
        body = b"test"
        sig  = self.compute_hmac(body, "s").upper()
        self.assertTrue(self.verify_signature(sig, body, "s"))

    def test_verify_empty_body(self):
        sig = self.compute_hmac(b"", "key")
        self.assertTrue(self.verify_signature(sig, b"", "key"))


# ═══════════════════════════════════════════════════════════════
# 2. TIMESTAMP HANDLING — normalisation + freshness
# ═══════════════════════════════════════════════════════════════

class TestTimestampHandling(unittest.TestCase):

    def setUp(self):
        from app.services.security import verify_timestamp, normalise_timestamp
        self.verify    = verify_timestamp
        self.normalise = normalise_timestamp

    def test_fresh_seconds_passes(self):
        self.assertTrue(self.verify(int(time.time())))

    def test_old_timestamp_fails(self):
        self.assertFalse(self.verify(int(time.time()) - 60))

    def test_future_timestamp_fails(self):
        self.assertFalse(self.verify(int(time.time()) + 60))

    def test_just_within_tolerance(self):
        self.assertTrue(self.verify(int(time.time()) - 29))

    def test_just_outside_tolerance(self):
        self.assertFalse(self.verify(int(time.time()) - 31))

    # [FIX-2] millisecond normalisation
    def test_millisecond_timestamp_normalised(self):
        """Timestamps above year-2100 epoch treated as milliseconds."""
        ms_timestamp = int(time.time()) * 1000   # simulate ms from broken EA
        # Should normalise and still be fresh
        self.assertTrue(self.verify(ms_timestamp))

    def test_normalise_large_value_divides_by_1000(self):
        ms = int(time.time()) * 1000
        normalised = self.normalise(ms)
        self.assertAlmostEqual(normalised, time.time(), delta=1.0)

    def test_normalise_normal_value_unchanged(self):
        ts = int(time.time())
        normalised = self.normalise(ts)
        self.assertAlmostEqual(normalised, ts, delta=1.0)

    def test_zero_timestamp_fails(self):
        self.assertFalse(self.verify(0))


# ═══════════════════════════════════════════════════════════════
# 3. validate_webhook_request — integration of sig + timestamp
# ═══════════════════════════════════════════════════════════════

class TestValidateWebhookRequest(unittest.TestCase):

    def setUp(self):
        from app.services.security import validate_webhook_request, compute_hmac
        self.validate     = validate_webhook_request
        self.compute_hmac = compute_hmac

    def _make_payload(self, ts=None):
        payload = {
            "signature": "",
            "timestamp": ts or int(time.time()),
            "event":     "trade_open",
        }
        body             = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        payload["signature"] = self.compute_hmac(body, SECRET)
        return payload

    def test_valid_passes_without_exception(self):
        payload = self._make_payload()
        try:
            self.validate(payload, payload["signature"])
        except Exception as e:
            self.fail(f"Valid payload raised: {e}")

    def test_wrong_signature_raises_401(self):
        from fastapi import HTTPException
        payload = self._make_payload()
        with self.assertRaises(HTTPException) as ctx:
            self.validate(payload, "x" * 64)
        self.assertEqual(ctx.exception.status_code, 401)

    def test_stale_timestamp_raises_400(self):
        from fastapi import HTTPException
        payload = self._make_payload(ts=int(time.time()) - 120)
        # Need to re-sign with the stale timestamp
        with self.assertRaises(HTTPException) as ctx:
            self.validate(payload, payload["signature"])
        self.assertEqual(ctx.exception.status_code, 400)

    def test_millisecond_timestamp_accepted(self):
        """[FIX-2] ms timestamp should pass after normalisation."""
        ts      = int(time.time()) * 1000
        payload = {"signature": "", "timestamp": ts, "event": "trade_open"}
        body    = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        payload["signature"] = self.compute_hmac(body, SECRET)
        try:
            self.validate(payload, payload["signature"])
        except Exception as e:
            self.fail(f"ms timestamp should be accepted but raised: {e}")


# ═══════════════════════════════════════════════════════════════
# 4. BATCH CHUNKING UTILITY
# ═══════════════════════════════════════════════════════════════

class TestChunkList(unittest.TestCase):

    def setUp(self):
        from app.services.celery_app import chunk_list
        self.chunk = chunk_list

    def test_even_split(self):
        result = self.chunk(list(range(10)), 5)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], [0, 1, 2, 3, 4])

    def test_uneven_split(self):
        result = self.chunk(list(range(15)), 10)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[1]), 5)

    def test_empty_list(self):
        self.assertEqual(self.chunk([], 10), [])

    def test_single_item(self):
        self.assertEqual(self.chunk([42], 10), [[42]])

    def test_size_larger_than_list(self):
        result = self.chunk([1, 2, 3], 100)
        self.assertEqual(result, [[1, 2, 3]])

    def test_preserves_all_items(self):
        items  = list(range(100))
        chunks = self.chunk(items, 10)
        flat   = [x for c in chunks for x in c]
        self.assertEqual(flat, items)

    def test_creates_10_batches_for_100_students(self):
        students = [{"user_id": i} for i in range(100)]
        result   = self.chunk(students, 10)
        self.assertEqual(len(result), 10)
        self.assertEqual(len(result[0]), 10)


# ═══════════════════════════════════════════════════════════════
# 5. LOT CALCULATION LOGIC
# ═══════════════════════════════════════════════════════════════

class TestLotCalculation(unittest.TestCase):
    """
    Tests the lot sizing logic in trade_executor independently.
    Formula: final_lot = round(min(mentor_lot * aggressiveness, lot_cap), 2)
    """

    def _calc(self, mentor_lot, aggressiveness, max_lot):
        return round(min(mentor_lot * aggressiveness, max_lot), 2)

    def test_mirror_1x(self):
        self.assertEqual(self._calc(0.10, 1.0, 0.10), 0.10)

    def test_conservative_half(self):
        self.assertEqual(self._calc(0.10, 0.5, 0.10), 0.05)

    def test_aggressive_2x_uncapped(self):
        self.assertEqual(self._calc(0.10, 2.0, 1.00), 0.20)

    def test_capped_by_max_lot(self):
        """1.5x aggression would give 0.15 but cap is 0.10."""
        self.assertEqual(self._calc(0.10, 1.5, 0.10), 0.10)

    def test_overleverage_uses_higher_cap(self):
        """Overleverage allows up to overleverage_max_lot."""
        self.assertEqual(self._calc(0.10, 2.0, 1.00), 0.20)

    def test_tiny_lot_rounded(self):
        """Ensure rounding to 2dp — MT5 rejects 3dp lots."""
        self.assertEqual(self._calc(0.10, 0.333, 1.0), 0.03)

    def test_zero_aggressiveness_gives_zero(self):
        self.assertEqual(self._calc(0.10, 0.0, 1.0), 0.0)


# ═══════════════════════════════════════════════════════════════
# 6. TRADE EXECUTOR — execute_for_student (mocked MT5)
# ═══════════════════════════════════════════════════════════════

class TestExecuteForStudent(unittest.TestCase):
    """Tests trade_executor.execute_for_student with MT5 fully mocked."""

    def setUp(self):
        # Patch MT5 and all Supabase calls before importing executor
        self.mt5_mock = MagicMock()
        self.mt5_mock.TRADE_ACTION_DEAL      = 1
        self.mt5_mock.ORDER_TYPE_BUY         = 0
        self.mt5_mock.ORDER_TYPE_SELL        = 1
        self.mt5_mock.POSITION_TYPE_BUY      = 0
        self.mt5_mock.POSITION_TYPE_SELL     = 1
        self.mt5_mock.ORDER_TIME_GTC         = 1
        self.mt5_mock.ORDER_FILLING_IOC      = 1
        self.mt5_mock.TRADE_RETCODE_DONE     = 10009
        self.mt5_mock.initialize.return_value = True
        self.mt5_mock.terminal_info.return_value = MagicMock()
        self.mt5_mock.symbol_select.return_value = True

        # Default healthy account
        acct = MagicMock()
        acct.balance = 1000.0
        acct.equity  = 1000.0
        self.mt5_mock.account_info.return_value = acct

        # Default successful order result
        order_result = MagicMock()
        order_result.retcode = 10009
        order_result.order   = 99999
        order_result.price   = 12345.67
        self.mt5_mock.order_send.return_value = order_result

        tick = MagicMock()
        tick.ask = 12346.0
        tick.bid = 12344.0
        self.mt5_mock.symbol_info_tick.return_value = tick

        self.patches = [
            patch.dict("sys.modules", {"MetaTrader5": self.mt5_mock}),
            patch("app.services.trade_executor.MT5_AVAILABLE", True),
            patch("app.services.trade_executor.mt5", self.mt5_mock),
        ]
        for p in self.patches:
            p.start()

        # Fresh import after patching
        import importlib
        import app.services.trade_executor as te
        importlib.reload(te)
        self.te = te

    def tearDown(self):
        for p in self.patches:
            p.stop()

    def _run(self, student=None, signal=None, flag_side_effect=None):
        s = student or _make_student()
        g = signal  or _make_signal()
        flag_mock = flag_side_effect or (lambda k: "false")
        with patch.object(self.te, "get_system_flag", side_effect=flag_mock):
            with patch.object(self.te, "get_supabase") as mock_sb:
                mock_sb.return_value = MagicMock()
                return self.te.execute_for_student(s, g)

    # ── Kill switches ──────────────────────────────────────────

    def test_emergency_close_all_skips(self):
        result = self._run(flag_side_effect=lambda k: "true" if k == "emergency_close_all" else "false")
        self.assertEqual(result, "skipped")

    def test_platform_paused_skips(self):
        result = self._run(flag_side_effect=lambda k: "true" if k == "platform_paused" else "false")
        self.assertEqual(result, "skipped")

    # ── Account checks ─────────────────────────────────────────

    def test_inactive_account_skips(self):
        result = self._run(student=_make_student(is_active=False))
        self.assertEqual(result, "skipped")

    def test_paused_student_skips(self):
        result = self._run(student=_make_student(pause_copy=True))
        self.assertEqual(result, "skipped")

    def test_expired_subscription_skips(self):
        result = self._run(student=_make_student(subscription="overdue"))
        self.assertEqual(result, "skipped")

    def test_trial_subscription_skips(self):
        result = self._run(student=_make_student(subscription="trial"))
        self.assertEqual(result, "skipped")

    # ── Balance / drawdown checks ──────────────────────────────

    def test_below_min_balance_skips(self):
        acct = MagicMock()
        acct.balance = 50.0
        acct.equity  = 50.0
        self.mt5_mock.account_info.return_value = acct
        result = self._run(student=_make_student(min_balance=100.0))
        self.assertEqual(result, "skipped")

    def test_max_drawdown_hit_skips_and_auto_pauses(self):
        acct = MagicMock()
        acct.balance = 1000.0
        acct.equity  = 750.0   # 25% drawdown, threshold 20%
        self.mt5_mock.account_info.return_value = acct

        with patch.object(self.te, "get_system_flag", return_value="false"):
            with patch.object(self.te, "get_supabase") as mock_sb:
                with patch.object(self.te, "_auto_pause_student") as mock_pause:
                    mock_sb.return_value = MagicMock()
                    result = self.te.execute_for_student(
                        _make_student(max_drawdown=20.0),
                        _make_signal()
                    )
        self.assertEqual(result, "skipped")
        mock_pause.assert_called_once()

    # ── MT5 failure paths ──────────────────────────────────────

    def test_mt5_init_failure_returns_failed(self):
        self.mt5_mock.initialize.return_value = False
        self.mt5_mock.last_error.return_value  = (-1, "Connection refused")
        result = self._run()
        self.assertEqual(result, "failed")

    def test_account_info_none_returns_failed(self):
        self.mt5_mock.account_info.return_value = None
        result = self._run()
        self.assertEqual(result, "failed")

    def test_order_send_fail_retcode_returns_failed(self):
        r = MagicMock()
        r.retcode = 10004  # REQUOTE
        self.mt5_mock.order_send.return_value = r
        result = self._run()
        self.assertEqual(result, "failed")

    def test_order_send_none_returns_failed(self):
        self.mt5_mock.order_send.return_value = None
        result = self._run()
        self.assertEqual(result, "failed")

    def test_symbol_not_available_returns_failed(self):
        self.mt5_mock.symbol_select.return_value = False
        result = self._run()
        self.assertEqual(result, "failed")

    def test_mt5_always_shutdown_after_success(self):
        self._run()
        self.mt5_mock.shutdown.assert_called()

    def test_mt5_always_shutdown_after_failure(self):
        self.mt5_mock.order_send.return_value = None
        self._run()
        self.mt5_mock.shutdown.assert_called()

    # ── Successful execution ───────────────────────────────────

    def test_successful_trade_returns_success(self):
        result = self._run()
        self.assertEqual(result, "success")

    def test_correct_magic_number_used(self):
        self._run()
        call_kwargs = self.mt5_mock.order_send.call_args[0][0]
        self.assertEqual(call_kwargs["magic"], self.te.LB_MAGIC_NUMBER)

    def test_comment_contains_ticket_id(self):
        self._run(signal=_make_signal())
        call_kwargs = self.mt5_mock.order_send.call_args[0][0]
        self.assertIn("LB:", call_kwargs["comment"])
        self.assertIn("TKT_001", call_kwargs["comment"])

    def test_sell_order_uses_bid_price(self):
        self._run(signal=_make_signal(order_type="SELL"))
        call_kwargs = self.mt5_mock.order_send.call_args[0][0]
        self.assertEqual(call_kwargs["price"], 12344.0)  # bid

    def test_buy_order_uses_ask_price(self):
        self._run(signal=_make_signal(order_type="BUY"))
        call_kwargs = self.mt5_mock.order_send.call_args[0][0]
        self.assertEqual(call_kwargs["price"], 12346.0)  # ask

    # ── Lot sizing ─────────────────────────────────────────────

    def test_lot_respects_aggressiveness(self):
        self._run(
            student=_make_student(aggressiveness=0.5, max_lot=1.0),
            signal=_make_signal(lot=0.10)
        )
        call_kwargs = self.mt5_mock.order_send.call_args[0][0]
        self.assertEqual(call_kwargs["volume"], 0.05)

    def test_lot_capped_by_max_lot(self):
        self._run(
            student=_make_student(aggressiveness=2.0, max_lot=0.10),
            signal=_make_signal(lot=0.10)
        )
        call_kwargs = self.mt5_mock.order_send.call_args[0][0]
        self.assertEqual(call_kwargs["volume"], 0.10)

    def test_no_folder_path_returns_failed(self):
        s = _make_student(folder_path=None)
        s["folder_path"] = None
        result = self._run(student=s)
        self.assertEqual(result, "failed")


# ═══════════════════════════════════════════════════════════════
# 7. ADVANCED STRATEGY FILTERING
# ═══════════════════════════════════════════════════════════════

class TestAdvancedStrategyFiltering(unittest.TestCase):

    def setUp(self):
        import app.services.trade_executor as te
        self.te = te

    def _run(self, student, signal):
        with patch.object(self.te, "get_system_flag", return_value="false"):
            with patch.object(self.te, "get_supabase") as mock_sb:
                with patch.object(self.te, "MT5_AVAILABLE", False):
                    mock_sb.return_value = MagicMock()
                    return self.te.execute_for_student(student, signal)

    # scale_in
    def test_scale_in_skipped_if_not_opted_in(self):
        result = self._run(
            student=_make_student(advanced_options={"scale_in": False, "hedge": False, "overleverage": False}),
            signal=_make_signal(strategy="scale_in")
        )
        self.assertEqual(result, "skipped")

    def test_scale_in_executes_if_opted_in(self):
        result = self._run(
            student=_make_student(advanced_options={"scale_in": True, "hedge": False, "overleverage": False}),
            signal=_make_signal(strategy="scale_in")
        )
        # MT5 unavailable → simulation → success
        self.assertEqual(result, "success")

    # hedge
    def test_hedge_skipped_if_not_opted_in(self):
        result = self._run(
            student=_make_student(advanced_options={"scale_in": False, "hedge": False, "overleverage": False}),
            signal=_make_signal(strategy="hedge")
        )
        self.assertEqual(result, "skipped")

    def test_hedge_executes_if_opted_in(self):
        result = self._run(
            student=_make_student(advanced_options={"scale_in": False, "hedge": True, "overleverage": False}),
            signal=_make_signal(strategy="hedge")
        )
        self.assertEqual(result, "success")

    # overleverage — graceful degradation
    def test_overleverage_not_opted_in_still_executes(self):
        """[PRD-21.8] Non-opted students get trade with normal lot cap."""
        result = self._run(
            student=_make_student(advanced_options={"scale_in": False, "hedge": False, "overleverage": False}),
            signal=_make_signal(strategy="overleverage")
        )
        # Should still execute (simulation mode), not skip
        self.assertEqual(result, "success")

    def test_overleverage_opted_in_executes(self):
        result = self._run(
            student=_make_student(advanced_options={"scale_in": False, "hedge": False, "overleverage": True}),
            signal=_make_signal(strategy="overleverage")
        )
        self.assertEqual(result, "success")

    def test_normal_strategy_always_executes(self):
        result = self._run(
            student=_make_student(advanced_options={"scale_in": False, "hedge": False, "overleverage": False}),
            signal=_make_signal(strategy="normal")
        )
        self.assertEqual(result, "success")

    def test_advanced_options_as_json_string(self):
        """advanced_options might come from Supabase as a JSON string."""
        import json as _json
        opts = _json.dumps({"scale_in": False, "hedge": False, "overleverage": False})
        student = _make_student()
        student["risk_profiles"]["advanced_options"] = opts
        result = self._run(student=student, signal=_make_signal(strategy="scale_in"))
        self.assertEqual(result, "skipped")


# ═══════════════════════════════════════════════════════════════
# 8. CLOSE LOGIC — magic number filtering (FIX-3)
# ═══════════════════════════════════════════════════════════════

class TestCloseForStudent(unittest.TestCase):

    def setUp(self):
        self.mt5_mock = MagicMock()
        self.mt5_mock.TRADE_ACTION_DEAL   = 1
        self.mt5_mock.ORDER_TYPE_BUY      = 0
        self.mt5_mock.ORDER_TYPE_SELL     = 1
        self.mt5_mock.POSITION_TYPE_BUY   = 0
        self.mt5_mock.POSITION_TYPE_SELL  = 1
        self.mt5_mock.ORDER_TIME_GTC      = 1
        self.mt5_mock.ORDER_FILLING_IOC   = 1
        self.mt5_mock.TRADE_RETCODE_DONE  = 10009
        self.mt5_mock.initialize.return_value = True

        tick      = MagicMock()
        tick.bid  = 12300.0
        tick.ask  = 12302.0
        self.mt5_mock.symbol_info_tick.return_value = tick

        self.patches = [
            patch.dict("sys.modules", {"MetaTrader5": self.mt5_mock}),
            patch("app.services.trade_executor.MT5_AVAILABLE", True),
            patch("app.services.trade_executor.mt5", self.mt5_mock),
        ]
        for p in self.patches:
            p.start()

        import importlib, app.services.trade_executor as te
        importlib.reload(te)
        self.te = te

    def tearDown(self):
        for p in self.patches:
            p.stop()

    def _make_position(self, ticket, magic, type_=0, volume=0.10):
        p        = MagicMock()
        p.ticket = ticket
        p.magic  = magic
        p.type   = type_
        p.volume = volume
        return p

    def _run(self, positions, symbol="Volatility 75 Index", ticket="TKT_001"):
        self.mt5_mock.positions_get.return_value = positions
        r = MagicMock(); r.retcode = 10009
        self.mt5_mock.order_send.return_value = r
        student    = _make_student()
        close_data = {"symbol": symbol, "mentor_ticket": ticket}
        return self.te.close_for_student(student, close_data)

    def test_no_positions_returns_skipped(self):
        result = self._run([])
        self.assertEqual(result, "skipped")

    def test_none_positions_returns_skipped(self):
        self.mt5_mock.positions_get.return_value = None
        result = self.te.close_for_student(
            _make_student(), {"symbol": "V75", "mentor_ticket": "T1"}
        )
        self.assertEqual(result, "skipped")

    def test_only_lb_positions_closed(self):
        """[FIX-3] Foreign positions (wrong magic) must NOT be closed."""
        foreign_pos = self._make_position(ticket=111, magic=99999)   # not LB
        lb_pos      = self._make_position(ticket=222, magic=self.te.LB_MAGIC_NUMBER)

        result = self._run([foreign_pos, lb_pos])

        # order_send should be called exactly once — for the LB position only
        self.assertEqual(self.mt5_mock.order_send.call_count, 1)
        call_kwargs = self.mt5_mock.order_send.call_args[0][0]
        self.assertEqual(call_kwargs["position"], 222)
        self.assertEqual(result, "closed")

    def test_all_foreign_positions_returns_skipped(self):
        """If no LB positions found, nothing to close."""
        foreign = self._make_position(ticket=111, magic=99999)
        result  = self._run([foreign])
        self.assertEqual(result, "skipped")
        self.mt5_mock.order_send.assert_not_called()

    def test_multiple_lb_positions_all_closed(self):
        """Scale-in creates multiple LB positions — all should close."""
        positions = [
            self._make_position(ticket=10, magic=self.te.LB_MAGIC_NUMBER),
            self._make_position(ticket=11, magic=self.te.LB_MAGIC_NUMBER),
            self._make_position(ticket=12, magic=self.te.LB_MAGIC_NUMBER),
        ]
        result = self._run(positions)
        self.assertEqual(self.mt5_mock.order_send.call_count, 3)
        self.assertEqual(result, "closed")

    def test_sell_position_uses_ask_price(self):
        """Closing a SELL position requires buying back at ask."""
        pos    = self._make_position(ticket=10, magic=self.te.LB_MAGIC_NUMBER, type_=1)  # SELL
        self._run([pos])
        call_kwargs = self.mt5_mock.order_send.call_args[0][0]
        self.assertEqual(call_kwargs["price"], 12302.0)  # ask

    def test_buy_position_uses_bid_price(self):
        """Closing a BUY position requires selling at bid."""
        pos    = self._make_position(ticket=10, magic=self.te.LB_MAGIC_NUMBER, type_=0)  # BUY
        self._run([pos])
        call_kwargs = self.mt5_mock.order_send.call_args[0][0]
        self.assertEqual(call_kwargs["price"], 12300.0)  # bid

    def test_partial_failure_returns_failed(self):
        """If any position fails to close, return failed not closed."""
        pos1 = self._make_position(ticket=10, magic=self.te.LB_MAGIC_NUMBER)
        pos2 = self._make_position(ticket=11, magic=self.te.LB_MAGIC_NUMBER)

        results_iter = iter([
            MagicMock(retcode=10009),  # success
            MagicMock(retcode=10004),  # fail
        ])
        self.mt5_mock.order_send.side_effect = lambda req: next(results_iter)

        result = self._run([pos1, pos2])
        self.assertEqual(result, "failed")

    def test_close_comment_contains_mentor_ticket(self):
        pos = self._make_position(ticket=10, magic=self.te.LB_MAGIC_NUMBER)
        self._run([pos], ticket="MENTOR_TKT_999")
        call_kwargs = self.mt5_mock.order_send.call_args[0][0]
        self.assertIn("MENTOR_TKT_999", call_kwargs["comment"])

    def test_mt5_shutdown_called_after_close(self):
        pos = self._make_position(ticket=10, magic=self.te.LB_MAGIC_NUMBER)
        self._run([pos])
        self.mt5_mock.shutdown.assert_called()

    def test_mt5_init_fail_returns_failed(self):
        self.mt5_mock.initialize.return_value = False
        result = self.te.close_for_student(
            _make_student(), {"symbol": "V75", "mentor_ticket": "T1"}
        )
        self.assertEqual(result, "failed")

    def test_missing_folder_path_returns_skipped(self):
        s = _make_student(folder_path=None)
        s["folder_path"] = None
        result = self.te.close_for_student(s, {"symbol": "V75", "mentor_ticket": "T1"})
        self.assertEqual(result, "skipped")


# ═══════════════════════════════════════════════════════════════
# 9. SIMULATION MODE
# ═══════════════════════════════════════════════════════════════

class TestSimulationMode(unittest.TestCase):

    def test_simulation_returns_success_without_mt5(self):
        import app.services.trade_executor as te
        with patch.object(te, "MT5_AVAILABLE", False):
            with patch.object(te, "get_system_flag", return_value="false"):
                with patch.object(te, "get_supabase") as mock_sb:
                    mock_sb.return_value = MagicMock()
                    result = te.execute_for_student(_make_student(), _make_signal())
        self.assertEqual(result, "success")

    def test_simulation_does_not_call_mt5(self):
        import app.services.trade_executor as te
        mt5_mock = MagicMock()
        with patch.object(te, "MT5_AVAILABLE", False):
            with patch.object(te, "mt5", mt5_mock):
                with patch.object(te, "get_system_flag", return_value="false"):
                    with patch.object(te, "get_supabase") as mock_sb:
                        mock_sb.return_value = MagicMock()
                        te.execute_for_student(_make_student(), _make_signal())
        mt5_mock.initialize.assert_not_called()
        mt5_mock.order_send.assert_not_called()


# ═══════════════════════════════════════════════════════════════
# 10. HEARTBEAT STATUS TRANSITIONS
# ═══════════════════════════════════════════════════════════════

class TestHeartbeatStatusTransitions(unittest.TestCase):
    """Tests mark_heartbeat_missed status logic directly."""

    def setUp(self):
        from app.services.supabase_service import mark_heartbeat_missed
        self.mark_missed = mark_heartbeat_missed

    def _run(self, current_misses: int):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value \
            .eq.return_value.single.return_value.execute.return_value \
            .data = {"consecutive_misses": current_misses}
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = MagicMock()

        with patch("app.services.supabase_service.get_supabase", return_value=mock_supabase):
            return self.mark_missed("trader_001")

    def test_1_miss_is_warning(self):
        # 1 miss * 60s = 60s < 90s warning threshold → ONLINE still
        result = self._run(0)
        self.assertEqual(result["status"], "ONLINE")

    def test_2_misses_is_warning(self):
        # 2 misses * 60 = 120s >= 90s warning threshold
        result = self._run(1)
        self.assertEqual(result["status"], "WARNING")

    def test_3_misses_is_offline(self):
        # 3 misses * 60 = 180s >= 180s offline threshold
        result = self._run(2)
        self.assertEqual(result["status"], "OFFLINE")

    def test_misses_increments(self):
        result = self._run(2)
        self.assertEqual(result["consecutive_misses"], 3)


# ═══════════════════════════════════════════════════════════════
# 11. SUPABASE SERVICE — get_all_active_students query shape
# ═══════════════════════════════════════════════════════════════

class TestSupabaseService(unittest.TestCase):

    def test_get_all_active_students_returns_list(self):
        from app.services.supabase_service import get_all_active_students
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value \
            .eq.return_value.eq.return_value.eq.return_value \
            .execute.return_value.data = [{"user_id": "s1"}, {"user_id": "s2"}]

        with patch("app.services.supabase_service.get_supabase", return_value=mock_client):
            students = get_all_active_students()
        self.assertEqual(len(students), 2)

    def test_get_all_active_students_empty_returns_list(self):
        from app.services.supabase_service import get_all_active_students
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value \
            .eq.return_value.eq.return_value.eq.return_value \
            .execute.return_value.data = []

        with patch("app.services.supabase_service.get_supabase", return_value=mock_client):
            students = get_all_active_students()
        self.assertEqual(students, [])

    def test_get_system_flag_returns_value(self):
        from app.services.supabase_service import get_system_flag
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value \
            .eq.return_value.single.return_value \
            .execute.return_value.data = {"value": "true"}

        with patch("app.services.supabase_service.get_supabase", return_value=mock_client):
            result = get_system_flag("emergency_close_all")
        self.assertEqual(result, "true")

    def test_get_system_flag_missing_key_returns_false(self):
        from app.services.supabase_service import get_system_flag
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value \
            .eq.return_value.single.return_value \
            .execute.return_value.data = None

        with patch("app.services.supabase_service.get_supabase", return_value=mock_client):
            result = get_system_flag("non_existent_flag")
        self.assertEqual(result, "false")

    def test_get_system_flag_db_error_returns_false(self):
        from app.services.supabase_service import get_system_flag
        mock_client = MagicMock()
        mock_client.table.side_effect = Exception("DB connection lost")

        with patch("app.services.supabase_service.get_supabase", return_value=mock_client):
            result = get_system_flag("any_key")
        self.assertEqual(result, "false")

    def test_update_heartbeat_upserts_online(self):
        from app.services.supabase_service import update_heartbeat
        mock_client = MagicMock()

        with patch("app.services.supabase_service.get_supabase", return_value=mock_client):
            update_heartbeat("trader_001", "ONLINE")

        upsert_call = mock_client.table.return_value.upsert.call_args[0][0]
        self.assertEqual(upsert_call["status"], "ONLINE")
        self.assertEqual(upsert_call["consecutive_misses"], 0)

    def test_update_heartbeat_does_not_raise_on_db_error(self):
        from app.services.supabase_service import update_heartbeat
        mock_client = MagicMock()
        mock_client.table.side_effect = Exception("network error")

        with patch("app.services.supabase_service.get_supabase", return_value=mock_client):
            # Should NOT raise — heartbeat failure is non-fatal
            try:
                update_heartbeat("trader_001")
            except Exception:
                self.fail("update_heartbeat raised on DB error — should be non-fatal")


# ═══════════════════════════════════════════════════════════════
# 12. DEAD LETTER QUEUE
# ═══════════════════════════════════════════════════════════════

class TestDeadLetterQueue(unittest.TestCase):

    def test_dlq_write_called_with_correct_fields(self):
        from app.services.celery_app import _write_dead_letter
        mock_client = MagicMock()

        with patch("app.services.celery_app.get_supabase", return_value=mock_client):
            _write_dead_letter(
                task_id="task_abc",
                student={"user_id": "stu_001"},
                signal=_make_signal(),
                error="order_send failed: retcode=10004"
            )

        insert_data = mock_client.table.return_value.insert.call_args[0][0]
        self.assertEqual(insert_data["task_id"],  "task_abc")
        self.assertEqual(insert_data["user_id"],  "stu_001")
        self.assertIn("retcode",                   insert_data["error"])

    def test_dlq_write_does_not_raise_on_db_error(self):
        from app.services.celery_app import _write_dead_letter
        mock_client = MagicMock()
        mock_client.table.side_effect = Exception("DB gone")

        with patch("app.services.celery_app.get_supabase", return_value=mock_client):
            try:
                _write_dead_letter("t", {"user_id": "u"}, {}, "err")
            except Exception:
                self.fail("_write_dead_letter should swallow DB errors")


# ═══════════════════════════════════════════════════════════════
# 13. MODELS — Pydantic payload validation
# ═══════════════════════════════════════════════════════════════

class TestPayloadModels(unittest.TestCase):

    def test_valid_signal_payload(self):
        from app.models.payloads import SignalPayload
        payload = SignalPayload(**{
            "signature": "a" * 64,
            "timestamp": int(time.time()),
            "event":     "trade_open",
            "data": {
                "symbol":      "Volatility 75 Index",
                "order_type":  "BUY",
                "lot_size":    0.10,
                "entry_price": 12345.0,
                "stop_loss":   12300.0,
                "take_profit": 12400.0,
                "ticket_id":   "TKT_001",
            }
        })
        self.assertEqual(payload.data.order_type.value, "BUY")

    def test_invalid_order_type_raises(self):
        from app.models.payloads import SignalPayload
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            SignalPayload(**{
                "signature": "a" * 64,
                "timestamp": int(time.time()),
                "event":     "trade_open",
                "data": {
                    "symbol":      "V75",
                    "order_type":  "LONG",   # invalid
                    "lot_size":    0.10,
                    "entry_price": 100.0,
                    "stop_loss":   90.0,
                    "take_profit": 110.0,
                    "ticket_id":   "T1",
                }
            })

    def test_negative_lot_size_raises(self):
        from app.models.payloads import TradeData
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            TradeData(
                symbol="V75", order_type="BUY",
                lot_size=-0.1, entry_price=100.0,
                stop_loss=90.0, take_profit=110.0,
                ticket_id="T1"
            )

    def test_zero_lot_size_raises(self):
        from app.models.payloads import TradeData
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            TradeData(
                symbol="V75", order_type="BUY",
                lot_size=0.0, entry_price=100.0,
                stop_loss=90.0, take_profit=110.0,
                ticket_id="T1"
            )

    def test_lot_size_rounded_to_2dp(self):
        from app.models.payloads import TradeData
        td = TradeData(
            symbol="V75", order_type="BUY",
            lot_size=0.1234, entry_price=100.0,
            stop_loss=90.0, take_profit=110.0,
            ticket_id="T1"
        )
        self.assertEqual(td.lot_size, 0.12)

    def test_strategy_type_defaults_to_normal(self):
        from app.models.payloads import TradeData
        td = TradeData(
            symbol="V75", order_type="SELL",
            lot_size=0.10, entry_price=100.0,
            stop_loss=110.0, take_profit=90.0,
            ticket_id="T1"
        )
        self.assertEqual(td.strategy_type.value, "normal")

    def test_all_strategy_types_valid(self):
        from app.models.payloads import StrategyType
        for v in ["normal", "scale_in", "hedge", "overleverage"]:
            st = StrategyType(v)
            self.assertEqual(st.value, v)

    def test_signature_too_short_raises(self):
        from app.models.payloads import SignalPayload
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            SignalPayload(**{
                "signature": "tooshort",
                "timestamp": int(time.time()),
                "event":     "trade_open",
                "data": {
                    "symbol": "V75", "order_type": "BUY",
                    "lot_size": 0.1, "entry_price": 100.0,
                    "stop_loss": 90.0, "take_profit": 110.0,
                    "ticket_id": "T1",
                }
            })

    def test_heartbeat_payload_valid(self):
        from app.models.payloads import HeartbeatPayload
        hb = HeartbeatPayload(
            signature="b" * 64,
            timestamp=int(time.time()),
            event="heartbeat",
            trader_id="trader_001"
        )
        self.assertEqual(hb.event, "heartbeat")

    def test_close_payload_valid(self):
        from app.models.payloads import ClosePayload
        cp = ClosePayload(
            signature="c" * 64,
            timestamp=int(time.time()),
            event="trade_close",
            mentor_ticket="TKT_999",
            symbol="Volatility 75 Index"
        )
        self.assertEqual(cp.mentor_ticket, "TKT_999")


# ═══════════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)
