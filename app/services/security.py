# app/services/security.py
# ============================================================
# LEAN-BRIDGE — HMAC SIGNATURE VERIFICATION
#
# FIXES APPLIED:
# [FIX-1] Removed unused `Request` import from fastapi
# [FIX-2] verify_timestamp() now accepts int|float and handles
#         millisecond timestamps from MT5 builds that return ms
#         instead of seconds (TimeCurrent() vs GetTickCount()).
#         Any value > year-2100 epoch is divided by 1000.
# ============================================================

import hmac
import hashlib
import time
import json
import logging
from fastapi import HTTPException
from app.config import get_settings

logger = logging.getLogger(__name__)

# Largest plausible Unix timestamp in *seconds* (year 2100)
_MAX_UNIX_SECONDS: int = 4_102_444_800


def compute_hmac(body: bytes, secret: str) -> str:
    """
    Compute HMAC-SHA256 of raw bytes using the shared secret.
    Returns lowercase hex digest (64 characters).
    """
    return hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256
    ).hexdigest()


def verify_signature(provided_signature: str, body: bytes, secret: str | None = None) -> bool:
    """
    Constant-time comparison of provided vs computed HMAC.
    Uses hmac.compare_digest to prevent timing attacks.
    """
    if secret is None:
        secret = get_settings().webhook_secret
    expected = compute_hmac(body, secret)
    return hmac.compare_digest(
        provided_signature.lower(),
        expected.lower()
    )


def normalise_timestamp(timestamp: int | float) -> float:
    """
    [FIX-2] Guard against MT5 builds that send milliseconds.

    MT5's TimeCurrent() returns seconds since epoch (correct).
    Some wrappers accidentally send GetTickCount() or multiply
    by 1000, producing a millisecond value. If the raw value
    is larger than the year-2100 second epoch we assume ms and
    divide by 1000.

    Returns the timestamp normalised to seconds (float).
    """
    ts = float(timestamp)
    if ts > _MAX_UNIX_SECONDS:
        logger.warning(
            "Timestamp %.0f looks like milliseconds — dividing by 1000. "
            "Fix the EA to send TimeCurrent() not GetTickCount().", ts
        )
        ts /= 1000.0
    return ts


def verify_timestamp(timestamp: int | float) -> bool:
    """
    Reject signals outside the freshness window.
    Normalises ms→s before comparison.
    Default tolerance: 30 seconds (SIGNAL_TIMESTAMP_TOLERANCE_SECONDS).
    """
    settings   = get_settings()
    normalised = normalise_timestamp(timestamp)
    age        = abs(time.time() - normalised)
    return age <= settings.signal_timestamp_tolerance_seconds


def validate_webhook_request(payload_dict: dict, provided_signature: str) -> None:
    """
    Full webhook validation — called at the top of every webhook handler.

    Steps:
    1. Re-build canonical body (signature field blanked to "")
    2. Verify HMAC-SHA256 signature
    3. Check timestamp freshness (with ms normalisation)

    Raises HTTPException(401) on bad signature.
    Raises HTTPException(400) on stale timestamp.
    """
    signing_dict = {**payload_dict, "signature": ""}
    body_bytes   = json.dumps(
        signing_dict, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")

    if not verify_signature(provided_signature, body_bytes):
        logger.warning(
            "Rejected webhook — invalid HMAC. Prefix: %s",
            provided_signature[:16] + "..."
        )
        raise HTTPException(status_code=401, detail="Invalid signature")

    timestamp = payload_dict.get("timestamp", 0)
    if not verify_timestamp(timestamp):
        age = abs(time.time() - normalise_timestamp(timestamp))
        logger.warning(
            "Rejected webhook — stale signal. Age: %.1fs (tolerance: %ds)",
            age, get_settings().signal_timestamp_tolerance_seconds
        )
        raise HTTPException(
            status_code=400,
            detail=f"Stale signal rejected. Age: {age:.1f}s"
        )

    logger.debug("Webhook validated — HMAC and timestamp OK.")
