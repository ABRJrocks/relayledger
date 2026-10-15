"""Timestamped HMAC over the exact immutable delivery bytes."""

import hashlib
import hmac
import re

from .validation import MAX_BODY, RelayError, instant, integer


def validate_secret(secret):
    if not isinstance(secret, bytes) or not 32 <= len(secret) <= 4096:
        raise RelayError("Signing secret must contain 32 to 4096 bytes")


def sign(body, secret, *, now_ms=None):
    validate_secret(secret)
    if not isinstance(body, bytes) or len(body) > MAX_BODY:
        raise RelayError("Invalid signature body")
    seconds = instant(now_ms) // 1000
    digest = hmac.new(
        secret, str(seconds).encode() + b"." + body, hashlib.sha256
    ).hexdigest()
    return f"t={seconds},v1={digest}"


def verify(body, header, secret, *, now_ms=None, tolerance_seconds=300):
    validate_secret(secret)
    integer(tolerance_seconds, 0, 3600)
    if (
        not isinstance(body, bytes)
        or len(body) > MAX_BODY
        or not isinstance(header, str)
    ):
        return False
    match = re.fullmatch(r"t=([0-9]{1,12}),v1=([0-9a-f]{64})", header)
    if not match:
        return False
    seconds = int(match.group(1))
    if abs(instant(now_ms) // 1000 - seconds) > tolerance_seconds:
        return False
    expected = hmac.new(
        secret, str(seconds).encode() + b"." + body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, match.group(2))
