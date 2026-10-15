"""Bounded values and deterministic event intent."""

import json
import math
import re
import time
from datetime import datetime, timezone

MAX_BODY = 1024 * 1024
LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}\Z")


class RelayError(ValueError):
    """Invalid configuration or an unsafe state transition."""


class Conflict(RelayError):
    """An existing idempotency key has different intent."""


class LeaseLost(RelayError):
    """Worker no longer owns a live delivery lease."""


def label(value):
    if not isinstance(value, str) or not LABEL.fullmatch(value):
        raise RelayError("Expected a safe identifier of 1 to 128 characters")
    return value


def integer(value, minimum=0, maximum=4_102_444_800_000):
    if type(value) is not int or not minimum <= value <= maximum:
        raise RelayError("Integer outside supported bounds")
    return value


def instant(value=None):
    return integer(time.time_ns() // 1_000_000 if value is None else value)


def canonical(value):
    remaining = 10_000

    def validate(item, depth=0):
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > 32:
            raise RelayError("JSON exceeds item or nesting limit")
        if isinstance(item, dict):
            if any(not isinstance(key, str) or len(key) > MAX_BODY for key in item):
                raise RelayError("JSON object keys must be bounded strings")
            for child in item.values():
                validate(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                validate(child, depth + 1)
        elif isinstance(item, str):
            if len(item) > MAX_BODY:
                raise RelayError("JSON string exceeds byte budget")
        elif type(item) is float:
            if not math.isfinite(item):
                raise RelayError("JSON numbers must be finite")
        elif item is not None and type(item) not in (bool, int):
            raise RelayError("Payload contains an unsupported JSON value")

    validate(value)
    try:
        parts, size = [], 0
        encoder = json.JSONEncoder(
            sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        for chunk in encoder.iterencode(value):
            raw = chunk.encode("utf-8")
            size += len(raw)
            if size > MAX_BODY:
                raise RelayError("JSON exceeds one MiB")
            parts.append(raw)
        return b"".join(parts)
    except (ValueError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, RelayError):
            raise
        raise RelayError("Cannot encode a bounded JSON payload") from exc


def timestamp(milliseconds):
    return (
        datetime.fromtimestamp(instant(milliseconds) / 1000, timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
