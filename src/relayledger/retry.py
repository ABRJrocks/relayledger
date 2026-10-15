"""Bounded equal-jitter backoff and Retry-After interpretation."""

import math
import random
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime

from .validation import RelayError, instant, integer

MAX_RETRY_AFTER_MS = 86_400_000


@dataclass(frozen=True)
class RetryPolicy:
    base_ms: int = 1000
    cap_ms: int = 60_000

    def __post_init__(self):
        integer(self.base_ms, 1, 3_600_000)
        integer(self.cap_ms, self.base_ms, 3_600_000)

    def delay_ms(self, attempt, *, random_value=None):
        integer(attempt, 1, 1000)
        value = random.random() if random_value is None else random_value
        if (
            type(value) not in (float, int)
            or not math.isfinite(value)
            or not 0 <= value <= 1
        ):
            raise RelayError("Jitter sample must be between zero and one")
        ceiling = min(self.cap_ms, self.base_ms * 2 ** min(attempt - 1, 32))
        floor = max(1, ceiling // 2)
        return floor + int((ceiling - floor) * value)


def retry_after_ms(header, *, now_ms):
    now = instant(now_ms)
    if not isinstance(header, str) or len(header) > 128:
        return None
    try:
        text = header.strip()
        if text.isascii() and text.isdigit():
            delay = int(text) * 1000
        else:
            date = parsedate_to_datetime(text)
            if date.tzinfo is None:
                return None
            delay = max(0, int(date.astimezone(timezone.utc).timestamp() * 1000) - now)
    except (ValueError, TypeError, OverflowError):
        return None
    if delay > MAX_RETRY_AFTER_MS:
        raise RelayError("Retry-After exceeds the supported one-day delay")
    return delay
