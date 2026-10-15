"""Receiver receipts belong in the same transaction as local business effects."""

import hashlib
import json
import uuid
from datetime import datetime

from .database import check_schema, require_transaction
from .validation import MAX_BODY, Conflict, RelayError, canonical, instant, label


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RelayError("Duplicate JSON object member")
        result[key] = value
    return result


def _invalid(value):
    raise RelayError("Non-finite JSON number")


def decode_event(body):
    if not isinstance(body, bytes) or len(body) > MAX_BODY:
        raise RelayError("Invalid event body size")
    try:
        event = json.loads(
            body.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_invalid
        )
        fields = {
            "specversion",
            "id",
            "source",
            "type",
            "time",
            "datacontenttype",
            "data",
        }
        if not isinstance(event, dict) or set(event) != fields:
            raise RelayError("Invalid event envelope fields")
        if (
            event["specversion"] != "1.0"
            or event["source"] != "urn:relayledger"
            or event["datacontenttype"] != "application/json"
        ):
            raise RelayError("Unsupported event envelope profile")
        if (
            not isinstance(event["id"], str)
            or str(uuid.UUID(event["id"])) != event["id"]
        ):
            raise RelayError("Event ID must be a canonical UUID")
        label(event["type"])
        if not isinstance(event["time"], str) or not event["time"].endswith("Z"):
            raise RelayError("Event timestamp must be UTC")
        datetime.fromisoformat(event["time"].replace("Z", "+00:00"))
        if not isinstance(event["data"], dict):
            raise RelayError("Event data must be an object")
        canonical(event)
        return event
    except (ValueError, TypeError, AttributeError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, RelayError):
            raise
        raise RelayError("Cannot decode the event envelope") from exc


def record_once(database, body, *, now_ms=None):
    require_transaction(database)
    check_schema(database)
    event = decode_event(body)
    digest = hashlib.sha256(body).hexdigest()
    existing = database.execute(
        "SELECT body_hash FROM relayledger_inbox WHERE source=? AND event_id=?",
        (event["source"], event["id"]),
    ).fetchone()
    if existing:
        if existing[0] != digest:
            raise Conflict("Event ID was previously received with different bytes")
        return False
    database.execute(
        "INSERT INTO relayledger_inbox(source,event_id,body_hash,recorded_at_ms) VALUES(?,?,?,?)",
        (event["source"], event["id"], digest, instant(now_ms)),
    )
    return True
