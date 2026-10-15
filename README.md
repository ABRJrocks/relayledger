# RelayLedger

**Commit the business change. Keep the delivery promise.**

[![CI](https://github.com/ABRJrocks/relayledger/actions/workflows/ci.yml/badge.svg)](https://github.com/ABRJrocks/relayledger/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-30363d)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-30363d)](LICENSE)

A transactional outbox for Python applications using SQLite. Save an event in the same transaction as an order, job or workflow update. A separate worker delivers signed HTTP requests, recovers expired leases and keeps each stream in order. A receiver inbox prevents duplicate local effects when a request succeeds but the sender crashes before recording it.

**No broker. No runtime dependencies. A library and CLI you can inspect.**

```text
Your SQLite transaction                Background worker               Receiver transaction
┌──────────────────────────┐          ┌──────────────────┐            ┌──────────────────────┐
│ UPDATE application state │  commit  │ claim with lease │ signed POST│ record inbox receipt │
│ INSERT event intent      │ ───────► │ send outside SQL │ ─────────► │ apply local effect   │
└──────────────────────────┘          │ acknowledge      │            └──────────────────────┘
                                     └──────────────────┘
                           Crash after receiver commit? Same event ID, same bytes, safe local replay.
```

## Try the failure, then watch recovery

Python 3.10+ and SQLite 3.24+ are required. Install a tagged source checkout or the wheel attached to the [release](https://github.com/ABRJrocks/relayledger/releases). This project is not published to PyPI.

```bash
git clone https://github.com/ABRJrocks/relayledger.git
cd relayledger
python -m venv .venv
# Activate your environment using your platform's command.
python -m pip install .
relayledger demo
```

The demo runs real loopback HTTP between two temporary SQLite databases. It simulates a sender failure after the receiver commits, advances the lease clock, then resumes delivery. No external account, service or secret is needed.

```json
{"attempts":3,"business_effects":2,"delivered_events":2,"duplicate_deliveries":1,"expired_attempts":1,"http_deliveries":3,"verified":true}
```

The `verified` result is checked against database state. The automated suite separately kills real subprocesses to exercise rollback and abandoned-lease recovery.

## Put it inside your transaction

```python
from relayledger import connect, enqueue, initialize, transaction

db = connect("application.db", create=True)
initialize(db)  # Creates only namespaced RelayLedger tables.

with transaction(db):
    db.execute("CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY, state TEXT)")
    db.execute("INSERT INTO orders VALUES(?, ?)", ("order-42", "confirmed"))
    event_id = enqueue(
        db,
        key="order-42:confirmed:v1",
        topic="order.confirmed",
        stream="order-42",
        data={"order_id": "order-42", "state": "confirmed"},
        max_attempts=8,
    )
db.close()
```

Both writes commit or both roll back. `enqueue` requires an active transaction and never commits it. Calling it again with the same key and identical intent returns the existing event ID; changed data, topic, stream or retry budget raises `Conflict`. Use a distinct key for each business event. Keys are retained with the event.

## Deliver from a separate process

Create `routes.json`:

```json
{
  "version": 1,
  "routes": {
    "order.confirmed": {
      "url": "https://your-service.example/events",
      "secret_env": "ORDERS_SIGNING_SECRET",
      "timeout_seconds": 5
    }
  }
}
```

Supply `ORDERS_SIGNING_SECRET` from your secret manager or environment, with 32 to 4096 UTF-8 bytes. `url_env` can replace `url` if the endpoint is sensitive. [Example configuration](examples/routes.json).

```bash
relayledger work application.db --routes routes.json --once
relayledger work application.db --routes routes.json --limit 100
relayledger work application.db --routes routes.json --watch --poll-ms 1000
```

The default lease is 30 seconds. It must exceed the socket timeout by more than one second. A worker releases the database transaction before making an HTTP request. Multiple local processes can claim different ready streams. Configure every unresolved topic; missing routes fail before consuming an attempt.

## Make the receiver idempotent

Verify `RelayLedger-Signature` over the **raw bytes**, then store the inbox receipt and business effect in one receiver transaction:

```python
from relayledger import decode_event, record_once, transaction, verify

if not verify(raw_body, signature_header, signing_secret):
    raise ValueError("Invalid delivery signature")

with transaction(receiver_db):
    if record_once(receiver_db, raw_body):
        event = decode_event(raw_body)
        receiver_db.execute(
            "INSERT INTO received_events(event_id, topic) VALUES(?, ?)",
            (event["id"], event["type"]),
        )
# Return 2xx only after commit succeeds, including for a matching duplicate.
```

Initialize the receiver schema and business table at startup. See the [complete adapter](examples/receiver.py) and [integration guide](docs/integrations.md), including signature verification in another language.

The receiver must use the same secret, enforce a request-size limit and use a synchronized clock. A matching inbox receipt skips the local effect. The same event ID with different body bytes raises `Conflict`. A remote API call inside this receiver transaction still needs that API's own idempotency support.

## Delivery and recovery rules

| Situation | Behavior |
| --- | --- |
| Business transaction aborts | Application changes and event intent both roll back |
| Worker disappears | Lease expires; the same ID and body can be claimed again |
| Old worker finishes late | Lease token and deadline prevent a stale acknowledgement |
| Receiver returns any 2xx | Mark transport delivery accepted |
| 408, 425, 429 or 5xx | Retry with persisted exponential equal jitter |
| `Retry-After` seconds or date | Respect the later of server minimum and backoff |
| Server minimum exceeds one day | Dead-letter for operator review rather than retry early |
| Other 4xx, redirects or TLS failure | Dead-letter; redirects are never followed |
| Earlier event is pending, leased or dead | Later events in that stream wait; other streams progress |
| Attempt budget exhausted | Dead-letter; claim consumes an attempt even if HTTP never starts |

A 2xx means endpoint acceptance, not proof of a downstream business outcome. Delivery can occur more than once. Ordering is per stream at the sender; an expired request may still be running remotely, so receiver deduplication and application version checks remain necessary.

## Operate deliberately

```bash
relayledger stats application.db
relayledger events application.db --limit 50
relayledger attempts application.db EVENT_ID
relayledger retry application.db EVENT_ID --reason endpoint_fixed
relayledger discard application.db EVENT_ID --reason superseded_by_reconciliation
```

Only dead events can be retried or discarded. A retry preserves event identity, body and total attempt history while resetting the current retry budget. A discard deliberately unblocks the stream and leaves an audit record. Reasons use short labels such as `endpoint_fixed`.

CLI output includes queue counts and delivery metadata, omitting payloads, event keys, stream names, endpoint URLs, signing secrets and lease tokens. Treat topic names and operator reasons as observable metadata. Exit codes: `0` successful command, `1` a work invocation observed a dead event or lost lease, `2` configuration or storage failure, `130` interrupted. An idle worker does not mean every event has delivered: work may be delayed, leased or blocked. Inspect `stats` and alert on dead events and old pending work.

## Fit and limits

Good fit: a single-host internal tool, Python service, agent job runner or local workflow that already uses SQLite and needs recoverable webhook delivery.

- Keep the database on a local filesystem. SQLite still has one writer. Short claims reduce contention; they do not remove that constraint. No throughput benchmark or distributed deployment guarantee is claimed.
- Existing journaling mode is preserved. WAL is accepted only with SQLite 3.51.3+ or the documented fixed backports 3.50.7 and 3.44.6 because of the [WAL-reset issue](https://www.sqlite.org/wal.html). New databases use SQLite's default journal mode. Check the SQLite linked to Python, not just a system CLI.
- The public API uses connections returned by `relayledger.connect`. It enables foreign keys and FULL synchronous durability. Avoid raw writes to its tables. Transactions cannot be nested; put `enqueue` inside your existing transaction instead.
- HTTPS uses certificate and hostname verification. Plain HTTP is available only for numeric loopback addresses with explicit `--allow-http-loopback`. Routes are trusted operator configuration, not user input. There is no SSRF sandbox.
- Socket timeout is per operation. DNS resolution and the complete request have no hard total deadline. Use a process supervisor with an outer execution limit, keep leases conservative, and make receivers idempotent.
- Events, receipts and audit history are retained. There is no automatic pruning or migration across schema versions yet. Plan disk monitoring, backups and a retention policy before long-running production use.
- Stable event IDs and the inbox can protect local SQL effects. They do not provide exactly-once effects across arbitrary external services.

For an existing PostgreSQL or broker platform, prefer an established outbox/CDC stack that fits it. The [research notes](docs/research.md) explain the design choices and alternatives.

## Development

```bash
python -m pip install -e . build
python -m unittest discover -s tests -v
python -m unittest discover -s tests_integration -v  # Requires openssl on PATH.
python -m relayledger demo
python -m build
```

CI covers Python 3.10, 3.12 and 3.13 on Linux plus Python 3.12 on macOS and Windows. It also exercises a real TLS receiver, builds wheel and source archives, installs the wheel in isolation and runs the crash-recovery demo from that installation. See [contributing](CONTRIBUTING.md), [security](SECURITY.md) and [release notes](CHANGELOG.md).

MIT licensed. Built and maintained by [Abdul Rafay](https://github.com/ABRJrocks).
