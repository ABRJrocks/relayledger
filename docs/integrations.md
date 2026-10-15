# Integrating RelayLedger

## Application lifecycle

Use one connection per thread or process. Initialize the schema during startup or an explicit setup command. `initialize` is idempotent for schema version 1 and does not touch your application's `PRAGMA user_version`. If you already have an open transaction, call `enqueue` directly within it instead of nesting `transaction`.

An event key identifies a business intent, not an HTTP attempt. Include an entity ID and transition/version, for example `invoice-42:paid:v2`. Keys, topics and streams are 1 to 128 ASCII characters, beginning with an alphanumeric character and otherwise allowing letters, digits, `_`, `.`, `:`, `/` and `-`. Event data must be a JSON object. Depth, item count and the full encoded envelope are bounded; the body limit is 1 MiB. Non-finite numbers and duplicate input JSON members are rejected.

Use one stream per ordered entity, such as an order ID. A dead head blocks its stream until an operator retries or discards it. Do not put every unrelated workflow into one stream unless global head-of-line blocking is intentional.

## Worker service

Run the worker separately from request handling, against the same local database file:

```bash
relayledger work /var/lib/my-service/application.db \
  --routes /etc/my-service/relay-routes.json --watch --poll-ms 1000
```

Pass signing secrets through your service manager's environment. Protect route files, environment configuration and the database directory. Configure service restart on failures and termination handling. A stopped worker can resume after leases expire. Lease duration must cover the configured socket timeout with more than one second of margin; choose additional margin for the environment's latency. A lease is not a request cancellation mechanism. Avoid multiple worker machines opening a network-mounted SQLite file.

The default retry policy has a one-second base and a 60-second cap, with equal jitter. The server's bounded `Retry-After` minimum can extend that delay. Each claim consumes an attempt; repeated crashes can exhaust the budget without a successful request. Retry state is persisted, so process restarts do not reset backoff. `--limit` bounds attempted deliveries in a drain invocation, not wall-clock duration. Default drain stops at the first idle result. Use `--watch` or schedule another invocation to process future retries.

`stats` and event metadata support operational dashboards. Alert on `dead`, increasing `blocked_pending`, stale pending timestamps, repeated lease expirations and disk usage. CLI exit status 0 alone does not establish that all work has finished.

## HTTP receiver contract

1. Limit the raw request body to 1 MiB before reading it into memory. Do not parse and reserialize before signature verification.
2. Verify `RelayLedger-Signature` using the configured secret and a synchronized clock.
3. Begin a receiver database transaction. Call `record_once` and apply local SQL effects only when it returns `True`.
4. Commit. Return 2xx after commit, including when an identical receipt already exists.
5. On storage failure, roll back and return a retryable status. A permanently unsupported event can return a 4xx and will require operator attention at the sender.

[receiver.py](../examples/receiver.py) provides the transaction adapter. Create its application table during startup:

```python
initialize(receiver_db)
with transaction(receiver_db):
    receiver_db.execute("CREATE TABLE IF NOT EXISTS received_events(event_id TEXT PRIMARY KEY, topic TEXT NOT NULL)")
```

`record_once` does not verify signatures itself. It accepts RelayLedger's fixed CloudEvents JSON profile, not every possible CloudEvents extension. Receipts are keyed by `(source, event_id)` and bind to the SHA-256 hash of the raw body. RelayLedger emits `source=urn:relayledger` and UUID event IDs. If you accept multiple producers, authenticate and authorize each route and use the appropriate secret. Do not let external callers select a secret or configure delivery destinations.

### Verifying in another language

The header is `t=<Unix seconds>,v1=<lowercase HMAC-SHA256 hex>`. Compute HMAC over ASCII timestamp, a period byte and the exact raw body:

```text
HMAC_SHA256(secret, ASCII(timestamp) || "." || body)
```

Reject malformed/duplicate fields, use a constant-time comparison, and reject timestamps outside the tolerance window (300 seconds by default). Retries are signed again with a fresh delivery timestamp but keep the same body and event ID. A valid signature can be replayed within that window, so signature verification does not replace inbox deduplication.

`Idempotency-Key` equals the event UUID. It is a convenience for destinations with an explicit idempotency contract. An arbitrary service may ignore it. `X-RelayLedger-Attempt` is the lifetime claim count, including abandoned claims. No secret or business key is placed in a custom header.

## Failure recovery

Inspect `events` and `attempts` before changing dead-letter state. Fix the endpoint or receiver first, then retry with an audit label. Retrying preserves the identity and body so the receiver can recognize prior acceptance. Discard only when intentionally abandoning that delivery; it releases later events in the stream.

No automatic deletion is supplied. Removing sender rows also removes enqueue deduplication memory; removing receiver receipts permits old events to be applied again. Back up application state and RelayLedger tables consistently and test restore behavior. Review privacy and retention before storing personal data in event bodies.
