# Research and scope

## Problem

A workflow can commit a business record, then crash before publishing its notification. RelayLedger stores event intent alongside application changes in the same SQLite transaction. A separate process handles HTTP delivery. It is for single-host SQLite applications, internal services and local automation, with no broker requirement.

## Sources and existing options

- [AWS transactional outbox guidance](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html) describes the dual-write failure and why consumers must handle duplicate events. This determines the same-transaction API and duplicate-aware receiver example.
- [Debezium Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html) is an established CDC approach for larger database/event infrastructure. Use it when a supported source database, CDC and a broker fit the deployment. RelayLedger is a focused SQLite polling relay, not a Debezium replacement.
- [SQLite transactions](https://www.sqlite.org/lang_transaction.html) allow one writer at a time. Short BEGIN IMMEDIATE transactions serialize claims; HTTP happens after the claim transaction commits.
- [SQLite WAL](https://www.sqlite.org/wal.html) requires one host and documents version-specific WAL-reset risks. RelayLedger does not switch an application into WAL mode. Its default SQLite connection uses the database's existing journaling configuration.
- [HTTP semantics](https://datatracker.ietf.org/doc/html/rfc9110#section-10.2.3) defines Retry-After delays and dates. The relay records a bounded retry schedule and does not follow redirects.
- [CloudEvents JSON format](https://github.com/cloudevents/spec/blob/main/cloudevents/formats/json-format.md) provides an interoperable event envelope. The immutable delivery body contains version, source, ID, type, time, content type and application data.
- [Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests) demonstrate explicit destination-side idempotency semantics and finite key retention. An Idempotency-Key header alone does not make an arbitrary endpoint idempotent.

## Delivery contract

Leases fence stale workers from updating local delivery state, but cannot cancel an HTTP request that has already reached a receiver. A crash after a receiver commits and before the sender records success can cause a duplicate. The event ID, body and idempotency key stay stable across retries. The inbox API lets a SQLite receiver commit its receipt and local business effect together.

Ordering applies within an explicitly named stream. A pending, leased or dead head blocks later events in that stream; unrelated streams can progress. Operators may retry a dead event or deliberately discard it with an audit reason. There is no global delivery ordering across streams and no promise of exactly-once effects in external systems.

The automated suite verifies local transactions, separate-process concurrency, lease expiration, crash recovery, duplicate receiver delivery and actual loopback HTTP behavior. A separate integration test exercises certificate rejection and trusted TLS delivery. It does not claim PostgreSQL compatibility, distributed SQLite operation, production throughput certification or a formal security audit.
