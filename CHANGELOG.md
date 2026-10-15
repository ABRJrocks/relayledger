# Changelog

## 0.1.0

Initial public release for single-host SQLite applications.

- Atomic event intent in caller-owned application transactions.
- Stable CloudEvents bodies, event IDs and enqueue deduplication keys.
- Ordered streams, short leased claims and stale-worker fencing.
- Signed HTTP delivery, bounded retry policy, Retry-After and dead letters.
- Receiver inbox receipts that commit with local business effects.
- Audited retry/discard operations and metadata-only CLI inspection.
- Executable crash/duplicate demo, subprocess recovery tests and real HTTPS verification.

Early release: no automatic pruning, distributed deployment guarantee, hard total request deadline or independent security audit. Public API changes before 1.0 will be documented.
