# Security

RelayLedger 0.1.x is an early release. It has automated reliability and transport checks, but no independent security audit.

Report suspected vulnerabilities privately to abdul.rafay@deventities.com. Include the version, a minimal reproduction and impact without production secrets or personal data. Do not publish exploitable details in a public issue before coordination. No response-time guarantee is offered.

## Trust boundaries

- Routes, environment configuration, SQLite files and local operators are trusted. This is not a sandbox for user-supplied destination URLs. HTTPS can still reach internal networks if configured that way.
- HMAC authenticates raw delivery bytes. It does not encrypt stored data or replace destination authorization. Use HTTPS and least-privilege secrets, rotate them deliberately and synchronize clocks.
- SQLite stores payloads, keys and stream names in plaintext. New database files are created with owner-only permissions where the OS supports them. Existing file permissions are preserved. Protect directories, journals and backups; metadata may also be sensitive.
- The receiver must enforce input limits before reading, verify signatures and commit receipts with local effects. Generic remote side effects do not become idempotent by holding a local SQL transaction.
- The relay never follows redirects or prints raw response bodies. Certificate failures are terminal so operators fix trust configuration rather than silently downgrading transport.
- There is no hard total DNS/request deadline or automatic retention. Use process supervision, disk monitoring and a reviewed backup/retention policy.
