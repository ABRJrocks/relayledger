"""Caller-owned transactions and isolated application-table namespaces."""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .validation import RelayError

SCHEMA = (
    "CREATE TABLE IF NOT EXISTS relayledger_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL)",
    """CREATE TABLE IF NOT EXISTS relayledger_events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, event_key TEXT NOT NULL UNIQUE,
        topic TEXT NOT NULL, stream TEXT NOT NULL, body BLOB NOT NULL, intent_hash TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','inflight','delivered','dead','discarded')),
        attempts INTEGER NOT NULL DEFAULT 0, total_attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL, available_at_ms INTEGER NOT NULL,
        lease_until_ms INTEGER, lease_token TEXT, created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL, last_code TEXT,
        CHECK((state='inflight' AND lease_token IS NOT NULL AND lease_until_ms IS NOT NULL)
           OR (state!='inflight' AND lease_token IS NULL AND lease_until_ms IS NULL)))""",
    "CREATE INDEX IF NOT EXISTS relayledger_ready ON relayledger_events(state,available_at_ms,seq)",
    "CREATE INDEX IF NOT EXISTS relayledger_stream ON relayledger_events(stream,seq,state)",
    """CREATE TABLE IF NOT EXISTS relayledger_attempts (
        event_id TEXT NOT NULL REFERENCES relayledger_events(id), number INTEGER NOT NULL,
        token TEXT NOT NULL, started_at_ms INTEGER NOT NULL, finished_at_ms INTEGER,
        outcome TEXT NOT NULL, http_status INTEGER, PRIMARY KEY(event_id,number))""",
    """CREATE TABLE IF NOT EXISTS relayledger_inbox (
        source TEXT NOT NULL, event_id TEXT NOT NULL, body_hash TEXT NOT NULL,
        recorded_at_ms INTEGER NOT NULL, PRIMARY KEY(source,event_id))""",
    """CREATE TABLE IF NOT EXISTS relayledger_operations (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL REFERENCES relayledger_events(id),
        action TEXT NOT NULL, reason TEXT NOT NULL, at_ms INTEGER NOT NULL)""",
)


def wal_fixed(version):
    return (
        version >= (3, 51, 3)
        or (3, 50, 7) <= version < (3, 51, 0)
        or (3, 44, 6) <= version < (3, 45, 0)
    )


def connect(path, *, create=False):
    if sqlite3.sqlite_version_info < (3, 24, 0):
        raise RelayError("SQLite 3.24+ is required")
    if str(path) == ":memory:":
        database = sqlite3.connect(":memory:", isolation_level=None, timeout=5)
    else:
        path = Path(path).resolve()
        if create:
            try:
                descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
                os.close(descriptor)
            except FileExistsError:
                pass
        database = sqlite3.connect(
            path.as_uri() + "?mode=rw", uri=True, isolation_level=None, timeout=5
        )
    database.row_factory = sqlite3.Row
    database.execute("PRAGMA foreign_keys=ON")
    database.execute("PRAGMA synchronous=FULL")
    if database.execute("PRAGMA journal_mode").fetchone()[0] == "wal" and not wal_fixed(
        sqlite3.sqlite_version_info
    ):
        database.close()
        raise RelayError(
            "WAL mode requires SQLite 3.51.3+ or a documented fixed backport"
        )
    return database


@contextmanager
def transaction(database):
    if database.in_transaction:
        raise RelayError(
            "Nested transactions are not supported; use the existing application transaction"
        )
    database.execute("BEGIN IMMEDIATE")
    try:
        yield database
        database.commit()
    except BaseException:
        if database.in_transaction:
            database.rollback()
        raise


def require_transaction(database):
    if not database.in_transaction:
        raise RelayError(
            "Enqueue and inbox writes require an active application transaction"
        )


def initialize(database):
    with transaction(database):
        for statement in SCHEMA:
            database.execute(statement)
        database.execute(
            "INSERT INTO relayledger_meta(singleton,version) VALUES(1,1) ON CONFLICT(singleton) DO NOTHING"
        )
        if (
            database.execute(
                "SELECT version FROM relayledger_meta WHERE singleton=1"
            ).fetchone()[0]
            != 1
        ):
            raise RelayError("Unsupported RelayLedger schema version")


def check_schema(database):
    try:
        version = database.execute(
            "SELECT version FROM relayledger_meta WHERE singleton=1"
        ).fetchone()
    except sqlite3.DatabaseError as exc:
        raise RelayError("Initialize the RelayLedger schema before use") from exc
    if version is None or version[0] != 1:
        raise RelayError("Unsupported RelayLedger schema version")
