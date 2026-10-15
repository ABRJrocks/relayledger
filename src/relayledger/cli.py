"""Explicit initialization, bounded workers and audited operator decisions."""

import argparse
import json
import sqlite3
import sys
import time
from dataclasses import asdict

from . import __version__
from .config import load_routes, read_json
from .database import connect, initialize, transaction
from .operations import attempts, events, resolve_dead, stats
from .outbox import enqueue
from .validation import RelayError
from .worker import Worker


def bounded(maximum):
    def parse(value):
        try:
            number = int(value)
        except ValueError:
            raise argparse.ArgumentTypeError("Expected an integer") from None
        if not 1 <= number <= maximum:
            raise argparse.ArgumentTypeError(f"Expected 1 to {maximum}")
        return number

    return parse


def parser():
    root = argparse.ArgumentParser(
        prog="relayledger",
        description="Transactional SQLite event intent and recoverable HTTP delivery.",
    )
    root.add_argument(
        "--version", action="version", version=f"RelayLedger {__version__}"
    )
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "demo", help="Exercise duplicate recovery on a temporary loopback receiver"
    )
    for name in (
        "init",
        "enqueue",
        "work",
        "stats",
        "events",
        "attempts",
        "retry",
        "discard",
    ):
        sub = commands.add_parser(name)
        sub.add_argument("database")
        if name == "enqueue":
            for key in ("key", "topic", "stream", "data"):
                sub.add_argument("--" + key, required=True)
            sub.add_argument("--max-attempts", type=bounded(1000), default=8)
        elif name == "work":
            sub.add_argument("--routes", required=True)
            sub.add_argument("--lease-ms", type=bounded(3_600_000), default=30_000)
            sub.add_argument("--allow-http-loopback", action="store_true")
            mode = sub.add_mutually_exclusive_group()
            mode.add_argument("--once", action="store_true")
            mode.add_argument("--watch", action="store_true")
            sub.add_argument("--limit", type=bounded(100_000), default=100)
            sub.add_argument("--poll-ms", type=bounded(60_000), default=1000)
        elif name == "events":
            sub.add_argument("--limit", type=bounded(1000), default=50)
        elif name in ("attempts", "retry", "discard"):
            sub.add_argument("event_id")
            if name != "attempts":
                sub.add_argument("--reason", required=True)
    return root


def emit(value):
    print(json.dumps(value, sort_keys=True, allow_nan=False), flush=True)


def main(argv=None):
    args = parser().parse_args(argv)
    database = None
    try:
        if args.command == "demo":
            from .demo import run_demo

            emit(run_demo())
            return 0
        routes = (
            load_routes(args.routes, allow_http_loopback=args.allow_http_loopback)
            if args.command == "work"
            else None
        )
        database = connect(args.database, create=args.command == "init")
        if args.command == "init":
            initialize(database)
            emit({"schema": 1, "initialized": True})
        elif args.command == "enqueue":
            data = read_json(args.data)
            with transaction(database):
                event_id = enqueue(
                    database,
                    key=args.key,
                    topic=args.topic,
                    stream=args.stream,
                    data=data,
                    max_attempts=args.max_attempts,
                )
            emit({"event_id": event_id, "intent_saved": True})
        elif args.command == "stats":
            emit(stats(database))
        elif args.command == "events":
            emit(events(database, limit=args.limit))
        elif args.command == "attempts":
            emit(attempts(database, args.event_id))
        elif args.command in ("retry", "discard"):
            resolve_dead(
                database, args.event_id, action=args.command, reason=args.reason
            )
            emit({"event_id": args.event_id, "action": args.command, "recorded": True})
        elif args.command == "work":
            worker = Worker(database, routes, lease_ms=args.lease_ms)
            processed, failed = 0, False
            while args.watch or processed < (1 if args.once else args.limit):
                result = worker.run_once()
                if result.state == "idle":
                    if not args.watch:
                        break
                    time.sleep(args.poll_ms / 1000)
                    continue
                emit(asdict(result))
                processed += 1
                failed |= result.state in ("dead", "lease_lost")
            emit({"processed": processed, "queue": stats(database)})
            return 1 if failed else 0
        return 0
    except KeyboardInterrupt:
        return 130
    except RelayError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except (sqlite3.Error, OSError, UnicodeError):
        print("ERROR: Local storage or encoding operation failed", file=sys.stderr)
        return 2
    finally:
        if database is not None:
            database.close()


if __name__ == "__main__":
    raise SystemExit(main())
