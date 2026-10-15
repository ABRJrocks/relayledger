import importlib.util
import subprocess
import sys
from pathlib import Path

from helpers import DatabaseTest

from relayledger import claim, connect, enqueue, initialize, sign, transaction


class ExampleTests(DatabaseTest):
    def test_producer_repeated_command_keeps_one_order_and_one_event(self):
        script = Path(__file__).resolve().parents[1] / "examples" / "producer.py"
        for _ in range(2):
            result = subprocess.run(
                [sys.executable, str(script), str(self.path), "order-42"],
                check=True,
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertTrue(result.stdout.strip())
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 1
        )
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM relayledger_events").fetchone()[0], 1
        )

    def test_receiver_authenticates_and_commits_effect_only_once(self):
        script = Path(__file__).resolve().parents[1] / "examples" / "receiver.py"
        spec = importlib.util.spec_from_file_location("example_receiver", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with transaction(self.db):
            enqueue(
                self.db,
                key="example",
                topic="order.confirmed",
                stream="order-42",
                data={},
            )
        work = claim(self.db)
        receiver = connect(":memory:")
        secret = b"example-test-secret-only-32-bytes-000"
        try:
            initialize(receiver)
            receiver.execute(
                "CREATE TABLE received_events(event_id TEXT PRIMARY KEY,topic TEXT NOT NULL)"
            )
            with self.assertRaises(ValueError):
                module.apply_event(receiver, work.body, "invalid", secret)
            self.assertEqual(
                receiver.execute("SELECT COUNT(*) FROM relayledger_inbox").fetchone()[
                    0
                ],
                0,
            )
            signature = sign(work.body, secret)
            self.assertTrue(module.apply_event(receiver, work.body, signature, secret))
            self.assertFalse(module.apply_event(receiver, work.body, signature, secret))
            self.assertEqual(
                receiver.execute("SELECT COUNT(*) FROM received_events").fetchone()[0],
                1,
            )
        finally:
            receiver.close()
