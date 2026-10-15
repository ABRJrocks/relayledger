import json

from helpers import NOW, DatabaseTest

from relayledger.database import transaction
from relayledger.inbox import decode_event, record_once
from relayledger.outbox import claim, enqueue
from relayledger.validation import Conflict, RelayError


class InboxTests(DatabaseTest):
    def body(self):
        with transaction(self.db):
            enqueue(
                self.db,
                key="one",
                topic="order.created",
                stream="one",
                data={"amount": 5},
                now_ms=NOW,
            )
        return claim(self.db, now_ms=NOW).body

    def test_duplicate_delivery_has_one_local_business_effect(self):
        body = self.body()
        self.db.execute("CREATE TABLE effects(id TEXT PRIMARY KEY)")
        for _ in range(3):
            with transaction(self.db):
                if record_once(self.db, body, now_ms=NOW):
                    self.db.execute(
                        "INSERT INTO effects VALUES(?)", (decode_event(body)["id"],)
                    )
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM effects").fetchone()[0], 1
        )

    def test_failed_effect_rolls_back_the_receipt(self):
        body = self.body()
        with self.assertRaises(RuntimeError), transaction(self.db):
            self.assertTrue(record_once(self.db, body, now_ms=NOW))
            raise RuntimeError("business operation failed")
        with transaction(self.db):
            self.assertTrue(record_once(self.db, body, now_ms=NOW))

    def test_conflicting_bytes_do_not_reuse_a_receipt(self):
        body = self.body()
        with transaction(self.db):
            record_once(self.db, body, now_ms=NOW)
        changed = json.loads(body)
        changed["data"]["amount"] = 6
        with self.assertRaises(Conflict), transaction(self.db):
            record_once(self.db, json.dumps(changed).encode(), now_ms=NOW)

    def test_validation_and_transaction_boundary(self):
        body = self.body()
        with self.assertRaises(RelayError):
            record_once(self.db, body, now_ms=NOW)
        for bad in (b"{}", b"[]", b'{"id":"a","id":"b"}', b"NaN", b"\xff"):
            with self.assertRaises(RelayError):
                decode_event(bad)
