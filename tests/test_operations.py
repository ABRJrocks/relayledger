from helpers import NOW, DatabaseTest

from relayledger.database import transaction
from relayledger.operations import attempts, events, resolve_dead, stats
from relayledger.outbox import claim, enqueue, finish
from relayledger.validation import RelayError


class OperationsTests(DatabaseTest):
    def prepare(self):
        with transaction(self.db):
            event = enqueue(
                self.db,
                key="private-key",
                topic="order.created",
                stream="private-stream",
                data={"private": "value"},
                now_ms=NOW,
            )
            enqueue(
                self.db,
                key="second",
                topic="order.created",
                stream="private-stream",
                data={},
                now_ms=NOW,
            )
        work = claim(self.db, now_ms=NOW)
        finish(self.db, work, accepted=False, code="http_400", now_ms=NOW)
        return event

    def test_dead_head_can_be_retried_with_the_same_event_identity(self):
        event = self.prepare()
        self.assertEqual(stats(self.db, now_ms=NOW)["blocked_pending"], 1)
        resolve_dead(
            self.db, event, action="retry", reason="receiver-fixed", now_ms=NOW
        )
        work = claim(self.db, now_ms=NOW)
        self.assertEqual(work.event_id, event)
        self.assertEqual((work.attempt, work.total_attempt), (1, 2))
        self.assertEqual(len(attempts(self.db, event)), 2)

    def test_discard_is_audited_and_unblocks_the_next_event(self):
        event = self.prepare()
        resolve_dead(self.db, event, action="discard", reason="superseded", now_ms=NOW)
        self.assertNotEqual(claim(self.db, now_ms=NOW).event_id, event)
        self.assertEqual(
            self.db.execute("SELECT reason FROM relayledger_operations").fetchone()[0],
            "superseded",
        )
        with self.assertRaises(RelayError):
            resolve_dead(self.db, event, action="retry", reason="unsafe", now_ms=NOW)

    def test_metadata_inspection_omits_payload_keys_streams_and_tokens(self):
        event = self.prepare()
        output = (
            repr(events(self.db))
            + repr(attempts(self.db, event))
            + repr(stats(self.db, now_ms=NOW))
        )
        for value in ("private-key", "private-stream", "value", "lease_token"):
            self.assertNotIn(value, output)
