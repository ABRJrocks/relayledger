import json

from relayledger.database import connect, transaction
from relayledger.outbox import claim, enqueue, finish
from relayledger.validation import Conflict, LeaseLost, RelayError
from helpers import DatabaseTest, NOW


class OutboxTests(DatabaseTest):
    def event(self, key='order-1', stream='order-1', **changes):
        with transaction(self.db):
            return enqueue(self.db,key=key,topic='order.created',stream=stream,data={'amount':25},now_ms=NOW,**changes)

    def test_enqueue_needs_the_business_transaction(self):
        with self.assertRaises(RelayError):
            enqueue(self.db,key='a',topic='order.created',stream='a',data={})

    def test_business_record_and_intent_commit_or_rollback_together(self):
        self.db.execute('CREATE TABLE orders(id TEXT PRIMARY KEY)')
        with self.assertRaises(RuntimeError),transaction(self.db):
            self.db.execute("INSERT INTO orders VALUES('order-1')")
            enqueue(self.db,key='order-1',topic='order.created',stream='order-1',data={},now_ms=NOW)
            raise RuntimeError('abort')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0],0)
        self.assertIsNone(claim(self.db,now_ms=NOW))

    def test_deduplication_and_conflicting_intent(self):
        event = self.event()
        self.assertEqual(event,self.event())
        with self.assertRaises(Conflict),transaction(self.db):
            enqueue(self.db,key='order-1',topic='order.created',stream='order-1',data={'amount':26},now_ms=NOW)

    def test_stable_body_and_lease_fencing(self):
        event = self.event()
        old = claim(self.db,lease_ms=100,now_ms=NOW)
        self.assertEqual(json.loads(old.body)['id'],event)
        self.assertNotIn('amount',repr(old))
        new = claim(self.db,lease_ms=100,now_ms=NOW+100)
        self.assertEqual(new.body,old.body)
        self.assertNotEqual(new.token,old.token)
        with self.assertRaises(LeaseLost):
            finish(self.db,old,accepted=True,code='http_200',now_ms=NOW+101)
        self.assertEqual(finish(self.db,new,accepted=True,code='http_200',now_ms=NOW+101),'delivered')
        self.assertIsNone(claim(self.db,now_ms=NOW+102))
        self.assertEqual(self.event(),event)

    def test_one_stream_waits_while_another_progresses(self):
        first = self.event()
        self.event(key='order-1-second',stream='order-1')
        other = self.event(key='order-2',stream='order-2')
        a = claim(self.db,now_ms=NOW)
        b = claim(self.db,now_ms=NOW)
        self.assertEqual((a.event_id,b.event_id),(first,other))
        self.assertIsNone(claim(self.db,now_ms=NOW))

    def test_expiration_on_last_attempt_dead_letters_and_blocks_stream(self):
        self.event(max_attempts=1)
        self.event(key='second',stream='order-1')
        claim(self.db,lease_ms=100,now_ms=NOW)
        self.assertIsNone(claim(self.db,now_ms=NOW+100))
        states = [r[0] for r in self.db.execute('SELECT state FROM relayledger_events ORDER BY seq')]
        self.assertEqual(states,['dead','pending'])

    def test_retry_schedule_and_terminal_failures(self):
        self.event()
        a = claim(self.db,now_ms=NOW)
        self.assertEqual(finish(self.db,a,accepted=False,code='http_503',retry_at_ms=NOW+1000,now_ms=NOW),'pending')
        self.assertIsNone(claim(self.db,now_ms=NOW+999))
        b = claim(self.db,now_ms=NOW+1000)
        self.assertEqual(b.total_attempt,2)
        self.assertEqual(finish(self.db,b,accepted=False,code='http_400',now_ms=NOW+1000),'dead')

    def test_other_connections_do_not_see_uncommitted_intent(self):
        other = connect(self.path)
        self.addCleanup(other.close)
        with transaction(self.db):
            enqueue(self.db,key='a',topic='order.created',stream='a',data={},now_ms=NOW)
            self.assertEqual(other.execute('SELECT COUNT(*) FROM relayledger_events').fetchone()[0],0)
        self.assertEqual(other.execute('SELECT COUNT(*) FROM relayledger_events').fetchone()[0],1)
