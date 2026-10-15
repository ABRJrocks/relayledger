from relayledger.database import connect, transaction
from relayledger.outbox import enqueue
from relayledger.transport import Delivery, Route
from relayledger.validation import RelayError
from relayledger.worker import Worker
from helpers import DatabaseTest, NOW
from test_delivery_policy import SECRET


class WorkerTests(DatabaseTest):
    def event(self):
        with transaction(self.db): enqueue(self.db,key='one',topic='order.created',stream='one',data={},now_ms=NOW)

    def worker(self,transport,clock=lambda:NOW):
        return Worker(self.db,{'order.created':Route('https://example.com/events',SECRET)},transport=transport,clock=clock)

    def test_network_happens_after_claim_commit(self):
        self.event()
        def transport(*args,**kwargs):
            self.assertFalse(self.db.in_transaction)
            other = connect(self.path)
            try:
                with transaction(other): other.execute('CREATE TABLE independent_write(id INTEGER)')
            finally: other.close()
            return Delivery(True,False,'http_200',200)
        self.assertEqual(self.worker(transport).run_once().state,'delivered')

    def test_worker_persists_retry_delay_and_terminal_response(self):
        self.event()
        result = self.worker(lambda *a,**k:Delivery(False,True,'http_429',429,30000)).run_once()
        self.assertEqual(result.state,'pending')
        self.assertEqual(self.db.execute('SELECT available_at_ms FROM relayledger_events').fetchone()[0],NOW+30000)
        self.assertEqual(self.worker(lambda *a,**k:Delivery(False,False,'http_400',400),lambda:NOW+30000).run_once().state,'dead')

    def test_socket_budget_must_fit_lease(self):
        with self.assertRaises(RelayError): Worker(self.db,{'a':Route('https://example.com',SECRET)},lease_ms=5000)

    def test_missing_route_does_not_consume_attempt(self):
        self.event()
        worker = Worker(self.db,{'different':Route('https://example.com',SECRET)})
        with self.assertRaises(RelayError): worker.run_once()
        self.assertEqual(self.db.execute('SELECT total_attempts FROM relayledger_events').fetchone()[0],0)

    def test_late_success_cannot_ack_expired_work(self):
        self.event()
        clock = iter([NOW,NOW,NOW+30_000])
        result = self.worker(lambda *a,**k:Delivery(True,False,'http_200',200),lambda:next(clock)).run_once()
        self.assertEqual(result.state,'lease_lost')
