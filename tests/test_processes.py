import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

from helpers import NOW, DatabaseTest

from relayledger.database import transaction
from relayledger.outbox import claim, enqueue, finish


def process(code, *args):
    return subprocess.run(
        [sys.executable, "-c", code, *map(str, args)],
        capture_output=True,
        text=True,
        timeout=20,
    )


class ProcessTests(DatabaseTest):
    def test_process_exit_before_commit_rolls_back_business_and_outbox(self):
        self.db.execute("CREATE TABLE business(id INTEGER PRIMARY KEY)")
        code = """
import os,sys
from relayledger.database import connect,transaction
from relayledger.outbox import enqueue
db=connect(sys.argv[1])
with transaction(db):
    db.execute('INSERT INTO business VALUES(1)')
    enqueue(db,key='one',topic='order.created',stream='one',data={})
    os._exit(17)
"""
        result = process(code, self.path)
        self.assertEqual(result.returncode, 17, result.stderr)
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM business").fetchone()[0], 0
        )
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM relayledger_events").fetchone()[0], 0
        )

    def test_process_crash_after_claim_recovers_same_body(self):
        with transaction(self.db):
            enqueue(
                self.db,
                key="one",
                topic="order.created",
                stream="one",
                data={"a": 1},
                now_ms=NOW,
            )
        code = """
import os,sys
from relayledger.database import connect
from relayledger.outbox import claim
db=connect(sys.argv[1])
claim(db,lease_ms=100,now_ms=int(sys.argv[2]))
os._exit(18)
"""
        original = self.db.execute("SELECT body FROM relayledger_events").fetchone()[0]
        result = process(code, self.path, NOW)
        self.assertEqual(result.returncode, 18, result.stderr)
        work = claim(self.db, lease_ms=100, now_ms=NOW + 100)
        self.assertEqual(work.total_attempt, 2)
        self.assertEqual(work.body, original)
        self.assertEqual(
            finish(self.db, work, accepted=True, code="http_200", now_ms=NOW + 101),
            "delivered",
        )

    def test_separate_process_claimers_never_share_a_live_lease(self):
        with transaction(self.db):
            for index in range(12):
                enqueue(
                    self.db,
                    key=f"event-{index}",
                    topic="order.created",
                    stream=f"stream-{index}",
                    data={},
                    now_ms=NOW,
                )
        code = """
import sys
from relayledger.database import connect
from relayledger.outbox import claim
db=connect(sys.argv[1])
work=claim(db,now_ms=int(sys.argv[2]))
print(work.event_id if work else 'idle')
"""
        with ThreadPoolExecutor(max_workers=6) as executor:
            results = list(
                executor.map(lambda _: process(code, self.path, NOW), range(12))
            )
        self.assertTrue(
            all(r.returncode == 0 for r in results), [r.stderr for r in results]
        )
        ids = [r.stdout.strip() for r in results]
        self.assertEqual(len(set(ids)), 12)
        self.assertNotIn("idle", ids)
