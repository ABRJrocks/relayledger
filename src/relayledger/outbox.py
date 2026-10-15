"""Atomic notification intent, claimed in short transactions."""

from dataclasses import dataclass, field
import hashlib
import uuid

from .database import check_schema, require_transaction, transaction
from .validation import Conflict, LeaseLost, RelayError, canonical, instant, integer, label, timestamp


@dataclass(frozen=True)
class Claim:
    event_id: str
    topic: str
    attempt: int
    total_attempt: int
    deadline_ms: int
    body: bytes = field(repr=False)
    token: str = field(repr=False)


def enqueue(database, *, key, topic, stream, data, max_attempts=8, now_ms=None):
    require_transaction(database)
    check_schema(database)
    for value in (key,topic,stream): label(value)
    integer(max_attempts,1,1000)
    if not isinstance(data,dict): raise RelayError("Event data must be a JSON object")
    now = instant(now_ms)
    intent = canonical({'topic':topic,'stream':stream,'data':data,'max_attempts':max_attempts})
    digest = hashlib.sha256(intent).hexdigest()
    existing = database.execute('SELECT id,intent_hash FROM relayledger_events WHERE event_key=?',(key,)).fetchone()
    if existing:
        if existing['intent_hash'] != digest: raise Conflict("Idempotency key already has different event intent")
        return existing['id']
    event_id = str(uuid.uuid4())
    body = canonical({'specversion':'1.0','id':event_id,'source':'urn:relayledger',
                      'type':topic,'time':timestamp(now),'datacontenttype':'application/json','data':data})
    database.execute('''INSERT INTO relayledger_events
        (id,event_key,topic,stream,body,intent_hash,state,max_attempts,available_at_ms,created_at_ms,updated_at_ms)
        VALUES(?,?,?,?,?,?,'pending',?,?,?,?)''',
        (event_id,key,topic,stream,body,digest,max_attempts,now,now,now))
    return event_id


def _expire(database, now):
    rows = database.execute("SELECT id,total_attempts,attempts,max_attempts,lease_token FROM relayledger_events WHERE state='inflight' AND lease_until_ms<=? ORDER BY seq LIMIT 100",(now,)).fetchall()
    for row in rows:
        state = 'dead' if row['attempts'] >= row['max_attempts'] else 'pending'
        database.execute('''UPDATE relayledger_attempts SET outcome='expired',finished_at_ms=?
            WHERE event_id=? AND number=? AND token=? AND outcome='claimed' ''',
            (now,row['id'],row['total_attempts'],row['lease_token']))
        database.execute('''UPDATE relayledger_events SET state=?,lease_token=NULL,lease_until_ms=NULL,
            available_at_ms=?,updated_at_ms=?,last_code='lease_expired' WHERE id=?''',
            (state,now,now,row['id']))


def claim(database, *, lease_ms=30_000, now_ms=None):
    integer(lease_ms,1,3_600_000)
    now = instant(now_ms)
    deadline = instant(now+lease_ms)
    check_schema(database)
    with transaction(database):
        _expire(database,now)
        row = database.execute('''SELECT e.* FROM relayledger_events e
            WHERE e.state='pending' AND e.available_at_ms<=?
            AND NOT EXISTS (SELECT 1 FROM relayledger_events older
                WHERE older.stream=e.stream AND older.seq<e.seq
                AND older.state IN ('pending','inflight','dead'))
            ORDER BY e.seq LIMIT 1''',(now,)).fetchone()
        if row is None: return None
        token = str(uuid.uuid4())
        attempt, total = row['attempts']+1, row['total_attempts']+1
        database.execute('''UPDATE relayledger_events SET state='inflight',attempts=?,total_attempts=?,
            lease_token=?,lease_until_ms=?,updated_at_ms=? WHERE id=?''',
            (attempt,total,token,deadline,now,row['id']))
        database.execute('''INSERT INTO relayledger_attempts
            (event_id,number,token,started_at_ms,outcome) VALUES(?,?,?,?,'claimed')''',
            (row['id'],total,token,now))
        return Claim(row['id'],row['topic'],attempt,total,deadline,bytes(row['body']),token)


def finish(database, work, *, accepted, code, retry_at_ms=None, http_status=None, now_ms=None):
    now = instant(now_ms)
    label(code)
    if type(accepted) is not bool: raise RelayError("Acceptance must be boolean")
    if http_status is not None: integer(http_status,100,599)
    if retry_at_ms is not None:
        instant(retry_at_ms)
        if retry_at_ms < now: raise RelayError("Retry cannot be scheduled in the past")
    with transaction(database):
        row = database.execute('''SELECT * FROM relayledger_events
            WHERE id=? AND state='inflight' AND lease_token=? AND lease_until_ms>?''',
            (work.event_id,work.token,now)).fetchone()
        if row is None: raise LeaseLost("Delivery lease expired or was replaced")
        if accepted:
            state, outcome, available = 'delivered','accepted',now
        elif retry_at_ms is not None and row['attempts'] < row['max_attempts']:
            state, outcome, available = 'pending','retry',retry_at_ms
        else:
            state, outcome, available = 'dead','dead',now
        database.execute('''UPDATE relayledger_events SET state=?,available_at_ms=?,lease_token=NULL,
            lease_until_ms=NULL,updated_at_ms=?,last_code=? WHERE id=?''',
            (state,available,now,code,work.event_id))
        database.execute('''UPDATE relayledger_attempts SET finished_at_ms=?,outcome=?,http_status=?
            WHERE event_id=? AND number=? AND token=? AND outcome='claimed' ''',
            (now,outcome,http_status,work.event_id,work.total_attempt,work.token))
        return state
