"""Audited dead-letter decisions and metadata-only inspection."""

from .database import check_schema, transaction
from .validation import RelayError, instant, integer, label


def resolve_dead(database, event_id, *, action, reason, now_ms=None):
    if action not in ('retry','discard'): raise RelayError("Action must be retry or discard")
    label(reason)
    now = instant(now_ms)
    with transaction(database):
        row = database.execute("SELECT state FROM relayledger_events WHERE id=?",(event_id,)).fetchone()
        if row is None or row[0]!='dead': raise RelayError("Only dead events can be retried or discarded")
        state = 'pending' if action=='retry' else 'discarded'
        database.execute('''UPDATE relayledger_events SET state=?,attempts=0,available_at_ms=?,updated_at_ms=?,
            last_code=? WHERE id=?''',(state,now,now,'operator_'+action,event_id))
        database.execute('INSERT INTO relayledger_operations(event_id,action,reason,at_ms) VALUES(?,?,?,?)',
                         (event_id,action,reason,now))


def stats(database, *, now_ms=None):
    now = instant(now_ms)
    check_schema(database)
    states = {name:0 for name in ('pending','inflight','delivered','dead','discarded')}
    with transaction(database):
        for row in database.execute('SELECT state,COUNT(*) FROM relayledger_events GROUP BY state'):
            states[row[0]] = row[1]
        blocked = database.execute('''SELECT COUNT(*) FROM relayledger_events e WHERE e.state='pending'
            AND EXISTS (SELECT 1 FROM relayledger_events older WHERE older.stream=e.stream AND older.seq<e.seq
                AND older.state IN ('pending','inflight','dead'))''').fetchone()[0]
        expired = database.execute("SELECT COUNT(*) FROM relayledger_events WHERE state='inflight' AND lease_until_ms<=?",(now,)).fetchone()[0]
    return {'states':states,'blocked_pending':blocked,'expired_leases':expired}


def events(database, *, limit=50):
    integer(limit,1,1000)
    check_schema(database)
    rows = database.execute('''SELECT id,topic,state,attempts,total_attempts,max_attempts,available_at_ms,
        created_at_ms,updated_at_ms,last_code FROM relayledger_events ORDER BY seq DESC LIMIT ?''',(limit,))
    return [dict(row) for row in rows]


def attempts(database, event_id):
    check_schema(database)
    return [dict(row) for row in database.execute('''SELECT event_id,number,started_at_ms,finished_at_ms,
        outcome,http_status FROM relayledger_attempts WHERE event_id=? ORDER BY number''',(event_id,))]
