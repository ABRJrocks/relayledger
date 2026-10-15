"""Call this adapter from an authenticated HTTP route after enforcing body limits.

Initialize the RelayLedger schema and received_events table at application startup.
Return 2xx after this function succeeds, including for a matching duplicate.
"""

from relayledger import decode_event, record_once, transaction, verify


def apply_event(database, raw_body, signature_header, signing_secret):
    if not verify(raw_body,signature_header,signing_secret):
        raise ValueError('Invalid delivery signature')
    with transaction(database):
        fresh = record_once(database,raw_body)
        if fresh:
            event = decode_event(raw_body)
            database.execute('INSERT INTO received_events(event_id,topic) VALUES(?,?)',
                             (event['id'],event['type']))
    return fresh
