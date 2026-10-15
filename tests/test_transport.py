from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import unittest

from relayledger.database import transaction
from relayledger.outbox import claim, enqueue
from relayledger.signing import verify
from relayledger.transport import Route, send
from relayledger.validation import RelayError
from helpers import DatabaseTest, NOW
from test_delivery_policy import SECRET


@contextmanager
def receiver(status=200, extra=None):
    requests = []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers['Content-Length']))
            requests.append((self.path,body,dict(self.headers)))
            self.send_response(status)
            for key,value in (extra or {}).items(): self.send_header(key,value)
            self.send_header('Content-Length','0')
            self.end_headers()
        def log_message(self,*args): pass
    server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/events',requests
    finally:
        server.shutdown(); server.server_close(); thread.join()


class TransportTests(DatabaseTest):
    def work(self):
        with transaction(self.db): enqueue(self.db,key='one',topic='order.created',stream='one',data={'private':'value'},now_ms=NOW)
        return claim(self.db,now_ms=NOW)

    def test_signed_delivery_exact_body_and_idempotency_key(self):
        work = self.work()
        with receiver() as (url,requests):
            result = send(work,Route(url,SECRET,allow_http_loopback=True),now_ms=NOW)
        self.assertTrue(result.accepted)
        _,body,headers = requests[0]
        self.assertEqual(body,work.body)
        self.assertEqual(headers['Idempotency-Key'],work.event_id)
        self.assertTrue(verify(body,headers['RelayLedger-Signature'],SECRET,now_ms=NOW))
        self.assertNotIn('value',repr(result))

    def test_redirects_and_client_failures_not_retried(self):
        work = self.work()
        for status in (302,400,401,409):
            with receiver(status,{'Location':'http://127.0.0.1/elsewhere'}) as (url,requests):
                result = send(work,Route(url,SECRET,allow_http_loopback=True),now_ms=NOW)
            self.assertFalse(result.accepted or result.retryable)
            self.assertEqual(len(requests),1)

    def test_transient_statuses_and_retry_after(self):
        work = self.work()
        for status in (408,425,429,500,503):
            with receiver(status,{'Retry-After':'30'}) as (url,_):
                result = send(work,Route(url,SECRET,allow_http_loopback=True),now_ms=NOW)
            self.assertTrue(result.retryable)
            self.assertEqual(result.retry_after_ms,30000)

    def test_excessive_retry_hint_dead_letters_instead_of_retrying_early(self):
        with receiver(429,{'Retry-After':'9999999'}) as (url,_):
            result = send(self.work(),Route(url,SECRET,allow_http_loopback=True),now_ms=NOW)
        self.assertFalse(result.retryable)
        self.assertEqual(result.code,'retry_after_excessive')

    def test_route_security_validation(self):
        for url in ('http://example.com/events','http://localhost/events','https://user:pass@example.com',
                    'https://example.com/#fragment','https://example.com/\r\nInjected','https://example.com:0'):
            with self.assertRaises(RelayError): Route(url,SECRET,allow_http_loopback=True)
        self.assertNotIn('example.com',repr(Route('https://example.com',SECRET)))
