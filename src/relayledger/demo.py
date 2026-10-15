"""An executable duplicate-delivery scenario using real HTTP and two databases."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading

from .database import connect, initialize, transaction
from .inbox import decode_event, record_once
from .operations import stats
from .outbox import enqueue
from .signing import verify
from .transport import Route, send
from .validation import RelayError, instant
from .worker import Worker


def run_demo():
    # Public demonstration key. Applications must supply their own secret.
    secret = b'relayledger-public-demo-key-only-0001'
    clock = [instant()]
    with tempfile.TemporaryDirectory(prefix='relayledger-demo-') as directory:
        sender = connect(Path(directory)/'sender.db',create=True)
        receiver_path = Path(directory)/'receiver.db'
        receiver = connect(receiver_path,create=True)
        initialize(sender)
        initialize(receiver)
        with transaction(sender):
            sender.execute('CREATE TABLE orders(id TEXT PRIMARY KEY, state TEXT NOT NULL)')
            sender.execute("INSERT INTO orders VALUES('order-1','confirmed')")
            for topic in ('order.created','order.confirmed'):
                enqueue(sender,key='order-1:'+topic,topic=topic,stream='order-1',
                        data={'order_id':'order-1'},now_ms=clock[0])
        with transaction(receiver):
            receiver.execute('CREATE TABLE effects(event_id TEXT PRIMARY KEY, topic TEXT NOT NULL)')
        receiver.close()
        counters = {'http_deliveries':0,'duplicate_deliveries':0}

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                if not verify(body,self.headers.get('RelayLedger-Signature',''),secret,now_ms=clock[0]):
                    self.send_error(401)
                    return
                database = connect(receiver_path)
                try:
                    with transaction(database):
                        fresh = record_once(database,body,now_ms=clock[0])
                        if fresh:
                            event = decode_event(body)
                            database.execute('INSERT INTO effects VALUES(?,?)',(event['id'],event['type']))
                    counters['http_deliveries'] += 1
                    counters['duplicate_deliveries'] += int(not fresh)
                    self.send_response(204)
                    self.send_header('Content-Length','0')
                    self.end_headers()
                finally:
                    database.close()

            def log_message(self,*args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        try:
            route = Route(f'http://127.0.0.1:{server.server_port}/events',secret,allow_http_loopback=True)
            routes = {topic:route for topic in ('order.created','order.confirmed')}

            class SimulatedCrash(Exception):
                pass

            def crash_after_receipt(*args,**kwargs):
                response = send(*args,**kwargs)
                if not response.accepted: raise RelayError('Demo receiver did not accept the request')
                raise SimulatedCrash()

            try:
                Worker(sender,routes,clock=lambda:clock[0],transport=crash_after_receipt).run_once()
            except SimulatedCrash:
                pass
            clock[0] += 30_001  # Advance the logical clock instead of sleeping for lease expiry.
            worker = Worker(sender,routes,clock=lambda:clock[0])
            recovered = worker.run_once()
            following = worker.run_once()
            receiver = connect(receiver_path)
            try:
                result = {**counters,'business_effects':receiver.execute('SELECT COUNT(*) FROM effects').fetchone()[0],
                          'delivered_events':stats(sender)['states']['delivered'],
                          'attempts':sender.execute('SELECT COUNT(*) FROM relayledger_attempts').fetchone()[0],
                          'expired_attempts':sender.execute("SELECT COUNT(*) FROM relayledger_attempts WHERE outcome='expired'").fetchone()[0]}
            finally:
                receiver.close()
            expected = {'http_deliveries':3,'duplicate_deliveries':1,'business_effects':2,
                        'delivered_events':2,'attempts':3,'expired_attempts':1}
            if result != expected or recovered.state!='delivered' or following.state!='delivered':
                raise RelayError('Demo recovery invariant failed')
            return {**result,'verified':True}
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
            sender.close()
