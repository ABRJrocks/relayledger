"""Exercise the real default TLS verifier with a temporary certificate authority."""

import os
import ssl
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from relayledger.database import connect, initialize, transaction
from relayledger.outbox import claim, enqueue
from relayledger.transport import Route, send


class HttpsTests(unittest.TestCase):
    def test_untrusted_certificate_rejected_and_explicit_ca_trusted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            configuration = root / "openssl.cnf"
            configuration.write_text("""[req]
distinguished_name=dn
x509_extensions=extensions
prompt=no
[dn]
CN=RelayLedger local test
[extensions]
basicConstraints=critical,CA:TRUE
keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign
subjectAltName=IP:127.0.0.1
""")
            certificate, key = root / "certificate.pem", root / "key.pem"
            subprocess.run(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-keyout",
                    str(key),
                    "-out",
                    str(certificate),
                    "-config",
                    str(configuration),
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            received = []

            class Handler(BaseHTTPRequestHandler):
                def do_POST(self):
                    received.append(
                        self.rfile.read(int(self.headers["Content-Length"]))
                    )
                    self.send_response(204)
                    self.send_header("Content-Length", "0")
                    self.end_headers()

                def log_message(self, *args):
                    pass

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(certificate, key)
            server.socket = context.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            database = connect(":memory:")
            try:
                initialize(database)
                with transaction(database):
                    enqueue(
                        database,
                        key="https-test",
                        topic="order.created",
                        stream="order-1",
                        data={},
                    )
                work = claim(database)
                route = Route(
                    f"https://127.0.0.1:{server.server_port}/events",
                    b"local-tls-test-secret-32-bytes-long",
                )
                with patch.dict(
                    os.environ,
                    {
                        "SSL_CERT_FILE": str(root / "missing.pem"),
                        "SSL_CERT_DIR": str(root / "missing-dir"),
                    },
                ):
                    result = send(work, route, now_ms=1_700_000_000_000)
                self.assertEqual(result.code, "tls_error")
                self.assertEqual(received, [])
                with patch.dict(os.environ, {"SSL_CERT_FILE": str(certificate)}):
                    self.assertTrue(
                        send(work, route, now_ms=1_700_000_000_000).accepted
                    )
                self.assertEqual(received, [work.body])
            finally:
                database.close()
                server.shutdown()
                server.server_close()
                thread.join()
