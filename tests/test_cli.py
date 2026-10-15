import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from relayledger.config import load_routes
from relayledger.validation import RelayError


class CliTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.database = self.root/'app.db'

    def run_cli(self,*args):
        return subprocess.run([sys.executable,'-m','relayledger',*map(str,args)],capture_output=True,text=True,timeout=20)

    def test_initialize_enqueue_and_inspect(self):
        self.assertEqual(self.run_cli('init',self.database).returncode,0)
        payload = self.root/'data.json'
        payload.write_text('{"private":"secret-value"}')
        result = self.run_cli('enqueue',self.database,'--key','one','--topic','order.created','--stream','one','--data',payload)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertNotIn('secret-value',result.stdout)
        values = json.loads(self.run_cli('stats',self.database).stdout)
        self.assertEqual(values['states']['pending'],1)
        self.assertNotIn('secret-value',self.run_cli('events',self.database).stdout)

    def test_missing_database_does_not_create_one(self):
        self.assertEqual(self.run_cli('stats',self.database).returncode,2)
        self.assertFalse(self.database.exists())

    def test_bad_payload_and_arguments(self):
        self.run_cli('init',self.database)
        payload = self.root/'data.json'
        payload.write_text('{"private":"do-not-echo",bad}')
        result = self.run_cli('enqueue',self.database,'--key','one','--topic','order.created','--stream','one','--data',payload)
        self.assertEqual(result.returncode,2)
        self.assertNotIn('do-not-echo',result.stderr)
        self.assertEqual(self.run_cli('events',self.database,'--limit','0').returncode,2)

    def test_routes_are_strict_and_do_not_embed_secret_values(self):
        path = self.root/'routes.json'
        path.write_text(json.dumps({'version':1,'routes':{'order.created':{'url_env':'EVENT_URL','secret_env':'EVENT_SECRET'}}}))
        routes = load_routes(path,environ={'EVENT_URL':'https://example.com/events','EVENT_SECRET':'s'*32})
        self.assertNotIn('s'*32,repr(routes))
        with self.assertRaises(RelayError): load_routes(path,environ={})
        path.write_text('{"version":1,"version":1,"routes":{}}')
        with self.assertRaises(RelayError): load_routes(path)
