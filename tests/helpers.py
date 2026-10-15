from pathlib import Path
import tempfile
import unittest

from relayledger.database import connect, initialize

NOW = 1_700_000_000_000


class DatabaseTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)/'application.db'
        self.db = connect(self.path,create=True)
        self.addCleanup(self.db.close)
        initialize(self.db)
