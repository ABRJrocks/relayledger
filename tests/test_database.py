import sqlite3

from relayledger.database import check_schema, connect, initialize, transaction, wal_fixed
from relayledger.validation import RelayError
from helpers import DatabaseTest


class DatabaseTests(DatabaseTest):
    def test_transaction_commit_and_rollback(self):
        self.db.execute('CREATE TABLE orders(id INTEGER PRIMARY KEY)')
        with transaction(self.db): self.db.execute('INSERT INTO orders VALUES(1)')
        with self.assertRaises(RuntimeError), transaction(self.db):
            self.db.execute('INSERT INTO orders VALUES(2)')
            raise RuntimeError('abort')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0],1)

    def test_nested_transaction_never_commits_caller_work(self):
        with transaction(self.db):
            with self.assertRaises(RelayError): initialize(self.db)
            self.assertTrue(self.db.in_transaction)

    def test_schema_version_and_idempotent_init(self):
        initialize(self.db)
        check_schema(self.db)
        self.db.execute('UPDATE relayledger_meta SET version=2')
        with self.assertRaises(RelayError): initialize(self.db)

    def test_worker_open_does_not_create_missing_database(self):
        missing = self.path.parent/'missing.db'
        with self.assertRaises(sqlite3.OperationalError): connect(missing)
        self.assertFalse(missing.exists())

    def test_no_journal_mode_change_and_wal_versions(self):
        self.assertEqual(self.db.execute('PRAGMA journal_mode').fetchone()[0],'delete')
        for version in ((3,51,3),(3,50,7),(3,44,6),(3,52,0)):
            self.assertTrue(wal_fixed(version))
        for version in ((3,51,2),(3,50,6),(3,45,0)):
            self.assertFalse(wal_fixed(version))
