import sqlite3
import shutil
import threading
import unittest
import uuid
from contextlib import closing
from pathlib import Path

from monitor_canonical import CanonicalIndex


class CanonicalConnectionTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / (".canonical-connections-" + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.index = CanonicalIndex(self.root / "index.sqlite3")
        self.addCleanup(self.index.close)

    def test_reuses_connection_across_threads_without_leaving_transaction_open(self):
        with self.index.transaction() as db:
            self.index.put(db, "test", 1)
        self.assertFalse(db.in_transaction)
        connections, errors = [], []

        def read():
            try:
                with self.index.transaction() as worker_db:
                    connections.append(worker_db)
                    self.assertEqual(self.index.get(worker_db, "test"), 1)
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=read)
        worker.start()
        worker.join(10)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(connections, [db])
        self.assertFalse(db.in_transaction)
        with closing(sqlite3.connect(self.index.path, timeout=0)) as independent, independent:
            independent.execute("INSERT INTO processing_meta VALUES('independent','true')")

    def test_rollback_and_subsequent_reuse(self):
        with self.assertRaisesRegex(ValueError, "abort"):
            with self.index.transaction() as db:
                self.index.put(db, "test", 1)
                raise ValueError("abort")
        self.assertFalse(db.in_transaction)
        with self.index.transaction() as reused:
            self.assertIs(db, reused)
            self.assertIsNone(self.index.get(reused, "test"))
            self.index.put(reused, "test", 2)

    def test_nested_transaction_does_not_commit_outer_transaction(self):
        with self.assertRaises(ValueError):
            with self.index.transaction() as db:
                with self.index.transaction() as nested:
                    self.assertIs(db, nested)
                    self.index.put(nested, "test", 1)
                raise ValueError()
        with self.index.transaction() as db:
            self.assertIsNone(self.index.get(db, "test"))
            self.index.put(db, "outer", 1)
            with self.assertRaises(ValueError):
                with self.index.transaction() as nested:
                    self.index.put(nested, "inner", 1)
                    raise ValueError()
            self.assertIsNone(self.index.get(db, "inner"))
        with self.index.transaction() as db:
            self.assertEqual(self.index.get(db, "outer"), 1)

    def test_close_releases_connection_and_rejects_future_transactions(self):
        with self.index.transaction() as db:
            with self.assertRaises(RuntimeError):
                self.index.close()
        self.index.close()
        self.index.close()
        with self.assertRaises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")
        with self.assertRaises(RuntimeError):
            with self.index.transaction():
                pass


if __name__ == "__main__":
    unittest.main()
