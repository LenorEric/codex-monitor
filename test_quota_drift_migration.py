import shutil
import threading
import unittest
import uuid
import json
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

from monitor_canonical import CanonicalDataStore
from monitor_canonical_store import CanonicalRepository as records
from monitor_final_delivery import initialize
from monitor_migration_quota_drift import migrate_local
from monitor_delivery import DashboardDelivery
import test_zero_quota_merge


class QuotaDriftMigrationTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / "tmp" / ("quota-drift-" + uuid.uuid4().hex)
        self.root.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.root)
        self.store = CanonicalDataStore(self.root / "quota.jsonl", self.root / "ledger.jsonl", "test", lambda _: "v3:test", threading.RLock())
        self.addCleanup(self.store.index.close)
        with self.store.index.transaction() as db:
            initialize(db)

    def test_compaction_boundaries_delivery_and_retry(self):
        with self.store.index.transaction() as db:
            for dataset in ("local", "merged"):
                for first, reset, value in ((100, 1000, 5), (200, 1015, 5), (300, 1016, 5), (400, 1032, 5), (500, 1033, 6), (20000, 1034, 6)):
                    records.write(db, dataset, "quota", test_zero_quota_merge.ZeroQuotaMergeTests.row(first, reset, value=value))
            db.execute("DELETE FROM delivery_pending")
        paths = {"processing_index": self.store.index.path}
        with mock.patch.object(records, "write", side_effect=RuntimeError("interrupted")), self.assertRaisesRegex(RuntimeError, "interrupted"):
            migrate_local(paths)
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM canonical_quota").fetchone()[0], 12)
            self.assertEqual(db.execute("SELECT count(*) FROM delivery_pending").fetchone()[0], 0)
        migrate_local(paths)
        with self.store.index.transaction() as db:
            for dataset in ("local", "merged"):
                windows = [row["windows"]["5h"] for row in records.rows(db, dataset, "quota", order="first")]
                self.assertEqual([(w["firstAt"], w["lastAt"], w["resetAt"]) for w in windows],
                    [(100, 300, 1016), (400, 400, 1032), (500, 500, 1033), (20000, 20000, 1034)])
            self.assertTrue(db.execute("SELECT 1 FROM delivery_pending WHERE first=400 AND last=500").fetchone())
            before = db.execute("SELECT * FROM canonical_quota ORDER BY dataset,first").fetchall()
        migrate_local(paths)
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT * FROM canonical_quota ORDER BY dataset,first").fetchall(), before)

    def test_live_canonical_tolerance(self):
        with self.store.index.transaction() as db:
            for first, reset in ((100, 1000), (200, 1015), (300, 1031)):
                self.store._quota_merge(db, "local", test_zero_quota_merge.ZeroQuotaMergeTests.row(first, reset, value=5))
            windows = [row["windows"]["5h"] for row in records.rows(db, "local", "quota", order="first")]
            self.assertEqual([(w["firstAt"], w["lastAt"]) for w in windows], [(100, 200), (300, 300)])

    def test_existing_dashboard_breaks_are_recalculated(self):
        from monitor_interpretation import connected
        with self.store.index.transaction() as db:
            for first, reset, value in ((100, 1000, 5), (200, 1001, 5), (300, 1002, 6)):
                records.write(db, "local", "quota", test_zero_quota_merge.ZeroQuotaMergeTests.row(first, reset, value=value))
        delivery = DashboardDelivery(self.store.index)
        self.addCleanup(delivery.close)

        @contextmanager
        def staging(**_):
            path = self.root / ("stage-" + uuid.uuid4().hex)
            path.mkdir()
            try:
                yield str(path)
            finally:
                shutil.rmtree(path)

        with mock.patch("monitor_final_delivery.tempfile.TemporaryDirectory", side_effect=staging):
            with mock.patch("monitor_final_delivery.connected", side_effect=lambda a, b, w: a[6] == b[6] and connected(a, b, w)):
                delivery.prepare()
            with self.store.index.transaction() as db:
                before = [json.loads(row[0])[5] for row in db.execute("SELECT body FROM delivery_items WHERE channel='usage:5h' ORDER BY first")]
            self.assertEqual(before, [1, 1, 1])
            migrate_local({"processing_index": self.store.index.path})
            delivery.prepare()
        with self.store.index.transaction() as db:
            after = [json.loads(row[0])[5] for row in db.execute("SELECT body FROM delivery_items WHERE channel='usage:5h' ORDER BY first")]
        self.assertEqual(after, [1, 0])


if __name__ == "__main__":
    unittest.main()
