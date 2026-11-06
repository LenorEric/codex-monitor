import copy
import shutil
import threading
import unittest
import uuid
from pathlib import Path

from monitor_canonical import CanonicalDataStore
from monitor_usage_sync import add_record_provenance
import test_canonical_pipeline as pipeline
import test_sqlite_authority as authority


class CanonicalIdentityTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / "tmp" / ("canonical-identity-" + uuid.uuid4().hex)
        self.root.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.root)
        self.open_store()

    def open_store(self):
        self.store = CanonicalDataStore(self.root / "quota.jsonl", self.root / "tokens.jsonl", "a", lambda _: "v3:account", threading.RLock())
        self.addCleanup(self.store.index.close)

    def cloud_quota(self, package, at, value):
        row = add_record_provenance("quota", pipeline.CanonicalPipelineTests.quota(at, value), "peer", "v3:account")
        self.store.import_package("destination", "peer", package, [{"kind": "quota", "row": row}])

    def test_quota_ids_are_not_stored_and_repeated_points_and_ranges_are_ignored(self):
        for at in ("2030-01-01T10:00:00Z", "2030-01-01T10:01:00Z", "2030-01-01T10:02:00Z"):
            self.store.record_quota(pipeline.CanonicalPipelineTests.quota(at, seven=10))
        before = {view: self.store.datasets(view) for view in ("local", "merged")}
        revision = self.store.index.revision()
        for at in ("2030-01-01T10:00:00Z", "2030-01-01T10:01:00Z", "2030-01-01T10:02:00Z"):
            self.store.record_quota(pipeline.CanonicalPipelineTests.quota(at, seven=10))
            self.cloud_quota(at, at, 50)
        for view in before:
            self.assertEqual(self.store.datasets(view), before[view])
        self.assertEqual(self.store.index.revision(), revision)
        with self.store.index.transaction() as db:
            self.assertEqual([column[1] for column in db.execute("PRAGMA table_info(canonical_seen)")], ["view", "record_key"])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM canonical_seen").fetchone()[0], 0)

    def test_conflicting_points_inside_range_are_discarded(self):
        for minute in range(3):
            self.store.record_quota(pipeline.CanonicalPipelineTests.quota(f"2030-01-01T10:0{minute}:00Z"))
        local = self.store.datasets("local")
        self.cloud_quota("new-value", "2030-01-01T10:01:00Z", 51)
        merged = self.store.datasets("merged")
        self.assertEqual([row["windows"]["5h"]["usedPercent"] for row in merged[0]], [50])
        self.cloud_quota("duplicate", "2030-01-01T10:01:00Z", 51)
        self.cloud_quota("point-conflict", "2030-01-01T10:01:00Z", 52)
        self.assertEqual(self.store.datasets("merged"), merged)
        self.assertEqual(self.store.datasets("local"), local)

    def test_cloud_points_extend_nearest_predecessor_or_successor(self):
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota())
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota("2030-01-01T11:00:00Z", 60))
        local = self.store.datasets("local")
        self.cloud_quota("successor", "2030-01-01T10:30:00Z", 60)
        self.cloud_quota("predecessor", "2030-01-01T10:10:00Z", 50)
        self.cloud_quota("before-first", "2030-01-01T09:00:00Z", 50)
        self.cloud_quota("after-last", "2030-01-01T12:00:00Z", 60)
        self.cloud_quota("different", "2030-01-01T10:20:00Z", 55)
        self.cloud_quota("barrier", "2030-01-01T10:25:00Z", 50)
        windows = [row["windows"]["5h"] for row in self.store.datasets("merged")[0]]
        self.assertEqual([(w["firstAt"], w["lastAt"], w["usedPercent"]) for w in windows], [
            (pipeline.moment("2030-01-01T09:00:00Z"), pipeline.moment("2030-01-01T10:10:00Z"), 50),
            (pipeline.moment("2030-01-01T10:20:00Z"), pipeline.moment("2030-01-01T10:20:00Z"), 55),
            (pipeline.moment("2030-01-01T10:25:00Z"), pipeline.moment("2030-01-01T10:25:00Z"), 50),
            (pipeline.moment("2030-01-01T10:30:00Z"), pipeline.moment("2030-01-01T12:00:00Z"), 60)])
        self.assertEqual(self.store.datasets("local"), local)
        revision = self.store.index.revision()
        for at in ("2030-01-01T09:00:00Z", "2030-01-01T09:30:00Z", "2030-01-01T10:10:00Z", "2030-01-01T10:20:00Z"):
            self.cloud_quota(at, at, 99)
        self.assertEqual(self.store.index.revision(), revision)

    def test_successor_with_different_reset_or_plan_remains_separate(self):
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota())
        for index, changes in enumerate(({"resetAt": 1893500001}, {"plan": "pro"})):
            with self.subTest(changes=changes):
                row = pipeline.CanonicalPipelineTests.quota(f"2030-01-01T09:0{index}:00Z")
                row["windows"]["5h"].update(changes)
                self.store.import_package("destination", "peer", str(index), [{"kind": "quota", "row": add_record_provenance("quota", row, "peer", "v3:account")}])
                self.assertEqual(len(self.store.datasets("merged")[0]), index + 2)

    def test_repeated_pending_point_cannot_confirm_itself(self):
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota(value=50))
        pending = pipeline.CanonicalPipelineTests.quota("2030-01-01T10:01:00Z", 40)
        self.store.record_quota(pending)
        self.store.record_quota(pending)
        for view in ("local", "merged"):
            self.assertEqual([row["windows"]["5h"]["usedPercent"] for row in self.store.datasets(view)[0]], [50])
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 1)
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota("2030-01-01T10:02:00Z", 41))
        for view in ("local", "merged"):
            self.assertEqual([row["windows"]["5h"]["usedPercent"] for row in self.store.datasets(view)[0]], [50, 40, 41])

    def test_cost_identity_deduplication_survives_restart_and_cross_machine_import(self):
        row = authority.SQLiteAuthorityTests.ledger(cost=2)
        self.store.record_ledger([row])
        self.store.index.close()
        self.open_store()
        conflicting = copy.deepcopy(row)
        conflicting["cost"]["totalCostUsd"] = 99
        self.assertFalse(self.store.record_ledger([conflicting]))
        remote = add_record_provenance("tokenLedger", conflicting, "peer", "v3:account")
        self.store.import_package("destination", "peer", "cost-duplicate", [{"kind": "tokenLedger", "row": remote}])
        for view in ("local", "merged"):
            self.assertEqual(len(self.store.datasets(view)[1]), 1)
            self.assertEqual(self.store.datasets(view)[1][0]["cost"]["totalCostUsd"], 2)
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM canonical_seen").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
