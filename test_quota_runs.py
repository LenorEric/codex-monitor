import copy
import shutil
import threading
import unittest
import uuid
from pathlib import Path
from unittest import mock

from monitor_interpretation import usage_runs, valid_intervals
from monitor_sampling import sample_rows, stored_points
from monitor_usage_sync import UsageDataStore, record_key, validate_sync_operation
from test_sampling import reading


class QuotaRunTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-quota-runs-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.open_store()

    def open_store(self):
        self.store = UsageDataStore(self.root / "quota.jsonl", self.root / "ledger.jsonl", "device", lambda slot: slot, threading.RLock())

    def rows(self):
        return self.store.index.rows("quota")

    def window(self, row):
        return next(iter(row["windows"].values()))

    def test_plateau_extends_canonical_row_and_backup_without_original_points(self):
        for minute in (0, 1, 2, 400):
            self.store.record_quota(reading(minute, 10))
        self.assertEqual(len(self.rows()), 1)
        window = self.window(self.rows()[0])
        self.assertEqual((window["firstAt"], window["lastAt"]), (1893510000, 1893534000))
        self.assertEqual(len(self.store.quota_path.read_text().splitlines()), 1)
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM processing_facts WHERE source='local:quota'").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM sample_records WHERE kind='quota'").fetchone()[0], 1)
            points = stored_points(db, "local")
        self.assertEqual(len(points), 1)
        self.assertEqual(usage_runs(points, "5h")[0][1:4], [1893510000, 1893534000, 10])

    def test_extension_does_not_replay_sampling_or_rebuild_quota_projection(self):
        from test_support_projection import DashboardProjector
        projector = DashboardProjector(self.store)
        self.store.record_quota(reading(0, 10))
        projector.refresh({}, 1893510000)
        rebuilds = self.store.index.metrics["quotaSamplingRebuilds"]
        with mock.patch.object(projector, "_rebuild", side_effect=AssertionError("run extension replayed quota")):
            for minute in range(1, 5):
                self.store.record_quota(reading(minute, 10))
                projector.refresh({}, 1893510000 + minute * 60)
        self.assertEqual(self.store.index.metrics["quotaSamplingRebuilds"], rebuilds)

    def test_independent_windows_and_restart_extend_same_identity(self):
        self.store.record_quota(reading(0, 10, seven=20))
        keys = [record_key("quota", row) for row in self.rows()]
        self.open_store()
        self.store.record_quota(reading(1, 10, seven=21))
        five = [row for row in self.rows() if "5h" in row["windows"]]
        seven = [row for row in self.rows() if "7d" in row["windows"]]
        self.assertEqual((len(five), len(seven)), (1, 2))
        self.assertEqual(record_key("quota", five[0]), keys[0])
        self.assertEqual(five[0]["windows"]["5h"]["lastAt"], 1893510060)

    def test_first_new_sample_extends_legacy_tail_without_losing_sibling(self):
        legacy = reading(0, 10, seven=20)
        with self.store.index.transaction() as db:
            self.store.index.write_records(db, "quota", [legacy])
        self.store.record_quota(reading(1, 10, seven=21))
        self.assertEqual(len(self.rows()), 2)
        self.assertEqual(record_key("quota", self.rows()[0]), record_key("quota", legacy))
        self.assertEqual(self.rows()[0]["windows"]["5h"]["lastAt"], 1893510060)
        self.assertEqual(self.rows()[0]["windows"]["7d"]["lastAt"], 1893510000)
        self.assertEqual(self.store.snapshot()[0][record_key("quota", legacy)]["row"]["windows"], legacy["windows"])

    def test_rejection_barrier_duplicate_and_confirmation(self):
        for row in (reading(0, 40), reading(1, 20), reading(1, 20), reading(2, 20), reading(3, 20)):
            self.store.record_quota(row)
        self.assertEqual([self.window(row)["continuous"] for row in self.rows()], [40, None, 20])
        self.assertEqual(self.window(self.rows()[-1])["lastAt"], 1893510180)
        self.open_store()
        self.store.record_quota(reading(4, 20))
        self.assertEqual(len(self.rows()), 3)

    def test_changes_split_runs_and_saved_decisions_are_not_resampled(self):
        for row in (reading(0, 10), reading(1, 11), reading(2, 10), reading(3, 10), reading(4, 10, reset=1893520900), reading(5, 10, plan="pro")):
            self.store.record_quota(row)
        self.assertEqual(len(self.rows()), 6)
        original = copy.deepcopy(self.rows())
        points = sample_rows(original)
        self.assertEqual([p["fiveHour"]["continuous"] for p in points], [10, 11, None, 10, 10, 10])
        self.assertEqual(original, self.rows())

    def test_local_runs_never_enter_cloud_records(self):
        self.store.record_quota(reading(0, 10))
        self.store.record_quota(reading(1, 10))
        records, _ = self.store.snapshot()
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(len(records), 2)
        for key, record in records.items():
            validate_sync_operation({"action": "upsert", "key": key, "record": record})
            self.assertNotIn("quotaRunVersion", record["row"])
            self.assertEqual(set(record["row"]["windows"]["5h"]), {"usedPercent", "resetAt", "plan"})
        with self.assertRaisesRegex(ValueError, "not cloud records"):
            self.store._transport_record("quota", self.rows()[0])

    def test_partial_refresh_preserves_existing_publication_and_sibling_window(self):
        first = reading(0, 10, seven=20)
        self.store.record_quota(first)
        self.store.initialize_v4_index(1893511800)
        before = self.store.snapshot()[0]
        later = reading(1, 10, seven=21)
        later["windows"]["5h"]["percentCheckedAt"] = first["checkedAt"]
        self.store.record_quota(later)
        after = self.store.snapshot()[0]
        self.assertEqual(after[record_key("quota", first)], before[record_key("quota", first)])
        self.store.record_quota(reading(1, 11))
        pending = self.store.snapshot()[0][record_key("quota", later)]["row"]["windows"]
        self.assertEqual((pending["5h"]["usedPercent"], pending["7d"]["usedPercent"]), (11, 21))

    def test_run_bounds_feed_cost_intervals_directly(self):
        for row in (reading(0, 10), reading(1, 10), reading(2, 20), reading(3, 20)):
            self.store.record_quota(row)
        with self.store.index.transaction() as db:
            runs = usage_runs(stored_points(db, "local"), "5h")
        self.assertEqual([row[1:4] for row in runs], [[1893510000, 1893510060, 10], [1893510120, 1893510180, 20]])
        self.assertEqual(valid_intervals(runs)[0][1:4], [1893510060, 1893510120, 10])
        from monitor_events import historical_console_events
        with self.store.index.transaction() as db:
            points = stored_points(db, "local")
        events = historical_console_events(points, [{"recordType": "usage", "eventId": "e", "occurredAt": 1893510090,
            "usageAccountId": "v3:a", "rawModel": "m", "cost": {"totalCostUsd": 1}}])
        self.assertEqual(len(events), 1)


if __name__ == "__main__":
    unittest.main()
