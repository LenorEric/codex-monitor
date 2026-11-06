import shutil
import threading
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from monitor_canonical import CanonicalDataStore
from monitor_dashboard import UsageDashboardState
from monitor_interpretation import usage_runs, valid_intervals
from monitor_sampling import quota_point
from monitor_usage_sync import add_record_provenance
from test_sampling import reading


class CanonicalSamplingTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / (".canonical-sampling-" + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.open_store()

    def open_store(self):
        self.store = CanonicalDataStore(self.root / "quota.jsonl", self.root / "tokens.jsonl", "device", lambda slot: slot, threading.RLock())
        self.addCleanup(self.store.index.close)

    def windows(self, view="local", label="5h", account="v3:a"):
        return [row["windows"][label] for row in self.store.datasets(view)[0] if label in row["windows"] and row["sync"]["accountId"] == account]

    def test_pending_drop_is_memory_only_and_confirmation_keeps_both_times(self):
        for minute, value in enumerate((40, 20)):
            self.store.record_quota(reading(minute, value))
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40])
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 1)
        with self.store.index.transaction() as db:
            state = self.store.index.get(db, "samplingState:local:v3:a")
            self.assertEqual(state["windows"]["fiveHour"]["raw"], 40)
            self.assertNotIn("pending", str(state))
            self.assertIsNone(self.store.index.get(db, "quotaObservation:v3:a"))
        self.store.record_quota(reading(2, 20))
        for view in ("local", "merged"):
            self.assertEqual([(w["usedPercent"], w["firstAt"], w["lastAt"]) for w in self.windows(view)],
                [(40, 1893510000, 1893510000), (20, 1893510060, 1893510120)])
            self.assertTrue(all("continuous" not in w for w in self.windows(view)))
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 3)

    def test_falling_chain_discards_superseded_points_and_keeps_increasing_pair(self):
        for minute, value in enumerate((40, 20, 19, 18, 21)):
            self.store.record_quota(reading(minute, value))
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40, 18, 21])
        self.assertEqual([w["firstAt"] for w in self.windows()], [1893510000, 1893510180, 1893510240])
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 3)

    def test_any_reset_change_accepts_drop_and_discards_pending_without_zero_point(self):
        for reset in (1893520700, 1893520900, None):
            with self.subTest(reset=reset):
                account = "v3:" + str(reset)
                for row in (reading(0, 40, account=account), reading(1, 20, account=account), reading(2, 10, account=account, reset=reset)):
                    self.store.record_quota(row)
                self.assertEqual([w["usedPercent"] for w in self.windows(account=account)], [40, 10])
                self.assertEqual(self.windows(account=account)[-1]["resetAt"], reset)

    def test_reset_change_prevents_cost_interval_across_increasing_values(self):
        self.store.record_quota(reading(0, 40))
        self.store.record_quota(reading(1, 50, reset=1893520900))
        runs = usage_runs([quota_point(row) for row in self.store.datasets()[0]], "5h")
        self.assertEqual(valid_intervals(runs), [])

    def test_pending_windows_and_accounts_are_independent(self):
        self.store.record_quota(reading(0, 40, seven=10))
        self.store.record_quota(reading(1, 20, seven=11))
        self.store.record_quota(reading(2, 5, account="v3:b"))
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40])
        self.assertEqual([w["usedPercent"] for w in self.windows(label="7d")], [10, 11])
        self.store.record_quota(reading(3, 21))
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40, 20, 21])

    def test_restart_discards_unconfirmed_point(self):
        self.store.record_quota(reading(0, 40))
        self.store.record_quota(reading(1, 20))
        self.store.index.close()
        self.open_store()
        self.store.record_quota(reading(2, 21))
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40])
        self.store.record_quota(reading(3, 22))
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40, 21, 22])

    def test_failed_confirmation_reports_error_and_does_not_restore_or_retry_pending(self):
        self.store.record_quota(reading(0, 40))
        self.store.record_quota(reading(1, 20))
        with mock.patch.object(self.store, "_save", side_effect=RuntimeError("write failed")), mock.patch("monitor_canonical.traceback.print_exc") as report:
            with self.assertRaisesRegex(RuntimeError, "write failed"):
                self.store.record_quota(reading(2, 21))
        report.assert_called_once_with()
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40])
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 1)
        with self.store.index.transaction() as db:
            self.assertEqual(self.store.index.get(db, "samplingState:local:v3:a")["windows"]["fiveHour"]["raw"], 21)
        self.store.record_quota(reading(3, 22))
        for view in ("local", "merged"):
            self.assertEqual([w["usedPercent"] for w in self.windows(view)], [40, 22])

    def test_cloud_accepted_points_bypass_preprocessing_and_do_not_change_local_pending(self):
        def import_row(minute, value):
            row = reading(minute, value)
            row.pop("sync")
            with mock.patch.object(self.store.quota_preprocessor, "process", side_effect=AssertionError("cloud quota sampled again")):
                self.store.import_package("destination", "peer", str(minute), [{"kind": "quota",
                    "row": add_record_provenance("quota", row, "peer", "v3:a")}])
        self.store.record_quota(reading(0, 40))
        self.store.record_quota(reading(1, 30))
        import_row(2, 20)
        self.assertEqual([w["usedPercent"] for w in self.windows("merged")], [40, 20])
        import_row(3, 21)
        self.assertEqual([w["usedPercent"] for w in self.windows("merged")], [40, 20, 21])
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40])
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 1)
        with self.store.index.transaction() as db:
            self.assertIsNone(self.store.index.get(db, "samplingState:merged:v3:a"))
        self.store.record_quota(reading(4, 31))
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40, 30, 31])
        self.assertEqual([w["usedPercent"] for w in self.windows("merged")], [40, 30, 20, 21, 31])
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 3)

    def test_failed_merged_write_keeps_completed_local_write_and_discards_remaining_work(self):
        self.store.record_quota(reading(0, 40))
        self.store.record_quota(reading(1, 20))
        save = self.store._save
        def fail_merged(db, view, kind, row):
            if view == "merged":
                raise RuntimeError("merged write failed")
            return save(db, view, kind, row)
        with mock.patch.object(self.store, "_save", side_effect=fail_merged), mock.patch("monitor_canonical.traceback.print_exc") as report, self.assertRaisesRegex(RuntimeError, "merged write failed"):
            self.store.record_quota(reading(2, 21))
        report.assert_called_once_with()
        self.store.index.close()
        self.open_store()
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40, 20])
        self.assertEqual([w["usedPercent"] for w in self.windows("merged")], [40])
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 1)
        self.store.record_quota(reading(3, 22))
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40, 20, 22])
        self.assertEqual([w["usedPercent"] for w in self.windows("merged")], [40, 22])
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 2)

    def test_confirmation_is_preprocessed_once_for_both_canonical_views(self):
        self.store.record_quota(reading(0, 40))
        self.store.record_quota(reading(1, 20))
        with mock.patch.object(self.store.quota_preprocessor, "process", wraps=self.store.quota_preprocessor.process) as process:
            self.store.record_quota(reading(2, 21))
        self.assertEqual(process.call_count, 1)
        for view in ("local", "merged"):
            self.assertEqual([w["usedPercent"] for w in self.windows(view)], [40, 20, 21])

    def test_missing_preprocessing_state_restores_only_local_canonical_baseline(self):
        self.store.record_quota(reading(0, 40))
        self.store.record_quota(reading(1, 40))
        with self.store.index.transaction() as db:
            db.execute("DELETE FROM processing_meta WHERE name LIKE 'samplingState:%'")
        self.store.index.close()
        self.open_store()
        self.store.record_quota(reading(2, 20))
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40])
        self.store.record_quota(reading(3, 21))
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40, 20, 21])

    def test_live_status_keeps_last_accepted_value_until_confirmation(self):
        dashboard = UsageDashboardState.__new__(UsageDashboardState)
        dashboard.usage_data = self.store
        dashboard.cloud = SimpleNamespace(machine_id="device", usage_account_id=lambda slot: slot)
        first, pending, confirmed = reading(0, 40), reading(1, 20), reading(2, 21)
        dashboard._record_quota_sample_locked(first)
        dashboard._record_quota_sample_locked(pending)
        self.assertEqual(pending["windows"]["5h"]["usedPercent"], 40)
        self.assertTrue(pending["usingPreviousWindows"])
        dashboard._record_quota_sample_locked(confirmed)
        self.assertEqual(confirmed["windows"]["5h"]["usedPercent"], 21)
        self.assertFalse(confirmed["usingPreviousWindows"])
        self.assertEqual([w["usedPercent"] for w in self.windows()], [40, 20, 21])



if __name__ == "__main__":
    unittest.main()
