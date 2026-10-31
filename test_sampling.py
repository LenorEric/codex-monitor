import copy
import json
import shutil
import uuid
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from monitor_dashboard import UsageDashboardState
from monitor_processing import ProcessingIndex
from monitor_quota import compact_quota
from monitor_sampling import sample_rows, stored_points
from monitor_usage_sync import UsageDataStore, add_record_provenance, quota_sync_boundary_rows, record_key, v4_logical_hash, v4_record_envelope


def reading(minute, percent, *, account="v3:a", reset=1893520800, seven=None, plan="plus"):
    row = {"checkedAt": datetime.fromtimestamp(1893510000 + minute * 60, timezone.utc).isoformat(), "accountSlotId": account,
        "accountLabel": account, "windows": {"5h": {"usedPercent": percent, "resetAt": reset, "plan": plan}}}
    if seven is not None:
        row["windows"]["7d"] = {"usedPercent": seven, "resetAt": 1894114800, "plan": plan}
    return add_record_provenance("quota", row, "device", account)


class SamplingTests(unittest.TestCase):
    def values(self, values):
        return [point["fiveHour"]["continuous"] for point in sample_rows([reading(index, value) for index, value in enumerate(values)])]

    def test_decrease_protection_accepts_equal_or_increasing_followup(self):
        for values, expected in (([40, 20, 20], [40, None, 20]), ([40, 20, 21], [40, None, 21]),
                ([40, 20, 19, 19], [40, None, None, 19]), ([40, 20, 39], [40, None, 39]), ([40, 20, 45], [40, None, 45]),
                ([40, 39, 39], [40, None, 39]), ([40, 0, 0], [40, None, 0]), ([1, 99], [1, 99])):
            with self.subTest(values=values):
                self.assertEqual(self.values(values), expected)

    def test_reset_comparison_is_strict_for_both_periods(self):
        for label, output, period in (("5h", "fiveHour", 18000), ("7d", "sevenDay", 604800)):
            for offset, expected in ((-1, None), (0, None), (1, 20)):
                with self.subTest(label=label, offset=offset):
                    rows = [reading(0, 40), reading(1, 20)]
                    rows[0]["windows"] = {label: {"usedPercent": 40, "resetAt": 1893510000 + period - 7200, "plan": "plus"}}
                    rows[1]["windows"] = {label: {"usedPercent": 20, "resetAt": 1893510000 + period + offset, "plan": "plus"}}
                    self.assertEqual(sample_rows(rows)[-1][output]["continuous"], expected)

    def test_reset_boundary_for_each_window_preserves_raw_and_state(self):
        for label, name, period in (("5h", "fiveHour", 18000), ("7d", "sevenDay", 604800)):
            with self.subTest(label=label):
                rows = [reading(0, 90), reading(120, 10)]
                rows[0]["windows"] = {label: {"usedPercent": 90, "resetAt": 1893510000 + period, "plan": "pro"}}
                rows[1]["windows"] = {label: {"usedPercent": 10, "resetAt": 1893513600 + period, "plan": "pro"}}
                original, states = copy.deepcopy(rows), {}
                points = sample_rows(rows, states=states)
                self.assertEqual([point[name]["continuous"] for point in points], [90, 0, 10])
                self.assertEqual(points[1]["timestamp"], 1893513600)
                self.assertEqual(points[1]["resetWindow"], name)
                self.assertEqual(points[1][name]["plan"], "pro")
                self.assertEqual(states["v3:a"][name]["raw"], 10)
                self.assertEqual(rows, original)

    def test_reset_boundary_requires_nonzero_accepted_value_and_valid_time(self):
        for initial, reset, plan in ((0, 1893531600, "plus"), (90, None, "plus"), (90, 1893520800, "plus"),
                (90, 1893517200, "plus"), (90, 1893535201, "plus"), (90, 1893531600, "pro")):
            with self.subTest(initial=initial, reset=reset, plan=plan):
                self.assertFalse(any(point.get("resetWindow") for point in sample_rows([reading(0, initial), reading(120, 10, reset=reset, plan=plan)])))
        # Forward reset times also establish a zero baseline when usage has already overtaken the previous reading.
        self.assertEqual([point["fiveHour"]["continuous"] for point in sample_rows([reading(0, 10), reading(120, 20, reset=1893531600)])], [10, 0, 20])

    def test_supplied_forward_reset_example_and_plateau_timestamp(self):
        self.assertEqual(sample_rows([reading(0, 40), reading(1, 20, reset=1893531600)])[1]["fiveHour"]["continuous"], 20)
        rows = [reading(0, 40), reading(60, 40), reading(61, 20, reset=1893531600)]
        self.assertIsNone(sample_rows(rows)[2]["fiveHour"]["continuous"])

    def test_no_due_reset_or_backward_reset_exceptions(self):
        for reset in (1893520800, 1893517200):
            points = sample_rows([reading(0, 40), reading(240, 20, reset=reset), reading(241, 20, reset=reset)])
            self.assertEqual([point["fiveHour"]["continuous"] for point in points], [40, None, 20])

    def test_accounts_windows_plans_and_missing_are_independent(self):
        rows = [reading(0, 40, seven=10), reading(1, 20, seven=11), reading(2, None), reading(3, 20, seven=12)]
        points = sample_rows(rows)
        self.assertEqual([point["fiveHour"]["continuous"] for point in points], [40, None, None, 20])
        self.assertEqual([point["sevenDay"]["continuous"] for point in points], [10, 11, None, 12])
        self.assertEqual(sample_rows([reading(0, 40), reading(1, 20, plan="pro")])[1]["fiveHour"]["continuous"], 20)
        self.assertEqual(sample_rows([reading(0, 40), reading(1, 20, account="v3:b")])[1]["fiveHour"]["continuous"], 20)

    def test_ignore_remaining_duration_and_isolate_malformed_window(self):
        response = {"plan_type": "plus", "rate_limit": {"primary_window": {"limit_window_seconds": 18000, "reset_at": 1893520800, "used_percent": 40},
            "secondary_window": {"limit_window_seconds": 604800, "reset_at": 1894114800, "used_percent": 11}}}
        for remaining in (None, "invalid", -10, {}, 999999999):
            response["rate_limit"]["primary_window"]["reset_after_seconds"] = remaining
            self.assertEqual(compact_quota(response)["windows"]["5h"]["values"]["reset_at"], 1893520800)
        response["rate_limit"]["primary_window"]["reset_at"] = "invalid"
        quota = compact_quota(response)
        self.assertTrue(quota["windows"]["5h"]["unavailable"])
        self.assertEqual(quota["windows"]["7d"]["values"]["used_percent"], 11)
        self.assertIn("reset_at", quota["errors"][0])


class SamplingStorageTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-sampling-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.store = UsageDataStore(self.root / "quota.jsonl", self.root / "ledger.jsonl", "device", lambda slot: slot, threading.RLock())
        self.store.ingest_local()

    def append(self, row):
        self.store.record_quota(row)
        self.store.ingest_local()

    def points(self, view="local"):
        with self.store.index.transaction() as db:
            return stored_points(db, view)

    def test_independent_refresh_does_not_advance_or_replace_other_window(self):
        first = reading(0, 10, seven=20)
        self.append(first)
        later = reading(1, 10, seven=21)
        later["windows"]["5h"]["percentCheckedAt"] = first["checkedAt"]
        self.append(later)
        with self.store.index.transaction() as db:
            state = self.store.index.get(db, "samplingState:local:v3:a")["windows"]
            self.assertEqual(state["fiveHour"]["checkedAt"], first["checkedAt"])
            self.assertEqual(state["sevenDay"]["checkedAt"], later["checkedAt"])
            self.assertEqual(db.execute("SELECT count(*) FROM quota_windows WHERE window='5h'").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM quota_windows WHERE window='7d'").fetchone()[0], 1)
            self.assertEqual(
                [row[0] for row in db.execute("SELECT seven_day_usedPercent FROM sample_records ORDER BY id")],
                [20.0, 21.0],
            )
        rows = self.store.index.rows("quota")
        self.assertEqual(rows[0]["windows"]["7d"]["usedPercent"], 20)
        self.assertEqual(set(rows[1]["windows"]), {"7d"})
        # A second partial reading at the same timestamp preserves its sibling.
        partial = reading(1, 11)
        self.append(partial)
        self.assertEqual(set(self.store.index.rows("quota")[1]["windows"]), {"5h", "7d"})
        restarted = ProcessingIndex(self.store.v4_cache_path)
        with restarted.transaction() as db:
            self.assertEqual(restarted.get(db, "samplingState:local:v3:a")["windows"]["sevenDay"]["raw"], 21)
        reverse = reading(2, 12, seven=21)
        reverse["windows"]["7d"]["percentCheckedAt"] = later["checkedAt"]
        self.append(reverse)
        with self.store.index.transaction() as db:
            state = self.store.index.get(db, "samplingState:local:v3:a")["windows"]
            self.assertEqual(state["fiveHour"]["checkedAt"], reverse["checkedAt"])
            self.assertEqual(state["sevenDay"]["checkedAt"], later["checkedAt"])
            self.assertEqual(db.execute("SELECT count(*) FROM sample_records WHERE seven_day_usedPercent IS NOT NULL").fetchone()[0], 2)


    def test_reset_projection_costs_restart_correction_and_migration(self):
        import monitor_migrations
        from monitor_projection import DashboardProjector
        from monitor_dashboard import dashboard_cost_usage
        rows = [reading(0, 90, seven=10), reading(120, 10, seven=12, reset=1893531600)]
        accounts = {"items": []}
        projector = DashboardProjector(self.store)
        self.append(rows[0])
        projector.refresh(accounts, 1893510000)
        self.append(rows[1])
        projector.refresh(accounts, 1893517200)
        expected = sample_rows(rows)
        self.assertEqual(self.points(), expected)
        for view in ("local", "merged"):
            frame = projector.snapshot(view)
            self.assertEqual(len(frame["quotaPoints"]), 3)
            self.assertEqual(len({point["_key"] for point in frame["quotaPoints"]}), 3)
            self.assertEqual([point["deltaPercent"] for point in frame["costUsage"]["fiveHour"]], [10])
            self.assertEqual([point["deltaPercent"] for point in frame["costUsage"]["sevenDay"]], [2])
        ledger = [{"recordType": "usage", "occurredAt": stamp, "usageAccountId": "v3:a", "rawModel": "m", "cost": {"totalCostUsd": cost}}
            for stamp, cost in ((1893511800, 99), (1893515400, 2))]
        costs = dashboard_cost_usage(expected, ledger)
        self.assertEqual(costs["fiveHour"][0]["totalCostUsd"], 2)
        self.assertEqual(costs["sevenDay"][0]["totalCostUsd"], 101)
        original = self.store.quota_path.read_bytes()
        (self.root / monitor_migrations.DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":16}', encoding="utf-8")
        args = (self.root, self.root / "history.jsonl", self.store.quota_path, self.root / "sessions.jsonl", self.store.token_ledger_path, self.root / "samples.jsonl")
        migration = monitor_migrations.DATA_MIGRATIONS[17]
        def interrupted(paths):
            migration(paths)
            raise RuntimeError("migration interrupted")
        with mock.patch.dict(monitor_migrations.DATA_MIGRATIONS, {17: interrupted}), self.assertRaisesRegex(RuntimeError, "migration interrupted"):
            monitor_migrations.migrate_history_data(*args)
        self.assertEqual(self.store.quota_path.read_bytes(), original)
        self.assertEqual(self.points(), expected)
        self.assertEqual(monitor_migrations.migrate_history_data(*args), [17, 18, 19, 20])
        self.assertEqual(self.store.quota_path.read_bytes(), original)
        self.store.index = ProcessingIndex(self.store.v4_cache_path)
        self.store.ingest_local()
        self.assertEqual(self.points(), expected)
        self.assertEqual(monitor_migrations.migrate_history_data(*args), [])
        # Removing the reset in a corrected source must remove its derived zero point too.
        rows[1]["windows"]["5h"]["resetAt"] = rows[0]["windows"]["5h"]["resetAt"]
        for row in rows:
            self.store.record_quota(row)
        self.store.ingest_local()
        projector = DashboardProjector(self.store)
        projector.refresh(accounts, 1893517200)
        self.assertEqual(len(projector.snapshot("local")["quotaPoints"]), 2)

    def test_both_reset_windows_at_observation_time_keep_distinct_points(self):
        rows = [reading(0, 90, seven=80), reading(60, 10, seven=20)]
        for index, row in enumerate(rows):
            for label, period in (("5h", 18000), ("7d", 604800)):
                row["windows"][label]["resetAt"] = 1893510000 + index * 3600 + period
            self.append(row)
        self.assertEqual(self.points(), sample_rows(rows))
        self.assertEqual(len(self.points()), 4)
        self.assertEqual([point.get("resetWindow") for point in self.points()], [None, "fiveHour", "sevenDay", None])
        from monitor_projection import DashboardProjector
        projector = DashboardProjector(self.store)
        projector.refresh({"items": []}, 1893513600)
        points = projector.snapshot("local")["quotaPoints"]
        self.assertEqual(len(points), 4)
        self.assertEqual(len({point["_key"] for point in points}), 4)

    def test_archive_append_incremental_processing_restart_and_reconstruction(self):
        rows = [reading(index, value) for index, value in enumerate([40, 20, 19, 19, 80, 80])]
        for row in rows:
            self.append(row)
        self.assertEqual(len([line for line in self.store.quota_path.read_text().splitlines() if line.strip()]), len(rows))
        self.assertEqual(len(quota_sync_boundary_rows(rows, "device")), len(rows))
        self.assertEqual(self.points(), sample_rows(rows))
        rebuilds = self.store.index.metrics["quotaSamplingRebuilds"]
        self.append(reading(6, 70))
        self.assertEqual(self.store.index.metrics["quotaSamplingRebuilds"], rebuilds)
        self.store.index = ProcessingIndex(self.store.v4_cache_path)
        self.append(reading(7, 70))
        expected = copy.deepcopy(self.points())
        self.assertEqual([point["fiveHour"]["continuous"] for point in expected], [40, None, None, 19, 80, 80, None, 70])
        # A new database must reproduce every decision, including rejected points.
        rebuilt = UsageDataStore(self.store.quota_path, self.store.token_ledger_path, "device", lambda slot: slot, threading.RLock(), cache_path=self.root / "rebuilt.json")
        self.assertEqual(rebuilt.datasets(), ([], []))
        for row in self.store.index.rows("quota"):
            rebuilt.record_quota(row)
        with rebuilt.index.transaction() as db:
            self.assertEqual(stored_points(db, "local"), expected)

    def test_duplicate_does_not_confirm_and_late_correction_replays(self):
        self.append(reading(0, 40))
        self.append(reading(2, 20))
        self.append(reading(2, 20))
        self.assertEqual([point["fiveHour"]["continuous"] for point in self.points()], [40, None])
        self.append(reading(1, 20))
        self.assertEqual([point["fiveHour"]["continuous"] for point in self.points()], [40, None, 20])
        self.store.record_quota(reading(0, 10))
        self.assertEqual([point["fiveHour"]["continuous"] for point in self.points()], [10, 20, 20])

    def test_failed_sql_transaction_replays_durable_raw_without_double_confirmation(self):
        self.append(reading(0, 40))
        import monitor_sampling
        refresh = monitor_sampling.refresh
        def fail(db, index):
            refresh(db, index)
            raise RuntimeError("interrupted before commit")
        with mock.patch.object(monitor_sampling, "refresh", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                self.store.record_quota(reading(1, 20))
        self.assertEqual([point["fiveHour"]["continuous"] for point in self.points()], [40])
        self.store.record_quota(reading(1, 20))
        self.assertEqual([point["fiveHour"]["continuous"] for point in self.points()], [40, None])
        self.append(reading(2, 20))
        self.assertEqual(self.points()[-1]["fiveHour"]["continuous"], 20)

    def test_live_status_keeps_rejected_window_timestamp_and_updates_sibling(self):
        state = UsageDashboardState.__new__(UsageDashboardState)
        state.args = SimpleNamespace(quota_history=self.store.quota_path)
        state.cloud = SimpleNamespace(machine_id="device", usage_account_id=lambda account: account)
        state.usage_data = self.store
        first, second, third = reading(0, 40, seven=10), reading(1, 20, seven=11), reading(2, 20, seven=12)
        for row in (first, second):
            state._record_quota_sample_locked(row)
        self.assertEqual(second["windows"]["5h"]["usedPercent"], 40)
        self.assertEqual(second["windows"]["5h"]["percentCheckedAt"], first["checkedAt"])
        self.assertEqual(second["windows"]["7d"]["usedPercent"], 11)
        state._record_quota_sample_locked(third)
        self.assertEqual(third["windows"]["5h"]["usedPercent"], 20)
        raw = [json.loads(line) for line in self.store.quota_path.read_text().splitlines() if line.strip()]
        self.assertEqual([row["windows"]["5h"]["usedPercent"] for row in raw], [40, 20, 20])
        self.assertNotIn("continuous", self.store.quota_path.read_text())
        fourth = reading(3, 20, seven=13)
        fourth["windows"]["5h"]["percentCheckedAt"] = third["checkedAt"]
        state._record_quota_sample_locked(fourth)
        self.assertNotIn("5h", fourth["rejectedWindows"])
        self.assertEqual(fourth["windows"]["5h"]["percentCheckedAt"], third["checkedAt"])
        self.assertEqual(fourth["windows"]["7d"]["percentCheckedAt"], fourth["checkedAt"])


    def test_cloud_raw_replay_matches_local_and_preserves_rejected_records(self):
        rows = [reading(index, value) for index, value in enumerate([40, 20, 20, 19, 19])]
        for row in rows:
            self.append(row)
        peer = UsageDataStore(self.root / "peer-quota.jsonl", self.root / "peer-ledger.jsonl", "other", lambda slot: slot, threading.RLock(), account_mapper=lambda *_: ("v3:a", "v3:a"))
        entries = [v4_record_envelope("device", record_key("quota", row), {"kind": "quota", "row": row}, "2030-01-01T00:00Z") for row in rows]
        peer.v4_apply_day("device", {"day": "2030-01-01", "parts": {"bulk": "hash"}, "logicalHash": v4_logical_hash(entries)}, {"bulk": entries})
        with peer.index.transaction() as db:
            self.assertEqual(stored_points(db, "merged"), self.points())
            self.assertEqual(stored_points(db, "local"), [])

    def test_inactive_session_and_external_auth_observations_share_protection(self):
        state = UsageDashboardState.__new__(UsageDashboardState)
        auth_path = self.root / "auth.json"
        auth_path.write_text(json.dumps({"tokens": {"account_id": "a"}}), encoding="utf-8")
        state.args = SimpleNamespace(quota_history=self.store.quota_path, auth=auth_path, timeout=1, retry_limit=0)
        state.cloud = SimpleNamespace(machine_id="device", usage_account_id=lambda account: "v3:a")
        state.usage_data, state.lock, state.opener = self.store, threading.RLock(), object()
        state.account_statuses = {}
        state.accounts = SimpleNamespace(lock=state.lock, commit_polled_credentials=lambda *_: True, status=lambda: {"items": [{"id": "slot", "label": "Account"}]})
        state.external_auth_validation_fingerprints, state.external_auth_validation_threads = set(), set()
        state._signal_dashboard_cache = mock.Mock()
        credential = {"id": "slot", "label": "Account", "active": True, "data": auth_path.read_bytes(), "fingerprint": "original"}
        responses = [{"usage": {"windows": {"5h": {"values": {"used_percent": value, "reset_at": 1893520800, "plan_type": "plus"}}}}}
            for value in (40, 20, 20, 19)]
        times = [reading(index, 0)["checkedAt"] for index in range(4)]
        with mock.patch("monitor_dashboard.fetch_usage", side_effect=responses), mock.patch("monitor_dashboard.now_iso", side_effect=times), mock.patch("builtins.print"):
            state._poll_inactive_account(credential)
            state._poll_inactive_account(credential)
            state._retrieve_session_refresh_reset_at(credential, "5h")
            state._start_external_auth_validation(credential)
            state.wait_for_external_auth_validations(5)
        self.assertFalse(state.external_auth_validation_threads)
        self.assertEqual([point["fiveHour"]["continuous"] for point in self.points()], [40, None, 20, None])
        self.assertEqual(state.account_statuses["slot"]["windows"]["5h"]["usedPercent"], 20)
        self.assertEqual(len([line for line in self.store.quota_path.read_text().splitlines() if line.strip()]), 4)


if __name__ == "__main__":
    unittest.main()
