import gzip
import json
import unittest
import shutil
import uuid
import io
import http.server
import threading
import urllib.request
import urllib.error
from types import SimpleNamespace
from pathlib import Path
from unittest import mock

import monitor_migrations
from monitor_delivery import DAY, DashboardDelivery, cost_groups, reduce_cost, reduce_quota, session_groups
from monitor_processing import ProcessingIndex


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path.cwd() / f".test-delivery-{uuid.uuid4().hex}"
        self.directory.mkdir()
        self.addCleanup(shutil.rmtree, self.directory)
        self.index = ProcessingIndex(self.directory / "index.sqlite3")
        self.delivery = DashboardDelivery(self.index)
        self.start = 1893456000
        with self.index.transaction() as db:
            self.index.put(db, "displayRevision", 1)

    def quota(self, stamp, value=10, key=None, view="local", **extra):
        from datetime import datetime, timezone
        row = {"timestamp": stamp, "checkedAt": datetime.fromtimestamp(stamp, timezone.utc).isoformat(), "usageAccountId": "a", "accountSlotId": "slot", "accountLabel": "Account",
            "fiveHour": {"raw": value, "continuous": value, "plan": "plus", "resetAt": "2030-02-01T00:00:00Z"},
            "sevenDay": {"raw": value, "continuous": value, "plan": "plus", "resetAt": "2030-02-07T00:00:00Z"}, **extra}
        with self.index.transaction() as db:
            db.execute("INSERT OR REPLACE INTO processing_quota_points VALUES(?,?,?,?,?)", (view, "a", stamp, key or str(stamp), json.dumps(row)))
        return row

    def token(self, stamp, key="s", models=None, view="local"):
        from datetime import datetime, timezone
        row = {"_key": key, "usageAccountId": "a", "accountSlotId": "slot", "accountLabel": "Account", "sessionId": key,
            "updatedAt": datetime.fromtimestamp(stamp, timezone.utc).isoformat(), "startedAt": "2029-12-01T00:00:00Z",
            "byModel": models or {"m": {"usageTokens": {"freshInputTokens": 10, "outputTokens": 2}, "cost": {"totalCostUsd": 3}}}}
        with self.index.transaction() as db:
            db.execute("INSERT OR REPLACE INTO processing_display VALUES(?,?,?,?,?,?)", (view, "a", "tokenSessions", key, stamp, json.dumps(row)))
        return row

    def materialize(self, manifest):
        return [json.loads(self.delivery.blob(digest, False)) for digest in manifest["chunks"]]

    def test_compact_account_records_rebuild_after_v12(self):
        from monitor_migrations import _migrate_history_to_v12
        self.quota(self.start)
        self.token(self.start)
        self.delivery.prepare(self.start + 100)
        paths = {"processing_index": self.index.path, "dashboard_cache": self.directory / "display.json"}
        for _ in range(2):
            _migrate_history_to_v12(paths)
            with self.index.transaction() as db:
                self.assertEqual(db.execute("SELECT count(*) FROM processing_quota_points").fetchone()[0], 0)
                self.assertEqual(db.execute("SELECT count(*) FROM dashboard_blobs").fetchone()[0], 0)
        self.quota(self.start)
        self.token(self.start)
        self.delivery.prepare(self.start + 100)
        for manifest in (self.delivery.manifest("local", "All"), self.delivery.manifest("local", "Date", self.start, self.start + DAY)):
            self.assertEqual(manifest["tokenSessions"][0]["usageAccountId"], "a")
            self.assertNotIn("accountSlotId", manifest["tokenSessions"][0])
            for frame in self.materialize(manifest):
                for point in frame["quotaPoints"]:
                    self.assertEqual(point["timestamp"], self.start)
                    self.assertEqual(point["usageAccountId"], "a")
                    self.assertFalse({"checkedAt", "accountSlotId", "accountLabel"} & point.keys())

    def test_contract_12_failure_and_crash_roll_back_and_retry_automatically(self):
        import sqlite3
        from contextlib import closing
        self.quota(self.start)
        self.delivery.prepare(self.start + 100)
        database = self.directory / "cache-v4.sqlite3"
        with closing(sqlite3.connect(self.index.path)) as source, closing(sqlite3.connect(database)) as target:
            source.backup(target)
        state = self.directory / monitor_migrations.DATA_MIGRATION_STATE_FILENAME
        state.write_text('{"dataContractVersion":11}', encoding="utf-8")
        history = self.directory / "quota.jsonl"
        history.write_text('{"original":"preserved"}\n', encoding="utf-8")
        account_manifest = self.directory / "accounts" / "accounts.json"
        account_manifest.parent.mkdir()
        account_manifest.write_text(json.dumps({"version": 3, "accounts": [{"id": "a"}, {"id": "b"}]}), encoding="utf-8")
        legacy_manifest = self.directory / "codex" / "usage-monitor-accounts" / "accounts.json"
        legacy_manifest.parent.mkdir(parents=True)
        legacy_manifest.write_text(json.dumps({"version": 3, "accounts": [{"id": "x"}, {"id": "y"}], "slotOrder": ["y", "x"]}), encoding="utf-8")
        original_manifest = account_manifest.read_bytes()
        original_legacy = legacy_manifest.read_bytes()
        def migrate():
            return monitor_migrations.migrate_history_data(self.directory, self.directory / "history", history, self.directory / "sessions", self.directory / "ledger",
                self.directory / "samples", target_version=12, usage_sync_cache_path=self.directory / "cache.json", codex_home=self.directory / "codex")
        def interrupted(paths):
            monitor_migrations._migrate_history_to_v12(paths)
            raise RuntimeError("interrupted migration")
        with mock.patch.dict(monitor_migrations.DATA_MIGRATIONS, {12: interrupted}), self.assertRaisesRegex(RuntimeError, "interrupted migration"):
            migrate()
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 11)
        self.assertEqual(account_manifest.read_bytes(), original_manifest)
        self.assertEqual(legacy_manifest.read_bytes(), original_legacy)
        with closing(sqlite3.connect(database)) as db:
            self.assertGreater(db.execute("SELECT count(*) FROM dashboard_blobs").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT count(*) FROM processing_quota_points").fetchone()[0], 1)
        def crashed(paths):
            monitor_migrations._migrate_history_to_v12(paths)
            raise KeyboardInterrupt("simulated process interruption")
        with mock.patch.dict(monitor_migrations.DATA_MIGRATIONS, {12: crashed}), self.assertRaises(KeyboardInterrupt):
            migrate()
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 11)
        self.assertTrue((self.directory / monitor_migrations.DATA_MIGRATION_JOURNAL_FILENAME).exists())
        # Recovery must restore both manifests before the next migration attempt.
        def verify_recovery(paths):
            self.assertEqual(account_manifest.read_bytes(), original_manifest)
            self.assertEqual(legacy_manifest.read_bytes(), original_legacy)
            with closing(sqlite3.connect(database)) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM processing_quota_points").fetchone()[0], 1)
            monitor_migrations._migrate_history_to_v12(paths)
        with mock.patch.dict(monitor_migrations.DATA_MIGRATIONS, {12: verify_recovery}):
            self.assertEqual(migrate(), [12])
        self.assertEqual(migrate(), [])
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 12)
        self.assertEqual(json.loads(account_manifest.read_text())["slotOrder"], ["a", "b"])
        self.assertEqual(json.loads(legacy_manifest.read_text())["slotOrder"], ["y", "x"])
        self.assertFalse((self.directory / monitor_migrations.DATA_MIGRATION_JOURNAL_FILENAME).exists())
        with closing(sqlite3.connect(database)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM processing_quota_points").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT count(*) FROM dashboard_blobs").fetchone()[0], 0)
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='processing_slot_cost_activity'").fetchone())
        self.assertEqual(history.read_text(), '{"original":"preserved"}\n')

    def test_warm_manifest_and_idle_preparation_do_not_reload_or_encode_history(self):
        self.quota(self.start)
        self.token(self.start)
        self.delivery.prepare(self.start + 100)
        manifest = self.delivery.manifest("local", "All")
        self.assertEqual(manifest["tokenSessions"][0]["sessionCount"], 1)
        self.assertEqual(manifest["protocolVersion"], 4)
        for digest in manifest["chunks"]:
            self.assertEqual(gzip.decompress(self.delivery.blob(digest, True)), self.delivery.blob(digest, False))
            self.assertEqual(json.loads(self.delivery.blob(digest, False))["tokenSessions"], [])
        count = self.delivery.metrics["chunksEncoded"]
        with mock.patch.object(self.delivery, "_load", side_effect=AssertionError("history reload")), mock.patch("monitor_delivery.gzip.compress", side_effect=AssertionError("recompression")):
            self.assertEqual(self.delivery.manifest("local", "All"), manifest)
            self.delivery.prepare(self.start + 101)
        self.assertEqual(self.delivery.metrics["chunksEncoded"], count)
        self.assertEqual(DashboardDelivery(self.index).manifest("local", "All"), manifest)

    def test_append_replaces_only_affected_day_and_old_chunks_remain_downloadable(self):
        self.quota(self.start)
        self.quota(self.start + DAY)
        self.delivery.prepare(self.start + 2 * DAY)
        old = self.delivery.manifest("local", "All")
        self.quota(self.start + DAY + 60, value=11)
        self.delivery.prepare(self.start + 2 * DAY)
        new = self.delivery.manifest("local", "All")
        self.assertNotEqual(old["generation"], new["generation"])
        self.assertEqual(old["chunks"][0], new["chunks"][0])
        self.assertNotEqual(old["chunks"][1], new["chunks"][1])
        self.assertTrue(self.delivery.blob(old["chunks"][1], False))

    def test_day_has_full_detail_and_uses_explicit_local_midnight_instants(self):
        for minute in range(20):
            self.quota(self.start + minute * 60, value=10 + minute)
        self.quota(self.start - 30, value=9)
        self.quota(self.start + 2000, value=30)
        self.token(self.start)
        self.token(self.start + DAY, key="next-day")
        self.delivery.prepare(self.start + 100 * DAY)
        manifest = self.delivery.manifest("local", "Date", self.start, self.start + 1500)
        points = [row for part in self.materialize(manifest) for row in part["quotaPoints"] if not row.get("contextOnly")]
        self.assertEqual(len(points), 20)
        self.assertEqual(manifest["tokenSessions"][0]["sessionCount"], 1)
        self.assertEqual(manifest["bounds"], [self.start, self.start + 1500])
        with self.assertRaises(ValueError):
            self.delivery.manifest("local", "Date", self.start, self.start + 27 * 3600)
        for hours in (23, 25):
            self.assertIsNotNone(self.delivery.manifest("local", "Date", self.start, self.start + hours * 3600))

    def test_moved_session_removes_old_summary_contribution(self):
        self.token(self.start)
        self.delivery.prepare(self.start + DAY)
        self.token(self.start + DAY)
        self.delivery.prepare(self.start + DAY)
        old = self.delivery.manifest("local", "Date", self.start, self.start + DAY)
        new = self.delivery.manifest("local", "Date", self.start + DAY, self.start + 2 * DAY)
        self.assertEqual(old["tokenSessions"], [])
        self.assertEqual(new["tokenSessions"][0]["sessionCount"], 1)
        self.assertEqual(self.delivery.manifest("local", "All")["tokenSessions"][0]["sessionCount"], 1)

    def test_unrelated_day_changes_do_not_invalidate_cached_detail(self):
        self.quota(self.start)
        day = self.delivery.manifest("local", "Date", self.start, self.start + DAY)
        self.token(self.start + 10 * DAY)
        with mock.patch.object(self.delivery, "_load", side_effect=AssertionError("unrelated day reload")):
            self.assertEqual(self.delivery.manifest("local", "Date", self.start, self.start + DAY), day)

    def test_projection_committing_during_prepare_cannot_publish_partial_generation(self):
        self.quota(self.start)
        self.delivery.prepare(self.start)
        old = self.delivery.manifest("local", "All")
        self.quota(self.start + 60)
        load = self.delivery._load
        def concurrent(*args):
            result = load(*args)
            self.quota(self.start + 120)
            return result
        with mock.patch.object(self.delivery, "_load", side_effect=concurrent):
            self.delivery.prepare(self.start)
        self.assertEqual(self.delivery.manifest("local", "All"), old)
        self.delivery.prepare(self.start)
        new = self.delivery.manifest("local", "All")
        self.assertEqual(sum(len(part["quotaPoints"]) for part in self.materialize(new)), 3)

    def test_model_set_groups_preserve_arbitrary_filter_counts_and_cost_attribution(self):
        a = self.token(self.start, "one", {"m": {"usageTokens": {"outputTokens": 2}, "cost": {"totalCostUsd": 1}}})
        b = self.token(self.start + 1, "two", {"m": {"usageTokens": {"outputTokens": 3}}, "n": {"usageTokens": {"outputTokens": 4}}})
        c = self.token(self.start + 2, "three", {"n": {"usageTokens": {"outputTokens": 5}}})
        groups = session_groups([a, b, c])
        self.assertEqual(sum(row["sessionCount"] for row in groups if set(row["byModel"]) & {"m", "n"}), 3)
        self.assertEqual(sum(row["sessionCount"] for row in groups if "m" in row["byModel"]), 2)
        costs = [{"usageAccountId": "a", "deltaPercent": 10, "totalCostUsd": 10, "costByModelUsd": {"m": 10}},
            {"usageAccountId": "a", "deltaPercent": 20, "totalCostUsd": 100, "costByModelUsd": {"m": 50, "n": 50}}]
        grouped = cost_groups(costs)[0]
        self.assertEqual(grouped["usageByModel"], {"m": 20, "n": 10})
        self.assertEqual(cost_groups([grouped])[0], grouped)

    def test_quota_delivery_preserves_raw_values_and_rejection_boundaries(self):
        rows = [self.quota(self.start + i, value=value) | {"_key": str(i)} for i, value in enumerate([10, 11, 99, 12, 13, 14])]
        rows[3]["fiveHour"]["continuous"] = None
        for seconds in (0, 300, 3600, DAY):
            self.assertEqual(reduce_quota(rows, seconds), rows)
            self.assertIsNone(reduce_quota(rows, seconds)[3]["fiveHour"]["continuous"])

    def test_quota_rollups_keep_every_account_observation(self):
        rows = [self.quota(self.start + offset, value=value) | {"_key": str(i)}
            for i, (offset, value) in enumerate([(0, 10), (1, 20), (2, 30), (301, 40)])]
        other = rows[0] | {"usageAccountId": "b", "_key": "other"}
        self.assertEqual(reduce_quota(reduce_quota(rows + [other], 300), 3600), rows + [other])

    def test_prepared_overview_and_selected_day_retain_exact_observations(self):
        for minute, value in enumerate([10, 11, 12, 13]):
            self.quota(self.start + minute * 60, value=value)
        self.delivery.prepare(self.start + 4 * DAY)
        overview = [row for part in self.materialize(self.delivery.manifest("local", "All")) for row in part["quotaPoints"]]
        self.assertEqual([row["fiveHour"]["raw"] for row in overview], [10, 11, 12, 13])
        detail = [row for part in self.materialize(self.delivery.manifest("local", "Date", self.start, self.start + DAY)) for row in part["quotaPoints"]]
        self.assertEqual([row["fiveHour"]["raw"] for row in detail], [10, 11, 12, 13])

    def test_cost_buckets_sum_even_noncontiguous_intervals_and_preserve_rates(self):
        costs = [{"usageAccountId": "a", "plan": "plus", "timestamp": self.start + i, "startedAt": str(i * 2), "endedAt": str(i * 2 + 1),
            "deltaPercent": i + 1, "totalCostUsd": 2 * (i + 1), "costByModelUsd": {"m": 2 * (i + 1)}} for i in range(3)]
        reduced = reduce_cost(costs, 300)
        self.assertEqual(len(reduced), 1)
        self.assertEqual(reduced[0]["totalCostUsd"], 12)
        self.assertEqual(reduced[0]["deltaPercent"], 6)
        self.assertEqual(reduced[0]["costByModelUsd"], {"m": 12})
        self.assertEqual(reduced[0]["costPer100PercentUsd"], 200)
        costs[2]["plan"] = "pro"
        self.assertEqual(len(reduce_cost(costs, 300)), 2)

    def test_http_scope_conditional_requests_and_precompressed_chunks_do_not_refresh_history(self):
        import monitor_dashboard
        fake_state = mock.Mock(delivery=self.delivery)
        fake_state.cloud.config.return_value = {"control": {"passwordHash": "", "cookieSecret": "test"}}
        fake_state.status_payload.return_value = {"revision": "test", "lastSample": None}
        fake_server = mock.Mock()
        fake_server.serve_forever.side_effect = KeyboardInterrupt
        with mock.patch.object(monitor_dashboard, "DashboardInstanceLock"), mock.patch.object(monitor_dashboard, "DashboardHTTPServer", return_value=fake_server) as factory, \
                mock.patch.object(monitor_dashboard, "UsageDashboardState", return_value=fake_state), mock.patch.object(monitor_dashboard.threading, "Thread"), mock.patch("sys.stdout", new=io.StringIO()):
            monitor_dashboard.serve_dashboard(SimpleNamespace(data_home=self.directory, dashboard=False, timeout=1), None)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), factory.call_args.args[1])
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            with self.assertRaises(urllib.error.HTTPError) as retired:
                urllib.request.urlopen(base + "/api/series", timeout=3)
            self.assertEqual(retired.exception.code, 410)
            self.assertIn("/api/dashboard", json.load(retired.exception)["error"])
            retired.exception.close()
            with self.assertRaises(urllib.error.HTTPError) as preparing:
                urllib.request.urlopen(base + "/api/dashboard?view=local&range=All", timeout=3)
            self.assertEqual(preparing.exception.code, 503)
            preparing.exception.close()
            self.quota(self.start)
            self.delivery.prepare(self.start)
            with mock.patch.object(self.delivery, "prepare", side_effect=AssertionError("request prepared history")), mock.patch.object(self.delivery, "_load", side_effect=AssertionError("request scanned history")):
                with urllib.request.urlopen(base + "/api/dashboard?view=local&range=All", timeout=3) as response:
                    manifest, etag = json.load(response), response.headers["ETag"]
                with self.assertRaises(urllib.error.HTTPError) as unchanged:
                    urllib.request.urlopen(urllib.request.Request(base + "/api/dashboard?view=local&range=All", headers={"If-None-Match": etag}), timeout=3)
                self.assertEqual(unchanged.exception.code, 304)
                unchanged.exception.close()
                with urllib.request.urlopen(urllib.request.Request(base + "/api/dashboard/chunk?id=" + manifest["chunks"][0], headers={"Accept-Encoding": "gzip"}), timeout=3) as response:
                    self.assertEqual(response.headers["Content-Encoding"], "gzip")
                    self.assertEqual(len(json.loads(gzip.decompress(response.read()))["quotaPoints"]), 1)
            fake_state.refresh_dashboard_cache.assert_not_called()
        finally:
            server.shutdown()
            server.server_close()
            worker.join(3)
            self.assertFalse(worker.is_alive())

    def test_long_overview_reuses_rollups_and_keeps_recent_detail(self):
        sample = self.quota(self.start)
        with self.index.transaction() as db:
            for day in range(1, 400):
                row = sample | {"timestamp": self.start + day * DAY}
                db.execute("INSERT INTO processing_quota_points VALUES(?,?,?,?,?)", ("local", "a", row["timestamp"], str(day), json.dumps(row)))
            for minute in range(10):
                row = sample | {"timestamp": self.start + 399 * DAY + minute * 60}
                db.execute("INSERT OR REPLACE INTO processing_quota_points VALUES(?,?,?,?,?)", ("local", "a", row["timestamp"], f"recent-{minute}", json.dumps(row)))
        now = self.start + 399 * DAY + 3600
        self.delivery.prepare(now)
        manifest = self.delivery.manifest("local", "All")
        self.assertLess(len(manifest["chunks"]), 100)
        points = [row for part in self.materialize(manifest) for row in part["quotaPoints"]]
        self.assertEqual(len([row for row in points if row["timestamp"] >= self.start + 399 * DAY]), 11)
        before = self.delivery.metrics["chunksEncoded"]
        # A correction in one old partition rebuilds its levels and its one rollup, not every month.
        self.quota(self.start + 10 * DAY, key="10", value=99)
        self.delivery.prepare(now)
        self.assertEqual(self.delivery.metrics["chunksEncoded"] - before, 6)

    def test_missing_artifact_is_rebuilt_without_repairing_canonical_history(self):
        self.quota(self.start)
        self.delivery.prepare(self.start)
        manifest = self.delivery.manifest("local", "All")
        digest = manifest["chunks"][0]
        with self.index.transaction() as db:
            db.execute("DELETE FROM dashboard_blobs WHERE digest=?", (digest,))
        self.assertIsNone(self.delivery.blob(digest, False))
        self.assertIsNone(self.delivery.manifest("local", "All"))
        self.delivery.prepare(self.start)
        self.assertIsNotNone(self.delivery.blob(digest, False))
        self.assertEqual(self.delivery.manifest("local", "All")["generation"], manifest["generation"])

    def test_preset_requests_union_of_dataset_windows_not_every_day_between_anchors(self):
        # Quota and token capture can end on very different dates.
        self.token(self.start)
        self.quota(self.start + 100 * DAY)
        self.quota(self.start + 50 * DAY)
        self.delivery.prepare(self.start + 100 * DAY)
        manifest = self.delivery.manifest("local", "5h")
        points = [row for part in self.materialize(manifest) for row in part["quotaPoints"]]
        self.assertEqual([row["timestamp"] for row in points], [self.start + 100 * DAY])
        self.assertEqual(manifest["tokenSessions"][0]["sessionCount"], 1)
        self.assertLessEqual(len(manifest["chunks"]), 2)


if __name__ == "__main__":
    unittest.main()
