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

    def test_reduction_preserves_extrema_and_reset_boundaries(self):
        rows = [self.quota(self.start + i, value=value) | {"_key": str(i)} for i, value in enumerate([10, 11, 99, 12, 13, 14])]
        reduced = reduce_quota(rows, 300)
        self.assertIn(99, [row["fiveHour"]["raw"] for row in reduced])
        self.assertEqual(reduced[0]["timestamp"], self.start)
        self.assertEqual(reduced[-1]["timestamp"], self.start + 5)
        rows[3]["fiveHour"]["resetAt"] = "2030-02-02T00:00:00Z"
        self.assertTrue({"2", "3"}.issubset({row["_key"] for row in reduce_quota(rows, 300)}))
        costs = [{"usageAccountId": "a", "plan": "plus", "timestamp": self.start + i, "startedAt": str(i), "endedAt": str(i + 1),
            "deltaPercent": i + 1, "totalCostUsd": 2 * (i + 1), "costByModelUsd": {"m": 2 * (i + 1)}} for i in range(3)]
        self.assertEqual(reduce_cost(costs, 300)[0]["costPer100PercentUsd"], 200)
        costs[2]["startedAt"] = "reset"
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
