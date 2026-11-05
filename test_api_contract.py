import json
import shutil
import subprocess
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import monitor_common
import monitor_history
import monitor_migrations as migrations
import monitor_quota
import monitor_sample_store as samples
from monitor_processing import initialize_processing_index
from monitor_usage_sync import content_hash, initialize_v4_cache, v4_record_envelope, validate_v4_envelope


def usage_response(plan="plus"):
    # Shape verified against live wham usage and retained diagnostic responses.
    return {"plan_type": plan, "rate_limit": {
        "primary_window": {"used_percent": 19, "limit_window_seconds": 18000, "reset_at": 1793071819, "reset_after_seconds": 812},
        "secondary_window": {"used_percent": 19, "limit_window_seconds": 604800, "reset_at": 1793599772, "reset_after_seconds": 528765},
    }}


class ApiContractTests(unittest.TestCase):
    def test_timestamp_stays_numeric_and_other_limits_are_ignored(self):
        data = usage_response()
        data["additional_rate_limits"] = [{"rate_limit": usage_response()["rate_limit"]}]
        data["code_review_rate_limit"] = usage_response()["rate_limit"]
        windows = monitor_history.quota_history_windows({"usage": monitor_quota.compact_quota(data)})
        self.assertEqual(windows["5h"]["resetAt"], data["rate_limit"]["primary_window"]["reset_at"])
        self.assertIs(type(windows["5h"]["resetAt"]), int)
        del data["rate_limit"]
        with self.assertRaisesRegex(monitor_common.UsageError, "rate_limit"):
            monitor_quota.compact_quota(data)

    def test_weekly_primary_and_null_secondary_are_expected_for_pro_and_plus(self):
        for plan in ("pro", "prolite", "plus"):
            data = usage_response(plan)
            data["rate_limit"]["primary_window"] = data["rate_limit"]["secondary_window"]
            data["rate_limit"]["secondary_window"] = None
            windows = monitor_history.quota_history_windows({"usage": monitor_quota.compact_quota(data)})
            self.assertTrue(windows["5h"]["unavailable"])
            self.assertEqual(windows["7d"]["resetAt"], 1793599772)

    def test_incompatible_required_fields_are_reported_without_coercion(self):
        for key, bad in (("reset_at", "2030-01-01T00:00:00Z"), ("reset_at", True), ("reset_at", None),
                ("used_percent", "19"), ("used_percent", float("nan")), ("limit_window_seconds", "18000")):
            with self.subTest(key=key, value=bad):
                data = usage_response()
                data["rate_limit"]["primary_window"][key] = bad
                data["rate_limit"]["secondary_window"] = None
                with self.assertRaisesRegex(monitor_common.UsageError, "Incompatible usage API response.*" + key):
                    monitor_quota.compact_quota(data)
        for key in ("reset_at", "used_percent", "limit_window_seconds"):
            data = usage_response()
            del data["rate_limit"]["primary_window"][key]
            data["rate_limit"]["secondary_window"] = None
            with self.assertRaisesRegex(monitor_common.UsageError, key):
                monitor_quota.compact_quota(data)
        for data in ([], None, {}, {"plan_type": "pro", "rate_limit": {"primary_window": None, "secondary_window": None}}):
            with self.assertRaisesRegex(monitor_common.UsageError, "Incompatible usage API response"):
                monitor_quota.compact_quota(data)

    def test_fetch_keeps_failed_response_diagnostics(self):
        data = usage_response()
        del data["rate_limit"]["primary_window"]["reset_at"]
        data["rate_limit"]["secondary_window"] = None
        debug = {}
        with mock.patch.object(monitor_quota, "refresh_access_token", return_value="token"), mock.patch.object(monitor_quota, "request_json", return_value=(200, data)):
            with self.assertRaisesRegex(monitor_common.UsageError, "reset_at"):
                monitor_quota.fetch_usage({}, object(), None, 1, debug)
        self.assertEqual(debug["rawResponse"], data)
        self.assertEqual(debug["status"], 200)

    def test_invalid_json_has_endpoint_error(self):
        response = mock.MagicMock(status=200)
        response.__enter__.return_value = response
        response.read.return_value = b"not json"
        opener = SimpleNamespace(open=mock.Mock(return_value=response))
        with self.assertRaisesRegex(monitor_common.UsageError, "Incompatible API response.*expected UTF-8 JSON"):
            monitor_common.request_json(opener, "GET", "https://example.test/usage", {}, retries=0)

    def test_numeric_reset_time_is_rendered_in_milliseconds(self):
        html = Path("dashboard.html").read_text(encoding="utf-8")
        script = html[html.index("const remainingTime="):html.index("const remainingTime=") + html[html.index("const remainingTime="):].index("\n")]
        start = html.index("function updateWindowTime(")
        script += "\n" + html[start:html.index("\n}", start) + 2]
        script += """
Date.now=()=>1793071007000;
const elements={};
global.document={getElementById:id=>elements[id]??=( {style:{}} )};
updateWindowTime("time5h",{},1793071819,18000);
if(!elements.time5hReset.textContent.includes("13m remaining"))throw Error(elements.time5hReset.textContent);
updateWindowTime("time5h",{},null,18000);
if(elements.time5hReset.textContent.includes("1970"))throw Error("null rendered as epoch");
"""
        subprocess.run(["node", "-e", script], check=True, capture_output=True)


class TimestampMigrationTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-api-contract-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.paths = [self.root / name for name in ("history.jsonl", "quota.jsonl", "sessions.jsonl", "ledger.jsonl", "samples.jsonl")]
        migrations.write_json(self.root / migrations.DATA_MIGRATION_STATE_FILENAME, {"dataContractVersion": 15})
        self.row = {"checkedAt": "2030-01-01T00:00:00Z", "windows": {"5h": {"usedPercent": 19, "resetAt": "2030-01-01T05:00:00Z", "plan": "plus"}},
            "sync": {"version": 1, "originMachineId": "machine-a", "accountId": "v3:account", "recordId": "quota-a"}}
        self.paths[1].write_text(json.dumps(self.row) + "\n", encoding="utf-8")

    def migrate(self):
        return migrations.migrate_history_data(self.root, *self.paths, target_version=21)

    def test_local_conversion_preserves_peers_and_rehashes_records(self):
        path = self.root / "usage_monitor_sync_cache-v4.sqlite3"
        initialize_v4_cache(path)
        initialize_processing_index(path)
        record = {"kind": "quota", "row": self.row}
        envelope = v4_record_envelope("machine-a", "quota:quota-a", record, "2030-01-01T00:00Z")
        with closing(samples.connect(path)) as db, db:
            samples.put_local(db, "quota:quota-a", "2030-01-01T00:00Z", content_hash(record), record)
            samples.put_remote(db, "machine-a", "2030-01-01", "bulk", "quota:quota-a", content_hash(record), envelope)
            db.execute("INSERT INTO remote_days VALUES('machine-a','2030-01-01','old','{}')")
        self.assertEqual(self.migrate(), [16, 17, 18, 19, 20, 21])
        self.assertEqual(json.loads(self.paths[1].read_text())["windows"]["5h"]["resetAt"], 1893474000)
        with closing(samples.connect(path)) as db:
            data = json.loads(db.execute("SELECT record_json FROM remote_records").fetchone()[0])
            validate_v4_envelope(data, "machine-a")
            self.assertEqual(data["record"]["row"]["windows"]["5h"]["resetAt"], 1893474000)
            data, digest = db.execute("SELECT record_json,content_hash FROM local_records").fetchone()
            self.assertEqual(content_hash(json.loads(data)), digest)
        self.assertEqual(self.migrate(), [])

    def test_local_failure_preserves_originals_and_retry(self):
        original = self.paths[1].read_bytes()
        migration = migrations.DATA_MIGRATIONS[16]
        def fail(paths):
            migration(paths)
            raise RuntimeError("interrupted")
        with mock.patch.dict(migrations.DATA_MIGRATIONS, {16: fail}), self.assertRaisesRegex(RuntimeError, "interrupted"):
            self.migrate()
        self.assertEqual(self.paths[1].read_bytes(), original)
        self.assertEqual(self.migrate(), [16, 17, 18, 19, 20, 21])

    def test_pending_migration_rebuilds_quota_decisions_from_preserved_raw(self):
        import threading
        from monitor_sampling import stored_points
        from monitor_usage_sync import UsageDataStore
        rows = [self.row | {"checkedAt": f"2030-01-01T00:0{index}:00Z", "windows": {"5h": self.row["windows"]["5h"] | {"usedPercent": value}},
            "sync": self.row["sync"] | {"recordId": f"reading-{index}"}} for index, value in enumerate((40, 20, 20))]
        self.paths[1].write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        # Complete SQLite facts are the migration input; the recorder is only backup.
        store = UsageDataStore(self.paths[1], self.paths[3], "machine-a", lambda _: "v3:account", threading.RLock(), cache_path=self.root / "usage_monitor_sync_cache.json")
        for row in rows:
            store.record_quota(row)
        self.assertEqual(self.migrate(), [16, 17, 18, 19, 20, 21])
        original = self.paths[1].read_bytes()
        store = UsageDataStore(self.paths[1], self.paths[3], "machine-a", lambda _: "v3:account", threading.RLock(), cache_path=self.root / "usage_monitor_sync_cache.json")
        store.ingest_local()
        with store.index.transaction() as db:
            self.assertEqual([point["fiveHour"]["continuous"] for point in stored_points(db, "local")], [40, None, 20])
        self.assertEqual(self.paths[1].read_bytes(), original)
        self.assertEqual(self.migrate(), [])


if __name__ == "__main__":
    unittest.main()
