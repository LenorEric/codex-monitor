import json
import shutil
import threading
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from monitor_auto_update import AutoUpdateError, migrate_history_data
from monitor_cloud import CloudError, CloudManager, CryptoBox, passphrase_hash
from monitor_usage_sync import UsageDataStore, content_hash, v4_logical_hash


def moment(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


class MemoryWebDav:
    def __init__(self):
        self.files, self.revision, self.transfers, self.transfer_category = {}, 0, [], "metadata"

    def ensure_directories(self, path):
        pass

    def put(self, path, data, etag=None, create=False):
        self.transfers.append({"category": self.transfer_category, "attemptedUploadBytes": len(data), "successfulUploadBytes": len(data), "downloadBytes": 0})
        if create and path in self.files or etag and (path not in self.files or self.files[path][1] != etag):
            raise CloudError("HTTP 412", 409, http_status=412)
        self.revision += 1
        self.files[path] = data, f'"{self.revision}"'
        return self.files[path][1]

    def get(self, path):
        if path not in self.files:
            raise CloudError("HTTP 404", 502, http_status=404)
        data, etag = self.files[path]
        self.transfers.append({"category": self.transfer_category, "attemptedUploadBytes": 0, "successfulUploadBytes": 0, "downloadBytes": len(data)})
        return data, etag

    def list_details(self, path):
        prefix = path.rstrip("/") + "/"
        return [{"name": name[len(prefix):], "etag": etag} for name, (_, etag) in self.files.items() if name.startswith(prefix) and "/" not in name[len(prefix):]]

    def list(self, path):
        return [item["name"] for item in self.list_details(path)]

    def delete(self, path, etag=None):
        self.files.pop(path, None)


class UsageV4Tests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).parent / f".usage-v4-test-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root, True)
        self.client = MemoryWebDav()
        key = passphrase_hash("test passphrase", "https://example.test", "user")
        self.box = CryptoBox(key, CryptoBox.descriptor(key))

    def device(self, name):
        root = self.root / name
        cloud = CloudManager(root / "private", SimpleNamespace(), None)
        cloud._config["webdav"]["enabled"] = True
        cloud._state["conditionalWritesVerified"] = True
        cloud._connection = lambda initialize=False: (self.client, self.box)
        quota, ledger = root / "quota.jsonl", root / "ledger.jsonl"
        store = UsageDataStore(quota, ledger, cloud.machine_id, lambda _slot: "account-a", threading.RLock())
        cloud.configure_usage_sync(store)
        return cloud, quota

    @staticmethod
    def append(quota, machine_id, key, checked_at, used=1):
        quota.parent.mkdir(parents=True, exist_ok=True)
        row = {"checkedAt": checked_at, "windows": {"5h": {"usedPercent": used, "plan": "plus"}}, "sync": {"version": 1, "originMachineId": machine_id, "accountId": "account-a", "recordId": key}}
        with quota.open("a", encoding="utf-8") as output:
            output.write(json.dumps(row) + "\n")

    def test_periods_consolidation_and_reader_digest(self):
        author, quota = self.device("author")
        reader, _ = self.device("reader")
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z")
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T00:31:00Z")):
            first = author.sync_usage_data()
            fetched = reader.sync_usage_data()
            idle = author.sync_usage_data()
        self.assertEqual(first["published"]["partsUploaded"], 1)
        self.assertEqual(fetched["fetched"]["payloadsDownloaded"], 1)
        self.assertEqual(idle["published"]["partsUploaded"], 0)
        self.append(quota, author.machine_id, "two", "2030-01-01T00:45:00Z")
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T00:46:00Z")):
            self.assertEqual(author.sync_usage_data()["published"]["partsUploaded"], 0)
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T01:01:00Z")):
            second = author.sync_usage_data()
            reader.sync_usage_data()
        self.assertEqual(second["published"]["partsUploaded"], 1)
        old_digest = reader._usage_data.v4_cached_days(author.machine_id)["2030-01-01"]["logicalHash"]
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-02T00:01:00Z")):
            consolidated = author.sync_usage_data()
            read_back = reader.sync_usage_data()
        self.assertEqual(consolidated["published"]["partsUploaded"], 1)
        self.assertEqual(read_back["fetched"]["payloadsDownloaded"], 0)
        self.assertEqual(reader._usage_data.v4_cached_days(author.machine_id)["2030-01-01"]["logicalHash"], old_digest)
        self.assertEqual(len([path for path in self.client.files if path.startswith(f"usage/v4/data/{author.machine_id}/")]), 1)
        self.assertFalse(any(path.startswith("usage/machines/") for path in self.client.files))

    def test_failed_head_commit_preserves_previous_publication(self):
        author, quota = self.device("author")
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z")
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T00:31:00Z")):
            author.sync_usage_data()
        head_path = author._v4_head_path(author.machine_id)
        original = self.client.files[head_path]
        self.append(quota, author.machine_id, "two", "2030-01-01T00:35:00Z")
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T00:36:00Z")):
            self.assertEqual(author.sync_usage_data()["published"]["partsUploaded"], 0)
        put = self.client.put

        def fail_head(path, data, etag=None, create=False):
            if path == head_path:
                raise CloudError("quota exhausted", 502, http_status=507)
            return put(path, data, etag, create)

        with mock.patch.object(self.client, "put", side_effect=fail_head), mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T01:01:00Z")):
            with self.assertRaises(CloudError):
                author.sync_usage_data()
        self.assertEqual(self.client.files[head_path], original)
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T01:01:00Z")):
            self.assertEqual(author.sync_usage_data()["published"]["partsUploaded"], 1)

    def test_correction_replaces_owning_part_then_daily_file(self):
        author, quota = self.device("author")
        reader, _ = self.device("reader")
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z")
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T00:31:00Z")):
            author.sync_usage_data()
            reader.sync_usage_data()
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z", 2)
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T00:41:00Z")):
            changed = author.sync_usage_data()
            reread = reader.sync_usage_data()
        self.assertEqual(changed["published"]["partsUploaded"], 1)
        self.assertEqual(reread["fetched"]["payloadsDownloaded"], 1)
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-02T00:01:00Z")):
            author.sync_usage_data()
            reader.sync_usage_data()
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z", 3)
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-02T00:11:00Z")):
            changed_bulk = author.sync_usage_data()
            reread_bulk = reader.sync_usage_data()
        self.assertEqual(changed_bulk["published"]["partsUploaded"], 1)
        self.assertEqual(reread_bulk["fetched"]["payloadsDownloaded"], 1)
        self.assertTrue(all(path.startswith("usage/v4/") for path in self.client.files))

    def test_cold_reader_fetches_daily_file_and_rejects_corruption(self):
        author, quota = self.device("author")
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z")
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-02T00:01:00Z")):
            author.sync_usage_data()
        reader, _ = self.device("reader")
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-02T00:02:00Z")):
            self.assertEqual(reader.sync_usage_data()["fetched"]["payloadsDownloaded"], 1)
        data_path = next(path for path in self.client.files if path.startswith(f"usage/v4/data/{author.machine_id}/"))
        self.client.files[data_path] = b"corrupt", self.client.files[data_path][1]
        reader._state["usageV4"]["remote"].clear()
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-02T00:03:00Z")):
            with self.assertRaises((CloudError, ValueError)):
                reader._fetch_usage_data(self.client, self.box, True)
        self.assertIn("2030-01-01", reader._usage_data.v4_cached_days(author.machine_id))

    def test_old_checkpoint_does_not_suppress_first_v4_publication(self):
        author, quota = self.device("author")
        author._state["usage"]["published"] = {"old": "checkpoint"}
        self.client.files["usage/machines/legacy.enc"] = (b"legacy cloud content", '"old"')
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z")
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-02T00:01:00Z")):
            result = author.sync_usage_data()
        self.assertEqual(result["published"]["partsUploaded"], 1)
        self.assertIn(author._v4_head_path(author.machine_id), self.client.files)
        self.assertEqual(self.client.files["usage/machines/legacy.enc"][0], b"legacy cloud content")
        monthly = author._state["usageV4"]["transferByMonth"]
        self.assertGreater(monthly[next(iter(monthly))]["backfill"]["successfulUploadBytes"], 0)

    def test_etag_conflict_restarts_from_latest_head(self):
        author, quota = self.device("author")
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z")
        with mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T00:31:00Z")):
            author.sync_usage_data()
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z", 2)
        put, head_path, rejected = self.client.put, author._v4_head_path(author.machine_id), []

        def conflict(path, data, etag=None, create=False):
            if path == head_path and not rejected:
                rejected.append(True)
                raise CloudError("HTTP 412", 409, http_status=412)
            return put(path, data, etag, create)

        with mock.patch.object(self.client, "put", side_effect=conflict), mock.patch("monitor_cloud.time.time", return_value=moment("2030-01-01T00:41:00Z")):
            self.assertEqual(author.sync_usage_data()["published"]["partsUploaded"], 1)
        self.assertEqual(len(rejected), 1)

    def test_migration_failure_preserves_files_and_retries(self):
        data = self.root / "migration"
        data.mkdir()
        history = data / "history.jsonl"
        cloud_state = data / "cloud-state.json"
        contract_state = data / "usage_monitor_data_contract.json"
        history.write_text('{"original":true}\n', encoding="utf-8")
        cloud_state.write_text("[]", encoding="utf-8")
        contract_state.write_text('{"dataContractVersion":5}\n', encoding="utf-8")
        kwargs = {"data_home": data, "history_path": history, "quota_history_path": data / "quota.jsonl", "token_session_history_path": data / "sessions.jsonl", "token_ledger_path": data / "ledger.jsonl", "sample_log_path": data / "samples.jsonl", "target_version": 6, "cloud_state_path": cloud_state}
        with self.assertRaises((AutoUpdateError, ValueError)):
            migrate_history_data(**kwargs)
        self.assertEqual(history.read_text(encoding="utf-8"), '{"original":true}\n')
        self.assertEqual(json.loads(contract_state.read_text(encoding="utf-8"))["dataContractVersion"], 5)
        cloud_state.write_text("{}", encoding="utf-8")
        self.assertEqual(migrate_history_data(**kwargs), [6])
        self.assertEqual(json.loads(contract_state.read_text(encoding="utf-8"))["dataContractVersion"], 6)

    def test_future_record_type_is_cached_without_interpretation(self):
        reader, _ = self.device("reader")
        record = {"kind": "futureMetric", "schemaVersion": 3, "eventAt": "2030-01-01T00:00:00Z", "value": {"future": 5}}
        envelope = {"kind": "futureMetric", "schemaVersion": 3, "recordKey": "futureMetric:one", "sourceMachineId": "future-client", "collectionPeriod": "2030-01-01T00:00Z", "eventAt": record["eventAt"], "contentSha256": content_hash(record), "record": record, "futureEnvelopeField": 7}
        manifest = {"day": "2030-01-01", "logicalHash": v4_logical_hash([envelope]), "parts": {"bulk": "a" * 64}}
        reader._usage_data.v4_apply_day("future-client", manifest, {"bulk": [envelope]})
        self.assertEqual(reader._usage_data.v4_cached_days("future-client")["2030-01-01"]["logicalHash"], manifest["logicalHash"])
        self.assertEqual(reader._usage_data.datasets("merged"), ([], []))

    def test_appended_quota_uses_tail_and_rewrite_reconciles(self):
        author, quota = self.device("author")
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z")
        self.assertEqual(len(author._usage_data.v4_local_snapshot(moment("2030-01-01T00:05:00Z"))["2030-01-01"]["parts"]), 1)
        self.append(quota, author.machine_id, "two", "2030-01-01T00:06:00Z")
        with mock.patch.object(author._usage_data, "_load", side_effect=AssertionError("full source scan")):
            author._usage_data.v4_local_snapshot(moment("2030-01-01T00:07:00Z"))
        self.assertEqual(len(author._usage_data.v4_local_snapshot(moment("2030-01-01T00:08:00Z"))["2030-01-01"]["parts"]), 1)
        quota.write_text("", encoding="utf-8")
        self.assertEqual(author._usage_data.v4_local_snapshot(moment("2030-01-01T00:09:00Z")), {})


if __name__ == "__main__":
    unittest.main()
