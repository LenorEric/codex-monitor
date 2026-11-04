import json
from pathlib import Path
import shutil
from types import SimpleNamespace
import unittest
import uuid
from unittest import mock

from monitor_cloud import CloudError, CloudManager, CryptoBox
from monitor_cloud_folder import FolderCloudClient
from monitor_migrations import ensure_cloud_contract
from monitor_usage_packs import UsageObjectReader
from monitor_usage_sync import canonical_json, record_key, v4_logical_hash, v4_record_envelope


class CloudFolderMigrationTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).parent / (".cloud-folder-test-" + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        (self.root / "working").mkdir()
        self.client = FolderCloudClient(self.root / "working")
        self.manager = CloudManager(self.root / "private", SimpleNamespace(), None)
        self.addCleanup(self.manager.operation_queue.close)
        self.client._migration_manager = self.manager
        self.box = CryptoBox("", CryptoBox.descriptor(""))
        self.client.put("crypto.json", canonical_json(self.box.descriptor("")))
        self.client.put("data-contract.json", b'{"dataContractVersion":2}')

    def record(self, day, identity="one"):
        return {"kind": "quota", "row": {"checkedAt": day + "T00:01:00Z", "windows": {"5h": {"usedPercent": 10, "plan": "plus"}},
            "sync": {"version": 1, "originMachineId": "machine", "accountId": "v3:a", "recordId": identity}}}

    def seed(self):
        record = self.record("2025-12-31")
        digest = self.manager._put_usage_payload(self.client, self.box, "packs", "machine",
            {"version": 1, "machineId": "machine", "records": [{"key": "old", "record": record}]})[0]
        self.client.put("usage/machines/machine.enc", self.box.encrypt("usage-pointer:machine",
            canonical_json({"version": 2, "machineId": "machine", "packs": {"one": digest}})))
        self.manager._put_usage_payload(self.client, self.box, "packs", "machine",
            {"version": 1, "machineId": "machine", "records": [{"key": "orphan", "record": self.record("2020-01-01", "orphan")}]})
        record = self.record("2026-01-01", "retained")
        entry = v4_record_envelope("machine", record_key(record["kind"], record["row"]), record, "2026-01-01T00:00Z")
        digest = self.manager._put_usage_payload(self.client, self.box, "v4/data", "machine",
            {"version": 1, "machineId": "machine", "day": "2026-01-01", "partId": "bulk", "records": [entry]})[0]
        logical = v4_logical_hash([entry])
        manifest = self.manager._put_usage_payload(self.client, self.box, "v4/days", "machine",
            {"version": 1, "machineId": "machine", "day": "2026-01-01", "layout": "bulk", "logicalHash": logical, "parts": {"bulk": digest}})[0]
        month = self.manager._put_usage_payload(self.client, self.box, "v4/months", "machine",
            {"version": 1, "machineId": "machine", "month": "2026-01", "days": {"2026-01-01": {"manifestId": manifest, "logicalHash": logical, "layout": "bulk"}}})[0]
        self.manager._v4_write_head(self.client, self.box,
            {"version": 4, "machineId": "machine", "generation": 1, "months": {"2026-01": month}, "activeDay": None}, None)

    def assert_final(self):
        self.assertEqual(json.loads(self.client.get("data-contract.json")[0])["dataContractVersion"], 4)
        head = self.manager._v4_head(self.client, self.box, "machine")[0]
        catalog = self.manager._v4_catalog(self.client, self.box, "machine", head)
        self.assertEqual(set(catalog), {"2025-12-31", "2026-01-01"})
        with UsageObjectReader(self.manager, self.client, self.box, "machine") as reader:
            for day, reference in catalog.items():
                rows = list(reader.entries(day, reference))
                self.assertEqual(len(rows), 1)
                self.assertEqual(v4_logical_hash(rows), reference["logicalHash"])
            self.assertEqual(reader.downloaded, 1)
        self.assertFalse((self.client.root / "usage/packs").exists())
        self.assertEqual(len(list((self.client.root / "usage/v4/data/machine").glob("*.enc"))), 1)

    def test_ordered_upgrade_merges_old_and_partial_v4_without_resurrecting_orphans(self):
        self.seed()
        ensure_cloud_contract(self.client, target_version=4)
        self.assert_final()
        before = self.manager._v4_head(self.client, self.box, "machine")[0]
        ensure_cloud_contract(self.client, target_version=4)
        self.assertEqual(self.manager._v4_head(self.client, self.box, "machine")[0], before)

    def test_pack_upload_failure_preserves_sources_and_retries_from_contract_three(self):
        self.seed()
        put = self.client.put_file
        def fail(name, source, **kwargs):
            if name.startswith("usage/v4/data/") and json.loads(self.client.get("data-contract.json")[0])["dataContractVersion"] == 3:
                raise CloudError("simulated pack failure")
            return put(name, source, **kwargs)
        with mock.patch.object(self.client, "put_file", side_effect=fail), self.assertRaisesRegex(CloudError, "simulated pack"):
            ensure_cloud_contract(self.client, target_version=4)
        self.assertEqual(json.loads(self.client.get("data-contract.json")[0])["dataContractVersion"], 3)
        self.assertEqual(len(self.manager._v4_catalog(self.client, self.box, "machine", self.manager._v4_head(self.client, self.box, "machine")[0])), 2)
        ensure_cloud_contract(self.client, target_version=4)
        self.assert_final()

    def test_transport_rejects_escaping_paths_root_deletion_and_stale_writes(self):
        for name in ("../escape", "C:/escape", "/escape", "usage/../../escape"):
            with self.assertRaises(CloudError):
                self.client.put(name, b"x")
        with self.assertRaises(CloudError):
            self.client.delete("")
        self.client.put("test/file.enc", b"original")
        with self.assertRaises(CloudError):
            self.client.put("test/file.enc", b"replacement", etag='"stale"')
        self.assertEqual(self.client.get("test/file.enc")[0], b"original")

    def test_streamed_pack_may_contain_more_than_one_hundred_thousand_records(self):
        from monitor_streaming import RecordSpool
        from monitor_usage_sync import content_hash
        record = {"kind": "futureMetric", "schemaVersion": 3, "value": 1}
        template = {"kind": "futureMetric", "schemaVersion": 3, "sourceMachineId": "machine", "collectionPeriod": "2026-01-01T00:00Z",
            "contentSha256": content_hash(record), "record": record, "futureEnvelopeField": 7}
        rows = RecordSpool()
        self.addCleanup(rows.close)
        for index in range(100001):
            rows.append(template | {"recordKey": f"future:{index:06d}"})
        digest = self.manager._put_usage_payload(self.client, self.box, "v4/data", "machine", {"version": 2, "machineId": "machine", "records": rows})[0]
        with UsageObjectReader(self.manager, self.client, self.box, "machine") as reader:
            decoded = reader.payload(digest)["records"]
            self.assertEqual(len(decoded), 100001)
            self.assertEqual(next(decoded.range_rows(100000, 1))["futureEnvelopeField"], 7)


if __name__ == "__main__":
    unittest.main()
