import hashlib
import json
import threading
import unittest
import uuid
import shutil
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from monitor_cloud import CloudError, CloudManager, USAGE_PACK_BUCKET_BITS, passphrase_hash
from monitor_usage_sync import content_hash


class CloudContractV3Tests(unittest.TestCase):
    @contextmanager
    def directory(self):
        path = Path(__file__).parent / f".cloud-v3-test-{uuid.uuid4().hex}"
        path.mkdir()
        try:
            yield path
        finally:
            shutil.rmtree(path, ignore_errors=True)

    @staticmethod
    def manager(root: Path, accounts=None) -> CloudManager:
        return CloudManager(root / "private", SimpleNamespace(), accounts)

    def test_usage_account_id_is_stable_across_passphrases_and_remembers_legacy_alias(self):
        with self.directory() as directory:
            accounts = SimpleNamespace(lock=threading.RLock(), manifest={"accounts": [{"id": "slot-a", "label": "A", "identity": {"accountId": "acct-a", "idTokenHash": "token-a"}}]})
            cloud = self.manager(directory, accounts)
            cloud._config["webdav"]["encryptionPassphraseHash"] = passphrase_hash("old secret", "https://example.test/dav", "user")
            stable = cloud.usage_account_id("slot-a")
            legacy = cloud._legacy_usage_account_id("slot-a")
            cloud._remember_legacy_usage_aliases()
            cloud._config["webdav"]["encryptionPassphraseHash"] = passphrase_hash("new secret", "https://example.test/dav", "user")
            cloud._usage_account_ids.clear()

            self.assertTrue(stable.startswith("v3:"))
            self.assertEqual(cloud.usage_account_id("slot-a"), stable)
            self.assertEqual(cloud.local_usage_account(legacy, None, None), ("slot-a", "A"))

    def test_hash_prefix_packs_isolate_record_changes_and_validate_each_record(self):
        keys = []
        candidate = 0
        while len(keys) < 96:
            key = f"quota:key-{candidate}"
            if hashlib.sha256(key.encode()).digest()[0] >> (8 - USAGE_PACK_BUCKET_BITS) == 0:
                keys.append(key)
            candidate += 1
        records = {key: {"kind": "quota", "row": {"blob": "".join(hashlib.sha256(f"{key}:{index}".encode()).hexdigest() for index in range(48))}} for key in keys}
        before = CloudManager._usage_record_packs("machine-a", records)
        changed_key = before[next(iter(before))]["value"]["records"][0]["key"]
        records[changed_key] = {"kind": "quota", "row": {"blob": "".join(hashlib.sha256(f"changed:{index}".encode()).hexdigest() for index in range(48))}}
        after = CloudManager._usage_record_packs("machine-a", records)

        unchanged = set(before) & set(after) - {pack_id for pack_id in before if any(entry["key"] == changed_key for entry in before[pack_id]["value"]["records"])}
        self.assertTrue(unchanged)
        self.assertTrue(all(before[pack_id]["hash"] == after[pack_id]["hash"] for pack_id in unchanged))
        pack = next(iter(after.values()))["value"]
        pack["records"][0]["record"] = {"kind": "quota", "row": {"blob": "tampered"}}
        with self.assertRaises(CloudError):
            CloudManager._validate_usage_pack_records(pack, True)

    def test_new_pointer_omits_obsolete_transport_metadata_but_old_pointer_parses(self):
        legacy = {"version": 2, "machineId": "machine-a", "packs": {"a0-0000": "a" * 64}, "recordCount": 1, "packBytes": {"a0-0000": 1}, "updatedAt": "then", "verification": {"mode": "sampled", "verifiedPacks": ["a0-0000"], "fullVerifiedAt": None}}
        box = SimpleNamespace(decrypt=lambda _purpose, payload, _limit: payload)
        self.assertEqual(CloudManager._parse_usage_pointer(box, "machine-a", json.dumps(legacy).encode()), legacy)
        with self.directory() as directory:
            cloud = self.manager(directory)
            captured = {}

            def write(_client, _box, pointer, _etag):
                captured.update(pointer)
                return '"etag"'

            with mock.patch.object(cloud, "_usage_pointer", return_value=(None, None)), mock.patch.object(cloud, "_write_usage_pointer", side_effect=write), mock.patch.object(cloud, "_verify_usage_pointer", return_value='"etag"'):
                cloud._publish_usage(object(), object(), {}, set())

            self.assertFalse({"packBytes", "updatedAt"} & captured.keys())
            self.assertEqual(set(captured["verification"]), {"fullVerifiedAt"})

    def test_force_verification_keeps_compacted_snapshot(self):
        with self.directory() as directory:
            cloud = self.manager(directory)
            store = SimpleNamespace(snapshot=mock.Mock(return_value=({}, set())))
            cloud._usage_data = store
            with mock.patch.object(cloud, "_require_conditional_writes"), mock.patch.object(cloud, "_connection", return_value=(SimpleNamespace(ensure_directories=lambda _path: None), object())), mock.patch.object(cloud, "_publish_usage", return_value={}) as publish:
                cloud._push_usage_data(True)

            store.snapshot.assert_called_once_with()
            self.assertTrue(publish.call_args.args[-1])

    def test_complete_empty_machine_listing_clears_remote_cache(self):
        with self.directory() as directory:
            cloud = self.manager(directory)
            store = SimpleNamespace(remove_origins_not_in=mock.Mock(return_value=1))
            cloud._usage_data = store
            client = SimpleNamespace(list_details=lambda _path: [])

            self.assertEqual(cloud._fetch_usage(client, object()), {"machinesChanged": 1, "machinesMigrated": 0, "payloadsDownloaded": 0, "conflicts": 0})
            store.remove_origins_not_in.assert_called_once_with(set())

    def test_stale_missing_pack_retries_the_latest_pointer_snapshot(self):
        with self.directory() as directory:
            cloud = self.manager(directory)
            row = {"kind": "quota", "row": {"sync": {"originMachineId": "machine-b"}}}
            initial = {"version": 2, "machineId": "machine-b", "packs": {"a0-006": "a" * 64}, "packFormat": {"version": 3}}
            latest = {"version": 2, "machineId": "machine-b", "packs": {"a0-006": "b" * 64}, "packFormat": {"version": 3}}
            store = SimpleNamespace(pack_hashes=lambda _machine: {}, apply_pack_snapshot=mock.Mock(return_value=[]), remove_origins_not_in=mock.Mock(return_value=0))
            cloud._usage_data = store
            client = SimpleNamespace(list_details=lambda _path: [{"name": "machine-b.enc", "etag": '"old"'}], get=lambda _path: (b"pointer", '"old"'))
            payload = {"version": 1, "machineId": "machine-b", "packId": "a0-006", "records": [{"key": "quota:key", "record": row, "contentSha256": content_hash(row)}]}

            with mock.patch.object(cloud, "_parse_usage_pointer", return_value=initial), mock.patch.object(cloud, "_usage_pointer", return_value=(latest, '"new"')), mock.patch.object(cloud, "_download_usage_payload", side_effect=(CloudError("HTTP 404", 502, http_status=404), payload)):
                cloud._fetch_usage(client, object())

            store.apply_pack_snapshot.assert_called_once_with("machine-b", latest["packs"], {"a0-006": payload["records"]}, True)

    def test_fully_verified_empty_pointer_clears_loose_cached_records(self):
        with self.directory() as directory:
            cloud = self.manager(directory)
            pointer = {"version": 2, "machineId": "machine-b", "packs": {}, "packFormat": cloud._usage_pack_format(), "verification": {"fullVerifiedAt": None}}
            store = SimpleNamespace(pack_hashes=lambda _machine: {}, apply_pack_snapshot=mock.Mock(return_value=[]), remove_origins_not_in=mock.Mock(return_value=0))
            cloud._usage_data = store
            client = SimpleNamespace(list_details=lambda _path: [{"name": "machine-b.enc", "etag": '"etag"'}], get=lambda _path: (b"pointer", '"etag"'))

            with mock.patch.object(cloud, "_parse_usage_pointer", return_value=pointer):
                result = cloud._fetch_usage(client, object())

            store.apply_pack_snapshot.assert_called_once_with("machine-b", {}, {}, True)
            self.assertEqual(result["machinesChanged"], 1)


if __name__ == "__main__":
    unittest.main()
