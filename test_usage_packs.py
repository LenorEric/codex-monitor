import copy
import json
import unittest
from unittest import mock

from monitor_cloud import CloudError, passphrase_hash
from monitor_migration_events import migrate_cloud_event_only
from monitor_migrations import ensure_cloud_contract
from monitor_usage_packs import UsageObjectReader
import test_usage_v4 as usage_fixture
from test_usage_v4 import moment


class UsagePackTests(unittest.TestCase):
    setUp = usage_fixture.UsageV4Tests.setUp
    append = usage_fixture.UsageV4Tests.append

    def device(self, name):
        device, quota = usage_fixture.UsageV4Tests.device(self, name)
        self.addCleanup(device.operation_queue.close)
        device._config["webdav"]["encryptionPassphraseHash"] = passphrase_hash("test passphrase", "https://example.test", "user")
        return device, quota

    def archive(self, dates=None):
        author, quota = self.device("author")
        for i, date in enumerate(dates or [f"2030-01-{i:02d}" for i in range(1, 11)]):
            self.append(quota, author.machine_id, str(i), date + "T00:01:00Z", i + 1)
        with mock.patch("time.time", return_value=moment("2030-03-02T00:01:00Z")):
            author.sync_usage_data()
        self.client._migration_manager = author
        self.client.put("crypto.json", json.dumps(self.box.descriptor(author._config["webdav"]["encryptionPassphraseHash"])).encode())
        self.client.put("data-contract.json", b'{"dataContractVersion":3}')
        return author, quota

    def pack(self, author):
        with mock.patch("time.time", return_value=moment("2030-03-02T00:01:00Z")):
            ensure_cloud_contract(self.client, target_version=4)
        return author._v4_catalog(self.client, self.box, author.machine_id, author._v4_head(self.client, self.box, author.machine_id)[0])

    def data_gets(self, get, machine):
        return [call.args[0] for call in get.call_args_list if call.args[0].startswith(f"usage/v4/data/{machine}/")]

    def test_repacking_ten_days_preserves_warm_cache_and_source_publication(self):
        author, _ = self.archive()
        reader, _ = self.device("reader")
        reader._v4_fetch(self.client, self.box)
        before = reader._usage_data.v4_cached_days(author.machine_id)
        datasets = reader._usage_data.datasets("merged")
        catalog = self.pack(author)
        self.assertTrue(all(ref["layout"] == "packed" for ref in catalog.values()))
        self.assertEqual(len({segment["payloadId"] for ref in catalog.values() for segment in ref["segments"]}), 1)
        with mock.patch.object(self.client, "get", wraps=self.client.get) as get:
            self.assertEqual(reader._v4_fetch(self.client, self.box)["payloadsDownloaded"], 1)
        self.assertEqual(len(self.data_gets(get, author.machine_id)), 1)
        self.assertEqual(set(reader._usage_data.v4_cached_days(author.machine_id)), set(before))
        self.assertEqual(reader._usage_data.datasets("merged"), datasets)
        head = author._v4_head(self.client, self.box, author.machine_id)[0]
        with mock.patch("time.time", return_value=moment("2030-03-02T00:01:00Z")):
            self.assertEqual(author._v4_publish(self.client, self.box, True)["partsUploaded"], 0)
        self.assertEqual(author._v4_head(self.client, self.box, author.machine_id)[0], head)
        self.assertFalse(any(path.startswith(f"usage/v4/days/{author.machine_id}/") for path in self.client.files))

    def test_cross_month_pack_downloaded_once_and_migration_retry_idempotent(self):
        author, _ = self.archive(["2030-01-31", "2030-02-01"])
        catalog = self.pack(author)
        reader, _ = self.device("reader")
        with mock.patch.object(self.client, "get", wraps=self.client.get) as get:
            self.assertEqual(reader._v4_fetch(self.client, self.box)["payloadsDownloaded"], 1)
        self.assertEqual(len(self.data_gets(get, author.machine_id)), 1)
        self.assertEqual({day: ref["reference"] for day, ref in reader._usage_data.v4_cached_days(author.machine_id).items()}, catalog)
        head = author._v4_head(self.client, self.box, author.machine_id)[0]
        with mock.patch("time.time", return_value=moment("2030-03-02T00:01:00Z")), mock.patch.object(self.client, "get", wraps=self.client.get) as get:
            migrate_cloud_event_only(self.client)
        self.assertEqual(len(self.data_gets(get, author.machine_id)), 1)
        self.assertEqual(author._v4_head(self.client, self.box, author.machine_id)[0], head)

    def test_migration_gc_does_not_download_just_uploaded_objects(self):
        self.archive()
        written = set()
        get, put = self.client.get, self.client.put
        def tracked_put(name, data, **kwargs):
            result = put(name, data, **kwargs)
            written.add(name)
            return result
        def checked_get(name):
            if name in written and name.startswith(("usage/v4/data/", "usage/v4/days/", "usage/v4/months/")):
                self.fail("Migration downloaded an object it just uploaded")
            return get(name)
        with mock.patch.object(self.client, "get", side_effect=checked_get), mock.patch.object(self.client, "put", side_effect=tracked_put), \
                mock.patch("time.time", return_value=moment("2030-03-02T00:01:00Z")):
            ensure_cloud_contract(self.client, target_version=4)
        self.assertTrue(any(name.startswith("usage/v4/data/") for name in written))

    def test_threshold_split_keeps_the_crossing_record_and_tail(self):
        author, _ = self.archive(["2030-01-01"] * 10)
        from monitor_usage_sync import canonical_json
        with mock.patch("monitor_usage_packs.PACK_RAW_BYTES", 1800):
            catalog = self.pack(author)
        segments = catalog["2030-01-01"]["segments"]
        self.assertGreater(len(segments), 1)
        self.assertEqual(sum(segment["recordCount"] for segment in segments), 10)
        for segment in segments[:-1]:
            value = author._download_usage_payload(self.client, self.box, "v4/data", author.machine_id, segment["payloadId"])
            self.assertGreater(len(canonical_json(value)), 1800)
            self.assertLessEqual(len(canonical_json(value | {"records": value["records"][:-1]})), 1800)
        reader, _ = self.device("reader")
        self.assertEqual(reader._v4_fetch(self.client, self.box)["payloadsDownloaded"], len(segments))

    def test_correction_and_gc_keep_shared_pack_until_last_reference(self):
        author, quota = self.archive()
        catalog = self.pack(author)
        digest = catalog["2030-01-01"]["segments"][0]["payloadId"]
        shared = author._usage_payload_path("v4/data", author.machine_id, digest)
        reader, _ = self.device("reader")
        reader._v4_fetch(self.client, self.box)
        unchanged = reader._usage_data.v4_cached_days(author.machine_id)["2030-01-02"]["logicalHash"]
        self.append(quota, author.machine_id, "0", "2030-01-01T00:01:00Z", 99)
        with mock.patch("time.time", return_value=moment("2030-03-02T00:01:00Z")):
            self.assertEqual(author._v4_publish(self.client, self.box)["partsUploaded"], 1)
        self.assertEqual(reader._v4_fetch(self.client, self.box)["payloadsDownloaded"], 1)
        self.assertEqual(reader._usage_data.v4_cached_days(author.machine_id)["2030-01-02"]["logicalHash"], unchanged)
        head, etag = author._v4_head(self.client, self.box, author.machine_id)
        author._v4_gc(self.client, self.box, author.machine_id, head)
        self.assertIn(shared, self.client.files)
        author._v4_write_head(self.client, self.box, head | {"generation": head["generation"] + 1, "months": {}}, etag)
        author._v4_gc(self.client, self.box, author.machine_id, head | {"months": {}})
        self.assertNotIn(shared, self.client.files)

    def test_typed_ranges_and_overlapping_index_references_are_accepted(self):
        author, _ = self.archive()
        catalog = self.pack(author)
        day = "2030-01-01"
        reference = catalog[day]
        for change in (lambda ref: ref["segments"][0].update(recordOffset=1000000),
                lambda ref: ref["segments"][0].update(recordOffset=1)):
            bad = copy.deepcopy(reference)
            change(bad)
            with UsageObjectReader(author, self.client, self.box, author.machine_id) as reader:
                list(reader.entries(day, bad))
        head, etag = author._v4_head(self.client, self.box, author.machine_id)
        refs = copy.deepcopy(catalog)
        refs[day]["segments"].append(copy.deepcopy(refs[day]["segments"][0]))
        month = author._put_usage_payload(self.client, self.box, "v4/months", author.machine_id,
            {"version": 2, "machineId": author.machine_id, "month": "2030-01", "days": refs})[0]
        author._v4_write_head(self.client, self.box, head | {"generation": head["generation"] + 1, "months": {"2030-01": month}}, etag)
        self.assertEqual(author._v4_catalog(self.client, self.box, author.machine_id, author._v4_head(self.client, self.box, author.machine_id)[0]), refs)

    def test_invalid_owner_duplicates_and_corrupt_ciphertext_preserve_cached_days(self):
        author, _ = self.archive()
        catalog = self.pack(author)
        day, reference = next(iter(catalog.items()))
        digest = reference["segments"][0]["payloadId"]
        value = author._download_usage_payload(self.client, self.box, "v4/data", author.machine_id, digest)
        for bad_value in (value | {"machineId": "wrong-owner"}, value | {"records": [value["records"][0], value["records"][0]]}):
            bad_digest = author._put_usage_payload(self.client, self.box, "v4/data", author.machine_id, bad_value)[0]
            with UsageObjectReader(author, self.client, self.box, author.machine_id) as object_reader:
                self.assertEqual(object_reader.payload(bad_digest), bad_value)
        reader, _ = self.device("reader")
        reader._v4_fetch(self.client, self.box)
        cached = reader._usage_data.v4_cached_days(author.machine_id)
        path = author._usage_payload_path("v4/data", author.machine_id, digest)
        self.client.files[path] = b"corrupt", self.client.files[path][1]
        with self.assertRaises(CloudError):
            reader._v4_fetch(self.client, self.box, True)
        self.assertEqual(reader._usage_data.v4_cached_days(author.machine_id), cached)


if __name__ == "__main__":
    unittest.main()
