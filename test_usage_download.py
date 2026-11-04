import copy
import io
import unittest
import zlib
from unittest import mock

from monitor_cloud import CloudManager
from monitor_streaming import decode_payload
from monitor_usage_sync import canonical_json, require_usage_fields, validate_v4_envelope, v4_record_envelope
import test_usage_v4 as usage_fixture
from test_usage_v4 import moment


class UsageDownloadTests(unittest.TestCase):
    setUp = usage_fixture.UsageV4Tests.setUp
    device = usage_fixture.UsageV4Tests.device
    append = usage_fixture.UsageV4Tests.append

    def envelope(self):
        return v4_record_envelope("author", "arbitrary-key", {"kind": "quota", "row": {
            "checkedAt": "not-a-date", "windows": {"future-window": {"usedPercent": 150, "plan": ""}},
            "sync": {"version": -1, "originMachineId": "different-owner", "accountId": "", "recordId": "different-key"}}}, "not-a-period")

    def test_download_accepts_typed_values_without_semantic_or_hash_checks(self):
        entry = self.envelope() | {"sourceMachineId": "different-owner", "schemaVersion": -1,
            "eventAt": "different-time", "contentSha256": {"ignored": True}}
        validate_v4_envelope(entry, "author")
        entry.pop("contentSha256")
        validate_v4_envelope(entry, "author")
        reader, _ = self.device("reader")
        manifest = {"day": "2030-01-01", "parts": {"bulk": "first"}, "logicalHash": "incorrect"}
        reader._usage_data.v4_apply_day("author", manifest, {"bulk": [entry, entry]})
        with reader._usage_data.index.transaction() as db:
            db.execute("UPDATE remote_days SET logical_hash='legacy'")
        entry["record"]["row"]["windows"]["future-window"]["usedPercent"] = -5
        reader._usage_data.v4_apply_day("author", manifest | {"parts": {"bulk": "second"}}, {"bulk": [entry]})
        self.assertEqual(reader._usage_data.v4_cached_days("author")["2030-01-01"]["logicalHash"], "legacy")
        self.assertEqual(reader._usage_data._v4_remote_cache()[0][0]["record"], entry["record"])

    def test_missing_fields_and_wrong_types_are_rejected(self):
        for change in (lambda entry: entry.pop("recordKey"), lambda entry: entry.update(schemaVersion=True),
                lambda entry: entry["record"]["row"].pop("windows"),
                lambda entry: entry["record"]["row"]["windows"]["future-window"].update(usedPercent="5")):
            entry = copy.deepcopy(self.envelope())
            change(entry)
            with self.assertRaises(ValueError):
                validate_v4_envelope(entry, "author")
        with self.assertRaises(ValueError):
            require_usage_fields({"version": True}, version=int)

    def test_payload_object_identity_is_not_validated_in_either_decoder(self):
        value = {"version": -1, "machineId": "other", "records": [self.envelope()]}
        compressed = zlib.compress(canonical_json(value))
        encrypted = self.box.encrypt("purpose", compressed)
        self.assertEqual(CloudManager._decode_usage_payload(self.box, "purpose", encrypted, "wrong-id"), value)
        decoded = decode_payload(self.box, "purpose", io.BytesIO(encrypted), "wrong-id")
        try:
            self.assertEqual(list(decoded["records"]), value["records"])
        finally:
            decoded["records"].close()

    def test_head_checks_types_without_value_or_identity_validation(self):
        author, _ = self.device("author")
        head = {"version": -4, "machineId": "other", "generation": -1, "months": {"not-a-month": "not-a-hash"},
            "activeDay": {"day": "not-a-date", "manifestId": "id", "layout": "future", "logicalHash": 5}}
        self.client.put(author._v4_head_path(author.machine_id), self.box.encrypt(f"usage-v4-head:{author.machine_id}", canonical_json(head)))
        self.assertEqual(author._v4_head(self.client, self.box, author.machine_id)[0], head)

    def test_publication_and_incremental_fetch_ignore_stale_day_hash(self):
        author, quota = self.device("author")
        reader, _ = self.device("reader")
        self.append(quota, author.machine_id, "one", "2030-01-01T00:01:00Z", 1)
        with mock.patch("time.time", return_value=moment("2030-01-01T00:31:00Z")):
            author._v4_publish(self.client, self.box)
        head, etag = author._v4_head(self.client, self.box, author.machine_id)
        self.assertNotIn("logicalHash", head["activeDay"])
        reference = head["activeDay"] | {"logicalHash": "stale"}
        author._v4_write_head(self.client, self.box, head | {"activeDay": reference}, etag)
        reader._v4_fetch(self.client, self.box)
        self.append(quota, author.machine_id, "two", "2030-01-01T00:35:00Z", 2)
        with mock.patch("time.time", return_value=moment("2030-01-01T00:35:00Z")):
            author._usage_data.initialize_v4_index()
        with mock.patch("time.time", return_value=moment("2030-01-01T01:01:00Z")):
            author._v4_publish(self.client, self.box)
        head = author._v4_head(self.client, self.box, author.machine_id)[0]
        self.assertEqual(head["activeDay"]["logicalHash"], "stale")
        self.assertEqual(reader._v4_fetch(self.client, self.box)["payloadsDownloaded"], 1)
        self.assertEqual(len(reader._usage_data._v4_remote_cache()[0]), 2)
        self.assertEqual(reader._v4_fetch(self.client, self.box)["payloadsDownloaded"], 0)


if __name__ == "__main__":
    unittest.main()
