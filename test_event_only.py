import json
import shutil
import threading
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import monitor_migration_events as migration
import monitor_migrations
import monitor_sample_store as samples
from monitor_cloud import CloudError, CloudManager, CryptoBox
from monitor_common import empty_cost_totals, empty_token_totals
from monitor_token_ledger import _usage_records, canonical_token_ledger_rows, token_sessions_from_ledger
from monitor_usage_sync import UsageDataStore, content_hash, record_key, v4_logical_hash, v4_record_envelope, validate_sync_operation
from test_usage_v4 import MemoryWebDav


class EventOnlyTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-event-only-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.store = UsageDataStore(self.root / "quota.jsonl", self.root / "ledger.jsonl", "machine", lambda _: "v3:a", threading.RLock())
        self.paths = {"processing_index": self.store.index.path}

    @staticmethod
    def event(identity="E2", count=200):
        return {"eventId": identity, "sessionId": "s", "model": "gpt-5", "checkedAt": "2030-01-01T00:00:00Z",
            "tokens": {"input": count, "cachedInput": 0, "cacheWriteInput": 0, "output": 0}}

    def usage(self, identity="E2", count=100):
        return self.store.normalize_new_ledger(_usage_records([self.event(identity, count)], "slot", "A", [])[0])

    def baseline(self):
        return {"schemaVersion": 2, "recordType": "sessionBaseline", "sync": {"version": 1, "originMachineId": "machine", "accountId": "v3:a"},
            "session": {"sessionId": "s", "accountSlotId": "slot", "accountLabel": "A", "tokens": empty_token_totals() | {"inputTokens": 100, "freshInputTokens": 100, "totalTokens": 100},
                "cost": empty_cost_totals(), "byModel": {}}}

    def seed(self, rows):
        with self.store.index.transaction() as db:
            for position, row in enumerate(rows):
                key = record_key("tokenLedger", row)
                samples.put_fact(db, ("local:tokenLedger", position, "tokenLedger", key, "v3:a", (row.get("session") or row)["sessionId"], row.get("eventId", ""), 1893456000, json.dumps(row)))
                record = migration.historical_transport_record("tokenLedger", row)
                samples.put_local(db, key, "2030-01-01T00:00Z", content_hash(record), record)

    def test_runtime_rejects_baselines_and_coverage_instead_of_mixing_them(self):
        for row in (self.baseline(), self.usage() | {"sourceCoverage": {}}, self.usage() | {"sourceTotals": {}}):
            with self.subTest(row=row), self.assertRaises(ValueError):
                canonical_token_ledger_rows([row])
            with self.assertRaises(ValueError):
                self.store.record_ledger([row])
            with self.assertRaises(ValueError):
                token_sessions_from_ledger([row])
            with self.assertRaises(ValueError):
                validate_sync_operation({"action": "upsert", "key": record_key("tokenLedger", row), "record": {"kind": "tokenLedger", "row": row}})

    def test_current_events_are_never_reduced_by_legacy_coverage(self):
        initial = _usage_records([self.event()], "slot", "A", [])
        replay = _usage_records([self.event("E1", 20), self.event()], "slot", "A", [])
        self.assertEqual(initial[0]["tokens"]["totalTokens"], 200)
        self.assertEqual([row["tokens"]["totalTokens"] for row in replay], [20, 200])

    def test_migration_discards_unconvertible_data_and_preserves_valid_events_and_periods(self):
        valid = self.usage("valid", 12) | {"sessionId": "unrelated"}
        self.seed([self.baseline(), self.usage(), valid, self.usage("coverage") | {"sessionId": "missing", "sourceCoverage": {}}])
        self.store.token_ledger_path.write_bytes(b"not an ingestion source\n")
        migration.migrate_event_only(self.paths)
        self.assertEqual(self.store.datasets()[1], [valid])
        self.assertEqual(self.store.index.ledger_cost()[0], valid["cost"])
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM sample_records WHERE subtype='sessionBaseline'").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT period FROM local_records").fetchone()[0], "2030-01-01T00:00Z")
            self.assertFalse(db.execute("PRAGMA foreign_key_check").fetchall())
        self.assertEqual(self.store.token_ledger_path.read_bytes(), b"not an ingestion source\n")
        migration.migrate_event_only(self.paths)
        self.assertEqual(self.store.datasets()[1], [valid])

    def test_migration_rebuilds_complete_events_from_sql_and_keeps_empty_day_publication(self):
        self.seed([self.baseline(), self.usage()])
        with self.store.index.transaction() as db:
            for identity, count in (("E1", 20), ("E2", 200)):
                db.execute("INSERT INTO processing_scan_events VALUES(?,?,?,?,?,?,?)", (identity, str(self.root), "s", 1893456000, json.dumps(self.event(identity, count)), 1, 1))
        migration.migrate_event_only(self.paths)
        rows = self.store.datasets()[1]
        self.assertEqual({row["eventId"]: row["tokens"]["totalTokens"] for row in rows}, {"E1": 20, "E2": 200})
        with self.store.index.transaction() as db:
            self.assertEqual(json.loads(db.execute("SELECT data FROM processing_ledger_sessions WHERE source='local:tokenLedger'").fetchone()[0])["tokens"]["totalTokens"], 220)
            self.assertIsNotNone(db.execute("SELECT 1 FROM processing_dirty_days WHERE day='2030-01-01'").fetchone())
            self.assertEqual(db.execute("SELECT count(*) FROM processing_pending_records").fetchone()[0], 2)
        # A retry must not add the reconstructed usage a second time.
        migration.migrate_event_only(self.paths)
        self.assertEqual(self.store.datasets()[1], rows)

    def test_failure_rolls_back_discard_and_contract_marker(self):
        self.seed([self.baseline(), self.usage()])
        state, journal = self.root / "state.json", self.root / "journal.json"
        state.write_text('{"dataContractVersion":19}', encoding="utf-8")
        with self.store.index.transaction() as db:
            before = list(db.execute("SELECT source,position,data FROM processing_facts"))
        def fail(paths):
            migration.migrate_event_only(paths)
            raise RuntimeError("interrupted")
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            monitor_migrations._run_data_migration(fail, self.paths, 20, state, journal)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 19)
        with self.store.index.transaction() as db:
            self.assertEqual(list(db.execute("SELECT source,position,data FROM processing_facts")), before)
        monitor_migrations._run_data_migration(migration.migrate_event_only, self.paths, 20, state, journal)
        self.assertEqual(self.store.datasets()[1], [])

    def cloud(self, quota=None):
        manager = CloudManager(self.root / "cloud", SimpleNamespace(), None)
        self.addCleanup(manager.operation_queue.close)
        client = MemoryWebDav()
        client._migration_manager = manager
        box = CryptoBox("", CryptoBox.descriptor(""))
        client.put("crypto.json", json.dumps(box.descriptor("")).encode())
        client.put("data-contract.json", b'{"dataContractVersion":4}')
        days = {}
        rows = [("2030-01-01", self.baseline()), ("2030-01-02", self.usage()), ("2030-01-03", self.usage("valid") | {"sessionId": "other"})]
        if quota is not None:
            rows.append(("2030-01-04", quota))
        for day, row in rows:
            kind = "quota" if "windows" in row else "tokenLedger"
            key = record_key(kind, row)
            records = [v4_record_envelope("machine", key, migration.historical_transport_record(kind, row), day + "T00:00Z")]
            digest = manager._put_usage_payload(client, box, "v4/data", "machine", {"version": 1, "machineId": "machine", "day": day, "partId": "bulk", "records": records})[0]
            logical = v4_logical_hash(records)
            digest = manager._put_usage_payload(client, box, "v4/days", "machine", {"version": 1, "machineId": "machine", "day": day, "layout": "bulk", "logicalHash": logical, "parts": {"bulk": digest}})[0]
            days[day] = {"manifestId": digest, "logicalHash": logical, "layout": "bulk"}
        month = manager._put_usage_payload(client, box, "v4/months", "machine", {"version": 1, "machineId": "machine", "month": "2030-01", "days": days})[0]
        manager._v4_write_head(client, box, {"version": 4, "machineId": "machine", "generation": 1, "months": {"2030-01": month}, "activeDay": None}, None)
        return manager, client, box

    def test_cloud_removes_baseline_and_dependent_events_across_days(self):
        manager, client, box = self.cloud()
        old_catalog = manager._v4_catalog(client, box, "machine", manager._v4_head(client, box, "machine")[0])
        obsolete = manager._v4_day_manifest(client, box, "machine", "2030-01-01", old_catalog["2030-01-01"])["parts"]["bulk"]
        monitor_migrations.ensure_cloud_contract(client)
        head = manager._v4_head(client, box, "machine")[0]
        self.assertEqual(set(manager._v4_catalog(client, box, "machine", head)), {"2030-01-03"})
        self.assertEqual(json.loads(client.get("data-contract.json")[0])["dataContractVersion"], 5)
        self.assertNotIn(manager._usage_payload_path("v4/data", "machine", obsolete), client.files)
        self.assertNotIn("migrations/contract-5/machine.enc", client.files)
        monitor_migrations.ensure_cloud_contract(client)
        self.assertEqual(manager._v4_head(client, box, "machine")[0], head)

    def test_cloud_removes_quota_compaction_and_rehashes_envelopes(self):
        from monitor_usage_sync import add_record_provenance
        quota = add_record_provenance("quota", {"checkedAt": "2030-01-04T00:00:00Z",
            "windows": {"5h": {"usedPercent": 25, "resetAt": None, "plan": "plus"}},
            "compaction": {"continuousFrom": "2030-01-03T23:00:00Z", "omittedSamples": 12}}, "machine", "v3:a")
        manager, client, box = self.cloud(quota)
        monitor_migrations.ensure_cloud_contract(client)
        head = manager._v4_head(client, box, "machine")[0]
        catalog = manager._v4_catalog(client, box, "machine", head)
        manifest = manager._v4_day_manifest(client, box, "machine", "2030-01-04", catalog["2030-01-04"])
        rows = list(manager._v4_data(client, box, "machine", "2030-01-04", "bulk", manifest["parts"]["bulk"]))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["record"]["row"], {key: value for key, value in quota.items() if key != "compaction"})
        self.assertEqual(rows[0]["contentSha256"], content_hash(rows[0]["record"]))
        self.assertEqual(manifest["logicalHash"], v4_logical_hash(rows))
        monitor_migrations.ensure_cloud_contract(client)
        self.assertEqual(manager._v4_head(client, box, "machine")[0], head)

    def test_cloud_marker_failure_restores_head_for_retry(self):
        manager, client, box = self.cloud()
        original = client.get(manager._v4_head_path("machine"))[0]
        put = client.put
        def fail(path, data, **kwargs):
            if path == "data-contract.json":
                raise CloudError("marker unavailable")
            return put(path, data, **kwargs)
        with mock.patch.object(client, "put", side_effect=fail), self.assertRaisesRegex(CloudError, "marker unavailable"):
            monitor_migrations.ensure_cloud_contract(client)
        self.assertEqual(client.get(manager._v4_head_path("machine"))[0], original)
        self.assertEqual(json.loads(client.get("data-contract.json")[0])["dataContractVersion"], 4)
        monitor_migrations.ensure_cloud_contract(client)
        self.assertEqual(set(manager._v4_catalog(client, box, "machine", manager._v4_head(client, box, "machine")[0])), {"2030-01-03"})


if __name__ == "__main__":
    unittest.main()
