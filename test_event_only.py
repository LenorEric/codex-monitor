import json
import shutil
import threading
import unittest
import uuid
from pathlib import Path

import monitor_migration_events as migration
import monitor_migrations
import monitor_sample_store as samples
from monitor_common import empty_cost_totals, empty_token_totals
from monitor_token_ledger import _usage_records, canonical_token_ledger_rows, token_sessions_from_ledger
from monitor_usage_sync import UsageDataStore, content_hash, record_key, validate_sync_operation


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


if __name__ == "__main__":
    unittest.main()
