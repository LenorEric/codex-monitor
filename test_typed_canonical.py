import copy
import json
import shutil
import sqlite3
import threading
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from unittest import mock

from monitor_canonical import CanonicalDataStore, CanonicalIndex
from monitor_canonical_store import CanonicalRepository as records, TABLES
from monitor_common import parse_timestamp
from monitor_delivery import DashboardDelivery
from monitor_migration_canonical import migrate_local
from monitor_migration_packages import initialize_v22
from monitor_usage_sync import canonical_json, record_account_key, record_key
import test_canonical_pipeline as pipeline
import test_sqlite_authority as authority


class TypedCanonicalTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / (".typed-test-" + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.store = CanonicalDataStore(self.root / "seed" / "quota.jsonl", self.root / "seed" / "token.jsonl", "a", lambda _: "v3:account", threading.RLock())
        self.addCleanup(self.store.index.close)
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota(seven=10))
        self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=0.123456789012345)])
        self.path = self.root / "legacy.sqlite3"

    def legacy(self):
        DashboardDelivery(self.store.index).prepare()
        with self.store.index.transaction() as source, closing(sqlite3.connect(self.path)) as target:
            # backup() cannot run from a connection with an active transaction.
            with closing(sqlite3.connect(self.store.index.path)) as reader:
                reader.backup(target)
        with closing(sqlite3.connect(self.path)) as db:
            for table, _ in TABLES.values():
                db.execute(f"DROP TABLE {table}")
            initialize_v22(db)
            for dataset in ("local", "merged"):
                for kind, rows in zip(("quota", "tokenLedger"), self.store.datasets(dataset)):
                    for row in rows:
                        stamp = int(parse_timestamp(row.get("checkedAt") or row.get("occurredAt")))
                        last = max((w["lastAt"] for w in row.get("windows", {}).values()), default=stamp)
                        db.execute("INSERT INTO canonical_records VALUES(?,?,?,?,?,?,?)", (dataset, kind, record_account_key(row), record_key(kind, row), stamp, last, canonical_json(row).decode()))
            db.commit()
        return self.path

    def round_trip(self, kind, row):
        with self.store.index.transaction() as db:
            records.write(db, "local", kind, row)
            return records.rows(db, "local", kind, condition="record_key=?", parameters=(record_key(kind, row),))[0]

    def test_same_typed_structure_and_no_core_json(self):
        with self.store.index.transaction() as db:
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_records'").fetchone())
            for kind, (table, _) in TABLES.items():
                self.assertEqual(records.rows(db, "local", kind), records.rows(db, "merged", kind))
                self.assertEqual(db.execute(f"SELECT COUNT(*) FROM {table} WHERE extras_json IS NOT NULL").fetchone()[0], 0)
                self.assertNotIn("data", [row[1] for row in db.execute(f"PRAGMA table_info({table})")])

    def test_unknown_metadata_and_optional_nulls_round_trip(self):
        for kind, row in zip(("quota", "tokenLedger"), (self.store.datasets()[0][0], self.store.datasets()[1][0])):
            row = copy.deepcopy(row)
            row["extension"] = {"list": [1, None, {"unicode": "配额"}]}
            row["sync"]["extension"] = {"empty": {}}
            row["sync"]["localOnly"] = None
            row["sourceFile"] = None
            if kind == "quota":
                row["windows"][next(iter(row["windows"]))]["extension"] = {"flag": True}
            self.assertEqual(canonical_json(self.round_trip(kind, row)), canonical_json(row))
            del row["sync"]["localOnly"]
            del row["sourceFile"]
            self.assertEqual(canonical_json(self.round_trip(kind, row)), canonical_json(row))

    def test_fractional_timestamps_original_spelling_and_exact_costs(self):
        row = self.store.datasets()[1][0] | {"occurredAt": "2030-01-01T10:00:00.123456+00:00"}
        row["cost"]["inputCostUsd"] = 0.123456789012345
        row["cost"]["outputCostUsd"] = 0
        restored = self.round_trip("tokenLedger", row)
        self.assertEqual(canonical_json(restored), canonical_json(row))
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT occurred_at FROM canonical_tokens WHERE dataset='local'").fetchone()[0], parse_timestamp(row["occurredAt"]))

    def test_quota_numeric_types_and_saved_rejection(self):
        row = copy.deepcopy(self.store.datasets()[0][0])
        window = row["windows"][next(iter(row["windows"]))]
        window.update(usedPercent=50.0, continuous=None, multiplier=5, resetAt="2030-01-02T00:00:00Z", resetBoundary=True)
        self.assertEqual(canonical_json(self.round_trip("quota", row)), canonical_json(row))
        window["resetAt"] = None
        self.assertEqual(canonical_json(self.round_trip("quota", row)), canonical_json(row))

    def test_database_constraints(self):
        changes = {"dataset": "wrong", "window": "1h", "first": 9999999999, "range_first": 9999999999,
            "used_percent": 101, "multiplier": -1, "reset_boundary": 2, "continuous": float("inf"), "run_version": 2}
        with self.store.index.transaction() as db:
            for column, value in changes.items():
                with self.subTest(column=column), self.assertRaises(sqlite3.IntegrityError):
                    db.execute(f"UPDATE canonical_quota SET {column}=? WHERE dataset='local'", (value,))
            for column, value in {"service_tier": "invalid", "token_inputTokens": -1, "token_outputTokens": 1.5,
                    "cost_totalCostUsd": -1, "cost_inputCostUsd": float("inf"), "occurred_at": float("inf"), "event_id": None}.items():
                with self.subTest(column=column), self.assertRaises(sqlite3.IntegrityError):
                    db.execute(f"UPDATE canonical_tokens SET {column}=? WHERE dataset='local'", (value,))

    def test_writer_rejects_invalid_types_and_nonfinite_values(self):
        for path, value in (("continuous", float("nan")), ("multiplier", True), ("firstAt", 1.5), ("resetBoundary", 1)):
            row = copy.deepcopy(self.store.datasets()[0][0])
            row["windows"][next(iter(row["windows"]))][path] = value
            with self.subTest(path=path), self.assertRaises((ValueError, sqlite3.IntegrityError)):
                self.round_trip("quota", row)

    def test_migration_preserves_every_other_table_and_delivery_bytes(self):
        path = self.legacy()
        with closing(sqlite3.connect(path)) as db:
            tables = [name for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name!='canonical_records'")]
            before = {name: db.execute(f'SELECT * FROM "{name}"').fetchall() for name in tables}
        migrate_local({"processing_index": path})
        migrate_local({"processing_index": path})
        with closing(sqlite3.connect(path)) as db:
            for table in tables:
                self.assertEqual(db.execute(f'SELECT * FROM "{table}"').fetchall(), before[table], table)
            for dataset in ("local", "merged"):
                self.assertEqual(tuple(records.rows(db, dataset, kind) for kind in TABLES), self.store.datasets(dataset))
        index = CanonicalIndex(path)
        self.addCleanup(index.close)
        delivery = DashboardDelivery(index)
        self.assertEqual(delivery.bulk(False), DashboardDelivery(self.store.index).bulk(False))
        delivery.prepare()
        self.assertEqual(json.loads(delivery.bulk(False))["accounts"], json.loads(DashboardDelivery(self.store.index).bulk(False))["accounts"])

    def test_failed_conversion_rolls_back_schema_and_preserves_source(self):
        path = self.legacy()
        with closing(sqlite3.connect(path)) as db:
            data = json.loads(db.execute("SELECT data FROM canonical_records WHERE kind='tokenLedger' LIMIT 1").fetchone()[0])
            data["tokens"]["inputTokens"] = -1
            db.execute("UPDATE canonical_records SET data=? WHERE kind='tokenLedger'", (json.dumps(data),))
            db.commit()
            before = db.execute("SELECT * FROM canonical_records").fetchall()
        with self.assertRaisesRegex(ValueError, "Cannot convert canonical"):
            migrate_local({"processing_index": path})
        with closing(sqlite3.connect(path)) as db:
            self.assertEqual(db.execute("SELECT * FROM canonical_records").fetchall(), before)
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_quota'").fetchone())
            data["tokens"]["inputTokens"] = 1
            db.execute("UPDATE canonical_records SET data=? WHERE kind='tokenLedger'", (json.dumps(data),))
            db.commit()
        migrate_local({"processing_index": path})

    def test_interruption_rolls_back_even_after_partial_population(self):
        path = self.legacy()
        original = records.write
        calls = []
        def interrupt(*args, **kwargs):
            original(*args, **kwargs)
            calls.append(True)
            if len(calls) == 2:
                raise KeyboardInterrupt()
        with mock.patch.object(records, "write", side_effect=interrupt), self.assertRaises(KeyboardInterrupt):
            migrate_local({"processing_index": path})
        with closing(sqlite3.connect(path)) as db:
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_quota'").fetchone())
        migrate_local({"processing_index": path})

    def test_marker_crash_recovers_original_before_retry(self):
        import monitor_migrations as migrations
        path = self.legacy()
        state, journal = self.root / "contract.json", self.root / "journal.json"
        state.write_text('{"dataContractVersion":22}', encoding="utf-8")
        write = migrations._atomic_write
        def crash(target, data):
            if target == state:
                raise SystemExit("crash before marker")
            write(target, data)
        with mock.patch.object(migrations, "_atomic_write", side_effect=crash), self.assertRaises(SystemExit):
            migrations._run_data_migration(migrate_local, {"processing_index": path}, 23, state, journal)
        self.assertTrue(journal.exists())
        migrations._recover_data_migration(journal, state, {"processing_index": path, "config": self.root / "config.json"})
        with closing(sqlite3.connect(path)) as db:
            self.assertTrue(db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_records'").fetchone())
        migrations._run_data_migration(migrate_local, {"processing_index": path}, 23, state, journal)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 23)
        self.assertFalse(journal.exists())
        self.assertFalse(list(self.root.glob("*.migration-backup")))

    def test_runtime_refuses_unconverted_contract22(self):
        with self.assertRaisesRegex(ValueError, "contract 23"):
            CanonicalIndex(self.legacy())

    def test_empty_and_missing_stores_and_reusable_pages(self):
        migrate_local({"processing_index": self.path})
        with closing(sqlite3.connect(self.path)) as db:
            initialize_v22(db)
        migrate_local({"processing_index": self.path})
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM canonical_quota").fetchone()[0], 0)
        with mock.patch("sqlite3.connect", side_effect=AssertionError("compaction must not run")):
            self.assertEqual(self.store.index.maintain_disk_space(), 0)

    def test_account_update_and_rename_are_typed_for_both_datasets(self):
        self.store.account_mapper = lambda *_: ("bound", "Bound")
        self.store.refresh_accounts()
        self.store.rename_account("bound", "Renamed")
        for dataset in ("local", "merged"):
            for rows in self.store.datasets(dataset):
                for row in rows:
                    self.assertEqual((row["accountSlotId"], row["accountLabel"]), ("bound", "Renamed"))

    def test_sequential_upgrade_and_older_client_rejection(self):
        import monitor_migrations as migrations
        from monitor_usage_sync import UsageDataStore
        root = self.root / "chain"
        old = UsageDataStore(root / "quota.jsonl", root / "token.jsonl", "a", lambda _: "v3:account", threading.RLock())
        old.backup.enabled = lambda: False
        old.record_quota(pipeline.CanonicalPipelineTests.quota())
        old.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=3)])
        old.initialize_v4_index()
        (root / migrations.DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":21}', encoding="utf-8")
        args = (root, root / "history", old.quota_path, root / "sessions", old.token_ledger_path, root / "diagnostics")
        with mock.patch("monitor_common.request_json", side_effect=AssertionError("offline migration used network")):
            self.assertEqual(migrations.migrate_history_data(*args, usage_sync_cache_path=old.cache_path), [22, 23])
            self.assertEqual(migrations.migrate_history_data(*args, usage_sync_cache_path=old.cache_path), [])
        new = CanonicalDataStore(old.quota_path, old.token_ledger_path, "a", lambda _: "v3:account", threading.RLock())
        self.addCleanup(new.index.close)
        self.assertEqual(new.index.ledger_cost()[0]["totalCostUsd"], 3)
        self.assertEqual(new.datasets("local"), new.datasets("merged"))
        with self.assertRaisesRegex(migrations.MigrationError, "Unsupported data contract version"):
            migrations.migrate_history_data(*args, usage_sync_cache_path=old.cache_path, target_version=22)

    def test_live_updates_and_restart_keep_both_streams(self):
        delivery = DashboardDelivery(self.store.index)
        delivery.prepare()
        original = json.loads(delivery.bulk(False))
        row = authority.SQLiteAuthorityTests.ledger(cost=2) | {"eventId": "second", "occurredAt": "2030-01-01T10:01:00Z"}
        self.store.record_ledger([row])
        delivery.prepare(force=True)
        with self.store.index.transaction() as db:
            for dataset in ("local", "merged"):
                self.assertTrue(db.execute("SELECT 1 FROM delivery_journal WHERE stream=? AND body LIKE ?", (dataset, '%second%')).fetchone())
        self.assertNotEqual(json.loads(delivery.bulk(False))["accounts"], original["accounts"])
        before = delivery.bulk(False)
        index = CanonicalIndex(self.store.index.path)
        self.addCleanup(index.close)
        self.assertEqual(DashboardDelivery(index).bulk(False), before)

    def test_range_endpoints_overlaps_and_explicit_conflicts(self):
        template = next(row for row in self.store.datasets()[0] if "5h" in row["windows"])
        with self.store.index.transaction() as db:
            db.execute("DELETE FROM canonical_quota WHERE dataset='merged' AND window='5h'")
            def insert(first, last, value):
                row = copy.deepcopy(template)
                row["windows"]["5h"].update(usedPercent=value, continuous=value)
                self.store._quota_merge(db, "merged", self.store._fragment(row, "5h", first, last))
            insert(100, 200, 50)
            insert(100, 100, 51)
            insert(200, 200, 52)
            insert(150, 150, 53)
            insert(150, 150, 99)
            insert(125, 175, 54)
            windows = [row["windows"]["5h"] for row in records.rows(db, "merged", "quota", condition="window='5h'")]
            self.assertEqual([(w["firstAt"], w["lastAt"], w["usedPercent"]) for w in windows],
                [(100, 100, 51), (101, 124, 50), (125, 149, 54), (150, 150, 53), (151, 175, 54), (176, 199, 50), (200, 200, 52)])
            self.assertEqual(len(records.rows(db, "merged", "quota", condition="window='7d'")), 1)

    def test_rejection_and_reset_barriers_survive_typed_round_trip(self):
        template = next(row for row in self.store.datasets()[0] if "5h" in row["windows"])
        with self.store.index.transaction() as db:
            db.execute("DELETE FROM canonical_quota WHERE dataset='merged' AND window='5h'")
            for stamp, continuous, reset in ((100, 50, False), (110, None, False), (120, 50, False), (130, 50, True)):
                row = copy.deepcopy(template)
                row["windows"]["5h"].update(continuous=continuous, resetBoundary=reset)
                self.store._quota_merge(db, "merged", self.store._fragment(row, "5h", stamp, stamp))
            windows = [row["windows"]["5h"] for row in records.rows(db, "merged", "quota", condition="window='5h'")]
            self.assertEqual([(w["firstAt"], w["lastAt"], w["continuous"], w["resetBoundary"]) for w in windows],
                [(100, 100, 50, False), (110, 110, None, False), (120, 120, 50, False), (130, 130, 50, True)])

    def test_late_cloud_observation_after_migration_preserves_local_and_costs(self):
        from monitor_usage_sync import add_record_provenance
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota("2030-01-01T11:00:00Z", seven=10))
        path = self.legacy()
        migrate_local({"processing_index": path})
        converted = copy.copy(self.store)
        converted.index = CanonicalIndex(path)
        self.addCleanup(converted.index.close)
        local = converted.datasets("local")
        cost = converted.index.ledger_cost()
        row = add_record_provenance("quota", pipeline.CanonicalPipelineTests.quota("2030-01-01T10:30:00Z", 51), "peer", "v3:account")
        converted.import_package("destination", "peer", "late", [{"kind": "quota", "row": row}])
        self.assertEqual(converted.datasets("local"), local)
        self.assertEqual(converted.index.ledger_cost(), cost)
        windows = [row["windows"]["5h"] for row in converted.datasets("merged")[0] if "5h" in row["windows"]]
        self.assertEqual([w["usedPercent"] for w in windows], [50, 51, 50])


if __name__ == "__main__":
    unittest.main()
