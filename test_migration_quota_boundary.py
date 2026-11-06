import json
import shutil
import sqlite3
import threading
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from unittest import mock

from monitor_canonical import CanonicalDataStore
from monitor_canonical_store import CanonicalRepository as records
from monitor_delivery import DashboardDelivery
from monitor_migration_quota_boundary import migrate_local
from monitor_migration_canonical_schema import CanonicalRepository as released_records, QUOTA as RELEASED_QUOTA
import monitor_migrations as migrations
import test_canonical_pipeline as pipeline
import test_sqlite_authority as authority


class QuotaBoundaryMigrationTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / (".boundary-migration-" + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.store = CanonicalDataStore(self.root / "quota.jsonl", self.root / "ledger.jsonl", "a", lambda _: "v3:account", threading.RLock())
        self.addCleanup(self.store.index.close)
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota(seven=10))
        self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=2)])
        DashboardDelivery(self.store.index).prepare()
        self.paths = {"processing_index": self.store.index.path}

    def insert_quota(self, db, key, first, last=None, dataset="local", account="history", window="5h", **state):
        records.write(db, dataset, "quota", {"checkedAt": first, "quotaRunVersion": 1,
            "sync": {"version": 1, "originMachineId": "a", "accountId": account, "recordId": key},
            "windows": {window: {"usedPercent": 50, "multiplier": 1, "plan": "plus", "resetAt": 1893500000,
                "firstAt": first, "lastAt": first if last is None else last} | state}}, key=key, account=account)

    def test_compacts_chains_in_both_datasets_preserving_provenance_and_other_tables(self):
        with closing(sqlite3.connect(self.store.index.path)) as db:
            for dataset in ("local", "merged"):
                for window in ("5h", "7d"):
                    self.insert_quota(db, window + "a", 100, 110, dataset, window=window)
                    self.insert_quota(db, window + "b", 120, dataset=dataset, window=window)
                    self.insert_quota(db, window + "c", 200, 220, dataset, window=window)
            db.commit()
        self.legacy_continuous()
        with closing(sqlite3.connect(self.store.index.path)) as db:
            others = {name: db.execute(f'SELECT * FROM "{name}"').fetchall()
                for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name!='canonical_quota'").fetchall()}
        migrate_local(self.paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute("SELECT dataset,window,record_key,first,last,range_first,range_last,sync_record "
                "FROM canonical_quota WHERE account='history' ORDER BY dataset,window").fetchall(),
                [(dataset, window, window + "a", 100, 220, 100, 220, window + "a") for dataset in ("local", "merged") for window in ("5h", "7d")])
            for name, rows in others.items():
                self.assertEqual(db.execute(f'SELECT * FROM "{name}"').fetchall(), rows, name)
            compacted = db.execute("SELECT * FROM canonical_quota ORDER BY dataset,account,record_key").fetchall()
        migrate_local(self.paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute("SELECT * FROM canonical_quota ORDER BY dataset,account,record_key").fetchall(), compacted)

    def test_compaction_respects_state_changes_accounts_and_windows(self):
        with closing(sqlite3.connect(self.store.index.path)) as db:
            for field, value in (("usedPercent", 51), ("multiplier", 5), ("plan", "pro"), ("resetAt", None), ("resetAt", "2030-01-01T00:00:00Z")):
                account = str(value) + field
                self.insert_quota(db, "a", 100, account=account)
                self.insert_quota(db, "b", 110, account=account, **{field: value})
                self.insert_quota(db, "c", 120, account=account, usedPercent=52 if field == "usedPercent" else 50)
            self.insert_quota(db, "isolated-a", 100, account="isolated")
            self.insert_quota(db, "isolated-b", 110, account="other")
            self.insert_quota(db, "isolated-c", 120, account="isolated", window="7d")
            self.insert_quota(db, "isolated-d", 130, dataset="merged", account="isolated")
            db.commit()
            before = db.execute("SELECT * FROM canonical_quota ORDER BY dataset,account,record_key").fetchall()
        migrate_local(self.paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute("SELECT * FROM canonical_quota ORDER BY dataset,account,record_key").fetchall(), before)

    def test_chronological_compaction_discards_later_conflicts_and_keeps_earliest_identity(self):
        with closing(sqlite3.connect(self.store.index.path)) as db:
            # Deliberately insert backwards: chronological order determines ownership.
            self.insert_quota(db, "outside", 210, account="overlap", usedPercent=51)
            self.insert_quota(db, "conflicting-range", 150, 250, account="overlap", usedPercent=52)
            self.insert_quota(db, "inside", 110, account="overlap", usedPercent=51)
            self.insert_quota(db, "long", 100, 200, account="overlap")
            self.insert_quota(db, "tie-b", 100, account="ties", usedPercent=51)
            self.insert_quota(db, "tie-a", 100, account="ties")
            self.insert_quota(db, "after-tie", 110, account="ties")
            self.insert_quota(db, "compatible-later", 150, 250, account="compatible")
            self.insert_quota(db, "compatible-earlier", 100, 200, account="compatible")
            self.insert_quota(db, "nested", 120, account="compatible")
            # The merged earlier run covers the later conflicting observation.
            self.insert_quota(db, "chain-first", 100, account="chain")
            self.insert_quota(db, "chain-next", 110, 200, account="chain")
            self.insert_quota(db, "chain-conflict", 150, account="chain", usedPercent=51)
            db.commit()
        migrate_local(self.paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute("SELECT account,record_key,range_first,range_last,used_percent FROM canonical_quota "
                "WHERE account IN ('overlap','ties','compatible','chain') ORDER BY account,range_first").fetchall(),
                [("chain", "chain-first", 100, 200, 50), ("compatible", "compatible-earlier", 100, 250, 50),
                    ("overlap", "long", 100, 200, 50), ("overlap", "outside", 210, 210, 51), ("ties", "tie-a", 100, 110, 50)])

    def test_compaction_failure_rolls_back_entire_migration_and_can_retry(self):
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.insert_quota(db, "a", 100)
            self.insert_quota(db, "b", 110)
            db.commit()
        self.legacy_continuous()
        from monitor_migration_quota_boundary import _compact_quota
        def fail_after_compaction(db):
            _compact_quota(db)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM canonical_quota WHERE account='history'").fetchone()[0], 1)
            raise RuntimeError("interrupted compaction")
        with closing(sqlite3.connect(self.store.index.path)) as db:
            before = db.execute("SELECT * FROM canonical_quota ORDER BY dataset,account,record_key").fetchall()
        with mock.patch("monitor_migration_quota_boundary._compact_quota", side_effect=fail_after_compaction), self.assertRaisesRegex(RuntimeError, "interrupted compaction"):
            migrate_local(self.paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute("SELECT * FROM canonical_quota ORDER BY dataset,account,record_key").fetchall(), before)
        migrate_local(self.paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute("SELECT range_first,range_last FROM canonical_quota WHERE account='history'").fetchall(), [(100, 110)])

    def test_raw_history_replays_decreases_confirmation_resets_and_range_endpoints(self):
        cases = {
            "falling": ([(100, 40, None, {}), (110, 20, None, {}), (120, 19, None, {}), (130, 18, None, {}), (140, 21, None, {})],
                [(100, 100, 40), (130, 130, 18), (140, 140, 21)]),
            "unconfirmed": ([(100, 40, None, {}), (110, 20, None, {})], [(100, 100, 40)]),
            "confirmed": ([(100, 40, None, {}), (110, 20, None, {}), (120, 20, None, {})], [(100, 100, 40), (110, 120, 20)]),
            "reset": ([(100, 40, None, {}), (110, 20, None, {}), (120, 10, None, {"resetAt": None})], [(100, 100, 40), (120, 120, 10)]),
            "duplicate-time": ([(100, 40, None, {}), (110, 20, None, {}), (110, 21, None, {}), (120, 22, None, {})],
                [(100, 100, 40), (110, 110, 20), (120, 120, 22)]),
            "range-confirmation": ([(100, 40, None, {}), (110, 20, 130, {})], [(100, 100, 40), (110, 130, 20)]),
        }
        with closing(sqlite3.connect(self.store.index.path)) as db:
            for dataset in ("local", "merged"):
                for account, (observations, _) in cases.items():
                    for index, (first, value, last, state) in reversed(list(enumerate(observations))):
                        self.insert_quota(db, str(index), first, last, dataset, account, usedPercent=value, **state)
            db.execute("INSERT INTO processing_meta VALUES('samplingState:local:unconfirmed',?)", (json.dumps({"windows": {"fiveHour": {"raw": 20}}}),))
            db.commit()
        migrate_local(self.paths)
        migrate_local(self.paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            for dataset in ("local", "merged"):
                for account, (_, expected) in cases.items():
                    with self.subTest(dataset=dataset, account=account):
                        self.assertEqual(db.execute("SELECT range_first,range_last,used_percent FROM canonical_quota WHERE dataset=? AND account=? ORDER BY range_first",
                            (dataset, account)).fetchall(), expected)
            self.assertIsNone(db.execute("SELECT value FROM processing_meta WHERE name='samplingState:local:unconfirmed'").fetchone())
            pending = json.loads(db.execute("SELECT value FROM processing_meta WHERE name='migrationRebuildPending'").fetchone()[0])
            self.assertFalse(pending["sampling"])
            self.assertFalse(pending["projection"])
            self.assertEqual(len(pending["reasons"]), 1)
        migrations._apply_migration_rebuild(self.paths)
        DashboardDelivery(self.store.index).prepare()
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertIsNone(db.execute("SELECT value FROM processing_meta WHERE name='migrationRebuildPending'").fetchone())

    def legacy(self):
        with closing(sqlite3.connect(self.store.index.path)) as db:
            db.execute("ALTER TABLE canonical_quota ADD COLUMN reset_boundary INTEGER NOT NULL DEFAULT 0 CHECK(reset_boundary IN (0,1))")
            db.execute("UPDATE canonical_quota SET reset_boundary=1")
            db.commit()

    def legacy_seen(self):
        with closing(sqlite3.connect(self.store.index.path)) as db:
            db.execute("ALTER TABLE canonical_seen RENAME TO cost_seen")
            db.execute("CREATE TABLE canonical_seen(view TEXT,kind TEXT,record_key TEXT,PRIMARY KEY(view,kind,record_key))")
            db.execute("INSERT INTO canonical_seen SELECT view,'tokenLedger',record_key FROM cost_seen")
            db.execute("DROP TABLE cost_seen")
            db.executemany("INSERT INTO canonical_seen VALUES(?,?,?)", ((view, "quota", f"quota:{index}:5h") for view in ("local", "merged") for index in range(100)))
            db.commit()

    def test_seen_migration_removes_quota_ids_and_kind_preserves_costs_and_other_tables(self):
        self.legacy_seen()
        with closing(sqlite3.connect(self.store.index.path)) as db:
            expected = db.execute("SELECT view,record_key FROM canonical_seen WHERE kind='tokenLedger' ORDER BY view,record_key").fetchall()
            others = {name: db.execute(f'SELECT * FROM "{name}"').fetchall()
                for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name!='canonical_seen'").fetchall()}
        migrate_local(self.paths)
        migrate_local(self.paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual([row[1] for row in db.execute("PRAGMA table_info(canonical_seen)")], ["view", "record_key"])
            self.assertEqual(db.execute("SELECT * FROM canonical_seen ORDER BY view,record_key").fetchall(), expected)
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_seen_legacy'").fetchone())
            for name, rows in others.items():
                self.assertEqual(db.execute(f'SELECT * FROM "{name}"').fetchall(), rows, name)
        self.assertFalse(self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=2)]))

    def has_column(self):
        with closing(sqlite3.connect(self.store.index.path)) as db:
            return any(row[1] == "reset_boundary" for row in db.execute("PRAGMA table_info(canonical_quota)"))

    def legacy_continuous(self):
        self.legacy()
        bit = next(index for index, field in enumerate(RELEASED_QUOTA) if field[0] == "continuous")
        with closing(sqlite3.connect(self.store.index.path)) as db:
            db.execute("ALTER TABLE canonical_quota ADD COLUMN continuous REAL CHECK(continuous BETWEEN -1.7976931348623157e308 AND 1.7976931348623157e308)")
            for mask in ("present", "integers"):
                db.execute(f"UPDATE canonical_quota SET {mask}=({mask} & ?) | (({mask} >> ?) << ?)", ((1 << bit) - 1, bit, bit + 1))
            db.execute("UPDATE canonical_quota SET continuous=used_percent,present=present | ?", (1 << bit,))
            db.commit()
        return bit

    def test_replays_raw_points_ignoring_old_decisions_and_preserves_masks(self):
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota("2030-01-01T10:01:00Z", value=51, seven=11))
        bit = self.legacy_continuous()
        with closing(sqlite3.connect(self.store.index.path)) as db:
            db.execute("UPDATE canonical_quota SET continuous=NULL WHERE window='5h' AND used_percent=50")
            # Development records without this optional field were already accepted.
            db.execute("UPDATE canonical_quota SET continuous=NULL,present=present & ? WHERE window='7d' AND used_percent=10", (~(1 << bit),))
            db.execute("UPDATE canonical_quota SET used_percent=0,continuous=0 WHERE window='5h' AND used_percent=51")
            db.execute("UPDATE canonical_quota SET multiplier=5,reset_at_text='2030-01-02T00:00:00Z' WHERE window='7d'")
            db.commit()
            expected = {}
            for view in ("local", "merged"):
                expected[view] = []
                for row in released_records.rows(db, view, "quota"):
                    window = next(iter(row["windows"].values()))
                    # The old accepted zero is now an unconfirmed decrease;
                    # the old rejected 50 is the first valid raw observation.
                    if window["usedPercent"] == 0:
                        continue
                    window.pop("continuous", None)
                    window.pop("resetBoundary", None)
                    expected[view].append(row)
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
            others = {name: db.execute(f'SELECT * FROM "{name}"').fetchall()
                for name, in tables if name not in {"canonical_quota", "processing_meta"}}
        migrate_local(self.paths)
        migrate_local(self.paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertFalse({"continuous", "reset_boundary"} & {row[1] for row in db.execute("PRAGMA table_info(canonical_quota)")})
            self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall(), tables)
            self.assertTrue(all(data is None for data, in db.execute("SELECT extras_json FROM canonical_quota")))
            for name, rows in others.items():
                self.assertEqual(db.execute(f'SELECT * FROM "{name}"').fetchall(), rows, name)
        for view in ("local", "merged"):
            self.assertEqual(self.store.datasets(view)[0], expected[view])
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota("2030-01-01T10:02:00Z", value=52))

    def test_failed_cleanup_restores_deleted_points_masks_and_columns(self):
        self.legacy_continuous()
        self.legacy_seen()
        with closing(sqlite3.connect(self.store.index.path)) as db:
            db.execute("UPDATE canonical_quota SET continuous=NULL WHERE window='5h'")
            db.execute("UPDATE canonical_quota SET extras_json='invalid-json' WHERE window='7d'")
            db.commit()
            before = db.execute("SELECT * FROM canonical_quota ORDER BY dataset,window").fetchall()
            identities = db.execute("SELECT * FROM canonical_seen ORDER BY view,kind,record_key").fetchall()
        with self.assertRaises(json.JSONDecodeError):
            migrate_local(self.paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertIn("continuous", [row[1] for row in db.execute("PRAGMA table_info(canonical_quota)")])
            self.assertEqual(db.execute("SELECT * FROM canonical_quota ORDER BY dataset,window").fetchall(), before)
            self.assertEqual(db.execute("SELECT * FROM canonical_seen ORDER BY view,kind,record_key").fetchall(), identities)
            db.execute("UPDATE canonical_quota SET extras_json=NULL")
            db.commit()
        migrate_local(self.paths)
        self.assertTrue(any("5h" in row["windows"] for row in self.store.datasets()[0]))

    def test_fresh_store_has_no_retired_column(self):
        self.assertFalse(self.has_column())
        migrate_local(self.paths)
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota("2030-01-01T10:01:00Z"))
        self.assertFalse(self.has_column())

    def test_drop_preserves_every_remaining_column_and_other_table(self):
        self.legacy()
        with closing(sqlite3.connect(self.store.index.path)) as db:
            columns = [row[1] for row in db.execute("PRAGMA table_info(canonical_quota)") if row[1] != "reset_boundary"]
            before = db.execute(f"SELECT {','.join(columns)} FROM canonical_quota").fetchall()
            others = {name: db.execute(f'SELECT * FROM "{name}"').fetchall()
                for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name!='canonical_quota'").fetchall()}
        migrate_local(self.paths)
        migrate_local(self.paths)
        self.assertFalse(self.has_column())
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute(f"SELECT {','.join(columns)} FROM canonical_quota").fetchall(), before)
            for name, rows in others.items():
                self.assertEqual(db.execute(f'SELECT * FROM "{name}"').fetchall(), rows, name)
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota("2030-01-01T10:01:00Z"))

    def test_extension_cleanup_rolls_back_column_drop_on_failure_and_retries(self):
        self.legacy()
        with closing(sqlite3.connect(self.store.index.path)) as db:
            db.execute("UPDATE canonical_quota SET extras_json='invalid-json'")
            db.commit()
            before = db.execute("SELECT * FROM canonical_quota").fetchall()
        with self.assertRaises(json.JSONDecodeError):
            migrate_local(self.paths)
        self.assertTrue(self.has_column())
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute("SELECT * FROM canonical_quota").fetchall(), before)
            for window in ("5h", "7d"):
                db.execute("UPDATE canonical_quota SET extras_json=? WHERE window=?",
                    (json.dumps({"extension": 7, "windows": {window: {"resetBoundary": True}}}), window))
            db.commit()
        migrate_local(self.paths)
        self.assertFalse(self.has_column())
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertTrue(all(json.loads(data) == {"extension": 7} for data, in db.execute("SELECT extras_json FROM canonical_quota")))

    def test_marker_interruption_restores_column_before_retry(self):
        self.legacy_continuous()
        self.legacy_seen()
        state, journal = self.root / "contract.json", self.root / "journal.json"
        state.write_text('{"dataContractVersion":23}', encoding="utf-8")
        original = migrations._atomic_write
        def interrupt(path, data):
            if path == state:
                raise SystemExit("interrupted marker publication")
            original(path, data)
        with mock.patch.object(migrations, "_atomic_write", side_effect=interrupt), self.assertRaises(SystemExit):
            migrations._run_data_migration(migrate_local, self.paths, 24, state, journal)
        migrations._recover_data_migration(journal, state, self.paths | {"config": self.root / "config.json"})
        self.assertTrue(self.has_column())
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM canonical_seen WHERE kind='quota'").fetchone()[0], 200)
        migrations._run_data_migration(migrate_local, self.paths, 24, state, journal)
        self.assertFalse(self.has_column())
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM canonical_seen").fetchone()[0], 2)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 24)
        self.assertFalse(journal.exists())

    def test_startup_migrates_released_contract23_once(self):
        self.legacy_continuous()
        self.legacy_seen()
        (self.root / migrations.DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":23}', encoding="utf-8")
        args = (self.root, self.root / "history", self.store.quota_path, self.root / "sessions", self.store.token_ledger_path, self.root / "diagnostics")
        self.assertEqual(migrations.migrate_history_data(*args, usage_sync_cache_path=self.store.cache_path), [24])
        self.assertEqual(migrations.migrate_history_data(*args, usage_sync_cache_path=self.store.cache_path), [])
        self.assertFalse(self.has_column())
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual([row[1] for row in db.execute("PRAGMA table_info(canonical_seen)")], ["view", "record_key"])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM canonical_seen").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
