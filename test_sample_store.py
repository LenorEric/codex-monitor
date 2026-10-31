import copy
import json
import shutil
import sqlite3
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from unittest import mock

import monitor_migrations
import monitor_sample_store as samples
from monitor_processing import ProcessingIndex
from monitor_usage_sync import content_hash, v4_record_envelope, v4_logical_hash


class SampleStoreTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / (".test-samples-" + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.path = self.root / "cache.sqlite3"

    @staticmethod
    def records():
        sync = {"version": 1, "accountId": "account", "originMachineId": "device", "localOnly": False}
        return [
            ("quota", {"checkedAt": "2026-10-01T00:00:00.000+00:00", "sync": sync, "accountSlotId": "slot", "accountLabel": "Name",
                "windows": {"5h": {"usedPercent": 1.0, "resetAt": None, "plan": "plus"}, "7d": None},
                "future": {"nested": True}}),
            ("quota", {"checkedAt": None, "windows": {"5h": {}, "newWindow": {"future": 1}}, "sync": None}),
            ("quota", {}),
            ("tokenLedger", {"schemaVersion": 2, "recordType": "usage", "eventId": "event", "occurredAt": "2026-10-01T00:00:00.000Z",
                "sessionId": "session", "rawModel": "gpt-5", "billingModel": "gpt-5", "serviceTier": "fast", "sourceFile": "rollout.jsonl", "sync": sync,
                "tokens": {name: i for i, name in enumerate(samples.TOKENS)} | {"future": 4, "totalCostUsd": "not a cost"},
                "cost": {name: i * .000000000013 for i, name in enumerate(samples.COSTS)} | {"future": None, "inputTokens": 92}}),
            ("tokenLedger", {"schemaVersion": 2.0, "recordType": "usage", "tokens": {"inputTokens": 0.0, "outputTokens": None, "requests": True},
                "cost": {"inputCostUsd": 1, "totalCostUsd": -0.0, "outputCostUsd": 2**60 + 1}, "serviceTier": None}),
            ("tokenLedger", {"schemaVersion": 1, "recordType": "sessionBaseline", "sync": sync,
                "session": {"sessionId": "session", "accountSlotId": "old", "accountLabel": None, "startedAt": None,
                    "tokens": {"inputTokens": 23}, "cost": {"totalCostUsd": .12345678901234568}, "fastTokens": None,
                    "byModel": {"gpt-5": {"tokens": {"requests": 3}, "cost": {"inputCostUsd": 1.0}, "fastTokens": {"requests": 1}, "future": []},
                        "empty": {"tokens": None, "cost": {}, "fastTokens": {}}, "unknown": None}},
                "sourceTotals": {"gpt-5": {"fast": {"requests": 7}, "default": {}}, "empty": {}, "unknown": None}}),
            ("tokenLedger", {"recordType": "sessionBaseline", "session": None, "sourceTotals": None}),
            ("futureKind", {"payload": [None, 1, 1.0, True], "session": {"accountLabel": "Name"}}),
        ]

    def test_typed_round_trip_exact_canonical_and_hash_with_no_full_payload(self):
        with closing(samples.connect(self.path)) as db, db:
            samples.prepare(db)
            for kind, row in self.records():
                with self.subTest(kind=kind, row=row):
                    sample, attrs = samples.insert_sample(db, kind, row)
                    result = samples.select_sample(db, sample, attrs)
                    self.assertEqual(samples.canonical(result), samples.canonical(row))
                    self.assertEqual(content_hash(result), content_hash(row))
                    opaque, = db.execute("SELECT opaque FROM sample_records WHERE id=?", (sample,)).fetchone()
                    self.assertEqual(opaque is None, kind != "futureKind")
            self.assertFalse(db.execute("PRAGMA foreign_key_check").fetchall())

    def test_ownership_dedup_versions_overlay_and_final_reference_cleanup(self):
        index = ProcessingIndex(self.path)
        kind, row = self.records()[0]
        record = {"kind": kind, "row": samples._payload(row)}
        digest = content_hash(record)
        envelope = v4_record_envelope("device", "key", record, "2026-10-01T00:00Z")
        with index.transaction() as db:
            samples.put_fact(db, ("source", 0, kind, "key", "account", "", "", 0, samples.canonical(row)))
            samples.put_local(db, "key", "2026-10-01T00:00Z", digest, record)
            samples.put_remote(db, "device", "2026-10-01", "bulk", "key", digest, envelope)
            self.assertEqual(db.execute("SELECT count(*) FROM sample_records").fetchone()[0], 1)
            self.assertEqual(json.loads(db.execute("SELECT data FROM processing_facts").fetchone()[0]), row)
            self.assertEqual(json.loads(db.execute("SELECT record_json FROM remote_records").fetchone()[0]), envelope)
            corrected = copy.deepcopy(record)
            corrected["row"]["windows"]["5h"]["usedPercent"] = 2
            samples.put_local(db, "key", "later ignored", content_hash(corrected), corrected)
            self.assertEqual(db.execute("SELECT count(*) FROM sample_records").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT period FROM local_records").fetchone()[0], "2026-10-01T00:00Z")
            db.execute("DELETE FROM processing_facts")
            db.execute("DELETE FROM remote_records")
        with index.transaction() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM sample_records").fetchone()[0], 1)
            db.execute("DELETE FROM local_records")
        with index.transaction() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM sample_records").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT count(*) FROM quota_windows").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT count(*) FROM sample_releases").fetchone()[0], 0)

    def test_inline_one_to_one_nodes_and_independent_window_ownership(self):
        row = self.records()[0][1]
        row["windows"] = {
            "5h": {"usedPercent": 10, "percentCheckedAt": row["checkedAt"], "resetAt": 1790830800, "plan": "plus"},
            "7d": {"usedPercent": 20, "resetAt": 1791417600, "plan": "plus"},
        }
        later = copy.deepcopy(row)
        later["checkedAt"] = "2026-10-01T00:01:00Z"
        later["windows"]["7d"]["usedPercent"] = 21
        rolled = copy.deepcopy(later)
        rolled["checkedAt"] = "2026-10-01T00:02:00Z"
        rolled["windows"]["7d"]["resetAt"] = 1792022400
        index = ProcessingIndex(self.path)
        with index.transaction() as db:
            samples.put_local(db, "first", "period", "hash", {"kind": "quota", "row": row})
            samples.put_local(db, "later", "period", "hash", {"kind": "quota", "row": later})
            names = {name for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue(set(samples.INLINE).isdisjoint(names))
            refs = db.execute(
                "SELECT five_hour_id,seven_day_id,five_hour_percentCheckedAt,seven_day_usedPercent FROM sample_records ORDER BY id"
            ).fetchall()
            self.assertEqual(refs[0][:2], refs[1][:2])
            self.assertEqual(refs[0][2], row["checkedAt"])
            self.assertEqual([refs[0][3], refs[1][3]], [20.0, 21.0])
            self.assertEqual(db.execute("SELECT count(*) FROM quota_windows").fetchone()[0], 2)
            self.assertEqual(
                db.execute("SELECT resetAt,extensions FROM quota_windows WHERE id=?", (refs[0][0],)).fetchone(),
                (1790830800.0, None),
            )
            samples.put_local(db, "rolled", "period", "hash", {"kind": "quota", "row": rolled})
            refs = db.execute("SELECT five_hour_id,seven_day_id FROM sample_records ORDER BY id").fetchall()
            self.assertEqual(refs[0][0], refs[2][0])
            self.assertNotEqual(refs[0][1], refs[2][1])
            self.assertEqual(db.execute("SELECT count(*) FROM quota_windows").fetchone()[0], 3)
            db.execute("DELETE FROM local_records WHERE record_key IN ('first','later')")
        with index.transaction() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM quota_windows").fetchone()[0], 2)
            self.assertEqual(
                samples.select_sample(db, db.execute("SELECT sample_id FROM local_record_refs").fetchone()[0]),
                samples._payload(rolled),
            )
            db.execute("DELETE FROM local_records")
        with index.transaction() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM quota_windows").fetchone()[0], 0)

    def test_redundancy_fixes_shared_payloads_and_delivery_tables(self):
        index = ProcessingIndex(self.path)
        _, usage_row = self.records()[3]
        quota_point = {
            "checkedAt": "2026-10-01T00:00:00.000+00:00",
            "accountSlotId": "account",
            "windows": {"5h": {"usedPercent": 10.0, "resetAt": 1790830800, "plan": "plus"}},
        }
        with index.transaction() as db:
            virtual_cols = {
                row[1] for row in db.execute("PRAGMA table_xinfo(sample_records)") if row[6] == 2
            }
            self.assertTrue({"subtype", "account_id", "origin_device_id", "identity_session_id"}.issubset(virtual_cols))
            fact_cols = [row[1] for row in db.execute("PRAGMA table_xinfo(processing_fact_refs)") if row[6] == 0]
            self.assertEqual(fact_cols, ["source_id", "position", "record_key", "account_id", "sample_id", "attribution"])
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_peer_rows'").fetchone())
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_members'").fetchone())

            samples.put_fact(db, ("source", 0, "tokenLedger", "u1", "account", "session", "event", 1790812800.0, samples.canonical(usage_row)))
            attr, = db.execute("SELECT attribution FROM processing_fact_refs WHERE record_key='u1'").fetchone()
            self.assertIsNone(attr)

            index._effective(db, "u1")
            self.assertEqual(db.execute("SELECT count(*) FROM processing_effective_refs").fetchone()[0], 1)
            self.assertEqual(json.loads(db.execute("SELECT data FROM processing_effective WHERE record_key='u1'").fetchone()[0]), usage_row)

            raw_point = samples.canonical(quota_point)
            display_point = samples.canonical(quota_point | {"_key": "q1"})
            db.execute(
                "INSERT INTO processing_sampled_quota VALUES(?,?,?,?,?)",
                ("q1", "account", "device", 1790812800.0, raw_point),
            )
            db.execute(
                "INSERT INTO processing_quota_points VALUES(?,?,?,?,?)",
                ("local", "account", 1790812800.0, "q1", raw_point),
            )
            db.execute(
                "INSERT INTO processing_quota_points VALUES(?,?,?,?,?)",
                ("merged", "account", 1790812800.0, "q1", raw_point),
            )
            db.execute(
                "INSERT INTO processing_display VALUES(?,?,?,?,?,?)",
                ("local", "account", "quotaPoints", "q1", 1790812800.0, display_point),
            )
            db.execute(
                "INSERT INTO processing_display VALUES(?,?,?,?,?,?)",
                ("merged", "account", "quotaPoints", "q1", 1790812800.0, display_point),
            )
            self.assertEqual(db.execute("SELECT count(*) FROM processing_payloads").fetchone()[0], 1)
            self.assertEqual(
                json.loads(db.execute("SELECT data FROM processing_display WHERE view='local' AND record_key='q1'").fetchone()[0]),
                quota_point | {"_key": "q1"},
            )

            token_args = ("account", "u1", 1790812800.0, "gpt-5", "fast", 10, 0, 20, 0, 100)
            db.execute("INSERT INTO delivery_tokens VALUES('local',?,?,?,?,?,?,?,?,?,?)", token_args)
            db.execute("INSERT INTO delivery_tokens VALUES('merged',?,?,?,?,?,?,?,?,?,?)", token_args)
            self.assertEqual(db.execute("SELECT count(*) FROM delivery_token_payloads").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM delivery_token_refs").fetchone()[0], 2)

            run_args = ("account", "5h", 100, 100.0, 200.0, 10.0, 1.0, 0, 300.0, "plus")
            db.execute("INSERT INTO delivery_runs VALUES('local',?,?,?,?,?,?,?,?,?,?)", run_args)
            db.execute("INSERT INTO delivery_runs VALUES('merged',?,?,?,?,?,?,?,?,?,?)", run_args)
            self.assertEqual(db.execute("SELECT count(*) FROM delivery_run_payloads").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM delivery_run_refs").fetchone()[0], 2)

    def test_layout_migration_is_lossless_atomic_and_retryable(self):
        import monitor_migration_samples as legacy
        with closing(legacy.connect(self.path)) as db, db:
            legacy.initialize(db)
            legacy.create_views(db)
            for i, (kind, row) in enumerate(self.records()):
                legacy.put_local(db, str(i), "period", content_hash({"kind": kind, "row": row}), {"kind": kind, "row": row})
            before = db.execute("SELECT record_key,record_json FROM local_records ORDER BY record_key").fetchall()
        with closing(samples.connect(self.path)) as db, db:
            with mock.patch.object(samples, "_insert_window", side_effect=RuntimeError("interrupted")):
                with self.assertRaisesRegex(RuntimeError, "interrupted"):
                    samples.migrate_layout(db)
            self.assertIsNotNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='sample_sync'").fetchone())
            self.assertNotIn("five_hour_id", {column[1] for column in db.execute("PRAGMA table_info(sample_records)")})
            samples.migrate_layout(db)
            samples.migrate_layout(db)
            after = db.execute("SELECT record_key,record_json FROM local_records ORDER BY record_key").fetchall()
            self.assertEqual(before, after)
            self.assertFalse(db.execute("PRAGMA foreign_key_check").fetchall())
        with closing(samples.connect(self.path)) as db:
            samples.prepare(db)
            self.assertEqual(before, db.execute("SELECT record_key,record_json FROM local_records ORDER BY record_key").fetchall())

    def test_retired_compaction_is_removed_and_migration_merges_equivalent_samples(self):
        import monitor_migration_samples as legacy
        row = self.records()[0][1]
        with closing(legacy.connect(self.path)) as db, db:
            legacy.initialize(db)
            legacy.create_views(db)
            for i in range(2):
                old = row | {"compaction": {"continuousFrom": "2026-09-01T00:00:00Z", "omittedSamples": i + 1}}
                record = {"kind": "quota", "row": legacy._payload(old)}
                legacy.put_fact(db, ("source", i, "quota", str(i), "account", "", "", 0, samples.canonical(old)))
                legacy.put_local(db, str(i), "period", content_hash(record), record)
                legacy.put_remote(db, "device", "2026-10-01", "bulk", str(i), content_hash(record),
                    v4_record_envelope("device", str(i), record, "2026-10-01T00:00Z"))
        with closing(samples.connect(self.path)) as db, db:
            with mock.patch.object(samples, "_insert_window", side_effect=RuntimeError("interrupted")):
                with self.assertRaisesRegex(RuntimeError, "interrupted"):
                    samples.migrate_layout(db)
            self.assertEqual(db.execute("SELECT count(*) FROM quota_samples WHERE omittedSamples IS NOT NULL").fetchone()[0], 2)
            samples.migrate_layout(db)
            samples.migrate_layout(db)
        with closing(samples.connect(self.path)) as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='quota_samples'").fetchone())
            self.assertFalse(any(column[1].startswith("quota_") for column in db.execute("PRAGMA table_info(sample_records)")))
            self.assertEqual(db.execute("SELECT count(*) FROM sample_records").fetchone()[0], 1)
            for data, in db.execute("SELECT data FROM processing_facts"):
                self.assertEqual(json.loads(data), row)
            for digest, data in db.execute("SELECT content_hash,record_json FROM local_records"):
                self.assertNotIn("compaction", json.loads(data)["row"])
                self.assertEqual(digest, content_hash(json.loads(data)))
            for digest, data in db.execute("SELECT content_hash,record_json FROM remote_records"):
                envelope = json.loads(data)
                self.assertNotIn("compaction", envelope["record"]["row"])
                self.assertEqual(digest, content_hash(envelope["record"]))
                self.assertEqual(digest, envelope["contentSha256"])
            self.assertFalse(db.execute("PRAGMA foreign_key_check").fetchall())
        with closing(samples.connect(self.root / "fresh.sqlite3")) as db, db:
            samples.prepare(db)
            sample, attrs = samples.insert_sample(db, "quota", row | {"compaction": {"omittedSamples": 1}})
            self.assertEqual(samples.select_sample(db, sample, attrs), row)

    def legacy(self):
        kind, row = self.records()[0]
        record = {"kind": kind, "row": samples._payload(row)}
        envelope = v4_record_envelope("device", "key", record, "2026-10-01T00:00Z")
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("CREATE TABLE processing_facts(source TEXT,position INTEGER,kind TEXT,record_key TEXT,account TEXT,session TEXT,event_id TEXT,event_at REAL,data TEXT,PRIMARY KEY(source,position))")
            db.execute("CREATE TABLE local_records(record_key TEXT PRIMARY KEY,period TEXT,content_hash TEXT,record_json TEXT)")
            db.execute("CREATE TABLE remote_records(machine_id TEXT,day TEXT,part_id TEXT,record_key TEXT,content_hash TEXT,record_json TEXT,PRIMARY KEY(machine_id,record_key))")
            db.execute("INSERT INTO processing_facts VALUES(?,?,?,?,?,?,?,?,?)", ("source", 0, kind, "key", "account", "", "", 0, samples.canonical(row)))
            db.execute("INSERT INTO local_records VALUES(?,?,?,?)", ("key", "2026-10-01T00:00Z", content_hash(record), samples.canonical(record)))
            db.execute("INSERT INTO remote_records VALUES(?,?,?,?,?,?)", ("device", "2026-10-01", "bulk", "key", content_hash(record), samples.canonical(envelope)))
            db.execute("CREATE TABLE processing_meta(name TEXT PRIMARY KEY,value TEXT)")
            db.executemany("INSERT INTO processing_meta VALUES(?,?)", [("ledgerBatch", '{"pending":true}'), ("checkpoint", '42')])
        return row, record, envelope

    def test_migration_bounded_verify_rollback_retry_restart_and_unknown_envelopes(self):
        row, record, envelope = self.legacy()
        with mock.patch.object(__import__("monitor_migration_samples"), "select_sample", side_effect=RuntimeError("interrupted")):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                __import__("monitor_migrations").migrate_sample_store(self.path, batch_size=1)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(json.loads(db.execute("SELECT data FROM processing_facts").fetchone()[0]), row)
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='sample_records'").fetchone())
        __import__("monitor_migrations").migrate_sample_store(self.path, batch_size=1)
        __import__("monitor_migrations").migrate_sample_store(self.path, batch_size=1)
        with closing(samples.connect(self.path)) as db, db:
            self.assertEqual(json.loads(db.execute("SELECT data FROM processing_facts").fetchone()[0]), row)
            self.assertEqual(json.loads(db.execute("SELECT record_json FROM local_records").fetchone()[0]), record)
            remote = json.loads(db.execute("SELECT record_json FROM remote_records").fetchone()[0])
            self.assertEqual(samples.canonical(remote), samples.canonical(envelope))
            self.assertEqual(v4_logical_hash([remote]), v4_logical_hash([envelope]))
            self.assertEqual(db.execute("SELECT value FROM processing_meta WHERE name='checkpoint'").fetchone()[0], "42")
            unknown = {"record": {"kind": "future", "data": [1, None]}, "schemaVersion": 9, "collectionPeriod": "2026-10-01T00:00Z"}
            samples.put_remote(db, "device", "2026-10-01", "bulk", "unknown", content_hash(unknown["record"]), unknown)
            self.assertEqual(json.loads(db.execute("SELECT record_json FROM remote_records WHERE record_key='unknown'").fetchone()[0]), unknown)
            self.assertFalse(db.execute("PRAGMA foreign_key_check").fetchall())
            for table in ("processing_fact_refs", "local_record_refs", "remote_record_refs"):
                self.assertTrue({"data", "record_json"}.isdisjoint({column[1] for column in db.execute(f"PRAGMA table_info({table})")}))

    def test_journal_recovery_and_sequential_v13_to_v14_keeps_sources_and_marker_on_failure(self):
        self.legacy()
        state = self.root / "state.json"
        journal = self.root / "journal.json"
        state.write_text('{"dataContractVersion":13}')
        quota = self.root / "quota.jsonl"
        quota.write_bytes(b'authoritative original\n')
        paths = {"processing_index": self.path, "quota_history": quota}
        def interrupted(paths):
            __import__("monitor_migrations").migrate_sample_store(paths["processing_index"])
            raise RuntimeError("after conversion")
        with self.assertRaisesRegex(RuntimeError, "after conversion"):
            monitor_migrations._run_data_migration(interrupted, paths, 14, state, journal)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 13)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT type FROM sqlite_master WHERE name='processing_facts'").fetchone()[0], "table")
        monitor_migrations._run_data_migration(monitor_migrations.DATA_MIGRATIONS[14], paths, 14, state, journal)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 14)
        self.assertEqual(quota.read_bytes(), b'authoritative original\n')
        self.assertFalse(journal.exists())
        self.assertEqual(ProcessingIndex(self.path).metrics["canonicalBytesRead"], 0)

    def test_unexpected_exit_leaves_journal_for_startup_recovery(self):
        row, _, _ = self.legacy()
        state, journal = self.root / "state.json", self.root / "journal.json"
        state.write_text('{"dataContractVersion":13}')
        paths = {"processing_index": self.path}
        def crash(paths):
            __import__("monitor_migrations").migrate_sample_store(paths["processing_index"])
            raise SystemExit("simulated power interruption after DB commit")
        with self.assertRaises(SystemExit):
            monitor_migrations._run_data_migration(crash, paths, 14, state, journal)
        self.assertTrue(journal.exists())
        monitor_migrations._recover_data_migration(journal, state, paths)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(json.loads(db.execute("SELECT data FROM processing_facts").fetchone()[0]), row)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 13)
        __import__("monitor_migrations").migrate_sample_store(self.path)

    def test_hash_mismatch_preserves_original_and_idle_has_no_sample_scan(self):
        self.legacy()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE local_records SET content_hash='damaged'")
        with self.assertRaisesRegex(ValueError, "synchronization hash mismatch"):
            __import__("monitor_migrations").migrate_sample_store(self.path)
        with closing(sqlite3.connect(self.path)) as db, db:
            self.assertEqual(db.execute("SELECT type FROM sqlite_master WHERE name='processing_facts'").fetchone()[0], "table")
        other = ProcessingIndex(self.root / "idle.sqlite3")
        with other.transaction() as db:
            samples.put_fact(db, ("source", 0, "quota", "key", "account", "", "", 0, '{}'))
        with mock.patch.object(__import__("monitor_migration_samples"), "select_sample", side_effect=AssertionError("idle payload read")):
            with other.transaction() as db:
                self.assertEqual(db.execute("SELECT 1 FROM processing_facts LIMIT 1").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT count(*) FROM sample_releases").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
