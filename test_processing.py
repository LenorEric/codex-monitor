import json
import tempfile
import shutil
import uuid
import threading
import unittest
from pathlib import Path
from unittest import mock

import monitor_processing
import monitor_token_ledger
import monitor_tokens
from monitor_usage_sync import UsageDataStore, add_record_provenance


class ProcessingTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-processing-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(self.cleanup)
        self.store = UsageDataStore(self.root / "quota.jsonl", self.root / "ledger.jsonl", "machine", lambda _: "account", threading.RLock())
        self.log = self.root / "sessions" / "2030" / "01" / "01" / "rollout.jsonl"
        self.log.parent.mkdir(parents=True)
        self.write([
            {"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "session"}},
            {"timestamp": "2030-01-01T00:00:01Z", "type": "turn_context", "payload": {"model": "gpt-5.5"}},
            self.event(1, 100),
        ])

    def cleanup(self):
        if not self.root.resolve().is_relative_to(Path.cwd().resolve()) or not self.root.name.startswith(".test-processing-"):
            raise ValueError("Unexpected test directory")
        shutil.rmtree(self.root)

    @staticmethod
    def event(minute, tokens):
        return {"timestamp": f"2030-01-01T00:{minute:02}:00Z", "type": "event_msg", "payload": {"type": "token_count",
            "info": {"total_token_usage": {"input_tokens": tokens, "cached_input_tokens": 10, "output_tokens": minute * 10}}}}

    def write(self, rows, append=False):
        with self.log.open("a" if append else "w", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row) + "\n")

    def sync(self, store=None):
        store = store or self.store
        store.index.scan(self.root)
        return store.index.sync_ledger(store.token_ledger_path, "slot", "Account", [],
            lambda row: add_record_provenance("tokenLedger", row, "machine", "account"), store.normalize_new_ledger, "machine")

    def test_restart_and_idle_do_not_read_or_hash_history(self):
        expected = monitor_tokens.scan_codex_token_usage(self.root)
        actual = self.store.index.scan(self.root)
        self.assertEqual(actual["totals"], expected["totals"])
        self.sync()
        first = dict(self.store.index.metrics)
        self.sync()
        self.assertEqual(self.store.index.metrics["sourceBytesRead"], first["sourceBytesRead"])
        self.assertEqual(self.store.index.metrics["eventsHashed"], first["eventsHashed"])
        restarted = UsageDataStore(self.store.quota_path, self.store.token_ledger_path, "machine", lambda _: "account", threading.RLock())
        with mock.patch.object(monitor_token_ledger, "load_token_ledger", side_effect=AssertionError("history reload")):
            self.assertEqual(self.sync(restarted), self.store.index.ledger_cost(self.store.token_ledger_path))
        self.assertEqual(restarted.index.metrics["sourceBytesRead"], 0)
        self.assertEqual(restarted.index.metrics["eventsHashed"], 0)

    def test_new_event_appends_only_new_ledger_usage(self):
        self.sync()
        before = self.store.token_ledger_path.read_bytes()
        self.write([self.event(2, 150)], append=True)
        with mock.patch.object(monitor_token_ledger, "load_token_ledger", side_effect=AssertionError("history reload")), mock.patch.object(self.store.index, "_rebuild_merged_session", side_effect=AssertionError("session replay")):
            self.sync()
        self.assertTrue(self.store.token_ledger_path.read_bytes().startswith(before))
        rows = monitor_token_ledger.load_token_ledger(self.store.token_ledger_path)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[-1]["tokens"]["inputTokens"], 50)
        self.assertEqual(self.store.index.ledger_cost(self.store.token_ledger_path), monitor_token_ledger.token_cost_snapshot(monitor_token_ledger.token_sessions_from_ledger(rows)))

    def test_crash_after_append_replays_without_duplicate(self):
        append = monitor_token_ledger.append_token_ledger

        def interrupted(path, rows):
            append(path, rows)
            raise RuntimeError("interrupted after durable append")

        with mock.patch.object(monitor_token_ledger, "append_token_ledger", side_effect=interrupted), self.assertRaises(RuntimeError):
            self.sync()
        restarted = UsageDataStore(self.store.quota_path, self.store.token_ledger_path, "machine", lambda _: "account", threading.RLock())
        self.sync(restarted)
        self.assertEqual(len(monitor_token_ledger.load_token_ledger(self.store.token_ledger_path)), 1)

    def test_replacement_reconciles_and_missing_source_preserves_ledger(self):
        self.sync()
        self.write([
            {"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "session"}},
            {"timestamp": "2030-01-01T00:00:01Z", "type": "turn_context", "payload": {"model": "gpt-5.5"}},
            self.event(1, 200),
        ])
        self.sync()
        self.assertEqual(monitor_token_ledger.load_token_ledger(self.store.token_ledger_path)[0]["tokens"]["inputTokens"], 200)
        self.log.unlink()
        with self.store.index.transaction() as db:
            self.store.index.put(db, f"discovery:{str(self.root.resolve())}", 0)
        self.sync()
        self.assertEqual(len(monitor_token_ledger.load_token_ledger(self.store.token_ledger_path)), 1)

    def test_quota_tailing_and_no_change_publication_avoid_history_hashes(self):
        row = {"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "slot", "windows": {"5h": {"usedPercent": 1}}}
        self.store.quota_path.write_text(json.dumps(row) + "\n", encoding="utf-8")
        self.store.initialize_v4_index(1893456300)
        with self.store.index.transaction() as db:
            claims = dict(db.execute("SELECT day,generation FROM processing_dirty_days"))
        self.store.acknowledge_publication(claims)
        with mock.patch.object(self.store, "_v4_index_day", side_effect=AssertionError("historical hashing")):
            content, claims = self.store.v4_publication_changes(1893456400, {})
            self.assertEqual((content, claims), ({}, {}))
        row["checkedAt"] = "2030-01-01T00:01:00Z"
        with self.store.quota_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row) + "\n")
        before = self.store.index.metrics["fullImports"]
        self.store.ingest_local()
        self.assertEqual(self.store.index.metrics["fullImports"], before)
        self.assertEqual(len(self.store.index.rows(self.store.quota_path, "quota")), 2)

    def test_projector_matches_reference_and_monotonic_appends_do_not_replay(self):
        import monitor_dashboard
        from monitor_projection import DashboardProjector
        projector = DashboardProjector(self.store)
        accounts = {"activeAccountId": "slot", "items": [{"id": "slot", "label": "Account", "usageAccountId": "account"}]}
        self.sync()
        initial_rebuilds = None
        for minute in range(5):
            row = {"checkedAt": f"2030-01-01T00:{minute:02}:00Z", "accountSlotId": "slot", "accountLabel": "Account",
                "windows": {"5h": {"usedPercent": minute, "plan": "plus"}, "7d": {"usedPercent": minute, "plan": "plus"}}}
            with self.store.quota_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row) + "\n")
            now = 1893456000 + minute * 60
            projector.refresh(accounts, now)
            for view in ("local", "merged"):
                expected = monitor_dashboard.dashboard_transfer_view(monitor_dashboard._dashboard_display_data(*self.store.datasets(view), accounts, now))
                self.assertEqual(projector.snapshot(view), expected)
            if initial_rebuilds is None:
                initial_rebuilds = projector.metrics["accountRebuilds"]
            self.assertEqual(projector.metrics["accountRebuilds"], initial_rebuilds)
        self.assertEqual(projector.metrics["quotaFastAppends"], 8)
        self.assertIsNone(projector.refresh(accounts, now))

    def test_quota_rejections_missing_windows_resets_and_late_samples_match_reference(self):
        import monitor_dashboard
        from monitor_projection import DashboardProjector
        from datetime import datetime, timezone
        projector = DashboardProjector(self.store)
        accounts = {"activeAccountId": "slot", "items": [{"id": "slot", "label": "Account", "usageAccountId": "account"}]}
        values = [12, 13, 12.8, 12.9, 13, 14, 60, None, 15, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 100, 16]
        for minute, value in enumerate(values):
            now = 1893456000 + minute * 60
            row = {"checkedAt": datetime.fromtimestamp(now, timezone.utc).isoformat(), "accountSlotId": "slot", "accountLabel": "Account",
                "windows": {"5h": {"usedPercent": value, "resetAt": "2030-01-01T05:00:00Z", "plan": "plus"}, "7d": {"usedPercent": value, "plan": "plus"}}}
            if minute == 20:
                row["checkedAt"] = "2030-01-01T00:01:30Z"
            with self.store.quota_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row) + "\n")
            projector.refresh(accounts, now)
            for view in ("local", "merged"):
                self.assertEqual(projector.snapshot(view), monitor_dashboard.dashboard_transfer_view(monitor_dashboard._dashboard_display_data(*self.store.datasets(view), accounts, now)), (minute, view))

    def test_prefix_rewrite_followed_by_append_reconciles(self):
        self.sync()
        self.log.write_text(self.log.read_text().replace('"input_tokens": 100', '"input_tokens": 200'), encoding="utf-8")
        self.write([self.event(2, 250)], append=True)
        self.sync()
        rows = monitor_token_ledger.load_token_ledger(self.store.token_ledger_path)
        self.assertEqual(sum(row["tokens"]["inputTokens"] for row in rows), 250)

    def test_audit_detects_middle_rewrite_hidden_by_append(self):
        self.sync()
        self.store.quota_path.write_text("".join(json.dumps({"checkedAt": f"2030-01-01T00:{index:02}:00Z", "accountSlotId": "slot", "windows": {"5h": {"usedPercent": 1}}, "padding": "x" * 500}) + "\n" for index in range(40)), encoding="utf-8")
        self.store.ingest_local()
        self.store.quota_path.write_text(self.store.quota_path.read_text().replace('00:20:00Z', '00:20:01Z') + json.dumps({"checkedAt": "2030-01-01T00:41:00Z", "accountSlotId": "slot", "windows": {"5h": {"usedPercent": 1}}}) + "\n", encoding="utf-8")
        self.store.ingest_local()
        self.store.index.audit(force=True)
        self.store.ingest_local()
        self.assertTrue(any(row["checkedAt"] == "2030-01-01T00:20:01Z" for row in self.store.index.rows(self.store.quota_path, "quota")))

    def test_repair_preserves_collection_periods_and_ledger(self):
        self.sync()
        self.store.initialize_v4_index(1893456000)
        with self.store.index.transaction() as db:
            before = list(db.execute("SELECT record_key,period,content_hash FROM local_records ORDER BY record_key"))
        ledger = self.store.token_ledger_path.read_bytes()
        self.store.index.repair()
        self.sync()
        self.store.initialize_v4_index(1893556000)
        with self.store.index.transaction() as db:
            self.assertEqual(list(db.execute("SELECT record_key,period,content_hash FROM local_records ORDER BY record_key")), before)
        self.assertEqual(self.store.token_ledger_path.read_bytes(), ledger)

    def test_peer_day_removal_removes_merged_effective_ledger(self):
        from monitor_usage_sync import v4_record_envelope, record_key, v4_logical_hash
        self.store.account_mapper = lambda *_: ("slot", "Account")
        self.sync()
        row = monitor_token_ledger.load_token_ledger(self.store.token_ledger_path)[0]
        row = row | {"eventId": "remote-event", "sessionId": "remote-session", "sync": {"version": 1, "originMachineId": "remote", "accountId": "account"}}
        record = {"kind": "tokenLedger", "row": row}
        entry = v4_record_envelope("remote", record_key("tokenLedger", row), record, "2030-01-01T00:00Z")
        self.store.v4_apply_day("remote", {"day": "2030-01-01", "parts": {"bulk": "hash"}, "logicalHash": v4_logical_hash([entry])}, {"bulk": [entry]})
        with self.store.index.transaction() as db:
            self.assertIsNotNone(db.execute("SELECT 1 FROM processing_effective WHERE session='remote-session'").fetchone())
        self.store.v4_remove_days("remote", {"2030-01-01"})
        with self.store.index.transaction() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM processing_effective WHERE session='remote-session'").fetchone())
            self.assertIsNone(db.execute("SELECT 1 FROM processing_ledger_sessions WHERE source LIKE 'view:merged:%' AND session='remote-session'").fetchone())

    def test_os_watcher_stops_and_delivers_new_session_changes(self):
        watcher = monitor_processing.SourceWatcher(self.root)
        self.addCleanup(watcher.stop)
        self.store.index.source_watcher = watcher
        self.sync()
        self.write([self.event(2, 150)], append=True)
        import time
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with watcher.lock:
                if str(self.log.resolve()) in watcher.pending:
                    break
            time.sleep(.01)
        self.sync()
        self.assertEqual(self.store.index.ledger_cost(self.store.token_ledger_path), monitor_token_ledger.token_cost_snapshot(
            monitor_token_ledger.token_sessions_from_ledger(monitor_token_ledger.load_token_ledger(self.store.token_ledger_path))))
        self.assertEqual(len(monitor_token_ledger.load_token_ledger(self.store.token_ledger_path)), 2)

    def test_streamed_snapshot_matches_regular_snapshot(self):
        from monitor_projection import DashboardProjector
        from monitor_streaming import json_chunks
        self.sync()
        projector = DashboardProjector(self.store)
        projector.refresh({"activeAccountId": "slot", "items": [{"id": "slot", "label": "Account", "usageAccountId": "account"}]}, 1893456000)
        for view in ("local", "merged"):
            streamed = projector.snapshot(view, True)
            try:
                self.assertEqual(json.loads(b"".join(json_chunks(streamed))), projector.snapshot(view))
            finally:
                streamed["quotaPoints"].close()
                streamed["tokenSessions"].close()
                for rows in streamed["costUsage"].values():
                    rows.close()

    def test_streamed_snapshot_is_consistent_while_new_projection_commits(self):
        from monitor_projection import DashboardProjector
        from monitor_streaming import json_chunks, close_records
        self.sync()
        projector = DashboardProjector(self.store)
        accounts = {"activeAccountId": "slot", "items": [{"id": "slot", "label": "Account", "usageAccountId": "account"}]}
        projector.refresh(accounts, 1893456000)
        expected = projector.snapshot("local")
        snapshot = projector.snapshot("local", True)
        try:
            self.store.quota_path.write_text(json.dumps({"checkedAt": "2030-01-01T00:01:00Z", "accountSlotId": "slot", "windows": {"5h": {"usedPercent": 1}}}) + "\n", encoding="utf-8")
            projector.refresh(accounts, 1893456060)
            self.assertEqual(json.loads(b"".join(json_chunks(snapshot))), expected)
            self.assertEqual(len(projector.snapshot("local")["quotaPoints"]), 1)
        finally:
            close_records(snapshot)

    def test_peer_bulk_append_imports_only_new_rows_without_session_replay(self):
        from monitor_usage_sync import v4_record_envelope, record_key, v4_logical_hash
        self.store.account_mapper = lambda *_: ("slot", "Account")
        self.sync()
        row = monitor_token_ledger.load_token_ledger(self.store.token_ledger_path)[0] | {"eventId": "remote-one", "sessionId": "remote-session",
            "sync": {"version": 1, "originMachineId": "remote", "accountId": "account"}}
        def envelope(row):
            return v4_record_envelope("remote", record_key("tokenLedger", row), {"kind": "tokenLedger", "row": row}, "2030-01-01T00:00Z")
        entries = [envelope(row)]
        self.store.v4_apply_day("remote", {"day": "2030-01-01", "parts": {"bulk": "one"}, "logicalHash": v4_logical_hash(entries)}, {"bulk": entries})
        before = self.store.index.metrics["recordsImported"]
        entries.append(envelope(row | {"eventId": "remote-two", "occurredAt": "2030-01-01T00:02:00Z"}))
        with mock.patch.object(self.store.index, "_rebuild_merged_session", side_effect=AssertionError("session replay")):
            self.store.v4_apply_day("remote", {"day": "2030-01-01", "parts": {"bulk": "two"}, "logicalHash": v4_logical_hash(entries)}, {"bulk": entries})
        self.assertEqual(self.store.index.metrics["recordsImported"] - before, 1)


if __name__ == "__main__":
    unittest.main()
