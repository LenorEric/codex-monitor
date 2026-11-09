import copy
import gzip
import io
import json
import random
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from monitor_canonical import CanonicalDataStore, CanonicalIndex
from monitor_canonical_store import CanonicalRepository
from monitor_delivery import DashboardDelivery
from monitor_delivery import Totals, encoded
from monitor_interpretation import chart_rows, usage_runs, valid_intervals
from monitor_live import send_bulk
from monitor_sampling import quota_point
from test_support_delivery import released_delivery
from monitor_common import empty_token_totals, empty_cost_totals


def make_test_ledger(event="event", cost=1):
    return {
        "schemaVersion": 2,
        "recordType": "usage",
        "eventId": event,
        "sessionId": "session",
        "occurredAt": "2030-01-01T00:00:00Z",
        "rawModel": "gpt-5",
        "billingModel": "gpt-5",
        "serviceTier": "default",
        "accountSlotId": "slot",
        "accountLabel": "A",
        "tokens": empty_token_totals(),
        "cost": empty_cost_totals() | {"totalCostUsd": cost},
    }


class FinalDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="codex-final-test-")
        self.root = Path(self.directory.name)
        self.addCleanup(self.directory.cleanup)
        self.store = CanonicalDataStore(self.root / "quota.jsonl", self.root / "token.jsonl", "test", lambda _: "v3:account", threading.RLock())
        self.addCleanup(self.store.index.close)
        self.delivery = DashboardDelivery(self.store.index)
        self.addCleanup(self.delivery.close)

    @staticmethod
    def quota(first, value, window="5h", last=None, plan="plus", factor=1, reset=100000, key=None):
        return {"checkedAt": first, "quotaRunVersion": 1, "accountSlotId": "slot", "accountLabel": "Test",
            "sync": {"version": 1, "originMachineId": "test", "accountId": "v3:account", "recordId": key or f"{first}:{value}:{window}"},
            "windows": {window: {"firstAt": first, "lastAt": first if last is None else last, "usedPercent": value, "multiplier": factor, "plan": plan, "resetAt": reset}}}

    def put_quota(self, first, value, window="5h", **kwargs):
        with self.store.index.transaction() as db:
            row = self.quota(first, value, window, **kwargs)
            self.store._save(db, "local", "quota", row)
            self.store._save(db, "merged", "quota", row)
        return row

    def token(self, key, stamp, cost=1):
        row = make_test_ledger(event=key, cost=cost)
        row["occurredAt"] = datetime.fromtimestamp(stamp, timezone.utc).isoformat()
        row["tokens"]["freshInputTokens"] = 10
        self.store.record_ledger([row])
        return row

    def bulk(self):
        return json.loads(self.delivery.bulk(False))

    def view(self, view="local"):
        return self.bulk()["accounts"][0][view]

    def reference(self, view="local", window="5h"):
        with self.store.index.transaction() as db:
            quotas = CanonicalRepository.rows(db, view, "quota", "v3:account")
            from monitor_delivery import token_projection
            tokens = list(token_projection(db, view, "v3:account"))
        return chart_rows(tokens, valid_intervals(usage_runs([quota_point(row) for row in quotas], window)))

    def assert_charts(self):
        for view in ("local", "merged"):
            for window in ("5h", "7d"):
                actual = sorted(self.view(view)["chart:" + window], key=lambda row: row[1:3])
                expected = sorted(self.reference(view, window), key=lambda row: row[1:3])
                models = self.bulk()["models"]
                for row in actual:
                    for entry in row[6]:
                        entry[0] = models[entry[0]]
                self.assertEqual([row[1:] for row in actual], [row[1:] for row in expected])

    def test_frozen_groups_and_live_snapshot_restart_equivalence(self):
        self.put_quota(1000, 10)
        self.put_quota(2000, 20)
        for index, stamp in enumerate((1100, 1300, 1600, 1800)):
            self.token(str(index), stamp)
        self.delivery.prepare()
        baseline = self.bulk()
        before = copy.deepcopy(self.view()["tokens"])
        self.token("new", 1900)
        self.delivery.prepare()
        appended = copy.deepcopy(self.view()["tokens"])
        self.assertEqual([row for row in appended if row[0] in {r[0] for r in before}], before)
        self.put_quota(1500, 15)
        epoch = self.delivery.head[0]
        self.delivery.cloud_pending = True
        self.delivery.prepare()
        self.assertEqual(self.view()["tokens"], appended)
        self.assertEqual(self.delivery.head[0], epoch)
        self.assert_charts()
        maps = {channel: {row[0]: row for row in baseline["accounts"][0]["local"][channel]} for channel in ("tokens", "usage:5h", "chart:5h")}
        for channel in ("tokens", "chart:5h"):
            for row in maps[channel].values():
                for model in row[2 if channel == "tokens" else 6]:
                    model[0] = baseline["models"][model[0]]
        cursor = dict(baseline["cursors"])
        while True:
            packets = self.delivery.replay(baseline["epoch"], cursor, page_bytes=400)
            if not packets:
                break
            for packet in packets:
                cursor[packet["stream"]] = packet["seq"]
                if packet["stream"] != "local" or packet["channel"] not in maps:
                    continue
                target, data = maps[packet["channel"]], packet["data"]
                if packet["channel"] == "tokens":
                    for row in data:
                        target[row[0]] = row
                else:
                    for key in data["remove"]:
                        target.pop(key, None)
                    for row in data["set"]:
                        target[row[0]] = row
        for channel, target in maps.items():
            expected = copy.deepcopy(self.view()[channel])
            if channel == "tokens" or channel.startswith("chart:"):
                for row in expected:
                    for model in row[2 if channel == "tokens" else 6]:
                        model[0] = self.bulk()["models"][model[0]]
            self.assertEqual(target, {row[0]: row for row in expected})
        restarted = DashboardDelivery(self.store.index)
        self.addCleanup(restarted.close)
        self.assertEqual(json.loads(restarted.bulk(False)), self.bulk())

    def test_late_quota_updates_chart_without_reading_or_replacing_old_tokens(self):
        for stamp in (1100, 1500, 1900):
            self.token(str(stamp), stamp)
        self.delivery.prepare()
        tokens = self.view()["tokens"]
        self.put_quota(1000, 10)
        self.put_quota(2000, 20)
        self.delivery.prepare()
        self.assertEqual(self.view()["tokens"], tokens)
        self.assert_charts()
        self.assertEqual(sum(model[1] for row in self.view()["chart:5h"] for model in row[6]), 300000000)

    def test_full_rebuild_clears_delivery_and_retries_after_failure(self):
        self.put_quota(1000, 0, reset=10000)
        self.put_quota(1100, 1, reset=20000)
        self.token("event", 1050)
        self.delivery.prepare()
        before = self.bulk()
        with self.store.index.transaction() as db:
            canonical = db.execute("SELECT COUNT(*) FROM canonical_quota").fetchone()[0], db.execute("SELECT COUNT(*) FROM canonical_tokens").fetchone()[0]
            db.execute("INSERT INTO delivery_items VALUES('local','v3:account','tokens','stale',0,0,0,'[\"stale\",0,[]]')")
            db.execute("INSERT INTO delivery_items VALUES('local','v3:account','legacy','legacy',0,0,0,'[0,0,[]]')")
        with mock.patch.object(self.delivery, "_publish", side_effect=RuntimeError("interrupted")):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                self.delivery.rebuild()
        with self.store.index.transaction() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM delivery_items WHERE id='stale'").fetchone())
            self.assertIsNotNone(db.execute("SELECT 1 FROM delivery_items WHERE id='legacy'").fetchone())
        self.delivery.rebuild()
        after = self.bulk()
        self.assertNotEqual(after["epoch"], before["epoch"])
        self.assertIsNone(self.delivery.replay(before["epoch"], before["cursors"]))
        self.assertNotIn("stale", [row[0] for row in self.view()["tokens"]])
        self.assertEqual([row[5] for row in self.view()["usage:5h"]], [1, 0])
        self.assert_charts()
        with self.store.index.transaction() as db:
            self.assertEqual((db.execute("SELECT COUNT(*) FROM canonical_quota").fetchone()[0], db.execute("SELECT COUNT(*) FROM canonical_tokens").fetchone()[0]), canonical)

    def test_random_insert_delete_and_reset_neighborhoods_match_reference(self):
        random.seed(342)
        for index in range(30):
            self.token(f"token-{index}", 900 + index * 130, cost=(index % 5) / 10)
        positions = [1000 + index * 150 for index in range(20)]
        random.shuffle(positions)
        for index, stamp in enumerate(positions):
            row = self.put_quota(stamp, (stamp - 1000) // 150 + 10, "5h" if index % 2 else "7d", reset=100000 if index % 4 else 200000)
            self.delivery.prepare()
            self.assert_charts()
        with self.store.index.transaction() as db:
            db.execute("DELETE FROM canonical_quota WHERE range_first BETWEEN 1600 AND 2300")
        self.delivery.prepare()
        self.assert_charts()

    def test_zero_cost_ties_600_seconds_and_dense_same_timestamp(self):
        for stamp, value in ((1000, 10), (1200, 20), (2000, 30), (2200, 40)):
            self.put_quota(stamp, value)
        for index, stamp in enumerate((399, 400, 1200, 1600, 2200, 2800, 2801, 1600.5)):
            self.token(str(index), stamp, cost=0 if index % 2 else .12345678)
        self.delivery.prepare()
        self.assert_charts()

    def test_unchanged_and_tail_extension_do_not_scan_historical_tokens(self):
        self.put_quota(1000, 10, last=2000)
        self.token("old", 1500)
        self.delivery.prepare()
        before = self.delivery.metrics["sourceTokenRowsRead"]
        usage = self.view()["usage:5h"][0][0]
        with self.store.index.transaction() as db:
            db.execute("UPDATE canonical_quota SET last=3000,range_last=3000,reset_at=100001 WHERE window='5h'")
        self.delivery.prepare()
        self.assertEqual(self.delivery.metrics["sourceTokenRowsRead"], before)
        self.assertEqual(self.view()["usage:5h"][0][0], usage)
        self.assertFalse(self.delivery.prepare())

    def test_sparse_unrelated_history_does_not_expand_local_scan(self):
        for stamp, value in ((1000, 10), (2000, 20), (3000, 30)):
            self.put_quota(stamp, value)
        self.token("near", 1800)
        self.delivery.prepare()
        self.put_quota(1500, 15)
        self.delivery.prepare()
        initial = self.delivery.metrics["sourceTokenRowsRead"]
        for index in range(200):
            self.token(f"remote-{index}", 100000000 + index * 1000000)
        self.delivery.prepare()
        before = self.delivery.metrics["sourceTokenRowsRead"]
        self.put_quota(1700, 17)
        self.delivery.prepare()
        self.assertLessEqual(self.delivery.metrics["sourceTokenRowsRead"] - before, initial + 5)

    def test_processing_failure_and_publication_failure_are_retryable(self):
        self.token("a", 1000)
        original = self.delivery._publish
        def fail(path, cutoff, state, now):
            with mock.patch.object(self.delivery, "_packet", side_effect=RuntimeError("crash while publishing")):
                original(path, cutoff, state, now)
        with mock.patch.object(self.delivery, "_publish", side_effect=fail), self.assertRaises(RuntimeError):
            self.delivery.prepare()
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM delivery_items WHERE channel='tokens'").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM delivery_pending WHERE kind='tokenLedger'").fetchone()[0], 2)
        self.delivery.prepare()
        self.assertEqual(len(self.view()["tokens"]), 1)
        self.assertFalse(self.delivery.prepare())

    def test_arrival_during_computation_remains_pending(self):
        self.token("first", 1000)
        original = self.delivery._publish
        def publish(*args):
            self.token("concurrent", 1100)
            original(*args)
        with mock.patch.object(self.delivery, "_publish", side_effect=publish):
            self.delivery.prepare()
        self.assertEqual(len(self.view()["tokens"]), 1)
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM delivery_pending WHERE kind='tokenLedger'").fetchone()[0], 2)
        self.delivery.prepare()
        self.assertEqual(len(self.view()["tokens"]), 2)

    def test_committed_publication_recovers_without_duplicate_append(self):
        self.token("first", 1000)
        self.delivery.prepare()
        self.bulk()  # Cache the old delivery version.
        self.token("second", 1100)
        original = self.delivery._publish
        def interrupted(*args):
            original(*args)
            raise RuntimeError("publication committed before acknowledgement")
        with mock.patch.object(self.delivery, "_publish", side_effect=interrupted), self.assertRaises(RuntimeError):
            self.delivery.prepare()
        self.assertTrue(self.delivery.prepare())
        self.assertEqual(len(self.view()["tokens"]), 2)
        self.assertFalse(self.delivery.prepare())

    def test_cloud_import_preserves_frozen_groups_and_local_view(self):
        self.put_quota(1000, 10)
        self.put_quota(2000, 20)
        self.token("first", 1500)
        self.delivery.prepare()
        before = copy.deepcopy(self.view())
        epoch = self.delivery.head[0]
        remote = self.quota(1600, 16)
        with self.store.index.transaction() as db:
            self.store._save(db, "merged", "quota", remote)
        self.delivery.prepare()
        self.assertEqual(self.delivery.head[0], epoch)
        self.assertEqual(self.view(), before)
        self.assertEqual(self.view("merged")["tokens"], before["tokens"])
        self.assert_charts()

    def test_actual_http_and_websocket_use_final_rows(self):
        import http.client
        import socket
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from wsproto import ConnectionType, WSConnection
        from wsproto.events import AcceptConnection, CloseConnection, Request, TextMessage
        from monitor_live import serve_live
        self.token("first", 1000)
        self.delivery.prepare()
        delivery = self.delivery
        running = threading.Event()
        running.set()
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.endswith("/live"):
                    serve_live(self, delivery, running.is_set)
                else:
                    send_bulk(self, delivery)
            def log_message(self, *args): pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        http = http.client.HTTPConnection(*server.server_address, timeout=5)
        peer = None
        try:
            http.request("GET", "/api/dashboard", headers={"Accept-Encoding": "gzip"})
            response = http.getresponse()
            self.assertEqual(response.status, 200)
            baseline = json.loads(gzip.decompress(response.read()))
            ws = WSConnection(ConnectionType.CLIENT)
            peer = socket.create_connection(server.server_address, timeout=5)
            peer.sendall(ws.send(Request(host="localhost", target="/api/dashboard/live")))
            ws.receive_data(peer.recv(65536))
            self.assertTrue(any(isinstance(event, AcceptConnection) for event in ws.events()))
            peer.sendall(ws.send(TextMessage(data=json.dumps({"type": "subscribe", "epoch": baseline["epoch"], "cursors": baseline["cursors"]}))))
            self.token("second", 1100)
            self.delivery.prepare()
            packets, fragments = [], ""
            while not any(packet.get("stream") == "merged" and packet.get("channel") == "tokens" for packet in packets):
                ws.receive_data(peer.recv(65536))
                for event in ws.events():
                    if isinstance(event, TextMessage):
                        fragments += event.data
                        if event.message_finished:
                            packets.append(json.loads(fragments))
                            fragments = ""
            for view in ("local", "merged"):
                self.assertEqual(sum(len(packet["data"]) for packet in packets if packet.get("stream") == view and packet.get("channel") == "tokens"), 1)
            peer.sendall(ws.send(CloseConnection(code=1000)))
        finally:
            running.clear()
            if peer is not None:
                peer.close()
            http.close()
            server.shutdown()
            server.server_close()
            thread.join(5)
            self.assertFalse(thread.is_alive())

    def test_snapshots_are_streamed_shared_and_old_readers_survive_new_head(self):
        self.token("first", 1000)
        self.delivery.prepare()
        with self.delivery.snapshot_file() as (old, size):
            saved = old.read()
            self.assertEqual(size, len(saved))
            with self.delivery.snapshot_file() as (other, _):
                self.assertEqual(other.name, old.name)
                self.assertEqual(other.read(), saved)
            self.token("second", 1100)
            self.delivery.prepare()
            old.seek(0)
            self.assertEqual(old.read(), saved)
        self.assertEqual(gzip.decompress(self.delivery.bulk()), self.delivery.bulk(False))
        class Handler:
            headers = {"Accept-Encoding": "gzip"}
            wfile = io.BytesIO()
            def send_response(self, value): self.status = value
            def send_header(self, key, value): pass
            def end_headers(self): pass
        handler = Handler()
        with mock.patch.object(self.delivery, "bulk", side_effect=AssertionError("HTTP materialized full bytes")):
            self.assertTrue(send_bulk(handler, self.delivery))
        self.assertEqual(json.loads(gzip.decompress(handler.wfile.getvalue())), self.bulk())

    def test_no_preparation_tables_or_durable_snapshot(self):
        self.token("a", 1000)
        self.delivery.prepare()
        with self.store.index.transaction() as db:
            names = {name for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue({"delivery_items", "delivery_pending", "delivery_models"}.issubset(names))
            self.assertFalse({"delivery_snapshot", "delivery_token_refs", "delivery_token_payloads", "delivery_run_refs", "delivery_charts"} & names)

    def test_snapshot_cannot_read_canonical_and_replay_is_paged(self):
        for index in range(30):
            self.token(str(index), 1000 + index)
        self.delivery.prepare()
        original = self.delivery._connect
        def connect():
            db = original()
            db.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and table.startswith('canonical_') else sqlite3.SQLITE_OK)
            return db
        with mock.patch.object(self.delivery, "_connect", side_effect=connect):
            with self.delivery.snapshot_file() as (file, size):
                self.assertEqual(len(file.read()), size)
        cursor = dict.fromkeys(self.delivery.head[1], 0)
        packets = self.delivery.replay(self.delivery.head[0], cursor, page_bytes=500)
        self.assertGreater(len(packets), 0)
        self.assertLess(len(packets), sum(self.delivery.head[1].values()))
        with self.store.index.transaction() as db:
            db.execute("DELETE FROM delivery_journal WHERE stream='local' AND seq=1")
        self.assertIsNone(self.delivery.replay(self.delivery.head[0], cursor))

    def test_migration_recalculation_removes_orphans_without_discarding_tokens(self):
        from monitor_delivery import request_attribution_rebuild
        self.put_quota(1000, 10)
        self.put_quota(2000, 20)
        self.token("a", 1500)
        self.delivery.prepare()
        before = self.view()["tokens"]
        with self.store.index.transaction() as db:
            db.execute("DELETE FROM canonical_quota")
            db.execute("DELETE FROM delivery_pending")  # Simulate lost derived progress.
        with self.store.index.transaction() as db:
            request_attribution_rebuild(db)
        self.delivery.prepare()
        self.assertEqual(self.view()["tokens"], before)
        self.assertEqual(self.view()["usage:5h"], [])
        self.assertEqual(self.view()["chart:5h"], [])

    def test_journal_retention_bounds_large_publications(self):
        for index in range(20):
            self.token(str(index), 1000 + index)
        with mock.patch('monitor_delivery.JOURNAL_BYTES', 1200):
            self.delivery.prepare()
        with self.store.index.transaction() as db:
            self.assertLessEqual(db.execute("SELECT SUM(length(body)) FROM delivery_journal").fetchone()[0], 1200)
        self.assertEqual(len(self.view()["tokens"]), 20)
        self.assertIsNone(self.delivery.replay(self.delivery.head[0], {"local": 0, "merged": 0}))

    def test_metadata_changes_during_publication_remain_pending(self):
        self.token("a", 1500)
        self.delivery.prepare()
        tokens = self.view()["tokens"]
        self.store.rename_account("slot", "Before")
        original = self.delivery._publish
        def publish(*args):
            self.store.rename_account("slot", "After")
            original(*args)
        with mock.patch.object(self.delivery, "_publish", side_effect=publish):
            self.delivery.prepare()
        self.assertEqual(self.bulk()["accounts"][0]["label"], "Before")
        self.delivery.prepare()
        self.assertEqual(self.bulk()["accounts"][0]["label"], "After")
        self.assertEqual(self.view()["tokens"], tokens)

    def test_same_second_usage_keeps_ascending_percentage_order(self):
        self.put_quota(1000, 20)
        self.put_quota(1000, 10)
        self.delivery.prepare()
        self.assertEqual([row[3] for row in self.view()["usage:5h"]], [10, 20])

    def test_interval_seek_work_does_not_scale_with_unrelated_final_history(self):
        with closing(self.delivery._connect()) as db:
            db.execute("ATTACH ':memory:' AS stage")
            self.delivery._stage(db)
            def insert(start, count):
                db.executemany("INSERT INTO delivery_items VALUES('local','a','chart:5h',?,?,?,?,?)",
                    ((str(index), index * 1000, index * 1000, index * 1000 + 100, '[]') for index in range(start, start + count)))
            def steps():
                count = [0]
                def tick():
                    count[0] += 1
                    return 0
                db.set_progress_handler(tick, 1)
                try:
                    self.delivery.ordered_items(db, 'local', 'a', 'chart:5h', 'last>?', (0,), 'last,id', 1).fetchone()
                finally:
                    db.set_progress_handler(None, 0)
                return count[0]
            insert(1, 20)
            before = steps()
            insert(21, 2000)
            self.assertLess(steps(), before * 2 + 100)


class FinalDeliveryMigrationTests(unittest.TestCase):
    setUp = FinalDeliveryTests.setUp
    quota = staticmethod(FinalDeliveryTests.quota)
    put_quota = FinalDeliveryTests.put_quota
    token = FinalDeliveryTests.token

if __name__ == '__main__':
    unittest.main()
