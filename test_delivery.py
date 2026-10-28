
import gzip
import http.server
import json
import shutil
import unittest
import uuid
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from monitor_delivery import DashboardDelivery
from monitor_interpretation import PRO_CUTOVER, aggregate_tokens, chart_rows, connected, multiplier, nearest, usage_runs, valid_intervals, weighted
from monitor_processing import ProcessingIndex
from monitor_common import empty_cost_totals, empty_token_totals


def point(stamp, value, reset=100000, plan="plus", weekly=None):
    return {"timestamp": stamp, "fiveHour": {"continuous": value, "plan": plan, "resetAt": reset},
        "sevenDay": {"continuous": value if weekly is None else weekly, "plan": plan, "resetAt": reset}}


class InterpretationTests(unittest.TestCase):
    def test_multiplier_boundary(self):
        self.assertEqual(multiplier("pro", PRO_CUTOVER - 1), 20)
        self.assertEqual(multiplier("pro", PRO_CUTOVER), 10)
        self.assertEqual(multiplier("pro_lite", PRO_CUTOVER), 5)

    def test_only_consecutive_identical_values_and_resets_merge(self):
        rows = usage_runs([point(0, 10), point(90, 10), point(180, 11), point(270, 10), point(360, 10, 200000)], "5h")
        self.assertEqual([(r[1], r[2], r[3]) for r in rows], [(0, 90, 10), (180, 180, 11), (270, 270, 10), (360, 360, 10)])

    def test_invalid_samples_break_curves(self):
        rows = usage_runs([point(0, 10), point(90, None), point(180, 11)], "5h")
        self.assertEqual([row[5] for row in rows], [1, 1, 1])
        self.assertEqual(valid_intervals(rows), [])

    def test_continuity_boundaries(self):
        base = ["a", 0, 0, 10, 1]
        self.assertTrue(connected(base, ["b", 1800, 1800, 0, 1], "5h"))
        self.assertFalse(connected(base, ["b", 1801, 1801, 7, 1], "5h"))
        self.assertTrue(connected(base, ["b", 14400, 14400, 11, 1], "5h"))
        self.assertFalse(connected(base, ["b", 14401, 14401, 11, 1], "5h"))
        self.assertFalse(connected(base, ["b", 3600, 3600, 16, 1], "7d"))

    def test_union_of_window_boundaries(self):
        intervals = {"5h": [["a", 0, 3, 1, 1], ["b", 3, 7, 1, 1]], "7d": [["c", 2, 6, 1, 1]]}
        events = [[str(t), t, "m", "default", 1, 2, 3, 4, 10] for t in (.5, 1, 2.5, 4, 5, 6.5)]
        rows = aggregate_tokens(events, intervals)
        self.assertEqual(len(rows), 4)
        self.assertEqual(sum(model[-1] for row in rows for model in row[2]), 60)
        self.assertEqual(sum(model[2] for row in rows for model in row[2]), 6)

    def test_nearest_ties_choose_earlier_interval(self):
        intervals = [["a", 0, 10], ["b", 20, 30]]
        self.assertEqual(nearest(intervals, 15), 0)
        self.assertEqual(nearest(intervals, 16), 1)
        self.assertEqual(nearest(intervals, -100), 0)
        self.assertEqual(nearest(intervals, 100), 1)

    def test_flat_and_outside_costs_are_never_lost(self):
        events = [[str(t), t, "m", "default", 1, 0, 0, 0, 100] for t in (-10, 5, 15, 25, 50)]
        intervals = [["a", 0, 10, 1, 1], ["b", 20, 30, 2, 1]]
        rows = chart_rows(events, intervals)
        self.assertEqual(sum(m[1] for row in rows for m in row[6]), 500)
        self.assertEqual([row[6][0][1] for row in rows], [300, 200])
        self.assertEqual(len(aggregate_tokens(events, {"5h": [], "7d": []})), 5)

    def test_api_minute_groups_and_zero_cost_average(self):
        events = [[str(t), t, "m", "default", 1, 0, 0, 0, 0] for t in (1, 59, 60)]
        rows = aggregate_tokens(events, {}, api=True)
        self.assertEqual([row[1] for row in rows], [30, 60])
        self.assertEqual(weighted([["a", 10, 1], ["b", 20, 3]]), 18)


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path.cwd() / f".test-delivery-{uuid.uuid4().hex}"
        self.directory.mkdir()
        self.addCleanup(shutil.rmtree, self.directory)
        self.index = ProcessingIndex(self.directory / "index.sqlite3")
        self.delivery = DashboardDelivery(self.index)
        self.events = []
        self.projector = SimpleNamespace(_datasets=lambda *args: ([], self.events), store=SimpleNamespace(token_ledger_path=self.directory / "ledger"))

    def quota(self, stamp, value=10, view="local"):
        with self.index.transaction() as db:
            db.execute("INSERT OR REPLACE INTO processing_quota_points VALUES(?,?,?,?,?)", (view, "a", stamp, str(stamp), json.dumps(point(stamp, value))))

    def prepare(self, now=1000, **kwargs):
        self.delivery.prepare(now, self.projector, {"accounts": {"items": []}}, **kwargs)
        return json.loads(self.delivery.bulk(False))

    def test_bulk_has_both_views_and_no_protocol_number(self):
        self.quota(10);self.quota(20, view="merged")
        bulk = self.prepare()
        self.assertNotIn("protocolVersion", bulk)
        self.assertEqual(bulk["accounts"][0]["local"]["usage:5h"][0][1], 10)
        self.assertEqual(bulk["accounts"][0]["merged"]["usage:5h"][0][1], 20)
        self.assertEqual(gzip.decompress(self.delivery.bulk()), self.delivery.bulk(False))

    def test_independent_live_streams_and_restart_replay(self):
        self.quota(10);bulk = self.prepare()
        self.quota(100)
        self.prepare(1002)
        packets = self.delivery.replay(bulk["epoch"], bulk["cursors"])
        self.assertTrue(packets)
        self.assertEqual({row["stream"] for row in packets}, {"local"})
        restarted = DashboardDelivery(self.index)
        self.assertEqual(restarted.replay(bulk["epoch"], bulk["cursors"]), packets)

    def test_unchanged_cloud_fetch_does_not_reset_epoch(self):
        self.quota(10);bulk = self.prepare()
        self.delivery.cloud_pending = True
        self.assertEqual(self.prepare(1002)["epoch"], bulk["epoch"])
        self.delivery.cloud_pending = True
        self.quota(20, 11, "merged")
        self.assertNotEqual(self.prepare(1004)["epoch"], bulk["epoch"])
        self.assertIsNone(self.delivery.replay(bulk["epoch"], bulk["cursors"]))

    def test_journal_expiry_requests_bulk(self):
        self.quota(10);bulk = self.prepare()
        self.quota(100);self.prepare(1002)
        self.prepare(100000)
        self.assertIsNone(self.delivery.replay(bulk["epoch"], bulk["cursors"]))

    def test_idle_and_bulk_requests_do_not_reinterpret_history(self):
        self.quota(10);self.prepare()
        with mock.patch.object(self.delivery, "_account", side_effect=AssertionError("replay")):
            self.assertFalse(self.delivery.prepare(1002, self.projector))
            self.assertTrue(self.delivery.bulk())

    def test_v18_migration_rolls_back_derived_store_and_keeps_originals(self):
        import monitor_migrations as migrations
        self.quota(10);self.prepare()
        before = self.delivery.bulk(False)
        quota = self.directory / "quota.jsonl"
        quota.write_bytes(b'{"original":"retained"}\n')
        state, journal = self.directory / "contract.json", self.directory / "journal.json"
        state.write_text('{"dataContractVersion":17}')
        paths = {"processing_index": self.index.path, "dashboard_cache": self.directory / "cache.json", "quota_history": quota}
        def interrupted(paths):
            migrations._migrate_history_to_v18(paths)
            raise RuntimeError("interrupted")
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            migrations._run_data_migration(interrupted, paths, 18, state, journal)
        self.assertEqual(DashboardDelivery(self.index).bulk(False), before)
        self.assertEqual(quota.read_bytes(), b'{"original":"retained"}\n')

        migrations._run_data_migration(migrations._migrate_history_to_v18, paths, 18, state, journal)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 18)
        self.assertEqual(quota.read_bytes(), b'{"original":"retained"}\n')

    def test_cloud_signal_during_preparation_is_consumed_by_next_projection(self):
        from monitor_dashboard import UsageDashboardState
        state = UsageDashboardState.__new__(UsageDashboardState)
        state.running = True
        state.lock = threading.RLock()
        state._dashboard_signal_lock = threading.Lock()
        state._dashboard_cloud_pending = False
        state.dashboard_cache_event = threading.Event()
        state.dashboard_cache_wait_seconds = lambda: 0
        state.status_payload = lambda: {}
        state.projector = None
        state.usage_data = SimpleNamespace(index=SimpleNamespace(maintain_disk_space=lambda: None))
        observed = []
        def project():
            if not observed:
                state._signal_dashboard_cache("cloud")
        def prepare(**kwargs):
            observed.append(state.delivery.cloud_pending)
            state.delivery.cloud_pending = False
            state.running = len(observed) < 2
        state.delivery = SimpleNamespace(cloud_pending=False, prepare=prepare)
        state.refresh_dashboard_cache = project
        state.run_dashboard_cache_maintenance()
        self.assertEqual(observed, [False, True])

    def test_token_only_update_updates_closest_chart_and_preserves_totals(self):
        from monitor_usage_sync import record_key
        self.events = [{"schemaVersion": 2, "recordType": "usage", "eventId": "first", "sessionId": "s", "occurredAt": "2030-01-01T00:00:00Z",
            "rawModel": "gpt-5.5", "tokens": empty_token_totals() | {"freshInputTokens": 10}, "cost": empty_cost_totals() | {"totalCostUsd": 1},
            "accountSlotId": "slot", "accountLabel": "Account", "sync": {"version": 1, "originMachineId": "m", "accountId": "a"}}]
        self.quota(1893456000, 10);self.quota(1893456100, 11)
        bulk = self.prepare()
        event = self.events[0] | {"eventId": "new", "occurredAt": "2030-01-01T01:00:00Z"}
        with self.index.transaction() as db:
            self.index.fact(db, str(self.projector.store.token_ledger_path.resolve()), 0, "tokenLedger", event)
            db.execute("INSERT INTO delivery_work VALUES('local','a','tokenLedger',?)", (record_key("tokenLedger", event),))
            db.execute("INSERT OR IGNORE INTO delivery_dirty VALUES('local','a')")
        self.prepare(1002)
        packets = self.delivery.replay(bulk["epoch"], bulk["cursors"])
        self.assertEqual({row["channel"] for row in packets}, {"tokens", "chart:5h", "chart:7d"})
        with self.index.transaction() as db:
            self.assertEqual(db.execute("SELECT SUM(cost) FROM delivery_tokens WHERE view='local'").fetchone()[0], 200000000)
        # New spending outside the increasing interval attaches to the preceding interval.
        charts = [row for row in packets if row["channel"] == "chart:5h"][0]["data"]["set"]
        self.assertEqual(charts[0][6][0][1], 200000000)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_real_http_bulk_and_websocket_replay(self):
        from monitor_live import send_bulk, serve_live
        self.quota(10);self.prepare()
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass
            def do_GET(self):
                if self.path.endswith("/live"):
                    serve_live(self, owner.delivery)
                else:
                    send_bulk(self, owner.delivery)
            def do_POST(self):
                owner.quota(100, 11)
                owner.prepare(1002)
                self.send_response(200);self.send_header("Content-Length", "0");self.end_headers()
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        script = r'''
const http=require('http'),zlib=require('zlib'),WebSocket=require('ws'),url=process.argv[1];
const timeout=setTimeout(()=>{console.error('timeout');process.exit(1)},5000);
http.get(url+'/api/dashboard',{headers:{'Accept-Encoding':'gzip'}},response=>{const chunks=[];response.on('data',v=>chunks.push(v));response.on('end',()=>{
const bulk=JSON.parse(zlib.gunzipSync(Buffer.concat(chunks))),ws=new WebSocket(url.replace('http:','ws:')+'/api/dashboard/live');
const packets=[];ws.on('open',()=>{ws.send(JSON.stringify({type:'subscribe',epoch:bulk.epoch,cursors:bulk.cursors}));const r=http.request(url+'/advance',{method:'POST'},v=>v.resume());r.end()});
ws.on('message',data=>{packets.push(JSON.parse(data));if(packets.some(p=>p.channel==='chart:7d')){console.log(JSON.stringify(packets));clearTimeout(timeout);ws.close()}});
ws.on('error',error=>{console.error(error);process.exit(1)});
})});
'''
        try:
            result = subprocess.run(["node", "-e", script, f"http://127.0.0.1:{server.server_port}"], capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            packets = json.loads(result.stdout)
            self.assertTrue(any(row["channel"] == "usage:5h" for row in packets))
            self.assertTrue(all(row["stream"] == "local" for row in packets))
        finally:
            server.shutdown();server.server_close();thread.join()


if __name__ == "__main__":
    unittest.main()
