
"""Measure bulk/live delivery; --source-db uses a consistent read-only source transaction."""

import argparse
import gzip
import http.server
import json
import shutil
import subprocess
import threading
import time
import urllib.request
import uuid
from collections import defaultdict
from contextlib import closing
from pathlib import Path

import monitor_sample_store as samples
from monitor_delivery import DashboardDelivery, encoded
from monitor_interpretation import token_row
from monitor_live import send_bulk, serve_live
from monitor_processing import ProcessingIndex


def read(url):
    start = time.perf_counter()
    with urllib.request.urlopen(urllib.request.Request(url, headers={"Accept-Encoding": "gzip"}), timeout=60) as response:
        body = response.read()
        compressed = response.headers.get("Content-Encoding") == "gzip"
    elapsed = (time.perf_counter() - start) * 1000
    decoded = gzip.decompress(body) if compressed else body
    return json.loads(decoded), body, elapsed


def keyed(bulk):
    """Equivalent expanded row objects for an apples-to-apples gzip comparison."""
    result = json.loads(json.dumps(bulk))
    for account in result["accounts"]:
        for view in ("local", "merged"):
            data = account[view]
            for window in ("5h", "7d"):
                data["usage:" + window] = [dict(zip(("id", "first", "last", "percentage", "multiplier", "flags"), row)) for row in data["usage:" + window]]
                data["chart:" + window] = [dict(zip(("id", "start", "end", "timestamp", "multiplier", "usage", "models"), row)) for row in data["chart:" + window]]
            data["tokens"] = [dict(zip(("id", "timestamp", "models"), row)) for row in data["tokens"]]
    return encoded(result)


def measure(url, directory, advance=False):
    bulk, body, elapsed = read(url + "/api/dashboard")
    path = directory / "bulk.json"
    path.write_bytes(encoded(bulk))
    node = r'''
const fs=require('fs'),vm=require('vm'),{performance}=require('perf_hooks');
const html=fs.readFileSync('dashboard.html','utf8'),script=html.match(/<script>([\s\S]*?)<\/script>/)[1].split('restoreSelections();setupControls();setupDataView();')[0];
const ctx=vm.createContext({performance,console,setTimeout,clearTimeout,addEventListener(){},document:{getElementById(){return {}}}});
vm.runInContext(script,ctx);vm.runInContext('showBulkView=()=>{};connectLive=()=>{};ensureControlPassword=()=>{};',ctx);
const start=performance.now();ctx.input=JSON.parse(fs.readFileSync(process.argv[1],'utf8'));
vm.runInContext('acceptBulk(input);for(const [account,views] of transportAccounts)for(const view of Object.values(views))accountFrame(account,view)',ctx);
console.log(JSON.stringify({decodeMs:performance.now()-start}));
'''
    decoded = json.loads(subprocess.run(["node", "-e", node, str(path)], check=True, capture_output=True, text=True).stdout)
    expanded = keyed(bulk)
    result = {"bulkBytes": len(body), "jsonBytes": len(encoded(bulk)), "warmHttpMs": round(elapsed, 3), "accounts": len(bulk["accounts"]),
        "keyedJsonBytes": len(expanded), "keyedGzipBytes": len(gzip.compress(expanded, compresslevel=3, mtime=0)), **decoded}
    if advance:
        script = r'''
const http=require('http'),WebSocket=require('ws'),{performance}=require('perf_hooks');
const bulk=JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')),url=process.argv[2],ws=new WebSocket(url.replace('http:','ws:')+'/api/dashboard/live');
const bytes={},counts={};let idleBytes=0,active=false,baseline=0,started=0,firstPacketMs=null;
ws.on('open',()=>{ws.send(JSON.stringify({type:'subscribe',epoch:bulk.epoch,cursors:bulk.cursors}));baseline=ws._socket.bytesRead;
setTimeout(()=>{idleBytes=ws._socket.bytesRead-baseline;baseline=ws._socket.bytesRead;active=true;started=performance.now();const req=http.request(url+'/advance',{method:'POST'},r=>{r.resume();r.on('end',()=>setTimeout(()=>{console.log(JSON.stringify({idleObservationMs:600,heartbeatIntervalSeconds:20,idleWireBytes:idleBytes,liveWireBytes:ws._socket.bytesRead-baseline,packetBytes:bytes,packetCounts:counts,firstPacketMs,liveObserveMs:performance.now()-started}));ws.close();},400));});req.end();},600);});
ws.on('message',data=>{if(!active)return;if(firstPacketMs===null)firstPacketMs=performance.now()-started;const p=JSON.parse(data.toString());const key=p.stream+':'+p.channel;bytes[key]=(bytes[key]||0)+data.length;counts[key]=(counts[key]||0)+1;});
ws.on('error',e=>{console.error(e);process.exitCode=1;});
'''
        result.update(json.loads(subprocess.run(["node", "-e", script, str(path), url], check=True, capture_output=True, text=True, timeout=20).stdout))
    return result


def isolated(source, directory):
    started = time.perf_counter()
    ledger = {view: defaultdict(list) for view in ("local", "merged")}
    with closing(samples.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("BEGIN")
        quota = list(db.execute("SELECT view,account,event_at,record_key,data FROM processing_quota_points"))
        facts = list(db.execute("SELECT source,position,data FROM processing_facts WHERE kind='tokenLedger' ORDER BY source,position"))
        for account, data in db.execute("SELECT account,data FROM processing_facts WHERE kind='tokenLedger' AND source NOT LIKE 'peer:%' ORDER BY account,event_at,record_key"):
            ledger["local"][account].append(json.loads(data))
        for account, data in db.execute("SELECT account,data FROM processing_effective ORDER BY account,event_at,record_key"):
            ledger["merged"][account].append(json.loads(data))
    index = ProcessingIndex(directory / "benchmark.sqlite3")
    with index.transaction() as db:
        db.executemany("INSERT INTO processing_quota_points VALUES(?,?,?,?,?)", quota)
        for source_name, position, data in facts:
            index.fact(db, source_name, position, "tokenLedger", json.loads(data))
        for view in ledger:
            db.executemany("INSERT OR IGNORE INTO delivery_dirty VALUES(?,?)", [(view, account) for account in ledger[view]])
    delivery = DashboardDelivery(index)
    try:
        status = read("http://127.0.0.1:8765/api/status")[0]
    except OSError:
        status = {"accounts": {"items": []}}
    prepare_started = time.perf_counter()
    delivery.prepare(status=status, force=True)
    preparation_ms = (time.perf_counter() - prepare_started) * 1000
    source_ms = (prepare_started - started) * 1000

    def advance():
        from monitor_usage_sync import record_key
        visible = {item["id"] for item in json.loads(delivery.bulk(False))["accounts"]}
        eligible = {item["id"] for item in json.loads(delivery.bulk(False))["accounts"] if item["local"]["chart:5h"] and item["local"]["chart:7d"]}
        account = next(key for key in ledger["local"] if key in (eligible or visible) and any(token_row(row) for row in ledger["local"][key]))
        row = next(row for row in reversed(ledger["local"][account]) if token_row(row) and (row.get("cost") or {}).get("totalCostUsd", 0) > 0) | {"eventId": uuid.uuid4().hex, "occurredAt": time.time()}
        key = record_key("tokenLedger", row)
        with index.transaction() as db:
            index.fact(db, "local:tokenLedger", 0, "tokenLedger", row)
            for view in ("local", "merged"):
                db.execute("INSERT OR IGNORE INTO delivery_work VALUES(?,?,?,?)", (view, account, "tokenLedger", key))
                db.execute("INSERT OR IGNORE INTO delivery_dirty VALUES(?,?)", (view, account))
        delivery.prepare(status=status)

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            if self.path == "/api/dashboard":
                send_bulk(self, delivery)
            elif self.path == "/api/dashboard/live":
                serve_live(self, delivery)
            else:
                self.send_error(404)

        def do_POST(self):
            advance()
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        result = measure(f"http://127.0.0.1:{server.server_port}", directory, True)
        result.update(sourceSnapshotMs=round(source_ms, 3), preparationMs=round(preparation_ms, 3), preparation=delivery.metrics)
        return result
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8765")
    parser.add_argument("--source-db", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    directory = Path.cwd() / (".dashboard-benchmark-" + uuid.uuid4().hex)
    directory.mkdir()
    try:
        result = isolated(args.source_db, directory) if args.source_db else measure(args.url.rstrip("/"), directory)
        print(json.dumps(result, indent=2))
        if args.report:
            args.report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    finally:
        shutil.rmtree(directory)


if __name__ == "__main__":
    main()
