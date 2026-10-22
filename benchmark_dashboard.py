"""Replay a dashboard snapshot into an isolated delivery index; never modify the live monitor."""

import argparse
import gzip
import json
import shutil
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

from monitor_common import parse_timestamp
from monitor_delivery import DashboardDelivery
from monitor_processing import ProcessingIndex
from monitor_streaming import json_chunks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8765", help="Running monitor to read; defaults to localhost. Only GET /api/series is used.")
    args = parser.parse_args()
    started = time.perf_counter()
    with urllib.request.urlopen(urllib.request.Request(args.url.rstrip("/") + "/api/series", headers={"Accept-Encoding": "gzip"}), timeout=60) as response:
        data = response.read()
        snapshot = json.loads(gzip.decompress(data) if response.headers.get("Content-Encoding") == "gzip" else data)
    print(json.dumps({"liveSnapshotSeconds": round(time.perf_counter() - started, 4), "wireBytes": len(data)}), flush=True)
    root = Path.cwd() / f".dashboard-benchmark-{uuid.uuid4().hex}"
    root.mkdir()
    try:
        index = ProcessingIndex(root / "index.sqlite3")
        counts = {}
        with index.transaction() as db:
            index.put(db, "displayRevision", 1)
            for view, frame in snapshot["views"].items():
                counts[view] = len(frame["quotaPoints"]) + len(frame["tokenSessions"]) + sum(map(len, frame["costUsage"].values()))
                for row in frame["quotaPoints"]:
                    db.execute("INSERT OR REPLACE INTO processing_quota_points VALUES(?,?,?,?,?)",
                        (view, row.get("usageAccountId") or "unknown", row["timestamp"], row["_key"], json.dumps(row)))
                for kind, rows in (("tokenSessions", frame["tokenSessions"]), ("costUsage:fiveHour", frame["costUsage"]["fiveHour"]), ("costUsage:sevenDay", frame["costUsage"]["sevenDay"])):
                    for row in rows:
                        stamp = parse_timestamp(row.get("updatedAt") or row.get("startedAt")) if kind == "tokenSessions" else row.get("timestamp")
                        db.execute("INSERT OR REPLACE INTO processing_display VALUES(?,?,?,?,?,?)", (view, row.get("usageAccountId") or "unknown", kind, row["_key"], stamp, json.dumps(row)))
        delivery = DashboardDelivery(index)
        started = time.perf_counter()
        delivery.prepare()
        print(json.dumps({"recordsReplayed": counts, "backgroundBootstrapSeconds": round(time.perf_counter() - started, 4), "metrics": delivery.metrics}), flush=True)
        for selected in ("5h", "All"):
            started = time.perf_counter()
            manifest = delivery.manifest("merged", selected)
            manifest_ms = (time.perf_counter() - started) * 1000
            bodies = [delivery.blob(digest, True) for digest in manifest["chunks"]]
            print(json.dumps({"range": selected, "manifestMs": round(manifest_ms, 3), "readPreparedManifestAndChunksMs": round((time.perf_counter() - started) * 1000, 3),
                "chunks": len(bodies), "compressedBytes": sum(map(len, bodies)), "manifestBytes": len(json.dumps(manifest).encode()), "exactSessionCount": sum(row["sessionCount"] for row in manifest["tokenSessions"])}), flush=True)
        before = delivery.metrics["chunksEncoded"]
        latest = max(row["timestamp"] for frame in snapshot["views"].values() for row in frame["quotaPoints"])
        start = int(latest // 86400) * 86400
        for label in ("uncachedDay", "cachedDay"):
            started = time.perf_counter()
            manifest = delivery.manifest("merged", "Date", start, start + 86400)
            print(json.dumps({"scope": label, "manifestMs": round((time.perf_counter() - started) * 1000, 3), "chunks": len(manifest["chunks"])}), flush=True)
        started = time.perf_counter()
        delivery.prepare()
        print(json.dumps({"idleMaintenanceMs": round((time.perf_counter() - started) * 1000, 3), "chunksReencoded": delivery.metrics["chunksEncoded"] - before}), flush=True)
        started = time.perf_counter()
        count = 0
        with tempfile.TemporaryFile() as output:
            with gzip.GzipFile(fileobj=output, mode="wb") as compressed:
                for chunk in json_chunks(snapshot):
                    compressed.write(chunk)
                    count += 1
        print(json.dumps({"legacySerializationSeconds": round(time.perf_counter() - started, 4), "legacyWrites": count}), flush=True)
    finally:
        if root.resolve().parent != Path.cwd().resolve() or not root.name.startswith(".dashboard-benchmark-"):
            raise ValueError("Unexpected benchmark directory")
        shutil.rmtree(root)


if __name__ == "__main__":
    main()
