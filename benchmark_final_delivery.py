"""Measure final delivery on isolated system-temp copies; source stays read-only."""

import argparse
import gc
import gzip
import json
import math
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import closing, contextmanager
from pathlib import Path

import psutil

from monitor_canonical import CanonicalIndex
from monitor_canonical_store import CanonicalRepository
from monitor_delivery import DashboardDelivery, encoded
from monitor_migration_delivery import migrate_local
from monitor_sqlite import IndexState


class ReleasedIndex(IndexState):
    canonical = True

    def __init__(self, path):
        self.path, self.lock = path, threading.RLock()

    @contextmanager
    def transaction(self):
        with self.lock, closing(sqlite3.connect(self.path)) as db, db:
            db.execute("PRAGMA recursive_triggers=ON")
            yield db


def allocations(path):
    import apsw
    db = apsw.Connection(str(path), flags=apsw.SQLITE_OPEN_READONLY)
    try:
        owners = dict(db.execute("SELECT name,tbl_name FROM sqlite_master WHERE type IN ('table','index')"))
        sizes, tables, indexes = {}, {}, {}
        for name, size in db.execute("SELECT name,SUM(pgsize) FROM dbstat GROUP BY name"):
            table = owners.get(name, name)
            sizes[table] = sizes.get(table, 0) + size
            target = tables if name == table else indexes
            target[table] = target.get(table, 0) + size
        return {"tableBytesIncludingIndexes": sizes, "tableBytes": tables, "indexBytes": indexes,
            "entries": {table: list(db.execute('SELECT COUNT(*) FROM "' + table + '"'))[0][0] for table in sizes if table in owners and table != 'sqlite_schema'},
            "fileBytes": list(db.execute("PRAGMA page_count"))[0][0] * list(db.execute("PRAGMA page_size"))[0][0],
            "freeBytes": list(db.execute("PRAGMA freelist_count"))[0][0] * list(db.execute("PRAGMA page_size"))[0][0]}
    finally:
        db.close()


def worker(path, mode):
    legacy = mode.startswith("legacy_")
    index = ReleasedIndex(path) if legacy else CanonicalIndex(path) if mode not in {"migration", "restart"} else None
    delivery = DashboardDelivery(index) if index is not None else None
    # Mutations are fixtures outside the measured operation. The source is never
    # used by a worker; every worker gets its own temporary database backup.
    if mode in {"append", "tail", "late_quota", "legacy_append"}:
        with index.transaction() as db:
            view, account = db.execute("SELECT dataset,account FROM canonical_tokens GROUP BY dataset,account ORDER BY COUNT(*) DESC LIMIT 1").fetchone()
            edge = db.execute("SELECT first,last FROM delivery_items WHERE view=? AND account=? AND channel='chart:5h' ORDER BY last DESC LIMIT 1", (view, account)).fetchone() if not legacy else None
            if mode in {"append", "legacy_append"}:
                row = CanonicalRepository.rows(db, view, "tokenLedger", account, limit=1)[0]
                row["eventId"] = "benchmark-event"
                stamp = (edge[0] + edge[1]) / 2 if edge else db.execute("SELECT MAX(occurred_at)+1 FROM canonical_tokens WHERE dataset=? AND account=?", (view, account)).fetchone()[0]
                from datetime import datetime, timezone
                row["occurredAt"] = datetime.fromtimestamp(stamp, timezone.utc).isoformat()
                CanonicalRepository.write(db, view, "tokenLedger", row, insert_only=True)
                if legacy:
                    from monitor_usage_sync import record_key
                    db.execute("INSERT OR IGNORE INTO delivery_work VALUES(?,?,'tokenLedger',?)", (view, account, record_key("tokenLedger", row)))
                    db.execute("INSERT OR IGNORE INTO delivery_dirty VALUES(?,?)", (view, account))
            elif mode == "tail":
                key, last = db.execute("SELECT record_key,range_last FROM canonical_quota WHERE dataset=? AND account=? ORDER BY last DESC LIMIT 1", (view, account)).fetchone()
                db.execute("UPDATE canonical_quota SET last=?,range_last=? WHERE dataset=? AND account=? AND record_key=?", (last + 90, last + 90, view, account, key))
            elif edge:
                stamp = math.floor(sum(edge) / 2)
                key, row = CanonicalRepository.items(db, view, "quota", account, condition="window='5h' AND first<=?", parameters=(stamp,), order="first DESC", limit=1)[0]
                from datetime import datetime, timezone
                row["checkedAt"] = datetime.fromtimestamp(stamp, timezone.utc).isoformat()
                row["sync"]["recordId"] = "benchmark-late-quota"
                window = row["windows"]["5h"]
                window.update(firstAt=stamp, lastAt=stamp, usedPercent=min(100, window["usedPercent"] + .25))
                CanonicalRepository.write(db, view, "quota", row)
    if mode.startswith(("sparse_", "dense_", "gap_", "cloud_batch_")):
        with index.transaction() as db:
            view, account, first, last = db.execute("SELECT view,account,first,last FROM delivery_items WHERE channel='chart:5h' " +
                ("AND view='merged' " if mode.startswith('cloud_batch_') else "") + "ORDER BY last DESC LIMIT 1").fetchone()
            columns = [row[1] for row in db.execute('PRAGMA table_info(canonical_tokens)')]
            template = list(db.execute("SELECT * FROM canonical_tokens LIMIT 1").fetchone())
            count = int(mode.split('_')[-1]) if not mode.startswith('gap_') else 1
            def rows():
                for number in range(count):
                    row = template.copy()
                    stamp = (first + last) / 2 if mode.startswith(('dense_', 'cloud_batch_')) else last + 3155760000 + number * 1000000
                    values = {'dataset': view, 'account': account, 'record_key': f'benchmark:{number}', 'event_id': f'benchmark:{number}',
                        'first': math.floor(stamp), 'last': math.floor(stamp), 'occurred_at': stamp, 'occurred_at_text': None}
                    for key, value in values.items():
                        row[columns.index(key)] = value
                    yield row
            db.executemany("INSERT INTO canonical_tokens VALUES(" + ','.join('?' for _ in columns) + ')', rows())
        if not mode.startswith('cloud_batch_'):
            delivery.prepare()  # Publish fixture additions before measuring the local edit.
            delivery.metrics = dict.fromkeys(delivery.metrics, 0)
            with index.transaction() as db:
                key, row = CanonicalRepository.items(db, view, "quota", account, condition="window='5h' AND first<=?", parameters=((first + last) / 2,), order="first DESC", limit=1)[0]
                from datetime import datetime, timezone
                stamp = math.floor((first + last) / 2)
                row['checkedAt'] = datetime.fromtimestamp(stamp, timezone.utc).isoformat()
                row['sync']['recordId'] = 'benchmark-neighborhood'
                row['windows']['5h'].update(firstAt=stamp, lastAt=stamp, usedPercent=min(100, row['windows']['5h']['usedPercent'] + .25))
                CanonicalRepository.write(db, view, 'quota', row)
    gc.collect()
    process = psutil.Process()
    baseline, stop = process.memory_info().rss, threading.Event()
    peak = [baseline]

    def watch():
        while not stop.wait(.002):
            peak[0] = max(peak[0], process.memory_info().rss)

    watcher = threading.Thread(target=watch)
    watcher.start()
    wall, cpu = time.perf_counter(), time.process_time()
    written = process.io_counters().write_bytes
    bytes_sent = None
    try:
        if mode == "migration":
            migrate_local({"processing_index": path})
        elif mode == "legacy_snapshot":
            with index.transaction() as db:
                value = encoded(delivery._bulk(db, delivery.state or {}))
            bytes_sent = len(gzip.compress(value, compresslevel=3, mtime=0))
        elif mode == "snapshot":
            with delivery.snapshot_file() as (file, size):
                while file.read(64 * 1024):
                    pass
                bytes_sent = size
        elif mode == "restart":
            index = CanonicalIndex(path)
            delivery = DashboardDelivery(index)
        else:
            delivery.prepare(force=legacy)
        elapsed, cpu = time.perf_counter() - wall, time.process_time() - cpu
    finally:
        peak[0] = max(peak[0], process.memory_info().rss)
        stop.set()
        watcher.join()
    result = {"wallSeconds": elapsed, "cpuSeconds": cpu, "extraPeakRSSMiB": (peak[0] - baseline) / 1048576,
        "peakRSSMiB": peak[0] / 1048576, "bytesSent": bytes_sent, "processWrittenBytes": process.io_counters().write_bytes - written}
    if delivery is not None:
        result["metrics"] = delivery.metrics
        if hasattr(delivery, "close"):
            delivery.close()
    if index is not None and hasattr(index, "close"):
        index.close()
    return result


def benchmark(source, report, repeats=3):
    with tempfile.TemporaryDirectory(prefix="codex-final-benchmark-") as directory:
        root = Path(directory)
        original, final = root / "released.sqlite3", root / "final.sqlite3"
        with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as reader, closing(sqlite3.connect(original)) as writer:
            reader.backup(writer)
        shutil.copyfile(original, final)
        before = allocations(original)
        result = {"before": before, "measurements": {}}
        with closing(sqlite3.connect(original)) as db:
            legacy = db.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_snapshot'").fetchone() is not None
            result["canonicalCounts"] = {table: db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0] for table in ("canonical_quota", "canonical_tokens")}
        modes = (["migration", "legacy_snapshot", "legacy_append"] if legacy else []) + ["restart", "append", "tail", "late_quota", "snapshot",
            "gap_100years", "sparse_10000", "dense_10000", "dense_30000", "cloud_batch_1000"]
        for mode in modes:
            if mode == "restart":
                migrate_local({"processing_index": final})
                result["after"] = allocations(final)
            values = []
            for _ in range(repeats):
                work = root / "worker.sqlite3"
                shutil.copyfile(original if mode.startswith("legacy_") or mode == "migration" else final, work)
                output = subprocess.check_output([sys.executable, str(Path(__file__).resolve()), "--worker", str(work), mode], text=True)
                values.append(json.loads(output))
                for suffix in ("", "-wal", "-shm"):
                    Path(str(work) + suffix).unlink(missing_ok=True)
            row = {name: statistics.median(value[name] for value in values) if values[0][name] is not None else None
                for name in ("wallSeconds", "cpuSeconds", "extraPeakRSSMiB", "peakRSSMiB", "bytesSent", "processWrittenBytes")}
            if "metrics" in values[0]:
                row["metrics"] = values[0]["metrics"]
            result["measurements"][mode] = row
            print(mode, json.dumps(row), flush=True)
        result["notes"] = ["Read-only source; SQLite backup; all mutations use independent system-temp copies.",
            "Median of fresh worker processes. Peak RSS sampled every 2 ms; OS cache is warm.",
            "Legacy append includes eager snapshot generation; final append does not generate an unrequested snapshot.",
            "Final snapshot reads final wire rows only, with streaming JSON/gzip and bounded memory.",
            "Restart measures opening CanonicalIndex and Delivery metadata, not daemon network/authentication startup.",
            "Dense/sparse fixture construction and initial publication happen before measurement; compare absolute RSS as well as incremental RSS.",
            "Conversion does not VACUUM. Freed pages remain in the database for reuse."]
        report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        print(json.dumps(worker(Path(sys.argv[2]), sys.argv[3])))
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--source-db", type=Path, required=True)
        parser.add_argument("--report", type=Path, default=Path("final-delivery-benchmark.json"))
        parser.add_argument("--repeats", type=int, default=3)
        args = parser.parse_args()
        benchmark(args.source_db, args.report, args.repeats)
