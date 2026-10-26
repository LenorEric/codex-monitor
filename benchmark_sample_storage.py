"""Benchmark migration 14 on a consistent SQLite backup; never mutate the source.

Usage: python benchmark_sample_storage.py SOURCE COPY REPORT
Requires APSW for dbstat when the system sqlite3 module lacks it.
"""

import argparse
import hashlib
import json
import sqlite3
import statistics
import time
from contextlib import closing
from pathlib import Path

from monitor_migrations import migrate_sample_store
import monitor_sample_store as samples


def allocation(path, normalized):
    import apsw
    with apsw.Connection(str(path), flags=apsw.SQLITE_OPEN_READONLY) as db:
        tables = {name for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table'") if
            name.startswith(("sample_", "baseline_")) or name in {"quota_samples", "quota_windows", "token_samples", "processing_fact_refs", "local_record_refs", "remote_record_refs"}} if normalized else {"processing_facts", "local_records", "remote_records"}
        objects = {name: table for name, table in db.execute("SELECT name,tbl_name FROM sqlite_master WHERE type IN ('table','index')") if table in tables}
        sizes = {name: size for name, size in db.execute("SELECT name,sum(pgsize) FROM dbstat GROUP BY name") if name in objects}
        return {"tablesBytes": sum(size for name, size in sizes.items() if name in tables),
            "indexesBytes": sum(size for name, size in sizes.items() if name not in tables), "objects": sizes}


def timings(path, parameters):
    source, position, account, session, machine, day = parameters
    queries = {
        "sourcePosition": ("SELECT data FROM processing_facts WHERE source=? AND position=?", (source, position)),
        "accountRecent100": ("SELECT data FROM processing_facts WHERE account=? AND kind='quota' ORDER BY event_at DESC LIMIT 100", (account,)),
        "session100": ("SELECT data FROM processing_facts WHERE source=? AND session=? AND kind='tokenLedger' LIMIT 100", (source, session)),
        "remotePartition100": ("SELECT record_json FROM remote_records WHERE machine_id=? AND day=? LIMIT 100", (machine, day)),
    }
    with closing(samples.connect(path)) as db:
        result = {}
        for name, (query, values) in queries.items():
            elapsed = []
            for _ in range(7):
                start = time.perf_counter()
                rows = db.execute(query, values).fetchall()
                elapsed.append((time.perf_counter() - start) * 1000)
            result[name] = {"medianMs": statistics.median(elapsed), "rows": len(rows),
                "plan": [row[3] for row in db.execute("EXPLAIN QUERY PLAN " + query, values)]}
        return result


def fingerprints(path):
    """Compare actual owner reconstruction, including attribution and envelopes."""
    result = {}
    with closing(samples.connect(path)) as db:
        for table, keys, payload in (("processing_facts", "source,position", "data"), ("local_records", "record_key", "record_json"),
                ("remote_records", "machine_id,record_key", "record_json")):
            digest = hashlib.sha256()
            for row in db.execute(f"SELECT {keys},{payload} FROM {table} ORDER BY {keys}"):
                digest.update(samples.canonical([*row[:-1], json.loads(row[-1])]).encode() + b"\n")
            result[table] = digest.hexdigest()
    return result


def run(source, target):
    if target.exists() or source.resolve() == target.resolve():
        raise ValueError("Benchmark copy must be a new file distinct from source")
    with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as db, closing(sqlite3.connect(target)) as copy:
        db.backup(copy)
        source_position = copy.execute("SELECT source,position FROM processing_facts WHERE kind='tokenLedger' LIMIT 1").fetchone()
        account, = copy.execute("SELECT account FROM processing_facts WHERE kind='quota' LIMIT 1").fetchone()
        session, = copy.execute("SELECT session FROM processing_facts WHERE kind='tokenLedger' AND source=? LIMIT 1", (source_position[0],)).fetchone()
        remote = copy.execute("SELECT machine_id,day FROM remote_records LIMIT 1").fetchone()
        counts = {table: copy.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in ("processing_facts", "local_records", "remote_records")}
    parameters = (*source_position, account, session, *remote)
    before = {"allocation": allocation(target, False), "queries": timings(target, parameters), "fingerprints": fingerprints(target)}
    started = time.perf_counter()
    migrate_sample_store(target)
    duration = time.perf_counter() - started
    after = {"allocation": allocation(target, True), "queries": timings(target, parameters), "fingerprints": fingerprints(target)}
    if before["fingerprints"] != after["fingerprints"]:
        raise ValueError("Reconstructed sampled history changed")
    with closing(samples.connect(target)) as db:
        shared = db.execute("SELECT count(*) FROM sample_records").fetchone()[0]
        for table, count in counts.items():
            if db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] != count:
                raise ValueError("Ownership counts changed")
    return {"source": str(source), "counts": counts, "sharedSamples": shared, "migrationSeconds": duration, "before": before, "after": after}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("copy", type=Path)
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    args.report.write_text(json.dumps(run(args.source, args.copy), indent=2) + "\n", encoding="utf-8")
    print(args.report)
