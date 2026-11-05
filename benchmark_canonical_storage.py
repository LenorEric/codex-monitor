"""Compare a contract-22 snapshot with its isolated contract-23 conversion.

Requires APSW only when the system sqlite3 lacks dbstat. Never migrates the source.
The report contains counts/sizes/timings, not account IDs or usage payloads.
"""

import argparse
import json
import shutil
import sqlite3
import statistics
import tempfile
import time
from contextlib import closing
from pathlib import Path

from monitor_canonical_store import CanonicalRepository
from monitor_interpretation import token_row, usage_runs
from monitor_migration_canonical import migrate_local
from monitor_sampling import quota_point


def storage(path):
    with closing(sqlite3.connect(path)) as db:
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        result = {"fileBytes": db.execute("PRAGMA page_count").fetchone()[0] * page_size,
            "freeBytes": db.execute("PRAGMA freelist_count").fetchone()[0] * page_size}
        owners = {name: (kind, table) for name, kind, table in db.execute("SELECT name,type,tbl_name FROM sqlite_master WHERE type IN ('table','index')")}
        try:
            sizes = list(db.execute("SELECT name,SUM(pgsize) FROM dbstat GROUP BY name"))
        except sqlite3.OperationalError:
            import apsw
            reader = apsw.Connection(str(path), flags=apsw.SQLITE_OPEN_READONLY)
            try:
                sizes = list(reader.execute("SELECT name,SUM(pgsize) FROM dbstat GROUP BY name"))
            finally:
                reader.close()
        result["canonicalTables"] = {name: size for name, size in sizes if name in {"canonical_records", "canonical_quota", "canonical_tokens"}}
        result["canonicalIndexes"] = {name: size for name, size in sizes if owners.get(name, (None, None))[0] == "index"
            and owners[name][1] in {"canonical_records", "canonical_quota", "canonical_tokens"}}
        result["canonicalBytes"] = sum(result["canonicalTables"].values()) + sum(result["canonicalIndexes"].values())
        result["identityBytes"] = sum(size for name, size in sizes if owners.get(name, (None, None))[1] == "canonical_seen")
        result["otherAllocatedBytes"] = sum(size for name, size in sizes) - result["canonicalBytes"] - result["identityBytes"]
        return result


def measured(callback):
    timings = []
    for _ in range(3):
        start = time.perf_counter()
        callback()
        timings.append(time.perf_counter() - start)
    return round(statistics.median(timings), 6)


def workload(path, typed, account):
    with closing(sqlite3.connect(path)) as db:
        def read():
            if typed:
                return tuple(CanonicalRepository.rows(db, "merged", kind, account) for kind in ("quota", "tokenLedger"))
            return tuple([json.loads(data) for data, in db.execute("SELECT data FROM canonical_records WHERE view='merged' AND kind=? AND account=? ORDER BY first,record_key", (kind, account))]
                for kind in ("quota", "tokenLedger"))
        rows = read()
        def prepare():
            quotas, tokens = read()
            points = [quota_point(row) for row in quotas]
            for window in ("5h", "7d"):
                usage_runs(points, window)
            for row in tokens:
                token_row(row)
        def edit():
            db.execute("BEGIN")
            try:
                if typed:
                    db.execute("UPDATE canonical_quota SET used_percent=CASE WHEN used_percent<99 THEN used_percent+0.5 ELSE used_percent-0.5 END WHERE dataset='merged' AND account=? AND record_key=?", (account, key))
                else:
                    value = json.loads(original)
                    window = next(iter(value["windows"].values()))
                    window["usedPercent"] += .5 if window["usedPercent"] < 99 else -.5
                    db.execute("UPDATE canonical_records SET data=? WHERE view='merged' AND kind='quota' AND account=? AND record_key=?", (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")), account, key))
            finally:
                db.rollback()
        if typed:
            key = db.execute("SELECT record_key FROM canonical_quota WHERE dataset='merged' AND account=? LIMIT 1", (account,)).fetchone()[0]
        else:
            key, original = db.execute("SELECT record_key,data FROM canonical_records WHERE view='merged' AND kind='quota' AND account=? LIMIT 1", (account,)).fetchone()
        return {"quotaRows": len(rows[0]), "tokenRows": len(rows[1]), "readSeconds": measured(read),
            "prepareSeconds": measured(prepare), "singleValueEditRollbackSeconds": measured(edit)}


def compare(source, output):
    output.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=".typed-benchmark-", dir=output.parent))
    try:
        before, after = work / "baseline.sqlite3", work / "typed.sqlite3"
        with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as reader, closing(sqlite3.connect(before)) as writer:
            reader.backup(writer)
        with closing(sqlite3.connect(before)) as reader, closing(sqlite3.connect(after)) as writer:
            reader.backup(writer)
        with closing(sqlite3.connect(before)) as db:
            counts = list(db.execute("SELECT view,kind,COUNT(*) FROM canonical_records GROUP BY view,kind"))
            account = db.execute("SELECT account FROM canonical_records WHERE view='merged' AND kind='quota' GROUP BY account ORDER BY COUNT(*) DESC LIMIT 1").fetchone()[0]
        report = {"counts": counts, "before": storage(before)}
        start = time.perf_counter()
        migrate_local({"processing_index": after})
        report["conversionSeconds"] = round(time.perf_counter() - start, 6)
        report["after"] = storage(after)
        report["workload"] = {"before": workload(before, False, account), "after": workload(after, True, account)}
        report["canonicalReductionPercent"] = round(100 * (1 - report["after"]["canonicalBytes"] / report["before"]["canonicalBytes"]), 2)
        report["notes"] = ["Read-only source; independent SQLite backups; no compaction.",
            "Median of 3 warm runs on the largest Merged quota account.",
            "Preparation measures account reads, quota_point, usage_runs and token_row; excludes delivery writes and gzip.",
            "Edit measures one observed-value UPDATE followed by rollback, not the full range merge processor.",
            "Conversion timing excludes migration-journal backup time."]
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        shutil.rmtree(work)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Existing contract-22 SQLite database; opened read-only")
    parser.add_argument("--output", type=Path, default=Path("canonical-storage-benchmark.json"))
    args = parser.parse_args()
    compare(args.source, args.output)
