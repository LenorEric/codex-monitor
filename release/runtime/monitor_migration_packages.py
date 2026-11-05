"""One-time local conversion from released stores to canonical views and the upload buffer."""

import copy
import hashlib
import json
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone

import monitor_sample_store as samples
from monitor_common import parse_timestamp
from monitor_usage_sync import canonical_json, record_account_key, record_key


def initialize_v22(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS processing_meta(name TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS canonical_records(
            view TEXT NOT NULL CHECK(view IN ('local','merged')),kind TEXT NOT NULL,account TEXT NOT NULL,
            record_key TEXT NOT NULL,first INTEGER NOT NULL,last INTEGER NOT NULL,data TEXT NOT NULL,
            PRIMARY KEY(view,kind,account,record_key));
        CREATE INDEX IF NOT EXISTS canonical_time ON canonical_records(view,kind,account,first,last);
        CREATE INDEX IF NOT EXISTS canonical_event ON canonical_records(view,json_extract(data,'$.eventId')) WHERE kind='tokenLedger';
        CREATE TABLE IF NOT EXISTS canonical_seen(view TEXT,kind TEXT,record_key TEXT,PRIMARY KEY(view,kind,record_key));
        CREATE TABLE IF NOT EXISTS upload_buffer(seq INTEGER PRIMARY KEY AUTOINCREMENT,record_key TEXT UNIQUE NOT NULL,arrived REAL NOT NULL,data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS package_receipts(destination TEXT,machine TEXT,package TEXT,PRIMARY KEY(destination,machine,package));
        CREATE TABLE IF NOT EXISTS source_checkpoints(path TEXT PRIMARY KEY,state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS source_blocks(path TEXT,block INTEGER,size INTEGER,digest TEXT,PRIMARY KEY(path,block));
    """)


def fragment_v22(row, label, first, last):
    row = copy.deepcopy(row)
    row["checkedAt"] = datetime.fromtimestamp(first, timezone.utc).isoformat()
    row["windows"] = {label: row["windows"][label] | {"firstAt": first, "lastAt": last}}
    row["sync"]["recordId"] = hashlib.sha256(canonical_json([record_account_key(row), label, first, last, row["windows"][label]])).hexdigest()
    return row


def migrate_local(paths):
    from monitor_processing import ProcessingIndex
    from monitor_delivery import initialize_delivery
    from monitor_sampling import WINDOWS
    path = paths["processing_index"]
    if not path.exists():
        return
    with closing(sqlite3.connect(path)) as check:
        if check.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_records'").fetchone() and check.execute("SELECT 1 FROM processing_meta WHERE name='canonicalStore'").fetchone():
            return
    # Finish released pending sampling against the released representation first.
    index = ProcessingIndex(path)
    # A source checkpoint may be ahead of its ledger at shutdown. Convert that
    # pending evidence before retiring the old event queue, even if its file vanished.
    from monitor_migrations import FormatConverter, read_json
    from monitor_token_ledger import _usage_records
    manifest = read_json(paths["account_manifest"]) if paths.get("account_manifest") and paths["account_manifest"].exists() else {}
    machine = read_json(paths["machine"]).get("machineId", "unknown") if paths.get("machine") and paths["machine"].exists() else "unknown"
    timeline = sorted(manifest.get("activationHistory", []), key=lambda row: parse_timestamp(row.get("checkedAt")) or 0)
    converter = FormatConverter(manifest, {}, {}, machine)
    with index.transaction() as db:
        existing = {event for event, in db.execute("SELECT event_id FROM processing_facts WHERE kind='tokenLedger' AND source NOT LIKE 'peer:%'")}
        pending = [(home, json.loads(data)) for home, data in db.execute("SELECT home,data FROM processing_scan_events WHERE ledger_done=0 AND valid_refs>0 ORDER BY event_at,event_id")]
        for row in _usage_records([event for _, event in pending], manifest.get("activeAccountId", "unknown"), "Unknown", timeline):
            if row["eventId"] in existing:
                continue
            activation = next((value for value in reversed(timeline) if (parse_timestamp(value["checkedAt"]) or 0) <= (parse_timestamp(row["occurredAt"]) or 0)), {})
            row["sync"] = {"version": 1, "originMachineId": machine, "accountId": converter.account(activation.get("usageAccountId"), row["accountSlotId"], machine)}
            index.write_records(db, "tokenLedger", [row])
            home = next(home for home, event in pending if event["eventId"] == row["eventId"])
            index.put(db, f"ledgerNotifications:{home}", index.get(db, f"ledgerNotifications:{home}", []) + [row])
    with closing(samples.connect(path)) as db:
        initialize_v22(db)
        initialize_delivery(db)
        with db:
            def put(view, kind, row):
                account, key = record_account_key(row), record_key(kind, row)
                first = int(parse_timestamp(row.get("checkedAt") or row.get("occurredAt")))
                last = max((window.get("lastAt", first) for window in row.get("windows", {}).values()), default=first)
                db.execute("INSERT OR IGNORE INTO canonical_records VALUES(?,?,?,?,?,?,?)", (view, kind, account, key, first, last, canonical_json(row).decode()))

            for source, kind, key, data in db.execute("SELECT source,kind,record_key,data FROM processing_facts ORDER BY source,position"):
                row = json.loads(data)
                for view in (("local", "merged") if not source.startswith("peer:") else ("merged",)):
                    for identity in ([key + ":" + label for label in row["windows"]] if kind == "quota" else [key]):
                        db.execute("INSERT OR IGNORE INTO canonical_seen VALUES(?,?,?)", (view, kind, identity))
                if kind == "tokenLedger":
                    if not source.startswith("peer:"):
                        put("local", kind, row)
                    put("merged", kind, row)
            # Saved sampling decisions, including legacy points and compressed runs,
            # become canonical data. Do not replay their acceptance rules.
            for view, account, data in db.execute("SELECT view,account,data FROM processing_sampled_quota ORDER BY view,account,event_at"):
                point = json.loads(data)
                for name, (label, _) in WINDOWS.items():
                    value = point.get(name) or {}
                    if value.get("raw") is None or point.get("resetWindow") not in (None, name):
                        continue
                    first, last = int(value.get("firstAt", point["timestamp"])), int(value.get("lastAt", point["timestamp"]))
                    row = {"quotaRunVersion": 1, "checkedAt": point["checkedAt"], "accountSlotId": point.get("accountSlotId", "unknown"),
                        "accountLabel": point.get("accountLabel", "Unknown"), "sync": {"version": 1, "accountId": account, "originMachineId": "migrated"},
                        "windows": {label: {"usedPercent": value["raw"], "continuous": value.get("continuous"), "multiplier": value.get("multiplier", 1),
                        "resetAt": value.get("resetAt"), "plan": value.get("plan", "plus"), "firstAt": first, "lastAt": last, "resetBoundary": bool(point.get("resetWindow"))}}}
                    put(view, "quota", fragment_v22(row, label, first, last))
            # Publication history is converted to temporary candidates. The first
            # online sync removes candidates already present in migrated packages.
            for key, period, data in db.execute("SELECT record_key,period,record_json FROM local_records ORDER BY period,record_key"):
                db.execute("INSERT OR IGNORE INTO upload_buffer(record_key,arrived,data) VALUES(?,?,?)", (key, parse_timestamp(period) or time.time(), data))
                record = json.loads(data)
                for view in ("local", "merged"):
                    for identity in ([key + ":" + label for label in record["row"]["windows"]] if record["kind"] == "quota" else [key]):
                        db.execute("INSERT OR IGNORE INTO canonical_seen VALUES(?,?,?)", (view, record["kind"], identity))
            for key, kind, data in db.execute("SELECT record_key,kind,data FROM processing_pending_records"):
                if data is None:
                    db.execute("DELETE FROM upload_buffer WHERE record_key=?", (key,))
                else:
                    row = json.loads(data)
                    db.execute("INSERT OR REPLACE INTO upload_buffer(record_key,arrived,data) VALUES(?,?,?)", (key, time.time(), canonical_json({"kind": kind, "row": row}).decode()))
                    for view in ("local", "merged"):
                        for identity in ([key + ":" + label for label in row["windows"]] if kind == "quota" else [key]):
                            db.execute("INSERT OR IGNORE INTO canonical_seen VALUES(?,?,?)", (view, kind, identity))
            cutoff = db.execute("SELECT COALESCE(MAX(seq),0) FROM upload_buffer").fetchone()[0]
            db.execute("INSERT OR REPLACE INTO processing_meta VALUES('legacyPublicationPending','true')")
            db.execute("INSERT OR REPLACE INTO processing_meta VALUES('legacyPublicationCutoff',?)", (json.dumps(cutoff),))
            for source, state in db.execute("SELECT path,state FROM processing_scan_sources"):
                db.execute("INSERT OR REPLACE INTO source_checkpoints VALUES(?,?)", (source, state))
            db.execute("INSERT OR REPLACE INTO source_blocks SELECT source,block,size,digest FROM processing_blocks")
            db.execute("DELETE FROM processing_meta WHERE name='migrationRebuildPending'")
            db.execute("DELETE FROM processing_meta WHERE name NOT IN ('runtimeState','consoleState','legacyPublicationPending','legacyPublicationCutoff') "
                "AND name NOT LIKE 'samplingState:%' AND name NOT LIKE 'ledgerNotifications:%'")
            db.execute("INSERT OR REPLACE INTO processing_meta VALUES('canonicalStore','true')")
            # No input payloads or package ownership survive the representation change.
            db.execute("PRAGMA defer_foreign_keys=ON")
            for name, in list(db.execute("SELECT name FROM sqlite_master WHERE type='view' AND (name LIKE 'processing_%' OR name IN ('local_records','remote_records'))")):
                db.execute(f'DROP VIEW "{name}"')
            for name, in list(db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name NOT LIKE 'delivery_%'")):
                db.execute(f'DROP TRIGGER "{name}"')
            tables = [name for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table'") if (name.startswith(("processing_", "sample_", "quota_", "token_", "baseline_", "local_record", "remote_")) and name != "processing_meta")]
            # Drop referencing tables first; the remaining shared payload layout can
            # be retired with foreign keys disabled after the conversion commits.
            for name in tables:
                db.execute(f'DELETE FROM "{name}"')
            db.execute("INSERT OR IGNORE INTO delivery_dirty SELECT DISTINCT view,account FROM canonical_records")
        db.execute("PRAGMA foreign_keys=OFF")
        with db:
            for name in tables:
                db.execute(f'DROP TABLE "{name}"')
    if paths.get("config") and paths["config"].exists():
        from monitor_accounts import atomic_write_json
        config = json.loads(paths["config"].read_text(encoding="utf-8"))
        config.pop("saveJsonl", None)
        config.setdefault("saveRawQuotaResponses", False)
        atomic_write_json(paths["config"], config)
