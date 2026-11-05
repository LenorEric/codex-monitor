"""Permanent, identically shaped Local/Merged data and a disposable upload buffer."""

import copy
import hashlib
import json
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path

from monitor_common import parse_timestamp
from monitor_sqlite import IndexState
from monitor_canonical_store import CanonicalRepository as records, initialize as initialize_typed
from monitor_usage_sync import NormalizedRecords, canonical_json, record_account_key, record_key


def initialize(db):
    initialize_typed(db)
    for statement in """
        CREATE TABLE IF NOT EXISTS processing_meta(name TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS canonical_seen(view TEXT,kind TEXT,record_key TEXT,PRIMARY KEY(view,kind,record_key));
        CREATE TABLE IF NOT EXISTS upload_buffer(seq INTEGER PRIMARY KEY AUTOINCREMENT,record_key TEXT UNIQUE NOT NULL,arrived REAL NOT NULL,data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS package_receipts(destination TEXT,machine TEXT,package TEXT,PRIMARY KEY(destination,machine,package));
        CREATE TABLE IF NOT EXISTS source_checkpoints(path TEXT PRIMARY KEY,state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS source_blocks(path TEXT,block INTEGER,size INTEGER,digest TEXT,PRIMARY KEY(path,block));
    """.split(";"):
        if statement.strip():
            db.execute(statement)


class CanonicalIndex(IndexState):
    canonical = True

    def maintain_disk_space(self):
        # Contract 23 deliberately retains freed pages for subsequent ingestion.
        return 0

    def __init__(self, path):
        self.path, self.lock = Path(path), threading.RLock()
        self._db = None
        self._closed = False
        self._transaction_depth = 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.metrics = {"recordsImported": 0, "sourceBytesRead": 0, "sourceVerificationBytes": 0, "sourcesChecked": 0}
        with closing(sqlite3.connect(self.path)) as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE name IN ('processing_facts','canonical_records')").fetchone():
                raise ValueError("Local contract 23 migration must complete before typed canonical data can be used")
            db.execute("PRAGMA journal_mode=WAL")
            initialize(db)
            db.execute("INSERT OR IGNORE INTO processing_meta VALUES('canonicalStore','true')")
            from monitor_delivery import initialize_delivery
            initialize_delivery(db)
            db.commit()

    @contextmanager
    def transaction(self):
        with self.lock:
            if self._closed:
                raise RuntimeError("Canonical index is closed")
            if self._db is None:
                # All access, including shutdown, is serialized by self.lock.
                db = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
                try:
                    db.execute("PRAGMA foreign_keys=ON")
                    db.execute("PRAGMA recursive_triggers=ON")
                except BaseException:
                    db.close()
                    raise
                self._db = db
            db = self._db
            depth = self._transaction_depth
            self._transaction_depth += 1
            try:
                if depth:
                    db.execute(f"SAVEPOINT canonical_{depth}")
                    try:
                        yield db
                    except BaseException:
                        db.execute(f"ROLLBACK TO canonical_{depth}")
                        raise
                    finally:
                        db.execute(f"RELEASE canonical_{depth}")
                else:
                    with db:
                        db.execute("BEGIN")
                        yield db
            finally:
                self._transaction_depth -= 1

    def close(self):
        with self.lock:
            if self._transaction_depth:
                raise RuntimeError("Cannot close canonical index during a transaction")
            if self._db is not None:
                self._db.close()
                self._db = None
            self._closed = True

    def __del__(self):
        # Also release handles for short-lived indexes and failed initialization.
        db = getattr(self, "_db", None)
        if db is not None:
            db.close()

    def rows(self, kind, account=None, session=None):
        with self.transaction() as db:
            rows = records.rows(db, "local", kind, account)
        return [row for row in rows if (account is None or record_account_key(row) == account) and (session is None or row.get("sessionId") == session)]

    def ledger_cost(self):
        from monitor_token_ledger import token_cost_snapshot, token_sessions_from_ledger
        with self.transaction() as db:
            cached = self.get(db, "canonicalCost:local")
            if cached is None:
                cached = token_cost_snapshot(token_sessions_from_ledger(records.rows(db, "local", "tokenLedger")))
                self.put(db, "canonicalCost:local", cached)
            return tuple(cached)

    def usage_totals(self, db):
        cached = self.get(db, "canonicalUsageTotals")
        if cached is None:
            from monitor_common import empty_token_totals
            cached = {"totals": empty_token_totals(), "byModel": {}, "fastByModel": {}}
            for row in records.rows(db, "local", "tokenLedger"):
                self.add_usage(cached, row)
            self.put(db, "canonicalUsageTotals", cached)
        return cached

    @staticmethod
    def add_usage(cached, row):
        from monitor_common import empty_token_totals
        for name, amount in row["tokens"].items():
            cached["totals"][name] += amount
            cached["byModel"].setdefault(row["rawModel"], empty_token_totals())[name] += amount
            if row.get("serviceTier") == "fast":
                cached["fastByModel"].setdefault(row["rawModel"], empty_token_totals())[name] += amount

    def revision(self):
        with self.transaction() as db:
            return self.get(db, "canonicalRevision", 0)

    def repair(self):
        from monitor_delivery import request_attribution_rebuild
        with self.transaction() as db:
            for table in ("delivery_run_refs", "delivery_token_refs", "delivery_charts", "delivery_accounts", "delivery_work", "source_checkpoints", "source_blocks"):
                db.execute(f"DELETE FROM {table}")
            request_attribution_rebuild(db)
            db.execute("DELETE FROM processing_meta WHERE name LIKE 'canonicalCost:%' OR name='canonicalUsageTotals'")


class CanonicalDataStore(NormalizedRecords):
    def __init__(self, quota_path, token_ledger_path, machine_id, account_id_resolver, lock, account_mapper=None, cache_path=None):
        from monitor_usage_sync import default_usage_sync_cache_path
        self.quota_path, self.token_ledger_path = Path(quota_path), Path(token_ledger_path)
        self.cache_path = Path(cache_path) if cache_path is not None else default_usage_sync_cache_path(self.quota_path)
        self.v4_cache_path = self.cache_path.with_name(f"{self.cache_path.stem}-v4.sqlite3")
        self.machine_id, self.account_id_resolver, self.lock = machine_id, account_id_resolver, lock
        self.account_mapper = account_mapper
        self.index = CanonicalIndex(self.v4_cache_path)
        from monitor_canonical_ingestion import CanonicalSourceIngestor
        self.sources = CanonicalSourceIngestor(self)

    def refresh_accounts(self):
        # Canonical account IDs are independent of credential slots and labels.
        with self.index.transaction() as db:
            for account in {account for _, account in records.accounts(db)}:
                mapped = self.account_mapper(account, None, None) if self.account_mapper else None
                if mapped:
                    records.update_account(db, account, mapped[0], mapped[1])
            records.dirty_all(db)

    def rename_account(self, account_id, label):
        with self.index.transaction() as db:
            records.rename_account(db, account_id, label)

    def datasets(self, view="local"):
        with self.index.transaction() as db:
            return tuple(records.rows(db, view, kind) for kind in ("quota", "tokenLedger"))

    def buffer_status(self):
        with self.index.transaction() as db:
            count, oldest = db.execute("SELECT COUNT(*),MIN(arrived) FROM upload_buffer").fetchone()
        return {"pendingRecords": count, "oldestBufferedAt": oldest, "nextUploadAt": oldest + 1800 if oldest is not None else None}

    def _enqueue(self, db, kind, row, now=None):
        if row.get("sync", {}).get("localOnly"):
            return
        if kind == "quota":
            for label, window in row["windows"].items():
                db.execute("INSERT OR IGNORE INTO upload_buffer(record_key,arrived,data) VALUES(?,?,?)", (record_key(kind, row) + ":" + label,
                    time.time() if now is None else now, canonical_json(self._transport_record(kind, row | {"windows": {label: window}})).decode()))
            return
        db.execute("INSERT OR IGNORE INTO upload_buffer(record_key,arrived,data) VALUES(?,?,?)", (
            record_key(kind, row), time.time() if now is None else now, canonical_json(self._transport_record(kind, row)).decode()))

    def _changed(self, db, view, kind, account, key):
        db.execute("INSERT OR IGNORE INTO delivery_dirty VALUES(?,?)", (view, account))
        db.execute("INSERT OR IGNORE INTO delivery_work VALUES(?,?,?,?)", (view, account, kind, key))
        self.index.put(db, "canonicalRevision", self.index.get(db, "canonicalRevision", 0) + 1)

    def _save(self, db, view, kind, row):
        row = {"accountSlotId": "unknown", "accountLabel": "Unknown"} | row
        stamp = int(parse_timestamp(row.get("checkedAt") or row.get("occurredAt")))
        last = max((window.get("lastAt", stamp) for window in row.get("windows", {}).values()), default=stamp)
        key, account = record_key(kind, row), record_account_key(row)
        records.write(db, view, kind, row, key, account, (stamp, last))
        if kind == "tokenLedger" and view == "local":
            from monitor_tokens import normalize_codex_model, sum_cost_totals
            cost = self.index.get(db, "canonicalCost:local")
            if cost is not None:
                cost[0] = sum_cost_totals(cost[0], row["cost"])
                model = normalize_codex_model(row["rawModel"])
                cost[1][model] = sum_cost_totals(cost[1].get(model), row["cost"])
                self.index.put(db, "canonicalCost:local", cost)
            usage = self.index.get(db, "canonicalUsageTotals")
            if usage is not None:
                self.index.add_usage(usage, row)
                self.index.put(db, "canonicalUsageTotals", usage)
        self._changed(db, view, kind, account, key)

    @staticmethod
    def _fragment(row, label, first, last):
        row = copy.deepcopy(row)
        row["checkedAt"] = datetime.fromtimestamp(first, timezone.utc).isoformat()
        row["windows"] = {label: row["windows"][label] | {"firstAt": first, "lastAt": last}}
        row["sync"]["recordId"] = hashlib.sha256(canonical_json([record_account_key(row), label, first, last, row["windows"][label]])).hexdigest()
        return row

    def _quota_merge(self, db, view, row):
        """Overlay only overlapping spans, retaining explicit observations and barriers."""
        for label, window in row["windows"].items():
            first, last = int(window["firstAt"]), int(window["lastAt"])
            account = record_account_key(row)
            existing = records.items(db, view, "quota", account, condition="window=? AND first<=? AND last>=?", parameters=(label, last, first))
            if first == last and any(old["windows"][label]["firstAt"] == old["windows"][label]["lastAt"] == first for _, old in existing):
                continue
            spans = [(first, last)]
            for key, old in existing:
                prior = old["windows"][label]
                a, b = int(prior["firstAt"]), int(prior["lastAt"])
                if a == b:
                    spans = [(x, y) for start, end in spans for x, y in ((start, min(end, a - 1)), (max(start, a + 1), end)) if x <= y]
                    continue
                if first == last and all(prior.get(field) == window.get(field) for field in ("usedPercent", "continuous", "multiplier", "resetAt", "plan")):
                    return
                records.delete(db, view, "quota", account, key)
                for a, b in ((a, min(b, first - 1)), (max(a, last + 1), b)):
                    if a <= b:
                        self._save(db, view, "quota", self._fragment(old, label, a, b))
            for a, b in spans:
                incoming = self._fragment(row, label, a, b)
                # Extend a compatible predecessor only when no intervening evidence exists.
                previous = records.items(db, view, "quota", account, condition="window=? AND last<?", parameters=(label, a), order="last DESC", limit=1)
                if previous:
                    prior = previous[0][1]
                    value = prior.get("windows", {}).get(label)
                    if value and window["continuous"] is not None and not window["resetBoundary"] and all(value.get(field) == window.get(field) for field in ("usedPercent", "continuous", "multiplier", "resetAt", "plan")):
                        records.delete(db, view, "quota", account, previous[0][0])
                        incoming = self._fragment(incoming, label, int(value["firstAt"]), b)
                self._save(db, view, "quota", incoming)

    def _quota_interpret(self, db, row, view):
        from monitor_sampling import WINDOWS, quota_point, sample_point
        account = record_account_key(row)
        state = self.index.get(db, f"samplingState:{view}:{account}", {"windows": {}})
        point, resets = quota_point(row), []
        stamp = int(point["timestamp"])
        if stamp < state.get("timestamp", -1):
            state = {"windows": {}}
            for name, (label, _) in WINDOWS.items():
                previous = records.rows(db, view, "quota", account, condition="window=? AND continuous IS NOT NULL AND first<?",
                    parameters=(label, stamp), order="first DESC", limit=1)
                for previous_row in previous:
                    window = previous_row.get("windows", {}).get(label)
                    if window and window.get("continuous") is not None:
                        state["windows"][name] = {"raw": window["continuous"], "timestamp": min(window["lastAt"], stamp - 1), "resetAt": window.get("resetAt"), "plan": window["plan"]}
                        break
        sample_point(point, state["windows"], resets)
        state["timestamp"] = stamp
        if stamp >= self.index.get(db, f"samplingState:{view}:{account}", {}).get("timestamp", -1):
            self.index.put(db, f"samplingState:{view}:{account}", state)
        if view == "local":
            self.index.put(db, f"quotaObservation:{account}", point)
        result = []
        for observation in resets + [point]:
            for name, (label, _) in WINDOWS.items():
                value = observation[name]
                if label not in row["windows"] or value["raw"] is None or observation.get("resetWindow") not in (None, name):
                    continue
                stamp = int(observation["timestamp"])
                canonical = copy.deepcopy(row) | {"quotaRunVersion": 1, "windows": {label: {
                    "usedPercent": value["raw"], "continuous": value["continuous"], "multiplier": value.get("multiplier", point[name]["multiplier"]),
                    "resetAt": value.get("resetAt"), "plan": value["plan"], "firstAt": stamp, "lastAt": stamp, "resetBoundary": bool(observation.get("resetWindow"))}}}
                result.append(self._fragment(canonical, label, stamp, stamp))
        return result

    def _apply(self, db, kind, row, local=False):
        key = record_key(kind, row)
        views = ("local", "merged") if local else ("merged",)
        changed = False
        canonical = None
        local_row = None
        for view in views:
            if kind == "quota":
                labels = [label for label in row["windows"] if db.execute("INSERT OR IGNORE INTO canonical_seen VALUES(?,?,?)", (view, kind, key + ":" + label)).rowcount]
                if not labels:
                    continue
                if view == "local":
                    local_row = row | {"windows": {label: row["windows"][label] for label in labels}}
                if canonical is None:
                    canonical = self._quota_interpret(db, row | {"windows": {label: row["windows"][label] for label in labels}}, view)
                for item in canonical:
                    if any(label in item["windows"] for label in labels):
                        self._quota_merge(db, view, item)
            else:
                if not db.execute("INSERT OR IGNORE INTO canonical_seen VALUES(?,?,?)", (view, kind, key)).rowcount:
                    continue
                if view == "local":
                    local_row = row
                self._save(db, view, kind, row)
            changed = True
        if local_row is not None:
            self._enqueue(db, kind, local_row)
        self.index.metrics["recordsImported"] += int(changed)
        return changed

    def record_quota(self, sample):
        from monitor_history import quota_history_rows_from_sample
        with self.lock, self.index.transaction() as db:
            changed = False
            for row in quota_history_rows_from_sample(sample):
                if (row := self.normalize_new_quota(row)) is not None:
                    changed |= self._apply(db, "quota", row, True)
            return changed

    def record_ledger(self, rows):
        with self.lock, self.index.transaction() as db:
            return self._record_ledger(db, rows)

    def _record_ledger(self, db, rows):
        changed = False
        for row in rows:
            if (row := self.normalize_new_ledger(row)) is not None:
                changed |= self._apply(db, "tokenLedger", row, True)
        return changed

    def import_package(self, destination, machine, package, records):
        interpreted = []
        for record in records:
            row = record["row"]
            mapped = self.account_mapper(record_account_key(row), row.get("accountSlotId"), row.get("accountLabel")) if self.account_mapper else None
            interpreted.append(record | {"row": row | {"accountSlotId": mapped[0] if mapped else "unknown", "accountLabel": mapped[1] if mapped else "Unknown"}})
        with self.lock, self.index.transaction() as db:
            if db.execute("SELECT 1 FROM package_receipts WHERE destination=? AND machine=? AND package=?", (destination, machine, package)).fetchone():
                return False
            for record in sorted(interpreted, key=lambda value: (parse_timestamp(value["row"].get("checkedAt") or value["row"].get("occurredAt")) or 0, record_key(value["kind"], value["row"]))):
                self._apply(db, record["kind"], record["row"])
            db.execute("INSERT INTO package_receipts VALUES(?,?,?)", (destination, machine, package))
            return True
