
"""Prepared account snapshots and durable, independently sequenced live channels."""

import gzip
import hashlib
import json
import threading
import time
import uuid
from contextlib import closing

import monitor_sample_store as samples
from monitor_common import parse_timestamp
from monitor_interpretation import WINDOWS, aggregate_tokens, chart_rows, token_row, usage_runs, valid_intervals

DAY = 86400
JOURNAL_BYTES = 64 * 1024 * 1024


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def initialize_delivery(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS delivery_meta(name TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS delivery_dirty(view TEXT,account TEXT,PRIMARY KEY(view,account));
        CREATE TABLE IF NOT EXISTS delivery_work(view TEXT,account TEXT,kind TEXT,record_key TEXT,PRIMARY KEY(view,account,kind,record_key));
        CREATE TABLE IF NOT EXISTS delivery_accounts(view TEXT,account TEXT,legacy TEXT NOT NULL,label TEXT NOT NULL DEFAULT 'Unknown',PRIMARY KEY(view,account));
        CREATE TABLE IF NOT EXISTS delivery_runs(view TEXT,account TEXT,window TEXT,id TEXT,first REAL,last REAL,value REAL,multiplier REAL,flags INTEGER,reset REAL,plan TEXT,PRIMARY KEY(view,account,window,id));
        CREATE INDEX IF NOT EXISTS delivery_run_time ON delivery_runs(view,account,window,first);
        CREATE TABLE IF NOT EXISTS delivery_tokens(view TEXT,account TEXT,id TEXT,at REAL,model TEXT,tier TEXT,fresh INTEGER,cached INTEGER,output INTEGER,cache_write INTEGER,cost INTEGER,PRIMARY KEY(view,account,id));
        CREATE INDEX IF NOT EXISTS delivery_token_time ON delivery_tokens(view,account,at);
        CREATE TABLE IF NOT EXISTS delivery_charts(view TEXT,account TEXT,window TEXT,id TEXT,start REAL,end REAL,at REAL,multiplier REAL,delta REAL,models TEXT,PRIMARY KEY(view,account,window,id));
        CREATE TABLE IF NOT EXISTS delivery_members(view TEXT,account TEXT,window TEXT,event TEXT,interval TEXT,PRIMARY KEY(view,account,window,event));
        CREATE INDEX IF NOT EXISTS delivery_member_interval ON delivery_members(view,account,window,interval);
        CREATE TABLE IF NOT EXISTS delivery_journal(stream TEXT,seq INTEGER,created REAL,body BLOB,PRIMARY KEY(stream,seq));
        CREATE TABLE IF NOT EXISTS delivery_snapshot(id INTEGER PRIMARY KEY CHECK(id=1),body BLOB,gzip BLOB,created REAL);
    """)
    for table in ("processing_quota_points", "processing_display"):
        for action, ref in (("INSERT", "NEW"), ("UPDATE", "NEW"), ("DELETE", "OLD")):
            db.execute(f"CREATE TRIGGER IF NOT EXISTS delivery_{table}_{action.lower()} AFTER {action} ON {table} BEGIN INSERT OR IGNORE INTO delivery_dirty VALUES({ref}.view,{ref}.account); END")
    db.execute("INSERT OR IGNORE INTO delivery_meta VALUES('epoch',?)", (uuid.uuid4().hex,))
    for stream in ("local", "merged", "state"):
        db.execute("INSERT OR IGNORE INTO delivery_meta VALUES(?, '0')", (stream,))
    if not db.execute("SELECT 1 FROM delivery_meta WHERE name='initialized'").fetchone():
        db.execute("INSERT OR IGNORE INTO delivery_dirty SELECT DISTINCT view,account FROM processing_quota_points")
        db.execute("INSERT OR IGNORE INTO delivery_dirty SELECT DISTINCT view,account FROM processing_display")
        db.execute("INSERT INTO delivery_meta VALUES('initialized','1')")


def numeric_state(value):
    if isinstance(value, list):
        return [numeric_state(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key in {"processing", "revision", "plan", "statusBarText", "resetText", "remainingText", "tooltip", "lastUpdateText", "timePercent", "timeText", "configHeaderToml", "cost", "costByModel", "tokenUsage"}:
            continue
        if key in {"checkedAt", "percentCheckedAt", "pollErrorAt", "startedAt", "updatedAt", "resetAt"}:
            result[key] = parse_timestamp(item)
        else:
            result[key] = numeric_state(item)
    return result


class DashboardDelivery:
    def __init__(self, index):
        self.index = index
        self.lock = threading.RLock()
        self.changed = threading.Condition()
        self.cloud_pending = False
        self.last_prepare = 0
        self.pending_snapshot = False
        self.resetting = False
        self.metrics = {"accountsPrepared": 0, "bulkBytes": 0, "gzipBytes": 0, "prepareSeconds": 0}
        with closing(samples.connect(index.path)) as db:
            row = db.execute("SELECT body,gzip FROM delivery_snapshot WHERE id=1").fetchone()
            self.snapshot = tuple(row) if row else None
            row = db.execute("SELECT value FROM delivery_meta WHERE name='statePayload'").fetchone()
            self.state = json.loads(row[0]) if row else None
            self.head = (db.execute("SELECT value FROM delivery_meta WHERE name='epoch'").fetchone()[0], self.cursors(db))
        if self.snapshot:
            saved = json.loads(self.snapshot[0])
            self.pending_snapshot = saved["epoch"] != self.head[0] or saved["cursors"] != self.head[1]
            if self.pending_snapshot and self.replay(saved["epoch"], saved["cursors"]) is None:
                self.snapshot = None

    @staticmethod
    def cursors(db):
        return {stream: int(value) for stream, value in db.execute("SELECT name,value FROM delivery_meta WHERE name IN ('local','merged','state')")}

    def _packet(self, db, stream, channel, account, data, now):
        sequence = int(db.execute("SELECT value FROM delivery_meta WHERE name=?", (stream,)).fetchone()[0]) + 1
        db.execute("UPDATE delivery_meta SET value=? WHERE name=?", (str(sequence), stream))
        packet = {"epoch": db.execute("SELECT value FROM delivery_meta WHERE name='epoch'").fetchone()[0], "stream": stream, "seq": sequence, "channel": channel, "account": account, "data": data}
        db.execute("INSERT INTO delivery_journal VALUES(?,?,?,?)", (stream, sequence, now, encoded(packet)))

    @staticmethod
    def _runs(db, view, account, window):
        return [list(row) for row in db.execute("SELECT id,first,last,value,multiplier,flags,reset,plan FROM delivery_runs WHERE view=? AND account=? AND window=? ORDER BY first,id", (view, account, window))]

    @staticmethod
    def _tokens(db, view, account):
        return [list(row) for row in db.execute("SELECT id,at,model,tier,fresh,cached,output,cache_write,cost FROM delivery_tokens WHERE view=? AND account=? ORDER BY at,id", (view, account))]

    def _account(self, db, projector, view, account, now):
        existing = db.execute("SELECT legacy,label FROM delivery_accounts WHERE view=? AND account=?", (view, account)).fetchone()
        work = list(db.execute("SELECT kind,record_key FROM delivery_work WHERE view=? AND account=?", (view, account)))
        legacy = json.loads(existing[0]) if existing else []
        events = []
        label = existing[1] if existing else "Unknown"
        if existing is None or self.cloud_pending:
            if projector is not None:
                _, events = projector._datasets(db, view, account)
            legacy = [row["session"] for row in events if row.get("recordType") == "sessionBaseline" and isinstance(row.get("session"), dict)]
            label = next((row.get("accountLabel") or (row.get("session") or {}).get("accountLabel") for row in reversed(events) if row.get("accountLabel") or (row.get("session") or {}).get("accountLabel")), label)
        elif projector is not None:
            for kind, key in work:
                if kind != "tokenLedger":
                    continue
                if view == "merged":
                    rows = db.execute("SELECT data FROM processing_effective WHERE account=? AND record_key=?", (account, key))
                else:
                    rows = db.execute("SELECT data FROM processing_facts WHERE source=? AND kind='tokenLedger' AND account=? AND record_key=?", ("local:tokenLedger", account, key))
                events.extend(json.loads(row[0]) for row in rows)
        if existing is None:
            point = db.execute("SELECT data FROM processing_quota_points WHERE view=? AND account=? ORDER BY event_at DESC LIMIT 1", (view, account)).fetchone()
            if point:
                label = json.loads(point[0]).get("accountLabel") or label
            self._packet(db, view, "account", account, {"label": label}, now)
        if existing is not None and self.cloud_pending:
            desired = {row[0]: row for event in events if (row := token_row(event)) is not None}
            before = {row[0]: row for row in self._tokens(db, view, account)}
            if before != desired or json.loads(existing[0]) != legacy:
                self.force_epoch = True
                db.execute("DELETE FROM delivery_tokens WHERE view=? AND account=?", (view, account))
        additions, added_rows = [], []
        for event in events:
            row = token_row(event)
            if row is not None and not db.execute("SELECT 1 FROM delivery_tokens WHERE view=? AND account=? AND id=?", (view, account, row[0])).fetchone():
                db.execute("INSERT INTO delivery_tokens VALUES(?,?,?,?,?,?,?,?,?,?,?)", (view, account, *row))
                additions.append([row[0], row[1], [[row[2], row[3], *row[4:]]]])
                added_rows.append(row)
        if additions:
            self._packet(db, view, "tokens", account, additions, now)
        intervals = {}
        for window, _ in WINDOWS:
            old = self._runs(db, view, account, window)
            if existing is None or not work or any(kind == "quota" for kind, _ in work):
                points = [json.loads(row[0]) for row in db.execute("SELECT data FROM processing_quota_points WHERE view=? AND account=? ORDER BY event_at,record_key", (view, account))]
                label = next((row["accountLabel"] for row in reversed(points) if row.get("accountLabel")), label)
                runs = usage_runs(points, window)
            else:
                runs = old
            before, after = {row[0]: row for row in old}, {row[0]: row for row in runs}
            remove = [key for key in before if key not in after]
            update = [row for key, row in after.items() if before.get(key) != row]
            for key in remove:
                db.execute("DELETE FROM delivery_runs WHERE view=? AND account=? AND window=? AND id=?", (view, account, window, key))
            for row in update:
                db.execute("INSERT OR REPLACE INTO delivery_runs VALUES(?,?,?,?,?,?,?,?,?,?,?)", (view, account, window, *row))
            if remove or update:
                self._packet(db, view, "usage:" + window, account, {"set": [row[:6] for row in update], "remove": remove}, now)
                latest = next((row for row in reversed(runs) if row[3] is not None), None)
                previous = next((row for row in reversed(old) if row[3] is not None), None)
                if (latest[6] if latest else None) != (previous[6] if previous else None):
                    self._packet(db, view, "quota:" + window, account, [latest[6] if latest else None], now)
            intervals[window] = valid_intervals(runs)
        fast_tokens = existing is not None and not self.cloud_pending and work and all(kind == "tokenLedger" for kind, _ in work)
        tokens = None if fast_tokens else self._tokens(db, view, account)
        for window, values in intervals.items():
            from monitor_interpretation import nearest
            ends = [row[2] for row in values]
            if fast_tokens:
                affected = set()
                for row in added_rows:
                    index = nearest(values, row[1], ends)
                    if index is not None:
                        interval = values[index][0]
                        affected.add(interval)
                        db.execute("INSERT OR REPLACE INTO delivery_members VALUES(?,?,?,?,?)", (view, account, window, row[0], interval))
                rows = []
                for interval in values:
                    if interval[0] not in affected:
                        continue
                    events = [list(row) for row in db.execute("SELECT t.id,t.at,t.model,t.tier,t.fresh,t.cached,t.output,t.cache_write,t.cost FROM delivery_members m JOIN delivery_tokens t ON t.view=m.view AND t.account=m.account AND t.id=m.event WHERE m.view=? AND m.account=? AND m.window=? AND m.interval=?", (view, account, window, interval[0]))]
                    rows.extend(chart_rows(events, [interval]))
            else:
                db.execute("DELETE FROM delivery_members WHERE view=? AND account=? AND window=?", (view, account, window))
                members = []
                for row in tokens:
                    index = nearest(values, row[1], ends)
                    if index is not None:
                        members.append((view, account, window, row[0], values[index][0]))
                db.executemany("INSERT INTO delivery_members VALUES(?,?,?,?,?)", members)
                rows = chart_rows(tokens, values)
            before = {row[0]: list(row[:6]) + [json.loads(row[6])] for row in db.execute("SELECT id,start,end,at,multiplier,delta,models FROM delivery_charts WHERE view=? AND account=? AND window=?", (view, account, window))}
            after = {row[0]: row for row in rows}
            remove = [] if fast_tokens else [key for key in before if key not in after]
            update = [row for key, row in after.items() if before.get(key) != row]
            for key in remove:
                db.execute("DELETE FROM delivery_charts WHERE view=? AND account=? AND window=? AND id=?", (view, account, window, key))
            for row in update:
                db.execute("INSERT OR REPLACE INTO delivery_charts VALUES(?,?,?,?,?,?,?,?,?,?)", (view, account, window, *row[:6], encoded(row[6]).decode()))
            if remove or update:
                self._packet(db, view, "chart:" + window, account, {"set": update, "remove": remove}, now)
        db.execute("INSERT OR REPLACE INTO delivery_accounts VALUES(?,?,?,?)", (view, account, encoded(legacy).decode(), label))
        db.execute("DELETE FROM delivery_work WHERE view=? AND account=?", (view, account))
        db.execute("DELETE FROM delivery_dirty WHERE view=? AND account=?", (view, account))
        self.metrics["accountsPrepared"] += 1

    def prepare(self, now=None, projector=None, status=None, force=False):
        with self.lock:
            return self._prepare(now, projector, status, force)

    def _prepare(self, now=None, projector=None, status=None, force=False):
        now = time.time() if now is None else now
        started = time.perf_counter()
        with self.index.transaction() as db:
            self.force_epoch = False
            cursors = self.cursors(db)
            for view, account in list(db.execute("SELECT view,account FROM delivery_dirty")):
                self._account(db, projector, view, account, now)
            if status is not None:
                from monitor_dashboard import dashboard_safe_json
                state = numeric_state(dashboard_safe_json(status))
                state["revision"] = hashlib.sha256(encoded(state)).hexdigest()
            else:
                state = self.state or {}
            if state.get("revision") != (self.state or {}).get("revision") or self.state is None:
                self._packet(db, "state", "state", None, state, now)
                db.execute("INSERT OR REPLACE INTO delivery_meta VALUES('statePayload',?)", (encoded(state).decode(),))
            changed = cursors != self.cursors(db)
            old_accounts = {row.get("usageAccountId") for row in (self.state or {}).get("accounts", {}).get("items", [])}
            new_accounts = {row.get("usageAccountId") for row in state.get("accounts", {}).get("items", [])}
            self.force_epoch |= old_accounts != new_accounts and self.state is not None
            if self.cloud_pending:
                if any(cursors[v] != self.cursors(db)[v] for v in ("local", "merged")):
                    self.force_epoch = True
                self.cloud_pending = False
            if self.force_epoch:
                self.resetting = True
                db.execute("UPDATE delivery_meta SET value=? WHERE name='epoch'", (uuid.uuid4().hex,))
                db.execute("DELETE FROM delivery_journal")
                force = True
            db.execute("DELETE FROM delivery_journal WHERE created<?", (now - DAY,))
            size = db.execute("SELECT COALESCE(SUM(length(body)),0) FROM delivery_journal").fetchone()[0]
            while size > JOURNAL_BYTES:
                oldest = db.execute("SELECT stream,seq,length(body) FROM delivery_journal ORDER BY created,stream,seq LIMIT 1").fetchone()
                db.execute("DELETE FROM delivery_journal WHERE stream=? AND seq=?", oldest[:2])
                size -= oldest[2]
            self.pending_snapshot |= changed
            need_snapshot = force or self.snapshot is None or self.pending_snapshot and now - self.last_prepare >= 1
            head = (db.execute("SELECT value FROM delivery_meta WHERE name='epoch'").fetchone()[0], self.cursors(db))
        self.state = state
        if not self.resetting:
            self.head = head
        with self.changed:
            self.changed.notify_all()
        # Serialization and compression never hold the ingestion writer lock.
        if need_snapshot:
            with closing(samples.connect(self.index.path)) as db:
                db.execute("BEGIN")
                body = encoded(self._bulk(db, state))
            compressed = gzip.compress(body, compresslevel=3, mtime=0)
            with self.index.transaction() as db:
                db.execute("INSERT OR REPLACE INTO delivery_snapshot VALUES(1,?,?,?)", (body, compressed, now))
            self.snapshot = (body, compressed)
            self.head = head
            self.resetting = False
            self.last_prepare = now
            self.pending_snapshot = False
            self.metrics.update(bulkBytes=len(body), gzipBytes=len(compressed))
        self.metrics["prepareSeconds"] = round(time.perf_counter() - started, 6)
        with self.changed:
            self.changed.notify_all()
        return changed

    def _bulk(self, db, state):
        models = sorted({row[0] for row in db.execute("SELECT DISTINCT model FROM delivery_tokens")})
        def model(name):
            if name not in models:
                models.append(name)
            return models.index(name)
        accounts = []
        api = {item["usageAccountId"] for item in state.get("accounts", {}).get("items", []) if item.get("isApiAccount")}
        known = {item["id"] for item in state.get("accounts", {}).get("usageGroups", [])} | {item["usageAccountId"] for item in state.get("accounts", {}).get("items", [])}
        for account, in db.execute("SELECT DISTINCT account FROM delivery_accounts ORDER BY account"):
            item = {"id": account, "label": db.execute("SELECT label FROM delivery_accounts WHERE account=? ORDER BY view LIMIT 1", (account,)).fetchone()[0]}
            for view in ("local", "merged"):
                runs = {window: self._runs(db, view, account, window) for window, _ in WINDOWS}
                intervals = {window: valid_intervals(rows) for window, rows in runs.items()}
                tokens = aggregate_tokens(self._tokens(db, view, account), intervals, account in api)
                for row in tokens:
                    for entry in row[2]:
                        entry[0] = model(entry[0])
                data = {"tokens": tokens, "quota": {window: next((row[6] for row in reversed(rows) if row[3] is not None), None) for window, rows in runs.items()}}
                for window in runs:
                    data["usage:" + window] = [row[:6] for row in runs[window]]
                    charts = [list(row[:6]) + [json.loads(row[6])] for row in db.execute("SELECT id,start,end,at,multiplier,delta,models FROM delivery_charts WHERE view=? AND account=? AND window=? ORDER BY start,id", (view, account, window))]
                    for row in charts:
                        for entry in row[6]:
                            entry[0] = model(entry[0])
                    data["chart:" + window] = charts
                legacy = db.execute("SELECT legacy FROM delivery_accounts WHERE view=? AND account=?", (view, account)).fetchone()
                data["legacy"] = []
                for session in json.loads(legacy[0]) if legacy else []:
                    entries = []
                    for name, value in (session.get("byModel") or {}).items():
                        row = token_row({"recordType": "usage", "occurredAt": session.get("updatedAt"), "rawModel": name, **value})
                        if row:
                            entries.append([model(name), row[3], *row[4:]])
                    data["legacy"].append([parse_timestamp(session.get("startedAt")), parse_timestamp(session.get("updatedAt")), entries])
                item[view] = data
            local = item["local"]
            if not known or account in known or account == "unknown" or local["tokens"] or local["usage:5h"] or local["usage:7d"] or local["legacy"]:
                accounts.append(item)
        return {"epoch": db.execute("SELECT value FROM delivery_meta WHERE name='epoch'").fetchone()[0], "cursors": self.cursors(db), "models": models, "state": state, "accounts": accounts}

    def bulk(self, compressed=True):
        return self.snapshot[1 if compressed else 0] if self.snapshot else None

    def replay(self, epoch, cursors):
        if not isinstance(cursors, dict) or not cursors or any(stream not in {"local", "merged", "state"} or type(sequence) is not int or sequence < 0 for stream, sequence in cursors.items()):
            raise ValueError("Invalid stream cursor")
        if self.resetting and epoch == self.head[0]:
            return []
        if epoch == self.head[0] and all(sequence == self.head[1][stream] for stream, sequence in cursors.items()):
            return []
        with closing(samples.connect(self.index.path)) as db:
            db.execute("BEGIN")
            if epoch != db.execute("SELECT value FROM delivery_meta WHERE name='epoch'").fetchone()[0]:
                return None
            result = []
            for stream, sequence in cursors.items():
                if stream not in {"local", "merged", "state"} or type(sequence) is not int or sequence < 0:
                    raise ValueError("Invalid stream cursor")
                current = int(db.execute("SELECT value FROM delivery_meta WHERE name=?", (stream,)).fetchone()[0])
                rows = db.execute("SELECT seq,body FROM delivery_journal WHERE stream=? AND seq>? ORDER BY seq", (stream, sequence))
                expected = sequence + 1
                size = 0
                for number, body in rows:
                    size += len(body)
                    if number != expected or size > 4 * 1024 * 1024:
                        return None
                    result.append(json.loads(body))
                    expected += 1
                if sequence > current or expected != current + 1:
                    return None
            return result
