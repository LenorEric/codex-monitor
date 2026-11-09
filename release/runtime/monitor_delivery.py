"""Canonical -> final wire rows, with bounded-memory incremental publication.

Only final output and unconsumed invalidations are durable. No event copies or
event-to-interval memberships are retained. All expensive work shares one lock.
"""

import gzip
import hashlib
import json
import math
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from collections import defaultdict
from contextlib import closing, contextmanager

from monitor_interpretation import MAX_ATTRIBUTION_DISTANCE_SECONDS, connected, nearest, valid_intervals


CHANNELS = ("tokens", "usage:5h", "usage:7d", "chart:5h", "chart:7d")
ITEM_COLUMNS = "view,account,channel,id,at,first,last,body"
ITEM_DDL = "(view TEXT,account TEXT,channel TEXT,id TEXT,at REAL,first REAL,last REAL,body TEXT,PRIMARY KEY(view,account,channel,id))"
DAY = 86400
JOURNAL_BYTES = 64 * 1024 * 1024


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def initialize(db):
    statements = [
        "CREATE TABLE IF NOT EXISTS delivery_meta(name TEXT PRIMARY KEY,value TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS delivery_accounts(view TEXT,account TEXT,legacy TEXT NOT NULL DEFAULT '[]',label TEXT NOT NULL DEFAULT 'Unknown',PRIMARY KEY(view,account))",
        "CREATE TABLE IF NOT EXISTS delivery_items" + ITEM_DDL,
        "CREATE INDEX IF NOT EXISTS delivery_items_time ON delivery_items(view,account,channel,first,last)",
        "CREATE INDEX IF NOT EXISTS delivery_items_end ON delivery_items(view,account,channel,last)",
        "CREATE INDEX IF NOT EXISTS delivery_items_order ON delivery_items(view,account,channel,at,id)",
        "CREATE TABLE IF NOT EXISTS delivery_models(name TEXT PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS delivery_pending(seq INTEGER PRIMARY KEY AUTOINCREMENT,view TEXT,account TEXT,kind TEXT,record_key TEXT,window TEXT,first REAL,last REAL)",
        "CREATE INDEX IF NOT EXISTS delivery_pending_scope ON delivery_pending(view,account,seq)",
        "CREATE UNIQUE INDEX IF NOT EXISTS delivery_pending_metadata ON delivery_pending(view,account) WHERE kind='account'",
        "CREATE TABLE IF NOT EXISTS delivery_journal(stream TEXT,seq INTEGER,created REAL,body BLOB,PRIMARY KEY(stream,seq))",
    ]
    for statement in statements:
        db.execute(statement)
    db.execute("INSERT OR IGNORE INTO delivery_meta VALUES('epoch',?)", (uuid.uuid4().hex,))
    for stream in ("local", "merged", "state"):
        db.execute("INSERT OR IGNORE INTO delivery_meta VALUES(?,'0')", (stream,))
    # Source invalidations commit with facts, even when a caller writes SQL directly.
    for kind, table in (("tokenLedger", "canonical_tokens"), ("quota", "canonical_quota")):
        window = "NEW.window" if kind == "quota" else "NULL"
        db.execute(f"CREATE TRIGGER IF NOT EXISTS final_{table}_insert AFTER INSERT ON {table} BEGIN "
            f"INSERT INTO delivery_pending(view,account,kind,record_key,window,first,last) VALUES(NEW.dataset,NEW.account,'{kind}',NEW.record_key,{window},NEW.first,NEW.last); END")
        if kind == "quota":
            db.execute(f"CREATE TRIGGER IF NOT EXISTS final_{table}_delete AFTER DELETE ON {table} BEGIN "
                "INSERT INTO delivery_pending(view,account,kind,record_key,window,first,last) VALUES(OLD.dataset,OLD.account,'quota',OLD.record_key,OLD.window,OLD.first,OLD.last); END")
            db.execute(f"CREATE TRIGGER IF NOT EXISTS final_{table}_update AFTER UPDATE OF first,last,range_first,range_last,used_percent,multiplier,plan,reset_at ON {table} "
                "WHEN OLD.first IS NOT NEW.first OR OLD.last IS NOT NEW.last OR OLD.range_first IS NOT NEW.range_first OR OLD.range_last IS NOT NEW.range_last "
                "OR OLD.used_percent IS NOT NEW.used_percent OR OLD.multiplier IS NOT NEW.multiplier OR OLD.plan IS NOT NEW.plan OR OLD.reset_at IS NOT NEW.reset_at BEGIN "
                "INSERT INTO delivery_pending(view,account,kind,record_key,window,first,last) VALUES(OLD.dataset,OLD.account,'quota',OLD.record_key,OLD.window,OLD.first,OLD.last); "
                "INSERT INTO delivery_pending(view,account,kind,record_key,window,first,last) VALUES(NEW.dataset,NEW.account,'quota',NEW.record_key,NEW.window,NEW.first,NEW.last); END")
        db.execute(f"CREATE TRIGGER IF NOT EXISTS final_{table}_label AFTER UPDATE OF account_slot,account_label ON {table} "
            "WHEN OLD.account_slot IS NOT NEW.account_slot OR OLD.account_label IS NOT NEW.account_label BEGIN "
            "INSERT OR REPLACE INTO delivery_pending(view,account,kind) VALUES(NEW.dataset,NEW.account,'account'); END")
    if not db.execute("SELECT 1 FROM delivery_meta WHERE name='finalRows'").fetchone():
        # Empty/fresh databases need no replay. A missing derived store can backfill
        # once, with the same streaming algorithm used by explicit recovery.
        db.execute("INSERT INTO delivery_pending(view,account,kind) SELECT dataset,account,'bootstrap' FROM canonical_quota "
            "UNION SELECT dataset,account,'bootstrap' FROM canonical_tokens")
        db.execute("INSERT INTO delivery_meta VALUES('finalRows','1')")


def enqueue_accounts(db):
    db.execute("INSERT OR REPLACE INTO delivery_pending(view,account,kind) SELECT dataset,account,'account' FROM canonical_quota "
        "UNION SELECT dataset,account,'account' FROM canonical_tokens")


def enqueue_quota_repair(db):
    """Explicit recovery also removes orphan final rows; keep frozen tokens."""
    db.execute("INSERT INTO delivery_pending(view,account,kind,window,first,last) "
        "SELECT view,account,'quota',window,MIN(first),MAX(last) FROM "
        "(SELECT dataset AS view,account,window,first,last FROM canonical_quota UNION ALL "
        "SELECT view,account,substr(channel,7),first,last FROM delivery_items WHERE channel LIKE 'usage:%' OR channel LIKE 'chart:%') "
        "GROUP BY view,account,window")
    enqueue_accounts(db)


def usage_id(window, first, value, factor, plan):
    # End/reset/provenance may change when a constant run extends. Its visible
    # identity must not invalidate the incoming cost edge on every observation.
    return "q:" + hashlib.sha256(encoded([window, first, float(value), float(factor), plan])).hexdigest()[:32]


class Totals:
    """Streaming sufficient statistics, never a list of contributing events."""
    def __init__(self):
        self.models = defaultdict(lambda: [0] * 5)
        self.count = 0
        self.stamps = self.weighted = self.cost = 0

    def add(self, row):
        self.count += 1
        self.stamps += row[1]
        self.weighted += row[1] * row[-1]
        self.cost += row[-1]
        for index, value in enumerate(row[4:]):
            self.models[row[2], row[3]][index] += value

    def stamp(self, fallback=0):
        return math.floor((self.weighted / self.cost if self.cost else self.stamps / self.count) + .5) if self.count else int(fallback)

    def tokens(self, key):
        return [key, self.stamp(), [[model, tier, *values] for (model, tier), values in sorted(self.models.items())]]

    def chart(self, interval):
        costs = defaultdict(int)
        for (model, _), values in self.models.items():
            costs[model] += values[-1]
        return [interval[0], interval[1], interval[2], self.stamp(interval[2]), interval[4], interval[3],
            [[model, cost, interval[3] * cost / self.cost if self.cost else 0] for model, cost in sorted(costs.items())]]


class Snapshot:
    """One disk artifact shared by independent readers, including old versions."""
    def __init__(self):
        self.file = tempfile.NamedTemporaryFile(prefix="codex-delivery-snapshot-", suffix=".tmp", delete=False)
        self.path = self.file.name
        self.references = 1
        self.size = 0

    def release(self):
        self.references -= 1
        if not self.references:
            self.file.close()
            os.unlink(self.path)


def token_projection(db, view, account, low=None, high=None, pending=None):
    sql = "SELECT c.event_id,c.occurred_at,c.raw_model,c.service_tier,c.token_freshInputTokens,c.token_cachedInputTokens," \
        "c.token_outputTokens,c.token_cacheWriteInputTokens,c.cost_totalCostUsd FROM "
    args = []
    if pending is not None:
        # Drive identity work from the small pending set, then primary-key lookup.
        # Chronological ordering sorts only this batch, never all account tokens.
        sql += "delivery_pending p CROSS JOIN canonical_tokens c ON p.view=c.dataset AND p.account=c.account AND p.record_key=c.record_key AND p.kind='tokenLedger' "
    else:
        sql += "canonical_tokens c "
    sql += "WHERE c.dataset=? AND c.account=?"
    args.extend((view, account))
    if pending is not None:
        sql += " AND p.seq<=?"
        args.append(pending)
    if low is not None:
        sql += " AND c.first>=? AND c.first<=? AND c.occurred_at>=? AND c.occurred_at<=?"
        args.extend((math.floor(low), math.floor(high), low, high))
    sql += " ORDER BY c.first,c.occurred_at,c.event_id"
    for row in db.execute(sql, args):
        yield [*row[:-1], round(row[-1] * 100_000_000)]


class FinalDelivery:
    def __init__(self, index):
        self.index = index
        self.lock = threading.RLock()
        self.changed = threading.Condition()
        self.cloud_pending = False  # Accepted for old callers; no epoch reset.
        self._cache = {}
        self.metrics = {"accountsPrepared": 0, "sourceTokenRowsRead": 0, "sourceQuotaRowsRead": 0, "outputRowsWritten": 0,
            "bulkBytes": 0, "gzipBytes": 0, "prepareSeconds": 0}
        with closing(self._connect()) as db:
            self.head = self._head(db)
            row = db.execute("SELECT value FROM delivery_meta WHERE name='statePayload'").fetchone()
            self.state = json.loads(row[0]) if row else None

    def _connect(self):
        db = sqlite3.connect(self.index.path, timeout=10)
        db.execute("PRAGMA cache_size=-2048")
        db.execute("PRAGMA temp_store=FILE")
        db.execute("PRAGMA temp.cache_size=-2048")
        return db

    @staticmethod
    def cursors(db):
        return {name: int(value) for name, value in db.execute("SELECT name,value FROM delivery_meta WHERE name IN ('local','merged','state')")}

    @classmethod
    def _head(cls, db):
        return db.execute("SELECT value FROM delivery_meta WHERE name='epoch'").fetchone()[0], cls.cursors(db)

    @staticmethod
    def _runs(db, view, account, window):
        return [json.loads(body) for body, in db.execute("SELECT body FROM delivery_items WHERE view=? AND account=? AND channel=? "
            "ORDER BY first,json_extract(body,'$[3]') IS NULL,json_extract(body,'$[3]'),id", (view, account, "usage:" + window))]

    @staticmethod
    def _stage(db):
        db.execute("CREATE TABLE stage.edits" + ITEM_DDL)
        db.execute("CREATE INDEX stage.edits_end ON edits(view,account,channel,last,id)")
        db.execute("CREATE INDEX stage.edits_time ON edits(view,account,channel,first,id)")
        db.execute("CREATE TABLE stage.accounts(view TEXT,account TEXT,label TEXT,PRIMARY KEY(view,account))")
        db.execute("CREATE TABLE stage.recalc(view TEXT,account TEXT,channel TEXT,id TEXT,PRIMARY KEY(view,account,channel,id))")
        db.execute("CREATE TABLE stage.models(name TEXT PRIMARY KEY)")
        db.execute("CREATE TEMP VIEW items AS SELECT " + ITEM_COLUMNS + " FROM stage.edits WHERE body IS NOT NULL UNION ALL SELECT " +
            ",".join("m." + column for column in ITEM_COLUMNS.split(",")) + " FROM main.delivery_items m WHERE NOT EXISTS "
            "(SELECT 1 FROM stage.edits e WHERE e.view=m.view AND e.account=m.account AND e.channel=m.channel AND e.id=m.id)")

    @staticmethod
    def ordered_items(db, view, account, channel, condition, parameters, order, limit=None, staged=True):
        # An ORDER BY outside the overlay view forces SQLite to scan and sort
        # the whole candidate history, even for LIMIT 1. A compound ordered
        # query lets it merge two indexed cursors and stop immediately.
        where = "i.view=? AND i.account=? AND i.channel=? AND " + condition
        args = (view, account, channel, *parameters)
        if not staged:
            return db.execute("SELECT i.body,i.first,i.last,i.id FROM delivery_items i WHERE " + where + " ORDER BY " + order +
                (f" LIMIT {limit}" if limit is not None else ""), args)
        sql = "SELECT i.body,i.first,i.last,i.id FROM stage.edits i WHERE i.body IS NOT NULL AND " + where + " UNION ALL "
        sql += "SELECT i.body,i.first,i.last,i.id FROM main.delivery_items i WHERE " + where + " AND NOT EXISTS "
        sql += "(SELECT 1 FROM stage.edits e WHERE e.view=i.view AND e.account=i.account AND e.channel=i.channel AND e.id=i.id) ORDER BY " + order
        return db.execute(sql + (f" LIMIT {limit}" if limit is not None else ""), args + args)

    @staticmethod
    def _item(db, view, account, channel, key):
        return db.execute("SELECT body FROM items WHERE view=? AND account=? AND channel=? AND id=?", (view, account, channel, key)).fetchone()

    def _set(self, db, view, account, channel, row, first=None, last=None):
        body = encoded(row).decode()
        before = self._item(db, view, account, channel, str(row[0]))
        if before and before[0] == body:
            return False
        at = row[1] if channel == "tokens" else row[3] if channel.startswith("chart:") else row[1]
        first = at if first is None else first
        last = first if last is None else last
        db.execute("INSERT OR REPLACE INTO stage.edits VALUES(?,?,?,?,?,?,?,?)", (view, account, channel, str(row[0]), at, first, last, body))
        if channel == "tokens" or channel.startswith("chart:"):
            db.executemany("INSERT OR IGNORE INTO stage.models VALUES(?)", ((model[0],) for model in row[2 if channel == "tokens" else 6]))
        return True

    @staticmethod
    def _remove(db, view, account, channel, key):
        db.execute("INSERT OR REPLACE INTO stage.edits(view,account,channel,id) VALUES(?,?,?,?)", (view, account, channel, key))

    @staticmethod
    def _interval(row):
        value = json.loads(row[0])
        return [value[0], value[1], value[2], value[5], value[4]]

    @classmethod
    def _mark_neighbors(cls, db, view, account, channel, first, last):
        for query, stamp in (("last<=? ORDER BY last DESC,id", first), ("first>=? ORDER BY first,id", last)):
            condition, order = query.split(" ORDER BY ")
            row = cls.ordered_items(db, view, account, channel, condition, (stamp,), order, 1).fetchone()
            if row:
                db.execute("INSERT OR IGNORE INTO stage.recalc VALUES(?,?,?,?)", (view, account, channel, row[3]))

    @staticmethod
    def _quota_cursor(db, view, account, window, first, last):
        return db.execute("SELECT range_first,range_last,used_percent,multiplier,reset_at,plan FROM canonical_quota "
            "WHERE dataset=? AND account=? AND window=? AND first>=? AND first<=? ORDER BY first,used_percent,record_key", (view, account, window, first, last))

    def _quota(self, db, view, account, window, low, high):
        # Read actual neighboring records, never time buckets. Include the next run
        # because its connection flag depends on the end of the changed run.
        args = (view, account, window)
        left = db.execute("SELECT first FROM canonical_quota WHERE dataset=? AND account=? AND window=? AND first<=? ORDER BY first DESC LIMIT 1", (*args, low)).fetchone()
        right = db.execute("SELECT first FROM canonical_quota WHERE dataset=? AND account=? AND window=? AND first>? ORDER BY first LIMIT 1", (*args, high)).fetchone()
        start, end = min(low, left[0]) if left else low, max(high, right[0]) if right else high
        previous = db.execute("SELECT range_first,range_last,used_percent,multiplier,reset_at,plan FROM canonical_quota "
            "WHERE dataset=? AND account=? AND window=? AND first<? ORDER BY first DESC,used_percent DESC LIMIT 1", (*args, start)).fetchone()

        def run(value):
            first, last, percent, factor, reset, plan = value
            return [usage_id(window, first, percent, factor, plan), first, last, percent, factor, 1, reset, plan]

        db.execute("CREATE TEMP TABLE IF NOT EXISTS wanted_usage(id TEXT PRIMARY KEY)")
        db.execute("CREATE TEMP TABLE IF NOT EXISTS wanted_chart(id TEXT PRIMARY KEY)")
        db.execute("DELETE FROM wanted_usage")
        db.execute("DELETE FROM wanted_chart")
        prior = run(previous) if previous else None
        self.metrics["sourceQuotaRowsRead"] += int(previous is not None)
        for value in self._quota_cursor(db, view, account, window, start, end):
            self.metrics["sourceQuotaRowsRead"] += 1
            current = run(value)
            if prior and connected(prior, current, window):
                current[5] = 0
            db.execute("INSERT OR IGNORE INTO wanted_usage VALUES(?)", (current[0],))
            self._set(db, view, account, "usage:" + window, current[:6], current[1], current[2])
            for interval in valid_intervals([prior, current]) if prior else ():
                db.execute("INSERT OR IGNORE INTO wanted_chart VALUES(?)", (interval[0],))
                before = self._item(db, view, account, "chart:" + window, interval[0])
                old = self._interval(before) if before else None
                if old != interval:
                    self._mark_neighbors(db, view, account, "chart:" + window, interval[1], interval[2])
                    self._set(db, view, account, "chart:" + window, [interval[0], interval[1], interval[2], int(interval[2]), interval[4], interval[3], []], interval[1], interval[2])
                    self._mark_neighbors(db, view, account, "chart:" + window, interval[1], interval[2])
                    db.execute("INSERT OR IGNORE INTO stage.recalc VALUES(?,?,?,?)", (view, account, "chart:" + window, interval[0]))
            prior = current
        for channel, wanted in (("usage:" + window, "wanted_usage"), ("chart:" + window, "wanted_chart")):
            # Usage needs the intersecting old run too, including deleted coverage.
            # Charts include the edge entering the first recomputed run.
            sql = "SELECT id,first,last FROM items WHERE view=? AND account=? AND channel=? AND "
            sql += "first<=? AND last>=?" if channel.startswith("usage:") else "last>=? AND last<=?"
            bounds = (end, start) if channel.startswith("usage:") else (start, end)
            sql += f" AND id NOT IN (SELECT id FROM {wanted})"
            for key, first, last in db.execute(sql, (view, account, channel, *bounds)):
                if channel.startswith("chart:"):
                    self._mark_neighbors(db, view, account, channel, first, last)
                self._remove(db, view, account, channel, key)

    def _rechart(self, db, view, account, channel, key):
        body = self._item(db, view, account, channel, key)
        if body is None:
            return
        interval = self._interval(body)
        before = self.ordered_items(db, view, account, channel, "last<=? AND id!=?", (interval[1], key), "last DESC,id", 1).fetchone()
        after = self.ordered_items(db, view, account, channel, "first>=? AND id!=?", (interval[2], key), "first,id", 1).fetchone()
        values = [self._interval(before)] if before else []
        position = len(values)
        values.append(interval)
        if after:
            values.append(self._interval(after))
        totals = Totals()
        for row in token_projection(db, view, account, interval[1] - MAX_ATTRIBUTION_DISTANCE_SECONDS, interval[2] + MAX_ATTRIBUTION_DISTANCE_SECONDS):
            self.metrics["sourceTokenRowsRead"] += 1
            if nearest(values, row[1]) == position:
                totals.add(row)
        self._set(db, view, account, channel, totals.chart(interval), interval[1], interval[2])

    @classmethod
    def grouped(cls, db, view, account, rows, api=False, staged=True):
        """Merge ordered event and interval cursors; retain two edges per window."""
        cursors = None
        for row in rows:
            if cursors is None:
                cursors = []
                for window in ("5h", "7d"):
                    before = cls.ordered_items(db, view, account, "chart:" + window, "last<=?", (row[1],), "last DESC,id", 1, staged).fetchone()
                    following = iter(cls.ordered_items(db, view, account, "chart:" + window, "last>?", (row[1],), "last,id", staged=staged))
                    current = next(following, None)
                    cursors.append([cls._interval(before) if before else None, cls._interval(current) if current else None, following])
            assignments, boundary, has_intervals = [], None, False
            for cursor in cursors or ():
                while cursor[1] and cursor[1][2] <= row[1]:
                    cursor[0] = cursor[1]
                    value = next(cursor[2], None)
                    cursor[1] = cls._interval(value) if value else None
                values = [value for value in cursor[:2] if value]
                has_intervals |= bool(values)
                index = nearest(values, row[1])
                assignments.append(values[index] if index is not None else None)
                for interval in values:
                    for stamp in interval[1:3]:
                        if stamp <= row[1]:
                            boundary = max(boundary, stamp) if boundary is not None else stamp
            key = ("minute", math.floor(row[1] / 60)) if api else (boundary, *[value[0] if value else None for value in assignments]) if has_intervals else ("event", row[0])
            yield row, key, assignments

    def _tokens(self, db, view, account, cutoff, bootstrap=False):
        state = self._working_state or {}
        api = any(row.get("usageAccountId") == account and row.get("isApiAccount") for row in state.get("accounts", {}).get("items", []))
        group, totals, number = None, None, 0
        for row, key, assignments in self.grouped(db, view, account, token_projection(db, view, account, pending=None if bootstrap else cutoff), api):
            self.metrics["sourceTokenRowsRead"] += 1
            if totals is not None and group != key:
                self._set(db, view, account, "tokens", totals.tokens(f"append:{cutoff}:{number}"))
                number += 1
                totals = None
            if totals is None:
                group, totals = key, Totals()
            totals.add(row)
            for window, interval in zip(("5h", "7d"), assignments):
                if interval:
                    db.execute("INSERT OR IGNORE INTO stage.recalc VALUES(?,?,?,?)", (view, account, "chart:" + window, interval[0]))
        if totals is not None:
            self._set(db, view, account, "tokens", totals.tokens(f"append:{cutoff}:{number}"))

    def _account(self, db, view, account, cutoff):
        before = db.execute("SELECT label FROM delivery_accounts WHERE view=? AND account=?", (view, account)).fetchone()
        label = before[0] if before else "Unknown"
        for table in ("canonical_tokens", "canonical_quota"):
            row = db.execute(f"SELECT account_label FROM {table} WHERE dataset=? AND account=? ORDER BY first DESC LIMIT 1", (view, account)).fetchone()
            if row and row[0]:
                label = row[0]
        db.execute("INSERT OR REPLACE INTO stage.accounts VALUES(?,?,?)", (view, account, label))
        bootstrap = db.execute("SELECT 1 FROM delivery_pending WHERE view=? AND account=? AND seq<=? AND kind='bootstrap' LIMIT 1", (view, account, cutoff)).fetchone() is not None
        for window in ("5h", "7d"):
            if bootstrap:
                span = db.execute("SELECT MIN(first),MAX(last) FROM canonical_quota WHERE dataset=? AND account=? AND window=?", (view, account, window)).fetchone()
                if span[0] is not None:
                    self._quota(db, view, account, window, *span)
            else:
                low = high = None
                for first, last in db.execute("SELECT first,last FROM delivery_pending WHERE view=? AND account=? AND kind='quota' AND window=? AND seq<=? ORDER BY first,last",
                        (view, account, window, cutoff)):
                    if low is not None and first > high:
                        self._quota(db, view, account, window, low, high)
                        low = None
                    if low is None:
                        low, high = first, last
                    else:
                        high = max(high, last)
                if low is not None:
                    self._quota(db, view, account, window, low, high)
            latest = db.execute("SELECT reset_at FROM canonical_quota WHERE dataset=? AND account=? AND window=? ORDER BY last DESC,first DESC LIMIT 1", (view, account, window)).fetchone()
            # Scalar rows are represented internally as [window, reset]; the wire
            # shape remains [reset], not a new history channel.
            self._set(db, view, account, "quota:" + window, [window, latest[0] if latest else None], 0, 0)
        self._tokens(db, view, account, cutoff, bootstrap)
        for channel, key in db.execute("SELECT channel,id FROM stage.recalc WHERE view=? AND account=? ORDER BY channel,id", (view, account)):
            self._rechart(db, view, account, channel, key)
        self.metrics["accountsPrepared"] += 1

    @staticmethod
    def _packet(db, stream, channel, account, data, now):
        seq = int(db.execute("SELECT value FROM delivery_meta WHERE name=?", (stream,)).fetchone()[0]) + 1
        db.execute("UPDATE delivery_meta SET value=? WHERE name=?", (str(seq), stream))
        packet = {"epoch": db.execute("SELECT value FROM delivery_meta WHERE name='epoch'").fetchone()[0], "stream": stream, "seq": seq,
            "channel": channel, "account": account, "data": data}
        db.execute("INSERT INTO delivery_journal VALUES(?,?,?,?)", (stream, seq, now, encoded(packet)))

    def _publish(self, path, cutoff, state, now):
        with self.index.lock, closing(self._connect()) as db, db:
            db.execute("ATTACH DATABASE ? AS output", (path,))
            db.execute("BEGIN IMMEDIATE")
            for view, account, label in db.execute("SELECT view,account,label FROM output.accounts"):
                before = db.execute("SELECT label FROM delivery_accounts WHERE view=? AND account=?", (view, account)).fetchone()
                if before is None or before[0] != label:
                    self._packet(db, view, "account", account, {"label": label}, now)
                db.execute("INSERT INTO delivery_accounts VALUES(?,?,'[]',?) ON CONFLICT(view,account) DO UPDATE SET label=excluded.label", (view, account, label))
            db.execute("INSERT OR IGNORE INTO delivery_models SELECT name FROM output.models")
            for view, account, channel, key, at, first, last, body in db.execute("SELECT " + ITEM_COLUMNS + " FROM output.edits ORDER BY view,account,channel,at,id"):
                old = db.execute("SELECT body FROM delivery_items WHERE view=? AND account=? AND channel=? AND id=?", (view, account, channel, key)).fetchone()
                if (old[0] if old else None) == body:
                    continue
                if body is None:
                    db.execute("DELETE FROM delivery_items WHERE view=? AND account=? AND channel=? AND id=?", (view, account, channel, key))
                else:
                    db.execute("INSERT OR REPLACE INTO delivery_items VALUES(?,?,?,?,?,?,?,?)", (view, account, channel, key, at, first, last, body))
                self.metrics["outputRowsWritten"] += 1
                row = json.loads(body) if body else None
                data = [row] if channel == "tokens" else [row[1]] if channel.startswith("quota:") else {"set": [row] if row else [], "remove": [] if row else [key]}
                self._packet(db, view, channel, account, data, now)
            db.execute("DELETE FROM delivery_pending WHERE seq<=?", (cutoff,))
            if state is not None and state != (self.state or {}):
                self._packet(db, "state", "state", None, state, now)
                db.execute("INSERT OR REPLACE INTO delivery_meta VALUES('statePayload',?)", (encoded(state).decode(),))
            db.execute("DELETE FROM delivery_journal WHERE created<?", (now - DAY,))
            size = db.execute("SELECT COALESCE(SUM(length(body)),0) FROM delivery_journal").fetchone()[0]
            if size > JOURNAL_BYTES:
                # One disk-backed ordered pass, rather than sorting the backlog
                # again for every expired packet during a large publication.
                db.execute("DELETE FROM delivery_journal WHERE rowid IN (SELECT rowid FROM "
                    "(SELECT rowid,SUM(length(body)) OVER (ORDER BY created DESC,stream DESC,seq DESC) AS bytes FROM delivery_journal) WHERE bytes>?)", (JOURNAL_BYTES,))

    def rebuild(self):
        """Clear derived rows and rebuild from canonical data before workers start."""
        with self.lock, self.index.lock, closing(self._connect()) as db, db:
            db.execute("DELETE FROM delivery_items WHERE channel IN ('tokens','usage:5h','usage:7d','chart:5h','chart:7d','quota:5h','quota:7d')")
            db.execute("DELETE FROM delivery_journal")
            db.execute("UPDATE delivery_meta SET value=? WHERE name='epoch'", (uuid.uuid4().hex,))
            db.execute("UPDATE delivery_meta SET value='0' WHERE name IN ('local','merged','state')")
            db.execute("INSERT INTO delivery_pending(view,account,kind) SELECT dataset,account,'bootstrap' FROM canonical_quota "
                "UNION SELECT dataset,account,'bootstrap' FROM canonical_tokens")
        self.prepare()

    def prepare(self, now=None, status=None, force=False):
        from monitor_dashboard import dashboard_safe_json
        started = time.perf_counter()
        now = time.time() if now is None else now
        state = numeric_state(dashboard_safe_json(status)) if status is not None else self.state
        if status is not None:
            state = dict(state)
            state.pop("revision", None)
            state["revision"] = hashlib.sha256(encoded(state)).hexdigest()
        with self.lock:
            before = self.head
            with closing(self._connect()) as check:
                # A publication may have committed before its caller was interrupted.
                # Recover the in-process head/cache too, without appending again.
                self.head = self._head(check)
                if before != self.head:
                    self._clear_cache()
                    saved = check.execute("SELECT value FROM delivery_meta WHERE name='statePayload'").fetchone()
                    self.state = json.loads(saved[0]) if saved else None
                    if status is None:
                        state = self.state
                if state == self.state and not check.execute("SELECT 1 FROM delivery_pending LIMIT 1").fetchone():
                    self.metrics["prepareSeconds"] = round(time.perf_counter() - started, 6)
                    self.cloud_pending = False
                    return before != self.head
            self._working_state = state
            try:
                with tempfile.TemporaryDirectory(prefix="codex-delivery-") as directory, closing(self._connect()) as db:
                    path = os.path.join(directory, "stage.sqlite3")
                    db.execute("ATTACH DATABASE ? AS stage", (path,))
                    self._stage(db)
                    db.commit()
                    db.execute("BEGIN")
                    cutoff = db.execute("SELECT COALESCE(MAX(seq),0) FROM delivery_pending").fetchone()[0]
                    for view, account in db.execute("SELECT DISTINCT view,account FROM delivery_pending WHERE seq<=? ORDER BY view,account", (cutoff,)):
                        self._account(db, view, account, cutoff)
                    db.commit()
                    # Release the source snapshot before taking the publication lock.
                    db.execute("DETACH DATABASE stage")
                    self._publish(path, cutoff, state, now)
                with closing(self._connect()) as db:
                    self.head = self._head(db)
                self.state = state
                self.cloud_pending = False
            finally:
                self.metrics["prepareSeconds"] = round(time.perf_counter() - started, 6)
            changed = before != self.head
            if changed:
                self._clear_cache()
            with self.changed:
                self.changed.notify_all()
            return changed

    def _json_chunks(self, db):
        """Only final rows are read here. One output row is decoded at a time."""
        models = [row[0] for row in db.execute("SELECT name FROM delivery_models ORDER BY name")]
        model_ids = {model: index for index, model in enumerate(models)}
        state = db.execute("SELECT value FROM delivery_meta WHERE name='statePayload'").fetchone()
        head = self._head(db)
        header = {"epoch": head[0], "cursors": head[1], "models": models, "state": json.loads(state[0]) if state else {}}
        yield encoded(header)[:-1] + b',"accounts":['
        known = {row.get("id") for row in header["state"].get("accounts", {}).get("usageGroups", [])} | {
            row.get("usageAccountId") for row in header["state"].get("accounts", {}).get("items", [])}
        first_account = True
        for account, label in db.execute("SELECT account,MIN(label) FROM delivery_accounts GROUP BY account ORDER BY account"):
            if known and account not in known and account != "unknown" and not db.execute(
                    "SELECT 1 FROM delivery_items WHERE view='local' AND account=? AND channel IN ('tokens','usage:5h','usage:7d') LIMIT 1", (account,)).fetchone():
                continue
            yield (b"" if first_account else b",") + encoded({"id": account, "label": label})[:-1]
            first_account = False
            for view in ("local", "merged"):
                quotas = {}
                for window in ("5h", "7d"):
                    row = db.execute("SELECT body FROM delivery_items WHERE view=? AND account=? AND channel=?", (view, account, "quota:" + window)).fetchone()
                    quotas[window] = json.loads(row[0])[1] if row else None
                yield b"," + encoded(view) + b':{"quota":' + encoded(quotas) + b',"legacy":['
                first_legacy = True
                for body, in db.execute("SELECT body FROM delivery_items WHERE view=? AND account=? AND channel='legacy' ORDER BY at,id", (view, account)):
                    row = json.loads(body)
                    for entry in row[2]:
                        entry[0] = model_ids[entry[0]]
                    yield (b"" if first_legacy else b",") + encoded(row)
                    first_legacy = False
                yield b"]"
                for channel in CHANNELS:
                    yield b"," + encoded(channel) + b":["
                    first_row = True
                    order = "at,json_extract(body,'$[3]') IS NULL,json_extract(body,'$[3]'),id" if channel.startswith("usage:") else "at,id"
                    for body, in db.execute("SELECT body FROM delivery_items WHERE view=? AND account=? AND channel=? ORDER BY " + order, (view, account, channel)):
                        row = json.loads(body)
                        if channel == "tokens" or channel.startswith("chart:"):
                            for entry in row[2 if channel == "tokens" else 6]:
                                entry[0] = model_ids[entry[0]]
                        yield (b"" if first_row else b",") + encoded(row)
                        first_row = False
                    yield b"]"
                yield b"}"
            yield b"}"
        yield b"]}"

    def _clear_cache(self):
        for file in self._cache.values():
            file.release()
        self._cache.clear()

    @contextmanager
    def snapshot_file(self, compressed=True):
        with self.lock:
            key = compressed
            if key not in self._cache:
                artifact = Snapshot()
                file = artifact.file
                try:
                    with closing(self._connect()) as db:
                        db.execute("BEGIN")
                        if compressed:
                            with gzip.GzipFile(fileobj=file, mode="wb", compresslevel=3, mtime=0) as output:
                                for chunk in self._json_chunks(db):
                                    output.write(chunk)
                        else:
                            for chunk in self._json_chunks(db):
                                file.write(chunk)
                        db.rollback()
                    file.flush()
                    artifact.size = file.tell()
                    file.close()
                    self.metrics["gzipBytes" if compressed else "bulkBytes"] = artifact.size
                    self._cache[key] = artifact
                except BaseException:
                    artifact.release()
                    raise
            artifact = self._cache[key]
            sender = open(artifact.path, "rb")
            artifact.references += 1
        try:
            yield sender, artifact.size
        finally:
            sender.close()
            with self.lock:
                artifact.release()

    def bulk(self, compressed=True):
        """Compatibility for tests/exporters. HTTP must use snapshot_file()."""
        with self.snapshot_file(compressed) as (file, _):
            return file.read()

    def replay(self, epoch, cursors, page_bytes=256 * 1024):
        if not isinstance(cursors, dict) or not cursors or any(stream not in {"local", "merged", "state"} or type(seq) is not int or seq < 0 for stream, seq in cursors.items()):
            raise ValueError("Invalid stream cursor")
        with closing(self._connect()) as db:
            db.execute("BEGIN")
            head = self._head(db)
            if epoch != head[0]:
                return None
            for stream, seq in cursors.items():
                if seq > head[1][stream]:
                    return None
                if seq < head[1][stream]:
                    first = db.execute("SELECT MIN(seq) FROM delivery_journal WHERE stream=? AND seq>?", (stream, seq)).fetchone()[0]
                    if first != seq + 1:
                        return None
            result, size = [], 0
            for stream, seq in cursors.items():
                expected = seq + 1
                for number, body in db.execute("SELECT seq,body FROM delivery_journal WHERE stream=? AND seq>? ORDER BY seq", (stream, seq)):
                    if number != expected:
                        return None
                    result.append(json.loads(body))
                    size += len(body)
                    expected += 1
                    if size >= page_bytes:
                        return result
            return result

    def close(self):
        with self.lock:
            self._clear_cache()

    def __del__(self):
        for file in getattr(self, "_cache", {}).values():
            file.release()


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
            from monitor_common import parse_timestamp
            result[key] = parse_timestamp(item)
        else:
            result[key] = numeric_state(item)
    return result


def request_attribution_rebuild(db):
    enqueue_quota_repair(db)
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_meta'").fetchone():
        db.execute("INSERT OR REPLACE INTO delivery_meta VALUES('attributionRule',?)", ("1",))


initialize_delivery = initialize
DashboardDelivery = FinalDelivery
