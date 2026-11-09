"""Contract 26: preserve saved wire results while retiring preparation copies."""

import codecs
import json
import sqlite3
import uuid
from contextlib import closing

from monitor_final_delivery import FinalDelivery, Totals, encoded, initialize


class JsonReader:
    """Incremental reader for the known bulk envelope; materialize one row only.

    Containers containing history are traversed explicitly. value() is used only
    for one output row or small account/control metadata, never an account view.
    """
    def __init__(self, stream):
        self.stream = stream
        self.decoder = codecs.getincrementaldecoder("utf-8")()
        self.json = json.JSONDecoder()
        self.buffer, self.position, self.eof = "", 0, False

    def fill(self):
        data = self.stream.read(64 * 1024)
        self.buffer = self.buffer[self.position:] + self.decoder.decode(data, final=not data)
        self.position = 0
        self.eof = not data

    def peek(self):
        while True:
            while self.position < len(self.buffer) and self.buffer[self.position].isspace():
                self.position += 1
            if self.position < len(self.buffer):
                return self.buffer[self.position]
            if self.eof:
                raise ValueError("Truncated saved dashboard JSON")
            self.fill()

    def take(self, char):
        if self.peek() != char:
            raise ValueError("Invalid saved dashboard JSON")
        self.position += 1

    def value(self):
        self.peek()
        while True:
            try:
                value, end = self.json.raw_decode(self.buffer, self.position)
                # A numeric token at a buffer boundary may still have more digits.
                if end == len(self.buffer) and not self.eof:
                    self.fill()
                    continue
                self.position = end
                return value
            except json.JSONDecodeError:
                if self.eof:
                    raise ValueError("Invalid saved dashboard JSON")
                self.fill()

    def members(self):
        self.take("{")
        first = True
        while self.peek() != "}":
            if not first:
                self.take(",")
            first = False
            key = self.value()
            if not isinstance(key, str):
                raise ValueError("Invalid saved dashboard object key")
            self.take(":")
            yield key
        self.take("}")

    def array(self):
        self.take("[")
        first = True
        while self.peek() != "]":
            if not first:
                self.take(",")
            first = False
            yield None
        self.take("]")


def put(db, view, account, channel, row, models=(), key=None, replace=True):
    if channel == "tokens" or channel.startswith("chart:") or channel == "legacy":
        for model in row[2 if channel in {"tokens", "legacy"} else 6]:
            if isinstance(model[0], int):
                model[0] = models[model[0]]
            db.execute("INSERT OR IGNORE INTO delivery_models VALUES(?)", (model[0],))
    at = row[1] if channel == "tokens" else row[3] if channel.startswith("chart:") else row[1] if channel != "legacy" else row[1]
    first, last = (row[1], row[2]) if channel.startswith(("usage:", "chart:")) else (at, at)
    if channel.startswith("quota:"):
        at = first = last = 0
    key = str(row[0]) if key is None else key
    body = encoded(row).decode()
    db.execute(("INSERT OR REPLACE" if replace else "INSERT") + " INTO delivery_items VALUES(?,?,?,?,?,?,?,?)", (view, account, channel, key, at, first, last, body))
    if db.execute("SELECT body FROM delivery_items WHERE view=? AND account=? AND channel=? AND id=?", (view, account, channel, key)).fetchone() != (body,):
        raise ValueError(f"Saved Delivery row failed round-trip verification: {view}/{account}/{channel}/{key}")


def snapshot(db):
    row = db.execute("SELECT id FROM delivery_snapshot WHERE body IS NOT NULL").fetchone()
    if row is None:
        return False
    models, epoch, cursors = [], None, None
    with db.blobopen("delivery_snapshot", "body", row[0], readonly=True) as blob:
        reader = JsonReader(blob)
        for name in reader.members():
            if name in {"models", "epoch", "cursors", "state"}:
                value = reader.value()
                if name == "models":
                    models = value
                elif name == "epoch":
                    epoch = value
                elif name == "cursors":
                    cursors = value
                else:
                    db.execute("INSERT OR REPLACE INTO delivery_meta VALUES('statePayload',?)", (encoded(value).decode(),))
            elif name == "accounts":
                for _ in reader.array():
                    account, label = None, "Unknown"
                    for field in reader.members():
                        if field in {"id", "label"}:
                            value = reader.value()
                            if field == "id":
                                account = value
                            else:
                                label = value
                        elif field in {"local", "merged"}:
                            if account is None:
                                raise ValueError("Saved dashboard view precedes account identity")
                            db.execute("INSERT OR REPLACE INTO delivery_accounts VALUES(?,?,'[]',?)", (field, account, label))
                            for channel in reader.members():
                                if channel == "quota":
                                    for window, reset in reader.value().items():
                                        put(db, field, account, "quota:" + window, [window, reset])
                                elif channel in {"tokens", "usage:5h", "usage:7d", "chart:5h", "chart:7d", "legacy"}:
                                    number = 0
                                    for _ in reader.array():
                                        put(db, field, account, channel, reader.value(), models, f"legacy:{number}" if channel == "legacy" else None, replace=False)
                                        number += 1
                                else:
                                    raise ValueError(f"Unknown saved dashboard channel: {channel}")
                        else:
                            reader.value()
            else:
                reader.value()
    if epoch != db.execute("SELECT value FROM delivery_meta WHERE name='epoch'").fetchone()[0] or not isinstance(cursors, dict):
        return False
    for stream in ("local", "merged", "state"):
        head = int(db.execute("SELECT value FROM delivery_meta WHERE name=?", (stream,)).fetchone()[0])
        start = cursors.get(stream)
        if type(start) is not int or start > head:
            return False
        count, first, last = db.execute("SELECT COUNT(*),MIN(seq),MAX(seq) FROM delivery_journal WHERE stream=? AND seq>?", (stream, start)).fetchone()
        if count != head - start or count and (first != start + 1 or last != head):
            return False
    for stream in ("local", "merged", "state"):
        for body, in db.execute("SELECT body FROM delivery_journal WHERE stream=? AND seq>? ORDER BY seq", (stream, cursors[stream])):
            packet = json.loads(body)
            channel, account, data = packet["channel"], packet["account"], packet["data"]
            if channel == "state":
                db.execute("INSERT OR REPLACE INTO delivery_meta VALUES('statePayload',?)", (encoded(data).decode(),))
            elif channel == "account":
                db.execute("INSERT INTO delivery_accounts VALUES(?,?,'[]',?) ON CONFLICT(view,account) DO UPDATE SET label=excluded.label", (stream, account, data["label"]))
            elif channel == "tokens":
                for row in data:
                    put(db, stream, account, channel, row)
            elif channel.startswith("quota:"):
                put(db, stream, account, channel, [channel.split(":")[1], data[0]])
            else:
                for key in data["remove"]:
                    db.execute("DELETE FROM delivery_items WHERE view=? AND account=? AND channel=? AND id=?", (stream, account, channel, key))
                for row in data["set"]:
                    put(db, stream, account, channel, row)
    return True


def prepared(db):
    """Fallback: encode saved prepared values, not canonical business history."""
    db.execute("CREATE TEMP VIEW items AS SELECT * FROM delivery_items")
    for view, account in db.execute("SELECT view,account FROM delivery_accounts ORDER BY view,account"):
        rowid = db.execute("SELECT rowid FROM delivery_accounts WHERE view=? AND account=?", (view, account)).fetchone()[0]
        with db.blobopen("delivery_accounts", "legacy", rowid, readonly=True) as blob:
            reader = JsonReader(blob)
            for number, _ in enumerate(reader.array()):
                put(db, view, account, "legacy", reader.value(), key=f"legacy:{number}", replace=False)
        for window in ("5h", "7d"):
            reset = None
            for row in db.execute("SELECT id,first,last,value,multiplier,flags,reset,plan FROM delivery_runs WHERE view=? AND account=? AND window=? ORDER BY first,value IS NULL,value,id", (view, account, window)):
                put(db, view, account, "usage:" + window, list(row[:6]))
                if row[3] is not None:
                    reset = row[6]
            put(db, view, account, "quota:" + window, [window, reset])
            for row in db.execute("SELECT id,start,end,at,multiplier,delta,models FROM delivery_charts WHERE view=? AND account=? AND window=? ORDER BY start,id", (view, account, window)):
                put(db, view, account, "chart:" + window, [*row[:6], json.loads(row[6])])
        state = db.execute("SELECT value FROM delivery_meta WHERE name='statePayload'").fetchone()
        api = any(row.get("usageAccountId") == account and row.get("isApiAccount") for row in (json.loads(state[0]) if state else {}).get("accounts", {}).get("items", []))
        group, totals, number = None, None, 0
        rows = db.execute("SELECT id,at,model,tier,fresh,cached,output,cache_write,cost FROM delivery_tokens WHERE view=? AND account=? ORDER BY at,id", (view, account))
        for row, key, _ in FinalDelivery.grouped(db, view, account, rows, api, staged=False):
            if totals is not None and group != key:
                put(db, view, account, "tokens", totals.tokens(f"baseline:{number}"))
                number += 1
                totals = None
            if totals is None:
                group, totals = key, Totals()
            totals.add(row)
        if totals is not None:
            put(db, view, account, "tokens", totals.tokens(f"baseline:{number}"))
    db.execute("DROP VIEW temp.items")


def migrate_delivery(paths):
    path = paths["processing_index"]
    if not path.exists():
        return
    with closing(sqlite3.connect(path)) as db:
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_items'").fetchone():
            return
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_quota'").fetchone():
            return
        db.execute("BEGIN IMMEDIATE")
        try:
            legacy = db.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_snapshot'").fetchone() is not None
            if legacy:
                db.execute("INSERT OR REPLACE INTO delivery_meta VALUES('finalRows','1')")
            initialize(db)
            if legacy:
                db.execute("SAVEPOINT saved_snapshot")
                if snapshot(db):
                    db.execute("RELEASE saved_snapshot")
                else:
                    db.execute("ROLLBACK TO saved_snapshot")
                    db.execute("RELEASE saved_snapshot")
                    prepared(db)
                for view, account in db.execute("SELECT view,account FROM delivery_dirty UNION SELECT view,account FROM delivery_work"):
                    db.execute("INSERT OR REPLACE INTO delivery_pending(view,account,kind) VALUES(?,?,'account')", (view, account))
                    # A legacy dirty scope without identity work meant a full
                    # preparation pass. Queue only events absent from saved output.
                    db.execute("INSERT INTO delivery_pending(view,account,kind,record_key,first,last) "
                        "SELECT dataset,account,'tokenLedger',record_key,first,last FROM canonical_tokens c WHERE dataset=? AND account=? "
                        "AND NOT EXISTS(SELECT 1 FROM delivery_token_refs r WHERE r.view=c.dataset AND r.account=c.account AND r.id=c.event_id)", (view, account))
                    work = db.execute("SELECT 1 FROM delivery_work WHERE view=? AND account=? LIMIT 1", (view, account)).fetchone()
                    quota = db.execute("SELECT 1 FROM delivery_work WHERE view=? AND account=? AND kind!='tokenLedger' LIMIT 1", (view, account)).fetchone()
                    if not work or quota:
                        # Deleted extents may survive only in saved final rows.
                        # Include them so deletion of the last point is recoverable.
                        db.execute("INSERT INTO delivery_pending(view,account,kind,window,first,last) "
                            "SELECT ?,?,'quota',window,MIN(first),MAX(last) FROM "
                            "(SELECT window,first,last FROM canonical_quota WHERE dataset=? AND account=? UNION ALL "
                            "SELECT substr(channel,7),first,last FROM delivery_items WHERE view=? AND account=? AND (channel LIKE 'usage:%' OR channel LIKE 'chart:%')) "
                            "GROUP BY window", (view, account, view, account, view, account))
                for name, in list(db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'delivery_%'")):
                    db.execute(f'DROP TRIGGER "{name}"')
                for name, in list(db.execute("SELECT name FROM sqlite_master WHERE type='view' AND name LIKE 'delivery_%'")):
                    db.execute(f'DROP VIEW "{name}"')
                for name in ("delivery_token_refs", "delivery_run_refs", "delivery_token_payloads", "delivery_run_payloads", "delivery_charts", "delivery_snapshot", "delivery_dirty", "delivery_work"):
                    db.execute(f'DROP TABLE IF EXISTS "{name}"')
                db.execute("DELETE FROM delivery_journal")
                db.execute("UPDATE delivery_meta SET value='0' WHERE name IN ('local','merged','state')")
                db.execute("UPDATE delivery_meta SET value=? WHERE name='epoch'", (uuid.uuid4().hex,))
            if db.execute("PRAGMA foreign_key_check").fetchone():
                raise ValueError("Foreign key validation failed during final Delivery conversion")
            db.commit()
        except BaseException:
            db.rollback()
            raise


def migrate_local(paths):
    from monitor_migration_canonical import remove_seen
    remove_seen(paths)
    migrate_delivery(paths)
