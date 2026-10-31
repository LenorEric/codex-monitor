#!/usr/bin/env python3
"""Read original Codex session files and coordinate direct database ingestion."""

import base64
import hashlib
import json
import threading
import time
from functools import wraps
from pathlib import Path

from monitor_common import empty_token_totals
from monitor_processing import encode

SOURCE_READ_BYTES = 1024 * 1024
SOURCE_DISCOVERY_SECONDS = 15 * 60
SOURCE_CHECK_BATCH = 128

class SourceWatcher:
    """OS notifications are hints; persistent checkpoints and audits remain authoritative."""
    def __init__(self, home):
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer
        self.home, self.lock, self.pending, self.overflow = Path(home).resolve(), threading.Lock(), set(), True
        self.observer, self.roots = Observer(), set()
        watcher = self
        class Handler(FileSystemEventHandler):
            def on_any_event(self, event):
                if event.event_type not in {"created", "modified", "deleted", "moved"}:
                    return
                with watcher.lock:
                    for name in (event.src_path, getattr(event, "dest_path", "")):
                        if not name:
                            continue
                        path = Path(name).resolve()
                        if event.is_directory:
                            if path.parent == watcher.home and path.name in {"sessions", "archived_sessions"}:
                                watcher.overflow = True
                            continue
                        if path.suffix == ".jsonl" and any(path.is_relative_to(watcher.home / root) for root in ("sessions", "archived_sessions")):
                            if len(watcher.pending) < 4096:
                                watcher.pending.add(str(path))
                            else:
                                watcher.overflow = True
        self.handler = Handler()
        if self.home.exists():
            self.observer.schedule(self.handler, str(self.home), recursive=False)
        self.enroll()
        self.observer.start()

    def enroll(self):
        for name in ("sessions", "archived_sessions"):
            root = self.home / name
            if root.is_dir() and root not in self.roots:
                self.observer.schedule(self.handler, str(root), recursive=True)
                self.roots.add(root)

    def drain(self):
        self.enroll()
        with self.lock:
            paths = sorted(self.pending)[:SOURCE_CHECK_BATCH]
            self.pending.difference_update(paths)
            overflow, self.overflow = self.overflow, False
            return paths, overflow

    def stop(self):
        self.observer.stop()
        self.observer.join()


def serialized(method):
    @wraps(method)
    def run(self, *args, **kwargs):
        with self.lock:
            return method(self, *args, **kwargs)
    return run


class SourceIngestor:
    def __init__(self, store):
        self.store, self.index = store, store.index
        self.metrics = self.index.metrics
        self.lock = threading.RLock()
        import monitor_tokens
        self.parser_rule = hashlib.sha256(Path(monitor_tokens.__file__).read_bytes()).hexdigest()

    @staticmethod
    def signature(path):
        try:
            stat = path.stat()
            return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns]
        except FileNotFoundError:
            return None


    def _record_blocks(self, db, path, start, end):
        source = str(Path(path).resolve())
        if start == 0:
            db.execute("DELETE FROM processing_blocks WHERE source=?", (source,))
        if end <= start:
            return
        with Path(path).open("rb") as stream:
            stream.seek(start // 4096 * 4096)
            while stream.tell() < end:
                block = stream.tell() // 4096
                data = stream.read(min(4096, end - stream.tell()))
                self.metrics["sourceVerificationBytes"] += len(data)
                db.execute("INSERT OR REPLACE INTO processing_blocks VALUES(?,?,?,?)", (source, block, len(data), hashlib.sha256(data).hexdigest()))


    def _prefix_valid(self, db, path):
        source = str(Path(path).resolve())
        rows = list(db.execute("SELECT block,size,digest FROM processing_blocks WHERE source=? ORDER BY block LIMIT 1", (source,)))
        last = db.execute("SELECT block,size,digest FROM processing_blocks WHERE source=? ORDER BY block DESC LIMIT 1", (source,)).fetchone()
        if last and last not in rows:
            rows.append(last)
        if rows:
            with Path(path).open("rb") as stream:
                for block, size, digest in rows:
                    stream.seek(block * 4096)
                    data = stream.read(size)
                    self.metrics["sourceVerificationBytes"] += len(data)
                    if hashlib.sha256(data).hexdigest() != digest:
                        return False
        return True


    def audit(self, force=False):
        """Rotate a bounded prefix audit; detects edits hidden by a later append."""
        with self.index.transaction() as db:
            if not force and time.time() - self.index.get(db, "sourceAuditAt", time.time()) < SOURCE_DISCOVERY_SECONDS:
                if self.index.get(db, "sourceAuditAt") is None:
                    self.index.put(db, "sourceAuditAt", time.time())
                return
            cursor = self.index.get(db, "sourceAuditCursor", ["", -1])
            rows = list(db.execute("SELECT b.source,b.block,b.size,b.digest FROM processing_blocks b JOIN processing_scan_sources s ON s.path=b.source WHERE (b.source,b.block)>(?,?) ORDER BY b.source,b.block LIMIT ?", (*cursor, SOURCE_CHECK_BATCH)))
            if not rows:
                rows = list(db.execute("SELECT b.source,b.block,b.size,b.digest FROM processing_blocks b JOIN processing_scan_sources s ON s.path=b.source ORDER BY b.source,b.block LIMIT ?", (SOURCE_CHECK_BATCH,)))
            for source, block, size, digest in rows:
                try:
                    with Path(source).open("rb") as stream:
                        stream.seek(block * 4096)
                        data = stream.read(size)
                    self.metrics["sourceVerificationBytes"] += len(data)
                except FileNotFoundError:
                    continue
                if hashlib.sha256(data).hexdigest() != digest:
                    row = db.execute("SELECT state,home FROM processing_scan_sources WHERE path=?", (source,)).fetchone()
                    if row:
                        state = json.loads(row[0])
                        state["forceReparse"] = True
                        db.execute("UPDATE processing_scan_sources SET state=? WHERE path=?", (encode(state), source))
                        db.execute("INSERT OR IGNORE INTO processing_reparse VALUES(?)", (source,))
                        self.index.put(db, f"reconcile:{row[1]}", True)
            if rows:
                self.index.put(db, "sourceAuditCursor", list(rows[-1][:2]))
            self.index.put(db, "sourceAuditAt", time.time())


    def _winner(self, db, event_id):
        row = db.execute("SELECT o.data FROM processing_occurrences o JOIN processing_scan_sources s ON s.path=o.source "
            "WHERE o.event_id=? AND s.valid=1 ORDER BY o.source,o.event_index LIMIT 1", (event_id,)).fetchone()
        previous = db.execute("SELECT data,valid_refs,ledger_done FROM processing_scan_events WHERE event_id=?", (event_id,)).fetchone()
        if row and previous[0] != row[0]:
            if previous[1]:
                self._contribution(db, json.loads(previous[0]), -1)
                self._contribution(db, json.loads(row[0]), 1)
            db.execute("UPDATE processing_scan_events SET data=?,ledger_done=0 WHERE event_id=?", (row[0], event_id))
            if previous[2]:
                self.index.put(db, f"reconcile:{self.home}", True)


    def _contribution(self, db, event, direction):
        from monitor_tokens import add_token_delta
        key = (event["model"], event.get("serviceTier") or "default")
        row = db.execute("SELECT data FROM processing_totals WHERE home=? AND model=? AND tier=?", (self.home, *key)).fetchone()
        delta = empty_token_totals()
        add_token_delta(delta, event["tokens"])
        totals = json.loads(row[0]) if row else empty_token_totals()
        for name, value in delta.items():
            totals[name] += value * direction
        if any(totals.values()):
            db.execute("INSERT OR REPLACE INTO processing_totals VALUES(?,?,?,?)", (self.home, *key, encode(totals)))
        else:
            db.execute("DELETE FROM processing_totals WHERE home=? AND model=? AND tier=?", (self.home, *key))


    def _refs(self, db, event_id, direction):
        row = db.execute("SELECT data,valid_refs FROM processing_scan_events WHERE event_id=?", (event_id,)).fetchone()
        if row[1] == 0 and direction > 0 or row[1] == 1 and direction < 0:
            self._contribution(db, json.loads(row[0]), direction)
        db.execute("UPDATE processing_scan_events SET valid_refs=valid_refs+? WHERE event_id=?", (direction, event_id))


    def _remove_source(self, db, source, valid):
        events = [row[0] for row in db.execute("SELECT event_id FROM processing_occurrences WHERE source=?", (source,))]
        if valid:
            for (event_id,) in db.execute("SELECT event_id FROM processing_occurrences WHERE source=?", (source,)):
                self._refs(db, event_id, -1)
        db.execute("DELETE FROM processing_occurrences WHERE source=?", (source,))
        for event_id in events:
            if db.execute("SELECT 1 FROM processing_occurrences WHERE event_id=? LIMIT 1", (event_id,)).fetchone():
                self._winner(db, event_id)
            else:
                db.execute("DELETE FROM processing_scan_events WHERE event_id=? AND valid_refs<=0", (event_id,))


    @serialized
    def scan(self, home):
        from monitor_tokens import _update_codex_file_state, collect_codex_session_files
        self.audit()
        with self.index.transaction() as db:
            self.home = str(Path(home).resolve())
            import monitor_tokens
            rule = self.parser_rule
            if self.index.get(db, f"parserRule:{self.home}", rule) != rule:
                for source, data in list(db.execute("SELECT path,state FROM processing_scan_sources WHERE home=?", (self.home,))):
                    state = json.loads(data)
                    state["forceReparse"] = True
                    db.execute("UPDATE processing_scan_sources SET state=? WHERE path=?", (encode(state), source))
                    db.execute("INSERT OR IGNORE INTO processing_reparse VALUES(?)", (source,))
            self.index.put(db, f"parserRule:{self.home}", rule)
            discovery = self.index.get(db, f"discovery:{self.home}", 0)
            paths = set()
            if getattr(self, "source_watcher", None) is not None:
                notifications, overflow = self.source_watcher.drain()
                paths.update(notifications)
                if overflow:
                    discovery = 0
            if not discovery or time.time() - discovery >= SOURCE_DISCOVERY_SECONDS:
                paths = {str(path.resolve()) for path in collect_codex_session_files(Path(home))}
                inventory = dict(db.execute("SELECT path,valid FROM processing_scan_sources WHERE home=?", (self.home,)))
                for source in inventory.keys() - paths:
                    self._remove_source(db, source, inventory[source])
                    db.execute("DELETE FROM processing_scan_sources WHERE path=?", (source,))
                    db.execute("DELETE FROM processing_blocks WHERE source=?", (source,))
                    db.execute("DELETE FROM processing_health WHERE source=?", (source,))
                    db.execute("DELETE FROM processing_reparse WHERE source=?", (source,))
                self.index.put(db, f"discovery:{self.home}", time.time())
            else:
                # Recent creation is checked promptly; cold-source reconciliation is rotated.
                from datetime import datetime
                today = datetime.now()
                recent = Path(home) / "sessions" / f"{today.year:04}" / f"{today.month:02}" / f"{today.day:02}"
                for directory in (recent, Path(home) / "archived_sessions"):
                    if directory.is_dir():
                        stamp = directory.stat().st_mtime_ns
                        if self.index.get(db, f"directory:{directory}") != stamp:
                            paths.update(str(path.resolve()) for path in directory.glob("*.jsonl"))
                            self.index.put(db, f"directory:{directory}", stamp)
                cursor = self.index.get(db, f"discoveryCursor:{self.home}", "")
                cold = [row[0] for row in db.execute("SELECT path FROM processing_scan_sources WHERE home=? AND path>? ORDER BY path LIMIT ?", (self.home, cursor, SOURCE_CHECK_BATCH))]
                if not cold:
                    cold = [row[0] for row in db.execute("SELECT path FROM processing_scan_sources WHERE home=? ORDER BY path LIMIT ?", (self.home, SOURCE_CHECK_BATCH))]
                paths.update(cold)
                if getattr(self, "source_watcher", None) is None:
                    paths.update(row[0] for row in db.execute("SELECT path FROM processing_scan_sources WHERE home=? AND modified_ns>? ORDER BY modified_ns DESC LIMIT ?", (self.home, time.time_ns() - 60 * 60 * 1_000_000_000, SOURCE_CHECK_BATCH)))
                if cold:
                    self.index.put(db, f"discoveryCursor:{self.home}", cold[-1])
            paths.update(row[0] for row in db.execute("SELECT r.source FROM processing_reparse r JOIN processing_scan_sources s ON s.path=r.source WHERE s.home=? ORDER BY r.source LIMIT ?", (self.home, SOURCE_CHECK_BATCH)))
            for source in sorted(paths):
                stored = db.execute("SELECT state,valid FROM processing_scan_sources WHERE path=?", (source,)).fetchone()
                previous, was_valid = (json.loads(stored[0]), stored[1]) if stored else (None, 0)
                state = dict(previous) if previous else None
                if state:
                    state["identity"] = tuple(state["identity"])
                    state["partial"] = base64.b64decode(state["partial"])
                    state["events"] = []
                    if state.get("forceReparse"):
                        state = None
                    elif Path(source).exists() and self.signature(Path(source))[2:] != [state["size"], state["mtimeNs"]] and not self._prefix_valid(db, Path(source)):
                        state = None
                self.metrics["sourcesChecked"] += 1
                updated = _update_codex_file_state(Path(source), state, max_bytes=SOURCE_READ_BYTES)
                if updated is None:
                    if stored and not Path(source).exists():
                        self._remove_source(db, source, was_valid)
                        db.execute("DELETE FROM processing_scan_sources WHERE path=?", (source,))
                        db.execute("DELETE FROM processing_blocks WHERE source=?", (source,))
                        db.execute("DELETE FROM processing_reparse WHERE source=?", (source,))
                        db.execute("DELETE FROM processing_health WHERE source=?", (source,))
                    continue
                replacement = bool(previous and (tuple(previous["identity"]) != updated["identity"] or updated["lineIndex"] < previous["lineIndex"]
                    or updated["offset"] < previous["offset"] or updated is not state))
                self.metrics["sourceBytesRead"] += max(0, updated["offset"] - (0 if replacement else (previous or {}).get("offset", 0)))
                if updated["invalidLines"]:
                    db.execute("INSERT OR REPLACE INTO processing_health VALUES(?,?)", (source, f'{updated["invalidLines"]} invalid session log lines; confirmed ledger retained'))
                else:
                    db.execute("DELETE FROM processing_health WHERE source=?", (source,))
                if replacement:
                    db.execute("DELETE FROM processing_staged_occurrences WHERE source=?", (source,))
                    updated["replacementPending"] = True
                staging = updated.get("replacementPending", False)
                valid = int(not updated["invalidLines"] and updated["hasSessionMeta"])
                if not staging and valid != was_valid:
                    for (event_id,) in db.execute("SELECT event_id FROM processing_occurrences WHERE source=?", (source,)):
                        self._refs(db, event_id, 1 if valid else -1)
                for event in updated.pop("events"):
                    identity = [updated["sessionId"], event["checkedAt"], event["cumulative"] or event["tokens"]]
                    event_id = f'{updated["sessionId"]}:{hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:24]}'
                    self.metrics["eventsHashed"] += 1
                    value = {"eventId": event_id, "sessionId": updated["sessionId"], "sourceFile": Path(source).name,
                        **{key: value for key, value in event.items() if key not in {"index", "lineIndex", "cumulative"}}}
                    if staging:
                        db.execute("INSERT OR REPLACE INTO processing_staged_occurrences VALUES(?,?,?,?)", (source, event_id, event["index"], encode(value)))
                        continue
                    db.execute("INSERT OR IGNORE INTO processing_scan_events(event_id,home,session,event_at,data) VALUES(?,?,?,?,?)",
                        (event_id, self.home, updated["sessionId"], value.get("timestamp"), encode(value)))
                    if db.execute("INSERT OR IGNORE INTO processing_occurrences VALUES(?,?,?,?)", (source, event_id, event["index"], encode(value))).rowcount and valid:
                        self._refs(db, event_id, 1)
                updated["targetSize"] = Path(source).stat().st_size
                if not previous or updated["offset"] != previous["offset"] or replacement:
                    self._record_blocks(db, Path(source), 0 if replacement else (previous or {}).get("offset", 0), updated["offset"])
                if updated["offset"] < updated["targetSize"]:
                    db.execute("INSERT OR IGNORE INTO processing_reparse VALUES(?)", (source,))
                else:
                    db.execute("DELETE FROM processing_reparse WHERE source=?", (source,))
                committed_staging = staging and valid and updated["offset"] == updated["targetSize"] and not updated["partial"]
                if committed_staging:
                    self._remove_source(db, source, was_valid)
                    for event_id, event_index, data in list(db.execute("SELECT event_id,event_index,data FROM processing_staged_occurrences WHERE source=? ORDER BY event_index", (source,))):
                        value = json.loads(data)
                        db.execute("INSERT OR IGNORE INTO processing_scan_events(event_id,home,session,event_at,data) VALUES(?,?,?,?,?)",
                            (event_id, self.home, updated["sessionId"], value.get("timestamp"), data))
                        if db.execute("INSERT OR IGNORE INTO processing_occurrences VALUES(?,?,?,?)", (source, event_id, event_index, data)).rowcount:
                            self._refs(db, event_id, 1)
                    db.execute("DELETE FROM processing_staged_occurrences WHERE source=?", (source,))
                    updated["replacementPending"] = False
                    self.index.put(db, f"reconcile:{self.home}", True)
                elif staging:
                    valid = was_valid
                updated["partial"] = base64.b64encode(updated["partial"]).decode()
                updated.pop("events", None)
                db.execute("INSERT OR REPLACE INTO processing_scan_sources VALUES(?,?,?,?,?)", (source, self.home, encode(updated), valid, updated["mtimeNs"]))
                for (event_id,) in db.execute("SELECT event_id FROM processing_occurrences WHERE source=? AND event_index>?", (source, 0 if committed_staging else (previous or {}).get("eventIndex", 0))):
                    self._winner(db, event_id)
            totals, by_model, fast = empty_token_totals(), {}, {}
            for model, tier, data in db.execute("SELECT model,tier,data FROM processing_totals WHERE home=?", (self.home,)):
                value = json.loads(data)
                for name, count in value.items():
                    totals[name] += count
                    by_model.setdefault(model, empty_token_totals())[name] += count
                    if tier == "fast":
                        fast.setdefault(model, empty_token_totals())[name] += count
            latest = db.execute("SELECT MAX(event_at) FROM processing_scan_events WHERE home=? AND valid_refs>0", (self.home,)).fetchone()[0]
            from datetime import datetime, timezone
            return {"source": "codex_session_logs", "home": str(home), "filesScanned": db.execute(
                "SELECT COALESCE(SUM(count),0) FROM processing_counts WHERE scope='sources' AND account=?", (self.home,)).fetchone()[0],
                "latestEventAt": datetime.fromtimestamp(latest, timezone.utc).isoformat().replace("+00:00", "Z") if latest else None,
                "totals": totals, "byModel": by_model, "fastByModel": fast, "errors": [f"{source}: {error}" for source, error in db.execute("SELECT source,error FROM processing_health ORDER BY source LIMIT 20")], "incremental": True}


    def _scan_rows(self, db, pending=False):
        return db.execute("SELECT e.event_id,e.data,o.source,o.event_index FROM processing_scan_events e "
            "JOIN processing_occurrences o ON o.event_id=e.event_id JOIN processing_scan_sources s ON s.path=o.source "
            "WHERE e.home=? AND s.valid=1 AND o.source=(SELECT MIN(o2.source) FROM processing_occurrences o2 "
            "JOIN processing_scan_sources s2 ON s2.path=o2.source WHERE o2.event_id=e.event_id AND s2.valid=1) "
            + ("AND e.ledger_done=0 " if pending else "") + "ORDER BY o.source,o.event_index" + (" LIMIT 1000" if pending else ""), (self.home,))


    @serialized
    def sync_ledger(self, path, slot, label, timeline, provenance, normalize, machine_id):
        """Commit ledger records, source checkpoints and backup output in one transaction."""
        from monitor_token_ledger import _usage_records, reconcile_token_ledger
        with self.index.transaction() as db:
            reconcile = self.index.get(db, f"reconcile:{self.home}", False)
            if reconcile and db.execute("SELECT 1 FROM processing_scan_sources WHERE home=? AND (json_extract(state,'$.forceReparse')=1 OR "
                "(json_extract(state,'$.replacementPending')=1 AND json_extract(state,'$.invalidLines')=0 AND "
                "(json_extract(state,'$.offset')<json_extract(state,'$.targetSize') OR json_extract(state,'$.partial')<>''))) LIMIT 1", (self.home,)).fetchone():
                return self.index.ledger_cost()
        self._notify_costs()
        if reconcile:
            with self.index.transaction() as db:
                events = [json.loads(row[1]) for row in self._scan_rows(db)]
                source_files = [Path(row[0]).name for row in db.execute("SELECT path FROM processing_scan_sources WHERE home=? AND valid=1", (self.home,))]
                rows = [json.loads(row[0]) for row in db.execute("SELECT data FROM processing_facts WHERE source=? AND kind='tokenLedger' ORDER BY position", ("local:tokenLedger",))]
                updated, new_costs = reconcile_token_ledger(rows, events, slot, label, timeline, provenance, source_files, machine_id)
                self.index.write_records(db, "tokenLedger", updated, replace=True, backup_path=path)
                # Notify after commit; persist the callback batch for retry on failure.
                if new_costs:
                    self.index.put(db, f"ledgerNotifications:{self.home}", new_costs)
                cumulative = {}
                _usage_records(events, slot, label, timeline, cumulative)
                db.execute("DELETE FROM processing_coverage WHERE home=?", (self.home,))
                db.executemany("INSERT INTO processing_coverage VALUES(?,?,?,?,?)", ((self.home, *key, encode(value)) for key, value in cumulative.items()))
                last = {}
                for _, data, source, index in self._scan_rows(db):
                    last[json.loads(data)["sessionId"]] = [source, index]
                db.executemany("INSERT OR REPLACE INTO processing_ledger_order VALUES(?,?,?,?)", ((self.home, key, *value) for key, value in last.items()))
                db.execute("UPDATE processing_scan_events SET ledger_done=1 WHERE home=? AND valid_refs>0", (self.home,))
                self.index.put(db, f"reconcile:{self.home}", False)
        for _ in range(3):
            with self.index.transaction() as db:
                pending = list(self._scan_rows(db, True))
                if not pending:
                    break
                sessions = {json.loads(row[1])["sessionId"] for row in pending}
                last, cumulative = {}, {}
                for session in sessions:
                    order = db.execute("SELECT source,event_index FROM processing_ledger_order WHERE home=? AND session=?", (self.home, session)).fetchone()
                    if order:
                        last[session] = list(order)
                    for model, tier, data in db.execute("SELECT model,tier,data FROM processing_coverage WHERE home=? AND session=?", (self.home, session)):
                        cumulative[(session, model, tier)] = json.loads(data)
                late = any([source, index] < last.get(json.loads(data)["sessionId"], ["", -1]) for _, data, source, index in pending)
                if late:
                    self.index.put(db, f"reconcile:{self.home}", True)
                else:
                    additions, replacements = [], {}
                    for event_id, data, source, index in pending:
                        event = json.loads(data)
                        for usage in _usage_records([event], slot, label, timeline, cumulative):
                            previous = db.execute("SELECT data FROM processing_facts WHERE source=? AND event_id=? LIMIT 1", ("local:tokenLedger", event_id)).fetchone()
                            if previous:
                                previous = json.loads(previous[0])
                                if previous.get("accountSlotId") not in {None, "", "unknown"}:
                                    usage |= {"accountSlotId": previous["accountSlotId"], "accountLabel": previous.get("accountLabel") or label}
                                    if previous.get("sync"):
                                        usage["sync"] = previous["sync"]
                            usage = provenance(usage)
                            if previous is None:
                                additions.append(usage)
                            elif usage != previous:
                                replacements[event_id] = usage
                        last[event["sessionId"]] = [source, index]
                    self.index.write_records(db, "tokenLedger", [normalize(row) for row in replacements.values()] + [normalize(row) for row in additions], backup_path=path)
                    db.executemany("UPDATE processing_scan_events SET ledger_done=1 WHERE event_id=?", ((row[0],) for row in pending))
                    db.executemany("INSERT OR REPLACE INTO processing_coverage VALUES(?,?,?,?,?)", ((self.home, *key, encode(value)) for key, value in cumulative.items()))
                    db.executemany("INSERT OR REPLACE INTO processing_ledger_order VALUES(?,?,?,?)", ((self.home, key, *value) for key, value in last.items()))
                    if additions:
                        self.index.put(db, f"ledgerNotifications:{self.home}", additions)
            if late:
                return self.sync_ledger(path, slot, label, timeline, provenance, normalize, machine_id)
            self._notify_costs()
        self.store.backup.flush()
        self._notify_costs()
        return self.index.ledger_cost()


    def _notify_costs(self):
        with self.index.transaction() as db:
            rows = self.index.get(db, f"ledgerNotifications:{self.home}", [])
        if not rows:
            return
        if getattr(self, "on_new_cost", None) is not None:
            for row in rows:
                self.on_new_cost(row)
        with self.index.transaction() as db:
            db.execute("DELETE FROM processing_meta WHERE name=?", (f"ledgerNotifications:{self.home}",))
