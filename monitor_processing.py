#!/usr/bin/env python3
"""Persistent incremental processing. Canonical JSONL files remain recoverable facts."""

import base64
import hashlib
import json
import os
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from pathlib import Path

from monitor_common import empty_token_totals, parse_timestamp

PROCESSING_SCHEMA_VERSION = 1
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


def encode(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def initialize_processing_index(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS processing_meta (name TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS processing_files (path TEXT PRIMARY KEY, signature TEXT NOT NULL, offset INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS processing_facts (
                source TEXT NOT NULL, position INTEGER NOT NULL, kind TEXT NOT NULL, record_key TEXT NOT NULL,
                account TEXT NOT NULL, session TEXT NOT NULL, event_id TEXT NOT NULL, event_at REAL, data TEXT NOT NULL,
                PRIMARY KEY(source, position)
            );
            CREATE INDEX IF NOT EXISTS processing_facts_key ON processing_facts(kind, record_key);
            CREATE INDEX IF NOT EXISTS processing_facts_account ON processing_facts(kind, account, event_at, position);
            CREATE INDEX IF NOT EXISTS processing_facts_session ON processing_facts(kind, session, event_at);
            CREATE INDEX IF NOT EXISTS processing_facts_event ON processing_facts(kind, event_id);
            CREATE TABLE IF NOT EXISTS processing_scan_sources (path TEXT PRIMARY KEY, home TEXT NOT NULL, state TEXT NOT NULL, valid INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS processing_scan_home ON processing_scan_sources(home);
            CREATE TABLE IF NOT EXISTS processing_scan_events (
                event_id TEXT PRIMARY KEY, home TEXT NOT NULL, session TEXT NOT NULL, event_at REAL,
                data TEXT NOT NULL, valid_refs INTEGER NOT NULL DEFAULT 0, ledger_done INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS processing_scan_pending ON processing_scan_events(home, ledger_done, valid_refs);
            CREATE INDEX IF NOT EXISTS processing_scan_session ON processing_scan_events(home, session);
            CREATE INDEX IF NOT EXISTS processing_scan_latest ON processing_scan_events(home, valid_refs, event_at);
            CREATE INDEX IF NOT EXISTS processing_scan_latest_active ON processing_scan_events(home,event_at) WHERE valid_refs>0;
            CREATE TABLE IF NOT EXISTS processing_occurrences (
                source TEXT NOT NULL, event_id TEXT NOT NULL, event_index INTEGER NOT NULL, data TEXT NOT NULL,
                PRIMARY KEY(source, event_id)
            );
            CREATE INDEX IF NOT EXISTS processing_occurrence_event ON processing_occurrences(event_id, source);
            CREATE INDEX IF NOT EXISTS processing_occurrence_order ON processing_occurrences(source,event_index);
            CREATE INDEX IF NOT EXISTS processing_fact_source_event ON processing_facts(source,event_id);
            CREATE INDEX IF NOT EXISTS processing_fact_source_time ON processing_facts(source,kind,event_at);
            CREATE INDEX IF NOT EXISTS processing_fact_source_key ON processing_facts(source,kind,record_key);
            CREATE INDEX IF NOT EXISTS processing_fact_account_key ON processing_facts(account,kind,record_key,source);
            CREATE TABLE IF NOT EXISTS processing_staged_occurrences (source TEXT NOT NULL, event_id TEXT NOT NULL, event_index INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY(source,event_id));
            CREATE TABLE IF NOT EXISTS processing_totals (home TEXT NOT NULL, model TEXT NOT NULL, tier TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(home, model, tier));
            CREATE TABLE IF NOT EXISTS processing_dirty (view TEXT NOT NULL, account TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(view, account));
            CREATE TABLE IF NOT EXISTS processing_projections (view TEXT NOT NULL, account TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(view, account));
            CREATE TABLE IF NOT EXISTS processing_dirty_days (day TEXT PRIMARY KEY, generation INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS processing_day_cache (day TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS processing_pending_records (record_key TEXT PRIMARY KEY, kind TEXT NOT NULL, data TEXT);
            CREATE TABLE IF NOT EXISTS processing_ledger_sessions (source TEXT NOT NULL, session TEXT NOT NULL, slot TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(source,session,slot));
            CREATE TABLE IF NOT EXISTS processing_coverage (home TEXT NOT NULL, session TEXT NOT NULL, model TEXT NOT NULL, tier TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(home,session,model,tier));
            CREATE TABLE IF NOT EXISTS processing_ledger_order (home TEXT NOT NULL, session TEXT NOT NULL, source TEXT NOT NULL, event_index INTEGER NOT NULL, PRIMARY KEY(home,session));
            CREATE TABLE IF NOT EXISTS processing_effective (record_key TEXT PRIMARY KEY, account TEXT NOT NULL, session TEXT NOT NULL, event_at REAL, data TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS processing_effective_account ON processing_effective(account,event_at);
            CREATE TABLE IF NOT EXISTS processing_changes (view TEXT NOT NULL, account TEXT NOT NULL, kind TEXT NOT NULL, record_key TEXT NOT NULL, event_at REAL, session TEXT NOT NULL, action TEXT NOT NULL, PRIMARY KEY(view,account,kind,record_key));
            CREATE TABLE IF NOT EXISTS processing_display (view TEXT NOT NULL, account TEXT NOT NULL, kind TEXT NOT NULL, record_key TEXT NOT NULL, event_at REAL, data TEXT NOT NULL, PRIMARY KEY(view,kind,record_key));
            CREATE INDEX IF NOT EXISTS processing_display_account ON processing_display(view,account,kind,event_at);
            CREATE INDEX IF NOT EXISTS processing_display_session ON processing_display(view,account,kind,json_extract(data,'$.sessionId'));
            CREATE TABLE IF NOT EXISTS processing_cost_ranges (view TEXT NOT NULL, account TEXT NOT NULL, kind TEXT NOT NULL, record_key TEXT NOT NULL, start REAL NOT NULL, end REAL NOT NULL, PRIMARY KEY(view,kind,record_key));
            CREATE INDEX IF NOT EXISTS processing_cost_range_end ON processing_cost_ranges(view,account,end,start);
            CREATE INDEX IF NOT EXISTS processing_baselines ON processing_facts(source,session) WHERE json_extract(data,'$.recordType')='legacyBaseline';
            CREATE TABLE IF NOT EXISTS processing_quota_points (view TEXT NOT NULL, account TEXT NOT NULL, event_at REAL NOT NULL, record_key TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(view,account,record_key));
            CREATE INDEX IF NOT EXISTS processing_quota_order ON processing_quota_points(view,account,event_at,record_key);
            CREATE TABLE IF NOT EXISTS processing_rank_nodes (scope TEXT NOT NULL, value REAL NOT NULL, priority INTEGER NOT NULL, frequency INTEGER NOT NULL, size INTEGER NOT NULL, left_value REAL, right_value REAL, PRIMARY KEY(scope,value));
            CREATE TABLE IF NOT EXISTS processing_health(source TEXT PRIMARY KEY, error TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS processing_blocks(source TEXT NOT NULL,block INTEGER NOT NULL,size INTEGER NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(source,block));
            CREATE TABLE IF NOT EXISTS processing_reparse(source TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS processing_peer_rows(source TEXT NOT NULL,record_key TEXT NOT NULL,position INTEGER NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(source,record_key));
        """)
        db.execute("INSERT OR IGNORE INTO processing_meta VALUES('schemaVersion', ?)", (str(PROCESSING_SCHEMA_VERSION),))
        if "modified_ns" not in {row[1] for row in db.execute("PRAGMA table_info(processing_scan_sources)")}:
            db.execute("ALTER TABLE processing_scan_sources ADD COLUMN modified_ns INTEGER NOT NULL DEFAULT 0")
        db.execute("CREATE INDEX IF NOT EXISTS processing_scan_active ON processing_scan_sources(home,modified_ns)")
        db.execute("CREATE TABLE IF NOT EXISTS processing_counts(scope TEXT NOT NULL,account TEXT NOT NULL,kind TEXT NOT NULL,count INTEGER NOT NULL,PRIMARY KEY(scope,account,kind))")
        for table, scope, account, kind in (("facts", "NEW.source", "NEW.account", "NEW.kind"), ("effective", "'effective'", "NEW.account", "'tokenLedger'"),
            ("display", "'display:'||NEW.view", "NEW.account", "NEW.kind"), ("quota_points", "'quota:'||NEW.view", "NEW.account", "'quota'"),
            ("scan_sources", "'sources'", "NEW.home", "'files'")):
            db.executescript(f"""
                CREATE TRIGGER IF NOT EXISTS processing_count_{table}_insert AFTER INSERT ON processing_{table} BEGIN
                    INSERT INTO processing_counts VALUES({scope},{account},{kind},1) ON CONFLICT(scope,account,kind) DO UPDATE SET count=count+1;
                END;
                CREATE TRIGGER IF NOT EXISTS processing_count_{table}_delete AFTER DELETE ON processing_{table} BEGIN
                    UPDATE processing_counts SET count=count-1 WHERE scope={scope.replace('NEW.', 'OLD.')} AND account={account.replace('NEW.', 'OLD.')} AND kind={kind.replace('NEW.', 'OLD.')};
                END;
            """)
        if db.execute("SELECT 1 FROM processing_meta WHERE name='countsInitialized'").fetchone() is None:
            for table, scope, account, kind in (("facts", "source", "account", "kind"), ("effective", "'effective'", "account", "'tokenLedger'"),
                ("display", "'display:'||view", "account", "kind"), ("quota_points", "'quota:'||view", "account", "'quota'"), ("scan_sources", "'sources'", "home", "'files'")):
                db.execute(f"INSERT OR REPLACE INTO processing_counts SELECT {scope},{account},{kind},COUNT(*) FROM processing_{table} GROUP BY 1,2,3")
            db.execute("INSERT INTO processing_meta VALUES('countsInitialized','true')")
        if db.execute("SELECT value FROM processing_meta WHERE name='schemaVersion'").fetchone()[0] != str(PROCESSING_SCHEMA_VERSION):
            raise ValueError("Unsupported processing index schema")


class ProcessingIndex:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.lock = threading.RLock()
        self.metrics = {"sourceBytesRead": 0, "canonicalBytesRead": 0, "sourceVerificationBytes": 0, "eventsHashed": 0, "recordsImported": 0, "fullImports": 0, "sourcesChecked": 0}
        import monitor_tokens
        self.parser_rule = hashlib.sha256(Path(monitor_tokens.__file__).read_bytes()).hexdigest()
        initialize_processing_index(self.path)

    @contextmanager
    def transaction(self):
        with self.lock:
            db = sqlite3.connect(self.path, timeout=10)
            db.execute("PRAGMA recursive_triggers=ON")
            try:
                with db:
                    yield db
            finally:
                db.close()

    @staticmethod
    def get(db, name, default=None):
        row = db.execute("SELECT value FROM processing_meta WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else default

    @staticmethod
    def put(db, name, value):
        db.execute("INSERT INTO processing_meta VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value", (name, encode(value)))

    @staticmethod
    def signature(path):
        try:
            stat = path.stat()
            return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns]
        except FileNotFoundError:
            return None

    def dirty(self, db, account, reason="records", views=("local", "merged")):
        for view in views:
            db.execute("INSERT INTO processing_dirty VALUES(?,?,?) ON CONFLICT(view,account) DO UPDATE SET reason=CASE "
                "WHEN excluded.reason='records' THEN processing_dirty.reason ELSE excluded.reason END", (view, account, reason))

    @contextmanager
    def defer_sessions(self, db, enabled=True):
        if not enabled or getattr(self, "_deferred_sessions", None) is not None:
            yield
            return
        self._deferred_sessions = set()
        try:
            yield
            for account, session in self._deferred_sessions:
                self._rebuild_merged_session(db, account, session)
        finally:
            self._deferred_sessions = None

    def _rebuild_merged_session(self, db, account, session):
        from monitor_token_ledger import token_sessions_from_ledger
        source = f"view:merged:{account}"
        db.execute("DELETE FROM processing_ledger_sessions WHERE source=? AND session=?", (source, session))
        records = [json.loads(row[0]) for row in db.execute("SELECT data FROM processing_effective WHERE account=? AND session=? ORDER BY event_at,record_key", (account, session))]
        for item in token_sessions_from_ledger(records):
            db.execute("INSERT INTO processing_ledger_sessions VALUES(?,?,?,?)", (source, session, str(item.get("accountSlotId") or "unknown"), encode(item)))

    def fact(self, db, source, position, kind, row):
        from monitor_usage_sync import record_account_key, record_key
        session = row.get("session") or row
        previous = db.execute("SELECT account,data FROM processing_facts WHERE source=? AND position=?", (source, position)).fetchone()
        data = encode(row)
        if previous and previous[1] == data:
            return False
        if previous:
            self.dirty(db, previous[0], "record correction", views=("merged",) if source.startswith("peer:") else ("local", "merged"))
        account = record_account_key(row)
        duplicate = db.execute("SELECT 1 FROM processing_facts WHERE source=? AND kind=? AND record_key=? LIMIT 1", (source, kind, record_key(kind, row))).fetchone()
        db.execute("INSERT OR REPLACE INTO processing_facts VALUES(?,?,?,?,?,?,?,?,?)", (
            source, position, kind, record_key(kind, row), account, str(session.get("sessionId") or ""), str(row.get("eventId") or ""),
            parse_timestamp(row.get("checkedAt") or row.get("occurredAt") or session.get("updatedAt")), data,
        ))
        if kind == "tokenLedger" and previous is None and not source.startswith("peer:"):
            self._ledger_add(db, source, row)
        self.dirty(db, account, views=("merged",) if source.startswith("peer:") else ("local", "merged"))
        if kind == "tokenLedger":
            if previous and (old_key := record_key(kind, json.loads(previous[1]))) != record_key(kind, row):
                self._effective(db, old_key)
            self._effective(db, record_key(kind, row))
        for view in (("merged",) if source.startswith("peer:") else ("local", "merged")):
            if view == "merged" and kind == "tokenLedger":
                continue
            db.execute("INSERT OR REPLACE INTO processing_changes VALUES(?,?,?,?,?,?,?)", (view, account, kind, record_key(kind, row),
                parse_timestamp(row.get("checkedAt") or row.get("occurredAt") or session.get("updatedAt")), str(session.get("sessionId") or ""),
                "replace" if previous or duplicate or row.get("recordType") == "legacyBaseline" else "append"))
        if not source.startswith("peer:"):
            db.execute("INSERT OR REPLACE INTO processing_pending_records VALUES(?,?,?)", (record_key(kind, row), kind, data))
        self.metrics["recordsImported"] += 1
        return True

    def refresh(self, path, kind, normalize=lambda row: row):
        """Tail an append-only canonical source; stage replacements in one transaction."""
        path = Path(path)
        source = str(path.resolve())
        signature = self.signature(path)
        with self.transaction() as db:
            previous = db.execute("SELECT signature,offset FROM processing_files WHERE path=?", (source,)).fetchone()
            old = json.loads(previous[0]) if previous else None
            if previous and old == signature:
                return False
            append = bool(old and signature and old[:2] == signature[:2] and signature[2] > old[2] and previous[1] <= old[2])
            if append and not self._prefix_valid(db, path):
                append = False
            offset = previous[1] if append else 0
            with self.defer_sessions(db, enabled=not append):
                if not append:
                    self.metrics["fullImports"] += 1
                    for (account,) in list(db.execute("SELECT DISTINCT account FROM processing_facts WHERE source=?", (source,))):
                        self.dirty(db, account, "source replacement")
                    db.execute("INSERT OR REPLACE INTO processing_pending_records SELECT record_key,kind,NULL FROM processing_facts WHERE source=?", (source,))
                    old_keys = [row[0] for row in db.execute("SELECT record_key FROM processing_facts WHERE source=? AND kind='tokenLedger'", (source,))]
                    db.execute("DELETE FROM processing_facts WHERE source=?", (source,))
                    for key in old_keys:
                        self._effective(db, key)
                    if kind == "tokenLedger":
                        db.execute("DELETE FROM processing_ledger_sessions WHERE source=?", (source,))
                        from monitor_common import empty_cost_totals
                        self.put(db, f"ledgerCost:{source}", [empty_cost_totals(), {}])
                changed = not append
                if signature is not None:
                    with path.open("rb") as stream:
                        stream.seek(offset)
                        while stream.tell() < signature[2]:
                            position = stream.tell()
                            line = stream.readline(signature[2] - stream.tell())
                            self.metrics["canonicalBytesRead"] += len(line)
                            if not line.strip():
                                offset = stream.tell()
                                continue
                            try:
                                row = json.loads(line)
                            except (UnicodeDecodeError, json.JSONDecodeError):
                                if stream.tell() == signature[2] and not line.endswith((b"\n", b"\r")):
                                    break
                                raise ValueError(f"Invalid {kind} record at byte {position} in {path.name}")
                            if not isinstance(row, dict):
                                raise ValueError(f"Unsupported {kind} record in {path.name}")
                            row = normalize(row)
                            if row is not None:
                                changed = self.fact(db, source, position, kind, row) or changed
                            offset = stream.tell()
                db.execute("INSERT OR REPLACE INTO processing_files VALUES(?,?,?)", (source, encode(signature), offset))
                self._record_blocks(db, path, previous[1] if append else 0, offset)
                if changed:
                    self.put(db, "factRevision", self.get(db, "factRevision", 0) + 1)
                return changed

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
        with self.transaction() as db:
            if not force and time.time() - self.get(db, "sourceAuditAt", time.time()) < SOURCE_DISCOVERY_SECONDS:
                if self.get(db, "sourceAuditAt") is None:
                    self.put(db, "sourceAuditAt", time.time())
                return
            cursor = self.get(db, "sourceAuditCursor", ["", -1])
            rows = list(db.execute("SELECT source,block,size,digest FROM processing_blocks WHERE (source,block)>(?,?) ORDER BY source,block LIMIT ?", (*cursor, SOURCE_CHECK_BATCH)))
            if not rows:
                rows = list(db.execute("SELECT source,block,size,digest FROM processing_blocks ORDER BY source,block LIMIT ?", (SOURCE_CHECK_BATCH,)))
            for source, block, size, digest in rows:
                try:
                    with Path(source).open("rb") as stream:
                        stream.seek(block * 4096)
                        data = stream.read(size)
                    self.metrics["sourceVerificationBytes"] += len(data)
                except FileNotFoundError:
                    continue
                if hashlib.sha256(data).hexdigest() != digest:
                    db.execute("DELETE FROM processing_files WHERE path=?", (source,))
                    row = db.execute("SELECT state,home FROM processing_scan_sources WHERE path=?", (source,)).fetchone()
                    if row:
                        state = json.loads(row[0])
                        state["forceReparse"] = True
                        db.execute("UPDATE processing_scan_sources SET state=? WHERE path=?", (encode(state), source))
                        db.execute("INSERT OR IGNORE INTO processing_reparse VALUES(?)", (source,))
                        self.put(db, f"reconcile:{row[1]}", True)
            if rows:
                self.put(db, "sourceAuditCursor", list(rows[-1][:2]))
            self.put(db, "sourceAuditAt", time.time())

    def rows(self, source, kind, account=None, session=None):
        with self.transaction() as db:
            query, values = "SELECT data FROM processing_facts WHERE source=? AND kind=?", [str(Path(source).resolve()), kind]
            if account is not None:
                query += " AND account=?"
                values.append(account)
            if session is not None:
                query += " AND session=?"
                values.append(session)
            return [json.loads(row[0]) for row in db.execute(query + " ORDER BY position", values)]

    def revision(self):
        with self.transaction() as db:
            return self.get(db, "factRevision", 0)

    def repair(self):
        """Invalidate derived checkpoints while retaining facts, prepared batches and v4 ownership."""
        with self.transaction() as db:
            db.execute("DELETE FROM processing_files")
            for table in ("display", "projections", "quota_points", "rank_nodes", "cost_ranges"):
                db.execute(f"DELETE FROM processing_{table}")
            for (account,) in list(db.execute("SELECT DISTINCT account FROM processing_facts")):
                self.dirty(db, account, "explicit repair")
            for source, home, data in list(db.execute("SELECT path,home,state FROM processing_scan_sources")):
                state = json.loads(data)
                state["forceReparse"] = True
                db.execute("UPDATE processing_scan_sources SET state=? WHERE path=?", (encode(state), source))
                db.execute("INSERT OR IGNORE INTO processing_reparse VALUES(?)", (source,))
                self.put(db, f"reconcile:{home}", True)
            db.execute("DELETE FROM processing_meta WHERE name='projectionRule' OR name='peersInitialized' OR name LIKE 'peerRevision:%'")

    def _ledger_add(self, db, source, row):
        from monitor_token_ledger import _empty_session, _add_tokens
        from monitor_tokens import normalize_codex_model, sum_cost_totals
        from monitor_common import empty_cost_totals
        if row.get("recordType") not in {"usage", "legacyBaseline"}:
            return
        value = row.get("session") or row
        key = (source, str(value.get("sessionId") or ""), str(value.get("accountSlotId") or "unknown"))
        previous = db.execute("SELECT data FROM processing_ledger_sessions WHERE source=? AND session=? AND slot=?", key).fetchone()
        session = json.loads(previous[0]) if previous else _empty_session(row)
        if row.get("recordType") == "legacyBaseline":
            session = json.loads(encode(value))
            if (row.get("sync") or {}).get("accountId"):
                session.setdefault("usageAccountId", row["sync"]["accountId"])
            if previous:
                from monitor_token_ledger import token_sessions_from_ledger
                sessions = token_sessions_from_ledger([json.loads(item[0]) for item in db.execute(
                    "SELECT data FROM processing_facts WHERE source=? AND kind='tokenLedger' AND session=? ORDER BY position", (source, key[1]))])
                session = next((item for item in sessions if str(item.get("accountSlotId") or "unknown") == key[2]), session)
        else:
            occurred = row.get("occurredAt")
            if occurred and (parse_timestamp(occurred) or 0) < (parse_timestamp(session.get("startedAt")) or float("inf")):
                session["startedAt"] = occurred
            if occurred and (parse_timestamp(occurred) or 0) >= (parse_timestamp(session.get("updatedAt")) or 0):
                session["updatedAt"] = occurred
            _add_tokens(session["tokens"], row.get("tokens"))
            session["cost"] = sum_cost_totals(session.get("cost"), row.get("cost"))
            model = session["byModel"].setdefault(str(row.get("rawModel") or "unknown"), {"tokens": empty_token_totals(), "cost": empty_cost_totals()})
            _add_tokens(model["tokens"], row.get("tokens"))
            model["cost"] = sum_cost_totals(model.get("cost"), row.get("cost"))
            if row.get("serviceTier") == "fast":
                _add_tokens(model.setdefault("fastTokens", empty_token_totals()), row.get("tokens"))
        db.execute("INSERT OR REPLACE INTO processing_ledger_sessions VALUES(?,?,?,?)", (*key, encode(session)))
        totals, by_model = self.get(db, f"ledgerCost:{source}", [{}, {}])
        # Sum fixed-point contributions rather than rescanning every stored session.
        old = json.loads(previous[0]) if previous else {}
        for name in empty_cost_totals():
            totals[name] = round((totals.get(name) or 0) + (session.get("cost", {}).get(name) or 0) - (old.get("cost", {}).get(name) or 0), 8)
        for sign, item in ((-1, old), (1, session)):
            for model, data in (item.get("byModel") or {}).items():
                target = by_model.setdefault(normalize_codex_model(model), empty_cost_totals())
                for name in target:
                    target[name] = round(target[name] + sign * (data.get("cost", {}).get(name) or 0), 8)
        self.put(db, f"ledgerCost:{source}", [totals, by_model])

    def _effective(self, db, key):
        from monitor_usage_sync import canonical_ledger_row, _ledger_content, content_hash, record_account_key
        from monitor_token_ledger import token_sessions_from_ledger
        previous = db.execute("SELECT account,session,event_at,data FROM processing_effective WHERE record_key=?", (key,)).fetchone()
        rows = [canonical_ledger_row(json.loads(row[0])) for row in db.execute(
            "SELECT data FROM processing_facts WHERE kind='tokenLedger' AND record_key=? ORDER BY source,position", (key,))]
        rows = [row for row in rows if row is not None]
        winner = max(enumerate(rows), key=lambda item: (content_hash(_ledger_content(item[1])), item[0]))[1] if rows else None
        if previous and winner is not None and previous[3] == encode(winner):
            return
        if winner is None:
            db.execute("DELETE FROM processing_effective WHERE record_key=?", (key,))
        else:
            value = winner.get("session") or winner
            db.execute("INSERT OR REPLACE INTO processing_effective VALUES(?,?,?,?,?)", (key, record_account_key(winner), str(value.get("sessionId") or ""),
                parse_timestamp(winner.get("occurredAt") or value.get("updatedAt")), encode(winner)))
        affected = {(previous[0], previous[1])} if previous else set()
        if winner is not None:
            affected.add((record_account_key(winner), str((winner.get("session") or winner).get("sessionId") or "")))
        for account, session in affected:
            source = f"view:merged:{account}"
            deferred = getattr(self, "_deferred_sessions", None)
            if previous is None and winner is not None and winner.get("recordType") == "usage" and (deferred is None or (account, session) not in deferred):
                self._ledger_add(db, source, winner)
            elif deferred is not None:
                deferred.add((account, session))
            else:
                self._rebuild_merged_session(db, account, session)
            self.dirty(db, account, "ledger correction" if previous else "records", ("merged",))
            db.execute("INSERT OR REPLACE INTO processing_changes VALUES(?,?,?,?,?,?,?)", ("merged", account, "tokenLedger", key,
                previous[2] if previous else parse_timestamp((winner or {}).get("occurredAt")), session, "replace" if previous else "append"))

    def ledger_cost(self, path):
        from monitor_common import empty_cost_totals
        with self.transaction() as db:
            return tuple(self.get(db, f"ledgerCost:{str(Path(path).resolve())}", [empty_cost_totals(), {}]))

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
                self.put(db, f"reconcile:{self.home}", True)

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
            self._winner(db, event_id)

    def scan(self, home):
        from monitor_tokens import _update_codex_file_state, collect_codex_session_files
        self.audit()
        with self.transaction() as db:
            self.home = str(Path(home).resolve())
            import monitor_tokens
            rule = self.parser_rule
            if self.get(db, f"parserRule:{self.home}", rule) != rule:
                for source, data in list(db.execute("SELECT path,state FROM processing_scan_sources WHERE home=?", (self.home,))):
                    state = json.loads(data)
                    state["forceReparse"] = True
                    db.execute("UPDATE processing_scan_sources SET state=? WHERE path=?", (encode(state), source))
                    db.execute("INSERT OR IGNORE INTO processing_reparse VALUES(?)", (source,))
            self.put(db, f"parserRule:{self.home}", rule)
            discovery = self.get(db, f"discovery:{self.home}", 0)
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
                self.put(db, f"discovery:{self.home}", time.time())
            else:
                # Recent creation is checked promptly; cold-source reconciliation is rotated.
                from datetime import datetime
                today = datetime.now()
                recent = Path(home) / "sessions" / f"{today.year:04}" / f"{today.month:02}" / f"{today.day:02}"
                for directory in (recent, Path(home) / "archived_sessions"):
                    if directory.is_dir():
                        stamp = directory.stat().st_mtime_ns
                        if self.get(db, f"directory:{directory}") != stamp:
                            paths.update(str(path.resolve()) for path in directory.glob("*.jsonl"))
                            self.put(db, f"directory:{directory}", stamp)
                cursor = self.get(db, f"discoveryCursor:{self.home}", "")
                cold = [row[0] for row in db.execute("SELECT path FROM processing_scan_sources WHERE home=? AND path>? ORDER BY path LIMIT ?", (self.home, cursor, SOURCE_CHECK_BATCH))]
                if not cold:
                    cold = [row[0] for row in db.execute("SELECT path FROM processing_scan_sources WHERE home=? ORDER BY path LIMIT ?", (self.home, SOURCE_CHECK_BATCH))]
                paths.update(cold)
                if getattr(self, "source_watcher", None) is None:
                    paths.update(row[0] for row in db.execute("SELECT path FROM processing_scan_sources WHERE home=? AND modified_ns>? ORDER BY modified_ns DESC LIMIT ?", (self.home, time.time_ns() - 60 * 60 * 1_000_000_000, SOURCE_CHECK_BATCH)))
                if cold:
                    self.put(db, f"discoveryCursor:{self.home}", cold[-1])
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
                    self.put(db, f"reconcile:{self.home}", True)
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

    def sync_ledger(self, path, slot, label, timeline, provenance, normalize, machine_id):
        """Durable raw-event queue plus a prepared append batch makes retries idempotent."""
        from monitor_token_ledger import _usage_records, _source_highwaters, append_token_ledger, sync_token_ledger, load_token_ledger, write_token_ledger
        self.refresh(path, "tokenLedger", normalize)
        with self.transaction() as db:
            prepared = self.get(db, f"ledgerBatch:{self.home}")
            reconcile = self.get(db, f"reconcile:{self.home}", False)
            if reconcile and db.execute("SELECT 1 FROM processing_scan_sources WHERE home=? AND (json_extract(state,'$.forceReparse')=1 OR "
                "(json_extract(state,'$.replacementPending')=1 AND json_extract(state,'$.invalidLines')=0 AND "
                "(json_extract(state,'$.offset')<json_extract(state,'$.targetSize') OR json_extract(state,'$.partial')<>''))) LIMIT 1", (self.home,)).fetchone():
                return self.ledger_cost(path)
        if prepared is not None:
            self._finish_ledger_batch(path, prepared, normalize)
        if reconcile:
            with self.transaction() as db:
                events = [json.loads(row[1]) for row in self._scan_rows(db)]
                source_files = [Path(row[0]).name for row in db.execute("SELECT path FROM processing_scan_sources WHERE home=? AND valid=1", (self.home,))]
                legacy_ids = [f'{json.loads(state)["sessionId"]}:{index}' for state, index in db.execute(
                    "SELECT s.state,o.event_index FROM processing_scan_sources s JOIN processing_occurrences o ON o.source=s.path WHERE s.home=? AND s.valid=1", (self.home,))]
            sync_token_ledger(path, [], events, slot, label, timeline, record_provenance=provenance, source_files=source_files, legacy_event_ids=legacy_ids, own_machine_id=machine_id)
            self.refresh(path, "tokenLedger", normalize)
            cumulative = {}
            _usage_records(events, _source_highwaters(self.rows(path, "tokenLedger")), slot, label, timeline, cumulative)
            with self.transaction() as db:
                db.execute("DELETE FROM processing_coverage WHERE home=?", (self.home,))
                db.executemany("INSERT INTO processing_coverage VALUES(?,?,?,?,?)", ((self.home, *key, encode(value)) for key, value in cumulative.items()))
                last = {}
                for _, data, source, index in self._scan_rows(db):
                    last[json.loads(data)["sessionId"]] = [source, index]
                db.executemany("INSERT OR REPLACE INTO processing_ledger_order VALUES(?,?,?,?)", ((self.home, key, *value) for key, value in last.items()))
                db.execute("UPDATE processing_scan_events SET ledger_done=1 WHERE home=? AND valid_refs>0", (self.home,))
                self.put(db, f"reconcile:{self.home}", False)
        for _ in range(3):
            with self.transaction() as db:
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
                    self.put(db, f"reconcile:{self.home}", True)
            if late:
                return self.sync_ledger(path, slot, label, timeline, provenance, normalize, machine_id)
            with self.transaction() as db:
                baselines = [json.loads(row[0]) for session in sessions for row in db.execute("SELECT data FROM processing_facts WHERE source=? AND session=? "
                    "AND json_extract(data,'$.recordType')='legacyBaseline'", (str(Path(path).resolve()), session))]
                additions, replacements = [], {}
                for event_id, data, source, index in pending:
                    event = json.loads(data)
                    for usage in _usage_records([event], _source_highwaters(baselines), slot, label, timeline, cumulative):
                        previous = db.execute("SELECT data FROM processing_facts WHERE source=? AND event_id=? LIMIT 1", (str(Path(path).resolve()), event_id)).fetchone()
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
                prepared = {"ids": [row[0] for row in pending], "additions": additions, "replacements": replacements,
                    "coverage": [[list(key), value] for key, value in cumulative.items()], "order": last}
                self.put(db, f"ledgerBatch:{self.home}", prepared)
            self._finish_ledger_batch(path, prepared, normalize)
        return self.ledger_cost(path)

    def _finish_ledger_batch(self, path, batch, normalize):
        from monitor_token_ledger import append_token_ledger, load_token_ledger, write_token_ledger
        self.refresh(path, "tokenLedger", normalize)
        with self.transaction() as db:
            additions = [row for row in batch["additions"] if not db.execute("SELECT 1 FROM processing_facts WHERE source=? AND event_id=? LIMIT 1",
                (str(Path(path).resolve()), row["eventId"])).fetchone()]
        if batch["replacements"]:
            rows = load_token_ledger(path)
            updated = [batch["replacements"].get(row.get("eventId"), row) for row in rows]
            if updated != rows:
                write_token_ledger(path, updated)
        append_token_ledger(path, additions)
        self.refresh(path, "tokenLedger", normalize)
        with self.transaction() as db:
            db.executemany("UPDATE processing_scan_events SET ledger_done=1 WHERE event_id=?", ((key,) for key in batch["ids"]))
            db.executemany("INSERT OR REPLACE INTO processing_coverage VALUES(?,?,?,?,?)", ((self.home, *key, encode(value)) for key, value in batch["coverage"]))
            db.executemany("INSERT OR REPLACE INTO processing_ledger_order VALUES(?,?,?,?)", ((self.home, key, *value) for key, value in batch["order"].items()))
            db.execute("DELETE FROM processing_meta WHERE name=?", (f"ledgerBatch:{self.home}",))
