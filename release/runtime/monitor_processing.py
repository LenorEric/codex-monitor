#!/usr/bin/env python3
"""SQLite access and transactional processing state; source I/O lives in monitor_ingestion."""

import json
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from pathlib import Path

from monitor_common import empty_token_totals, parse_timestamp
import monitor_sample_store as samples

PROCESSING_SCHEMA_VERSION = 1
VACUUM_FREE_BYTES = 100 * 1024 * 1024
VACUUM_FREE_RATIO = .20
VACUUM_BATCH_PAGES = 256
VACUUM_BATCH_SECONDS = .05
VACUUM_CHECK_SECONDS = 60


def encode(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _migrate_processing_tables(db):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='processing_effective'").fetchone():
        for name, in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='processing_effective'").fetchall():
            db.execute(f"DROP TRIGGER IF EXISTS {name}")
        db.execute(
            "CREATE TABLE IF NOT EXISTS processing_effective_refs("
            "record_key TEXT PRIMARY KEY,"
            "account_id INTEGER NOT NULL REFERENCES sample_accounts(id),"
            "session_id INTEGER NOT NULL REFERENCES sample_sessions(id),"
            "event_at REAL,"
            "sample_id INTEGER NOT NULL REFERENCES sample_records(id),"
            "attribution TEXT)"
        )
        for key, account, session, event_at, data in db.execute("SELECT record_key,account,session,event_at,data FROM processing_effective").fetchall():
            sample_id, attrs = samples.insert_sample(db, "tokenLedger", json.loads(data))
            db.execute(
                "INSERT OR REPLACE INTO processing_effective_refs VALUES(?,?,?,?,?,?)",
                (key, samples._dimension(db, "account", account), samples._dimension(db, "session", session), event_at, sample_id, attrs),
            )
        db.execute("DROP TABLE processing_effective")
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='processing_quota_points'").fetchone():
        for name, in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='processing_quota_points'").fetchall():
            db.execute(f"DROP TRIGGER IF EXISTS {name}")
        db.execute(
            "CREATE TABLE IF NOT EXISTS processing_quota_point_refs("
            "view TEXT NOT NULL,"
            "account TEXT NOT NULL,"
            "event_at REAL NOT NULL,"
            "record_key TEXT NOT NULL,"
            "payload_id INTEGER NOT NULL REFERENCES processing_payloads(id),"
            "PRIMARY KEY(view,account,record_key))"
        )
        db.execute(
            "INSERT OR REPLACE INTO processing_quota_point_refs(view,account,event_at,record_key,payload_id) "
            "SELECT view,account,event_at,record_key,put_payload(data,NULL) FROM processing_quota_points"
        )
        db.execute("DROP TABLE processing_quota_points")
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='processing_display'").fetchone():
        for name, in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='processing_display'").fetchall():
            db.execute(f"DROP TRIGGER IF EXISTS {name}")
        db.execute(
            "CREATE TABLE IF NOT EXISTS processing_display_refs("
            "view TEXT NOT NULL,"
            "account TEXT NOT NULL,"
            "kind TEXT NOT NULL,"
            "record_key TEXT NOT NULL,"
            "event_at REAL,"
            "session_id TEXT,"
            "payload_id INTEGER NOT NULL REFERENCES processing_payloads(id),"
            "PRIMARY KEY(view,kind,record_key))"
        )
        db.execute(
            "INSERT OR REPLACE INTO processing_display_refs(view,account,kind,record_key,event_at,session_id,payload_id) "
            "SELECT view,account,kind,record_key,event_at,json_extract(data,'$.sessionId'),"
            "put_payload(data,CASE WHEN kind='quotaPoints' THEN record_key END) FROM processing_display"
        )
        db.execute("DROP TABLE processing_display")


def _install_count_triggers(db):
    specs = (
        (
            "facts",
            "fact_refs",
            "(SELECT value FROM sample_sources WHERE id=NEW.source_id)",
            "(SELECT value FROM sample_accounts WHERE id=NEW.account_id)",
            "(SELECT kind FROM sample_records WHERE id=NEW.sample_id)",
        ),
        (
            "effective",
            "effective_refs",
            "'effective'",
            "(SELECT value FROM sample_accounts WHERE id=NEW.account_id)",
            "'tokenLedger'",
        ),
        ("display", "display_refs", "'display:'||NEW.view", "NEW.account", "NEW.kind"),
        ("quota_points", "quota_point_refs", "'quota:'||NEW.view", "NEW.account", "'quota'"),
        ("scan_sources", "scan_sources", "'sources'", "NEW.home", "'files'"),
    )
    for table, physical, scope, account, kind in specs:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (f"processing_{physical}",)).fetchone():
            continue
        db.execute(f"DROP TRIGGER IF EXISTS processing_count_{table}_insert")
        db.execute(f"DROP TRIGGER IF EXISTS processing_count_{table}_delete")
        db.execute(
            f"CREATE TRIGGER processing_count_{table}_insert AFTER INSERT ON processing_{physical} BEGIN "
            f"UPDATE processing_counts SET count=count+1 WHERE scope={scope} AND account={account} AND kind={kind}; "
            f"INSERT INTO processing_counts SELECT {scope},{account},{kind},1 "
            f"WHERE NOT EXISTS(SELECT 1 FROM processing_counts WHERE scope={scope} AND account={account} AND kind={kind}); END"
        )
        db.execute(
            f"CREATE TRIGGER processing_count_{table}_delete AFTER DELETE ON processing_{physical} BEGIN "
            f"UPDATE processing_counts SET count=count-1 WHERE scope={scope.replace('NEW.', 'OLD.')} "
            f"AND account={account.replace('NEW.', 'OLD.')} AND kind={kind.replace('NEW.', 'OLD.')}; END"
        )


def _rebuild_counts(db):
    db.execute("DELETE FROM processing_counts")
    for table, scope, account, kind in (
        ("facts", "source", "account", "kind"),
        ("effective", "'effective'", "account", "'tokenLedger'"),
        ("display", "'display:'||view", "account", "kind"),
        ("quota_points", "'quota:'||view", "account", "'quota'"),
        ("scan_sources", "'sources'", "home", "'files'"),
    ):
        if db.execute("SELECT 1 FROM sqlite_master WHERE name=?", (f"processing_{table}",)).fetchone():
            db.execute(f"INSERT OR REPLACE INTO processing_counts SELECT {scope},{account},{kind},COUNT(*) FROM processing_{table} GROUP BY 1,2,3")


def initialize_processing_index(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(samples.connect(path)) as db, db:
        db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        db.execute("PRAGMA journal_mode=WAL")
        samples.prepare(db)
        _migrate_processing_tables(db)
        db.executescript("""
            CREATE TABLE IF NOT EXISTS processing_meta (name TEXT PRIMARY KEY, value TEXT NOT NULL);
            -- Kept empty for released migration 15, which still clears this table.
            CREATE TABLE IF NOT EXISTS processing_files (path TEXT PRIMARY KEY, signature TEXT NOT NULL, offset INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS processing_backup_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT NOT NULL, data TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS processing_backup_path ON processing_backup_queue(path,id);
            CREATE TABLE IF NOT EXISTS processing_scan_sources (path TEXT PRIMARY KEY, home TEXT NOT NULL, state TEXT NOT NULL, valid INTEGER NOT NULL, modified_ns INTEGER NOT NULL DEFAULT 0);
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
            CREATE TABLE IF NOT EXISTS processing_effective_refs (
                record_key TEXT PRIMARY KEY,
                account_id INTEGER NOT NULL REFERENCES sample_accounts(id),
                session_id INTEGER NOT NULL REFERENCES sample_sessions(id),
                event_at REAL,
                sample_id INTEGER NOT NULL REFERENCES sample_records(id),
                attribution TEXT
            );
            CREATE INDEX IF NOT EXISTS processing_effective_account ON processing_effective_refs(account_id,event_at);
            CREATE INDEX IF NOT EXISTS processing_effective_sample ON processing_effective_refs(sample_id);
            CREATE TRIGGER IF NOT EXISTS processing_effective_refs_delete AFTER DELETE ON processing_effective_refs BEGIN
                INSERT OR IGNORE INTO sample_releases VALUES(OLD.sample_id);
            END;
            CREATE TRIGGER IF NOT EXISTS processing_effective_refs_update AFTER UPDATE ON processing_effective_refs BEGIN
                INSERT OR IGNORE INTO sample_releases VALUES(OLD.sample_id);
            END;
            CREATE VIEW IF NOT EXISTS processing_effective AS
                SELECT e.record_key,a.value AS account,s.value AS session,e.event_at,sample_row(e.sample_id,e.attribution) AS data
                FROM processing_effective_refs e
                JOIN sample_accounts a ON a.id=e.account_id
                JOIN sample_sessions s ON s.id=e.session_id;
            CREATE TRIGGER IF NOT EXISTS processing_effective_delete INSTEAD OF DELETE ON processing_effective BEGIN
                DELETE FROM processing_effective_refs WHERE record_key=OLD.record_key;
            END;
            CREATE TABLE IF NOT EXISTS processing_changes (view TEXT NOT NULL, account TEXT NOT NULL, kind TEXT NOT NULL, record_key TEXT NOT NULL, event_at REAL, session TEXT NOT NULL, action TEXT NOT NULL, PRIMARY KEY(view,account,kind,record_key));
            -- Empty compatibility schema for released display migrations; runtime uses delivery_* instead.
            CREATE TABLE IF NOT EXISTS processing_display_refs (
                view TEXT NOT NULL,
                account TEXT NOT NULL,
                kind TEXT NOT NULL,
                record_key TEXT NOT NULL,
                event_at REAL,
                session_id TEXT,
                payload_id INTEGER NOT NULL REFERENCES processing_payloads(id),
                PRIMARY KEY(view,kind,record_key)
            );
            CREATE INDEX IF NOT EXISTS processing_display_account ON processing_display_refs(view,account,kind,event_at);
            CREATE INDEX IF NOT EXISTS processing_display_session ON processing_display_refs(view,account,kind,session_id);
            CREATE INDEX IF NOT EXISTS processing_display_payload ON processing_display_refs(payload_id);
            CREATE TRIGGER IF NOT EXISTS processing_display_refs_delete AFTER DELETE ON processing_display_refs BEGIN
                INSERT OR IGNORE INTO processing_payload_releases VALUES(OLD.payload_id);
            END;
            CREATE TRIGGER IF NOT EXISTS processing_display_refs_update AFTER UPDATE ON processing_display_refs BEGIN
                INSERT OR IGNORE INTO processing_payload_releases VALUES(OLD.payload_id);
            END;
            CREATE VIEW IF NOT EXISTS processing_display AS
                SELECT r.view,r.account,r.kind,r.record_key,r.event_at,
                       display_payload(p.data, CASE WHEN r.kind='quotaPoints' THEN r.record_key END) AS data
                FROM processing_display_refs r
                JOIN processing_payloads p ON p.id=r.payload_id;
            CREATE TRIGGER IF NOT EXISTS processing_display_insert INSTEAD OF INSERT ON processing_display BEGIN
                INSERT OR REPLACE INTO processing_display_refs(view,account,kind,record_key,event_at,session_id,payload_id)
                VALUES(NEW.view,NEW.account,NEW.kind,NEW.record_key,NEW.event_at,json_extract(NEW.data,'$.sessionId'),
                       put_payload(NEW.data, CASE WHEN NEW.kind='quotaPoints' THEN NEW.record_key END));
            END;
            CREATE TRIGGER IF NOT EXISTS processing_display_update INSTEAD OF UPDATE ON processing_display BEGIN
                UPDATE processing_display_refs
                SET view=NEW.view,account=NEW.account,kind=NEW.kind,record_key=NEW.record_key,event_at=NEW.event_at,
                    session_id=json_extract(NEW.data,'$.sessionId'),
                    payload_id=put_payload(NEW.data, CASE WHEN NEW.kind='quotaPoints' THEN NEW.record_key END)
                WHERE view=OLD.view AND kind=OLD.kind AND record_key=OLD.record_key;
            END;
            CREATE TRIGGER IF NOT EXISTS processing_display_delete INSTEAD OF DELETE ON processing_display BEGIN
                DELETE FROM processing_display_refs WHERE view=OLD.view AND kind=OLD.kind AND record_key=OLD.record_key;
            END;
            CREATE TABLE IF NOT EXISTS processing_cost_ranges (view TEXT NOT NULL, account TEXT NOT NULL, kind TEXT NOT NULL, record_key TEXT NOT NULL, start REAL NOT NULL, end REAL NOT NULL, PRIMARY KEY(view,kind,record_key));
            CREATE INDEX IF NOT EXISTS processing_cost_range_end ON processing_cost_ranges(view,account,end,start);
            CREATE TABLE IF NOT EXISTS processing_quota_point_refs (
                view TEXT NOT NULL,
                account TEXT NOT NULL,
                event_at REAL NOT NULL,
                record_key TEXT NOT NULL,
                payload_id INTEGER NOT NULL REFERENCES processing_payloads(id),
                PRIMARY KEY(view,account,record_key)
            );
            CREATE INDEX IF NOT EXISTS processing_quota_order ON processing_quota_point_refs(view,account,event_at,record_key);
            CREATE INDEX IF NOT EXISTS processing_quota_payload ON processing_quota_point_refs(payload_id);
            CREATE TRIGGER IF NOT EXISTS processing_quota_point_refs_delete AFTER DELETE ON processing_quota_point_refs BEGIN
                INSERT OR IGNORE INTO processing_payload_releases VALUES(OLD.payload_id);
            END;
            CREATE TRIGGER IF NOT EXISTS processing_quota_point_refs_update AFTER UPDATE ON processing_quota_point_refs BEGIN
                INSERT OR IGNORE INTO processing_payload_releases VALUES(OLD.payload_id);
            END;
            CREATE VIEW IF NOT EXISTS processing_quota_points AS
                SELECT r.view,r.account,r.event_at,r.record_key,p.data
                FROM processing_quota_point_refs r
                JOIN processing_payloads p ON p.id=r.payload_id;
            CREATE TRIGGER IF NOT EXISTS processing_quota_points_insert INSTEAD OF INSERT ON processing_quota_points BEGIN
                INSERT OR REPLACE INTO processing_quota_point_refs(view,account,event_at,record_key,payload_id)
                VALUES(NEW.view,NEW.account,NEW.event_at,NEW.record_key,put_payload(NEW.data, NULL));
            END;
            CREATE TRIGGER IF NOT EXISTS processing_quota_points_update INSTEAD OF UPDATE ON processing_quota_points BEGIN
                UPDATE processing_quota_point_refs
                SET view=NEW.view,account=NEW.account,event_at=NEW.event_at,record_key=NEW.record_key,
                    payload_id=put_payload(NEW.data, NULL)
                WHERE view=OLD.view AND account=OLD.account AND record_key=OLD.record_key;
            END;
            CREATE TRIGGER IF NOT EXISTS processing_quota_points_delete INSTEAD OF DELETE ON processing_quota_points BEGIN
                DELETE FROM processing_quota_point_refs WHERE view=OLD.view AND account=OLD.account AND record_key=OLD.record_key;
            END;
            -- Kept empty for released migration 15, which still clears this legacy table.
            CREATE TABLE IF NOT EXISTS processing_rank_nodes (scope TEXT NOT NULL, value REAL NOT NULL, priority INTEGER NOT NULL, frequency INTEGER NOT NULL, size INTEGER NOT NULL, left_value REAL, right_value REAL, PRIMARY KEY(scope,value));
            CREATE TABLE IF NOT EXISTS processing_health(source TEXT PRIMARY KEY, error TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS processing_blocks(source TEXT NOT NULL,block INTEGER NOT NULL,size INTEGER NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(source,block));
            CREATE TABLE IF NOT EXISTS processing_reparse(source TEXT PRIMARY KEY);
        """)
        db.execute("INSERT OR IGNORE INTO processing_meta VALUES('schemaVersion', ?)", (str(PROCESSING_SCHEMA_VERSION),))
        db.execute("CREATE INDEX IF NOT EXISTS processing_scan_active ON processing_scan_sources(home,modified_ns)")
        db.execute("CREATE TABLE IF NOT EXISTS processing_counts(scope TEXT NOT NULL,account TEXT NOT NULL,kind TEXT NOT NULL,count INTEGER NOT NULL,PRIMARY KEY(scope,account,kind))")
        _install_count_triggers(db)
        if db.execute("SELECT 1 FROM processing_meta WHERE name='countsInitialized'").fetchone() is None:
            _rebuild_counts(db)
            db.execute("INSERT INTO processing_meta VALUES('countsInitialized','true')")
        if db.execute("SELECT value FROM processing_meta WHERE name='schemaVersion'").fetchone()[0] != str(PROCESSING_SCHEMA_VERSION):
            raise ValueError("Unsupported processing index schema")
        from monitor_delivery import initialize_delivery
        initialize_delivery(db)
        from monitor_sampling import initialize
        initialize(db)


class ProcessingIndex:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.lock = threading.RLock()
        self.metrics = {"sourceBytesRead": 0, "canonicalBytesRead": 0, "sourceVerificationBytes": 0, "eventsHashed": 0, "recordsImported": 0, "fullImports": 0, "sourcesChecked": 0}
        initialize_processing_index(self.path)
        # Finish queued migration/recovery sampling before any consumer can read it.
        with self.transaction():
            pass

    @contextmanager
    def transaction(self):
        with self.lock:
            db = samples.connect(self.path, timeout=10)
            db.execute("PRAGMA recursive_triggers=ON")
            try:
                with db:
                    yield db
                    from monitor_sampling import refresh
                    refresh(db, self)
            finally:
                db.close()

    def maintain_disk_space(self):
        """Reclaim a bounded batch after cleanup; leave headroom for routine cache churn."""
        now = time.monotonic()
        if now < getattr(self, "_vacuum_check_at", 0) or not self.lock.acquire(blocking=False):
            return 0
        try:
            self._vacuum_check_at = now + VACUUM_CHECK_SECONDS
            with closing(samples.connect(self.path, timeout=0, isolation_level=None)) as db:
                if db.execute("PRAGMA auto_vacuum").fetchone()[0] != 2:
                    return 0  # Existing NONE databases convert only during startup migration.
                page_size = db.execute("PRAGMA page_size").fetchone()[0]
                pages = db.execute("PRAGMA page_count").fetchone()[0]
                free = db.execute("PRAGMA freelist_count").fetchone()[0]
                if free * page_size <= VACUUM_FREE_BYTES or free / max(pages, 1) <= VACUUM_FREE_RATIO:
                    return 0
                reclaimed = 0
                while reclaimed < VACUUM_BATCH_PAGES and time.monotonic() - now < VACUUM_BATCH_SECONDS:
                    db.execute("PRAGMA incremental_vacuum(1)").fetchall()
                    reclaimed += 1
                # PASSIVE never waits for readers; another maintenance pass can finish checkpointing.
                db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchall()
                return reclaimed
        except sqlite3.OperationalError as exc:
            if getattr(exc, "sqlite_errorcode", 0) & 255 not in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                raise
            return 0
        finally:
            self.lock.release()

    @staticmethod
    def get(db, name, default=None):
        row = db.execute("SELECT value FROM processing_meta WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else default

    @staticmethod
    def put(db, name, value):
        db.execute("INSERT INTO processing_meta VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value", (name, encode(value)))


    def dirty(self, db, account, reason="records", views=("local", "merged")):
        if reason != "records":
            from monitor_sampling import changed
            changed(db, account, views)
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
        extension = False
        if kind == "quota" and row.get("quotaRunVersion") == 1 and previous:
            old = json.loads(previous[1])
            extension = old.get("quotaRunVersion") == 1 and all(
                label in old["windows"] and window | {"lastAt": old["windows"][label]["lastAt"]} == old["windows"][label]
                and window["lastAt"] >= old["windows"][label]["lastAt"] for label, window in row["windows"].items())
        if previous and not extension:
            self.dirty(db, previous[0], "record correction", views=("merged",) if source.startswith("peer:") else ("local", "merged"))
        account = record_account_key(row)
        duplicate = db.execute("SELECT 1 FROM processing_facts WHERE source=? AND kind=? AND record_key=? LIMIT 1", (source, kind, record_key(kind, row))).fetchone()
        samples.put_fact(db, (
            source, position, kind, record_key(kind, row), account, str(session.get("sessionId") or ""), str(row.get("eventId") or ""),
            parse_timestamp(row.get("checkedAt") or row.get("occurredAt") or session.get("updatedAt")), data,
        ))
        if kind == "tokenLedger" and previous is None and not source.startswith("peer:"):
            self._ledger_add(db, source, row)
        self.dirty(db, account, views=("merged",) if source.startswith("peer:") else ("local", "merged"))
        if kind == "quota":
            from monitor_sampling import changed
            changed(db, account, ("merged",) if source.startswith("peer:") else ("local", "merged"), record_key(kind, row))
        if kind == "tokenLedger":
            if previous and (old_key := record_key(kind, json.loads(previous[1]))) != record_key(kind, row):
                self._effective(db, old_key)
            self._effective(db, record_key(kind, row))
        for view in (("merged",) if source.startswith("peer:") else ("local", "merged")):
            if view == "merged" and kind == "tokenLedger":
                continue
            db.execute("INSERT OR REPLACE INTO processing_changes VALUES(?,?,?,?,?,?,?)", (view, account, kind, record_key(kind, row),
                parse_timestamp(row.get("checkedAt") or row.get("occurredAt") or session.get("updatedAt")), str(session.get("sessionId") or ""),
                "extend" if extension else "replace" if previous or duplicate else "append"))
        if not source.startswith("peer:") and not (kind == "quota" and row.get("quotaRunVersion") == 1):
            db.execute("INSERT OR REPLACE INTO processing_pending_records VALUES(?,?,?)", (record_key(kind, row), kind, data))
        self.metrics["recordsImported"] += 1
        return True


    def rows(self, kind, account=None, session=None):
        with self.transaction() as db:
            query, values = "SELECT data FROM processing_facts WHERE source=? AND kind=?", [f"local:{kind}", kind]
            if account is not None:
                query += " AND account=?"
                values.append(account)
            if session is not None:
                query += " AND session=?"
                values.append(session)
            return [json.loads(row[0]) for row in db.execute(query + " ORDER BY position", values)]

    def write_records(self, db, kind, rows, replace=False, backup_path=None):
        """Apply records by identity, independently of any recorder file contents."""
        from monitor_usage_sync import record_key, record_account_key
        from monitor_backup import BackupRecorder
        source = f"local:{kind}"
        positions = {key: (position, data) for key, position, data in db.execute(
            "SELECT record_key,position,data FROM processing_facts WHERE source=? AND kind=?", (source, kind))} if replace else {}
        changed, corrected = False, False
        if replace:
            retained = {record_key(kind, row) for row in rows}
            for key, (position, data) in positions.items():
                if key in retained:
                    continue
                self.dirty(db, record_account_key(json.loads(data)), "record removal")
                db.execute("DELETE FROM processing_facts WHERE source=? AND position=?", (source, position))
                if kind != "quota" or json.loads(data).get("quotaRunVersion") != 1:
                    db.execute("INSERT OR REPLACE INTO processing_pending_records VALUES(?,?,NULL)", (key, kind))
                if kind == "tokenLedger":
                    self._effective(db, key)
                changed = corrected = True
        position = db.execute("SELECT COALESCE(MAX(position),-1)+1 FROM processing_facts WHERE source=?", (source,)).fetchone()[0]
        for row in rows:
            key = record_key(kind, row)
            previous = db.execute("SELECT position,data FROM processing_facts WHERE source=? AND kind=? AND record_key=? LIMIT 1", (source, kind, key)).fetchone()
            if previous and previous[1] == encode(row):
                continue
            self.fact(db, source, previous[0] if previous else position, kind, row)
            if not previous:
                position += 1
            else:
                corrected = True
            if backup_path is not None:
                BackupRecorder.enqueue(db, backup_path, [row])
            changed = True
        if corrected and kind == "tokenLedger":
            db.execute("DELETE FROM processing_ledger_sessions WHERE source=?", (source,))
            from monitor_common import empty_cost_totals
            self.put(db, f"ledgerCost:{source}", [empty_cost_totals(), {}])
            from monitor_token_ledger import token_sessions_from_ledger, token_cost_snapshot
            sessions = token_sessions_from_ledger([json.loads(row[0]) for row in db.execute("SELECT data FROM processing_facts WHERE source=? AND kind='tokenLedger' ORDER BY position", (source,))])
            for session in sessions:
                db.execute("INSERT INTO processing_ledger_sessions VALUES(?,?,?,?)", (source, session["sessionId"], session["accountSlotId"], encode(session)))
            self.put(db, f"ledgerCost:{source}", token_cost_snapshot(sessions))
        if changed:
            self.put(db, "factRevision", self.get(db, "factRevision", 0) + 1)
        return changed

    def revision(self):
        with self.transaction() as db:
            return self.get(db, "factRevision", 0)

    def repair(self):
        """Invalidate derived checkpoints while retaining facts, pending output and v4 ownership."""
        with self.transaction() as db:
            for table in ("display", "projections", "quota_points", "cost_ranges"):
                db.execute(f"DELETE FROM processing_{table}")
            db.execute("UPDATE remote_record_refs SET peer_position=NULL")
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
        if row.get("recordType") != "usage":
            return
        value = row.get("session") or row
        key = (source, str(value.get("sessionId") or ""), str(value.get("accountSlotId") or "unknown"))
        previous = db.execute("SELECT data FROM processing_ledger_sessions WHERE source=? AND session=? AND slot=?", key).fetchone()
        session = json.loads(previous[0]) if previous else _empty_session(row)
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
        from monitor_usage_sync import canonical_ledger_row, record_account_key
        from monitor_token_ledger import token_sessions_from_ledger
        previous = db.execute("SELECT account,session,event_at,data FROM processing_effective WHERE record_key=?", (key,)).fetchone()
        rows = [json.loads(row[0]) for row in db.execute(
            "SELECT data FROM processing_facts WHERE kind='tokenLedger' AND record_key=? ORDER BY source,position", (key,))]
        rows = [canonical_ledger_row(row) for row in rows if row.get("recordType") == "usage" and not any(key in row for key in ("sourceCoverage", "sourceTotals"))]
        winner = rows[0] if rows else None  # ORDER BY source puts local:tokenLedger before peer:*.
        if previous and winner is not None and previous[3] == encode(winner):
            return
        if winner is None:
            db.execute("DELETE FROM processing_effective_refs WHERE record_key=?", (key,))
        else:
            value = winner.get("session") or winner
            sample_id, attrs = samples.insert_sample(db, "tokenLedger", winner)
            db.execute(
                "INSERT OR REPLACE INTO processing_effective_refs VALUES(?,?,?,?,?,?)",
                (
                    key,
                    samples._dimension(db, "account", record_account_key(winner)),
                    samples._dimension(db, "session", str(value.get("sessionId") or "")),
                    parse_timestamp(winner.get("occurredAt") or value.get("updatedAt")),
                    sample_id,
                    attrs,
                ),
            )
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

    def ledger_cost(self):
        from monitor_common import empty_cost_totals
        with self.transaction() as db:
            return tuple(self.get(db, "ledgerCost:local:tokenLedger", [empty_cost_totals(), {}]))
