"""Frozen readers for ordered data migrations only. Never import from runtime paths."""
"""Typed sampled history and transactional ownership. JSON exists only at codec boundaries.

The public SQL views are read compatibility boundaries for processing and sync. Their
JSON columns are computed, never stored. All writes go through this repository.
"""

import hashlib
import json
import math
import sqlite3
from pathlib import Path
from contextlib import closing

from monitor_common import parse_timestamp


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


TOKENS = "inputTokens freshInputTokens cachedInputTokens cacheWriteInputTokens outputTokens totalTokens requests".split()
COSTS = "inputCostUsd cachedInputCostUsd cacheWriteInputCostUsd outputCostUsd totalCostUsd".split()
# Each node has explicit columns plus presence/null/float masks. Unknown values alone
# live in extensions; masks preserve {}, null, absent, booleans and numeric spelling.
FIELDS = {
    "sample_records": [("schemaVersion", "INTEGER"), ("recordType", "TEXT"), ("checkedAt", "TEXT"), ("occurredAt", "TEXT"),
        ("eventId", "TEXT"), ("sessionId", "session"), ("rawModel", "model"), ("billingModel", "model"),
        ("serviceTier", "tier"), ("sourceFile", "source"), ("usageAccountId", "account")],
    "quota_samples": [("continuousFrom", "TEXT"), ("omittedSamples", "INTEGER")],
    "quota_windows": [("usedPercent", "REAL"), ("resetAt", "TEXT"), ("plan", "plan")],
    "token_samples": [(name, "INTEGER") for name in TOKENS] + [(name, "REAL") for name in COSTS],
    "sample_sync": [("version", "INTEGER"), ("originMachineId", "device"), ("accountId", "account"), ("recordId", "TEXT"), ("localOnly", "INTEGER")],
    "baseline_sessions": [("sessionId", "session"), ("startedAt", "TEXT"), ("updatedAt", "TEXT"), ("usageAccountId", "account")],
    "baseline_models": [],
    "baseline_tiers": [(name, "INTEGER") for name in TOKENS],
}
DIMENSIONS = {"account", "device", "source", "session", "model", "plan", "tier"}


def _column(name, kind):
    return name + "_id" if kind in DIMENSIONS else name


def _dimension(db, kind, value):
    db.execute(f"INSERT OR IGNORE INTO sample_{kind}s(value) VALUES(?)", (value,))
    return db.execute(f"SELECT id FROM sample_{kind}s WHERE value=?", (value,)).fetchone()[0]


def _write_node(db, table, owner, value, children=()):
    extra = {key: item for key, item in value.items() if key not in {name for name, _ in FIELDS[table]} and key not in children}
    present = nulls = floats = bools = 0
    values = []
    for i, (name, kind) in enumerate(FIELDS[table]):
        item = value.get(name)
        if name in value:
            present |= 1 << i
            if item is None:
                nulls |= 1 << i
            elif isinstance(item, bool):
                bools |= 1 << i
            elif isinstance(item, float):
                floats |= 1 << i
        # Unexpected types remain compatible without coercion or data loss.
        if item is not None and ((kind in DIMENSIONS or kind == "TEXT") and not isinstance(item, str)
                or kind in {"INTEGER", "REAL"} and (not isinstance(item, (int, float)) or isinstance(item, int) and not -(2**63) <= item < 2**63
                    or isinstance(item, float) and (not math.isfinite(item) or item == 0 and math.copysign(1, item) < 0)
                    or kind == "REAL" and isinstance(item, int) and abs(item) >= 2**53)):
            extra[name] = item
            item = None
        values.append(_dimension(db, kind, item) if kind in DIMENSIONS and item is not None else item)
    columns = list(owner) + ["present", "nulls", "floats", "bools", "extensions"] + [_column(name, kind) for name, kind in FIELDS[table]]
    db.execute(f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
        (*owner.values(), present, nulls, floats, bools, canonical(extra) if extra else None, *values))


def _read_node(db, table, where, values):
    names = ["present", "nulls", "floats", "bools", "extensions"] + [_column(name, kind) for name, kind in FIELDS[table]]
    row = db.execute(f"SELECT {','.join(names)} FROM {table} WHERE {where}", values).fetchone()
    if row is None:
        return None
    result = json.loads(row[4]) if row[4] else {}
    for i, ((name, kind), item) in enumerate(zip(FIELDS[table], row[5:])):
        if not row[0] & (1 << i) or name in result:
            continue
        if row[1] & (1 << i):
            item = None
        elif kind in DIMENSIONS:
            item = db.execute(f"SELECT value FROM sample_{kind}s WHERE id=?", (item,)).fetchone()[0]
        elif row[3] & (1 << i):
            item = bool(item)
        elif kind in {"REAL", "INTEGER"}:
            item = float(item) if row[2] & (1 << i) else int(item)
        result[name] = item
    return result


def _child(db, sample, parent, key, table, value, **owner):
    if key not in value:
        return
    if isinstance(value[key], dict):
        parent[key] = {}  # Container marker only; fields live in the child table.
        _write_node(db, table, {"sample_id": sample, **owner}, value[key])
    else:
        parent[key] = value[key]


def _attribution(row):
    if not isinstance(row, dict):
        return {}
    result = {key: row[key] for key in ("accountSlotId", "accountLabel") if key in row}
    if isinstance(row.get("session"), dict):
        session = {key: row["session"][key] for key in ("accountSlotId", "accountLabel") if key in row["session"]}
        if session:
            result["session"] = session
    return result


def _payload(row):
    if not isinstance(row, dict):
        return row
    result = {key: value for key, value in row.items() if key not in {"accountSlotId", "accountLabel"}}
    if isinstance(result.get("session"), dict):
        result["session"] = {key: value for key, value in result["session"].items() if key not in {"accountSlotId", "accountLabel"}}
    return result


def insert_sample(db, kind, row):
    payload = _payload(row)
    digest = hashlib.sha256(canonical([kind, payload]).encode()).digest()
    found = db.execute("SELECT id FROM sample_records WHERE content_hash=?", (digest,)).fetchone()
    if found:
        return found[0], canonical(_attribution(row))
    subtype = row.get("recordType") if isinstance(row, dict) else None
    known = isinstance(row, dict) and (kind == "quota" or kind == "tokenLedger" and subtype in {"usage", "legacyBaseline"})
    children = {"sync"}
    children |= {"windows", "compaction"} if kind == "quota" else {"tokens", "cost"} if subtype == "usage" else {"session", "sourceTotals"}
    event = row.get("checkedAt") or row.get("occurredAt") or ((row.get("session") or {}).get("updatedAt") if isinstance(row.get("session"), dict) else None) if isinstance(row, dict) else None
    sync = row.get("sync") if isinstance(row, dict) and isinstance(row.get("sync"), dict) else {}
    session = row.get("session") if isinstance(row, dict) and isinstance(row.get("session"), dict) else {}
    account = sync.get("accountId") or (row.get("usageAccountId") if isinstance(row, dict) else None) or session.get("usageAccountId")
    identity_session = (row.get("sessionId") if isinstance(row, dict) else None) or session.get("sessionId")
    _write_node(db, "sample_records", {"kind": kind, "subtype": subtype if isinstance(subtype, str) else None,
        "content_hash": digest, "event_at": parse_timestamp(event),
        "account_id": _dimension(db, "account", account) if isinstance(account, str) else None,
        "origin_device_id": _dimension(db, "device", sync["originMachineId"]) if isinstance(sync.get("originMachineId"), str) else None,
        "identity_session_id": _dimension(db, "session", identity_session) if isinstance(identity_session, str) else None,
        "opaque": canonical(payload) if not known else None}, payload if known else {}, children)
    sample = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    if not known:
        return sample, canonical(_attribution(row))
    containers = {}
    _child(db, sample, containers, "sync", "sample_sync", payload)
    if kind == "quota":
        if not isinstance(payload.get("compaction"), dict):
            _write_node(db, "quota_samples", {"sample_id": sample}, {})
        _child(db, sample, containers, "compaction", "quota_samples", payload)
        if isinstance(payload.get("windows"), dict):
            containers["windows"] = {key: value for key, value in payload["windows"].items() if key not in {"5h", "7d"}}
            for key in ("5h", "7d"):
                _child(db, sample, containers["windows"], key, "quota_windows", payload["windows"], window=key)
        elif "windows" in payload:
            containers["windows"] = payload["windows"]
    elif subtype == "usage":
        # Tokens and costs share one row, with independently preserved containers.
        for key in ("tokens", "cost"):
            if isinstance(payload.get(key), dict):
                containers[key] = {}
            elif key in payload:
                containers[key] = payload[key]
        totals = {name: payload[key][name] for key, fields in (("tokens", TOKENS), ("cost", COSTS))
            if isinstance(payload.get(key), dict) for name in fields if name in payload[key]}
        _write_node(db, "token_samples", {"sample_id": sample}, totals)
        # Unknown members need their original container, even when names collide.
        for key, fields in (("tokens", TOKENS), ("cost", COSTS)):
            if isinstance(payload.get(key), dict):
                containers[key] = {name: value for name, value in payload[key].items() if name not in fields}
    else:
        session = payload.get("session")
        if isinstance(session, dict):
            containers["session"] = {}
            _write_node(db, "baseline_sessions", {"sample_id": sample}, session, ("tokens", "cost", "byModel", "fastTokens"))
            for key in ("tokens", "cost", "fastTokens"):
                if key in session:
                    containers["session"][key] = {} if isinstance(session[key], dict) else session[key]
            _write_totals(db, sample, 0, session)
            if isinstance(session.get("byModel"), dict):
                containers["session"]["byModel"] = {}
                for model, value in session["byModel"].items():
                    if not isinstance(value, dict):
                        containers["session"]["byModel"][model] = value
                        continue
                    model_id = _dimension(db, "model", model)
                    _write_node(db, "baseline_models", {"sample_id": sample, "model_id": model_id}, value, tuple(key for key in ("tokens", "cost", "fastTokens") if isinstance(value.get(key), dict)))
                    _write_totals(db, sample, model_id, value)
            elif "byModel" in session:
                containers["session"]["byModel"] = session["byModel"]
        elif "session" in payload:
            containers["session"] = session
        if isinstance(payload.get("sourceTotals"), dict):
            containers["sourceTotals"] = {}
            for model, tiers in payload["sourceTotals"].items():
                if not isinstance(tiers, dict):
                    containers["sourceTotals"][model] = tiers
                    continue
                containers["sourceTotals"][model] = {}
                for tier, totals in tiers.items():
                    if isinstance(totals, dict):
                        _write_node(db, "baseline_tiers", {"sample_id": sample, "model_id": _dimension(db, "model", model), "tier_id": _dimension(db, "tier", "source:" + tier)}, totals)
                    else:
                        containers["sourceTotals"][model][tier] = totals
        elif "sourceTotals" in payload:
            containers["sourceTotals"] = payload["sourceTotals"]
    db.execute("UPDATE sample_records SET containers=? WHERE id=?", (canonical(containers), sample))
    return sample, canonical(_attribution(row))


def _write_totals(db, sample, model, value):
    for key in ("tokens", "cost", "fastTokens"):
        if isinstance(value.get(key), dict):
            _write_node(db, "baseline_totals", {"sample_id": sample, "model_id": model, "category": key}, value[key])


FIELDS["baseline_totals"] = FIELDS["token_samples"]


def select_sample(db, sample, attribution="{}"):
    kind, subtype, opaque, containers = db.execute("SELECT kind,subtype,opaque,containers FROM sample_records WHERE id=?", (sample,)).fetchone()
    if opaque is not None:
        row = json.loads(opaque)
    else:
        row = _read_node(db, "sample_records", "id=?", (sample,))
        containers = json.loads(containers)
        row.update(containers)
        for key, table in (("sync", "sample_sync"), ("compaction", "quota_samples")):
            value = _read_node(db, table, "sample_id=?", (sample,))
            if value is not None and key in containers and isinstance(containers[key], dict):
                row[key] = value
        if kind == "quota" and isinstance(row.get("windows"), dict):
            for key in ("5h", "7d"):
                value = _read_node(db, "quota_windows", "sample_id=? AND window=?", (sample, key))
                if value is not None:
                    row["windows"][key] = value
        elif subtype == "usage":
            totals = _read_node(db, "token_samples", "sample_id=?", (sample,)) or {}
            for key, fields in (("tokens", TOKENS), ("cost", COSTS)):
                if isinstance(row.get(key), dict):
                    row[key].update({name: value for name, value in totals.items() if name in fields})
        elif subtype == "legacyBaseline":
            session = _read_node(db, "baseline_sessions", "sample_id=?", (sample,))
            if session is not None:
                saved = row["session"]
                row["session"] = session
                session.update(saved)
                _read_totals(db, sample, 0, session)
                if isinstance(session.get("byModel"), dict):
                    for model_id, model in db.execute("SELECT b.model_id,m.value FROM baseline_models b JOIN sample_models m ON m.id=b.model_id WHERE sample_id=?", (sample,)):
                        value = _read_node(db, "baseline_models", "sample_id=? AND model_id=?", (sample, model_id))
                        _read_totals(db, sample, model_id, value)
                        session["byModel"][model] = value
            if isinstance(row.get("sourceTotals"), dict):
                for model, tier, model_id, tier_id in db.execute(('SELECT m.value,t.value,b.model_id,b.tier_id FROM baseline_tiers b JOIN sample_models m ON m.id=b.model_id JOIN sample_tiers t ON t.id=b.tier_id WHERE '
                    "sample_id=? AND t.value LIKE 'source:%'"), (sample,)):
                    row["sourceTotals"].setdefault(model, {})[tier[7:]] = _read_node(db, "baseline_tiers", "sample_id=? AND model_id=? AND tier_id=?", (sample, model_id, tier_id))
    attrs = json.loads(attribution)
    if "session" in attrs:
        row["session"].update(attrs.pop("session"))
    if attrs:
        row.update(attrs)
    return row


def _read_totals(db, sample, model, value):
    for category, in db.execute("SELECT category FROM baseline_totals WHERE sample_id=? AND model_id=?", (sample, model)):
        value[category] = _read_node(db, "baseline_totals", "sample_id=? AND model_id=? AND category=?", (sample, model, category))


def collect(db):
    # Candidate-only cleanup avoids a full-history scan on idle commits.
    db.execute("DELETE FROM sample_records WHERE id IN (SELECT sample_id FROM sample_releases) "
        "AND NOT EXISTS(SELECT 1 FROM processing_fact_refs WHERE sample_id=sample_records.id) "
        "AND NOT EXISTS(SELECT 1 FROM local_record_refs WHERE sample_id=sample_records.id) "
        "AND NOT EXISTS(SELECT 1 FROM remote_record_refs WHERE sample_id=sample_records.id)")
    db.execute("DELETE FROM sample_releases")


class Connection(sqlite3.Connection):
    def __exit__(self, typ, value, traceback):
        if typ is None and self.in_transaction and self.execute("SELECT 1 FROM sqlite_master WHERE name='sample_releases'").fetchone():
            try:
                collect(self)
            except Exception:
                self.rollback()
                raise
        return super().__exit__(typ, value, traceback)

    def commit(self):
        if self.in_transaction and self.execute("SELECT 1 FROM sqlite_master WHERE name='sample_releases'").fetchone():
            collect(self)
        super().commit()


def connect(*args, **kwargs):
    db = sqlite3.connect(*args, **kwargs, factory=Connection)
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA recursive_triggers=ON")
    db.create_function("sample_row", 2, lambda sample, attrs: canonical(select_sample(db, sample, attrs)))
    db.create_function("sample_record", 3, lambda sample, attrs, extra: canonical(_record(db, sample, attrs, extra)))
    db.create_function("sample_envelope", 10, lambda sample, attrs, extra, envelope, version, period, event, machine, key, digest:
        canonical(_envelope(db, sample, attrs, extra, envelope, version, period, event, machine, key, digest)))
    return db


def _record(db, sample, attrs, extra):
    row = select_sample(db, sample, attrs)
    return {"kind": db.execute("SELECT kind FROM sample_records WHERE id=?", (sample,)).fetchone()[0], **json.loads(extra), "row": row} if extra is not None else row


def _envelope(db, sample, attrs, extra, envelope, version, period, event, machine, key, digest):
    meta = json.loads(envelope)
    presence, result = meta[0], meta[1]
    kind = db.execute("SELECT kind FROM sample_records WHERE id=?", (sample,)).fetchone()[0]
    for i, (key, value) in enumerate((("schemaVersion", version), ("collectionPeriod", period), ("eventAt", event),
            ("kind", kind), ("sourceMachineId", machine), ("recordKey", key), ("contentSha256", digest))):
        if presence & (1 << i):
            result[key] = value
    result["record"] = _record(db, sample, attrs, extra)
    return result


def put_fact(db, values):
    sample, attrs = insert_sample(db, values[2], json.loads(values[8]))
    db.execute("INSERT OR REPLACE INTO processing_fact_refs VALUES(?,?,?,?,?,?,?,?,?,?,?)", (_dimension(db, "source", values[0]), *values[1:4],
        _dimension(db, "account", values[4]), _dimension(db, "session", values[5]), *values[6:8], sample, attrs, json.loads(values[8]).get("recordType")))


def put_local(db, key, period, digest, record):
    sample, attrs = insert_sample(db, record["kind"], record["row"])
    db.execute("INSERT INTO local_record_refs VALUES(?,?,?,?,?,?) ON CONFLICT(record_key) DO UPDATE SET content_hash=excluded.content_hash,sample_id=excluded.sample_id,attribution=excluded.attribution,record_meta=excluded.record_meta",
        (key, period, digest, sample, attrs, canonical({key: value for key, value in record.items() if key not in {"kind", "row"}})))


def put_remote(db, machine, day, part, key, digest, envelope):
    record = envelope["record"]
    sample, attrs = insert_sample(db, record["kind"], record["row"] if "row" in record else record)
    db.execute("INSERT OR REPLACE INTO remote_record_refs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (_dimension(db, "device", machine), day, part, key, digest, sample, attrs,
        canonical({key: value for key, value in record.items() if key not in {"row", "kind"}}) if "row" in record else None, envelope.get("schemaVersion"), envelope.get("collectionPeriod"), envelope.get("eventAt"),
        canonical([sum(1 << i for i, key in enumerate(("schemaVersion", "collectionPeriod", "eventAt", "kind", "sourceMachineId", "recordKey", "contentSha256")) if key in envelope),
            {key: value for key, value in envelope.items() if key not in {"record", "schemaVersion", "collectionPeriod", "eventAt", "kind", "sourceMachineId", "recordKey", "contentSha256"}}])))


def initialize(db):
    for kind in sorted(DIMENSIONS):
        db.execute(f"CREATE TABLE IF NOT EXISTS sample_{kind}s(id INTEGER PRIMARY KEY,value TEXT NOT NULL UNIQUE)")
    owners = {"sample_records": ('id INTEGER PRIMARY KEY,kind TEXT NOT NULL,subtype TEXT,content_hash BLOB NOT NULL UNIQUE,event_at REAL,account_id INTEGER REFERENCES sample_accounts(id),'
        'origin_device_id INTEGER REFERENCES sample_devices(id),identity_session_id INTEGER REFERENCES sample_sessions(id),opaque TEXT,containers TEXT'),
        "quota_samples": "sample_id INTEGER PRIMARY KEY REFERENCES sample_records(id) ON DELETE CASCADE",
        "quota_windows": "sample_id INTEGER REFERENCES sample_records(id) ON DELETE CASCADE,window TEXT",
        "token_samples": "sample_id INTEGER PRIMARY KEY REFERENCES sample_records(id) ON DELETE CASCADE",
        "sample_sync": "sample_id INTEGER PRIMARY KEY REFERENCES sample_records(id) ON DELETE CASCADE",
        "baseline_sessions": "sample_id INTEGER PRIMARY KEY REFERENCES sample_records(id) ON DELETE CASCADE",
        "baseline_models": "sample_id INTEGER REFERENCES sample_records(id) ON DELETE CASCADE,model_id INTEGER REFERENCES sample_models(id)",
        "baseline_tiers": "sample_id INTEGER REFERENCES sample_records(id) ON DELETE CASCADE,model_id INTEGER,tier_id INTEGER REFERENCES sample_tiers(id)",
        "baseline_totals": "sample_id INTEGER REFERENCES sample_records(id) ON DELETE CASCADE,model_id INTEGER,category TEXT"}
    for table, fields in FIELDS.items():
        columns = [owners[table], "present INTEGER NOT NULL", "nulls INTEGER NOT NULL", "floats INTEGER NOT NULL", "bools INTEGER NOT NULL", "extensions TEXT"]
        columns += [f'{_column(name, kind)} INTEGER REFERENCES sample_{kind}s(id)' if kind in DIMENSIONS else f'{name} {kind}' for name, kind in fields]
        if table in {"quota_windows", "baseline_models", "baseline_tiers", "baseline_totals"}:
            columns += [{"quota_windows": "PRIMARY KEY(sample_id,window)", "baseline_models": "PRIMARY KEY(sample_id,model_id)",
                "baseline_tiers": "PRIMARY KEY(sample_id,model_id,tier_id)", "baseline_totals": "PRIMARY KEY(sample_id,model_id,category)"}[table]]
        db.execute(f"CREATE TABLE IF NOT EXISTS {table}({','.join(columns)})")
    db.execute("CREATE INDEX IF NOT EXISTS sample_account_time ON sample_sync(accountId_id,sample_id)")
    db.execute("CREATE INDEX IF NOT EXISTS sample_account_event_time ON sample_records(account_id,event_at)")
    db.execute("CREATE INDEX IF NOT EXISTS sample_event_time ON sample_records(event_at,id)")
    db.execute("CREATE INDEX IF NOT EXISTS sample_session ON sample_records(identity_session_id,event_at)")
    db.execute("CREATE INDEX IF NOT EXISTS sample_event ON sample_records(eventId)")
    db.execute("CREATE TABLE IF NOT EXISTS sample_releases(sample_id INTEGER PRIMARY KEY)")
    db.execute(('CREATE TABLE IF NOT EXISTS processing_fact_refs(source_id INTEGER NOT NULL REFERENCES sample_sources(id),position INTEGER NOT NULL,kind TEXT NOT NULL,'
        'record_key TEXT NOT NULL,account_id INTEGER NOT NULL REFERENCES sample_accounts(id),session_id INTEGER NOT NULL REFERENCES sample_sessions(id),event_id TEXT '
        'NOT NULL,event_at REAL,sample_id INTEGER NOT NULL REFERENCES sample_records(id),attribution TEXT NOT NULL,subtype TEXT,PRIMARY KEY(source_id,position))'))
    db.execute("CREATE TABLE IF NOT EXISTS local_record_refs(record_key TEXT PRIMARY KEY,period TEXT NOT NULL,content_hash TEXT NOT NULL,sample_id INTEGER NOT NULL REFERENCES sample_records(id),attribution TEXT NOT NULL,record_meta TEXT NOT NULL)")
    db.execute(('CREATE TABLE IF NOT EXISTS remote_record_refs(device_id INTEGER NOT NULL REFERENCES sample_devices(id),day TEXT NOT NULL,part_id TEXT NOT NULL,record_key TEXT '
        'NOT NULL,content_hash TEXT NOT NULL,sample_id INTEGER NOT NULL REFERENCES sample_records(id),attribution TEXT NOT NULL,record_meta TEXT,schema_version INTEGER,'
        'collection_period TEXT,event_at TEXT,envelope_meta TEXT NOT NULL,PRIMARY KEY(device_id,record_key))'))
    for table in ("processing_fact_refs", "local_record_refs", "remote_record_refs"):
        db.execute(f"CREATE INDEX IF NOT EXISTS {table}_sample ON {table}(sample_id)")
        for action in ("DELETE", "UPDATE"):
            db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_{action.lower()} AFTER {action} ON {table} BEGIN INSERT OR IGNORE INTO sample_releases VALUES(OLD.sample_id); END")
    for name, fields in {"processing_facts_key": "kind,record_key", "processing_facts_account": "kind,account,event_at,position", "processing_facts_session": "kind,session,event_at",
        "processing_facts_event": "kind,event_id", "processing_fact_source_event": "source,event_id", "processing_fact_source_time": "source,kind,event_at",
        "processing_fact_source_key": "source,kind,record_key", "processing_fact_account_key": "account,kind,record_key,source"}.items():
        fields = ','.join(field + '_id' if field in {'source', 'account', 'session'} else field for field in fields.split(','))
        db.execute(f"CREATE INDEX IF NOT EXISTS {name} ON processing_fact_refs({fields})")
    db.execute("CREATE INDEX IF NOT EXISTS processing_baselines ON processing_fact_refs(source_id,session_id) WHERE subtype='legacyBaseline'")
    db.execute("CREATE INDEX IF NOT EXISTS processing_fact_source_session ON processing_fact_refs(source_id,session_id,kind,event_at)")
    db.execute("CREATE INDEX IF NOT EXISTS local_records_period ON local_record_refs(period)")
    db.execute("CREATE INDEX IF NOT EXISTS remote_records_day ON remote_record_refs(device_id,day,part_id)")


def create_views(db):
    db.execute(('CREATE VIEW IF NOT EXISTS processing_facts AS SELECT s.value AS source,f.position,f.kind,f.record_key,a.value AS account,x.value AS session,f.event_id,'
        'f.event_at,sample_row(f.sample_id,f.attribution) AS data,f.subtype FROM processing_fact_refs f JOIN sample_sources s ON s.id=f.source_id JOIN sample_accounts a '
        'ON a.id=f.account_id JOIN sample_sessions x ON x.id=f.session_id'))
    db.execute("CREATE VIEW IF NOT EXISTS local_records AS SELECT record_key,period,content_hash,sample_record(sample_id,attribution,record_meta) AS record_json FROM local_record_refs")
    db.execute(('CREATE VIEW IF NOT EXISTS remote_records AS SELECT d.value AS machine_id,r.day,r.part_id,r.record_key,r.content_hash,sample_envelope(sample_id,attribution,'
        'record_meta,envelope_meta,schema_version,collection_period,event_at,d.value,r.record_key,r.content_hash) AS record_json FROM remote_record_refs r JOIN '
        'sample_devices d ON d.id=r.device_id'))
    for view, table, condition in (("processing_facts", "processing_fact_refs", "source_id=(SELECT id FROM sample_sources WHERE value=OLD.source) AND position=OLD.position"),
        ("local_records", "local_record_refs", "record_key=OLD.record_key"), ("remote_records", "remote_record_refs", "device_id=(SELECT id FROM sample_devices WHERE value=OLD.machine_id) AND record_key=OLD.record_key")):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS {view}_delete INSTEAD OF DELETE ON {view} BEGIN DELETE FROM {table} WHERE {condition}; END")


def prepare(db):
    """Initialize fresh stores. Existing legacy stores are converted by migration 14."""
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name IN ('processing_facts','local_records','remote_records')").fetchone():
        db.execute(('CREATE TABLE IF NOT EXISTS processing_facts(source TEXT NOT NULL,position INTEGER NOT NULL,kind TEXT NOT NULL,record_key TEXT NOT NULL,account TEXT NOT NULL,'
            'session TEXT NOT NULL,event_id TEXT NOT NULL,event_at REAL,data TEXT NOT NULL,PRIMARY KEY(source,position))'))
        return False
    initialize(db)
    create_views(db)
    return True


PROCESSING_SCHEMA_VERSION = 1


def initialize_processing_index(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(connect(path)) as db, db:
        db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        db.execute("PRAGMA journal_mode=WAL")
        normalized = prepare(db)
        db.executescript("""
            CREATE TABLE IF NOT EXISTS processing_meta (name TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS processing_files (path TEXT PRIMARY KEY, signature TEXT NOT NULL, offset INTEGER NOT NULL);
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
            physical = "fact_refs" if table == "facts" and normalized else table
            if table == "facts" and normalized:
                scope = "(SELECT value FROM sample_sources WHERE id=NEW.source_id)"
                account = "(SELECT value FROM sample_accounts WHERE id=NEW.account_id)"
            db.executescript(f"""
                CREATE TRIGGER IF NOT EXISTS processing_count_{table}_insert AFTER INSERT ON processing_{physical} BEGIN
                    INSERT INTO processing_counts VALUES({scope},{account},{kind},1) ON CONFLICT(scope,account,kind) DO UPDATE SET count=count+1;
                END;
                CREATE TRIGGER IF NOT EXISTS processing_count_{table}_delete AFTER DELETE ON processing_{physical} BEGIN
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
        _initialize_legacy_delivery(db)


def initialize_v4_cache(path: Path) -> None:
    """Create the local v4 index without altering the original history or legacy cache."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(connect(path)) as db, db:
        db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        normalized = prepare(db)
        if not normalized:
            db.execute("CREATE TABLE IF NOT EXISTS local_records (record_key TEXT PRIMARY KEY, period TEXT NOT NULL, content_hash TEXT NOT NULL, record_json TEXT NOT NULL)")
            db.execute("CREATE INDEX IF NOT EXISTS local_records_period ON local_records(period)")
        db.execute("CREATE TABLE IF NOT EXISTS remote_days (machine_id TEXT NOT NULL, day TEXT NOT NULL, logical_hash TEXT NOT NULL, layout_json TEXT NOT NULL, PRIMARY KEY(machine_id, day))")
        if not normalized:
            db.execute("CREATE TABLE IF NOT EXISTS remote_records (machine_id TEXT NOT NULL, day TEXT NOT NULL, part_id TEXT NOT NULL, record_key TEXT NOT NULL, content_hash TEXT NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY(machine_id, record_key))")
            db.execute("CREATE INDEX IF NOT EXISTS remote_records_day ON remote_records(machine_id, day, part_id)")
        db.execute("CREATE TABLE IF NOT EXISTS remote_origins (machine_id TEXT PRIMARY KEY)")
        db.execute("CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL)")


def _initialize_legacy_delivery(db):
    """Retain old delivery schemas for released sequential migration steps."""
    db.executescript("""
        CREATE TABLE IF NOT EXISTS dashboard_days(view TEXT,day INTEGER,revision INTEGER NOT NULL,dirty INTEGER NOT NULL,PRIMARY KEY(view,day));
        CREATE TABLE IF NOT EXISTS dashboard_parts(view TEXT,day INTEGER,level INTEGER,digest TEXT NOT NULL,PRIMARY KEY(view,day,level));
        CREATE TABLE IF NOT EXISTS dashboard_day_meta(view TEXT,day INTEGER,data TEXT NOT NULL,PRIMARY KEY(view,day));
        CREATE TABLE IF NOT EXISTS dashboard_blobs(digest TEXT PRIMARY KEY,body BLOB NOT NULL,compressed BLOB NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dashboard_manifests(scope TEXT PRIMARY KEY,signature TEXT NOT NULL,data TEXT NOT NULL,updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dashboard_rollups(signature TEXT PRIMARY KEY,digest TEXT NOT NULL,created REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS dashboard_quota_time ON processing_quota_points(view,event_at,account);
        CREATE INDEX IF NOT EXISTS dashboard_display_time ON processing_display(view,kind,event_at,account);
    """)
