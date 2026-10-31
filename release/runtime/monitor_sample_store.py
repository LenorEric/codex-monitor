"""Typed sampled history and transactional ownership. JSON exists only at codec boundaries.

The public SQL views are read interfaces for processing and sync. Their
JSON columns are computed, never stored. All writes go through this repository.
"""

import hashlib
import json
import math
import sqlite3
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
    "five_hour": [("usedPercent", "REAL"), ("percentCheckedAt", "TEXT")],
    "seven_day": [("usedPercent", "REAL"), ("percentCheckedAt", "TEXT")],
    "quota_windows": [("resetAt", "REAL"), ("plan", "plan")],
    "token_samples": [(name, "INTEGER") for name in TOKENS] + [(name, "REAL") for name in COSTS],
    "sample_sync": [("version", "INTEGER"), ("originMachineId", "device"), ("accountId", "account"), ("recordId", "TEXT"), ("localOnly", "INTEGER")],
    "baseline_sessions": [("sessionId", "session"), ("startedAt", "TEXT"), ("updatedAt", "TEXT"), ("usageAccountId", "account")],
    "baseline_models": [],
    "baseline_tiers": [(name, "INTEGER") for name in TOKENS],
}
INLINE = {
    "sample_sync": "sync_",
    "token_samples": "token_",
    "baseline_sessions": "baseline_",
    "five_hour": "five_hour_",
    "seven_day": "seven_day_",
}
LEGACY_WINDOW_FIELDS = [("usedPercent", "REAL"), ("resetAt", "TEXT"), ("plan", "plan")]
MASKS = [("present", "INTEGER"), ("nulls", "INTEGER"), ("floats", "INTEGER"), ("bools", "INTEGER"), ("extensions", "TEXT")]
DIMENSIONS = {"account", "device", "source", "session", "model", "plan", "tier"}


def _column(name, kind):
    return name + "_id" if kind in DIMENSIONS else name


def _definition(name, kind, prefix=""):
    return prefix + _column(name, kind) + " " + (f"INTEGER REFERENCES sample_{kind}s(id)" if kind in DIMENSIONS else kind)


def _dimension(db, kind, value):
    db.execute(f"INSERT OR IGNORE INTO sample_{kind}s(value) VALUES(?)", (value,))
    return db.execute(f"SELECT id FROM sample_{kind}s WHERE value=?", (value,)).fetchone()[0]


def _encode_attrs(attrs):
    return canonical(attrs) if attrs else None


def _decode_attrs(attrs):
    return json.loads(attrs) if attrs else {}


def _write_node(db, table, owner, value, children=(), fields=None):
    spec = FIELDS[table] if fields is None else fields
    extra = {key: item for key, item in value.items() if key not in {name for name, _ in spec} and key not in children}
    present = nulls = floats = bools = 0
    values = []
    for i, (name, kind) in enumerate(spec):
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
    columns = list(owner) + ["present", "nulls", "floats", "bools", "extensions"] + [_column(name, kind) for name, kind in spec]
    if table in INLINE:
        columns = [INLINE[table] + name for name in columns[len(owner):]]
        db.execute(f"UPDATE sample_records SET {','.join(name + '=?' for name in columns)} WHERE id=?",
            (present, nulls, floats, bools, canonical(extra) if extra else None, *values, owner["sample_id"]))
    else:
        db.execute(f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
            (*owner.values(), present, nulls, floats, bools, canonical(extra) if extra else None, *values))


def _read_node(db, table, where, values, storage=None, fields=None):
    spec = FIELDS[table] if fields is None else fields
    names = ["present", "nulls", "floats", "bools", "extensions"] + [_column(name, kind) for name, kind in spec]
    if table in INLINE and storage is None:
        row = db.execute(f"SELECT {','.join(INLINE[table] + name for name in names)} FROM sample_records WHERE id=?", values).fetchone()
    else:
        row = db.execute(f"SELECT {','.join(names)} FROM {storage or table} WHERE {where}", values).fetchone()
    if row is None or row[0] is None:
        return None
    result = json.loads(row[4]) if row[4] else {}
    for i, ((name, kind), item) in enumerate(zip(spec, row[5:])):
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


def _insert_window(db, window, value, payload):
    sync = payload.get("sync") if isinstance(payload.get("sync"), dict) else {}
    account = sync.get("accountId") or payload.get("usageAccountId")
    origin = sync.get("originMachineId")
    period = {key: value[key] for key in ("resetAt", "plan") if key in value}
    digest = hashlib.sha256(canonical([account, origin, window, period]).encode()).digest()
    found = db.execute("SELECT id FROM quota_windows WHERE content_hash=?", (digest,)).fetchone()
    if found:
        return found[0]
    _write_node(db, "quota_windows", {
        "content_hash": digest,
        "window": window,
        "account_id": _dimension(db, "account", account) if isinstance(account, str) else None,
        "origin_device_id": _dimension(db, "device", origin) if isinstance(origin, str) else None,
    }, period)
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


def put_payload(db, data, strip_key=None):
    if isinstance(data, str):
        try:
            parsed = json.loads(data)
            if strip_key is not None and isinstance(parsed, dict) and parsed.get("_key") == strip_key:
                parsed.pop("_key", None)
            data = canonical(parsed)
        except Exception:
            pass
    else:
        data = canonical(data)
    digest = hashlib.sha256(data.encode("utf-8")).digest()
    found = db.execute("SELECT id FROM processing_payloads WHERE content_hash=?", (digest,)).fetchone()
    if found:
        return found[0]
    db.execute("INSERT INTO processing_payloads(content_hash,data) VALUES(?,?)", (digest, data))
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


def display_payload(data, key=None):
    if key is None or data is None:
        return data
    parsed = json.loads(data)
    if isinstance(parsed, dict) and "_key" not in parsed:
        parsed["_key"] = key
        return canonical(parsed)
    return data


def _virtualize_sample_metadata(db):
    stored = {row[1] for row in db.execute("PRAGMA table_xinfo(sample_records)") if row[6] == 0}
    target = {"subtype", "account_id", "origin_device_id", "identity_session_id"}
    empty_session = _dimension(db, "session", "")
    if target & stored:
        db.execute(
            "UPDATE sample_records SET recordType=COALESCE(recordType,subtype),"
            "usageAccountId_id=COALESCE(usageAccountId_id,account_id),"
            "sync_originMachineId_id=COALESCE(sync_originMachineId_id,origin_device_id),"
            "sessionId_id=COALESCE(sessionId_id,baseline_sessionId_id,identity_session_id,?),"
            "eventId=COALESCE(eventId,'')",
            (empty_session,),
        )
        for index in ("sample_account_event_time", "sample_session", "sample_event_time", "sample_event", "sample_account_time"):
            db.execute(f"DROP INDEX IF EXISTS {index}")
        for column in ("subtype", "account_id", "origin_device_id", "identity_session_id"):
            if column in stored:
                db.execute(f"ALTER TABLE sample_records DROP COLUMN {column}")
        for definition in (
            "subtype TEXT GENERATED ALWAYS AS (recordType) VIRTUAL",
            "account_id INTEGER GENERATED ALWAYS AS (COALESCE(sync_accountId_id,usageAccountId_id,baseline_usageAccountId_id)) VIRTUAL",
            "origin_device_id INTEGER GENERATED ALWAYS AS (sync_originMachineId_id) VIRTUAL",
            "identity_session_id INTEGER GENERATED ALWAYS AS (COALESCE(sessionId_id,baseline_sessionId_id)) VIRTUAL",
        ):
            db.execute(f"ALTER TABLE sample_records ADD COLUMN {definition}")
    else:
        db.execute(
            "UPDATE sample_records SET sessionId_id=COALESCE(sessionId_id,baseline_sessionId_id,?),"
            "eventId=COALESCE(eventId,'') WHERE COALESCE(sessionId_id,baseline_sessionId_id) IS NULL OR eventId IS NULL",
            (empty_session,),
        )


def _migrate_ref_tables(db):
    if db.execute("SELECT 1 FROM pragma_table_info('processing_fact_refs') WHERE name='kind'").fetchone():
        for name, in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='processing_fact_refs'").fetchall():
            db.execute(f"DROP TRIGGER IF EXISTS {name}")
        db.execute(
            "CREATE TABLE processing_fact_refs_v20("
            "source_id INTEGER NOT NULL REFERENCES sample_sources(id),"
            "position INTEGER NOT NULL,"
            "record_key TEXT NOT NULL,"
            "account_id INTEGER NOT NULL REFERENCES sample_accounts(id),"
            "sample_id INTEGER NOT NULL REFERENCES sample_records(id),"
            "attribution TEXT,"
            "session_id INTEGER GENERATED ALWAYS AS (NULL) VIRTUAL,"
            "subtype TEXT GENERATED ALWAYS AS (NULL) VIRTUAL,"
            "PRIMARY KEY(source_id,position))"
        )
        db.execute(
            "INSERT INTO processing_fact_refs_v20(source_id,position,record_key,account_id,sample_id,attribution) "
            "SELECT source_id,position,record_key,account_id,sample_id,"
            "CASE WHEN attribution IN ('','{}') THEN NULL ELSE attribution END FROM processing_fact_refs"
        )
        db.execute("DROP TABLE processing_fact_refs")
        db.execute("ALTER TABLE processing_fact_refs_v20 RENAME TO processing_fact_refs")
    else:
        fact_cols = {row[1] for row in db.execute("PRAGMA table_xinfo(processing_fact_refs)")}
        if fact_cols and "session_id" not in fact_cols:
            db.execute("ALTER TABLE processing_fact_refs ADD COLUMN session_id INTEGER GENERATED ALWAYS AS (NULL) VIRTUAL")
        if fact_cols and "subtype" not in fact_cols:
            db.execute("ALTER TABLE processing_fact_refs ADD COLUMN subtype TEXT GENERATED ALWAYS AS (NULL) VIRTUAL")
    if any(row[1] in {"attribution", "record_meta"} and row[3] for row in db.execute("PRAGMA table_info(local_record_refs)")):
        for name, in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='local_record_refs'").fetchall():
            db.execute(f"DROP TRIGGER IF EXISTS {name}")
        db.execute(
            "CREATE TABLE local_record_refs_v20("
            "record_key TEXT PRIMARY KEY,"
            "period TEXT NOT NULL,"
            "content_hash TEXT NOT NULL,"
            "sample_id INTEGER NOT NULL REFERENCES sample_records(id),"
            "attribution TEXT,"
            "record_meta TEXT)"
        )
        db.execute(
            "INSERT INTO local_record_refs_v20(record_key,period,content_hash,sample_id,attribution,record_meta) "
            "SELECT record_key,period,content_hash,sample_id,"
            "CASE WHEN attribution IN ('','{}') THEN NULL ELSE attribution END,"
            "CASE WHEN record_meta IN ('','{}') THEN NULL ELSE record_meta END FROM local_record_refs"
        )
        db.execute("DROP TABLE local_record_refs")
        db.execute("ALTER TABLE local_record_refs_v20 RENAME TO local_record_refs")
    remote_cols = {row[1]: row[3] for row in db.execute("PRAGMA table_info(remote_record_refs)")}
    if remote_cols and ("peer_position" not in remote_cols or remote_cols.get("attribution")):
        for name, in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='remote_record_refs'").fetchall():
            db.execute(f"DROP TRIGGER IF EXISTS {name}")
        has_peer_col = "peer_position" in remote_cols
        db.execute(
            "CREATE TABLE remote_record_refs_v20("
            "device_id INTEGER NOT NULL REFERENCES sample_devices(id),"
            "day TEXT NOT NULL,"
            "part_id TEXT NOT NULL,"
            "record_key TEXT NOT NULL,"
            "content_hash TEXT NOT NULL,"
            "sample_id INTEGER NOT NULL REFERENCES sample_records(id),"
            "attribution TEXT,"
            "record_meta TEXT,"
            "schema_version INTEGER,"
            "collection_period TEXT,"
            "event_at TEXT,"
            "envelope_meta TEXT NOT NULL,"
            "peer_position INTEGER,"
            "PRIMARY KEY(device_id,record_key))"
        )
        db.execute(
            "INSERT INTO remote_record_refs_v20(device_id,day,part_id,record_key,content_hash,sample_id,attribution,"
            "record_meta,schema_version,collection_period,event_at,envelope_meta,peer_position) "
            "SELECT device_id,day,part_id,record_key,content_hash,sample_id,"
            "CASE WHEN attribution IN ('','{}') THEN NULL ELSE attribution END,"
            "CASE WHEN record_meta IS NULL THEN 'null' WHEN record_meta IN ('','{}') THEN NULL ELSE record_meta END,"
            f"schema_version,collection_period,event_at,envelope_meta,{'peer_position' if has_peer_col else 'NULL'} "
            "FROM remote_record_refs"
        )
        db.execute("DROP TABLE remote_record_refs")
        db.execute("ALTER TABLE remote_record_refs_v20 RENAME TO remote_record_refs")
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='processing_peer_rows'").fetchone():
        db.execute(
            "UPDATE remote_record_refs SET peer_position=("
            "SELECT p.position FROM processing_peer_rows p "
            "JOIN sample_devices d ON d.id=remote_record_refs.device_id "
            "WHERE p.source='peer:'||d.value AND p.record_key=remote_record_refs.record_key AND p.digest=remote_record_refs.content_hash)"
        )
        db.execute("DROP TABLE processing_peer_rows")


def migrate_layout(db):
    """Contract 20: consolidate rows, inline 5h/7d readings, share period metadata, and retire quota compaction."""
    has_peer_table = bool(db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_peer_rows' AND type='table'").fetchone())
    if not db.execute(
        "SELECT 1 FROM sqlite_master WHERE (name='sample_sync' AND type='table') "
        "OR (name='sample_records' AND sql NOT LIKE '%five_hour_usedPercent%') "
        "OR (name='processing_fact_refs' AND sql LIKE '%event_id%')"
    ).fetchone() and not (has_peer_table and db.execute("SELECT 1 FROM processing_peer_rows LIMIT 1").fetchone()):
        return
    db.execute("SAVEPOINT quota_layout")
    try:
        has_legacy_sync = bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sample_sync'").fetchone())
        has_inline_windows = bool(db.execute("SELECT 1 FROM pragma_table_info('sample_records') WHERE name='five_hour_usedPercent'").fetchone())
        views_existed = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='view' AND name IN ('processing_facts','local_records','remote_records')").fetchall()]
        effective_view_sql = db.execute("SELECT sql FROM sqlite_master WHERE type='view' AND name='processing_effective'").fetchone()
        for view in views_existed:
            db.execute(f"DROP VIEW IF EXISTS {view}")
        if effective_view_sql:
            db.execute("DROP VIEW IF EXISTS processing_effective")
        normalized = {}
        if has_legacy_sync:
            # Released rows retain their exact decoder until this migration removes retired metadata.
            from monitor_migration_samples import select_sample as legacy_select
            for sample, kind, digest in db.execute("SELECT id,kind,content_hash FROM sample_records WHERE kind='quota'"):
                row = legacy_select(db, sample)
                if "compaction" in row:
                    if hashlib.sha256(canonical([kind, row]).encode()).digest() != digest:
                        raise ValueError(f"Quota layout round-trip failed for sample {sample}")
                    row.pop("compaction")
                    normalized[sample] = hashlib.sha256(canonical([kind, row]).encode()).digest()
                    containers = json.loads(db.execute("SELECT containers FROM sample_records WHERE id=?", (sample,)).fetchone()[0])
                    containers.pop("compaction", None)
                    db.execute("UPDATE sample_records SET containers=? WHERE id=?", (canonical(containers), sample))
            existing_cols = {row[1] for row in db.execute("PRAGMA table_xinfo(sample_records)")}
            for table, prefix in INLINE.items():
                for name, kind in MASKS + FIELDS[table]:
                    col_name = prefix + _column(name, kind)
                    if col_name not in existing_cols:
                        db.execute(f"ALTER TABLE sample_records ADD COLUMN {_definition(name, kind, prefix)}")
                if table in {"sample_sync", "token_samples", "baseline_sessions"}:
                    columns = [_column(name, kind) for name, kind in MASKS + FIELDS[table]]
                    db.execute("UPDATE sample_records SET " + ",".join(
                        f"{prefix}{column}=(SELECT {column} FROM {table} WHERE sample_id=sample_records.id)" for column in columns))
            db.execute("CREATE TABLE legacy_quota_windows AS SELECT * FROM quota_windows")
            db.execute("DROP TABLE quota_windows")
            for column in ("five_hour_id", "seven_day_id"):
                if column not in existing_cols:
                    db.execute(f"ALTER TABLE sample_records ADD COLUMN {column} INTEGER REFERENCES quota_windows(id)")
            for table in ("sample_sync", "token_samples", "baseline_sessions", "quota_samples"):
                db.execute(f"DROP TABLE IF EXISTS {table}")
        elif not has_inline_windows:
            existing_cols = {row[1] for row in db.execute("PRAGMA table_xinfo(sample_records)")}
            for table in ("five_hour", "seven_day"):
                prefix = INLINE[table]
                for name, kind in MASKS + FIELDS[table]:
                    col_name = prefix + _column(name, kind)
                    if col_name not in existing_cols:
                        db.execute(f"ALTER TABLE sample_records ADD COLUMN {_definition(name, kind, prefix)}")
            db.execute("DROP TRIGGER IF EXISTS quota_window_release")
            db.execute("DROP TABLE IF EXISTS quota_window_releases")
            db.execute("CREATE TABLE legacy_quota_windows AS SELECT * FROM quota_windows")
            db.execute("CREATE TABLE legacy_sample_window_refs AS SELECT id AS sample_id,five_hour_id,seven_day_id FROM sample_records WHERE five_hour_id IS NOT NULL OR seven_day_id IS NOT NULL")
            db.execute("UPDATE sample_records SET five_hour_id=NULL,seven_day_id=NULL WHERE five_hour_id IS NOT NULL OR seven_day_id IS NOT NULL")
            db.execute("DROP TABLE quota_windows")
        _virtualize_sample_metadata(db)
        _migrate_ref_tables(db)
        initialize(db)
        if views_existed:
            create_views(db)
        if effective_view_sql and effective_view_sql[0]:
            db.execute(effective_view_sql[0])
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='legacy_quota_windows'").fetchone():
            if has_legacy_sync:
                for sample, window in db.execute("SELECT sample_id,window FROM legacy_quota_windows"):
                    value = _read_node(db, "quota_windows", "sample_id=? AND window=?", (sample, window), storage="legacy_quota_windows", fields=LEGACY_WINDOW_FIELDS)
                    payload = _read_node(db, "sample_records", "id=?", (sample,))
                    payload["sync"] = _read_node(db, "sample_sync", "sample_id=?", (sample,)) or {}
                    _write_node(db, "five_hour" if window == "5h" else "seven_day", {"sample_id": sample}, value, ("resetAt", "plan"))
                    window_id = _insert_window(db, window, value, payload)
                    db.execute(f"UPDATE sample_records SET {'five_hour_id' if window == '5h' else 'seven_day_id'}=? WHERE id=?", (window_id, sample))
            else:
                for sample, five_id, seven_id in db.execute("SELECT sample_id,five_hour_id,seven_day_id FROM legacy_sample_window_refs"):
                    payload = _read_node(db, "sample_records", "id=?", (sample,)) or {}
                    payload["sync"] = _read_node(db, "sample_sync", "sample_id=?", (sample,)) or {}
                    for window, old_id, col, table in (("5h", five_id, "five_hour_id", "five_hour"), ("7d", seven_id, "seven_day_id", "seven_day")):
                        if old_id is None:
                            continue
                        value = _read_node(db, "quota_windows", "id=?", (old_id,), storage="legacy_quota_windows", fields=LEGACY_WINDOW_FIELDS)
                        if value is not None:
                            _write_node(db, table, {"sample_id": sample}, value, ("resetAt", "plan"))
                            window_id = _insert_window(db, window, value, payload)
                            db.execute(f"UPDATE sample_records SET {col}=? WHERE id=?", (window_id, sample))
                db.execute("DROP TABLE legacy_sample_window_refs")
            for sample, kind, digest in db.execute("SELECT id,kind,content_hash FROM sample_records"):
                if hashlib.sha256(canonical([kind, select_sample(db, sample)]).encode()).digest() != normalized.get(sample, digest):
                    raise ValueError(f"Quota layout round-trip failed for sample {sample}")
            for sample, digest in normalized.items():
                found = db.execute("SELECT id FROM sample_records WHERE content_hash=?", (digest,)).fetchone()
                if found and found[0] != sample:
                    for table in ("processing_fact_refs", "local_record_refs", "remote_record_refs"):
                        db.execute(f"UPDATE {table} SET sample_id=? WHERE sample_id=?", (found[0], sample))
                    db.execute("INSERT OR IGNORE INTO sample_releases VALUES(?)", (sample,))
                else:
                    db.execute("UPDATE sample_records SET content_hash=? WHERE id=?", (digest, sample))
            if normalized:
                from monitor_usage_sync import content_hash
                create_views(db)
                for key, data in db.execute("SELECT record_key,record_json FROM local_records").fetchall():
                    db.execute("UPDATE local_record_refs SET content_hash=? WHERE record_key=?", (content_hash(json.loads(data)), key))
                for machine, key, data in db.execute("SELECT machine_id,record_key,record_json FROM remote_records").fetchall():
                    db.execute("UPDATE remote_record_refs SET content_hash=? WHERE device_id=(SELECT id FROM sample_devices WHERE value=?) AND record_key=?",
                        (content_hash(json.loads(data)["record"]), machine, key))
            db.execute("DROP TABLE legacy_quota_windows")
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='processing_counts'").fetchone():
            from monitor_processing import _rebuild_counts, _install_count_triggers
            _install_count_triggers(db)
            _rebuild_counts(db)
        collect(db)
        if db.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("Quota layout foreign key check failed")
        db.execute("RELEASE quota_layout")
    except BaseException:
        db.execute("ROLLBACK TO quota_layout")
        db.execute("RELEASE quota_layout")
        raise


def insert_sample(db, kind, row):
    migrate_layout(db)
    payload = _payload(row)
    if kind == "quota" and isinstance(payload, dict):
        payload.pop("compaction", None)
    digest = hashlib.sha256(canonical([kind, payload]).encode()).digest()
    found = db.execute("SELECT id FROM sample_records WHERE content_hash=?", (digest,)).fetchone()
    if found:
        return found[0], _encode_attrs(_attribution(row))
    subtype = row.get("recordType") if isinstance(row, dict) else None
    known = isinstance(row, dict) and (kind == "quota" or kind == "tokenLedger" and subtype in {"usage", "sessionBaseline"})
    children = {"sync"}
    children |= {"windows"} if kind == "quota" else {"tokens", "cost"} if subtype == "usage" else {"session", "sourceTotals"}
    event = row.get("checkedAt") or row.get("occurredAt") or ((row.get("session") or {}).get("updatedAt") if isinstance(row.get("session"), dict) else None) if isinstance(row, dict) else None
    sync = row.get("sync") if isinstance(row, dict) and isinstance(row.get("sync"), dict) else {}
    session = row.get("session") if isinstance(row, dict) and isinstance(row.get("session"), dict) else {}
    account = sync.get("accountId") or (row.get("usageAccountId") if isinstance(row, dict) else None) or session.get("usageAccountId")
    identity_session = (row.get("sessionId") if isinstance(row, dict) else None) or session.get("sessionId")
    empty_session = _dimension(db, "session", "")
    opaque_meta = {}
    if not known and isinstance(row, dict):
        for k in ("recordType", "eventId", "sessionId", "usageAccountId"):
            if isinstance(row.get(k), str):
                opaque_meta[k] = row[k]
    _write_node(db, "sample_records", {
        "kind": kind,
        "content_hash": digest,
        "event_at": parse_timestamp(event),
        "opaque": canonical(payload) if not known else None,
    }, payload if known else opaque_meta, children)
    sample = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    if not known:
        db.execute(
            "UPDATE sample_records SET sessionId_id=COALESCE(sessionId_id,?),eventId=COALESCE(eventId,''),"
            "usageAccountId_id=COALESCE(usageAccountId_id,?),sync_originMachineId_id=COALESCE(sync_originMachineId_id,?) WHERE id=?",
            (
                _dimension(db, "session", identity_session) if isinstance(identity_session, str) else empty_session,
                _dimension(db, "account", account) if isinstance(account, str) else None,
                _dimension(db, "device", sync["originMachineId"]) if isinstance(sync.get("originMachineId"), str) else None,
                sample,
            ),
        )
        return sample, _encode_attrs(_attribution(row))
    containers = {}
    _child(db, sample, containers, "sync", "sample_sync", payload)
    if kind == "quota":
        if isinstance(payload.get("windows"), dict):
            containers["windows"] = {key: value for key, value in payload["windows"].items() if key not in {"5h", "7d"}}
            for key in ("5h", "7d"):
                value = payload["windows"].get(key)
                if isinstance(value, dict):
                    containers["windows"][key] = {}
                    _write_node(db, "five_hour" if key == "5h" else "seven_day", {"sample_id": sample}, value, ("resetAt", "plan"))
                    window_id = _insert_window(db, key, value, payload)
                    db.execute(f"UPDATE sample_records SET {'five_hour_id' if key == '5h' else 'seven_day_id'}=? WHERE id=?", (window_id, sample))
                elif key in payload["windows"]:
                    containers["windows"][key] = value
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
    db.execute(
        "UPDATE sample_records SET containers=?,sessionId_id=COALESCE(sessionId_id,baseline_sessionId_id,?),eventId=COALESCE(eventId,'') WHERE id=?",
        (canonical(containers), empty_session, sample),
    )
    return sample, _encode_attrs(_attribution(row))


def _write_totals(db, sample, model, value):
    for key in ("tokens", "cost", "fastTokens"):
        if isinstance(value.get(key), dict):
            _write_node(db, "baseline_totals", {"sample_id": sample, "model_id": model, "category": key}, value[key])


FIELDS["baseline_totals"] = FIELDS["token_samples"]


def select_sample(db, sample, attribution=None):
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='sample_sync'").fetchone():
        from monitor_migration_samples import select_sample as legacy_select
        return legacy_select(db, sample, attribution or "{}")
    kind, subtype, opaque, containers = db.execute("SELECT kind,subtype,opaque,containers FROM sample_records WHERE id=?", (sample,)).fetchone()
    if opaque is not None:
        row = json.loads(opaque)
    else:
        row = _read_node(db, "sample_records", "id=?", (sample,))
        containers = json.loads(containers)
        row.update(containers)
        for key, table in (("sync", "sample_sync"),):
            value = _read_node(db, table, "sample_id=?", (sample,))
            if value is not None and key in containers and isinstance(containers[key], dict):
                row[key] = value
        if kind == "quota" and isinstance(row.get("windows"), dict):
            for key, table, col in (("5h", "five_hour", "five_hour_id"), ("7d", "seven_day", "seven_day_id")):
                value = _read_node(db, table, "id=?", (sample,))
                if value is not None:
                    window_id = db.execute(f"SELECT {col} FROM sample_records WHERE id=?", (sample,)).fetchone()[0]
                    period = _read_node(db, "quota_windows", "id=?", (window_id,)) if window_id is not None else None
                    if period:
                        value.update(period)
                    row["windows"][key] = value
        elif subtype == "usage":
            totals = _read_node(db, "token_samples", "sample_id=?", (sample,)) or {}
            for key, fields in (("tokens", TOKENS), ("cost", COSTS)):
                if isinstance(row.get(key), dict):
                    row[key].update({name: value for name, value in totals.items() if name in fields})
        elif subtype == "sessionBaseline":
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
    attrs = _decode_attrs(attribution)
    if "session" in attrs and isinstance(row, dict) and isinstance(row.get("session"), dict):
        row["session"].update(attrs.pop("session"))
    if attrs and isinstance(row, dict):
        row.update(attrs)
    return row


def _read_totals(db, sample, model, value):
    for category, in db.execute("SELECT category FROM baseline_totals WHERE sample_id=? AND model_id=?", (sample, model)):
        value[category] = _read_node(db, "baseline_totals", "sample_id=? AND model_id=? AND category=?", (sample, model, category))


def collect(db):
    # Candidate-only cleanup avoids a full-history scan on idle commits.
    has_effective = bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='processing_effective_refs'").fetchone())
    db.execute(
        "DELETE FROM sample_records WHERE id IN (SELECT sample_id FROM sample_releases) "
        "AND NOT EXISTS(SELECT 1 FROM processing_fact_refs WHERE sample_id=sample_records.id) "
        "AND NOT EXISTS(SELECT 1 FROM local_record_refs WHERE sample_id=sample_records.id) "
        "AND NOT EXISTS(SELECT 1 FROM remote_record_refs WHERE sample_id=sample_records.id)"
        + (" AND NOT EXISTS(SELECT 1 FROM processing_effective_refs WHERE sample_id=sample_records.id)" if has_effective else "")
    )
    db.execute("DELETE FROM sample_releases")
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='quota_window_releases'").fetchone():
        db.execute("DELETE FROM quota_windows WHERE id IN (SELECT id FROM quota_window_releases) "
            "AND NOT EXISTS(SELECT 1 FROM sample_records WHERE five_hour_id=quota_windows.id) "
            "AND NOT EXISTS(SELECT 1 FROM sample_records WHERE seven_day_id=quota_windows.id)")
        db.execute("DELETE FROM quota_window_releases")
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_payload_releases'").fetchone():
        guards = [
            f"AND NOT EXISTS(SELECT 1 FROM {table} WHERE payload_id=processing_payloads.id)"
            for table in ("processing_sampled_quota_refs", "processing_quota_point_refs", "processing_display_refs")
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        ]
        db.execute("DELETE FROM processing_payloads WHERE id IN (SELECT id FROM processing_payload_releases) " + " ".join(guards))
        db.execute("DELETE FROM processing_payload_releases")


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
    db.create_function("put_payload", 2, lambda data, key: put_payload(db, data, key))
    db.create_function("display_payload", 2, display_payload)
    return db


def _record(db, sample, attrs, extra):
    row = select_sample(db, sample, attrs)
    meta = json.loads(extra) if extra else {}
    return row if meta is None else {"kind": db.execute("SELECT kind FROM sample_records WHERE id=?", (sample,)).fetchone()[0], **meta, "row": row}


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
    if values[7] is not None:
        db.execute("UPDATE sample_records SET event_at=COALESCE(event_at,?) WHERE id=?", (values[7], sample))
    db.execute(
        "INSERT OR REPLACE INTO processing_fact_refs(source_id,position,record_key,account_id,sample_id,attribution) VALUES(?,?,?,?,?,?)",
        (_dimension(db, "source", values[0]), values[1], values[3], _dimension(db, "account", values[4]), sample, attrs),
    )


def put_local(db, key, period, digest, record):
    sample, attrs = insert_sample(db, record["kind"], record["row"])
    meta = {k: v for k, v in record.items() if k not in {"kind", "row"}}
    db.execute(
        "INSERT INTO local_record_refs VALUES(?,?,?,?,?,?) ON CONFLICT(record_key) DO UPDATE SET "
        "content_hash=excluded.content_hash,sample_id=excluded.sample_id,attribution=excluded.attribution,record_meta=excluded.record_meta",
        (key, period, digest, sample, attrs, canonical(meta) if meta else None),
    )


def put_remote(db, machine, day, part, key, digest, envelope):
    record = envelope["record"]
    sample, attrs = insert_sample(db, record["kind"], record["row"] if "row" in record else record)
    meta = {k: v for k, v in record.items() if k not in {"row", "kind"}} if "row" in record else None
    db.execute(
        "INSERT INTO remote_record_refs(device_id,day,part_id,record_key,content_hash,sample_id,attribution,record_meta,"
        "schema_version,collection_period,event_at,envelope_meta,peer_position) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,NULL) "
        "ON CONFLICT(device_id,record_key) DO UPDATE SET day=excluded.day,part_id=excluded.part_id,"
        "content_hash=excluded.content_hash,sample_id=excluded.sample_id,attribution=excluded.attribution,"
        "record_meta=excluded.record_meta,schema_version=excluded.schema_version,collection_period=excluded.collection_period,"
        "event_at=excluded.event_at,envelope_meta=excluded.envelope_meta,"
        "peer_position=CASE WHEN remote_record_refs.day=excluded.day AND remote_record_refs.content_hash=excluded.content_hash "
        "THEN remote_record_refs.peer_position ELSE NULL END",
        (
            _dimension(db, "device", machine),
            day,
            part,
            key,
            digest,
            sample,
            attrs,
            "null" if meta is None else (canonical(meta) if meta else None),
            envelope.get("schemaVersion"),
            envelope.get("collectionPeriod"),
            envelope.get("eventAt"),
            canonical([
                sum(1 << i for i, k in enumerate(("schemaVersion", "collectionPeriod", "eventAt", "kind", "sourceMachineId", "recordKey", "contentSha256")) if k in envelope),
                {k: v for k, v in envelope.items() if k not in {"record", "schemaVersion", "collectionPeriod", "eventAt", "kind", "sourceMachineId", "recordKey", "contentSha256"}},
            ]),
        ),
    )


def initialize(db):
    for kind in sorted(DIMENSIONS):
        db.execute(f"CREATE TABLE IF NOT EXISTS sample_{kind}s(id INTEGER PRIMARY KEY,value TEXT NOT NULL UNIQUE)")
    owners = {
        "sample_records": (
            "id INTEGER PRIMARY KEY,kind TEXT NOT NULL,content_hash BLOB NOT NULL UNIQUE,event_at REAL,"
            "opaque TEXT,containers TEXT,five_hour_id INTEGER REFERENCES quota_windows(id),seven_day_id INTEGER REFERENCES quota_windows(id)"
            + "".join("," + _definition(name, kind, prefix) for table, prefix in INLINE.items() for name, kind in MASKS + FIELDS[table])
            + ",subtype TEXT GENERATED ALWAYS AS (recordType) VIRTUAL"
            + ",account_id INTEGER GENERATED ALWAYS AS (COALESCE(sync_accountId_id,usageAccountId_id,baseline_usageAccountId_id)) VIRTUAL"
            + ",origin_device_id INTEGER GENERATED ALWAYS AS (sync_originMachineId_id) VIRTUAL"
            + ",identity_session_id INTEGER GENERATED ALWAYS AS (COALESCE(sessionId_id,baseline_sessionId_id)) VIRTUAL"
        ),
        "quota_windows": (
            "id INTEGER PRIMARY KEY,content_hash BLOB NOT NULL UNIQUE,window TEXT NOT NULL,"
            "account_id INTEGER REFERENCES sample_accounts(id),origin_device_id INTEGER REFERENCES sample_devices(id)"
        ),
        "baseline_models": "sample_id INTEGER REFERENCES sample_records(id) ON DELETE CASCADE,model_id INTEGER REFERENCES sample_models(id)",
        "baseline_tiers": "sample_id INTEGER REFERENCES sample_records(id) ON DELETE CASCADE,model_id INTEGER,tier_id INTEGER REFERENCES sample_tiers(id)",
        "baseline_totals": "sample_id INTEGER REFERENCES sample_records(id) ON DELETE CASCADE,model_id INTEGER,category TEXT",
    }
    for table, fields in FIELDS.items():
        if table in INLINE:
            continue
        columns = [owners[table], "present INTEGER NOT NULL", "nulls INTEGER NOT NULL", "floats INTEGER NOT NULL", "bools INTEGER NOT NULL", "extensions TEXT"]
        columns += [_definition(name, kind) for name, kind in fields]
        if table in {"baseline_models", "baseline_tiers", "baseline_totals"}:
            columns += [{
                "baseline_models": "PRIMARY KEY(sample_id,model_id)",
                "baseline_tiers": "PRIMARY KEY(sample_id,model_id,tier_id)",
                "baseline_totals": "PRIMARY KEY(sample_id,model_id,category)",
            }[table]]
        db.execute(f"CREATE TABLE IF NOT EXISTS {table}({','.join(columns)})")
    db.execute("CREATE INDEX IF NOT EXISTS sample_account_time ON sample_records(sync_accountId_id,id)")
    for column in ("five_hour_id", "seven_day_id"):
        db.execute(f"CREATE INDEX IF NOT EXISTS sample_{column} ON sample_records({column})")
    db.execute("CREATE INDEX IF NOT EXISTS quota_window_account_reset ON quota_windows(account_id,window,resetAt)")
    db.execute("CREATE TABLE IF NOT EXISTS quota_window_releases(id INTEGER PRIMARY KEY)")
    db.execute(
        "CREATE TRIGGER IF NOT EXISTS quota_window_release AFTER DELETE ON sample_records BEGIN "
        "INSERT OR IGNORE INTO quota_window_releases SELECT OLD.five_hour_id WHERE OLD.five_hour_id IS NOT NULL; "
        "INSERT OR IGNORE INTO quota_window_releases SELECT OLD.seven_day_id WHERE OLD.seven_day_id IS NOT NULL; END"
    )
    db.execute("CREATE INDEX IF NOT EXISTS sample_account_event_time ON sample_records(account_id,event_at)")
    db.execute("CREATE INDEX IF NOT EXISTS sample_event_time ON sample_records(event_at,id)")
    db.execute("CREATE INDEX IF NOT EXISTS sample_session ON sample_records(identity_session_id,event_at)")
    db.execute("CREATE INDEX IF NOT EXISTS sample_event ON sample_records(eventId)")
    db.execute("CREATE TABLE IF NOT EXISTS sample_releases(sample_id INTEGER PRIMARY KEY)")
    db.execute("CREATE TABLE IF NOT EXISTS processing_payloads(id INTEGER PRIMARY KEY,content_hash BLOB NOT NULL UNIQUE,data TEXT NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS processing_payload_releases(id INTEGER PRIMARY KEY)")
    db.execute(
        "CREATE TABLE IF NOT EXISTS processing_fact_refs("
        "source_id INTEGER NOT NULL REFERENCES sample_sources(id),"
        "position INTEGER NOT NULL,"
        "record_key TEXT NOT NULL,"
        "account_id INTEGER NOT NULL REFERENCES sample_accounts(id),"
        "sample_id INTEGER NOT NULL REFERENCES sample_records(id),"
        "attribution TEXT,"
        "session_id INTEGER GENERATED ALWAYS AS (NULL) VIRTUAL,"
        "subtype TEXT GENERATED ALWAYS AS (NULL) VIRTUAL,"
        "PRIMARY KEY(source_id,position))"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS local_record_refs("
        "record_key TEXT PRIMARY KEY,"
        "period TEXT NOT NULL,"
        "content_hash TEXT NOT NULL,"
        "sample_id INTEGER NOT NULL REFERENCES sample_records(id),"
        "attribution TEXT,"
        "record_meta TEXT)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS remote_record_refs("
        "device_id INTEGER NOT NULL REFERENCES sample_devices(id),"
        "day TEXT NOT NULL,"
        "part_id TEXT NOT NULL,"
        "record_key TEXT NOT NULL,"
        "content_hash TEXT NOT NULL,"
        "sample_id INTEGER NOT NULL REFERENCES sample_records(id),"
        "attribution TEXT,"
        "record_meta TEXT,"
        "schema_version INTEGER,"
        "collection_period TEXT,"
        "event_at TEXT,"
        "envelope_meta TEXT NOT NULL,"
        "peer_position INTEGER,"
        "PRIMARY KEY(device_id,record_key))"
    )
    for table in ("processing_fact_refs", "local_record_refs", "remote_record_refs"):
        db.execute(f"CREATE INDEX IF NOT EXISTS {table}_sample ON {table}(sample_id)")
        for action in ("DELETE", "UPDATE"):
            db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_{action.lower()} AFTER {action} ON {table} BEGIN INSERT OR IGNORE INTO sample_releases VALUES(OLD.sample_id); END")
    for obsolete in (
        "processing_facts_session",
        "processing_facts_event",
        "processing_fact_source_event",
        "processing_fact_source_time",
        "processing_baselines",
        "processing_fact_source_session",
        "quota_window_account_time",
    ):
        db.execute(f"DROP INDEX IF EXISTS {obsolete}")
    for name, fields in {
        "processing_facts_key": "record_key",
        "processing_facts_account": "account_id,position",
        "processing_fact_source_key": "source_id,record_key",
        "processing_fact_account_key": "account_id,record_key,source_id",
    }.items():
        db.execute(f"CREATE INDEX IF NOT EXISTS {name} ON processing_fact_refs({fields})")
    db.execute("CREATE INDEX IF NOT EXISTS local_records_period ON local_record_refs(period)")
    db.execute("CREATE INDEX IF NOT EXISTS remote_records_day ON remote_record_refs(device_id,day,part_id)")


def create_views(db):
    db.execute(
        "CREATE VIEW IF NOT EXISTS processing_facts AS "
        "SELECT s.value AS source,f.position,r.kind,f.record_key,a.value AS account,x.value AS session,"
        "r.eventId AS event_id,r.event_at,sample_row(f.sample_id,f.attribution) AS data,r.subtype "
        "FROM processing_fact_refs f "
        "JOIN sample_records r ON r.id=f.sample_id "
        "JOIN sample_sources s ON s.id=f.source_id "
        "JOIN sample_accounts a ON a.id=f.account_id "
        "JOIN sample_sessions x ON x.id=r.identity_session_id"
    )
    db.execute("CREATE VIEW IF NOT EXISTS local_records AS SELECT record_key,period,content_hash,sample_record(sample_id,attribution,record_meta) AS record_json FROM local_record_refs")
    db.execute(
        "CREATE VIEW IF NOT EXISTS remote_records AS "
        "SELECT d.value AS machine_id,r.day,r.part_id,r.record_key,r.content_hash,"
        "sample_envelope(sample_id,attribution,record_meta,envelope_meta,schema_version,collection_period,event_at,d.value,r.record_key,r.content_hash) AS record_json "
        "FROM remote_record_refs r JOIN sample_devices d ON d.id=r.device_id"
    )
    for view, table, condition in (
        ("processing_facts", "processing_fact_refs", "source_id=(SELECT id FROM sample_sources WHERE value=OLD.source) AND position=OLD.position"),
        ("local_records", "local_record_refs", "record_key=OLD.record_key"),
        ("remote_records", "remote_record_refs", "device_id=(SELECT id FROM sample_devices WHERE value=OLD.machine_id) AND record_key=OLD.record_key"),
    ):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS {view}_delete INSTEAD OF DELETE ON {view} BEGIN DELETE FROM {table} WHERE {condition}; END")


def prepare(db):
    """Initialize typed stores, adopting the pending contract-20 layout if needed."""
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name IN ('processing_facts','local_records','remote_records')").fetchone():
        raise ValueError("Sample store requires startup data migration")
    migrate_layout(db)
    initialize(db)
    create_views(db)
    return True
