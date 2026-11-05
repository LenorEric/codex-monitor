"""Typed canonical persistence; the same mappings serve both datasets.

Presence/integer masks retain optional-field and numeric spelling semantics used by
stable identities. Timestamp text retains original ISO spelling, not a JSON copy.
Only unrecognized extension metadata is serialized.
"""

import copy
import json
import math
from functools import lru_cache

from monitor_common import empty_cost_totals, empty_token_totals, parse_timestamp
from monitor_usage_sync import canonical_json, record_account_key, record_key


COMMON = (
    ("account_slot", "accountSlotId", "TEXT", False),
    ("account_label", "accountLabel", "TEXT", False),
    ("usage_account", "usageAccountId", "TEXT", False),
    ("sync_version", "sync.version", "INTEGER", True),
    ("origin_machine", "sync.originMachineId", "TEXT", True),
    ("sync_account", "sync.accountId", "TEXT", True),
    ("sync_record", "sync.recordId", "TEXT", False),
    ("local_only", "sync.localOnly", "BOOL", False),
)
QUOTA = COMMON + (
    ("run_version", "quotaRunVersion", "INTEGER", True),
    ("observed_at", "checkedAt", "TIME", True),
    ("used_percent", "windows.$.usedPercent", "REAL", True),
    ("continuous", "windows.$.continuous", "REAL", False),
    ("multiplier", "windows.$.multiplier", "REAL", True),
    ("plan", "windows.$.plan", "TEXT", True),
    ("reset_at", "windows.$.resetAt", "TIME", False),
    ("range_first", "windows.$.firstAt", "INTEGER", True),
    ("range_last", "windows.$.lastAt", "INTEGER", True),
    ("reset_boundary", "windows.$.resetBoundary", "BOOL", True),
)
TOKEN = COMMON + (
    ("schema_version", "schemaVersion", "INTEGER", True),
    ("record_type", "recordType", "TEXT", True),
    ("event_id", "eventId", "TEXT", True),
    ("session_id", "sessionId", "TEXT", True),
    ("occurred_at", "occurredAt", "TIME", True),
    ("raw_model", "rawModel", "TEXT", True),
    ("billing_model", "billingModel", "TEXT", True),
    ("service_tier", "serviceTier", "TEXT", True),
    ("source_file", "sourceFile", "TEXT", False),
) + tuple(("token_" + key, "tokens." + key, "INTEGER", True) for key in empty_token_totals()) + tuple(
    ("cost_" + key, "cost." + key, "REAL", True) for key in empty_cost_totals())
TABLES = {"quota": ("canonical_quota", QUOTA), "tokenLedger": ("canonical_tokens", TOKEN)}
MAX_FLOAT = "1.7976931348623157e308"


def initialize(db):
    """DDL without executescript: callers may already own a migration transaction."""
    for kind, (table, fields) in TABLES.items():
        columns = ["dataset TEXT NOT NULL CHECK(dataset IN ('local','merged'))", "account TEXT NOT NULL", "record_key TEXT NOT NULL",
            "first INTEGER NOT NULL", "last INTEGER NOT NULL CHECK(last>=first)", "present INTEGER NOT NULL", "integers INTEGER NOT NULL", "extras_json TEXT"]
        if kind == "quota":
            columns.append("window TEXT NOT NULL CHECK(window IN ('5h','7d'))")
        for column, _, dtype, required in fields:
            sql_type = {"BOOL": "INTEGER", "TIME": "REAL"}.get(dtype, dtype)
            definition = f'"{column}" {sql_type}' + (" NOT NULL" if required else "")
            if dtype in {"REAL", "TIME"}:
                definition += f' CHECK("{column}" BETWEEN -{MAX_FLOAT} AND {MAX_FLOAT})'
            if dtype == "BOOL":
                definition += f' CHECK("{column}" IN (0,1))'
            if column.startswith(("token_", "cost_")) or column == "multiplier":
                definition += f' CHECK("{column}">=0)'
            if column == "used_percent":
                definition += ' CHECK(used_percent BETWEEN 0 AND 100)'
            if column in {"run_version", "sync_version"}:
                definition += f' CHECK("{column}"=1)'
            if column == "schema_version":
                definition += ' CHECK(schema_version=2)'
            if column == "record_type":
                definition += " CHECK(record_type='usage')"
            if column == "service_tier":
                definition += " CHECK(service_tier IN ('default','fast'))"
            columns.append(definition)
            if dtype == "TIME":
                columns.append(column + "_text TEXT")
        if kind == "quota":
            columns.append("CHECK(range_first<=range_last)")
        columns.append("PRIMARY KEY(dataset,account,record_key)")
        db.execute(f"CREATE TABLE IF NOT EXISTS {table}({','.join(columns)}) STRICT")
        db.execute(f"CREATE INDEX IF NOT EXISTS {table}_time ON {table}(dataset,account,first)")
    db.execute("CREATE INDEX IF NOT EXISTS canonical_quota_overlap ON canonical_quota(dataset,account,window,first,last)")
    db.execute("CREATE INDEX IF NOT EXISTS canonical_quota_predecessor ON canonical_quota(dataset,account,window,last)")
    db.execute("CREATE INDEX IF NOT EXISTS canonical_tokens_event ON canonical_tokens(dataset,event_id)")


def _paths(path, window):
    return path.replace("$", window or "").split(".")


def _pop(value, path):
    if len(path) == 1:
        return (True, value.pop(path[0])) if path[0] in value else (False, None)
    child = value.get(path[0])
    if not isinstance(child, dict):
        return False, None
    result = _pop(child, path[1:])
    if not child:
        del value[path[0]]
    return result


class CanonicalRepository:
    @staticmethod
    def write(db, dataset, kind, row, key=None, account=None, bounds=None):
        table, fields = TABLES[kind]
        window = None
        if kind == "quota":
            if len(row.get("windows", {})) != 1:
                raise ValueError("Canonical quota records must contain exactly one window")
            window = next(iter(row["windows"]))
        stamp = parse_timestamp(row.get("checkedAt") if kind == "quota" else row.get("occurredAt"))
        if stamp is None or not math.isfinite(stamp):
            raise ValueError("Invalid canonical timestamp")
        first, last = bounds or (int(stamp), row["windows"][window]["lastAt"] if kind == "quota" else int(stamp))
        columns = ["dataset", "account", "record_key", "first", "last", "present", "integers", "extras_json"]
        values = [dataset, account if account is not None else record_account_key(row), key if key is not None else record_key(kind, row), first, last, 0, 0, None]
        if kind == "quota":
            columns.append("window")
            values.append(window)
        extras = copy.deepcopy(row)
        for bit, (column, path, dtype, required) in enumerate(fields):
            present, value = _pop(extras, _paths(path, window))
            if required and (not present or value is None):
                raise ValueError(f"Missing canonical {path}")
            values[5] |= int(present) << bit
            values[6] |= int(type(value) is int) << bit
            text = None
            if value is not None:
                if dtype == "TIME":
                    text = value if isinstance(value, str) else None
                    value = parse_timestamp(value)
                    if value is None or not math.isfinite(value):
                        raise ValueError(f"Invalid canonical {path}")
                elif dtype == "BOOL":
                    if type(value) is not bool:
                        raise ValueError(f"Invalid canonical boolean {path}")
                    value = int(value)
                elif dtype == "INTEGER":
                    if type(value) is not int:
                        raise ValueError(f"Invalid canonical integer {path}")
                elif dtype == "REAL":
                    if type(value) not in (int, float) or not math.isfinite(value):
                        raise ValueError(f"Invalid canonical number {path}")
                elif not isinstance(value, str):
                    raise ValueError(f"Invalid canonical text {path}")
            columns.append(column)
            values.append(value)
            if dtype == "TIME":
                columns.append(column + "_text")
                values.append(text)
        values[7] = canonical_json(extras).decode() if extras else None
        db.execute(f"INSERT OR REPLACE INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in values)})", values)

    @staticmethod
    @lru_cache(maxsize=8)
    def decoder(kind, names):
        """Compile fixed-schema tuple access once, avoiding per-field path walking.

        Generated expressions use only this module's field definitions and numeric
        column offsets. No stored strings, SQL conditions or extensions are code.
        """
        positions = {name: index for index, name in enumerate(names)}
        source = ["def decode(values):", f"    present = values[{positions['present']}]", f"    integers = values[{positions['integers']}]",
            f"    extra = values[{positions['extras_json']}]", "    row = json.loads(extra) if extra else {}", "    sync = row.setdefault('sync', {})"]
        if kind == "quota":
            source.append(f"    window = row.setdefault('windows', {{}}).setdefault(values[{positions['window']}], {{}})")
        else:
            source.extend(["    tokens = row.setdefault('tokens', {})", "    cost = row.setdefault('cost', {})"])
        for bit, (column, path, dtype, required) in enumerate(TABLES[kind][1]):
            target = "window" if path.startswith("windows.") else path.split(".")[0] if "." in path else "row"
            key = path.rsplit(".", 1)[-1]
            value = f"values[{positions[column]}]"
            if dtype in {"REAL", "TIME"}:
                expression = f"int({value}) if integers & {1 << bit} and {value} is not None and {value}.is_integer() else {value}"
                if dtype == "TIME":
                    text = f"values[{positions[column + '_text']}]"
                    expression = f"{text} if {text} is not None else ({expression})"
            elif dtype == "BOOL":
                expression = f"None if {value} is None else bool({value})"
            else:
                expression = value
            if not required:
                source.append(f"    if present & {1 << bit}:")
            source.append(("    " if required else "        ") + f"{target}[{key!r}] = {expression}")
        source.append("    return row")
        namespace = {"json": json}
        exec(compile("\n".join(source), "<canonical typed mapper>", "exec"), namespace)
        return namespace["decode"]

    @classmethod
    def decode(cls, kind, names, values):
        return cls.decoder(kind, tuple(names))(values)

    @classmethod
    def items(cls, db, dataset, kind, account=None, condition="", parameters=(), order="first,record_key", limit=None):
        table = TABLES[kind][0]
        where, args = ["dataset=?"], [dataset]
        if account is not None:
            where.append("account=?")
            args.append(account)
        if condition:
            where.append("(" + condition + ")")
            args.extend(parameters)
        cursor = db.execute(f"SELECT * FROM {table} WHERE {' AND '.join(where)} ORDER BY {order}" + (f" LIMIT {int(limit)}" if limit is not None else ""), args)
        names = [value[0] for value in cursor.description]
        key_index = names.index("record_key")
        decode = cls.decoder(kind, tuple(names))
        return [(values[key_index], decode(values)) for values in cursor]

    @classmethod
    def rows(cls, db, dataset, kind, account=None, **kwargs):
        return [row for _, row in cls.items(db, dataset, kind, account, **kwargs)]

    @staticmethod
    def delete(db, dataset, kind, account, key):
        db.execute(f"DELETE FROM {TABLES[kind][0]} WHERE dataset=? AND account=? AND record_key=?", (dataset, account, key))

    @staticmethod
    def has_event(db, event):
        return db.execute("SELECT 1 FROM canonical_tokens WHERE dataset='local' AND event_id=? LIMIT 1", (event,)).fetchone() is not None

    @staticmethod
    def accounts(db):
        return list(db.execute("SELECT dataset,account FROM canonical_quota UNION SELECT dataset,account FROM canonical_tokens"))

    @classmethod
    def dirty_all(cls, db):
        db.executemany("INSERT OR IGNORE INTO delivery_dirty VALUES(?,?)", cls.accounts(db))

    @staticmethod
    def update_account(db, account, slot, label):
        for table, fields in TABLES.values():
            mask = sum(1 << bit for bit, field in enumerate(fields) if field[0] in {"account_slot", "account_label"})
            db.execute(f"UPDATE {table} SET account_slot=?,account_label=?,present=present|? WHERE account=?", (slot, label, mask, account))

    @staticmethod
    def rename_account(db, slot, label):
        for table, fields in TABLES.values():
            db.execute(f"INSERT OR IGNORE INTO delivery_dirty SELECT dataset,account FROM {table} WHERE account_slot=?", (slot,))
            mask = 1 << next(bit for bit, field in enumerate(fields) if field[0] == "account_label")
            db.execute(f"UPDATE {table} SET account_label=?,present=present|? WHERE account_slot=?", (label, mask, slot))
