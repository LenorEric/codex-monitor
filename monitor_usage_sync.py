#!/usr/bin/env python3

import hashlib
import heapq
import json
import math
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from monitor_common import MIN_DELTA_COST_PER_PERCENT_USD, RESET_TIME_JITTER_SECONDS, coerce_float, empty_cost_totals, empty_token_totals, parse_timestamp
from monitor_tokens import normalize_codex_model


SYNC_META_KEY = "sync"
COST_INTERVAL_TYPE = "costInterval"
MAX_SYNC_RECORD_BYTES = 256 * 1024
MAX_SYNC_STRING_LENGTH = 4096
MAX_SYNC_COLLECTION_ITEMS = 4096
MAX_SYNC_NESTING_DEPTH = 12


def canonical_json(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def content_hash(value) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def sync_meta(row: dict) -> dict:
    value = row.get(SYNC_META_KEY)
    return value if isinstance(value, dict) else {}


def is_cost_interval_row(row: dict) -> bool:
    return isinstance(row, dict) and row.get("recordType") == COST_INTERVAL_TYPE and row.get("window") in {"5h", "7d"}


def record_account_key(row: dict) -> str:
    return str(sync_meta(row).get("accountId") or f"local:{sync_meta(row).get('originMachineId') or 'unknown'}:{row.get('accountSlotId') or 'unknown'}")


def record_key(kind: str, row: dict) -> str:
    meta = sync_meta(row)
    if kind == "tokenLedger":
        if row.get("recordType") == "usage":
            return f"tokenLedger:usage:{record_account_key(row)}:{row.get('eventId')}"
        session = row.get("session") or {}
        return f"tokenLedger:sessionBaseline:{record_account_key(row)}:{session.get('sessionId')}"
    if meta.get("recordId"):
        return f"{kind}:{meta['recordId']}"
    return f"{kind}:{content_hash(row)}"


def syncable_record(kind: str, row: dict, machine_id: str) -> bool:
    meta = sync_meta(row)
    return meta.get("originMachineId") == machine_id and not meta.get("localOnly")


def _validate_sync_value(value, depth: int = 0) -> None:
    if depth > MAX_SYNC_NESTING_DEPTH:
        raise ValueError("Synchronized usage record nesting is too deep")
    if isinstance(value, str):
        if len(value) > MAX_SYNC_STRING_LENGTH:
            raise ValueError("Synchronized usage record string is too long")
    elif isinstance(value, dict):
        if len(value) > MAX_SYNC_COLLECTION_ITEMS:
            raise ValueError("Synchronized usage record object has too many fields")
        for key, item in value.items():
            if not isinstance(key, str) or len(key) > MAX_SYNC_STRING_LENGTH:
                raise ValueError("Synchronized usage record has an invalid field name")
            _validate_sync_value(item, depth + 1)
    elif isinstance(value, list):
        if len(value) > MAX_SYNC_COLLECTION_ITEMS:
            raise ValueError("Synchronized usage record list has too many items")
        for item in value:
            _validate_sync_value(item, depth + 1)
    elif isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Synchronized usage record contains a non-finite number")
    elif value is not None and not isinstance(value, (bool, int, float)):
        raise ValueError("Synchronized usage record contains an unsupported value")


def _validate_token_ledger_row(row: dict) -> None:
    if row.get("schemaVersion") != 2:
        raise ValueError("Invalid synchronized token ledger schema")
    meta = sync_meta(row)
    if meta.get("version") != 1 or not isinstance(meta.get("originMachineId"), str) or not meta["originMachineId"] or not isinstance(meta.get("accountId"), str) or not meta["accountId"].startswith("v3:"):
        raise ValueError("Invalid synchronized token ledger provenance")
    if meta.get("localOnly") not in {None, False}:
        raise ValueError("Local-only token ledger records cannot be synchronized")
    from monitor_token_ledger import canonical_token_ledger_rows
    canonical_token_ledger_rows([row])
    if (
        not isinstance(row.get("eventId"), str) or not row["eventId"] or not isinstance(row.get("sessionId"), str) or not row["sessionId"] or parse_timestamp(row.get("occurredAt")) is None
        or not isinstance(row.get("rawModel"), str) or not row["rawModel"] or not isinstance(row.get("billingModel"), str) or not row["billingModel"] or row.get("serviceTier") not in {"default", "fast"}
    ):
        raise ValueError("Invalid synchronized token usage record")
    _validate_totals(row.get("tokens"), empty_token_totals(), integral=True, label="token")
    _validate_totals(row.get("cost"), empty_cost_totals(), integral=False, label="cost")


def _validate_totals(value: object, template: dict, integral: bool, label: str) -> None:
    if not isinstance(value, dict) or set(value) != set(template):
        raise ValueError(f"Invalid synchronized {label} totals")
    for item in value.values():
        if isinstance(item, bool) or not isinstance(item, int if integral else (int, float)) or item < 0 or isinstance(item, float) and not math.isfinite(item):
            raise ValueError(f"Invalid synchronized {label} totals")


def _validate_quota_row(row: dict) -> None:
    meta, windows = sync_meta(row), row.get("windows")
    if parse_timestamp(row.get("checkedAt")) is None or not isinstance(windows, dict) or not windows or not set(windows).issubset({"5h", "7d"}):
        raise ValueError("Invalid synchronized quota record")
    if meta.get("version") != 1 or (not all(isinstance(meta.get(key), str) and meta[key] for key in ("originMachineId", "accountId", "recordId")) or not meta["accountId"].startswith("v3:")) or meta.get("localOnly") not in {None, False}:
        raise ValueError("Invalid synchronized quota provenance")
    for window in windows.values():
        used = window.get("usedPercent") if isinstance(window, dict) else None
        if (
            isinstance(used, bool) or not isinstance(used, (int, float)) or not math.isfinite(used) or not 0 <= used <= 100
            or window.get("resetAt") is not None and parse_timestamp(window.get("resetAt")) is None or not isinstance(window.get("plan"), str) or not window["plan"]
        ):
            raise ValueError("Invalid synchronized quota window")


def validate_sync_operation(operation: dict) -> None:
    if not isinstance(operation, dict) or not isinstance(operation.get("action"), str) or operation["action"] not in {"delete", "upsert"} or not isinstance(operation.get("key"), str) or not operation["key"] or len(operation["key"]) > 512:
        raise ValueError("Invalid synchronized usage operation")
    if operation["action"] == "delete":
        return
    record = operation.get("record")
    if not isinstance(record, dict) or record.get("kind") not in {"quota", "tokenLedger"} or not isinstance(record.get("row"), dict):
        raise ValueError("Invalid synchronized usage record")
    row = record["row"]
    if record["kind"] == "tokenLedger":
        _validate_token_ledger_row(row)
    else:
        _validate_quota_row(row)
    _validate_sync_value(record)
    if len(canonical_json(record)) > MAX_SYNC_RECORD_BYTES:
        raise ValueError("Synchronized usage record is too large")
    if operation["key"] != record_key(record["kind"], record["row"]):
        raise ValueError("Synchronized usage record key does not match its content")


def add_record_provenance(kind: str, row: dict, machine_id: str, account_id: str, local_only: bool = False) -> dict:
    meta = sync_meta(row)
    if meta.get("originMachineId") and meta.get("accountId") and (kind == "tokenLedger" or meta.get("recordId")):
        return row
    if kind == "quota":
        identity = f"quota:{account_id}:{row.get('checkedAt')}"
    elif kind == "tokenLedger" and row.get("recordType") == "usage":
        identity = f"tokenLedger:usage:{account_id}:{row.get('eventId')}"
    elif kind == "tokenLedger":
        identity = f"tokenLedger:sessionBaseline:{account_id}:{(row.get('session') or {}).get('sessionId')}"
    else:
        raise ValueError(f"Unsupported synchronized usage kind: {kind}")
    meta = {"version": 1, "originMachineId": machine_id, "accountId": account_id}
    if kind == "quota":
        meta["recordId"] = hashlib.sha256(identity.encode()).hexdigest()
    if local_only:
        meta["localOnly"] = True
    return row | {SYNC_META_KEY: meta}


def merge_quota_rows(rows: list[dict]) -> list[dict]:
    merged = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get("checkedAt"):
            continue
        key = (record_account_key(row), row["checkedAt"], record_key("quota", row) if row.get("quotaRunVersion") == 1 else None)
        if key in merged:
            previous = merged[key]
            row = previous | row | {"windows": (previous.get("windows") or {}) | (row.get("windows") or {})}
            if sync_meta(previous).get("originMachineId") and not sync_meta(row).get("originMachineId"):
                row[SYNC_META_KEY] = previous[SYNC_META_KEY]
        merged[key] = row
    return sorted(merged.values(), key=lambda row: (parse_timestamp(row.get("checkedAt")) or 0, row.get("checkedAt") or "", record_account_key(row)))


def _ledger_identity(row: dict) -> tuple | None:
    record_type = row.get("recordType")
    if record_type == "usage" and row.get("eventId"):
        return record_type, record_account_key(row), str(row["eventId"])
    return None


def canonical_ledger_row(row: dict) -> dict:
    from monitor_token_ledger import canonical_token_ledger_rows
    canonical_token_ledger_rows([row])
    return row


def merge_token_ledger_rows(rows: list[dict]) -> list[dict]:
    """Keep the first copy of each event; callers supply local rows before peers."""
    merged = {}
    for source in rows:
        if (row := canonical_ledger_row(source)) is None:
            continue
        if not isinstance(row, dict) or (identity := _ledger_identity(row)) is None:
            continue
        merged.setdefault(identity, row)
    def sort_key(row: dict) -> tuple:
        if row.get("recordType") == "usage":
            return 2, parse_timestamp(row.get("occurredAt")) or 0, str(row.get("eventId") or "")
        return 0, 0, ""
    return sorted(merged.values(), key=sort_key)


def _cycle_key(row: dict) -> tuple:
    meta = sync_meta(row)
    reset_at = parse_timestamp(row.get("resetAt"))
    return record_account_key(row), row.get("window"), row.get("plan") or "unknown", coerce_float(row.get("planMultiplier")) or 1.0, reset_at, None if reset_at is not None else meta.get("originMachineId") or "unknown"


def _normalized_cost_interval(row: dict, index: int) -> dict | None:
    if not is_cost_interval_row(row):
        return None
    start, end = coerce_float(row.get("startPercent")), coerce_float(row.get("endPercent"))
    if start is None or end is None or end <= start:
        return None
    model_rates = {}
    for model, cost in (row.get("modelCostsUsd") or {}).items():
        if (value := coerce_float(cost)) is not None and value > 0:
            normalized = normalize_codex_model(model)
            model_rates[normalized] = model_rates.get(normalized, 0.0) + value / (end - start)
    return {"index": index, "row": row, "start": start, "end": end, "modelRates": model_rates, "totalRate": sum(model_rates.values()), "cycleKey": _cycle_key(row)}


def _cost_interval_groups(rows: list[dict]) -> list[list[dict]]:
    without_reset, with_reset = {}, {}
    for index, row in enumerate(rows):
        if (interval := _normalized_cost_interval(row, index)) is None:
            continue
        key = interval["cycleKey"]
        if key[4] is None:
            without_reset.setdefault(key[:4] + (key[5],), []).append(interval)
        else:
            with_reset.setdefault(key[:4], []).append(interval)
    groups = list(without_reset.values())
    for intervals in with_reset.values():
        anchor, group = None, None
        for interval in sorted(intervals, key=lambda item: (item["cycleKey"][4], item["index"])):
            if anchor is None or interval["cycleKey"][4] - anchor > RESET_TIME_JITTER_SECONDS:
                anchor, group = interval["cycleKey"][4], []
                groups.append(group)
            group.append(interval)
    for group in groups:
        group.sort(key=lambda item: item["index"])
    return sorted(groups, key=lambda group: group[0]["index"])


def aggregate_cost_intervals(rows: list[dict]) -> list[dict]:
    aggregated = []
    for intervals in _cost_interval_groups(rows):
        starts, ends, observed = {}, {}, {}
        for interval in intervals:
            starts.setdefault(interval["start"], []).append(interval)
            ends.setdefault(interval["end"], []).append(interval)
            row = interval["row"]
            if row.get("startedAt") and (interval["start"] not in observed or row["startedAt"] < observed[interval["start"]]):
                observed[interval["start"]] = row["startedAt"]
            if row.get("checkedAt") and (interval["end"] not in observed or row["checkedAt"] < observed[interval["end"]]):
                observed[interval["end"]] = row["checkedAt"]
        boundaries = sorted(set(starts) | set(ends))
        active, active_rows, active_checked, model_rates, model_counts, total_rate = set(), [], [], {}, {}, 0.0
        for start, end in zip(boundaries, boundaries[1:]):
            for interval in ends.get(start, ()):
                active.discard(interval["index"])
                total_rate -= interval["totalRate"]
                for model, rate in interval["modelRates"].items():
                    if model_counts[model] == 1:
                        model_counts.pop(model)
                        model_rates.pop(model)
                    else:
                        model_counts[model] -= 1
                        model_rates[model] -= rate
            for interval in starts.get(start, ()):
                active.add(interval["index"])
                total_rate += interval["totalRate"]
                heapq.heappush(active_rows, (interval["index"], interval))
                if interval["row"].get("checkedAt"):
                    heapq.heappush(active_checked, (interval["row"]["checkedAt"], interval["index"]))
                for model, rate in interval["modelRates"].items():
                    model_counts[model] = model_counts.get(model, 0) + 1
                    model_rates[model] = model_rates.get(model, 0.0) + rate
            if end <= start or not active:
                continue
            while active_rows and active_rows[0][0] not in active:
                heapq.heappop(active_rows)
            while active_checked and active_checked[0][1] not in active:
                heapq.heappop(active_checked)
            delta_percent = end - start
            if not model_rates or total_rate < MIN_DELTA_COST_PER_PERCENT_USD:
                continue
            model_costs = {model: rate * delta_percent for model, rate in model_rates.items() if rate > 0}
            total_cost = sum(model_costs.values())
            checked_at = observed.get(end) or (active_checked[0][0] if active_checked else None)
            representative, used_percent = active_rows[0][1]["row"], 0.0
            models = sorted(model_costs)
            for index, model in enumerate(models):
                model_cost = model_costs[model]
                model_percent = delta_percent - used_percent if index == len(models) - 1 else delta_percent * model_cost / total_cost
                aggregated.append({
                    "checkedAt": checked_at, "window": representative["window"], "model": model, "accountSlotId": representative.get("accountSlotId"), "accountLabel": representative.get("accountLabel"),
                    "usageAccountId": intervals[0]["cycleKey"][0], "deltaPercent": round(model_percent, 8), "deltaCostUsd": round(model_cost, 8), "costPercentRatio": round(total_cost / delta_percent, 8),
                })
                used_percent += model_percent
    return sorted(aggregated, key=lambda row: (row.get("checkedAt") or "", row.get("window") or "", row.get("model") or ""))


def initialize_v4_cache(path: Path) -> None:
    """Create the local v4 index without altering canonical histories."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    import sqlite3
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        db.execute("CREATE TABLE IF NOT EXISTS remote_days (machine_id TEXT NOT NULL, day TEXT NOT NULL, logical_hash TEXT NOT NULL, layout_json TEXT NOT NULL, PRIMARY KEY(machine_id, day))")
        db.execute("CREATE TABLE IF NOT EXISTS remote_origins (machine_id TEXT PRIMARY KEY)")
        db.execute("CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL)")


def v4_period(timestamp: float) -> str:
    observed = datetime.fromtimestamp(timestamp, timezone.utc)
    return observed.strftime("%Y-%m-%dT%H:") + ("30" if observed.minute >= 30 else "00") + "Z"


def v4_record_envelope(machine_id: str, key: str, record: dict, collection_period: str | None = None) -> dict:
    row = record.get("row") or {}
    event_at = row.get("checkedAt") if record["kind"] == "quota" else row.get("occurredAt") or (row.get("session") or {}).get("updatedAt") or (row.get("session") or {}).get("startedAt")
    return {"kind": record["kind"], "schemaVersion": row.get("schemaVersion", record.get("schemaVersion", 1)), "recordKey": key, "sourceMachineId": machine_id, "collectionPeriod": collection_period, "eventAt": event_at or record.get("eventAt"), "contentSha256": content_hash(record), "record": record}


def validate_v4_envelope(entry: dict, machine_id: str) -> None:
    """Downloaded records require field presence and types, without content validation."""
    require_usage_fields(entry, sourceMachineId=str, recordKey=str, kind=str, schemaVersion=int,
        record=dict, collectionPeriod=str, eventAt=(str, type(None)))
    record = entry["record"]
    require_usage_fields(record, kind=str)
    if record["kind"] not in {"quota", "tokenLedger"}:
        return
    require_usage_fields(record, row=dict)
    row = record["row"]
    require_usage_fields(row, sync=dict)
    require_usage_fields(row["sync"], version=int, originMachineId=str, accountId=str)
    if record["kind"] == "quota":
        require_usage_fields(row["sync"], recordId=str)
        require_usage_fields(row, checkedAt=str, windows=dict)
        for window in row["windows"].values():
            require_usage_fields(window, usedPercent=(int, float), plan=str)
            if "resetAt" in window:
                require_usage_fields(window, resetAt=(str, int, float, type(None)))
    else:
        require_usage_fields(row, schemaVersion=int, eventId=str, sessionId=str, occurredAt=str,
            rawModel=str, billingModel=str, serviceTier=str, tokens=dict, cost=dict)
        require_usage_fields(row["tokens"], **{key: int for key in empty_token_totals()})
        require_usage_fields(row["cost"], **{key: (int, float) for key in empty_cost_totals()})


def require_usage_fields(value, **fields) -> None:
    if not isinstance(value, dict) or any(key not in value or type(value[key]) not in (types if isinstance(types, tuple) else (types,)) for key, types in fields.items()):
        raise ValueError("Missing usage field or incorrect field type")


def v4_logical_hash(entries: list[dict]) -> str:
    return content_hash([[entry["recordKey"], content_hash(entry["record"])] for entry in sorted(entries, key=lambda item: item["recordKey"])])


class NormalizedRecords:
    """Common preprocessing and transport contract, independent of stored histories."""
    def normalize_new_quota(self, row):
        if sync_meta(row).get("originMachineId", self.machine_id) != self.machine_id:
            raise ValueError("Foreign records in local history require data migration")
        from monitor_history import normalize_quota_history_row
        row = normalize_quota_history_row(row)
        if row is None:
            return None
        row = add_record_provenance("quota", row, self.machine_id, self.account_id_resolver(row.get("accountSlotId")))
        if not str(sync_meta(row).get("accountId")).startswith("v3:"):
            raise ValueError("Quota identity requires data migration")
        return row

    def normalize_new_ledger(self, row):
        if sync_meta(row).get("originMachineId", self.machine_id) != self.machine_id:
            raise ValueError("Foreign records in local history require data migration")
        if (row := canonical_ledger_row(row)) is None:
            return None
        source = row.get("session") or row
        row = add_record_provenance("tokenLedger", row, self.machine_id, self.account_id_resolver(source.get("accountSlotId")))
        if not str(sync_meta(row).get("accountId")).startswith("v3:"):
            raise ValueError("Ledger identity requires data migration")
        return row

    @staticmethod
    def _transport_record(kind: str, row: dict) -> dict:
        if kind == "quota" and row.get("quotaRunVersion") == 1:
            raise ValueError("Local quota runs are not cloud records")
        if kind == "tokenLedger":
            row = canonical_ledger_row(row)
        transport = {key: value for key, value in row.items() if key not in {"accountSlotId", "accountLabel"}}
        return {"kind": kind, "row": transport}


def UsageDataStore(*args, **kwargs):
    from monitor_canonical import CanonicalDataStore
    return CanonicalDataStore(*args, **kwargs)
