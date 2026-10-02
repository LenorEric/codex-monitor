#!/usr/bin/env python3

import hashlib
import heapq
import json
import math
import os
import sqlite3
import tempfile
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from monitor_common import MIN_DELTA_COST_PER_PERCENT_USD, RESET_TIME_JITTER_SECONDS, coerce_float, empty_cost_totals, empty_token_totals, parse_timestamp
from monitor_history import compact_quota_history_rows
from monitor_tokens import normalize_codex_model


SYNC_META_KEY = "sync"
COST_INTERVAL_TYPE = "costInterval"
MAX_SYNC_RECORD_BYTES = 256 * 1024
MAX_SYNC_STRING_LENGTH = 4096
MAX_SYNC_COLLECTION_ITEMS = 4096
MAX_SYNC_NESTING_DEPTH = 12
CACHE_VERSION = 3
CACHE_LAYOUT = "origin-pack-shards-v1"


def canonical_json(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def content_hash(value) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def default_usage_sync_cache_path(data_path: Path) -> Path:
    data_path = Path(data_path)
    return data_path.with_name("usage_monitor_sync_cache.json") if data_path.name in {"usage_monitor_history.jsonl", "usage_monitor_quota_history.jsonl", "usage_monitor_token_ledger.jsonl", "usage_monitor_quota_readings.jsonl", "usage_monitor_token_events.jsonl"} else data_path.with_suffix(".sync-cache.json")


def sync_meta(row: dict) -> dict:
    value = row.get(SYNC_META_KEY)
    return value if isinstance(value, dict) else {}


def is_cost_interval_row(row: dict) -> bool:
    return isinstance(row, dict) and row.get("recordType") == COST_INTERVAL_TYPE and row.get("window") in {"5h", "7d"}


def record_account_key(row: dict) -> str:
    return str(sync_meta(row).get("accountId") or f"local:{sync_meta(row).get('originMachineId') or 'legacy'}:{row.get('accountSlotId') or 'unknown'}")


def record_key(kind: str, row: dict) -> str:
    meta = sync_meta(row)
    if kind == "tokenLedger":
        if row.get("recordType") == "usage":
            return f"tokenLedger:usage:{record_account_key(row)}:{row.get('eventId')}"
        session = row.get("session") or {}
        return f"tokenLedger:legacyBaseline:{record_account_key(row)}:{session.get('sessionId')}"
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
    if row.get("schemaVersion") not in {1, 2}:
        raise ValueError("Invalid synchronized token ledger schema")
    meta = sync_meta(row)
    if meta.get("version") != 1 or not isinstance(meta.get("originMachineId"), str) or not meta["originMachineId"] or not isinstance(meta.get("accountId"), str) or not meta["accountId"]:
        raise ValueError("Invalid synchronized token ledger provenance")
    if meta.get("localOnly") not in {None, False}:
        raise ValueError("Local-only token ledger records cannot be synchronized")
    if row.get("recordType") == "usage":
        if (
            not isinstance(row.get("eventId"), str) or not row["eventId"] or not isinstance(row.get("sessionId"), str) or not row["sessionId"] or parse_timestamp(row.get("occurredAt")) is None
            or not isinstance(row.get("rawModel"), str) or not row["rawModel"] or not isinstance(row.get("billingModel"), str) or not row["billingModel"] or row.get("serviceTier") not in {"default", "fast"}
        ):
            raise ValueError("Invalid synchronized token usage record")
        _validate_totals(row.get("tokens"), empty_token_totals(), integral=True, label="token")
        _validate_totals(row.get("cost"), empty_cost_totals(), integral=False, label="cost")
    elif row.get("recordType") == "legacyBaseline":
        session = row.get("session")
        if not isinstance(session, dict) or not isinstance(session.get("sessionId"), str) or not session["sessionId"]:
            raise ValueError("Invalid synchronized token baseline record")
        _validate_totals(session.get("tokens"), empty_token_totals(), integral=True, label="token")
        _validate_totals(session.get("cost"), empty_cost_totals(), integral=False, label="cost")
        if any(session.get(key) is not None and parse_timestamp(session.get(key)) is None for key in ("startedAt", "updatedAt")) or not isinstance(session.get("byModel"), dict):
            raise ValueError("Invalid synchronized token baseline session")
        for model, value in session["byModel"].items():
            if not isinstance(model, str) or not model or not isinstance(value, dict):
                raise ValueError("Invalid synchronized token baseline model")
            _validate_totals(value.get("tokens"), empty_token_totals(), integral=True, label="token")
            _validate_totals(value.get("cost"), empty_cost_totals(), integral=False, label="cost")
            if "fastTokens" in value:
                _validate_totals(value["fastTokens"], empty_token_totals(), integral=True, label="token")
    else:
        raise ValueError("Invalid synchronized token ledger record")


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
    if meta.get("version") != 1 or not all(isinstance(meta.get(key), str) and meta[key] for key in ("originMachineId", "accountId", "recordId")) or meta.get("localOnly") not in {None, False}:
        raise ValueError("Invalid synchronized quota provenance")
    for window in windows.values():
        used = window.get("usedPercent") if isinstance(window, dict) else None
        if (
            isinstance(used, bool) or not isinstance(used, (int, float)) or not math.isfinite(used) or not 0 <= used <= 100
            or window.get("resetAt") is not None and parse_timestamp(window.get("resetAt")) is None or not isinstance(window.get("plan"), str) or not window["plan"]
        ):
            raise ValueError("Invalid synchronized quota window")
    compaction = row.get("compaction")
    if compaction is not None and (
        not isinstance(compaction, dict) or parse_timestamp(compaction.get("continuousFrom")) is None or isinstance(compaction.get("omittedSamples"), bool)
        or not isinstance(compaction.get("omittedSamples"), int) or compaction["omittedSamples"] <= 0
    ):
        raise ValueError("Invalid synchronized quota compaction")


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
        identity = f"tokenLedger:legacyBaseline:{account_id}:{(row.get('session') or {}).get('sessionId')}"
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
        key = (record_account_key(row), row["checkedAt"])
        if key in merged:
            previous = merged[key]
            row = previous | row | {"windows": (previous.get("windows") or {}) | (row.get("windows") or {})}
            if previous.get("compaction") and not row.get("compaction"):
                row["compaction"] = previous["compaction"]
            if sync_meta(previous).get("originMachineId") and not sync_meta(row).get("originMachineId"):
                row[SYNC_META_KEY] = previous[SYNC_META_KEY]
        merged[key] = row
    return sorted(merged.values(), key=lambda row: (parse_timestamp(row.get("checkedAt")) or 0, row.get("checkedAt") or "", record_account_key(row)))


def _ledger_identity(row: dict) -> tuple | None:
    record_type = row.get("recordType")
    if record_type == "usage" and row.get("eventId"):
        return record_type, record_account_key(row), str(row["eventId"])
    session = row.get("session") or {}
    if record_type == "legacyBaseline" and session.get("sessionId"):
        return record_type, record_account_key(row), str(session["sessionId"])
    return None


def _ledger_content(row: dict) -> dict:
    value = {key: item for key, item in row.items() if key not in {"accountSlotId", "accountLabel", SYNC_META_KEY}}
    if row.get("recordType") == "legacyBaseline" and isinstance(value.get("session"), dict):
        value["session"] = {key: item for key, item in value["session"].items() if key not in {"accountSlotId", "accountLabel", SYNC_META_KEY}}
    return value


def canonical_ledger_row(row: dict, preserve_legacy_highwater: bool = False) -> dict | None:
    if row.get("recordType") == "priceEpoch":
        return None
    if preserve_legacy_highwater:
        from monitor_token_ledger import canonical_token_ledger_rows
        output = canonical_token_ledger_rows([row])[0]
    else:
        output = {key: value for key, value in row.items() if key not in {"pricingId", "pricingBasis", "sourceTotals"}} | {"schemaVersion": 2}
    meta = sync_meta(output)
    if meta:
        output[SYNC_META_KEY] = {key: value for key, value in meta.items() if key not in {"recordId", "localOnly"} or key == "localOnly" and value}
    return output


def merge_token_ledger_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    merged, conflicts = {}, []
    for source in rows:
        if (row := canonical_ledger_row(source)) is None:
            continue
        if not isinstance(row, dict) or (identity := _ledger_identity(row)) is None:
            continue
        if identity in merged and _ledger_content(merged[identity]) != _ledger_content(row):
            conflicts.append({"recordType": identity[0], "accountId": identity[1] if len(identity) > 2 else None, "recordId": identity[-1]})
            if content_hash(_ledger_content(row)) <= content_hash(_ledger_content(merged[identity])):
                continue
        merged[identity] = row
    def sort_key(row: dict) -> tuple:
        if row.get("recordType") == "usage":
            return 2, parse_timestamp(row.get("occurredAt")) or 0, str(row.get("eventId") or "")
        if row.get("recordType") == "legacyBaseline":
            return 1, 0, str((row.get("session") or {}).get("sessionId") or "")
        return 0, 0, ""
    return sorted(merged.values(), key=sort_key), conflicts


def quota_sync_boundary_rows(rows: list[dict], machine_id: str) -> list[dict]:
    syncable = [row for row in rows if syncable_record("quota", row, machine_id)]
    compacted = compact_quota_history_rows(syncable)
    account_order = list(dict.fromkeys(record_account_key(row) for row in syncable))
    return [row for account_id in account_order for row in compacted if record_account_key(row) == account_id]


def _cycle_key(row: dict) -> tuple:
    meta = sync_meta(row)
    reset_at = parse_timestamp(row.get("resetAt"))
    return record_account_key(row), row.get("window"), row.get("plan") or "unknown", coerce_float(row.get("planMultiplier")) or 1.0, reset_at, None if reset_at is not None else meta.get("originMachineId") or "legacy"


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


def active_records(quota: list[dict], ledger: list[dict], machine_id: str, account_id_resolver, necessary_only: bool = True) -> dict[str, dict]:
    records = {}
    for row in quota_sync_boundary_rows(quota, machine_id) if necessary_only else quota:
        if syncable_record("quota", row, machine_id):
            records[record_key("quota", row)] = {"kind": "quota", "row": {key: value for key, value in row.items() if key not in {"accountSlotId", "accountLabel"}}}
    for source in ledger:
        legacy_schema = source.get("schemaVersion") == 1
        if (source := canonical_ledger_row(source)) is None:
            continue
        session = source.get("session") or {}
        account_id = account_id_resolver(source.get("accountSlotId") or session.get("accountSlotId"))
        if legacy_schema and sync_meta(source).get("accountId") != account_id:
            source = {key: value for key, value in source.items() if key != SYNC_META_KEY}
        row = add_record_provenance("tokenLedger", source, machine_id, account_id)
        if not syncable_record("tokenLedger", row, machine_id):
            continue
        transport = {key: value for key, value in row.items() if key not in {"accountSlotId", "accountLabel"}}
        if source.get("recordType") == "legacyBaseline" and isinstance(transport.get("session"), dict):
            transport["session"] = {key: value for key, value in transport["session"].items() if key not in {"accountSlotId", "accountLabel"}}
        records[record_key("tokenLedger", row)] = {"kind": "tokenLedger", "row": transport}
    return records


def initialize_v4_cache(path: Path) -> None:
    """Create the local v4 index without altering the original history or legacy cache."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE IF NOT EXISTS local_records (record_key TEXT PRIMARY KEY, period TEXT NOT NULL, content_hash TEXT NOT NULL, record_json TEXT NOT NULL)")
        db.execute("CREATE INDEX IF NOT EXISTS local_records_period ON local_records(period)")
        db.execute("CREATE TABLE IF NOT EXISTS remote_days (machine_id TEXT NOT NULL, day TEXT NOT NULL, logical_hash TEXT NOT NULL, layout_json TEXT NOT NULL, PRIMARY KEY(machine_id, day))")
        db.execute("CREATE TABLE IF NOT EXISTS remote_records (machine_id TEXT NOT NULL, day TEXT NOT NULL, part_id TEXT NOT NULL, record_key TEXT NOT NULL, content_hash TEXT NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY(machine_id, record_key))")
        db.execute("CREATE INDEX IF NOT EXISTS remote_records_day ON remote_records(machine_id, day, part_id)")
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
    if not isinstance(entry, dict) or entry.get("sourceMachineId") != machine_id or not isinstance(entry.get("recordKey"), str) or not entry["recordKey"] or len(entry["recordKey"]) > 512 or not isinstance(entry.get("kind"), str) or not entry["kind"] or not isinstance(entry.get("schemaVersion"), int) or isinstance(entry["schemaVersion"], bool) or entry["schemaVersion"] < 1 or not isinstance(entry.get("record"), dict) or entry["record"].get("kind") != entry["kind"] or entry.get("contentSha256") != content_hash(entry["record"]) or not isinstance(entry.get("collectionPeriod"), str) or parse_timestamp(entry["collectionPeriod"]) is None or v4_period(parse_timestamp(entry["collectionPeriod"])) != entry["collectionPeriod"]:
        raise ValueError("Invalid usage v4 record envelope")
    if entry["kind"] in {"quota", "tokenLedger"}:
        validate_sync_operation({"action": "upsert", "key": entry["recordKey"], "record": entry["record"]})
        if sync_meta(entry["record"]["row"]).get("originMachineId") != machine_id or entry["schemaVersion"] != entry["record"]["row"].get("schemaVersion", 1) or entry.get("eventAt") != v4_record_envelope(machine_id, entry["recordKey"], entry["record"])["eventAt"]:
            raise ValueError("Usage v4 record owner or schema does not match its envelope")
    else:
        _validate_sync_value(entry["record"])
        if len(canonical_json(entry["record"])) > MAX_SYNC_RECORD_BYTES:
            raise ValueError("Unknown synchronized usage record is too large")
        if entry.get("eventAt") is not None and (not isinstance(entry["eventAt"], str) or parse_timestamp(entry["eventAt"]) is None):
            raise ValueError("Unknown synchronized usage record has an invalid event time")


def v4_logical_hash(entries: list[dict]) -> str:
    return content_hash([[entry["recordKey"], content_hash(entry["record"])] for entry in sorted(entries, key=lambda item: item["recordKey"])])




class UsageDataStore:
    def __init__(self, quota_path: Path, token_ledger_path: Path, machine_id: str, account_id_resolver, lock, account_mapper=None, cache_path: Path | None = None, account_revision_resolver=None):
        self.quota_path, self.token_ledger_path = Path(quota_path), Path(token_ledger_path)
        self.cache_path = Path(cache_path) if cache_path is not None else default_usage_sync_cache_path(self.quota_path)
        self.v4_cache_path = self.cache_path.with_name(f"{self.cache_path.stem}-v4.sqlite3")
        self.machine_id, self.account_id_resolver, self.lock, self.account_mapper = machine_id, account_id_resolver, lock, account_mapper
        self.account_revision_resolver = account_revision_resolver
        self.conflicts = []
        self.needs_remote_rebuild = not self.cache_path.exists()
        self._local_datasets_cache = None
        self._merged_datasets_cache = None
        self._account_revision = None
        self._cache_repair_needed = False
        self._v4_source_stats = None
        from monitor_processing import ProcessingIndex
        initialize_v4_cache(self.v4_cache_path)
        self.index = ProcessingIndex(self.v4_cache_path)
        self._legacy_records = None

    def normalize_new_quota(self, row):
        from monitor_history import normalize_quota_history_row
        row = normalize_quota_history_row(row)
        return add_record_provenance("quota", row, self.machine_id, self.account_id_resolver(row.get("accountSlotId"))) if row is not None else None

    def normalize_new_ledger(self, row):
        if (row := canonical_ledger_row(row, preserve_legacy_highwater=True)) is None:
            return None
        source = row.get("session") or row
        return add_record_provenance("tokenLedger", row, self.machine_id, self.account_id_resolver(source.get("accountSlotId")))

    def ingest_local(self):
        with self.lock:
            return self._ingest_local()

    def retain_quota(self, days):
        if days is None or days <= 0:
            return False
        with self.lock:
            self._ingest_local()
            source, cutoff = str(self.quota_path.resolve()), time.time() - days * 86400
            with self.index.transaction() as db:
                if not db.execute("SELECT 1 FROM processing_facts WHERE source=? AND kind='quota' AND event_at<? LIMIT 1", (source, cutoff)).fetchone():
                    return False
                descriptor, name = tempfile.mkstemp(prefix=f".{self.quota_path.name}.", suffix=".retention", dir=self.quota_path.parent)
                try:
                    with os.fdopen(descriptor, "wb") as output:
                        for (data,) in db.execute("SELECT data FROM processing_facts WHERE source=? AND kind='quota' AND (event_at IS NULL OR event_at>=?) ORDER BY position", (source, cutoff)):
                            output.write(data.encode() + b"\n")
                        output.flush()
                        os.fsync(output.fileno())
                    os.replace(name, self.quota_path)
                finally:
                    Path(name).unlink(missing_ok=True)
            self._ingest_local()
            return True

    def _ingest_local(self):
        with self.index.transaction() as db:
            normalized = self.index.get(db, "canonicalNormalized", False)
        if not normalized:
            self._normalize_local()
            with self.index.transaction() as db:
                self.index.put(db, "canonicalNormalized", True)
        self.index.audit()
        changed = self.index.refresh(self.quota_path, "quota", self.normalize_new_quota)
        changed = self.index.refresh(self.token_ledger_path, "tokenLedger", self.normalize_new_ledger) or changed
        if changed:
            self._local_datasets_cache = self._merged_datasets_cache = None
        return changed

    def account_datasets(self, view, account):
        self.ingest_local()
        quota = self.index.rows(self.quota_path, "quota", account=account)
        ledger = self.index.rows(self.token_ledger_path, "tokenLedger", account=account)
        if view == "local":
            return quota, ledger
        with self.index.transaction() as db:
            for kind, data in db.execute("SELECT kind,data FROM processing_facts WHERE source LIKE 'peer:%' AND account=? ORDER BY source,position", (account,)):
                if kind == "quota":
                    quota.append(json.loads(data))
                elif kind == "tokenLedger":
                    ledger.append(json.loads(data))
        ledger, _ = merge_token_ledger_rows(ledger)
        return merge_quota_rows(quota), ledger

    def refresh_peer_partition(self, machine_id, day, force=False):
        source = f"peer:{machine_id}:{day}"
        with self.index.transaction() as db, self.index.defer_sessions(db):
            manifest = db.execute("SELECT logical_hash FROM remote_days WHERE machine_id=? AND day=?", (machine_id, day)).fetchone()
            revision = [manifest[0] if manifest else None, self.account_revision_resolver() if self.account_revision_resolver else None]
            if not force and self.index.get(db, f"peerRevision:{source}", "uninitialized") == revision:
                return
            legacy = f"peer:{machine_id}:legacy"
            for (account,) in list(db.execute("SELECT DISTINCT account FROM processing_facts WHERE source=?", (legacy,))):
                self.index.dirty(db, account, "peer update", ("merged",))
            legacy_keys = [row[0] for row in db.execute("SELECT record_key FROM processing_facts WHERE source=? AND kind='tokenLedger'", (legacy,))]
            db.execute("DELETE FROM processing_facts WHERE source=?", (legacy,))
            previous_revision = self.index.get(db, f"peerRevision:{source}")
            remap = force or previous_revision is None or previous_revision[1] != revision[1]
            def remove(position):
                previous = db.execute("SELECT kind,record_key,account FROM processing_facts WHERE source=? AND position=?", (source, position)).fetchone()
                if previous:
                    db.execute("DELETE FROM processing_facts WHERE source=? AND position=?", (source, position))
                    self.index.dirty(db, previous[2], "peer update", ("merged",))
                    if previous[0] == "tokenLedger":
                        self.index._effective(db, previous[1])
            for key, position in list(db.execute("SELECT p.record_key,p.position FROM processing_peer_rows p WHERE p.source=? AND NOT EXISTS "
                "(SELECT 1 FROM remote_records r WHERE r.machine_id=? AND r.day=? AND r.record_key=p.record_key)", (source, machine_id, day))):
                remove(position)
                db.execute("DELETE FROM processing_peer_rows WHERE source=? AND record_key=?", (source, key))
            position = db.execute("SELECT COALESCE(MAX(position),-1)+1 FROM processing_peer_rows WHERE source=?", (source,)).fetchone()[0]
            for key, digest, data in db.execute("SELECT r.record_key,r.content_hash,r.record_json FROM remote_records r LEFT JOIN processing_peer_rows p ON p.source=? AND p.record_key=r.record_key "
                "WHERE r.machine_id=? AND r.day=? AND (? OR p.record_key IS NULL OR p.digest<>r.content_hash) ORDER BY r.record_key", (source, machine_id, day, remap)):
                previous = db.execute("SELECT position FROM processing_peer_rows WHERE source=? AND record_key=?", (source, key)).fetchone()
                target = previous[0] if previous else position
                record = json.loads(data)["record"]
                row = self._map_account(record["kind"], record["row"]) if record.get("kind") in {"quota", "tokenLedger"} else None
                if previous:
                    remove(target)
                if row is not None:
                    self.index.fact(db, source, target, record["kind"], row)
                db.execute("INSERT OR REPLACE INTO processing_peer_rows VALUES(?,?,?,?)", (source, key, target, digest))
                if not previous:
                    position += 1
            for key in legacy_keys:
                self.index._effective(db, key)
            db.execute("DELETE FROM processing_ledger_sessions WHERE source=?", (source,))
            db.execute("DELETE FROM processing_ledger_sessions WHERE source=?", (legacy,))
            self.index.put(db, f"peerRevision:{source}", revision)
            self.index.put(db, "factRevision", self.index.get(db, "factRevision", 0) + 1)
        self._merged_datasets_cache = None

    def initialize_processing(self):
        self.ingest_local()
        with self.index.transaction() as db:
            initialized = self.index.get(db, "peersInitialized", False)
            peers = list(db.execute("SELECT machine_id,day FROM remote_days")) if not initialized else []
        for machine_id, day in peers:
            self.refresh_peer_partition(machine_id, day)
        if not initialized:
            # Preserve provisional legacy peers until their v4 origin becomes authoritative.
            cache, _ = self._load_cache()
            with self.index.transaction() as db:
                origins = {row[0] for row in db.execute("SELECT machine_id FROM remote_origins")}
                for position, entry in enumerate(cache.values()):
                    record = entry["record"]
                    if entry["sourceMachineId"] not in origins and entry["sourceMachineId"] != self.machine_id and record["kind"] in {"quota", "tokenLedger"}:
                        if (row := self._map_account(record["kind"], record["row"])) is not None:
                            self.index.fact(db, f'peer:{entry["sourceMachineId"]}:legacy', position, record["kind"], row)
                self.index.put(db, "peersInitialized", True)

    def initialize_v4_index(self, now=None):
        self.ingest_local()
        current = time.time() if now is None else now
        with self.index.transaction() as db:
            historical = db.execute("SELECT value FROM metadata WHERE name='localInitialized'").fetchone() is None
            initialized = self.index.get(db, "localFactsInitialized", False)
            if not initialized:
                db.execute("INSERT OR IGNORE INTO processing_pending_records SELECT l.record_key,json_extract(l.record_json,'$.kind'),NULL "
                    "FROM local_records l WHERE NOT EXISTS(SELECT 1 FROM processing_facts f WHERE f.record_key=l.record_key AND f.source NOT LIKE 'peer:%')")
            generation = self.index.get(db, "publishGeneration", 0)
            for key, kind, data in list(db.execute("SELECT record_key,kind,data FROM processing_pending_records")):
                previous = db.execute("SELECT period,content_hash FROM local_records WHERE record_key=?", (key,)).fetchone()
                if data is None:
                    if previous:
                        db.execute("DELETE FROM local_records WHERE record_key=?", (key,))
                        generation += 1
                        db.execute("INSERT OR REPLACE INTO processing_dirty_days VALUES(?,?)", (previous[0][:10], generation))
                    continue
                row = json.loads(data)
                if not syncable_record(kind, row, self.machine_id):
                    continue
                record = self._transport_record(kind, row)
                validate_sync_operation({"action": "upsert", "key": key, "record": record})
                digest = content_hash(record)
                if previous and previous[1] == digest:
                    continue
                event = row.get("checkedAt") or row.get("occurredAt") or (row.get("session") or {}).get("updatedAt") or (row.get("session") or {}).get("startedAt")
                period = previous[0] if previous else v4_period((parse_timestamp(event) or current) if historical else current)
                db.execute("INSERT INTO local_records VALUES(?,?,?,?) ON CONFLICT(record_key) DO UPDATE SET content_hash=excluded.content_hash,record_json=excluded.record_json",
                    (key, period, digest, canonical_json(record).decode()))
                generation += 1
                db.execute("INSERT OR REPLACE INTO processing_dirty_days VALUES(?,?)", (period[:10], generation))
            db.execute("DELETE FROM processing_pending_records")
            db.execute("INSERT OR IGNORE INTO metadata VALUES('localInitialized','1')")
            self.index.put(db, "localFactsInitialized", True)
            self.index.put(db, "publishGeneration", generation)

    def _v4_index_day(self, db, day):
        from monitor_streaming import RecordSpool
        parts = {}
        for key, period, data in db.execute("SELECT record_key,period,record_json FROM local_records WHERE period>=? AND period<? ORDER BY period,record_key", (day, day + "~")):
            if period not in parts:
                parts[period] = RecordSpool()
            parts[period].append(v4_record_envelope(self.machine_id, key, json.loads(data), period))
        return {"parts": parts} if parts else None

    def v4_publication_changes(self, now, catalog, force=False):
        self.initialize_v4_index(now)
        with self.index.transaction() as db:
            claims = dict(db.execute("SELECT day,generation FROM processing_dirty_days"))
            days = set(claims) | {day for day, row in catalog.items() if row["layout"] == "parts" and day < v4_period(now)[:10]}
            if not force:
                for day in list(days):
                    previous = self.index.get(db, f"publicationBoundary:{day}")
                    if day >= v4_period(now)[:10] and db.execute("SELECT 1 FROM local_records WHERE period>=? AND period<? LIMIT 1", (day, day + "~")).fetchone() and not db.execute(
                        "SELECT 1 FROM local_records WHERE period>=? AND period<? LIMIT 1", (day, min(day + "~", v4_period(now)))).fetchone():
                        self.index.put(db, f"publicationBoundary:{day}", [claims.get(day), v4_period(now)])
                        days.remove(day)
                    elif previous and previous[0] == claims.get(day) and day >= v4_period(now)[:10] and not db.execute(
                        "SELECT 1 FROM local_records WHERE period>=? AND period<? AND substr(period,1,10)=? LIMIT 1", (previous[1], v4_period(now), day)).fetchone():
                        days.remove(day)
            if force:
                days.update(row[0] for row in db.execute("SELECT DISTINCT substr(period,1,10) FROM local_records"))
            return {day: self._v4_index_day(db, day) for day in days}, claims

    def acknowledge_publication(self, claims, period=None):
        with self.index.transaction() as db:
            for day, generation in claims.items():
                if period is not None and db.execute("SELECT 1 FROM local_records WHERE period>=? AND substr(period,1,10)=? LIMIT 1", (period, day)).fetchone():
                    self.index.put(db, f"publicationBoundary:{day}", [generation, period])
                else:
                    db.execute("DELETE FROM processing_dirty_days WHERE day=? AND generation=?", (day, generation))

    @staticmethod
    def _v4_stat(path: Path) -> tuple[int, int, int] | None:
        try:
            value = path.stat()
        except FileNotFoundError:
            return None
        return value.st_ino, value.st_size, value.st_mtime_ns

    def _v4_local_sources(self) -> tuple[list[dict], list[dict]]:
        quota_stat, ledger_stat = self._v4_stat(self.quota_path), self._v4_stat(self.token_ledger_path)
        previous = self._v4_source_stats
        if previous and self._local_datasets_cache is not None and (self.account_revision_resolver is None or self.account_revision_resolver() == self._account_revision):
            if previous == (quota_stat, ledger_stat):
                return self._local_datasets_cache
            if previous[1] == ledger_stat and previous[0] and quota_stat and previous[0][0] == quota_stat[0] and quota_stat[1] > previous[0][1]:
                from monitor_history import normalize_quota_history_row
                with self.quota_path.open("rb") as source:
                    source.seek(previous[0][1])
                    tail = source.read()
                if tail.endswith(b"\n"):
                    try:
                        rows = [normalize_quota_history_row(json.loads(line)) for line in tail.decode("utf-8").splitlines() if line]
                        if all(row is not None and sync_meta(row).get("originMachineId") == self.machine_id and sync_meta(row).get("accountId") and sync_meta(row).get("recordId") for row in rows):
                            for key, record in active_records(rows, [], self.machine_id, self.account_id_resolver, False).items():
                                validate_sync_operation({"action": "upsert", "key": key, "record": record})
                            local = self._local_datasets_cache[0] + rows, self._local_datasets_cache[1]
                            self._materialize_datasets(local, self._load_cache()[0])
                            self._v4_source_stats = quota_stat, ledger_stat
                            return local
                    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                        pass
        local = self._normalize_local()
        self._v4_source_stats = self._v4_stat(self.quota_path), self._v4_stat(self.token_ledger_path)
        return local

    def _load(self) -> tuple[list[dict], list[dict]]:
        from monitor_history import load_quota_history
        from monitor_token_ledger import load_token_ledger
        return load_quota_history(self.quota_path), load_token_ledger(self.token_ledger_path)

    def _load_cache(self) -> tuple[dict[tuple[str, str], dict], dict[str, dict[str, str]]]:
        try:
            payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}, {}
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Cannot read synchronized usage cache: {exc}") from exc
        if not isinstance(payload, dict) or payload.get("version") not in {1, 2, CACHE_VERSION}:
            raise ValueError("Unsupported or invalid synchronized usage cache")
        if payload.get("layout") == CACHE_LAYOUT:
            return self._load_sharded_cache(payload)
        if not isinstance(payload.get("records"), list):
            raise ValueError("Unsupported or invalid synchronized usage cache")
        legacy_cache = payload.get("version") != CACHE_VERSION
        if not legacy_cache:
            self._cache_repair_needed = True
        if legacy_cache:
            self.needs_remote_rebuild = True
            self._cache_repair_needed = True
        raw_packs = payload.get("packs", {}) if payload.get("version") == CACHE_VERSION else {}
        if not isinstance(raw_packs, dict):
            raise ValueError("Invalid synchronized usage pack inventory")
        packs = {}
        for machine_id, inventory in raw_packs.items():
            if not isinstance(machine_id, str) or not machine_id or not isinstance(inventory, dict):
                raise ValueError("Invalid synchronized usage pack inventory")
            packs[machine_id] = {}
            for pack_id, pack_hash in inventory.items():
                if not isinstance(pack_id, str) or not pack_id or len(pack_id) > 128 or not isinstance(pack_hash, str) or len(pack_hash) != 64 or any(character not in "0123456789abcdef" for character in pack_hash):
                    raise ValueError("Invalid synchronized usage pack hash")
                packs[machine_id][pack_id] = pack_hash
        records, observed_packs = {}, set()
        for entry in payload["records"]:
            if not isinstance(entry, dict) or not isinstance(entry.get("sourceMachineId"), str) or not entry["sourceMachineId"] or not isinstance(entry.get("key"), str):
                raise ValueError("Invalid synchronized usage cache entry")
            if legacy_cache and (entry.get("record") or {}).get("kind") != "quota":
                continue
            validate_sync_operation({"action": "upsert", "key": entry["key"], "record": entry.get("record")})
            if sync_meta(entry["record"]["row"]).get("originMachineId") != entry["sourceMachineId"]:
                raise ValueError("Synchronized usage cache origin does not match its record")
            pack_id, pack_hash = entry.get("sourcePackId"), entry.get("sourcePackHash")
            if (pack_id is None) != (pack_hash is None) or pack_id is not None and packs.get(entry["sourceMachineId"], {}).get(pack_id) != pack_hash:
                self.needs_remote_rebuild = True
                self._cache_repair_needed = True
                continue
            if pack_id is not None:
                observed_packs.add((entry["sourceMachineId"], pack_id))
            records[(entry["sourceMachineId"], entry["key"])] = entry
        for machine_id, inventory in list(packs.items()):
            missing = set(inventory) - {pack_id for source, pack_id in observed_packs if source == machine_id}
            if missing:
                self.needs_remote_rebuild = True
                self._cache_repair_needed = True
                for pack_id in missing:
                    inventory.pop(pack_id)
            if not inventory:
                packs.pop(machine_id)
        return records, packs

    @property
    def _cache_shard_path(self) -> Path:
        return self.cache_path.with_name(f"{self.cache_path.name}.d")

    @staticmethod
    def _atomic_write(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def _load_sharded_cache(self, payload: dict) -> tuple[dict[tuple[str, str], dict], dict[str, dict[str, str]]]:
        origins = payload.get("origins")
        if not isinstance(origins, dict):
            raise ValueError("Invalid synchronized usage shard inventory")
        records, packs = {}, {}
        for machine_id, origin in origins.items():
            if not isinstance(machine_id, str) or not machine_id or not isinstance(origin, dict) or not isinstance(origin.get("shards"), dict):
                raise ValueError("Invalid synchronized usage shard inventory")
            origin_records, origin_packs, valid = {}, {}, True
            for shard_id, descriptor in origin["shards"].items():
                if (
                    not isinstance(shard_id, str) or not isinstance(descriptor, dict) or not isinstance(descriptor.get("file"), str) or Path(descriptor["file"]).name != descriptor["file"]
                    or not isinstance(descriptor.get("contentHash"), str) or len(descriptor["contentHash"]) != 64 or any(character not in "0123456789abcdef" for character in descriptor["contentHash"])
                    or isinstance(descriptor.get("recordCount"), bool) or not isinstance(descriptor.get("recordCount"), int) or descriptor["recordCount"] < 0
                ):
                    raise ValueError("Invalid synchronized usage shard descriptor")
                try:
                    data = (self._cache_shard_path / descriptor["file"]).read_bytes()
                    entries = json.loads(data)
                except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                    valid = False
                    break
                if not isinstance(entries, list) or content_hash(entries) != descriptor["contentHash"] or len(entries) != descriptor["recordCount"]:
                    valid = False
                    break
                pack_id = None if shard_id == "loose" else shard_id
                if pack_id is not None:
                    pack_hash = descriptor.get("packHash")
                    if not isinstance(pack_hash, str) or len(pack_hash) != 64 or any(character not in "0123456789abcdef" for character in pack_hash):
                        raise ValueError("Invalid synchronized usage pack hash")
                    origin_packs[pack_id] = pack_hash
                for entry in entries:
                    if (
                        not isinstance(entry, dict) or entry.get("sourceMachineId") != machine_id or entry.get("sourcePackId") != pack_id or not isinstance(entry.get("key"), str)
                        or pack_id is not None and entry.get("sourcePackHash") != origin_packs[pack_id]
                    ):
                        valid = False
                        break
                    validate_sync_operation({"action": "upsert", "key": entry["key"], "record": entry.get("record")})
                    if sync_meta(entry["record"]["row"]).get("originMachineId") != machine_id:
                        valid = False
                        break
                    origin_records[(machine_id, entry["key"])] = entry
                if not valid:
                    break
            if not valid:
                self.needs_remote_rebuild = True
                self._cache_repair_needed = True
                continue
            records.update(origin_records)
            if origin_packs:
                packs[machine_id] = origin_packs
        return records, packs

    def _store_cache(self, records: dict[tuple[str, str], dict], packs: dict[str, dict[str, str]]) -> None:
        origins, referenced = {}, set()
        for machine_id in sorted({key[0] for key in records} | set(packs)):
            shards = {}
            grouped = {pack_id: [] for pack_id in packs.get(machine_id, {})}
            for (source, _), entry in records.items():
                if source == machine_id:
                    grouped.setdefault(entry.get("sourcePackId") or "loose", []).append(entry)
            for shard_id, entries in sorted(grouped.items()):
                entries.sort(key=lambda entry: entry["key"])
                if shard_id != "loose" and shard_id not in packs.get(machine_id, {}):
                    continue
                digest = content_hash(entries)
                filename = f"{hashlib.sha256(machine_id.encode()).hexdigest()[:16]}-{hashlib.sha256(shard_id.encode()).hexdigest()[:16]}-{digest}.json"
                shard_path = self._cache_shard_path / filename
                if not shard_path.exists():
                    self._atomic_write(shard_path, canonical_json(entries) + b"\n")
                referenced.add(filename)
                descriptor = {"file": filename, "contentHash": digest, "recordCount": len(entries)}
                if shard_id != "loose":
                    descriptor["packHash"] = packs[machine_id][shard_id]
                shards[shard_id] = descriptor
            if shards:
                origins[machine_id] = {"shards": shards}
        self._atomic_write(self.cache_path, canonical_json({"version": CACHE_VERSION, "layout": CACHE_LAYOUT, "origins": origins}) + b"\n")
        if self._cache_shard_path.exists():
            for path in self._cache_shard_path.iterdir():
                if path.is_file() and path.name not in referenced:
                    try:
                        path.unlink(missing_ok=True)
                    except OSError:
                        pass
        self._cache_repair_needed = False

    def _map_account(self, kind: str, row: dict) -> dict | None:
        account_id = sync_meta(row).get("accountId")
        if not account_id or self.account_mapper is None:
            return None
        source = row.get("session") or row
        mapped = self.account_mapper(account_id, source.get("accountSlotId"), source.get("accountLabel"))
        if mapped is None:
            return None
        slot_id, label = mapped
        projected = row
        if not str(account_id).startswith("v3:"):
            projected = row | {SYNC_META_KEY: sync_meta(row) | {"accountId": self.account_id_resolver(slot_id)}}
        if kind == "tokenLedger" and row.get("recordType") == "legacyBaseline":
            return projected | {"session": source | {"accountSlotId": slot_id, "accountLabel": label}}
        return projected | {"accountSlotId": slot_id, "accountLabel": label}

    def _materialize_datasets(self, local: tuple[list[dict], list[dict]], cache: dict[tuple[str, str], dict]) -> None:
        v4_records, v4_origins = self._v4_remote_cache()
        mapped = [(record["kind"], row) for entry in [*(entry for entry in cache.values() if entry.get("sourceMachineId") not in v4_origins), *v4_records] if entry.get("sourceMachineId") != self.machine_id and (record := entry["record"]) and record.get("kind") in {"quota", "tokenLedger"} and (row := self._map_account(record["kind"], record["row"])) is not None]
        local_ledger = []
        for source in local[1]:
            legacy_schema = source.get("schemaVersion") == 1
            if (row := canonical_ledger_row(source)) is None:
                continue
            session = row.get("session") or {}
            account_id = self.account_id_resolver(row.get("accountSlotId") or session.get("accountSlotId"))
            if legacy_schema and sync_meta(row).get("accountId") != account_id:
                row = {key: value for key, value in row.items() if key != SYNC_META_KEY}
            local_ledger.append(add_record_provenance("tokenLedger", row, self.machine_id, account_id))
        ledger, self.conflicts = merge_token_ledger_rows(local_ledger + [row for kind, row in mapped if kind == "tokenLedger"])
        self._local_datasets_cache = local
        self._merged_datasets_cache = merge_quota_rows(local[0] + [row for kind, row in mapped if kind == "quota"]), ledger
        self._account_revision = self.account_revision_resolver() if self.account_revision_resolver is not None else None

    @staticmethod
    def _transport_record(kind: str, row: dict) -> dict:
        if kind == "tokenLedger":
            row = canonical_ledger_row(row)
        transport = {key: value for key, value in row.items() if key not in {"accountSlotId", "accountLabel"}}
        if kind == "tokenLedger" and row.get("recordType") == "legacyBaseline" and isinstance(transport.get("session"), dict):
            transport["session"] = {key: value for key, value in transport["session"].items() if key not in {"accountSlotId", "accountLabel"}}
        return {"kind": kind, "row": transport}

    @staticmethod
    def _quota_bytes(rows: list[dict]) -> bytes:
        from monitor_history import normalize_quota_history_row
        return "".join(json.dumps(normalized, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows if (normalized := normalize_quota_history_row(row)) is not None).encode()

    def _normalize_local(self) -> tuple[list[dict], list[dict]]:
        quota, ledger = self._load()
        cache, packs = self._load_cache()
        cache_changed = not self.cache_path.exists() or self._cache_repair_needed
        def normalize_quota(row: dict) -> dict | None:
            nonlocal cache_changed
            meta = sync_meta(row)
            if meta.get("originMachineId") and meta["originMachineId"] != self.machine_id:
                key = record_key("quota", row)
                cache[(meta["originMachineId"], key)] = {"sourceMachineId": meta["originMachineId"], "key": key, "record": self._transport_record("quota", row)}
                cache_changed = True
                self.needs_remote_rebuild = True
                return None
            account_id = meta.get("accountId") or self.account_id_resolver(row.get("accountSlotId"))
            if not meta.get("accountId") or not meta.get("recordId"):
                row = {key: value for key, value in row.items() if key != SYNC_META_KEY}
            if str(meta.get("accountId") or "").startswith("local:"):
                resolved = self.account_id_resolver(row.get("accountSlotId"))
                if not str(resolved).startswith("local:"):
                    row = {key: value for key, value in row.items() if key != SYNC_META_KEY}
                    account_id = resolved
            elif meta.get("originMachineId") == self.machine_id and not str(account_id).startswith("v3:") and self.account_mapper is not None:
                mapped = self.account_mapper(account_id, row.get("accountSlotId"), row.get("accountLabel"))
                if mapped is not None:
                    row, account_id = {key: value for key, value in row.items() if key != SYNC_META_KEY}, self.account_id_resolver(mapped[0])
            return add_record_provenance("quota", row, self.machine_id, account_id) if not sync_meta(row).get("recordId") else row

        def normalize_ledger(row: dict) -> dict | None:
            nonlocal cache_changed
            if (row := canonical_ledger_row(row, preserve_legacy_highwater=True)) is None:
                return None
            meta, source = sync_meta(row), row.get("session") or row
            if meta.get("originMachineId") and meta["originMachineId"] != self.machine_id:
                key = record_key("tokenLedger", row)
                cache[(meta["originMachineId"], key)] = {"sourceMachineId": meta["originMachineId"], "key": key, "record": self._transport_record("tokenLedger", row)}
                cache_changed = True
                self.needs_remote_rebuild = True
                return None
            if not meta.get("originMachineId") or not meta.get("accountId"):
                return add_record_provenance("tokenLedger", {key: value for key, value in row.items() if key != SYNC_META_KEY}, self.machine_id, self.account_id_resolver(source.get("accountSlotId")))
            if not str(meta.get("accountId") or "").startswith("v3:") and self.account_mapper is not None:
                mapped = self.account_mapper(meta.get("accountId"), source.get("accountSlotId"), source.get("accountLabel"))
                if mapped is not None:
                    return add_record_provenance("tokenLedger", {key: value for key, value in row.items() if key != SYNC_META_KEY}, self.machine_id, self.account_id_resolver(mapped[0]))
            return row

        normalized_quota = [normalized for row in quota if (normalized := normalize_quota(row)) is not None]
        normalized_ledger = [normalized for row in ledger if (normalized := normalize_ledger(row)) is not None]
        if cache_changed:
            self._store_cache(cache, packs)
        if normalized_quota != quota:
            self._atomic_write(self.quota_path, self._quota_bytes(normalized_quota))
        if normalized_ledger != ledger:
            from monitor_token_ledger import write_token_ledger
            write_token_ledger(self.token_ledger_path, normalized_ledger)
        local = normalized_quota, normalized_ledger
        if cache_changed or local != self._local_datasets_cache or self._merged_datasets_cache is None:
            self._materialize_datasets(local, cache)
        return local

    def normalize_local(self) -> tuple[list[dict], list[dict]]:
        with self.lock:
            return self._normalize_local()

    def refresh_accounts(self) -> None:
        with self.lock:
            self._normalize_local()
            with self.index.transaction() as db:
                db.execute("DELETE FROM processing_files")
                peers = list(db.execute("SELECT machine_id,day FROM remote_days"))
                for source, position, kind, data in list(db.execute("SELECT source,position,kind,data FROM processing_facts WHERE source LIKE 'peer:%:legacy'")):
                    if (row := self._map_account(kind, json.loads(data))) is not None:
                        self.index.fact(db, source, position, kind, row)
            self._ingest_local()
            for machine_id, day in peers:
                self.refresh_peer_partition(machine_id, day, force=True)

    def _datasets(self, view: str) -> tuple[list[dict], list[dict]]:
        if self._local_datasets_cache is None or self._merged_datasets_cache is None:
            self._normalize_local()
        elif self.account_revision_resolver is not None and self.account_revision_resolver() != self._account_revision:
            self._materialize_datasets(self._local_datasets_cache, self._load_cache()[0])
        return self._merged_datasets_cache if view == "merged" else self._local_datasets_cache

    def datasets(self, view: str = "local") -> tuple[list[dict], list[dict]]:
        with self.lock:
            self._ingest_local()
            if self._local_datasets_cache is None or self._merged_datasets_cache is None:
                self._materialize_datasets((self.index.rows(self.quota_path, "quota"), self.index.rows(self.token_ledger_path, "tokenLedger")), self._load_cache()[0])
            return self._merged_datasets_cache if view == "merged" else self._local_datasets_cache

    def snapshot(self, necessary_only: bool = True) -> tuple[dict[str, dict], set[str]]:
        with self.lock:
            quota, ledger = self._normalize_local()
            local = active_records(quota, ledger, self.machine_id, self.account_id_resolver, necessary_only)
            return local, set(local)

    def pack_hashes(self, machine_id: str) -> dict[str, str]:
        with self.lock:
            return self._load_cache()[1].get(machine_id, {}).copy()

    def apply_pack_snapshot(self, machine_id: str, manifest: dict[str, str], downloaded: dict[str, list[dict]], replace_all: bool = False) -> list[dict]:
        with self.lock:
            local = self._normalize_local()
            cache, packs = self._load_cache()
            cached = packs.get(machine_id, {})
            changed = set(manifest) if replace_all else {pack_id for pack_id, pack_hash in manifest.items() if cached.get(pack_id) != pack_hash}
            if set(downloaded) != changed:
                raise ValueError("Synchronized usage pack download set does not match the manifest changes")
            replace = changed | (set(cached) - set(manifest))
            if replace_all or not cached:
                cache = {key: entry for key, entry in cache.items() if key[0] != machine_id}
            elif replace:
                cache = {key: entry for key, entry in cache.items() if key[0] != machine_id or entry.get("sourcePackId") not in replace}
            for pack_id, entries in downloaded.items():
                for item in entries:
                    operation = {"action": "upsert", **item}
                    if (operation.get("record") or {}).get("kind") in {"cost", "token"}:
                        continue
                    if (operation.get("record") or {}).get("kind") == "tokenLedger":
                        if (row := canonical_ledger_row(operation["record"]["row"])) is None:
                            continue
                        operation["record"] = operation["record"] | {"row": row}
                    validate_sync_operation(operation)
                    if sync_meta(operation["record"]["row"]).get("originMachineId") != machine_id:
                        raise ValueError("Synchronized usage pack origin does not match its record")
                    cache[(machine_id, operation["key"])] = {
                        "sourceMachineId": machine_id, "sourcePackId": pack_id, "sourcePackHash": manifest[pack_id], "key": operation["key"], "record": operation["record"],
                    }
            if manifest:
                packs[machine_id] = manifest.copy()
            else:
                packs.pop(machine_id, None)
            self._store_cache(cache, packs)
            self._materialize_datasets(local, cache)
            return self.conflicts

    def apply(self, operations: list[dict], checkpoint_origin: str | None = None, operation_origin: str | None = None) -> list[dict]:
        with self.lock:
            local = self._normalize_local()
            cache, packs = self._load_cache()
            previous = cache.copy()
            if checkpoint_origin:
                cache = {key: value for key, value in cache.items() if key[0] != checkpoint_origin}
                packs.pop(checkpoint_origin, None)
            for operation in operations:
                if operation.get("action") == "upsert" and (operation.get("record") or {}).get("kind") in {"cost", "token"}:
                    continue
                if operation.get("action") == "upsert" and (operation.get("record") or {}).get("kind") == "tokenLedger":
                    if (row := canonical_ledger_row(operation["record"]["row"])) is None:
                        continue
                    operation = operation | {"record": operation["record"] | {"row": row}}
                validate_sync_operation(operation)
                if operation["action"] == "upsert":
                    source = operation_origin or sync_meta(operation["record"]["row"]).get("originMachineId")
                    if not source or sync_meta(operation["record"]["row"]).get("originMachineId") != source:
                        raise ValueError("Synchronized usage operation origin does not match its record")
                    cache[(source, operation["key"])] = {"sourceMachineId": source, "key": operation["key"], "record": operation["record"]}
                    packs.pop(source, None)
                else:
                    if not operation_origin:
                        raise ValueError("Synchronized usage deletion is missing its origin")
                    cache.pop((operation_origin, operation["key"]), None)
                    packs.pop(operation_origin, None)
            if cache != previous:
                self._store_cache(cache, packs)
                self._materialize_datasets(local, cache)
            return self.conflicts

    def remove_origins_not_in(self, authoritative_machine_ids) -> int:
        authoritative = set(authoritative_machine_ids)
        if not all(isinstance(machine_id, str) and machine_id for machine_id in authoritative):
            raise ValueError("Invalid authoritative machine inventory")
        with self.lock:
            local = self._normalize_local()
            cache, packs = self._load_cache()
            retained = {key: entry for key, entry in cache.items() if key[0] in authoritative}
            retained_packs = {machine_id: inventory for machine_id, inventory in packs.items() if machine_id in authoritative}
            removed = len(({key[0] for key in cache} | set(packs)) - authoritative)
            if retained != cache or retained_packs != packs:
                self._store_cache(retained, retained_packs)
                self._materialize_datasets(local, retained)
            return removed

    def _v4_remote_cache(self) -> tuple[list[dict], set[str]]:
        initialize_v4_cache(self.v4_cache_path)
        with closing(sqlite3.connect(self.v4_cache_path)) as db, db:
            origins = {row[0] for row in db.execute("SELECT machine_id FROM remote_origins")}
            return [{"sourceMachineId": machine_id, "key": key, "record": json.loads(record_json)["record"]} for machine_id, key, record_json in db.execute("SELECT machine_id, record_key, record_json FROM remote_records")], origins

    def v4_local_snapshot(self, now: float | None = None) -> dict[str, dict]:
        """Assign collection periods once; a later edit keeps its original owner."""
        with self.lock:
            current = datetime.now(timezone.utc).timestamp() if now is None else now
            quota, ledger = self._v4_local_sources()
            records = active_records(quota, ledger, self.machine_id, self.account_id_resolver, False)
            initialize_v4_cache(self.v4_cache_path)
            with closing(sqlite3.connect(self.v4_cache_path)) as db, db:
                existing = {key: (period, digest) for key, period, digest in db.execute("SELECT record_key, period, content_hash FROM local_records")}
                historical = db.execute("SELECT value FROM metadata WHERE name='localInitialized'").fetchone() is None
                for key, record in records.items():
                    validate_sync_operation({"action": "upsert", "key": key, "record": record})
                    if key in existing:
                        period = existing[key][0]
                    else:
                        row = record["row"]
                        event_at = row.get("checkedAt") if record["kind"] == "quota" else row.get("occurredAt") or (row.get("session") or {}).get("updatedAt") or (row.get("session") or {}).get("startedAt")
                        period = v4_period((parse_timestamp(event_at) or current) if historical else current)
                    digest = content_hash(record)
                    if existing.get(key) != (period, digest):
                        db.execute("INSERT INTO local_records(record_key, period, content_hash, record_json) VALUES(?,?,?,?) ON CONFLICT(record_key) DO UPDATE SET content_hash=excluded.content_hash, record_json=excluded.record_json", (key, period, digest, canonical_json(record).decode()))
                for key in existing.keys() - records.keys():
                    db.execute("DELETE FROM local_records WHERE record_key=?", (key,))
                db.execute("INSERT OR IGNORE INTO metadata(name, value) VALUES('localInitialized', '1')")
                days = {}
                for key, period, record_json in db.execute("SELECT record_key, period, record_json FROM local_records ORDER BY period, record_key"):
                    day = days.setdefault(period[:10], {"parts": {}})
                    day["parts"].setdefault(period, []).append(v4_record_envelope(self.machine_id, key, json.loads(record_json), period))
                for day in days.values():
                    day["logicalHash"] = v4_logical_hash([entry for part in day["parts"].values() for entry in part])
                return days

    def v4_cached_days(self, machine_id: str) -> dict[str, dict]:
        with self.lock:
            initialize_v4_cache(self.v4_cache_path)
            with closing(sqlite3.connect(self.v4_cache_path)) as db, db:
                return {day: {"logicalHash": digest, "layout": json.loads(layout)} for day, digest, layout in db.execute("SELECT day, logical_hash, layout_json FROM remote_days WHERE machine_id=?", (machine_id,))}

    def v4_apply_day(self, machine_id: str, manifest: dict, downloaded: dict[str, list[dict]], force: bool = False) -> None:
        with self.lock:
            day, parts = manifest["day"], manifest["parts"]
            initialize_v4_cache(self.v4_cache_path)
            with closing(sqlite3.connect(self.v4_cache_path)) as db, db:
                previous = db.execute("SELECT logical_hash, layout_json FROM remote_days WHERE machine_id=? AND day=?", (machine_id, day)).fetchone()
                old_parts = json.loads(previous[1]) if previous else {}
                if force or previous is None or previous[0] != manifest["logicalHash"]:
                    changed = set(parts) if force or "bulk" in parts or "bulk" in old_parts else {part for part, digest in parts.items() if old_parts.get(part) != digest}
                    if set(downloaded) != changed:
                        raise ValueError("Downloaded usage parts do not match the changed day manifest")
                    for part in (set(old_parts) - set(parts)) | changed:
                        db.execute("DELETE FROM remote_records WHERE machine_id=? AND day=? AND part_id=?", (machine_id, day, part))
                    for part, entries in downloaded.items():
                        for entry in entries:
                            validate_v4_envelope(entry, machine_id)
                            if entry["collectionPeriod"][:10] != day or part != "bulk" and entry["collectionPeriod"] != part or db.execute("SELECT 1 FROM remote_records WHERE machine_id=? AND record_key=?", (machine_id, entry["recordKey"])).fetchone():
                                raise ValueError("Usage v4 record is stored outside its owning collection period")
                            db.execute("INSERT OR REPLACE INTO remote_records(machine_id, day, part_id, record_key, content_hash, record_json) VALUES(?,?,?,?,?,?)", (machine_id, day, part, entry["recordKey"], content_hash(entry["record"]), canonical_json(entry).decode()))
                    from monitor_streaming import logical_hash_sorted
                    rows = (json.loads(record_json) for (record_json,) in db.execute("SELECT record_json FROM remote_records WHERE machine_id=? AND day=? ORDER BY record_key", (machine_id, day)))
                    if logical_hash_sorted(rows) != manifest["logicalHash"]:
                        raise ValueError("Usage day logical digest does not match downloaded records")
                elif downloaded:
                    raise ValueError("Unchanged usage day unexpectedly downloaded payloads")
                db.execute("INSERT INTO remote_origins(machine_id) VALUES(?) ON CONFLICT DO NOTHING", (machine_id,))
                db.execute("INSERT INTO remote_days(machine_id, day, logical_hash, layout_json) VALUES(?,?,?,?) ON CONFLICT(machine_id, day) DO UPDATE SET logical_hash=excluded.logical_hash, layout_json=excluded.layout_json", (machine_id, day, manifest["logicalHash"], canonical_json(parts).decode()))
            self.refresh_peer_partition(machine_id, day)

    def v4_remove_days(self, machine_id: str, days: set[str]) -> None:
        with self.lock:
            initialize_v4_cache(self.v4_cache_path)
            with closing(sqlite3.connect(self.v4_cache_path)) as db, db:
                for day in days:
                    db.execute("DELETE FROM remote_records WHERE machine_id=? AND day=?", (machine_id, day))
                    db.execute("DELETE FROM remote_days WHERE machine_id=? AND day=?", (machine_id, day))
                db.execute("INSERT INTO remote_origins(machine_id) VALUES(?) ON CONFLICT DO NOTHING", (machine_id,))
            for day in days:
                self.refresh_peer_partition(machine_id, day)
