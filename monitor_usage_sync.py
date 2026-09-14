#!/usr/bin/env python3

import hashlib
import heapq
import json
import math
import os
import tempfile
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




class UsageDataStore:
    def __init__(self, quota_path: Path, token_ledger_path: Path, machine_id: str, account_id_resolver, lock, account_mapper=None, cache_path: Path | None = None, account_revision_resolver=None):
        self.quota_path, self.token_ledger_path = Path(quota_path), Path(token_ledger_path)
        self.cache_path = Path(cache_path) if cache_path is not None else default_usage_sync_cache_path(self.quota_path)
        self.machine_id, self.account_id_resolver, self.lock, self.account_mapper = machine_id, account_id_resolver, lock, account_mapper
        self.account_revision_resolver = account_revision_resolver
        self.conflicts = []
        self.needs_remote_rebuild = not self.cache_path.exists()
        self._local_datasets_cache = None
        self._merged_datasets_cache = None
        self._account_revision = None
        self._cache_repair_needed = False

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
        mapped = [(record["kind"], row) for entry in cache.values() if (record := entry["record"]) and (row := self._map_account(record["kind"], record["row"])) is not None]
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
            if self._local_datasets_cache is None:
                self._normalize_local()
            else:
                self._materialize_datasets(self._load(), self._load_cache()[0])

    def _datasets(self, view: str) -> tuple[list[dict], list[dict]]:
        if self._local_datasets_cache is None or self._merged_datasets_cache is None:
            self._normalize_local()
        elif self.account_revision_resolver is not None and self.account_revision_resolver() != self._account_revision:
            self._materialize_datasets(self._local_datasets_cache, self._load_cache()[0])
        return self._merged_datasets_cache if view == "merged" else self._local_datasets_cache

    def datasets(self, view: str = "local") -> tuple[list[dict], list[dict]]:
        with self.lock:
            return self._datasets("merged" if view == "merged" else "local")

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
