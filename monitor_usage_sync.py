#!/usr/bin/env python3

import hashlib
import heapq
import json
import math
import os
import tempfile
from pathlib import Path

from monitor_common import MIN_DELTA_COST_PER_PERCENT_USD, RESET_TIME_JITTER_SECONDS, coerce_float, parse_timestamp
from monitor_history import compact_quota_history_rows
from monitor_tokens import normalize_codex_model


SYNC_META_KEY = "sync"
COST_INTERVAL_TYPE = "costInterval"
MAX_SYNC_RECORD_BYTES = 256 * 1024
MAX_SYNC_STRING_LENGTH = 4096
MAX_SYNC_COLLECTION_ITEMS = 4096
MAX_SYNC_NESTING_DEPTH = 12
CACHE_VERSION = 2


def canonical_json(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def content_hash(value) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def default_usage_sync_cache_path(history_path: Path) -> Path:
    history_path = Path(history_path)
    return history_path.with_name("usage_monitor_sync_cache.json") if history_path.name == "usage_monitor_history.jsonl" else history_path.with_suffix(".sync-cache.json")


def sync_meta(row: dict) -> dict:
    value = row.get(SYNC_META_KEY)
    return value if isinstance(value, dict) else {}


def is_cost_interval_row(row: dict) -> bool:
    return isinstance(row, dict) and row.get("recordType") == COST_INTERVAL_TYPE and row.get("window") in {"5h", "7d"}


def record_account_key(row: dict) -> str:
    return str(sync_meta(row).get("accountId") or f"local:{sync_meta(row).get('originMachineId') or 'legacy'}:{row.get('accountSlotId') or 'unknown'}")


def record_key(kind: str, row: dict) -> str:
    meta = sync_meta(row)
    if meta.get("recordId"):
        return f"{kind}:{meta['recordId']}"
    if kind == "token":
        return f"token:{record_account_key(row)}:{row.get('sessionId')}"
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


def validate_sync_operation(operation: dict) -> None:
    if not isinstance(operation, dict) or not isinstance(operation.get("action"), str) or operation["action"] not in {"delete", "upsert"} or not isinstance(operation.get("key"), str) or not operation["key"] or len(operation["key"]) > 512:
        raise ValueError("Invalid synchronized usage operation")
    if operation["action"] == "delete":
        return
    record = operation.get("record")
    if not isinstance(record, dict) or record.get("kind") not in {"cost", "quota", "token"} or not isinstance(record.get("row"), dict):
        raise ValueError("Invalid synchronized usage record")
    _validate_sync_value(record)
    if len(canonical_json(record)) > MAX_SYNC_RECORD_BYTES:
        raise ValueError("Synchronized usage record is too large")
    if operation["key"] != record_key(record["kind"], record["row"]):
        raise ValueError("Synchronized usage record key does not match its content")


def add_record_provenance(kind: str, row: dict, machine_id: str, account_id: str, local_only: bool = False) -> dict:
    if sync_meta(row).get("recordId"):
        return row
    if kind == "quota":
        identity = f"quota:{account_id}:{row.get('checkedAt')}"
    elif kind == "token":
        identity = f"token:{account_id}:{row.get('sessionId')}"
    else:
        identity = f"{machine_id}:cost:{row.get('window')}:{row.get('checkedAt')}:{row.get('accountSlotId')}"
    return row | {SYNC_META_KEY: {"version": 1, "originMachineId": machine_id, "accountId": account_id, "recordId": hashlib.sha256(identity.encode()).hexdigest(), "localOnly": bool(local_only)}}


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


def _token_total(row: dict) -> int:
    return int(coerce_float((row.get("tokens") or {}).get("totalTokens")) or 0)


def _prefer_token_row(previous: dict, current: dict, conflicts: list[dict]) -> dict:
    previous_at, current_at = parse_timestamp(previous.get("updatedAt")), parse_timestamp(current.get("updatedAt"))
    if (current_at or 0) != (previous_at or 0):
        return current if (current_at or 0) > (previous_at or 0) else previous
    previous_total, current_total = _token_total(previous), _token_total(current)
    if previous_total != current_total:
        return current if current_total > previous_total else previous
    previous_hash, current_hash = (content_hash({key: value for key, value in row.items() if key not in {"accountSlotId", "accountLabel", SYNC_META_KEY}}) for row in (previous, current))
    if previous_hash != current_hash:
        conflicts.append({"sessionId": current.get("sessionId"), "accountId": record_account_key(current), "updatedAt": current.get("updatedAt")})
    return current if current_hash > previous_hash else previous


def merge_token_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    merged, conflicts = {}, []
    for row in rows:
        if not isinstance(row, dict) or not row.get("sessionId"):
            continue
        key = (record_account_key(row), str(row["sessionId"]))
        merged[key] = _prefer_token_row(merged[key], row, conflicts) if key in merged else row
    return sorted(merged.values(), key=lambda row: (parse_timestamp(row.get("updatedAt")) or 0, row.get("sessionId") or "")), conflicts


def merge_cost_rows(rows: list[dict]) -> list[dict]:
    legacy, intervals = [], {}
    for row in rows:
        if not is_cost_interval_row(row):
            legacy.append(row)
            continue
        intervals[record_key("cost", row)] = row
    return legacy + sorted(intervals.values(), key=lambda row: (row.get("checkedAt") or "", row.get("window") or "", record_key("cost", row)))


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
                    "deltaPercent": round(model_percent, 8), "deltaCostUsd": round(model_cost, 8), "costPercentRatio": round(total_cost / delta_percent, 8),
                })
                used_percent += model_percent
    return sorted(aggregated, key=lambda row: (row.get("checkedAt") or "", row.get("window") or "", row.get("model") or ""))


def active_records(history: list[dict], quota: list[dict], tokens: list[dict], machine_id: str, necessary_only: bool = True) -> dict[str, dict]:
    records = {}
    for kind, rows in (("cost", history), ("quota", quota_sync_boundary_rows(quota, machine_id) if necessary_only else quota), ("token", tokens)):
        for row in rows:
            if syncable_record(kind, row, machine_id):
                records[record_key(kind, row)] = {"kind": kind, "row": {key: value for key, value in row.items() if key not in {"accountSlotId", "accountLabel"}}}
    return records


def apply_operations(history: list[dict], quota: list[dict], tokens: list[dict], operations: list[dict], checkpoint_origin: str | None = None) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    current = {}
    for kind, rows in (("cost", history), ("quota", quota), ("token", tokens)):
        for row in rows:
            current[record_key(kind, row)] = {"kind": kind, "row": row}
    if checkpoint_origin:
        current = {key: value for key, value in current.items() if sync_meta(value["row"]).get("originMachineId") != checkpoint_origin}
    for operation in operations:
        validate_sync_operation(operation)
        key = operation.get("key")
        if operation.get("action") == "delete":
            current.pop(key, None)
        elif operation.get("action") == "upsert" and isinstance(operation.get("record"), dict):
            current[key] = operation["record"]
    history_rows = merge_cost_rows([value["row"] for value in current.values() if value["kind"] == "cost"])
    quota_rows = merge_quota_rows([value["row"] for value in current.values() if value["kind"] == "quota"])
    token_rows, conflicts = merge_token_rows([value["row"] for value in current.values() if value["kind"] == "token"])
    return history_rows, quota_rows, token_rows, conflicts


def transactional_replace(paths_and_data: list[tuple[Path, bytes]]) -> None:
    originals, staged = {}, []
    try:
        for path, data in paths_and_data:
            path.parent.mkdir(parents=True, exist_ok=True)
            originals[path] = path.read_bytes() if path.exists() else None
            fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            staged.append((path, Path(temp_name)))
        for path, temporary in staged:
            os.replace(temporary, path)
    except Exception:
        for path, data in originals.items():
            if data is None:
                path.unlink(missing_ok=True)
            else:
                fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".rollback", dir=path.parent)
                with os.fdopen(fd, "wb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temp_name, path)
        raise
    finally:
        for _, temporary in staged:
            temporary.unlink(missing_ok=True)


class UsageDataStore:
    def __init__(self, history_path: Path, quota_path: Path, token_path: Path, machine_id: str, account_id_resolver, lock, account_mapper=None, cache_path: Path | None = None, account_revision_resolver=None):
        self.history_path, self.quota_path, self.token_path = Path(history_path), Path(quota_path), Path(token_path)
        self.cache_path = Path(cache_path) if cache_path is not None else default_usage_sync_cache_path(self.history_path)
        self.machine_id, self.account_id_resolver, self.lock, self.account_mapper = machine_id, account_id_resolver, lock, account_mapper
        self.account_revision_resolver = account_revision_resolver
        self.conflicts = []
        self.needs_remote_rebuild = not self.cache_path.exists()
        self._local_datasets_cache = None
        self._merged_datasets_cache = None
        self._account_revision = None
        self._cache_repair_needed = False

    def _account_id(self, row: dict) -> str:
        return sync_meta(row).get("accountId") or self.account_id_resolver(row.get("accountSlotId"))

    @staticmethod
    def _history_bytes(rows: list[dict]) -> bytes:
        from monitor_history import format_history_row, grouped_delta_event_rows, is_delta_event_row
        output = grouped_delta_event_rows(rows) if rows and all(is_delta_event_row(row) for row in rows) else rows
        return "".join(format_history_row(row) + "\n" for row in output).encode()

    @staticmethod
    def _quota_bytes(rows: list[dict]) -> bytes:
        from monitor_history import normalize_quota_history_row
        return "".join(json.dumps(normalized, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows if (normalized := normalize_quota_history_row(row)) is not None).encode()

    @staticmethod
    def _token_bytes(rows: list[dict]) -> bytes:
        from monitor_history import normalize_token_session_row
        return "".join(json.dumps(normalized, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows if (normalized := normalize_token_session_row(row)) is not None).encode()

    def _load(self) -> tuple[list[dict], list[dict], list[dict]]:
        from monitor_history import load_history, load_quota_history, load_token_session_history
        return load_history(self.history_path), load_quota_history(self.quota_path), load_token_session_history(self.token_path)

    def _load_cache(self) -> tuple[dict[tuple[str, str], dict], dict[str, dict[str, str]]]:
        try:
            payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}, {}
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Cannot read synchronized usage cache: {exc}") from exc
        if not isinstance(payload, dict) or payload.get("version") not in {1, CACHE_VERSION} or not isinstance(payload.get("records"), list):
            raise ValueError("Unsupported or invalid synchronized usage cache")
        if payload.get("version") == 1:
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
        records = {}
        for entry in payload["records"]:
            if not isinstance(entry, dict) or not isinstance(entry.get("sourceMachineId"), str) or not entry["sourceMachineId"] or not isinstance(entry.get("key"), str):
                raise ValueError("Invalid synchronized usage cache entry")
            validate_sync_operation({"action": "upsert", "key": entry["key"], "record": entry.get("record")})
            if sync_meta(entry["record"]["row"]).get("originMachineId") != entry["sourceMachineId"]:
                raise ValueError("Synchronized usage cache origin does not match its record")
            pack_id, pack_hash = entry.get("sourcePackId"), entry.get("sourcePackHash")
            if (pack_id is None) != (pack_hash is None) or pack_id is not None and packs.get(entry["sourceMachineId"], {}).get(pack_id) != pack_hash:
                self.needs_remote_rebuild = True
                self._cache_repair_needed = True
                continue
            records[(entry["sourceMachineId"], entry["key"])] = entry
        represented = {(entry["sourceMachineId"], entry.get("sourcePackId")) for entry in records.values() if entry.get("sourcePackId") is not None}
        for machine_id, inventory in list(packs.items()):
            for pack_id in list(inventory):
                if (machine_id, pack_id) not in represented:
                    del inventory[pack_id]
                    self.needs_remote_rebuild = True
                    self._cache_repair_needed = True
            if not inventory:
                packs.pop(machine_id)
        return records, packs

    @staticmethod
    def _cache_bytes(records: dict[tuple[str, str], dict], packs: dict[str, dict[str, str]]) -> bytes:
        return canonical_json({"version": CACHE_VERSION, "packs": {machine_id: packs[machine_id] for machine_id in sorted(packs)}, "records": [records[key] for key in sorted(records)]}) + b"\n"

    def _map_account(self, row: dict) -> dict | None:
        account_id = sync_meta(row).get("accountId")
        if not account_id or self.account_mapper is None:
            return None
        mapped = self.account_mapper(account_id, row.get("accountSlotId"), row.get("accountLabel"))
        if mapped is None:
            return None
        slot_id, label = mapped
        return row | {"accountSlotId": slot_id, "accountLabel": label}

    def _materialize_datasets(self, local: tuple[list[dict], list[dict], list[dict]], cache: dict[tuple[str, str], dict]) -> None:
        mapped = [(record["kind"], row) for entry in cache.values() if (record := entry["record"]) and (row := self._map_account(record["row"])) is not None]
        tokens, self.conflicts = merge_token_rows(local[2] + [row for kind, row in mapped if kind == "token"])
        self._local_datasets_cache = local
        self._merged_datasets_cache = (
            merge_cost_rows(local[0] + [row for kind, row in mapped if kind == "cost"]),
            merge_quota_rows(local[1] + [row for kind, row in mapped if kind == "quota"]),
            tokens,
        )
        self._account_revision = self.account_revision_resolver() if self.account_revision_resolver is not None else None

    @staticmethod
    def _transport_record(kind: str, row: dict) -> dict:
        return {"kind": kind, "row": {key: value for key, value in row.items() if key not in {"accountSlotId", "accountLabel"}}}

    def _write(self, history: list[dict], quota: list[dict], tokens: list[dict], cache: dict[tuple[str, str], dict], packs: dict[str, dict[str, str]]) -> None:
        transactional_replace([(self.history_path, self._history_bytes(history)), (self.quota_path, self._quota_bytes(quota)), (self.token_path, self._token_bytes(tokens)), (self.cache_path, self._cache_bytes(cache, packs))])

    def _normalize_local(self) -> tuple[list[dict], list[dict], list[dict]]:
        history, quota, tokens = self._load()
        cache, packs = self._load_cache()
        cache_changed = not self.cache_path.exists() or self._cache_repair_needed
        def normalize(kind: str, row: dict, local_only: bool = False) -> dict | None:
            nonlocal cache_changed
            meta = sync_meta(row)
            if meta.get("originMachineId") and meta["originMachineId"] != self.machine_id:
                key = record_key(kind, row)
                cache[(meta["originMachineId"], key)] = {"sourceMachineId": meta["originMachineId"], "key": key, "record": self._transport_record(kind, row)}
                cache_changed = True
                self.needs_remote_rebuild = True
                return None
            account_id = self._account_id(row)
            if str(meta.get("accountId") or "").startswith("local:"):
                resolved = self.account_id_resolver(row.get("accountSlotId"))
                if not str(resolved).startswith("local:"):
                    row = {key: value for key, value in row.items() if key != SYNC_META_KEY}
                    account_id = resolved
            return add_record_provenance(kind, row, self.machine_id, account_id, local_only) if not sync_meta(row).get("recordId") else row
        normalized_history = [normalized for row in history if (normalized := normalize("cost", row, not is_cost_interval_row(row))) is not None]
        normalized_quota = [normalized for row in quota if (normalized := normalize("quota", row)) is not None]
        normalized_tokens = [normalized for row in tokens if (normalized := normalize("token", row)) is not None]
        if cache_changed or (normalized_history, normalized_quota, normalized_tokens) != (history, quota, tokens):
            self._write(normalized_history, normalized_quota, normalized_tokens, cache, packs)
            self._cache_repair_needed = False
        local = normalized_history, normalized_quota, normalized_tokens
        if cache_changed or local != self._local_datasets_cache or self._merged_datasets_cache is None:
            self._materialize_datasets(local, cache)
        return local

    def normalize_local(self) -> tuple[list[dict], list[dict], list[dict]]:
        with self.lock:
            return self._normalize_local()

    def refresh_accounts(self) -> None:
        with self.lock:
            if self._local_datasets_cache is None:
                self._normalize_local()
            else:
                self._materialize_datasets(self._load(), self._load_cache()[0])

    def _datasets(self, view: str) -> tuple[list[dict], list[dict], list[dict]]:
        if self._local_datasets_cache is None or self._merged_datasets_cache is None:
            self._normalize_local()
        elif self.account_revision_resolver is not None and self.account_revision_resolver() != self._account_revision:
            self._materialize_datasets(self._local_datasets_cache, self._load_cache()[0])
        return self._merged_datasets_cache if view == "merged" else self._local_datasets_cache

    def datasets(self, view: str = "local") -> tuple[list[dict], list[dict], list[dict]]:
        with self.lock:
            return self._datasets("merged" if view == "merged" else "local")

    def snapshot(self, necessary_only: bool = True) -> tuple[dict[str, dict], set[str]]:
        with self.lock:
            history, quota, tokens = self._normalize_local()
            local = active_records(history, quota, tokens, self.machine_id, necessary_only)
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
            transactional_replace([(self.cache_path, self._cache_bytes(cache, packs))])
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
                transactional_replace([(self.cache_path, self._cache_bytes(cache, packs))])
                self._materialize_datasets(local, cache)
            return self.conflicts
