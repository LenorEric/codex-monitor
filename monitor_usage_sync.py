#!/usr/bin/env python3

import hashlib
import heapq
import json
import math
import monitor_sample_store as samples
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


def default_usage_sync_cache_path(data_path: Path) -> Path:
    data_path = Path(data_path)
    return data_path.with_name("usage_monitor_sync_cache.json") if data_path.name in {"usage_monitor_history.jsonl", "usage_monitor_quota_history.jsonl", "usage_monitor_token_ledger.jsonl", "usage_monitor_quota_readings.jsonl", "usage_monitor_token_events.jsonl"} else data_path.with_suffix(".sync-cache.json")


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
    with closing(samples.connect(path)) as db, db:
        db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        samples.prepare(db)
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


class UsageDataStore(NormalizedRecords):
    """Released fact/publication layout retained for ordered migrations and fixtures.

    The daemon uses monitor_canonical.CanonicalDataStore for new runtime data.
    """
    def __init__(self, quota_path: Path, token_ledger_path: Path, machine_id: str, account_id_resolver, lock, account_mapper=None, cache_path: Path | None = None, account_revision_resolver=None):
        self.quota_path, self.token_ledger_path = Path(quota_path), Path(token_ledger_path)
        self.cache_path = Path(cache_path) if cache_path is not None else default_usage_sync_cache_path(self.quota_path)
        self.v4_cache_path = self.cache_path.with_name(f"{self.cache_path.stem}-v4.sqlite3")
        self.machine_id, self.account_id_resolver, self.lock, self.account_mapper = machine_id, account_id_resolver, lock, account_mapper
        self.account_revision_resolver = account_revision_resolver
        self._local_datasets_cache = None
        self._merged_datasets_cache = None
        self._account_revision = None
        from monitor_processing import ProcessingIndex
        initialize_v4_cache(self.v4_cache_path)
        self.index = ProcessingIndex(self.v4_cache_path)
        from monitor_backup import BackupRecorder
        from monitor_ingestion import SourceIngestor
        self.backup = BackupRecorder(self.index)
        self.sources = SourceIngestor(self)
        self._database_revision = None

    def ingest_local(self):
        with self.lock:
            return self._ingest_local()

    def _ingest_local(self):
        # Recorder logs are never ingestion inputs. Refresh only in-memory SQL views.
        revision = self.index.revision()
        changed = revision != self._database_revision
        if changed:
            self._local_datasets_cache = self._merged_datasets_cache = None
            self._database_revision = revision
        return changed

    def record_quota(self, sample):
        from monitor_history import quota_history_rows_from_sample
        rows = [self.normalize_new_quota(row) for row in quota_history_rows_from_sample(sample)]
        if not rows:
            return False
        with self.lock, self.index.transaction() as db:
            from monitor_quota_runs import record_runs
            changed, observations = record_runs(self, db, rows)
            # Publication retains its original point format and identities. Local
            # run extensions must never become edits to previously published rows.
            for row in observations:
                key = record_key("quota", row)
                previous = db.execute("SELECT data FROM processing_pending_records WHERE kind='quota' AND record_key=?", (key,)).fetchone()
                if previous is None:
                    previous = db.execute("SELECT record_json FROM local_records WHERE record_key=?", (key,)).fetchone()
                    previous = json.loads(previous[0])["row"] if previous else None
                else:
                    previous = json.loads(previous[0]) if previous[0] else None
                if previous:
                    row = row | {"windows": previous["windows"] | row["windows"]}
                db.execute("INSERT OR REPLACE INTO processing_pending_records VALUES(?,?,?)", (key, "quota", json.dumps(row)))
            if changed and self.backup.enabled():
                self.backup.enqueue(db, self.quota_path, [{"quotaRunSnapshot": True}])
        self.backup.flush()
        return changed

    def record_ledger(self, rows, *, replace=False):
        rows = [normalized for row in rows if (normalized := self.normalize_new_ledger(row)) is not None]
        with self.lock, self.index.transaction() as db:
            changed = self.index.write_records(db, "tokenLedger", rows, replace=replace, backup_path=self.token_ledger_path if self.backup.enabled() else None)
        self.backup.flush()
        return changed

    def refresh_peer_partition(self, machine_id, day, force=False):
        source = f"peer:{machine_id}:{day}"
        with self.index.transaction() as db, self.index.defer_sessions(db):
            manifest = db.execute("SELECT layout_json FROM remote_days WHERE machine_id=? AND day=?", (machine_id, day)).fetchone()
            revision = [manifest[0] if manifest else None, self.account_revision_resolver() if self.account_revision_resolver else None,
                self.index.get(db, f"remoteDayRevision:{machine_id}:{day}", 0)]
            if not force and self.index.get(db, f"peerRevision:{source}", "uninitialized") == revision:
                return
            previous_revision = self.index.get(db, f"peerRevision:{source}")
            remap = force or previous_revision is None or previous_revision[1] != revision[1]
            device_id = samples._dimension(db, "device", machine_id)
            def remove(position):
                previous = db.execute("SELECT kind,record_key,account FROM processing_facts WHERE source=? AND position=?", (source, position)).fetchone()
                if previous:
                    db.execute("DELETE FROM processing_facts WHERE source=? AND position=?", (source, position))
                    self.index.dirty(db, previous[2], "peer update", ("merged",))
                    if previous[0] == "tokenLedger":
                        self.index._effective(db, previous[1])
            for (position,) in list(db.execute(
                "SELECT f.position FROM processing_facts f WHERE f.source=? AND NOT EXISTS ("
                "SELECT 1 FROM remote_record_refs r WHERE r.device_id=? AND r.day=? AND r.record_key=f.record_key)",
                (source, device_id, day),
            )):
                remove(position)
            max_fact = db.execute("SELECT COALESCE(MAX(position),-1) FROM processing_facts WHERE source=?", (source,)).fetchone()[0]
            max_peer = db.execute("SELECT COALESCE(MAX(peer_position),-1) FROM remote_record_refs WHERE device_id=? AND day=?", (device_id, day)).fetchone()[0]
            position = max(max_fact, max_peer) + 1
            for key, peer_pos, data in db.execute(
                "SELECT r.record_key,x.peer_position,r.record_json FROM remote_records r "
                "JOIN remote_record_refs x ON x.device_id=? AND x.record_key=r.record_key "
                "WHERE r.machine_id=? AND r.day=? AND (? OR x.peer_position IS NULL) ORDER BY r.record_key",
                (device_id, machine_id, day, remap),
            ):
                existing = db.execute("SELECT position FROM processing_facts WHERE source=? AND record_key=? LIMIT 1", (source, key)).fetchone()
                had_previous = existing is not None or (peer_pos is not None and peer_pos >= 0)
                target = existing[0] if existing is not None else (peer_pos if peer_pos is not None and peer_pos >= 0 else position)
                record = json.loads(data)["record"]
                row = self._map_account(record["kind"], record["row"]) if record.get("kind") in {"quota", "tokenLedger"} else None
                if had_previous:
                    remove(target)
                if row is not None:
                    self.index.fact(db, source, target, record["kind"], row)
                    new_peer_pos = target
                    if not had_previous:
                        position += 1
                else:
                    new_peer_pos = -1
                db.execute("UPDATE remote_record_refs SET peer_position=? WHERE device_id=? AND record_key=?", (new_peer_pos, device_id, key))
            db.execute("DELETE FROM processing_ledger_sessions WHERE source=?", (source,))
            self.index.put(db, f"peerRevision:{source}", revision)
            self.index.put(db, "factRevision", self.index.get(db, "factRevision", 0) + 1)
        self._merged_datasets_cache = None

    def initialize_processing(self, *, ingest=True):
        if ingest:
            self.ingest_local()
        with self.index.transaction() as db:
            initialized = self.index.get(db, "peersInitialized", False)
            peers = list(db.execute("SELECT machine_id,day FROM remote_days")) if not initialized else []
        for machine_id, day in peers:
            self.refresh_peer_partition(machine_id, day)
        if not initialized:
            with self.index.transaction() as db:
                self.index.put(db, "peersInitialized", True)

    def initialize_v4_index(self, now=None, *, ingest=True):
        if ingest:
            self.ingest_local()
        current = time.time() if now is None else now
        with self.index.transaction() as db:
            historical = db.execute("SELECT value FROM metadata WHERE name='localInitialized'").fetchone() is None
            initialized = self.index.get(db, "localFactsInitialized", False)
            if not initialized:
                db.execute("INSERT OR IGNORE INTO processing_pending_records SELECT l.record_key,s.kind,NULL "
                    "FROM local_record_refs l JOIN sample_records s ON s.id=l.sample_id "
                    "WHERE s.kind<>'quota' AND NOT EXISTS(SELECT 1 FROM processing_facts f WHERE f.record_key=l.record_key AND f.source NOT LIKE 'peer:%')")
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
                samples.put_local(db, key, period, digest, record)
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
        return projected | {"accountSlotId": slot_id, "accountLabel": label}

    def _materialize_datasets(self, local: tuple[list[dict], list[dict]]) -> None:
        v4_records, _ = self._v4_remote_cache()
        mapped = [(record["kind"], row) for entry in v4_records if entry.get("sourceMachineId") != self.machine_id and (record := entry["record"]) and record.get("kind") in {"quota", "tokenLedger"} and (row := self._map_account(record["kind"], record["row"])) is not None]
        local_ledger = []
        for source in local[1]:
            if (row := canonical_ledger_row(source)) is None:
                continue
            session = row.get("session") or {}
            account_id = self.account_id_resolver(row.get("accountSlotId") or session.get("accountSlotId"))
            local_ledger.append(add_record_provenance("tokenLedger", row, self.machine_id, account_id))
        ledger = merge_token_ledger_rows(local_ledger + [row for kind, row in mapped if kind == "tokenLedger"])
        self._local_datasets_cache = local
        self._merged_datasets_cache = merge_quota_rows(local[0] + [row for kind, row in mapped if kind == "quota"]), ledger
        self._account_revision = self.account_revision_resolver() if self.account_revision_resolver is not None else None

    def refresh_accounts(self) -> None:
        with self.lock:
            with self.index.transaction() as db:
                peers = list(db.execute("SELECT machine_id,day FROM remote_days"))
            self._local_datasets_cache = self._merged_datasets_cache = None
            for machine_id, day in peers:
                self.refresh_peer_partition(machine_id, day, force=True)

    def rename_account(self, account_id, label):
        from monitor_history import replace_account_label
        with self.lock, self.index.transaction() as db:
            for path, kind in ((self.quota_path, "quota"), (self.token_ledger_path, "tokenLedger")):
                rows = [json.loads(row[0]) for row in db.execute("SELECT data FROM processing_facts WHERE source=? AND kind=?", (f"local:{kind}", kind))]
                for row in rows:
                    replace_account_label(row, account_id, label)
                self.index.write_records(db, kind, rows, replace=True, backup_path=path if kind == "tokenLedger" and self.backup.enabled() else None)
                if kind == "quota" and self.backup.enabled():
                    self.backup.enqueue(db, path, [{"quotaRunSnapshot": True}])
        self.backup.flush()
        self.refresh_accounts()

    def _datasets_locked(self, view):
        self._ingest_local()
        if self._local_datasets_cache is None or self._merged_datasets_cache is None:
            self._materialize_datasets((self.index.rows("quota"), self.index.rows("tokenLedger")))
        elif self.account_revision_resolver is not None and self.account_revision_resolver() != self._account_revision:
            self._materialize_datasets(self._local_datasets_cache)
        return self._merged_datasets_cache if view == "merged" else self._local_datasets_cache

    def datasets(self, view: str = "local") -> tuple[list[dict], list[dict]]:
        with self.lock:
            return self._datasets_locked(view)

    def snapshot(self) -> tuple[dict[str, dict], set[str]]:
        """Read original publication records, independently of local quota runs."""
        with self.lock:
            with self.index.transaction() as db:
                local = {key: json.loads(data) for key, data in db.execute("SELECT record_key,record_json FROM local_records")}
                for key, kind, data in db.execute("SELECT record_key,kind,data FROM processing_pending_records"):
                    if data is None:
                        local.pop(key, None)
                    elif syncable_record(kind, row := json.loads(data), self.machine_id):
                        local[key] = self._transport_record(kind, row)
            return local, set(local)

    def remove_origins_not_in(self, authoritative_machine_ids) -> int:
        with self.index.transaction() as db:
            removed = [row[0] for row in db.execute("SELECT machine_id FROM remote_origins") if row[0] not in set(authoritative_machine_ids)]
        for machine in removed:
            self.v4_remove_days(machine, set(self.v4_cached_days(machine)))
        with self.index.transaction() as db:
            db.executemany("DELETE FROM remote_origins WHERE machine_id=?", ((machine,) for machine in removed))
        self._local_datasets_cache = self._merged_datasets_cache = None
        return len(removed)

    def _v4_remote_cache(self) -> tuple[list[dict], set[str]]:
        initialize_v4_cache(self.v4_cache_path)
        with closing(samples.connect(self.v4_cache_path)) as db, db:
            origins = {row[0] for row in db.execute("SELECT machine_id FROM remote_origins")}
            return [{"sourceMachineId": machine_id, "key": key, "record": json.loads(record_json)["record"]} for machine_id, key, record_json in db.execute("SELECT machine_id, record_key, record_json FROM remote_records")], origins

    def v4_local_snapshot(self, now: float | None = None) -> dict[str, dict]:
        """Read the publication index, independently of local quota compaction."""
        with self.lock:
            self.initialize_v4_index(now, ingest=False)
            with self.index.transaction() as db:
                days = {}
                for key, period, record_json in db.execute("SELECT record_key, period, record_json FROM local_records ORDER BY period, record_key"):
                    day = days.setdefault(period[:10], {"parts": {}})
                    day["parts"].setdefault(period, []).append(v4_record_envelope(self.machine_id, key, json.loads(record_json), period))
                return days

    def v4_cached_days(self, machine_id: str) -> dict[str, dict]:
        with self.lock:
            initialize_v4_cache(self.v4_cache_path)
            with closing(samples.connect(self.v4_cache_path)) as db, db:
                return {day: {"logicalHash": digest, "layout": json.loads(layout), "reference": self.index.get(db, f"remoteReference:{machine_id}:{day}")}
                    for day, digest, layout in db.execute("SELECT day, logical_hash, layout_json FROM remote_days WHERE machine_id=?", (machine_id,))}

    def v4_apply_day(self, machine_id: str, manifest: dict, downloaded: dict[str, list[dict]], force: bool = False, reference=None) -> None:
        with self.lock:
            day, parts = manifest["day"], manifest["parts"]
            initialize_v4_cache(self.v4_cache_path)
            with closing(samples.connect(self.v4_cache_path)) as db, db:
                previous = db.execute("SELECT layout_json FROM remote_days WHERE machine_id=? AND day=?", (machine_id, day)).fetchone()
                old_parts = json.loads(previous[0]) if previous else {}
                if force or previous is None or old_parts != parts or downloaded:
                    device_id = samples._dimension(db, "device", machine_id)
                    prior_peers = {
                        key: (digest, pos)
                        for key, digest, pos in db.execute(
                            "SELECT record_key,content_hash,peer_position FROM remote_record_refs WHERE device_id=? AND day=? AND peer_position IS NOT NULL",
                            (device_id, day),
                        )
                    }
                    replace_day = force or previous is None or not old_parts or manifest.get("layout") == "packed" or "bulk" in parts or "bulk" in old_parts
                    if replace_day:
                        db.execute("DELETE FROM remote_records WHERE machine_id=? AND day=?", (machine_id, day))
                    else:
                        for part in (set(old_parts) - set(parts)) | set(downloaded):
                            db.execute("DELETE FROM remote_records WHERE machine_id=? AND day=? AND part_id=?", (machine_id, day, part))
                    for part, entries in downloaded.items():
                        for entry in entries:
                            validate_v4_envelope(entry, machine_id)
                            digest = content_hash(entry["record"])
                            samples.put_remote(db, machine_id, day, part, entry["recordKey"], digest, entry)
                            prior = prior_peers.get(entry["recordKey"])
                            if prior and prior[0] == digest:
                                db.execute("UPDATE remote_record_refs SET peer_position=? WHERE device_id=? AND record_key=?", (prior[1], device_id, entry["recordKey"]))
                db.execute("INSERT INTO remote_origins(machine_id) VALUES(?) ON CONFLICT DO NOTHING", (machine_id,))
                db.execute("INSERT INTO remote_days(machine_id, day, logical_hash, layout_json) VALUES(?,?,?,?) ON CONFLICT(machine_id, day) DO UPDATE SET layout_json=excluded.layout_json",
                    (machine_id, day, "", canonical_json(parts).decode()))
                if reference is not None:
                    self.index.put(db, f"remoteReference:{machine_id}:{day}", reference)
                if downloaded:
                    self.index.put(db, f"remoteDayRevision:{machine_id}:{day}", self.index.get(db, f"remoteDayRevision:{machine_id}:{day}", 0) + 1)
            self.refresh_peer_partition(machine_id, day)

    def v4_remove_days(self, machine_id: str, days: set[str]) -> None:
        with self.lock:
            initialize_v4_cache(self.v4_cache_path)
            with closing(samples.connect(self.v4_cache_path)) as db, db:
                for day in days:
                    db.execute("DELETE FROM remote_records WHERE machine_id=? AND day=?", (machine_id, day))
                    db.execute("DELETE FROM remote_days WHERE machine_id=? AND day=?", (machine_id, day))
                    db.execute("DELETE FROM processing_meta WHERE name IN (?,?)", (f"remoteReference:{machine_id}:{day}", f"remoteDayRevision:{machine_id}:{day}"))
                db.execute("INSERT INTO remote_origins(machine_id) VALUES(?) ON CONFLICT DO NOTHING", (machine_id,))
            for day in days:
                self.refresh_peer_partition(machine_id, day)
