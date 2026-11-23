#!/usr/bin/env python3

import hashlib
import json
import math

from monitor_common import empty_cost_totals, empty_token_totals, parse_timestamp


SYNC_META_KEY = "sync"
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


def canonical_ledger_row(row: dict) -> dict:
    from monitor_token_ledger import canonical_token_ledger_rows
    canonical_token_ledger_rows([row])
    return row


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
