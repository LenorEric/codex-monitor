"""Migration-only contract-19 decoders. Runtime calculation accepts event records only."""

import math
import json
import re
from contextlib import closing
from monitor_common import empty_cost_totals, empty_token_totals, parse_timestamp
from monitor_usage_sync import (MAX_SYNC_RECORD_BYTES, _validate_totals, _validate_quota_row, _validate_sync_value,
    canonical_json, content_hash, record_key, sync_meta, v4_period, v4_record_envelope)

TOKEN_KEYS = tuple(empty_token_totals())


def historical_transport_record(kind, row):
    """Frozen transport conversion for released migration 15 only."""
    transport = {key: value for key, value in row.items() if key not in {"accountSlotId", "accountLabel", "sourceTotals"}}
    if row.get("recordType") == "sessionBaseline" and isinstance(transport.get("session"), dict):
        transport["session"] = {key: value for key, value in transport["session"].items() if key not in {"accountSlotId", "accountLabel"}}
    return {"kind": kind, "row": transport}


def _session_identity(row):
    if not isinstance(row, dict):
        return (None, None, None)
    session = row["session"] if isinstance(row.get("session"), dict) else row
    return tuple(value if isinstance(value, str) else None for value in (sync_meta(row).get("originMachineId"), sync_meta(row).get("accountId"), session.get("sessionId")))


def _retired(row):
    return not isinstance(row, dict) or row.get("recordType") != "usage" or any(key in row for key in ("sourceCoverage", "sourceTotals"))


def _current_event(row):
    from monitor_token_ledger import canonical_token_ledger_rows
    try:
        canonical_token_ledger_rows([row])
        if not all(isinstance(row.get(key), str) and row[key] for key in ("eventId", "sessionId", "rawModel", "billingModel")):
            return False
        if parse_timestamp(row.get("occurredAt")) is None or row.get("serviceTier") not in {"default", "fast"}:
            return False
        _validate_totals(row.get("tokens"), empty_token_totals(), True, "token")
        _validate_totals(row.get("cost"), empty_cost_totals(), False, "cost")
        return True
    except (TypeError, ValueError, AttributeError):
        return False


def migrate_event_only(paths):
    """Contract 20: convert from retained SQL events or discard; never read recorder logs."""
    import monitor_sample_store as samples
    from monitor_processing import ProcessingIndex, initialize_processing_index
    from monitor_token_ledger import _usage_records
    from monitor_usage_sync import initialize_v4_cache
    initialize_v4_cache(paths["processing_index"])
    initialize_processing_index(paths["processing_index"])
    with closing(samples.connect(paths["processing_index"])) as db, db:
        days = {period[:10] for (period,) in db.execute("SELECT DISTINCT period FROM local_records")}
        db.execute("CREATE TEMP TABLE event_migration_samples AS SELECT id FROM sample_records WHERE kind='tokenLedger'")
        affected = {_session_identity(row) for (sample,) in db.execute("SELECT id FROM event_migration_samples")
            if _retired(row := samples.select_sample(db, sample))}
        local = [json.loads(data) for (data,) in db.execute("SELECT data FROM processing_facts WHERE kind='tokenLedger' AND source NOT LIKE 'peer:%' ORDER BY position")]
        templates = {_session_identity(row): row for row in reversed(local) if isinstance(row, dict) and _session_identity(row) in affected and _session_identity(row)[2]}
        previous = {row.get("eventId"): row for row in local if isinstance(row, dict) and row.get("recordType") == "usage"}
        rebuilt = {}
        for identity, template in templates.items():
            events = [json.loads(data) for (data,) in db.execute("SELECT data FROM processing_scan_events WHERE session=? AND valid_refs>0 ORDER BY event_at,event_id", (identity[2],))]
            # A raw session alone cannot attribute a previously hidden event across multiple accounts.
            events = [event for event in events if (_session_identity(previous[event["eventId"]]) == identity if event.get("eventId") in previous
                else sum(key[2] == identity[2] for key in templates) == 1)]
            session = template["session"] if isinstance(template.get("session"), dict) else template
            for row in _usage_records(events, session.get("accountSlotId") or "unknown", session.get("accountLabel") or "Unknown", []):
                owner = previous.get(row["eventId"], template)
                row.update({key: owner[key] for key in ("sync", "accountSlotId", "accountLabel") if key in owner})
                if _current_event(row):
                    rebuilt[row["eventId"]] = row
        for (sample,) in db.execute("SELECT id FROM event_migration_samples"):
            row = samples.select_sample(db, sample)
            if _session_identity(row) in affected:
                row = rebuilt.get(row.get("eventId")) if isinstance(row, dict) and _session_identity(row) in templates and row.get("recordType") == "usage" else None
            if row is not None and _current_event(row):
                replacement, _ = samples.insert_sample(db, "tokenLedger", row)
                for table in ("processing_fact_refs", "local_record_refs", "remote_record_refs"):
                    db.execute(f"UPDATE {table} SET sample_id=? WHERE sample_id=?", (replacement, sample))
            else:
                for table in ("processing_fact_refs", "local_record_refs", "remote_record_refs"):
                    db.execute(f"DELETE FROM {table} WHERE sample_id=?", (sample,))
        # Remove all derived interpretations, including accounts that lost their final fact.
        for table in ("effective", "ledger_sessions", "coverage", "ledger_order", "pending_records", "display", "projections", "quota_points", "cost_ranges", "changes", "day_cache"):
            db.execute(f"DELETE FROM processing_{table}")
        db.execute("UPDATE remote_record_refs SET peer_position=NULL")
        db.execute("DROP TABLE IF EXISTS processing_peer_rows")
        db.execute(
            "DELETE FROM processing_scan_events WHERE valid_refs<=0 "
            "AND NOT EXISTS(SELECT 1 FROM processing_occurrences o WHERE o.event_id=processing_scan_events.event_id)"
        )
        delivery_tables = sorted(
            [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'delivery_%'").fetchall()],
            key=lambda name: (name.endswith("_payloads"), name),
        )
        for table in delivery_tables:
            db.execute(f'DELETE FROM "{table}"')
        db.execute("DELETE FROM processing_meta WHERE name IN ('projectionRule','localFactsInitialized','peersInitialized','displayRevision','runtimeState','consoleState') OR name LIKE 'ledgerCost:%' OR name LIKE 'peerRevision:%' OR name LIKE 'ledgerNotifications:%'")
        db.execute("UPDATE processing_scan_events SET ledger_done=0")
        for (home,) in db.execute("SELECT DISTINCT home FROM processing_scan_sources").fetchall():
            db.execute("INSERT OR REPLACE INTO processing_meta VALUES(?, 'true')", (f"reconcile:{home}",))
        for account, in db.execute("SELECT DISTINCT account FROM processing_facts").fetchall():
            for view in ("local", "merged"):
                db.execute("INSERT OR REPLACE INTO processing_dirty VALUES(?,?,?)", (view, account, "event-only migration"))
        # Hashes describe the complete transport record, not only its shared sample.
        for key, data in db.execute("SELECT record_key,record_json FROM local_records").fetchall():
            db.execute("UPDATE local_record_refs SET content_hash=? WHERE record_key=?", (content_hash(json.loads(data)), key))
        for machine, key, data in db.execute("SELECT machine_id,record_key,record_json FROM remote_records").fetchall():
            db.execute("UPDATE remote_record_refs SET content_hash=? WHERE device_id=(SELECT id FROM sample_devices WHERE value=?) AND record_key=?", (content_hash(json.loads(data)["record"]), machine, key))
        generation = json.loads(db.execute("SELECT value FROM processing_meta WHERE name='publishGeneration'").fetchone()[0]) if db.execute("SELECT 1 FROM processing_meta WHERE name='publishGeneration'").fetchone() else 0
        # Include empty days: their previous cloud records must also disappear.
        for day in days | {day for (day,) in db.execute("SELECT day FROM processing_dirty_days")}:
            db.execute("INSERT OR REPLACE INTO processing_dirty_days VALUES(?,?)", (day, generation + 1))
        db.execute("INSERT OR REPLACE INTO processing_meta VALUES('publishGeneration',?)", (str(generation + 1),))
        db.execute("INSERT OR REPLACE INTO processing_meta VALUES('eventOnlyInitialized','false')")
        db.execute("INSERT OR IGNORE INTO sample_releases SELECT id FROM event_migration_samples")
        db.execute("DROP TABLE event_migration_samples")
    index = ProcessingIndex(paths["processing_index"])
    with index.transaction() as db, index.defer_sessions(db):
        for row in rebuilt.values():
            index.write_records(db, "tokenLedger", [row])
        db.execute("DELETE FROM processing_effective")
        db.execute("DELETE FROM processing_ledger_sessions")
        db.execute("DELETE FROM processing_meta WHERE name LIKE 'ledgerCost:%'")
        for source, kind, key, data in db.execute("SELECT source,kind,record_key,data FROM processing_facts WHERE kind='tokenLedger'").fetchall():
            index._effective(db, key)
            if not source.startswith("peer:"):
                index._ledger_add(db, source, json.loads(data))
                db.execute("INSERT OR REPLACE INTO processing_pending_records VALUES(?,?,?)", (key, kind, data))
        index.put(db, "factRevision", index.get(db, "factRevision", 0) + 1)
        index.put(db, "eventOnlyInitialized", True)
    if "cloud_state" in paths and paths["cloud_state"].exists():
        from monitor_migrations import read_json, write_json
        state = read_json(paths["cloud_state"])
        state["usageV4"] = {"remote": {}, "localHeadEtag": None, "transferByMonth": state.get("usageV4", {}).get("transferByMonth", {})}
        write_json(paths["cloud_state"], state)


def finish_cloud_event_migration(client):
    """Discard obsolete backup artifacts without restoring cloud contents."""
    client.delete("migrations/contract-4")


def migrate_cloud_event_only(client):
    from monitor_cloud import CloudError, CryptoBox
    from monitor_migrations import convert_reset_timestamps
    try:
        descriptor = client.get("crypto.json")[0]
    except CloudError as exc:
        if exc.http_status == 404:
            return
        raise
    manager = getattr(client, "_migration_manager", None)
    if manager is None:
        raise CloudError("Event migration requires the configured monitor", 409)
    box = CryptoBox(manager.config()["webdav"]["encryptionPassphraseHash"], json.loads(descriptor))
    import sqlite3
    import time
    from monitor_usage_packs import HistoricalPackWriter, UsageObjectReader
    today = v4_period(time.time())[:10]
    for filename in manager._optional_remote_list(client, "usage/v4/heads"):
        if not filename.endswith(".enc"):
            continue
        machine = filename[:-4]
        if re.fullmatch(r"[A-Za-z0-9_-]{1,128}", machine) is None:
            raise CloudError("Invalid event migration machine ID", 409)
        head, etag = manager._v4_head(client, box, machine)
        catalog = manager._v4_catalog(client, box, machine, head)
        affected = set()
        # SQLite owns and removes this temporary database when the connection closes.
        with closing(sqlite3.connect("")) as db:
            db.execute("CREATE TABLE records (record_key TEXT PRIMARY KEY, day TEXT NOT NULL, period TEXT NOT NULL, data BLOB NOT NULL)")
            with UsageObjectReader(manager, client, box, machine) as reader:
                for day, reference in sorted(catalog.items()):
                    for entry in reader.entries(day, reference):
                        validate_v4_envelope(entry, machine)
                        db.execute("INSERT INTO records VALUES(?,?,?,?)", (entry["recordKey"], day, entry["collectionPeriod"], canonical_json(entry)))
                        record = entry.get("record", {})
                        if record.get("kind") == "tokenLedger" and _retired(record.get("row", {})):
                            affected.add(_session_identity(record["row"]))
            # A retained source can affect legacy events in any other day partition.
            for key, data in db.execute("SELECT record_key,data FROM records"):
                entry = json.loads(data)
                record, row = entry["record"], entry["record"].get("row", {})
                if record.get("kind") == "tokenLedger" and (_session_identity(row) in affected or not _current_event(row)):
                    db.execute("DELETE FROM records WHERE record_key=?", (key,))
                    continue
                record = convert_reset_timestamps(record)
                if record.get("kind") == "quota" and "compaction" in row:
                    record = record | {"row": {name: value for name, value in record["row"].items() if name != "compaction"}}
                if record != entry["record"]:
                    entry = entry | {"record": record, "contentSha256": content_hash(record)}
                    db.execute("UPDATE records SET data=? WHERE record_key=?", (canonical_json(entry), key))
            db.execute("CREATE INDEX chronological_records ON records(period,record_key)")
            days = [day for day, in db.execute("SELECT DISTINCT day FROM records ORDER BY day")]
            legacy_hashes = {day: {"logicalHash": reference["logicalHash"]} for day, reference in catalog.items() if "logicalHash" in reference}
            with HistoricalPackWriter(manager, client, box, machine) as writer:
                for day, data in db.execute("SELECT day,data FROM records WHERE day<? ORDER BY period,record_key", (today,)):
                    writer.add(day, json.loads(data), data)
                updated_catalog = {day: reference | legacy_hashes.get(day, {}) for day, reference in writer.finish().items()}
            active, manifests = None, {}
            for day in (day for day in days if day >= today):
                parts = {}
                periods = [period for period, in db.execute("SELECT DISTINCT period FROM records WHERE day=? ORDER BY period", (day,))] if day == today else ["bulk"]
                for period in periods:
                    query, params = ("SELECT data FROM records WHERE day=? ORDER BY record_key", (day,)) if period == "bulk" else (
                        "SELECT data FROM records WHERE period=? ORDER BY record_key", (period,))
                    parts[period] = manager._put_usage_payload(client, box, "v4/data", machine,
                        {"version": 1, "machineId": machine, "day": day, "partId": period, "records": [json.loads(data) for data, in db.execute(query, params)]})[0]
                layout = "parts" if day == today else "bulk"
                manifest = {"version": 1, "machineId": machine, "day": day, "layout": layout, "parts": parts} | legacy_hashes.get(day, {})
                manifests[day] = manifest
                digest = manager._put_usage_payload(client, box, "v4/days", machine, manifest)[0]
                reference = {"manifestId": digest, "layout": layout} | legacy_hashes.get(day, {})
                if day == today:
                    active = {"day": day, **reference}
                else:
                    updated_catalog[day] = reference
            old_historical = {day: reference for day, reference in catalog.items() if reference["layout"] != "parts"}
            final_catalog = updated_catalog | ({active["day"]: {key: value for key, value in active.items() if key != "day"}} if active else {})
            if updated_catalog == old_historical and active == head.get("activeDay"):
                manager._v4_gc(client, box, machine, head, catalog=final_catalog, manifests=manifests)
                continue
            months = {}
            for month in sorted({day[:7] for day in updated_catalog}):
                refs = {day: ref for day, ref in updated_catalog.items() if day[:7] == month}
                value = {"version": 2 if any(ref["layout"] == "packed" for ref in refs.values()) else 1, "machineId": machine, "month": month, "days": refs}
                months[month] = manager._put_usage_payload(client, box, "v4/months", machine, value)[0]
                manager._v4_month_cache[(machine, months[month])] = value
            updated = head | {"generation": head["generation"] + 1, "months": months, "activeDay": active}
            manager._v4_write_head(client, box, updated, etag)
            manager._v4_gc(client, box, machine, updated, catalog=final_catalog, manifests=manifests)


def validate_source_coverage(coverage):
    if not isinstance(coverage, dict):
        raise ValueError("Invalid token source coverage")
    for model, tiers in coverage.items():
        if not isinstance(model, str) or not model or not isinstance(tiers, dict) or not set(tiers).issubset({"default", "fast"}):
            raise ValueError("Invalid token source coverage")
        for totals in tiers.values():
            if not isinstance(totals, dict) or set(totals) != set(TOKEN_KEYS) or any(type(value) is not int or value < 0 for value in totals.values()):
                raise ValueError("Invalid token source coverage")


def _validate_token_ledger_row(row: dict) -> None:
    if row.get("schemaVersion") != 2:
        raise ValueError("Invalid synchronized token ledger schema")
    meta = sync_meta(row)
    if meta.get("version") != 1 or not isinstance(meta.get("originMachineId"), str) or not meta["originMachineId"] or not isinstance(meta.get("accountId"), str) or not meta["accountId"].startswith("v3:"):
        raise ValueError("Invalid synchronized token ledger provenance")
    if meta.get("localOnly") not in {None, False}:
        raise ValueError("Local-only token ledger records cannot be synchronized")
    if "sourceCoverage" in row:
        validate_source_coverage(row["sourceCoverage"])
    if row.get("recordType") == "usage":
        if (
            not isinstance(row.get("eventId"), str) or not row["eventId"] or not isinstance(row.get("sessionId"), str) or not row["sessionId"] or parse_timestamp(row.get("occurredAt")) is None
            or not isinstance(row.get("rawModel"), str) or not row["rawModel"] or not isinstance(row.get("billingModel"), str) or not row["billingModel"] or row.get("serviceTier") not in {"default", "fast"}
        ):
            raise ValueError("Invalid synchronized token usage record")
        _validate_totals(row.get("tokens"), empty_token_totals(), integral=True, label="token")
        _validate_totals(row.get("cost"), empty_cost_totals(), integral=False, label="cost")
    elif row.get("recordType") == "sessionBaseline":
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


def validate_v4_envelope(entry: dict, machine_id: str) -> None:
    from monitor_usage_sync import require_usage_fields, validate_v4_envelope as validate_download
    require_usage_fields(entry, sourceMachineId=str, recordKey=str, kind=str, schemaVersion=int,
        record=dict, collectionPeriod=str, eventAt=(str, type(None)))
    require_usage_fields(entry["record"], kind=str)
    if entry["record"]["kind"] == "tokenLedger":
        require_usage_fields(entry["record"], row=dict)
        if entry["record"]["row"].get("recordType") == "sessionBaseline":
            require_usage_fields(entry["record"]["row"], session=dict, sync=dict)
            return
    validate_download(entry, machine_id)



# Coverage decoding is needed only while applying released migration 15.
from monitor_tokens import normalize_saved_token_totals
from monitor_token_ledger import _add_tokens

def _subtract_tokens(current: dict | None, previous: dict | None) -> dict:
    current, previous = normalize_saved_token_totals(current), normalize_saved_token_totals(previous)
    return normalize_saved_token_totals({key: max(0, current[key] - previous[key]) for key in TOKEN_KEYS})


def _tier_totals(value: dict) -> dict[str, dict]:
    fast = normalize_saved_token_totals(value.get("fastTokens"))
    return {"default": _subtract_tokens(value.get("tokens"), fast), "fast": fast}


def _source_highwaters(rows: list[dict]) -> dict[tuple[str, str, str], dict]:
    result = {}
    for row in rows:
        if row.get("recordType") == "sessionBaseline" or row.get("sourceCoverage"):
            session = row.get("session") or row
            session_id = str(session.get("sessionId") or "")
            source_totals = row.get("sourceCoverage") or row.get("sourceTotals")
            if not isinstance(source_totals, dict):
                by_model = session.get("byModel") or {"gpt-5.5": {"tokens": session.get("tokens"), "cost": session.get("cost")}}
                source_totals = {model: _tier_totals(value) for model, value in by_model.items() if isinstance(value, dict)}
            for model, tiers in source_totals.items():
                for tier, totals in (tiers or {}).items():
                    key = (session_id, str(model), str(tier))
                    value = normalize_saved_token_totals(totals)
                    result[key] = {name: max(value[name], result.get(key, {}).get(name, 0)) for name in TOKEN_KEYS}
    return result
