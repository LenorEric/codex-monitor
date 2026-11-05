"""Migration-only contract-19 decoders. Runtime calculation accepts event records only."""

import json
from contextlib import closing
from monitor_common import empty_cost_totals, empty_token_totals, parse_timestamp
from monitor_usage_sync import _validate_totals, content_hash, sync_meta

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
