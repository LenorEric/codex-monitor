"""Own local data upgrades, journal recovery and old-format conversion.

Released local steps are immutable. Frozen local historical codecs remain separate implementation
helpers; application modules only invoke the entry points here.
"""

import base64
import hashlib
import hmac
import json
import os
import secrets
import shutil
import tempfile
import uuid
import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path

from monitor_common import atomic_write_bytes, parse_timestamp
import monitor_sample_store as samples


DATA_CONTRACT_VERSION = 27
DATA_MIGRATION_STATE_FILENAME = "usage_monitor_data_contract.json"
DATA_MIGRATION_JOURNAL_FILENAME = "usage_monitor_data_migration.json"
from monitor_cloud_contract import CLOUD_DATA_CONTRACT_VERSION, CLOUD_DATA_CONTRACT_PATH


class MigrationError(RuntimeError):
    pass


def request_migration_rebuild(paths: dict[str, Path], reason: str, *, sampling: bool = False, delivery_only: bool = False) -> None:
    """Explicitly queue one rebuild for the final representation of a cascade.

    The request commits with the migration's protected database, so an interrupted
    later step does not lose it. delivery_only preserves quota sampling/projections;
    a full or sampling request in the same cascade takes precedence.
    """
    if "processing_index" not in paths or not paths["processing_index"].exists():
        return  # A fresh store has no derived results to preserve.
    with closing(samples.connect(paths["processing_index"])) as db, db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_meta'").fetchone():
            return
        row = db.execute("SELECT value FROM processing_meta WHERE name='migrationRebuildPending'").fetchone()
        pending = json.loads(row[0]) if row else {"reasons": [], "sampling": False, "projection": False}
        if reason not in pending["reasons"]:
            pending["reasons"].append(reason)
        pending["projection"] = pending.get("projection", True) or not delivery_only or sampling
        pending["sampling"] |= sampling
        db.execute("INSERT OR REPLACE INTO processing_meta VALUES('migrationRebuildPending',?)", (json.dumps(pending),))


def _apply_migration_rebuild(paths: dict[str, Path]) -> None:
    """Publish requested work once, after all ordered migrations have succeeded."""
    if not paths["processing_index"].exists():
        return
    with closing(samples.connect(paths["processing_index"])) as db, db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_meta'").fetchone():
            return
        row = db.execute("SELECT value FROM processing_meta WHERE name='migrationRebuildPending'").fetchone()
        if row is None:
            return
        pending = json.loads(row[0])
        for view in ("local", "merged") if pending.get("projection", True) else ():
            db.execute("INSERT OR REPLACE INTO processing_dirty SELECT ?,account,? FROM processing_facts GROUP BY account", (view, "; ".join(pending["reasons"])))
            if pending["sampling"] and db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_sampling_changes'").fetchone():
                db.execute("INSERT OR IGNORE INTO processing_sampling_changes SELECT ?,account,'' FROM processing_facts WHERE kind='quota' GROUP BY account", (view,))
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_meta'").fetchone():
            from monitor_delivery import request_attribution_rebuild
            request_attribution_rebuild(db)
        db.execute("DELETE FROM processing_meta WHERE name='migrationRebuildPending'")


def _with_migration_rebuild(migration, reason: str, *, sampling: bool = False):
    """Keep released conversion steps intact while declaring their rebuild needs."""
    def run(paths):
        migration(paths)
        request_migration_rebuild(paths, reason, sampling=sampling)
    return run


@contextmanager
def _preserve_projection_work(db):
    """Rekeying identical facts must not turn valid projections into rebuild work."""
    for table in ("dirty", "changes", "sampling_changes"):
        db.execute(f"CREATE TEMP TABLE migration_saved_{table} AS SELECT * FROM processing_{table}")
    try:
        yield
    finally:
        for table in ("dirty", "changes", "sampling_changes"):
            db.execute(f"DELETE FROM processing_{table}")
            db.execute(f"INSERT INTO processing_{table} SELECT * FROM migration_saved_{table}")
            db.execute(f"DROP TABLE migration_saved_{table}")


def validate_contract_metadata(metadata: dict) -> None:
    for field, target in (("dataContractVersion", DATA_CONTRACT_VERSION), ("cloudDataContractVersion", CLOUD_DATA_CONTRACT_VERSION)):
        if type(metadata.get(field)) is not int or metadata[field] != target:
            raise MigrationError(f"Package {field} must match contract target {target}")
    if set(DATA_MIGRATIONS) != set(range(1, DATA_CONTRACT_VERSION + 1)) or not all(callable(step) for step in DATA_MIGRATIONS.values()):
        raise MigrationError(f"Local migration registry must contain every step from 1 through {DATA_CONTRACT_VERSION}")


def installed_data_contract_version(runtime_dir: Path, fallback: int = DATA_CONTRACT_VERSION) -> int:
    for path in (Path(runtime_dir) / "version.json", Path(runtime_dir) / "package.json"):
        try:
            version = json.loads(path.read_text(encoding="utf-8")).get("dataContractVersion")
            if not isinstance(version, int) or isinstance(version, bool) or version < 0:
                raise MigrationError(f"Invalid data contract version: {version!r}")
            return version
        except FileNotFoundError:
            pass
        except (OSError, AttributeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MigrationError(f"Cannot read local program metadata: {exc}") from exc
    return fallback


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _json_records(path: Path) -> list[dict]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    decoder, records, offset = json.JSONDecoder(), [], 0
    while offset < len(text):
        while offset < len(text) and text[offset].isspace():
            offset += 1
        if offset >= len(text):
            break
        record, offset = decoder.raw_decode(text, offset)
        if not isinstance(record, dict):
            raise MigrationError(f"History file contains a non-object record: {path}")
        records.append(record)
    return records


def _jsonl_bytes(records: list[dict]) -> bytes:
    return (("\n".join(json.dumps(record, ensure_ascii=False, separators=(",", ":")) for record in records) + "\n") if records else "").encode("utf-8")


def _json_records_with_torn_tail(path: Path) -> list[dict]:
    try:
        content = path.read_bytes()
    except FileNotFoundError:
        return []
    try:
        return _json_records(path)
    except (UnicodeDecodeError, json.JSONDecodeError):
        if content.endswith((b"\n", b"\r")) or b"\n" not in content:
            raise
        temporary = path.with_name(f".{path.name}.recover-{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_bytes(content[:content.rfind(b"\n") + 1])
            return _json_records(temporary)
        finally:
            temporary.unlink(missing_ok=True)


def _migrate_history_to_v1(paths: dict[str, Path]) -> None:
    from monitor_migration_history import normalize_quota_history_row, normalize_token_session_row, parse_timestamp
    from monitor_migration_ledger import load_token_ledger

    for path in (paths[name] for name in ("history", "quota_history", "token_session_history", "token_ledger", "sample_log")):
        if path.exists():
            _json_records(path)
    if (path := paths["quota_history"]).exists():
        rows = []
        for row in _json_records(path):
            if (normalized := normalize_quota_history_row(row)) is None:
                raise MigrationError("Quota history contains an unsupported row")
            rows.append(normalized)
        rows.sort(key=lambda row: (parse_timestamp(row["checkedAt"]) is None, parse_timestamp(row["checkedAt"]) or 0, row["checkedAt"], row["accountSlotId"]))
        _atomic_write(path, _jsonl_bytes(rows))
    if (path := paths["token_session_history"]).exists():
        rows = []
        for row in _json_records(path):
            if (normalized := normalize_token_session_row(row)) is None:
                raise MigrationError("Token session history contains an unsupported row")
            rows.append(normalized)
        _atomic_write(path, _jsonl_bytes(rows))
    if (path := paths["token_ledger"]).exists():
        _atomic_write(path, _jsonl_bytes(load_token_ledger(path)))
    if (path := paths["sample_log"]).exists():
        _atomic_write(path, _jsonl_bytes(_json_records(path)))


def _migrate_history_to_v2(paths: dict[str, Path]) -> None:
    from monitor_migration_history import normalize_token_session_row
    from monitor_migration_ledger import normalize_saved_token_totals
    from monitor_migration_ledger import append_token_ledger, legacy_baselines_from_sessions, load_token_ledger, token_sessions_from_ledger

    ledger = load_token_ledger(paths["token_ledger"])
    sessions = []
    for row in _json_records(paths["token_session_history"]):
        if (normalized := normalize_token_session_row(row)) is None:
            raise MigrationError("Token session history contains an unsupported row")
        sessions.append(normalized)
    represented = {(str(row.get("sessionId")), str(row.get("accountSlotId"))) for row in token_sessions_from_ledger(ledger)}
    missing = [row for row in sessions if (str(row.get("sessionId")), str(row.get("accountSlotId"))) not in represented]
    append_token_ledger(paths["token_ledger"], [row | {"schemaVersion": 1} for row in legacy_baselines_from_sessions(missing)])
    regenerated = {(str(row.get("sessionId")), str(row.get("accountSlotId"))): normalize_token_session_row(row) for row in token_sessions_from_ledger(load_token_ledger(paths["token_ledger"]))}
    if any((normalized := normalize_token_session_row(row)) is None or regenerated.get((str(row.get("sessionId")), str(row.get("accountSlotId")))) != normalized for row in missing):
        raise MigrationError("Token session migration verification failed")
    def covers(rebuilt_values, source_values) -> bool:
        return all(float((rebuilt_values or {}).get(key) or 0) + 1e-8 >= float(value or 0) for key, value in (source_values or {}).items())

    for source in sessions:
        rebuilt = regenerated.get((str(source.get("sessionId")), str(source.get("accountSlotId"))))
        if rebuilt is None or not covers(normalize_saved_token_totals(rebuilt.get("tokens")), normalize_saved_token_totals(source.get("tokens"))) or not covers(rebuilt.get("cost"), source.get("cost")):
            raise MigrationError("Token session migration does not cover all historical token totals")
        for model, source_model in (source.get("byModel") or {}).items():
            rebuilt_model = (rebuilt.get("byModel") or {}).get(model)
            if not isinstance(rebuilt_model, dict) or not covers(normalize_saved_token_totals(rebuilt_model.get("tokens")), normalize_saved_token_totals(source_model.get("tokens"))) or not covers(rebuilt_model.get("cost"), source_model.get("cost")) or not covers(normalize_saved_token_totals(rebuilt_model.get("fastTokens")), normalize_saved_token_totals(source_model.get("fastTokens"))):
                raise MigrationError("Token session migration does not cover all historical model totals")
    if paths["quota_history"].exists() or paths["token_ledger"].exists():
        paths["history"].unlink(missing_ok=True)
    paths["token_session_history"].unlink(missing_ok=True)
    paths["dashboard_cache"].unlink(missing_ok=True)
    paths["usage_sync_cache"].unlink(missing_ok=True)
    state_path = paths["runtime_state"]
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        state = None
    if state is not None:
        if not isinstance(state, dict):
            raise MigrationError("Invalid monitor runtime state")
        for key in ("runCostUsd", "runCostByModelUsd", "measuredCostIntervals", "hasRuntimeCostBaseline", "_pendingCostIntervals", "_specialEvents"):
            state.pop(key, None)
        for window in (state.get("windows") or {}).values():
            for key in tuple(window):
                if "Cost" in key or key.startswith("baseline") or key.startswith("rollback") or key.startswith("backwardReset") or key == "awaitingTrustedPercentBaseline":
                    window.pop(key, None)
        if isinstance(state.get("lastSample"), dict):
            for key in ("costDelta", "costDeltaByModel", "eventCostUsd", "eventCostByModelUsd", "eventCostReady"):
                state["lastSample"].pop(key, None)
        _atomic_write(state_path, (json.dumps(state, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
    try:
        cloud_state = json.loads(paths["cloud_state"].read_text(encoding="utf-8"))
    except FileNotFoundError:
        cloud_state = None
    if cloud_state is not None:
        if not isinstance(cloud_state, dict) or not isinstance(cloud_state.get("usage"), dict):
            raise MigrationError("Invalid cloud runtime state")
        cloud_state["usage"].update({"remote": {}, "lastSuccessAt": None, "lastAttemptAt": None, "failure": None})
        cloud_state["usage"].pop("lastFullVerificationAt", None)
        cloud_state["usage"].pop("pendingLegacyPurge", None)
        _atomic_write(paths["cloud_state"], (json.dumps(cloud_state, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))


def _migrate_history_to_v3(paths: dict[str, Path]) -> None:
    from monitor_history import normalize_quota_history_row
    from monitor_migration_ledger import canonical_token_ledger_rows, load_token_ledger, token_sessions_from_ledger, write_token_ledger

    quota_rows = []
    for row in _json_records_with_torn_tail(paths["quota_history"]):
        if (normalized := normalize_quota_history_row(row)) is None:
            raise MigrationError("Quota history contains an unsupported row")
        quota_rows.append(normalized)
    ledger = load_token_ledger(paths["token_ledger"])
    canonical = canonical_token_ledger_rows(ledger)
    if token_sessions_from_ledger(ledger) != token_sessions_from_ledger(canonical):
        raise MigrationError("Token ledger v3 normalization verification failed")
    if paths["quota_history"].exists():
        _atomic_write(paths["quota_history"], _jsonl_bytes(quota_rows))
    if paths["token_ledger"].exists():
        write_token_ledger(paths["token_ledger"], canonical)
    if paths["quota_history"].exists() or paths["token_ledger"].exists():
        paths["history"].unlink(missing_ok=True)
    paths["token_session_history"].unlink(missing_ok=True)
    paths["dashboard_cache"].unlink(missing_ok=True)
    try:
        cloud_state = json.loads(paths["cloud_state"].read_text(encoding="utf-8"))
    except FileNotFoundError:
        cloud_state = None
    if cloud_state is not None:
        if not isinstance(cloud_state, dict) or not isinstance(cloud_state.get("usage"), dict):
            raise MigrationError("Invalid cloud runtime state")
        cloud_state["usage"].pop("lastFullVerificationAt", None)
        cloud_state["usage"].pop("pendingLegacyPurge", None)
        _atomic_write(paths["cloud_state"], (json.dumps(cloud_state, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))


def _migrate_history_to_v4(paths: dict[str, Path]) -> None:
    for legacy_name, current_name in (
        ("legacy_quota_history", "quota_history"), ("legacy_token_ledger", "token_ledger"), ("legacy_sample_log", "sample_log"),
    ):
        source = paths.get(legacy_name)
        if source is None or not source.exists():
            continue
        target = paths[current_name]
        source_rows = _json_records(source)
        if target.exists():
            _atomic_write(target, _jsonl_bytes(source_rows + _json_records(target)))
            source.unlink()
        else:
            os.replace(source, target)


def _migrate_history_to_v5(paths: dict[str, Path], codex_home: Path | None = None) -> None:
    if codex_home is None:
        raise MigrationError("Codex session directory is required for token ledger migration")
    from monitor_migration_ledger import load_token_ledger, sync_token_ledger, positional_event_ids
    from monitor_tokens import scan_codex_token_usage

    scan = scan_codex_token_usage(codex_home)
    if scan.get("errors"):
        raise MigrationError("Cannot reconcile token ledger while session scanning is incomplete")
    complete_sessions = set(scan["completeSessionIds"])
    if not complete_sessions:
        return
    ledger = load_token_ledger(paths["token_ledger"])
    machine_path = paths["token_ledger"].parent / "machine.json"
    machine_id = json.loads(machine_path.read_text(encoding="utf-8")).get("machineId") if machine_path.exists() else None
    if machine_id is None:
        origins = {(row.get("sync") or {}).get("originMachineId") for row in ledger if row.get("recordType") == "usage" and (row.get("sync") or {}).get("originMachineId")}
        machine_id = next(iter(origins)) if len(origins) == 1 else None
    timeline = []
    account_manifest = paths["token_ledger"].parent / "accounts" / "accounts.json"
    if account_manifest.exists():
        manifest = json.loads(account_manifest.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict) or not isinstance(manifest.get("activationHistory", []), list):
            raise MigrationError("Cannot read account attribution history during token ledger migration")
        timeline.extend(row for row in manifest.get("activationHistory", []) if isinstance(row, dict) and row.get("checkedAt") and row.get("accountSlotId"))
    timeline.extend(
        {"checkedAt": row["checkedAt"], "accountSlotId": row["accountSlotId"], "accountLabel": row.get("accountLabel")}
        for row in _json_records_with_torn_tail(paths["quota_history"])
        if row.get("checkedAt") and row.get("accountSlotId") and (not row.get("originMachineId") or row.get("originMachineId") == machine_id)
    )
    timeline.extend(
        {"checkedAt": row["occurredAt"], "accountSlotId": row.get("accountSlotId"), "accountLabel": row.get("accountLabel")}
        for row in ledger
        if row.get("recordType") == "usage" and row.get("occurredAt") and row.get("accountSlotId")
        and (not (row.get("sync") or {}).get("originMachineId") or (row.get("sync") or {}).get("originMachineId") == machine_id)
    )
    from monitor_common import parse_timestamp
    timeline.sort(key=lambda row: parse_timestamp(row.get("checkedAt")) or 0)
    fallback = timeline[-1] if timeline else {}
    sync_token_ledger(
        paths["token_ledger"], [], scan["events"], str(fallback.get("accountSlotId") or "unknown"), str(fallback.get("accountLabel") or "Unknown"),
        timeline, source_files=scan["sourceFiles"], legacy_event_ids=positional_event_ids(codex_home, complete_sessions), own_machine_id=machine_id,
    )
    paths["dashboard_cache"].unlink(missing_ok=True)


def _migrate_history_to_v6(paths: dict[str, Path]) -> None:
    from monitor_migration_samples import initialize_v4_cache

    cache = paths["usage_sync_cache"]
    initialize_v4_cache(cache.with_name(f"{cache.stem}-v4.sqlite3"))
    if paths["cloud_state"].exists():
        state = json.loads(paths["cloud_state"].read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            raise MigrationError("Invalid cloud state during usage v4 migration")
        state["usageV4"] = {"remote": {}, "localHeadEtag": None, "transferByMonth": {}}
        _atomic_write(paths["cloud_state"], (json.dumps(state, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))


def _migrate_history_to_v7(paths: dict[str, Path]) -> None:
    # Derived quota baselines are rebuilt from the retained raw readings on startup.
    paths["dashboard_cache"].unlink(missing_ok=True)


def _migrate_history_to_v8(paths: dict[str, Path]) -> None:
    from monitor_migration_samples import initialize_processing_index
    initialize_processing_index(paths["processing_index"])
    # Raw facts and v4 periods/peer records are preserved; derived state is rebuilt on first use.
    paths["dashboard_cache"].unlink(missing_ok=True)


def _migrate_history_to_v9(paths: dict[str, Path]) -> None:
    import sqlite3
    from contextlib import closing
    from monitor_migration_samples import initialize_processing_index
    from monitor_common import parse_timestamp
    initialize_processing_index(paths["processing_index"])
    with closing(samples.connect(paths["processing_index"])) as db, db:
        for view, key, data in db.execute("SELECT view,record_key,data FROM processing_display WHERE kind='tokenSessions' AND event_at IS NULL").fetchall():
            row = json.loads(data)
            db.execute("UPDATE processing_display SET event_at=? WHERE view=? AND kind='tokenSessions' AND record_key=?",
                (parse_timestamp(row.get("updatedAt") or row.get("startedAt")), view, key))
    # Existing exact quota points and sessions rebuild delivery artifacts automatically in the maintenance worker.
    # Canonical files, v4 peer ownership, collection periods and parser checkpoints are preserved.


def _migrate_history_to_v10(paths: dict[str, Path]) -> None:
    import sqlite3
    from contextlib import closing
    from monitor_migration_samples import initialize_processing_index
    initialize_processing_index(paths["processing_index"])
    with closing(samples.connect(paths["processing_index"])) as db, db:
        for table in ("display", "projections", "quota_points", "rank_nodes", "cost_ranges"):
            db.execute(f"DELETE FROM processing_{table}")
        for table in ("days", "parts", "day_meta", "blobs", "manifests", "rollups"):
            db.execute(f"DELETE FROM dashboard_{table}")
        db.execute("DELETE FROM processing_meta WHERE name IN ('projectionRule','displayRevision','dashboardDeliveryInitialized') OR name LIKE 'displayStats:%'")
        for (account,) in db.execute("SELECT DISTINCT account FROM processing_facts").fetchall():
            for view in ("local", "merged"):
                db.execute("INSERT OR REPLACE INTO processing_dirty VALUES(?,?,?)", (view, account, "cost time migration"))
    paths["dashboard_cache"].unlink(missing_ok=True)


def _migrate_history_to_v11(paths: dict[str, Path]) -> None:
    import sqlite3
    from contextlib import closing
    from monitor_migration_samples import initialize_processing_index
    initialize_processing_index(paths["processing_index"])
    with closing(samples.connect(paths["processing_index"])) as db, db:
        for table in ("parts", "day_meta", "blobs", "manifests", "rollups"):
            db.execute(f"DELETE FROM dashboard_{table}")
        db.execute("UPDATE dashboard_days SET dirty=1,revision=revision+1")
    paths["dashboard_cache"].unlink(missing_ok=True)


def _migrate_history_to_v12(paths: dict[str, Path]) -> None:
    _migrate_history_to_v10(paths)
    from monitor_accounts import normalized_slot_order
    for name in ("account_manifest", "legacy_account_manifest"):
        if name in paths and paths[name].exists():
            manifest = json.loads(paths[name].read_text(encoding="utf-8"))
            manifest["slotOrder"] = normalized_slot_order(manifest)
            _atomic_write(paths[name], (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))


def _migrate_history_to_v13(paths: dict[str, Path]) -> None:
    import sqlite3
    from contextlib import closing
    from monitor_migration_samples import initialize_processing_index
    initialize_processing_index(paths["processing_index"])
    # Outside a transaction and before workers start. SQLite and the migration backup
    # preserve the original data if conversion/compaction fails or is interrupted.
    with closing(sqlite3.connect(paths["processing_index"], isolation_level=None)) as db:
        db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        db.execute("VACUUM")
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()


def _migrate_history_to_v14(paths: dict[str, Path]) -> None:
    # Cloud contract 2 still provisions sync-lock.json lazily under file ownership.
    # Local sampled storage shares typed payloads; JSONL and derived caches survive.
    from monitor_migration_samples import initialize_processing_index
    migrate_sample_store(paths["processing_index"])
    initialize_processing_index(paths["processing_index"])


def _migrate_history_to_v15(paths: dict[str, Path], codex_home: Path | None = None) -> None:
    migrate_local_formats(paths, codex_home)


def _migrate_history_to_v17(paths: dict[str, Path]) -> None:
    # Retained raw quota facts reconstruct reset boundaries and their cost intervals.
    _migrate_history_to_v10(paths)
    with closing(samples.connect(paths["processing_index"])) as db, db:
        db.execute("DELETE FROM processing_meta WHERE name='samplingRule' OR name LIKE 'samplingState:%' OR name LIKE 'quotaState:%'")
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_sampled_quota'").fetchone():
            db.execute("DELETE FROM processing_sampled_quota")


def _migrate_history_to_v18(paths: dict[str, Path]) -> None:
    _migrate_history_to_v17(paths)
    with closing(samples.connect(paths["processing_index"])) as db, db:
        for (name,) in list(db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'dashboard_%'")):
            db.execute(f'DROP TRIGGER "{name}"')
        for (name,) in list(db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'delivery_%'")):
            db.execute(f'DELETE FROM "{name}"')
        for table in ("days", "parts", "day_meta", "blobs", "manifests", "rollups"):
            db.execute(f"DROP TABLE IF EXISTS dashboard_{table}")
        for name in ("dashboard_quota_time", "dashboard_display_time"):
            db.execute(f"DROP INDEX IF EXISTS {name}")
        from monitor_delivery import initialize_delivery
        initialize_delivery(db)


def _load_v18_token_ledger(path: Path) -> list[dict]:
    """Frozen contract-18 reader used only by the ordered upgrade migration."""
    try:
        content = path.read_bytes()
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise ValueError(f"Cannot read token ledger: {exc}") from exc
    rows = []
    lines = content.splitlines()
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            row = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            if index == len(lines) - 1 and not content.endswith((b"\n", b"\r")):
                break
            raise ValueError(f"Invalid token ledger row {index + 1}: {exc}") from exc
        if not isinstance(row, dict) or row.get("schemaVersion") != 2 or row.get("recordType") not in {"sessionBaseline", "usage"}:
            raise ValueError(f"Unsupported token ledger row {index + 1}")
        rows.append(row)
    return rows


def _migrate_history_to_v19(paths: dict[str, Path]) -> None:
    """Convert complete SQLite data to path-independent local source identities.

    Recorder logs are neither read nor used to supplement or replace SQL facts.
    The migration journal protects the database while its derived state changes.
    """
    from monitor_processing import ProcessingIndex
    from monitor_usage_sync import initialize_v4_cache, record_key
    initialize_v4_cache(paths["processing_index"])
    index = ProcessingIndex(paths["processing_index"])
    conflicting_sources = False
    with index.transaction() as db, index.defer_sessions(db):
        with _preserve_projection_work(db):
            for kind in ("quota", "tokenLedger"):
                rows = [json.loads(row[0]) for row in db.execute("SELECT data FROM processing_facts WHERE kind=? AND source NOT LIKE 'peer:%' ORDER BY source,position", (kind,))]
                identities = {}
                for row in rows:
                    key = record_key(kind, row)
                    conflicting_sources |= key in identities and identities[key] != row
                    identities[key] = row
                # Logical source identities change; the readings and projections do not.
                db.execute("DELETE FROM processing_facts WHERE kind=? AND source NOT LIKE 'peer:%'", (kind,))
                if kind == "tokenLedger":
                    db.execute("DELETE FROM processing_ledger_sessions WHERE source NOT LIKE 'peer:%' AND source NOT LIKE 'view:%'")
                    db.execute("DELETE FROM processing_meta WHERE name LIKE 'ledgerCost:%' AND name NOT LIKE 'ledgerCost:view:%'")
                index.write_records(db, kind, rows)
        # Finish released batches once; new ingestion commits everything together.
        for name, data in list(db.execute("SELECT name,value FROM processing_meta WHERE name LIKE 'ledgerBatch:%'")):
            home, batch = name[len("ledgerBatch:"):], json.loads(data)
            index.write_records(db, "tokenLedger", list(batch["replacements"].values()) + batch["additions"], backup_path=paths["token_ledger"])
            db.executemany("UPDATE processing_scan_events SET ledger_done=1 WHERE event_id=?", ((key,) for key in batch["ids"]))
            db.executemany("INSERT OR REPLACE INTO processing_coverage VALUES(?,?,?,?,?)", ((home, *key, json.dumps(value)) for key, value in batch["coverage"]))
            db.executemany("INSERT OR REPLACE INTO processing_ledger_order VALUES(?,?,?,?)", ((home, key, *value) for key, value in batch["order"].items()))
            if batch["additions"]:
                index.put(db, f"ledgerNotifications:{home}", index.get(db, f"ledgerNotifications:{home}", []) + batch["additions"])
            db.execute("DELETE FROM processing_meta WHERE name=?", (name,))
        db.execute("DELETE FROM processing_blocks WHERE source NOT IN (SELECT path FROM processing_scan_sources)")
        db.execute("DELETE FROM processing_files")
        db.execute("DELETE FROM processing_meta WHERE name='canonicalNormalized'")
        index.put(db, "factRevision", index.get(db, "factRevision", 0) + 1)
        index.put(db, "sqliteAuthoritative", True)
    if conflicting_sources:
        request_migration_rebuild(paths, "conflicting source identities", sampling=True)


def _migrate_history_to_v20(paths: dict[str, Path]) -> None:
    # One pending migration combines all changes since released contract 19.
    if paths["processing_index"].exists():
        with closing(samples.connect(paths["processing_index"])) as db, db:
            samples.migrate_layout(db)
    __import__("monitor_migration_events").migrate_event_only(paths)


def _migrate_history_to_v21(paths: dict[str, Path]) -> None:
    """Retire duplicate projections and queue bounded-distance cost attribution."""
    if not paths["processing_index"].exists():
        return
    with closing(samples.connect(paths["processing_index"])) as db, db:
        # Released migrations still use the empty compatibility schema.
        for action in ("insert", "update", "delete"):
            db.execute(f"DROP TRIGGER IF EXISTS delivery_processing_display_{action}")
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_display'").fetchone():
            db.execute("DELETE FROM processing_display")
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_cost_ranges'").fetchone():
            db.execute("DELETE FROM processing_cost_ranges")
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_projections'").fetchone():
            db.execute("UPDATE processing_projections SET data='{}'")
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_meta'").fetchone():
            db.execute("DELETE FROM processing_meta WHERE name='displayRevision' OR name LIKE 'displayStats:%'")

    request_migration_rebuild(paths, "ten-minute cost attribution limit", delivery_only=True)


DATA_MIGRATIONS = {
    1: _migrate_history_to_v1,
    2: _migrate_history_to_v2,
    3: _migrate_history_to_v3,
    4: _migrate_history_to_v4,
    5: _migrate_history_to_v5,
    6: _migrate_history_to_v6,
    7: _migrate_history_to_v7,
    8: _migrate_history_to_v8,
    9: _migrate_history_to_v9,
    10: _with_migration_rebuild(_migrate_history_to_v10, "cost time migration"),
    11: _migrate_history_to_v11,
    12: _with_migration_rebuild(_migrate_history_to_v12, "cost time migration"),
    13: _migrate_history_to_v13,
    14: _migrate_history_to_v14,
    15: _with_migration_rebuild(_migrate_history_to_v15, "token identity conversion", sampling=True),
    16: _with_migration_rebuild(lambda paths: migrate_reset_timestamps(paths), "reset timestamp conversion", sampling=True),
    17: _with_migration_rebuild(_migrate_history_to_v17, "quota reset boundaries", sampling=True),
    18: _with_migration_rebuild(_migrate_history_to_v18, "quota reset boundaries and delivery format", sampling=True),
    19: _migrate_history_to_v19,
    20: _with_migration_rebuild(_migrate_history_to_v20, "event-only and independent quota storage migration"),
    21: _migrate_history_to_v21,
    22: lambda paths: __import__("monitor_migration_packages").migrate_local(paths),
    23: lambda paths: __import__("monitor_migration_canonical").migrate_local(paths),
    24: lambda paths: __import__("monitor_migration_quota_boundary").migrate_local(paths),
    25: lambda paths: __import__("monitor_migration_zero_quota").migrate_local(paths),
    26: lambda paths: __import__("monitor_migration_delivery").migrate_local(paths),
    27: lambda paths: __import__("monitor_migration_quota_drift").migrate_local(paths),
}


def _restore_data_backup(backup: Path, path: Path) -> None:
    if path.suffix == ".sqlite3":
        import sqlite3
        from contextlib import closing
        with closing(sqlite3.connect(backup)) as source, closing(sqlite3.connect(path)) as target:
            source.backup(target)
            target.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    else:
        os.replace(backup, path)


def _recover_data_migration(journal_path: Path, state_path: Path, paths: dict[str, Path]) -> None:
    try:
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationError(f"Cannot recover data migration: {exc}") from exc
    if not isinstance(journal, dict) or not isinstance(journal.get("version"), int) or isinstance(journal.get("version"), bool) or not isinstance(journal.get("targets"), dict) or not isinstance(journal.get("backups"), dict) or not isinstance(journal.get("existing"), list):
        raise MigrationError("Cannot recover data migration: invalid journal")
    expected_targets = {name: str(path.resolve()) for name, path in paths.items()}
    if journal["version"] in {19, 23, 24, 25, 26} and set(journal["targets"]) == {"processing_index"}:
        expected_targets = {"processing_index": expected_targets["processing_index"]}
    if journal["version"] == 22:
        expected_targets = {name: target for name, target in expected_targets.items() if name in {"processing_index", "config"}}
    if journal["version"] < 15:
        for name in list(expected_targets):
            if name in {"config", "machine", "skills"} or name.startswith(("vault_", "cache_source_")):
                expected_targets.pop(name)
    if journal["version"] < 12:
        expected_targets.pop("account_manifest", None)
        expected_targets.pop("legacy_account_manifest", None)
    if journal["version"] < 8 and "processing_index" not in journal["targets"]:
        expected_targets.pop("processing_index", None)
    base_targets = {name: target for name, target in expected_targets.items() if not name.startswith("legacy_")}
    legacy_targets = {"quota_history": "legacy_quota_history", "token_ledger": "legacy_token_ledger", "sample_log": "legacy_sample_log"}
    valid_base_targets = set(journal["targets"]) == set(base_targets) and all(
        target == base_targets[name] or name in legacy_targets and legacy_targets[name] in expected_targets and target == expected_targets[legacy_targets[name]]
        for name, target in journal["targets"].items()
    )
    if journal["targets"] != expected_targets and not valid_base_targets:
        raise MigrationError("Cannot recover data migration: invalid journal")
    for name, backup in journal["backups"].items():
        if name not in journal["targets"] or not isinstance(backup, str) or Path(backup).parent.resolve() != Path(journal["targets"][name]).parent.resolve():
            raise MigrationError("Cannot recover data migration: invalid backup path")
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        committed = isinstance(state, dict) and isinstance(state.get("dataContractVersion"), int) and state["dataContractVersion"] >= journal["version"]
    except FileNotFoundError:
        committed = False
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationError(f"Cannot recover data migration state: {exc}") from exc
    if not committed:
        for name, target in journal["targets"].items():
            path = Path(target)
            backup = Path(journal["backups"][name]) if name in journal["backups"] else None
            if name in journal["existing"]:
                if backup is not None and backup.exists():
                    _restore_data_backup(backup, path)
                elif not path.exists():
                    raise MigrationError(f"Cannot recover migrated data file: {path}")
            else:
                path.unlink(missing_ok=True)
    for backup in journal["backups"].values():
        Path(backup).unlink(missing_ok=True)
    journal_path.unlink(missing_ok=True)


def _run_data_migration(migration, paths: dict[str, Path], version: int, state_path: Path, journal_path: Path) -> None:
    protected_paths = {"processing_index": paths["processing_index"]} if version == 19 else paths
    if version in {23, 24, 25, 26, 27}:
        protected_paths = {"processing_index": paths["processing_index"]}
    if version == 22:
        protected_paths = {name: paths[name] for name in ("processing_index", "config") if name in paths}
    if version == 15:
        # Historical display/diagnostic logs are not inputs or outputs of v15.
        protected_paths = {name: path for name, path in paths.items() if name not in {"history", "sample_log", "legacy_sample_log"}}
    existing = {name for name, path in protected_paths.items() if path.exists()}
    backups = {}
    try:
        for name in existing:
            descriptor, backup = tempfile.mkstemp(prefix=f".{paths[name].name}.", suffix=".migration-backup", dir=paths[name].parent)
            os.close(descriptor)
            try:
                if paths[name].suffix == ".sqlite3":
                    import sqlite3
                    from contextlib import closing
                    with closing(sqlite3.connect(paths[name])) as source, closing(sqlite3.connect(backup)) as target:
                        source.backup(target)
                else:
                    with paths[name].open("rb") as source, Path(backup).open("wb") as target:
                        shutil.copyfileobj(source, target)
                        target.flush()
                        os.fsync(target.fileno())
            except Exception:
                Path(backup).unlink(missing_ok=True)
                raise
            backups[name] = Path(backup)
        _atomic_write(journal_path, (json.dumps({
            "version": version, "existing": sorted(existing), "backups": {name: str(path.resolve()) for name, path in backups.items()},
            "targets": {name: str(path.resolve()) for name, path in protected_paths.items()},
        }, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
        migration(paths)
        _atomic_write(state_path, (json.dumps({"dataContractVersion": version}, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
    except Exception as exc:
        rollback_errors = []
        for name, path in protected_paths.items():
            try:
                if name in backups:
                    _restore_data_backup(backups[name], path)
                elif name not in existing:
                    path.unlink(missing_ok=True)
            except OSError as rollback_exc:
                rollback_errors.append(f"{path}: {rollback_exc}")
        if rollback_errors:
            raise MigrationError(f"Data migration failed and recovery is incomplete: {'; '.join(rollback_errors)}") from exc
        for backup in backups.values():
            backup.unlink(missing_ok=True)
        journal_path.unlink(missing_ok=True)
        raise
    for backup in backups.values():
        backup.unlink(missing_ok=True)
    journal_path.unlink(missing_ok=True)


def migrate_history_data(
    data_home: Path, history_path: Path, quota_history_path: Path, token_session_history_path: Path, token_ledger_path: Path, sample_log_path: Path,
    target_version: int | None = None, runtime_state_path: Path | None = None, dashboard_cache_path: Path | None = None,
    usage_sync_cache_path: Path | None = None, cloud_state_path: Path | None = None, codex_home: Path | None = None,
) -> list[int]:
    """Migrate every configured history store to the latest data contract once."""
    data_home = Path(data_home)
    data_home.mkdir(parents=True, exist_ok=True)
    state_path = data_home / DATA_MIGRATION_STATE_FILENAME
    journal_path = data_home / DATA_MIGRATION_JOURNAL_FILENAME
    target_version = DATA_CONTRACT_VERSION if target_version is None else target_version
    if not isinstance(target_version, int) or isinstance(target_version, bool) or not 0 <= target_version <= DATA_CONTRACT_VERSION:
        raise MigrationError(f"Unsupported target data contract version: {target_version!r}")
    paths = {
        "history": Path(history_path), "quota_history": Path(quota_history_path), "token_session_history": Path(token_session_history_path),
        "token_ledger": Path(token_ledger_path), "sample_log": Path(sample_log_path), "runtime_state": Path(runtime_state_path) if runtime_state_path is not None else data_home / "usage_monitor_state.json",
        "dashboard_cache": Path(dashboard_cache_path) if dashboard_cache_path is not None else data_home / "usage_monitor_dashboard_cache.json",
        "usage_sync_cache": Path(usage_sync_cache_path) if usage_sync_cache_path is not None else data_home / "usage_monitor_sync_cache.json",
        "cloud_state": Path(cloud_state_path) if cloud_state_path is not None else data_home / "cloud-state.json",
    }
    for name, current_name, legacy_filename, current_filename in (
        ("legacy_quota_history", "quota_history", "usage_monitor_quota_history.jsonl", "usage_monitor_quota_readings.jsonl"),
        ("legacy_token_ledger", "token_ledger", "usage_monitor_token_ledger.jsonl", "usage_monitor_token_events.jsonl"),
        ("legacy_sample_log", "sample_log", "usage_monitor_samples.jsonl", "usage_monitor_diagnostic_samples.jsonl"),
    ):
        if paths[current_name].resolve() == (data_home / current_filename).resolve():
            paths[name] = data_home / legacy_filename
    paths["processing_index"] = paths["usage_sync_cache"].with_name(f"{paths['usage_sync_cache'].stem}-v4.sqlite3")
    paths["account_manifest"] = data_home / "accounts" / "accounts.json"
    if codex_home is not None:
        paths["legacy_account_manifest"] = Path(codex_home) / "usage-monitor-accounts" / "accounts.json"
    paths["config"] = data_home / "config.json"
    paths["machine"] = data_home / "machine.json"
    paths["skills"] = data_home / "skills.json"
    if "legacy_account_manifest" in paths:
        vault = paths["legacy_account_manifest"].parent
        # Stable names are based on relative paths, including files removed before
        # a crash; the journal supplies the validated source list on recovery.
        names = {str(path.relative_to(vault)) for path in vault.rglob("*") if path.is_file()}
        if journal_path.exists():
            journal = json.loads(journal_path.read_text(encoding="utf-8"))
            for name, target in journal.get("targets", {}).items():
                if name.startswith("vault_source_") and Path(target).resolve().is_relative_to(vault.resolve()):
                    names.add(str(Path(target).resolve().relative_to(vault.resolve())))
        for name in sorted(names):
            key = hashlib.sha256(name.encode()).hexdigest()
            paths[f"vault_source_{key}"] = vault / name
            paths[f"vault_target_{key}"] = paths["account_manifest"].parent / name
    shard_root = paths["usage_sync_cache"].with_name(paths["usage_sync_cache"].name + ".d")
    shard_names = {path.name for path in shard_root.glob("*.json")}
    if journal_path.exists():
        for name, target in json.loads(journal_path.read_text(encoding="utf-8")).get("targets", {}).items():
            if name.startswith("cache_source_") and Path(target).parent.resolve() == shard_root.resolve():
                shard_names.add(Path(target).name)
    for name in sorted(shard_names):
        paths["cache_source_" + hashlib.sha256(name.encode()).hexdigest()] = shard_root / name
    _recover_data_migration(journal_path, state_path, paths)
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        current = state.get("dataContractVersion", 0) if isinstance(state, dict) else 0
    except FileNotFoundError:
        current = 0
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationError(f"Cannot read data migration state: {exc}") from exc
    if not isinstance(current, int) or isinstance(current, bool) or current < 0 or current > target_version:
        raise MigrationError(f"Unsupported data contract version: {current!r}")
    completed = []
    for version in range(current + 1, target_version + 1):
        migration = DATA_MIGRATIONS.get(version)
        if migration is None:
            raise MigrationError(f"Missing data migration for contract version {version}")
        if version == 5:
            migration = lambda migration_paths: _migrate_history_to_v5(migration_paths, codex_home)
        if version == 15:
            migration = _with_migration_rebuild(lambda migration_paths: _migrate_history_to_v15(migration_paths, codex_home), "token identity conversion", sampling=True)
        migration_paths = paths
        if version < 15:
            migration_paths = {name: path for name, path in migration_paths.items() if name not in {"config", "machine", "skills"} and not name.startswith(("vault_", "cache_source_"))}
        if version < 12:
            migration_paths = {name: path for name, path in migration_paths.items() if name not in {"account_manifest", "legacy_account_manifest"}}
        if version < 8:
            migration_paths = {name: path for name, path in migration_paths.items() if name != "processing_index"}
        if version < 4:
            migration_paths = {name: path for name, path in migration_paths.items() if not name.startswith("legacy_")}
            for legacy_name, current_name in (("legacy_quota_history", "quota_history"), ("legacy_token_ledger", "token_ledger"), ("legacy_sample_log", "sample_log")):
                if legacy_name in paths and paths[legacy_name].exists():
                    migration_paths[current_name] = paths[legacy_name]
        from monitor_sampling import defer_refresh
        with defer_refresh():
            _run_data_migration(migration, migration_paths, version, state_path, journal_path)
        completed.append(version)
    _apply_migration_rebuild(paths)
    if target_version == DATA_CONTRACT_VERSION and paths["processing_index"].exists():
        if target_version >= 23:
            from monitor_canonical import CanonicalIndex
            CanonicalIndex(paths["processing_index"]).close()
        elif target_version < 22:
            from monitor_processing import ProcessingIndex
            ProcessingIndex(paths["processing_index"])
    return completed


def migrate_installed_data(
    runtime_dir: Path, data_home: Path, quota_history_path: Path | None = None, token_ledger_path: Path | None = None,
    sample_log_path: Path | None = None, target_version: int | None = None, runtime_state_path: Path | None = None,
    dashboard_cache_path: Path | None = None, usage_sync_cache_path: Path | None = None, cloud_state_path: Path | None = None,
    codex_home: Path | None = None,
) -> list[int]:
    from monitor_history import default_history_path, default_quota_history_path, default_sample_log_path
    from monitor_migration_ledger import default_token_ledger_path

    migrate_default_monitor_data(runtime_dir, data_home)
    history = default_history_path(data_home)
    return migrate_history_data(
        data_home, history, quota_history_path or default_quota_history_path(history), Path(data_home) / "usage_monitor_token_sessions.jsonl",
        token_ledger_path or default_token_ledger_path(history), sample_log_path or default_sample_log_path(history),
        installed_data_contract_version(runtime_dir) if target_version is None else target_version, runtime_state_path, dashboard_cache_path, usage_sync_cache_path, cloud_state_path, codex_home,
    )


DEFAULT_DATA_FILES = (
    "usage_monitor_history.jsonl", "usage_monitor_quota_history.jsonl", "usage_monitor_token_sessions.jsonl", "usage_monitor_token_ledger.jsonl", "usage_monitor_samples.jsonl",
    "usage_monitor_quota_readings.jsonl", "usage_monitor_token_events.jsonl", "usage_monitor_diagnostic_samples.jsonl", "usage_monitor_state.json", "usage_monitor_dashboard_cache.json",
)

def migrate_default_monitor_data(legacy_home: Path, data_home: Path | None = None) -> list[str]:
    from monitor_common import codex_switch_home
    data_home = Path(data_home) if data_home is not None else codex_switch_home()
    data_home.mkdir(parents=True, exist_ok=True)
    moved = []
    for name in DEFAULT_DATA_FILES:
        source, target = Path(legacy_home) / name, data_home / name
        if not source.exists() or target.exists() or source.resolve() == target.resolve():
            continue
        fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
        try:
            with source.open("rb") as input_stream, os.fdopen(fd, "wb") as output_stream:
                shutil.copyfileobj(input_stream, output_stream)
                output_stream.flush()
                os.fsync(output_stream.fileno())
            try:
                os.chmod(temp_name, 0o600)
            except OSError:
                pass
            os.replace(temp_name, target)
            source.unlink()
        except Exception:
            try:
                os.unlink(temp_name)
            except OSError:
                pass
            raise
        moved.append(name)
    return moved


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def write_json(path, value):
    atomic_write_bytes(Path(path), (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode(), private=True)


def stable_id(kind, value):
    return "v3:" + hashlib.sha256(f"codex-monitor-usage-account-v3\0{kind}\0{value}".encode()).hexdigest()


def account_identity(account, machine):
    from monitor_accounts import api_identity_id
    identity = account.get("identity") or {}
    if account.get("accountType") == "api" or identity.get("accountType") == "api":
        if value := api_identity_id(identity | {"accountType": "api"}):
            return "api", value
    if identity.get("accountId"):
        return "account", str(identity["accountId"])
    if identity.get("idTokenHash"):
        return "id-token", str(identity["idTokenHash"])
    return "machine", f"{machine}\0{account['id']}"


def convert_verifier(verifier, webdav):
    """Preserve the encryption key and salt; changing either would orphan ciphertext."""
    if not verifier:
        return ""
    from monitor_cloud import normalized_webdav_identity
    parts = verifier.split("$")
    if parts[0] == "scrypt-key-v3":
        return verifier
    if len(parts) != 6 or parts[0] not in {"scrypt-key-v1", "scrypt-key-v2"} or parts[1:4] != ["32768", "8", "1"]:
        raise ValueError("Unsupported saved encryption key")
    if len(base64.b64decode(parts[5], validate=True)) != 32 or not base64.b64decode(parts[4], validate=True):
        raise ValueError("Invalid saved encryption key")
    endpoint = hashlib.sha256(normalized_webdav_identity(webdav.get("baseUrl", ""), webdav.get("username", "")).encode()).hexdigest()
    return "$".join(["scrypt-key-v3", *parts[1:], endpoint])


class FormatConverter:
    def __init__(self, manifest, state, config, machine):
        self.machine = machine
        self.aliases = dict((state.get("formatMigration") or {}).get("accountAliases") or {})
        self.aliases.update((state.get("usage") or {}).get("accountAliases") or {})
        self.slots = {}
        verifier = (config.get("webdav") or {}).get("encryptionPassphraseHash")
        key = base64.b64decode(verifier.split("$")[5], validate=True) if verifier else None
        for account in (manifest or {}).get("accounts", []):
            kind, value = account_identity(account, machine)
            current = stable_id(kind, value)
            self.slots[account["id"]] = current
            identity = account.get("identity") or {}
            old = f"account:{identity['accountId']}" if identity.get("accountId") else f"id-token:{identity['idTokenHash']}" if identity.get("idTokenHash") else f"machine:{machine}:slot:{account['id']}"
            if key:
                self.aliases[hmac.new(hashlib.sha256(key).digest(), f"usage:{old}".encode(), hashlib.sha256).hexdigest()] = current

    def account(self, account, slot=None, origin=None):
        seen = set()
        while account in self.aliases and account not in seen:
            seen.add(account)
            account = self.aliases[account]
        if account and str(account).startswith("v3:"):
            return account
        if slot in self.slots and (not origin or origin == self.machine):
            return self.slots[slot]
        if account:
            # Deleted/unbound accounts retain a deterministic independent identity.
            # Never guess a live account from a mutable display label.
            return stable_id("historical", str(account))
        return stable_id("machine", f"{origin or self.machine}\0{slot or 'unknown'}")

    def row(self, kind, source):
        row = convert_saved_fields(json.loads(json.dumps(source)))
        if kind == "tokenLedger":
            if row.get("schemaVersion") not in {1, 2} or row.get("recordType") not in {"priceEpoch", "legacyBaseline", "sessionBaseline", "usage"}:
                raise ValueError("Unknown token ledger format; originals preserved")
            from monitor_migration_ledger import canonical_token_ledger_rows
            canonical = canonical_token_ledger_rows([row])
            if not canonical:
                return None
            row = canonical[0]
            if row.get("recordType") == "legacyBaseline":
                row["recordType"] = "sessionBaseline"
        meta = row.get("sync") or {}
        session = row.get("session") or row
        slot = session.get("accountSlotId") or row.get("accountSlotId")
        origin = meta.get("originMachineId") or self.machine
        account = self.account(meta.get("accountId") or row.get("usageAccountId") or session.get("usageAccountId"), slot, origin)
        meta = {key: value for key, value in meta.items() if key != "recordId" and (key != "localOnly" or value)} | {"version": 1, "originMachineId": origin, "accountId": account}
        if kind == "quota":
            meta["recordId"] = hashlib.sha256(f"quota:{account}:{row.get('checkedAt')}".encode()).hexdigest()
        if kind == "quota":
            from monitor_history import normalize_quota_history_row
            row = normalize_quota_history_row(row)
            if row is None:
                raise ValueError("Invalid historical quota row; originals preserved")
        row["sync"] = meta
        for target in (row, session):
            if "usageAccountId" in target:
                target["usageAccountId"] = account
        return row

    def record(self, record, origin=None):
        kind = record.get("kind")
        if kind not in {"quota", "tokenLedger", "token", "cost"}:
            return record
        source = record["row"]
        if origin is not None:
            if (source.get("sync") or {}).get("originMachineId", origin) != origin:
                raise ValueError("Historical usage owner mismatch")
            source = source | {"sync": (source.get("sync") or {}) | {"originMachineId": origin}}
        if kind == "token":
            from monitor_migration_history import normalize_token_session_row
            session = normalize_token_session_row(convert_saved_fields(source))
            if session is None:
                raise ValueError("Invalid historical token snapshot")
            row = self.row("tokenLedger", {"schemaVersion": 1, "recordType": "legacyBaseline", "session": session, "sync": source.get("sync", {})})
            return {"kind": "tokenLedger", "row": row}
        if kind == "cost":
            # Derived-only costs remain a current immutable summary. They are not
            # injected as extra token charges: retained canonical usage owns cost.
            row = self.row("costSummary", source)
            row["schemaVersion"], row["recordType"] = 1, "costSummary"
            row["sync"]["recordId"] = hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            return {"kind": "costSummary", "row": row}
        row = self.row(kind, source)
        if row is not None and kind == "tokenLedger":
            convert_historical_event_id(row)
        return record | {"row": row} if row is not None else None


def convert_saved_fields(value):
    if isinstance(value, list):
        return [convert_saved_fields(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {key: convert_saved_fields(item) for key, item in value.items()}
    if "cachedOutputTokens" in result:
        result["cacheWriteInputTokens"] = result.get("cacheWriteInputTokens") or result.pop("cachedOutputTokens")
        result.pop("cachedOutputTokens", None)
        from monitor_migration_ledger import normalize_saved_token_totals
        result = normalize_saved_token_totals(result)
    if "freshOutputTokens" in result:
        result.pop("freshOutputTokens")
    if "events" in result and result.get("window") in {"5h", "7d"} and "delta" not in result:
        result["delta"] = result.pop("events")
    return result


def convert_historical_event_id(row):
    import re
    if row.get("recordType") != "usage" or not re.fullmatch(re.escape(str(row.get("sessionId"))) + r":\d+", str(row.get("eventId"))):
        return
    if row.get("sourceFile"):
        row["historicalSourceFile"] = row.pop("sourceFile")
    identity = [row["sessionId"], row["eventId"], row.get("occurredAt")]
    row["eventId"] = f'{row["sessionId"]}:{hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()[:24]}'


def resolve_unavailable_event_ids(rows, event_ids):
    """Preserve detailed usage when its old positional ID cannot be reconstructed."""
    import re
    from monitor_migration_events import _source_highwaters, _add_tokens
    from monitor_common import empty_token_totals
    unresolved = [row for row in rows if row.get("recordType") == "usage" and row.get("eventId") not in event_ids
        and re.fullmatch(re.escape(str(row.get("sessionId"))) + r":\d+", str(row.get("eventId")))]
    highwaters = _source_highwaters(rows)
    for row in unresolved:
        key = (row["sessionId"], str(row.get("rawModel") or "unknown"), str(row.get("serviceTier") or "default"))
        _add_tokens(highwaters.setdefault(key, empty_token_totals()), row.get("tokens"))
    covered = set()
    for row in unresolved:
        # Keep the full original usage event, including its timestamp and recorded
        # cost. Coverage suppresses the unavailable historical prefix if its source
        # file is later restored; ordinary current events still use their content ID.
        convert_historical_event_id(row)
        if row["sessionId"] not in covered:
            row["sourceCoverage"] = {}
            for (session, model, tier), totals in highwaters.items():
                if session == row["sessionId"]:
                    row["sourceCoverage"].setdefault(model, {})[tier] = totals
            covered.add(row["sessionId"])
    return rows


def migrate_manifest(path):
    from monitor_accounts import default_session_refresh, normalize_session_refresh, normalized_slot_order
    manifest = read_json(path)
    if manifest is None:
        return {}
    if manifest.get("version") not in {1, 2, 3} or not isinstance(manifest.get("accounts"), list):
        raise ValueError("Invalid account manifest during migration")
    for account in manifest["accounts"]:
        account.setdefault("cloud", {"accountKey": None, "keyType": None})
        account["sessionRefresh"] = normalize_session_refresh(account["sessionRefresh"]) if account.get("sessionRefresh") is not None else default_session_refresh(bool(account.get("sessionRefreshEnabled")))
        account.pop("sessionRefreshEnabled", None)
        account.setdefault("accountType", (account.get("identity") or {}).get("accountType", "account"))
        if account.get("configHeader"):
            import tomlkit
            if not account.get("configHeaderToml"):
                account["configHeaderToml"] = tomlkit.dumps(account["configHeader"])
        account.pop("configHeader", None)
        account.setdefault("configHeaderToml", "")
    manifest["version"] = 3
    manifest["slotOrder"] = normalized_slot_order(manifest)
    manifest.setdefault("commonAccountHeaderToml", "")
    write_json(path, manifest)
    return manifest


def cache_entries(path):
    payload = read_json(path)
    if payload is None:
        return []
    if payload.get("version") not in {1, 2, 3}:
        raise ValueError("Unknown usage cache format")
    if payload.get("layout") != "origin-pack-shards-v1":
        if not isinstance(payload.get("records"), list):
            raise ValueError("Invalid usage cache")
        return payload["records"]
    from monitor_usage_sync import content_hash
    result = []
    for origin in payload.get("origins", {}).values():
        for descriptor in origin.get("shards", {}).values():
            name = descriptor["file"]
            if Path(name).name != name:
                raise ValueError("Invalid cache shard path")
            entries = read_json(path.with_name(path.name + ".d") / name)
            if not isinstance(entries, list) or len(entries) != descriptor["recordCount"] or content_hash(entries) != descriptor["contentHash"]:
                raise ValueError("Cache shard verification failed; originals preserved")
            result.extend(entries)
    return result


def migrate_local_formats(paths, codex_home=None):
    from monitor_usage_sync import canonical_json, content_hash, record_key, v4_record_envelope
    import monitor_migration_samples as old_samples
    import monitor_sample_store as samples
    home = paths["config"].parent
    # This reader upgrades configuration only inside the journaled migration.
    _migrate_config_files(home, paths["config"], paths["cloud_state"], paths["machine"])
    for name, source in paths.items():
        if name.startswith("vault_source_") and source.exists():
            target = paths[name.replace("vault_source_", "vault_target_", 1)]
            if target.exists() and target.read_bytes() != source.read_bytes():
                raise ValueError("Conflicting old account vault; originals preserved")
            atomic_write_bytes(target, source.read_bytes(), private=True)
    manifest = migrate_manifest(paths["account_manifest"])
    state, config = read_json(paths["cloud_state"], {}), read_json(paths["config"], {})
    machine = read_json(paths["machine"], {})["machineId"]
    converter = FormatConverter(manifest, state, config, machine)
    def convert_tree(value):
        if isinstance(value, list):
            return [convert_tree(item) for item in value]
        if not isinstance(value, dict):
            return value
        result = {key: convert_tree(item) for key, item in convert_saved_fields(value).items()}
        if isinstance(result.get("sync"), dict) and result["sync"].get("accountId"):
            result["sync"]["accountId"] = converter.account(result["sync"]["accountId"], result.get("accountSlotId"), result["sync"].get("originMachineId"))
        if result.get("usageAccountId"):
            result["usageAccountId"] = converter.account(result["usageAccountId"], result.get("accountSlotId"))
        return result
    # Preserve retired history and diagnostic logs verbatim; neither feeds the conversion.
    if paths["runtime_state"].exists():
        write_json(paths["runtime_state"], convert_tree(read_json(paths["runtime_state"])))
    event_ids = {}
    migrated_events = {}
    if paths["token_ledger"].exists():
        import re
        old_rows = [json.loads(line) for line in paths["token_ledger"].read_text(encoding="utf-8").splitlines() if line.strip()]
        needed = {str(row.get("eventId")) for row in old_rows if row.get("recordType") == "usage" and re.fullmatch(re.escape(str(row.get("sessionId"))) + r":\d+", str(row.get("eventId"))) and (row.get("sync") or {}).get("originMachineId", machine) == machine}
        if needed and codex_home is not None:
            from monitor_tokens import _update_codex_file_state
            for folder in ("sessions", "archived_sessions"):
                for path in (Path(codex_home) / folder).rglob("*.jsonl"):
                    parsed = _update_codex_file_state(path, None, False)
                    if parsed is None:
                        continue
                    for event in parsed["events"]:
                        old = f'{parsed["sessionId"]}:{event["index"]}'
                        if old in needed:
                            identity = [parsed["sessionId"], event["checkedAt"], event.get("cumulative") or event["tokens"]]
                            event_ids[old] = (f'{parsed["sessionId"]}:{hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:24]}', path.name)
    imported = {}
    for name, kind in (("quota_history", "quota"), ("token_ledger", "tokenLedger")):
        if paths[name].exists():
            rows = []
            for line in paths[name].read_text(encoding="utf-8").splitlines():
                if line.strip() and (row := converter.row(kind, json.loads(line))) is not None:
                    original_event = row.get("eventId")
                    if kind == "tokenLedger" and original_event in event_ids:
                        row["eventId"], row["sourceFile"] = event_ids[original_event]
                    if kind == "tokenLedger" and original_event:
                        migrated_events[original_event] = row
                    if row["sync"]["originMachineId"] != machine:
                        if kind == "tokenLedger":
                            convert_historical_event_id(row)
                        key = record_key(kind, row)
                        imported[(row["sync"]["originMachineId"], key)] = {"sourceMachineId": row["sync"]["originMachineId"], "key": key, "record": {"kind": kind, "row": row}}
                    else:
                        rows.append(row)
            if kind == "tokenLedger":
                rows = resolve_unavailable_event_ids(rows, event_ids)

            atomic_write_bytes(paths[name], b"".join(canonical_json(row) + b"\n" for row in rows), private=True)
    entries = cache_entries(paths["usage_sync_cache"])
    converted = imported
    for entry in entries:
        record = converter.record(entry["record"], entry["sourceMachineId"])
        if record is not None:
            key = record_key(record["kind"], record["row"])
            converted[(entry["sourceMachineId"], key)] = {"sourceMachineId": entry["sourceMachineId"], "key": key, "record": record}
    # Keep pre-v4 peer facts as canonical retained records, not a runtime legacy reader.
    if converted and not paths["processing_index"].exists():
        from monitor_usage_sync import initialize_v4_cache
        from monitor_processing import initialize_processing_index
        initialize_v4_cache(paths["processing_index"])
        initialize_processing_index(paths["processing_index"])
    if paths["processing_index"].exists():
        # Stage decoded rows in SQLite, keeping the conversion bounded in memory.
        # The outer journal restores the whole DB if a stage or conversion fails.
        with closing(old_samples.connect(paths["processing_index"])) as db, db:
            old_samples._initialize_legacy_delivery(db)
            for name, query in (
                ("peers", "SELECT source,kind,data FROM processing_facts WHERE source LIKE 'peer:%:legacy'"),
                ("local", "SELECT record_key,period,record_json FROM local_records"),
                ("remote", "SELECT machine_id,day,part_id,record_key,record_json FROM remote_records"),
            ):
                db.execute(f"DROP TABLE IF EXISTS migration15_{name}")
                db.execute(f"CREATE TABLE migration15_{name} AS {query}")
        with closing(samples.connect(paths["processing_index"])) as db, db:
            for table in ("processing_facts", "local_records", "remote_records"):
                db.execute(f"DELETE FROM {table}")
            for _, period, data in db.execute("SELECT record_key,period,record_json FROM migration15_local"):
                record = json.loads(data)
                row = converter.row(record["kind"], record["row"])
                if row is not None:
                    if record["kind"] == "tokenLedger" and row.get("eventId") in migrated_events:
                        row = migrated_events[row["eventId"]]
                    from monitor_migration_events import historical_transport_record
                    record = historical_transport_record(record["kind"], row)
                    samples.put_local(db, record_key(record["kind"], row), period, content_hash(record), record)
            for origin, day, part, original_key, data in db.execute("SELECT machine_id,day,part_id,record_key,record_json FROM migration15_remote"):
                envelope = json.loads(data)
                record = envelope["record"]
                if record.get("kind") not in {"quota", "tokenLedger", "token", "cost"}:
                    samples.put_remote(db, origin, day, part, original_key, content_hash(record), envelope)
                    continue
                record = converter.record(record, origin)
                if record is not None:
                    key = record_key(record["kind"], record["row"])
                    value = v4_record_envelope(origin, key, record, envelope.get("collectionPeriod"))
                    samples.put_remote(db, origin, day, part, key, content_hash(record), value)
            for _, kind, data in db.execute("SELECT source,kind,data FROM migration15_peers"):
                record = converter.record({"kind": kind, "row": json.loads(data)})
                if record is not None:
                    key = record_key(record["kind"], record["row"])
                    origin = record["row"]["sync"]["originMachineId"]
                    converted.setdefault((origin, key), {"sourceMachineId": origin, "key": key, "record": record})
            for name in ("peers", "local", "remote"):
                db.execute(f"DROP TABLE migration15_{name}")
            # Recreate the predicate index for the current baseline tag.
            db.execute("DROP INDEX IF EXISTS processing_baselines")
            db.execute("CREATE INDEX processing_baselines ON processing_fact_refs(source_id,session_id) WHERE subtype='sessionBaseline'")
            # Preserve scanner checkpoints, collection periods and source ownership.
            for table in ("display", "projections", "quota_points", "rank_nodes", "cost_ranges", "effective", "files", "changes", "peer_rows", "ledger_sessions", "pending_records"):
                db.execute(f"DELETE FROM processing_{table}")
            for table in ("days", "parts", "day_meta", "blobs", "manifests", "rollups"):
                db.execute(f"DELETE FROM dashboard_{table}")
            db.execute("DELETE FROM processing_meta WHERE name IN ('projectionRule','displayRevision','dashboardDeliveryInitialized','countsInitialized','peersInitialized','canonicalNormalized','localFactsInitialized') OR name LIKE 'displayStats:%' OR name LIKE 'peerRevision:%'")
            db.execute("UPDATE remote_days SET logical_hash=''")
            # Force one reconciliation from retained parsed events without deleting
            # coverage/checkpoints or repeating already persisted token usage.
            for home, in db.execute("SELECT DISTINCT home FROM processing_scan_sources").fetchall():
                db.execute("INSERT OR REPLACE INTO processing_meta VALUES(?, 'true')", (f"reconcile:{home}",))
            db.execute("DELETE FROM metadata WHERE name='localInitialized'")
            for day, in db.execute("SELECT DISTINCT substr(period,1,10) FROM local_records").fetchall():
                db.execute("INSERT OR REPLACE INTO processing_dirty_days VALUES(?,1)", (day,))
    if converted:
        from monitor_usage_sync import v4_period, v4_logical_hash
        with closing(samples.connect(paths["processing_index"])) as db, db:
            for (origin, key), entry in converted.items():
                record = entry["record"]
                stamp = record["row"].get("checkedAt") or record["row"].get("occurredAt") or (record["row"].get("session") or {}).get("updatedAt")
                period = v4_period(parse_timestamp(stamp) or 0)
                if not db.execute("SELECT 1 FROM remote_records WHERE machine_id=? AND record_key=?", (origin, key)).fetchone():
                    value = v4_record_envelope(origin, key, record, period)
                    samples.put_remote(db, origin, period[:10], "bulk", key, content_hash(record), value)
                db.execute("INSERT OR IGNORE INTO remote_origins VALUES(?)", (origin,))
            for origin, day in db.execute("SELECT DISTINCT machine_id,day FROM remote_records").fetchall():
                entries = [json.loads(row[0]) for row in db.execute("SELECT record_json FROM remote_records WHERE machine_id=? AND day=?", (origin, day))]
                db.execute("INSERT OR REPLACE INTO remote_days VALUES(?,?,?,?)", (origin, day, v4_logical_hash(entries), "{}"))
    paths["usage_sync_cache"].unlink(missing_ok=True)
    for name, path in paths.items():
        if name.startswith("cache_source_"):
            path.unlink(missing_ok=True)
    webdav = config.get("webdav") or {}
    webdav["encryptionPassphraseHash"] = convert_verifier(webdav.get("encryptionPassphraseHash"), webdav)
    config["webdav"] = webdav
    write_json(paths["config"], config)
    state["formatMigration"] = {"accountAliases": converter.aliases}
    state.get("usage", {}).pop("accountAliases", None)
    state["skills"] = {"indexEtag": None, "indexId": None, "localSha256": {}}
    state.setdefault("remote", {})["skills"] = {}
    state["usageV4"] = {"remote": {}, "localHeadEtag": None, "transferByMonth": state.get("usageV4", {}).get("transferByMonth", {})}
    write_json(paths["cloud_state"], state)
    skills = read_json(paths["skills"])
    if skills is not None:
        skills.setdefault("deletions", {})
        for record in skills.get("skills", {}).values():
            record.setdefault("shared", True)
        write_json(paths["skills"], skills)
    paths["dashboard_cache"].unlink(missing_ok=True)
    for name, source in paths.items():
        if name.startswith("vault_source_"):
            source.unlink(missing_ok=True)


def _migrate_config_files(private_root, config_path, state_path, machine_path) -> None:
    from monitor_accounts import atomic_write_json
    from monitor_common import now_iso as _timestamp
    from monitor_cloud import hash_control_password, new_control_password_salt, passphrase_hash
    private_root.mkdir(parents=True, exist_ok=True)
    if not machine_path.exists():
        atomic_write_json(machine_path, {"version": 1, "machineId": uuid.uuid4().hex, "createdAt": _timestamp()})
    if not config_path.exists():
        atomic_write_json(config_path, {
            "version": 1, "control": {"password": "", "passwordHash": "", "passwordSalt": "", "cookieSecret": secrets.token_urlsafe(32)},
            "server": {"host": "0.0.0.0"},
            "autoUpdate": {"enabled": False},
            "webdav": {
                "enabled": False, "baseUrl": "https://dav.jianguoyun.com/dav/", "username": "", "password": "", "remoteRoot": "codex-switch-sync", "encryptionPassphrase": "", "encryptionPassphraseHash": "",
                "skillsAutoUpload": True, "usageDataAutoSync": True, "allowOptimisticWrites": True,
            },
        })
    else:
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            config = None
        if isinstance(config, dict):
            control = config.get("control")
            if not isinstance(control, dict):
                config["control"] = control = {}
            changed = False
            if "machineName" in config:
                del config["machineName"]
                changed = True
            if "server" not in config:
                config["server"] = {"host": "0.0.0.0"}
                changed = True
            if not isinstance(config.get("autoUpdate"), dict) or not isinstance(config["autoUpdate"].get("enabled"), bool):
                config["autoUpdate"] = {"enabled": False}
                changed = True
            if isinstance(control.get("password"), str) and control["password"]:
                if control["password"] == "123456":
                    control["passwordHash"], control["passwordSalt"] = "", ""
                else:
                    control["passwordSalt"] = new_control_password_salt()
                    control["passwordHash"] = hash_control_password(control["password"], control["passwordSalt"])
                control["password"] = ""
                changed = True
            elif "password" not in control:
                control["password"] = ""
                changed = True
            if "passwordHash" not in control:
                control["passwordHash"] = ""
                control["passwordSalt"] = ""
                changed = True
            if "passwordSalt" not in control:
                control["passwordSalt"] = ""
                changed = True
            if not isinstance(control.get("cookieSecret"), str) or len(control["cookieSecret"]) < 32:
                control["cookieSecret"] = secrets.token_urlsafe(32)
                changed = True
            webdav = config.get("webdav")
            if isinstance(webdav, dict):
                if "usageDataAutoSync" not in webdav:
                    webdav["usageDataAutoSync"] = True
                    changed = True
                if "password" not in webdav:
                    webdav["password"] = ""
                    changed = True
                if "encryptionPassphraseHash" not in webdav:
                    webdav["encryptionPassphraseHash"] = ""
                    changed = True
                if "encryptionPassphrase" not in webdav:
                    webdav["encryptionPassphrase"] = ""
                    changed = True
                if isinstance(webdav.get("encryptionPassphrase"), str) and webdav["encryptionPassphrase"]:
                    webdav["encryptionPassphraseHash"] = passphrase_hash(webdav["encryptionPassphrase"].strip(), str(webdav.get("baseUrl", "")), str(webdav.get("username", "")))
                    webdav["encryptionPassphrase"] = ""
                    changed = True
            if changed:
                atomic_write_json(config_path, config)
    if not state_path.exists():
        atomic_write_json(state_path, {
            "version": 1, "lastAutoFetchAt": None, "skills": {"indexEtag": None, "indexId": None, "localSha256": {}},
            "usage": {"published": {}, "remote": {}, "lastSuccessAt": None, "lastAttemptAt": None, "failure": None},
            "remote": {"accounts": {}, "skills": {}}, "pendingAccountOperation": None, "conditionalWritesVerified": False, "decryptFailure": None,
        })
    else:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        changed = False
        if "lastAutoFetchAt" not in state:
            state["lastAutoFetchAt"] = None
            changed = True
        if not isinstance(state.get("remote"), dict) or not isinstance(state["remote"].get("accounts"), dict) or not isinstance(state["remote"].get("skills"), dict):
            state["remote"] = {"accounts": {}, "skills": {}}
            changed = True
        skills = state.get("skills") if isinstance(state.get("skills"), dict) else {}
        if "indexEtag" not in skills or "indexId" not in skills or not isinstance(skills.get("localSha256"), dict):
            state["skills"] = {"indexEtag": skills.get("pointerEtag"), "indexId": skills.get("snapshotId"), "localSha256": {}}
            changed = True
        remote_skills = state["remote"].get("skills", {})
        if remote_skills and "indexEtag" not in remote_skills:
            state["remote"]["skills"] = {"version": 1, "indexEtag": remote_skills.get("pointerEtag"), "indexId": remote_skills.get("snapshotId"), "legacySnapshotId": remote_skills.get("snapshotId"), "updatedAt": remote_skills.get("updatedAt")}
            changed = True
        if not isinstance(state.get("usage"), dict):
            state["usage"] = {"published": {}, "remote": {}, "lastSuccessAt": None, "lastAttemptAt": None, "failure": None}
            changed = True
        else:
            for key, value in (("published", {}), ("remote", {}), ("lastSuccessAt", None), ("lastAttemptAt", None), ("failure", None)):
                if key not in state["usage"]:
                    state["usage"][key] = value
                    changed = True
            for key in ("lastFullVerificationAt", "pendingLegacyPurge"):
                if key in state["usage"]:
                    del state["usage"][key]
                    changed = True
            if not isinstance(state["usage"].get("accountAliases", {}), dict):
                state["usage"]["accountAliases"] = {}
                changed = True
            if "sequence" in state["usage"]:
                del state["usage"]["sequence"]
                changed = True
        if "decryptFailure" not in state:
            state["decryptFailure"] = None
            changed = True
        if changed:
            atomic_write_json(state_path, state)


def migrate_sample_store(path, batch_size=256):
    """One atomic bounded-memory conversion, protected additionally by startup's journal."""
    from monitor_migration_samples import connect, initialize, create_views, put_fact, put_local, put_remote, select_sample, _record, _envelope, canonical
    with closing(connect(path)) as db, db:
        old = {name for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('processing_facts','local_records','remote_records')")}
        # Remove old index names before creating their typed replacements. All DDL is
        # inside the same transaction, so an interruption leaves the old DB intact.
        db.execute("BEGIN IMMEDIATE")
        for table in old:
            for name, in db.execute("SELECT name FROM sqlite_master WHERE type IN ('index','trigger') AND tbl_name=? AND sql IS NOT NULL", (table,)).fetchall():
                typ = db.execute("SELECT type FROM sqlite_master WHERE name=?", (name,)).fetchone()[0]
                db.execute(f'DROP {typ} "{name}"')
        initialize(db)
        for table in ("processing_facts", "local_records", "remote_records"):
            if table not in old:
                continue
            count = 0
            cursor = db.execute(f"SELECT * FROM {table}")
            while batch := cursor.fetchmany(batch_size):
                for values in batch:
                    if table == "processing_facts":
                        put_fact(db, values)
                        stored = db.execute("SELECT sample_id,attribution FROM processing_fact_refs WHERE source_id=(SELECT id FROM sample_sources WHERE value=?) AND position=?", values[:2]).fetchone()
                        reconstructed = select_sample(db, *stored)
                        original = json.loads(values[8])
                    elif table == "local_records":
                        original = json.loads(values[3])
                        put_local(db, *values[:3], original)
                        stored = db.execute("SELECT sample_id,attribution,record_meta FROM local_record_refs WHERE record_key=?", values[:1]).fetchone()
                        reconstructed = _record(db, *stored)
                    else:
                        original = json.loads(values[5])
                        put_remote(db, *values[:5], original)
                        stored = db.execute("SELECT sample_id,attribution FROM remote_record_refs WHERE device_id=(SELECT id FROM sample_devices WHERE value=?) AND record_key=?", (values[0], values[3])).fetchone()
                        metadata = db.execute(('SELECT record_meta,envelope_meta,schema_version,collection_period,event_at FROM remote_record_refs WHERE device_id=(SELECT id FROM sample_devices WHERE '
                            'value=?) AND record_key=?'), (values[0], values[3])).fetchone()
                        reconstructed = _envelope(db, *stored, *metadata, values[0], values[3], values[4])
                    if canonical(original) != canonical(reconstructed):
                        raise ValueError(f"Sample round-trip failed: {table} record {count}")
                    if table != "processing_facts":
                        record = reconstructed if table == "local_records" else reconstructed["record"]
                        if hashlib.sha256(canonical(record).encode()).hexdigest() != values[2 if table == "local_records" else 4]:
                            raise ValueError(f"Sample synchronization hash mismatch: {table} record {count}")
                    count += 1
            target = {"processing_facts": "processing_fact_refs", "local_records": "local_record_refs", "remote_records": "remote_record_refs"}[table]
            if count != db.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0]:
                raise ValueError(f"Sample ownership count mismatch: {table}")
        for table in old:
            db.execute(f"DROP TABLE {table}")
        create_views(db)
        if db.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("Sample ownership foreign key check failed")


RESET_TIMESTAMP_FIELDS = {"resetAt", "baselineResetAt", "rollbackBaselineResetAt", "backwardResetCandidateAt"}


def convert_reset_timestamps(value):
    """Convert released ISO reset fields only; diagnostic API payloads stay intact."""
    if isinstance(value, list):
        return [convert_reset_timestamps(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key in RESET_TIMESTAMP_FIELDS and isinstance(item, str):
            item = parse_timestamp(item)
            if item is None:
                raise MigrationError(f"Invalid saved {key}; originals preserved")
        result[key] = item if key == "rawResponse" else convert_reset_timestamps(item)
    return result


def migrate_reset_timestamps(paths):
    from monitor_usage_sync import content_hash, v4_logical_hash
    # The outer startup journal backs up every file and the database before writing.
    for name in ("history", "quota_history", "token_session_history", "token_ledger", "sample_log"):
        if paths[name].exists():
            _atomic_write(paths[name], _jsonl_bytes([convert_reset_timestamps(row) for row in _json_records(paths[name])]))
    for name in ("runtime_state", "account_manifest"):
        if paths[name].exists():
            write_json(paths[name], convert_reset_timestamps(read_json(paths[name])))
    if paths["processing_index"].exists():
        with closing(samples.connect(paths["processing_index"])) as db, db:
            # Stage immutable samples before updating references. Peers can be the
            # only retained source, so they must survive cache invalidation.
            db.execute("CREATE TEMP TABLE reset_samples AS SELECT id,kind FROM sample_records")
            for sample_id, kind in db.execute("SELECT id,kind FROM reset_samples"):
                original = samples.select_sample(db, sample_id)
                converted = convert_reset_timestamps(original)
                if converted == original:
                    continue
                replacement, _ = samples.insert_sample(db, kind, converted)
                for table in ("processing_fact_refs", "local_record_refs", "remote_record_refs"):
                    db.execute(f"UPDATE {table} SET sample_id=? WHERE sample_id=?", (replacement, sample_id))
            for key, data in db.execute("SELECT record_key,record_json FROM local_records").fetchall():
                db.execute("UPDATE local_record_refs SET content_hash=? WHERE record_key=?", (content_hash(json.loads(data)), key))
            for machine, key, data in db.execute("SELECT machine_id,record_key,record_json FROM remote_records").fetchall():
                db.execute("UPDATE remote_record_refs SET content_hash=? WHERE device_id=(SELECT id FROM sample_devices WHERE value=?) AND record_key=?",
                    (content_hash(json.loads(data)["record"]), machine, key))
            for machine, day in db.execute("SELECT DISTINCT machine_id,day FROM remote_records").fetchall():
                entries = [json.loads(row[0]) for row in db.execute("SELECT record_json FROM remote_records WHERE machine_id=? AND day=?", (machine, day))]
                db.execute("UPDATE remote_days SET logical_hash=?,layout_json=? WHERE machine_id=? AND day=?", (v4_logical_hash(entries), "{}", machine, day))
            generation = json.loads(db.execute("SELECT value FROM processing_meta WHERE name='publishGeneration'").fetchone()[0]) if db.execute("SELECT 1 FROM processing_meta WHERE name='publishGeneration'").fetchone() else 0
            generation += 1
            db.execute("INSERT OR REPLACE INTO processing_meta VALUES('publishGeneration',?)", (json.dumps(generation),))
            for (period,) in db.execute("SELECT DISTINCT period FROM local_records").fetchall():
                db.execute("INSERT OR REPLACE INTO processing_dirty_days VALUES(?,?)", (period[:10], generation))
            for key, data in db.execute("SELECT record_key,data FROM processing_pending_records WHERE data IS NOT NULL").fetchall():
                db.execute("UPDATE processing_pending_records SET data=? WHERE record_key=?", (json.dumps(convert_reset_timestamps(json.loads(data))), key))
            db.execute("DROP TABLE reset_samples")
        _migrate_history_to_v10(paths)
        with closing(samples.connect(paths["processing_index"])) as db, db:
            db.execute("DELETE FROM processing_meta WHERE name IN ('samplingRule','runtimeState','consoleState') OR name LIKE 'samplingState:%' OR name LIKE 'quotaState:%' OR name LIKE 'rankRoot:%'")
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_sampled_quota'").fetchone():
                db.execute("DELETE FROM processing_sampled_quota")
        from monitor_processing import initialize_processing_index
        initialize_processing_index(paths["processing_index"])
    if paths["runtime_state"].exists():
        state = read_json(paths["runtime_state"])
        for key in ("windows", "lastSample", "remoteUsageIdentity"):
            state.pop(key, None)
        write_json(paths["runtime_state"], state)
    paths["dashboard_cache"].unlink(missing_ok=True)
    if paths["cloud_state"].exists():
        state = read_json(paths["cloud_state"])
        state["usageV4"] = {"remote": {}, "localHeadEtag": None, "transferByMonth": state.get("usageV4", {}).get("transferByMonth", {})}
        write_json(paths["cloud_state"], state)
