#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path


UPDATE_INTERVAL_SECONDS = 60 * 60
RELEASE_VERSION_URL = "https://raw.githubusercontent.com/LenorEric/codex-monitor/refs/heads/master/release/version.json"
RUNTIME_FILE_URL = "https://raw.githubusercontent.com/LenorEric/codex-monitor/refs/heads/master/release/runtime/{name}"
VERSION_PATTERN = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_RUNTIME_FILE_BYTES = 32 * 1024 * 1024
MAX_RUNTIME_BYTES = 128 * 1024 * 1024
AUTO_UPDATE_RESTART = 75
DATA_CONTRACT_VERSION = 4
DATA_MIGRATION_STATE_FILENAME = "usage_monitor_data_contract.json"
DATA_MIGRATION_JOURNAL_FILENAME = "usage_monitor_data_migration.json"


class AutoUpdateError(RuntimeError):
    pass


def parse_version(value) -> tuple[int, int, int]:
    if not isinstance(value, str) or (match := VERSION_PATTERN.fullmatch(value)) is None:
        raise AutoUpdateError(f"Invalid release version: {value!r}")
    return tuple(int(part) for part in match.groups())


def installed_version(runtime_dir: Path, fallback: str = "1.5.0") -> str:
    for path in (runtime_dir / "version.json", runtime_dir / "package.json"):
        try:
            version = json.loads(path.read_text(encoding="utf-8")).get("version")
            parse_version(version)
            return version
        except (FileNotFoundError, OSError, AttributeError, json.JSONDecodeError, AutoUpdateError):
            pass
    return fallback


def installed_data_contract_version(runtime_dir: Path, fallback: int = DATA_CONTRACT_VERSION) -> int:
    for path in (Path(runtime_dir) / "version.json", Path(runtime_dir) / "package.json"):
        try:
            version = json.loads(path.read_text(encoding="utf-8")).get("dataContractVersion")
            if not isinstance(version, int) or isinstance(version, bool) or version < 0:
                raise AutoUpdateError(f"Invalid data contract version: {version!r}")
            return version
        except FileNotFoundError:
            pass
        except (OSError, AttributeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AutoUpdateError(f"Cannot read local program metadata: {exc}") from exc
    return fallback


def validate_manifest(value) -> tuple[str, dict[str, dict]]:
    if not isinstance(value, dict):
        raise AutoUpdateError("Release manifest must be a JSON object")
    version, files = value.get("version"), value.get("files")
    parse_version(version)
    if not isinstance(files, dict) or not files:
        raise AutoUpdateError("Release manifest must contain runtime files")
    normalized, total_size, names = {}, 0, set()
    for name, descriptor in files.items():
        if not isinstance(name, str) or not name or name in {".", ".."} or Path(name).name != name or any(char in name for char in "/\\\x00?#%"):
            raise AutoUpdateError(f"Unsafe runtime filename: {name!r}")
        folded = name.casefold()
        if folded in names:
            raise AutoUpdateError(f"Duplicate runtime filename: {name}")
        names.add(folded)
        if not isinstance(descriptor, dict) or not isinstance(descriptor.get("size"), int) or isinstance(descriptor.get("size"), bool) or not 0 <= descriptor["size"] <= MAX_RUNTIME_FILE_BYTES:
            raise AutoUpdateError(f"Invalid size for runtime file: {name}")
        digest = descriptor.get("sha256")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise AutoUpdateError(f"Invalid SHA-256 for runtime file: {name}")
        total_size += descriptor["size"]
        if total_size > MAX_RUNTIME_BYTES:
            raise AutoUpdateError("Runtime update exceeds the maximum total size")
        normalized[name] = {"size": descriptor["size"], "sha256": digest}
    return version, normalized


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
            raise AutoUpdateError(f"History file contains a non-object record: {path}")
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
    from monitor_history import normalize_quota_history_row, normalize_token_session_row, parse_timestamp
    from monitor_token_ledger import load_token_ledger

    for path in (paths[name] for name in ("history", "quota_history", "token_session_history", "token_ledger", "sample_log")):
        if path.exists():
            _json_records(path)
    if (path := paths["quota_history"]).exists():
        rows = []
        for row in _json_records(path):
            if (normalized := normalize_quota_history_row(row)) is None:
                raise AutoUpdateError("Quota history contains an unsupported row")
            rows.append(normalized)
        rows.sort(key=lambda row: (parse_timestamp(row["checkedAt"]) is None, parse_timestamp(row["checkedAt"]) or 0, row["checkedAt"], row["accountSlotId"]))
        _atomic_write(path, _jsonl_bytes(rows))
    if (path := paths["token_session_history"]).exists():
        rows = []
        for row in _json_records(path):
            if (normalized := normalize_token_session_row(row)) is None:
                raise AutoUpdateError("Token session history contains an unsupported row")
            rows.append(normalized)
        _atomic_write(path, _jsonl_bytes(rows))
    if (path := paths["token_ledger"]).exists():
        _atomic_write(path, _jsonl_bytes(load_token_ledger(path)))
    if (path := paths["sample_log"]).exists():
        _atomic_write(path, _jsonl_bytes(_json_records(path)))


def _migrate_history_to_v2(paths: dict[str, Path]) -> None:
    from monitor_history import normalize_token_session_row
    from monitor_tokens import normalize_saved_token_totals
    from monitor_token_ledger import append_token_ledger, legacy_baselines_from_sessions, load_token_ledger, token_sessions_from_ledger

    ledger = load_token_ledger(paths["token_ledger"])
    sessions = []
    for row in _json_records(paths["token_session_history"]):
        if (normalized := normalize_token_session_row(row)) is None:
            raise AutoUpdateError("Token session history contains an unsupported row")
        sessions.append(normalized)
    represented = {(str(row.get("sessionId")), str(row.get("accountSlotId"))) for row in token_sessions_from_ledger(ledger)}
    missing = [row for row in sessions if (str(row.get("sessionId")), str(row.get("accountSlotId"))) not in represented]
    append_token_ledger(paths["token_ledger"], [row | {"schemaVersion": 1} for row in legacy_baselines_from_sessions(missing)])
    regenerated = {(str(row.get("sessionId")), str(row.get("accountSlotId"))): normalize_token_session_row(row) for row in token_sessions_from_ledger(load_token_ledger(paths["token_ledger"]))}
    if any((normalized := normalize_token_session_row(row)) is None or regenerated.get((str(row.get("sessionId")), str(row.get("accountSlotId")))) != normalized for row in missing):
        raise AutoUpdateError("Token session migration verification failed")
    def covers(rebuilt_values, source_values) -> bool:
        return all(float((rebuilt_values or {}).get(key) or 0) + 1e-8 >= float(value or 0) for key, value in (source_values or {}).items())

    for source in sessions:
        rebuilt = regenerated.get((str(source.get("sessionId")), str(source.get("accountSlotId"))))
        if rebuilt is None or not covers(normalize_saved_token_totals(rebuilt.get("tokens")), normalize_saved_token_totals(source.get("tokens"))) or not covers(rebuilt.get("cost"), source.get("cost")):
            raise AutoUpdateError("Token session migration does not cover all historical token totals")
        for model, source_model in (source.get("byModel") or {}).items():
            rebuilt_model = (rebuilt.get("byModel") or {}).get(model)
            if not isinstance(rebuilt_model, dict) or not covers(normalize_saved_token_totals(rebuilt_model.get("tokens")), normalize_saved_token_totals(source_model.get("tokens"))) or not covers(rebuilt_model.get("cost"), source_model.get("cost")) or not covers(normalize_saved_token_totals(rebuilt_model.get("fastTokens")), normalize_saved_token_totals(source_model.get("fastTokens"))):
                raise AutoUpdateError("Token session migration does not cover all historical model totals")
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
            raise AutoUpdateError("Invalid monitor runtime state")
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
            raise AutoUpdateError("Invalid cloud runtime state")
        cloud_state["usage"].update({"remote": {}, "lastSuccessAt": None, "lastAttemptAt": None, "failure": None})
        cloud_state["usage"].pop("lastFullVerificationAt", None)
        cloud_state["usage"].pop("pendingLegacyPurge", None)
        _atomic_write(paths["cloud_state"], (json.dumps(cloud_state, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))


def _migrate_history_to_v3(paths: dict[str, Path]) -> None:
    from monitor_history import normalize_quota_history_row
    from monitor_token_ledger import canonical_token_ledger_rows, load_token_ledger, token_sessions_from_ledger, write_token_ledger

    quota_rows = []
    for row in _json_records_with_torn_tail(paths["quota_history"]):
        if (normalized := normalize_quota_history_row(row)) is None:
            raise AutoUpdateError("Quota history contains an unsupported row")
        quota_rows.append(normalized)
    ledger = load_token_ledger(paths["token_ledger"])
    canonical = canonical_token_ledger_rows(ledger)
    if token_sessions_from_ledger(ledger) != token_sessions_from_ledger(canonical):
        raise AutoUpdateError("Token ledger v3 normalization verification failed")
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
            raise AutoUpdateError("Invalid cloud runtime state")
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


DATA_MIGRATIONS = {1: _migrate_history_to_v1, 2: _migrate_history_to_v2, 3: _migrate_history_to_v3, 4: _migrate_history_to_v4}


def _recover_data_migration(journal_path: Path, state_path: Path, paths: dict[str, Path]) -> None:
    try:
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AutoUpdateError(f"Cannot recover data migration: {exc}") from exc
    if not isinstance(journal, dict) or not isinstance(journal.get("version"), int) or isinstance(journal.get("version"), bool) or not isinstance(journal.get("targets"), dict) or not isinstance(journal.get("backups"), dict) or not isinstance(journal.get("existing"), list):
        raise AutoUpdateError("Cannot recover data migration: invalid journal")
    expected_targets = {name: str(path.resolve()) for name, path in paths.items()}
    base_targets = {name: target for name, target in expected_targets.items() if not name.startswith("legacy_")}
    legacy_targets = {"quota_history": "legacy_quota_history", "token_ledger": "legacy_token_ledger", "sample_log": "legacy_sample_log"}
    valid_base_targets = set(journal["targets"]) == set(base_targets) and all(
        target == base_targets[name] or name in legacy_targets and legacy_targets[name] in expected_targets and target == expected_targets[legacy_targets[name]]
        for name, target in journal["targets"].items()
    )
    if journal["targets"] != expected_targets and not valid_base_targets:
        raise AutoUpdateError("Cannot recover data migration: invalid journal")
    for name, backup in journal["backups"].items():
        if name not in journal["targets"] or not isinstance(backup, str) or Path(backup).parent.resolve() != Path(journal["targets"][name]).parent.resolve():
            raise AutoUpdateError("Cannot recover data migration: invalid backup path")
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        committed = isinstance(state, dict) and isinstance(state.get("dataContractVersion"), int) and state["dataContractVersion"] >= journal["version"]
    except FileNotFoundError:
        committed = False
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AutoUpdateError(f"Cannot recover data migration state: {exc}") from exc
    if not committed:
        for name, target in journal["targets"].items():
            path = Path(target)
            backup = Path(journal["backups"][name]) if name in journal["backups"] else None
            if name in journal["existing"]:
                if backup is not None and backup.exists():
                    os.replace(backup, path)
                elif not path.exists():
                    raise AutoUpdateError(f"Cannot recover migrated data file: {path}")
            else:
                path.unlink(missing_ok=True)
    for backup in journal["backups"].values():
        Path(backup).unlink(missing_ok=True)
    journal_path.unlink(missing_ok=True)


def _run_data_migration(migration, paths: dict[str, Path], version: int, state_path: Path, journal_path: Path) -> None:
    existing = {name for name, path in paths.items() if path.exists()}
    backups = {}
    try:
        for name in existing:
            descriptor, backup = tempfile.mkstemp(prefix=f".{paths[name].name}.", suffix=".migration-backup", dir=paths[name].parent)
            os.close(descriptor)
            try:
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
            "targets": {name: str(path.resolve()) for name, path in paths.items()},
        }, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
        migration(paths)
        _atomic_write(state_path, (json.dumps({"dataContractVersion": version}, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
    except Exception as exc:
        rollback_errors = []
        for name, path in paths.items():
            try:
                if name in backups:
                    os.replace(backups[name], path)
                elif name not in existing:
                    path.unlink(missing_ok=True)
            except OSError as rollback_exc:
                rollback_errors.append(f"{path}: {rollback_exc}")
        if rollback_errors:
            raise AutoUpdateError(f"Data migration failed and recovery is incomplete: {'; '.join(rollback_errors)}") from exc
        for backup in backups.values():
            backup.unlink(missing_ok=True)
        journal_path.unlink(missing_ok=True)
        raise
    for backup in backups.values():
        backup.unlink(missing_ok=True)
    journal_path.unlink(missing_ok=True)


def migrate_history_data(data_home: Path, history_path: Path, quota_history_path: Path, token_session_history_path: Path, token_ledger_path: Path, sample_log_path: Path, target_version: int | None = None, runtime_state_path: Path | None = None, dashboard_cache_path: Path | None = None, usage_sync_cache_path: Path | None = None, cloud_state_path: Path | None = None) -> list[int]:
    """Migrate every configured history store to the latest data contract once."""
    data_home = Path(data_home)
    data_home.mkdir(parents=True, exist_ok=True)
    state_path = data_home / DATA_MIGRATION_STATE_FILENAME
    journal_path = data_home / DATA_MIGRATION_JOURNAL_FILENAME
    target_version = DATA_CONTRACT_VERSION if target_version is None else target_version
    if not isinstance(target_version, int) or isinstance(target_version, bool) or not 0 <= target_version <= DATA_CONTRACT_VERSION:
        raise AutoUpdateError(f"Unsupported target data contract version: {target_version!r}")
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
    _recover_data_migration(journal_path, state_path, paths)
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        current = state.get("dataContractVersion", 0) if isinstance(state, dict) else 0
    except FileNotFoundError:
        current = 0
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AutoUpdateError(f"Cannot read data migration state: {exc}") from exc
    if not isinstance(current, int) or isinstance(current, bool) or current < 0 or current > target_version:
        raise AutoUpdateError(f"Unsupported data contract version: {current!r}")
    completed = []
    for version in range(current + 1, target_version + 1):
        migration = DATA_MIGRATIONS.get(version)
        if migration is None:
            raise AutoUpdateError(f"Missing data migration for contract version {version}")
        migration_paths = paths
        if version < 4:
            migration_paths = {name: path for name, path in paths.items() if not name.startswith("legacy_")}
            for legacy_name, current_name in (("legacy_quota_history", "quota_history"), ("legacy_token_ledger", "token_ledger"), ("legacy_sample_log", "sample_log")):
                if legacy_name in paths and paths[legacy_name].exists():
                    migration_paths[current_name] = paths[legacy_name]
        _run_data_migration(migration, migration_paths, version, state_path, journal_path)
        completed.append(version)
    return completed


def migrate_installed_usage_data(runtime_dir: Path, data_home: Path, quota_history_path: Path | None = None, token_ledger_path: Path | None = None, sample_log_path: Path | None = None, target_version: int | None = None, runtime_state_path: Path | None = None, dashboard_cache_path: Path | None = None, usage_sync_cache_path: Path | None = None, cloud_state_path: Path | None = None) -> list[int]:
    from monitor_history import default_history_path, default_quota_history_path, default_sample_log_path
    from monitor_token_ledger import default_token_ledger_path

    history = default_history_path(data_home)
    return migrate_history_data(
        data_home, history, quota_history_path or default_quota_history_path(history), Path(data_home) / "usage_monitor_token_sessions.jsonl",
        token_ledger_path or default_token_ledger_path(history), sample_log_path or default_sample_log_path(history),
        installed_data_contract_version(runtime_dir) if target_version is None else target_version, runtime_state_path, dashboard_cache_path, usage_sync_cache_path, cloud_state_path,
    )


class AutoUpdater:
    def __init__(self, runtime_dir: Path, enabled, opener_factory, timeout: int = 10, interval: int = UPDATE_INTERVAL_SECONDS, clock=time.monotonic):
        self.runtime_dir, self.enabled, self.opener_factory = Path(runtime_dir).resolve(), enabled, opener_factory
        self.timeout, self.interval, self.clock = max(int(timeout), 1), max(int(interval), 1), clock
        self.restart_requested = False
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = None
        self._on_update = None

    def start(self, on_update) -> None:
        if self._thread is not None:
            return
        self._on_update = on_update
        self._thread = threading.Thread(target=self._run, name="codex-monitor-auto-update", daemon=True)
        self._thread.start()

    def notify_config_changed(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(self.timeout + 1)

    def _run(self) -> None:
        was_enabled, next_check = False, 0.0
        while not self._stop.is_set():
            try:
                enabled = self.enabled() is True
            except Exception as exc:
                enabled = False
                print(f"Automatic update configuration check failed: {exc}", file=sys.stderr, flush=True)
            now = self.clock()
            if enabled and (not was_enabled or now >= next_check):
                next_check = now + self.interval
                try:
                    if self.check_for_update():
                        self.restart_requested = True
                        self._on_update()
                        return
                except Exception as exc:
                    print(f"Automatic update failed: {exc}", file=sys.stderr, flush=True)
            was_enabled = enabled
            wait_seconds = min(max(next_check - self.clock(), 0.0), 60.0) if enabled else 60.0
            self._wake.wait(wait_seconds)
            self._wake.clear()

    def _read(self, opener, url: str, maximum: int) -> bytes:
        request = urllib.request.Request(url, headers={"Cache-Control": "no-cache", "User-Agent": "codex-monitor-auto-update"})
        with opener.open(request, timeout=self.timeout) as response:
            data = response.read(maximum + 1)
        if len(data) > maximum:
            raise AutoUpdateError(f"Download exceeds the maximum size: {url}")
        return data

    def _manifest(self, opener) -> tuple[str, dict[str, dict]]:
        try:
            value = json.loads(self._read(opener, RELEASE_VERSION_URL, MAX_MANIFEST_BYTES).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AutoUpdateError(f"Release manifest is not valid UTF-8 JSON: {exc}") from exc
        return validate_manifest(value)

    def check_for_update(self) -> bool:
        opener = self.opener_factory()
        version, files = self._manifest(opener)
        if parse_version(version) <= parse_version(installed_version(self.runtime_dir)):
            return False
        self._install(opener, version, files)
        print(f"Codex Monitor updated to v{version}; restarting.", flush=True)
        return True

    def _install(self, opener, version: str, files: dict[str, dict]) -> None:
        work_root = self.runtime_dir / f".codex-monitor-update-{uuid.uuid4().hex}"
        staging, backup = work_root / "staging", work_root / "backup"
        staging.mkdir(parents=True)
        backup.mkdir()
        replaced = []
        try:
            for name, descriptor in files.items():
                data = self._read(opener, RUNTIME_FILE_URL.format(name=urllib.parse.quote(name)), descriptor["size"])
                if len(data) != descriptor["size"] or hashlib.sha256(data).hexdigest() != descriptor["sha256"]:
                    raise AutoUpdateError(f"Runtime file verification failed: {name}")
                (staging / name).write_bytes(data)
            try:
                metadata = json.loads((staging / "version.json").read_text(encoding="utf-8"))
                if not isinstance(metadata, dict) or metadata.get("version") != version or not isinstance(metadata.get("dataContractVersion"), int) or isinstance(metadata["dataContractVersion"], bool) or metadata["dataContractVersion"] < 0:
                    raise AutoUpdateError("Runtime version metadata does not match the release manifest")
            except (FileNotFoundError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AutoUpdateError("Runtime version metadata is missing or invalid") from exc
            for name in [name for name in files if name != "version.json"] + ["version.json"]:
                target = self.runtime_dir / name
                if target.is_symlink() or target.exists() and not target.is_file():
                    raise AutoUpdateError(f"Runtime target is not a regular file: {name}")
                if target.exists():
                    shutil.copy2(target, backup / name)
                os.replace(staging / name, target)
                replaced.append(name)
        except Exception:
            rollback_errors = []
            for name in reversed(replaced):
                target, saved = self.runtime_dir / name, backup / name
                try:
                    if saved.exists():
                        os.replace(saved, target)
                    else:
                        target.unlink(missing_ok=True)
                except OSError as exc:
                    rollback_errors.append(f"{name}: {exc}")
            if rollback_errors:
                raise AutoUpdateError(f"Update failed and rollback was incomplete: {'; '.join(rollback_errors)}")
            raise
        finally:
            shutil.rmtree(work_root, ignore_errors=True)


def restart_process(entry: Path, arguments: list[str] | None = None) -> None:
    os.execv(sys.executable, [sys.executable, str(Path(entry).resolve()), *(sys.argv[1:] if arguments is None else arguments)])
