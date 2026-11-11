"""Own local data upgrades, journal recovery and old-format conversion.

All local data contracts are at version 27.
Historical migration functional code has been retired; application modules invoke the entry points here.
"""

import base64
import hashlib
import hmac
import json
import os
import shutil
import tempfile
import sqlite3
from contextlib import closing
from pathlib import Path

from monitor_common import atomic_write_bytes, parse_timestamp
from monitor_cloud_contract import CLOUD_DATA_CONTRACT_VERSION, CLOUD_DATA_CONTRACT_PATH


DATA_CONTRACT_VERSION = 27
DATA_MIGRATION_STATE_FILENAME = "usage_monitor_data_contract.json"
DATA_MIGRATION_JOURNAL_FILENAME = "usage_monitor_data_migration.json"


class MigrationError(RuntimeError):
    pass


def request_migration_rebuild(paths: dict[str, Path], reason: str, *, sampling: bool = False, delivery_only: bool = False) -> None:
    if "processing_index" not in paths or not paths["processing_index"].exists():
        return
    with closing(sqlite3.connect(paths["processing_index"])) as db, db:
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
    if not paths["processing_index"].exists():
        return
    with closing(sqlite3.connect(paths["processing_index"])) as db, db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_meta'").fetchone():
            return
        row = db.execute("SELECT value FROM processing_meta WHERE name='migrationRebuildPending'").fetchone()
        if row is None:
            return
        pending = json.loads(row[0])
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_dirty'").fetchone():
            for view in ("local", "merged") if pending.get("projection", True) else ():
                db.execute("INSERT OR REPLACE INTO processing_dirty SELECT ?,account,? FROM processing_facts GROUP BY account", (view, "; ".join(pending["reasons"])))
                if pending["sampling"] and db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_sampling_changes'").fetchone():
                    db.execute("INSERT OR IGNORE INTO processing_sampling_changes SELECT ?,account,'' FROM processing_facts WHERE kind='quota' GROUP BY account", (view,))
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_meta'").fetchone():
            from monitor_delivery import request_attribution_rebuild
            request_attribution_rebuild(db)
        db.execute("DELETE FROM processing_meta WHERE name='migrationRebuildPending'")


def _with_migration_rebuild(migration, reason: str, *, sampling: bool = False):
    def run(paths):
        migration(paths)
        request_migration_rebuild(paths, reason, sampling=sampling)
    return run


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


def _noop_migration(paths: dict[str, Path]) -> None:
    pass


def _migrate_history_to_v13(paths: dict[str, Path]) -> None:
    pass


def _migrate_history_to_v19(paths: dict[str, Path]) -> None:
    pass


DATA_MIGRATIONS = {version: _noop_migration for version in range(1, DATA_CONTRACT_VERSION + 1)}


def _restore_data_backup(backup: Path, path: Path, *, sqlite_database: bool) -> None:
    if sqlite_database:
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
    if journal["version"] in {19, 23, 24, 25, 26, 27} and set(journal["targets"]) == {"processing_index"}:
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
                    _restore_data_backup(backup, path, sqlite_database=name == "processing_index" or path.suffix == ".sqlite3")
                elif not path.exists():
                    raise MigrationError(f"Cannot recover migrated data file: {path}")
            else:
                path.unlink(missing_ok=True)
    for backup in journal["backups"].values():
        Path(backup).unlink(missing_ok=True)
    journal_path.unlink(missing_ok=True)


def _run_data_migration(migration, paths: dict[str, Path], version: int, state_path: Path, journal_path: Path) -> None:
    if migration is _noop_migration:
        _atomic_write(state_path, (json.dumps({"dataContractVersion": version}, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
        return
    protected_paths = {"processing_index": paths["processing_index"]} if version == 19 else paths
    if version in {23, 24, 25, 26, 27}:
        protected_paths = {"processing_index": paths["processing_index"]}
    if version == 22:
        protected_paths = {name: paths[name] for name in ("processing_index", "config") if name in paths}
    if version == 15:
        protected_paths = {name: path for name, path in paths.items() if name not in {"history", "sample_log", "legacy_sample_log"}}
    existing = {name for name, path in protected_paths.items() if path.exists()}
    backups = {}
    try:
        for name in existing:
            descriptor, backup = tempfile.mkstemp(prefix=f".{paths[name].name}.", suffix=".migration-backup", dir=paths[name].parent)
            os.close(descriptor)
            try:
                if name == "processing_index" or paths[name].suffix == ".sqlite3":
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
                    _restore_data_backup(backups[name], path, sqlite_database=name == "processing_index" or path.suffix == ".sqlite3")
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
    data_home: Path, history_path: Path, quota_history_path: Path, token_session_history_path: Path, sample_log_path: Path,
    target_version: int | None = None, runtime_state_path: Path | None = None, dashboard_cache_path: Path | None = None,
    usage_sync_cache_path: Path | None = None, cloud_state_path: Path | None = None, codex_home: Path | None = None, *, database_path: Path | None = None,
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
        "token_ledger": data_home / "usage_monitor_token_events.jsonl", "sample_log": Path(sample_log_path), "runtime_state": Path(runtime_state_path) if runtime_state_path is not None else data_home / "usage_monitor_state.json",
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
    paths["processing_index"] = Path(database_path) if database_path is not None else paths["usage_sync_cache"].with_name(f"{paths['usage_sync_cache'].stem}-v4.sqlite3")
    paths["account_manifest"] = data_home / "accounts" / "accounts.json"
    if codex_home is not None:
        paths["legacy_account_manifest"] = Path(codex_home) / "usage-monitor-accounts" / "accounts.json"
    paths["config"] = data_home / "config.json"
    paths["machine"] = data_home / "machine.json"
    paths["skills"] = data_home / "skills.json"
    if "legacy_account_manifest" in paths:
        vault = paths["legacy_account_manifest"].parent
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
        from monitor_canonical import CanonicalIndex
        CanonicalIndex(paths["processing_index"]).close()
    return completed


def migrate_installed_data(
    runtime_dir: Path, data_home: Path, *, runtime_state_path: Path | None = None, codex_home: Path | None = None, database_path: Path | None = None,
) -> list[int]:
    from monitor_history import default_history_path, default_quota_history_path, default_sample_log_path

    migrate_default_monitor_data(runtime_dir, data_home)
    history = default_history_path(data_home)
    return migrate_history_data(
        data_home, history, default_quota_history_path(history), Path(data_home) / "usage_monitor_token_sessions.jsonl", default_sample_log_path(history),
        target_version=installed_data_contract_version(runtime_dir), runtime_state_path=runtime_state_path, codex_home=codex_home, database_path=database_path,
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
            return stable_id("historical", str(account))
        return stable_id("machine", f"{origin or self.machine}\0{slot or 'unknown'}")


def convert_saved_fields(value):
    if isinstance(value, list):
        return [convert_saved_fields(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {key: convert_saved_fields(item) for key, item in value.items()}
    if "cachedOutputTokens" in result:
        result["cacheWriteInputTokens"] = result.get("cacheWriteInputTokens") or result.pop("cachedOutputTokens")
        result.pop("cachedOutputTokens", None)
    if "freshOutputTokens" in result:
        result.pop("freshOutputTokens")
    if "events" in result and result.get("window") in {"5h", "7d"} and "delta" not in result:
        result["delta"] = result.pop("events")
    return result


def _load_v18_token_ledger(path: Path) -> list[dict]:
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
