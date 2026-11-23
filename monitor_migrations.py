"""Own local data contract version verification and startup gating.

All local data contracts are at version 27.
Historical migration functional code has been retired; only current-version gating remains.
"""

import json
from pathlib import Path

from monitor_common import atomic_write_bytes
from monitor_cloud_contract import CLOUD_DATA_CONTRACT_VERSION, CLOUD_DATA_CONTRACT_PATH


DATA_CONTRACT_VERSION = 27
DATA_MIGRATION_STATE_FILENAME = "usage_monitor_data_contract.json"


class MigrationError(RuntimeError):
    pass


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


def _noop_migration(paths: dict[str, Path]) -> None:
    pass


DATA_MIGRATIONS = {version: _noop_migration for version in range(1, DATA_CONTRACT_VERSION + 1)}


def migrate_history_data(
    data_home: Path, history_path: Path | None = None, quota_history_path: Path | None = None, token_session_history_path: Path | None = None,
    sample_log_path: Path | None = None, target_version: int | None = None, runtime_state_path: Path | None = None, dashboard_cache_path: Path | None = None,
    usage_sync_cache_path: Path | None = None, cloud_state_path: Path | None = None, codex_home: Path | None = None, *, database_path: Path | None = None,
) -> list[int]:
    """Verify or initialize the local data contract marker without replaying retired migrations."""
    data_home = Path(data_home)
    data_home.mkdir(parents=True, exist_ok=True)
    state_path = data_home / DATA_MIGRATION_STATE_FILENAME
    target_version = DATA_CONTRACT_VERSION if target_version is None else target_version
    if not isinstance(target_version, int) or isinstance(target_version, bool) or not 0 <= target_version <= DATA_CONTRACT_VERSION:
        raise MigrationError(f"Unsupported target data contract version: {target_version!r}")
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        current = state.get("dataContractVersion", 0) if isinstance(state, dict) else 0
    except FileNotFoundError:
        current = 0
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationError(f"Cannot read data migration state: {exc}") from exc
    if not isinstance(current, int) or isinstance(current, bool) or current < 0 or current > target_version:
        raise MigrationError(f"Unsupported data contract version: {current!r}")
    if current == target_version:
        return []
    sync_cache = Path(usage_sync_cache_path) if usage_sync_cache_path is not None else data_home / "usage_monitor_sync_cache.json"
    paths = {
        "runtime_state": Path(runtime_state_path) if runtime_state_path is not None else data_home / "usage_monitor_state.json",
        "processing_index": Path(database_path) if database_path is not None else sync_cache.with_name(f"{sync_cache.stem}-v4.sqlite3"),
    }
    completed, pending_state = [], False
    for version in range(current + 1, target_version + 1):
        migration = DATA_MIGRATIONS.get(version)
        if migration is None:
            raise MigrationError(f"Missing data migration for contract version {version}")
        if migration is not _noop_migration:
            migration(paths)
            atomic_write_bytes(state_path, (json.dumps({"dataContractVersion": version}, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
            pending_state = False
        else:
            pending_state = True
        completed.append(version)
    if pending_state:
        atomic_write_bytes(state_path, (json.dumps({"dataContractVersion": target_version}, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
    return completed


def migrate_installed_data(
    runtime_dir: Path, data_home: Path, *, runtime_state_path: Path | None = None, codex_home: Path | None = None, database_path: Path | None = None,
) -> list[int]:
    return migrate_history_data(
        data_home, target_version=installed_data_contract_version(runtime_dir),
        runtime_state_path=runtime_state_path, codex_home=codex_home, database_path=database_path,
    )
