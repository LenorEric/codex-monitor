#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

if sys.version_info < (3, 12):
    raise SystemExit("Codex Usage Monitor requires Python 3.12 or newer.")

from monitor_common import console_log
from monitor_dependencies import validate_dependencies

console_log("Startup: validating Python libraries...")
if not validate_dependencies(Path(__file__).resolve().parent):
    raise SystemExit(1)

from monitor_auto_update import AUTO_UPDATE_RESTART, check_startup_update, installed_version, restart_process
from monitor_canonical import CanonicalIndex, default_database_path
from monitor_migrations import migrate_installed_data
from monitor_common import *
from monitor_dashboard import *
from monitor_events import *
from monitor_history import *
from monitor_quota import *
from monitor_tokens import *


def _main(instance_lock) -> int:
    console_log(f"Codex Usage Monitor v{installed_version(Path(__file__).resolve().parent)}")
    parser = argparse.ArgumentParser(description="Poll Codex ChatGPT-account usage/rate-limit data and local Codex session token usage.")
    parser.add_argument("--auth", type=Path, default=codex_home() / "auth.json")
    parser.add_argument("--codex-home", type=Path, default=codex_home(), help="Codex config directory containing sessions/. Defaults to CODEX_HOME or ~/.codex.")
    parser.add_argument("--database", type=Path, help="Path to the SQLite usage database. Defaults to ~/.codex-switch/usage_monitor_sync_cache-v4.sqlite3.")
    parser.add_argument("--raw-quota-log", type=Path, help="Optional raw quota API response JSONL path; writing requires the management setting to be enabled.")
    parser.add_argument("--dashboard", action="store_true", help="Open the local dashboard in the default browser after starting the server.")
    args = parser.parse_args()
    console_log("Startup: acquiring the single-instance lock...")
    if not instance_lock.acquire():
        console_log("Cannot start dashboard: another monitor instance is already running.", level="error")
        return 1
    args.instance_lock = instance_lock

    args.data_home = codex_switch_home()
    console_log("Startup: checking for runtime updates...")
    if check_startup_update(Path(__file__).resolve().parent, args.data_home, lambda: opener_for(args.auth.parent)):
        instance_lock.release()
        restart_process(Path(sys.argv[0]).resolve())
        return 0
    args.startup_update_checked = True
    args.database = args.database or default_database_path(args.data_home)
    args.state = args.data_home / "usage_monitor_state.json"
    console_log("Startup: checking and applying pending data migrations...")
    migrate_installed_data(
        Path(__file__).resolve().parent, args.data_home, runtime_state_path=args.state, codex_home=args.codex_home, database_path=args.database,
    )
    args.account_root = args.data_home / "accounts"
    console_log("Startup: initializing the processing index...")
    index = CanonicalIndex(args.database)
    index.close()
    opener = opener_for(args.auth.parent)
    entry = Path(sys.argv[0]).resolve()
    result = serve_dashboard(args, opener, lambda: opener, entry.parent)
    if result == AUTO_UPDATE_RESTART:
        restart_process(entry)
        return 0
    return result


def main() -> int:
    instance_lock = DashboardInstanceLock()
    try:
        return _main(instance_lock)
    finally:
        instance_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
