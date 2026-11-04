#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

if sys.version_info < (3, 12):
    raise SystemExit("Codex Usage Monitor requires Python 3.12 or newer.")

from monitor_dependencies import validate_dependencies

print("Startup: validating Python libraries...", flush=True)
if not validate_dependencies(Path(__file__).resolve().parent):
    raise SystemExit(1)

from monitor_auto_update import AUTO_UPDATE_RESTART, check_startup_update, installed_version, restart_process
from monitor_migrations import migrate_installed_data
from monitor_cloud import CloudManager
from monitor_common import *
from monitor_dashboard import *
from monitor_events import *
from monitor_history import *
from monitor_quota import *
from monitor_skills import SkillManager
from monitor_token_ledger import default_token_ledger_path
from monitor_tokens import *
from monitor_usage_sync import default_usage_sync_cache_path


def _main(instance_lock) -> int:
    print(f"Codex Usage Monitor v{installed_version(Path(__file__).resolve().parent)}", flush=True)
    parser = argparse.ArgumentParser(description="Poll Codex ChatGPT-account usage/rate-limit data and local Codex session token usage.")
    parser.add_argument("--auth", type=Path, default=codex_home() / "auth.json")
    parser.add_argument("--codex-home", type=Path, default=codex_home(), help="Codex config directory containing sessions/. Defaults to CODEX_HOME or ~/.codex.")
    parser.add_argument("--interval", type=int, default=90)
    parser.add_argument("--timeout", type=int, default=10, help="Per-request timeout in seconds.")
    parser.add_argument("--quota-history", type=Path, help="Legacy quota path used to locate the canonical database; canonical JSONL output is retired.")
    parser.add_argument("--token-ledger", type=Path, help="Legacy token ledger path retained for migration and custom data locations.")
    parser.add_argument("--raw-quota-log", type=Path, help="Optional raw quota API response JSONL path; writing requires the management setting to be enabled.")
    parser.add_argument("--dashboard", action="store_true", help="Open the local dashboard in the default browser after starting the server.")
    parser.add_argument("--reencrypt-cloud", action="store_true", help="Re-encrypt and verify every encrypted WebDAV payload with the configured hash key, then exit.")
    parser.add_argument("--local-only", action="store_true", help="Only scan local Codex session logs; do not call ChatGPT usage endpoints.")
    parser.add_argument("--no-token-scan", action="store_true", help="Disable local Codex session token usage scanning.")
    parser.add_argument("--repair-processing", action="store_true", help="Rebuild derived processing checkpoints and dashboard rows; preserve canonical histories, pending uploads, and package receipts.")
    parser.add_argument("--retry-limit", type=int, default=DEFAULT_RETRY_LIMIT, help="Retries for HTTP and dashboard polling failures before raising; network errors retry indefinitely. Defaults to 3.")
    args = parser.parse_args()
    print("Startup: acquiring the single-instance lock...", flush=True)
    if not instance_lock.acquire():
        print("Cannot start dashboard: another monitor instance is already running.", file=sys.stderr, flush=True)
        return 1
    args.instance_lock = instance_lock

    args.data_home = codex_switch_home()
    print("Startup: checking for runtime updates...", flush=True)
    if check_startup_update(Path(__file__).resolve().parent, args.data_home, lambda: opener_for(args.auth.parent), args.timeout):
        instance_lock.release()
        restart_process(Path(sys.argv[0]).resolve())
        return 0
    args.startup_update_checked = True
    legacy_history = default_history_path(args.data_home)
    args.quota_history = args.quota_history or default_quota_history_path(legacy_history)
    args.token_ledger = args.token_ledger or default_token_ledger_path(legacy_history)
    args.usage_sync_cache = default_usage_sync_cache_path(args.quota_history)
    args.state = args.data_home / "usage_monitor_state.json"
    args.dashboard_cache = default_dashboard_cache_path(args.quota_history)
    print("Startup: checking and applying pending data migrations...", flush=True)
    migrate_installed_data(
        Path(__file__).resolve().parent, args.data_home, args.quota_history, args.token_ledger,
        runtime_state_path=args.state, dashboard_cache_path=args.dashboard_cache, usage_sync_cache_path=args.usage_sync_cache,
        codex_home=args.codex_home,
    )
    args.account_root = args.data_home / "accounts"
    from monitor_canonical import CanonicalIndex
    print("Startup: initializing the processing index...", flush=True)
    index = CanonicalIndex(args.usage_sync_cache.with_name(f"{args.usage_sync_cache.stem}-v4.sqlite3"))
    if args.repair_processing:
        print("Startup: repairing derived processing data...", flush=True)
        index.repair()
    if args.reencrypt_cloud:
        result = CloudManager(args.data_home, SkillManager(args.codex_home, args.data_home), None).reencrypt_remote_data()
        print(f"Re-encrypted and verified {result['reencrypted']} cloud payloads.")
        return 0

    opener = None
    if not args.local_only:
        opener = opener_for(args.auth.parent)
    entry = Path(sys.argv[0]).resolve()
    result = serve_dashboard(args, opener, lambda: opener or opener_for(args.auth.parent), entry.parent)
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
