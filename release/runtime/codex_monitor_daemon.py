#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

if sys.version_info < (3, 12):
    raise SystemExit("Codex Usage Monitor requires Python 3.12 or newer.")

from monitor_accounts import migrate_account_vault
from monitor_auto_update import AUTO_UPDATE_RESTART, installed_version, migrate_installed_usage_data, restart_process
from monitor_cloud import CloudManager
from monitor_common import *
from monitor_dashboard import *
from monitor_events import *
from monitor_history import *
from monitor_quota import *
from monitor_skills import SkillManager
from monitor_token_ledger import default_token_ledger_path
from monitor_tokens import *


def _main(instance_lock) -> int:
    print(f"Codex Usage Monitor v{installed_version(Path(__file__).resolve().parent)}", flush=True)
    parser = argparse.ArgumentParser(description="Poll Codex ChatGPT-account usage/rate-limit data and local Codex session token usage.")
    parser.add_argument("--auth", type=Path, default=codex_home() / "auth.json")
    parser.add_argument("--codex-home", type=Path, default=codex_home(), help="Codex config directory containing sessions/. Defaults to CODEX_HOME or ~/.codex.")
    parser.add_argument("--interval", type=int, default=90)
    parser.add_argument("--timeout", type=int, default=10, help="Per-request timeout in seconds.")
    parser.add_argument("--quota-history", type=Path, help="Per-account quota reading path. Defaults to ~/.codex-switch/usage_monitor_quota_readings.jsonl.")
    parser.add_argument("--token-ledger", type=Path, help="Append-only token event and stored-cost path. Defaults to ~/.codex-switch/usage_monitor_token_events.jsonl.")
    parser.add_argument("--sample-log", type=Path, help="Detailed JSONL diagnostic sample path. Defaults to ~/.codex-switch/usage_monitor_diagnostic_samples.jsonl.")
    parser.add_argument("--sample-log-max-bytes", type=int, default=DEFAULT_SAMPLE_LOG_MAX_BYTES, help="Maximum detailed sample debug log size before oldest rows are trimmed. Defaults to 50 MiB.")
    parser.add_argument("--dashboard", action="store_true", help="Open the local dashboard in the default browser after starting the server.")
    parser.add_argument("--reencrypt-cloud", action="store_true", help="Re-encrypt and verify every encrypted WebDAV payload with the configured hash key, then exit.")
    parser.add_argument("--compact-history-days", type=int, help="Rewrite quota history to keep only samples newer than this many days.")
    parser.add_argument("--local-only", action="store_true", help="Only scan local Codex session logs; do not call ChatGPT usage endpoints.")
    parser.add_argument("--no-token-scan", action="store_true", help="Disable local Codex session token usage scanning.")
    parser.add_argument("--repair-processing", action="store_true", help="Rebuild derived processing checkpoints and dashboard rows; preserve canonical histories and v4 collection periods.")
    parser.add_argument("--retry-limit", type=int, default=DEFAULT_RETRY_LIMIT, help="Retries for HTTP and dashboard polling failures before raising; network errors retry indefinitely. Defaults to 3.")
    args = parser.parse_args()
    if not instance_lock.acquire():
        print("Cannot start dashboard: another monitor instance is already running.", file=sys.stderr, flush=True)
        return 1
    args.instance_lock = instance_lock

    args.data_home = codex_switch_home()
    migrate_default_monitor_data(Path(__file__).resolve().parent, args.data_home)
    legacy_history = default_history_path(args.data_home)
    args.quota_history = args.quota_history or default_quota_history_path(legacy_history)
    args.token_ledger = args.token_ledger or default_token_ledger_path(legacy_history)
    args.sample_log = args.sample_log or default_sample_log_path(legacy_history)
    args.usage_sync_cache = default_usage_sync_cache_path(args.quota_history)
    args.state = args.data_home / "usage_monitor_state.json"
    args.dashboard_cache = default_dashboard_cache_path(args.quota_history)
    migrate_installed_usage_data(
        Path(__file__).resolve().parent, args.data_home, args.quota_history, args.token_ledger, args.sample_log,
        runtime_state_path=args.state, dashboard_cache_path=args.dashboard_cache, usage_sync_cache_path=args.usage_sync_cache,
        codex_home=args.codex_home,
    )
    args.account_root = args.data_home / "accounts"
    args.legacy_account_root = args.auth.parent / "usage-monitor-accounts"
    migrate_account_vault(args.legacy_account_root, args.account_root)
    from monitor_processing import ProcessingIndex
    index = ProcessingIndex(args.usage_sync_cache.with_name(f"{args.usage_sync_cache.stem}-v4.sqlite3"))
    if args.repair_processing:
        index.repair()
    with index.transaction() as db:
        backfilled = index.get(db, "diagnosticBackfillComplete", False)
    if not backfilled:
        backfill_quota_history(args.sample_log, args.quota_history)
        with index.transaction() as db:
            index.put(db, "diagnosticBackfillComplete", True)
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
