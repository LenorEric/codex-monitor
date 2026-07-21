#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

if sys.version_info < (3, 12):
    raise SystemExit("Codex Usage Monitor requires Python 3.12 or newer.")

from monitor_accounts import migrate_account_vault
from monitor_cloud import CloudManager
from monitor_common import *
from monitor_dashboard import *
from monitor_events import *
from monitor_history import *
from monitor_quota import *
from monitor_skills import SkillManager
from monitor_tokens import *


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll Codex ChatGPT-account usage/rate-limit data and local Codex session token usage.")
    parser.add_argument("--auth", type=Path, default=codex_home() / "auth.json")
    parser.add_argument("--codex-home", type=Path, default=codex_home(), help="Codex config directory containing sessions/. Defaults to CODEX_HOME or ~/.codex.")
    parser.add_argument("--interval", type=int, default=90)
    parser.add_argument("--timeout", type=int, default=10, help="Per-request timeout in seconds.")
    parser.add_argument("--history", type=Path, help="JSONL history path. Defaults to ~/.codex-switch/usage_monitor_history.jsonl.")
    parser.add_argument("--quota-history", type=Path, help="Per-account quota history path. Defaults to usage_monitor_quota_history.jsonl beside the delta history file.")
    parser.add_argument("--token-session-history", type=Path, help="Per-session token and cost history path. Defaults to usage_monitor_token_sessions.jsonl beside the delta history file.")
    parser.add_argument("--sample-log", type=Path, help="Detailed JSONL sample debug log path. Defaults to usage_monitor_samples.jsonl beside the history file.")
    parser.add_argument("--sample-log-max-bytes", type=int, default=DEFAULT_SAMPLE_LOG_MAX_BYTES, help="Maximum detailed sample debug log size before oldest rows are trimmed. Defaults to 50 MiB.")
    parser.add_argument("--dashboard", action="store_true", help="Open the local dashboard in the default browser after starting the server.")
    parser.add_argument("--process-history", action="store_true", help="Extract and print valid delta cost/percent pairs from the local JSONL history, then exit.")
    parser.add_argument("--reencrypt-cloud", action="store_true", help="Re-encrypt and verify every encrypted WebDAV payload with the configured hash key, then exit.")
    parser.add_argument("--compact-history-days", type=int, help="Rewrite delta and quota history to keep only samples newer than this many days.")
    parser.add_argument("--local-only", action="store_true", help="Only scan local Codex session logs; do not call ChatGPT usage endpoints.")
    parser.add_argument("--no-token-scan", action="store_true", help="Disable local Codex session token usage scanning.")
    parser.add_argument("--retry-limit", type=int, default=DEFAULT_RETRY_LIMIT, help="Retries for HTTP and dashboard polling failures before raising; network errors retry indefinitely. Defaults to 3.")
    args = parser.parse_args()

    args.data_home = codex_switch_home()
    if args.history is None:
        migrate_default_monitor_data(Path(__file__).resolve().parent, args.data_home)
        args.history = default_history_path(args.data_home)
    args.sample_log = args.sample_log or default_sample_log_path(args.history)
    args.quota_history = args.quota_history or default_quota_history_path(args.history)
    args.token_session_history = args.token_session_history or default_token_session_history_path(args.history)
    args.usage_sync_cache = default_usage_sync_cache_path(args.history)
    args.state = state_path_for(args.history)
    args.account_root = args.data_home / "accounts"
    args.legacy_account_root = args.auth.parent / "usage-monitor-accounts"
    migrate_account_vault(args.legacy_account_root, args.account_root)
    backfill_quota_history(args.sample_log, args.quota_history)
    if args.process_history:
        history = load_history(args.history)
        print_valid_delta_events(valid_delta_events(derive_history_events(history)), (load_state(args.state) or {}).get("lastSample"))
        return 0
    if args.reencrypt_cloud:
        result = CloudManager(args.data_home, SkillManager(args.codex_home, args.data_home), None).reencrypt_remote_data()
        print(f"Re-encrypted and verified {result['reencrypted']} cloud payloads.")
        return 0

    opener = None
    if not args.local_only:
        opener = opener_for(args.auth.parent)
    return serve_dashboard(args, opener)


if __name__ == "__main__":
    raise SystemExit(main())
