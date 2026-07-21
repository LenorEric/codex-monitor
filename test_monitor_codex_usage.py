import http.server
import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import unittest
import urllib.error
import urllib.request
import uuid
import zipfile
from io import BytesIO, StringIO
from contextlib import contextmanager
from unittest import mock
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import monitor_common
import monitor_codex_usage
import monitor_dashboard
import monitor_history
import monitor_quota
import monitor_tokens
from monitor_accounts import AccountError, AccountManager, atomic_write_json
from monitor_cloud import (
    AUTO_FETCH_INTERVAL_SECONDS, AUTO_PUSH_MAX_ATTEMPTS, AUTO_PUSH_RETRY_SECONDS, AUTO_PUSH_STABLE_SECONDS, USAGE_PACK_MAX_BYTES, USAGE_SYNC_INTERVAL_SECONDS, CloudError, CloudManager, CryptoBox, WebDavClient,
    control_password_matches, hash_control_password, load_server_config, new_control_password_salt, normalized_webdav_identity, passphrase_hash, valid_passphrase_hash, webdav_passphrase_salt,
)
from monitor_skills import MANIFEST_FULL_REHASH_SECONDS, SkillError, SkillManager, _safe_name
from monitor_usage_sync import UsageDataStore, aggregate_cost_intervals, merge_token_rows, record_key, validate_sync_operation
from monitor_codex_usage import (
    add_token_delta,
    append_capped_jsonl,
    append_history,
    apply_runtime_cost_measurement,
    build_delta_event_series,
    calculate_token_costs,
    compact_delta_event,
    compact_history,
    compact_monitor_state,
    compact_quota,
    compact_quota_for_debug,
    collect_with_bad_remote_usage_retry,
    collect_usage_sample,
    dashboard_html,
    dashboard_display,
    DashboardHTTPServer,
    derive_history_events,
    empty_token_totals,
    format_ratio_warning,
    format_special_event,
    format_valid_delta_event,
    is_client_disconnect,
    load_history,
    make_history_sample,
    new_valid_delta_events,
    parse_token_usage,
    poll_sleep_seconds,
    pricing_for_model,
    process_sample_delta_events,
    quota_history_windows,
    request_json,
    refresh_access_token,
    reset_runtime_baselines,
    sample_debug_log_row,
    token_expired,
    UsageError,
    write_history,
)


class MonitorCodexUsageTests(unittest.TestCase):
    def test_cloud_maintenance_reports_each_network_outage_once(self):
        state = monitor_dashboard.UsageDashboardState.__new__(monitor_dashboard.UsageDashboardState)
        state.cloud = SimpleNamespace(maintenance_tick=mock.Mock(side_effect=(CloudError("offline", 502, category="network"), CloudError("offline", 502, category="network"), None, CloudError("offline", 502, category="network"))))
        state.cloud_maintenance_event = mock.Mock(wait=mock.Mock(side_effect=lambda _: setattr(state, "running", False) if state.cloud.maintenance_tick.call_count == 4 else None), clear=mock.Mock())
        state.cloud_maintenance_connection_failed = False
        state.running = True
        with mock.patch("sys.stderr", new_callable=StringIO) as stderr:
            state.run_cloud_maintenance()
        self.assertEqual(stderr.getvalue().count("Cloud maintenance failed: offline"), 2)

    def test_proxy_config_uses_urllib_scheme_names_and_all_proxy_fallback(self):
        with self.account_directory() as directory:
            (directory / "config.toml").write_text('HTTP_PROXY = "http-proxy:8080"\nALL_PROXY = "all-proxy:1080"\n', encoding="utf-8")
            self.assertEqual(monitor_common.load_proxy_config(directory), {"http": "http://http-proxy:8080", "https": "http://all-proxy:1080"})

    def test_non_windows_proxy_discovery_does_not_require_winreg(self):
        with mock.patch.object(monitor_common, "winreg", None), mock.patch("urllib.request.getproxies", return_value={"http": "proxy:8080", "all": "fallback:1080", "no": "localhost"}):
            self.assertEqual(monitor_common.load_windows_system_proxy(), {})
            self.assertEqual(monitor_common.load_environment_proxy(), {"http": "http://proxy:8080", "https": "http://fallback:1080"})

    def test_opener_uses_direct_connection_when_no_proxy_is_configured(self):
        with self.account_directory() as directory:
            with (
                mock.patch.object(monitor_common, "load_windows_system_proxy", return_value={}), mock.patch.object(monitor_common, "load_environment_proxy", return_value={}),
                mock.patch.object(monitor_common, "load_proxy_config", return_value={}), mock.patch("urllib.request.build_opener") as build_opener,
            ):
                monitor_common.opener_for(directory)
        self.assertEqual(build_opener.call_args.args[0].proxies, {})
        self.assertIsInstance(build_opener.call_args.args[1], monitor_common.SafeRedirectHandler)

    def test_opener_uses_system_proxy_when_configured(self):
        with self.account_directory() as directory:
            with (
                mock.patch.object(monitor_common, "load_windows_system_proxy", return_value={"http": "http://system-proxy:8080", "https": "http://system-proxy:8080"}),
                mock.patch.object(monitor_common, "load_environment_proxy") as environment_proxy, mock.patch.object(monitor_common, "load_proxy_config") as config_proxy,
                mock.patch("urllib.request.build_opener") as build_opener,
            ):
                monitor_common.opener_for(directory)
        self.assertEqual(build_opener.call_args.args[0].proxies, {"http": "http://system-proxy:8080", "https": "http://system-proxy:8080"})
        environment_proxy.assert_not_called()
        config_proxy.assert_not_called()

    def test_process_history_does_not_initialize_proxy(self):
        with self.account_directory() as directory:
            with (
                mock.patch.object(sys, "argv", ["monitor_codex_usage.py", "--process-history", "--history", str(directory / "history.jsonl")]),
                mock.patch.object(monitor_codex_usage, "migrate_account_vault"), mock.patch.object(monitor_codex_usage, "backfill_quota_history"),
                mock.patch.object(monitor_codex_usage, "load_history", return_value=[]), mock.patch.object(monitor_codex_usage, "load_state", return_value={}),
                mock.patch.object(monitor_codex_usage, "print_valid_delta_events"), mock.patch.object(monitor_codex_usage, "opener_for") as opener_for,
            ):
                self.assertEqual(monitor_codex_usage.main(), 0)
        opener_for.assert_not_called()

    def test_local_only_does_not_initialize_proxy(self):
        with self.account_directory() as directory:
            with (
                mock.patch.object(sys, "argv", ["monitor_codex_usage.py", "--local-only", "--history", str(directory / "history.jsonl")]),
                mock.patch.object(monitor_codex_usage, "migrate_account_vault"), mock.patch.object(monitor_codex_usage, "backfill_quota_history"),
                mock.patch.object(monitor_codex_usage, "serve_dashboard", return_value=0), mock.patch.object(monitor_codex_usage, "opener_for") as opener_for,
            ):
                self.assertEqual(monitor_codex_usage.main(), 0)
        opener_for.assert_not_called()

    def test_codex_home_expands_cross_platform_home_syntax(self):
        with mock.patch.dict("os.environ", {"CODEX_HOME": "~/.custom-codex"}):
            self.assertEqual(monitor_common.codex_home(), Path.home() / ".custom-codex")

    @contextmanager
    def account_directory(self):
        path = Path(__file__).parent / f".account-test-{uuid.uuid4().hex}"
        path.mkdir()
        try:
            yield path
        finally:
            shutil.rmtree(path, ignore_errors=True)

    def account_auth(self, account_id: str, refresh_token: str, **extra) -> dict:
        return {"auth_mode": "chatgpt", "tokens": {"account_id": account_id, "access_token": f"access-{account_id}", "id_token": f"id-{account_id}", "refresh_token": refresh_token}, **extra}

    def test_account_manager_bootstraps_current_account_without_changing_auth_bytes(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            original = json.dumps(self.account_auth("acct-a", "refresh-a", unknown={"kept": True}), separators=(",", ":")).encode()
            auth_path.write_bytes(original)
            manager = AccountManager(auth_path)
            self.assertEqual(auth_path.read_bytes(), original)
            self.assertEqual((manager.root / "ppl-pro" / "auth.json").read_bytes(), original)
            self.assertEqual(manager.status()["items"][0]["label"], "Current account")
            self.assertEqual(manager.status()["activeAccountId"], "ppl-pro")
            self.assertNotIn("refresh-a", json.dumps(manager.status()))
            self.assertNotIn("access-acct-a", json.dumps(manager.status()))
            self.assertNotIn("acct-a", json.dumps(manager.status()))

    def test_account_manager_bootstraps_without_an_account_when_auth_is_missing(self):
        with self.account_directory() as directory:
            manager = AccountManager(directory / "auth.json")
            self.assertTrue(manager.status()["awaitingLogin"])
            self.assertIsNone(manager.status()["activeAccountId"])
            self.assertEqual(manager.status()["items"], [])

    def test_account_manager_moves_legacy_vault_to_requested_data_root(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            legacy_manager = AccountManager(auth_path)
            legacy_root = legacy_manager.root
            target_root = directory / ".codex-switch" / "accounts"

            manager = AccountManager(auth_path, target_root, legacy_root)

            self.assertEqual(manager.root, target_root)
            self.assertFalse(legacy_root.exists())
            self.assertTrue((target_root / "accounts.json").exists())
            self.assertEqual((target_root / "ppl-pro" / "auth.json").read_bytes(), auth_path.read_bytes())

    def test_default_monitor_paths_and_migration_use_codex_switch_home(self):
        with self.account_directory() as directory:
            legacy_home, data_home = directory / "legacy", directory / ".codex-switch"
            legacy_home.mkdir()
            expected = {
                "usage_monitor_history.jsonl": b'{"history":true}\n',
                "usage_monitor_quota_history.jsonl": b'{"quota":true}\n',
                "usage_monitor_samples.jsonl": b'{"samples":true}\n',
                "usage_monitor_state.json": b'{"state":true}\n',
            }
            for name, data in expected.items():
                (legacy_home / name).write_bytes(data)

            moved = monitor_history.migrate_default_monitor_data(legacy_home, data_home)

            self.assertEqual(moved, list(expected))
            self.assertEqual(monitor_history.default_history_path(data_home), data_home / "usage_monitor_history.jsonl")
            self.assertEqual(monitor_history.default_quota_history_path(data_home / "usage_monitor_history.jsonl"), data_home / "usage_monitor_quota_history.jsonl")
            self.assertEqual(monitor_history.default_quota_history_path(data_home / "custom.jsonl"), data_home / "custom.quota.jsonl")
            for name, data in expected.items():
                self.assertFalse((legacy_home / name).exists())
                self.assertEqual((data_home / name).read_bytes(), data)

    def test_account_manager_new_login_and_switch_preserve_rotated_refresh_token(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-original")), encoding="utf-8")
            manager = AccountManager(auth_path)
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-rotated", unknown="preserved")), encoding="utf-8")
            manager.sync_active_from_live()
            manager.create_account("Second")
            self.assertFalse(auth_path.exists())
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            self.assertFalse(manager.status()["awaitingLogin"])
            manager.switch("ppl-pro")
            restored = json.loads(auth_path.read_text(encoding="utf-8"))
            self.assertEqual(restored["tokens"]["refresh_token"], "refresh-rotated")
            self.assertEqual(restored["unknown"], "preserved")

    def test_refreshed_auth_is_atomically_mirrored_to_active_account(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth = self.account_auth("acct-a", "refresh-original")
            auth["tokens"]["access_token"] = "x.eyJleHAiOjB9.x"
            auth_path.write_text(json.dumps(auth), encoding="utf-8")
            manager = AccountManager(auth_path)
            with mock.patch.object(monitor_common, "request_json", return_value=(200, {"access_token": "access-rotated", "refresh_token": "refresh-rotated", "id_token": "id-rotated"})):
                refresh_access_token(auth, object(), auth_path, 10)
            manager.sync_active_from_live()
            saved = json.loads((manager.root / "ppl-pro" / "auth.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["tokens"]["refresh_token"], "refresh-rotated")
            self.assertEqual(saved["tokens"]["access_token"], "access-rotated")

    def test_actual_token_refresh_holds_account_lock_and_mirrors_rotation(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth = self.account_auth("acct-a", "refresh-original")
            auth["tokens"]["access_token"] = "x.eyJleHAiOjB9.x"
            auth_path.write_text(json.dumps(auth), encoding="utf-8")
            manager = AccountManager(auth_path)
            contender_acquired = []

            def refresh_response(*_args, **_kwargs):
                def contend_for_lock():
                    contender_acquired.append(manager.lock.acquire(timeout=0.05))
                    if contender_acquired[-1]:
                        manager.lock.release()

                thread = threading.Thread(target=contend_for_lock)
                thread.start()
                thread.join()
                return 200, {"access_token": "access-rotated", "refresh_token": "refresh-rotated"}

            with mock.patch.object(monitor_common, "request_json", side_effect=refresh_response):
                refresh_access_token(auth, object(), auth_path, 10, auth_lock=manager.lock, refreshed_callback=manager.sync_active_from_live)
            self.assertEqual(contender_acquired, [False])
            saved = json.loads((manager.root / "ppl-pro" / "auth.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["tokens"]["refresh_token"], "refresh-rotated")

    def test_token_refresh_starts_when_thirty_percent_of_lifetime_remains(self):
        with mock.patch.object(monitor_common, "jwt_payload", return_value={"iat": 100, "exp": 1100}):
            with mock.patch.object(monitor_common.time, "time", return_value=799.999):
                self.assertFalse(token_expired("synthetic-token"))
            with mock.patch.object(monitor_common.time, "time", return_value=800):
                self.assertTrue(token_expired("synthetic-token"))

    def test_token_refresh_uses_sixty_second_fallback_without_issued_at(self):
        with mock.patch.object(monitor_common, "jwt_payload", return_value={"exp": 1100}):
            with mock.patch.object(monitor_common.time, "time", return_value=1039.999):
                self.assertFalse(token_expired("synthetic-token"))
            with mock.patch.object(monitor_common.time, "time", return_value=1040):
                self.assertTrue(token_expired("synthetic-token"))

    def test_token_with_unusable_expiration_is_refreshed(self):
        with mock.patch.object(monitor_common, "jwt_payload", return_value={"iat": 100}):
            self.assertTrue(token_expired("synthetic-token"))

    def test_token_refresh_margin_is_capped_at_ten_minutes(self):
        with mock.patch.object(monitor_common, "jwt_payload", return_value={"iat": 100, "exp": 10100}):
            with mock.patch.object(monitor_common.time, "time", return_value=9499.999):
                self.assertFalse(token_expired("synthetic-token"))
            with mock.patch.object(monitor_common.time, "time", return_value=9500):
                self.assertTrue(token_expired("synthetic-token"))

    def test_rotating_token_refresh_does_not_retry_ambiguous_network_failure(self):
        class FailingOpener:
            def __init__(self):
                self.count = 0

            def open(self, request, timeout):
                self.count += 1
                raise urllib.error.URLError("response lost")

        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth = self.account_auth("acct-a", "refresh-a")
            auth["tokens"]["access_token"] = "x.eyJleHAiOjB9.x"
            auth_path.write_text(json.dumps(auth), encoding="utf-8")
            opener = FailingOpener()
            with self.assertRaisesRegex(UsageError, "not safely repeatable"):
                refresh_access_token(auth, opener, auth_path, 1, retries=3)
            self.assertEqual(opener.count, 1)

    def test_bootstrap_waits_for_complete_auth_instead_of_vaulting_it(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps({"tokens": {"account_id": "acct-a", "access_token": "access-a"}}), encoding="utf-8")
            manager = AccountManager(auth_path)
            self.assertTrue(manager.status()["awaitingLogin"])
            self.assertIn("refresh token", manager.status()["error"])
            self.assertFalse(manager._account_path("ppl-pro").exists())

    def test_incomplete_live_update_does_not_replace_complete_saved_credentials(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            auth_path.write_text(json.dumps({"tokens": {"account_id": "acct-a", "access_token": "partial-access"}}), encoding="utf-8")
            self.assertFalse(manager.sync_active_from_live())
            saved = json.loads(manager._account_path("ppl-pro").read_text(encoding="utf-8"))
            self.assertEqual(saved["tokens"]["refresh_token"], "refresh-a")

    def test_active_account_waits_for_codex_instead_of_refreshing(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-original")), encoding="utf-8")
            args = SimpleNamespace(
                auth=auth_path, state=directory / "state.json", history=directory / "history.jsonl", sample_log=directory / "samples.jsonl", codex_home=directory,
                local_only=False, no_token_scan=True, interval=90, timeout=10, retry_limit=0, sample_log_max_bytes=1024, compact_history_days=None,
            )
            state = monitor_dashboard.UsageDashboardState(args, object())

            with mock.patch.object(monitor_common, "request_json") as refresh_request:
                with mock.patch.object(monitor_quota, "request_json") as usage_request:
                    with self.assertRaisesRegex(UsageError, "Waiting for Codex to refresh"):
                        state.poll_once()
            saved = json.loads((state.accounts.root / "ppl-pro" / "auth.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["tokens"]["refresh_token"], "refresh-original")
            refresh_request.assert_not_called()
            usage_request.assert_not_called()

    def test_active_account_uses_access_token_until_actual_expiration(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth = self.account_auth("acct-a", "refresh-a")
            auth["tokens"]["access_token"] = "synthetic-token"
            with mock.patch.object(monitor_common, "jwt_payload", return_value={"iat": 100, "exp": 1100}), mock.patch.object(monitor_common.time, "time", return_value=1000), mock.patch.object(monitor_common, "request_json") as refresh_request:
                self.assertEqual(refresh_access_token(auth, object(), auth_path, 10, allow_refresh=False), "synthetic-token")
            refresh_request.assert_not_called()

    def test_token_refresh_updates_local_vault_without_cloud_upload(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-original")), encoding="utf-8")
            args = SimpleNamespace(
                auth=auth_path, state=directory / "state.json", history=directory / "history.jsonl", sample_log=directory / "samples.jsonl", codex_home=directory,
                local_only=False, no_token_scan=True, interval=90, timeout=10, retry_limit=0, sample_log_max_bytes=1024, compact_history_days=None,
            )
            state = monitor_dashboard.UsageDashboardState(args, object())
            state.accounts.manifest["cloudBindingEnabled"] = True
            state.accounts.active_account()["cloud"] = {"state": "bound-local", "accountKey": "opaque", "boundMachineId": "machine"}
            state.accounts._save_manifest()
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-rotated")), encoding="utf-8")

            with mock.patch.object(state.cloud, "push") as push:
                self.assertTrue(args.auth_refreshed_callback())

            push.assert_not_called()
            self.assertEqual(json.loads((state.accounts.root / "ppl-pro" / "auth.json").read_text(encoding="utf-8"))["tokens"]["refresh_token"], "refresh-rotated")

    def test_valid_token_poll_does_not_hold_account_lock(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            args = SimpleNamespace(
                auth=auth_path, state=directory / "state.json", history=directory / "history.jsonl", sample_log=directory / "samples.jsonl", codex_home=directory,
                local_only=False, no_token_scan=True, interval=90, timeout=10, retry_limit=0, sample_log_max_bytes=1024, compact_history_days=None,
            )
            state = monitor_dashboard.UsageDashboardState(args, object())

            def verify_unlocked(*_args, **_kwargs):
                acquired = []

                def acquire_from_another_thread():
                    acquired.append(state.accounts.lock.acquire(timeout=0.2))
                    if acquired[-1]:
                        state.accounts.lock.release()

                thread = threading.Thread(target=acquire_from_another_thread)
                thread.start()
                thread.join()
                self.assertEqual(acquired, [True])
                raise UsageError("stop after lock check")

            with mock.patch.object(monitor_dashboard, "collect_with_bad_remote_usage_retry", side_effect=verify_unlocked):
                with self.assertRaises(UsageError):
                    state.poll_once()

    def test_switch_during_unlocked_poll_discards_old_account_sample(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            args = SimpleNamespace(
                auth=auth_path, state=directory / "state.json", history=directory / "history.jsonl", sample_log=directory / "samples.jsonl", codex_home=directory,
                local_only=False, no_token_scan=True, interval=90, timeout=10, retry_limit=0, sample_log_max_bytes=1024, compact_history_days=None,
            )
            state = monitor_dashboard.UsageDashboardState(args, object())
            with mock.patch("builtins.print"):
                second_id = state.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            state.accounts.status()
            with mock.patch("builtins.print"):
                state.switch_account("ppl-pro")

            def switch_while_request_is_in_flight(*_args, **_kwargs):
                with mock.patch("builtins.print"):
                    state.switch_account(second_id)
                return {"checkedAt": "2030-01-01T00:00:00Z", "windows": {}, "errors": {}}

            with mock.patch.object(monitor_dashboard, "collect_with_bad_remote_usage_retry", side_effect=switch_while_request_is_in_flight):
                self.assertEqual(state.poll_once(), {})
            self.assertEqual(state.accounts.status()["activeAccountId"], second_id)
            self.assertIsNone(state.last_sample)

    def test_inactive_local_account_usage_is_recorded_every_ten_minutes(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            args = SimpleNamespace(
                auth=auth_path, state=directory / "state.json", history=directory / "history.jsonl", quota_history=directory / "quota.jsonl", sample_log=directory / "samples.jsonl",
                codex_home=directory, local_only=False, no_token_scan=True, interval=90, timeout=10, retry_limit=0, sample_log_max_bytes=1024, compact_history_days=None,
            )
            state = monitor_dashboard.UsageDashboardState(args, object())
            with mock.patch("builtins.print"):
                second_id = state.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            state.accounts.status()
            with mock.patch("builtins.print"):
                state.switch_account("ppl-pro")
            live_auth = auth_path.read_bytes()
            calls = []

            def usage(auth, _opener, polled_auth_path, *_args, **_kwargs):
                calls.append((auth["tokens"]["account_id"], polled_auth_path))
                polled_auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-rotated")), encoding="utf-8")
                return {"usage": {"windows": {
                    "5h": {"path": "$.rate_limit.primary", "values": {"used_percent": 12, "reset_at": 1893456000}},
                    "7d": {"path": "$.rate_limit.secondary", "values": {"used_percent": 34, "reset_at": 1893888000}},
                }}}

            with mock.patch.object(monitor_dashboard, "fetch_usage_with_percent_arbitration", side_effect=usage):
                self.assertEqual(state.poll_due_inactive_accounts(now=100), 1)
                self.assertEqual(state.inactive_account_poll_wait_seconds(now=100), 600)
                self.assertEqual(state.poll_due_inactive_accounts(now=699), 0)
                self.assertEqual(state.inactive_account_poll_wait_seconds(now=699), 1)
                self.assertEqual(state.poll_due_inactive_accounts(now=700), 1)

            self.assertEqual([account_id for account_id, _path in calls], ["acct-b", "acct-b"])
            self.assertTrue(all(path != auth_path and path != state.accounts._account_path(second_id) for _account_id, path in calls))
            self.assertEqual(auth_path.read_bytes(), live_auth)
            self.assertEqual(json.loads(state.accounts._account_path(second_id).read_text(encoding="utf-8"))["tokens"]["refresh_token"], "refresh-rotated")
            rows = monitor_history.load_quota_history(args.quota_history)
            self.assertEqual([(row["accountSlotId"], row["accountLabel"]) for row in rows], [(second_id, "Second"), (second_id, "Second")])
            self.assertEqual(rows[0]["windows"]["5h"]["usedPercent"], 12.0)
            self.assertEqual(state.account_statuses[second_id]["windows"]["7d"]["usedPercent"], 34.0)
            with mock.patch("builtins.print"):
                state.switch_account(second_id)
            cached_status = state.status_payload()
            self.assertIsNone(state.last_sample)
            self.assertEqual(cached_status["display"]["statusBarText"], "5h 12.0% · 7d 34.0%")
            self.assertEqual(cached_status["lastSample"]["windows"]["5h"]["resetAt"], "2030-01-01T00:00:00Z")
            self.assertEqual(state.cached_series_response()[0]["lastSample"]["windows"]["7d"]["usedPercent"], 34.0)
            self.assertTrue(state.wake_event.is_set())
            self.assertFalse(any(state.accounts.root.glob(".inactive-usage-*.json")))

    def test_inactive_poll_refresh_commit_does_not_overwrite_changed_credentials(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            second_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            manager.status()
            manager.switch("ppl-pro")
            credential = manager.inactive_ready_credentials()[0]
            changed = json.dumps(self.account_auth("acct-b", "refresh-newer")).encode()
            manager._account_path(credential["id"]).write_bytes(changed)
            refreshed = json.dumps(self.account_auth("acct-b", "refresh-polled")).encode()

            self.assertFalse(manager.commit_polled_credentials(credential["id"], credential["fingerprint"], refreshed))
            self.assertEqual(manager._account_path(credential["id"]).read_bytes(), changed)

    def test_inactive_ready_account_refreshes_expiring_access_token_but_empty_account_is_skipped(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            args = SimpleNamespace(
                auth=auth_path, state=directory / "state.json", history=directory / "history.jsonl", quota_history=directory / "quota.jsonl", sample_log=directory / "samples.jsonl",
                codex_home=directory, local_only=False, no_token_scan=True, interval=90, timeout=10, retry_limit=0, sample_log_max_bytes=1024, compact_history_days=None,
            )
            state = monitor_dashboard.UsageDashboardState(args, object())
            with mock.patch("builtins.print"):
                second_id = state.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            state.accounts.status()
            with mock.patch("builtins.print"):
                empty_id = state.create_account("Empty")["activeAccountId"]
                state.switch_account("ppl-pro")

            usage_response = {"rate_limit": {
                "primary": {"window_minutes": 300, "used_percent": 12, "reset_at": 1893456000},
                "secondary": {"window_minutes": 10080, "used_percent": 34, "reset_at": 1893888000},
            }}
            with mock.patch.object(monitor_common, "token_expired", return_value=True), mock.patch.object(
                monitor_common, "request_json", return_value=(200, {"access_token": "access-rotated", "refresh_token": "refresh-rotated"}),
            ) as refresh_request, mock.patch.object(monitor_quota, "request_json", return_value=(200, usage_response)):
                self.assertEqual(state.poll_due_inactive_accounts(now=100), 1)

            refresh_request.assert_called_once()
            self.assertEqual(json.loads(state.accounts._account_path(second_id).read_text(encoding="utf-8"))["tokens"]["refresh_token"], "refresh-rotated")
            self.assertFalse(state.accounts._account_path(empty_id).exists())
            self.assertEqual([credential["id"] for credential in state.accounts.inactive_ready_credentials()], [second_id])

    def test_inactive_poll_failure_is_exposed_as_stale_account_health(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            args = SimpleNamespace(
                auth=auth_path, state=directory / "state.json", history=directory / "history.jsonl", quota_history=directory / "quota.jsonl", sample_log=directory / "samples.jsonl",
                codex_home=directory, local_only=False, no_token_scan=True, interval=90, timeout=10, retry_limit=0, sample_log_max_bytes=1024, compact_history_days=None,
            )
            state = monitor_dashboard.UsageDashboardState(args, object())
            with mock.patch("builtins.print"):
                second_id = state.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            state.accounts.status()
            with mock.patch("builtins.print"):
                state.switch_account("ppl-pro")

            with mock.patch.object(state, "_poll_inactive_account", side_effect=UsageError("refresh rejected")), mock.patch("sys.stderr"):
                self.assertEqual(state.poll_due_inactive_accounts(now=100), 0)

            account = next(account for account in state.status_payload()["accounts"]["items"] if account["id"] == second_id)
            self.assertTrue(account["pollError"])
            self.assertTrue(account["stale"])
            self.assertIsNotNone(account["pollErrorAt"])

    def test_dashboard_prints_secret_free_account_events(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-secret-a")), encoding="utf-8")
            args = SimpleNamespace(
                auth=auth_path, state=directory / "state.json", history=directory / "history.jsonl", sample_log=directory / "samples.jsonl", codex_home=directory,
                local_only=False, no_token_scan=True, interval=90, timeout=10, retry_limit=0, sample_log_max_bytes=1024, compact_history_days=None,
            )
            state = monitor_dashboard.UsageDashboardState(args, object())
            with mock.patch("builtins.print") as printed:
                second_id = state.create_account("Second\nAccount")["activeAccountId"]
                auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-secret-b")), encoding="utf-8")
                state.accounts.status()
                state.switch_account("ppl-pro")
                state.switch_account(second_id)
                state.rename_account(second_id, "Renamed Second")
                state.switch_account("ppl-pro")
                state.delete_account(second_id)
            output = "\n".join(call.args[0] for call in printed.call_args_list)
            self.assertIn("Account event: saved 'Current account' and prepared 'Second\\nAccount' for sign-in.", output)
            self.assertIn("Account event: switched from 'Second\\nAccount' to 'Current account'.", output)
            self.assertIn("Account event: switched from 'Current account' to 'Second\\nAccount'.", output)
            self.assertIn("Account event: renamed 'Second\\nAccount' to 'Renamed Second'.", output)
            self.assertIn("Account event: deleted 'Renamed Second'.", output)
            self.assertNotIn("refresh-secret", output)

    def test_account_rename_updates_all_local_history_and_log_labels(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            args = SimpleNamespace(
                auth=auth_path, state=directory / "state.json", history=directory / "history.jsonl", quota_history=directory / "quota.jsonl", sample_log=directory / "samples.jsonl",
                codex_home=directory, local_only=False, no_token_scan=True, interval=90, timeout=10, retry_limit=0, sample_log_max_bytes=1024, compact_history_days=None,
            )
            state = monitor_dashboard.UsageDashboardState(args, object())
            args.history.write_text(json.dumps({"window": "5h", "delta": [{"accountSlotId": "ppl-pro", "accountLabel": "Current account", "deltaPercent": 1}, {"accountSlotId": "other", "accountLabel": "Other"}]}) + "\n", encoding="utf-8")
            args.quota_history.write_text(json.dumps({"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "ppl-pro", "accountLabel": "Current account", "windows": {"5h": {"usedPercent": 1}}}) + "\n", encoding="utf-8")
            args.sample_log.write_text(json.dumps({"sample": {"accountSlotId": "ppl-pro", "accountLabel": "Current account"}, "events": [{"accountSlotId": "ppl-pro", "accountLabel": "Current account"}, {"accountLabel": "Legacy name"}]}) + "\n", encoding="utf-8")
            args.state.write_text(json.dumps({"lastSample": {"accountSlotId": "ppl-pro", "accountLabel": "Current account"}}) + "\n", encoding="utf-8")
            state.runtime_state = {"lastSample": {"accountSlotId": "ppl-pro", "accountLabel": "Current account"}}
            state.last_sample = {"accountSlotId": "ppl-pro", "accountLabel": "Current account"}

            with mock.patch("builtins.print"):
                state.rename_account("ppl-pro", "Renamed Pro")

            for path in (args.history, args.quota_history, args.sample_log, args.state):
                values = monitor_history.parse_json_sequence(path.read_text(encoding="utf-8"))
                matching_labels = []
                def collect(value):
                    if isinstance(value, dict):
                        if value.get("accountSlotId") == "ppl-pro":
                            matching_labels.append(value.get("accountLabel"))
                        for child in value.values():
                            collect(child)
                    elif isinstance(value, list):
                        for child in value:
                            collect(child)
                collect(values)
                self.assertTrue(matching_labels)
                self.assertEqual(set(matching_labels), {"Renamed Pro"})
            self.assertEqual(json.loads(args.history.read_text(encoding="utf-8"))["delta"][1]["accountLabel"], "Other")
            self.assertEqual(json.loads(args.sample_log.read_text(encoding="utf-8"))["events"][1]["accountLabel"], "Legacy name")
            self.assertEqual(state.runtime_state["lastSample"]["accountLabel"], "Renamed Pro")
            self.assertEqual(state.last_sample["accountLabel"], "Renamed Pro")

    def test_account_rename_rolls_back_local_data_when_manifest_save_fails(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            history = directory / "history.jsonl"
            history.write_text('{"accountSlotId":"ppl-pro","accountLabel":"Current account"}\n', encoding="utf-8")
            original = history.read_bytes()

            with mock.patch.object(manager, "_save_manifest", side_effect=OSError("disk full")):
                with self.assertRaises(AccountError):
                    manager.rename("ppl-pro", "Renamed Pro", lambda account_id, label: monitor_history.rewrite_account_labels((history,), account_id, label))

            self.assertEqual(manager.active_account()["label"], "Current account")
            self.assertEqual(history.read_bytes(), original)

    def test_account_rename_keeps_all_data_when_a_local_log_is_invalid(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            history, invalid_log = directory / "history.jsonl", directory / "samples.jsonl"
            history.write_text('{"accountSlotId":"ppl-pro","accountLabel":"Current account"}\n', encoding="utf-8")
            invalid_log.write_text('{invalid json\n', encoding="utf-8")
            originals = history.read_bytes(), invalid_log.read_bytes()

            with self.assertRaises(AccountError):
                manager.rename("ppl-pro", "Renamed Pro", lambda account_id, label: monitor_history.rewrite_account_labels((history, invalid_log), account_id, label))

            self.assertEqual(manager.active_account()["label"], "Current account")
            self.assertEqual((history.read_bytes(), invalid_log.read_bytes()), originals)

    def test_account_manager_refuses_live_identity_mismatch(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            with self.assertRaises(AccountError) as raised:
                manager.sync_active_from_live()
            self.assertEqual(raised.exception.status, 409)

    def test_new_account_accepts_rotated_id_token_and_saves_current_auth(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            changed = self.account_auth("acct-a", "refresh-new")
            changed["tokens"]["id_token"] = "id-another-login"
            auth_path.write_text(json.dumps(changed), encoding="utf-8")
            manager.create_account("Second")

            self.assertEqual(json.loads((manager.root / "ppl-pro" / "auth.json").read_text(encoding="utf-8"))["tokens"]["id_token"], "id-another-login")
            self.assertFalse(auth_path.exists())
            self.assertNotEqual(manager.status()["activeAccountId"], "ppl-pro")
            self.assertEqual(len(manager.status()["items"]), 2)

    def test_switch_refuses_changed_account_id_even_when_id_token_matches(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            second_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            manager.status()
            manager.switch("ppl-pro")
            changed = self.account_auth("acct-other", "refresh-other")
            changed["tokens"]["id_token"] = "id-acct-a"
            auth_path.write_text(json.dumps(changed), encoding="utf-8")
            saved = (manager.root / "ppl-pro" / "auth.json").read_bytes()

            with self.assertRaises(AccountError) as raised:
                manager.switch(second_id)

            self.assertEqual(raised.exception.status, 409)
            self.assertIn("account_id", str(raised.exception))
            self.assertEqual(json.loads(auth_path.read_text(encoding="utf-8"))["tokens"]["account_id"], "acct-other")
            self.assertEqual((manager.root / "ppl-pro" / "auth.json").read_bytes(), saved)
            self.assertEqual(manager.status()["activeAccountId"], "ppl-pro")

    def test_account_change_refuses_when_verification_fields_are_missing(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            live = self.account_auth("acct-a", "refresh-a")
            del live["tokens"]["account_id"]
            auth_path.write_text(json.dumps(live), encoding="utf-8")

            with self.assertRaises(AccountError) as raised:
                manager.create_account("Second")

            self.assertEqual(raised.exception.status, 409)
            self.assertIn("account_id is missing", str(raised.exception))

    def test_signed_out_account_can_be_saved_switched_away_from_and_restored(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            second_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            manager.status()
            manager.switch("ppl-pro")
            signed_out = {"auth_mode": "chatgpt", "tokens": {}}
            auth_path.write_text(json.dumps(signed_out), encoding="utf-8")

            manager.switch(second_id)
            status = manager.switch("ppl-pro")

            self.assertEqual(status["activeAccountId"], "ppl-pro")
            self.assertTrue(next(account for account in status["items"] if account["id"] == "ppl-pro")["ready"])
            self.assertEqual(json.loads(auth_path.read_text(encoding="utf-8")), signed_out)

    def test_signed_out_inactive_account_can_be_released_without_identity_verification(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            manager.manifest["cloudBindingEnabled"] = True
            manager._find("ppl-pro")["cloud"] = {"state": "bound-local", "accountKey": "key-a", "boundMachineId": "machine"}
            manager._save_manifest()
            signed_out = {"auth_mode": "chatgpt", "tokens": {}}
            auth_path.write_text(json.dumps(signed_out), encoding="utf-8")
            second_id = manager.create_account("Second")["activeAccountId"]
            cloud = SimpleNamespace(begin_account_transition=mock.Mock(), clear_account_transition=mock.Mock(), release_account=mock.Mock(return_value={}))

            status = manager.release_cloud_account(cloud, "ppl-pro")

            self.assertEqual(status["activeAccountId"], second_id)
            self.assertEqual([account["id"] for account in status["items"]], [second_id])
            self.assertEqual(json.loads(cloud.release_account.call_args.args[1].decode("utf-8")), signed_out)
            cloud.begin_account_transition.assert_called_once_with("release", accountId="ppl-pro", accountKey="key-a", revisionId=hashlib.sha256(json.dumps(signed_out).encode()).hexdigest())

    def test_new_empty_accounts_support_create_switch_rename_and_delete(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            manager = AccountManager(auth_path)
            first_id = manager.create_account("First")["activeAccountId"]
            second_id = manager.create_account("Second")["activeAccountId"]
            manager.manifest["cloudBindingEnabled"] = True
            manager._save_manifest()

            self.assertEqual(manager.switch(first_id)["activeAccountId"], first_id)
            self.assertFalse(auth_path.exists())
            self.assertEqual(manager.rename(first_id, "First empty")["items"][0]["label"], "First empty")
            self.assertEqual(manager.switch(second_id)["activeAccountId"], second_id)
            status = manager.delete(first_id)

            self.assertEqual(status["activeAccountId"], second_id)
            self.assertEqual([account["id"] for account in status["items"]], [second_id])
            self.assertTrue(status["awaitingLogin"])
            self.assertFalse(auth_path.exists())

    def test_releasing_active_account_is_rejected(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            empty_id = manager.create_account("Empty")["activeAccountId"]
            manager.manifest["cloudBindingEnabled"] = True
            manager._find("ppl-pro")["cloud"] = {"state": "bound-local", "accountKey": "key-a", "boundMachineId": "machine"}
            manager._save_manifest()
            manager.switch("ppl-pro")
            cloud = SimpleNamespace(begin_account_transition=mock.Mock(), clear_account_transition=mock.Mock(), release_account=mock.Mock(return_value={}))

            with self.assertRaises(AccountError) as raised:
                manager.release_cloud_account(cloud, "ppl-pro")

            self.assertEqual(raised.exception.status, 409)
            self.assertEqual(manager.status()["activeAccountId"], "ppl-pro")
            self.assertTrue(auth_path.exists())
            cloud.release_account.assert_not_called()

    def test_new_empty_account_release_uploads_transferable_placeholder(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            empty_id = manager.create_account("Empty")["activeAccountId"]
            manager.manifest["cloudBindingEnabled"] = True
            manager._save_manifest()
            manager.switch("ppl-pro")

            cloud = SimpleNamespace(
                new_placeholder_account_key=mock.Mock(return_value="placeholder-key"), begin_account_transition=mock.Mock(), clear_account_transition=mock.Mock(),
                release_account=mock.Mock(return_value={}),
            )

            status = manager.release_cloud_account(cloud, empty_id)

            self.assertEqual(status["activeAccountId"], "ppl-pro")
            self.assertEqual([account["id"] for account in status["items"]], ["ppl-pro"])
            self.assertEqual(json.loads(auth_path.read_text(encoding="utf-8"))["tokens"]["account_id"], "acct-a")
            cloud.new_placeholder_account_key.assert_called_once_with()
            cloud.begin_account_transition.assert_called_once_with("release", accountId=empty_id, accountKey="placeholder-key", revisionId=hashlib.sha256(b"{}").hexdigest())
            cloud.release_account.assert_called_once_with("placeholder-key", b"{}", {"accountId": None, "email": None}, "Empty", ready=False, key_type="opaque")

    def test_empty_cloud_account_binds_as_login_placeholder(self):
        with self.account_directory() as directory:
            manager = AccountManager(directory / "auth.json")
            cloud = SimpleNamespace(
                begin_account_transition=mock.Mock(), clear_account_transition=mock.Mock(), machine_id="machine", delete_account_payloads=mock.Mock(),
                bind_account=mock.Mock(return_value=({"accountKey": "placeholder-key", "keyType": "opaque", "accountId": None, "label": "Empty", "ready": False}, b"{}", '"etag"')),
            )

            status = manager.bind_cloud_account(cloud, "placeholder-key")

            bound = next(account for account in status["items"] if account["accountKey"] == "placeholder-key")
            self.assertFalse(bound["ready"])
            self.assertFalse((manager.root / bound["id"] / "auth.json").exists())
            cloud.delete_account_payloads.assert_called_once_with("placeholder-key", '"etag"')

            manager.manifest["activeAccountId"] = bound["id"]
            manager._save_manifest()
            (directory / "auth.json").write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            self.assertTrue(manager.reconcile_pending_login())
            other_id = manager.create_account("Other")["activeAccountId"]
            release_cloud = SimpleNamespace(begin_account_transition=mock.Mock(), clear_account_transition=mock.Mock(), release_account=mock.Mock(return_value={}))
            manager.switch(other_id)
            manager.release_cloud_account(release_cloud, bound["id"])
            self.assertEqual(release_cloud.release_account.call_args.args[0], "placeholder-key")
            self.assertTrue(release_cloud.release_account.call_args.kwargs["ready"])
            self.assertEqual(release_cloud.release_account.call_args.kwargs["key_type"], "opaque")

    def test_account_change_refuses_when_live_auth_is_missing(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            auth_path.unlink()

            with self.assertRaises(AccountError) as raised:
                manager.create_account("Second")

            self.assertEqual(raised.exception.status, 409)
            self.assertIn("current auth.json is missing", str(raised.exception))
            self.assertEqual(manager.status()["activeAccountId"], "ppl-pro")

    def test_account_attribution_matches_registered_identity_and_marks_unregistered_unknown(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            manager.rename("ppl-pro", "Recorded Account")
            self.assertEqual(manager.attribution_for_auth(self.account_auth("acct-a", "another-refresh")), {"accountSlotId": "ppl-pro", "accountLabel": "Recorded Account"})
            self.assertEqual(manager.attribution_for_auth(self.account_auth("not-registered", "refresh-x")), {"accountSlotId": "unknown", "accountLabel": "Unknown"})

    def test_duplicate_pending_login_refreshes_existing_account_and_leaves_new_slot_empty(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            pending_id = manager.create_account("Duplicate")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-new")), encoding="utf-8")
            status = manager.status()

            self.assertEqual(status["activeAccountId"], pending_id)
            self.assertTrue(status["awaitingLogin"])
            self.assertIsNone(status["error"])
            self.assertIn("already belongs to Current account", status["message"])
            self.assertIn("remains empty", status["message"])
            self.assertFalse(auth_path.exists())
            self.assertEqual(json.loads((manager.root / "ppl-pro" / "auth.json").read_text(encoding="utf-8"))["tokens"]["refresh_token"], "refresh-new")
            self.assertFalse(manager._find(pending_id)["ready"])
            self.assertEqual(monitor_dashboard.dashboard_account_status(status)["message"], status["message"])
            self.assertIn('showMessage("Existing Account Update Completed"', Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8"))

    def test_duplicate_pending_login_matches_id_token_when_account_id_is_missing(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps({"tokens": {"account_id": "acct-a", "id_token": "stable-id-token", "refresh_token": "refresh-a"}}), encoding="utf-8")
            manager = AccountManager(auth_path)
            pending_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps({"tokens": {"id_token": "stable-id-token", "refresh_token": "refresh-new"}}), encoding="utf-8")

            status = manager.status()

            self.assertEqual(status["activeAccountId"], pending_id)
            self.assertTrue(status["awaitingLogin"])
            self.assertFalse(auth_path.exists())
            self.assertEqual(json.loads((manager.root / "ppl-pro" / "auth.json").read_text(encoding="utf-8"))["tokens"]["refresh_token"], "refresh-new")

    def test_pending_account_survives_restart_and_ignores_incomplete_auth(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            pending_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps({"auth_mode": "chatgpt", "tokens": {"account_id": "acct-b", "access_token": "access-b"}}), encoding="utf-8")
            reloaded = AccountManager(auth_path)
            self.assertEqual(reloaded.status()["activeAccountId"], pending_id)
            self.assertTrue(reloaded.status()["awaitingLogin"])
            self.assertIn("refresh token", reloaded.status()["error"])

    def test_account_switch_rolls_live_auth_back_when_manifest_commit_fails(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            second_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            manager.status()

            def fail_save():
                raise OSError("simulated manifest failure")

            with mock.patch.object(manager, "_save_manifest", side_effect=fail_save):
                with self.assertRaises(AccountError) as raised:
                    manager.switch("ppl-pro")
            self.assertEqual(raised.exception.status, 500)
            self.assertEqual(json.loads(auth_path.read_text(encoding="utf-8"))["tokens"]["account_id"], "acct-b")
            self.assertEqual(manager.status()["activeAccountId"], second_id)

    def test_account_switch_does_not_touch_universal_config_or_sessions(self):
        with self.account_directory() as home:
            auth_path = home / "auth.json"
            config_path = home / "config.toml"
            session_path = home / "sessions" / "sample.jsonl"
            session_path.parent.mkdir()
            config_path.write_bytes(b'model = "universal"\n')
            session_path.write_bytes(b'{"conversation":"shared"}\n')
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            second_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            manager.status()
            manager.switch("ppl-pro")
            self.assertEqual(config_path.read_bytes(), b'model = "universal"\n')
            self.assertEqual(session_path.read_bytes(), b'{"conversation":"shared"}\n')

    def test_account_rename_changes_only_manifest_label(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            original = json.dumps(self.account_auth("acct-a", "refresh-a"), separators=(",", ":")).encode()
            auth_path.write_bytes(original)
            manager = AccountManager(auth_path)
            status = manager.rename("ppl-pro", "Renamed Pro")
            self.assertEqual(status["items"][0]["label"], "Renamed Pro")
            self.assertEqual(auth_path.read_bytes(), original)
            self.assertEqual((manager.root / "ppl-pro" / "auth.json").read_bytes(), original)

    def test_bound_local_account_rename_never_calls_cloud(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            manager.manifest["cloudBindingEnabled"] = True
            manager.active_account()["cloud"] = {"state": "bound-local", "accountKey": "key-a", "boundMachineId": "machine", "etag": '"old"'}
            manager._save_manifest()

            cloud = SimpleNamespace(rename_account_state=mock.Mock(side_effect=AssertionError("cloud rename must not be called")))

            status = manager.rename("ppl-pro", "Local Name")

            self.assertEqual(status["items"][0]["label"], "Local Name")
            self.assertEqual(manager.active_account()["cloud"]["etag"], '"old"')
            cloud.rename_account_state.assert_not_called()

    def test_deleting_active_account_is_rejected_and_keeps_shared_files(self):
        with self.account_directory() as home:
            auth_path = home / "auth.json"
            config_path = home / "config.toml"
            session_path = home / "sessions" / "sample.jsonl"
            session_path.parent.mkdir()
            config_path.write_bytes(b'model = "universal"\n')
            session_path.write_bytes(b'{"conversation":"shared"}\n')
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            second_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            manager.status()
            manager.switch("ppl-pro")
            with self.assertRaises(AccountError) as raised:
                manager.delete("ppl-pro")
            self.assertEqual(raised.exception.status, 409)
            self.assertEqual(manager.status()["activeAccountId"], "ppl-pro")
            self.assertEqual(json.loads(auth_path.read_text(encoding="utf-8"))["tokens"]["account_id"], "acct-a")
            self.assertTrue((manager.root / "ppl-pro").exists())
            self.assertEqual(config_path.read_bytes(), b'model = "universal"\n')
            self.assertEqual(session_path.read_bytes(), b'{"conversation":"shared"}\n')

    def test_deleting_only_account_is_rejected(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            with self.assertRaises(AccountError) as raised:
                manager.delete("ppl-pro")
            self.assertEqual(raised.exception.status, 409)
            self.assertTrue(auth_path.exists())

    def test_releasing_only_account_is_rejected(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            cloud = SimpleNamespace(begin_account_transition=mock.Mock(), clear_account_transition=mock.Mock(), release_account=mock.Mock())

            with self.assertRaises(AccountError) as raised:
                manager.release_cloud_account(cloud, "ppl-pro")

            self.assertEqual(raised.exception.status, 409)
            self.assertTrue(auth_path.exists())
            cloud.release_account.assert_not_called()

    def test_account_delete_rolls_back_live_and_vault_when_manifest_save_fails(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            second_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            manager.status()
            manager.switch("ppl-pro")
            manager.switch(second_id)
            with mock.patch.object(manager, "_save_manifest", side_effect=OSError("simulated manifest failure")):
                with self.assertRaises(AccountError) as raised:
                    manager.delete("ppl-pro")
            self.assertEqual(raised.exception.status, 500)
            self.assertEqual(manager.status()["activeAccountId"], second_id)
            self.assertEqual(json.loads(auth_path.read_text(encoding="utf-8"))["tokens"]["account_id"], "acct-b")
            self.assertTrue((manager.root / "ppl-pro" / "auth.json").exists())

    def test_deleting_bound_local_account_never_calls_cloud(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            second_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            manager.status()
            manager.switch("ppl-pro")
            manager.manifest["cloudBindingEnabled"] = True
            manager._find("ppl-pro")["cloud"] = {"state": "bound-local", "accountKey": "key-a", "boundMachineId": "machine"}
            manager._find(second_id)["cloud"] = {"state": "bound-local", "accountKey": "key-b", "boundMachineId": "machine"}
            manager._save_manifest()
            cloud = SimpleNamespace(delete_account_payloads=mock.Mock(return_value={"deleted": True}))

            status = manager.delete(second_id)

            self.assertEqual(status["activeAccountId"], "ppl-pro")
            self.assertEqual(json.loads(auth_path.read_text(encoding="utf-8"))["tokens"]["account_id"], "acct-a")
            self.assertFalse((manager.root / second_id).exists())
            cloud.delete_account_payloads.assert_not_called()

    def test_cloud_account_release_failure_leaves_local_credentials_and_active_account_unchanged(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            second_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            manager.status()
            manager.manifest["cloudBindingEnabled"] = True
            manager._find("ppl-pro")["cloud"] = {"state": "bound-local", "accountKey": "key-a", "boundMachineId": "machine"}
            manager._save_manifest()
            before_live, before_vault, before_manifest = auth_path.read_bytes(), (manager.root / "ppl-pro" / "auth.json").read_bytes(), manager.manifest_path.read_bytes()
            cloud = SimpleNamespace(begin_account_transition=mock.Mock(), clear_account_transition=mock.Mock(), release_account=mock.Mock(side_effect=CloudError("offline", 502)))

            with self.assertRaises(AccountError) as raised:
                manager.release_cloud_account(cloud, "ppl-pro")

            self.assertEqual(raised.exception.status, 502)
            self.assertEqual(auth_path.read_bytes(), before_live)
            self.assertEqual((manager.root / "ppl-pro" / "auth.json").read_bytes(), before_vault)
            self.assertEqual(manager.manifest_path.read_bytes(), before_manifest)
            self.assertEqual(manager.status()["activeAccountId"], second_id)
            cloud.clear_account_transition.assert_called_once_with()

    def test_compact_monitor_state_preserves_account_slot_on_state_and_sample(self):
        compact = compact_monitor_state({"activeAccountSlotId": "ppl-pro", "lastSample": {"checkedAt": "2030-01-01T00:00:00Z", "activeAccountSlotId": "ppl-pro", "windows": {}, "errors": {}}})
        self.assertEqual(compact["activeAccountSlotId"], "ppl-pro")
        self.assertEqual(compact["lastSample"]["activeAccountSlotId"], "ppl-pro")

    def test_poll_sleep_seconds_counts_from_acquire_start(self):
        self.assertEqual(poll_sleep_seconds(100, 60, now=112), 48)

    def test_poll_sleep_seconds_returns_zero_after_interval_overrun(self):
        self.assertEqual(poll_sleep_seconds(100, 60, now=175), 0)

    def test_poll_sleep_seconds_keeps_minimum_interval_of_one_second(self):
        self.assertEqual(poll_sleep_seconds(100, 0, now=100.25), 0.75)

    def test_quota_extraction_finds_5h_and_7d_windows(self):
        compact = compact_quota({
            "rate_limit": {
                "primary_window": {
                    "used_percent": 42.5,
                    "limit_window_seconds": 18000,
                    "reset_at": 1893456000,
                    "planType": "plus",
                },
                "secondary_window": {
                    "used_percent": 61,
                    "limit_window_seconds": 604800,
                    "reset_at": 1893888000,
                    "planType": "pro_lite",
                },
            },
        })

        windows = quota_history_windows({"usage": compact})

        self.assertTrue(compact["complete"])
        self.assertEqual(windows["5h"]["usedPercent"], 42.5)
        self.assertEqual(windows["7d"]["usedPercent"], 61)
        self.assertEqual(windows["5h"]["resetAt"], "2030-01-01T00:00:00Z")
        self.assertEqual(windows["5h"]["plan"], "plus")
        self.assertEqual(windows["7d"]["plan"], "pro_lite")
        self.assertEqual(windows["7d"]["planMultiplier"], 5.0)

    def test_quota_extraction_uses_parent_plan_as_window_fallback(self):
        compact = compact_quota({
            "planType": "pro",
            "rate_limit": {
                "primary_window": {
                    "used_percent": 3,
                    "limit_window_seconds": 18000,
                },
                "secondary_window": {
                    "used_percent": 4,
                    "limit_window_seconds": 604800,
                },
            },
        })

        windows = quota_history_windows({"usage": compact})

        self.assertEqual(windows["5h"]["plan"], "pro")
        self.assertEqual(windows["5h"]["planMultiplier"], 20.0)
        self.assertEqual(windows["7d"]["plan"], "pro")
        self.assertEqual(windows["7d"]["planMultiplier"], 20.0)

    def test_quota_extraction_treats_missing_5h_as_unavailable_zero(self):
        compact = compact_quota({
            "rate_limit": {
                "secondary_window": {
                    "used_percent": 61,
                    "limit_window_seconds": 604800,
                    "planType": "plus",
                },
            },
        })

        windows = quota_history_windows({"usage": compact})

        self.assertTrue(compact["complete"])
        self.assertEqual(compact["missingWindows"], [])
        self.assertEqual(windows["5h"]["usedPercent"], 0)
        self.assertTrue(windows["5h"]["unavailable"])
        self.assertFalse(windows["7d"]["unavailable"])

    def test_quota_extraction_treats_missing_7d_as_unavailable_zero(self):
        compact = compact_quota({
            "rate_limit": {
                "primary_window": {
                    "used_percent": 42,
                    "limit_window_seconds": 18000,
                    "planType": "plus",
                },
            },
        })

        windows = quota_history_windows({"usage": compact})

        self.assertTrue(compact["complete"])
        self.assertEqual(compact["missingWindows"], [])
        self.assertEqual(windows["7d"]["usedPercent"], 0)
        self.assertTrue(windows["7d"]["unavailable"])
        self.assertFalse(windows["5h"]["unavailable"])

    def test_collect_usage_sample_loads_auth_for_remote_usage(self):
        original_load_json = monitor_history.load_json
        original_fetch_usage = monitor_history.fetch_usage
        try:
            monitor_history.load_json = lambda path: {"authPath": str(path)}
            monitor_history.fetch_usage = lambda auth, opener, auth_path, timeout, debug, retries=3, *args: {
                "endpoint": "test",
                "status": 200,
                "usage": compact_quota({
                    "rate_limit": {
                        "primary_window": {"used_percent": 12, "limit_window_seconds": 18000},
                        "secondary_window": {"used_percent": 34, "limit_window_seconds": 604800},
                    },
                }),
            }

            sample = collect_usage_sample(SimpleNamespace(local_only=False, no_token_scan=True, auth=Path("auth.json"), timeout=10), object(), None)

            self.assertEqual(sample["windows"]["5h"]["usedPercent"], 12)
            self.assertEqual(sample["windows"]["7d"]["usedPercent"], 34)
            self.assertEqual(sample["errors"], {})
        finally:
            monitor_history.load_json = original_load_json
            monitor_history.fetch_usage = original_fetch_usage

    def test_collect_usage_sample_arbitrates_weird_percent_and_accepts_stable_retry(self):
        original_load_json = monitor_history.load_json
        original_fetch_usage = monitor_history.fetch_usage
        calls = []
        try:
            monitor_history.load_json = lambda path: {"authPath": str(path)}

            def fake_fetch_usage(auth, opener, auth_path, timeout, debug, retries=3, *args):
                calls.append(1)
                percent = 70 if len(calls) == 1 else 71
                debug.update({"rawResponse": remote_identity("user-old") | {"sample": len(calls)}})
                return {
                    "endpoint": "test",
                    "status": 200,
                    "usage": compact_quota({
                        "rate_limit": {
                            "primary_window": {"used_percent": percent, "limit_window_seconds": 18000, "planType": "plus"},
                            "secondary_window": {"used_percent": 10, "limit_window_seconds": 604800, "planType": "plus"},
                        },
                    }),
                }

            monitor_history.fetch_usage = fake_fetch_usage

            sample = collect_usage_sample(
                SimpleNamespace(local_only=False, no_token_scan=True, auth=Path("auth.json"), timeout=10),
                object(),
                None,
                runtime_state={
                    "remoteUsageIdentity": remote_identity("user-old"),
                    "windows": {
                        "5h": {"baselinePercent": 10, "baselinePlan": "plus", "baselineMultiplier": 1.0},
                        "7d": {"baselinePercent": 10, "baselinePlan": "plus", "baselineMultiplier": 1.0},
                    },
                },
            )

            self.assertEqual(len(calls), 2)
            self.assertEqual(sample["windows"]["5h"]["usedPercent"], 71)
            self.assertEqual(sample["remoteUsage"]["rawResponse"]["sample"], 2)
            self.assertEqual(sample["remoteUsage"]["percentArbitration"]["acceptedResponse"], 2)
        finally:
            monitor_history.load_json = original_load_json
            monitor_history.fetch_usage = original_fetch_usage

    def test_collect_usage_sample_accepts_weird_percent_on_account_switch(self):
        original_load_json = monitor_history.load_json
        original_fetch_usage = monitor_history.fetch_usage
        calls = []
        try:
            monitor_history.load_json = lambda path: {"authPath": str(path)}

            def fake_fetch_usage(auth, opener, auth_path, timeout, debug, retries=3, *args):
                calls.append(1)
                debug.update({"rawResponse": remote_identity("user-new")})
                return {
                    "endpoint": "test",
                    "status": 200,
                    "usage": compact_quota({
                        "rate_limit": {
                            "primary_window": {"used_percent": 80, "limit_window_seconds": 18000, "planType": "plus"},
                            "secondary_window": {"used_percent": 10, "limit_window_seconds": 604800, "planType": "plus"},
                        },
                    }),
                }

            monitor_history.fetch_usage = fake_fetch_usage

            sample = collect_usage_sample(
                SimpleNamespace(local_only=False, no_token_scan=True, auth=Path("auth.json"), timeout=10),
                object(),
                None,
                runtime_state={
                    "remoteUsageIdentity": remote_identity("user-old"),
                    "windows": {"5h": {"baselinePercent": 10, "baselinePlan": "plus", "baselineMultiplier": 1.0}},
                },
            )

            self.assertEqual(len(calls), 1)
            self.assertEqual(sample["windows"]["5h"]["usedPercent"], 80)
            self.assertNotIn("percentArbitration", sample["remoteUsage"])
        finally:
            monitor_history.load_json = original_load_json
            monitor_history.fetch_usage = original_fetch_usage

    def test_collect_usage_sample_raises_when_percent_arbitration_never_stabilizes(self):
        original_load_json = monitor_history.load_json
        original_fetch_usage = monitor_history.fetch_usage
        calls = []
        try:
            monitor_history.load_json = lambda path: {"authPath": str(path)}

            def fake_fetch_usage(auth, opener, auth_path, timeout, debug, retries=3, *args):
                calls.append(1)
                debug.update({"rawResponse": remote_identity("user-old")})
                return {
                    "endpoint": "test",
                    "status": 200,
                    "usage": compact_quota({
                        "rate_limit": {
                            "primary_window": {"used_percent": 10 + len(calls) * 50, "limit_window_seconds": 18000, "planType": "plus"},
                            "secondary_window": {"used_percent": 10, "limit_window_seconds": 604800, "planType": "plus"},
                        },
                    }),
                }

            monitor_history.fetch_usage = fake_fetch_usage

            with self.assertRaises(UsageError):
                collect_usage_sample(
                    SimpleNamespace(local_only=False, no_token_scan=True, auth=Path("auth.json"), timeout=10),
                    object(),
                    None,
                    runtime_state={
                        "remoteUsageIdentity": remote_identity("user-old"),
                        "windows": {"5h": {"baselinePercent": 10, "baselinePlan": "plus", "baselineMultiplier": 1.0}},
                    },
                )

            self.assertEqual(len(calls), 5)
        finally:
            monitor_history.load_json = original_load_json
            monitor_history.fetch_usage = original_fetch_usage

    def test_request_json_retries_network_errors_without_limit(self):
        class SuccessfulResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read(self):
                return b'{"ok": true}'

        class FailingOpener:
            def __init__(self):
                self.count = 0

            def open(self, request, timeout):
                self.count += 1
                if self.count <= 4:
                    raise urllib.error.URLError("temporary failure")
                return SuccessfulResponse()

        opener = FailingOpener()
        original_sleep = monitor_common.time.sleep
        try:
            monitor_common.time.sleep = lambda seconds: None
            status, data = request_json(opener, "GET", "https://example.invalid", {}, timeout=1, retries=2)

            self.assertEqual(status, 200)
            self.assertEqual(data, {"ok": True})
            self.assertEqual(opener.count, 5)
        finally:
            monitor_common.time.sleep = original_sleep

    def test_token_delta_uses_previous_persisted_sample(self):
        previous = {"totals": {"inputTokens": 10, "freshInputTokens": 9, "cachedInputTokens": 1, "outputTokens": 5, "totalTokens": 15, "requests": 1}}
        current = {"totals": {"inputTokens": 25, "freshInputTokens": 20, "cachedInputTokens": 5, "outputTokens": 15, "totalTokens": 40, "requests": 3}}

        sample = make_history_sample({"checkedAt": "2030-01-01T00:00:00Z", "tokenUsage": current}, previous)

        self.assertEqual(sample["tokenDelta"]["inputTokens"], 15)
        self.assertEqual(sample["tokenDelta"]["freshInputTokens"], 11)
        self.assertEqual(sample["tokenDelta"]["cachedInputTokens"], 4)
        self.assertEqual(sample["tokenDelta"]["cacheWriteInputTokens"], 0)
        self.assertEqual(sample["tokenDelta"]["outputTokens"], 10)
        self.assertEqual(sample["tokenDelta"]["totalTokens"], 25)
        self.assertEqual(sample["tokenDelta"]["requests"], 2)

    def test_cost_uses_uncached_cached_and_output_prices(self):
        costs = calculate_token_costs({
            "byModel": {
                "gpt-5.5": {
                    "freshInputTokens": 1_000_000,
                    "cachedInputTokens": 1_000_000,
                    "outputTokens": 1_000_000,
                },
            },
        })

        self.assertEqual(costs["inputCostUsd"], 5.0)
        self.assertEqual(costs["cachedInputCostUsd"], 0.5)
        self.assertEqual(costs["outputCostUsd"], 30.0)
        self.assertEqual(costs["totalCostUsd"], 35.5)

    def test_gpt_5_6_family_uses_tier_specific_prices(self):
        expected = {
            "gpt-5.6-sol": (5.0, 0.5, 6.25, 30.0, 41.75),
            "gpt-5.6-terra": (2.5, 0.25, 3.125, 15.0, 20.875),
            "gpt-5.6-luna": (1.0, 0.1, 1.25, 6.0, 8.35),
        }
        for model, (input_cost, cached_cost, cache_write_cost, output_cost, total_cost) in expected.items():
            with self.subTest(model=model):
                costs = calculate_token_costs({"byModel": {model: {"freshInputTokens": 1_000_000, "cachedInputTokens": 1_000_000, "cacheWriteInputTokens": 1_000_000, "outputTokens": 1_000_000}}})
                self.assertEqual(costs["inputCostUsd"], input_cost)
                self.assertEqual(costs["cachedInputCostUsd"], cached_cost)
                self.assertEqual(costs["cacheWriteInputCostUsd"], cache_write_cost)
                self.assertEqual(costs["outputCostUsd"], output_cost)
                self.assertEqual(costs["totalCostUsd"], total_cost)

    def test_fast_mode_changes_cost_without_changing_raw_token_counts(self):
        cases = {
            "gpt-5.4": (2.0, 2.5),
            "gpt-5.5": (2.5, 5.0),
            "gpt-5.6-sol": (2.5, 5.0),
            "gpt-5.3-codex": (2.0, 1.75),
        }
        for model, (multiplier, input_price) in cases.items():
            with self.subTest(model=model):
                raw_tokens = {"freshInputTokens": 1_000_000, "cachedInputTokens": 0, "cacheWriteInputTokens": 0, "outputTokens": 0}
                token_usage = {"byModel": {model: raw_tokens.copy()}, "fastByModel": {model: raw_tokens.copy()}}

                costs = calculate_token_costs(token_usage)

                self.assertEqual(costs["inputCostUsd"], input_price * multiplier)
                self.assertEqual(token_usage["byModel"][model], raw_tokens)
                self.assertEqual(monitor_tokens.fast_mode_cost_multiplier(model), multiplier)

    def test_gpt_5_6_pricing_accepts_provider_and_snapshot_model_ids(self):
        self.assertEqual(pricing_for_model("openai/gpt-5.6-sol-2026-06-26"), {"input": 5.0, "cachedInput": 0.5, "cacheWriteInput": 6.25, "output": 30.0})
        self.assertEqual(pricing_for_model("gpt-5.6-terra-20260626"), {"input": 2.5, "cachedInput": 0.25, "cacheWriteInput": 3.125, "output": 15.0})
        self.assertEqual(pricing_for_model("GPT-5.6-LUNA"), {"input": 1.0, "cachedInput": 0.1, "cacheWriteInput": 1.25, "output": 6.0})

    def test_history_sample_preserves_cumulative_cost_by_normalized_model(self):
        token_usage = {
            "totals": empty_token_totals(),
            "byModel": {
                "openai/gpt-5.6-sol-2026-06-26": {"freshInputTokens": 1_000_000, "cachedInputTokens": 0, "cacheWriteInputTokens": 0, "outputTokens": 0},
                "gpt-5.5": {"freshInputTokens": 1_000_000, "cachedInputTokens": 0, "cacheWriteInputTokens": 0, "outputTokens": 0},
            },
        }

        sample = make_history_sample({"checkedAt": "2030-01-01T00:00:00Z", "tokenUsage": token_usage}, None)

        self.assertEqual(sample["costByModel"]["gpt-5.6-sol"]["totalCostUsd"], 5)
        self.assertEqual(sample["costByModel"]["gpt-5.5"]["totalCostUsd"], 5)
        self.assertEqual(sample["cost"]["totalCostUsd"], 10)

    def test_parse_token_usage_accepts_cache_write_fields(self):
        self.assertEqual(parse_token_usage({"input_tokens": 100, "cached_input_tokens": 20, "cache_write_input_tokens": 30, "output_tokens": 10}), {"input": 100, "cachedInput": 20, "cacheWriteInput": 30, "output": 10})
        self.assertEqual(parse_token_usage({"input_tokens": 100, "cache_creation_input_tokens": 40}), {"input": 100, "cachedInput": 0, "cacheWriteInput": 40, "output": 0})
        self.assertEqual(parse_token_usage({"input_tokens": 100, "input_tokens_details": {"cache_write_tokens": 50}}), {"input": 100, "cachedInput": 0, "cacheWriteInput": 50, "output": 0})
        self.assertEqual(parse_token_usage({"prompt_tokens": 100, "completion_tokens": 10, "prompt_tokens_details": {"cached_tokens": 20, "cache_write_tokens": 30}}), {"input": 100, "cachedInput": 20, "cacheWriteInput": 30, "output": 10})

    def test_codex_session_scan_records_cumulative_deltas_by_session_and_model(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            rows = [
                {"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "session-a", "session_id": "session-a"}},
                {"timestamp": "2030-01-01T00:00:01Z", "type": "turn_context", "payload": {"model": "openai/gpt-5.5-2030-01-01"}},
                {"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 100, "cached_input_tokens": 40, "cache_write_tokens": 2, "output_tokens": 10}}}},
                {"timestamp": "2030-01-01T00:02:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 150, "cached_input_tokens": 80, "cache_write_tokens": 5, "output_tokens": 20}}}},
            ]
            (session_dir / "rollout.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

            usage = monitor_tokens.scan_codex_token_usage(directory)

            self.assertEqual(len(usage["sessions"]), 1)
            session = usage["sessions"][0]
            self.assertEqual(session["sessionId"], "session-a")
            self.assertEqual(session["tokens"]["freshInputTokens"], 65)
            self.assertEqual(session["tokens"]["cachedInputTokens"], 80)
            self.assertEqual(session["tokens"]["cacheWriteInputTokens"], 5)
            self.assertEqual(session["tokens"]["outputTokens"], 20)
            self.assertEqual(session["tokens"]["requests"], 2)
            self.assertEqual(list(session["byModel"]), ["gpt-5.5"])
            self.assertEqual(session["cost"]["totalCostUsd"], 0.00099)

    def test_codex_session_scan_attributes_only_fast_turn_tokens_to_adjusted_cost(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            rows = [
                {"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "session-fast", "session_id": "session-fast"}},
                {"timestamp": "2030-01-01T00:00:01Z", "type": "turn_context", "payload": {"model": "gpt-5.5", "thread_settings": {"service_tier": "default"}}},
                {"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 100, "output_tokens": 10}}}},
                {"timestamp": "2030-01-01T00:01:01Z", "type": "turn_context", "payload": {"model": "gpt-5.5", "thread_settings": {"service_tier": "fast"}}},
                {"timestamp": "2030-01-01T00:02:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 150, "output_tokens": 20}}}},
            ]
            (session_dir / "rollout.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

            usage = monitor_tokens.scan_codex_token_usage(directory)

            self.assertEqual(usage["totals"]["inputTokens"], 150)
            self.assertEqual(usage["totals"]["outputTokens"], 20)
            self.assertEqual(usage["fastByModel"]["gpt-5.5"]["inputTokens"], 50)
            self.assertEqual(usage["fastByModel"]["gpt-5.5"]["outputTokens"], 10)
            session_model = usage["sessions"][0]["byModel"]["gpt-5.5"]
            self.assertEqual(session_model["tokens"]["totalTokens"], 170)
            self.assertEqual(session_model["fastTokens"]["totalTokens"], 60)
            self.assertEqual(session_model["cost"]["totalCostUsd"], 0.002175)

    def test_codex_session_scan_reuses_unchanged_file_without_content_reads(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            path = session_dir / "stable.jsonl"
            path.write_text("\n".join(json.dumps(row) for row in [
                {"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "stable"}},
                {"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 10, "output_tokens": 2}}}},
            ]) + "\n", encoding="utf-8")
            expected = monitor_tokens.scan_codex_token_usage(directory)

            with mock.patch.object(Path, "open", autospec=True, side_effect=Path.open) as opened:
                actual = monitor_tokens.scan_codex_token_usage(directory)

            opened.assert_not_called()
            self.assertEqual(actual, expected)

    def test_codex_session_scan_reads_only_appended_bytes(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            path = session_dir / "append.jsonl"
            path.write_text("\n".join(json.dumps(row) for row in [
                {"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "append"}},
                {"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 10, "output_tokens": 2}}}},
            ]) + "\n", encoding="utf-8")
            monitor_tokens.scan_codex_token_usage(directory)
            offset = path.stat().st_size
            appended = (json.dumps({"timestamp": "2030-01-01T00:02:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 25, "output_tokens": 5}}}}) + "\n").encode()
            with path.open("ab") as stream:
                stream.write(appended)
            positions, read_sizes = [], []
            original_open = Path.open

            class TrackedStream:
                def __init__(self, stream):
                    self.stream = stream

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    self.stream.close()

                def seek(self, position, *args):
                    positions.append(position)
                    return self.stream.seek(position, *args)

                def read(self, *args):
                    data = self.stream.read(*args)
                    read_sizes.append(len(data))
                    return data

            with mock.patch.object(Path, "open", autospec=True, side_effect=lambda opened_path, *args, **kwargs: TrackedStream(original_open(opened_path, *args, **kwargs))):
                usage = monitor_tokens.scan_codex_token_usage(directory)

            self.assertEqual(positions, [offset])
            self.assertEqual(read_sizes, [len(appended)])
            self.assertEqual(usage["totals"]["inputTokens"], 25)
            self.assertEqual(usage["totals"]["outputTokens"], 5)
            self.assertEqual(usage["totals"]["requests"], 2)

    def test_codex_session_scan_finishes_bytes_appended_during_read(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            path = session_dir / "growing.jsonl"
            path.write_text(json.dumps({"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 10, "output_tokens": 2}}}}) + "\n", encoding="utf-8")
            appended = (json.dumps({"timestamp": "2030-01-01T00:02:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 20, "output_tokens": 4}}}}) + "\n").encode()
            original_open, grew = Path.open, []

            class GrowingStream:
                def __init__(self, stream):
                    self.stream = stream

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    self.stream.close()

                def __getattr__(self, name):
                    return getattr(self.stream, name)

                def read(self, *args):
                    data = self.stream.read(*args)
                    if not grew:
                        grew.append(True)
                        with original_open(path, "ab") as writer:
                            writer.write(appended)
                    return data

            with mock.patch.object(Path, "open", autospec=True, side_effect=lambda opened_path, *args, **kwargs: GrowingStream(original_open(opened_path, *args, **kwargs))):
                usage = monitor_tokens.scan_codex_token_usage(directory)

            self.assertEqual(usage["totals"]["inputTokens"], 20)
            self.assertEqual(usage["totals"]["requests"], 2)

    def test_codex_session_scan_bounds_continuous_append_drain(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            path = session_dir / "continuously-growing.jsonl"
            def token_line(value):
                return (json.dumps({
                    "timestamp": f"2030-01-01T00:0{value // 10}:00Z", "type": "event_msg",
                    "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": value, "output_tokens": value // 5}}},
                }) + "\n").encode()
            path.write_bytes(token_line(10))
            original_open, appended_values = Path.open, []

            class GrowingStream:
                def __init__(self, stream):
                    self.stream = stream

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    self.stream.close()

                def __getattr__(self, name):
                    return getattr(self.stream, name)

                def read(self, *args):
                    data = self.stream.read(*args)
                    value = 20 + len(appended_values) * 10
                    appended_values.append(value)
                    with original_open(path, "ab") as writer:
                        writer.write(token_line(value))
                    return data

            with mock.patch.object(Path, "open", autospec=True, side_effect=lambda opened_path, *args, **kwargs: GrowingStream(original_open(opened_path, *args, **kwargs))):
                usage = monitor_tokens.scan_codex_token_usage(directory)

            self.assertEqual(appended_values, [20, 30, 40])
            self.assertEqual((usage["totals"]["inputTokens"], usage["totals"]["requests"]), (30, 3))
            caught_up = monitor_tokens.scan_codex_token_usage(directory)
            self.assertEqual((caught_up["totals"]["inputTokens"], caught_up["totals"]["requests"]), (40, 4))

    def test_codex_session_scan_waits_for_partial_line_completion(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            path = session_dir / "partial.jsonl"
            metadata = json.dumps({"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "partial"}}).encode() + b"\n"
            token_line = json.dumps({"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 13, "output_tokens": 3}}}}).encode()
            path.write_bytes(metadata + token_line[:len(token_line) // 2])

            self.assertEqual(monitor_tokens.scan_codex_token_usage(directory)["totals"]["requests"], 0)
            with path.open("ab") as stream:
                stream.write(token_line[len(token_line) // 2:] + b"\n")
            usage = monitor_tokens.scan_codex_token_usage(directory)

            self.assertEqual(usage["totals"]["inputTokens"], 13)
            self.assertEqual(usage["totals"]["requests"], 1)

    def test_codex_session_scan_preserves_valid_final_line_without_newline(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            path = session_dir / "final.jsonl"
            path.write_text(json.dumps({"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 11, "output_tokens": 2}}}}), encoding="utf-8")

            usage = monitor_tokens.scan_codex_token_usage(directory)

            self.assertEqual(usage["totals"]["inputTokens"], 11)
            self.assertEqual(usage["totals"]["requests"], 1)
            with path.open("a", encoding="utf-8") as stream:
                stream.write("\n" + json.dumps({"timestamp": "2030-01-01T00:02:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 15, "output_tokens": 3}}}}) + "\n")

            usage = monitor_tokens.scan_codex_token_usage(directory)

            self.assertEqual(usage["totals"]["inputTokens"], 15)
            self.assertEqual(usage["totals"]["requests"], 2)

    def test_codex_session_scan_does_not_drop_token_events_with_metadata_keywords(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            (session_dir / "keyword.jsonl").write_text(json.dumps({
                "timestamp": "2030-01-01T00:01:00Z", "type": "event_msg",
                "payload": {"type": "token_count", "note": "session_meta thread_settings_applied", "info": {"last_token_usage": {"input_tokens": 9, "output_tokens": 2}}},
            }) + "\n", encoding="utf-8")

            usage = monitor_tokens.scan_codex_token_usage(directory)

            self.assertEqual(usage["totals"]["inputTokens"], 9)
            self.assertEqual(usage["totals"]["requests"], 1)

    def test_codex_session_scan_rebuilds_truncated_and_replaced_files(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            path = session_dir / "changed.jsonl"
            rows = [
                {"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "original", "padding": "x" * 500}},
                {"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 100, "output_tokens": 10}}}},
            ]
            path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            monitor_tokens.scan_codex_token_usage(directory)
            truncated = [
                {"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "truncated"}},
                {"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 7, "output_tokens": 1}}}},
            ]
            path.write_text("\n".join(json.dumps(row) for row in truncated) + "\n", encoding="utf-8")

            usage = monitor_tokens.scan_codex_token_usage(directory)
            self.assertEqual((usage["sessions"][0]["sessionId"], usage["totals"]["inputTokens"]), ("truncated", 7))
            replacement = path.with_suffix(".replacement")
            replaced = [
                {"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "replaced"}},
                {"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 19, "output_tokens": 4}}}},
            ]
            replacement.write_text("\n".join(json.dumps(row) for row in replaced) + "\n", encoding="utf-8")
            os.replace(replacement, path)

            usage = monitor_tokens.scan_codex_token_usage(directory)
            self.assertEqual((usage["sessions"][0]["sessionId"], usage["totals"]["inputTokens"]), ("replaced", 19))

    def test_codex_session_scan_removes_deleted_files_from_cache_and_totals(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            path = session_dir / "deleted.jsonl"
            path.write_text(json.dumps({"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"last_token_usage": {"input_tokens": 8, "output_tokens": 2}}}}) + "\n", encoding="utf-8")
            self.assertEqual(monitor_tokens.scan_codex_token_usage(directory)["filesScanned"], 1)

            path.unlink()
            usage = monitor_tokens.scan_codex_token_usage(directory)

            self.assertEqual(usage["filesScanned"], 0)
            self.assertEqual(usage["totals"]["totalTokens"], 0)
            self.assertEqual(usage["sessions"], [])

    def test_codex_incremental_aggregation_matches_clean_rebuild(self):
        with self.account_directory() as directory:
            session_dir = directory / "sessions" / "2030" / "01" / "01"
            session_dir.mkdir(parents=True)
            path = session_dir / "equivalent.jsonl"
            initial = [
                {"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "child", "session_id": "parent"}},
                {"timestamp": "2030-01-01T00:00:01Z", "type": "turn_context", "payload": {"model": "gpt-5.5", "thread_settings": {"service_tier": "default"}}},
                {"timestamp": "2030-01-01T00:01:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 100, "output_tokens": 10}}}},
                {"timestamp": "2030-01-01T00:01:01Z", "type": "event_msg", "payload": {"type": "thread_settings_applied"}},
            ]
            path.write_text("\n".join(json.dumps(row) for row in initial) + "\n", encoding="utf-8")
            monitor_tokens.scan_codex_token_usage(directory)
            appended = [
                {"timestamp": "2030-01-01T00:01:02Z", "type": "turn_context", "payload": {"model": "gpt-5.5", "thread_settings": {"service_tier": "fast"}}},
                {"timestamp": "2030-01-01T00:02:00Z", "type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 140, "output_tokens": 20}}}},
            ]
            with path.open("a", encoding="utf-8") as stream:
                stream.write("\n".join(json.dumps(row) for row in appended) + "\n")
            incremental = monitor_tokens.scan_codex_token_usage(directory)

            monitor_tokens._CODEX_SESSION_SCAN_CACHE.pop(str(directory.resolve()))
            self.assertEqual(monitor_tokens.scan_codex_token_usage(directory), incremental)

    def test_priority_service_tier_is_treated_as_fast(self):
        self.assertEqual(monitor_tokens.normalize_service_tier("priority"), "fast")

    def test_token_session_history_preserves_original_account_attribution(self):
        with self.account_directory() as directory:
            path = directory / "token-sessions.jsonl"
            session = {"sessionId": "session-a", "startedAt": "2030-01-01T00:00:00Z", "updatedAt": "2030-01-01T00:01:00Z", "tokens": empty_token_totals(), "cost": {}, "byModel": {}}

            monitor_history.sync_token_session_history(path, [session], "account-a", "Account A")
            session["updatedAt"] = "2030-01-01T00:02:00Z"
            rows = monitor_history.sync_token_session_history(path, [session], "account-b", "Account B")

            self.assertEqual(rows[0]["accountSlotId"], "account-a")
            self.assertEqual(rows[0]["accountLabel"], "Account A")
            self.assertEqual(rows[0]["updatedAt"], "2030-01-01T00:02:00Z")

    def test_token_session_history_migrates_legacy_cached_output_to_cache_write(self):
        legacy_tokens = {"inputTokens": 100, "freshInputTokens": 80, "cachedInputTokens": 20, "outputTokens": 10, "freshOutputTokens": 0, "cachedOutputTokens": 30, "totalTokens": 110, "requests": 1}
        row = monitor_history.normalize_token_session_row({"sessionId": "legacy", "tokens": legacy_tokens, "byModel": {"gpt-5.6-sol": {"tokens": legacy_tokens, "cost": {}}}})

        self.assertEqual(row["tokens"], {"inputTokens": 100, "freshInputTokens": 50, "cachedInputTokens": 20, "cacheWriteInputTokens": 30, "outputTokens": 10, "totalTokens": 110, "requests": 1})
        self.assertEqual(row["byModel"]["gpt-5.6-sol"]["tokens"], row["tokens"])
        self.assertNotIn("cachedOutputTokens", row["tokens"])
        self.assertNotIn("freshOutputTokens", row["tokens"])
        self.assertEqual(row["cost"]["cacheWriteInputCostUsd"], 0.0001875)

    def test_cache_write_tokens_are_excluded_from_fresh_input(self):
        totals = empty_token_totals()

        add_token_delta(totals, {"input": 100, "cachedInput": 20, "cacheWriteInput": 30, "output": 10})

        self.assertEqual(totals["inputTokens"], 100)
        self.assertEqual(totals["freshInputTokens"], 50)
        self.assertEqual(totals["cachedInputTokens"], 20)
        self.assertEqual(totals["cacheWriteInputTokens"], 30)
        self.assertEqual(totals["outputTokens"], 10)

    def test_delta_event_series_records_only_percentage_increases(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12),
            event_sample("2030-01-01T01:00:00Z", five_hour=5, cost=13),
            event_sample("2030-01-01T02:00:00Z", five_hour=8, cost=15),
        ], "5h")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["cumulativePercent"], 0)
        self.assertEqual(events[0]["cumulativeCostUsd"], 0)
        self.assertEqual(events[1]["deltaPercent"], 3)
        self.assertEqual(events[1]["deltaCostUsd"], 3)
        self.assertEqual(events[1]["costPercentRatio"], 1)
        self.assertIsNone(events[1]["averageCostPercentRatio"])
        self.assertEqual(events[1]["cumulativePercent"], 3)
        self.assertEqual(events[1]["cumulativeCostUsd"], 3)

    def test_delta_event_series_does_not_record_unavailable_5h_usage(self):
        unavailable = event_sample("2030-01-01T01:00:00Z", five_hour=0, cost=15)
        unavailable["windows"]["5h"]["unavailable"] = True

        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12),
            unavailable,
            event_sample("2030-01-01T02:00:00Z", five_hour=8, cost=18),
        ], "5h")

        self.assertEqual([event["deltaPercent"] for event in events], [0, 0])
        self.assertEqual([event["deltaCostUsd"] for event in events], [0, 0])

    def test_delta_event_series_counts_flat_cost_into_next_valid_pair(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12),
            event_sample("2030-01-01T01:00:00Z", five_hour=5, cost=13),
            event_sample("2030-01-01T02:00:00Z", five_hour=5, cost=14),
            event_sample("2030-01-01T03:00:00Z", five_hour=8, cost=16),
        ], "5h")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[1]["deltaPercent"], 3)
        self.assertEqual(events[1]["deltaCostUsd"], 4)
        self.assertEqual(events[1]["cumulativePercent"], 3)
        self.assertEqual(events[1]["cumulativeCostUsd"], 4)

    def test_delta_event_series_ignores_cost_while_percentage_is_at_100(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=99, cost=100),
            event_sample("2030-01-01T01:00:00Z", five_hour=100, cost=102),
            event_sample("2030-01-01T02:00:00Z", five_hour=100, cost=108),
            event_sample("2030-01-01T03:00:00Z", five_hour=101, cost=110),
        ], "5h")

        self.assertEqual([(event["deltaPercent"], event["deltaCostUsd"]) for event in events[1:]], [(1, 2), (1, 2)])
        self.assertEqual(events[-1]["cumulativeCostUsd"], 4)

    def test_delta_event_series_resets_percent_baseline_but_keeps_cost_baseline(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12),
            event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=15),
            event_sample("2030-01-01T02:00:00Z", five_hour=10, cost=18),
            event_sample("2030-01-01T03:00:00Z", five_hour=3, cost=21),
        ], "5h")

        self.assertEqual(events[-1]["deltaPercent"], 3)
        self.assertEqual(events[-1]["deltaCostUsd"], 3)
        self.assertEqual(events[-1]["cumulativePercent"], 8)
        self.assertEqual(events[-1]["cumulativeCostUsd"], 9)

    def test_delta_event_series_normalizes_pro_lite_percentage_to_plus_plan(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=3, cost=12, plan="pro_lite"),
            event_sample("2030-01-01T01:00:00Z", five_hour=4, cost=15, plan="pro_lite"),
            event_sample("2030-01-01T02:00:00Z", five_hour=5, cost=18, plan="pro_lite"),
        ], "5h")

        self.assertEqual(events[0]["normalizedPercent"], 15)
        self.assertEqual(events[1]["deltaPercent"], 5)
        self.assertEqual(events[1]["deltaCostUsd"], 3)
        self.assertEqual(events[1]["cumulativePercent"], 5)
        self.assertEqual(events[1]["cumulativeCostUsd"], 3)

    def test_delta_event_series_discards_account_type_switch_sample(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=8, plan="plus"),
            event_sample("2030-01-01T01:00:00Z", five_hour=30, cost=12, plan="pro_lite"),
            event_sample("2030-01-01T02:00:00Z", five_hour=31, cost=15, plan="pro_lite"),
            event_sample("2030-01-01T03:00:00Z", five_hour=34, cost=18, plan="pro_lite"),
        ], "5h")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["cumulativePercent"], 0)
        self.assertEqual(events[0]["cumulativeCostUsd"], 0)
        self.assertEqual(events[1]["deltaPercent"], 15)
        self.assertEqual(events[1]["deltaCostUsd"], 3)
        self.assertEqual(events[1]["cumulativePercent"], 15)
        self.assertEqual(events[1]["cumulativeCostUsd"], 3)

    def test_delta_event_series_does_not_count_account_switch_gap_cost(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=8, plan="plus"),
            event_sample("2030-01-01T01:00:00Z", five_hour=30, cost=12, plan="pro_lite"),
            event_sample("2030-01-01T02:00:00Z", five_hour=30, cost=14, plan="pro_lite"),
            event_sample("2030-01-01T03:00:00Z", five_hour=31, cost=17, plan="pro_lite"),
            event_sample("2030-01-01T04:00:00Z", five_hour=34, cost=20, plan="pro_lite"),
        ], "5h")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[1]["deltaPercent"], 15)
        self.assertEqual(events[1]["deltaCostUsd"], 3)
        self.assertEqual(events[1]["cumulativePercent"], 15)
        self.assertEqual(events[1]["cumulativeCostUsd"], 3)

    def test_delta_event_series_discards_low_cost_external_usage(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=10),
            event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=10),
            event_sample("2030-01-01T02:00:00Z", five_hour=9, cost=11),
        ], "5h")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[1]["deltaPercent"], 1)
        self.assertEqual(events[1]["deltaCostUsd"], 1)

    def test_new_valid_delta_events_returns_only_new_pairs(self):
        before = [
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12),
            event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=15),
        ]
        after = [
            *before,
            event_sample("2030-01-01T02:00:00Z", five_hour=9, cost=18),
        ]

        events = new_valid_delta_events(before, after)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 3)

    def test_delta_event_console_format_includes_current_normalized_usage_and_plan(self):
        sample_row = event_sample("2030-01-01T02:00:00Z", five_hour=5, seven_day=2, cost=18, plan="pro_lite")
        event = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=3, cost=12, plan="pro_lite"),
            event_sample("2030-01-01T01:00:00Z", five_hour=4, cost=15, plan="pro_lite"), sample_row,
        ], "5h")[-1]

        text = format_valid_delta_event(event, sample_row)

        self.assertIn("2030-01-01T02:00:00Z (local " + datetime.fromisoformat("2030-01-01T02:00:00+00:00").astimezone().isoformat(timespec="seconds") + ")", text)
        self.assertIn("+5% / +$3", text)
        self.assertIn("ratio $0.6/%", text)
        self.assertIn("current 5h 25%", text)
        self.assertIn("7d 10%", text)
        self.assertIn("pro_lite 5x", text)

    def test_live_processing_records_only_compact_delta_pair(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12), history), [])

        events = process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=15), history)
        history.extend(compact_delta_event(event) for event in events)

        self.assertEqual(history, [{"checkedAt": "2030-01-01T01:00:00Z", "window": "5h", "model": "gpt-5.5", "accountSlotId": "unknown", "accountLabel": "Unknown", "deltaPercent": 3.0, "deltaCostUsd": 3.0, "costPercentRatio": 1.0}])
        self.assertNotIn("windows", history[0])
        self.assertEqual(derive_history_events(history)["fiveHour"][0]["cumulativePercent"], 0)
        self.assertTrue(derive_history_events(history)["fiveHour"][0]["synthetic"])
        self.assertEqual(derive_history_events(history)["fiveHour"][1]["cumulativePercent"], 3)

    def test_live_processing_records_registered_account_on_every_model_split(self):
        state = {}
        baseline = event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12)
        baseline.update({"accountSlotId": "work-account", "accountLabel": "Work Pro"})
        process_sample_delta_events(state, baseline, [])
        sample = event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=15)
        sample.update({"accountSlotId": "work-account", "accountLabel": "Work Pro"})
        events = process_sample_delta_events(state, sample, [])
        self.assertTrue(events)
        self.assertEqual({event["accountSlotId"] for event in events}, {"work-account"})
        self.assertEqual({event["accountLabel"] for event in events}, {"Work Pro"})
        self.assertEqual({compact_delta_event(event)["accountSlotId"] for event in events}, {"work-account"})

    def test_live_processing_flags_cost_percent_ratio_deviation_for_both_windows(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, seven_day=5, cost=10), history)
        events = process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=8, seven_day=8, cost=13), history)
        history.extend(compact_delta_event(event) for event in events)

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=9, seven_day=9, cost=16), history)

        self.assertEqual([event["window"] for event in events], ["5h", "7d"])
        self.assertEqual([event["costPercentRatio"] for event in events], [3.0, 3.0])
        self.assertEqual([event["averageCostPercentRatio"] for event in events], [1.0, 1.0])
        self.assertTrue(all(event["ratioDeviationWarning"] for event in events))
        self.assertIn("cost/percent ratio $3/% deviates from average $1/%", format_ratio_warning(events[0]))

    def test_live_processing_splits_percent_by_model_cost_for_both_windows(self):
        state = {}
        history = []
        first = event_sample("2030-01-01T00:00:00Z", five_hour=5, seven_day=5, cost=0)
        first["costByModel"] = {"gpt-5.5": {"totalCostUsd": 0}, "gpt-5.6-sol": {"totalCostUsd": 0}}
        second = event_sample("2030-01-01T01:00:00Z", five_hour=7, seven_day=7, cost=7)
        second["costByModel"] = {"gpt-5.5": {"totalCostUsd": 2}, "gpt-5.6-sol": {"totalCostUsd": 5}}

        self.assertEqual(process_sample_delta_events(state, first, history), [])
        events = process_sample_delta_events(state, second, history)

        self.assertEqual([(event["window"], event["model"]) for event in events], [("5h", "gpt-5.5"), ("5h", "gpt-5.6-sol"), ("7d", "gpt-5.5"), ("7d", "gpt-5.6-sol")])
        for label in ("5h", "7d"):
            split = [event for event in events if event["window"] == label]
            self.assertEqual(split[0]["deltaPercent"], 0.57142857)
            self.assertEqual(split[1]["deltaPercent"], 1.42857143)
            self.assertEqual(sum(event["deltaPercent"] for event in split), 2)
            self.assertEqual([event["deltaCostUsd"] for event in split], [2, 5])
            self.assertEqual([event["costPercentRatio"] for event in split], [3.5, 3.5])

    def test_model_ratio_average_is_calculated_separately(self):
        state = {}
        first = event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=0)
        first["costByModel"] = {"gpt-5.5": {"totalCostUsd": 0}, "gpt-5.6-sol": {"totalCostUsd": 0}}
        second = event_sample("2030-01-01T01:00:00Z", five_hour=7, cost=7)
        second["costByModel"] = {"gpt-5.5": {"totalCostUsd": 2}, "gpt-5.6-sol": {"totalCostUsd": 5}}
        process_sample_delta_events(state, first, [])
        history = [compact_delta_event(event) for event in process_sample_delta_events(state, second, [])]
        third = event_sample("2030-01-01T02:00:00Z", five_hour=8, cost=9)
        third["costByModel"] = {"gpt-5.5": {"totalCostUsd": 4}, "gpt-5.6-sol": {"totalCostUsd": 5}}

        events = process_sample_delta_events(state, third, history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["model"], "gpt-5.5")
        self.assertEqual(events[0]["costPercentRatio"], 2)
        self.assertAlmostEqual(events[0]["averageCostPercentRatio"], 3.5, places=7)

    def test_empty_compact_event_log_shows_zero_zero_baseline(self):
        events = derive_history_events([])

        self.assertEqual(events["fiveHour"][0]["cumulativePercent"], 0)
        self.assertEqual(events["fiveHour"][0]["cumulativeCostUsd"], 0)
        self.assertTrue(events["fiveHour"][0]["synthetic"])
        self.assertEqual(events["sevenDay"][0]["cumulativePercent"], 0)
        self.assertEqual(events["sevenDay"][0]["cumulativeCostUsd"], 0)
        self.assertTrue(events["sevenDay"][0]["synthetic"])

    def test_live_processing_discards_low_cost_delta_and_resets_baseline(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=10), history)
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=10), history), [])

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=9, cost=11), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 1)

    def test_precision_sensitive_windows_wait_for_next_percent_change_on_startup(self):
        for label, plan in (("7d", "plus"), ("5h", "pro_lite"), ("7d", "pro_lite"), ("5h", "pro"), ("7d", "pro")):
            with self.subTest(label=label, plan=plan):
                state = {}
                first = {"five_hour": 50} if label == "5h" else {"seven_day": 50}
                second = {"five_hour": 51} if label == "5h" else {"seven_day": 51}
                third = {"five_hour": 52} if label == "5h" else {"seven_day": 52}

                self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", cost=100, plan=plan, **first), []), [])
                self.assertTrue(state["windows"][label]["awaitingTrustedPercentBaseline"])
                self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", cost=110, plan=plan, **second), []), [])
                self.assertNotIn("awaitingTrustedPercentBaseline", state["windows"][label])
                self.assertEqual(state["windows"][label]["baselineCostUsd"], 110)

                events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", cost=112, plan=plan, **third), [])

                self.assertEqual(len(events), 1)
                self.assertEqual(events[0]["deltaPercent"], {"plus": 1, "pro_lite": 5, "pro": 20}[plan])
                self.assertEqual(events[0]["deltaCostUsd"], 2)

    def test_plus_five_hour_keeps_immediate_integer_baseline(self):
        state = {}
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=50, cost=100, plan="plus"), [])

        events = process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=51, cost=102, plan="plus"), [])

        self.assertNotIn("awaitingTrustedPercentBaseline", state["windows"]["5h"])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 2)

    def test_account_switch_waits_for_next_seven_day_percent_change(self):
        state = {}
        process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T00:00:00Z", seven_day=40, cost=100, plan="plus"), "user-old"), [])
        process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T01:00:00Z", seven_day=50, cost=105, plan="plus"), "user-new"), [])

        self.assertTrue(state["windows"]["7d"]["awaitingTrustedPercentBaseline"])
        self.assertEqual(process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T02:00:00Z", seven_day=51, cost=115, plan="plus"), "user-new"), []), [])
        events = process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T03:00:00Z", seven_day=52, cost=118, plan="plus"), "user-new"), [])

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 3)

    def test_external_update_restarts_trusted_seven_day_baseline_wait(self):
        state = {}
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", seven_day=50, cost=100, plan="plus"), [])
        process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", seven_day=51, cost=105, plan="plus"), [])
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", seven_day=52, cost=105, plan="plus"), []), [])

        self.assertEqual(state["_specialEvents"][0]["reason"], "low-cost-delta-discarded")
        self.assertTrue(state["windows"]["7d"]["awaitingTrustedPercentBaseline"])
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T03:00:00Z", seven_day=53, cost=110, plan="plus"), []), [])
        events = process_sample_delta_events(state, event_sample("2030-01-01T04:00:00Z", seven_day=54, cost=112, plan="plus"), [])

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 2)

    def test_live_processing_records_only_after_runtime_cost_baseline_is_ready(self):
        state = {}
        history = []
        first = event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=100)
        first["eventCostUsd"] = 0
        first["eventCostReady"] = False
        second = event_sample("2030-01-01T01:00:00Z", five_hour=6, cost=101)
        second["eventCostUsd"] = 1
        second["eventCostReady"] = False
        third = event_sample("2030-01-01T02:00:00Z", five_hour=7, cost=103)
        third["eventCostUsd"] = 3
        third["eventCostReady"] = True

        self.assertEqual(process_sample_delta_events(state, first, history), [])
        self.assertEqual(process_sample_delta_events(state, second, history), [])
        events = process_sample_delta_events(state, third, history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 2)

    def test_live_processing_keeps_independent_window_cost_baselines(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=10, seven_day=10, cost=100), history)
        events = process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=10, seven_day=11, cost=112), history)
        history.extend(compact_delta_event(event) for event in events)

        self.assertEqual([(event["window"], event["deltaCostUsd"]) for event in events], [("7d", 12)])
        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=11, seven_day=11, cost=115), history)

        self.assertEqual([(event["window"], event["deltaCostUsd"]) for event in events], [("5h", 15)])

    def test_live_processing_ignores_100_percent_overflow_per_window(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=99, seven_day=99, cost=100), history)
        events = process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=100, seven_day=99, cost=102), history)
        history.extend(compact_delta_event(event) for event in events)

        self.assertEqual([(event["window"], event["deltaPercent"], event["deltaCostUsd"]) for event in events], [("5h", 1, 2)])
        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=100, seven_day=100, cost=105), history)
        history.extend(compact_delta_event(event) for event in events)

        self.assertEqual([(event["window"], event["deltaPercent"], event["deltaCostUsd"]) for event in events], [("7d", 1, 5)])
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 105)
        events = process_sample_delta_events(state, event_sample("2030-01-01T03:00:00Z", five_hour=101, seven_day=100, cost=108), history)
        history.extend(compact_delta_event(event) for event in events)

        self.assertEqual([(event["window"], event["deltaPercent"], event["deltaCostUsd"]) for event in events], [("5h", 1, 3)])
        self.assertEqual(state["windows"]["7d"]["baselineCostUsd"], 108)
        events = process_sample_delta_events(state, event_sample("2030-01-01T04:00:00Z", five_hour=102, seven_day=101, cost=110), history)

        self.assertEqual([(event["window"], event["deltaPercent"], event["deltaCostUsd"]) for event in events], [("5h", 1, 2), ("7d", 1, 2)])

    def test_live_processing_recovers_from_transient_future_reset_rollback(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=24, cost=100, five_hour_reset="2030-01-01T06:00:00Z"), history), [])

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=6, cost=102, five_hour_reset="2030-01-01T08:00:00Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "bad-remote-usage-discarded")

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=37, cost=103, five_hour_reset="2030-01-01T06:00:00Z"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 13)
        self.assertEqual(events[0]["deltaCostUsd"], 3)

    def test_live_processing_recovers_from_transient_reset_rollback_with_jitter(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", seven_day=65, cost=100, seven_day_reset="2030-01-14T05:32:00Z"), history), [])
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", seven_day=3, cost=101, seven_day_reset="2030-01-15T03:18:06Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "bad-remote-usage-discarded")

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", seven_day=66, cost=101, seven_day_reset="2030-01-14T05:31:59Z"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 1)

    def test_live_processing_discards_stale_backward_reset_sample(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=6, cost=100, five_hour_reset="2030-01-01T11:00:00Z"), history), [])

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=16, cost=100.4, five_hour_reset="2030-01-01T08:00:00Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "reset-time-moved-backward-discarded")

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=8, cost=100.5, five_hour_reset="2030-01-01T11:00:00Z"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 2)
        self.assertEqual(events[0]["deltaCostUsd"], 0.5)

    def test_live_processing_rebases_after_consistent_backward_reset_samples(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", seven_day=3, cost=100, seven_day_reset="2030-01-15T03:18:06Z"), history), [])

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", seven_day=73, cost=107, seven_day_reset="2030-01-14T05:32:00Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "reset-time-moved-backward-discarded")

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", seven_day=73, cost=107, seven_day_reset="2030-01-14T05:31:59Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "consistent-backward-reset-rebased")
        self.assertEqual(state["windows"]["7d"]["baselinePercent"], 73)
        self.assertEqual(state["windows"]["7d"]["baselineCostUsd"], 107)
        self.assertEqual(state["windows"]["7d"]["baselineResetAt"], "2030-01-14T05:31:59Z")

        events = process_sample_delta_events(state, event_sample("2030-01-01T03:00:00Z", seven_day=74, cost=108, seven_day_reset="2030-01-14T05:32:00Z"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 1)

    def test_live_processing_requires_consecutive_backward_reset_samples_to_rebase(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", seven_day=3, cost=100, seven_day_reset="2030-01-15T03:18:06Z"), history), [])
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", seven_day=73, cost=107, seven_day_reset="2030-01-14T05:32:00Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "reset-time-moved-backward-discarded")

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", seven_day=3, cost=107, seven_day_reset="2030-01-15T03:18:06Z"), history), [])
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T03:00:00Z", seven_day=73, cost=108, seven_day_reset="2030-01-14T05:31:59Z"), history), [])

        self.assertEqual(state["_specialEvents"][0]["reason"], "reset-time-moved-backward-discarded")
        self.assertEqual(state["windows"]["7d"]["baselinePercent"], 3)
        self.assertEqual(state["windows"]["7d"]["baselineResetAt"], "2030-01-15T03:18:06Z")

    def test_bad_remote_usage_does_not_consume_cost_before_next_good_sample(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, five_hour_reset="2030-01-01T10:00:00Z"), history)

        bad = event_sample("2030-01-01T01:00:00Z", five_hour=10, cost=105, five_hour_reset="2030-01-01T12:00:00Z")
        self.assertEqual(process_sample_delta_events(state, bad, history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "bad-remote-usage-discarded")
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 40)
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 100)
        self.assertEqual(state["windows"]["5h"]["previousCostUsd"], 100)
        self.assertEqual(bad["windows"]["5h"]["usedPercent"], 40)
        self.assertEqual(bad["windows"]["5h"]["resetAt"], "2030-01-01T10:00:00Z")
        self.assertTrue(bad["usingPreviousWindows"])
        self.assertIn("remoteUsageRejected", bad["errors"])
        self.assertEqual(dashboard_display(bad)["statusBarText"], "5h 40.0% · 7d -")
        self.assertEqual(dashboard_display(bad)["windows"]["5h"]["resetAt"], "2030-01-01T10:00:00Z")

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=42, cost=110, five_hour_reset="2030-01-01T10:00:00Z"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 2)
        self.assertEqual(events[0]["deltaCostUsd"], 10)

    def test_bad_remote_usage_is_retried_immediately_before_processing(self):
        state = {"remoteUsageIdentity": remote_identity("user-old")}
        samples = [
            with_remote_identity(sample("2026-01-01T00:00:00Z"), "user-new"),
            with_remote_identity(sample("2026-01-01T00:00:01Z", five_hour=10, five_hour_reset="2026-01-01T05:00:00Z"), "user-old"),
        ]

        recovered = collect_with_bad_remote_usage_retry(lambda: samples.pop(0), state)

        self.assertEqual(recovered["checkedAt"], "2026-01-01T00:00:01Z")
        self.assertTrue(recovered["remoteUsage"]["badRemoteUsageRetry"]["attempted"])
        self.assertIn("identity changed without usable quota windows", recovered["remoteUsage"]["badRemoteUsageRetry"]["initialRejectionReasons"])
        self.assertEqual(samples, [])

    def test_bad_remote_usage_confirms_plus_manual_reset_with_window_specific_limits(self):
        cases = {
            "5h": ([10, 15, 20], "2030-01-01T12:00:00Z", 20),
            "7d": ([10, 11, 12], "2030-01-15T00:00:00Z", 12),
        }
        for label, (percents, reset_at, expected) in cases.items():
            with self.subTest(window=label):
                state = {}
                initial = with_remote_identity(event_sample(
                    "2030-01-01T00:00:00Z", five_hour=40 if label == "5h" else None, seven_day=40 if label == "7d" else None, cost=100, plan="plus",
                    five_hour_reset="2030-01-01T10:00:00Z" if label == "5h" else None, seven_day_reset="2030-01-14T00:00:00Z" if label == "7d" else None,
                ), "user-old")
                process_sample_delta_events(state, initial, [])
                samples = [
                    with_remote_identity(event_sample(
                        f"2030-01-01T01:00:0{index}Z", five_hour=percent if label == "5h" else None, seven_day=percent if label == "7d" else None,
                        cost=105 + index, plan="plus", five_hour_reset=reset_at if label == "5h" else None, seven_day_reset=reset_at if label == "7d" else None,
                    ), "user-old")
                    for index, percent in enumerate(percents)
                ]

                confirmed = collect_with_bad_remote_usage_retry(lambda: samples.pop(0), state)

                self.assertTrue(confirmed["remoteUsage"]["manualResetConfirmation"]["confirmed"])
                self.assertEqual(confirmed["remoteUsage"]["manualResetConfirmation"]["confirmedWindows"], [label])
                self.assertEqual(process_sample_delta_events(state, confirmed, []), [])
                self.assertEqual(state["_specialEvents"][0]["reason"], "manual-reset-confirmed")
                self.assertEqual(state["windows"][label]["baselinePercent"], expected)
                self.assertEqual(state["windows"][label]["baselineCostUsd"], 107)
                self.assertEqual(samples, [])

    def test_zero_or_missing_windows_accept_manual_reset_without_retry(self):
        cases = {
            "both-zero": (0, 0, {"5h", "7d"}),
            "zero-and-missing": (0, None, {"5h"}),
        }
        for name, (five_hour, seven_day, confirmed_windows) in cases.items():
            with self.subTest(case=name):
                state = {}
                process_sample_delta_events(state, with_remote_identity(event_sample(
                    "2030-01-01T00:00:00Z", five_hour=40, seven_day=40, cost=100, plan="plus",
                    five_hour_reset="2030-01-01T10:00:00Z", seven_day_reset="2030-01-14T00:00:00Z",
                ), "user-old"), [])
                calls = []

                def collect():
                    calls.append(1)
                    return with_remote_identity(event_sample(
                        "2030-01-01T01:00:00Z", five_hour=five_hour, seven_day=seven_day, cost=105, plan="plus",
                        five_hour_reset="2030-01-01T12:00:00Z", seven_day_reset="2030-01-15T00:00:00Z" if seven_day is not None else None,
                    ), "user-old")

                confirmed = collect_with_bad_remote_usage_retry(collect, state)

                self.assertEqual(calls, [1])
                self.assertTrue(confirmed["remoteUsage"]["manualResetConfirmation"]["directZeroOrMissing"])
                self.assertEqual(set(confirmed["remoteUsage"]["manualResetConfirmation"]["confirmedWindows"]), confirmed_windows)
                self.assertEqual(process_sample_delta_events(state, confirmed, []), [])
                for label in confirmed_windows:
                    self.assertEqual(state["windows"][label]["baselinePercent"], 0)
                    self.assertEqual(state["windows"][label]["baselineCostUsd"], 105)

    def test_both_missing_windows_are_not_accepted_as_manual_reset(self):
        state = {}
        process_sample_delta_events(state, with_remote_identity(event_sample(
            "2030-01-01T00:00:00Z", five_hour=40, seven_day=40, cost=100, plan="plus",
            five_hour_reset="2030-01-01T10:00:00Z", seven_day_reset="2030-01-14T00:00:00Z",
        ), "user-old"), [])
        samples = [with_remote_identity(event_sample(f"2030-01-01T01:00:0{index}Z", cost=105 + index, plan="plus"), "user-old") for index in range(2)]

        rejected = collect_with_bad_remote_usage_retry(lambda: samples.pop(0), state)

        self.assertNotIn("manualResetConfirmation", rejected["remoteUsage"])
        self.assertEqual(process_sample_delta_events(state, rejected, []), [])
        self.assertIn("both quota window percentages are missing", rejected["errors"]["remoteUsageRejected"])
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 40)
        self.assertEqual(state["windows"]["7d"]["baselinePercent"], 40)
        self.assertEqual(samples, [])

    def test_bad_remote_usage_confirms_pro_manual_reset_at_one_percent_per_response(self):
        for plan, multiplier in (("pro_lite", 5), ("pro", 20)):
            with self.subTest(plan=plan):
                state = {}
                process_sample_delta_events(state, with_remote_identity(event_sample(
                    "2030-01-01T00:00:00Z", five_hour=40, seven_day=40, cost=100, plan=plan,
                    five_hour_reset="2030-01-01T10:00:00Z", seven_day_reset="2030-01-14T00:00:00Z",
                ), "user-old"), [])
                samples = [
                    with_remote_identity(event_sample(
                        f"2030-01-01T01:00:0{index}Z", five_hour=percent, seven_day=percent, cost=105 + index, plan=plan,
                        five_hour_reset="2030-01-01T12:00:00Z", seven_day_reset="2030-01-15T00:00:00Z",
                    ), "user-old")
                    for index, percent in enumerate((10, 11, 12))
                ]

                confirmed = collect_with_bad_remote_usage_retry(lambda: samples.pop(0), state)

                self.assertEqual(set(confirmed["remoteUsage"]["manualResetConfirmation"]["confirmedWindows"]), {"5h", "7d"})
                self.assertEqual(process_sample_delta_events(state, confirmed, []), [])
                self.assertEqual(state["windows"]["5h"]["baselinePercent"], 12 * multiplier)
                self.assertEqual(state["windows"]["7d"]["baselinePercent"], 12 * multiplier)
                self.assertEqual([event["reason"] for event in state["_specialEvents"]], ["manual-reset-confirmed", "manual-reset-confirmed"])

    def test_bad_remote_usage_does_not_confirm_unstable_manual_reset(self):
        state = {}
        process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-old"), [])
        samples = [
            with_remote_identity(event_sample(f"2030-01-01T01:00:0{index}Z", five_hour=percent, cost=105 + index, plan="plus", five_hour_reset="2030-01-01T12:00:00Z"), "user-old")
            for index, percent in enumerate((10, 16, 22, 28, 34))
        ]

        rejected = collect_with_bad_remote_usage_retry(lambda: samples.pop(0), state)

        self.assertFalse(rejected["remoteUsage"]["manualResetConfirmation"]["confirmed"])
        self.assertEqual(process_sample_delta_events(state, rejected, []), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "bad-remote-usage-discarded")
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 40)
        self.assertEqual(samples, [])

    def test_repeated_bad_remote_usage_suppresses_duplicate_console_event(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, five_hour_reset="2030-01-01T10:00:00Z"), history)

        process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=10, cost=105, five_hour_reset="2030-01-01T12:00:00Z"), history)
        self.assertFalse(state["_specialEvents"][0]["extra"]["suppressConsole"])

        process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=10, cost=106, five_hour_reset="2030-01-01T12:00:00Z"), history)
        self.assertTrue(state["_specialEvents"][0]["extra"]["suppressConsole"])

    def test_consecutive_reset_time_moved_forward_events_are_suppressed_per_window(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=10, seven_day=10, cost=100, five_hour_reset="2030-01-01T10:00:00Z", seven_day_reset="2030-01-08T00:00:00Z"), history)

        process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=10, seven_day=10, cost=100, five_hour_reset="2030-01-01T11:00:00Z", seven_day_reset="2030-01-08T01:00:00Z"), history)
        self.assertEqual([event["reason"] for event in state["_specialEvents"]], ["reset-time-moved-forward", "reset-time-moved-forward"])
        self.assertEqual([event["extra"]["suppressConsole"] for event in state["_specialEvents"]], [False, False])

        process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=10, seven_day=10, cost=100, five_hour_reset="2030-01-01T12:00:00Z", seven_day_reset="2030-01-08T02:00:00Z"), history)
        self.assertEqual([event["extra"]["suppressConsole"] for event in state["_specialEvents"]], [True, True])

        process_sample_delta_events(state, event_sample("2030-01-01T03:00:00Z", five_hour=11, seven_day=10, cost=100, five_hour_reset="2030-01-01T12:00:00Z", seven_day_reset="2030-01-08T02:00:00Z"), history)
        self.assertEqual(state["_specialEvents"][0]["reason"], "low-cost-delta-discarded")

        process_sample_delta_events(state, event_sample("2030-01-01T04:00:00Z", five_hour=11, seven_day=10, cost=100, five_hour_reset="2030-01-01T13:00:00Z", seven_day_reset="2030-01-08T03:00:00Z"), history)
        self.assertEqual([event["extra"]["suppressConsole"] for event in state["_specialEvents"]], [False, True])

    def test_live_processing_rebases_on_remote_identity_switch_with_usable_windows(self):
        state = {}
        history = []
        process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-old"), history)

        switched = with_remote_identity(event_sample("2030-01-01T01:00:00Z", five_hour=10, cost=105, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-new")
        self.assertEqual(process_sample_delta_events(state, switched, history), [])

        self.assertTrue(switched["remoteUsage"]["accepted"])
        self.assertEqual(state["remoteUsageIdentity"]["user_id"], "user-new")
        self.assertEqual(state["_specialEvents"][0]["reason"], "account-switch")
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 10)
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 105)

        events = process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T02:00:00Z", five_hour=12, cost=107, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-new"), history)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 2)
        self.assertEqual(events[0]["deltaCostUsd"], 2)
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 12)
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 107)

        events = process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T03:00:00Z", five_hour=13, cost=109, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-new"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 2)

    def test_remote_identity_switch_with_rollback_shape_is_real_account_switch(self):
        state = {}
        history = []
        process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-old"), history)

        switched = with_remote_identity(event_sample("2030-01-01T01:00:00Z", five_hour=1, cost=105, plan="plus", five_hour_reset="2030-01-01T12:00:00Z"), "user-new")
        self.assertEqual(process_sample_delta_events(state, switched, history), [])

        self.assertTrue(switched["remoteUsage"]["accepted"])
        self.assertNotIn("remoteUsageRejected", switched["errors"])
        self.assertEqual(state["_specialEvents"][0]["reason"], "account-switch")
        self.assertEqual(state["remoteUsageIdentity"]["user_id"], "user-new")
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 1)
        self.assertEqual(state["windows"]["5h"]["baselineResetAt"], "2030-01-01T12:00:00Z")
        self.assertNotIn("awaitingTrustedPercentBaseline", state["windows"]["5h"])

    def test_auth_identity_switch_with_rollback_shape_is_real_account_switch(self):
        state = {}
        history = []
        process_sample_delta_events(state, with_auth_identity(event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "acct-old"), history)

        switched = with_auth_identity(event_sample("2030-01-01T01:00:00Z", five_hour=1, cost=105, plan="plus", five_hour_reset="2030-01-01T12:00:00Z"), "acct-new")
        self.assertEqual(process_sample_delta_events(state, switched, history), [])

        self.assertTrue(switched["remoteUsage"]["accepted"])
        self.assertNotIn("remoteUsageRejected", switched["errors"])
        self.assertEqual(state["_specialEvents"][0]["reason"], "account-switch")
        self.assertEqual(state["remoteUsageIdentity"]["account_id"], "acct-new")
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 1)
        self.assertNotIn("awaitingTrustedPercentBaseline", state["windows"]["5h"])

    def test_account_switch_counts_next_percent_from_switch_baseline(self):
        state = {}
        history = []
        process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, plan="plus"), "user-old"), history)
        self.assertEqual(process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T01:00:00Z", five_hour=4, cost=110, plan="plus"), "user-new"), history), [])

        events = process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T02:00:00Z", five_hour=5, cost=120, plan="plus"), "user-new"), history)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 10)
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 5)
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 120)
        self.assertNotIn("awaitingTrustedPercentBaseline", state["windows"]["5h"])

        events = process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T03:00:00Z", five_hour=6, cost=123, plan="plus"), "user-new"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 3)

    def test_remote_identity_switch_without_quota_windows_is_discarded(self):
        state = {"remoteUsageIdentity": remote_identity("user-old")}
        sample_row = with_remote_identity(event_sample("2030-01-01T00:00:00Z", cost=100), "user-new")

        self.assertEqual(process_sample_delta_events(state, sample_row, []), [])

        self.assertFalse(sample_row["remoteUsage"]["accepted"])
        self.assertEqual(state["_specialEvents"][0]["reason"], "bad-remote-usage-discarded")
        self.assertIn("identity changed without usable quota windows", sample_row["errors"]["remoteUsageRejected"])
        self.assertEqual(state["remoteUsageIdentity"]["user_id"], "user-old")

    def test_live_processing_reports_account_switch_baseline_update(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=8, plan="plus"), history)

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=30, cost=12, plan="pro_lite"), history), [])

        self.assertEqual(len(state["_specialEvents"]), 1)
        self.assertEqual(state["_specialEvents"][0]["reason"], "account-switch")
        text = format_special_event(state["_specialEvents"][0])
        self.assertIn("account-switch", text)
        self.assertIn("2030-01-01T01:00:00Z (local " + datetime.fromisoformat("2030-01-01T01:00:00+00:00").astimezone().isoformat(timespec="seconds") + ")", text)
        self.assertIn("plan plus (1x) -> pro_lite (5x)", text)
        self.assertIn("baseline 5% -> 150%", text)

    def test_live_processing_reports_low_cost_delta_baseline_update(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=10), history)

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=10), history), [])

        self.assertEqual(len(state["_specialEvents"]), 1)
        self.assertEqual(state["_specialEvents"][0]["reason"], "low-cost-delta-discarded")
        text = format_special_event(state["_specialEvents"][0])
        self.assertIn("low-cost-delta-discarded", text)
        self.assertIn("baseline 5% -> 8%", text)
        self.assertIn("discarded delta +3% / +$0", text)

    def test_startup_reset_clears_percent_and_cost_baselines(self):
        state = reset_runtime_baselines(compact_monitor_state({
            "windows": {
                "5h": {
                    "cumulativePercent": 10.0,
                    "cumulativeCostUsd": 5.0,
                    "baselinePercent": 10.0,
                    "baselineCostUsd": 5.0,
                    "baselinePlan": "unknown",
                    "baselineMultiplier": 1.0,
                    "previousCostUsd": 5.0,
                    "awaitingTrustedPercentBaseline": True,
                },
            },
            "runCostUsd": 5.0,
            "measuredCostIntervals": 3,
            "hasRuntimeCostBaseline": True,
        }))
        sample_row = event_sample("2030-01-01T01:00:00Z", five_hour=11, cost=101)
        sample_row["costDelta"] = {"totalCostUsd": 1.0}

        apply_runtime_cost_measurement(sample_row, state)
        events = process_sample_delta_events(state, sample_row, [])

        self.assertEqual(events, [])
        self.assertEqual(state["runCostUsd"], 0.0)
        self.assertEqual(state["measuredCostIntervals"], 0)
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 11)
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 0.0)
        self.assertNotIn("awaitingTrustedPercentBaseline", state["windows"]["5h"])

    def test_sample_debug_log_row_uses_lean_state_without_last_sample_or_cost_totals(self):
        row = sample_debug_log_row(sample("2030-01-01T00:00:00Z", five_hour=1), [], {
            "windows": {"5h": {"baselinePercent": 1}},
            "tokenUsage": {"totals": {"requests": 1}},
            "cost": {"totalCostUsd": 2},
            "lastSample": {"checkedAt": "2030-01-01T00:00:00Z"},
            "updatedAt": "2030-01-01T00:00:00Z",
            "runCostUsd": 3,
            "measuredCostIntervals": 4,
            "hasRuntimeCostBaseline": True,
            "remoteUsageIdentity": {"plan_type": "plus"},
        })

        self.assertEqual(row["state"]["windows"]["5h"]["baselinePercent"], 1)
        self.assertEqual(row["state"]["runCostUsd"], 3)
        self.assertNotIn("lastSample", row["state"])
        self.assertNotIn("tokenUsage", row["state"])
        self.assertNotIn("cost", row["state"])

    def test_compact_quota_for_debug_removes_matches_but_keeps_windows(self):
        compact = {"complete": True, "missingWindows": [], "windows": {"5h": {}}, "matches": [{"path": "$"}]}

        debug = compact_quota_for_debug(compact)

        self.assertEqual(debug["windows"], {"5h": {}})
        self.assertNotIn("matches", debug)

    def test_compact_event_log_ignores_low_cost_delta_rows(self):
        events = derive_history_events([
            {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 3, "deltaCostUsd": 0},
            {"checkedAt": "2030-01-01T01:00:00Z", "window": "5h", "deltaPercent": 2, "deltaCostUsd": 1},
        ])

        self.assertEqual(len(events["fiveHour"]), 2)
        self.assertTrue(events["fiveHour"][0]["synthetic"])
        self.assertEqual(events["fiveHour"][1]["cumulativePercent"], 2)
        self.assertEqual(events["fiveHour"][1]["cumulativeCostUsd"], 1)

    def test_live_processing_rehydrates_stale_cumulative_totals_from_history(self):
        state = {"windows": {"5h": {"cumulativePercent": 999, "cumulativeCostUsd": 999, "baselinePercent": 5, "baselineCostUsd": 10, "baselineResetAt": None, "baselinePlan": "unknown", "baselineMultiplier": 1.0, "previousCostUsd": 10}}}
        process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=5, cost=10), [{"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 2, "deltaCostUsd": 3}])

        self.assertEqual(state["windows"]["5h"]["cumulativePercent"], 2)
        self.assertEqual(state["windows"]["5h"]["cumulativeCostUsd"], 3)

    def test_dashboard_rebases_filtered_delta_charts_from_zero(self):
        html = dashboard_html()

        self.assertIn("function rebaseDeltaEvents(list)", html)
        self.assertIn("const events=list.filter(p=>!p.synthetic);", html)
        self.assertIn("if(!events.length)return [];", html)
        self.assertIn("for(const p of events){", html)
        self.assertIn('<div class="model-filter"><span class="model-label">Models</span><div class="model-buttons" id="models"></div></div>', html)
        self.assertIn('selectedModels=new Set()', html)
        self.assertIn('function modelDisplayName(model)', html)
        self.assertIn('part.toLowerCase()==="gpt"?"GPT":/^o\\d/i.test(part)?part.toUpperCase()', html)
        self.assertIn('function updateModelControls(events)', html)
        self.assertIn('events.filter(p=>!p.synthetic&&p.model).map(p=>p.model)', html)
        self.assertIn('function toggleScopeSelection(selection,candidate,available)', html)
        self.assertIn('if(candidate==="all"){if(selection.size)selection.clear();else for(const value of available)selection.add(value)}', html)
        self.assertIn('toggleScopeSelection(selectedModels,model,available)', html)
        self.assertIn('addButton("All","all",selectedModels.size===0)', html)
        self.assertIn('available.forEach(model=>addButton(modelDisplayName(model),model,selectedModels.has(model)))', html)
        self.assertIn('function modelFilteredEvents(list)', html)
        self.assertIn('list.filter(p=>p.synthetic||selectedModels.has(p.model))', html)
        self.assertIn('updateModelControls([...fiveInRange,...sevenInRange])', html)
        self.assertIn('<div class="account-filter"><span class="account-label">Accounts</span><div class="account-buttons" id="eventAccounts"></div></div>', html)
        self.assertIn('selectedAccounts=new Set()', html)
        self.assertIn('function updateAccountControls(events)', html)
        self.assertIn('toggleScopeSelection(selectedAccounts,accountId,available.keys())', html)
        self.assertIn('function accountFilteredEvents(list)', html)
        self.assertIn('function accountFilteredQuotaPoints(list)', html)
        self.assertIn('updateAccountControls([...fiveInRange,...sevenInRange,...quotaInRange])', html)
        self.assertIn('quota=accountFilteredQuotaPoints(quotaInRange)', html)
        self.assertIn('rows.push(`Model ${modelDisplayName(p.model)}`)', html)
        self.assertIn('rows.push(`Account ${accountDisplayName(p.accountSlotId,p.accountLabel)}`)', html)
        self.assertIn("const chartStates={}, usageTimeChartStates={}, NODE_RADIUS=5, CHART_MARGINS={l:48,r:18,t:16,b:34}", html)
        self.assertIn("function denseMergeMetrics(id,list)", html)
        self.assertIn("maxPercent:maxPercent*1.03||1,maxCost:maxCost*1.08||1,minDistance:NODE_RADIUS*4", html)
        self.assertIn("function denseDeltaDistance(a,b,metrics)", html)
        self.assertIn("Math.hypot(((b.cumulativePercent||0)-(a.cumulativePercent||0))/metrics.maxPercent*metrics.plotWidth", html)
        self.assertIn("function mergeDenseDeltaEvents(list,metrics)", html)
        self.assertIn("const mergeTrailing=()=>", html)
        self.assertIn("if(index<0)return flush()", html)
        self.assertIn(
            "if(denseDeltaDistance(result[result.length-1]||{cumulativePercent:0,cumulativeCostUsd:0},"
            "{cumulativePercent:percent+(pending.deltaPercent||0),cumulativeCostUsd:cost+(pending.deltaCostUsd||0)},metrics)>metrics.minDistance)flush()",
            html,
        )
        self.assertNotIn("function scaleFromZero(values", html)
        self.assertIn("const left=e.clientX>=innerWidth/2?e.clientX-offset-w:e.clientX+offset", html)
        self.assertIn("const top=e.clientY>=innerHeight/2?e.clientY-offset-h:e.clientY+offset", html)
        self.assertIn('button id="prevDate" aria-label="Previous day">&lt;</button>', html)
        self.assertIn('input class="date-range" id="rangeDate" type="date"', html)
        self.assertIn('button id="nextDate" aria-label="Next day">&gt;</button>', html)
        self.assertIn('<span class="date-selector" id="dateSelector">', html)
        self.assertIn('<div class="quota"><span id="top5h">5h: N/A</span><span id="top7d">7d: N/A</span></div>', html)
        self.assertIn("function pointFromSample(sample)", html)
        self.assertIn('const window=(sample.windows||{})[label]||{}', html)
        self.assertIn('return {checkedAt:sample.checkedAt,timestamp:eventTimestamp(sample),fiveHour:windowPoint("5h"),sevenDay:windowPoint("7d"),cost:sample.cost||{}}', html)
        self.assertIn('points=payload.points||[], latest=pointFromSample(status.lastSample)||points[points.length-1]||pointFromSample(payload.lastSample)', html)
        self.assertIn('document.getElementById("top5h").textContent=`5h: ${displayWindows["5h"]?.usageText??pct(latest?.fiveHour?.raw)}`', html)
        self.assertIn('document.getElementById("top7d").textContent=`7d: ${displayWindows["7d"]?.usageText??pct(latest?.sevenDay?.raw)}`', html)
        self.assertIn('<span class="progress-label">5h time</span>', html)
        self.assertIn('<span class="progress-label">7d time</span>', html)
        self.assertIn('<span class="progress-label">5h usage</span>', html)
        self.assertIn('<span class="progress-label">7d usage</span>', html)
        self.assertIn(".window-progress.weekly .time-fill,.window-progress.weekly .usage-fill{background:var(--green)}", html)
        self.assertIn("function updateWindowTime(id,display,resetAt,durationSeconds)", html)
        self.assertIn("if(Number.isFinite(display?.timePercent))", html)
        self.assertIn('const fmt=n=>n==null?"N/A"', html)
        self.assertIn('reset.textContent="Reset: N/A"', html)
        self.assertIn("Math.max(0,Math.min(100,(1-(resetMs-Date.now())/(durationSeconds*1000))*100))", html)

    def test_dashboard_token_summary_follows_shared_filters_and_precedes_usage_charts(self):
        html = dashboard_html()

        self.assertLess(html.index("<h2>Token Usage</h2>"), html.index("<h2>5h Usage vs Time</h2>"))
        for element_id in ("tokenInput", "tokenCachedInput", "tokenOutput", "tokenCacheWrite", "tokenCacheHit", "tokenCost"):
            self.assertIn(f'id="{element_id}"', html)
        self.assertIn("function filteredTokenSessions(list)", html)
        self.assertIn("function selectedTokenModelValues(session)", html)
        self.assertIn('<span id="tokenTimeSpan" hidden></span><span id="tokenSessionCount">0 sessions</span>', html)
        self.assertIn("function tokenDateSpan(sessions)", html)
        self.assertIn("function updateTokenSummary(sessions)", html)
        self.assertIn("const tokens=value.usageTokens||value.tokens||{}", html)
        self.assertIn('if(Math.abs(n)<10_000)return fmt(n)', html)
        self.assertIn('if(Math.abs(n)<1_000_000)return `${new Intl.NumberFormat(undefined,{maximumFractionDigits:0}).format(n/1_000)}K`', html)
        self.assertIn('Math.abs(n)<1_000_000_000?[1_000_000,"M"]:[1_000_000_000,"B"]', html)
        self.assertIn('Math.abs(scaled)<1_000?{maximumSignificantDigits:3}:{maximumFractionDigits:0}', html)
        for element_id, total in (("tokenInput", "freshInputTokens"), ("tokenCachedInput", "cachedInputTokens"), ("tokenOutput", "outputTokens"), ("tokenCacheWrite", "cacheWriteInputTokens")):
            self.assertIn(f'document.getElementById("{element_id}").textContent=tokenFmt(totals.{total})', html)
        self.assertIn("totals.cachedInputTokens/cacheable*100", html)
        self.assertIn('timeSpan.textContent=tokenDateSpan(included);timeSpan.hidden=selected!=="All"||!timeSpan.textContent', html)
        self.assertIn("const selectedTokenSessions=accountFilteredEvents(tokenSessions)", html)
        self.assertIn('updateWindowTime("time5h",displayWindows["5h"],latest?.fiveHour?.resetAt,5*3600)', html)
        self.assertIn('updateWindowTime("time7d",displayWindows["7d"],latest?.sevenDay?.resetAt,7*24*3600)', html)
        self.assertIn("function updateUsageProgress(id,percent)", html)
        self.assertIn('updateUsageProgress("usage5h",latest?.fiveHour?.raw)', html)
        self.assertIn('updateUsageProgress("usage7d",latest?.sevenDay?.raw)', html)
        self.assertIn('lastUpdateTimestamp=!payload.accounts?.awaitingLogin&&latest?(display.percentCheckedAt||latest.checkedAt):null', html)
        self.assertIn('function updateLastUpdateAge(){if(lastUpdateTimestamp&&!document.hidden)', html)
        self.assertIn('setInterval(updateLastUpdateAge,1000)', html)
        self.assertNotIn('| raw cost ${usd(latest.cost.totalCostUsd)}', html)
        self.assertIn('let selected="Date", previousRange="24h", selectedDate=localDateValue(new Date())', html)
        self.assertIn('const vscode=typeof acquireVsCodeApi==="function"?acquireVsCodeApi():null', html)
        self.assertIn('vscode.postMessage({type:"getCodexUsageSeries",view})', html)
        self.assertNotIn('id="refresh"', html)
        self.assertIn("function selectedDateBounds()", html)
        self.assertIn("const [year,month,day]=parts, start=new Date(year,month-1,day), end=new Date(year,month-1,day+1)", html)
        self.assertIn("return {startMs:start.getTime(),endMs:end.getTime()}", html)
        self.assertIn("function localDateValue(date)", html)
        self.assertIn('return `${date.getFullYear()}-${String(date.getMonth()+1).padStart(2,"0")}-${String(date.getDate()).padStart(2,"0")}`', html)
        self.assertIn("function latestSelectableDate()", html)
        self.assertIn("date.setDate(date.getDate()+1);return localDateValue(date)", html)
        self.assertIn("function shiftSelectedDate(days)", html)
        self.assertIn("date.setDate(date.getDate()+days)", html)
        self.assertIn('selectedDate=localDateValue(date);if(selectedDate>latestSelectableDate())selectedDate=latestSelectableDate();selected="Date";setupControls();drawAll(true)', html)
        self.assertIn("function syncControls()", html)
        self.assertIn('document.querySelectorAll("[data-range]").forEach(b=>b.classList.toggle("active",b.dataset.range===selected))', html)
        self.assertIn('date.max=latest', html)
        self.assertIn('date.classList.toggle("active",selected==="Date")', html)
        self.assertIn('document.getElementById("nextDate").disabled=selectedDate>=latest', html)
        self.assertIn("function activateDateInput()", html)
        self.assertIn("if(!selectedDate)selectedDate=localDateValue(new Date())", html)
        self.assertIn('if(selected==="Date"){syncControls();return}', html)
        self.assertIn('selected="Date";syncControls();drawAll(true)', html)
        self.assertIn('if(selected==="Date")', html)

        self.assertIn("p.synthetic||(eventTimestamp(p)!=null&&eventTimestamp(p)*1000>=bounds.startMs&&eventTimestamp(p)*1000<bounds.endMs)", html)
        self.assertIn("return hasRealEvents(filtered)?filtered:[]", html)
        self.assertIn("date.onclick=activateDateInput", html)
        self.assertIn("date.onfocus=activateDateInput", html)
        self.assertIn('date.onchange=()=>{selectedDate=date.value>latestSelectableDate()?latestSelectableDate():date.value;if(selectedDate){selected="Date"}else{selected=previousRange||"24h"}setupControls();drawAll(true)}', html)
        self.assertIn('function datePickerIsOpen(date)', html)
        self.assertIn('try{return date.matches(":open")}catch{return false}', html)
        self.assertIn('DATE_LEAVE_DELAY_MS=140, DATE_LEAVE_DISTANCE=50', html)
        self.assertIn('function pointInsideDateJudgeArea(rect)', html)
        self.assertIn('rect.left-DATE_LEAVE_DISTANCE', html)
        self.assertIn('function virtualDatePickerRect(dateRect)', html)
        self.assertIn('DATE_PICKER_VIRTUAL_WIDTH=280, DATE_PICKER_VIRTUAL_HEIGHT=360', html)
        self.assertIn('function pointInsideVirtualDatePicker()', html)
        self.assertIn('(pickerOpen&&(!dateLeaveIntent.pointerMoved||pointInsideVirtualDatePicker()))||(!pickerOpen&&pointInsideDateJudgeArea(dateLeaveIntent.rect))', html)
        self.assertIn('if(dateLeaveIntent.outsideAt==null){dateLeaveIntent.outsideAt=now', html)
        self.assertIn('now-dateLeaveIntent.outsideAt<DATE_LEAVE_DELAY_MS', html)
        self.assertIn('pickerRect:virtualDatePickerRect(date.getBoundingClientRect())', html)
        self.assertIn('function cancelDateInputSelection()', html)
        self.assertIn('document.getElementById("rangeDate").blur();cancelDateLeaveTracking()', html)
        self.assertIn('document.activeElement!==date&&!datePickerIsOpen(date)', html)
        self.assertIn('selector.onpointerenter=cancelDateLeaveTracking;selector.onpointerleave=startDateLeaveTracking', html)
        self.assertIn('addEventListener("pointermove",updateDateLeavePointer,{passive:true})', html)
        self.assertIn('document.getElementById("prevDate").onclick=()=>shiftSelectedDate(-1)', html)
        self.assertIn('document.getElementById("nextDate").onclick=()=>shiftSelectedDate(1)', html)
        self.assertIn("rawKeys:[p.checkedAt]", html)
        self.assertIn("rawKeys:[...(pending.rawKeys||[pending.checkedAt]),...(p.rawKeys||[p.checkedAt])]", html)
        self.assertIn("const rebased=rebaseDeltaEvents(list), merged=mergeDenseDeltaEvents(rebased,denseMergeMetrics(id,rebased))", html)
        self.assertIn("xDomain:extent(points.map(p=>p.x))", html)
        self.assertIn('function extent(values)', html)
        self.assertIn('yDomain:extent(points.map(p=>p.y)),minPoints:2,insufficientMessage:"Not enough data for the selected scope"', html)
        self.assertIn('const [x0,x1]=opts.xDomain||extent(xs), [y0,y1]=opts.yDomain||extent(ys)', html)
        self.assertIn('const X=x=>x0===x1?(m.l+w-m.r)/2:', html)
        self.assertIn('Y=y=>y0===y1?(m.t+h-m.b)/2:', html)
        self.assertIn("const chartStates={}", html)
        self.assertIn("const chartEaseInOut=progress=>(1-Math.cos(Math.PI*progress))/2", html)
        self.assertEqual(html.count("chartEaseInOut(Math.min(1,(now-started)/duration))"), 2)
        self.assertNotIn("1-Math.pow(1-t,3)", html)
        self.assertNotIn("1-Math.pow(1-progress,3)", html)
        self.assertIn("drawAll(true)", html)
        self.assertIn("const eventTime=p=>", html)
        self.assertIn("const pointKey=p=>", html)
        self.assertIn("const currentSeries=series.map", html)
        self.assertIn("const makeLineLayer=drawSeries=>", html)
        self.assertIn("const previousPoints=previousSeries[seriesIndex]?.points||[], oldOwners=new Map(), newOwners=new Map(), moves=new Map()", html)
        self.assertIn("for(const p of previousPoints)for(const rawKey of p.rawKeys||[p.key])oldOwners.set(rawKey,p)", html)
        self.assertIn("for(const rawKey of new Set([...oldOwners.keys(),...newOwners.keys()]))", html)
        self.assertIn("moves.get(key).rawKeys.push(rawKey)", html)
        self.assertIn("const snapshotSeries=progress=>", html)
        self.assertIn("const updateLiveLineLayer=progress=>", html)
        self.assertIn("drawLayer(previousLineLayer,1-progress);drawLayer(currentLineLayer,progress)", html)
        self.assertIn("const started=performance.now(), duration=820", html)
        self.assertIn("startX:oldPoint.x", html)
        self.assertIn("function eventTimestamp(p){", html)
        self.assertIn("return list.filter(p=>p.synthetic||eventTimestamp(p)==null||eventTimestamp(p)>=max-secs)", html)
        self.assertNotIn("const withFallback=", html)
        self.assertIn("requestAnimationFrame(step)", html)
        self.assertNotIn("dashboardLoading", html)
        self.assertNotIn("beginDashboardLoading", html)
        self.assertNotIn("redrawForScope", html)
        self.assertIn("chartStates[id]={series:snapshotSeries(progress),lineLayer:liveLineLayer,frame:null}", html)
        self.assertNotIn("const percentLimit=maxPercent*.0075, costLimit=maxCost*.015", html)

    def test_dashboard_axis_labels_adapt_to_chart_dimensions(self):
        html = dashboard_html()

        self.assertIn("function axisTicks(ctx,start,end,pixels,format,axis)", html)
        self.assertIn("function sameLocalDate(first,second)", html)
        self.assertIn("function usageTimeLabel(timestamp,mode)", html)
        self.assertIn("dateOnly=x1-x0>=7*24*3600, crossesDate=!sameLocalDate(x0,x1)", html)
        self.assertIn('if(mode==="date")return dateText', html)
        self.assertIn('return mode==="dateTime"?`${dateText} ${time}`:time', html)
        self.assertIn('(x,index,labelCount)=>{const timestamp=timeScale.timestampAt(x), previousTimestamp=index?timeScale.timestampAt(m.l+(w-m.l-m.r)*(index-1)/(labelCount-1)):null', html)
        self.assertIn('dateOnly?"date":crossesDate&&(!index||!sameLocalDate(previousTimestamp,timestamp))?"dateTime":"time"', html)
        self.assertIn('charWidth=ctx.measureText("0").width', html)
        self.assertIn('let labels=[format(start,0,2),format(end,1,2)]', html)
        self.assertIn('for(let labelCount=3;;labelCount++)', html)
        self.assertIn('start+(end-start)*index/(labelCount-1)', html)
        self.assertIn('spacing=pixels/(labelCount-1)', html)
        self.assertIn('2*Math.max(...candidate.map(label=>ctx.measureText(label).width))+8*charWidth', html)
        self.assertIn('Math.max(fontHeight,(metrics.fontBoundingBoxAscent||0)+(metrics.fontBoundingBoxDescent||0),metrics.actualBoundingBoxAscent+metrics.actualBoundingBoxDescent)', html)
        self.assertIn('3.5*Math.max(...candidate.map(labelHeight))', html)
        self.assertIn('return {labels}', html)
        self.assertNotIn('let intervals=Math.max', html)
        self.assertIn("function axisTickPosition(start,pixels,index,count)", html)
        self.assertIn('function drawXAxisTickLabels(ctx,labels,start,pixels,y)', html)
        self.assertIn('index===0?"left":index===labels.length-1?"right":"center"', html)
        self.assertEqual(html.count('axisTicks(ctx,')+html.count('axisTicks(layerContext,'), 5)
        self.assertEqual(html.count('axisTickPosition('), 4)
        self.assertEqual(html.count('drawXAxisTickLabels('), 3)

    def test_dashboard_payload_exposes_usage_tokens_without_exposing_token_fields(self):
        session = {
            "sessionId": "session-a", "startedAt": "2030-01-01T00:00:00Z", "updatedAt": "2030-01-01T00:01:00Z", "accountSlotId": "account-a", "accountLabel": "Account A",
            "tokens": {"freshInputTokens": 123}, "cost": {"totalCostUsd": 0.5}, "byModel": {"gpt-5.5": {"tokens": {"freshInputTokens": 123, "outputTokens": 45}, "cost": {"totalCostUsd": 0.5}}},
        }
        accounts = {"activeAccountId": "account-a", "awaitingLogin": False, "items": [{"id": "account-a", "label": "Account A"}]}
        state = SimpleNamespace(
            history=lambda: [], quota_history=lambda: [], token_session_history=lambda: [session], state=lambda: {}, accounts=SimpleNamespace(status=lambda: accounts), last_error=None, last_sample=None, wake_event=SimpleNamespace(set=lambda: None),
        )

        payload = monitor_dashboard.dashboard_safe_json(monitor_dashboard.dashboard_series_payload(SimpleNamespace(history=Path("history.jsonl"), quota_history=Path("quota.jsonl"), state=Path("state.json")), state))

        model = payload["tokenSessions"][0]["byModel"]["gpt-5.5"]
        self.assertEqual(model["usageTokens"], {"freshInputTokens": 123, "outputTokens": 45})
        self.assertEqual(model["cost"]["totalCostUsd"], 0.5)
        self.assertNotIn('"tokens"', json.dumps(payload))

    def test_dashboard_status_payload_uses_only_current_in_memory_state(self):
        sample = {
            "checkedAt": "2030-01-01T00:01:00Z", "percentCheckedAt": "2030-01-01T00:00:00Z", "activeAccountSlotId": "account-a", "cost": {"totalCostUsd": 1.5},
            "windows": {"5h": {"usedPercent": 12, "resetAt": "2030-01-01T05:00:00Z"}, "7d": {"usedPercent": 34, "resetAt": "2030-01-08T00:00:00Z"}},
        }
        state = monitor_dashboard.UsageDashboardState.__new__(monitor_dashboard.UsageDashboardState)
        state.args = SimpleNamespace(history=Path("missing-history"), quota_history=Path("missing-quota"), token_session_history=Path("missing-tokens"), state=Path("missing-state"))
        state.lock = threading.Lock()
        state.accounts = SimpleNamespace(status=lambda: {"activeAccountId": "account-a", "awaitingLogin": False, "items": [{"id": "account-a", "label": "A"}]})
        state.last_sample, state.last_error, state.runtime_state = sample, None, {}
        state.wake_event = threading.Event()

        with mock.patch.object(monitor_dashboard, "load_history", side_effect=AssertionError("status must not load history")):
            payload = state.status_payload()

        self.assertEqual(payload["display"]["statusBarText"], "5h 12.0% · 7d 34.0%")
        self.assertEqual(payload["lastSample"]["cost"]["totalCostUsd"], 1.5)
        self.assertEqual(len(payload["revision"]), 20)
        self.assertEqual(len(payload["seriesRevision"]), 20)

    def test_dashboard_series_cache_reuses_serialized_payload_until_file_revision_changes(self):
        with self.account_directory() as directory:
            args = SimpleNamespace(
                history=directory / "history.jsonl", quota_history=directory / "quota.jsonl", token_session_history=directory / "tokens.jsonl", state=directory / "state.json",
            )
            args.history.write_text("", encoding="utf-8")
            args.quota_history.write_text("", encoding="utf-8")
            args.token_session_history.write_text("", encoding="utf-8")
            args.state.write_text("{}", encoding="utf-8")
            state = monitor_dashboard.UsageDashboardState.__new__(monitor_dashboard.UsageDashboardState)
            state.args = args
            state.lock = threading.Lock()
            state._series_build_lock = threading.Lock()
            state._series_cache_revision = state._series_cache_payload = state._series_cache_body = None
            state.accounts = SimpleNamespace(status=lambda: {"activeAccountId": "account-a", "awaitingLogin": False, "items": [{"id": "account-a", "label": "A"}]})
            state.last_sample = state.last_error = None
            state.wake_event = threading.Event()

            with mock.patch.object(monitor_dashboard, "load_history", wraps=monitor_dashboard.load_history) as history_load:
                first_payload, first_body, first_revision = state.cached_series_response()
                second_payload, second_body, second_revision = state.cached_series_response()
                args.history.write_text('{"checkedAt":"2030-01-01T00:00:00Z"}\n', encoding="utf-8")
                third_payload, third_body, third_revision = state.cached_series_response()

            self.assertIs(first_payload, second_payload)
            self.assertIs(first_body, second_body)
            self.assertEqual(first_revision, second_revision)
            self.assertNotEqual(first_revision, third_revision)
            self.assertNotEqual(first_body, third_body)
            self.assertIsNot(first_payload, third_payload)
            self.assertEqual(history_load.call_count, 2)

    def test_dashboard_exposes_independent_local_and_merged_data_views(self):
        html = dashboard_html()
        extension = Path(__file__).with_name("extension.js").read_text(encoding="utf-8")

        self.assertIn('<button data-data-view="local">Local</button><button data-data-view="merged">Merged</button>', html)
        self.assertIn('dataView="merged"', html)
        self.assertIn('fetch(`/api/series?view=${encodeURIComponent(view)}`', html)
        self.assertIn('vscode.postMessage({type:"getCodexUsageSeries",view})', html)
        self.assertIn('button.classList.toggle("active",button.dataset.dataView===dataView)', html)
        self.assertIn('async getSeries(view = "local")', extension)
        self.assertIn('url.searchParams.set("view", view)', extension)
        self.assertIn('target.webview.postMessage({ type: "codexUsageSeries", view, payload: await monitor.getSeries(view) })', extension)

    def test_dashboard_local_view_uses_only_local_quota_tokens_and_cost(self):
        local_quota = {"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "a", "accountLabel": "A", "windows": {"5h": {"usedPercent": 1}}}
        merged_quota = {"checkedAt": "2030-01-01T00:01:00Z", "accountSlotId": "a", "accountLabel": "A", "windows": {"5h": {"usedPercent": 2}}}
        local_session = {"sessionId": "local", "tokens": {}, "byModel": {}}
        state = SimpleNamespace(
            usage_data=SimpleNamespace(datasets=lambda view: ([], [merged_quota], [{"sessionId": "merged", "tokens": {}, "byModel": {}}]) if view == "merged" else ([], [local_quota], [local_session])),
            state=lambda: {}, accounts=SimpleNamespace(status=lambda: {"activeAccountId": "a", "awaitingLogin": False, "items": [{"id": "a", "label": "A"}]}), last_sample=None,
        )

        payload = monitor_dashboard.dashboard_series_payload(SimpleNamespace(history=Path("history.jsonl")), state, "local")

        self.assertEqual(payload["dataView"], "local")
        self.assertEqual(payload["quotaDataView"], "local")
        self.assertEqual(payload["quotaPoints"][0]["fiveHour"]["raw"], 1)
        self.assertEqual(payload["tokenSessions"][0]["sessionId"], "local")
        self.assertNotIn("<span class=\"rate\">Merged</span>", dashboard_html())

    def test_extension_loads_live_dashboard_with_bundled_fallback(self):
        extension = Path(__file__).with_name("extension.js").read_text(encoding="utf-8")
        html = dashboard_html()
        manifest = json.loads(Path(__file__).with_name("package.json").read_text(encoding="utf-8"))

        self.assertEqual(manifest["version"], "1.1.0")
        self.assertIn('const DASHBOARD_URL = new URL("http://127.0.0.1:8765/")', extension)
        self.assertIn("PAGE_ALLOWLIST", extension)
        self.assertIn('asset: "dashboard.html"', extension)
        self.assertIn('asset: "management.html"', extension)
        self.assertEqual(extension.count("const html = await detailsHtml(context, target.webview, currentPage)"), 2)
        self.assertEqual(extension.count("if (panel === target) target.webview.html = html"), 2)
        self.assertIn('message.type === "codexUsageAccountAction"', extension)
        self.assertIn('ACCOUNT_SWITCH_URL', extension)
        self.assertIn('ACCOUNT_RENAME_URL', extension)
        self.assertIn('ACCOUNT_DELETE_URL', extension)
        self.assertIn('const ACCOUNT_ACTION_TIMEOUT_MS = 300000', extension)
        self.assertIn('timeoutMs: ACCOUNT_ACTION_TIMEOUT_MS', extension)
        self.assertIn('STATUS_API_URL = new URL("http://127.0.0.1:8765/api/status")', extension)
        self.assertIn("const display = (await this.getStatus()).display || {}", extension)
        self.assertIn("if (error.statusCode === 404) return this.getSeries()", extension)
        self.assertIn('message.type === "getCodexUsageStatus"', extension)
        self.assertIn('fetch("/api/status",{cache:"no-store"})', html)
        self.assertIn("if(currentSeriesRevision===null||status.seriesRevision!==currentSeriesRevision)await load(true)", html)
        self.assertIn("setInterval(pollStatus,5000)", html)
        self.assertIn('addEventListener("visibilitychange",()=>{if(!document.hidden){updateLastUpdateAge();pollStatus(true)}})', html)
        self.assertNotIn("setInterval(load,5000)", html)

        self.assertNotIn("RELATIVE_TIME_UPDATE_INTERVAL_MS", extension)
        self.assertNotIn("lastTooltipRevision", extension)
        self.assertIn("const TOOLTIP_HOVER_DELAY_SECONDS = 3", extension)
        self.assertIn("Math.floor((Date.now() - timestamp) / 1000) + TOOLTIP_HOVER_DELAY_SECONDS", extension)
        self.assertIn("const tooltip = stableTooltip(display)", extension)
        self.assertIn("if (tooltip !== this.lastTooltip)", extension)
        self.assertIn('`Last update ${secondsAgo(display.percentCheckedAt)}`', extension)

    def test_dashboard_and_management_pages_expose_stacked_pop_messages(self):
        for html in (dashboard_html(), Path(__file__).with_name("management.html").read_text(encoding="utf-8")):
            self.assertIn('class="message-stack" id="messageStack"', html)
            self.assertIn('function showMessage(title,summary,details=summary,type="info")', html)
            self.assertIn('message.ondblclick=showDetails', html)
            self.assertIn('setTimeout(dismiss,10000)', html)
            self.assertIn('message.dismiss=dismiss', html)
            self.assertIn('close.setAttribute("aria-label",`Dismiss ${title}`)', html)
            self.assertIn('document.getElementById("messageStack").appendChild(message)', html)
            self.assertIn('id="messageDetailModal" role="dialog"', html)
            self.assertIn('id="messageDetailSummary"', html)
            self.assertIn('document.getElementById("messageDetailSummary").textContent=summary', html)
            self.assertIn('className="pop-message-summary"', html)

    def test_dashboard_uses_text_nodes_instead_of_html_injection_sinks(self):
        html = dashboard_html()

        self.assertNotIn("innerHTML", html)
        self.assertNotIn("outerHTML", html)
        self.assertNotIn("insertAdjacentHTML", html)
        self.assertIn("node.textContent=String(row.text??\"\")", html)
        self.assertIn("tip.replaceChildren(fragment)", html)

    def test_management_password_prompt_has_close_control(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")

        self.assertIn('class="control-login-backdrop" id="controlLoginModal"', html)
        self.assertNotIn('class="control-login-backdrop open" id="controlLoginModal"', html)
        self.assertIn('if(error.status===401)showControlLogin(error.message)', html)
        self.assertIn('id="closeControlLogin" type="button" aria-label="Close password prompt"', html)
        self.assertIn('document.getElementById("closeControlLogin").onclick=navigateDashboard', html)
        self.assertIn('event.currentTarget.querySelector("button[type=submit]")', html)

    def test_skill_link_messages_use_specific_titles_and_skill_name_detail(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")

        self.assertIn('showMessage(`Skill ${body.enabled?"Link":"Unlink"} Completed`', html)
        self.assertIn('`${body.name} was ${body.enabled?"linked to":"unlinked from"} ${body.app}.`', html)
        self.assertIn('{name:"Managed skill validation",status:"passed"', html)
        self.assertIn('{name:`${body.app} projection`,status:"passed"', html)

    def test_skill_projection_status_has_stable_width(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")

        self.assertIn('.skill-status{flex:none;inline-size:calc(8ch + 18px);text-align:center}', html)
        self.assertIn('class="pill skill-status"', html)

    def test_cloud_operation_messages_cover_started_completion_and_error(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")

        self.assertIn('showMessage(`${startOperation.name} Started`,startOperation.message,detailReport(startOperation.name,"In progress"', html)
        self.assertIn('function showCloudResult(operation,result,body)', html)
        self.assertIn('showMessage(`${operation.name} Completed`', html)
        self.assertIn('showMessage(`${operation} Failed`,report.message,report.details,"error")', html)
        self.assertIn('function errorReport(operation,error)', html)
        self.assertIn('failed?`${failed.name} failed: ${failed.detail}`', html)
        self.assertIn('message.dismissTimer=type==="progress"?null:setTimeout(dismiss,10000)', html)
        self.assertIn('startMessage?.dismiss()', html)

    def test_all_slow_cloud_actions_have_progress_messages(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")

        for path, action in (("/cloud/test", "WebDAV Test"), ("/cloud/push", "Push"), ("/cloud/fetch", "Fetch"), ("/cloud/restore", "Restore"), ("/accounts/bind", "Bind"), ("/accounts/release", "Release"), ("/skills/unmanage", "Unmanage")):
            self.assertIn(f'path.endsWith("{path}")?{{name:"{action}"', html)

    def test_bind_messages_explicitly_cover_start_finished_and_error(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")

        self.assertIn('path.endsWith("/accounts/bind")?{name:"Bind",message:"Moving the selected cloud account to this machine."', html)
        self.assertIn('`${startOperation.name} Started`', html)
        self.assertIn('"WebDAV Test Passed"', html)
        self.assertIn('`${operation.name} Completed`', html)
        self.assertIn('`${operation} Failed`', html)
        for step in ("Cloud account download and decryption", "Account identity validation", "Local vault commit", "Cloud payload removal"):
            self.assertIn(step, html)

    def test_bind_and_release_start_without_browser_confirmation(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")

        self.assertIn('button.onclick=()=>run("/api/manage/accounts/release",{accountId:button.dataset.release})', html)
        self.assertIn('button.onclick=()=>run("/api/manage/accounts/bind",{accountKey:button.dataset.bind})', html)
        self.assertNotIn('confirm(button.dataset.local===', html)
        self.assertNotIn('confirm("Copy this account to this machine', html)

    def test_manual_push_reports_noop_and_lists_changed_items_in_detail(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")

        self.assertIn('showMessage("Push Completed"', html)
        self.assertIn('"Cloud skills and recorded usage were already current."', html)
        self.assertIn('`Added skills:\\n${added.map(name=>`- ${name}`).join("\\n")}`', html)
        self.assertIn('`Updated skills:\\n${updated.map(name=>`- ${name}`).join("\\n")}`', html)
        self.assertIn('`Deleted skills:\\n${deleted.map(name=>`- ${name}`).join("\\n")}`', html)
        self.assertIn('{name:"Local skill packaging",status:"passed"', html)
        self.assertIn('{name:"Cloud skill index",status:"passed"', html)
        self.assertIn('{name:"Recorded usage data",status:usage.skipped?"skipped":"passed"', html)
        self.assertIn('{name:"Account payloads",status:"skipped"', html)
        self.assertNotIn('Accounts updated:', html)

    def test_management_page_scans_skills_silently_on_entry_and_every_five_seconds(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")

        self.assertNotIn('data-action="scan"', html)
        self.assertNotIn('showMessage("Skills scanned"', html)
        self.assertIn('load(true);load(false,true);setInterval(load,1000);setInterval(()=>load(false,true),5000)', html)
        self.assertIn('await load(true);load(false,true)', html)
        self.assertIn('if(loading){if(refreshScan)scanPending=true;return}', html)

    def test_dashboard_exposes_clear_account_controls_without_credentials(self):
        html = dashboard_html()
        source = Path(__file__).with_name("monitor_dashboard.py").read_text(encoding="utf-8")
        self.assertIn('id="accountSelect"', html)
        self.assertIn('id="newAccount"', html)
        self.assertIn('id="renameAccount"', html)
        self.assertIn('id="deleteAccount"', html)
        self.assertIn('id="deleteAccountModal"', html)
        self.assertIn('id="newAccountModal"', html)
        self.assertIn('Run Codex login in a new or restarted terminal', html)
        self.assertIn('did not respond within five minutes', html)
        self.assertIn('Saving the current account and preparing a new sign-in slot', html)
        self.assertIn('Saving the outgoing account and activating the selected account', html)
        self.assertNotIn('Waiting for the current usage refresh', html)
        account_action_source = html[html.index('async function performAccountAction'):html.index('function setupAccountControls')]
        self.assertNotIn('await load()', account_action_source)
        self.assertIn('showMessage(`${actionName} Started`,workingDetail', account_action_source)
        self.assertIn('messageDetailReport(actionName,"In progress"', account_action_source)
        self.assertIn('finally{workingMessage.dismiss();actionBusy=false;renderAccounts(payload.accounts)}', account_action_source)
        self.assertIn('showMessage("Codex Login Required","Account preparation completed, but usage polling is paused until sign-in."', html)
        self.assertNotIn('if(actionBusy){banner.textContent=', html)
        self.assertNotIn('else if(accounts.error){banner.textContent=', html)
        self.assertIn('"/api/accounts/switch"', source)
        self.assertIn('"/api/accounts/rename"', source)
        self.assertIn('"/api/accounts/delete"', source)
        self.assertIn('Content-Type must be application/json', source)
        self.assertNotIn('refresh_token', html)

    def test_dashboard_server_starts_polling_after_binding(self):
        source = Path(__file__).with_name("monitor_dashboard.py").read_text(encoding="utf-8")

        serve_source = source[source.index("def serve_dashboard"):]
        self.assertNotIn("retry_operation(state.poll_once", serve_source)
        self.assertLess(serve_source.index('DashboardHTTPServer((server_host, DASHBOARD_PORT), Handler)'), serve_source.index("thread.start()"))
        self.assertLess(serve_source.index('DashboardHTTPServer((server_host, DASHBOARD_PORT), Handler)'), serve_source.index("state = UsageDashboardState(args, opener)"))
        self.assertIn("allow_reuse_address = True", source)
        self.assertLess(serve_source.index("instance_lock.acquire()"), serve_source.index('DashboardHTTPServer((server_host, DASHBOARD_PORT), Handler)'))
        self.assertIn("self.wake_event.wait(poll_sleep_seconds(self.last_acquire_started_at, self.args.interval))", source)
        self.assertIn("threading.Thread(target=state.run_cloud_maintenance, daemon=True)", source)
        self.assertIn("threading.Thread(target=state.run_inactive_account_polling, daemon=True)", source)
        self.assertIn("self.cloud_maintenance_event.wait(5)", source)
        self.assertIn('DASHBOARD_PORT = 8765', source)
        self.assertIn('if path == "/api/status":', source)
        self.assertIn('self.headers.get("If-None-Match") == etag', source)
        self.assertIn('self.send_header("ETag", etag)', source)

    def test_dashboard_instance_lock_rejects_only_a_live_instance(self):
        first = monitor_dashboard.DashboardInstanceLock()
        second = monitor_dashboard.DashboardInstanceLock()
        self.assertTrue(first.acquire())
        try:
            self.assertFalse(second.acquire())
        finally:
            first.release()
        self.assertTrue(second.acquire())
        second.release()

    def test_dashboard_server_reports_an_existing_instance_before_binding(self):
        lock = mock.Mock()
        lock.acquire.return_value = False
        with self.account_directory() as directory, mock.patch.object(monitor_dashboard, "DashboardInstanceLock", return_value=lock), mock.patch.object(monitor_dashboard, "DashboardHTTPServer") as server, mock.patch.object(monitor_dashboard, "UsageDashboardState") as state_class, mock.patch("sys.stderr", new_callable=StringIO) as stderr:
            self.assertEqual(monitor_dashboard.serve_dashboard(SimpleNamespace(data_home=directory), None), 1)

        server.assert_not_called()
        state_class.assert_not_called()
        self.assertIn("another monitor instance is already running", stderr.getvalue())

    def test_dashboard_server_reports_occupied_port_before_initializing_state(self):
        with self.account_directory() as directory, mock.patch.object(monitor_dashboard, "DashboardHTTPServer", side_effect=OSError(10048, "address already in use")), mock.patch.object(monitor_dashboard, "UsageDashboardState") as state_class, mock.patch("sys.stderr", new_callable=StringIO) as stderr:
            self.assertEqual(monitor_dashboard.serve_dashboard(SimpleNamespace(data_home=directory), None), 1)

        state_class.assert_not_called()
        self.assertIn("0.0.0.0:8765 is unavailable", stderr.getvalue())

    def test_dashboard_server_binds_configured_all_interfaces_ip(self):
        with self.account_directory() as directory:
            (directory / "config.json").write_text(json.dumps({"server": {"host": "0.0.0.0"}}), encoding="utf-8")
            with mock.patch.object(monitor_dashboard, "DashboardHTTPServer", side_effect=OSError("stop")) as server, mock.patch("sys.stderr", new_callable=StringIO):
                self.assertEqual(monitor_dashboard.serve_dashboard(SimpleNamespace(data_home=directory), None), 1)

        server.assert_called_once_with(("0.0.0.0", 8765), mock.ANY)

    def test_dashboard_control_requests_are_not_rejected_by_proxy_origin(self):
        source = Path(__file__).with_name("monitor_dashboard.py").read_text(encoding="utf-8")

        self.assertNotIn("Cross-site control requests are not allowed", source)
        self.assertNotIn("origin_is_allowed", source)

    def test_dashboard_server_config_keeps_fixed_port_and_validates_ip(self):
        with self.account_directory() as directory:
            config_path = directory / "config.json"
            self.assertEqual(load_server_config(config_path), {"host": "0.0.0.0"})
            config_path.write_text(json.dumps({"server": {"host": "0.0.0.0"}}), encoding="utf-8")
            self.assertEqual(load_server_config(config_path), {"host": "0.0.0.0"})
            config_path.write_text(json.dumps({"server": {"host": "localhost"}}), encoding="utf-8")
            with self.assertRaisesRegex(CloudError, "127.0.0.1 or 0.0.0.0"):
                load_server_config(config_path)

    def test_cloud_manager_saves_only_dashboard_bind_ip(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)

            self.assertEqual(cloud.update_server_config("0.0.0.0"), {"host": "0.0.0.0", "restartRequired": True})
            self.assertEqual(json.loads(cloud.config_path.read_text(encoding="utf-8"))["server"], {"host": "0.0.0.0"})

    def test_management_page_exposes_fixed_port_ip_config_tab(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")

        self.assertIn('data-tab="config">Config</button>', html)
        self.assertIn('<option value="127.0.0.1">', html)
        self.assertIn('<option value="0.0.0.0">', html)
        self.assertIn("Port 8765 is fixed.", html)
        self.assertIn("full password-protected dashboard through this machine's LAN or public IP", html)
        self.assertIn("prefer a trusted VPN or an HTTPS reverse proxy", html)
        self.assertNotIn('id="serverPort"', html)
        self.assertNotIn('id="machineName"', html)
        for field in ("webdavBaseUrl", "webdavUsername", "webdavPassword", "webdavRemoteRoot", "encryptionPassphrase", "skillsAutoUpload", "usageDataAutoSync", "allowOptimisticWrites", "newControlPassword"):
            self.assertIn(f'id="{field}"', html)
        self.assertIn('run("/api/manage/config"', html)
        self.assertIn("Leave the passphrase and its saved hash empty to disable second-layer encryption.", html)

    def test_dashboard_display_formats_usage_and_remaining_time_for_extension(self):
        now = datetime.fromisoformat("2030-01-01T00:00:00+00:00").timestamp()
        display = dashboard_display({
            "checkedAt": "2030-01-01T00:00:00Z",
            "windows": {
                "5h": {"usedPercent": 42.25, "resetAt": "2030-01-01T01:00:00Z"},
                "7d": {"usedPercent": 61, "resetAt": "2030-01-02T00:00:00Z"},
            },
        }, now)

        self.assertEqual(display["statusBarText"], "5h 42.2% · 7d 61.0%")
        self.assertEqual(display["windows"]["5h"]["timeText"], "80.0%")
        self.assertTrue(display["windows"]["5h"]["resetText"].endswith("(1h 0m remaining)"))
        self.assertIn("5h: 42.2% used", display["tooltip"])
        self.assertEqual(display["lastUpdateText"], "0s ago")
        self.assertIn("Last update 0s ago", display["tooltip"])

    def test_dashboard_display_uses_last_accepted_percent_timestamp(self):
        now = datetime.fromisoformat("2030-01-01T00:01:07+00:00").timestamp()
        display = dashboard_display({"checkedAt": "2030-01-01T00:01:05Z", "percentCheckedAt": "2030-01-01T00:00:00Z"}, now)

        self.assertEqual(display["percentCheckedAt"], "2030-01-01T00:00:00Z")
        self.assertEqual(display["lastUpdateText"], "67s ago")
        self.assertIn("Last update 67s ago", display["tooltip"])

    def test_capped_jsonl_drops_oldest_rows_after_append(self):
        path = Path(__file__).with_name("test_samples_tmp.jsonl")
        try:
            if path.exists():
                path.unlink()
            for index in range(1, 5):
                append_capped_jsonl(path, {"index": index, "payload": "x" * 24}, 120)

            rows = load_history(path)

            self.assertLessEqual(path.stat().st_size, 120)
            self.assertEqual(rows[-1]["index"], 4)
            self.assertGreater(rows[0]["index"], 1)
        finally:
            if path.exists():
                path.unlink()

    def test_capped_jsonl_compacts_to_hysteresis_target_without_full_file_read(self):
        with self.account_directory() as directory:
            path, max_bytes = directory / "samples.jsonl", 1024
            index = 0
            while True:
                row = {"index": index, "payload": "测" * 40}
                encoded_size = len(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) + 1
                if path.exists() and path.stat().st_size + encoded_size > max_bytes:
                    break
                append_capped_jsonl(path, row, max_bytes)
                index += 1

            with mock.patch.object(Path, "read_bytes", side_effect=AssertionError("compaction must stream the retained suffix")):
                append_capped_jsonl(path, row, max_bytes)

            rows = load_history(path)
            self.assertLessEqual(path.stat().st_size, int(max_bytes * monitor_history.SAMPLE_LOG_COMPACT_RATIO))
            self.assertEqual(rows[-1]["index"], index)
            self.assertGreater(rows[0]["index"], 0)

    def test_capped_jsonl_failed_atomic_compaction_keeps_complete_appended_log(self):
        with self.account_directory() as directory:
            path, max_bytes = directory / "samples.jsonl", 256
            append_capped_jsonl(path, {"index": 1, "payload": "x" * 120}, max_bytes)

            with mock.patch.object(monitor_history.os, "replace", side_effect=OSError("replace failed")), self.assertRaises(OSError):
                append_capped_jsonl(path, {"index": 2, "payload": "y" * 120}, max_bytes)

            self.assertEqual([row["index"] for row in load_history(path)], [1, 2])
            self.assertEqual(list(directory.glob(".samples.jsonl.*.tmp")), [])

    def test_quota_history_backfills_accepted_samples_per_account_idempotently(self):
        with self.account_directory() as directory:
            sample_log, quota_history = directory / "samples.jsonl", directory / "quota.jsonl"
            rows = [
                {"sample": {
                    "checkedAt": "2030-01-01T00:00:00Z", "percentCheckedAt": "2030-01-01T00:00:00Z", "accountSlotId": "account-a", "accountLabel": "Account A",
                    "remoteUsage": {"accepted": True}, "windows": {"5h": {"usedPercent": 12, "resetAt": "2030-01-01T05:00:00Z", "plan": "plus"}, "7d": {"usedPercent": 34, "resetAt": "2030-01-08T00:00:00Z", "plan": "pro_lite"}},
                }},
                {"sample": {"checkedAt": "2030-01-01T00:01:30Z", "accountSlotId": "account-b", "accountLabel": "Account B", "remoteUsage": {"accepted": True}, "windows": {"5h": {"usedPercent": 56}, "7d": {"usedPercent": None, "unavailable": True}}}},
                {"sample": {"checkedAt": "2030-01-01T00:03:00Z", "accountSlotId": "account-a", "remoteUsage": {"accepted": False}, "windows": {"5h": {"usedPercent": 99}}}},
                {"sample": {
                    "checkedAt": "2030-01-01T00:03:30Z", "accountSlotId": "account-a", "accountLabel": "Account A", "remoteUsage": {"accepted": False}, "usingPreviousWindows": True,
                    "windows": {"5h": {"usedPercent": 12}}, "rejectedWindows": {"5h": {"usedPercent": 88}},
                }},
                {"sample": {"checkedAt": "2030-01-01T00:04:30Z", "accountSlotId": "account-a", "usingPreviousWindows": True, "windows": {"5h": {"usedPercent": 12}}}},
            ]
            sample_log.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

            self.assertEqual(monitor_history.backfill_quota_history(sample_log, quota_history), 3)
            self.assertEqual(monitor_history.backfill_quota_history(sample_log, quota_history), 0)
            history = monitor_history.load_quota_history(quota_history)

            self.assertEqual([(row["accountSlotId"], sorted(row["windows"])) for row in history], [("account-a", ["5h", "7d"]), ("account-b", ["5h"]), ("account-a", ["5h"])])
            self.assertEqual(history[0]["windows"]["5h"]["usedPercent"], 12.0)
            self.assertEqual(history[0]["windows"]["5h"]["plan"], "plus")
            self.assertEqual(history[0]["windows"]["7d"]["plan"], "pro_lite")
            self.assertEqual(history[1]["windows"]["5h"]["plan"], "unknown")
            self.assertEqual(history[-1]["windows"]["5h"]["usedPercent"], 88.0)
            self.assertNotIn("model", quota_history.read_text(encoding="utf-8"))

    def test_append_quota_history_sample_skips_cached_fallback(self):
        with self.account_directory() as directory:
            path = directory / "quota.jsonl"
            accepted = {
                "checkedAt": "2030-01-01T00:00:00Z", "percentCheckedAt": "2030-01-01T00:00:00Z", "accountSlotId": "account-a", "accountLabel": "Account A",
                "remoteUsage": {"accepted": True}, "windows": {"5h": {"usedPercent": 12}, "7d": {"usedPercent": 34}},
            }

            self.assertTrue(monitor_history.append_quota_history_sample(path, accepted))
            self.assertFalse(monitor_history.append_quota_history_sample(path, accepted | {"checkedAt": "2030-01-01T00:01:30Z", "usingPreviousWindows": True}))
            self.assertEqual(len(monitor_history.load_quota_history(path)), 1)

    def test_append_quota_history_sample_keeps_complete_local_plateau(self):
        with self.account_directory() as directory:
            path = directory / "quota.jsonl"
            for index, percent in enumerate((5, 5, 5, 5, 5, 6)):
                self.assertTrue(monitor_history.append_quota_history_sample(path, {
                    "checkedAt": f"2030-01-01T00:0{index}:00Z", "accountSlotId": "account-a", "accountLabel": "Account A",
                    "windows": {"5h": {"usedPercent": percent, "resetAt": "2030-01-01T05:00:00Z", "plan": "plus"}},
                }))

            self.assertEqual([row["checkedAt"] for row in monitor_history.load_quota_history(path)], [f"2030-01-01T00:0{index}:00Z" for index in range(6)])

    def test_quota_history_compaction_tracks_interleaved_accounts_independently(self):
        rows = []
        for index in range(3):
            rows.extend({
                "checkedAt": f"2030-01-01T00:0{index}:0{offset}Z", "accountSlotId": account, "accountLabel": account,
                "windows": {"5h": {"usedPercent": 5, "resetAt": "2030-01-01T05:00:00Z", "plan": "plus"}},
            } for offset, account in enumerate(("account-a", "account-b")))

        compacted = monitor_history.compact_quota_history_rows(rows)

        self.assertEqual([(row["accountSlotId"], row["checkedAt"]) for row in compacted], [
            ("account-a", "2030-01-01T00:00:00Z"), ("account-b", "2030-01-01T00:00:01Z"),
            ("account-a", "2030-01-01T00:02:00Z"), ("account-b", "2030-01-01T00:02:01Z"),
        ])
        self.assertEqual([row["compaction"] for row in compacted if row.get("compaction")], [
            {"continuousFrom": "2030-01-01T00:00:00Z", "omittedSamples": 1},
            {"continuousFrom": "2030-01-01T00:00:01Z", "omittedSamples": 1},
        ])

    def test_quota_history_compaction_keeps_real_long_gap_discontinuous(self):
        rows = [{
            "checkedAt": checked_at, "accountSlotId": "account-a", "accountLabel": "Account A",
            "windows": {"5h": {"usedPercent": 5, "resetAt": "2030-01-02T05:00:00Z", "plan": "plus"}},
        } for checked_at in ("2030-01-01T00:00:00Z", "2030-01-01T00:10:00Z", "2030-01-01T00:20:00Z", "2030-01-01T08:00:00Z", "2030-01-01T08:10:00Z", "2030-01-01T08:20:00Z")]

        compacted = monitor_history.compact_quota_history_rows(rows)

        self.assertEqual([row["checkedAt"] for row in compacted], ["2030-01-01T00:00:00Z", "2030-01-01T00:20:00Z", "2030-01-01T08:00:00Z", "2030-01-01T08:20:00Z"])
        self.assertNotIn("compaction", compacted[2])
        self.assertEqual([row["compaction"]["continuousFrom"] for row in (compacted[1], compacted[3])], ["2030-01-01T00:00:00Z", "2030-01-01T08:00:00Z"])

    def test_quota_history_sample_does_not_require_cost_or_trusted_delta_baseline(self):
        sample = {"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "account-a", "accountLabel": "Account A", "windows": {"5h": {"usedPercent": 42}, "7d": {"usedPercent": 61}}}

        row = monitor_history.quota_history_row_from_sample(sample)

        self.assertEqual(row["windows"]["5h"]["usedPercent"], 42.0)
        self.assertEqual(row["windows"]["5h"]["plan"], "unknown")
        self.assertNotIn("cost", row)

    def test_quota_history_merge_preserves_known_plan_over_legacy_duplicate(self):
        known = {"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "account-a", "accountLabel": "Account A", "windows": {"5h": {"usedPercent": 42, "plan": "plus"}}}
        legacy = {"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "account-a", "accountLabel": "Account A", "windows": {"5h": {"usedPercent": 42}}}

        merged = monitor_history.merge_quota_history_rows([known, legacy])

        self.assertEqual(merged[0]["windows"]["5h"]["plan"], "plus")

    def test_poll_persists_quota_before_delta_validation(self):
        source = Path(monitor_dashboard.__file__).read_text(encoding="utf-8")

        self.assertLess(source.index("append_quota_history_sample(self.args.quota_history, sample)"), source.index("events = process_sample_delta_events(self.runtime_state, sample, history)"))

    def test_dashboard_series_returns_quota_points_for_all_accounts(self):
        accounts = {"awaitingLogin": False, "activeAccountId": "account-a", "items": [{"id": "account-a", "label": "Account A"}, {"id": "account-b", "label": "Account B"}]}
        state = SimpleNamespace(
            history=lambda: [],
            quota_history=lambda: [
                {"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "account-a", "accountLabel": "Account A", "windows": {"5h": {"usedPercent": 12, "resetAt": None, "plan": "plus"}}},
                {"checkedAt": "2030-01-01T00:01:30Z", "accountSlotId": "account-b", "accountLabel": "Account B", "windows": {"7d": {"usedPercent": 34, "resetAt": None}}, "compaction": {"continuousFrom": "2029-12-31T12:00:00Z", "omittedSamples": 3}},
            ],
            state=lambda: {},
            accounts=SimpleNamespace(status=lambda: accounts),
            last_error=None,
            last_sample=None,
            wake_event=SimpleNamespace(set=lambda: None),
        )

        payload = monitor_dashboard.dashboard_series_payload(SimpleNamespace(history=Path("history.jsonl"), quota_history=Path("quota.jsonl"), state=Path("state.json")), state)

        self.assertNotIn("quotaHistoryPath", payload)
        self.assertEqual([point["accountSlotId"] for point in payload["quotaPoints"]], ["account-a", "account-b"])
        self.assertEqual(payload["quotaPoints"][0]["fiveHour"]["raw"], 12.0)
        self.assertEqual(payload["quotaPoints"][0]["fiveHour"]["continuous"], 12.0)
        self.assertEqual(payload["quotaPoints"][0]["fiveHour"]["plan"], "plus")
        self.assertEqual(payload["quotaPoints"][1]["compactedFrom"], monitor_common.parse_timestamp("2029-12-31T12:00:00Z"))

    def test_usage_time_filter_removes_isolated_and_consecutive_false_drops_without_changing_raw_history(self):
        rows = [
            {"checkedAt": checked_at, "accountSlotId": "account-a", "windows": {"7d": {"usedPercent": percent, "resetAt": "2030-01-08T00:00:00Z"}}}
            for checked_at, percent in (
                ("2030-01-01T00:00:00Z", 38), ("2030-01-01T00:01:30Z", 3), ("2030-01-01T00:03:00Z", 3), ("2030-01-01T00:04:30Z", 38),
                ("2030-01-01T00:06:00Z", 13), ("2030-01-01T00:07:30Z", 13), ("2030-01-01T00:09:00Z", 39),
            )
        ]

        points = monitor_dashboard.dashboard_quota_points(rows)

        self.assertEqual([point["sevenDay"]["raw"] for point in points], [38, 3, 3, 38, 13, 13, 39])
        self.assertEqual([point["sevenDay"]["continuous"] for point in points], [38, None, None, 38, None, None, 39])

    def test_usage_time_filter_removes_consecutive_upward_spikes_using_historical_rate(self):
        rows = [
            {"checkedAt": checked_at, "accountSlotId": "account-a", "windows": {"7d": {"usedPercent": percent, "resetAt": "2030-01-08T00:00:00Z"}}}
            for checked_at, percent in (
                ("2030-01-01T00:00:00Z", 20), ("2030-01-01T00:01:30Z", 70), ("2030-01-01T00:03:00Z", 71), ("2030-01-01T00:04:30Z", 21),
            )
        ]

        points = monitor_dashboard.dashboard_quota_points(rows)

        self.assertEqual([point["sevenDay"]["continuous"] for point in points], [20, None, None, 21])

    def test_usage_time_filter_learns_a_legitimate_fast_rate_from_history(self):
        rows = [
            {"checkedAt": f"2030-01-01T00:{minute:02}:00Z", "accountSlotId": "account-a", "windows": {"5h": {"usedPercent": minute * 8, "resetAt": "2030-01-01T05:00:00Z"}}}
            for minute in range(11)
        ]

        points = monitor_dashboard.dashboard_quota_points(rows)

        self.assertEqual([point["fiveHour"]["continuous"] for point in points], [minute * 8 for minute in range(11)])

    def test_usage_time_filter_keeps_due_and_supported_manual_resets_with_post_reset_consumption(self):
        due_rows = [
            {"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "due", "windows": {"7d": {"usedPercent": 80, "resetAt": "2030-01-01T00:02:00Z"}}},
            {"checkedAt": "2030-01-01T00:03:00Z", "accountSlotId": "due", "windows": {"7d": {"usedPercent": 7, "resetAt": "2030-01-08T00:03:00Z"}}},
            {"checkedAt": "2030-01-01T00:04:30Z", "accountSlotId": "due", "windows": {"7d": {"usedPercent": 8, "resetAt": "2030-01-08T00:03:00Z"}}},
        ]
        manual_rows = [
            {"checkedAt": checked_at, "accountSlotId": "manual", "windows": {"7d": {"usedPercent": percent, "resetAt": reset_at}}}
            for checked_at, percent, reset_at in (
                ("2030-01-01T00:00:00Z", 70, "2030-01-05T00:00:00Z"), ("2030-01-01T01:00:00Z", 6, "2030-01-08T01:00:00Z"),
                ("2030-01-01T01:02:00Z", 7, "2030-01-08T01:00:00Z"), ("2030-01-01T01:04:00Z", 7, "2030-01-08T01:00:00Z"),
                ("2030-01-01T01:06:00Z", 8, "2030-01-08T01:00:00Z"),
            )
        ]

        points = monitor_dashboard.dashboard_quota_points(due_rows + manual_rows)

        self.assertEqual([point["sevenDay"]["continuous"] for point in points[:3]], [80, 7, 8])
        self.assertEqual([point["sevenDay"]["continuous"] for point in points[3:]], [70, 6, 7, 7, 8])

    def test_usage_time_filter_rejects_consecutive_foreign_reset_responses_when_original_stream_returns(self):
        rows = [
            {"checkedAt": checked_at, "accountSlotId": "account-a", "windows": {"7d": {"usedPercent": percent, "resetAt": reset_at}}}
            for checked_at, percent, reset_at in (
                ("2030-01-01T00:00:00Z", 40, "2030-01-05T00:00:00Z"), ("2030-01-01T01:00:00Z", 12, "2030-01-08T01:00:00Z"),
                ("2030-01-01T01:02:00Z", 12, "2030-01-08T01:00:00Z"), ("2030-01-01T01:04:00Z", 13, "2030-01-08T01:00:00Z"),
                ("2030-01-01T01:06:00Z", 13, "2030-01-08T01:00:00Z"), ("2030-01-01T01:08:00Z", 40, "2030-01-05T00:00:00Z"),
            )
        ]

        points = monitor_dashboard.dashboard_quota_points(rows)

        self.assertEqual([point["sevenDay"]["continuous"] for point in points], [40, None, None, None, None, 40])

    def test_dashboard_payload_removes_sensitive_account_and_sample_data(self):
        accounts = {"activeAccountId": "local-a", "awaitingLogin": False, "items": [{"id": "local-a", "label": "Account A", "email": "private@example.test", "ready": True, "active": True, "accountKey": "cloud-key"}]}
        sample = {
            "checkedAt": "2030-01-01T00:00:00Z", "activeAccountSlotId": "local-a", "windows": {"5h": {"usedPercent": 12, "resetAt": None, "path": "rate_limit.primary"}}, "cost": {},
            "remoteUsage": {"rawResponse": {"email": "private@example.test", "account_id": "acct-secret", "user_id": "user-secret"}, "authIdentity": {"account_id": "acct-secret"}},
            "tokenUsage": {"totals": {"requests": 1}},
        }
        state = SimpleNamespace(
            history=lambda: [], quota_history=lambda: [], state=lambda: {"lastSample": sample}, accounts=SimpleNamespace(status=lambda: accounts), last_error=None, last_sample=sample, wake_event=SimpleNamespace(set=lambda: None),
        )

        payload = monitor_dashboard.dashboard_safe_json(monitor_dashboard.dashboard_series_payload(SimpleNamespace(history=Path("history.jsonl"), quota_history=Path("quota.jsonl"), state=Path("state.json")), state))
        serialized = json.dumps(payload)

        self.assertIn("Account A", serialized)
        self.assertIn("local-a", serialized)
        for sensitive in ("private@example.test", "acct-secret", "user-secret", "rawResponse", "authIdentity", "tokenUsage", "rate_limit.primary"):
            self.assertNotIn(sensitive, serialized)
        self.assertEqual(monitor_dashboard.dashboard_safe_json({"password": True, "email": "private@example.test", "tokens": {"refresh_token": "secret"}}), {"password": True})

    def test_management_payload_exposes_only_non_sensitive_status_fields(self):
        state = SimpleNamespace(
            skills=SimpleNamespace(status=lambda: {"version": 1, "privatePath": "C:/private", "items": [{"name": "alpha", "assignments": {"codex": True}, "projections": {"codex": {"state": "linked", "target": "C:/private/alpha"}}, "errors": {}}]}, scan=lambda _refresh: []),
            cloud=SimpleNamespace(config=lambda: {"server": {"host": "127.0.0.1"}}, editable_config=lambda: {"server": {"host": "127.0.0.1"}, "webdav": {"enabled": True, "baseUrl": "https://private.example.test", "username": "private-user", "remoteRoot": "private-root"}, "secretsConfigured": {"password": True, "encryptionPassphrase": True, "controlPassword": True}}, redacted_status=lambda: {"configPath": "C:/private/config.json", "webdav": {"enabled": True, "baseUrl": "https://private.example.test", "username": "private-user"}, "secretsConfigured": {"password": True}, "autoSync": {}}, cached_remote_accounts=lambda: [{"accountKey": "opaque-key", "accountId": "acct-secret", "email": "private@example.test", "label": "Cloud A", "revisionId": "revision-secret"}]),
            accounts=SimpleNamespace(status=lambda: {"activeAccountId": "local-a", "awaitingLogin": False, "items": [{"id": "local-a", "label": "Account A", "email": "private@example.test", "ready": True, "active": True, "accountKey": "opaque-key"}]}), projection_errors=[],
        )

        serialized = json.dumps(monitor_dashboard.management_payload(state))

        for expected in ("Account A", "Cloud A", "alpha", "opaque-key"):
            self.assertIn(expected, serialized)
        for expected in ("https://private.example.test", "private-user", "private-root"):
            self.assertIn(expected, serialized)
        for sensitive in ("private@example.test", "acct-secret", "revision-secret", "C:/private", "passwordHash", "cookieSecret"):
            self.assertNotIn(sensitive, serialized)

    def test_dashboard_exposes_account_filtered_usage_time_curves_with_nodes(self):
        html = dashboard_html()

        self.assertIn('<canvas class="chart" id="usageTime5h"></canvas>', html)
        self.assertIn('<canvas class="chart" id="usageTime7d"></canvas>', html)
        self.assertIn('<div class="legend" id="usageTime5hLegend"></div>', html)
        self.assertIn('<div class="legend" id="usageTime7dLegend"></div>', html)
        self.assertIn('<button id="collapseUsageGaps" type="button">Collapse gaps</button>', html)
        self.assertIn('<button id="collapseUsageFlat" type="button">Collapse flat</button>', html)
        self.assertIn('function setupUsageFoldControls()', html)
        self.assertIn('button.setAttribute("aria-pressed",String(getValue()))', html)
        self.assertIn('setupAccountControls();setupUsageFoldControls();load(true)', html)
        self.assertIn('function quotaTimeDomain(points)', html)
        self.assertIn('function drawUsageTimeChart(id,label,points,animate=false)', html)
        self.assertIn('function accountCurveColor(accountId)', html)
        self.assertIn('.sort((a,b)=>accountDisplayName(a,', html)
        self.assertIn('{numeric:true})||a.localeCompare(b)', html)
        self.assertIn('function renderUsageLegend(id,groups)', html)
        self.assertIn('valueOf=point=>windowOf(point)?.continuous', html)
        self.assertNotIn('function usageTimeRenderedValues(points,valueOf)', html)
        self.assertIn('function usageTimeFoldMetrics(gaps,x0,x1,left,right,charWidth)', html)
        self.assertIn('const minWidth=charWidth*4.5', html)
        self.assertIn('base=Math.min(10,Math.max(2,', html)
        self.assertIn('const amplifier=Math.min(10,Math.max(4,', html)
        self.assertIn('plotWidth*.012', html)
        self.assertNotIn('maxWidth=', html)
        self.assertIn('const calculatedWidth=amplifier*Math.log1p(gap.duration/referenceGap)/Math.log(base);return {...gap,width:Math.max(minWidth,calculatedWidth)}', html)
        self.assertIn('function usageTimeAccountFoldRanges(points,label,valueOf,x0,x1)', html)
        self.assertIn('if(breakUsageCurve(previous,current,label,valueOf))add(start,end,"gap")', html)
        self.assertIn('else if(valueOf(previous)===valueOf(current))add(start,end,"flat")', html)
        self.assertIn('function usageTimeFoldCandidates(groups,label,valueOf,x0,x1,collapseGaps,collapseFlat)', html)
        self.assertIn('if(!states.every(kind=>kind==="gap"?collapseGaps:kind==="flat"&&collapseFlat))continue', html)
        self.assertIn('previous.kind=previous.hasGap&&previous.hasFlat?"mixed":previous.hasGap?"gap":"unchanged"', html)
        self.assertIn('function usageTimeEdgeFoldRange(range,x0,x1,left,right)', html)
        self.assertIn('const leadingMargin=range.start===x0?10:0, trailingMargin=range.end===x1?10:0', html)
        self.assertIn('sourceStart:range.start,sourceEnd:range.end,leadingMargin,trailingMargin', html)
        self.assertIn('function usageTimeFinalizeEdgeFolds(folds,x0,x1,left,right)', html)
        self.assertIn('const secondsPerPixel=unfoldedDuration/flexibleWidth', html)
        self.assertIn('if(unfoldedDuration<=0||flexibleWidth<=0)return []', html)
        self.assertIn('function usageTimeFolds(groups,label,valueOf,x0,x1,left,right,charWidth,collapseGaps=true,collapseFlat=true)', html)
        self.assertIn('usageTimeFoldMetrics(usageTimeFoldCandidates(groups,label,valueOf,x0,x1,collapseGaps,collapseFlat)', html)
        self.assertIn('.map(range=>usageTimeEdgeFoldRange(range,x0,x1,left,right)).filter(Boolean)', html)
        self.assertIn('timeScale.x(gap.end)-timeScale.x(gap.start)>gap.width*(gap.kind==="gap"?2:4)', html)
        self.assertIn('if(!additions.length)return usageTimeFinalizeEdgeFolds(folds,x0,x1,left,right)', html)
        self.assertIn('function usageTimeScale(x0,x1,folds,left,right)', html)
        self.assertIn('folds.reduce((sum,gap)=>sum+gap.width,0)', html)
        self.assertIn('function drawUsageTimeFolds(ctx,folds,top,bottom,part="all")', html)
        self.assertIn('if(seconds>=3600)return `${Math.round(seconds/3600)}h`', html)
        self.assertNotIn('h gap`', html)
        self.assertNotIn('ctx.fillRect(center-labelWidth/2-3,top+3,labelWidth+6,13)', html)
        self.assertIn('const renderGapLabelLayer=(folds,layer=document.createElement("canvas"))=>', html)
        self.assertIn('ctx.font="11px system-ui"', html)
        self.assertIn('usageTimeFolds(groups,label,renderedValueOf,x0,x1,m.l,w-m.r,ctx.measureText("0").width,collapseUsageGaps,collapseUsageFlat)', html)
        self.assertIn('drawUsageTimeFolds(layerContext,folds,m.t,h-m.b,"labels")', html)
        self.assertIn('drawUsageTimeFolds(layerContext,folds,m.t,h-m.b,"geometry")', html)
        self.assertIn('gapTransitions=currentFolds.map', html)
        self.assertIn('startX0:oldGap?.x0??gap.x0,startX1:oldGap?.x1??gap.x1', html)
        self.assertIn('x0:gap.startX0+(gap.x0-gap.startX0)*progress', html)
        self.assertIn('x1:gap.startX1+(gap.x1-gap.startX1)*progress', html)
        self.assertIn('const visibleGapLabels=transitioning?renderGapLabelLayer(animatedFolds,movingGapLabels):gapLabelLayer', html)
        self.assertIn('drawLayer(leavingGapLabelLayer,transitioning?1-progress:0,liveGapLabelContext)', html)
        self.assertIn('drawLayer(leavingGapLayer,transitioning?1-progress:0,liveGapContext)', html)
        self.assertIn('drawLayer(previousLabelLayer,transitioning?1-progress:0,liveLabelContext)', html)
        self.assertIn('drawLayer(labelLayer,transitioning?progress:1,liveLabelContext)', html)
        self.assertIn('(x,index,labelCount)=>{const timestamp=timeScale.timestampAt(x)', html)
        self.assertIn('values=valid.map(point=>Math.max(0,Math.min(100,renderedValueOf(point))))', html)
        self.assertIn('xDomain=extent(timestamps), yDomain=extent(values), x0=xDomain[0], x1=xDomain[1], y0=yDomain[0], y1=yDomain[1]', html)
        self.assertIn('Y=y=>y0===y1?(m.t+h-m.b)/2:h-m.b-(y-y0)/(y1-y0)*(h-m.t-m.b)', html)
        self.assertIn('if(x0===x1)return {x:()=>left+(right-left)/2,timestampAt:()=>x0,folds:[]}', html)
        self.assertIn('const groups=[...grouped].filter(([,accountPoints])=>accountPoints.length)', html)
        self.assertIn('layerContext.rect(m.l-layerContext.lineWidth,m.t-layerContext.lineWidth,w-m.l-m.r+layerContext.lineWidth*2,h-m.t-m.b+layerContext.lineWidth*2);layerContext.clip()', html)
        self.assertIn('for(const item of series){if(!item.points.length||item.alpha===0)continue;layerContext.globalAlpha=item.alpha??1;layerContext.strokeStyle=item.color;', html)
        self.assertIn('for(const point of item.points){if(!paths.length||point.breakBefore)paths.push([]);paths[paths.length-1].push(point)}', html)
        self.assertIn('const curveLength=path.slice(1).reduce((length,point,index)=>length+Math.hypot((point.markerX??point.x)-(path[index].markerX??path[index].x),(point.markerY??point.y)-(path[index].markerY??path[index].y)),0), pointDiameter=layerContext.lineWidth*2;', html)
        self.assertIn('if(curveLength>=pointDiameter)continue;', html)
        self.assertIn('layerContext.arc(center[0],center[1],layerContext.lineWidth,0,Math.PI*2)', html)
        self.assertNotIn('visiblePoints.some', html)
        self.assertIn('points.length?"Not enough data for the selected scope":"No data for this range"', html)
        self.assertNotIn("Not enough data for the selected accounts", html)
        self.assertIn('function plusEquivalentUsage(point,label,valueOf)', html)
        self.assertIn('{plus:1,pro_lite:5,pro:20}[point[label]?.plan]||1', html)
        self.assertIn('function compactedUsageGap(previous,current)', html)
        self.assertIn('if(compactedUsageGap(previous,current))return false', html)
        self.assertIn('function breakUsageCurve(previous,current,label,valueOf)', html)
        self.assertIn('if(gap>4*3600)return true', html)
        self.assertIn('if(gap<=30*60)return false', html)
        self.assertIn('const previousUsage=plusEquivalentUsage(previous,label,valueOf), currentUsage=plusEquivalentUsage(current,label,valueOf)', html)
        self.assertIn('if(previousUsage-currentUsage>=3)return true', html)
        self.assertIn('Math.abs(currentUsage-previousUsage)/(gap/60)>=(label==="fiveHour"?.5:.1)', html)
        self.assertIn('breakBefore:!index||breakUsageCurve(accountPoints[index-1],point,label,renderedValueOf)', html)
        self.assertIn('`${label==="fiveHour"?"5h":"7d"} usage ${Number(nearest.value).toFixed(1)}%`', html)
        self.assertIn('drawUsageTimeChart("usageTime5h","fiveHour",quota,animate)', html)
        self.assertIn('drawUsageTimeChart("usageTime7d","sevenDay",quota,animate)', html)
        self.assertIn('const previousSeries=previous?.series||[]', html)
        self.assertIn('const oldPoints=previousByAccount.get(series.accountId)?.points||[], oldOwners=new Map', html)
        self.assertIn('hasOverlap=series.points.some(point=>oldOwners.has(point.key))', html)
        self.assertIn('Math.abs(candidate.timestamp-point.timestamp)<Math.abs(nearest.timestamp-point.timestamp)', html)
        self.assertIn('point.startX+(point.x-point.startX)*progress', html)
        self.assertIn('point.startY+(point.y-point.startY)*progress', html)
        self.assertIn('markerX:point.x,markerY:point.y', html)
        self.assertIn('alpha:series.hasOverlap?1:progress', html)
        self.assertIn('const currentSegments=new Map', html)
        self.assertIn('point.breakBefore||currentSegments.get(series.accountId)?.has', html)
        self.assertIn('drawLayer(leavingCurve,transitioning?1-progress:0,liveContext)', html)
        self.assertIn('drawLayer(baseLayer);drawLayer(liveLabels);drawLayer(liveGap);drawLayer(liveGapLabels);drawLayer(liveCurve)', html)
        self.assertIn('drawLayer(baseLayer);drawLayer(labelLayer);drawLayer(gapLayer);drawLayer(gapLabelLayer);drawLayer(curveLayer)', html)
        self.assertNotIn('drawLayer(previousCurve', html)
        self.assertIn('const started=performance.now(), duration=820', html)
        self.assertIn('if(!animate||!previousLabelLayer||matchMedia("(prefers-reduced-motion: reduce)").matches){finish();return}', html)
        self.assertIn('state.series=animatedSeries', html)
        self.assertLess(html.index("5h Reset Time vs Usage"), html.index("5h Usage vs Time"))
        self.assertLess(html.index("5h Usage vs Time"), html.index("5h Cost vs Usage"))
        self.assertNotIn("Current active account", html)
        self.assertNotIn("Delta Cost vs Delta Usage", html)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for dashboard behavior tests")
    def test_dashboard_combines_gap_and_flat_usage_fold_intervals(self):
        html = dashboard_html()
        script = html[html.index("function eventTimestamp"):html.index("function updateWindowTime")] + html[html.index("function plusEquivalentUsage"):html.index("function accountCurveColor")] + r'''
const point=(timestamp,value,compactedFrom=null)=>({timestamp,compactedFrom,fiveHour:{continuous:value,plan:"plus"}}), valueOf=point=>point.fiveHour.continuous;
const candidates=(groups,x0,x1,gaps=true,flat=true)=>usageTimeFoldCandidates(groups,"fiveHour",valueOf,x0,x1,gaps,flat), folds=(groups,x0,x1,gaps=true,flat=true)=>usageTimeFolds(groups,"fiveHour",valueOf,x0,x1,0,1000,6,gaps,flat);
const outside=usageTimeAccountFoldRanges([point(1000,1),point(2000,2)],"fiveHour",valueOf,0,3000), expectedOutside=[{start:0,end:1000,kind:"gap"},{start:2000,end:3000,kind:"gap"}];
if(JSON.stringify(outside)!==JSON.stringify(expectedOutside))throw new Error(`Before/after sample gaps were not preserved: ${JSON.stringify(outside)}`);
const pureGap=candidates([["a",[point(0,1),point(20000,2)]],["b",[point(0,3),point(20000,4)]]],0,20000), pureFlat=candidates([["a",[point(0,1),point(1000,1)]],["b",[point(0,3),point(1000,3)]]],0,1000);
if(pureGap.length!==1||pureGap[0].kind!=="gap"||pureFlat.length!==1||pureFlat[0].kind!=="unchanged")throw new Error(`Pure fold kinds were incorrect: ${JSON.stringify({pureGap,pureFlat})}`);
const exactGroups=[["a",[point(0,1),point(20000,2),point(21000,3)]],["b",[point(0,10),point(1000,11),point(10000,11),point(21000,11)]]];
const exact=candidates(exactGroups,0,21000), expectedExact=[{start:1000,end:20000,duration:19000,kind:"mixed",hasGap:true,hasFlat:true}];
if(JSON.stringify(exact)!==JSON.stringify(expectedExact))throw new Error(`Expected exact B-C mixed fold, received ${JSON.stringify(exact)}`);
if(usageGapLabel(exact[0].duration)!=="5h")throw new Error("Mixed fold label did not use the combined duration");
const swapGroups=[["a",[point(0,0),point(1000,1),point(21000,2),point(31000,2),point(41000,2),point(42000,3)]],["b",[point(0,10),point(1000,11),point(11000,11),point(21000,11),point(41000,12),point(42000,13)]]];
const swapped=candidates(swapGroups,0,42000), expectedSwap=[{start:1000,end:41000,duration:40000,kind:"mixed",hasGap:true,hasFlat:true}];
if(JSON.stringify(swapped)!==JSON.stringify(expectedSwap))throw new Error(`Expected maximal state-switch fold, received ${JSON.stringify(swapped)}`);
if(candidates(swapGroups,0,42000,false,true).length||candidates(swapGroups,0,42000,true,false).length)throw new Error("Mixed folds must require both controls");
const changingGroups=[...swapGroups,["c",[point(0,20),point(1000,21),point(11000,21),point(12000,22),point(22000,22),point(32000,22),point(41000,22),point(42000,23)]]], split=candidates(changingGroups,0,42000);
if(split.length!==2||split[0].end!==11000||split[1].start!==12000)throw new Error(`Changing curve did not split mixed fold: ${JSON.stringify(split)}`);
if(split.some((candidate,index)=>index&&split[index-1].end>candidate.start))throw new Error(`Mixed folds overlap: ${JSON.stringify(split)}`);
const compacted=usageTimeAccountFoldRanges([point(0,5),point(20000,5,0)],"fiveHour",valueOf,0,20000);
if(compacted.length!==1||compacted[0].kind!=="flat")throw new Error(`Compacted plateau was not flat: ${JSON.stringify(compacted)}`);
const changingTail=(start,end,offset)=>Array.from({length:(end-start)/1000+1},(_,index)=>point(start+index*1000,offset+index));
const gapAccount=[point(0,0),point(15000,1),...changingTail(16000,200000,2)], flatAccount=[...Array.from({length:16},(_,index)=>point(index*1000,10)),...changingTail(16000,200000,11)];
if(!folds([["a",gapAccount],["b",gapAccount]],0,200000,true,false).some(fold=>fold.kind==="gap"))throw new Error("Pure gap did not pass 2x threshold");
if(folds([["a",flatAccount],["b",flatAccount]],0,200000,false,true).length)throw new Error("Pure flat incorrectly passed 4x threshold");
if(folds([["a",gapAccount],["b",flatAccount]],0,200000,true,true).length)throw new Error("Mixed fold incorrectly used 2x threshold");
const leadingGroups=[["a",[point(0,0),point(40000,1),...changingTail(41000,200000,2)]],["b",[point(0,10),point(10000,10),point(20000,10),point(30000,10),point(40000,10),...changingTail(41000,200000,11)]]];
const leading=folds(leadingGroups,0,200000), leadingScale=usageTimeScale(0,200000,leading,0,1000);
if(leading.length!==1||leading[0].kind!=="mixed"||Math.abs(leadingScale.folds[0].x0-10)>1e-9)throw new Error(`Leading mixed margin was not 10px: ${JSON.stringify(leadingScale.folds)}`);
const prefixA=changingTail(0,160000,0), prefixB=changingTail(0,160000,1000), trailingValue=prefixB.at(-1).fiveHour.continuous;
const trailingGroups=[["a",[...prefixA,point(200000,999)]],["b",[...prefixB,point(170000,trailingValue),point(180000,trailingValue),point(190000,trailingValue),point(200000,trailingValue)]]];
const trailing=folds(trailingGroups,0,200000), trailingScale=usageTimeScale(0,200000,trailing,0,1000);
if(trailing.length!==1||trailing[0].kind!=="mixed"||Math.abs(1000-trailingScale.folds[0].x1-10)>1e-9)throw new Error(`Trailing mixed margin was not 10px: ${JSON.stringify(trailingScale.folds)}`);
'''

        result = subprocess.run([shutil.which("node")], input=script, text=True, capture_output=True, cwd=Path(__file__).parent)

        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def test_write_history_groups_delta_events_by_window(self):
        path = Path(__file__).with_name("test_history_tmp.jsonl")
        try:
            write_history(path, [
                {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 1, "deltaCostUsd": 2},
                {"checkedAt": "2030-01-01T01:00:00Z", "window": "7d", "deltaPercent": 3, "deltaCostUsd": 4},
            ])

            text = path.read_text(encoding="utf-8")

            self.assertIn('{\n  "window": "5h",\n  "delta": [', text)
            self.assertIn('"accountSlotId":"unknown","accountLabel":"Unknown"', text)
            self.assertIn('{\n  "window": "7d",\n  "delta": [', text)
            self.assertEqual(text.count('"accountSlotId":"unknown"'), 2)
            self.assertEqual(load_history(path), [
                {"checkedAt": "2030-01-01T00:00:00Z", "deltaPercent": 1, "deltaCostUsd": 2, "accountSlotId": "unknown", "accountLabel": "Unknown", "window": "5h"},
                {"checkedAt": "2030-01-01T01:00:00Z", "deltaPercent": 3, "deltaCostUsd": 4, "accountSlotId": "unknown", "accountLabel": "Unknown", "window": "7d"},
            ])
        finally:
            if path.exists():
                path.unlink()

    def test_append_history_preserves_grouped_delta_event_history(self):
        path = Path(__file__).with_name("test_history_tmp.jsonl")
        try:
            if path.exists():
                path.unlink()

            append_history(path, {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 1, "deltaCostUsd": 2})
            append_history(path, {"checkedAt": "2030-01-01T01:00:00Z", "window": "7d", "deltaPercent": 3, "deltaCostUsd": 4})

            text = path.read_text(encoding="utf-8")

            self.assertIn('{\n  "window": "5h",\n  "delta": [', text)
            self.assertIn('"accountSlotId":"unknown","accountLabel":"Unknown"', text)
            self.assertIn('{\n  "window": "7d",\n  "delta": [', text)
            self.assertEqual(text.count('"accountSlotId":"unknown"'), 2)
            self.assertEqual(load_history(path)[-1]["window"], "7d")
        finally:
            if path.exists():
                path.unlink()

    def test_write_history_marks_model_split_as_nested_delta(self):
        path = Path(__file__).with_name("test_history_tmp.jsonl")
        try:
            write_history(path, [
                {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "model": "gpt-5.5", "deltaPercent": 0.5, "deltaCostUsd": 2, "costPercentRatio": 4},
                {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "model": "gpt-5.6-sol", "deltaPercent": 1.5, "deltaCostUsd": 6, "costPercentRatio": 4},
            ])

            text = path.read_text(encoding="utf-8")
            rows = load_history(path)

            self.assertIn('"accountSlotId":"unknown","accountLabel":"Unknown","deltaPercent":2.0,"models":[{"model":"gpt-5.5","deltaCostUsd":2},{"model":"gpt-5.6-sol","deltaCostUsd":6}]', text)
            self.assertEqual([row["model"] for row in rows], ["gpt-5.5", "gpt-5.6-sol"])
            self.assertEqual([row["deltaPercent"] for row in rows], [0.5, 1.5])
            self.assertEqual([row["costPercentRatio"] for row in rows], [4, 4])
        finally:
            if path.exists():
                path.unlink()

    def test_load_history_accepts_legacy_model_split_array(self):
        path = Path(__file__).with_name("test_history_tmp.jsonl")
        try:
            path.write_text('{"window":"5h","delta":[[{"checkedAt":"2030-01-01T00:00:00Z","model":"gpt-5.5","deltaPercent":0.5,"deltaCostUsd":2,"costPercentRatio":4},{"checkedAt":"2030-01-01T00:00:00Z","model":"gpt-5.6-sol","deltaPercent":1.5,"deltaCostUsd":6,"costPercentRatio":4}]]}\n', encoding="utf-8")

            rows = load_history(path)

            self.assertEqual([row["deltaPercent"] for row in rows], [0.5, 1.5])
            self.assertEqual([row["model"] for row in rows], ["gpt-5.5", "gpt-5.6-sol"])
        finally:
            if path.exists():
                path.unlink()

    def test_load_history_accepts_legacy_events_group_name(self):
        path = Path(__file__).with_name("test_history_tmp.jsonl")
        try:
            path.write_text('{"window":"5h","events":[{"checkedAt":"2030-01-01T00:00:00Z","deltaPercent":1,"deltaCostUsd":2}]}\n', encoding="utf-8")

            rows = load_history(path)

            self.assertEqual(rows, [{"checkedAt": "2030-01-01T00:00:00Z", "deltaPercent": 1, "deltaCostUsd": 2, "window": "5h", "accountSlotId": "unknown", "accountLabel": "Unknown"}])
            self.assertEqual(derive_history_events(rows)["fiveHour"][1]["model"], "gpt-5.5")
        finally:
            if path.exists():
                path.unlink()

    def test_compact_history_keeps_grouped_indented_delta_events(self):
        path = Path(__file__).with_name("test_history_tmp.jsonl")
        try:
            write_history(path, [
                {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 1, "deltaCostUsd": 2},
                {"checkedAt": "2030-01-01T01:00:00Z", "window": "7d", "deltaPercent": 3, "deltaCostUsd": 4},
            ])

            compact_history(path, 999999)
            text = path.read_text(encoding="utf-8")
            events = derive_history_events(load_history(path))

            self.assertIn('{\n  "window": "5h",\n  "delta": [', text)
            self.assertIn('"accountSlotId":"unknown","accountLabel":"Unknown"', text)
            self.assertIn('{\n  "window": "7d",\n  "delta": [', text)
            self.assertEqual(text.count('"accountSlotId":"unknown"'), 2)
            self.assertEqual(events["fiveHour"][1]["deltaPercent"], 1)
            self.assertEqual(events["sevenDay"][1]["deltaPercent"], 3)
        finally:
            if path.exists():
                path.unlink()

    def test_client_disconnect_detection_matches_browser_abort_errors(self):
        self.assertTrue(is_client_disconnect(ConnectionAbortedError(10053, "connection aborted")))
        self.assertTrue(is_client_disconnect(ConnectionResetError(10054, "connection reset")))
        self.assertTrue(is_client_disconnect(BrokenPipeError(32, "broken pipe")))
        self.assertFalse(is_client_disconnect(RuntimeError("server bug")))

    def test_dashboard_server_suppresses_client_disconnect_tracebacks(self):
        server = DashboardHTTPServer.__new__(DashboardHTTPServer)
        with mock.patch.object(http.server.ThreadingHTTPServer, "handle_error") as default_handler:
            try:
                raise ConnectionAbortedError(10053, "connection aborted")
            except ConnectionAbortedError:
                server.handle_error(None, ("127.0.0.1", 12345))
        default_handler.assert_not_called()

    def test_dashboard_server_reports_unexpected_exceptions(self):
        server = DashboardHTTPServer.__new__(DashboardHTTPServer)
        with mock.patch.object(http.server.ThreadingHTTPServer, "handle_error") as default_handler:
            try:
                raise RuntimeError("server bug")
            except RuntimeError:
                server.handle_error(None, ("127.0.0.1", 12345))
        default_handler.assert_called_once_with(None, ("127.0.0.1", 12345))


    def test_skill_management_moves_codex_source_and_assigns_strict_link(self):
        with self.account_directory() as directory:
            codex_home, gemini, private = directory / "codex", directory / "gemini" / "config" / "skills", directory / "private"
            source = codex_home / "skills" / "demo"
            source.mkdir(parents=True)
            (source / "SKILL.md").write_text("demo", encoding="utf-8")
            manager = SkillManager(codex_home, private, gemini)

            result = manager.manage(["demo"])

            self.assertTrue(result["results"][0]["managed"])
            self.assertTrue(source.exists())
            self.assertEqual((private / "skills" / "demo" / "SKILL.md").read_text(encoding="utf-8"), "demo")
            self.assertEqual(manager.status()["items"][0]["projections"]["codex"]["state"], "linked")
            self.assertEqual(source.resolve(), (private / "skills" / "demo").resolve())
            self.assertFalse(manager.status()["items"][0]["assignments"]["gemini"])

    def test_skill_names_reject_shell_metacharacters_and_windows_aliases(self):
        for name in ("demo", "demo.skill-2", "A_1"):
            self.assertEqual(_safe_name(name), name)
        for name in ("evil&whoami", "evil|whoami", "../demo", ".hidden", " demo", "demo ", "demo.", "CON", "nul.txt", "COM1", "LPT9.log", "a" * 129):
            with self.subTest(name=name), self.assertRaises(SkillError):
                _safe_name(name)

    @unittest.skipUnless(os.name == "nt", "Windows junction fallback")
    def test_skill_assignment_uses_native_junction_when_symlink_is_denied(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            managed = private / "skills" / "demo"
            managed.mkdir(parents=True)
            (managed / "SKILL.md").write_text("managed", encoding="utf-8")
            manager = SkillManager(codex_home, private, directory / "gemini")

            with mock.patch("monitor_skills.os.symlink", side_effect=OSError("privilege not held")):
                manager.assign("demo", "codex", True)

            projection = codex_home / "skills" / "demo"
            self.assertTrue(projection.is_junction())
            self.assertEqual(projection.resolve(strict=True), managed.resolve(strict=True))

    def test_skill_snapshot_rejects_nonportable_skill_name(self):
        with self.account_directory() as directory:
            manager = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            buffer = BytesIO()
            manifest = {"version": 1, "skills": {"evil&whoami": [{"path": "SKILL.md", "size": 4, "sha256": hashlib.sha256(b"demo").hexdigest()}]}, "deletions": {}, "clearedDeletions": {}}
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("manifest.json", json.dumps(manifest))
                archive.writestr("skills/evil&whoami/SKILL.md", b"demo")

            with self.assertRaises(SkillError):
                manager.inspect_snapshot(buffer.getvalue())

    def test_skill_snapshot_rejects_paths_that_collide_or_fail_on_windows(self):
        with self.account_directory() as directory:
            manager = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            for paths in (("SKILL.md", "Docs/Readme.md", "docs/README.md"), ("SKILL.md", "nested/CON.txt"), ("SKILL.md", "trailing. ")):
                with self.subTest(paths=paths):
                    buffer = BytesIO()
                    manifest = {"version": 1, "skills": {"demo": [{"path": path, "size": 1, "sha256": hashlib.sha256(b"x").hexdigest()} for path in paths]}, "deletions": {}, "clearedDeletions": {}}
                    with zipfile.ZipFile(buffer, "w") as archive:
                        archive.writestr("manifest.json", json.dumps(manifest))
                        for path in paths:
                            archive.writestr(f"skills/demo/{path}", b"x")
                    with self.assertRaisesRegex(SkillError, "portable|collide"):
                        manager.inspect_snapshot(buffer.getvalue())

    def test_skill_snapshot_rejects_case_colliding_skill_names(self):
        with self.account_directory() as directory:
            manager = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            buffer = BytesIO()
            row = [{"path": "SKILL.md", "size": 1, "sha256": hashlib.sha256(b"x").hexdigest()}]
            manifest = {"version": 1, "skills": {"Alpha": row, "alpha": row}, "deletions": {}, "clearedDeletions": {}}
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("manifest.json", json.dumps(manifest))
                archive.writestr("skills/Alpha/SKILL.md", b"x")
                archive.writestr("skills/alpha/SKILL.md", b"x")

            with self.assertRaisesRegex(SkillError, "collide"):
                manager.inspect_snapshot(buffer.getvalue())

    def test_non_windows_projection_copy_fallback_refreshes_and_removes_owned_copy(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            managed = private / "skills" / "demo"
            managed.mkdir(parents=True)
            (managed / "SKILL.md").write_text("one", encoding="utf-8")
            manager = SkillManager(codex_home, private, directory / "gemini")
            manager._create_projection_copy("demo", "codex")
            manager.state["skills"]["demo"]["codex"] = True
            manager._save()

            projection = codex_home / "skills" / "demo"
            self.assertEqual(manager.status()["items"][0]["projections"]["codex"]["kind"], "copy")
            (managed / "SKILL.md").write_text("two", encoding="utf-8")
            manager._invalidate_status()
            self.assertEqual(manager.status()["items"][0]["projections"]["codex"]["state"], "linked")
            self.assertEqual((projection / "SKILL.md").read_text(encoding="utf-8"), "two")

            manager.assign("demo", "codex", False)

            self.assertFalse(projection.exists())
            self.assertNotIn("codexCopyToken", manager.state["skills"]["demo"])

    def test_non_windows_assignment_falls_back_to_owned_copy_when_symlink_is_denied(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            managed = private / "skills" / "demo"
            managed.mkdir(parents=True)
            (managed / "SKILL.md").write_text("managed", encoding="utf-8")
            manager = SkillManager(codex_home, private, directory / "gemini")

            with mock.patch("monitor_skills.os.symlink", side_effect=OSError("not supported")), mock.patch("monitor_skills.os.name", "posix"):
                manager.assign("demo", "codex", True)

            projection = manager.status()["items"][0]["projections"]["codex"]
            self.assertEqual(projection["state"], "linked")
            self.assertEqual(projection["kind"], "copy")
            self.assertEqual((codex_home / "skills" / "demo" / "SKILL.md").read_text(encoding="utf-8"), "managed")

    def test_python_version_requirement_is_documented_and_checked_before_monitor_imports(self):
        source = Path("monitor_codex_usage.py").read_text(encoding="utf-8")

        self.assertIn("if sys.version_info < (3, 12):", source)
        self.assertLess(source.index("if sys.version_info < (3, 12):"), source.index("from monitor_accounts import"))
        self.assertIn("Python 3.12 or newer on Windows and Linux", Path("README.md").read_text(encoding="utf-8"))

    def test_legacy_skill_state_is_normalized_with_empty_deletions(self):
        with self.account_directory() as directory:
            private = directory / "private"
            private.mkdir()
            (private / "skills.json").write_text(json.dumps({"version": 1, "skills": {}}), encoding="utf-8")

            manager = SkillManager(directory / "codex", private, directory / "gemini")

            self.assertEqual(manager.state["deletions"], {})
            self.assertEqual(json.loads((private / "skills.json").read_text(encoding="utf-8"))["deletions"], {})

    def test_skill_management_codex_wins_duplicate_and_assigns_both(self):
        with self.account_directory() as directory:
            codex_home, gemini, private = directory / "codex", directory / "gemini" / "config" / "skills", directory / "private"
            for root, text in ((codex_home / "skills", "codex"), (gemini, "gemini")):
                (root / "same").mkdir(parents=True)
                (root / "same" / "SKILL.md").write_text(text, encoding="utf-8")
            manager = SkillManager(codex_home, private, gemini)

            manager.manage(["same"])

            self.assertEqual((private / "skills" / "same" / "SKILL.md").read_text(encoding="utf-8"), "codex")
            self.assertEqual(manager.status()["items"][0]["projections"]["codex"]["state"], "linked")
            self.assertEqual(manager.status()["items"][0]["projections"]["gemini"]["state"], "linked")
            self.assertEqual(manager.status()["items"][0]["assignments"], {"codex": True, "gemini": True})

    def test_skill_unmanage_replaces_assigned_links_with_independent_copies(self):
        with self.account_directory() as directory:
            codex_home, gemini, private = directory / "codex", directory / "gemini" / "config" / "skills", directory / "private"
            source = codex_home / "skills" / "demo"
            source.mkdir(parents=True)
            (source / "SKILL.md").write_text("demo", encoding="utf-8")
            (source / "nested").mkdir()
            (source / "nested" / "value.txt").write_text("value", encoding="utf-8")
            manager = SkillManager(codex_home, private, gemini)
            manager.manage(["demo"])
            manager.assign("demo", "gemini", True)

            result = manager.unmanage("demo")

            self.assertTrue(result["unmanaged"])
            self.assertFalse((private / "skills" / "demo").exists())
            self.assertNotIn("demo", manager.state["skills"])
            for target in (codex_home / "skills" / "demo", gemini / "demo"):
                self.assertTrue(target.is_dir())
                self.assertFalse(target.is_symlink())
                self.assertFalse(getattr(target, "is_junction", lambda: False)())
                self.assertEqual((target / "nested" / "value.txt").read_text(encoding="utf-8"), "value")
                self.assertEqual(list(target.parent.glob(".demo.*")), [])
            (codex_home / "skills" / "demo" / "nested" / "value.txt").write_text("codex", encoding="utf-8")
            self.assertEqual((gemini / "demo" / "nested" / "value.txt").read_text(encoding="utf-8"), "value")

    def test_skill_unmanage_records_deletion_tombstone_in_snapshot(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            source = codex_home / "skills" / "demo"
            source.mkdir(parents=True)
            (source / "SKILL.md").write_text("demo", encoding="utf-8")
            manager = SkillManager(codex_home, private, directory / "gemini")
            manager.manage(["demo"])

            manager.unmanage("demo")

            manifest, _ = manager.inspect_snapshot(manager.snapshot())
            self.assertNotIn("demo", manifest["skills"])
            self.assertEqual(len(manifest["deletions"]["demo"]), 1)
            self.assertEqual(manifest["deletions"]["demo"][0]["id"], manager.state["deletions"]["demo"][0]["id"])

    def test_skill_fetch_tombstone_removes_stale_managed_source_and_link(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            stale = SkillManager(codex_home, private, directory / "gemini")
            managed = private / "skills" / "demo"
            managed.mkdir(parents=True)
            (managed / "SKILL.md").write_text("stale", encoding="utf-8")
            stale.assign("demo", "codex", True)
            remote = SkillManager(directory / "remote-codex", directory / "remote-private", directory / "remote-gemini")
            remote_skill = remote.skills_root / "demo"
            remote_skill.mkdir(parents=True)
            (remote_skill / "SKILL.md").write_text("remote", encoding="utf-8")
            remote.unmanage("demo")

            result = stale.merge(remote.snapshot())

            self.assertEqual(result["deleted"], ["demo"])
            self.assertFalse(managed.exists())
            self.assertFalse((codex_home / "skills" / "demo").exists())
            self.assertNotIn("demo", stale.state["skills"])
            self.assertEqual(stale.state["deletions"], remote.state["deletions"])

    def test_skill_push_tombstone_suppresses_stale_local_copy(self):
        with self.account_directory() as directory:
            stale = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            stale_skill = stale.skills_root / "demo"
            stale_skill.mkdir(parents=True)
            (stale_skill / "SKILL.md").write_text("stale", encoding="utf-8")
            remote = SkillManager(directory / "remote-codex", directory / "remote-private", directory / "remote-gemini")
            remote_skill = remote.skills_root / "demo"
            remote_skill.mkdir(parents=True)
            (remote_skill / "SKILL.md").write_text("remote", encoding="utf-8")
            remote.unmanage("demo")

            merged, added, updated, deleted = stale.snapshot_merged_with_remote(remote.snapshot(), stale.snapshot())

            manifest, _ = stale.inspect_snapshot(merged)
            self.assertNotIn("demo", manifest["skills"])
            self.assertEqual((added, updated, deleted), ([], [], []))
            self.assertEqual(manifest["deletions"], remote.state["deletions"])

    def test_cloud_unmanage_immediately_uploads_skill_tombstone(self):
        with self.account_directory() as directory:
            skills = mock.Mock()
            skills.unmanage.return_value = {"name": "demo", "unmanaged": True}
            cloud = CloudManager(directory / "private", skills, None)

            with mock.patch.object(cloud, "upload_skills", return_value={"changed": True, "deleted": ["demo"]}) as upload:
                result = cloud.unmanage_skill("demo")

            skills.unmanage.assert_called_once_with("demo")
            upload.assert_called_once_with({"demo"})
            self.assertEqual(result["cloud"]["deleted"], ["demo"])

    def test_explicit_manage_clears_known_tombstone_and_allows_readding_skill(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            manager = SkillManager(codex_home, private, directory / "gemini")
            manager.state["deletions"]["demo"] = [{"id": "deleted-revision", "deletedAt": "2026-01-01T00:00:00Z"}]
            manager._save()
            source = codex_home / "skills" / "demo"
            source.mkdir(parents=True)
            (source / "SKILL.md").write_text("intentional re-add", encoding="utf-8")

            manager.manage(["demo"])

            manifest, _ = manager.inspect_snapshot(manager.snapshot())
            self.assertEqual(manifest["clearedDeletions"]["demo"], ["deleted-revision"])
            merged, _, _, deleted = manager.snapshot_merged_with_remote(manager._build_snapshot({}, {}, manager.state["deletions"], {}), manager.snapshot())
            self.assertIn("demo", manager.inspect_snapshot(merged)[0]["skills"])
            self.assertEqual(deleted, [])

    def test_skill_unmanage_preserves_unrelated_projection_and_managed_source(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            managed = private / "skills" / "demo"
            managed.mkdir(parents=True)
            (managed / "SKILL.md").write_text("managed", encoding="utf-8")
            conflict = codex_home / "skills" / "demo"
            conflict.mkdir(parents=True)
            (conflict / "keep.txt").write_text("keep", encoding="utf-8")
            manager = SkillManager(codex_home, private, directory / "gemini")

            with self.assertRaises(SkillError):
                manager.unmanage("demo")

            self.assertEqual((conflict / "keep.txt").read_text(encoding="utf-8"), "keep")
            self.assertEqual((managed / "SKILL.md").read_text(encoding="utf-8"), "managed")

    def test_skill_assignment_replaces_unrelated_same_name_path(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            managed = private / "skills" / "demo"
            managed.mkdir(parents=True)
            (managed / "SKILL.md").write_text("managed", encoding="utf-8")
            conflict = codex_home / "skills" / "demo"
            conflict.mkdir(parents=True)
            (conflict / "keep.txt").write_text("keep", encoding="utf-8")
            manager = SkillManager(codex_home, private, directory / "gemini")

            manager.assign("demo", "codex", True)

            self.assertEqual(conflict.resolve(), managed.resolve())
            self.assertFalse((conflict / "keep.txt").exists())
            self.assertEqual((conflict / "SKILL.md").read_text(encoding="utf-8"), "managed")

    def test_skill_assignment_replaces_same_name_local_skill_with_managed_link(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            managed = private / "skills" / "demo"
            managed.mkdir(parents=True)
            (managed / "SKILL.md").write_text("managed", encoding="utf-8")
            local = codex_home / "skills" / "demo"
            local.mkdir(parents=True)
            (local / "SKILL.md").write_text("local", encoding="utf-8")
            manager = SkillManager(codex_home, private, directory / "gemini")

            manager.assign("demo", "codex", True)

            self.assertEqual(local.resolve(), managed.resolve())
            self.assertEqual((local / "SKILL.md").read_text(encoding="utf-8"), "managed")
            self.assertTrue(manager.state["skills"]["demo"]["codex"])
            self.assertEqual(manager.status()["items"][0]["projections"]["codex"]["state"], "linked")

    def test_skill_reconcile_leaves_same_name_local_skill_as_unassigned_conflict(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            managed = private / "skills" / "demo"
            managed.mkdir(parents=True)
            (managed / "SKILL.md").write_text("managed", encoding="utf-8")
            local = codex_home / "skills" / "demo"
            local.mkdir(parents=True)
            (local / "SKILL.md").write_text("local", encoding="utf-8")
            manager = SkillManager(codex_home, private, directory / "gemini")
            manager.state["skills"]["demo"] = {"codex": True, "gemini": False, "managedAt": None, "codexError": "stale projection error"}
            manager._save()

            self.assertEqual(manager.reconcile(), [])

            self.assertNotEqual(local.resolve(), managed.resolve())
            self.assertEqual((local / "SKILL.md").read_text(encoding="utf-8"), "local")
            self.assertFalse(manager.state["skills"]["demo"]["codex"])
            self.assertEqual(manager.status()["items"][0]["projections"]["codex"]["state"], "conflict")
            self.assertEqual(manager.status()["items"][0]["errors"], {})

    def test_skill_merge_applies_remote_then_managed_then_local_precedence(self):
        with self.account_directory() as directory:
            codex_home, private = directory / "codex", directory / "private"
            manager = SkillManager(codex_home, private, directory / "gemini")
            for name, content in (("updated", "managed-old"), ("managed-only", "keep")):
                target = private / "skills" / name
                target.mkdir(parents=True)
                (target / "SKILL.md").write_text(content, encoding="utf-8")
            local = codex_home / "skills" / "updated"
            local.mkdir(parents=True)
            (local / "SKILL.md").write_text("local", encoding="utf-8")
            remote = SkillManager(directory / "remote-codex", directory / "remote-private", directory / "remote-gemini")
            for name, content in (("updated", "webdav"), ("new", "new-webdav")):
                target = remote.skills_root / name
                target.mkdir(parents=True)
                (target / "SKILL.md").write_text(content, encoding="utf-8")

            result = manager.merge(remote.snapshot())

            self.assertEqual(result["added"], ["new"])
            self.assertEqual(result["updated"], ["updated"])
            self.assertEqual((private / "skills" / "updated" / "SKILL.md").read_text(encoding="utf-8"), "webdav")
            self.assertEqual((private / "skills" / "managed-only" / "SKILL.md").read_text(encoding="utf-8"), "keep")
            self.assertFalse((codex_home / "skills" / "new").exists())
            self.assertNotEqual(local.resolve(), (private / "skills" / "updated").resolve())
            self.assertEqual((local / "SKILL.md").read_text(encoding="utf-8"), "local")
            self.assertFalse(manager.state["skills"]["updated"]["codex"])
            self.assertFalse(manager.state["skills"]["new"]["codex"])

    def test_skill_snapshot_rejects_traversal_and_preserves_assignments_outside_payload(self):
        with self.account_directory() as directory:
            manager = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            buffer = BytesIO()
            manifest = {"version": 1, "skills": {"demo": [{"path": "../escape", "size": 1, "sha256": "x"}]}}
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("manifest.json", json.dumps(manifest))
                archive.writestr("skills/demo/../escape", b"x")
            with self.assertRaises(SkillError):
                manager.inspect_snapshot(buffer.getvalue())
            self.assertNotIn(b"assignments", manager.snapshot())

    def test_skill_snapshot_hash_matches_managed_content_hash(self):
        with self.account_directory() as directory:
            manager = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            skill = manager.skills_root / "demo"
            skill.mkdir(parents=True)
            (skill / "SKILL.md").write_text("demo", encoding="utf-8")

            self.assertEqual(manager.snapshot_content_hash(manager.snapshot()), manager.content_hash())

    def test_skill_content_hash_detects_directly_added_managed_skill_after_status_cache(self):
        with self.account_directory() as directory:
            manager = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            initial_hash = manager.content_hash()
            skill = manager.skills_root / "added"
            skill.mkdir(parents=True)
            (skill / "SKILL.md").write_text("added", encoding="utf-8")

            self.assertNotEqual(manager.content_hash(), initial_hash)

    def test_skill_direct_hashes_match_packages_without_building_zips(self):
        with self.account_directory() as directory:
            manager = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            skill = manager.skills_root / "demo"
            skill.mkdir(parents=True)
            (skill / "SKILL.md").write_text("demo", encoding="utf-8")
            manager.status()
            manager.state["skills"]["demo"]["clearedDeletions"] = ["old-delete"]
            manager.state["deletions"] = {
                "demo": [{"id": "old-delete", "deletedAt": "2026-01-01T00:00:00Z"}],
                "removed": [{"id": "removed-delete", "deletedAt": "2026-01-02T00:00:00Z"}],
            }

            with mock.patch.object(manager, "skill_snapshots", side_effect=AssertionError("ZIP path used for change detection")):
                direct = manager.content_hashes()
            packages = manager.skill_snapshots()

            self.assertEqual(direct, {name: manager.skill_package_hash(package) for name, package in packages.items()})

    def test_skill_manifest_cache_reads_only_changed_files(self):
        with self.account_directory() as directory:
            manager = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            skill = manager.skills_root / "demo"
            skill.mkdir(parents=True)
            (skill / "SKILL.md").write_text("demo", encoding="utf-8")
            payload = skill / "payload.txt"
            payload.write_text("initial", encoding="utf-8")
            original = manager.content_hashes()
            original_read_bytes = Path.read_bytes
            reads = []

            def track_read(path):
                reads.append(path.resolve())
                return original_read_bytes(path)

            with mock.patch.object(Path, "read_bytes", new=track_read):
                self.assertEqual(manager.content_hashes(), original)
                self.assertEqual(reads, [])
                payload.write_text("modified-content", encoding="utf-8")
                changed = manager.content_hashes()

            self.assertNotEqual(changed, original)
            self.assertEqual(reads, [payload.resolve()])

    def test_skill_manifest_cache_performs_bounded_full_rehash(self):
        with self.account_directory() as directory:
            manager = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            skill = manager.skills_root / "demo"
            skill.mkdir(parents=True)
            (skill / "SKILL.md").write_text("demo", encoding="utf-8")
            (skill / "payload.txt").write_text("payload", encoding="utf-8")
            with mock.patch("monitor_skills.time.monotonic", return_value=0):
                expected = manager.content_hashes()
            original_read_bytes = Path.read_bytes
            reads = []

            def track_read(path):
                reads.append(path.resolve())
                return original_read_bytes(path)

            with mock.patch.object(Path, "read_bytes", new=track_read), mock.patch("monitor_skills.time.monotonic", return_value=MANIFEST_FULL_REHASH_SECONDS - 1):
                self.assertEqual(manager.content_hashes(), expected)
            self.assertEqual(reads, [])
            with mock.patch.object(Path, "read_bytes", new=track_read), mock.patch("monitor_skills.time.monotonic", return_value=MANIFEST_FULL_REHASH_SECONDS):
                self.assertEqual(manager.content_hashes(), expected)
            self.assertEqual(set(reads), {(skill / "SKILL.md").resolve(), (skill / "payload.txt").resolve()})

    def test_skill_snapshot_rejects_tree_change_during_packaging(self):
        with self.account_directory() as directory:
            manager = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            skill = manager.skills_root / "demo"
            skill.mkdir(parents=True)
            (skill / "SKILL.md").write_text("demo", encoding="utf-8")
            manifest = manager._tree_manifest_cached(skill)
            original_read_bytes = Path.read_bytes
            changed = False

            def change_tree_after_read(path):
                nonlocal changed
                data = original_read_bytes(path)
                if not changed:
                    changed = True
                    (skill / "added.txt").write_text("added", encoding="utf-8")
                return data

            with mock.patch.object(Path, "read_bytes", new=change_tree_after_read), self.assertRaises(SkillError) as raised:
                manager._snapshot_files(skill, manifest)
            self.assertEqual(raised.exception.status, 409)

    def test_crypto_box_authenticates_purpose_passphrase_and_completion(self):
        correct_hash = passphrase_hash("correct horse", "https://dav.example/", "user")
        descriptor = CryptoBox.descriptor(correct_hash)
        box = CryptoBox(correct_hash, descriptor)
        payload = box.encrypt("skills", b"private content")

        self.assertEqual(box.decrypt("skills", payload), b"private content")
        with self.assertRaises(CloudError):
            CryptoBox(passphrase_hash("wrong", "https://dav.example/", "user"), descriptor)
        with self.assertRaises(CloudError):
            box.decrypt("accounts", payload)
        with self.assertRaises(CloudError):
            box.decrypt("skills", payload[:-1])

    def test_sensitive_request_headers_are_not_copied_to_redirects(self):
        class Response:
            status = 200
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            @staticmethod
            def read():
                return b"{}"

        class Opener:
            request = None

            def open(self, request, timeout=None):
                self.request = request
                return Response()

        opener = Opener()
        monitor_common.request_json(opener, "GET", "https://origin.example/usage", {"Authorization": "Bearer secret", "ChatGPT-Account-Id": "account-secret", "Accept": "application/json"})
        handler = monitor_common.SafeRedirectHandler()
        redirected = handler.redirect_request(opener.request, None, 302, "Found", {}, "https://origin.example/other")
        self.assertEqual({key.lower() for key in opener.request.unredirected_hdrs}, {"authorization", "chatgpt-account-id"})
        self.assertFalse({"authorization", "chatgpt-account-id"} & {key.lower() for key, _ in redirected.header_items()})
        with self.assertRaises(urllib.error.HTTPError) as raised:
            handler.redirect_request(opener.request, None, 302, "Found", {}, "https://other.example/usage")
        raised.exception.close()

        webdav = WebDavClient({"baseUrl": "https://dav.example/", "remoteRoot": "root", "username": "user", "password": "secret"})
        webdav.opener = Opener()
        webdav.request("GET", "item")
        redirected = handler.redirect_request(webdav.opener.request, None, 302, "Found", {}, "https://dav.example/other")
        self.assertIn("authorization", {key.lower() for key in webdav.opener.request.unredirected_hdrs})
        self.assertNotIn("authorization", {key.lower() for key, _ in redirected.header_items()})

    def test_sensitive_redirect_handler_rejects_https_downgrade(self):
        request = urllib.request.Request("https://origin.example/usage")

        with self.assertRaises(urllib.error.HTTPError) as raised:
            monitor_common.SafeRedirectHandler().redirect_request(request, None, 302, "Found", {}, "http://origin.example/usage")
        raised.exception.close()

    def test_webdav_requires_https_except_literal_loopback(self):
        with self.assertRaises(CloudError):
            WebDavClient({"baseUrl": "http://dav.example/", "username": "user", "password": "secret"})
        for url in ("http://localhost:8080/", "http://127.0.0.1:8080/", "http://[::1]:8080/"):
            with self.subTest(url=url):
                self.assertEqual(WebDavClient({"baseUrl": url, "username": "user", "password": "secret"}).base, url)

    def test_webdav_list_method_does_not_shadow_annotation_builtin(self):
        self.assertEqual(WebDavClient.list_details.__annotations__["return"], "list[dict]")

    def test_cloud_crypto_initialization_requires_exact_http_404(self):
        manager = object.__new__(CloudManager)
        manager.config = mock.Mock(return_value={"webdav": {"enabled": True, "baseUrl": "https://dav.example/", "username": "user", "password": "secret", "encryptionPassphraseHash": passphrase_hash("passphrase", "https://dav.example/", "user")}})
        failures = (
            CloudError("network", 502, category="network"),
            CloudError("unauthorized", 502, http_status=401, category="http"),
            CloudError("server error", 502, http_status=500, category="http"),
            CloudError("protocol", 502, http_status=404, category="protocol"),
        )
        for failure in failures:
            client = SimpleNamespace(ensure_directories=mock.Mock(), get=mock.Mock(side_effect=failure), put=mock.Mock())
            with self.subTest(category=failure.category, http_status=failure.http_status), mock.patch("monitor_cloud.WebDavClient", return_value=client), self.assertRaises(CloudError):
                manager._connection(True)
            client.put.assert_not_called()

    def test_cloud_crypto_initialization_verifies_readback_and_conditional_create(self):
        manager = object.__new__(CloudManager)
        manager.config = mock.Mock(return_value={"webdav": {"enabled": True, "baseUrl": "https://dav.example/", "username": "user", "password": "secret", "encryptionPassphraseHash": passphrase_hash("passphrase", "https://dav.example/", "user")}})

        class Client:
            def __init__(self, enforce_condition=True):
                self.data = None
                self.enforce_condition = enforce_condition
                self.put_count = 0

            def ensure_directories(self, _path):
                pass

            def get(self, _path):
                if self.data is None:
                    raise CloudError("missing", 502, http_status=404, category="http")
                return self.data, '"etag"'

            def put(self, _path, data, create=False):
                self.put_count += 1
                if self.data is not None and create and self.enforce_condition:
                    raise CloudError("exists", 409, http_status=412, category="http")
                self.data = data
                return '"etag"'

        client = Client()
        with mock.patch("monitor_cloud.WebDavClient", return_value=client), mock.patch("monitor_cloud.CryptoBox") as crypto:
            crypto.descriptor.return_value = {"version": 1}
            connected_client, connected_box = manager._connection(True)
        self.assertIs(connected_client, client)
        self.assertIs(connected_box, crypto.return_value)
        self.assertEqual(client.put_count, 2)

        client = Client(enforce_condition=False)
        with mock.patch("monitor_cloud.WebDavClient", return_value=client), mock.patch("monitor_cloud.CryptoBox") as crypto:
            crypto.descriptor.return_value = {"version": 1}
            with self.assertRaisesRegex(CloudError, "ignored conditional"):
                manager._connection(True)

    def test_webdav_test_decrypts_existing_payload_before_protocol_writes(self):
        manager = object.__new__(CloudManager)
        manager._operation_lock = threading.RLock()
        manager._state = {"conditionalWritesVerified": False}
        manager._save_state = mock.Mock()
        manager.config = mock.Mock(return_value={"webdav": {"allowOptimisticWrites": True}})
        client = mock.Mock()
        client.get.side_effect = [(b"encrypted", '"payload-etag"'), (b"first", '"first-etag"'), (b"updated", '"updated-etag"')]
        client.put.side_effect = ['"first-etag"', CloudError("exists", 409, http_status=412, category="http"), CloudError("mismatch", 409, http_status=412, category="http"), '"updated-etag"']
        box = mock.Mock()
        with mock.patch.object(manager, "_connection", return_value=(client, box)), mock.patch.object(manager, "_encrypted_inventory", return_value=[("skills/packages/package.enc", "skill-package:package")]):
            result = manager.test()

        box.decrypt.assert_called_once_with("skill-package:package", b"encrypted")
        self.assertTrue(result["encryptedPayloadVerified"])
        self.assertEqual([check["status"] for check in result["checks"]], ["passed", "passed", "passed", "passed", "passed", "passed"])
        self.assertEqual(result["checks"][0]["name"], "Connection and account verification")
        self.assertEqual(result["checks"][1]["name"], "Encrypted data decryption")
        manager._save_state.assert_called_once_with()

    def test_webdav_test_rejects_existing_payload_that_cannot_be_decrypted(self):
        manager = object.__new__(CloudManager)
        manager._operation_lock = threading.RLock()
        client, box = mock.Mock(), mock.Mock()
        client.get.return_value = b"encrypted", '"payload-etag"'
        box.decrypt.side_effect = CloudError("Encrypted payload authentication failed", 409)
        with mock.patch.object(manager, "_connection", return_value=(client, box)), mock.patch.object(manager, "_encrypted_inventory", return_value=[("skills/packages/package.enc", "skill-package:package")]), self.assertRaisesRegex(CloudError, "authentication failed") as caught:
            manager.test()

        client.put.assert_not_called()
        self.assertEqual(caught.exception.details[-1]["name"], "Encrypted data decryption")
        self.assertEqual(caught.exception.details[-1]["status"], "failed")

    def test_webdav_test_allows_empty_encrypted_inventory(self):
        manager = object.__new__(CloudManager)
        manager._operation_lock = threading.RLock()
        manager._state = {"conditionalWritesVerified": False}
        manager._save_state = mock.Mock()
        manager.config = mock.Mock(return_value={"webdav": {"allowOptimisticWrites": True}})
        client, box = mock.Mock(), mock.Mock()
        client.get.side_effect = [(b"first", '"first-etag"'), (b"updated", '"updated-etag"')]
        client.put.side_effect = ['"first-etag"', CloudError("exists", 409, http_status=412, category="http"), CloudError("mismatch", 409, http_status=412, category="http"), '"updated-etag"']
        with mock.patch.object(manager, "_connection", return_value=(client, box)), mock.patch.object(manager, "_encrypted_inventory", return_value=[]):
            result = manager.test()

        box.decrypt.assert_not_called()
        self.assertFalse(result["encryptedPayloadVerified"])
        self.assertEqual(result["checks"][1], {"name": "Encrypted data decryption", "status": "skipped", "detail": "No encrypted cloud payload exists yet."})

    def test_sync_operations_reject_unbounded_or_inconsistent_remote_records(self):
        row = {"sessionId": "session", "tokens": {}, "byModel": {"<img src=x onerror=alert(1)>": {}}, "sync": {"recordId": "record", "originMachineId": "machine", "accountId": "account"}}
        operation = {"action": "upsert", "key": record_key("token", row), "record": {"kind": "token", "row": row}}
        validate_sync_operation(operation)

        invalid = [
            operation | {"key": "token:wrong"},
            {"action": "upsert", "key": operation["key"], "record": {"kind": "token", "row": row | {"accountLabel": "x" * 4097}}},
            {"action": "upsert", "key": operation["key"], "record": {"kind": "token", "row": row | {"tokens": {"inputTokens": float("inf")}}}},
            {"action": "upsert", "key": operation["key"], "record": {"kind": "token", "row": row | {"byModel": {f"model-{index}": "x" * 100 for index in range(3000)}}}},
        ]
        nested = {}
        cursor = nested
        for _ in range(14):
            cursor["next"] = {}
            cursor = cursor["next"]
        invalid.append({"action": "upsert", "key": operation["key"], "record": {"kind": "token", "row": row | {"nested": nested}}})
        for candidate in invalid:
            with self.subTest(candidate=list(candidate)), self.assertRaises(ValueError):
                validate_sync_operation(candidate)

    def test_webdav_put_uses_verified_get_etag_when_put_omits_it(self):
        client = object.__new__(WebDavClient)
        client.request = mock.Mock(return_value=(b"", None, 201))
        client.get = mock.Mock(return_value=(b"content", '"strong-etag"'))

        self.assertEqual(client.put("test.bin", b"content", create=True), '"strong-etag"')
        client.get.assert_called_once_with("test.bin")

    def test_webdav_conditional_get_reuses_unchanged_etag(self):
        client = object.__new__(WebDavClient)
        client.request = mock.Mock(return_value=(b"", None, 304))

        self.assertEqual(client.get_if_changed("skills/current.enc", '"current"'), (None, '"current"'))
        client.request.assert_called_once_with("GET", "skills/current.enc", headers={"If-None-Match": '"current"'}, expected=(200, 304))

    def test_webdav_delete_accepts_missing_payload(self):
        client = object.__new__(WebDavClient)
        client.request = mock.Mock(return_value=(b"", None, 404))

        client.delete("skills/snapshots/old.enc")

        client.request.assert_called_once_with("DELETE", "skills/snapshots/old.enc", expected=(200, 204, 404))

    def test_webdav_conditional_delete_uses_if_match(self):
        client = object.__new__(WebDavClient)
        client.request = mock.Mock(return_value=(b"", None, 204))

        client.delete("accounts/states/key.enc", etag='"state-etag"')

        client.request.assert_called_once_with("DELETE", "accounts/states/key.enc", headers={"If-Match": '"state-etag"'}, expected=(200, 204, 404))

    def test_cloud_account_payload_delete_verifies_state_then_removes_revisions(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)
            key = "account-key"

            class Client:
                def __init__(self):
                    self.state = json.dumps({"version": 1, "accountKey": key, "boundMachineId": cloud.machine_id}).encode()
                    self.revisions = {"old.enc", "current.enc"}
                    self.deleted = []

                def get(self, path):
                    if path == f"accounts/states/{key}.enc" and self.state is not None:
                        return self.state, '"state-etag"'
                    raise CloudError("WebDAV GET failed with HTTP 404", 502)

                def list(self, path):
                    if path == f"accounts/revisions/{key}":
                        return sorted(self.revisions)
                    return []

                def delete(self, path, etag=None):
                    self.deleted.append((path, etag))
                    if path == f"accounts/states/{key}.enc":
                        self.state = None
                    elif path.startswith(f"accounts/revisions/{key}/"):
                        self.revisions.discard(path.rsplit("/", 1)[1])

            client = Client()
            box = SimpleNamespace(decrypt=lambda _purpose, payload, *_args: payload)
            cloud._state["remote"]["accounts"][key] = {"state": {"accountKey": key}}
            with mock.patch.object(cloud, "_connection", return_value=(client, box)):
                result = cloud.delete_account_payloads(key)

            self.assertEqual(result, {"accountKey": key, "deleted": True})
            self.assertEqual(client.deleted[0], (f"accounts/states/{key}.enc", '"state-etag"'))
            self.assertIn((f"accounts/revisions/{key}/old.enc", None), client.deleted)
            self.assertIn((f"accounts/revisions/{key}/current.enc", None), client.deleted)
            self.assertNotIn(key, cloud._state["remote"]["accounts"])

    def test_skill_upload_removes_only_preexisting_unreferenced_packages(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            skill = skills.skills_root / "demo"
            skill.mkdir(parents=True)
            (skill / "SKILL.md").write_text("demo", encoding="utf-8")
            cloud = CloudManager(directory / "private", skills, None)

            class Client:
                def __init__(self):
                    self.files = {"skills/packages/old.enc": b"old", "skills/packages/older.enc": b"older"}
                    self.deleted = []

                def ensure_directories(self, _path):
                    pass

                def list(self, path):
                    return ["old.enc", "older.enc"] if path == "skills/packages" else []

                def put(self, path, data, **_kwargs):
                    self.files[path] = data
                    if path == "skills/current.enc":
                        self.files["skills/packages/concurrent.enc"] = b"concurrent"
                    return '"etag"'

                def get(self, path):
                    if path not in self.files:
                        raise CloudError("WebDAV GET failed with HTTP 404", 502)
                    return self.files[path], '"etag"'

                def delete(self, path):
                    self.deleted.append(path)
                    self.files.pop(path, None)

            client = Client()
            box = SimpleNamespace(encrypt=lambda _purpose, payload: payload, decrypt=lambda _purpose, payload: payload)
            with mock.patch.object(cloud, "_connection", return_value=(client, box)):
                result = cloud.upload_skills()

            self.assertEqual(set(client.deleted), {"skills/packages/old.enc", "skills/packages/older.enc"})
            self.assertIn(f"skills/packages/{next(iter(result['localSha256'].values()))}.enc", client.files)
            self.assertIn("skills/packages/concurrent.enc", client.files)

    def test_account_revision_cleanup_preserves_authoritative_revision_and_is_best_effort(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            key = "account-key"
            state = json.dumps({"version": 1, "accountKey": key, "revisionId": "current"}).encode()

            class Client:
                def __init__(self):
                    self.deleted = []

                def get(self, _path):
                    return state, '"etag"'

                def delete(self, path):
                    self.deleted.append(path)
                    if path.endswith("old.enc"):
                        raise CloudError("cleanup failed", 502)

            client = Client()
            cloud._cleanup_account_revisions(client, SimpleNamespace(decrypt=lambda _purpose, payload, *_args: payload), key, {"old", "current", "older"})

            self.assertEqual(set(client.deleted), {f"accounts/revisions/{key}/old.enc", f"accounts/revisions/{key}/older.enc"})

    def test_cloud_fetch_downloads_only_changed_remote_metadata(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            local = json.loads(cloud.state_path.read_text(encoding="utf-8"))
            local["remote"] = {
                "accounts": {"same": {"etag": '"same"', "state": {"version": 1, "accountKey": "same", "label": "Unchanged"}}, "removed": {"etag": '"removed"', "state": {"version": 1, "accountKey": "removed"}}},
                "skills": {"pointerEtag": '"old-pointer"', "snapshotId": "old"}
            }
            cloud.state_path.write_text(json.dumps(local), encoding="utf-8")
            cloud._state = local

            class Client:
                def __init__(self):
                    self.downloads = []

                def ensure_directories(self, path):
                    self.path = path

                def list_details(self, path):
                    return [{"name": "same.enc", "etag": '"same"'}, {"name": "changed.enc", "etag": '"changed"'}]

                def get(self, path):
                    self.downloads.append(path)
                    if path.startswith("skills/snapshots/"):
                        return b"snapshot", '"snapshot"'
                    return json.dumps({"version": 1, "accountKey": "changed", "label": "Changed", "boundMachineId": None}).encode(), '"changed"'

                def get_if_changed(self, path, etag):
                    return json.dumps({"version": 1, "snapshotId": "new", "updatedAt": "now"}).encode(), '"new-pointer"'

            client = Client()
            box = SimpleNamespace(decrypt=lambda _purpose, payload, *_args: payload)
            usage = {"machinesChanged": 1, "payloadsDownloaded": 2, "conflicts": 0, "fetchedAt": "now"}
            with mock.patch.object(cloud, "_connection", return_value=(client, box)), mock.patch.object(cloud, "_fetch_usage_data", return_value=usage) as fetch_usage, mock.patch.object(
                skills, "merge", return_value={"added": ["new-skill"], "updated": ["updated-skill"], "projectionErrors": []}
            ), mock.patch.object(skills, "snapshot_content_hash", return_value="remote-hash"), mock.patch.object(skills, "content_hash", return_value="merged-hash"):
                result = cloud.fetch()

            saved = json.loads(cloud.state_path.read_text(encoding="utf-8"))["remote"]
            self.assertEqual(client.downloads, ["accounts/states/changed.enc", "skills/snapshots/new.enc"])
            self.assertEqual(result, {
                "accountsChanged": 1, "accountsRemoved": 1, "skillsChanged": True, "skillsAdded": ["new-skill"], "skillsUpdated": ["updated-skill"], "skillsDeleted": [],
                "accountsUpdated": 0, "usage": usage, "projectionErrors": [], "fetchedAt": saved["fetchedAt"]
            })
            fetch_usage.assert_called_once_with(client, box)
            self.assertEqual(set(saved["accounts"]), {"same", "changed"})
            self.assertEqual(saved["skills"]["indexId"], "new")
            self.assertEqual(cloud._state["skills"]["localSha256"], {})

    def test_cloud_fetch_skips_unchanged_version_two_packages(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            content_hash = "a" * 64
            cloud._state["remote"]["skills"] = {"version": 2, "indexEtag": '"current"', "indexId": "index", "packages": {"alpha": {"packageId": content_hash, "contentSha256": content_hash}}}

            class Client:
                def __init__(self):
                    self.listed = False

                def ensure_directories(self, _path):
                    pass

                def list_details(self, _path):
                    self.listed = True
                    return []

                def get_if_changed(self, _path, etag):
                    return None, etag

            client = Client()
            with mock.patch.object(cloud, "_connection", return_value=(client, SimpleNamespace())), mock.patch.object(skills, "content_hashes", return_value={"alpha": content_hash}), mock.patch.object(cloud, "_download_skill_snapshot") as download, mock.patch.object(skills, "merge") as merge:
                result = cloud.fetch()

            self.assertTrue(client.listed)
            download.assert_not_called()
            merge.assert_not_called()
            self.assertFalse(result["skillsChanged"])
            self.assertEqual(cloud._state["skills"]["indexEtag"], '"current"')

    def test_cloud_fetch_records_changed_pointer_when_local_packages_already_match(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            content_hash = "a" * 64
            cloud._state["remote"]["skills"] = {"version": 2, "indexEtag": '"old"', "indexId": "old-index", "packages": {}}
            new_index = {"version": 2, "indexEtag": '"new"', "indexId": "new-index", "packages": {"alpha": {"packageId": content_hash, "contentSha256": content_hash}}}

            class Client:
                def ensure_directories(self, _path):
                    pass

                def list_details(self, _path):
                    return []

                def get_if_changed(self, _path, _etag):
                    return b"new-pointer", '"new"'

            with mock.patch.object(cloud, "_connection", return_value=(Client(), SimpleNamespace())), mock.patch.object(cloud, "_parse_skill_pointer", return_value=new_index), mock.patch.object(skills, "content_hashes", return_value={"alpha": content_hash}), mock.patch.object(cloud, "_download_skill_snapshot") as download, mock.patch.object(skills, "merge") as merge:
                result = cloud.fetch()

            download.assert_not_called()
            merge.assert_not_called()
            self.assertTrue(result["skillsChanged"])
            self.assertEqual(cloud._state["skills"]["indexId"], "new-index")
            self.assertEqual(cloud._state["remote"]["skills"], new_index)

    def test_cloud_fetch_downloads_only_mismatched_version_two_packages(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            alpha_hash, beta_hash = "a" * 64, "b" * 64
            index = {
                "version": 2, "indexEtag": '"current"', "indexId": "index",
                "packages": {"alpha": {"packageId": alpha_hash, "contentSha256": alpha_hash}, "beta": {"packageId": beta_hash, "contentSha256": beta_hash}},
            }
            cloud._state["remote"]["skills"] = index

            class Client:
                def ensure_directories(self, _path):
                    pass

                def list_details(self, _path):
                    return []

                def get_if_changed(self, _path, etag):
                    return None, etag

            def content_hashes(names=None):
                return {"alpha": alpha_hash, "beta": "local-beta"} if names is not None else {"alpha": alpha_hash, "beta": beta_hash}

            with mock.patch.object(cloud, "_connection", return_value=(Client(), SimpleNamespace())), mock.patch.object(skills, "content_hashes", side_effect=content_hashes), mock.patch.object(cloud, "_download_skill_snapshot", return_value=b"beta") as download, mock.patch.object(skills, "merge", return_value={"added": [], "updated": ["beta"], "deleted": [], "projectionErrors": []}) as merge:
                result = cloud.fetch()

            download.assert_called_once_with(mock.ANY, mock.ANY, index, {"beta"})
            merge.assert_called_once_with(b"beta")
            self.assertEqual(result["skillsUpdated"], ["beta"])

    def test_cloud_fetch_skips_unchanged_legacy_snapshot_with_applied_baseline(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            cloud._state["remote"]["skills"] = {"version": 1, "indexEtag": '"current"', "indexId": "snapshot", "legacySnapshotId": "snapshot"}
            cloud._state["skills"] = {"indexEtag": '"current"', "indexId": "snapshot", "localSha256": {"demo": "base"}}

            class Client:
                def ensure_directories(self, _path):
                    pass

                def list_details(self, _path):
                    return []

                def get_if_changed(self, _path, etag):
                    return None, etag

            with mock.patch.object(cloud, "_connection", return_value=(Client(), SimpleNamespace())), mock.patch.object(skills, "content_hashes", return_value={"demo": "base"}), mock.patch.object(cloud, "_download_skill_snapshot") as download, mock.patch.object(skills, "merge") as merge:
                result = cloud.fetch()

            download.assert_not_called()
            merge.assert_not_called()
            self.assertFalse(result["skillsChanged"])

    def test_cloud_fetch_reapplies_cached_webdav_snapshot_over_local_managed_changes(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            cloud._state["remote"]["skills"] = {"version": 1, "indexEtag": '"current"', "indexId": "snapshot", "legacySnapshotId": "snapshot", "updatedAt": "then"}
            cloud._state["skills"] = {"indexEtag": '"current"', "indexId": "snapshot", "localSha256": {"demo": "base"}}

            class Client:
                def ensure_directories(self, _path):
                    pass

                def list_details(self, _path):
                    return []

                def get_if_changed(self, _path, etag):
                    return None, etag

                def get(self, path):
                    self.path = path
                    return b"snapshot", '"snapshot-etag"'

            client = Client()
            box = SimpleNamespace(decrypt=lambda _purpose, payload, *_args: payload)
            with mock.patch.object(cloud, "_connection", return_value=(client, box)), mock.patch.object(
                skills, "merge", return_value={"added": [], "updated": ["demo"], "projectionErrors": []}
            ) as merge, mock.patch.object(skills, "content_hashes", side_effect=({"demo": "modified"}, {"demo": "base"}, {"demo": "base"})):
                result = cloud.fetch()

            merge.assert_called_once_with(b"snapshot")
            self.assertEqual(client.path, "skills/snapshots/snapshot.enc")
            self.assertTrue(result["skillsChanged"])
            self.assertEqual(result["skillsUpdated"], ["demo"])

    def test_cloud_fetch_lists_accounts_without_downloading_or_uploading_them(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            accounts = SimpleNamespace(overwrite_from_cloud=mock.Mock(return_value={"localWinners": ["local-id"]}), push_bound_accounts=mock.Mock(return_value=[]))
            cloud = CloudManager(directory / "private", skills, accounts)
            state = {"version": 1, "accountKey": "owned", "label": "Owned", "boundMachineId": cloud.machine_id, "revisionId": "revision"}

            class Client:
                def ensure_directories(self, _path):
                    pass

                def list_details(self, _path):
                    return [{"name": "owned.enc", "etag": '"owned-etag"'}]

                def get(self, _path):
                    return json.dumps(state).encode(), '"owned-etag"'

                def get_if_changed(self, _path, _etag):
                    raise CloudError("WebDAV GET failed with HTTP 404", 502)

            box = SimpleNamespace(decrypt=lambda _purpose, payload, *_args: payload)
            with mock.patch.object(cloud, "_connection", return_value=(Client(), box)), mock.patch.object(cloud, "_download_account_revision_with", return_value=b"remote-auth"):
                result = cloud.fetch()

            accounts.overwrite_from_cloud.assert_not_called()
            accounts.push_bound_accounts.assert_not_called()
            self.assertEqual(result["accountsUpdated"], 0)

    def test_cloud_push_uploads_skills_and_usage_but_no_account_payloads(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)

            usage = {"uploaded": 2, "deleted": 1, "fullSnapshot": False, "pushedAt": "now"}
            with mock.patch.object(cloud, "upload_skills", return_value={"snapshotId": "skills", "changed": False}) as upload_skills, mock.patch.object(cloud, "_push_usage_data", return_value=usage) as push_usage:
                result = cloud.push()

            upload_skills.assert_called_once_with()
            push_usage.assert_called_once_with()
            self.assertEqual(result, {"skills": {"snapshotId": "skills", "changed": False}, "accounts": {"pushedAccounts": []}, "usage": usage, "changed": True})

    def test_cloud_account_operations_wait_for_the_shared_push_fetch_serialization_lock(self):
        with self.account_directory() as directory:
            entered_delete, release_delete, entered_release = threading.Event(), threading.Event(), threading.Event()

            def delete_account(_account_id):
                entered_delete.set()
                release_delete.wait(2)
                return {"deleted": True}

            accounts = SimpleNamespace(delete=delete_account, release_cloud_account=lambda _cloud, _account_id: entered_release.set() or {"released": True})
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), accounts)
            delete_thread = threading.Thread(target=cloud.delete_local_account, args=("account-a",))
            release_thread = threading.Thread(target=cloud.release_local_account, args=("account-b",))

            delete_thread.start()
            self.assertTrue(entered_delete.wait(1))
            release_thread.start()
            self.assertFalse(entered_release.wait(0.05))
            release_delete.set()
            delete_thread.join(1)
            release_thread.join(1)

            self.assertFalse(delete_thread.is_alive())
            self.assertFalse(release_thread.is_alive())
            self.assertTrue(entered_release.is_set())

    def test_skill_push_retains_remote_only_skills_and_local_wins_same_name(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            for name, content in (("shared", "local-v2"), ("local-only", "local")):
                target = skills.skills_root / name
                target.mkdir(parents=True)
                (target / "SKILL.md").write_text(content, encoding="utf-8")
            remote = SkillManager(directory / "remote-codex", directory / "remote-private", directory / "remote-gemini")
            for name, content in (("shared", "remote-v1"), ("remote-only", "remote")):
                target = remote.skills_root / name
                target.mkdir(parents=True)
                (target / "SKILL.md").write_text(content, encoding="utf-8")
            remote_snapshot = remote.snapshot()
            remote_snapshot_id = hashlib.sha256(remote_snapshot).hexdigest()
            cloud = CloudManager(directory / "private", skills, None)
            stored = {
                "skills/current.enc": json.dumps({"version": 1, "snapshotId": remote_snapshot_id}).encode(),
                f"skills/snapshots/{remote_snapshot_id}.enc": remote_snapshot
            }

            def put(path, data, **_kwargs):
                stored[path] = data
                return '"new"'

            client = SimpleNamespace(
                ensure_directories=mock.Mock(), list=mock.Mock(return_value=[f"{remote_snapshot_id}.enc"]), put=mock.Mock(side_effect=put),
                get=mock.Mock(side_effect=lambda path: (stored[path], '"remote-pointer"' if path == "skills/current.enc" else '"snapshot"')),
                delete=mock.Mock(side_effect=lambda path: stored.pop(path, None))
            )
            box = SimpleNamespace(encrypt=lambda _purpose, payload: payload, decrypt=lambda _purpose, payload: payload)

            with mock.patch.object(cloud, "_connection", return_value=(client, box)):
                result = cloud.upload_skills()

            pointer = json.loads(stored["skills/current.enc"])
            self.assertEqual(pointer["version"], 2)
            self.assertTrue(all(set(skills.split_snapshot(stored[f"skills/packages/{item['packageId']}.enc"])) == {name} for name, item in pointer["packages"].items()))
            merged_snapshot = skills.combine_snapshots([stored[f"skills/packages/{item['packageId']}.enc"] for item in pointer["packages"].values()])
            _, merged = skills.inspect_snapshot(merged_snapshot)
            self.assertEqual(set(merged), {"shared", "local-only", "remote-only"})
            self.assertEqual(merged["shared"]["SKILL.md"], b"local-v2")
            self.assertEqual(merged["local-only"]["SKILL.md"], b"local")
            self.assertEqual(merged["remote-only"]["SKILL.md"], b"remote")
            client.put.assert_any_call("skills/current.enc", mock.ANY, etag='"remote-pointer"')
            self.assertEqual(result["localSha256"], skills.content_hashes())
            pointer_puts = len([call for call in client.put.call_args_list if call.args[0] == "skills/current.enc"])

            with mock.patch.object(cloud, "_connection", return_value=(client, box)):
                unchanged = cloud.upload_skills()

            self.assertFalse(unchanged["changed"])
            self.assertEqual(unchanged["added"], [])
            self.assertEqual(unchanged["updated"], [])
            self.assertEqual(len([call for call in client.put.call_args_list if call.args[0] == "skills/current.enc"]), pointer_puts)

    def test_management_page_auto_refreshes_locally_and_fetches_webdav_explicitly(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")
        extension = Path(__file__).with_name("extension.js").read_text(encoding="utf-8")
        dashboard = Path(__file__).with_name("monitor_dashboard.py").read_text(encoding="utf-8")

        self.assertNotIn('data-action="reload"', html)
        self.assertIn('<section class="card"><h2>WebDAV</h2>', html)
        self.assertNotIn('<h2>Encrypted WebDAV</h2>', html)
        self.assertNotIn('cloud.configPath', html)
        self.assertNotIn('Config:', html)
        self.assertIn('<button data-action="cloud-fetch">Fetch</button>', html)
        self.assertIn('<button id="pushButton" data-action="cloud-push" class="primary">Push</button>', html)
        self.assertIn('pushWarning?"Push (!)":"Push"', html)
        self.assertIn('showMessage("Automatic Push Failed",`Automatic skill upload failed: ${failure.message}`', html)
        self.assertIn('{name:"Cloud upload",status:"failed",detail:failure.message}', html)
        self.assertNotIn('Auto Push Success', html)
        self.assertLess(html.index('data-action="cloud-push"'), html.index('data-action="cloud-fetch"'))
        self.assertLess(html.index('data-action="cloud-fetch"'), html.index('data-action="cloud-test"'))
        self.assertNotIn('Upload skills', html)
        self.assertNotIn('Back up all accounts', html)
        self.assertIn('<button data-action="account-new">Add account</button>', html)
        self.assertNotIn('Create / login', html)
        self.assertNotIn('Refresh credentials', html)
        self.assertIn('setInterval(load,1000)', html)
        self.assertIn('api(`/api/manage/status${refreshScan?', html)
        self.assertIn('if(serialized!==serializedData)', html)
        self.assertNotIn('data-action="scan"', html)
        self.assertIn('setInterval(()=>load(false,true),5000)', html)
        self.assertIn('run("/api/manage/cloud/fetch")', html)
        self.assertIn('run("/api/manage/cloud/push")', html)
        self.assertIn('run("/api/accounts/delete",{accountId:button.dataset.delete})', html)
        self.assertIn('${account.active?"disabled":""}>Switch</button>', html)
        self.assertIn('"/api/manage/cloud/fetch"', extension)
        self.assertIn('"/api/manage/cloud/push"', extension)
        self.assertIn('["/api/manage/accounts/delete", { url: ACCOUNT_DELETE_URL, method: "POST" }]', extension)
        self.assertIn('elif path == "/api/manage/cloud/fetch":', dashboard)
        self.assertIn('elif path == "/api/manage/cloud/push":', dashboard)
        self.assertIn('state.cloud.fetch(include_usage=True, force_full=True)', dashboard)
        self.assertIn('state.cloud.push(force_full=True)', dashboard)
        self.assertIn('elif path == "/api/manage/accounts/delete":', dashboard)
        self.assertNotIn('/api/manage/cloud/upload', html + extension + dashboard)
        self.assertNotIn('/api/manage/accounts/backup-all', html + extension + dashboard)
        self.assertNotIn('/api/manage/accounts/refresh', html + extension + dashboard)
        self.assertFalse(hasattr(AccountManager, "refresh_cloud_accounts"))
        self.assertFalse(hasattr(CloudManager, "update_account"))
        self.assertNotIn('Preview restore', html)
        self.assertNotIn('/api/manage/cloud/preview', html)
        self.assertNotIn('/api/manage/cloud/preview', extension)
        self.assertNotIn('/api/manage/cloud/preview', dashboard)
        self.assertFalse(hasattr(CloudManager, "preview_skills"))
        self.assertFalse(hasattr(SkillManager, "preview_restore"))

    def test_cloud_config_starts_without_control_password_and_adds_cookie_secret(self):
        with self.account_directory() as directory:
            private = directory / "private"
            private.mkdir()
            (private / "config.json").write_text(json.dumps({"version": 1, "machineName": "Legacy name", "webdav": {"enabled": False}}), encoding="utf-8")

            CloudManager(private, SkillManager(directory / "codex", private, directory / "gemini"), None)

            control = json.loads((private / "config.json").read_text(encoding="utf-8"))["control"]
            self.assertNotIn("machineName", json.loads((private / "config.json").read_text(encoding="utf-8")))
            self.assertEqual(control["password"], "")
            self.assertEqual(control["passwordHash"], "")
            self.assertGreaterEqual(len(control["cookieSecret"]), 32)

    def test_cloud_config_uses_offline_passphrase_hash_without_adaptation_fields(self):
        with self.account_directory() as directory:
            private = directory / "private"
            private.mkdir()
            encryption_hash = passphrase_hash("cloud-secret", "https://example.test", "user")
            (private / "config.json").write_text(json.dumps({
                "version": 1, "control": {"password": "123456", "cookieSecret": "c" * 32},
                "webdav": {"enabled": True, "baseUrl": "https://example.test", "username": "user", "password": "webdav-secret", "remoteRoot": "root", "encryptionPassphraseHash": encryption_hash},
            }), encoding="utf-8")

            cloud = CloudManager(private, SkillManager(directory / "codex", private, directory / "gemini"), None)

            saved = json.loads(cloud.config_path.read_text(encoding="utf-8"))
            webdav, control = saved["webdav"], saved["control"]
            self.assertEqual(control["password"], "")
            self.assertEqual(control["passwordHash"], "")
            self.assertEqual(webdav["password"], "webdav-secret")
            self.assertNotIn("cloud-secret", json.dumps(webdav))
            self.assertEqual(webdav["encryptionPassphraseHash"], encryption_hash)
            self.assertEqual(webdav["encryptionPassphrase"], "")
            self.assertFalse(any("PassphraseProtected" in key or key.startswith("pendingEncryption") for key in webdav))
            self.assertTrue(webdav["usageDataAutoSync"])

    def test_empty_passphrase_disables_webdav_payload_encryption(self):
        box = CryptoBox("", CryptoBox.descriptor(""))

        self.assertEqual(box.encrypt("skills", b"plain cloud data"), b"plain cloud data")
        self.assertEqual(box.decrypt("skills", b"plain cloud data"), b"plain cloud data")
        with self.assertRaises(CloudError) as caught:
            CryptoBox("", CryptoBox.descriptor(passphrase_hash("secret", "https://example.test", "user")))
        self.assertTrue(caught.exception.decrypt_failed)

    def test_config_reload_consumes_raw_passphrase_and_clears_staging_field(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)
            config = json.loads(cloud.config_path.read_text(encoding="utf-8"))
            config["webdav"].update({"baseUrl": "https://example.test", "username": "user", "encryptionPassphrase": "edited outside the monitor"})
            cloud.config_path.write_text(json.dumps(config), encoding="utf-8")

            cloud.reload_config()

            saved = json.loads(cloud.config_path.read_text(encoding="utf-8"))["webdav"]
            self.assertEqual(saved["encryptionPassphrase"], "")
            self.assertEqual(saved["encryptionPassphraseHash"], passphrase_hash("edited outside the monitor", "https://example.test", "user"))

    def test_enabled_webdav_accepts_empty_passphrase_and_hash(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)

            cloud.update_config({
                "server": {"host": "127.0.0.1"},
                "webdav": {"enabled": True, "baseUrl": "https://example.test", "username": "user", "password": "password", "remoteRoot": "root", "encryptionPassphrase": "", "skillsAutoUpload": True, "usageDataAutoSync": True, "allowOptimisticWrites": True},
            })

            self.assertEqual(cloud.config()["webdav"]["encryptionPassphrase"], "")
            self.assertEqual(cloud.config()["webdav"]["encryptionPassphraseHash"], "")

    def test_cloud_overwrite_removes_remote_root_and_uploads_local_skills(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)
            cloud.update_config({"server": {"host": "127.0.0.1"}, "webdav": {"enabled": True, "baseUrl": "https://example.test", "username": "user", "password": "password", "remoteRoot": "root", "encryptionPassphrase": "", "skillsAutoUpload": True, "usageDataAutoSync": True, "allowOptimisticWrites": True}})
            client = mock.Mock()

            with mock.patch("monitor_cloud.WebDavClient", return_value=client), mock.patch.object(cloud, "upload_skills", return_value={"changed": True}) as upload:
                result = cloud.overwrite_cloud_from_local()

            client.delete.assert_called_once_with("")
            client.ensure_directories.assert_called_once_with("")
            upload.assert_called_once_with()
            self.assertTrue(result["overwritten"])
            self.assertFalse(result["encryptionEnabled"])

    def test_initial_control_password_rejects_old_default_and_can_only_be_set_once(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)

            with self.assertRaisesRegex(CloudError, "other than 123456"):
                cloud.initialize_control_password("123456")
            self.assertEqual(cloud.config()["control"]["passwordHash"], "")

            result = cloud.initialize_control_password("a unique control password")

            self.assertTrue(result["controlPasswordConfigured"])
            self.assertTrue(control_password_matches("a unique control password", cloud.config()["control"]["passwordHash"], cloud.config()["control"]["passwordSalt"]))
            with self.assertRaisesRegex(CloudError, "already configured"):
                cloud.initialize_control_password("another password")

    def test_control_password_hash_without_separate_salt_is_marked_compromised(self):
        with self.account_directory() as directory:
            private = directory / "private"
            private.mkdir()
            legacy_salt = new_control_password_salt()
            (private / "config.json").write_text(json.dumps({
                "version": 1, "control": {"password": "", "passwordHash": hash_control_password("old password", legacy_salt), "cookieSecret": "c" * 32},
                "server": {"host": "127.0.0.1"}, "webdav": {"enabled": False},
            }), encoding="utf-8")

            cloud = CloudManager(private, SkillManager(directory / "codex", private, directory / "gemini"), None)

            self.assertNotEqual(cloud.config()["control"]["passwordHash"], "")
            self.assertEqual(cloud.config()["control"]["passwordSalt"], "")
            with self.assertRaisesRegex(CloudError, "Control password compromised"):
                cloud.initialize_control_password("replacement password")

    def test_passphrase_hash_uses_normalized_webdav_identity_salt(self):
        first = passphrase_hash("cloud-secret", "https://dav.jianguoyun.com/dav/", "user")
        second = passphrase_hash("cloud-secret", "dav.jianguoyun.com/dav", "user")
        descriptor = CryptoBox.descriptor(first)

        self.assertEqual(first, second)
        self.assertTrue(valid_passphrase_hash(first))
        self.assertEqual(normalized_webdav_identity("https://dav.jianguoyun.com/dav/", "user"), "dav.jianguoyun.com/dav\nuser")
        self.assertEqual(base64.b64decode(descriptor["salt"]), webdav_passphrase_salt("dav.jianguoyun.com/dav", "user"))
        self.assertEqual(CryptoBox(first, descriptor).key, CryptoBox(second, descriptor).key)

    def test_config_update_applies_secrets_immediately_and_rotates_remote_passphrase(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)
            legacy_salt = b"codex-switch-passphrase-v1"
            legacy_key = hashlib.scrypt(b"old passphrase", salt=legacy_salt, n=1 << 15, r=8, p=1, dklen=32, maxmem=128 * 1024 * 1024)
            old_hash = f"scrypt-key-v1$32768$8$1${base64.b64encode(legacy_salt).decode()}${base64.b64encode(legacy_key).decode()}"
            new_hash = passphrase_hash("new passphrase", "https://new.example.test", "new-user")
            old_box = CryptoBox(old_hash, CryptoBox.descriptor(old_hash))
            stored = {
                "crypto.json": json.dumps(CryptoBox.descriptor(old_hash), separators=(",", ":")).encode(),
                "skills/packages/package.enc": old_box.encrypt("skill-package:package", b"skill data"),
                "accounts/states/account.enc": old_box.encrypt("account-state:account", b"account data"),
            }

            class Client:
                def ensure_directories(self, _path):
                    pass

                def get(self, path):
                    return stored[path], f'"{hashlib.sha256(stored[path]).hexdigest()}"'

                def put(self, path, data, etag=None):
                    self.assertEqual(etag, self.get(path)[1])
                    stored[path] = data
                    return self.get(path)[1]

                def list(self, path):
                    return {"skills": ["packages"], "skills/packages": ["package.enc"], "accounts/states": ["account.enc"]}.get(path, [])

                assertEqual = self.assertEqual

            cloud._config["webdav"].update({"enabled": True, "baseUrl": "https://old.example.test", "username": "old-user", "password": "old-password", "encryptionPassphraseHash": old_hash})
            atomic_write_json(cloud.config_path, cloud._config)
            values = {
                "server": {"host": "127.0.0.1"}, "controlPassword": "new-control-password",
                "webdav": {"enabled": True, "baseUrl": "https://new.example.test", "username": "new-user", "password": "new-webdav-password", "remoteRoot": "new-root", "encryptionPassphrase": "new passphrase", "skillsAutoUpload": False, "usageDataAutoSync": False, "allowOptimisticWrites": False},
            }
            with mock.patch("monitor_cloud.WebDavClient", return_value=Client()):
                result = cloud.update_config(values)

            saved = json.loads(cloud.config_path.read_text(encoding="utf-8"))
            self.assertTrue(result["passphraseChanged"])
            self.assertTrue(result["controlPasswordChanged"])
            self.assertTrue(result["restartRequired"])
            self.assertNotIn("machineName", saved)
            self.assertEqual(saved["webdav"]["password"], "new-webdav-password")
            self.assertEqual(saved["webdav"]["encryptionPassphraseHash"], new_hash)
            self.assertTrue(control_password_matches("new-control-password", saved["control"]["passwordHash"], saved["control"]["passwordSalt"]))
            self.assertNotIn("new passphrase", cloud.config_path.read_text(encoding="utf-8"))
            new_box = CryptoBox(new_hash, json.loads(stored["crypto.json"]))
            self.assertEqual(new_box.decrypt("skill-package:package", stored["skills/packages/package.enc"]), b"skill data")
            self.assertEqual(new_box.decrypt("account-state:account", stored["accounts/states/account.enc"]), b"account data")

    def test_failed_passphrase_rotation_restores_remote_payloads_and_rejects_config(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)
            old_hash = passphrase_hash("old passphrase", "https://example.test", "user")
            old_box = CryptoBox(old_hash, CryptoBox.descriptor(old_hash))
            stored = {"crypto.json": json.dumps(CryptoBox.descriptor(old_hash), separators=(",", ":")).encode()}
            for name in ("first", "second"):
                stored[f"skills/packages/{name}.enc"] = old_box.encrypt(f"skill-package:{name}", name.encode())
            original = dict(stored)

            class Client:
                failed = False

                def ensure_directories(self, _path):
                    pass

                def get(self, path):
                    return stored[path], f'"{hashlib.sha256(stored[path]).hexdigest()}"'

                def put(self, path, data, etag=None):
                    if path.endswith("second.enc") and not self.failed:
                        self.failed = True
                        raise CloudError("simulated upload failure", 502)
                    stored[path] = data
                    return self.get(path)[1]

                def list(self, path):
                    return {"skills/packages": ["first.enc", "second.enc"]}.get(path, [])

            cloud._config["webdav"].update({"enabled": True, "baseUrl": "https://example.test", "username": "user", "password": "password", "encryptionPassphraseHash": old_hash})
            atomic_write_json(cloud.config_path, cloud._config)
            values = {
                "server": cloud._config["server"], "controlPassword": "",
                "webdav": {**{key: cloud._config["webdav"][key] for key in ("enabled", "baseUrl", "username", "remoteRoot", "skillsAutoUpload", "usageDataAutoSync", "allowOptimisticWrites")}, "password": "", "encryptionPassphrase": "new passphrase"},
            }
            with mock.patch("monitor_cloud.WebDavClient", return_value=Client()), self.assertRaisesRegex(CloudError, "Passphrase update failed"):
                cloud.update_config(values)

            self.assertEqual(stored, original)
            self.assertEqual(json.loads(cloud.config_path.read_text(encoding="utf-8"))["webdav"]["encryptionPassphraseHash"], old_hash)

    def test_cloud_reencrypt_refreshes_and_verifies_every_payload_type(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)
            encryption_hash = passphrase_hash("passphrase", "https://example.test", "user")
            box = CryptoBox(encryption_hash, CryptoBox.descriptor(encryption_hash))
            purposes = {
                "skills/packages/package.enc": "skill-package:package", "skills/snapshots/snapshot.enc": "skills-snapshot:snapshot", "skills/current.enc": "skills-pointer",
                "accounts/states/account.enc": "account-state:account", "accounts/revisions/account/revision.enc": "account-revision:account:revision",
            }
            original = {path: box.encrypt(purpose, path.encode()) for path, purpose in purposes.items()}
            stored = dict(original)

            class Client:
                def get(self, path):
                    if path not in stored:
                        raise CloudError("WebDAV GET failed with HTTP 404", 502)
                    return stored[path], f'"{hashlib.sha256(stored[path]).hexdigest()}"'

                def put(self, path, data, etag=None):
                    stored[path] = data
                    return f'"{hashlib.sha256(data).hexdigest()}"'

                def list(self, path):
                    return {
                        "skills/packages": ["package.enc"], "skills/snapshots": ["snapshot.enc"], "accounts/states": ["account.enc"],
                        "accounts/revisions": ["account"], "accounts/revisions/account": ["revision.enc"],
                    }.get(path, [])

            with mock.patch.object(cloud, "_connection", return_value=(Client(), box)), mock.patch.object(cloud, "fetch", return_value={"verified": True}) as fetch:
                result = cloud.reencrypt_remote_data()

            self.assertEqual(result["reencrypted"], len(purposes))
            self.assertEqual(result["fetch"], {"verified": True})
            fetch.assert_called_once_with()
            for path, purpose in purposes.items():
                self.assertNotEqual(stored[path], original[path])
                self.assertEqual(box.decrypt(purpose, stored[path]), path.encode())

    def test_control_auth_rejects_wrong_password_tampering_and_expired_cookie(self):
        salt = new_control_password_salt()
        auth = monitor_dashboard.ControlAuth({"passwordHash": hash_control_password("123456", salt), "passwordSalt": salt, "cookieSecret": "a" * 32})
        token = auth.create_token(now=100)

        self.assertTrue(auth.password_matches("123456"))
        self.assertFalse(auth.password_matches("654321"))
        self.assertTrue(auth.token_is_valid(token, now=100 + monitor_dashboard.CONTROL_COOKIE_MAX_AGE_SECONDS))
        self.assertFalse(auth.token_is_valid(token + "x", now=101))
        self.assertFalse(auth.token_is_valid(token, now=101 + monitor_dashboard.CONTROL_COOKIE_MAX_AGE_SECONDS))
        changed_salt = new_control_password_salt()
        self.assertFalse(monitor_dashboard.ControlAuth({"passwordHash": hash_control_password("changed", changed_salt), "passwordSalt": changed_salt, "cookieSecret": "a" * 32}).token_is_valid(token, now=101))
        auth.update({"passwordHash": hash_control_password("changed", changed_salt), "passwordSalt": changed_salt, "cookieSecret": "a" * 32})
        self.assertTrue(auth.password_matches("changed"))
        self.assertFalse(auth.token_is_valid(token, now=101))

        compromised = monitor_dashboard.ControlAuth({"passwordHash": hash_control_password("legacy", new_control_password_salt()), "cookieSecret": "a" * 32})
        self.assertTrue(compromised.is_compromised())
        self.assertFalse(compromised.is_configured())

    def test_control_auth_is_required_by_management_and_account_api_clients(self):
        html = Path(__file__).with_name("management.html").read_text(encoding="utf-8")
        extension = Path(__file__).with_name("extension.js").read_text(encoding="utf-8")
        dashboard = Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")
        source = Path(__file__).with_name("monitor_dashboard.py").read_text(encoding="utf-8")

        self.assertIn('id="controlLoginModal"', html)
        self.assertIn('showMessage("Control password compromised"', html)
        self.assertIn('Remove passwordHash from config.json', source)
        self.assertIn('setup?"/api/control/setup":"/api/control/login"', html)
        self.assertIn('if not self.require_control_auth():', source)
        self.assertIn('"/api/control/login"', source)
        self.assertIn('"/api/control/setup"', source)
        self.assertIn('HttpOnly; SameSite=Strict; Path=/; Max-Age=', source)
        self.assertIn('["/api/control/login", { url: CONTROL_LOGIN_URL, method: "POST" }]', extension)
        self.assertIn('["/api/control/setup", { url: CONTROL_SETUP_URL, method: "POST" }]', extension)
        self.assertIn('cookie: controlCookie', extension)
        self.assertIn('authenticatedAccountAction(action,body)', dashboard)
        self.assertIn('if(error.status!==401&&error.status!==428)throw error;', dashboard)
        self.assertIn('await accountAction(setup?"setup":"login",{password});', dashboard)
        self.assertIn('document.getElementById("accountSelect").onchange=event=>performAccountAction("switch"', dashboard)
        self.assertIn('id="dashboardControlPassword" type="password"', dashboard)
        self.assertIn('id="dashboardControlPasswordConfirmation" type="password"', dashboard)
        self.assertIn('id="controlPasswordMatchStatus" aria-live="polite"', dashboard)
        self.assertIn('"✓ Passwords match":"✕ Passwords do not match"', dashboard)
        self.assertIn('password.oninput=confirmation.oninput=setup?updateMatchStatus:null', dashboard)
        self.assertIn('await requestControlPassword(true)', dashboard)
        self.assertIn('password=await requestControlPassword(setup)', dashboard)
        self.assertNotIn('prompt("Control password")', dashboard)
        self.assertIn('load(true);pollStatus(true);setInterval(pollStatus,5000)', dashboard)

    def test_initial_control_password_setup_is_loopback_only(self):
        self.assertTrue(monitor_dashboard.client_host_is_loopback("127.0.0.1"))
        self.assertTrue(monitor_dashboard.client_host_is_loopback("::1"))
        self.assertFalse(monitor_dashboard.client_host_is_loopback("192.168.1.2"))
        self.assertFalse(monitor_dashboard.client_host_is_loopback("invalid"))
        source = Path(__file__).with_name("monitor_dashboard.py").read_text(encoding="utf-8")
        self.assertIn('if not client_host_is_loopback(self.client_address[0]):', source)

    def test_cloud_write_gate_allows_explicit_optimistic_mode(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            config = json.loads(cloud.config_path.read_text(encoding="utf-8"))
            config["webdav"]["allowOptimisticWrites"] = True
            cloud.config_path.write_text(json.dumps(config), encoding="utf-8")

            cloud._require_conditional_writes()

            config["webdav"]["allowOptimisticWrites"] = False
            cloud.config_path.write_text(json.dumps(config), encoding="utf-8")

            cloud._require_conditional_writes()
            cloud = CloudManager(directory / "private", skills, None)
            with self.assertRaises(CloudError):
                cloud._require_conditional_writes()

    def test_cloud_state_normalizes_combined_snapshot_fields_for_package_migration(self):
        with self.account_directory() as directory:
            private = directory / "private"
            private.mkdir()
            (private / "machine.json").write_text(json.dumps({"version": 1, "machineId": "machine", "createdAt": "then"}), encoding="utf-8")
            (private / "cloud-state.json").write_text(json.dumps({
                "version": 1, "skills": {"pointerEtag": '"pointer"', "snapshotId": "legacy", "localSha256": "combined"},
                "remote": {"accounts": {}, "skills": {"pointerEtag": '"pointer"', "snapshotId": "legacy", "updatedAt": "then"}},
                "pendingAccountOperation": None, "conditionalWritesVerified": False
            }), encoding="utf-8")

            cloud = CloudManager(private, SkillManager(directory / "codex", private, directory / "gemini"), None)

            self.assertEqual(cloud._state["skills"], {"indexEtag": '"pointer"', "indexId": "legacy", "localSha256": {}})
            self.assertEqual(cloud._state["remote"]["skills"], {"version": 1, "indexEtag": '"pointer"', "indexId": "legacy", "legacySnapshotId": "legacy", "updatedAt": "then"})

    def test_skill_scan_is_cached_until_explicit_refresh(self):
        with self.account_directory() as directory:
            codex_home = directory / "codex"
            first = codex_home / "skills" / "first"
            first.mkdir(parents=True)
            (first / "SKILL.md").write_text("first", encoding="utf-8")
            manager = SkillManager(codex_home, directory / "private", directory / "gemini")

            self.assertEqual([item["name"] for item in manager.scan()], ["first"])
            second = codex_home / "skills" / "second"
            second.mkdir()
            (second / "SKILL.md").write_text("second", encoding="utf-8")
            self.assertEqual([item["name"] for item in manager.scan()], ["first"])
            self.assertEqual([item["name"] for item in manager.scan(refresh=True)], ["first", "second"])

    def test_skill_auto_push_waits_for_two_minutes_of_stable_content(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            cloud._config["webdav"]["enabled"] = True
            cloud._state["skills"]["localSha256"] = {"alpha": "base"}
            cloud._last_auto_fetch_at = 1000

            with mock.patch.object(skills, "content_hashes", return_value={"alpha": "base"}):
                cloud.maintenance_tick(now=0)
            with mock.patch.object(skills, "content_hashes", return_value={"alpha": "modified"}) as hashes, mock.patch.object(cloud, "upload_skills", return_value={"changed": True}) as push:
                cloud.maintenance_tick(now=1)
                cloud.maintenance_tick(now=AUTO_PUSH_STABLE_SECONDS)
                cloud.maintenance_tick(now=AUTO_PUSH_STABLE_SECONDS + 1)

            push.assert_called_once_with({"alpha"})
            self.assertEqual(hashes.call_args_list[-1], mock.call({"alpha"}))

    def test_skill_auto_push_tracks_each_skill_stability_window_separately(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            cloud._config["webdav"]["enabled"] = True
            cloud._state["skills"]["localSha256"] = {"alpha": "base-a", "beta": "base-b"}
            cloud._last_auto_fetch_at = 1000

            with mock.patch.object(skills, "content_hashes", side_effect=({"alpha": "base-a", "beta": "base-b"}, {"alpha": "new-a", "beta": "base-b"}, {"alpha": "new-a", "beta": "new-b"}, {"alpha": "new-a", "beta": "new-b"}, {"alpha": "new-a", "beta": "new-b"})), mock.patch.object(cloud, "upload_skills", return_value={"changed": True}) as upload:
                cloud.maintenance_tick(now=0)
                cloud.maintenance_tick(now=1)
                cloud.maintenance_tick(now=61)
                cloud.maintenance_tick(now=121)

            upload.assert_called_once_with({"alpha"})
            self.assertEqual(cloud._pending_skill_pushes["beta"]["nextAttemptAt"], 61 + AUTO_PUSH_STABLE_SECONDS)

    def test_skill_auto_push_respects_skills_auto_upload_setting(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            cloud._config["webdav"].update({"enabled": True, "skillsAutoUpload": False})
            cloud._state["skills"]["localSha256"] = {"alpha": "base"}
            cloud._observed_skill_hashes = {"alpha": "base"}
            cloud._last_auto_fetch_at = 1000

            with mock.patch.object(skills, "content_hashes", return_value={"alpha": "modified"}) as hashes, mock.patch.object(cloud, "upload_skills") as push:
                cloud.maintenance_tick(now=1)
                cloud.maintenance_tick(now=AUTO_PUSH_STABLE_SECONDS + 1)

            hashes.assert_not_called()
            push.assert_not_called()
            self.assertEqual(cloud._pending_skill_pushes, {})

    def test_skill_auto_push_retries_three_times_then_exposes_failure(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, SimpleNamespace(manifest={"cloudBindingEnabled": True}))
            cloud._config["webdav"]["enabled"] = True
            cloud._observed_skill_hashes = {"alpha": "modified"}
            cloud._pending_skill_pushes = {"alpha": {"hash": "modified", "since": 0, "nextAttemptAt": AUTO_PUSH_STABLE_SECONDS, "attempts": 0}}
            cloud._last_auto_fetch_at = 1000

            with mock.patch.object(skills, "content_hashes", return_value={"alpha": "modified"}), mock.patch.object(cloud, "upload_skills", side_effect=CloudError("offline")) as push:
                for now in (AUTO_PUSH_STABLE_SECONDS, AUTO_PUSH_STABLE_SECONDS + AUTO_PUSH_RETRY_SECONDS, AUTO_PUSH_STABLE_SECONDS + AUTO_PUSH_RETRY_SECONDS * 2):
                    cloud.maintenance_tick(now=now)
                cloud.maintenance_tick(now=AUTO_PUSH_STABLE_SECONDS + AUTO_PUSH_RETRY_SECONDS * 3)

            self.assertEqual(push.call_count, AUTO_PUSH_MAX_ATTEMPTS)
            self.assertEqual(cloud.redacted_status()["autoSync"]["failure"]["message"], "offline")

    def test_skill_edit_during_failed_upload_restarts_stability_window(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, SimpleNamespace(manifest={"cloudBindingEnabled": True}))
            cloud._config["webdav"]["enabled"] = True
            cloud._observed_skill_hashes = {"alpha": "before"}
            cloud._pending_skill_pushes = {"alpha": {"hash": "before", "since": 0, "nextAttemptAt": AUTO_PUSH_STABLE_SECONDS, "attempts": 0}}
            cloud._last_auto_fetch_at = 1000

            with mock.patch.object(skills, "content_hashes", side_effect=({"alpha": "before"}, {"alpha": "after"})), mock.patch.object(cloud, "upload_skills", side_effect=CloudError("editing")):
                cloud.maintenance_tick(now=AUTO_PUSH_STABLE_SECONDS)

            self.assertEqual(cloud._pending_skill_pushes["alpha"]["hash"], "after")
            self.assertEqual(cloud._pending_skill_pushes["alpha"]["attempts"], 0)
            self.assertEqual(cloud._pending_skill_pushes["alpha"]["nextAttemptAt"], AUTO_PUSH_STABLE_SECONDS * 2)

    def test_periodic_auto_fetch_refreshes_skills_and_account_candidates(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            cloud._last_auto_fetch_at = 0

            with mock.patch.object(cloud, "fetch", return_value={}) as fetch:
                self.assertTrue(cloud._auto_fetch_if_due(AUTO_FETCH_INTERVAL_SECONDS))

            fetch.assert_called_once_with(include_usage=False)

    def test_periodic_auto_fetch_waits_for_interval(self):
        with self.account_directory() as directory:
            skills = SkillManager(directory / "codex", directory / "private", directory / "gemini")
            cloud = CloudManager(directory / "private", skills, None)
            cloud._last_auto_fetch_at = 0

            with mock.patch.object(cloud, "fetch") as fetch:
                self.assertFalse(cloud._auto_fetch_if_due(AUTO_FETCH_INTERVAL_SECONDS - 1))

            fetch.assert_not_called()

    def test_account_binding_blocks_matching_local_identity_and_keeps_cloud_payload(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            data = json.dumps(self.account_auth("acct-a", "remote-refresh")).encode()
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "local-refresh")), encoding="utf-8")
            manager = AccountManager(auth_path)
            local_data = auth_path.read_bytes()
            cloud = SimpleNamespace(
                begin_account_transition=mock.Mock(), clear_account_transition=mock.Mock(), machine_id="machine", delete_account_payloads=mock.Mock(),
                bind_account=mock.Mock(return_value=({"accountKey": "key-a", "accountId": "acct-a", "label": "A"}, data, '"etag"'))
            )

            with self.assertRaises(AccountError) as raised:
                manager.bind_cloud_account(cloud, "key-a")

            self.assertEqual(raised.exception.status, 409)
            self.assertIn("already managed as Current account", str(raised.exception))
            cloud.delete_account_payloads.assert_not_called()
            cloud.clear_account_transition.assert_called_once_with()
            self.assertEqual(manager.active_account()["cloud"]["state"], "local-only")
            self.assertEqual(auth_path.read_bytes(), local_data)

    def test_usage_sync_identity_uses_id_token_hash_and_ignores_managed_label(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps({"tokens": {"id_token": "stable-id-token", "refresh_token": "refresh-a"}}), encoding="utf-8")
            manager = AccountManager(auth_path)
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), manager)
            first = cloud.usage_account_id("ppl-pro")
            manager.active_account()["label"] = "A completely different label"
            cloud._usage_account_ids.clear()

            self.assertEqual(cloud.usage_account_id("ppl-pro"), first)
            self.assertNotIn("stable-id-token", first)
            self.assertEqual(cloud.local_usage_account(first, None, None), ("ppl-pro", "A completely different label"))
            self.assertIsNone(cloud.local_usage_account("unmatched-usage-account", None, None))

    def test_cloud_bind_only_downloads_and_integrity_checks_account_file(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)
            state = {"version": 1, "accountKey": "key-a", "accountId": "acct-a", "revisionId": "revision"}
            client, box = SimpleNamespace(), SimpleNamespace()

            with mock.patch.object(cloud, "_require_conditional_writes") as require_writes, mock.patch.object(cloud, "_connection", return_value=(client, box)), mock.patch.object(cloud, "account_state", return_value=(state, '"etag"')), mock.patch.object(cloud, "_download_account_revision_with", return_value=b'{"tokens":{}}') as download:
                result = cloud.bind_account("key-a")

            require_writes.assert_called_once_with()
            download.assert_called_once_with(client, box, state)
            self.assertEqual(result, (state, b'{"tokens":{}}', '"etag"'))

    def test_cloud_account_revision_validation_supports_opaque_and_legacy_identity_keys(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)

            def download(state, auth_data, revision_data, account_key):
                revision = hashlib.sha256(auth_data).hexdigest()
                payload = json.dumps({"version": 1, "accountKey": state["accountKey"], "authSize": len(auth_data), "authSha256": revision, "auth": base64.b64encode(auth_data).decode(), **revision_data}).encode()
                client = SimpleNamespace(get=mock.Mock(return_value=(payload, '"etag"')))
                box = SimpleNamespace(decrypt=lambda _purpose, data: data, account_key=mock.Mock(return_value=account_key))
                self.assertEqual(cloud._download_account_revision_with(client, box, {**state, "revisionId": revision}), auth_data)
                return box

            opaque_box = download({"accountKey": "placeholder-key", "keyType": "opaque", "accountId": None}, b"{}", {"keyType": "opaque", "accountIdHash": None}, "wrong-key")
            opaque_box.account_key.assert_not_called()
            legacy_box = download({"accountKey": "identity-key", "accountId": "acct-a"}, json.dumps(self.account_auth("acct-a", "refresh-a")).encode(), {"accountIdHash": hashlib.sha256(b"acct-a").hexdigest()}, "identity-key")
            legacy_box.account_key.assert_called_once_with("acct-a")

    def test_cloud_release_creates_verified_remote_copy(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)
            auth_data = json.dumps(self.account_auth("acct-a", "refresh-a")).encode()
            identity = {"accountId": "acct-a", "email": "a@example.test"}
            key = "key-a"
            client = SimpleNamespace(ensure_directories=mock.Mock(), put=mock.Mock(return_value='"etag"'))
            box = SimpleNamespace(account_key=mock.Mock(return_value=key), encrypt=lambda _purpose, payload: payload)
            verified = {"version": 1, "accountKey": key, "accountId": "acct-a", "revisionId": hashlib.sha256(auth_data).hexdigest()}

            with mock.patch.object(cloud, "_require_conditional_writes"), mock.patch.object(cloud, "_connection", return_value=(client, box)), mock.patch.object(cloud, "account_state", side_effect=(CloudError("WebDAV GET failed with HTTP 404", 502), (verified, '"etag"'))), mock.patch.object(cloud, "_encrypted_payloads", return_value=set()), mock.patch.object(cloud, "_upload_revision") as upload, mock.patch.object(cloud, "_cleanup_account_revisions"), mock.patch.object(cloud, "_cache_remote_account") as cache:
                result = cloud.release_account(key, auth_data, identity, "Account A")

            upload.assert_called_once_with(client, box, key, auth_data, identity, "identity")
            state = json.loads(client.put.call_args.args[1])
            self.assertEqual(state["accountId"], "acct-a")
            self.assertIsNone(state["boundMachineId"])
            self.assertTrue(client.put.call_args.kwargs["create"])
            cache.assert_called_once_with(state, '"etag"')
            self.assertEqual(result["accountKey"], key)

    def test_cloud_release_accepts_empty_opaque_account(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)
            client = SimpleNamespace(ensure_directories=mock.Mock(), put=mock.Mock(return_value='"etag"'))
            box = SimpleNamespace(account_key=mock.Mock(), encrypt=lambda _purpose, payload: payload)
            verified = {"version": 1, "accountKey": "placeholder-key", "keyType": "opaque", "accountId": None, "ready": False, "revisionId": hashlib.sha256(b"{}").hexdigest()}

            with mock.patch.object(cloud, "_require_conditional_writes"), mock.patch.object(cloud, "_connection", return_value=(client, box)), mock.patch.object(cloud, "account_state", side_effect=(CloudError("WebDAV GET failed with HTTP 404", 502), (verified, '"etag"'))), mock.patch.object(cloud, "_encrypted_payloads", return_value=set()), mock.patch.object(cloud, "_upload_revision") as upload, mock.patch.object(cloud, "_cleanup_account_revisions"), mock.patch.object(cloud, "_cache_remote_account"):
                result = cloud.release_account("placeholder-key", b"{}", {"accountId": None, "email": None}, "Empty", ready=False)

            upload.assert_called_once_with(client, box, "placeholder-key", b"{}", {"accountId": None, "email": None}, "opaque")
            self.assertFalse(result["ready"])
            self.assertEqual(result["keyType"], "opaque")

    def test_recovered_release_finalizes_only_matching_verified_remote_revision(self):
        with self.account_directory() as directory:
            accounts = SimpleNamespace(finalize_recovered_release=mock.Mock(), rollback_recovered_release=mock.Mock())
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), accounts)
            data = b"released-auth"
            revision = hashlib.sha256(data).hexdigest()
            cloud._state["pendingAccountOperation"] = {"operation": "release", "accountId": "local-a", "accountKey": "key-a", "revisionId": revision}

            with mock.patch.object(cloud, "bind_account", return_value=({"revisionId": revision}, data, '"etag"')):
                result = cloud.recover_account_transition()

            self.assertEqual(result, "release")
            accounts.finalize_recovered_release.assert_called_once_with("local-a")
            accounts.rollback_recovered_release.assert_not_called()

    def test_account_release_removes_local_after_direct_upload(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth_path.write_text(json.dumps(self.account_auth("acct-a", "refresh-a")), encoding="utf-8")
            manager = AccountManager(auth_path)
            second_id = manager.create_account("Second")["activeAccountId"]
            auth_path.write_text(json.dumps(self.account_auth("acct-b", "refresh-b")), encoding="utf-8")
            manager.status()
            manager._find("ppl-pro")["cloud"] = {"state": "bound-local", "accountKey": "key-a", "boundMachineId": "machine"}
            manager.manifest["cloudBindingEnabled"] = True
            manager._save_manifest()

            def release_while_local_exists(*_args, **_kwargs):
                self.assertTrue(auth_path.exists())
                self.assertTrue((manager.root / "ppl-pro" / "auth.json").exists())
                return {}

            cloud = SimpleNamespace(begin_account_transition=mock.Mock(), clear_account_transition=mock.Mock(), release_account=mock.Mock(side_effect=release_while_local_exists))

            status = manager.release_cloud_account(cloud, "ppl-pro")

            self.assertEqual([account["id"] for account in status["items"]], [second_id])
            self.assertTrue(auth_path.exists())

    def test_account_manifest_v1_migrates_to_v2_without_rewriting_credentials(self):
        with self.account_directory() as directory:
            auth_path = directory / "auth.json"
            auth = json.dumps(self.account_auth("acct-a", "secret-refresh")).encode()
            auth_path.write_bytes(auth)
            manager = AccountManager(auth_path)
            manifest = json.loads(manager.manifest_path.read_text(encoding="utf-8"))
            manifest["version"] = 1
            manifest.pop("cloudBindingEnabled")
            for account in manifest["accounts"]:
                account.pop("cloud")
            manager.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            migrated = AccountManager(auth_path)

            self.assertEqual(migrated.manifest["version"], 2)
            self.assertFalse(migrated.manifest["cloudBindingEnabled"])
            self.assertEqual(migrated.active_account()["identity"]["idTokenHash"], hashlib.sha256(b"id-acct-a").hexdigest())
            self.assertEqual((migrated.root / "ppl-pro" / "auth.json").read_bytes(), auth)

    def test_usage_cost_intervals_sum_machine_cost_without_duplicate_percent(self):
        def interval(machine, start, end, cost, started_at, checked_at):
            return {
                "recordType": "costInterval", "startedAt": started_at, "checkedAt": checked_at, "window": "5h", "accountSlotId": "account-a", "accountLabel": "A",
                "startPercent": start, "endPercent": end, "plan": "plus", "planMultiplier": 1, "resetAt": "2030-01-01T05:00:00Z", "modelCostsUsd": {"gpt-5.5": cost},
                "sync": {"originMachineId": machine, "accountId": "usage-account", "recordId": f"{machine}:{checked_at}"},
            }

        merged = aggregate_cost_intervals([
            interval("machine-a", 0, 1, 1, "2030-01-01T00:00:00Z", "2030-01-01T00:01:30Z"),
            interval("machine-b", 0, 1, 1, "2030-01-01T00:00:30Z", "2030-01-01T00:02:00Z"),
        ])

        self.assertEqual([(row["deltaPercent"], row["deltaCostUsd"], row["costPercentRatio"]) for row in merged], [(1, 2, 2)])
        self.assertEqual(merged[0]["checkedAt"], "2030-01-01T00:01:30Z")

    def test_usage_quota_snapshot_keeps_only_idle_plateau_boundaries(self):
        with self.account_directory() as directory:
            history, quota, tokens = (directory / name for name in ("history.jsonl", "quota.jsonl", "tokens.jsonl"))
            history.write_text("", encoding="utf-8")
            tokens.write_text("", encoding="utf-8")
            rows = [{
                "checkedAt": f"2030-01-01T00:0{index}:00Z", "accountSlotId": "account-a", "accountLabel": "Managed name",
                "windows": {"5h": {"usedPercent": 5 if index < 5 else 6, "resetAt": "2030-01-01T05:00:00Z"}, "7d": {"usedPercent": 20, "resetAt": "2030-01-08T00:00:00Z"}},
                "sync": {"version": 1, "originMachineId": "machine-a", "accountId": "usage-a", "recordId": f"quota-{index}"},
            } for index in range(6)]
            quota.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8")
            store = UsageDataStore(history, quota, tokens, "machine-a", lambda _slot: "usage-a", threading.Lock())

            records, present = store.snapshot()
            full_records, full_present = store.snapshot(necessary_only=False)

            self.assertEqual([record["row"]["checkedAt"] for record in records.values()], ["2030-01-01T00:00:00Z", "2030-01-01T00:04:00Z", "2030-01-01T00:05:00Z"])
            self.assertEqual(next(record["row"]["compaction"] for record in records.values() if record["row"]["checkedAt"] == "2030-01-01T00:04:00Z"), {"continuousFrom": "2030-01-01T00:00:00Z", "omittedSamples": 3})
            self.assertEqual(present, set(records))
            self.assertEqual([record["row"]["checkedAt"] for record in full_records.values()], [row["checkedAt"] for row in rows])
            self.assertEqual(full_present, set(full_records))
            self.assertNotIn("Managed name", json.dumps(records))
            self.assertEqual([row["checkedAt"] for row in monitor_history.load_quota_history(quota)], [row["checkedAt"] for row in rows])

    def test_usage_quota_plateau_moves_latest_boundary_without_reuploading_middle_rows(self):
        with self.account_directory() as directory:
            history, quota, tokens = (directory / name for name in ("history.jsonl", "quota.jsonl", "tokens.jsonl"))
            history.write_text("", encoding="utf-8")
            tokens.write_text("", encoding="utf-8")
            make_row = lambda index: {"checkedAt": f"2030-01-01T00:0{index}:00Z", "accountSlotId": "account-a", "accountLabel": "A", "windows": {"5h": {"usedPercent": 5, "resetAt": "2030-01-01T05:00:00Z"}}}
            monitor_history.write_quota_history(quota, [make_row(index) for index in range(3)])
            store = UsageDataStore(history, quota, tokens, "machine-a", lambda _slot: "usage-a", threading.Lock())
            first, _ = store.snapshot()
            monitor_history.write_quota_history(quota, monitor_history.load_quota_history(quota) + [make_row(3), make_row(4)])

            second, present = store.snapshot()

            self.assertEqual([record["row"]["checkedAt"] for record in first.values()], ["2030-01-01T00:00:00Z", "2030-01-01T00:02:00Z"])
            self.assertEqual([record["row"]["checkedAt"] for record in second.values()], ["2030-01-01T00:00:00Z", "2030-01-01T00:04:00Z"])
            self.assertEqual(len(set(first) - present), 1)
            self.assertEqual(len(present - set(first)), 1)

    def test_usage_quota_plateaus_are_isolated_by_account_plan_and_reset_cycle(self):
        with self.account_directory() as directory:
            history, quota, tokens = (directory / name for name in ("history.jsonl", "quota.jsonl", "tokens.jsonl"))
            history.write_text("", encoding="utf-8")
            tokens.write_text("", encoding="utf-8")
            rows = [{
                "checkedAt": f"2030-01-01T00:0{index}:00Z", "accountSlotId": "account-a", "accountLabel": "A",
                "windows": {"5h": {"usedPercent": 5, "plan": "plus" if index < 3 else "pro", "resetAt": "2030-01-01T05:00:00Z" if index < 4 else "2030-01-01T10:00:00Z"}},
            } for index in range(5)] + [{
                "checkedAt": f"2030-01-01T00:0{index}:30Z", "accountSlotId": "account-b", "accountLabel": "B",
                "windows": {"5h": {"usedPercent": 5, "plan": "plus", "resetAt": "2030-01-01T05:00:00Z"}},
            } for index in range(3)]
            monitor_history.write_quota_history(quota, rows)
            store = UsageDataStore(history, quota, tokens, "machine-a", lambda slot: "usage-a" if slot == "account-a" else "usage-b", threading.Lock())

            records, _ = store.snapshot()
            selected = [(record["row"]["sync"]["accountId"], record["row"]["checkedAt"]) for record in records.values()]

            self.assertEqual(selected, [("usage-a", "2030-01-01T00:00:00Z"), ("usage-a", "2030-01-01T00:02:00Z"), ("usage-a", "2030-01-01T00:03:00Z"), ("usage-a", "2030-01-01T00:04:00Z"), ("usage-b", "2030-01-01T00:00:30Z"), ("usage-b", "2030-01-01T00:02:30Z")])

    def test_usage_cost_intervals_reconcile_coarse_and_fine_observations(self):
        def interval(machine, start, end, cost, started_at, checked_at):
            return {
                "recordType": "costInterval", "startedAt": started_at, "checkedAt": checked_at, "window": "5h", "accountSlotId": "account-a", "accountLabel": "A",
                "startPercent": start, "endPercent": end, "plan": "plus", "planMultiplier": 1, "resetAt": "2030-01-01T05:00:00Z", "modelCostsUsd": {"gpt-5.5": cost},
                "sync": {"originMachineId": machine, "accountId": "usage-account", "recordId": f"{machine}:{checked_at}"},
            }

        merged = aggregate_cost_intervals([
            interval("machine-a", 0, 2, 2, "2030-01-01T00:00:00Z", "2030-01-01T00:03:00Z"),
            interval("machine-b", 0, 1, 1, "2030-01-01T00:00:30Z", "2030-01-01T00:02:00Z"),
            interval("machine-b", 1, 2, 1, "2030-01-01T00:02:00Z", "2030-01-01T00:03:30Z"),
        ])

        self.assertEqual([(row["deltaPercent"], row["deltaCostUsd"]) for row in merged], [(1, 2), (1, 2)])
        self.assertEqual([row["checkedAt"] for row in merged], ["2030-01-01T00:02:00Z", "2030-01-01T00:03:00Z"])

    def test_usage_cost_intervals_combine_low_cost_and_isolate_reset_cycles(self):
        rows = []
        for machine, reset_at in (("machine-a", "2030-01-01T05:00:00Z"), ("machine-b", "2030-01-01T05:00:30Z"), ("machine-c", "2030-01-02T05:00:00Z")):
            rows.append({
                "recordType": "costInterval", "startedAt": "2030-01-01T00:00:00Z", "checkedAt": f"2030-01-01T00:0{len(rows) + 1}:00Z", "window": "5h", "accountSlotId": "account-a", "accountLabel": "A",
                "startPercent": 0, "endPercent": 1, "plan": "plus", "planMultiplier": 1, "resetAt": reset_at, "modelCostsUsd": {"gpt-5.5": 0.006},
                "sync": {"originMachineId": machine, "accountId": "usage-account", "recordId": machine},
            })

        merged = aggregate_cost_intervals(rows)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["deltaCostUsd"], 0.012)

    def test_usage_cost_interval_reset_jitter_grouping_is_order_independent(self):
        def interval(machine, reset_at, cost):
            return {
                "recordType": "costInterval", "startedAt": "2030-01-01T00:00:00Z", "checkedAt": "2030-01-01T00:05:00Z", "window": "5h", "accountSlotId": "account-a", "accountLabel": "A",
                "startPercent": 0, "endPercent": 1, "plan": "plus", "planMultiplier": 1, "resetAt": reset_at, "modelCostsUsd": {"gpt-5.5": cost},
                "sync": {"originMachineId": machine, "accountId": "usage-account", "recordId": machine},
            }

        rows = [interval("machine-a", "2030-01-01T05:00:00Z", 0.006), interval("machine-b", "2030-01-01T05:00:50Z", 0.007), interval("machine-c", "2030-01-01T05:01:40Z", 0.02)]

        forward, reverse = aggregate_cost_intervals(rows), aggregate_cost_intervals(list(reversed(rows)))

        self.assertEqual(sorted(row["deltaCostUsd"] for row in forward), sorted(row["deltaCostUsd"] for row in reverse))
        self.assertEqual(sorted(row["deltaCostUsd"] for row in forward), [0.013, 0.02])

    def test_usage_cost_interval_sweep_normalizes_each_source_row_once(self):
        rows = [{
            "recordType": "costInterval", "startedAt": "2030-01-01T00:00:00Z", "checkedAt": "2030-01-01T00:05:00Z", "window": "5h", "accountSlotId": "account-a", "accountLabel": "A",
            "startPercent": index / 10, "endPercent": (index + 1) / 10, "plan": "plus", "planMultiplier": 1, "resetAt": "2030-01-01T05:00:00Z", "modelCostsUsd": {"gpt-5.5": 0.02},
            "sync": {"originMachineId": "machine-a", "accountId": "usage-account", "recordId": str(index)},
        } for index in range(1000)]

        with mock.patch("monitor_usage_sync.coerce_float", wraps=monitor_common.coerce_float) as coerce:
            merged = aggregate_cost_intervals(rows)

        self.assertEqual(len(merged), len(rows))
        self.assertLessEqual(coerce.call_count, len(rows) * 5)

    def test_usage_token_merge_uses_timestamp_then_total_then_hash(self):
        base = {"sessionId": "session-a", "startedAt": "2030-01-01T00:00:00Z", "accountSlotId": "a", "accountLabel": "A", "cost": {}, "byModel": {}, "sync": {"accountId": "usage-a"}}
        older = base | {"updatedAt": "2030-01-01T00:01:00Z", "tokens": {"totalTokens": 100}}
        newer = base | {"updatedAt": "2030-01-01T00:02:00Z", "tokens": {"totalTokens": 90}}
        dominant = base | {"updatedAt": "2030-01-01T00:02:00Z", "tokens": {"totalTokens": 120}}

        merged, conflicts = merge_token_rows([older, newer, dominant])

        self.assertEqual(merged[0]["tokens"]["totalTokens"], 120)
        self.assertEqual(conflicts, [])

    def test_usage_store_initial_migration_syncs_quota_and_tokens_but_not_legacy_cost(self):
        with self.account_directory() as directory:
            history, quota, tokens = directory / "history.jsonl", directory / "quota.jsonl", directory / "tokens.jsonl"
            append_history(history, {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 1, "deltaCostUsd": 2, "costPercentRatio": 2})
            monitor_history.write_quota_history(quota, [{"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "a", "accountLabel": "A", "windows": {"5h": {"usedPercent": 1}}}])
            monitor_history.write_token_session_history(tokens, [{"sessionId": "s", "updatedAt": "2030-01-01T00:00:00Z", "accountSlotId": "a", "accountLabel": "A", "tokens": empty_token_totals()}])
            store = UsageDataStore(history, quota, tokens, "machine-a", lambda _: "usage-a", threading.Lock())

            records, _ = store.snapshot()

            self.assertEqual(sorted(record["kind"] for record in records.values()), ["quota", "token"])
            self.assertTrue(load_history(history)[0]["sync"]["localOnly"])
            self.assertEqual(monitor_history.load_quota_history(quota)[0]["sync"]["accountId"], "usage-a")

    def test_usage_remote_apply_changes_only_sync_cache_and_builds_separate_merged_dataset(self):
        with self.account_directory() as directory:
            history, quota, tokens = directory / "history.jsonl", directory / "quota.jsonl", directory / "tokens.jsonl"
            history.write_text("", encoding="utf-8")
            tokens.write_text("", encoding="utf-8")
            monitor_history.write_quota_history(quota, [{"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "a", "accountLabel": "A", "windows": {"5h": {"usedPercent": 1}}}])
            store = UsageDataStore(history, quota, tokens, "machine-a", lambda _: "usage-a", threading.Lock(), lambda _usage, _slot, _label: ("a", "A"))
            store.snapshot()
            local_bytes = history.read_bytes(), quota.read_bytes(), tokens.read_bytes()
            remote = {"checkedAt": "2030-01-01T00:01:00Z", "windows": {"5h": {"usedPercent": 2, "resetAt": None, "plan": "unknown"}}, "sync": {"version": 1, "originMachineId": "machine-b", "accountId": "usage-a", "recordId": "remote-quota"}}

            store.apply([{"action": "upsert", "key": record_key("quota", remote), "record": {"kind": "quota", "row": remote}}], operation_origin="machine-b")
            local, merged = store.datasets("local"), store.datasets("merged")

            self.assertEqual((history.read_bytes(), quota.read_bytes(), tokens.read_bytes()), local_bytes)
            self.assertEqual(len(local[1]), 1)
            self.assertEqual(len(merged[1]), 2)
            self.assertEqual(merged[1][-1]["accountSlotId"], "a")
            self.assertTrue(store.cache_path.exists())

            with mock.patch.object(store, "_load") as load, mock.patch("monitor_usage_sync.merge_cost_rows") as merge_cost, mock.patch("monitor_usage_sync.merge_quota_rows") as merge_quota, mock.patch("monitor_usage_sync.merge_token_rows") as merge_tokens:
                store.datasets("local")
                store.datasets("merged")
            load.assert_not_called()
            merge_cost.assert_not_called()
            merge_quota.assert_not_called()
            merge_tokens.assert_not_called()

            with mock.patch.object(store, "_materialize_datasets") as materialize:
                store.apply([{"action": "upsert", "key": record_key("quota", remote), "record": {"kind": "quota", "row": remote}}], operation_origin="machine-b")
            materialize.assert_not_called()

    def test_usage_merged_dataset_temporarily_ignores_remote_accounts_missing_locally(self):
        with self.account_directory() as directory:
            history, quota, tokens = directory / "history.jsonl", directory / "quota.jsonl", directory / "tokens.jsonl"
            for path in (history, quota, tokens):
                path.write_text("", encoding="utf-8")
            accounts = {"usage-a": ("a", "Local A")}
            revision = [1]
            store = UsageDataStore(history, quota, tokens, "machine-a", lambda _: "usage-a", threading.Lock(), lambda account_id, _slot, _label: accounts.get(account_id), account_revision_resolver=lambda: revision[0])
            known = {"checkedAt": "2030-01-01T00:01:00Z", "windows": {"5h": {"usedPercent": 2}}, "sync": {"version": 1, "originMachineId": "machine-b", "accountId": "usage-a", "recordId": "known"}}
            unknown = {"checkedAt": "2030-01-01T00:02:00Z", "windows": {"5h": {"usedPercent": 3}}, "sync": {"version": 1, "originMachineId": "machine-b", "accountId": "usage-b", "recordId": "unknown"}}

            store.apply([{"action": "upsert", "key": record_key("quota", row), "record": {"kind": "quota", "row": row}} for row in (known, unknown)], operation_origin="machine-b")

            self.assertEqual(store.datasets("local")[1], [])
            self.assertEqual([(row["checkedAt"], row["accountSlotId"], row["accountLabel"]) for row in store.datasets("merged")[1]], [("2030-01-01T00:01:00Z", "a", "Local A")])
            self.assertEqual(len(json.loads(store.cache_path.read_text(encoding="utf-8"))["records"]), 2)
            cache_bytes = store.cache_path.read_bytes()

            accounts["usage-b"] = ("b", "Local B")
            revision[0] += 1

            self.assertEqual([(row["checkedAt"], row["accountSlotId"], row["accountLabel"]) for row in store.datasets("merged")[1]], [("2030-01-01T00:01:00Z", "a", "Local A"), ("2030-01-01T00:02:00Z", "b", "Local B")])
            self.assertEqual(store.cache_path.read_bytes(), cache_bytes)

    def test_usage_cache_migration_removes_prior_remote_rows_from_local_raw_files(self):
        with self.account_directory() as directory:
            history, quota, tokens = directory / "history.jsonl", directory / "quota.jsonl", directory / "tokens.jsonl"
            history.write_text("", encoding="utf-8")
            tokens.write_text("", encoding="utf-8")
            rows = [
                {
                    "checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "a", "accountLabel": "Local", "windows": {"5h": {"usedPercent": 1}},
                    "sync": {"version": 1, "originMachineId": "machine-a", "accountId": "usage-a", "recordId": "local"},
                },
                {
                    "checkedAt": "2030-01-01T00:01:00Z", "accountSlotId": "cloud-a", "accountLabel": "Remote managed name", "windows": {"5h": {"usedPercent": 2}},
                    "sync": {"version": 1, "originMachineId": "machine-b", "accountId": "usage-a", "recordId": "remote"},
                },
            ]
            monitor_history.write_quota_history(quota, rows)
            store = UsageDataStore(history, quota, tokens, "machine-a", lambda _: "usage-a", threading.Lock(), lambda account_id, _slot, _label: ("a", "Local") if account_id == "usage-a" else None)

            local = store.normalize_local()

            self.assertTrue(store.needs_remote_rebuild)
            self.assertEqual([row["checkedAt"] for row in local[1]], ["2030-01-01T00:00:00Z"])
            self.assertEqual([row["checkedAt"] for row in store.datasets("merged")[1]], ["2030-01-01T00:00:00Z", "2030-01-01T00:01:00Z"])
            self.assertNotIn("Remote managed name", store.cache_path.read_text(encoding="utf-8"))

    def test_usage_cache_namespaces_same_record_key_by_remote_machine(self):
        with self.account_directory() as directory:
            history, quota, tokens = directory / "history.jsonl", directory / "quota.jsonl", directory / "tokens.jsonl"
            for path in (history, quota, tokens):
                path.write_text("", encoding="utf-8")
            store = UsageDataStore(history, quota, tokens, "machine-a", lambda _: "usage-a", threading.Lock())
            for machine in ("machine-b", "machine-c"):
                row = {"checkedAt": "2030-01-01T00:00:00Z", "windows": {"5h": {"usedPercent": 1, "resetAt": None, "plan": "unknown"}}, "sync": {"version": 1, "originMachineId": machine, "accountId": "usage-a", "recordId": "shared"}}
                store.apply([{"action": "upsert", "key": record_key("quota", row), "record": {"kind": "quota", "row": row}}], operation_origin=machine)

            self.assertEqual(len(json.loads(store.cache_path.read_text(encoding="utf-8"))["records"]), 2)
            store.apply([{"action": "delete", "key": "quota:shared"}], operation_origin="machine-b")
            self.assertEqual(json.loads(store.cache_path.read_text(encoding="utf-8"))["records"][0]["sourceMachineId"], "machine-c")

    def test_usage_pack_inventory_drops_hash_when_its_cached_records_are_missing(self):
        with self.account_directory() as directory:
            history, quota, tokens = directory / "history.jsonl", directory / "quota.jsonl", directory / "tokens.jsonl"
            for path in (history, quota, tokens):
                path.write_text("", encoding="utf-8")
            store = UsageDataStore(history, quota, tokens, "machine-a", lambda _: "usage-a", threading.Lock())
            records = {
                "pack-1": [{"key": "quota:one", "record": {"kind": "quota", "row": {"checkedAt": "2030-01-01T00:00:00Z", "windows": {}, "sync": {"originMachineId": "machine-b", "accountId": "usage-b", "recordId": "one"}}}}],
                "pack-2": [{"key": "quota:two", "record": {"kind": "quota", "row": {"checkedAt": "2030-01-01T00:01:00Z", "windows": {}, "sync": {"originMachineId": "machine-b", "accountId": "usage-b", "recordId": "two"}}}}],
            }
            manifest = {"pack-1": "1" * 64, "pack-2": "2" * 64}
            store.apply_pack_snapshot("machine-b", manifest, records)
            payload = json.loads(store.cache_path.read_text(encoding="utf-8"))
            payload["records"] = [entry for entry in payload["records"] if entry.get("sourcePackId") != "pack-1"]
            store.cache_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")

            recovered = UsageDataStore(history, quota, tokens, "machine-a", lambda _: "usage-a", threading.Lock(), cache_path=store.cache_path)

            self.assertEqual(recovered.pack_hashes("machine-b"), {"pack-2": "2" * 64})
            self.assertTrue(recovered.needs_remote_rebuild)

    def test_usage_packs_are_stable_hash_buckets_split_by_compressed_length(self):
        keys, candidate = [], 0
        while len(keys) < 80:
            key = f"quota:bucket-test-{candidate}"
            if hashlib.sha256(key.encode()).hexdigest()[0] == "a":
                keys.append(key)
            candidate += 1
        records = {key: {"kind": "quota", "row": {"blob": "".join(hashlib.sha256(f"{key}:{index}".encode()).hexdigest() for index in range(48))}} for key in keys}

        packs = CloudManager._usage_record_packs("machine-a", records)

        self.assertGreater(len(packs), 1)
        self.assertTrue(all(pack_id.startswith("a-") for pack_id in packs))
        self.assertTrue(all(pack["bytes"] <= USAGE_PACK_MAX_BYTES for pack in packs.values()))
        self.assertEqual(sum(pack["records"] for pack in packs.values()), len(records))

    def test_usage_sync_interval_is_thirty_minutes(self):
        self.assertEqual(USAGE_SYNC_INTERVAL_SECONDS, 1800)

    def test_usage_auto_sync_retries_at_one_five_and_fifteen_minutes(self):
        with self.account_directory() as directory:
            cloud = CloudManager(directory / "private", SkillManager(directory / "codex", directory / "private", directory / "gemini"), None)
            cloud._config["webdav"].update({"enabled": True, "usageDataAutoSync": True})
            cloud._usage_data = object()
            cloud._next_usage_sync_at = 0
            with mock.patch.object(cloud, "sync_usage_data", side_effect=CloudError("offline")) as sync:
                self.assertFalse(cloud._auto_usage_sync_if_due(0))
                self.assertEqual(cloud._next_usage_sync_at, 60)
                self.assertFalse(cloud._auto_usage_sync_if_due(59))
                self.assertFalse(cloud._auto_usage_sync_if_due(60))
                self.assertEqual(cloud._next_usage_sync_at, 360)
                self.assertFalse(cloud._auto_usage_sync_if_due(360))
                self.assertEqual(cloud._next_usage_sync_at, 1260)

            self.assertEqual(sync.call_count, 3)

    def test_new_delta_processing_records_source_interval_for_reaggregation(self):
        state = {"windows": {}}
        first = event_sample("2030-01-01T00:00:00Z", five_hour=0, cost=0, plan="plus", five_hour_reset="2030-01-01T05:00:00Z")
        second = event_sample("2030-01-01T00:01:30Z", five_hour=1, cost=2, plan="plus", five_hour_reset="2030-01-01T05:00:00Z")
        for row in (first, second):
            row.update({"originMachineId": "machine-a", "usageAccountId": "usage-a"})

        process_sample_delta_events(state, first, [])
        events = process_sample_delta_events(state, second, [])
        intervals = state["_pendingCostIntervals"]

        self.assertEqual(len(events), 1)
        self.assertEqual(len(intervals), 1)
        self.assertEqual((intervals[0]["startedAt"], intervals[0]["startPercent"], intervals[0]["endPercent"]), ("2030-01-01T00:00:00Z", 0, 1))
        self.assertEqual((intervals[0]["modelCostsUsd"]["gpt-5.5"], intervals[0]["deltaCostUsd"]), (2, 2))

    def test_usage_cloud_pack_manifest_fetches_each_missing_hash_and_updates_only_changed_buckets(self):
        class Client:
            def __init__(self):
                self.files, self.revision = {}, 0

            def ensure_directories(self, path):
                pass

            def put(self, path, data, etag=None, create=False):
                if create and path in self.files or etag is not None and (path not in self.files or self.files[path][1] != etag):
                    raise CloudError("HTTP 412", 409)
                self.revision += 1
                current = f'"{self.revision}"'
                self.files[path] = (data, current)
                return current

            def get(self, path):
                if path not in self.files:
                    raise CloudError("HTTP 404", 502)
                return self.files[path]

            def list_details(self, path):
                prefix = path.rstrip("/") + "/"
                return [{"name": name[len(prefix):], "etag": value[1]} for name, value in self.files.items() if name.startswith(prefix) and "/" not in name[len(prefix):]]

            def list(self, path):
                return [item["name"] for item in self.list_details(path)]

            def delete(self, path, etag=None):
                self.files.pop(path, None)

        class Store:
            def __init__(self, records):
                self.records, self.received, self.packs = records, [], {}

            def snapshot(self, necessary_only=True):
                self.necessary_only = necessary_only
                return self.records, set(self.records)

            def apply(self, operations, checkpoint_origin=None, operation_origin=None):
                self.received.extend(operations)
                return []

            def pack_hashes(self, machine_id):
                return self.packs.get(machine_id, {}).copy()

            def apply_pack_snapshot(self, machine_id, manifest, downloaded, replace_all=False):
                self.received.extend({"action": "upsert", **entry} for entries in downloaded.values() for entry in entries)
                self.packs[machine_id] = manifest.copy()
                return []

        with self.account_directory() as directory:
            encryption_hash = passphrase_hash("passphrase", "https://example.test", "user")
            client, box, clouds = Client(), CryptoBox(encryption_hash, CryptoBox.descriptor(encryption_hash)), []
            for name in ("a", "b"):
                root = directory / name
                cloud = CloudManager(root / "private", SkillManager(root / "codex", root / "private", root / "gemini"), None)
                cloud._config["webdav"]["enabled"] = True
                cloud._state["conditionalWritesVerified"] = True
                cloud._connection = lambda initialize=False: (client, box)
                clouds.append(cloud)
            records = {
                f"quota:quota-{index}": {"kind": "quota", "row": {
                    "checkedAt": f"2030-01-01T00:{index:02}:00Z", "windows": {"5h": {"usedPercent": index}},
                    "sync": {"originMachineId": clouds[0].machine_id, "accountId": "usage-a", "recordId": f"quota-{index}"},
                }}
                for index in range(32)
            }
            first_store, second_store = Store(records), Store({})
            clouds[0].configure_usage_sync(first_store)
            clouds[1].configure_usage_sync(second_store)

            first = clouds[0].sync_usage_data()
            second = clouds[1].sync_usage_data()
            files_after_snapshot = set(client.files)
            unchanged = clouds[0].sync_usage_data()
            manifest = clouds[0]._usage_pointer(client, box, clouds[0].machine_id)[0]["packs"]
            retained_pack = sorted(manifest)[-1]
            second_store.packs[clouds[0].machine_id] = {retained_pack: manifest[retained_pack]}
            second_store.received.clear()
            repair = clouds[1]._fetch_usage_data(client, box)
            self.assertEqual(repair["payloadsDownloaded"], len(manifest) - 1)
            self.assertEqual(second_store.packs[clouds[0].machine_id], manifest)
            first_store.records["quota:quota-new"] = {"kind": "quota", "row": {"checkedAt": "2030-01-01T01:00:00Z", "windows": {"5h": {"usedPercent": 33}}, "sync": {"originMachineId": clouds[0].machine_id, "accountId": "usage-a", "recordId": "quota-new"}}}
            incremental = clouds[0].sync_usage_data()

            self.assertTrue(first["published"]["fullSnapshot"])
            self.assertGreater(first["published"]["packs"], 1)
            self.assertGreater(len(second_store.received), 0)
            self.assertEqual(unchanged["published"]["uploaded"], 0)
            self.assertEqual(unchanged["published"]["packsUploaded"], 0)
            self.assertNotEqual(files_after_snapshot, set(client.files))
            self.assertEqual(incremental["published"]["uploaded"], 1)
            self.assertEqual(incremental["published"]["packsUploaded"], 1)
            full = clouds[0]._push_usage_data(True)
            self.assertTrue(full["fullSnapshot"])
            self.assertEqual(full["uploaded"], len(first_store.records))
            self.assertEqual(full["packsUploaded"], full["packs"])
            self.assertFalse(first_store.necessary_only)
            received_before_full_fetch = len(second_store.received)
            full_fetch = clouds[1]._fetch_usage_data(client, box, True)
            self.assertEqual(full_fetch["payloadsDownloaded"], full["packs"])
            self.assertEqual(len(second_store.received), received_before_full_fetch + len(first_store.records))

            legacy_machine = "legacy-machine"
            legacy_records = [{
                "key": "quota:legacy-a", "record": {"kind": "quota", "row": {
                    "checkedAt": "2029-12-31T23:58:00Z", "windows": {"5h": {"usedPercent": 1}},
                    "sync": {"originMachineId": legacy_machine, "accountId": "usage-legacy", "recordId": "legacy-a"},
                }},
            }]
            checkpoint_id, _ = clouds[0]._put_usage_payload(client, box, "checkpoints", legacy_machine, {"version": 1, "machineId": legacy_machine, "sequence": 0, "records": legacy_records})
            chunk_id, _ = clouds[0]._put_usage_payload(client, box, "chunks", legacy_machine, {
                "version": 1, "machineId": legacy_machine, "sequence": 1, "parentChunkId": None,
                "operations": [{"action": "upsert", "key": "quota:legacy-b", "record": {"kind": "quota", "row": {
                    "checkedAt": "2029-12-31T23:59:00Z", "windows": {"5h": {"usedPercent": 2}},
                    "sync": {"originMachineId": legacy_machine, "accountId": "usage-legacy", "recordId": "legacy-b"},
                }}}],
            })
            legacy_pointer = {"version": 1, "machineId": legacy_machine, "sequence": 1, "checkpointId": checkpoint_id, "headChunkId": chunk_id}
            client.put(clouds[0]._usage_pointer_path(legacy_machine), box.encrypt(f"usage-pointer:{legacy_machine}", json.dumps(legacy_pointer, separators=(",", ":")).encode()), create=True)

            migration = clouds[1]._fetch_usage_data(client, box)
            migrated_pointer = clouds[1]._usage_pointer(client, box, legacy_machine)[0]

            self.assertEqual(migration["machinesMigrated"], 1)
            self.assertEqual(migrated_pointer["version"], 2)
            self.assertEqual(migrated_pointer["recordCount"], 2)
            self.assertEqual(second_store.packs[legacy_machine], migrated_pointer["packs"])
            self.assertFalse(any(f"/{kind}/{legacy_machine}/" in path for path in client.files for kind in ("chunks", "checkpoints")))


def sample(checked_at, five_hour=None, seven_day=None, five_hour_reset=None, seven_day_reset=None):
    token_usage = {"totals": empty_token_totals()}
    windows = {}
    if five_hour is not None:
        windows["5h"] = {"usedPercent": five_hour, "resetAt": five_hour_reset, "path": "$.rate_limit.primary_window"}
    if seven_day is not None:
        windows["7d"] = {"usedPercent": seven_day, "resetAt": seven_day_reset, "path": "$.rate_limit.secondary_window"}
    return {"checkedAt": checked_at, "windows": windows, "errors": {}, "tokenUsage": token_usage, "tokenDelta": empty_token_totals()}


def event_sample(checked_at, five_hour=None, seven_day=None, cost=0, plan="unknown", five_hour_reset=None, seven_day_reset=None):
    row = sample(checked_at, five_hour=five_hour, seven_day=seven_day, five_hour_reset=five_hour_reset, seven_day_reset=seven_day_reset)
    for window in row["windows"].values():
        window["plan"] = plan
        window["planMultiplier"] = {"plus": 1.0, "pro_lite": 5.0, "pro": 20.0, "unknown": 1.0}[plan]
    row["cost"] = {"inputCostUsd": 0, "cachedInputCostUsd": 0, "outputCostUsd": 0, "totalCostUsd": cost}
    return row


def remote_identity(user_id, account_id=None, email=None, plan_type="plus"):
    return {"user_id": user_id, "account_id": account_id or user_id, "email": email or f"{user_id}@example.test", "plan_type": plan_type}


def with_remote_identity(row, user_id, account_id=None, email=None, plan_type="plus"):
    row["remoteUsage"] = {"rawResponse": remote_identity(user_id, account_id, email, plan_type)}
    return row


def with_auth_identity(row, account_id):
    row["remoteUsage"] = {"authIdentity": {"account_id": account_id}}
    return row


if __name__ == "__main__":
    unittest.main()
