import hashlib
import json
import shutil
import threading
import time
import unittest
import uuid
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from unittest import mock

import build_release
import monitor_auto_update
from monitor_auto_update import AutoUpdateError, AutoUpdater, DATA_MIGRATION_STATE_FILENAME, RELEASE_VERSION_URL, RUNTIME_FILE_URL, installed_data_contract_version, installed_version, migrate_history_data, parse_version, restart_process, validate_manifest


class Response(BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class Opener:
    def __init__(self, responses):
        self.responses = responses
        self.requests = []

    def open(self, request, timeout=None):
        self.requests.append((request.full_url, timeout))
        value = self.responses[request.full_url]
        return Response(value)


def descriptor(data: bytes) -> dict:
    return {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}


@contextmanager
def temporary_directory():
    path = Path(__file__).parent / f".auto-update-test-{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


class AutoUpdateTests(unittest.TestCase):
    def test_history_data_migration_rewrites_every_store_and_only_runs_once(self):
        with temporary_directory() as data_home:
            history = data_home / "custom.jsonl"
            quota = data_home / "custom.quota.jsonl"
            sessions = data_home / "custom.sessions.jsonl"
            ledger = data_home / "custom.ledger.jsonl"
            samples = data_home / "custom.samples.jsonl"
            history.write_text('{"window":"5h","delta":[{"checkedAt":"2026-01-01T00:00:00Z","deltaPercent":1,"deltaCostUsd":0.1}]}', encoding="utf-8")
            quota.write_text('{"checkedAt":"2026-01-01T00:00:00Z","windows":{"5h":{"used_percent":4,"usedPercent":4}}}', encoding="utf-8")
            sessions.write_text('{"sessionId":"s1","startedAt":"2026-01-01T00:00:00Z","tokens":{"input_tokens":2}}', encoding="utf-8")
            ledger.write_text('{"schemaVersion":1,"recordType":"priceEpoch","pricingId":"p1"}', encoding="utf-8")
            samples.write_text('{"sample":1}{"sample":2}', encoding="utf-8")
            (data_home / "usage_monitor_state.json").write_text('{"runCostUsd":3,"windows":{"5h":{"baselineCostUsd":2,"baselinePercent":4}},"lastSample":{"costDelta":{"totalCostUsd":1}}}', encoding="utf-8")
            (data_home / "usage_monitor_dashboard_cache.json").write_text('{"version":2}', encoding="utf-8")
            (data_home / "usage_monitor_sync_cache.json").write_text('{"version":2}', encoding="utf-8")
            (data_home / "cloud-state.json").write_text('{"usage":{"published":{"old":"pack"},"remote":{"machine":"cursor"},"lastSuccessAt":"old","lastAttemptAt":"old","lastFullVerificationAt":"old","failure":{"message":"old"}}}', encoding="utf-8")

            self.assertEqual(migrate_history_data(data_home, history, quota, sessions, ledger, samples), [1, 2, 3, 4])
            self.assertEqual(json.loads((data_home / DATA_MIGRATION_STATE_FILENAME).read_text(encoding="utf-8")), {"dataContractVersion": 4})
            self.assertEqual([json.loads(line) for line in samples.read_text(encoding="utf-8").splitlines()], [{"sample": 1}, {"sample": 2}])
            self.assertIn('"accountSlotId":"unknown"', quota.read_text(encoding="utf-8"))
            self.assertFalse(history.exists())
            self.assertFalse(sessions.exists())
            self.assertIn('"recordType":"legacyBaseline"', ledger.read_text(encoding="utf-8"))
            self.assertFalse((data_home / "usage_monitor_dashboard_cache.json").exists())
            self.assertFalse((data_home / "usage_monitor_sync_cache.json").exists())
            self.assertEqual(json.loads((data_home / "usage_monitor_state.json").read_text(encoding="utf-8")), {"windows": {"5h": {}}, "lastSample": {}})
            cloud_usage = json.loads((data_home / "cloud-state.json").read_text(encoding="utf-8"))["usage"]
            self.assertEqual(cloud_usage, {"published": {"old": "pack"}, "remote": {}, "lastSuccessAt": None, "lastAttemptAt": None, "failure": None})
            snapshots = {path: path.read_bytes() for path in (quota, ledger, samples, data_home / "usage_monitor_state.json", data_home / "cloud-state.json")}
            self.assertEqual(migrate_history_data(data_home, history, quota, sessions, ledger, samples), [])
            self.assertEqual(snapshots, {path: path.read_bytes() for path in snapshots})

    def test_successful_update_defers_data_migration_until_new_runtime_startup(self):
        with temporary_directory() as temporary:
            runtime, data_home = temporary / "runtime", temporary / "data"
            runtime.mkdir()
            data_home.mkdir()
            (runtime / "version.json").write_text('{"version":"1.5.0","dataContractVersion":1}', encoding="utf-8")
            (runtime / "monitor.py").write_bytes(b"old")
            (data_home / DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":1}', encoding="utf-8")
            (data_home / "usage_monitor_history.jsonl").write_text('{"window":"5h","delta":[]}', encoding="utf-8")
            files = {"monitor.py": b"new", "version.json": b'{"version":"1.5.1","dataContractVersion":2}\n'}
            responses = {RELEASE_VERSION_URL: json.dumps({"version": "1.5.1", "files": {name: descriptor(data) for name, data in files.items()}}).encode(), **{RUNTIME_FILE_URL.format(name=name): data for name, data in files.items()}}

            self.assertTrue(AutoUpdater(runtime, lambda: True, lambda: Opener(responses)).check_for_update())

            self.assertEqual(json.loads((data_home / DATA_MIGRATION_STATE_FILENAME).read_text(encoding="utf-8")), {"dataContractVersion": 1})
            self.assertTrue((data_home / "usage_monitor_history.jsonl").exists())
            self.assertEqual(installed_data_contract_version(runtime), 2)

    def test_history_data_migration_does_not_advance_after_invalid_history(self):
        with temporary_directory() as data_home:
            paths = [data_home / name for name in ("history", "quota", "sessions", "ledger", "samples")]
            paths[4].write_text('{"valid":true}\nnot-json\n', encoding="utf-8")
            with self.assertRaises(json.JSONDecodeError):
                migrate_history_data(data_home, *paths)
            self.assertFalse((data_home / DATA_MIGRATION_STATE_FILENAME).exists())
            self.assertEqual(paths[4].read_text(encoding="utf-8"), '{"valid":true}\nnot-json\n')

    def test_history_data_migration_runs_each_intermediate_version_and_checkpoints(self):
        with temporary_directory() as data_home:
            paths = [data_home / name for name in ("history", "quota", "sessions", "ledger", "samples")]
            (data_home / DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":1}\n', encoding="utf-8")
            applied = []
            with mock.patch.object(monitor_auto_update, "DATA_CONTRACT_VERSION", 3), mock.patch.object(monitor_auto_update, "DATA_MIGRATIONS", {2: lambda _paths: applied.append(2), 3: lambda _paths: applied.append(3)}):
                self.assertEqual(migrate_history_data(data_home, *paths, target_version=3), [2, 3])
            self.assertEqual(applied, [2, 3])
            self.assertEqual(json.loads((data_home / DATA_MIGRATION_STATE_FILENAME).read_text(encoding="utf-8")), {"dataContractVersion": 3})

    def test_version_parsing_and_installed_version_fallbacks(self):
        self.assertEqual(parse_version("1.5.0"), (1, 5, 0))
        for value in ("v1.5.0", "1.5", "01.5.0", "1.5.0-beta", None):
            with self.subTest(value=value), self.assertRaises(AutoUpdateError):
                parse_version(value)
        with temporary_directory() as runtime:
            self.assertEqual(installed_version(runtime), "1.5.0")
            (runtime / "package.json").write_text('{"version":"1.6.0"}', encoding="utf-8")
            self.assertEqual(installed_version(runtime), "1.6.0")
            (runtime / "version.json").write_text('{"version":"1.7.0"}', encoding="utf-8")
            self.assertEqual(installed_version(runtime), "1.7.0")
            (runtime / "version.json").write_text('{"version":"1.7.0","dataContractVersion":3}', encoding="utf-8")
            self.assertEqual(installed_data_contract_version(runtime), 3)

    def test_manifest_rejects_unsafe_duplicate_and_invalid_entries(self):
        valid = {"version": "1.5.1", "files": {"monitor.py": descriptor(b"code")}}
        self.assertEqual(validate_manifest(valid)[0], "1.5.1")
        invalid = [
            {"version": "1.5.1", "files": {"../monitor.py": descriptor(b"code")}},
            {"version": "1.5.1", "files": {"A.py": descriptor(b"a"), "a.py": descriptor(b"b")}},
            {"version": "1.5.1", "files": {"monitor.py": {"size": -1, "sha256": "0" * 64}}},
            {"version": "1.5.1", "files": {"monitor.py": {"size": 4, "sha256": "invalid"}}},
            {"version": "1.5.1", "files": {}},
        ]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(AutoUpdateError):
                validate_manifest(value)

    def test_newer_release_updates_renamed_standalone_folder_and_preserves_extra_files(self):
        with temporary_directory() as temporary:
            runtime = temporary / "user-renamed-folder"
            runtime.mkdir()
            (runtime / "version.json").write_text('{"version":"1.5.0"}', encoding="utf-8")
            (runtime / "monitor.py").write_bytes(b"old")
            (runtime / "personal.txt").write_bytes(b"keep")
            files = {"monitor.py": b"new", "version.json": b'{"version":"1.5.1","dataContractVersion":2}\n', "README.md": b"new readme"}
            manifest = json.dumps({"version": "1.5.1", "files": {name: descriptor(data) for name, data in files.items()}}).encode()
            responses = {RELEASE_VERSION_URL: manifest, **{RUNTIME_FILE_URL.format(name=name): data for name, data in files.items()}}
            updater = AutoUpdater(runtime, lambda: True, lambda: Opener(responses))

            self.assertTrue(updater.check_for_update())
            self.assertEqual((runtime / "monitor.py").read_bytes(), b"new")
            self.assertEqual((runtime / "README.md").read_bytes(), b"new readme")
            self.assertEqual((runtime / "personal.txt").read_bytes(), b"keep")
            self.assertEqual(installed_version(runtime), "1.5.1")
            self.assertEqual(installed_data_contract_version(runtime), 2)
            self.assertFalse(any(path.name.startswith(".codex-monitor-update-") for path in runtime.iterdir()))

    def test_equal_older_and_bad_downloads_do_not_change_runtime(self):
        with temporary_directory() as runtime:
            (runtime / "version.json").write_text('{"version":"1.5.0"}', encoding="utf-8")
            (runtime / "monitor.py").write_bytes(b"old")
            for version in ("1.5.0", "1.4.9"):
                manifest = json.dumps({"version": version, "files": {"monitor.py": descriptor(b"new")}}).encode()
                self.assertFalse(AutoUpdater(runtime, lambda: True, lambda: Opener({RELEASE_VERSION_URL: manifest})).check_for_update())
            manifest = json.dumps({"version": "1.5.1", "files": {"monitor.py": descriptor(b"expected")}}).encode()
            updater = AutoUpdater(runtime, lambda: True, lambda: Opener({RELEASE_VERSION_URL: manifest, RUNTIME_FILE_URL.format(name="monitor.py"): b"damaged"}))
            with self.assertRaises(AutoUpdateError):
                updater.check_for_update()
            self.assertEqual((runtime / "monitor.py").read_bytes(), b"old")

    def test_replacement_failure_rolls_back_already_replaced_files(self):
        with temporary_directory() as runtime:
            (runtime / "first.py").write_bytes(b"old-first")
            (runtime / "blocked.py").mkdir()
            files = {"first.py": b"new-first", "blocked.py": b"new-blocked", "version.json": b'{"version":"1.5.1","dataContractVersion":1}\n'}
            updater = AutoUpdater(runtime, lambda: True, lambda: None)
            opener = Opener({RUNTIME_FILE_URL.format(name=name): data for name, data in files.items()})
            with self.assertRaises(AutoUpdateError):
                updater._install(opener, "1.5.1", {name: descriptor(data) for name, data in files.items()})
            self.assertEqual((runtime / "first.py").read_bytes(), b"old-first")
            self.assertTrue((runtime / "blocked.py").is_dir())

    def test_installer_never_runs_migration_with_old_loaded_modules(self):
        with temporary_directory() as temporary:
            runtime, data_home = temporary / "runtime", temporary / "data"
            runtime.mkdir()
            data_home.mkdir()
            (runtime / "monitor.py").write_bytes(b"old")
            (runtime / "version.json").write_text('{"version":"1.5.0","dataContractVersion":1}', encoding="utf-8")
            files = {"monitor.py": b"new", "version.json": b'{"version":"1.5.1","dataContractVersion":2}\n'}
            updater = AutoUpdater(runtime, lambda: True, lambda: None)
            opener = Opener({RUNTIME_FILE_URL.format(name=name): data for name, data in files.items()})

            with mock.patch.object(monitor_auto_update, "migrate_installed_usage_data", side_effect=AssertionError("old runtime attempted migration")) as migrate:
                updater._install(opener, "1.5.1", {name: descriptor(data) for name, data in files.items()})
                migrate.assert_not_called()

            self.assertEqual((runtime / "monitor.py").read_bytes(), b"new")
            self.assertEqual(installed_version(runtime), "1.5.1")

    def test_start_returns_before_immediate_check_finishes_and_stop_joins_worker(self):
        entered, release = threading.Event(), threading.Event()

        class BlockingResponse(Response):
            def read(self, *_args):
                entered.set()
                release.wait(2)
                return json.dumps({"version": "1.5.0", "files": {"monitor.py": descriptor(b"code")}}).encode()

        class BlockingOpener:
            def open(self, *_args, **_kwargs):
                return BlockingResponse()

        with temporary_directory() as temporary:
            updater = AutoUpdater(temporary, lambda: True, BlockingOpener, timeout=1)
            started = time.monotonic()
            updater.start(lambda: self.fail("equal version must not restart"))
            self.assertLess(time.monotonic() - started, .2)
            self.assertTrue(entered.wait(1))
            release.set()
            updater.stop()
            self.assertFalse(updater._thread.is_alive())
            self.assertEqual(updater.interval, 60 * 60)

    def test_enabling_checks_immediately_and_enabled_checks_repeat_on_interval(self):
        enabled = False
        with temporary_directory() as temporary:
            updater = AutoUpdater(temporary, lambda: enabled, lambda: None, interval=1)
            updater.check_for_update = mock.Mock(return_value=False)
            updater.start(lambda: self.fail("no mocked update should restart"))
            time.sleep(.05)
            updater.notify_config_changed()
            time.sleep(.05)
            updater.check_for_update.assert_not_called()
            enabled = True
            updater.notify_config_changed()
            deadline = time.monotonic() + 2.5
            while updater.check_for_update.call_count < 2 and time.monotonic() < deadline:
                time.sleep(.02)
            updater.stop()
            self.assertGreaterEqual(updater.check_for_update.call_count, 2)

    def test_restart_uses_same_interpreter_entry_and_arguments(self):
        with mock.patch.object(monitor_auto_update.os, "execv") as execute, mock.patch.object(monitor_auto_update.sys, "executable", "C:/Python/python.exe"):
            restart_process(Path("renamed/runtime/codex_monitor_daemon.py"), ["--local-only", "--interval", "12"])
        command = execute.call_args.args
        self.assertEqual(command[0], "C:/Python/python.exe")
        self.assertEqual(command[1][0], "C:/Python/python.exe")
        self.assertTrue(Path(command[1][1]).is_absolute())
        self.assertEqual(command[1][2:], ["--local-only", "--interval", "12"])

    def test_release_manifest_covers_runtime_files_deterministically(self):
        with temporary_directory() as temporary:
            release = temporary / "release"
            runtime = release / "runtime"
            runtime.mkdir(parents=True)
            (runtime / "b.py").write_bytes(b"b")
            (runtime / "A.py").write_bytes(b"a")
            with mock.patch.object(build_release, "RELEASE_DIR", release), mock.patch.object(build_release, "RUNTIME_DIR", runtime):
                build_release.write_release_version("1.5.0")
            manifest = json.loads((release / "version.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["version"], "1.5.0")
            self.assertEqual(list(manifest["files"]), ["A.py", "b.py"])
            self.assertEqual(manifest["files"]["A.py"], descriptor(b"a"))

    def test_current_release_is_complete_version_1_5_2_and_next_build_is_patch(self):
        package = json.loads((build_release.ROOT / "package.json").read_text(encoding="utf-8"))
        manifest = json.loads((build_release.RELEASE_DIR / "version.json").read_text(encoding="utf-8"))
        actual = {path.name: descriptor(path.read_bytes()) for path in build_release.RUNTIME_DIR.iterdir() if path.is_file()}

        self.assertEqual(package["version"], "1.5.2")
        self.assertEqual(package["dataContractVersion"], monitor_auto_update.DATA_CONTRACT_VERSION)
        self.assertEqual(manifest, {"version": "1.5.2", "files": dict(sorted(actual.items(), key=lambda item: item[0].casefold()))})
        self.assertEqual(json.loads((build_release.RUNTIME_DIR / "version.json").read_text(encoding="utf-8")), {"version": "1.5.2", "dataContractVersion": 4})
        self.assertTrue(all((build_release.ROOT / name).read_text(encoding="utf-8") == (build_release.RUNTIME_DIR / name).read_text(encoding="utf-8") for name in build_release.RUNTIME_FILES))
        self.assertEqual(build_release.next_patch_version("1.5.2"), "1.5.3")
        ignore = (build_release.ROOT / ".vscodeignore").read_text(encoding="utf-8").splitlines()
        self.assertIn("release/**", ignore)
        self.assertIn("release_pack/**", ignore)


if __name__ == "__main__":
    unittest.main()
