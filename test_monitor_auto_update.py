import hashlib
import json
import shutil
import subprocess
import sys
import threading
import time
import unittest
import uuid
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from unittest import mock

import build_release
import monitor_migrations
import monitor_auto_update
from monitor_auto_update import AutoUpdateError, AutoUpdater, RELEASE_VERSION_URL, RUNTIME_FILE_URL, installed_version, parse_version, restart_process, validate_manifest
from monitor_migrations import DATA_MIGRATION_STATE_FILENAME, installed_data_contract_version, migrate_history_data


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
    def setUp(self):
        # Fixtures do not contain installable requirements; never run real pip.
        patcher = mock.patch.object(monitor_auto_update, "install_dependencies")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_startup_check_respects_setting_and_does_not_initialize_data(self):
        with temporary_directory() as root, mock.patch.object(monitor_auto_update.AutoUpdater, "check_for_update", return_value=True) as check:
            self.assertFalse(monitor_auto_update.check_startup_update(root, root, lambda: None))
            self.assertEqual(list(root.iterdir()), [])
            check.assert_not_called()
            (root / "config.json").write_text('{"autoUpdate":{"enabled":true}}', encoding="utf-8")
            self.assertTrue(monitor_auto_update.check_startup_update(root, root, lambda: None))
            check.assert_called_once_with()

    def test_startup_check_failure_allows_startup_to_continue(self):
        with temporary_directory() as root:
            (root / "config.json").write_text('{"autoUpdate":{"enabled":true}}', encoding="utf-8")
            with mock.patch.object(monitor_auto_update.AutoUpdater, "check_for_update", side_effect=OSError("offline")):
                self.assertFalse(monitor_auto_update.check_startup_update(root, root, lambda: None))

    def test_startup_checks_before_migration_failure_and_restarts_before_migration(self):
        import codex_monitor_daemon as daemon

        with mock.patch.object(daemon.sys, "argv", [str(Path(daemon.__file__)), "--database", "custom.sqlite3"]), mock.patch.object(daemon, "check_startup_update", return_value=False) as check, mock.patch.object(daemon, "migrate_installed_data", side_effect=RuntimeError("migration failed")) as migrate:
            with self.assertRaisesRegex(RuntimeError, "migration failed"):
                daemon._main(mock.Mock())
            check.assert_called_once()
            migrate.assert_called_once()
            self.assertEqual(migrate.call_args.kwargs["database_path"], Path("custom.sqlite3"))
            check.reset_mock()
            migrate.reset_mock()
            check.return_value = True
            with mock.patch.object(daemon, "restart_process") as restart:
                self.assertEqual(daemon._main(mock.Mock()), 0)
            restart.assert_called_once()
            migrate.assert_not_called()

    def test_periodic_worker_can_defer_duplicate_startup_check(self):
        with temporary_directory() as root:
            updater = AutoUpdater(root, lambda: True, lambda: None)
            updater.check_for_update = mock.Mock(return_value=False)
            updater._wake.wait = mock.Mock(side_effect=lambda _seconds: updater._stop.set())
            try:
                updater.start(lambda: None, immediate=False)
                updater._thread.join(1)
                updater.check_for_update.assert_not_called()
            finally:
                updater.stop()

    def test_history_data_migration_runs_each_intermediate_version_and_checkpoints(self):
        with temporary_directory() as data_home:
            paths = [data_home / name for name in ("history", "quota", "sessions", "samples")]
            (data_home / DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":1}\n', encoding="utf-8")
            applied = []
            with mock.patch.object(monitor_migrations, "DATA_CONTRACT_VERSION", 3), mock.patch.object(monitor_migrations, "DATA_MIGRATIONS", {2: lambda _paths: applied.append(2), 3: lambda _paths: applied.append(3)}):
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

    def test_manual_check_reports_installed_and_published_versions(self):
        with temporary_directory() as runtime:
            (runtime / "version.json").write_text('{"version":"1.5.0"}', encoding="utf-8")
            manifest = json.dumps({"version": "1.5.0", "files": {"monitor.py": descriptor(b"code")}}).encode()

            result = AutoUpdater(runtime, lambda: False, lambda: Opener({RELEASE_VERSION_URL: manifest})).check_for_update_details()

            self.assertEqual(result, {"currentVersion": "1.5.0", "latestVersion": "1.5.0", "updated": False})

    def test_download_and_validation_failures_preserve_old_runtime_and_version(self):
        for failure in (OSError("download failure"), b"damaged", b'{"version":"1.5.2","dataContractVersion":2}\n'):
            with self.subTest(failure=failure), temporary_directory() as runtime:
                (runtime / "monitor.py").write_bytes(b"old")
                (runtime / "version.json").write_text('{"version":"1.5.0"}', encoding="utf-8")
                metadata = failure if isinstance(failure, bytes) and failure.startswith(b"{") else b'{"version":"1.5.1","dataContractVersion":2}\n'
                updater = AutoUpdater(runtime, lambda: True, lambda: None)
                with mock.patch.object(updater, "_read", side_effect=[b"new", failure]), self.assertRaises((OSError, AutoUpdateError)):
                    updater._install(None, "1.5.1", {"monitor.py": descriptor(b"new"), "version.json": descriptor(metadata)})
                self.assertEqual((runtime / "monitor.py").read_bytes(), b"old")
                self.assertEqual(installed_version(runtime), "1.5.0")
                self.assertFalse(list(runtime.glob(".codex-monitor-update-*")))

    def test_restart_request_runs_callback_once(self):
        with temporary_directory() as runtime:
            restarted = []
            updater = AutoUpdater(runtime, lambda: False, lambda: None)
            updater._on_update = lambda: restarted.append(True)

            updater.request_restart()
            updater.request_restart()

            self.assertTrue(updater.restart_requested)
            self.assertEqual(restarted, [True])

    def test_replacement_failure_preserves_already_replaced_files(self):
        with temporary_directory() as runtime:
            (runtime / "first.py").write_bytes(b"old-first")
            (runtime / "blocked.py").mkdir()
            files = {"first.py": b"new-first", "blocked.py": b"new-blocked", "version.json": b'{"version":"1.5.1","dataContractVersion":1}\n'}
            updater = AutoUpdater(runtime, lambda: True, lambda: None)
            opener = Opener({RUNTIME_FILE_URL.format(name=name): data for name, data in files.items()})
            with self.assertRaises(AutoUpdateError):
                updater._install(opener, "1.5.1", {name: descriptor(data) for name, data in files.items()})
            self.assertEqual((runtime / "first.py").read_bytes(), b"new-first")
            self.assertTrue((runtime / "blocked.py").is_dir())

    def test_dependency_failure_keeps_new_runtime_and_version(self):
        with temporary_directory() as runtime:
            (runtime / "monitor.py").write_bytes(b"old")
            (runtime / "version.json").write_text('{"version":"1.5.0"}', encoding="utf-8")
            files = {"monitor.py": b"new", "version.json": b'{"version":"1.5.1","dataContractVersion":2}\n'}
            updater = AutoUpdater(runtime, lambda: True, lambda: None)
            opener = Opener({RUNTIME_FILE_URL.format(name=name): data for name, data in files.items()})
            with mock.patch.object(monitor_auto_update, "install_dependencies", side_effect=RuntimeError("dependency failure")), self.assertRaisesRegex(RuntimeError, "dependency failure"):
                updater._install(opener, "1.5.1", {name: descriptor(data) for name, data in files.items()})
            self.assertEqual((runtime / "monitor.py").read_bytes(), b"new")
            self.assertEqual(installed_version(runtime), "1.5.1")
            self.assertFalse(list(runtime.glob(".codex-monitor-update-*")))

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

            with mock.patch.object(monitor_migrations, "migrate_installed_data", side_effect=AssertionError("old runtime attempted migration")) as migrate:
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
            restart_process(Path("renamed/runtime/codex_monitor_daemon.py"), ["--dashboard", "--database", "custom.sqlite3"])
        command = execute.call_args.args
        self.assertEqual(command[0], "C:/Python/python.exe")
        self.assertEqual(command[1][0], "C:/Python/python.exe")
        self.assertTrue(Path(command[1][1]).is_absolute())
        self.assertEqual(command[1][2:], ["--dashboard", "--database", "custom.sqlite3"])

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

    def test_missing_runtime_import_cannot_bump_version_or_replace_runtime(self):
        with temporary_directory() as root:
            runtime = root / "runtime"
            runtime.mkdir()
            (runtime / "preserved.txt").write_text("existing release", encoding="utf-8")
            for omitted in ("monitor_dependencies.py", "monitor_common.py"):
                with self.subTest(omitted=omitted), mock.patch.object(build_release, "RUNTIME_FILES", tuple(name for name in build_release.RUNTIME_FILES if name != omitted)), mock.patch.object(build_release, "RUNTIME_DIR", runtime), mock.patch.object(build_release, "bump_package_version", return_value="1.5.23") as bump:
                    with self.assertRaisesRegex(ValueError, omitted):
                        build_release.main()
                    bump.assert_not_called()
                    with self.assertRaisesRegex(ValueError, omitted):
                        build_release.rebuild_runtime("1.5.22")
                self.assertEqual((runtime / "preserved.txt").read_text(encoding="utf-8"), "existing release")

    def test_packaged_runtime_imports_and_sqlite_migration_work_in_isolation(self):
        with temporary_directory() as root:
            release = root / "release"
            runtime = release / "runtime"
            with mock.patch.object(build_release, "RELEASE_DIR", release), mock.patch.object(build_release, "RUNTIME_DIR", runtime):
                build_release.rebuild_runtime("1.5.22")
            result = subprocess.run([sys.executable, "-I", "-c", """
import sys, threading
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import monitor_dashboard
from monitor_usage_sync import UsageDataStore
root = Path(sys.argv[2])
store = UsageDataStore(root / "usage.sqlite3", "machine", lambda _: "v3:a", threading.RLock())
store.record_quota({"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "slot", "windows": {"5h": {"usedPercent": 10}}})
assert len(store.datasets()[0]) == 1
assert all(Path(module.__file__).parent == Path(sys.argv[1]) for name, module in sys.modules.items() if name.startswith("monitor_") and getattr(module, "__file__", None))
print("Packaged runtime imports and SQLite migration OK")
""", str(runtime), str(root / "data")], cwd=root, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("SQLite migration OK", result.stdout)

    def test_release_commit_stages_everything_and_uses_version_message(self):
        with mock.patch.object(build_release.shutil, "which", return_value="C:/Git/git.exe"), mock.patch.object(build_release.subprocess, "run") as run:
            build_release.commit_release("1.5.5")

        self.assertEqual(run.call_args_list, [
            mock.call(["C:/Git/git.exe", "add", "--all"], cwd=build_release.ROOT, check=True),
            mock.call(["C:/Git/git.exe", "commit", "-m", "v1.5.5"], cwd=build_release.ROOT, check=True),
        ])

    def test_release_main_commits_only_after_successful_explicit_build(self):
        with mock.patch.object(build_release, "bump_package_version", return_value="1.5.5") as bump, mock.patch.object(build_release, "build_release") as build, mock.patch.object(build_release, "commit_release") as commit:
            build_release.main()

        bump.assert_called_once_with()
        build.assert_called_once_with("1.5.5")
        commit.assert_called_once_with("1.5.5")

    def test_release_main_does_not_commit_a_failed_build(self):
        with mock.patch.object(build_release, "bump_package_version", return_value="1.5.5"), mock.patch.object(build_release, "build_release", side_effect=RuntimeError("build failed")), mock.patch.object(build_release, "commit_release") as commit, self.assertRaisesRegex(RuntimeError, "build failed"):
            build_release.main()

        commit.assert_not_called()

    def test_mismatched_contract_cannot_bump_version_or_replace_runtime(self):
        with temporary_directory() as root:
            runtime = root / "runtime"
            runtime.mkdir()
            (runtime / "preserved.txt").write_text("existing release", encoding="utf-8")
            (root / "package.json").write_text('{"dataContractVersion":16,"cloudDataContractVersion":3}', encoding="utf-8")
            with mock.patch.object(build_release, "ROOT", root), mock.patch.object(build_release, "RUNTIME_DIR", runtime), mock.patch.object(build_release, "bump_package_version") as bump:
                with self.assertRaises(monitor_migrations.MigrationError):
                    build_release.main()
                bump.assert_not_called()
                with self.assertRaises(monitor_migrations.MigrationError):
                    build_release.rebuild_runtime("1.5.18")
            self.assertEqual((runtime / "preserved.txt").read_text(encoding="utf-8"), "existing release")

    def test_current_release_is_complete_and_next_build_is_patch(self):
        package = json.loads((build_release.ROOT / "package.json").read_text(encoding="utf-8"))
        manifest = json.loads((build_release.RELEASE_DIR / "version.json").read_text(encoding="utf-8"))
        actual = {path.name: descriptor(path.read_bytes()) for path in build_release.RUNTIME_DIR.iterdir() if path.is_file()}

        self.assertEqual(package["dataContractVersion"], monitor_migrations.DATA_CONTRACT_VERSION)
        self.assertEqual(sorted(monitor_migrations.DATA_MIGRATIONS), list(range(1, monitor_migrations.DATA_CONTRACT_VERSION + 1)))
        self.assertEqual(manifest, {"version": package["version"], "files": dict(sorted(actual.items(), key=lambda item: item[0].casefold()))})
        from monitor_cloud_contract import CLOUD_DATA_CONTRACT_VERSION
        self.assertEqual(package["cloudDataContractVersion"], CLOUD_DATA_CONTRACT_VERSION)
        self.assertFalse(hasattr(monitor_migrations, "CLOUD_DATA_MIGRATIONS"))
        released = json.loads((build_release.RUNTIME_DIR / "version.json").read_text(encoding="utf-8"))
        self.assertEqual(released["version"], package["version"])
        self.assertIn(monitor_migrations.DATA_CONTRACT_VERSION, {released["dataContractVersion"], released["dataContractVersion"] + 1})
        # Cloud migrations are retired; the current released cloud contract stays unchanged.
        self.assertEqual(CLOUD_DATA_CONTRACT_VERSION, released["cloudDataContractVersion"])
        self.assertTrue(all((build_release.ROOT / name).is_file() for name in build_release.RUNTIME_FILES))
        self.assertEqual(build_release.next_patch_version("1.5.5"), "1.5.6")
        ignore = (build_release.ROOT / ".vscodeignore").read_text(encoding="utf-8").splitlines()
        self.assertIn("release/**", ignore)
        self.assertIn("release_pack/**", ignore)


if __name__ == "__main__":
    unittest.main()
