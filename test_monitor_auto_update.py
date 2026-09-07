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
from monitor_auto_update import AutoUpdateError, AutoUpdater, RELEASE_VERSION_URL, RUNTIME_FILE_URL, installed_version, parse_version, restart_process, validate_manifest


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
            files = {"monitor.py": b"new", "version.json": b'{"version":"1.5.1"}\n', "README.md": b"new readme"}
            manifest = json.dumps({"version": "1.5.1", "files": {name: descriptor(data) for name, data in files.items()}}).encode()
            responses = {RELEASE_VERSION_URL: manifest, **{RUNTIME_FILE_URL.format(name=name): data for name, data in files.items()}}
            updater = AutoUpdater(runtime, lambda: True, lambda: Opener(responses))

            self.assertTrue(updater.check_for_update())
            self.assertEqual((runtime / "monitor.py").read_bytes(), b"new")
            self.assertEqual((runtime / "README.md").read_bytes(), b"new readme")
            self.assertEqual((runtime / "personal.txt").read_bytes(), b"keep")
            self.assertEqual(installed_version(runtime), "1.5.1")
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
            files = {"first.py": b"new-first", "blocked.py": b"new-blocked", "version.json": b'{"version":"1.5.1"}\n'}
            updater = AutoUpdater(runtime, lambda: True, lambda: None)
            opener = Opener({RUNTIME_FILE_URL.format(name=name): data for name, data in files.items()})
            with self.assertRaises(AutoUpdateError):
                updater._install(opener, "1.5.1", {name: descriptor(data) for name, data in files.items()})
            self.assertEqual((runtime / "first.py").read_bytes(), b"old-first")
            self.assertTrue((runtime / "blocked.py").is_dir())

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

    def test_current_release_is_complete_version_1_5_0_and_next_build_is_patch(self):
        package = json.loads((build_release.ROOT / "package.json").read_text(encoding="utf-8"))
        manifest = json.loads((build_release.RELEASE_DIR / "version.json").read_text(encoding="utf-8"))
        actual = {path.name: descriptor(path.read_bytes()) for path in build_release.RUNTIME_DIR.iterdir() if path.is_file()}

        self.assertEqual(package["version"], "1.5.0")
        self.assertEqual(manifest, {"version": "1.5.0", "files": dict(sorted(actual.items(), key=lambda item: item[0].casefold()))})
        self.assertEqual(json.loads((build_release.RUNTIME_DIR / "version.json").read_text(encoding="utf-8")), {"version": "1.5.0"})
        self.assertTrue(all((build_release.ROOT / name).read_text(encoding="utf-8") == (build_release.RUNTIME_DIR / name).read_text(encoding="utf-8") for name in build_release.RUNTIME_FILES))
        self.assertEqual(build_release.next_patch_version("1.5.0"), "1.5.1")
        ignore = (build_release.ROOT / ".vscodeignore").read_text(encoding="utf-8").splitlines()
        self.assertIn("release/**", ignore)
        self.assertIn("release_pack/**", ignore)


if __name__ == "__main__":
    unittest.main()
