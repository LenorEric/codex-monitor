import hashlib
import importlib.metadata
import json
import subprocess
import sys
import shutil
import unittest
import uuid
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import monitor_dependencies
from monitor_auto_update import AutoUpdater


@contextmanager
def startup_test_directory():
    root = Path(__file__).resolve().parent / f".tmp-startup-{uuid.uuid4().hex}"
    root.mkdir()
    try:
        yield root
    finally:
        shutil.rmtree(root)


class StartupDependencyTests(unittest.TestCase):
    def test_missing_libraries_are_all_reported_with_install_command(self):
        with startup_test_directory() as directory:
            root = Path(directory)
            (root / "requirements.txt").write_text("cryptography>=46,<47\ntomlkit>=0.13,<1\n", encoding="utf-8")
            output = StringIO()
            with mock.patch("monitor_dependencies.importlib.metadata.distribution", side_effect=importlib.metadata.PackageNotFoundError), redirect_stderr(output):
                self.assertFalse(monitor_dependencies.validate_dependencies(root))
            self.assertIn("Missing Python library: cryptography", output.getvalue())
            self.assertIn("Missing Python library: tomlkit", output.getvalue())
            self.assertIn("-m pip install -r", output.getvalue())

    def test_entrypoint_exits_before_application_imports_when_library_is_missing(self):
        root = Path(__file__).resolve().parent
        result = subprocess.run([
            sys.executable, "-c",
            "import importlib.metadata, runpy; from unittest import mock; "
            "patcher = mock.patch('importlib.metadata.distribution', side_effect=importlib.metadata.PackageNotFoundError); patcher.start(); "
            f"runpy.run_path({str(root / 'codex_monitor_daemon.py')!r}, run_name='__main__')",
        ], capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Missing Python library: watchdog", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertNotIn("monitor_dashboard", result.stderr)

    def test_broken_import_is_reported(self):
        with startup_test_directory() as directory:
            root = Path(directory)
            (root / "requirements.txt").write_text("cryptography\n", encoding="utf-8")
            output = StringIO()
            with mock.patch("monitor_dependencies.importlib.metadata.distribution"), mock.patch("monitor_dependencies.importlib.import_module", side_effect=ImportError("broken binary")), redirect_stderr(output):
                self.assertFalse(monitor_dependencies.validate_dependencies(root))
            self.assertIn("Cannot load Python library cryptography: broken binary", output.getvalue())

    def test_install_uses_current_python_and_streams_pip_output(self):
        with mock.patch("monitor_dependencies.subprocess.run") as run:
            monitor_dependencies.install_dependencies(Path.cwd())
        run.assert_called_once_with([sys.executable, "-m", "pip", "install", "-r", str(Path.cwd() / "requirements.txt")], check=True)

    def test_server_startup_stages_precede_dashboard_url(self):
        import monitor_dashboard

        with startup_test_directory() as root:
            output = StringIO()
            with mock.patch.object(monitor_dashboard, "DashboardHTTPServer"), mock.patch.object(monitor_dashboard, "UsageDashboardState"), mock.patch.object(monitor_dashboard, "ControlAuth"), mock.patch.object(monitor_dashboard.threading, "Thread"), redirect_stdout(output):
                self.assertEqual(monitor_dashboard.serve_dashboard(SimpleNamespace(data_home=root, no_token_scan=True, dashboard=False, timeout=10, instance_lock=mock.Mock()), None), 0)
            lines = output.getvalue().splitlines()
            self.assertTrue(lines[0].startswith("Startup: binding dashboard server"))
            self.assertTrue(any("starting token capture" in line for line in lines))
            self.assertEqual(lines[-2], "Startup: initialization complete; dashboard ready.")
            self.assertEqual(lines[-1], "Dashboard: http://127.0.0.1:8765/")

    def test_update_installs_new_requirements_and_rolls_back_files_on_pip_failure(self):
        for fail in (False, True):
            with self.subTest(fail=fail), startup_test_directory() as directory:
                root = Path(directory)
                old_metadata = json.dumps({"version": "1.0.0", "dataContractVersion": 1}).encode()
                (root / "version.json").write_bytes(old_metadata)
                (root / "requirements.txt").write_bytes(b"old-package\n")
                payloads = {"requirements.txt": b"new-package\n", "version.json": json.dumps({"version": "1.0.1", "dataContractVersion": 1}).encode()}
                files = {name: {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()} for name, data in payloads.items()}
                updater = AutoUpdater(root, lambda: True, lambda: None)

                def install(runtime_dir):
                    self.assertEqual((runtime_dir / "requirements.txt").read_bytes(), b"new-package\n")
                    if fail:
                        raise subprocess.CalledProcessError(1, "pip")

                with mock.patch.object(updater, "_read", side_effect=lambda opener, url, maximum: payloads[url.rsplit("/", 1)[-1]]), mock.patch("monitor_auto_update.install_dependencies", side_effect=install) as installed:
                    if fail:
                        with self.assertRaises(subprocess.CalledProcessError):
                            updater._install(None, "1.0.1", files)
                        self.assertEqual((root / "requirements.txt").read_bytes(), b"old-package\n")
                        self.assertEqual((root / "version.json").read_bytes(), old_metadata)
                    else:
                        updater._install(None, "1.0.1", files)
                        self.assertEqual((root / "version.json").read_bytes(), payloads["version.json"])
                    installed.assert_called_once_with(root)
                self.assertEqual(list(root.glob(".codex-monitor-update-*")), [])


if __name__ == "__main__":
    unittest.main()
