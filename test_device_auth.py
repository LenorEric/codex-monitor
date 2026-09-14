import time
import unittest
from pathlib import Path

from monitor_device_auth import DEVICE_CODE_REFRESH_REMAINING_SECONDS, DeviceAuthManager


class FakeProcess:
    def __init__(self, lines):
        self.stdout = iter(lines)
        self.returncode = None
        self.terminated = False

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -15

    def kill(self):
        self.returncode = -9

    def wait(self, timeout=None):
        return self.returncode


class FakeJob:
    def __init__(self):
        self.assigned = []
        self.closed = False

    def assign(self, process):
        self.assigned.append(process)
        return True

    def close(self):
        self.closed = True


def wait_until(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class DeviceAuthManagerTests(unittest.TestCase):
    def test_management_page_exposes_pending_account_device_code_controls(self):
        html = Path("management.html").read_text(encoding="utf-8")
        self.assertIn('data-device-code="${escapeHtml(account.id)}"', html)
        self.assertIn('id="deviceCodeExpiry"', html)
        self.assertIn('id="regenerateDeviceCode"', html)
        self.assertIn('api("/api/accounts/device-code"', html)

    def test_generates_parses_and_stops_device_auth(self):
        calls = []

        def popen(command, **kwargs):
            calls.append((command, kwargs))
            return FakeProcess(["Open https://auth.openai.com/codex/device\n", "Enter ABCD-EFGH to continue\n"])

        job = FakeJob()
        manager = DeviceAuthManager(Path.cwd(), Path.cwd() / ".device-auth-test.json", lambda: "codex", popen, process_job=job)
        try:
            manager.activate("account-a")
            manager.start()
            self.assertTrue(wait_until(lambda: (manager.status() or {}).get("status") == "ready"))
            self.assertEqual(manager.status()["code"], "ABCD-EFGH")
            self.assertEqual(manager.status()["verificationUrl"], "https://auth.openai.com/codex/device")
            self.assertEqual(calls[0][0], ["codex", "login", "--device-auth"])
            self.assertEqual(calls[0][1]["env"]["CODEX_HOME"], str(Path.cwd()))
            process = manager.process
        finally:
            manager.stop()
        self.assertTrue(process.terminated)
        self.assertEqual(job.assigned, [process])
        self.assertTrue(job.closed)

    def test_parses_current_codex_code_shape_and_reported_lifetime(self):
        now = [1_000.0]
        process = FakeProcess(["Enter this one-time code (expires in 15 minutes)\n", "CDTM-N2P1J\n"])
        manager = DeviceAuthManager(Path.cwd(), Path.cwd() / ".device-auth-test.json", lambda: "codex", lambda *_args, **_kwargs: process, lambda: now[0], FakeJob())
        try:
            manager.activate("account-a")
            manager.start()
            self.assertTrue(wait_until(lambda: (manager.status() or {}).get("status") == "ready"))
            self.assertEqual(manager.status()["code"], "CDTM-N2P1J")
            self.assertEqual(manager.status()["expiresAt"], "1970-01-01T00:31:40Z")
        finally:
            manager.stop()

    def test_refreshes_when_only_three_minutes_remain(self):
        now = [1_000.0]
        processes = []

        def popen(_command, **_kwargs):
            process = FakeProcess([f"Code {'AAAA-BBBB' if not processes else 'CCCC-DDDD'}\n"])
            processes.append(process)
            return process

        manager = DeviceAuthManager(Path.cwd(), Path.cwd() / ".device-auth-test.json", lambda: "codex", popen, lambda: now[0], FakeJob())
        try:
            manager.activate("account-a")
            manager.start()
            self.assertTrue(wait_until(lambda: (manager.status() or {}).get("code") == "AAAA-BBBB"))
            now[0] += 15 * 60 - DEVICE_CODE_REFRESH_REMAINING_SECONDS
            manager.event.set()
            self.assertTrue(wait_until(lambda: (manager.status() or {}).get("code") == "CCCC-DDDD"))
            self.assertEqual(len(processes), 2)
            self.assertTrue(processes[0].terminated)
        finally:
            manager.stop()

    def test_manual_regeneration_replaces_current_process(self):
        processes = []

        def popen(_command, **_kwargs):
            process = FakeProcess([f"Code {'AAAA-BBBB' if not processes else 'EEEE-FFFF'}\n"])
            processes.append(process)
            return process

        manager = DeviceAuthManager(Path.cwd(), Path.cwd() / ".device-auth-test.json", lambda: "codex", popen, process_job=FakeJob())
        try:
            manager.activate("account-a")
            manager.start()
            self.assertTrue(wait_until(lambda: (manager.status() or {}).get("code") == "AAAA-BBBB"))
            manager.regenerate("account-a")
            self.assertTrue(wait_until(lambda: (manager.status() or {}).get("code") == "EEEE-FFFF"))
            self.assertTrue(processes[0].terminated)
        finally:
            manager.stop()


if __name__ == "__main__":
    unittest.main()
