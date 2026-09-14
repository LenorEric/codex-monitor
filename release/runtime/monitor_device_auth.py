#!/usr/bin/env python3

import os
import re
import subprocess
import threading
import time
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
from pathlib import Path

from monitor_session_refresh import find_codex_executable


DEVICE_CODE_LIFETIME_SECONDS = 15 * 60
DEVICE_CODE_REFRESH_REMAINING_SECONDS = 3 * 60
DEVICE_CODE_PATTERN = re.compile(r"(?<![A-Z0-9])([A-Z0-9]{4,8}(?:-[A-Z0-9]{4,8})+)(?![A-Z0-9])", re.IGNORECASE)
DEVICE_URL_PATTERN = re.compile(r"https://[^\s<>]+", re.IGNORECASE)
DEVICE_EXPIRY_PATTERN = re.compile(r"expires in\s+(\d+)\s+minutes?", re.IGNORECASE)
ANSI_PATTERN = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


class WindowsKillOnCloseJob:
    def __init__(self):
        self.handle = None
        self.required = os.name == "nt"
        if os.name != "nt":
            return

        class IoCounters(ctypes.Structure):
            _fields_ = [(name, ctypes.c_ulonglong) for name in ("ReadOperationCount", "WriteOperationCount", "OtherOperationCount", "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = [("PerProcessUserTimeLimit", ctypes.c_longlong), ("PerJobUserTimeLimit", ctypes.c_longlong), ("LimitFlags", wintypes.DWORD), ("MinimumWorkingSetSize", ctypes.c_size_t), ("MaximumWorkingSetSize", ctypes.c_size_t), ("ActiveProcessLimit", wintypes.DWORD), ("Affinity", ctypes.c_size_t), ("PriorityClass", wintypes.DWORD), ("SchedulingClass", wintypes.DWORD)]

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [("BasicLimitInformation", BasicLimitInformation), ("IoInfo", IoCounters), ("ProcessMemoryLimit", ctypes.c_size_t), ("JobMemoryLimit", ctypes.c_size_t), ("PeakProcessMemoryUsed", ctypes.c_size_t), ("PeakJobMemoryUsed", ctypes.c_size_t)]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = (wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD)
        kernel32.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            return
        information = ExtendedLimitInformation()
        information.BasicLimitInformation.LimitFlags = 0x00002000
        if not kernel32.SetInformationJobObject(handle, 9, ctypes.byref(information), ctypes.sizeof(information)):
            kernel32.CloseHandle(handle)
            return
        self.handle = handle
        self.kernel32 = kernel32

    def assign(self, process) -> bool:
        return not self.required or bool(self.handle and getattr(process, "_handle", None) and self.kernel32.AssignProcessToJobObject(self.handle, wintypes.HANDLE(process._handle)))

    def close(self) -> None:
        if self.handle:
            self.kernel32.CloseHandle(self.handle)
            self.handle = None


def utc_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z")


class DeviceAuthManager:
    def __init__(self, codex_home: Path, auth_path: Path, executable_finder=None, popen=None, clock=time.time, process_job=None):
        self.codex_home = Path(codex_home)
        self.auth_path = Path(auth_path)
        self.executable_finder = executable_finder or find_codex_executable
        self.popen = popen or subprocess.Popen
        self.clock = clock
        self.process_job = process_job or WindowsKillOnCloseJob()
        self.lock = threading.RLock()
        self.event = threading.Event()
        self.running = False
        self.thread = None
        self.process = None
        self.desired_account_id = None
        self.generation = 0
        self.current = None

    def start(self) -> None:
        with self.lock:
            if self.running:
                return
            self.running = True
            self.thread = threading.Thread(target=self._run, name="codex-device-auth", daemon=True)
            self.thread.start()

    def activate(self, account_id: str | None) -> None:
        with self.lock:
            if account_id == self.desired_account_id:
                return
            self.desired_account_id = account_id
            self.generation += 1
            self.current = None
        self.event.set()

    def regenerate(self, account_id: str) -> dict:
        with self.lock:
            if not account_id or account_id != self.desired_account_id:
                raise ValueError("Device authentication is not active for this account")
            self.generation += 1
            self.current = None
        self.event.set()
        return self.status()

    def status(self) -> dict | None:
        with self.lock:
            return dict(self.current) if self.current is not None else ({"accountId": self.desired_account_id, "status": "queued"} if self.desired_account_id else None)

    def stop(self) -> None:
        with self.lock:
            self.running = False
            self.desired_account_id = None
            self.generation += 1
            process = self.process
        self.event.set()
        self._stop_process(process)
        if self.thread is not None:
            self.thread.join(5)
        self.process_job.close()

    @staticmethod
    def _stop_process(process) -> None:
        if process is None or process.poll() is not None:
            return
        try:
            process.terminate()
            process.wait(timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            try:
                process.kill()
                process.wait(timeout=3)
            except (OSError, subprocess.TimeoutExpired):
                pass

    def _set_current(self, generation: int, **values) -> None:
        with self.lock:
            if generation == self.generation:
                self.current = {**(self.current or {}), **values}

    def _read_output(self, process, account_id: str, generation: int, generated_at: float) -> None:
        output = []
        lifetime_seconds = DEVICE_CODE_LIFETIME_SECONDS
        try:
            for raw_line in process.stdout or ():
                line = ANSI_PATTERN.sub("", raw_line).strip()
                if line:
                    output.append(line)
                if expiry := DEVICE_EXPIRY_PATTERN.search(line):
                    lifetime_seconds = int(expiry.group(1)) * 60
                code = DEVICE_CODE_PATTERN.search(line)
                url = DEVICE_URL_PATTERN.search(line)
                if code:
                    generated_at = self.clock()
                    self._set_current(generation, accountId=account_id, status="ready", code=code.group(1).upper(), generatedAt=utc_iso(generated_at), expiresAt=utc_iso(generated_at + lifetime_seconds), **({"verificationUrl": url.group(0).rstrip(".,)")} if url else {}))
                elif url:
                    self._set_current(generation, verificationUrl=url.group(0).rstrip(".,)"))
        except (OSError, ValueError) as exc:
            self._set_current(generation, accountId=account_id, status="error", error=f"Could not read Codex device authentication output: {exc}")
        finally:
            return_code = process.poll()
            if return_code not in (None, 0):
                self._set_current(generation, accountId=account_id, status="error", error="Codex device authentication stopped before sign-in completed." + (f" {output[-1]}" if output else ""))
            self.event.set()

    def _launch(self, account_id: str, generation: int) -> None:
        generated_at = self.clock()
        self.codex_home.mkdir(parents=True, exist_ok=True)
        creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
        try:
            process = self.popen(
                [self.executable_finder(), "login", "--device-auth"], cwd=self.codex_home, env=os.environ | {"CODEX_HOME": str(self.codex_home)}, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", bufsize=1, creationflags=creation_flags,
            )
        except Exception as exc:
            self._set_current(generation, accountId=account_id, status="error", error=f"Could not start Codex device authentication: {exc}")
            return
        with self.lock:
            if generation != self.generation or account_id != self.desired_account_id or not self.running:
                self._stop_process(process)
                return
            self.process = process
            self.current = {"accountId": account_id, "status": "generating", "generatedAt": utc_iso(generated_at)}
            if not self.process_job.assign(process):
                self.process = None
                self.current = {"accountId": account_id, "status": "error", "error": "Could not attach Codex device authentication to the monitor process lifecycle."}
                self._stop_process(process)
                return
        threading.Thread(target=self._read_output, args=(process, account_id, generation, generated_at), name="codex-device-auth-output", daemon=True).start()

    def _run(self) -> None:
        while True:
            with self.lock:
                if not self.running:
                    break
                account_id = self.desired_account_id
                generation = self.generation
                current = dict(self.current) if self.current else None
                process = self.process
            if account_id is None:
                self._stop_process(process)
                with self.lock:
                    if process is self.process:
                        self.process = None
                self.event.wait(60)
                self.event.clear()
                continue
            expires_at = generated_at = None
            if current and current.get("expiresAt"):
                try:
                    expires_at = datetime.fromisoformat(current["expiresAt"].replace("Z", "+00:00")).timestamp()
                except (TypeError, ValueError):
                    pass
            if current and current.get("generatedAt"):
                try:
                    generated_at = datetime.fromisoformat(current["generatedAt"].replace("Z", "+00:00")).timestamp()
                except (TypeError, ValueError):
                    pass
            refresh_due = expires_at is not None and expires_at - self.clock() <= DEVICE_CODE_REFRESH_REMAINING_SECONDS
            launch_needed = current is None or current.get("accountId") != account_id or refresh_due or (current.get("status") == "generating" and generated_at is not None and self.clock() - generated_at > 30)
            if launch_needed:
                self._stop_process(process)
                with self.lock:
                    if generation == self.generation:
                        self.process = None
                        if current is not None:
                            self.generation += 1
                            generation = self.generation
                            self.current = None
                self._launch(account_id, generation)
            wait_seconds = max(min((expires_at - self.clock() - DEVICE_CODE_REFRESH_REMAINING_SECONDS) if expires_at is not None else 1, 60), 0.1)
            self.event.wait(wait_seconds)
            self.event.clear()
        self._stop_process(self.process)
