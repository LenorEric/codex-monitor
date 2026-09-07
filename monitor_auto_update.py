#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path


UPDATE_INTERVAL_SECONDS = 60 * 60
RELEASE_VERSION_URL = "https://raw.githubusercontent.com/LenorEric/codex-monitor/refs/heads/master/release/version.json"
RUNTIME_FILE_URL = "https://raw.githubusercontent.com/LenorEric/codex-monitor/refs/heads/master/release/runtime/{name}"
VERSION_PATTERN = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_RUNTIME_FILE_BYTES = 32 * 1024 * 1024
MAX_RUNTIME_BYTES = 128 * 1024 * 1024
AUTO_UPDATE_RESTART = 75


class AutoUpdateError(RuntimeError):
    pass


def parse_version(value) -> tuple[int, int, int]:
    if not isinstance(value, str) or (match := VERSION_PATTERN.fullmatch(value)) is None:
        raise AutoUpdateError(f"Invalid release version: {value!r}")
    return tuple(int(part) for part in match.groups())


def installed_version(runtime_dir: Path, fallback: str = "1.5.0") -> str:
    for path in (runtime_dir / "version.json", runtime_dir / "package.json"):
        try:
            version = json.loads(path.read_text(encoding="utf-8")).get("version")
            parse_version(version)
            return version
        except (FileNotFoundError, OSError, AttributeError, json.JSONDecodeError, AutoUpdateError):
            pass
    return fallback


def validate_manifest(value) -> tuple[str, dict[str, dict]]:
    if not isinstance(value, dict):
        raise AutoUpdateError("Release manifest must be a JSON object")
    version, files = value.get("version"), value.get("files")
    parse_version(version)
    if not isinstance(files, dict) or not files:
        raise AutoUpdateError("Release manifest must contain runtime files")
    normalized, total_size, names = {}, 0, set()
    for name, descriptor in files.items():
        if not isinstance(name, str) or not name or name in {".", ".."} or Path(name).name != name or any(char in name for char in "/\\\x00?#%"):
            raise AutoUpdateError(f"Unsafe runtime filename: {name!r}")
        folded = name.casefold()
        if folded in names:
            raise AutoUpdateError(f"Duplicate runtime filename: {name}")
        names.add(folded)
        if not isinstance(descriptor, dict) or not isinstance(descriptor.get("size"), int) or isinstance(descriptor.get("size"), bool) or not 0 <= descriptor["size"] <= MAX_RUNTIME_FILE_BYTES:
            raise AutoUpdateError(f"Invalid size for runtime file: {name}")
        digest = descriptor.get("sha256")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise AutoUpdateError(f"Invalid SHA-256 for runtime file: {name}")
        total_size += descriptor["size"]
        if total_size > MAX_RUNTIME_BYTES:
            raise AutoUpdateError("Runtime update exceeds the maximum total size")
        normalized[name] = {"size": descriptor["size"], "sha256": digest}
    return version, normalized


class AutoUpdater:
    def __init__(self, runtime_dir: Path, enabled, opener_factory, timeout: int = 10, interval: int = UPDATE_INTERVAL_SECONDS, clock=time.monotonic):
        self.runtime_dir, self.enabled, self.opener_factory = Path(runtime_dir).resolve(), enabled, opener_factory
        self.timeout, self.interval, self.clock = max(int(timeout), 1), max(int(interval), 1), clock
        self.restart_requested = False
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = None
        self._on_update = None

    def start(self, on_update) -> None:
        if self._thread is not None:
            return
        self._on_update = on_update
        self._thread = threading.Thread(target=self._run, name="codex-monitor-auto-update", daemon=True)
        self._thread.start()

    def notify_config_changed(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(self.timeout + 1)

    def _run(self) -> None:
        was_enabled, next_check = False, 0.0
        while not self._stop.is_set():
            try:
                enabled = self.enabled() is True
            except Exception as exc:
                enabled = False
                print(f"Automatic update configuration check failed: {exc}", file=sys.stderr, flush=True)
            now = self.clock()
            if enabled and (not was_enabled or now >= next_check):
                next_check = now + self.interval
                try:
                    if self.check_for_update():
                        self.restart_requested = True
                        self._on_update()
                        return
                except Exception as exc:
                    print(f"Automatic update failed: {exc}", file=sys.stderr, flush=True)
            was_enabled = enabled
            wait_seconds = min(max(next_check - self.clock(), 0.0), 60.0) if enabled else 60.0
            self._wake.wait(wait_seconds)
            self._wake.clear()

    def _read(self, opener, url: str, maximum: int) -> bytes:
        request = urllib.request.Request(url, headers={"Cache-Control": "no-cache", "User-Agent": "codex-monitor-auto-update"})
        with opener.open(request, timeout=self.timeout) as response:
            data = response.read(maximum + 1)
        if len(data) > maximum:
            raise AutoUpdateError(f"Download exceeds the maximum size: {url}")
        return data

    def _manifest(self, opener) -> tuple[str, dict[str, dict]]:
        try:
            value = json.loads(self._read(opener, RELEASE_VERSION_URL, MAX_MANIFEST_BYTES).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AutoUpdateError(f"Release manifest is not valid UTF-8 JSON: {exc}") from exc
        return validate_manifest(value)

    def check_for_update(self) -> bool:
        opener = self.opener_factory()
        version, files = self._manifest(opener)
        if parse_version(version) <= parse_version(installed_version(self.runtime_dir)):
            return False
        self._install(opener, version, files)
        print(f"Codex Monitor updated to v{version}; restarting.", flush=True)
        return True

    def _install(self, opener, version: str, files: dict[str, dict]) -> None:
        work_root = self.runtime_dir / f".codex-monitor-update-{uuid.uuid4().hex}"
        staging, backup = work_root / "staging", work_root / "backup"
        staging.mkdir(parents=True)
        backup.mkdir()
        replaced = []
        try:
            for name, descriptor in files.items():
                data = self._read(opener, RUNTIME_FILE_URL.format(name=urllib.parse.quote(name)), descriptor["size"])
                if len(data) != descriptor["size"] or hashlib.sha256(data).hexdigest() != descriptor["sha256"]:
                    raise AutoUpdateError(f"Runtime file verification failed: {name}")
                (staging / name).write_bytes(data)
            try:
                if json.loads((staging / "version.json").read_text(encoding="utf-8")) != {"version": version}:
                    raise AutoUpdateError("Runtime version metadata does not match the release manifest")
            except (FileNotFoundError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AutoUpdateError("Runtime version metadata is missing or invalid") from exc
            for name in [name for name in files if name != "version.json"] + ["version.json"]:
                target = self.runtime_dir / name
                if target.is_symlink() or target.exists() and not target.is_file():
                    raise AutoUpdateError(f"Runtime target is not a regular file: {name}")
                if target.exists():
                    shutil.copy2(target, backup / name)
                os.replace(staging / name, target)
                replaced.append(name)
        except Exception:
            rollback_errors = []
            for name in reversed(replaced):
                target, saved = self.runtime_dir / name, backup / name
                try:
                    if saved.exists():
                        os.replace(saved, target)
                    else:
                        target.unlink(missing_ok=True)
                except OSError as exc:
                    rollback_errors.append(f"{name}: {exc}")
            if rollback_errors:
                raise AutoUpdateError(f"Update failed and rollback was incomplete: {'; '.join(rollback_errors)}")
            raise
        finally:
            shutil.rmtree(work_root, ignore_errors=True)


def restart_process(entry: Path, arguments: list[str] | None = None) -> None:
    os.execv(sys.executable, [sys.executable, str(Path(entry).resolve()), *(sys.argv[1:] if arguments is None else arguments)])
