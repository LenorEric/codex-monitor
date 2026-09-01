#!/usr/bin/env python3

import json
import os
import shutil
import subprocess
from pathlib import Path

from monitor_tokens import parse_token_usage, pricing_for_model


MESSAGE_TIMEOUT_SECONDS = 120
REFRESH_PROMPT = "Explain how a modern operating system handles virtual memory"
REFRESH_MODEL = "gpt-5.6-sol"


def api_cost_from_output(output: bytes | str | None) -> float | None:
    if not output:
        return None
    if isinstance(output, bytes):
        output = output.decode("utf-8", errors="replace")
    usage = None
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(event, dict) and event.get("type") == "turn.completed" and isinstance(event.get("usage"), dict):
            usage = parse_token_usage(event["usage"])
    if usage is None:
        return None
    prices = pricing_for_model(REFRESH_MODEL)
    if prices is None:
        return None
    cached_input = min(usage["cachedInput"], usage["input"])
    cache_write_input = min(usage["cacheWriteInput"], usage["input"] - cached_input)
    fresh_input = max(0, usage["input"] - cached_input - cache_write_input)
    return round((fresh_input * prices["input"] + cached_input * prices["cachedInput"] + cache_write_input * prices["cacheWriteInput"] + usage["output"] * prices["output"]) / 1_000_000, 8)


def find_codex_executable() -> str:
    configured_executable = os.environ.get("CODEX_CLI_PATH")
    if configured_executable:
        configured_path = Path(configured_executable).expanduser()
        if configured_path.is_file():
            return str(configured_path)
    if os.name == "nt":
        local_app_data = os.environ.get("LOCALAPPDATA")
        if local_app_data:
            bundled_dir = Path(local_app_data) / "OpenAI" / "Codex" / "bin"
            try:
                bundled_executables = sorted((path for path in bundled_dir.glob("*/codex.exe") if path.is_file()), key=lambda path: path.stat().st_mtime, reverse=True)
            except OSError:
                bundled_executables = []
            if bundled_executables:
                return str(bundled_executables[0])
    codex_exe = shutil.which("codex")
    if codex_exe is None:
        raise RuntimeError("Codex CLI was not found on PATH")
    return codex_exe


def refresh_session(codex_home: Path, auth_data: bytes | None = None) -> tuple[bool, bytes | None, float | None]:
    codex_exe = find_codex_executable()
    if auth_data is not None:
        codex_home.mkdir(parents=True, exist_ok=True)
        (codex_home / "auth.json").write_bytes(auth_data)
    environment = os.environ | {"CODEX_HOME": str(codex_home)}
    completed = subprocess.run(
        [codex_exe, "exec", "-c", 'model_reasoning_effort="low"', "--skip-git-repo-check", "--sandbox", "read-only", "--color", "never", "--model", REFRESH_MODEL, "--cd", str(codex_home), "--json", REFRESH_PROMPT],
        cwd=codex_home,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        timeout=MESSAGE_TIMEOUT_SECONDS,
        check=False,
    )
    refresh_cost = api_cost_from_output(getattr(completed, "stdout", None))
    return completed.returncode == 0 and refresh_cost is not None, (codex_home / "auth.json").read_bytes() if auth_data is not None and (codex_home / "auth.json").exists() else None, refresh_cost
