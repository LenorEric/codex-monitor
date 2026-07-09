#!/usr/bin/env python3

import base64
import binascii
import json
import os
import time
import urllib.error
import urllib.request
import winreg
from datetime import datetime, timezone
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    tomllib = None

USAGE_ENDPOINT = "https://chatgpt.com/backend-api/wham/usage"

TARGET_WINDOWS = {
    "5h": 5 * 60,
    "7d": 7 * 24 * 60,
}

PLAN_MULTIPLIERS = {
    "plus": 1.0,
    "pro_lite": 5.0,
    "pro": 20.0,
    "unknown": 1.0,
}

MIN_DELTA_COST_PER_PERCENT_USD = 0.01
RATIO_DEVIATION_MULTIPLIER = 3.0
RESET_TIME_JITTER_SECONDS = 60
DEFAULT_SAMPLE_LOG_MAX_BYTES = 50 * 1024 * 1024
DEFAULT_RETRY_LIMIT = 3

class UsageError(RuntimeError):
    pass

def is_client_disconnect(exc: BaseException) -> bool:
    return isinstance(exc, (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)) or (
        isinstance(exc, OSError) and (getattr(exc, "winerror", None) in {10053, 10054} or getattr(exc, "errno", None) in {32, 104})
    )

def empty_token_totals() -> dict:
    return {
        "inputTokens": 0,
        "freshInputTokens": 0,
        "cachedInputTokens": 0,
        "outputTokens": 0,
        "totalTokens": 0,
        "requests": 0,
    }

def empty_cost_totals() -> dict:
    return {
        "inputCostUsd": 0.0,
        "cachedInputCostUsd": 0.0,
        "outputCostUsd": 0.0,
        "totalCostUsd": 0.0,
    }

def codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")

def load_json(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as exc:
        raise UsageError(f"missing {path}") from exc

def write_json(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2)
        f.write("\n")

def jwt_payload(token: str) -> dict:
    try:
        return json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=" * (-len(token.split(".")[1]) % 4)))
    except (binascii.Error, IndexError, json.JSONDecodeError, UnicodeDecodeError):
        return {}

def token_expired(token: str, skew_seconds: int = 60) -> bool:
    return int(jwt_payload(token).get("exp") or 0) <= int(time.time()) + skew_seconds

def normalize_proxy(value: str) -> str:
    return value if "://" in value else f"http://{value}"

def load_windows_system_proxy() -> dict:
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Internet Settings") as key:
            enabled = int(winreg.QueryValueEx(key, "ProxyEnable")[0])
            server = str(winreg.QueryValueEx(key, "ProxyServer")[0]) if enabled else ""
            auto_config = ""
            try:
                auto_config = str(winreg.QueryValueEx(key, "AutoConfigURL")[0])
            except FileNotFoundError:
                pass
    except FileNotFoundError:
        return {}
    if not server and auto_config:
        raise UsageError(f"system proxy uses PAC auto-config, which urllib cannot evaluate: {auto_config}")
    if not server:
        return {}
    if "=" not in server:
        return {"http": normalize_proxy(server), "https": normalize_proxy(server)}
    proxies = {}
    for item in server.split(";"):
        if "=" in item:
            scheme, proxy = item.split("=", 1)
            if scheme.lower() in {"http", "https"} and proxy:
                proxies[scheme.lower()] = normalize_proxy(proxy)
    return proxies

def load_proxy_config(home: Path) -> dict:
    if tomllib is None or not (home / "config.toml").exists():
        return {}
    with (home / "config.toml").open("rb") as f:
        config = tomllib.load(f)
    return {k.lower(): v for k, v in config.items() if k.upper() in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"} and isinstance(v, str) and v}

def opener_for(home: Path) -> urllib.request.OpenerDirector:
    proxies = load_windows_system_proxy() or load_proxy_config(home)
    if not proxies:
        raise UsageError("no system proxy is configured; refusing to connect directly")
    return urllib.request.build_opener(urllib.request.ProxyHandler(proxies))

def retry_operation(operation, retries: int = DEFAULT_RETRY_LIMIT, delay_seconds: float = 1.0):
    for attempt in range(retries + 1):
        try:
            return operation()
        except Exception:
            if attempt >= retries:
                raise
            time.sleep(delay_seconds)

def request_json(opener: urllib.request.OpenerDirector, method: str, url: str, headers: dict, body: dict | None = None, timeout: int = 10, retries: int = DEFAULT_RETRY_LIMIT) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method, headers=headers | ({"Content-Type": "application/json"} if body is not None else {}))
    attempt = 0
    while True:
        try:
            with opener.open(request, timeout=timeout) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if attempt >= retries:
                raise UsageError(f"{method} {url} -> HTTP {exc.code}: {exc.read().decode('utf-8', errors='replace')[:500]}") from exc
        except (urllib.error.URLError, OSError):
            time.sleep(1)
            continue
        attempt += 1
        time.sleep(1)

def refresh_access_token(auth: dict, opener: urllib.request.OpenerDirector, auth_path: Path, timeout: int, retries: int = DEFAULT_RETRY_LIMIT) -> str:
    tokens = auth.get("tokens") or {}
    if tokens.get("access_token") and not token_expired(tokens["access_token"]):
        return tokens["access_token"]
    if not tokens.get("refresh_token"):
        raise UsageError("access token is expired and auth.json has no refresh_token")
    payload = jwt_payload(tokens.get("access_token") or tokens.get("id_token") or "")
    status, refreshed = request_json(opener, "POST", "https://auth.openai.com/oauth/token", {}, {
        "client_id": payload.get("client_id") or "app_EMoamEEZ73f0CkXaXp7hrann",
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"],
    }, timeout, retries)
    if status != 200 or not refreshed.get("access_token"):
        raise UsageError(f"token refresh did not return access_token: HTTP {status}")
    tokens.update({k: refreshed[k] for k in ("access_token", "id_token", "refresh_token") if refreshed.get(k)})
    auth["last_refresh"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    write_json(auth_path, auth)
    return tokens["access_token"]

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_timestamp(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None

def coerce_float(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else None

def first_value(mapping: dict, *keys: str):
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None

def poll_sleep_seconds(acquire_started_at: float, interval: int | float, now: float | None = None) -> float:
    return max(max(interval, 1) - ((time.monotonic() if now is None else now) - acquire_started_at), 0)
