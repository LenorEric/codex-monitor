#!/usr/bin/env python3

import base64
import binascii
import json
import os
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

try:
    import winreg
except ImportError:
    winreg = None

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
TOKEN_REFRESH_REMAINING_FRACTION = 0.30
TOKEN_REFRESH_FALLBACK_SECONDS = 60
UNKNOWN_EVENT_ACCOUNT_ID = "unknown"
UNKNOWN_EVENT_ACCOUNT_LABEL = "Unknown"

class UsageError(RuntimeError):
    pass

class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        source, target = urllib.parse.urlsplit(req.full_url), urllib.parse.urlsplit(newurl)
        if source.scheme.lower() == "https" and target.scheme.lower() != "https":
            raise urllib.error.HTTPError(req.full_url, code, "Refusing HTTPS redirect downgrade", headers, fp)
        sensitive = {"authorization", "proxy-authorization", "chatgpt-account-id"} & {key.lower() for key in req.unredirected_hdrs}
        source_origin = (source.scheme.lower(), source.hostname, source.port or (443 if source.scheme.lower() == "https" else 80))
        target_origin = (target.scheme.lower(), target.hostname, target.port or (443 if target.scheme.lower() == "https" else 80))
        if sensitive and source_origin != target_origin:
            raise urllib.error.HTTPError(req.full_url, code, "Refusing authenticated cross-origin redirect", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)

def is_client_disconnect(exc: BaseException) -> bool:
    return isinstance(exc, (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)) or (
        isinstance(exc, OSError) and (getattr(exc, "winerror", None) in {10053, 10054} or getattr(exc, "errno", None) in {32, 104})
    )

def empty_token_totals() -> dict:
    return {
        "inputTokens": 0,
        "freshInputTokens": 0,
        "cachedInputTokens": 0,
        "cacheWriteInputTokens": 0,
        "outputTokens": 0,
        "totalTokens": 0,
        "requests": 0,
    }

def empty_cost_totals() -> dict:
    return {
        "inputCostUsd": 0.0,
        "cachedInputCostUsd": 0.0,
        "cacheWriteInputCostUsd": 0.0,
        "outputCostUsd": 0.0,
        "totalCostUsd": 0.0,
    }

def codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex").expanduser()

def codex_switch_home() -> Path:
    return Path.home() / ".codex-switch"

def load_json(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as exc:
        raise UsageError(f"missing {path}") from exc

def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(temp_name, 0o600)
        except OSError:
            pass
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise

def jwt_payload(token: str) -> dict:
    try:
        return json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=" * (-len(token.split(".")[1]) % 4)))
    except (binascii.Error, IndexError, json.JSONDecodeError, UnicodeDecodeError):
        return {}

def token_expired(token: str, skew_seconds: int = TOKEN_REFRESH_FALLBACK_SECONDS, remaining_fraction: float = TOKEN_REFRESH_REMAINING_FRACTION) -> bool:
    payload = jwt_payload(token)
    try:
        expires_at = float(payload["exp"])
    except (KeyError, TypeError, ValueError):
        return True
    try:
        issued_at = float(payload["iat"])
    except (KeyError, TypeError, ValueError):
        issued_at = expires_at
    refresh_margin = (expires_at - issued_at) * remaining_fraction if expires_at > issued_at else skew_seconds
    return expires_at <= time.time() + refresh_margin

def normalize_proxy(value: str) -> str:
    return value if "://" in value else f"http://{value}"

def load_windows_system_proxy() -> dict:
    if winreg is None:
        return {}
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

def load_environment_proxy() -> dict:
    proxies = urllib.request.getproxies()
    fallback = normalize_proxy(proxies["all"]) if proxies.get("all") else None
    return {scheme: normalize_proxy(proxies[scheme]) if proxies.get(scheme) else fallback for scheme in ("http", "https") if proxies.get(scheme) or fallback}

def load_proxy_config(home: Path) -> dict:
    if tomllib is None or not (home / "config.toml").exists():
        return {}
    with (home / "config.toml").open("rb") as f:
        config = tomllib.load(f)
    configured = {k.upper(): normalize_proxy(v) for k, v in config.items() if k.upper() in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"} and isinstance(v, str) and v}
    if configured.get("ALL_PROXY"):
        return {"http": configured.get("HTTP_PROXY", configured["ALL_PROXY"]), "https": configured.get("HTTPS_PROXY", configured["ALL_PROXY"])}
    return {scheme: configured[f"{scheme.upper()}_PROXY"] for scheme in ("http", "https") if configured.get(f"{scheme.upper()}_PROXY")}

def opener_for(home: Path) -> urllib.request.OpenerDirector:
    proxies = load_windows_system_proxy() or load_environment_proxy() or load_proxy_config(home)
    if not proxies:
        raise UsageError("no system proxy is configured; refusing to connect directly")
    return urllib.request.build_opener(urllib.request.ProxyHandler(proxies), SafeRedirectHandler())

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
    redirected_headers = {key: value for key, value in headers.items() if key.lower() not in {"authorization", "proxy-authorization", "chatgpt-account-id"}}
    request = urllib.request.Request(url, data=data, method=method, headers=redirected_headers | ({"Content-Type": "application/json"} if body is not None else {}))
    for key, value in headers.items():
        if key.lower() in {"authorization", "proxy-authorization", "chatgpt-account-id"}:
            request.add_unredirected_header(key, value)
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

def refresh_access_token(auth: dict, opener: urllib.request.OpenerDirector, auth_path: Path, timeout: int, retries: int = DEFAULT_RETRY_LIMIT, auth_lock=None, refreshed_callback=None) -> str:
    tokens = auth.get("tokens") or {}
    if tokens.get("access_token") and not token_expired(tokens["access_token"]):
        return tokens["access_token"]
    if auth_lock is not None:
        with auth_lock:
            auth.clear()
            auth.update(load_json(auth_path))
            return refresh_access_token(auth, opener, auth_path, timeout, retries, refreshed_callback=refreshed_callback)
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
    if refreshed_callback is not None:
        refreshed_callback()
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
