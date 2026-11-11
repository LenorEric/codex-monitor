#!/usr/bin/env python3

import base64
import binascii
import json
import math
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REQUEST_STOP = threading.local()

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
    "pro": 10.0,
    "unknown": 1.0,
}

MIN_DELTA_COST_PER_PERCENT_USD = 0.01
RATIO_DEVIATION_MULTIPLIER = 3.0
RESET_TIME_JITTER_SECONDS = 60
DEFAULT_RETRY_LIMIT = 3
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10
TOKEN_REFRESH_REMAINING_FRACTION = 0.30
TOKEN_REFRESH_FALLBACK_SECONDS = 60
TOKEN_REFRESH_MAX_MARGIN_SECONDS = 10 * 60
UNKNOWN_EVENT_ACCOUNT_ID = "unknown"
UNKNOWN_EVENT_ACCOUNT_LABEL = "Unknown"

class UsageError(RuntimeError):
    pass

class UsageHttpError(UsageError):
    def __init__(self, method: str, url: str, status: int, body: str):
        super().__init__(f"{method} {url} -> HTTP {status}: {body[:500]}")
        self.status = status
        self.body = body

    def is_expired_token(self) -> bool:
        text = self.body.casefold()
        return self.status == 401 and ("token_expired" in text or "authentication token is expired" in text)

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

def atomic_write_bytes(path: Path, data: bytes, *, private: bool = False, replace_attempts: int = 1) -> None:
    if replace_attempts < 1:
        raise ValueError("replace_attempts must be positive")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if private:
            try:
                os.chmod(temp_name, 0o600)
            except OSError:
                pass
        for attempt in range(replace_attempts):
            try:
                os.replace(temp_name, path)
                break
            except PermissionError:
                if attempt == replace_attempts - 1 or os.name != "nt":
                    raise
                time.sleep(0.02 * (attempt + 1))
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def write_json(path: Path, data: dict) -> None:
    atomic_write_bytes(path, (json.dumps(data, indent=2) + "\n").encode("utf-8"), private=True)

def jwt_payload(token: str) -> dict:
    try:
        return json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=" * (-len(token.split(".")[1]) % 4)))
    except (binascii.Error, IndexError, json.JSONDecodeError, UnicodeDecodeError):
        return {}

def token_account_id(tokens: dict | None) -> str | None:
    tokens = tokens if isinstance(tokens, dict) else {}
    if tokens.get("account_id"):
        return str(tokens["account_id"])
    for name in ("id_token", "access_token"):
        claims = jwt_payload(tokens.get(name) or "")
        auth_claims = claims.get("https://api.openai.com/auth") if isinstance(claims.get("https://api.openai.com/auth"), dict) else {}
        account_id = claims.get("chatgpt_account_id") or auth_claims.get("chatgpt_account_id")
        if account_id:
            return str(account_id)
    return None

def auth_account_id(auth: dict | None) -> str | None:
    return token_account_id((auth or {}).get("tokens") if isinstance(auth, dict) else None)

def token_expired(token: str, skew_seconds: int = TOKEN_REFRESH_FALLBACK_SECONDS, remaining_fraction: float = TOKEN_REFRESH_REMAINING_FRACTION, max_margin_seconds: int = TOKEN_REFRESH_MAX_MARGIN_SECONDS) -> bool:
    payload = jwt_payload(token)
    try:
        expires_at = float(payload["exp"])
    except (KeyError, TypeError, ValueError):
        return True
    try:
        issued_at = float(payload["iat"])
    except (KeyError, TypeError, ValueError):
        issued_at = expires_at
    refresh_margin = min((expires_at - issued_at) * remaining_fraction, max_margin_seconds) if expires_at > issued_at else skew_seconds
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
    return urllib.request.build_opener(urllib.request.ProxyHandler(proxies), SafeRedirectHandler())

def retry_operation(operation, retries: int = DEFAULT_RETRY_LIMIT, delay_seconds: float = 1.0):
    for attempt in range(retries + 1):
        try:
            return operation()
        except Exception:
            if attempt >= retries:
                raise
            time.sleep(delay_seconds)

def request_json(opener: urllib.request.OpenerDirector, method: str, url: str, headers: dict, body: dict | None = None, timeout: int = DEFAULT_REQUEST_TIMEOUT_SECONDS, retries: int = DEFAULT_RETRY_LIMIT, retry_network_errors_forever: bool = True, response_callback=None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    redirected_headers = {key: value for key, value in headers.items() if key.lower() not in {"authorization", "proxy-authorization", "chatgpt-account-id"}}
    request = urllib.request.Request(url, data=data, method=method, headers=redirected_headers | ({"Content-Type": "application/json"} if body is not None else {}))
    for key, value in headers.items():
        if key.lower() in {"authorization", "proxy-authorization", "chatgpt-account-id"}:
            request.add_unredirected_header(key, value)
    attempt = 0
    while True:
        stop = getattr(REQUEST_STOP, "event", None)
        if stop is not None and stop.is_set():
            raise UsageError("HTTP acquisition cancelled during shutdown")
        try:
            with opener.open(request, timeout=timeout) as response:
                raw = response.read()
                try:
                    decoded = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    if response_callback:
                        response_callback(response.status, raw.decode("utf-8", errors="replace"))
                    raise UsageError(f"Incompatible API response from {method} {url}: expected UTF-8 JSON") from exc
                if response_callback:
                    response_callback(response.status, decoded)
                return response.status, decoded
        except urllib.error.HTTPError as exc:
            error = UsageHttpError(method, url, exc.code, exc.read().decode("utf-8", errors="replace"))
            if response_callback:
                response_callback(exc.code, error.body)
            if error.is_expired_token() or attempt >= retries:
                raise error from exc
        except (urllib.error.URLError, OSError) as exc:
            if not retry_network_errors_forever and attempt >= retries:
                raise UsageError(f"{method} {url} -> network error: {exc}") from exc
            if retry_network_errors_forever:
                stop.wait(1) if stop is not None else time.sleep(1)
                continue
        attempt += 1
        stop.wait(1) if stop is not None else time.sleep(1)

def refresh_access_token(auth: dict, opener: urllib.request.OpenerDirector, auth_path: Path | None, timeout: int, retries: int = DEFAULT_RETRY_LIMIT, auth_lock=None, refreshed_callback=None, allow_refresh: bool = True, force_refresh: bool = False) -> str:
    tokens = auth.get("tokens") or {}
    access_token = tokens.get("access_token")
    if not force_refresh and access_token and not (token_expired(access_token) if allow_refresh else token_expired(access_token, 0, 0, 0)):
        return access_token
    if auth_path is None and (auth_lock is not None or refreshed_callback is None):
        raise UsageError("in-memory token refresh requires a persistence callback and no file lock")
    if auth_lock is not None:
        refreshed = False

        def mark_refreshed():
            nonlocal refreshed
            refreshed = True

        with auth_lock:
            auth.clear()
            auth.update(load_json(auth_path))
            access_token = refresh_access_token(auth, opener, auth_path, timeout, retries, refreshed_callback=mark_refreshed, allow_refresh=allow_refresh, force_refresh=force_refresh)
        # Reconciliation may acquire data locks; never invoke it while holding the account lock.
        if refreshed and refreshed_callback is not None:
            refreshed_callback()
        return access_token
    if not tokens.get("refresh_token"):
        raise UsageError("access token is expired and auth.json has no refresh_token")
    old_account_id = token_account_id(tokens)
    payload = jwt_payload(tokens.get("access_token") or tokens.get("id_token") or "")
    try:
        status, refreshed = request_json(opener, "POST", "https://auth.openai.com/oauth/token", {}, {
            "client_id": payload.get("client_id") or "app_EMoamEEZ73f0CkXaXp7hrann",
            "grant_type": "refresh_token",
            "refresh_token": tokens["refresh_token"],
        }, timeout, 0, False)
    except UsageError as exc:
        raise UsageError(f"token refresh failed without retry because rotating-token requests are not safely repeatable: {exc}") from exc
    if status != 200 or not isinstance(refreshed, dict) or not isinstance(refreshed.get("access_token"), str) or not refreshed["access_token"]:
        raise UsageError(f"token refresh did not return access_token: HTTP {status}")
    if any(refreshed.get(key) is not None and (not isinstance(refreshed[key], str) or not refreshed[key]) for key in ("id_token", "refresh_token", "account_id")):
        raise UsageError("Incompatible token refresh API response: expected nonempty token and account_id strings when present")
    new_account_id = token_account_id(refreshed)
    if not old_account_id or new_account_id != old_account_id:
        console_log("Token refresh ignored because the refreshed account_id does not match the recorded account_id.", level="warning")
        raise UsageError("token refresh returned credentials for an unverifiable or different account")
    tokens.update({k: refreshed[k] for k in ("access_token", "id_token", "refresh_token") if refreshed.get(k)})
    auth["last_refresh"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if auth_path is not None:
        write_json(auth_path, auth)
    if refreshed_callback is not None:
        refreshed_callback()
    return tokens["access_token"]

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def resets_match(first, second):
    first, second = parse_timestamp(first), parse_timestamp(second)
    return first == second or first is not None and second is not None and abs(first - second) <= 15


def parse_timestamp(value: str | int | float | None) -> int | float | None:
    # Saved history predates numeric reset times; API validation lives at ingress.
    if type(value) in (int, float):
        return value if math.isfinite(value) else None
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None

def format_console_timestamp(value: str | int | float | None) -> str | None:
    try:
        epoch = parse_timestamp(value)
        if epoch is not None:
            local_time = datetime.fromtimestamp(epoch, timezone.utc).astimezone().isoformat(sep=" ", timespec="seconds")
            return f"{local_time[:19]} {local_time[19:]}"
    except (OSError, OverflowError, ValueError):
        pass
    return None

def format_console_log(message: str, *, level: str = "event", at: str | int | float | None = None) -> str:
    local_time = format_console_timestamp(at) or format_console_timestamp(time.time())
    return f"[{level}]({local_time}) {message}"

def console_log(message: str, *, level: str = "event", at: str | int | float | None = None) -> None:
    print(format_console_log(message, level=level, at=at), file=sys.stderr if level in {"error", "warning"} else sys.stdout, flush=True)

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
