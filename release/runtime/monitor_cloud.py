#!/usr/bin/env python3

from __future__ import annotations

import base64
import functools
import hashlib
import hmac
import json
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from monitor_accounts import api_identity_id, atomic_write_json, auth_identity, parse_auth_bytes
from monitor_cloud_queue import CloudOperationQueue, OperationCancelled, OperationSkipped
from monitor_common import SafeRedirectHandler
from monitor_skills import SkillError
from monitor_usage_sync import canonical_json, content_hash, validate_sync_operation

AUTO_PUSH_STABLE_SECONDS = 120
AUTO_PUSH_RETRY_SECONDS = 30
AUTO_PUSH_MAX_ATTEMPTS = 3
WEBDAV_MKCOL_MAX_ATTEMPTS = 3
WEBDAV_MKCOL_RETRY_SECONDS = 1
AUTO_FETCH_INTERVAL_SECONDS = 60 * 60
USAGE_SYNC_INTERVAL_SECONDS = 60 * 60
LEGACY_PASSPHRASE_SALT = b"codex-switch-passphrase-v1"
USAGE_PACK_FORMAT_VERSION = 3
USAGE_PACK_BUCKET_BITS = 6
USAGE_PACK_MAX_BYTES = 16 * 1024
USAGE_REGULAR_VERIFY_PACKS = 2
USAGE_FULL_VERIFY_INTERVAL_SECONDS = 30 * 24 * 60 * 60
USAGE_ACCOUNT_ID_DOMAIN = "codex-monitor-usage-account-v3"


def _serialized_cloud_operation(method):
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        if self.operation_queue.in_worker():
            with self._operation_lock:
                return method(self, *args, **kwargs)
        return self.operation_queue.wait(self.submit_operation(method.__name__, args, kwargs, source="automatic" if threading.current_thread().name == "cloud-maintenance" else "internal"))
    return wrapped


class CloudError(RuntimeError):
    def __init__(self, message: str, status: int = 400, *, http_status: int | None = None, category: str | None = None, details: list[dict] | None = None, decrypt_failed: bool = False):
        super().__init__(message)
        self.status = status
        self.http_status = http_status
        self.category = category
        self.details = details
        self.decrypt_failed = decrypt_failed


def validate_server_host(host) -> str:
    if host not in {"127.0.0.1", "0.0.0.0"}:
        raise CloudError("Dashboard IP must be 127.0.0.1 or 0.0.0.0")
    return host


def load_server_config(config_path: Path) -> dict:
    try:
        config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"host": "0.0.0.0"}
    except (OSError, json.JSONDecodeError) as exc:
        raise CloudError(f"Cannot read dashboard config: {exc}", 500) from exc
    server = config.get("server", {"host": "0.0.0.0"}) if isinstance(config, dict) else None
    if not isinstance(server, dict):
        raise CloudError("Unsupported dashboard config", 500)
    return {"host": validate_server_host(server.get("host"))}


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _elapsed_since(timestamp: str | None, default: float) -> float:
    try:
        return max(0.0, time.time() - datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()) if timestamp else default
    except (AttributeError, TypeError, ValueError):
        return default


def new_control_password_salt() -> str:
    return base64.b64encode(secrets.token_bytes(16)).decode()


def hash_control_password(password: str, salt: str) -> str:
    derived = hashlib.scrypt(password.encode("utf-8"), salt=base64.b64decode(salt, validate=True), n=1 << 14, r=8, p=1, dklen=32, maxmem=64 * 1024 * 1024)
    return f"scrypt-v2$16384$8$1${base64.b64encode(derived).decode()}"


def control_password_matches(password, verifier: str, salt: str) -> bool:
    if not isinstance(password, str):
        return False
    try:
        scheme, n, r, p, expected = verifier.split("$", 4)
        if scheme != "scrypt-v2":
            return False
        derived = hashlib.scrypt(password.encode("utf-8"), salt=base64.b64decode(salt, validate=True), n=int(n), r=int(r), p=int(p), dklen=32, maxmem=64 * 1024 * 1024)
        return hmac.compare_digest(derived, base64.b64decode(expected, validate=True))
    except (ValueError, TypeError):
        return False


def valid_control_password_hash(verifier) -> bool:
    try:
        scheme, n, r, p, expected = verifier.split("$", 4)
        return scheme == "scrypt-v2" and int(n) == 1 << 14 and int(r) == 8 and int(p) == 1 and len(base64.b64decode(expected, validate=True)) == 32
    except (AttributeError, ValueError, TypeError):
        return False


def valid_control_password_salt(salt) -> bool:
    try:
        return len(base64.b64decode(salt, validate=True)) == 16
    except (ValueError, TypeError):
        return False


def control_password_is_configured(control: dict) -> bool:
    return isinstance(control, dict) and valid_control_password_hash(control.get("passwordHash")) and valid_control_password_salt(control.get("passwordSalt"))


def control_password_is_compromised(control: dict) -> bool:
    return isinstance(control, dict) and bool(control.get("passwordHash")) and not control_password_is_configured(control)


def validate_new_control_password(password) -> str:
    if not isinstance(password, str):
        raise CloudError("Control password must be text")
    password = password.strip()
    if not password:
        raise CloudError("Control password is required")
    if password == "123456":
        raise CloudError("Choose a control password other than 123456")
    if len(password) > 1024:
        raise CloudError("Control password is too long")
    return password


def normalized_webdav_identity(base_url: str, username: str) -> str:
    parsed = urllib.parse.urlsplit(base_url if "://" in base_url else f"//{base_url}")
    host = (parsed.hostname or "").lower().rstrip(".")
    port = f":{parsed.port}" if parsed.port and not (parsed.scheme.lower() == "https" and parsed.port == 443 or parsed.scheme.lower() == "http" and parsed.port == 80) else ""
    path = "/".join(segment for segment in parsed.path.replace("\\", "/").split("/") if segment)
    return f"{host}{port}{f'/{path}' if path else ''}\n{username.strip()}"


def webdav_passphrase_salt(base_url: str, username: str) -> bytes:
    return hashlib.sha256(f"codex-switch-webdav-passphrase-v2\n{normalized_webdav_identity(base_url, username)}".encode("utf-8")).digest()


def passphrase_hash(passphrase: str, base_url: str, username: str) -> str:
    salt = webdav_passphrase_salt(base_url, username)
    derived = hashlib.scrypt(passphrase.encode("utf-8"), salt=salt, n=1 << 15, r=8, p=1, dklen=32, maxmem=128 * 1024 * 1024)
    return f"scrypt-key-v2$32768$8$1${base64.b64encode(salt).decode()}${base64.b64encode(derived).decode()}"


def _passphrase_key(verifier: str) -> bytes:
    try:
        scheme, n, r, p, salt, derived = verifier.split("$", 5)
        key = base64.b64decode(derived, validate=True)
        decoded_salt = base64.b64decode(salt, validate=True)
        if (scheme not in {"scrypt-key-v1", "scrypt-key-v2"} or int(n) != 1 << 15 or int(r) != 8 or int(p) != 1
                or scheme == "scrypt-key-v1" and decoded_salt != LEGACY_PASSPHRASE_SALT or scheme == "scrypt-key-v2" and len(decoded_salt) != 32 or len(key) != 32):
            raise ValueError
        return key
    except (AttributeError, ValueError, TypeError) as exc:
        raise CloudError("Invalid saved encryption passphrase hash") from exc


def _passphrase_salt(verifier: str) -> bytes:
    try:
        scheme, n, r, p, salt, derived = verifier.split("$", 5)
        decoded = base64.b64decode(salt, validate=True)
        if (scheme not in {"scrypt-key-v1", "scrypt-key-v2"} or int(n) != 1 << 15 or int(r) != 8 or int(p) != 1
                or scheme == "scrypt-key-v1" and decoded != LEGACY_PASSPHRASE_SALT or scheme == "scrypt-key-v2" and len(decoded) != 32
                or len(base64.b64decode(derived, validate=True)) != 32):
            raise ValueError
        return decoded
    except (AttributeError, ValueError, TypeError) as exc:
        raise CloudError("Invalid saved encryption passphrase hash") from exc


def valid_passphrase_hash(verifier) -> bool:
    try:
        _passphrase_key(verifier)
        return True
    except CloudError:
        return False


def passphrase_hash_matches_webdav(verifier, base_url: str, username: str) -> bool:
    try:
        scheme = verifier.split("$", 1)[0]
        return valid_passphrase_hash(verifier) and (scheme == "scrypt-key-v1" or _passphrase_salt(verifier) == webdav_passphrase_salt(base_url, username))
    except (AttributeError, CloudError):
        return False


class CryptoBox:
    def __init__(self, passphrase_hash_value: str, descriptor: dict):
        if not passphrase_hash_value:
            if descriptor != {"format": "codex-switch-plain", "version": 1}:
                raise CloudError("The encryption passphrase does not match this remote root", 409, decrypt_failed=True)
            self.key = hashlib.sha256(b"codex-switch-plain-webdav-v1").digest()
            self.plaintext = True
            return
        self.key = _passphrase_key(passphrase_hash_value)
        self.plaintext = False
        try:
            expected_version = 1 if passphrase_hash_value.startswith("scrypt-key-v1$") else 2
            if (descriptor.get("format") != "codex-switch-crypto" or descriptor.get("version") != expected_version or descriptor.get("kdf") != "scrypt"
                    or base64.b64decode(descriptor["salt"], validate=True) != _passphrase_salt(passphrase_hash_value)
                    or int(descriptor["n"]) != 1 << 15 or int(descriptor["r"]) != 8 or int(descriptor["p"]) != 1):
                raise ValueError
        except (AttributeError, KeyError, ValueError, TypeError) as exc:
            raise CloudError("The encryption passphrase does not match this remote root", 409, decrypt_failed=True) from exc
        verifier = hmac.new(self.key, b"codex-switch-verifier-v1", hashlib.sha256).digest()
        if not hmac.compare_digest(verifier, base64.b64decode(descriptor.get("verifier", ""))):
            raise CloudError("The encryption passphrase does not match this remote root", 409, decrypt_failed=True)

    @staticmethod
    def descriptor(passphrase_hash_value: str) -> dict:
        if not passphrase_hash_value:
            return {"format": "codex-switch-plain", "version": 1}
        key = _passphrase_key(passphrase_hash_value)
        return {
            "format": "codex-switch-crypto", "version": 1 if passphrase_hash_value.startswith("scrypt-key-v1$") else 2, "kdf": "scrypt", "salt": base64.b64encode(_passphrase_salt(passphrase_hash_value)).decode(), "n": 1 << 15, "r": 8, "p": 1,
            "verifier": base64.b64encode(hmac.new(key, b"codex-switch-verifier-v1", hashlib.sha256).digest()).decode(),
        }

    def encrypt(self, purpose: str, data: bytes) -> bytes:
        if self.plaintext:
            return data
        nonce = secrets.token_bytes(12)
        header = {"format": "codex-switch-encrypted", "version": 1, "purpose": purpose, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        aad = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
        return json.dumps({"header": header, "nonce": base64.b64encode(nonce).decode(), "ciphertext": base64.b64encode(AESGCM(self.key).encrypt(nonce, data, aad)).decode()}, separators=(",", ":")).encode()

    def decrypt(self, purpose: str, payload: bytes, limit: int = 600 * 1024 * 1024) -> bytes:
        if self.plaintext:
            if len(payload) > limit:
                raise CloudError("WebDAV payload is too large", 409)
            return payload
        try:
            envelope = json.loads(payload)
            header = envelope["header"]
            if header.get("format") != "codex-switch-encrypted" or header.get("version") != 1 or header.get("purpose") != purpose or not isinstance(header.get("size"), int) or header["size"] < 0 or header["size"] > limit:
                raise ValueError
            aad = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
            data = AESGCM(self.key).decrypt(base64.b64decode(envelope["nonce"], validate=True), base64.b64decode(envelope["ciphertext"], validate=True), aad)
        except Exception as exc:
            raise CloudError("Encrypted payload authentication failed", 409, decrypt_failed=True) from exc
        if len(data) != header["size"] or hashlib.sha256(data).hexdigest() != header.get("sha256"):
            raise CloudError("Encrypted payload completion check failed", 409)
        return data

    def account_key(self, account_id: str) -> str:
        return hmac.new(self.key, b"account:" + account_id.encode(), hashlib.sha256).hexdigest()

    def placeholder_account_key(self) -> str:
        return hmac.new(self.key, b"account-placeholder:" + secrets.token_bytes(32), hashlib.sha256).hexdigest()


class WebDavClient:
    def __init__(self, config: dict):
        self.base = str(config.get("baseUrl", "")).rstrip("/") + "/"
        parsed = urllib.parse.urlsplit(self.base)
        if parsed.scheme.lower() != "https" and not (parsed.scheme.lower() == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}):
            raise CloudError("WebDAV requires HTTPS; plain HTTP is allowed only for loopback development servers", 400, category="configuration")
        self.root = str(config.get("remoteRoot", "codex-switch-sync")).strip("/")
        token = base64.b64encode(f"{config.get('username', '')}:{config.get('password', '')}".encode()).decode()
        self.opener = urllib.request.build_opener(SafeRedirectHandler())
        self.authorization = f"Basic {token}"

    def _url(self, path: str = "") -> str:
        parts = [urllib.parse.quote(part, safe="") for part in f"{self.root}/{path}".strip("/").split("/") if part]
        return urllib.parse.urljoin(self.base, "/".join(parts))

    def request(self, method: str, path: str = "", data: bytes | None = None, headers: dict | None = None, expected: tuple[int, ...] = (200, 201, 204)) -> tuple[bytes, str | None, int]:
        request = urllib.request.Request(self._url(path), data=data, method=method, headers=headers or {})
        request.add_unredirected_header("Authorization", self.authorization)
        try:
            with self.opener.open(request, timeout=30) as response:
                body, etag, status = response.read(), response.headers.get("ETag"), response.status
        except urllib.error.HTTPError as exc:
            if exc.code in expected:
                return exc.read(), exc.headers.get("ETag"), exc.code
            raise CloudError(f"WebDAV {method} failed with HTTP {exc.code}", 409 if exc.code in {409, 412} else 502, http_status=exc.code, category="http") from exc
        except OSError as exc:
            raise CloudError(f"WebDAV connection failed: {exc}", 502, category="network") from exc
        if status not in expected:
            raise CloudError(f"WebDAV {method} returned HTTP {status}", 502, http_status=status, category="protocol")
        return body, etag, status

    def ensure_directories(self, path: str) -> None:
        current = ""
        for part in path.strip("/").split("/"):
            current = f"{current}/{part}".strip("/")
            for attempt in range(WEBDAV_MKCOL_MAX_ATTEMPTS):
                try:
                    self.request("MKCOL", current, expected=(201, 405))
                    break
                except CloudError as exc:
                    if attempt + 1 >= WEBDAV_MKCOL_MAX_ATTEMPTS or exc.category != "network" and not (exc.category == "http" and exc.http_status in {408, 425, 429, 500, 502, 503, 504}):
                        raise
                    time.sleep(WEBDAV_MKCOL_RETRY_SECONDS)

    def get(self, path: str) -> tuple[bytes, str]:
        body, etag, _ = self.request("GET", path)
        if not etag or etag.startswith("W/"):
            raise CloudError("WebDAV requires strong ETags", 409)
        return body, etag

    def get_if_changed(self, path: str, etag: str | None) -> tuple[bytes | None, str]:
        body, current_etag, status = self.request("GET", path, headers={"If-None-Match": etag} if etag else None, expected=(200, 304))
        if status == 304:
            return None, etag
        if not current_etag or current_etag.startswith("W/"):
            raise CloudError("WebDAV requires strong ETags", 409)
        return body, current_etag

    def put(self, path: str, data: bytes, etag: str | None = None, create: bool = False) -> str:
        headers = {"Content-Type": "application/octet-stream"}
        if create:
            headers["If-None-Match"] = "*"
        elif etag:
            headers["If-Match"] = etag
        _, new_etag, _ = self.request("PUT", path, data, headers)
        if not new_etag or new_etag.startswith("W/"):
            downloaded, new_etag = self.get(path)
            if downloaded != data:
                raise CloudError("WebDAV PUT read-back verification failed", 409)
        return new_etag

    def delete(self, path: str, etag: str | None = None) -> None:
        if etag:
            self.request("DELETE", path, headers={"If-Match": etag}, expected=(200, 204, 404))
        else:
            self.request("DELETE", path, expected=(200, 204, 404))

    def list(self, path: str) -> list[str]:
        return [item["name"] for item in self.list_details(path)]

    def list_details(self, path: str) -> list[dict]:
        body, _, _ = self.request("PROPFIND", path, b"", {"Depth": "1"}, expected=(207,))
        import xml.etree.ElementTree as ET
        try:
            responses = [node for node in ET.fromstring(body).iter() if node.tag.endswith("response")]
        except ET.ParseError as exc:
            raise CloudError("WebDAV returned invalid directory metadata", 502) from exc
        prefix = urllib.parse.urlparse(self._url(path)).path.rstrip("/") + "/"
        items = {}
        for response in responses:
            href = next((node.text for node in response.iter() if node.tag.endswith("href") and node.text), None)
            remote_path = urllib.parse.urlparse(href).path if href else ""
            if not remote_path.startswith(prefix) or remote_path.rstrip("/") == prefix.rstrip("/"):
                continue
            name = urllib.parse.unquote(remote_path[len(prefix):].strip("/"))
            etag = next((node.text for node in response.iter() if node.tag.endswith("getetag") and node.text), None)
            items[name] = {"name": name, "etag": etag if etag and not etag.startswith("W/") else None}
        return [items[name] for name in sorted(items)]


class CloudManager:
    def __init__(self, private_root: Path, skills, accounts):
        self.private_root, self.skills, self.accounts = Path(private_root), skills, accounts
        self.config_path = self.private_root / "config.json"
        self.machine_path = self.private_root / "machine.json"
        self.state_path = self.private_root / "cloud-state.json"
        self._ensure_local_files()
        try:
            self._config, self._config_error = self._read_config(), None
        except CloudError as exc:
            self._config, self._config_error = {"version": 1, "webdav": {}}, str(exc)
        self._machine = json.loads(self.machine_path.read_text(encoding="utf-8"))
        self._state = json.loads(self.state_path.read_text(encoding="utf-8"))
        self._observed_skill_revision = None
        self._operation_lock = threading.RLock()
        self._queue = CloudOperationQueue()
        self._observed_skill_hashes = None
        self._pending_skill_pushes = {}
        self._auto_push_failures = {}
        self._auto_push_failure_id = 0
        self._last_auto_fetch_at = time.monotonic() - min(_elapsed_since(self._state.get("lastAutoFetchAt"), AUTO_FETCH_INTERVAL_SECONDS), AUTO_FETCH_INTERVAL_SECONDS)
        self._usage_data = None
        self._usage_account_ids = {}
        self._next_usage_sync_at = time.monotonic()
        if not self._config_error:
            self._remember_legacy_usage_aliases()

    @property
    def operation_queue(self):
        return self._queue

    def _queue_generation(self):
        webdav = getattr(self, "_config", {}).get("webdav", {})
        return hashlib.sha256(json.dumps({key: webdav.get(key) for key in ("enabled", "baseUrl", "username", "password", "remoteRoot", "encryptionPassphraseHash", "allowOptimisticWrites")}, sort_keys=True).encode()).hexdigest()

    def _queue_sanitize(self, value):
        if isinstance(value, Exception):
            return {"message": self._queue_sanitize(str(value)), "status": getattr(value, "status", 500)}
        if isinstance(value, dict):
            return {key: self._queue_sanitize(item) for key, item in value.items() if isinstance(item, bool) or key not in {"auth", "rawResponse", "authIdentity"} and not any(secret in key.lower() for secret in ("password", "passphrase", "apikey", "api_key", "token", "authorization", "cookie", "headertoml", "commontoml"))}
        if isinstance(value, (tuple, list)):
            return [self._queue_sanitize(item) for item in value]
        if isinstance(value, str):
            secret = getattr(self, "_config", {}).get("webdav", {}).get("password", "")
            if secret:
                value = value.replace(secret, "[redacted]")
        return value

    def submit_operation(self, method_name, args=(), kwargs=None, *, source="manual", context=None, on_success=None, validate=None):
        args, kwargs = deepcopy(args), deepcopy(kwargs or {})
        generation = self._queue_generation()
        secrets_to_redact = [self._config.get("webdav", {}).get("password", "")] if hasattr(self, "_config") else []
        if method_name == "update_config" and args and isinstance(args[0], dict):
            secrets_to_redact.extend(args[0].get("webdav", {}).get(key, "") for key in ("password", "encryptionPassphrase"))
            secrets_to_redact.append(args[0].get("controlPassword", ""))

        def sanitize(value):
            value = self._queue_sanitize(value)
            if isinstance(value, dict):
                return {key: sanitize(item) for key, item in value.items()}
            if isinstance(value, list):
                return [sanitize(item) for item in value]
            if isinstance(value, str):
                for secret in secrets_to_redact:
                    if isinstance(secret, str) and secret:
                        value = value.replace(secret, "[redacted]")
            return value

        def check():
            if self._queue_generation() != generation:
                raise OperationSkipped("WebDAV configuration changed while this operation was waiting; submit it again")
            if source == "automatic":
                if not self.config()["webdav"].get("enabled") or method_name == "sync_usage_data" and not self.config()["webdav"].get("usageDataAutoSync", True):
                    raise OperationSkipped("Automatic synchronization is disabled")
                self.ensure_no_pending_account_operation()
            if validate:
                validate()

        def execute():
            with self._operation_lock:
                result = getattr(self, method_name)(*args, **kwargs)
                if on_success:
                    result = on_success(result)
                return result

        # Only identical adjacent synchronization requests share execution.
        combine_key = (method_name, json.dumps([args, kwargs], sort_keys=True), generation, source) if method_name in {"push", "fetch", "sync_usage_data"} else None
        target = (context or {}).get("body", {})
        return self.operation_queue.submit(method_name, execute, target=str(target.get("name") or target.get("accountId") or target.get("accountKey") or target.get("snapshotId") or (", ".join(sorted(args[0])) if method_name in {"_upload_due_skills", "upload_skills"} and args and args[0] else "")),
            source=source, validate=check, combine_key=combine_key, context=context, sanitize=sanitize)

    def validate_queued_action(self, method_name, body, expected_remote=None):
        if method_name not in {"update_config", "reload_config", "unmanage_skill", "set_skill_shared"} and not self.config()["webdav"].get("enabled"):
            raise OperationSkipped("WebDAV is disabled")
        if method_name not in {"update_config", "reload_config", "test"}:
            self.ensure_no_pending_account_operation()
        if method_name in {"unmanage_skill", "set_skill_shared"}:
            if body["name"] not in self.skills.state["skills"] or not (self.skills.skills_root / body["name"] / "SKILL.md").is_file():
                raise OperationSkipped("The skill is no longer managed")
        if method_name in {"release_local_account", "share_local_account"}:
            with self.accounts.lock:
                account = next((item for item in self.accounts.manifest["accounts"] if item["id"] == body["accountId"]), None)
                if account is None:
                    raise OperationSkipped("The local account no longer exists")
                if method_name == "release_local_account" and (account.get("accountType") == "api" or account["id"] == self.accounts.manifest.get("activeAccountId") or len(self.accounts.manifest["accounts"]) <= 1):
                    raise OperationSkipped("The account can no longer be released: it is active, the only account, or an API account")
                if method_name == "share_local_account" and account.get("accountType") != "api":
                    raise OperationSkipped("Only API accounts can be shared")
        if method_name in {"bind_local_account", "link_local_account"}:
            with self.accounts.lock:
                if any((item.get("cloud") or {}).get("accountKey") == body["accountKey"] for item in self.accounts.manifest["accounts"]):
                    raise OperationSkipped("This cloud account is already managed locally")
        if method_name in {"bind_local_account", "link_local_account", "delete_remote_account"}:
            try:
                remote, etag = self.account_state(body["accountKey"])
            except CloudError as exc:
                if exc.http_status == 404 or "HTTP 404" in str(exc):
                    raise OperationSkipped("The remote account no longer exists") from exc
                raise
            if expected_remote and (expected_remote.get("etag") and etag != expected_remote["etag"] or expected_remote.get("state", {}).get("revisionId") != remote.get("revisionId")):
                raise OperationSkipped("The remote account changed while this operation was waiting; fetch and submit it again")
        if method_name == "restore_skills":
            if self._remote_snapshot()[1] != body["snapshotId"]:
                raise OperationSkipped("The remote skill snapshot changed before restore")

    def _ensure_local_files(self) -> None:
        self.private_root.mkdir(parents=True, exist_ok=True)
        if not self.machine_path.exists():
            atomic_write_json(self.machine_path, {"version": 1, "machineId": uuid.uuid4().hex, "createdAt": _timestamp()})
        if not self.config_path.exists():
            atomic_write_json(self.config_path, {
                "version": 1, "control": {"password": "", "passwordHash": "", "passwordSalt": "", "cookieSecret": secrets.token_urlsafe(32)},
                "server": {"host": "0.0.0.0"},
                "autoUpdate": {"enabled": False},
                "webdav": {
                    "enabled": False, "baseUrl": "https://dav.jianguoyun.com/dav/", "username": "", "password": "", "remoteRoot": "codex-switch-sync", "encryptionPassphrase": "", "encryptionPassphraseHash": "",
                    "skillsAutoUpload": True, "usageDataAutoSync": True, "allowOptimisticWrites": True,
                },
            })
        else:
            try:
                config = json.loads(self.config_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                config = None
            if isinstance(config, dict):
                control = config.get("control")
                if not isinstance(control, dict):
                    config["control"] = control = {}
                changed = False
                if "machineName" in config:
                    del config["machineName"]
                    changed = True
                if "server" not in config:
                    config["server"] = {"host": "0.0.0.0"}
                    changed = True
                if not isinstance(config.get("autoUpdate"), dict) or not isinstance(config["autoUpdate"].get("enabled"), bool):
                    config["autoUpdate"] = {"enabled": False}
                    changed = True
                if isinstance(control.get("password"), str) and control["password"]:
                    if control["password"] == "123456":
                        control["passwordHash"], control["passwordSalt"] = "", ""
                    else:
                        control["passwordSalt"] = new_control_password_salt()
                        control["passwordHash"] = hash_control_password(control["password"], control["passwordSalt"])
                    control["password"] = ""
                    changed = True
                elif "password" not in control:
                    control["password"] = ""
                    changed = True
                if "passwordHash" not in control:
                    control["passwordHash"] = ""
                    control["passwordSalt"] = ""
                    changed = True
                if "passwordSalt" not in control:
                    control["passwordSalt"] = ""
                    changed = True
                if not isinstance(control.get("cookieSecret"), str) or len(control["cookieSecret"]) < 32:
                    control["cookieSecret"] = secrets.token_urlsafe(32)
                    changed = True
                webdav = config.get("webdav")
                if isinstance(webdav, dict):
                    if "usageDataAutoSync" not in webdav:
                        webdav["usageDataAutoSync"] = True
                        changed = True
                    if "password" not in webdav:
                        webdav["password"] = ""
                        changed = True
                    if "encryptionPassphraseHash" not in webdav:
                        webdav["encryptionPassphraseHash"] = ""
                        changed = True
                    if "encryptionPassphrase" not in webdav:
                        webdav["encryptionPassphrase"] = ""
                        changed = True
                    if isinstance(webdav.get("encryptionPassphrase"), str) and webdav["encryptionPassphrase"]:
                        webdav["encryptionPassphraseHash"] = passphrase_hash(webdav["encryptionPassphrase"].strip(), str(webdav.get("baseUrl", "")), str(webdav.get("username", "")))
                        webdav["encryptionPassphrase"] = ""
                        changed = True
                if changed:
                    atomic_write_json(self.config_path, config)
        if not self.state_path.exists():
            atomic_write_json(self.state_path, {
                "version": 1, "lastAutoFetchAt": None, "skills": {"indexEtag": None, "indexId": None, "localSha256": {}},
                "usage": {"published": {}, "remote": {}, "lastSuccessAt": None, "lastAttemptAt": None, "failure": None},
                "remote": {"accounts": {}, "skills": {}}, "pendingAccountOperation": None, "conditionalWritesVerified": False, "decryptFailure": None,
            })
        else:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
            changed = False
            if "lastAutoFetchAt" not in state:
                state["lastAutoFetchAt"] = None
                changed = True
            if not isinstance(state.get("remote"), dict) or not isinstance(state["remote"].get("accounts"), dict) or not isinstance(state["remote"].get("skills"), dict):
                state["remote"] = {"accounts": {}, "skills": {}}
                changed = True
            skills = state.get("skills") if isinstance(state.get("skills"), dict) else {}
            if "indexEtag" not in skills or "indexId" not in skills or not isinstance(skills.get("localSha256"), dict):
                state["skills"] = {"indexEtag": skills.get("pointerEtag"), "indexId": skills.get("snapshotId"), "localSha256": {}}
                changed = True
            remote_skills = state["remote"].get("skills", {})
            if remote_skills and "indexEtag" not in remote_skills:
                state["remote"]["skills"] = {"version": 1, "indexEtag": remote_skills.get("pointerEtag"), "indexId": remote_skills.get("snapshotId"), "legacySnapshotId": remote_skills.get("snapshotId"), "updatedAt": remote_skills.get("updatedAt")}
                changed = True
            if not isinstance(state.get("usage"), dict):
                state["usage"] = {"published": {}, "remote": {}, "lastSuccessAt": None, "lastAttemptAt": None, "failure": None}
                changed = True
            else:
                for key, value in (("published", {}), ("remote", {}), ("lastSuccessAt", None), ("lastAttemptAt", None), ("failure", None)):
                    if key not in state["usage"]:
                        state["usage"][key] = value
                        changed = True
                for key in ("lastFullVerificationAt", "pendingLegacyPurge"):
                    if key in state["usage"]:
                        del state["usage"][key]
                        changed = True
                if not isinstance(state["usage"].get("accountAliases", {}), dict):
                    state["usage"]["accountAliases"] = {}
                    changed = True
                if "sequence" in state["usage"]:
                    del state["usage"]["sequence"]
                    changed = True
            if "decryptFailure" not in state:
                state["decryptFailure"] = None
                changed = True
            if changed:
                atomic_write_json(self.state_path, state)

    def _read_config(self) -> dict:
        try:
            config = json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CloudError(f"Cannot read WebDAV config: {exc}", 500) from exc
        return self._validate_config(config)

    @staticmethod
    def _validate_config(config: dict) -> dict:
        if not isinstance(config, dict):
            raise CloudError("Unsupported WebDAV config", 500)
        webdav, control, server, auto_update = config.get("webdav"), config.get("control"), config.get("server"), config.get("autoUpdate")
        if (config.get("version") != 1 or not isinstance(webdav, dict) or not isinstance(control, dict) or not isinstance(server, dict) or not isinstance(auto_update, dict) or not isinstance(auto_update.get("enabled"), bool) or control.get("password") != "" or webdav.get("encryptionPassphrase") != ""
                or not isinstance(control.get("passwordHash"), str) or len(control["passwordHash"]) > 1024 or not isinstance(control.get("passwordSalt"), str)
                or not isinstance(control.get("cookieSecret"), str) or len(control["cookieSecret"]) < 32):
            raise CloudError("Unsupported WebDAV config", 500)
        validate_server_host(server.get("host"))
        parsed_url = urllib.parse.urlsplit(str(webdav.get("baseUrl", "")))
        secure_url = parsed_url.scheme.lower() == "https" or parsed_url.scheme.lower() == "http" and parsed_url.hostname in {"localhost", "127.0.0.1", "::1"}
        if webdav.get("encryptionPassphraseHash") and not passphrase_hash_matches_webdav(webdav["encryptionPassphraseHash"], str(webdav.get("baseUrl", "")), str(webdav.get("username", ""))):
            raise CloudError("Invalid saved encryption passphrase hash", 500)
        if webdav.get("enabled") and (not secure_url or not webdav.get("username") or not webdav.get("password")):
            raise CloudError("Enabled WebDAV requires a URL, username, and password")
        return config

    def config(self) -> dict:
        if self._config_error:
            raise CloudError(self._config_error, 500)
        return self._config

    def update_server_config(self, host) -> dict:
        host = validate_server_host(host)
        with self._operation_lock:
            config = {**self.config(), "server": {"host": host}}
            atomic_write_json(self.config_path, config)
            self._config = config
        return {"host": host, "restartRequired": True}

    def editable_config(self) -> dict:
        config = self.config()
        webdav = config["webdav"]
        return {
            "server": {"host": config["server"]["host"]},
            "autoUpdate": {"enabled": config["autoUpdate"]["enabled"]},
            "webdav": {key: webdav.get(key) for key in ("enabled", "baseUrl", "username", "remoteRoot", "skillsAutoUpload", "usageDataAutoSync", "allowOptimisticWrites")},
            "secretsConfigured": {"password": bool(webdav.get("password")), "encryptionPassphrase": bool(webdav.get("encryptionPassphraseHash")), "controlPassword": control_password_is_configured(config["control"])},
        }

    @_serialized_cloud_operation
    def reload_config(self) -> dict:
        try:
            config = json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CloudError(f"Cannot read WebDAV config: {exc}", 500) from exc
        webdav = config.get("webdav") if isinstance(config, dict) else None
        if isinstance(webdav, dict) and isinstance(webdav.get("encryptionPassphrase"), str) and webdav["encryptionPassphrase"]:
            webdav["encryptionPassphraseHash"] = passphrase_hash(self._config_text(webdav["encryptionPassphrase"], "Encryption passphrase", 4096), str(webdav.get("baseUrl", "")), str(webdav.get("username", "")))
            webdav["encryptionPassphrase"] = ""
            atomic_write_json(self.config_path, config)
        config = self._validate_config(config)
        self._config, self._config_error = config, None
        self._usage_account_ids.clear()
        return {"config": self.editable_config(), "reloaded": True}

    def initialize_control_password(self, password) -> dict:
        with self._operation_lock:
            current = self.config()
            if control_password_is_configured(current["control"]):
                raise CloudError("Control password is already configured", 409)
            if current["control"].get("passwordHash"):
                raise CloudError("Control password compromised. Remove passwordHash from config.json, restart the monitor, and then create a new control password.", 409)
            salt = new_control_password_salt()
            config = {**current, "control": {**current["control"], "password": "", "passwordHash": hash_control_password(validate_new_control_password(password), salt), "passwordSalt": salt}}
            atomic_write_json(self.config_path, config)
            self._config = config
        return {"controlPasswordConfigured": True}

    @staticmethod
    def _config_text(value, label: str, maximum: int, *, required: bool = True) -> str:
        if not isinstance(value, str):
            raise CloudError(f"{label} must be text")
        value = value.strip()
        if required and not value:
            raise CloudError(f"{label} is required")
        if len(value) > maximum:
            raise CloudError(f"{label} is too long")
        return value

    def _commit_rotated_config(self, config: dict) -> None:
        self._state["remote"] = {"accounts": {}, "skills": {}}
        self._state["skills"]["indexEtag"] = None
        self._state["usage"]["remote"] = {}
        self._save_state()
        atomic_write_json(self.config_path, config)

    @_serialized_cloud_operation
    def update_config(self, values: dict) -> dict:
        if not isinstance(values, dict) or not isinstance(values.get("webdav"), dict) or not isinstance(values.get("server"), dict):
            raise CloudError("Invalid config update")
        current = self.config()
        webdav_values, auto_update_values = values["webdav"], values.get("autoUpdate", current["autoUpdate"])
        if not isinstance(auto_update_values, dict):
            raise CloudError("Invalid automatic update config")
        config = {
            **current,
            "server": {"host": validate_server_host(values["server"].get("host"))},
            "autoUpdate": {"enabled": auto_update_values.get("enabled") is True},
            "control": {**current["control"], "password": ""},
            "webdav": {
                **current["webdav"],
                "encryptionPassphrase": "",
                "enabled": webdav_values.get("enabled") is True,
                "baseUrl": self._config_text(webdav_values.get("baseUrl"), "WebDAV base URL", 2048),
                "username": self._config_text(webdav_values.get("username"), "WebDAV username", 512, required=False),
                "remoteRoot": self._config_text(webdav_values.get("remoteRoot"), "WebDAV remote root", 512),
                "skillsAutoUpload": webdav_values.get("skillsAutoUpload") is True,
                "usageDataAutoSync": webdav_values.get("usageDataAutoSync") is True,
                "allowOptimisticWrites": webdav_values.get("allowOptimisticWrites") is True,
            },
        }
        password = webdav_values.get("password")
        passphrase = webdav_values.get("encryptionPassphrase")
        control_password = values.get("controlPassword")
        if password not in (None, ""):
            config["webdav"]["password"] = self._config_text(password, "New WebDAV password", 4096)
        if control_password not in (None, ""):
            config["control"]["passwordSalt"] = new_control_password_salt()
            config["control"]["passwordHash"] = hash_control_password(validate_new_control_password(control_password), config["control"]["passwordSalt"])
        new_passphrase_hash = current["webdav"].get("encryptionPassphraseHash")
        if passphrase not in (None, ""):
            new_passphrase_hash = passphrase_hash(self._config_text(passphrase, "New encryption passphrase", 4096), config["webdav"]["baseUrl"], config["webdav"]["username"])
            config["webdav"]["encryptionPassphraseHash"] = new_passphrase_hash
        elif new_passphrase_hash and not passphrase_hash_matches_webdav(new_passphrase_hash, config["webdav"]["baseUrl"], config["webdav"]["username"]):
            raise CloudError("Enter the encryption passphrase when changing the WebDAV base URL or username so remote data can be re-encrypted with the new salt", 409)
        self._validate_config(config)
        passphrase_changed = bool(current["webdav"].get("encryptionPassphraseHash")) and new_passphrase_hash != current["webdav"].get("encryptionPassphraseHash")
        if passphrase_changed:
            self._remember_legacy_usage_aliases()
            self._rotate_remote_passphrase(config["webdav"], new_passphrase_hash, lambda: self._commit_rotated_config(config))
        else:
            atomic_write_json(self.config_path, config)
        self._config, self._config_error = config, None
        if passphrase_changed:
            self._usage_account_ids.clear()
        return {"config": self.editable_config(), "restartRequired": config["server"] != current["server"], "controlPasswordChanged": config["control"]["passwordHash"] != current["control"]["passwordHash"], "passphraseChanged": passphrase_changed}

    @property
    def machine_id(self) -> str:
        return self._machine["machineId"]

    def configure_usage_sync(self, usage_data) -> None:
        self._usage_data = usage_data
        usage = self._state.get("usage") or {}
        last_attempt = usage.get("lastAttemptAt") or usage.get("lastSuccessAt")
        elapsed = _elapsed_since(last_attempt, USAGE_SYNC_INTERVAL_SECONDS)
        self._next_usage_sync_at = time.monotonic() + max(0.0, USAGE_SYNC_INTERVAL_SECONDS - elapsed)

    def reset_usage_apply_cursors(self) -> None:
        self._state["usage"]["remote"] = {}
        self._state["usage"]["lastSuccessAt"] = None
        self._state["usage"]["lastAttemptAt"] = None
        self._save_state()

    def _usage_account_identity(self, account_slot_id: str | None) -> tuple[str, str]:
        identity = {}
        if self.accounts is not None:
            with self.accounts.lock:
                account = next((item for item in self.accounts.manifest.get("accounts", []) if item.get("id") == account_slot_id), None)
                identity = (account or {}).get("identity") or {}
                if (account or {}).get("accountType") == "api" or identity.get("accountType") == "api":
                    api_id = api_identity_id({**identity, "accountType": "api"})
                    if api_id:
                        return "api", api_id
        if identity.get("accountId"):
            return "account", str(identity["accountId"])
        if identity.get("idTokenHash"):
            return "id-token", str(identity["idTokenHash"])
        return "machine", f"{self.machine_id}\0{account_slot_id or 'unknown'}"

    @staticmethod
    def _stable_usage_account_id(kind: str, value: str) -> str:
        digest = hashlib.sha256(f"{USAGE_ACCOUNT_ID_DOMAIN}\0{kind}\0{value}".encode()).hexdigest()
        return f"v3:{digest}"

    def _legacy_usage_account_id(self, account_slot_id: str | None) -> str | None:
        passphrase_hash_value = (self.config().get("webdav") or {}).get("encryptionPassphraseHash")
        if not passphrase_hash_value:
            return None
        identity = {}
        if self.accounts is not None:
            account = next((item for item in self.accounts.manifest.get("accounts", []) if item.get("id") == account_slot_id), None)
            identity = (account or {}).get("identity") or {}
        if identity.get("accountId"):
            legacy_identity = f"account:{identity['accountId']}"
        elif identity.get("idTokenHash"):
            legacy_identity = f"id-token:{identity['idTokenHash']}"
        else:
            legacy_identity = f"machine:{self.machine_id}:slot:{account_slot_id or 'unknown'}"
        return hmac.new(hashlib.sha256(_passphrase_key(passphrase_hash_value)).digest(), f"usage:{legacy_identity}".encode(), hashlib.sha256).hexdigest()

    def usage_account_aliases(self, account_slot_id: str | None) -> tuple[str, ...]:
        kind, value = self._usage_account_identity(account_slot_id)
        stable = self._stable_usage_account_id(kind, value)
        legacy = self._legacy_usage_account_id(account_slot_id)
        remembered = [alias for alias, target in (self._state["usage"].get("accountAliases") or {}).items() if target == stable]
        return tuple(dict.fromkeys((stable, legacy, *remembered))) if legacy else tuple(dict.fromkeys((stable, *remembered)))

    def _remember_legacy_usage_aliases(self) -> None:
        if self.accounts is None:
            return
        aliases = self._state["usage"].setdefault("accountAliases", {})
        changed = False
        with self.accounts.lock:
            for account in self.accounts.manifest.get("accounts", []):
                kind, value = self._usage_account_identity(account.get("id"))
                legacy = self._legacy_usage_account_id(account.get("id"))
                if legacy and aliases.get(legacy) != self._stable_usage_account_id(kind, value):
                    aliases[legacy] = self._stable_usage_account_id(kind, value)
                    changed = True
        if changed:
            self._save_state()

    def usage_account_id(self, account_slot_id: str | None) -> str:
        kind, value = self._usage_account_identity(account_slot_id)
        cache_key = account_slot_id, kind, value
        if cache_key in self._usage_account_ids:
            return self._usage_account_ids[cache_key]
        self._usage_account_ids[cache_key] = self._stable_usage_account_id(kind, value)
        return self._usage_account_ids[cache_key]

    def local_usage_account(self, usage_account_id: str, fallback_slot_id: str | None, fallback_label: str | None) -> tuple[str, str] | None:
        if self.accounts is not None:
            with self.accounts.lock:
                matches = [account for account in self.accounts.manifest.get("accounts", []) if usage_account_id in self.usage_account_aliases(account.get("id"))]
                if matches:
                    match_ids = {account["id"] for account in matches}
                    primary_id = next((row.get("accountSlotId") for row in reversed(self.accounts.manifest.get("activationHistory") or []) if row.get("accountSlotId") in match_ids), self.accounts.manifest.get("activeAccountId") if self.accounts.manifest.get("activeAccountId") in match_ids else matches[0]["id"])
                    primary = next(account for account in matches if account["id"] == primary_id)
                    return primary["id"], primary.get("label") or fallback_label or "Unknown"
        return None

    def usage_account_revision(self) -> tuple:
        if self.accounts is None:
            return self.machine_id, ()
        with self.accounts.lock:
            return self.machine_id, tuple(
                (account.get("id"), account.get("label"), account.get("accountType"), (account.get("identity") or {}).get("accountId"), (account.get("identity") or {}).get("idTokenHash"))
                for account in self.accounts.manifest.get("accounts", [])
            )

    def _save_state(self) -> None:
        atomic_write_json(self.state_path, self._state)

    def _record_decrypt_failure(self, exc: Exception, operation: str) -> None:
        if getattr(exc, "decrypt_failed", False):
            current = self._state.get("decryptFailure") or {}
            if current.get("message") != str(exc) or current.get("operation") != operation:
                self._state["decryptFailure"] = {"id": uuid.uuid4().hex, "message": str(exc), "operation": operation, "failedAt": _timestamp()}
                self._save_state()

    def redacted_status(self) -> dict:
        config, error = self._config, self._config_error
        webdav = config.get("webdav", {})
        return {
            "configPath": str(self.config_path), "webdav": {key: webdav.get(key) for key in ("enabled", "baseUrl", "username", "remoteRoot", "skillsAutoUpload", "usageDataAutoSync", "allowOptimisticWrites")},
            "secretsConfigured": {"password": bool(webdav.get("password")), "encryptionPassphrase": bool(webdav.get("encryptionPassphraseHash"))},
            "conditionalWritesVerified": bool(self._state.get("conditionalWritesVerified")),
            "optimisticWritesActive": not bool(self._state.get("conditionalWritesVerified")) and bool(webdav.get("allowOptimisticWrites", True)), "skills": self._state.get("skills", {}), "error": error,
            "autoSync": {
                "pending": bool(self._pending_skill_pushes), "pendingSkills": sorted(self._pending_skill_pushes), "attempts": max((item["attempts"] for item in self._pending_skill_pushes.values()), default=0),
                "failure": next(reversed(self._auto_push_failures.values()), None),
            },
            "decryptFailure": self._state.get("decryptFailure"),
            "usageSync": {
                **{key: self._state.get("usage", {}).get(key) for key in ("lastSuccessAt", "lastAttemptAt", "failure")},
                "nextAttemptInSeconds": max(0, round(self._next_usage_sync_at - time.monotonic())) if self._usage_data is not None else None,
            },
        }

    def cached_remote_accounts(self) -> list[dict]:
        return [item["state"] for item in self._state.get("remote", {}).get("accounts", {}).values() if isinstance(item, dict) and isinstance(item.get("state"), dict)]

    def _cache_remote_account(self, account: dict, etag: str) -> None:
        self._state["remote"]["accounts"][account["accountKey"]] = {"etag": etag, "state": {**account, "etag": etag}}
        self._save_state()

    @staticmethod
    def _validated_api_identity(state: dict, data: bytes) -> tuple[dict, str]:
        identity = auth_identity(parse_auth_bytes(data))
        identity_id = api_identity_id(identity)
        if state.get("accountType", identity.get("accountType")) != "api" or identity_id is None:
            raise CloudError("Remote API account metadata does not match its credential payload", 409)
        if state.get("apiIdentityId") and state["apiIdentityId"] != identity_id:
            raise CloudError("Remote API account identity check failed", 409)
        return identity, identity_id

    def cache_remote_account(self, state: dict, etag: str, data: bytes | None = None) -> None:
        cached = dict(state)
        if cached.get("accountType") == "api":
            if data is None:
                _, data, etag = self.bind_account(cached["accountKey"])
            _, cached["_apiIdentityId"] = self._validated_api_identity(cached, data)
        self._cache_remote_account(cached, etag)

    def find_remote_api_accounts(self, identity_id: str) -> list[dict]:
        matches = []
        listed_accounts = self.list_accounts()
        remote_keys = {listed.get("accountKey") for listed in listed_accounts}
        for listed in listed_accounts:
            if listed.get("accountType") != "api":
                continue
            state, data, etag = self.bind_account(listed["accountKey"])
            _, resolved_identity_id = self._validated_api_identity(state, data)
            self._cache_remote_account({**state, "_apiIdentityId": resolved_identity_id}, etag)
            if resolved_identity_id == identity_id:
                matches.append(state)
        for key in set(self._state.get("remote", {}).get("accounts", {})) - remote_keys:
            self._state["remote"]["accounts"].pop(key, None)
        self._save_state()
        return matches

    def _account_state_with(self, client: WebDavClient, box: CryptoBox, key: str) -> tuple[dict, str]:
        data, etag = client.get(self._account_paths(key)[0])
        try:
            state = json.loads(box.decrypt(f"account-state:{key}", data, 1024 * 1024))
        except json.JSONDecodeError as exc:
            raise CloudError("Invalid account state", 409) from exc
        if state.get("version") != 1 or state.get("accountKey") != key:
            raise CloudError("Account state identity check failed", 409)
        return state, etag

    def _parse_skill_pointer(self, box: CryptoBox, data: bytes, etag: str) -> dict:
        try:
            pointer = json.loads(box.decrypt("skills-pointer", data))
        except json.JSONDecodeError as exc:
            raise CloudError("Invalid remote skill pointer", 409) from exc
        if pointer.get("version") == 1 and pointer.get("snapshotId"):
            return {"version": 1, "indexEtag": etag, "indexId": pointer["snapshotId"], "legacySnapshotId": pointer["snapshotId"], "updatedAt": pointer.get("updatedAt")}
        packages = pointer.get("packages")
        if pointer.get("version") != 2 or not isinstance(packages, dict) or any(
            not isinstance(name, str) or not name or not isinstance(item, dict) or len(str(item.get("packageId", ""))) != 64 or any(character not in "0123456789abcdef" for character in str(item.get("packageId", "")))
            or item.get("contentSha256") != item.get("packageId") for name, item in packages.items()
        ):
            raise CloudError("Invalid remote skill pointer", 409)
        index_id = hashlib.sha256(json.dumps(packages, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return {"version": 2, "indexEtag": etag, "indexId": index_id, "packages": packages, "updatedAt": pointer.get("updatedAt")}

    def _download_skill_snapshot(self, client: WebDavClient, box: CryptoBox, index: dict, names: set[str] | None = None) -> bytes:
        if index.get("version") == 1:
            snapshot_id = index["legacySnapshotId"]
            encrypted, _ = client.get(f"skills/snapshots/{snapshot_id}.enc")
            return box.decrypt(f"skills-snapshot:{snapshot_id}", encrypted)
        snapshots = []
        for name, item in sorted(index.get("packages", {}).items()):
            if names is not None and name not in names:
                continue
            snapshots.append(self._download_skill_package(client, box, name, item))
        return self.skills.combine_snapshots(snapshots)

    def _download_skill_package(self, client: WebDavClient, box: CryptoBox, name: str, item: dict) -> bytes:
        package_id = item["packageId"]
        encrypted, _ = client.get(f"skills/packages/{package_id}.enc")
        data = box.decrypt(f"skill-package:{package_id}", encrypted)
        if set(self.skills.split_snapshot(data)) != {name} or self.skills.skill_package_hash(data) != item["contentSha256"]:
            raise CloudError("Skill package identity check failed", 409)
        return data

    def _skill_packages_needing_fetch(self, index: dict, pointer_changed: bool) -> set[str] | None:
        if index.get("version") == 2:
            expected = {name: item["contentSha256"] for name, item in index.get("packages", {}).items()}
            current = self.skills.content_hashes(set(expected))
            return {name for name, content_hash in expected.items() if current.get(name) != content_hash}
        applied = self._state.get("skills", {})
        if not pointer_changed and applied.get("indexId") == index.get("indexId") and isinstance(applied.get("localSha256"), dict) and self.skills.content_hashes() == applied["localSha256"]:
            return set()
        return None

    @_serialized_cloud_operation
    def fetch(self, include_usage: bool = True, force_full: bool = False) -> dict:
        client, box = self._connection()
        client.ensure_directories("accounts/states")
        local = self._state
        cached = local.get("remote", {}) if isinstance(local.get("remote"), dict) else {}
        cached_accounts = cached.get("accounts", {}) if isinstance(cached.get("accounts"), dict) else {}
        accounts, changed = {}, []
        for item in client.list_details("accounts/states"):
            if not item["name"].endswith(".enc"):
                continue
            key = item["name"][:-4]
            previous = cached_accounts.get(key)
            if not force_full and item.get("etag") and isinstance(previous, dict) and previous.get("etag") == item["etag"] and isinstance(previous.get("state"), dict):
                if previous["state"].get("accountType") == "api" and not previous["state"].get("_apiIdentityId"):
                    data = self._download_account_revision_with(client, box, previous["state"])
                    _, previous["state"]["_apiIdentityId"] = self._validated_api_identity(previous["state"], data)
                accounts[key] = previous
                continue
            try:
                account, etag = self._account_state_with(client, box, key)
            except CloudError:
                continue
            if account.get("accountType") == "api":
                if account.get("apiIdentityId"):
                    account["_apiIdentityId"] = account["apiIdentityId"]
                else:
                    data = self._download_account_revision_with(client, box, account)
                    _, account["_apiIdentityId"] = self._validated_api_identity(account, data)
            account["etag"] = etag
            accounts[key] = {"etag": etag, "state": account}
            changed.append(key)
        removed = sorted(set(cached_accounts) - set(accounts))
        cached_skills = cached.get("skills", {}) if isinstance(cached.get("skills"), dict) else {}
        try:
            pointer_data, pointer_etag = client.get_if_changed("skills/current.enc", cached_skills.get("indexEtag"))
        except CloudError as exc:
            if "HTTP 404" not in str(exc):
                raise
            pointer_data, pointer_etag, cached_skills = None, None, {}
        skills_changed = pointer_data is not None or bool(cached.get("skills")) and not pointer_etag
        if pointer_data is not None:
            cached_skills = self._parse_skill_pointer(box, pointer_data, pointer_etag)
        if cached_skills.get("indexId"):
            package_names = set(cached_skills.get("packages", {})) if force_full and cached_skills.get("version") == 2 else self._skill_packages_needing_fetch(cached_skills, skills_changed)
            if force_full and cached_skills.get("version") == 1:
                package_names = None
            if package_names is None or package_names:
                snapshot = self._download_skill_snapshot(client, box, cached_skills, package_names)
                skill_merge = self.skills.merge(snapshot)
            else:
                skill_merge = {"added": [], "updated": [], "deleted": [], "projectionErrors": []}
            local["skills"] = {"indexEtag": cached_skills.get("indexEtag"), "indexId": cached_skills["indexId"], "localSha256": self.skills.content_hashes(), "fetchedAt": _timestamp()}
        else:
            skill_merge = {"added": [], "updated": [], "deleted": [], "projectionErrors": []}
        skills_changed = skills_changed or bool(skill_merge["added"] or skill_merge["updated"] or skill_merge.get("deleted"))
        usage = (self._fetch_usage_data(client, box, True) if force_full else self._fetch_usage_data(client, box)) if include_usage else {"skipped": True}
        cached = {"accounts": accounts, "skills": cached_skills, "fetchedAt": _timestamp()}
        local["remote"] = cached
        local["decryptFailure"] = None
        self._save_state()
        if cached_skills.get("indexId"):
            self._finish_fetch_baseline(time.monotonic())
        return {
            "accountsChanged": len(changed), "accountsRemoved": len(removed), "skillsChanged": skills_changed, "skillsAdded": skill_merge["added"],
            "skillsUpdated": skill_merge["updated"], "skillsDeleted": skill_merge.get("deleted", []), "accountsUpdated": 0, "usage": usage,
            "projectionErrors": skill_merge["projectionErrors"], "fetchedAt": cached["fetchedAt"]
        }

    def begin_account_transition(self, operation: str, **details) -> None:
        self._state["pendingAccountOperation"] = {"operation": operation, "startedAt": _timestamp(), **details}
        self._save_state()

    def clear_account_transition(self) -> None:
        self._state["pendingAccountOperation"] = None
        self._save_state()

    def ensure_no_pending_account_operation(self) -> None:
        if self._state.get("pendingAccountOperation"):
            raise CloudError("Finish the pending account transfer recovery before changing accounts", 409)

    def account_transition_targets(self, account_id: str) -> bool:
        return (self._state.get("pendingAccountOperation") or {}).get("accountId") == account_id

    def recover_account_transition(self) -> str | None:
        pending = self._state.get("pendingAccountOperation")
        if not isinstance(pending, dict):
            return None
        if not self.operation_queue.in_worker():
            return self.operation_queue.wait(self.submit_operation("recover_account_transition", source="recovery"))
        if pending.get("operation") == "bind":
            with self.accounts.lock:
                existing = next((account for account in self.accounts.manifest["accounts"] if account.get("id") == pending.get("accountId") and (account.get("cloud") or {}).get("accountKey") == pending.get("accountKey")), None)
                conflicting = next((account for account in self.accounts.manifest["accounts"] if (account.get("cloud") or {}).get("accountKey") == pending.get("accountKey") and account.get("id") != pending.get("accountId")), None)
            if conflicting is not None:
                raise CloudError("Pending cloud bind key belongs to a different local credential profile", 409)
            if existing is not None:
                credential_path = self.accounts._account_path(existing["id"])
                data = credential_path.read_bytes() if existing.get("ready") and credential_path.exists() else b"{}"
                if pending.get("revisionId") and hashlib.sha256(data).hexdigest() != pending["revisionId"]:
                    raise CloudError("Pending cloud bind does not match the committed local credential revision", 409)
                if existing.get("accountType") == "api":
                    identity = auth_identity(parse_auth_bytes(data))
                    try:
                        state, remote_data, etag = self.bind_account(pending.get("accountKey"))
                    except CloudError as exc:
                        if "HTTP 404" not in str(exc):
                            raise
                        self.release_account(pending.get("accountKey"), data, identity, existing.get("label") or "API account", key_type=(existing.get("cloud") or {}).get("keyType") or "api-identity", config_header=existing.get("configHeader"), account_type="api", config_header_toml=existing.get("configHeaderToml", ""))
                    else:
                        _, remote_identity_id = self._validated_api_identity(state, remote_data)
                        if remote_identity_id != api_identity_id(identity):
                            raise CloudError("Pending API link does not match the retained cloud credential", 409)
                        self._cache_remote_account({**state, "_apiIdentityId": remote_identity_id}, etag)
                else:
                    self.delete_account_payloads(pending.get("accountKey"), pending.get("etag"))
            else:
                self.accounts.bind_cloud_account(self, pending.get("accountKey"), record_transition=False, intended_account_id=pending.get("accountId"))
        elif pending.get("operation") == "release":
            try:
                state, data, _ = self.bind_account(pending.get("accountKey"))
            except CloudError as exc:
                if "HTTP 404" not in str(exc):
                    raise
                self.accounts.rollback_recovered_release(pending.get("accountId"))
            else:
                if pending.get("revisionId") is None or state.get("revisionId") == pending.get("revisionId") and hashlib.sha256(data).hexdigest() == pending.get("revisionId"):
                    self.accounts.finalize_recovered_release(pending.get("accountId"))
                else:
                    self.accounts.rollback_recovered_release(pending.get("accountId"))
        else:
            raise CloudError("Unsupported pending cloud account operation", 409)
        self.clear_account_transition()
        return pending["operation"]

    def _connection(self, initialize: bool = False) -> tuple[WebDavClient, CryptoBox]:
        config = self.config()
        if not config["webdav"].get("enabled"):
            raise CloudError("WebDAV is disabled", 409)
        webdav = config["webdav"]
        client = WebDavClient(webdav)
        client.ensure_directories("")
        try:
            descriptor, _ = client.get("crypto.json")
            parsed = json.loads(descriptor)
        except CloudError as exc:
            if not initialize or exc.category != "http" or exc.http_status != 404:
                raise
            parsed = CryptoBox.descriptor(webdav["encryptionPassphraseHash"])
            descriptor = json.dumps(parsed, separators=(",", ":")).encode()
            client.put("crypto.json", descriptor, create=True)
            if client.get("crypto.json")[0] != descriptor:
                raise CloudError("WebDAV crypto descriptor conditional creation verification failed", 409)
            try:
                client.put("crypto.json", descriptor, create=True)
            except CloudError as verification_error:
                if verification_error.http_status != 412:
                    raise
            else:
                raise CloudError("WebDAV server ignored conditional crypto descriptor creation", 409)
        return client, CryptoBox(webdav["encryptionPassphraseHash"], parsed)

    @_serialized_cloud_operation
    def overwrite_cloud_from_local(self) -> dict:
        if self._state.get("pendingAccountOperation"):
            raise CloudError("Finish the pending account operation before overwriting cloud data", 409)
        webdav = self.config()["webdav"]
        if not webdav.get("enabled"):
            raise CloudError("WebDAV is disabled", 409)
        client = WebDavClient(webdav)
        client.delete("")
        client.ensure_directories("")
        if self._usage_data is not None:
            self._usage_data.remove_origins_not_in(set())
        self._state["remote"] = {"accounts": {}, "skills": {}}
        self._state["skills"]["indexEtag"] = None
        self._state["usage"]["remote"] = {}
        self._state["conditionalWritesVerified"] = False
        self._state["decryptFailure"] = None
        self._save_state()
        result = self.upload_skills()
        return {"overwritten": True, "skills": result, "encryptionEnabled": bool(webdav.get("encryptionPassphraseHash"))}

    @_serialized_cloud_operation
    def test(self) -> dict:
        checks = []

        def fail(name: str, exc: Exception):
            checks.append({"name": name, "status": "failed", "detail": str(exc)})
            if isinstance(exc, CloudError):
                exc.details = checks
                raise exc
            raise CloudError(str(exc), 500, details=checks) from exc

        try:
            client, box = self._connection(True)
            checks.append({"name": "Connection and account verification", "status": "passed", "detail": "The WebDAV server accepted the configured URL and credentials."})
        except Exception as exc:
            fail("Connection and account verification", exc)
        try:
            inventory = self._encrypted_inventory(client)
            if inventory:
                path, purpose = inventory[0]
                box.decrypt(purpose, client.get(path)[0])
                checks.append({"name": "Encrypted data decryption", "status": "passed", "detail": f"Successfully authenticated and decrypted {path}."})
            else:
                checks.append({"name": "Encrypted data decryption", "status": "skipped", "detail": "No encrypted cloud payload exists yet."})
        except Exception as exc:
            fail("Encrypted data decryption", exc)
        try:
            client.ensure_directories("protocol-test")
            path, first = f"protocol-test/{uuid.uuid4().hex}.bin", secrets.token_bytes(32)
            etag = client.put(path, first, create=True)
            checks.append({"name": "Test object creation", "status": "passed", "detail": "Created a temporary protocol-test object."})
        except Exception as exc:
            fail("Test object creation", exc)
        conditional_writes = True
        try:
            client.put(path, b"duplicate", create=True)
            conditional_writes = False
        except CloudError as exc:
            if exc.http_status != 412:
                fail("Conditional create protection", exc)
        checks.append({"name": "Conditional create protection", "status": "passed" if conditional_writes else "warning", "detail": "The server rejected a duplicate create." if conditional_writes else "The server accepted a duplicate create and cannot reliably prevent create conflicts."})
        conditional_update = True
        try:
            client.put(path, b"wrong-etag", etag='"codex-switch-intentionally-wrong"')
            conditional_update = False
        except CloudError as exc:
            if exc.http_status != 412:
                fail("Conditional update protection", exc)
        conditional_writes = conditional_writes and conditional_update
        checks.append({"name": "Conditional update protection", "status": "passed" if conditional_update else "warning", "detail": "The server rejected an update with the wrong ETag." if conditional_update else "The server accepted an update with the wrong ETag and cannot reliably prevent overwrite conflicts."})
        try:
            etag = client.put(path, b"updated", etag=client.get(path)[1])
            if client.get(path)[0] != b"updated":
                raise CloudError("WebDAV read-back verification failed", 409)
            checks.append({"name": "Update and read-back verification", "status": "passed", "detail": "Updated the test object with its current ETag and verified the stored bytes."})
        except Exception as exc:
            fail("Update and read-back verification", exc)
        self._state["conditionalWritesVerified"] = conditional_writes
        self._state["decryptFailure"] = None
        self._save_state()
        return {"ok": True, "strongEtag": etag, "conditionalWrites": conditional_writes, "encryptedPayloadVerified": bool(inventory), "optimisticWrites": not conditional_writes and bool(self.config()["webdav"].get("allowOptimisticWrites", True)), "warning": None if conditional_writes else "The server ignores conditional writes; conflict prevention is best-effort.", "checks": checks}

    @staticmethod
    def _reencrypt_object(client: WebDavClient, box: CryptoBox, path: str, purpose: str) -> None:
        encrypted, etag = client.get(path)
        plaintext = box.decrypt(purpose, encrypted)
        client.put(path, box.encrypt(purpose, plaintext), etag=etag)
        verified, _ = client.get(path)
        if box.decrypt(purpose, verified) != plaintext:
            raise CloudError(f"Cloud re-encryption verification failed for {path}", 409)

    @staticmethod
    def _optional_remote_list(client: WebDavClient, path: str) -> list[str]:
        try:
            return client.list(path)
        except CloudError as exc:
            if exc.http_status == 404 or "HTTP 404" in str(exc):
                return []
            raise

    def _encrypted_inventory(self, client: WebDavClient) -> list[tuple[str, str]]:
        inventory = []
        for package_id in sorted(name[:-4] for name in self._optional_remote_list(client, "skills/packages") if name.endswith(".enc") and "/" not in name and "\\" not in name):
            inventory.append((f"skills/packages/{package_id}.enc", f"skill-package:{package_id}"))
        for snapshot_id in sorted(name[:-4] for name in self._optional_remote_list(client, "skills/snapshots") if name.endswith(".enc") and "/" not in name and "\\" not in name):
            inventory.append((f"skills/snapshots/{snapshot_id}.enc", f"skills-snapshot:{snapshot_id}"))
        if "current.enc" in self._optional_remote_list(client, "skills"):
            inventory.append(("skills/current.enc", "skills-pointer"))
        for key in sorted(name[:-4] for name in self._optional_remote_list(client, "accounts/states") if name.endswith(".enc") and "/" not in name and "\\" not in name):
            inventory.append((f"accounts/states/{key}.enc", f"account-state:{key}"))
        for key in sorted(name.strip("/") for name in self._optional_remote_list(client, "accounts/revisions") if name.strip("/") and "/" not in name.strip("/") and "\\" not in name):
            for revision in sorted(name[:-4] for name in self._optional_remote_list(client, f"accounts/revisions/{key}") if name.endswith(".enc") and "/" not in name and "\\" not in name):
                inventory.append((f"accounts/revisions/{key}/{revision}.enc", f"account-revision:{key}:{revision}"))
        for name in sorted(self._optional_remote_list(client, "usage/machines")):
            if not name.endswith(".enc") or "/" in name or "\\" in name:
                continue
            machine_id = name[:-4]
            inventory.append((self._usage_pointer_path(machine_id), f"usage-pointer:{machine_id}"))
            for kind in ("packs", "chunks", "checkpoints"):
                for payload_id in sorted(item[:-4] for item in self._optional_remote_list(client, f"usage/{kind}/{machine_id}") if item.endswith(".enc") and "/" not in item and "\\" not in item):
                    inventory.append((self._usage_payload_path(kind, machine_id, payload_id), f"usage-{kind}:{machine_id}:{payload_id}"))
        return inventory

    def _rotate_remote_passphrase(self, webdav: dict, new_passphrase_hash: str, commit) -> dict:
        if self._state.get("pendingAccountOperation"):
            raise CloudError("Finish the pending account operation before updating the encryption passphrase", 409)
        client = WebDavClient(webdav)
        client.ensure_directories("")
        descriptor_bytes, descriptor_etag = client.get("crypto.json")
        try:
            descriptor = json.loads(descriptor_bytes)
        except json.JSONDecodeError as exc:
            raise CloudError("Invalid remote crypto descriptor", 409) from exc
        old_box = CryptoBox(self.config()["webdav"]["encryptionPassphraseHash"], descriptor)
        new_descriptor = json.dumps(CryptoBox.descriptor(new_passphrase_hash), separators=(",", ":")).encode()
        new_box = CryptoBox(new_passphrase_hash, json.loads(new_descriptor))
        staged = []
        for path, purpose in self._encrypted_inventory(client):
            encrypted, etag = client.get(path)
            staged.append({"path": path, "purpose": purpose, "encrypted": encrypted, "etag": etag, "plaintext": old_box.decrypt(purpose, encrypted)})
        updated = []
        descriptor_updated = False
        try:
            for item in staged:
                item["newEtag"] = client.put(item["path"], new_box.encrypt(item["purpose"], item["plaintext"]), etag=item["etag"])
                updated.append(item)
                verified, item["newEtag"] = client.get(item["path"])
                if new_box.decrypt(item["purpose"], verified) != item["plaintext"]:
                    raise CloudError(f"Passphrase update verification failed for {item['path']}", 409)
            descriptor_etag = client.put("crypto.json", new_descriptor, etag=descriptor_etag)
            descriptor_updated = True
            verified_descriptor, descriptor_etag = client.get("crypto.json")
            if verified_descriptor != new_descriptor:
                raise CloudError("Passphrase update verification failed for crypto.json", 409)
            CryptoBox(new_passphrase_hash, json.loads(verified_descriptor))
            commit()
        except Exception as exc:
            rollback_errors = []
            if descriptor_updated:
                try:
                    client.put("crypto.json", descriptor_bytes, etag=client.get("crypto.json")[1])
                    if client.get("crypto.json")[0] != descriptor_bytes:
                        raise CloudError("crypto.json read-back mismatch")
                except Exception as rollback_exc:
                    rollback_errors.append(f"crypto.json: {rollback_exc}")
            for item in reversed(updated):
                try:
                    client.put(item["path"], item["encrypted"], etag=client.get(item["path"])[1])
                    if client.get(item["path"])[0] != item["encrypted"]:
                        raise CloudError("read-back mismatch")
                except Exception as rollback_exc:
                    rollback_errors.append(f"{item['path']}: {rollback_exc}")
            detail = f"Passphrase update failed: {exc}"
            if rollback_errors:
                detail += f". Remote rollback also failed for {'; '.join(rollback_errors)}"
            raise CloudError(detail, 409) from exc
        return {"reencrypted": len(staged), "paths": [item["path"] for item in staged], "verifiedAt": _timestamp()}

    @_serialized_cloud_operation
    def reencrypt_remote_data(self) -> dict:
        if self._state.get("pendingAccountOperation"):
            raise CloudError("Finish the pending account operation before re-encrypting cloud data", 409)
        client, box = self._connection()
        refreshed = []
        for package_id in sorted(self._encrypted_payloads(client, "skills/packages")):
            path = f"skills/packages/{package_id}.enc"
            self._reencrypt_object(client, box, path, f"skill-package:{package_id}")
            refreshed.append(path)
        for snapshot_id in sorted(self._encrypted_payloads(client, "skills/snapshots")):
            path = f"skills/snapshots/{snapshot_id}.enc"
            self._reencrypt_object(client, box, path, f"skills-snapshot:{snapshot_id}")
            refreshed.append(path)
        try:
            self._reencrypt_object(client, box, "skills/current.enc", "skills-pointer")
            refreshed.append("skills/current.enc")
        except CloudError as exc:
            if "HTTP 404" not in str(exc):
                raise
        for key in sorted(self._encrypted_payloads(client, "accounts/states")):
            path = f"accounts/states/{key}.enc"
            self._reencrypt_object(client, box, path, f"account-state:{key}")
            refreshed.append(path)
        try:
            account_keys = client.list("accounts/revisions")
        except CloudError as exc:
            if "HTTP 404" not in str(exc):
                raise
            account_keys = []
        for key in sorted(name.strip("/") for name in account_keys if name.strip("/") and "/" not in name.strip("/") and "\\" not in name):
            for revision in sorted(self._encrypted_payloads(client, f"accounts/revisions/{key}")):
                path = f"accounts/revisions/{key}/{revision}.enc"
                self._reencrypt_object(client, box, path, f"account-revision:{key}:{revision}")
                refreshed.append(path)
        try:
            usage_machines = [name[:-4] for name in client.list("usage/machines") if name.endswith(".enc")]
        except CloudError as exc:
            if "HTTP 404" not in str(exc):
                raise
            usage_machines = []
        for machine_id in sorted(usage_machines):
            path = self._usage_pointer_path(machine_id)
            self._reencrypt_object(client, box, path, f"usage-pointer:{machine_id}")
            refreshed.append(path)
            for kind in ("packs", "chunks", "checkpoints"):
                try:
                    payloads = self._encrypted_payloads(client, f"usage/{kind}/{machine_id}")
                except CloudError as exc:
                    if "HTTP 404" not in str(exc):
                        raise
                    payloads = set()
                for payload_id in sorted(payloads):
                    path = self._usage_payload_path(kind, machine_id, payload_id)
                    self._reencrypt_object(client, box, path, f"usage-{kind}:{machine_id}:{payload_id}")
                    refreshed.append(path)
        self._state["remote"] = {"accounts": {}, "skills": {}}
        self._state["skills"]["indexEtag"] = None
        self._save_state()
        verification = self.fetch()
        return {"reencrypted": len(refreshed), "paths": refreshed, "verifiedAt": _timestamp(), "fetch": verification}

    @_serialized_cloud_operation
    def push(self, force_full: bool = False) -> dict:
        accounts = {"pushedAccounts": []}
        result = {"skills": self.upload_skills(force=True) if force_full else self.upload_skills(), "accounts": accounts, "usage": self._push_usage_data(True) if force_full else self._push_usage_data()}
        result["changed"] = bool(result["skills"].get("changed") or result["usage"].get("uploaded") or result["usage"].get("deleted"))
        self._finish_successful_push(time.monotonic())
        self._state["decryptFailure"] = None
        self._save_state()
        return result

    @_serialized_cloud_operation
    def upload_skills(self, names: set[str] | None = None, force: bool = False) -> dict:
        client, box = self._connection(True)
        client.ensure_directories("skills/packages")
        existing_packages = self._encrypted_payloads(client, "skills/packages")
        existing_snapshots = self._encrypted_payloads(client, "skills/snapshots")
        local_packages = self.skills.skill_snapshots(names)
        local_hashes = {}
        added, updated, deleted = [], [], []
        try:
            pointer_data, current_pointer_etag = client.get("skills/current.enc")
        except CloudError as exc:
            if "HTTP 404" not in str(exc):
                raise
            current_pointer_etag, remote_index = None, {"version": 2, "packages": {}}
        else:
            remote_index = self._parse_skill_pointer(box, pointer_data, current_pointer_etag)
        packages = dict(remote_index.get("packages", {}))
        migrated = remote_index.get("version") == 1
        migrated_packages = {}
        if migrated:
            legacy = self.skills.split_snapshot(self._download_skill_snapshot(client, box, remote_index))
            for name, data in legacy.items():
                content_hash = self.skills.skill_package_hash(data)
                packages[name] = self._upload_skill_package(client, box, data, content_hash)
                migrated_packages[name] = data
        package_changed = False
        for name, data in local_packages.items():
            previous = packages.get(name)
            original_hash = self.skills.skill_package_hash(data)
            if previous and previous.get("contentSha256") != original_hash:
                data = self.skills.snapshot_merged_with_remote(migrated_packages.get(name) or self._download_skill_package(client, box, name, previous), data)[0]
                if self.skills.skill_package_hash(data) != original_hash:
                    self.skills.merge(data)
            local_hashes[name] = self.skills.skill_package_hash(data)
            unchanged = previous and previous.get("contentSha256") == local_hashes[name]
            if unchanged and not force:
                continue
            if not unchanged:
                manifest = self.skills.inspect_snapshot(data)[0]
                if previous is None and manifest["skills"]:
                    added.append(name)
                elif manifest["skills"]:
                    updated.append(name)
                else:
                    deleted.append(name)
                package_changed = True
            packages[name] = self._upload_skill_package(client, box, data, local_hashes[name])
        changed = package_changed
        if not changed and not migrated:
            baseline = self._state.get("skills", {}).get("localSha256", {})
            baseline = dict(baseline) if isinstance(baseline, dict) else {}
            baseline.update(local_hashes)
            index_id = remote_index.get("indexId") or hashlib.sha256(json.dumps(packages, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            self._state["skills"] = {"indexEtag": current_pointer_etag, "indexId": index_id, "localSha256": baseline, "updatedAt": remote_index.get("updatedAt")}
            self._state["remote"]["skills"] = remote_index
            self._observed_skill_revision = self.skills.content_revision
            self._save_state()
            return {**self._state["skills"], "snapshotId": index_id, "changed": False, "added": [], "updated": [], "deleted": []}
        updated_at = _timestamp()
        index_id = hashlib.sha256(json.dumps(packages, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        pointer = box.encrypt("skills-pointer", json.dumps({"version": 2, "packages": packages, "updatedAt": updated_at}, separators=(",", ":")).encode())
        pointer_etag = client.put("skills/current.enc", pointer, etag=current_pointer_etag) if current_pointer_etag else client.put("skills/current.enc", pointer, create=True)
        self._cleanup_skill_packages(client, box, existing_packages)
        if migrated:
            self._cleanup_legacy_skill_snapshots(client, existing_snapshots)
        state = self._state
        baseline = state.get("skills", {}).get("localSha256", {})
        baseline = dict(baseline) if isinstance(baseline, dict) else {}
        baseline.update(local_hashes)
        state["skills"] = {"indexEtag": pointer_etag, "indexId": index_id, "localSha256": baseline, "updatedAt": updated_at}
        state["remote"]["skills"] = {"version": 2, "indexEtag": pointer_etag, "indexId": index_id, "packages": packages, "updatedAt": updated_at}
        self._observed_skill_revision = self.skills.content_revision
        self._save_state()
        return {**state["skills"], "snapshotId": index_id, "changed": changed, "added": added, "updated": updated, "deleted": deleted}

    def _upload_skill_package(self, client: WebDavClient, box: CryptoBox, data: bytes, content_hash: str) -> dict:
        package_id = content_hash
        try:
            client.put(f"skills/packages/{package_id}.enc", box.encrypt(f"skill-package:{package_id}", data), create=True)
        except CloudError as exc:
            if "HTTP 412" not in str(exc):
                raise
        downloaded, _ = client.get(f"skills/packages/{package_id}.enc")
        if self.skills.skill_package_hash(box.decrypt(f"skill-package:{package_id}", downloaded)) != content_hash:
            raise CloudError("Skill package read-back verification failed", 409)
        return {"packageId": package_id, "contentSha256": content_hash, "updatedAt": _timestamp()}

    @_serialized_cloud_operation
    def unmanage_skill(self, name: str) -> dict:
        result = self.skills.unmanage(name)
        result["cloud"] = self._publish_skill_change(name)
        return result

    @_serialized_cloud_operation
    def set_skill_shared(self, name: str, shared: bool) -> dict:
        result = self.skills.set_shared(name, shared)
        result["cloud"] = self._publish_skill_change(name) if result["changed"] else {"changed": False, "pending": False}
        return result

    def _publish_skill_change(self, name: str) -> dict:
        current_hash = self.skills.content_hashes({name}).get(name)
        if current_hash is None:
            return {"changed": False, "pending": False}
        try:
            return {**self.upload_skills({name}), "pending": False}
        except (CloudError, OSError) as exc:
            self._schedule_skill_push(name, current_hash, time.monotonic())
            self._record_decrypt_failure(exc, "push")
            return {"changed": False, "pending": True, "error": str(exc)}

    def _remote_snapshot(self) -> tuple[bytes, str, str]:
        client, box = self._connection()
        pointer_data, pointer_etag = client.get("skills/current.enc")
        index = self._parse_skill_pointer(box, pointer_data, pointer_etag)
        return self._download_skill_snapshot(client, box, index), index["indexId"], pointer_etag

    @_serialized_cloud_operation
    def restore_skills(self, expected_snapshot_id: str) -> dict:
        snapshot, snapshot_id, pointer_etag = self._remote_snapshot()
        if snapshot_id != expected_snapshot_id:
            raise CloudError("The remote skill snapshot changed before restore", 409)
        result = self.skills.restore(snapshot)
        self._state["skills"] = {"indexEtag": pointer_etag, "indexId": snapshot_id, "localSha256": self.skills.content_hashes(), "restoredAt": _timestamp()}
        self._observed_skill_revision = self.skills.content_revision
        self._save_state()
        return result

    @_serialized_cloud_operation
    def bind_local_account(self, account_key: str) -> dict:
        self.ensure_no_pending_account_operation()
        return self.accounts.bind_cloud_account(self, account_key)

    @_serialized_cloud_operation
    def link_local_account(self, account_key: str) -> dict:
        self.ensure_no_pending_account_operation()
        return self.accounts.link_cloud_account(self, account_key)

    @_serialized_cloud_operation
    def release_local_account(self, account_id: str) -> dict:
        self.ensure_no_pending_account_operation()
        return self.accounts.release_cloud_account(self, account_id)

    @_serialized_cloud_operation
    def share_local_account(self, account_id: str) -> dict:
        self.ensure_no_pending_account_operation()
        return self.accounts.share_cloud_account(self, account_id)

    @_serialized_cloud_operation
    def delete_remote_account(self, account_key: str, expected_etag: str | None = None) -> list[dict]:
        self.ensure_no_pending_account_operation()
        return self.delete_account_payloads(str(account_key or ""), expected_etag) if expected_etag is not None else self.delete_account_payloads(str(account_key or ""))

    @_serialized_cloud_operation
    def delete_local_account(self, account_id: str) -> dict:
        self.ensure_no_pending_account_operation()
        return self.accounts.delete(account_id)

    @_serialized_cloud_operation
    def rename_local_account(self, account_id: str, label: str) -> dict:
        self.ensure_no_pending_account_operation()
        return self.accounts.rename(account_id, label)

    def delete_account_payloads(self, key: str, expected_etag: str | None = None) -> dict:
        self._require_conditional_writes()
        client, box = self._connection()
        state_path, revisions_path = self._account_paths(key)
        try:
            state, etag = self._account_state_with(client, box, key)
        except CloudError as exc:
            if "HTTP 404" not in str(exc):
                raise
            state, etag = None, None
        if state is not None:
            if expected_etag is not None and etag != expected_etag:
                raise CloudError("Cloud account changed before it could be bound", 409)
            client.delete(state_path, etag=etag)
            try:
                client.get(state_path)
            except CloudError as exc:
                if "HTTP 404" not in str(exc):
                    raise
            else:
                raise CloudError("Cloud account deletion verification failed", 409)
        try:
            revisions = [name for name in client.list(revisions_path) if name.endswith(".enc") and "/" not in name and "\\" not in name]
        except CloudError as exc:
            if "HTTP 404" not in str(exc):
                raise
            revisions = []
        for name in revisions:
            client.delete(f"{revisions_path}/{name}")
        try:
            remaining = [name for name in client.list(revisions_path) if name.endswith(".enc")]
        except CloudError as exc:
            if "HTTP 404" not in str(exc):
                raise
            remaining = []
        if remaining:
            raise CloudError("Cloud account revision deletion verification failed", 409)
        client.delete(revisions_path)
        self._state["remote"]["accounts"].pop(key, None)
        self._save_state()
        return {"accountKey": key, "deleted": True, "alreadyDeleted": state is None and not revisions}

    @staticmethod
    def _usage_pointer_path(machine_id: str) -> str:
        return f"usage/machines/{machine_id}.enc"

    @staticmethod
    def _usage_payload_path(kind: str, machine_id: str, payload_id: str) -> str:
        return f"usage/{kind}/{machine_id}/{payload_id}.enc"

    @staticmethod
    def _usage_payload_bytes(value: dict) -> tuple[str, bytes]:
        compressed = zlib.compress(canonical_json(value), 9)
        return hashlib.sha256(compressed).hexdigest(), compressed

    @staticmethod
    def _decode_usage_payload(box: CryptoBox, purpose: str, encrypted: bytes, expected_id: str) -> dict:
        compressed = box.decrypt(purpose, encrypted)
        if hashlib.sha256(compressed).hexdigest() != expected_id:
            raise CloudError("Usage payload identity check failed", 409)
        try:
            value = json.loads(zlib.decompress(compressed))
        except (json.JSONDecodeError, zlib.error) as exc:
            raise CloudError("Invalid usage payload", 409) from exc
        if value.get("version") != 1:
            raise CloudError("Unsupported usage payload", 409)
        return value

    def _put_usage_payload(self, client: WebDavClient, box: CryptoBox, kind: str, machine_id: str, value: dict, verify: bool = True) -> tuple[str, int]:
        payload_id, compressed = self._usage_payload_bytes(value)
        path, purpose = self._usage_payload_path(kind, machine_id, payload_id), f"usage-{kind}:{machine_id}:{payload_id}"
        try:
            client.put(path, box.encrypt(purpose, compressed), create=True)
        except CloudError as exc:
            if "HTTP 412" not in str(exc):
                raise
        if verify:
            downloaded, _ = client.get(path)
            self._decode_usage_payload(box, purpose, downloaded, payload_id)
        return payload_id, len(compressed)

    @staticmethod
    def _parse_usage_pointer(box: CryptoBox, machine_id: str, encrypted: bytes) -> dict:
        try:
            pointer = json.loads(box.decrypt(f"usage-pointer:{machine_id}", encrypted, 1024 * 1024))
        except json.JSONDecodeError as exc:
            raise CloudError("Invalid usage pointer", 409) from exc
        if pointer.get("machineId") != machine_id:
            raise CloudError("Usage pointer identity check failed", 409)
        if pointer.get("version") == 1 and isinstance(pointer.get("sequence"), int):
            return pointer
        packs, pack_format, verification = pointer.get("packs"), pointer.get("packFormat"), pointer.get("verification")
        pack_ids = packs if isinstance(packs, dict) else {}
        invalid_verification = verification is not None and (not isinstance(verification, dict)
            or verification.get("fullVerifiedAt") is not None and not isinstance(verification.get("fullVerifiedAt"), str)
            or "mode" in verification and verification.get("mode") not in {"sampled", "full"}
            or "verifiedPacks" in verification and (not isinstance(verification["verifiedPacks"], list)
                or any(not isinstance(pack_id, str) or pack_id not in pack_ids for pack_id in verification["verifiedPacks"])))
        if pointer.get("version") != 2 or not isinstance(packs, dict) or any(
            not isinstance(pack_id, str) or not pack_id or len(pack_id) > 128 or not isinstance(pack_hash, str) or len(pack_hash) != 64 or any(character not in "0123456789abcdef" for character in pack_hash)
            for pack_id, pack_hash in packs.items()
        ) or pack_format is not None and (
            not isinstance(pack_format, dict) or not isinstance(pack_format.get("version"), int) or not isinstance(pack_format.get("bucketBits"), int) or not isinstance(pack_format.get("maxCompressedBytes"), int)
        ) or invalid_verification:
            raise CloudError("Invalid usage pack manifest", 409)
        return pointer

    def _usage_pointer(self, client: WebDavClient, box: CryptoBox, machine_id: str) -> tuple[dict | None, str | None]:
        try:
            encrypted, etag = client.get(self._usage_pointer_path(machine_id))
        except CloudError as exc:
            if "HTTP 404" in str(exc):
                return None, None
            raise
        return self._parse_usage_pointer(box, machine_id, encrypted), etag

    def _write_usage_pointer(self, client: WebDavClient, box: CryptoBox, pointer: dict, etag: str | None) -> str:
        payload = box.encrypt(f"usage-pointer:{pointer['machineId']}", canonical_json(pointer))
        return client.put(self._usage_pointer_path(pointer["machineId"]), payload, etag=etag) if etag else client.put(self._usage_pointer_path(pointer["machineId"]), payload, create=True)

    def _verify_usage_pointer(self, client: WebDavClient, box: CryptoBox, expected: dict) -> str:
        verified, etag = self._usage_pointer(client, box, expected["machineId"])
        keys = ("packs", "recordCount", "packFormat", "verification") if expected.get("version") == 2 else ("sequence", "headChunkId", "checkpointId")
        if verified.get("version") != expected.get("version") or any(verified.get(key) != expected.get(key) for key in keys):
            raise CloudError("Usage pointer verification failed", 409)
        return etag

    @classmethod
    def _usage_record_packs(cls, machine_id: str, records: dict[str, dict]) -> dict[str, dict]:
        packs = {}
        entries = [(int.from_bytes(hashlib.sha256(key.encode()).digest(), "big"), {"key": key, "record": records[key], "contentSha256": content_hash(records[key])}) for key in sorted(records)]

        def build(prefix: int, bits: int, selected: list[tuple[int, dict]]) -> None:
            if not selected:
                return
            bucket = prefix >> (bits - USAGE_PACK_BUCKET_BITS)
            suffix_bits = bits - USAGE_PACK_BUCKET_BITS
            suffix = f"-{prefix & ((1 << suffix_bits) - 1):0{(suffix_bits + 3) // 4}x}" if suffix_bits else ""
            pack_id = f"{bucket << (8 - USAGE_PACK_BUCKET_BITS):02x}-{bits:03d}{suffix}"
            value = {"version": 1, "machineId": machine_id, "packId": pack_id, "records": [entry for _, entry in selected]}
            pack_hash, compressed = cls._usage_payload_bytes(value)
            if len(compressed) <= USAGE_PACK_MAX_BYTES:
                packs[pack_id] = {"hash": pack_hash, "bytes": len(compressed), "records": len(selected), "value": value}
                return
            if len(selected) == 1:
                packs[pack_id] = {"hash": pack_hash, "bytes": len(compressed), "records": 1, "value": value}
                return
            if bits >= 256:
                raise CloudError("A usage record is too large for a WebDAV pack", 409)
            next_bits = bits + 1
            build(prefix << 1, next_bits, [entry for entry in selected if entry[0] >> (256 - next_bits) == prefix << 1])
            build((prefix << 1) | 1, next_bits, [entry for entry in selected if entry[0] >> (256 - next_bits) == (prefix << 1) | 1])

        for bucket in range(1 << USAGE_PACK_BUCKET_BITS):
            build(bucket, USAGE_PACK_BUCKET_BITS, [entry for entry in entries if entry[0] >> (256 - USAGE_PACK_BUCKET_BITS) == bucket])
        return packs

    @staticmethod
    def _validate_usage_pack_records(pack: dict, require_record_hashes: bool = False) -> None:
        records = pack.get("records")
        if not isinstance(records, list):
            raise CloudError("Usage pack records are invalid", 409)
        keys = set()
        for entry in records:
            if not isinstance(entry, dict) or not isinstance(entry.get("key"), str) or not isinstance(entry.get("record"), dict) or entry["key"] in keys:
                raise CloudError("Usage pack records are invalid", 409)
            record_hash = entry.get("contentSha256")
            expected_hash = content_hash(entry["record"])
            if (require_record_hashes or record_hash is not None) and record_hash != expected_hash:
                raise CloudError("Usage record identity check failed", 409)
            keys.add(entry["key"])

    @staticmethod
    def _usage_pack_format() -> dict:
        return {"version": USAGE_PACK_FORMAT_VERSION, "bucketBits": USAGE_PACK_BUCKET_BITS, "maxCompressedBytes": USAGE_PACK_MAX_BYTES}

    @staticmethod
    def _usage_verification_sample(pack_ids: set[str], manifest: dict[str, str]) -> list[str]:
        ordered = sorted(pack_ids)
        if len(ordered) <= USAGE_REGULAR_VERIFY_PACKS:
            return ordered
        start = int(hashlib.sha256(canonical_json(manifest)).hexdigest()[:8], 16) % len(ordered)
        return [ordered[(start + index) % len(ordered)] for index in range(USAGE_REGULAR_VERIFY_PACKS)]

    def _verify_usage_packs(self, client: WebDavClient, box: CryptoBox, machine_id: str, manifest: dict[str, str], pack_ids, require_record_hashes: bool = True) -> None:
        for pack_id in pack_ids:
            pack = self._download_usage_payload(client, box, "packs", machine_id, manifest[pack_id])
            if pack.get("machineId") != machine_id or pack.get("packId") != pack_id or not isinstance(pack.get("records"), list):
                raise CloudError("Usage pack verification failed", 409)
            self._validate_usage_pack_records(pack, require_record_hashes)

    @staticmethod
    def _usage_full_verification_due(pointer: dict | None, force_full: bool = False) -> bool:
        if force_full or pointer is None or pointer.get("version") != 2 or pointer.get("packFormat") != CloudManager._usage_pack_format():
            return True
        return _elapsed_since((pointer.get("verification") or {}).get("fullVerifiedAt"), USAGE_FULL_VERIFY_INTERVAL_SECONDS) >= USAGE_FULL_VERIFY_INTERVAL_SECONDS

    def _publish_usage(self, client: WebDavClient, box: CryptoBox, records: dict[str, dict], present_keys: set[str], force_full: bool = False) -> dict:
        machine_id, usage = self.machine_id, self._state["usage"]
        pointer, pointer_etag = self._usage_pointer(client, box, machine_id)
        current_hashes = {key: content_hash(record) for key, record in records.items()}
        published = usage.get("published") if isinstance(usage.get("published"), dict) else {}
        built = self._usage_record_packs(machine_id, records)
        manifest = {pack_id: pack["hash"] for pack_id, pack in built.items()}
        old_manifest = pointer.get("packs", {}) if pointer and pointer.get("version") == 2 else {}
        full_verification = self._usage_full_verification_due(pointer, force_full)
        changed_packs = {pack_id for pack_id, pack_hash in manifest.items() if force_full or old_manifest.get(pack_id) != pack_hash}
        for pack_id in sorted(changed_packs):
            uploaded_hash, _ = self._put_usage_payload(client, box, "packs", machine_id, built[pack_id]["value"], verify=False)
            if uploaded_hash != manifest[pack_id]:
                raise CloudError("Usage pack hash changed during upload", 409)
        verified_packs = sorted(manifest) if full_verification else self._usage_verification_sample(changed_packs, manifest)
        self._verify_usage_packs(client, box, machine_id, manifest, verified_packs)
        full_verified_at = _timestamp() if full_verification else ((pointer or {}).get("verification") or {}).get("fullVerifiedAt")
        changed = pointer is None or pointer.get("version") != 2 or old_manifest != manifest or force_full or full_verification
        if changed:
            updated = {
                "version": 2, "machineId": machine_id, "packs": manifest, "packFormat": self._usage_pack_format(), "recordCount": len(records),
                "verification": {"fullVerifiedAt": full_verified_at},
            }
            pointer_etag = self._write_usage_pointer(client, box, updated, pointer_etag)
            if full_verification:
                pointer_etag = self._verify_usage_pointer(client, box, updated)
        usage.update({"published": current_hashes, "localPointerEtag": pointer_etag})
        usage.pop("sequence", None)
        self._save_state()
        if changed:
            for payload_hash in set(old_manifest.values()) - set(manifest.values()):
                client.delete(self._usage_payload_path("packs", machine_id, payload_hash))
        if changed and pointer and pointer.get("version") == 1:
            for payload_id in self._encrypted_payloads(client, f"usage/chunks/{machine_id}"):
                client.delete(self._usage_payload_path("chunks", machine_id, payload_id))
            if pointer.get("checkpointId"):
                client.delete(self._usage_payload_path("checkpoints", machine_id, pointer["checkpointId"]))
        return {
            "uploaded": len(records) if force_full or not published else sum(published.get(key) != current_hashes[key] for key in records),
            "deleted": len(set(published) - present_keys), "fullSnapshot": pointer is None or pointer.get("version") != 2 or pointer.get("packFormat") != self._usage_pack_format(),
            "packsUploaded": len(changed_packs), "packs": len(manifest), "packsVerified": len(verified_packs), "fullVerification": full_verification,
        }

    def _download_usage_payload(self, client: WebDavClient, box: CryptoBox, kind: str, machine_id: str, payload_id: str) -> dict:
        encrypted, _ = client.get(self._usage_payload_path(kind, machine_id, payload_id))
        return self._decode_usage_payload(box, f"usage-{kind}:{machine_id}:{payload_id}", encrypted, payload_id)

    def _fetch_legacy_usage(self, client: WebDavClient, box: CryptoBox, machine_id: str, pointer: dict) -> tuple[list[dict], int]:
        checkpoint = self._download_usage_payload(client, box, "checkpoints", machine_id, pointer["checkpointId"])
        operations = [{"action": "upsert", **record} for record in checkpoint.get("records", [])]
        chunks, chunk_id = [], pointer.get("headChunkId")
        while chunk_id:
            chunk = self._download_usage_payload(client, box, "chunks", machine_id, chunk_id)
            chunks.append(chunk)
            chunk_id = chunk.get("parentChunkId")
        for chunk in reversed(chunks):
            operations.extend(chunk.get("operations") or [])
        return operations, len(chunks) + 1

    def _migrate_legacy_usage(self, client: WebDavClient, box: CryptoBox, machine_id: str, pointer_etag: str, operations: list[dict]) -> tuple[dict[str, str], dict[str, list[dict]]]:
        records = {}
        for operation in operations:
            if operation.get("action") == "upsert" and (operation.get("record") or {}).get("kind") in {"cost", "token"}:
                continue
            validate_sync_operation(operation)
            if operation["action"] == "upsert":
                if operation["record"]["row"].get("sync", {}).get("originMachineId") != machine_id:
                    raise CloudError("Legacy usage operation origin does not match its machine", 409)
                records[operation["key"]] = operation["record"]
            else:
                records.pop(operation["key"], None)
        client.ensure_directories(f"usage/packs/{machine_id}")
        built = self._usage_record_packs(machine_id, records)
        for pack_id in sorted(built):
            uploaded_hash, _ = self._put_usage_payload(client, box, "packs", machine_id, built[pack_id]["value"], verify=False)
            if uploaded_hash != built[pack_id]["hash"]:
                raise CloudError("Usage pack hash changed during legacy migration", 409)
        manifest = {pack_id: pack["hash"] for pack_id, pack in built.items()}
        self._verify_usage_packs(client, box, machine_id, manifest, sorted(manifest))
        full_verified_at = _timestamp()
        updated = {
            "version": 2, "machineId": machine_id, "packs": manifest, "packFormat": self._usage_pack_format(), "recordCount": len(records),
            "verification": {"fullVerifiedAt": full_verified_at},
        }
        self._write_usage_pointer(client, box, updated, pointer_etag)
        self._verify_usage_pointer(client, box, updated)
        for kind in ("chunks", "checkpoints"):
            for payload_id in self._encrypted_payloads(client, f"usage/{kind}/{machine_id}"):
                client.delete(self._usage_payload_path(kind, machine_id, payload_id))
        return manifest, {pack_id: pack["value"]["records"] for pack_id, pack in built.items()}

    def _fetch_usage(self, client: WebDavClient, box: CryptoBox, force_full: bool = False) -> dict:
        remote = self._state["usage"].setdefault("remote", {})
        changed, downloaded, migrated, conflicts = 0, 0, 0, []
        items = client.list_details("usage/machines")
        machine_ids = {item["name"][:-4] for item in items if item["name"].endswith(".enc")}
        for item in items:
            if not item["name"].endswith(".enc"):
                continue
            machine_id = item["name"][:-4]
            if machine_id == self.machine_id and item.get("etag") == self._state["usage"].get("localPointerEtag"):
                continue
            cached_packs = self._usage_data.pack_hashes(machine_id) if machine_id != self.machine_id else {}
            remote_state = remote.get(machine_id) if isinstance(remote.get(machine_id), dict) else {}
            full_verification = force_full or _elapsed_since(remote_state.get("lastFullVerificationAt"), USAGE_FULL_VERIFY_INTERVAL_SECONDS) >= USAGE_FULL_VERIFY_INTERVAL_SECONDS
            if machine_id != self.machine_id and not full_verification and item.get("etag") and item["etag"] == remote_state.get("pointerEtag") and len(cached_packs) == remote_state.get("packCount"):
                continue
            encrypted, etag = client.get(self._usage_pointer_path(machine_id))
            pointer = self._parse_usage_pointer(box, machine_id, encrypted)
            if machine_id == self.machine_id:
                if pointer.get("version") == 1:
                    operations, legacy_downloaded = self._fetch_legacy_usage(client, box, machine_id, pointer)
                    self._migrate_legacy_usage(client, box, machine_id, etag, operations)
                    changed += 1
                    downloaded += legacy_downloaded
                    migrated += 1
                else:
                    self._state["usage"]["localPointerEtag"] = etag
                    self._save_state()
                continue
            if pointer.get("version") == 2:
                for attempt in range(2):
                    changed_packs = set(pointer["packs"]) if full_verification else {pack_id for pack_id, pack_hash in pointer["packs"].items() if cached_packs.get(pack_id) != pack_hash}
                    payloads = {}
                    try:
                        for pack_id in sorted(changed_packs):
                            pack = self._download_usage_payload(client, box, "packs", machine_id, pointer["packs"][pack_id])
                            if pack.get("machineId") != machine_id or pack.get("packId") != pack_id:
                                raise CloudError("Usage pack identity check failed", 409)
                            self._validate_usage_pack_records(pack, (pointer.get("packFormat") or {}).get("version", 0) >= 3)
                            payloads[pack_id] = pack["records"]
                        break
                    except CloudError as exc:
                        if attempt or exc.http_status != 404 and "HTTP 404" not in str(exc):
                            raise
                        latest, latest_etag = self._usage_pointer(client, box, machine_id)
                        if latest_etag == etag or latest is None or latest.get("version") != 2:
                            raise
                        pointer, etag = latest, latest_etag
                if full_verification or changed_packs or set(cached_packs) - set(pointer["packs"]):
                    conflicts.extend(self._usage_data.apply_pack_snapshot(machine_id, pointer["packs"], payloads, full_verification))
                    changed += 1
                downloaded += len(payloads)
                remote[machine_id] = {
                    "pointerEtag": etag, "version": 2, "packCount": len(pointer["packs"]),
                    "lastFullVerificationAt": _timestamp() if full_verification else remote_state.get("lastFullVerificationAt"),
                }
            else:
                operations, legacy_downloaded = self._fetch_legacy_usage(client, box, machine_id, pointer)
                manifest, payloads = self._migrate_legacy_usage(client, box, machine_id, etag, operations)
                conflicts.extend(self._usage_data.apply_pack_snapshot(machine_id, manifest, payloads, True))
                downloaded += legacy_downloaded
                changed += 1
                migrated += 1
                remote[machine_id] = {"version": 2, "packCount": len(manifest), "lastFullVerificationAt": _timestamp()}
            self._save_state()
        changed += self._usage_data.remove_origins_not_in(machine_ids) or 0
        for machine_id in set(remote) - machine_ids:
            remote.pop(machine_id, None)
        self._save_state()
        return {"machinesChanged": changed, "machinesMigrated": migrated, "payloadsDownloaded": downloaded, "conflicts": len(conflicts)}

    def _fetch_usage_data(self, client: WebDavClient, box: CryptoBox, force_full: bool = False) -> dict:
        if self._usage_data is None:
            return {"skipped": True}
        client.ensure_directories("usage/machines")
        return {**self._fetch_usage(client, box, force_full), "fetchedAt": _timestamp()}

    def _push_usage_data(self, force_full: bool = False) -> dict:
        if self._usage_data is None:
            return {"skipped": True}
        self._require_conditional_writes()
        client, box = self._connection(True)
        for path in ("usage/machines", f"usage/packs/{self.machine_id}"):
            client.ensure_directories(path)
        records, present_keys = self._usage_data.snapshot()
        return {**self._publish_usage(client, box, records, present_keys, force_full), "pushedAt": _timestamp()}

    @_serialized_cloud_operation
    def sync_usage_data(self) -> dict:
        if self._usage_data is None:
            return {"skipped": True}
        self._state["usage"]["lastAttemptAt"] = _timestamp()
        self._save_state()
        self._require_conditional_writes()
        client, box = self._connection(True)
        for path in ("usage/machines", f"usage/packs/{self.machine_id}"):
            client.ensure_directories(path)
        records, present_keys = self._usage_data.snapshot()
        published = self._publish_usage(client, box, records, present_keys)
        fetched = self._fetch_usage(client, box)
        self._state["usage"].update({"lastSuccessAt": _timestamp(), "failure": None})
        self._save_state()
        return {"published": published, "fetched": fetched, "syncedAt": self._state["usage"]["lastSuccessAt"]}

    def _schedule_skill_push(self, name: str, current_hash: str, now: float) -> None:
        self._pending_skill_pushes[name] = {"hash": current_hash, "since": now, "nextAttemptAt": now + AUTO_PUSH_STABLE_SECONDS, "attempts": 0}
        self._auto_push_failures.pop(name, None)

    def _observe_skill_content(self, now: float) -> None:
        try:
            current_hashes = self.skills.content_hashes()
        except (OSError, SkillError):
            for pending in self._pending_skill_pushes.values():
                pending.update({"since": now, "nextAttemptAt": now + AUTO_PUSH_STABLE_SECONDS, "attempts": 0})
            return
        baseline = self._state.get("skills", {}).get("localSha256", {})
        baseline = baseline if isinstance(baseline, dict) else {}
        previous = self._observed_skill_hashes or {}
        for name in current_hashes.keys() | previous.keys() | baseline.keys():
            current_hash = current_hashes.get(name)
            if self._observed_skill_hashes is None or current_hash != previous.get(name):
                if current_hash is None or current_hash == baseline.get(name):
                    self._pending_skill_pushes.pop(name, None)
                    self._auto_push_failures.pop(name, None)
                else:
                    self._schedule_skill_push(name, current_hash, now)
        self._observed_skill_hashes = current_hashes

    def _finish_successful_push(self, now: float) -> None:
        try:
            current_hashes = self.skills.content_hashes()
        except (OSError, SkillError):
            return
        self._observed_skill_hashes = current_hashes
        baseline = self._state.get("skills", {}).get("localSha256", {})
        baseline = baseline if isinstance(baseline, dict) else {}
        self._pending_skill_pushes.clear()
        self._auto_push_failures.clear()
        for name, current_hash in current_hashes.items():
            if current_hash != baseline.get(name):
                self._schedule_skill_push(name, current_hash, now)

    def _finish_fetch_baseline(self, now: float) -> None:
        try:
            current_hashes = self.skills.content_hashes()
        except (OSError, SkillError):
            return
        self._observed_skill_hashes = current_hashes
        baseline = self._state.get("skills", {}).get("localSha256", {})
        baseline = baseline if isinstance(baseline, dict) else {}
        for name, current_hash in current_hashes.items():
            if current_hash != baseline.get(name) and name not in self._auto_push_failures:
                self._schedule_skill_push(name, current_hash, now)
            elif current_hash == baseline.get(name):
                self._pending_skill_pushes.pop(name, None)

    def _auto_fetch_if_due(self, now: float) -> bool:
        if now - self._last_auto_fetch_at < AUTO_FETCH_INTERVAL_SECONDS:
            return False
        self._last_auto_fetch_at = now
        self._state["lastAutoFetchAt"] = _timestamp()
        self._save_state()
        try:
            self.fetch(include_usage=False)
        except Exception as exc:
            self._record_decrypt_failure(exc, "fetch")
            if getattr(exc, "decrypt_failed", False):
                return False
            raise
        return True

    def _auto_usage_sync_if_due(self, now: float) -> bool:
        if self._usage_data is None or not self.config()["webdav"].get("usageDataAutoSync", True) or now < self._next_usage_sync_at:
            return False
        usage = self._state["usage"]
        self._next_usage_sync_at = now + USAGE_SYNC_INTERVAL_SECONDS
        try:
            self.sync_usage_data()
        except Exception as exc:
            self._record_decrypt_failure(exc, "usage sync")
            attempt = int((usage.get("failure") or {}).get("attempt") or 0) + 1
            usage["failure"] = {"message": str(exc), "attempt": attempt, "failedAt": _timestamp()}
            self._save_state()
            return False
        return True

    @_serialized_cloud_operation
    def _upload_due_skills(self, names):
        if not self.config()["webdav"].get("enabled") or not self.config()["webdav"].get("skillsAutoUpload", True):
            raise OperationSkipped("Automatic skill upload is disabled")
        errors = {}
        try:
            self.upload_skills(names)
        except Exception as exc:
            if len(names) == 1:
                errors[next(iter(names))] = exc
            else:
                # Each attempt rebuilds against the latest pointer and local content.
                for name in sorted(names):
                    try:
                        self.upload_skills({name})
                    except Exception as failure:
                        errors[name] = failure
        return {"errors": errors, "skills": sorted(names)}

    def maintenance_tick(self, now: float | None = None) -> dict:
        now = time.monotonic() if now is None else now
        config = self.config()
        if not config["webdav"].get("enabled") or self._state.get("pendingAccountOperation"):
            return {"pushed": False, "fetched": False, "usageSynced": False}
        with self._operation_lock:
            if config["webdav"].get("skillsAutoUpload", True):
                self._observe_skill_content(now)
            due = {name for name, pending in self._pending_skill_pushes.items() if config["webdav"].get("skillsAutoUpload", True) and name not in self._auto_push_failures and now >= pending["nextAttemptAt"]}
            before_hashes = dict(self._observed_skill_hashes or {})
        errors = {}
        if due:
            try:
                errors = self._upload_due_skills(due)["errors"]
            except OperationSkipped:
                for name in due:
                    self._pending_skill_pushes.pop(name, None)
                due = set()
        pushed = False
        for name in sorted(due):
            pending = self._pending_skill_pushes.get(name)
            if not pending or not config["webdav"].get("skillsAutoUpload", True) or name in self._auto_push_failures or now < pending["nextAttemptAt"]:
                continue
            before_hash = before_hashes.get(name)
            try:
                if name in errors:
                    raise errors[name]
                pushed = True
            except Exception as exc:
                self._record_decrypt_failure(exc, "push")
                self._observe_skill_content(now)
                pending = self._pending_skill_pushes.get(name)
                if not pending or (self._observed_skill_hashes or {}).get(name) != before_hash:
                    continue
                pending["attempts"] += 1
                if pending["attempts"] >= AUTO_PUSH_MAX_ATTEMPTS:
                    self._auto_push_failure_id += 1
                    self._auto_push_failures[name] = {"id": self._auto_push_failure_id, "skill": name, "message": str(exc)}
                else:
                    pending["nextAttemptAt"] = now + AUTO_PUSH_RETRY_SECONDS
                continue
            current_hashes = self.skills.content_hashes({name})
            self._observed_skill_hashes = dict(self._observed_skill_hashes or {})
            if name in current_hashes:
                self._observed_skill_hashes[name] = current_hashes[name]
            else:
                self._observed_skill_hashes.pop(name, None)
            current_hash = current_hashes.get(name)
            if current_hash == self._state.get("skills", {}).get("localSha256", {}).get(name):
                self._pending_skill_pushes.pop(name, None)
                self._auto_push_failures.pop(name, None)
            elif current_hash is not None:
                self._schedule_skill_push(name, current_hash, now)
        fetched = self._auto_fetch_if_due(now)
        usage_synced = self._auto_usage_sync_if_due(now)
        return {"pushed": pushed, "fetched": fetched, "usageSynced": usage_synced}

    def account_key(self, account_id: str) -> str:
        return self._connection()[1].account_key(account_id)

    @staticmethod
    def api_account_key(identity: dict) -> str:
        if not api_identity_id(identity):
            raise CloudError("Cannot create a cloud key for an incomplete API account", 409)
        return hashlib.sha256(f"codex-monitor-api-cloud-key-v1:{identity['idTokenHash']}".encode()).hexdigest()

    def new_account_key(self) -> str:
        return self._connection()[1].placeholder_account_key()

    def new_placeholder_account_key(self) -> str:
        return self.new_account_key()

    def _require_conditional_writes(self) -> None:
        if not self._state.get("conditionalWritesVerified") and not self.config()["webdav"].get("allowOptimisticWrites", True):
            raise CloudError("Test WebDAV conditional writes or explicitly enable optimistic writes before using cloud synchronization", 409)

    def _account_paths(self, key: str, revision: str | None = None) -> tuple[str, str]:
        return f"accounts/states/{key}.enc", f"accounts/revisions/{key}/{revision}.enc" if revision else f"accounts/revisions/{key}"

    @staticmethod
    def _encrypted_payloads(client: WebDavClient, path: str) -> set[str]:
        try:
            return {name[:-4] for name in client.list(path) if name.endswith(".enc") and "/" not in name and "\\" not in name}
        except (CloudError, AttributeError):
            return set()

    def _cleanup_skill_packages(self, client: WebDavClient, box: CryptoBox, candidates: set[str]) -> None:
        if not candidates:
            return
        try:
            pointer_data, _ = client.get("skills/current.enc")
            pointer = json.loads(box.decrypt("skills-pointer", pointer_data))
            current = {item["packageId"] for item in pointer.get("packages", {}).values()} if pointer.get("version") == 2 else set()
        except (CloudError, json.JSONDecodeError, AttributeError):
            return
        if pointer.get("version") != 2:
            return
        for package_id in candidates - current:
            try:
                client.delete(f"skills/packages/{package_id}.enc")
            except CloudError:
                continue

    @staticmethod
    def _cleanup_legacy_skill_snapshots(client: WebDavClient, candidates: set[str]) -> None:
        for snapshot_id in candidates:
            try:
                client.delete(f"skills/snapshots/{snapshot_id}.enc")
            except CloudError:
                continue

    def _cleanup_account_revisions(self, client: WebDavClient, box: CryptoBox, key: str, candidates: set[str]) -> None:
        if not candidates:
            return
        try:
            current = self._account_state_with(client, box, key)[0].get("revisionId")
        except (CloudError, AttributeError):
            return
        if not current:
            return
        for revision in candidates - {current}:
            try:
                client.delete(self._account_paths(key, revision)[1])
            except CloudError:
                continue

    def account_state(self, key: str) -> tuple[dict, str]:
        client, box = self._connection()
        return self._account_state_with(client, box, key)

    def _upload_revision(self, client: WebDavClient, box: CryptoBox, key: str, auth_data: bytes, identity: dict, key_type: str) -> str:
        revision = hashlib.sha256(auth_data).hexdigest()
        client.ensure_directories(f"accounts/revisions/{key}")
        payload = box.encrypt(f"account-revision:{key}:{revision}", json.dumps({
            "version": 1, "accountKey": key, "keyType": key_type, "accountIdHash": hashlib.sha256(identity["accountId"].encode()).hexdigest() if identity.get("accountId") else None,
            "authSize": len(auth_data), "authSha256": revision, "auth": base64.b64encode(auth_data).decode(),
        }).encode())
        try:
            client.put(self._account_paths(key, revision)[1], payload, create=True)
        except CloudError as exc:
            if "HTTP 412" not in str(exc):
                raise
        downloaded, _ = client.get(self._account_paths(key, revision)[1])
        if box.decrypt(f"account-revision:{key}:{revision}", downloaded) != box.decrypt(f"account-revision:{key}:{revision}", payload):
            raise CloudError("Account revision read-back verification failed", 409)
        return revision

    def _download_account_revision_with(self, client: WebDavClient, box: CryptoBox, state: dict) -> bytes:
        key, revision = state.get("accountKey"), state.get("revisionId")
        if not key or not revision:
            raise CloudError("Remote account state is incomplete", 409)
        payload, _ = client.get(self._account_paths(key, revision)[1])
        try:
            revision_data = json.loads(box.decrypt(f"account-revision:{key}:{revision}", payload))
            auth_data = base64.b64decode(revision_data["auth"], validate=True)
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            raise CloudError("Invalid remote account revision", 409) from exc
        identity = auth_identity(parse_auth_bytes(auth_data))
        account_id = state.get("accountId") or identity.get("accountId")
        state_account_id = state.get("accountId")
        state_email = state.get("email")
        if (
            revision_data.get("version") != 1 or revision_data.get("accountKey") != key or len(auth_data) != revision_data.get("authSize") or hashlib.sha256(auth_data).hexdigest() != revision_data.get("authSha256")
            or (hashlib.sha256(account_id.encode()).hexdigest() if account_id else None) != revision_data.get("accountIdHash")
            or (state_account_id and identity.get("accountId") != state_account_id)
            or (state_email and identity.get("email") != state_email)
        ):
            raise CloudError("Account revision completion check failed", 409)
        return auth_data

    def list_accounts(self) -> list[dict]:
        client, _ = self._connection()
        client.ensure_directories("accounts/states")
        items = []
        for filename in client.list("accounts/states"):
            if not filename.endswith(".enc"):
                continue
            key = filename[:-4]
            try:
                state, etag = self.account_state(key)
                state["etag"] = etag
                items.append(state)
            except CloudError:
                continue
        return items

    def bind_account(self, key: str) -> tuple[dict, bytes, str]:
        self._require_conditional_writes()
        client, box = self._connection()
        state, etag = self.account_state(key)
        return state, self._download_account_revision_with(client, box, state), etag

    def release_account(self, key: str, auth_data: bytes, identity: dict, label: str, ready: bool = True, key_type: str | None = None, config_header: dict | None = None, account_type: str = "account", config_header_toml: str = "", reject_existing: bool = False) -> dict:
        self._require_conditional_writes()
        client, box = self._connection()
        key_type = key_type or "opaque"
        payload_identity = auth_identity(parse_auth_bytes(auth_data)) if ready else identity
        identity_id = api_identity_id(payload_identity) if account_type == "api" else None
        if account_type == "api" and identity_id is None:
            raise CloudError("The API account credential is incomplete", 409)
        try:
            state, etag = self.account_state(key)
        except CloudError as exc:
            if "HTTP 404" not in str(exc):
                raise
            state, etag = None, None
        revision = hashlib.sha256(auth_data).hexdigest()
        if state is not None:
            if reject_existing:
                _, existing_identity_id = self._validated_api_identity(state, self._download_account_revision_with(client, box, state))
                if existing_identity_id == identity_id:
                    raise CloudError("This API account is already shared", 409)
                raise CloudError("The API cloud account key belongs to a different credential", 409)
            if state.get("accountId") and state.get("accountId") != identity.get("accountId") or state.get("email") and state.get("email") != identity.get("email"):
                raise CloudError("Account release identity does not match the local binding", 409)
            if state.get("revisionId") == revision and state.get("accountId") == identity.get("accountId") and state.get("ready", True) == ready and state.get("accountType", "account") == account_type and (state.get("configHeader") or {}) == (config_header or {}) and state.get("configHeaderToml", "") == config_header_toml:
                if self._download_account_revision_with(client, box, state) != auth_data:
                    raise CloudError("Existing cloud account payload does not match the local account", 409)
                self._cache_remote_account(state, etag)
                return {**state, "etag": etag, "changed": False}
            raise CloudError("This account already exists in the cloud", 409)
        client.ensure_directories("accounts/states")
        client.ensure_directories(self._account_paths(key)[1])
        existing_revisions = self._encrypted_payloads(client, self._account_paths(key)[1])
        self._upload_revision(client, box, key, auth_data, identity, key_type)
        state = {"version": 1, "accountKey": key, "keyType": key_type, "accountId": identity.get("accountId"), "label": label, "email": identity.get("email"), "ready": ready, "accountType": account_type, "revisionId": revision, "updatedAt": _timestamp(), **({"apiIdentityId": identity_id} if identity_id else {}), **({"configHeader": config_header} if isinstance(config_header, dict) and config_header else {}), **({"configHeaderToml": config_header_toml} if config_header_toml else {})}
        new_etag = client.put(self._account_paths(key)[0], box.encrypt(f"account-state:{key}", json.dumps(state).encode()), create=True)
        verified, _ = self.account_state(key)
        if verified.get("accountId") != identity.get("accountId") or verified.get("ready", True) != ready or verified.get("revisionId") != revision or identity_id and verified.get("apiIdentityId") != identity_id:
            raise CloudError("Account release verification failed", 409)
        self._cleanup_account_revisions(client, box, key, existing_revisions)
        self._cache_remote_account(state, new_etag)
        return {**state, "etag": new_etag}
