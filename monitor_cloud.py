#!/usr/bin/env python3

import base64
import functools
import hashlib
import hmac
import json
import socket
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib
from datetime import datetime, timezone
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from monitor_accounts import atomic_write_json, auth_identity, parse_auth_bytes
from monitor_common import SafeRedirectHandler
from monitor_skills import SkillError
from monitor_usage_sync import canonical_json, content_hash

AUTO_PUSH_STABLE_SECONDS = 120
AUTO_PUSH_RETRY_SECONDS = 30
AUTO_PUSH_MAX_ATTEMPTS = 3
AUTO_FETCH_INTERVAL_SECONDS = 300
USAGE_SYNC_INTERVAL_SECONDS = 30 * 60
PASSPHRASE_SALT = b"codex-switch-passphrase-v1"
USAGE_SYNC_RETRY_SECONDS = (60, 5 * 60, 15 * 60)
USAGE_CHECKPOINT_MIN_CHUNKS = 128
USAGE_CHUNK_MAX_BYTES = 512 * 1024


def _serialized_cloud_operation(method):
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        with self._operation_lock:
            return method(self, *args, **kwargs)
    return wrapped


class CloudError(RuntimeError):
    def __init__(self, message: str, status: int = 400, *, http_status: int | None = None, category: str | None = None):
        super().__init__(message)
        self.status = status
        self.http_status = http_status
        self.category = category


def validate_server_host(host) -> str:
    if host not in {"127.0.0.1", "0.0.0.0"}:
        raise CloudError("Dashboard IP must be 127.0.0.1 or 0.0.0.0")
    return host


def load_server_config(config_path: Path) -> dict:
    try:
        config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"host": "127.0.0.1"}
    except (OSError, json.JSONDecodeError) as exc:
        raise CloudError(f"Cannot read dashboard config: {exc}", 500) from exc
    server = config.get("server", {"host": "127.0.0.1"}) if isinstance(config, dict) else None
    if not isinstance(server, dict):
        raise CloudError("Unsupported dashboard config", 500)
    return {"host": validate_server_host(server.get("host"))}


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def hash_control_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    derived = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=1 << 14, r=8, p=1, dklen=32, maxmem=64 * 1024 * 1024)
    return f"scrypt-v1$16384$8$1${base64.b64encode(salt).decode()}${base64.b64encode(derived).decode()}"


def control_password_matches(password, verifier: str) -> bool:
    if not isinstance(password, str):
        return False
    try:
        scheme, n, r, p, salt, expected = verifier.split("$", 5)
        if scheme != "scrypt-v1":
            return False
        derived = hashlib.scrypt(password.encode("utf-8"), salt=base64.b64decode(salt, validate=True), n=int(n), r=int(r), p=int(p), dklen=32, maxmem=64 * 1024 * 1024)
        return hmac.compare_digest(derived, base64.b64decode(expected, validate=True))
    except (ValueError, TypeError):
        return False


def valid_control_password_hash(verifier) -> bool:
    try:
        scheme, n, r, p, salt, expected = verifier.split("$", 5)
        return scheme == "scrypt-v1" and int(n) == 1 << 14 and int(r) == 8 and int(p) == 1 and len(base64.b64decode(salt, validate=True)) == 16 and len(base64.b64decode(expected, validate=True)) == 32
    except (AttributeError, ValueError, TypeError):
        return False


def passphrase_hash(passphrase: str) -> str:
    derived = hashlib.scrypt(passphrase.encode("utf-8"), salt=PASSPHRASE_SALT, n=1 << 15, r=8, p=1, dklen=32, maxmem=128 * 1024 * 1024)
    return f"scrypt-key-v1$32768$8$1${base64.b64encode(PASSPHRASE_SALT).decode()}${base64.b64encode(derived).decode()}"


def _passphrase_key(verifier: str) -> bytes:
    try:
        scheme, n, r, p, salt, derived = verifier.split("$", 5)
        key = base64.b64decode(derived, validate=True)
        if scheme != "scrypt-key-v1" or int(n) != 1 << 15 or int(r) != 8 or int(p) != 1 or base64.b64decode(salt, validate=True) != PASSPHRASE_SALT or len(key) != 32:
            raise ValueError
        return key
    except (AttributeError, ValueError, TypeError) as exc:
        raise CloudError("Invalid saved encryption passphrase hash") from exc


def valid_passphrase_hash(verifier) -> bool:
    try:
        _passphrase_key(verifier)
        return True
    except CloudError:
        return False


class CryptoBox:
    def __init__(self, passphrase_hash_value: str, descriptor: dict):
        self.key = _passphrase_key(passphrase_hash_value)
        try:
            if descriptor.get("format") != "codex-switch-crypto" or descriptor.get("version") != 1 or descriptor.get("kdf") != "scrypt" or base64.b64decode(descriptor["salt"], validate=True) != PASSPHRASE_SALT or int(descriptor["n"]) != 1 << 15 or int(descriptor["r"]) != 8 or int(descriptor["p"]) != 1:
                raise ValueError
        except (AttributeError, KeyError, ValueError, TypeError) as exc:
            raise CloudError("Invalid remote crypto descriptor") from exc
        verifier = hmac.new(self.key, b"codex-switch-verifier-v1", hashlib.sha256).digest()
        if not hmac.compare_digest(verifier, base64.b64decode(descriptor.get("verifier", ""))):
            raise CloudError("The encryption passphrase does not match this remote root", 409)

    @staticmethod
    def descriptor(passphrase_hash_value: str) -> dict:
        key = _passphrase_key(passphrase_hash_value)
        return {
            "format": "codex-switch-crypto", "version": 1, "kdf": "scrypt", "salt": base64.b64encode(PASSPHRASE_SALT).decode(), "n": 1 << 15, "r": 8, "p": 1,
            "verifier": base64.b64encode(hmac.new(key, b"codex-switch-verifier-v1", hashlib.sha256).digest()).decode(),
        }

    def encrypt(self, purpose: str, data: bytes) -> bytes:
        nonce = secrets.token_bytes(12)
        header = {"format": "codex-switch-encrypted", "version": 1, "purpose": purpose, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        aad = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
        return json.dumps({"header": header, "nonce": base64.b64encode(nonce).decode(), "ciphertext": base64.b64encode(AESGCM(self.key).encrypt(nonce, data, aad)).decode()}, separators=(",", ":")).encode()

    def decrypt(self, purpose: str, payload: bytes, limit: int = 600 * 1024 * 1024) -> bytes:
        try:
            envelope = json.loads(payload)
            header = envelope["header"]
            if header.get("format") != "codex-switch-encrypted" or header.get("version") != 1 or header.get("purpose") != purpose or not isinstance(header.get("size"), int) or header["size"] < 0 or header["size"] > limit:
                raise ValueError
            aad = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
            data = AESGCM(self.key).decrypt(base64.b64decode(envelope["nonce"], validate=True), base64.b64decode(envelope["ciphertext"], validate=True), aad)
        except Exception as exc:
            raise CloudError("Encrypted payload authentication failed", 409) from exc
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
            self.request("MKCOL", current, expected=(201, 405))

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
            self._config, self._config_error = {"version": 1, "machineName": None, "webdav": {}}, str(exc)
        self._machine = json.loads(self.machine_path.read_text(encoding="utf-8"))
        self._state = json.loads(self.state_path.read_text(encoding="utf-8"))
        self._observed_skill_revision = None
        self._operation_lock = threading.RLock()
        self._observed_skill_hashes = None
        self._pending_skill_pushes = {}
        self._auto_push_failures = {}
        self._auto_push_failure_id = 0
        self._last_auto_fetch_at = time.monotonic()
        self._usage_data = None
        self._usage_hmac_key = None
        self._usage_account_ids = {}
        self._next_usage_sync_at = time.monotonic()

    def _ensure_local_files(self) -> None:
        self.private_root.mkdir(parents=True, exist_ok=True)
        if not self.machine_path.exists():
            atomic_write_json(self.machine_path, {"version": 1, "machineId": uuid.uuid4().hex, "createdAt": _timestamp()})
        if not self.config_path.exists():
            atomic_write_json(self.config_path, {
                "version": 1, "machineName": socket.gethostname() or "My PC", "control": {"password": "", "passwordHash": hash_control_password("123456"), "cookieSecret": secrets.token_urlsafe(32)},
                "server": {"host": "127.0.0.1"},
                "webdav": {
                    "enabled": False, "baseUrl": "https://dav.jianguoyun.com/dav/", "username": "", "password": "", "remoteRoot": "codex-switch-sync", "encryptionPassphraseHash": "",
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
                if "server" not in config:
                    config["server"] = {"host": "127.0.0.1"}
                    changed = True
                if isinstance(control.get("password"), str) and control["password"]:
                    control["passwordHash"] = hash_control_password(control["password"])
                    control["password"] = ""
                    changed = True
                elif "password" not in control:
                    control["password"] = ""
                    changed = True
                if not valid_control_password_hash(control.get("passwordHash")):
                    control["passwordHash"] = hash_control_password("123456")
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
                if changed:
                    atomic_write_json(self.config_path, config)
        if not self.state_path.exists():
            atomic_write_json(self.state_path, {
                "version": 1, "skills": {"indexEtag": None, "indexId": None, "localSha256": {}}, "usage": {"published": {}, "remote": {}, "sequence": 0, "lastSuccessAt": None, "failure": None},
                "remote": {"accounts": {}, "skills": {}}, "pendingAccountOperation": None, "conditionalWritesVerified": False,
            })
        else:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
            changed = False
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
                state["usage"] = {"published": {}, "remote": {}, "sequence": 0, "lastSuccessAt": None, "failure": None}
                changed = True
            else:
                for key, value in (("published", {}), ("remote", {}), ("sequence", 0), ("lastSuccessAt", None), ("failure", None)):
                    if key not in state["usage"]:
                        state["usage"][key] = value
                        changed = True
            if changed:
                atomic_write_json(self.state_path, state)

    def _read_config(self) -> dict:
        try:
            config = json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CloudError(f"Cannot read WebDAV config: {exc}", 500) from exc
        webdav, control, server = config.get("webdav"), config.get("control"), config.get("server")
        if config.get("version") != 1 or not isinstance(webdav, dict) or not isinstance(control, dict) or not isinstance(server, dict) or control.get("password") != "" or not valid_control_password_hash(control.get("passwordHash")) or not isinstance(control.get("cookieSecret"), str) or len(control["cookieSecret"]) < 32:
            raise CloudError("Unsupported WebDAV config", 500)
        validate_server_host(server.get("host"))
        parsed_url = urllib.parse.urlsplit(str(webdav.get("baseUrl", "")))
        secure_url = parsed_url.scheme.lower() == "https" or parsed_url.scheme.lower() == "http" and parsed_url.hostname in {"localhost", "127.0.0.1", "::1"}
        if webdav.get("encryptionPassphraseHash") and not valid_passphrase_hash(webdav["encryptionPassphraseHash"]):
            raise CloudError("Invalid saved encryption passphrase hash", 500)
        if webdav.get("enabled") and (not secure_url or not webdav.get("username") or not webdav.get("password") or not webdav.get("encryptionPassphraseHash")):
            raise CloudError("Enabled WebDAV requires a URL, username, password, and encryption passphrase")
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

    @property
    def machine_id(self) -> str:
        return self._machine["machineId"]

    def configure_usage_sync(self, usage_data) -> None:
        self._usage_data = usage_data
        last_success = (self._state.get("usage") or {}).get("lastSuccessAt")
        try:
            elapsed = max(0.0, time.time() - datetime.fromisoformat(last_success.replace("Z", "+00:00")).timestamp()) if last_success else USAGE_SYNC_INTERVAL_SECONDS
        except (TypeError, ValueError):
            elapsed = USAGE_SYNC_INTERVAL_SECONDS
        self._next_usage_sync_at = time.monotonic() + max(0.0, USAGE_SYNC_INTERVAL_SECONDS - elapsed)

    def reset_usage_apply_cursors(self) -> None:
        self._state["usage"]["remote"] = {}
        self._state["usage"]["lastSuccessAt"] = None
        self._save_state()

    def usage_account_id(self, account_slot_id: str | None) -> str:
        account_id, id_token_hash = None, None
        if self.accounts is not None:
            with self.accounts.lock:
                account = next((item for item in self.accounts.manifest.get("accounts", []) if item.get("id") == account_slot_id), None)
                account_id = ((account or {}).get("identity") or {}).get("accountId")
                id_token_hash = ((account or {}).get("identity") or {}).get("idTokenHash")
        webdav = self.config().get("webdav") or {}
        passphrase_hash_value = webdav.get("encryptionPassphraseHash")
        cache_key = account_slot_id, account_id, id_token_hash, passphrase_hash_value
        if cache_key in self._usage_account_ids:
            return self._usage_account_ids[cache_key]
        if self._usage_hmac_key is None:
            self._usage_hmac_key = hashlib.sha256(_passphrase_key(passphrase_hash_value) if passphrase_hash_value else self.machine_id.encode()).digest()
        identity = f"account:{account_id}" if account_id else f"id-token:{id_token_hash}" if id_token_hash else f"machine:{self.machine_id}:slot:{account_slot_id or 'unknown'}"
        digest = hmac.new(self._usage_hmac_key, f"usage:{identity}".encode(), hashlib.sha256).hexdigest()
        self._usage_account_ids[cache_key] = digest if passphrase_hash_value else f"local:{digest}"
        return self._usage_account_ids[cache_key]

    def local_usage_account(self, usage_account_id: str, fallback_slot_id: str | None, fallback_label: str | None) -> tuple[str, str]:
        if self.accounts is not None:
            with self.accounts.lock:
                for account in self.accounts.manifest.get("accounts", []):
                    if self.usage_account_id(account.get("id")) == usage_account_id:
                        return account["id"], account.get("label") or fallback_label or "Unknown"
        return f"cloud-usage-{usage_account_id[:16]}", fallback_label or "Cloud usage"

    def _save_state(self) -> None:
        atomic_write_json(self.state_path, self._state)

    def redacted_status(self) -> dict:
        config, error = self._config, self._config_error
        webdav = config.get("webdav", {})
        return {
            "configPath": str(self.config_path), "machineName": config.get("machineName"), "webdav": {key: webdav.get(key) for key in ("enabled", "baseUrl", "username", "remoteRoot", "skillsAutoUpload", "usageDataAutoSync", "allowOptimisticWrites")},
            "secretsConfigured": {"password": bool(webdav.get("password")), "encryptionPassphrase": bool(webdav.get("encryptionPassphraseHash"))},
            "conditionalWritesVerified": bool(self._state.get("conditionalWritesVerified")),
            "optimisticWritesActive": not bool(self._state.get("conditionalWritesVerified")) and bool(webdav.get("allowOptimisticWrites", True)), "skills": self._state.get("skills", {}), "error": error,
            "autoSync": {
                "pending": bool(self._pending_skill_pushes), "pendingSkills": sorted(self._pending_skill_pushes), "attempts": max((item["attempts"] for item in self._pending_skill_pushes.values()), default=0),
                "failure": next(reversed(self._auto_push_failures.values()), None),
            },
            "usageSync": {
                **{key: self._state.get("usage", {}).get(key) for key in ("lastSuccessAt", "failure")},
                "nextAttemptInSeconds": max(0, round(self._next_usage_sync_at - time.monotonic())) if self._usage_data is not None else None,
            },
        }

    def cached_remote_accounts(self) -> list[dict]:
        return [item["state"] for item in self._state.get("remote", {}).get("accounts", {}).values() if isinstance(item, dict) and isinstance(item.get("state"), dict)]

    def _cache_remote_account(self, account: dict, etag: str) -> None:
        self._state["remote"]["accounts"][account["accountKey"]] = {"etag": etag, "state": {**account, "etag": etag}}
        self._save_state()

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
            package_id = item["packageId"]
            encrypted, _ = client.get(f"skills/packages/{package_id}.enc")
            data = box.decrypt(f"skill-package:{package_id}", encrypted)
            packages = self.skills.split_snapshot(data)
            if set(packages) != {name} or self.skills.skill_package_hash(data) != item["contentSha256"]:
                raise CloudError("Skill package identity check failed", 409)
            snapshots.append(data)
        return self.skills.combine_snapshots(snapshots)

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
    def fetch(self) -> dict:
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
            if item.get("etag") and isinstance(previous, dict) and previous.get("etag") == item["etag"] and isinstance(previous.get("state"), dict):
                accounts[key] = previous
                continue
            try:
                account, etag = self._account_state_with(client, box, key)
            except CloudError:
                continue
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
            package_names = self._skill_packages_needing_fetch(cached_skills, skills_changed)
            if package_names is None or package_names:
                snapshot = self._download_skill_snapshot(client, box, cached_skills, package_names)
                skill_merge = self.skills.merge(snapshot)
            else:
                skill_merge = {"added": [], "updated": [], "deleted": [], "projectionErrors": []}
            local["skills"] = {"indexEtag": cached_skills.get("indexEtag"), "indexId": cached_skills["indexId"], "localSha256": self.skills.content_hashes(), "fetchedAt": _timestamp()}
        else:
            skill_merge = {"added": [], "updated": [], "deleted": [], "projectionErrors": []}
        skills_changed = skills_changed or bool(skill_merge["added"] or skill_merge["updated"] or skill_merge.get("deleted"))
        cached = {"accounts": accounts, "skills": cached_skills, "fetchedAt": _timestamp()}
        local["remote"] = cached
        self._save_state()
        if cached_skills.get("indexId"):
            self._finish_fetch_baseline(time.monotonic())
        return {
            "accountsChanged": len(changed), "accountsRemoved": len(removed), "skillsChanged": skills_changed, "skillsAdded": skill_merge["added"],
            "skillsUpdated": skill_merge["updated"], "skillsDeleted": skill_merge.get("deleted", []), "accountsUpdated": 0, "projectionErrors": skill_merge["projectionErrors"], "fetchedAt": cached["fetchedAt"]
        }

    def begin_account_transition(self, operation: str, **details) -> None:
        self._state["pendingAccountOperation"] = {"operation": operation, "startedAt": _timestamp(), **details}
        self._save_state()

    def clear_account_transition(self) -> None:
        self._state["pendingAccountOperation"] = None
        self._save_state()

    def recover_account_transition(self) -> str | None:
        pending = self._state.get("pendingAccountOperation")
        if not isinstance(pending, dict):
            return None
        if pending.get("operation") == "bind":
            existing = next((account for account in self.accounts.manifest["accounts"] if (account.get("cloud") or {}).get("accountKey") == pending.get("accountKey")), None)
            if existing is None:
                self.accounts.bind_cloud_account(self, pending.get("accountKey"), record_transition=False)
            else:
                self.delete_account_payloads(pending.get("accountKey"))
        elif pending.get("operation") == "release":
            try:
                state, data, _ = self.bind_account(pending.get("accountKey"))
            except CloudError as exc:
                if "HTTP 404" not in str(exc):
                    raise
                self.accounts.rollback_recovered_release(pending.get("accountId"))
            else:
                if pending.get("revisionId") is None and state.get("boundMachineId") is None or state.get("revisionId") == pending.get("revisionId") and hashlib.sha256(data).hexdigest() == pending.get("revisionId"):
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
    def test(self) -> dict:
        client, _ = self._connection(True)
        client.ensure_directories("protocol-test")
        path, first = f"protocol-test/{uuid.uuid4().hex}.bin", secrets.token_bytes(32)
        etag = client.put(path, first, create=True)
        conditional_writes = True
        try:
            client.put(path, b"duplicate", create=True)
            conditional_writes = False
        except CloudError as exc:
            if exc.http_status != 412:
                raise
        try:
            client.put(path, b"wrong-etag", etag='"codex-switch-intentionally-wrong"')
            conditional_writes = False
        except CloudError as exc:
            if exc.http_status != 412:
                raise
        etag = client.put(path, b"updated", etag=client.get(path)[1])
        if client.get(path)[0] != b"updated":
            raise CloudError("WebDAV read-back verification failed", 409)
        self._state["conditionalWritesVerified"] = conditional_writes
        self._save_state()
        return {"ok": True, "strongEtag": etag, "conditionalWrites": conditional_writes, "optimisticWrites": not conditional_writes and bool(self.config()["webdav"].get("allowOptimisticWrites", True)), "warning": None if conditional_writes else "The server ignores conditional writes; conflict prevention is best-effort."}

    @staticmethod
    def _reencrypt_object(client: WebDavClient, box: CryptoBox, path: str, purpose: str) -> None:
        encrypted, etag = client.get(path)
        plaintext = box.decrypt(purpose, encrypted)
        client.put(path, box.encrypt(purpose, plaintext), etag=etag)
        verified, _ = client.get(path)
        if box.decrypt(purpose, verified) != plaintext:
            raise CloudError(f"Cloud re-encryption verification failed for {path}", 409)

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
            for kind in ("chunks", "checkpoints"):
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
    def push(self) -> dict:
        accounts = {"pushedAccounts": []}
        result = {"skills": self.upload_skills(), "accounts": accounts}
        result["changed"] = bool(result["skills"].get("changed"))
        self._finish_successful_push(time.monotonic())
        return result

    @_serialized_cloud_operation
    def upload_skills(self, names: set[str] | None = None) -> dict:
        client, box = self._connection(True)
        client.ensure_directories("skills/packages")
        existing_packages = self._encrypted_payloads(client, "skills/packages")
        existing_snapshots = self._encrypted_payloads(client, "skills/snapshots")
        local_packages = self.skills.skill_snapshots(names)
        local_hashes = {name: self.skills.skill_package_hash(data) for name, data in local_packages.items()}
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
        if migrated:
            legacy = self.skills.split_snapshot(self._download_skill_snapshot(client, box, remote_index))
            for name, data in legacy.items():
                content_hash = self.skills.skill_package_hash(data)
                packages[name] = self._upload_skill_package(client, box, data, content_hash)
        for name, data in local_packages.items():
            previous = packages.get(name)
            if previous and previous.get("contentSha256") == local_hashes[name]:
                continue
            manifest = self.skills.inspect_snapshot(data)[0]
            if previous is None and manifest["skills"]:
                added.append(name)
            elif manifest["skills"]:
                updated.append(name)
            else:
                deleted.append(name)
            packages[name] = self._upload_skill_package(client, box, data, local_hashes[name])
        changed = bool(added or updated or deleted)
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
        result["cloud"] = self.upload_skills({name})
        return result

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
        return self.accounts.bind_cloud_account(self, account_key)

    @_serialized_cloud_operation
    def release_local_account(self, account_id: str) -> dict:
        return self.accounts.release_cloud_account(self, account_id)

    @_serialized_cloud_operation
    def delete_local_account(self, account_id: str) -> dict:
        return self.accounts.delete(account_id)

    @_serialized_cloud_operation
    def rename_local_account(self, account_id: str, label: str) -> dict:
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
        return {"accountKey": key, "deleted": True}

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

    def _put_usage_payload(self, client: WebDavClient, box: CryptoBox, kind: str, machine_id: str, value: dict) -> tuple[str, int]:
        payload_id, compressed = self._usage_payload_bytes(value)
        path, purpose = self._usage_payload_path(kind, machine_id, payload_id), f"usage-{kind}:{machine_id}:{payload_id}"
        try:
            client.put(path, box.encrypt(purpose, compressed), create=True)
        except CloudError as exc:
            if "HTTP 412" not in str(exc):
                raise
        downloaded, _ = client.get(path)
        self._decode_usage_payload(box, purpose, downloaded, payload_id)
        return payload_id, len(compressed)

    @staticmethod
    def _parse_usage_pointer(box: CryptoBox, machine_id: str, encrypted: bytes) -> dict:
        try:
            pointer = json.loads(box.decrypt(f"usage-pointer:{machine_id}", encrypted, 1024 * 1024))
        except json.JSONDecodeError as exc:
            raise CloudError("Invalid usage pointer", 409) from exc
        if pointer.get("version") != 1 or pointer.get("machineId") != machine_id or not isinstance(pointer.get("sequence"), int):
            raise CloudError("Usage pointer identity check failed", 409)
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
        if any(verified.get(key) != expected.get(key) for key in ("sequence", "headChunkId", "checkpointId")):
            raise CloudError("Usage pointer verification failed", 409)
        return etag

    def _publish_usage(self, client: WebDavClient, box: CryptoBox, records: dict[str, dict], present_keys: set[str]) -> dict:
        machine_id, usage = self.machine_id, self._state["usage"]
        pointer, pointer_etag = self._usage_pointer(client, box, machine_id)
        current_hashes = {key: content_hash(record) for key, record in records.items()}
        published = usage.get("published") if isinstance(usage.get("published"), dict) else {}
        if pointer is None:
            checkpoint = {"version": 1, "machineId": machine_id, "sequence": 0, "records": [{"key": key, "record": records[key]} for key in sorted(records)]}
            checkpoint_id, checkpoint_bytes = self._put_usage_payload(client, box, "checkpoints", machine_id, checkpoint)
            pointer = {"version": 1, "machineId": machine_id, "sequence": 0, "headChunkId": None, "checkpointId": checkpoint_id, "checkpointSequence": 0, "checkpointBytes": checkpoint_bytes, "tailChunks": 0, "tailBytes": 0, "updatedAt": _timestamp()}
            pointer_etag = self._write_usage_pointer(client, box, pointer, None)
            pointer_etag = self._verify_usage_pointer(client, box, pointer)
            usage.update({"published": current_hashes, "localPointerEtag": pointer_etag, "sequence": 0})
            self._save_state()
            return {"uploaded": len(records), "deleted": 0, "checkpoint": True}
        operations = [
            {"action": "upsert", "key": key, "record": records[key]} for key in sorted(records) if published.get(key) != current_hashes[key]
        ] + [{"action": "delete", "key": key} for key in sorted(set(published) - present_keys)]
        if not operations:
            usage.update({"localPointerEtag": pointer_etag, "sequence": pointer["sequence"]})
            return {"uploaded": 0, "deleted": 0, "checkpoint": False}
        batches, batch, batch_size = [], [], 0
        for operation in operations:
            operation_size = len(canonical_json(operation))
            if batch and (len(batch) >= 500 or batch_size + operation_size > USAGE_CHUNK_MAX_BYTES):
                batches.append(batch)
                batch, batch_size = [], 0
            batch.append(operation)
            batch_size += operation_size
        if batch:
            batches.append(batch)
        parent, sequence, tail_bytes = pointer.get("headChunkId"), pointer["sequence"], pointer.get("tailBytes", 0)
        for batch in batches:
            sequence += 1
            chunk = {"version": 1, "machineId": machine_id, "sequence": sequence, "parentChunkId": parent, "operations": batch}
            parent, size = self._put_usage_payload(client, box, "chunks", machine_id, chunk)
            tail_bytes += size
        updated = pointer | {"sequence": sequence, "headChunkId": parent, "tailChunks": pointer.get("tailChunks", 0) + len(batches), "tailBytes": tail_bytes, "updatedAt": _timestamp()}
        pointer_etag = self._write_usage_pointer(client, box, updated, pointer_etag)
        pointer_etag = self._verify_usage_pointer(client, box, updated)
        usage.update({"published": current_hashes, "localPointerEtag": pointer_etag, "sequence": sequence})
        self._save_state()
        if updated["tailChunks"] >= USAGE_CHECKPOINT_MIN_CHUNKS:
            pointer_etag, updated = self._compact_usage_stream(client, box, updated, pointer_etag, records)
            usage.update({"localPointerEtag": pointer_etag, "sequence": updated["sequence"]})
            self._save_state()
        return {"uploaded": sum(operation["action"] == "upsert" for operation in operations), "deleted": sum(operation["action"] == "delete" for operation in operations), "checkpoint": updated.get("headChunkId") is None}

    def _compact_usage_stream(self, client: WebDavClient, box: CryptoBox, pointer: dict, pointer_etag: str, records: dict[str, dict]) -> tuple[str, dict]:
        machine_id = self.machine_id
        checkpoint = {"version": 1, "machineId": machine_id, "sequence": pointer["sequence"], "records": [{"key": key, "record": records[key]} for key in sorted(records)]}
        checkpoint_id, checkpoint_bytes = self._usage_payload_bytes(checkpoint)
        if len(checkpoint_bytes) > 0.75 * (pointer.get("checkpointBytes", 0) + pointer.get("tailBytes", 0)):
            return pointer_etag, pointer
        old_checkpoint = pointer.get("checkpointId")
        old_chunks = self._encrypted_payloads(client, f"usage/chunks/{machine_id}")
        checkpoint_id, checkpoint_size = self._put_usage_payload(client, box, "checkpoints", machine_id, checkpoint)
        compacted = pointer | {"checkpointId": checkpoint_id, "checkpointSequence": pointer["sequence"], "checkpointBytes": checkpoint_size, "headChunkId": None, "tailChunks": 0, "tailBytes": 0, "updatedAt": _timestamp()}
        pointer_etag = self._write_usage_pointer(client, box, compacted, pointer_etag)
        verified, _ = self._usage_pointer(client, box, machine_id)
        if verified.get("checkpointId") != checkpoint_id or verified.get("headChunkId") is not None:
            raise CloudError("Usage checkpoint verification failed", 409)
        try:
            authoritative, _ = self._usage_pointer(client, box, machine_id)
            if authoritative.get("checkpointId") == checkpoint_id and authoritative.get("headChunkId") is None:
                for payload_id in old_chunks:
                    client.delete(self._usage_payload_path("chunks", machine_id, payload_id))
                if old_checkpoint and old_checkpoint != checkpoint_id:
                    client.delete(self._usage_payload_path("checkpoints", machine_id, old_checkpoint))
        except CloudError:
            pass
        return pointer_etag, compacted

    def _download_usage_payload(self, client: WebDavClient, box: CryptoBox, kind: str, machine_id: str, payload_id: str) -> dict:
        encrypted, _ = client.get(self._usage_payload_path(kind, machine_id, payload_id))
        return self._decode_usage_payload(box, f"usage-{kind}:{machine_id}:{payload_id}", encrypted, payload_id)

    def _fetch_usage(self, client: WebDavClient, box: CryptoBox) -> dict:
        remote = self._state["usage"].setdefault("remote", {})
        changed, downloaded, conflicts = 0, 0, []
        for item in client.list_details("usage/machines"):
            if not item["name"].endswith(".enc"):
                continue
            machine_id = item["name"][:-4]
            if machine_id == self.machine_id:
                continue
            cached = remote.get(machine_id) if isinstance(remote.get(machine_id), dict) else {}
            if item.get("etag") and cached.get("pointerEtag") == item["etag"]:
                continue
            encrypted, etag = client.get(self._usage_pointer_path(machine_id))
            pointer = self._parse_usage_pointer(box, machine_id, encrypted)
            operations, checkpoint_origin = [], None
            checkpoint_changed = cached.get("checkpointId") != pointer.get("checkpointId")
            if checkpoint_changed:
                checkpoint = self._download_usage_payload(client, box, "checkpoints", machine_id, pointer["checkpointId"])
                operations.extend({"action": "upsert", **record} for record in checkpoint.get("records", []))
                checkpoint_origin = machine_id
                stop = None
                downloaded += 1
            else:
                stop = cached.get("headChunkId")
            chunks, chunk_id = [], pointer.get("headChunkId")
            while chunk_id and chunk_id != stop:
                chunk = self._download_usage_payload(client, box, "chunks", machine_id, chunk_id)
                chunks.append(chunk)
                chunk_id = chunk.get("parentChunkId")
                downloaded += 1
            if chunk_id != stop and not checkpoint_changed:
                checkpoint = self._download_usage_payload(client, box, "checkpoints", machine_id, pointer["checkpointId"])
                operations = [{"action": "upsert", **record} for record in checkpoint.get("records", [])]
                checkpoint_origin = machine_id
                chunks, chunk_id = [], pointer.get("headChunkId")
                while chunk_id:
                    chunk = self._download_usage_payload(client, box, "chunks", machine_id, chunk_id)
                    chunks.append(chunk)
                    chunk_id = chunk.get("parentChunkId")
                    downloaded += 1
            for chunk in reversed(chunks):
                operations.extend(chunk.get("operations") or [])
            if operations or checkpoint_origin:
                conflicts.extend(self._usage_data.apply(operations, checkpoint_origin, machine_id))
            remote[machine_id] = {"pointerEtag": etag, "checkpointId": pointer.get("checkpointId"), "headChunkId": pointer.get("headChunkId"), "sequence": pointer["sequence"]}
            changed += 1
            self._save_state()
        return {"machinesChanged": changed, "payloadsDownloaded": downloaded, "conflicts": len(conflicts)}

    @_serialized_cloud_operation
    def sync_usage_data(self) -> dict:
        if self._usage_data is None:
            return {"skipped": True}
        self._require_conditional_writes()
        client, box = self._connection(True)
        for path in ("usage/machines", f"usage/chunks/{self.machine_id}", f"usage/checkpoints/{self.machine_id}"):
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
        self.fetch()
        return True

    def _auto_usage_sync_if_due(self, now: float) -> bool:
        if self._usage_data is None or not self.config()["webdav"].get("usageDataAutoSync", True) or now < self._next_usage_sync_at:
            return False
        usage = self._state["usage"]
        try:
            self.sync_usage_data()
        except Exception as exc:
            attempt = int((usage.get("failure") or {}).get("attempt") or 0) + 1
            usage["failure"] = {"message": str(exc), "attempt": attempt, "failedAt": _timestamp()}
            self._next_usage_sync_at = now + (USAGE_SYNC_RETRY_SECONDS[attempt - 1] if attempt <= len(USAGE_SYNC_RETRY_SECONDS) else USAGE_SYNC_INTERVAL_SECONDS)
            if attempt > len(USAGE_SYNC_RETRY_SECONDS):
                usage["failure"]["attempt"] = 0
            self._save_state()
            return False
        self._next_usage_sync_at = now + USAGE_SYNC_INTERVAL_SECONDS
        return True

    @_serialized_cloud_operation
    def maintenance_tick(self, now: float | None = None) -> dict:
        now = time.monotonic() if now is None else now
        config = self.config()
        if not config["webdav"].get("enabled") or self._state.get("pendingAccountOperation"):
            return {"pushed": False, "fetched": False, "usageSynced": False}
        if config["webdav"].get("skillsAutoUpload", True):
            self._observe_skill_content(now)
        pushed = False
        for name in sorted(self._pending_skill_pushes):
            pending = self._pending_skill_pushes.get(name)
            if not pending or not config["webdav"].get("skillsAutoUpload", True) or name in self._auto_push_failures or now < pending["nextAttemptAt"]:
                continue
            before_hash = (self._observed_skill_hashes or {}).get(name)
            try:
                self.upload_skills({name})
                pushed = True
            except Exception as exc:
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

    def new_placeholder_account_key(self) -> str:
        return self._connection()[1].placeholder_account_key()

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
        key_type = state.get("keyType", "identity")
        if (
            revision_data.get("version") != 1 or revision_data.get("accountKey") != key or len(auth_data) != revision_data.get("authSize") or hashlib.sha256(auth_data).hexdigest() != revision_data.get("authSha256")
            or revision_data.get("keyType", "identity") != key_type or (hashlib.sha256(account_id.encode()).hexdigest() if account_id else None) != revision_data.get("accountIdHash")
            or key_type not in {"identity", "opaque"} or (key_type == "identity" and (not account_id or box.account_key(account_id) != key))
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

    def release_account(self, key: str, auth_data: bytes, identity: dict, label: str, ready: bool = True, key_type: str | None = None) -> dict:
        self._require_conditional_writes()
        client, box = self._connection()
        key_type = key_type or ("identity" if identity.get("accountId") and box.account_key(identity["accountId"]) == key else "opaque")
        if key_type not in {"identity", "opaque"} or key_type == "identity" and (not identity.get("accountId") or box.account_key(identity["accountId"]) != key):
            raise CloudError("Account release identity does not match the local binding", 409)
        try:
            state, etag = self.account_state(key)
        except CloudError as exc:
            if "HTTP 404" not in str(exc):
                raise
            state, etag = None, None
        revision = hashlib.sha256(auth_data).hexdigest()
        if state is not None:
            if state.get("revisionId") == revision and state.get("accountId") == identity.get("accountId") and state.get("ready", True) == ready:
                if self._download_account_revision_with(client, box, state) != auth_data:
                    raise CloudError("Existing cloud account payload does not match the local account", 409)
                self._cache_remote_account(state, etag)
                return {**state, "etag": etag, "changed": False}
            raise CloudError("This account already exists in the cloud", 409)
        client.ensure_directories("accounts/states")
        client.ensure_directories(self._account_paths(key)[1])
        existing_revisions = self._encrypted_payloads(client, self._account_paths(key)[1])
        self._upload_revision(client, box, key, auth_data, identity, key_type)
        state = {"version": 1, "accountKey": key, "keyType": key_type, "accountId": identity.get("accountId"), "label": label, "email": identity.get("email"), "ready": ready, "revisionId": revision, "boundMachineId": None, "updatedAt": _timestamp()}
        new_etag = client.put(self._account_paths(key)[0], box.encrypt(f"account-state:{key}", json.dumps(state).encode()), create=True)
        verified, _ = self.account_state(key)
        if verified.get("accountId") != identity.get("accountId") or verified.get("ready", True) != ready or verified.get("revisionId") != revision:
            raise CloudError("Account release verification failed", 409)
        self._cleanup_account_revisions(client, box, key, existing_revisions)
        self._cache_remote_account(state, new_etag)
        return {**state, "etag": new_etag}
