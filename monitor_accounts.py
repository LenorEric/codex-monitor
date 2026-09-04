#!/usr/bin/env python3

import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
import uuid
import re
import tomllib
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

import tomlkit

from monitor_common import UNKNOWN_EVENT_ACCOUNT_ID, UNKNOWN_EVENT_ACCOUNT_LABEL, auth_account_id, jwt_payload, parse_timestamp


LATEST_SESSION_PROVIDER_UPDATE_LIMIT = 50
SESSION_PROVIDER_PATTERN = re.compile(rb'(?P<prefix>(?:^|[,{])\s*"(?:model_provider_id|model_provider)"\s*:\s*)"(?:\\.|[^"\\])*"')
SESSION_REFRESH_TIME_PATTERN = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")


def default_session_refresh(enabled: bool = False) -> dict:
    return {"fiveHour": {"enabled": bool(enabled), "windowsUtc": []}, "sevenDay": {"enabled": bool(enabled)}}


def session_refresh_time_minutes(value: str, allow_end_of_day: bool = False) -> int:
    if allow_end_of_day and value == "24:00":
        return 24 * 60
    if not isinstance(value, str) or SESSION_REFRESH_TIME_PATTERN.fullmatch(value) is None:
        raise AccountError("Refresh window times must use HH:MM in the range 00:00 through 24:00")
    hour, minute = (int(part) for part in value.split(":"))
    return hour * 60 + minute


def session_refresh_time_text(minutes: int) -> str:
    return "24:00" if minutes == 24 * 60 else f"{minutes // 60:02d}:{minutes % 60:02d}"


def normalize_session_refresh_windows(windows: object) -> list[dict]:
    if not isinstance(windows, list):
        raise AccountError("5h refresh windows must be a list")
    normalized = []
    for window in windows:
        if not isinstance(window, dict):
            raise AccountError("Each 5h refresh window must contain start and end times")
        start, end = session_refresh_time_minutes(window.get("start")), session_refresh_time_minutes(window.get("end"), True)
        if start >= end:
            raise AccountError("Saved UTC refresh windows must not be empty or cross midnight")
        normalized.append((start, end))
    merged = []
    for start, end in sorted(normalized):
        if merged and start <= merged[-1][1]:
            merged[-1] = merged[-1][0], max(merged[-1][1], end)
        else:
            merged.append((start, end))
    return [{"start": session_refresh_time_text(start), "end": session_refresh_time_text(end)} for start, end in merged]


def normalize_session_refresh(value: object, legacy_enabled: bool = False) -> dict:
    if value is None:
        return default_session_refresh(legacy_enabled)
    if not isinstance(value, dict):
        raise AccountError("Session refresh configuration must be an object")
    five_hour, seven_day = value.get("fiveHour"), value.get("sevenDay")
    if not isinstance(five_hour, dict) or not isinstance(seven_day, dict):
        raise AccountError("Session refresh configuration must contain fiveHour and sevenDay objects")
    if type(five_hour.get("enabled")) is not bool or type(seven_day.get("enabled")) is not bool:
        raise AccountError("Session refresh enabled values must be booleans")
    return {
        "fiveHour": {"enabled": five_hour["enabled"], "windowsUtc": normalize_session_refresh_windows(five_hour.get("windowsUtc"))},
        "sevenDay": {"enabled": seven_day["enabled"]},
    }


def is_api_auth(auth: dict) -> bool:
    return isinstance(auth, dict) and isinstance(auth.get("OPENAI_API_KEY"), str) and bool(auth["OPENAI_API_KEY"].strip())


def auth_account_type(auth: dict) -> str:
    return "api" if is_api_auth(auth) else "account"


def is_complete_auth(auth: dict) -> bool:
    if is_api_auth(auth):
        return True
    tokens = auth.get("tokens") if isinstance(auth.get("tokens"), dict) else {}
    return bool(auth_account_id(auth) and all(isinstance(tokens.get(name), str) and tokens[name].strip() for name in ("access_token", "refresh_token")))


class AccountError(RuntimeError):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.chmod(temp_name, 0o600)
        except OSError:
            pass
        for attempt in range(5):
            try:
                os.replace(temp_name, path)
                break
            except PermissionError:
                if attempt == 4 or os.name != "nt":
                    raise
                time.sleep(0.02 * (attempt + 1))
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def atomic_write_json(path: Path, data: dict) -> None:
    atomic_write_bytes(path, (json.dumps(data, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))


def _make_writable_and_retry(function, path, exc_info) -> None:
    try:
        os.chmod(path, 0o700)
    except OSError:
        pass
    function(path)


def remove_directory(path: Path) -> None:
    path = Path(path)
    if path.is_symlink() or not path.is_dir():
        path.unlink(missing_ok=True)
        return
    for attempt in range(5):
        try:
            shutil.rmtree(path, onerror=_make_writable_and_retry)
            return
        except FileNotFoundError:
            return
        except OSError:
            if attempt == 4:
                raise
            time.sleep(0.05 * (attempt + 1))


def parse_auth_bytes(data: bytes) -> dict:
    try:
        auth = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AccountError("auth.json is not valid UTF-8 JSON") from exc
    if not isinstance(auth, dict):
        raise AccountError("auth.json must contain a JSON object")
    return auth


def auth_identity(auth: dict) -> dict:
    if is_api_auth(auth):
        key = auth["OPENAI_API_KEY"].strip()
        return {"accountId": None, "idTokenHash": hashlib.sha256(key.encode()).hexdigest(), "email": None, "accountType": "api"}
    tokens = auth.get("tokens") if isinstance(auth.get("tokens"), dict) else {}
    id_token = tokens.get("id_token") if isinstance(tokens.get("id_token"), str) and tokens["id_token"] else None
    claims = jwt_payload(id_token or tokens.get("access_token") or "")
    auth_claims = claims.get("https://api.openai.com/auth") if isinstance(claims.get("https://api.openai.com/auth"), dict) else {}
    account_id = tokens.get("account_id") or claims.get("chatgpt_account_id") or auth_claims.get("chatgpt_account_id")
    email = claims.get("email") or auth_claims.get("email")
    return {
        "accountId": str(account_id) if account_id else None,
        "idTokenHash": hashlib.sha256(id_token.encode()).hexdigest() if id_token else None,
        "email": str(email) if email else None,
        "accountType": "account",
    }


def api_identity_id(identity: dict | None) -> str | None:
    id_token_hash = (identity or {}).get("idTokenHash")
    if (identity or {}).get("accountType") != "api" or not id_token_hash:
        return None
    return hashlib.sha256(f"codex-monitor-api-account-v1:{id_token_hash}".encode()).hexdigest()


def same_auth_identity(left: dict | None, right: dict | None) -> bool:
    left, right = left if isinstance(left, dict) else {}, right if isinstance(right, dict) else {}
    return bool(left.get("accountId") and right.get("accountId") and left["accountId"] == right["accountId"] or left.get("idTokenHash") and right.get("idTokenHash") and left["idTokenHash"] == right["idTokenHash"])


def auth_fingerprint(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def migrate_account_vault(legacy_root: Path, root: Path) -> bool:
    legacy_root, root = Path(legacy_root), Path(root)
    if not legacy_root.exists() or root.exists() or legacy_root.resolve() == root.resolve():
        return False
    root.parent.mkdir(parents=True, exist_ok=True)
    temp_root = root.parent / f".{root.name}.{uuid.uuid4().hex}.tmp"
    temp_root.mkdir()
    try:
        for source in legacy_root.rglob("*"):
            target = temp_root / source.relative_to(legacy_root)
            if source.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                atomic_write_bytes(target, source.read_bytes())
        for attempt in range(5):
            try:
                os.replace(temp_root, root)
                break
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.02 * (attempt + 1))
        shutil.rmtree(legacy_root)
        return True
    except Exception:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise


class AccountManager:
    def __init__(self, auth_path: Path, root: Path | None = None, legacy_root: Path | None = None):
        self.auth_path = Path(auth_path)
        self.config_path = self.auth_path.parent / "config.toml"
        self.root = Path(root) if root is not None else self.auth_path.parent / "usage-monitor-accounts"
        self.manifest_path = self.root / "accounts.json"
        self.lock = threading.RLock()
        self._config_fingerprint = None
        self._ignored_config_fingerprint = None
        self._config_snapshot = self.config_path.read_text(encoding="utf-8") if self.config_path.exists() else ""
        self.error = None
        self.message = None
        self.cloud = None
        with self.lock:
            if legacy_root is not None:
                migrate_account_vault(legacy_root, self.root)
            self.manifest = self._load_or_bootstrap()
            self.reconcile_pending_login()

    def _account_path(self, account_id: str) -> Path:
        return self.root / account_id / "auth.json"

    def _new_record(self, account_id: str, label: str, ready: bool, data: bytes | None = None) -> dict:
        auth = parse_auth_bytes(data) if data is not None else {}
        identity = auth_identity(auth) if data is not None else {"accountId": None, "idTokenHash": None, "email": None, "accountType": "account"}
        now = timestamp()
        return {
            "id": account_id,
            "label": label,
            "ready": ready,
            "createdAt": now,
            "updatedAt": now,
            "identity": identity,
            "accountType": auth_account_type(auth),
            "sessionRefresh": default_session_refresh(),
            "configHeader": {},
            "fingerprint": auth_fingerprint(data) if data is not None else None,
            "cloud": {"accountKey": None, "keyType": None},
        }

    def _validate_label(self, label: str) -> str:
        label = str(label or "").strip()
        if not label:
            raise AccountError("Account name is required")
        if len(label) > 80:
            raise AccountError("Account name must be 80 characters or fewer")
        return label

    def _load_or_bootstrap(self) -> dict:
        if self.manifest_path.exists():
            try:
                manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise AccountError(f"Cannot read account manifest: {exc}", 500) from exc
            if not isinstance(manifest, dict) or manifest.get("version") not in {1, 2, 3} or not isinstance(manifest.get("accounts"), list):
                raise AccountError("Unsupported or invalid account manifest", 500)
            changed = manifest.get("version") in {1, 2}
            if manifest.get("version") == 1:
                manifest["version"] = 2
                for account in manifest["accounts"]:
                    account["cloud"] = {"accountKey": None, "keyType": None}
            if manifest.get("version") == 2:
                manifest["version"] = 3
            for account in manifest["accounts"]:
                session_refresh = normalize_session_refresh(account.get("sessionRefresh"), bool(account.get("sessionRefreshEnabled")))
                if account.get("sessionRefresh") != session_refresh or "sessionRefreshEnabled" in account:
                    account["sessionRefresh"] = session_refresh
                    account.pop("sessionRefreshEnabled", None)
                    changed = True
                credential_path = self._account_path(str(account.get("id") or ""))
                if not account.get("ready") or not credential_path.exists():
                    continue
                data = credential_path.read_bytes()
                identity = auth_identity(parse_auth_bytes(data))
                if (identity.get("accountId") or identity.get("idTokenHash")) and account.get("identity") != identity:
                    account["identity"] = identity
                    changed = True
                if account.get("fingerprint") != auth_fingerprint(data):
                    account["fingerprint"] = auth_fingerprint(data)
                    changed = True
                account.setdefault("accountType", (account.get("identity") or {}).get("accountType", "account"))
                account.setdefault("configHeader", {})
            normalized_activation_history = self._normalized_activation_history(manifest)
            if normalized_activation_history != manifest.get("activationHistory"):
                manifest["activationHistory"] = normalized_activation_history
                changed = True
            if "commonAccountHeaderToml" not in manifest:
                manifest["commonAccountHeaderToml"] = ""
                changed = True
            if changed:
                atomic_write_json(self.manifest_path, manifest)
            return manifest

        live = self.auth_path.read_bytes() if self.auth_path.exists() else None
        ready = False
        if live is not None:
            auth = parse_auth_bytes(live)
            ready = is_complete_auth(auth)
        if ready:
            atomic_write_bytes(self._account_path("ppl-pro"), live)
        manifest = {
            "version": 3,
            "activeAccountId": "ppl-pro" if live is not None else None,
            "commonAccountHeaderToml": "",
            "accounts": [self._new_record("ppl-pro", "Current account", ready, live if ready else None)] if live is not None else [],
            "activationHistory": [{"checkedAt": timestamp(), "accountSlotId": "ppl-pro", "accountLabel": "Current account"}] if live is not None else [],
        }
        atomic_write_json(self.manifest_path, manifest)
        return manifest

    @staticmethod
    def _toml_path(value: dict) -> tuple[str, ...]:
        path = []
        while isinstance(value, dict) and len(value) == 1:
            key, value = next(iter(value.items()))
            path.append(key)
            if isinstance(value, list) and value and isinstance(value[0], dict):
                value = value[0]
        return tuple(path)

    @classmethod
    def _toml_key_path(cls, statement: str) -> tuple[str, ...]:
        quoted = None
        escaped = False
        for index, character in enumerate(statement):
            if quoted:
                if quoted == '"' and character == "\\" and not escaped:
                    escaped = True
                    continue
                if character == quoted and not escaped:
                    quoted = None
                escaped = False
            elif character in "'\"":
                quoted = character
            elif character == "=":
                return cls._toml_path(tomllib.loads(f"{statement[:index]}= 0"))
        return ()

    @classmethod
    def _toml_table_path(cls, declaration: str) -> tuple[str, ...]:
        return cls._toml_path(tomllib.loads(f"{declaration}\n"))

    @staticmethod
    def _toml_leaf_paths(value, prefix: tuple[str, ...] = ()) -> set[tuple[str, ...]]:
        if isinstance(value, dict):
            paths = set()
            for key, child in value.items():
                paths.update(AccountManager._toml_leaf_paths(child, prefix + (key,)))
            return paths or {prefix}
        if isinstance(value, list) and value and all(isinstance(child, dict) for child in value):
            paths = set()
            for child in value:
                paths.update(AccountManager._toml_leaf_paths(child, prefix))
            return paths or {prefix}
        return {prefix}

    @classmethod
    def _toml_layout(cls, text: str) -> tuple[list[tuple[tuple[str, ...], list[str]]], list[tuple[tuple[str, ...], set[tuple[str, ...]], list[str]]]]:
        lines = text.splitlines()
        table_starts = [index for index, line in enumerate(lines) if re.match(r"^\s*\[\[?.+?\]\]?\s*(?:#.*)?$", line)]
        preamble_end = table_starts[0] if table_starts else len(lines)
        assignments, pending, index = [], [], 0
        while index < preamble_end:
            if not lines[index].strip() or lines[index].lstrip().startswith("#"):
                pending.append(lines[index])
                index += 1
                continue
            statement = []
            while index < preamble_end:
                statement.append(lines[index])
                index += 1
                try:
                    tomllib.loads("\n".join(statement))
                    break
                except tomllib.TOMLDecodeError:
                    continue
            assignments.append((cls._toml_key_path("\n".join(statement)), pending + statement))
            pending = []
        if pending:
            assignments.append(((), pending))
        tables = []
        for position, start in enumerate(table_starts):
            block = lines[start:table_starts[position + 1] if position + 1 < len(table_starts) else len(lines)]
            path = cls._toml_table_path(block[0])
            try:
                leaves = cls._toml_leaf_paths(tomllib.loads("\n".join(block)))
            except tomllib.TOMLDecodeError:
                leaves = {path}
            tables.append((path, leaves, block))
        return assignments, tables

    @classmethod
    def _toml_ownership(cls, text: str) -> tuple[set[tuple[str, ...]], set[tuple[str, ...]], set[tuple[str, ...]]]:
        assignments, tables = cls._toml_layout(text)
        return {path for path, _ in assignments if path}, {path for path, _, _ in tables}, cls._toml_leaf_paths(tomllib.loads(text)) if text.strip() else set()

    def _config_editor_parts(self, header_hint: str = "") -> tuple[str, str]:
        if not self.config_path.exists():
            return "", ""
        text = self.config_path.read_text(encoding="utf-8")
        try:
            tomllib.loads(text)
            known_keys, known_tables, _ = self._toml_ownership(header_hint) if header_hint.strip() else (set(), set(), set())
        except tomllib.TOMLDecodeError:
            return text, ""
        common, header = [], []
        assignments, tables = self._toml_layout(text)
        for path, lines in assignments:
            (header if path and path in known_keys else common).extend(lines)
        for path, _, lines in tables:
            owned = any(path[:len(table)] == table for table in known_tables)
            (header if owned else common).extend(lines)
        return self._normalize_toml_spacing(common), self._normalize_toml_spacing(header)

    @staticmethod
    def _normalize_toml_spacing(lines: list[str] | str) -> str:
        source = lines.splitlines() if isinstance(lines, str) else lines
        output, statement = [], []
        table = ""
        for line in source:
            if statement:
                statement.append(line)
                try:
                    tomllib.loads((table + "\n" if table else "") + "\n".join(statement))
                except tomllib.TOMLDecodeError:
                    continue
                output.extend(statement)
                statement = []
                continue
            if not line.strip():
                continue
            if line.lstrip().startswith("#"):
                output.append(line)
                continue
            if re.match(r"^\s*\[\[?.+?\]\]?\s*(?:#.*)?$", line):
                if output and output[-1]:
                    output.append("")
                output.append(line)
                table = line
                continue
            statement.append(line)
            try:
                tomllib.loads((table + "\n" if table else "") + line)
            except tomllib.TOMLDecodeError:
                continue
            output.append(line)
            statement = []
        output.extend(statement)
        return "\n".join(output).rstrip() + ("\n" if output else "")

    def _compose_toml_parts(self, common_toml: str, header_toml: str) -> str:
        tomllib.loads(common_toml)
        tomllib.loads(header_toml)
        header, common = tomlkit.parse(header_toml), tomlkit.parse(common_toml)

        def merge_body(base, body):
            for key, value in body.items():
                if key not in base:
                    base[key] = value
                elif isinstance(base[key], Mapping) and isinstance(value, Mapping):
                    merge_body(base[key], value)
            return base

        return tomlkit.dumps(merge_body(header, common)).rstrip() + "\n"

    @staticmethod
    def _format_toml(text: str) -> str:
        formatted = tomlkit.dumps(tomlkit.parse(text)).replace("\r\n", "\n")
        return AccountManager._normalize_toml_spacing(formatted)

    def _validate_config_parts(self, common_toml: str, header_tomls: list[str] | tuple[str, ...] = ()) -> None:
        try:
            tomllib.loads(common_toml)
        except tomllib.TOMLDecodeError as exc:
            raise AccountError(f"Common configuration is not valid TOML: {exc}", 409) from exc
        for header_toml in header_tomls:
            try:
                tomllib.loads(header_toml)
            except tomllib.TOMLDecodeError as exc:
                raise AccountError(f"Configuration is not valid TOML: {exc}", 409) from exc
            try:
                tomllib.loads(self._compose_toml_parts(common_toml, header_toml))
            except tomllib.TOMLDecodeError as exc:
                raise AccountError(f"Configuration contains conflicting or duplicate entries: {exc}", 409) from exc

    def _stored_header_configs(self) -> list[str]:
        return [
            account.get("configHeaderToml", "")
            for account in self.manifest.get("accounts", [])
            if isinstance(account.get("configHeaderToml", ""), str) and account.get("configHeaderToml", "").strip()
        ]

    def _header_for(self, account: dict | None) -> str:
        return (account or {}).get("configHeaderToml", "") if account is not None and account.get("accountType") == "api" else self.manifest.get("commonAccountHeaderToml", "")

    def config_editor_payload(self) -> dict:
        with self.lock:
            active = self._find(self.manifest.get("activeAccountId"))
            is_api = bool(active and active.get("accountType") == "api")
            stored_header = self._header_for(active)
            common, detected_header = self._config_editor_parts(stored_header)
            header = stored_header or detected_header
            return {"commonToml": common, "headerToml": header, "commonAccountHeaderToml": self.manifest.get("commonAccountHeaderToml", ""), "activeAccountId": self.manifest.get("activeAccountId"), "apiAccount": is_api}

    def _apply_config_for(self, account: dict | None, common_toml: str | None = None) -> None:
        if not self.config_path.exists():
            return
        stored_header = self._header_for(account)
        if common_toml is None:
            common_toml, _ = self._config_editor_parts(stored_header)
        try:
            tomllib.loads(stored_header)
        except tomllib.TOMLDecodeError as exc:
            raise AccountError(f"Header config is not valid TOML: {exc}", 409) from exc
        self._write_config(self._compose_toml_parts(common_toml, stored_header))

    @staticmethod
    def _model_provider(config_toml: str) -> str:
        provider = tomllib.loads(config_toml).get("model_provider", "openai")
        if not isinstance(provider, str):
            raise AccountError("model_provider must be a string", 409)
        return provider

    def _rewrite_recent_session_model_providers(self, merged_toml: str) -> dict[Path, bytes]:
        provider = self._model_provider(merged_toml)
        sessions_dir = self.auth_path.parent / "sessions"
        if not sessions_dir.is_dir():
            return {}
        originals = {}
        replacement = json.dumps(provider, ensure_ascii=False).encode("utf-8")
        try:
            for path in sorted(sessions_dir.rglob("*.jsonl"), key=lambda item: item.stat().st_mtime_ns, reverse=True)[:LATEST_SESSION_PROVIDER_UPDATE_LIMIT]:
                data = path.read_bytes()
                updated = SESSION_PROVIDER_PATTERN.sub(lambda match: match.group("prefix") + replacement, data)
                if updated != data:
                    originals[path] = data
                    atomic_write_bytes(path, updated)
        except Exception:
            for path, data in originals.items():
                atomic_write_bytes(path, data)
            raise
        return originals

    @staticmethod
    def _restore_session_files(originals: dict[Path, bytes]) -> None:
        for path, data in originals.items():
            atomic_write_bytes(path, data)

    def _save_manifest(self) -> None:
        atomic_write_json(self.manifest_path, self.manifest)

    def _normalized_activation_history(self, manifest: dict) -> list[dict]:
        labels = {str(account.get("id")): str(account.get("label") or "Unknown") for account in manifest.get("accounts", []) if account.get("id")}
        rows = []
        for row in manifest.get("activationHistory") or []:
            if not isinstance(row, dict) or parse_timestamp(row.get("checkedAt")) is None or not row.get("accountSlotId"):
                continue
            account_id = str(row["accountSlotId"])
            rows.append({"checkedAt": row["checkedAt"], "accountSlotId": account_id, "accountLabel": labels.get(account_id, str(row.get("accountLabel") or "Unknown"))})
        if not rows and manifest.get("activeAccountId"):
            account_id = str(manifest["activeAccountId"])
            rows.append({"checkedAt": timestamp(), "accountSlotId": account_id, "accountLabel": labels.get(account_id, "Unknown")})
        normalized = []
        for row in sorted(rows, key=lambda item: parse_timestamp(item["checkedAt"]) or 0):
            if normalized and normalized[-1]["accountSlotId"] == row["accountSlotId"]:
                normalized[-1]["accountLabel"] = row["accountLabel"]
            else:
                normalized.append(row)
        return normalized

    def _record_activation(self, account_id: str, checked_at: str | None = None) -> None:
        account = self._find(account_id)
        if account is None:
            return
        row = {"checkedAt": checked_at or timestamp(), "accountSlotId": account["id"], "accountLabel": account["label"]}
        history = self.manifest.setdefault("activationHistory", [])
        if history and history[-1].get("accountSlotId") == account["id"]:
            history[-1]["accountLabel"] = account["label"]
        else:
            history.append(row)

    def attribution_timeline(self) -> list[dict]:
        with self.lock:
            return self._normalized_activation_history(self.manifest)

    def _config_file_fingerprint(self) -> str | None:
        try:
            return hashlib.sha256(self.config_path.read_bytes()).hexdigest()
        except OSError:
            return None

    def _write_config(self, text: str) -> None:
        atomic_write_bytes(self.config_path, text.encode("utf-8"))
        self._config_snapshot = text
        self._ignored_config_fingerprint = self._config_file_fingerprint()

    def sync_config_from_disk(self) -> bool:
        with self.lock:
            fingerprint = self._config_file_fingerprint()
            if fingerprint is None or fingerprint == self._config_fingerprint:
                return False
            self._config_fingerprint = fingerprint
            if fingerprint == self._ignored_config_fingerprint:
                self._ignored_config_fingerprint = None
                self._config_snapshot = self.config_path.read_text(encoding="utf-8")
                return False
            active = self._find(self.manifest.get("activeAccountId"))
            if active is None:
                self._config_snapshot = self.config_path.read_text(encoding="utf-8")
                return False
            text = self.config_path.read_text(encoding="utf-8")
            try:
                tomllib.loads(text)
            except tomllib.TOMLDecodeError:
                return False
            is_api = active.get("accountType") == "api"
            hint = self._header_for(active)
            _, header_toml = self._config_editor_parts(hint)
            self._config_snapshot = text
            normalized = header_toml.strip() + "\n" if header_toml.strip() else ""
            current_header = active.get("configHeaderToml", "") if is_api else self.manifest.get("commonAccountHeaderToml", "")
            if normalized == current_header:
                return False
            if is_api:
                active.update({"configHeaderToml": normalized, "configHeader": {}, "updatedAt": timestamp()})
            else:
                self.manifest["commonAccountHeaderToml"] = normalized
            self._save_manifest()
            return True

    def _find(self, account_id: str) -> dict | None:
        return next((account for account in self.manifest["accounts"] if account.get("id") == account_id), None)

    def _find_identity(self, identity: dict, exclude_id: str | None = None) -> dict | None:
        return next((account for account in self.manifest["accounts"] if account.get("id") != exclude_id and same_auth_identity(account.get("identity"), identity)), None)

    def _find_disallowed_duplicate(self, identity: dict, exclude_id: str | None = None) -> dict | None:
        return self._find_identity(identity, exclude_id) if identity.get("accountType") == "api" else None

    def _find_cloud_key(self, account_key: str) -> dict | None:
        return next((account for account in self.manifest["accounts"] if (account.get("cloud") or {}).get("accountKey") == account_key), None)

    def api_identity_ids(self) -> dict[str, str]:
        with self.lock:
            return {account["id"]: identity_id for account in self.manifest["accounts"] if (identity_id := api_identity_id(account.get("identity"))) is not None}

    def cloud_account_keys(self) -> set[str]:
        with self.lock:
            return {key for account in self.manifest["accounts"] if (key := (account.get("cloud") or {}).get("accountKey"))}

    def _ensure_account_changes_allowed(self) -> None:
        if self.cloud is not None:
            self.cloud.ensure_no_pending_account_operation()

    def active_account(self) -> dict:
        account = self._find(self.manifest.get("activeAccountId"))
        if account is None:
            raise AccountError("The active account no longer exists", 500)
        return account

    def attribution_for_auth(self, auth: dict) -> dict:
        with self.lock:
            identity = auth_identity(auth)
            matches = [account for account in self.manifest["accounts"] if same_auth_identity(account.get("identity"), identity)]
            account = next((account for account in matches if account["id"] == self.manifest.get("activeAccountId")), matches[0] if matches else None)
            if account is None and identity.get("email"):
                matches = [account for account in self.manifest["accounts"] if (account.get("identity") or {}).get("email") == identity["email"]]
                account = matches[0] if len(matches) == 1 else None
            return {
                "accountSlotId": account["id"] if account is not None else UNKNOWN_EVENT_ACCOUNT_ID,
                "accountLabel": account["label"] if account is not None else UNKNOWN_EVENT_ACCOUNT_LABEL,
            }

    def _validate_live_matches_saved(self, account: dict, auth: dict) -> None:
        credential_path = self._account_path(account["id"])
        if not credential_path.exists():
            raise AccountError(f"Cannot verify the current login because saved credentials for {account['label']} are missing", 409)
        saved_auth = parse_auth_bytes(credential_path.read_bytes())
        if is_api_auth(auth):
            saved_identity = auth_identity(saved_auth)
            if not saved_identity.get("idTokenHash"):
                saved_identity = account.get("identity") if isinstance(account.get("identity"), dict) else {}
            if auth_identity(auth) != saved_identity:
                raise AccountError(f"Account change refused: current auth.json does not match saved API account {account['label']}", 409)
            return
        live_tokens = auth.get("tokens") if isinstance(auth.get("tokens"), dict) else {}
        live_identity, saved_identity = auth_identity(auth), auth_identity(saved_auth)
        if not saved_identity.get("accountId") and not saved_identity.get("idTokenHash"):
            saved_identity = account.get("identity") if isinstance(account.get("identity"), dict) else {}
        if live_identity.get("accountId") and saved_identity.get("accountId") and live_identity["accountId"] != saved_identity["accountId"]:
            raise AccountError(f"Account change refused: current auth.json does not match saved account {account['label']} (account_id differs; user changed)", 409)
        if not live_identity.get("accountId") and live_identity.get("idTokenHash") and saved_identity.get("idTokenHash") and live_identity["idTokenHash"] != saved_identity["idTokenHash"]:
            raise AccountError(f"Account change refused: current auth.json does not match saved account {account['label']} (user changed)", 409)
        if not isinstance(live_tokens.get("refresh_token"), str) or not live_tokens["refresh_token"].strip():
            return
        if not live_identity.get("accountId") or not saved_identity.get("accountId"):
            raise AccountError(f"Account change refused: cannot verify {account['label']} because account_id is missing from current or saved auth.json", 409)

    def _snapshot_live_into(self, account: dict, strict_identity: bool = True, require_saved_match: bool = False) -> bool:
        data = self.auth_path.read_bytes() if self.auth_path.exists() else b"{}"
        if not data.strip():
            data = b"{}"
        auth = parse_auth_bytes(data)
        tokens = auth.get("tokens") if isinstance(auth.get("tokens"), dict) else {}
        if require_saved_match:
            self._validate_live_matches_saved(account, auth)
        identity = auth_identity(auth)
        stored_identity = account.get("identity") if isinstance(account.get("identity"), dict) else {}
        if strict_identity and account.get("ready") and (stored_identity.get("accountId") or stored_identity.get("idTokenHash")) and (identity.get("accountId") or identity.get("idTokenHash")) and not same_auth_identity(stored_identity, identity):
            raise AccountError("Live auth.json belongs to a different account; use New account before replacing the current login", 409)
        if account.get("ready") and not is_api_auth(auth) and tokens and (not isinstance(tokens.get("refresh_token"), str) or not tokens["refresh_token"].strip()):
            return False
        if duplicate := self._find_disallowed_duplicate(identity, account["id"]):
            raise AccountError(f"Account change refused: this login is already managed as {duplicate['label']}", 409)
        fingerprint = auth_fingerprint(data)
        if account.get("ready") and account.get("fingerprint") == fingerprint and self._account_path(account["id"]).exists():
            return False
        atomic_write_bytes(self._account_path(account["id"]), data)
        account.update({"ready": True, "updatedAt": timestamp(), "fingerprint": fingerprint})
        if identity.get("accountId") or identity.get("idTokenHash"):
            account.update({"identity": identity, "accountType": identity.get("accountType", "account")})
        self._save_manifest()
        return True

    def reconcile_active_from_live(self) -> tuple[bool, dict | None]:
        with self.lock:
            if self.manifest.get("activeAccountId") is None:
                return False, None
            active = self.active_account()
            if not active.get("ready"):
                return self.reconcile_pending_login(), None
            credential_path = self._account_path(active["id"])
            if not credential_path.exists():
                raise AccountError(f"Saved credentials for {active['label']} are missing", 409)
            saved_data = credential_path.read_bytes()
            saved_auth = parse_auth_bytes(saved_data)
            if not is_complete_auth(saved_auth):
                raise AccountError(f"Saved credentials for {active['label']} are incomplete", 409)
            try:
                live_data = self.auth_path.read_bytes()
                live_auth = parse_auth_bytes(live_data)
            except (OSError, AccountError):
                atomic_write_bytes(self.auth_path, saved_data)
                print(f"External auth.json update was incomplete; restored saved credentials for {active['label']!r}.", flush=True)
                return False, None
            if not is_complete_auth(live_auth):
                atomic_write_bytes(self.auth_path, saved_data)
                print(f"External auth.json update was incomplete; restored saved credentials for {active['label']!r}.", flush=True)
                return False, None
            saved_identity, live_identity = auth_identity(saved_auth), auth_identity(live_auth)
            saved_account_id, live_account_id = auth_account_id(saved_auth), auth_account_id(live_auth)
            same_identity = same_auth_identity(saved_identity, live_identity) if active.get("accountType") == "api" else saved_account_id == live_account_id
            if same_identity:
                fingerprint = auth_fingerprint(live_data)
                if fingerprint == auth_fingerprint(saved_data):
                    return False, None
                atomic_write_bytes(credential_path, live_data)
                active.update({"ready": True, "updatedAt": timestamp(), "identity": live_identity, "fingerprint": fingerprint, "accountType": live_identity.get("accountType", "account")})
                self._save_manifest()
                return True, None
            atomic_write_bytes(self.auth_path, saved_data)
            target = next(
                (account for account in self.manifest["accounts"] if live_account_id and account["id"] != active["id"] and account.get("ready") and (account.get("identity") or {}).get("accountId") == live_account_id and self._account_path(account["id"]).exists()),
                None,
            )
            if target is None:
                print(f"External auth.json update belonged to an unrecorded account; restored saved credentials for {active['label']!r}.", flush=True)
                return False, None
            print(f"External auth.json update matched {target['label']!r}; restored {active['label']!r} and started background credential validation.", flush=True)
            return False, {"id": target["id"], "label": target["label"], "data": live_data, "fingerprint": auth_fingerprint(self._account_path(target["id"]).read_bytes())}

    def sync_active_from_live(self) -> bool:
        return self.reconcile_active_from_live()[0]

    def inactive_ready_credentials(self) -> list[dict]:
        with self.lock:
            active_id = self.manifest.get("activeAccountId")
            credentials = []
            for account in self.manifest["accounts"]:
                credential_path = self._account_path(account["id"])
                if account["id"] == active_id or account.get("accountType") == "api" or not account.get("ready") or not credential_path.exists():
                    continue
                data = credential_path.read_bytes()
                credentials.append({"id": account["id"], "label": account["label"], "data": data, "fingerprint": auth_fingerprint(data)})
            return credentials

    def session_refresh_credentials(self) -> list[dict]:
        with self.lock:
            active_id = self.manifest.get("activeAccountId")
            credentials = []
            for account in self.manifest["accounts"]:
                credential_path = self._account_path(account["id"])
                session_refresh = normalize_session_refresh(account.get("sessionRefresh"))
                if not (session_refresh["fiveHour"]["enabled"] or session_refresh["sevenDay"]["enabled"]) or account.get("accountType") == "api" or not account.get("ready") or not credential_path.exists():
                    continue
                data = self.auth_path.read_bytes() if account["id"] == active_id and self.auth_path.exists() else credential_path.read_bytes()
                credentials.append({"id": account["id"], "label": account["label"], "data": data, "fingerprint": auth_fingerprint(data), "active": account["id"] == active_id, "sessionRefresh": session_refresh})
            return credentials

    def set_session_refresh(self, account_id: str, session_refresh: object) -> dict:
        self._ensure_account_changes_allowed()
        with self.lock:
            account = self._find(str(account_id or ""))
            if account is None:
                raise AccountError("Account not found", 404)
            if account.get("accountType") == "api":
                raise AccountError("Session refresh is available only for normal accounts", 409)
            account["sessionRefresh"] = normalize_session_refresh(session_refresh)
            account["updatedAt"] = timestamp()
            self._save_manifest()
            return self.status()

    def commit_polled_credentials(self, account_id: str, expected_fingerprint: str, data: bytes) -> bool:
        if self.cloud is not None and self.cloud.account_transition_targets(account_id):
            return False
        fingerprint = auth_fingerprint(data)
        if fingerprint == expected_fingerprint:
            return True
        auth = parse_auth_bytes(data)
        with self.lock:
            account = self._find(account_id)
            if account is None:
                return False
            credential_path = self._account_path(account_id)
            if not credential_path.exists() or auth_fingerprint(credential_path.read_bytes()) != expected_fingerprint:
                return False
            identity = auth_identity(auth)
            identity_matches = same_auth_identity(account.get("identity"), identity) if account.get("accountType") == "api" else bool(auth_account_id(auth) and auth_account_id(auth) == (account.get("identity") or {}).get("accountId"))
            if not identity_matches or self._find_disallowed_duplicate(identity, account_id):
                return False
            if account_id == self.manifest.get("activeAccountId"):
                if not self.auth_path.exists() or auth_fingerprint(self.auth_path.read_bytes()) != expected_fingerprint:
                    return False
                atomic_write_bytes(self.auth_path, data)
            atomic_write_bytes(credential_path, data)
            account.update({"updatedAt": timestamp(), "identity": identity, "fingerprint": fingerprint, "accountType": identity.get("accountType", "account")})
            self._save_manifest()
            return True

    def reconcile_pending_login(self) -> bool:
        with self.lock:
            if self.manifest.get("activeAccountId") is None:
                return False
            active = self.active_account()
            if self.cloud is not None and self.cloud.account_transition_targets(active["id"]):
                self.error = "Waiting for the pending cloud account transfer to recover before accepting login credentials"
                return False
            if active.get("ready") or not self.auth_path.exists():
                return False
            try:
                data = self.auth_path.read_bytes()
                auth = parse_auth_bytes(data)
                if not is_complete_auth(auth):
                    self.error = "Waiting for Codex login to create complete auth.json with account_id, access token, and refresh token"
                    self.message = None
                    return False
                identity = auth_identity(auth)
                duplicate = self._find_disallowed_duplicate(identity, active["id"])
                if duplicate:
                    credential_path = self._account_path(duplicate["id"])
                    old_credential = credential_path.read_bytes() if credential_path.exists() else None
                    old_manifest = json.loads(json.dumps(self.manifest))
                    try:
                        atomic_write_bytes(credential_path, data)
                        duplicate.update({"ready": True, "updatedAt": timestamp(), "identity": identity, "fingerprint": auth_fingerprint(data), "accountType": identity.get("accountType", "account")})
                        self.auth_path.unlink()
                        self._save_manifest()
                    except Exception:
                        self.manifest = old_manifest
                        if old_credential is None:
                            credential_path.unlink(missing_ok=True)
                        else:
                            atomic_write_bytes(credential_path, old_credential)
                        if not self.auth_path.exists():
                            atomic_write_bytes(self.auth_path, data)
                        raise
                    self.error = None
                    self.message = f"This login already belongs to {duplicate['label']}. Its saved auth.json was updated, and {active['label']} remains empty for a different login."
                    return False
                common_toml, _ = self._config_editor_parts(self._header_for(active))
                atomic_write_bytes(self._account_path(active["id"]), data)
                active.update({"ready": True, "updatedAt": timestamp(), "identity": identity, "fingerprint": auth_fingerprint(data), "accountType": identity.get("accountType", "account")})
                self._save_manifest()
                self._apply_config_for(active, common_toml)
                self._save_manifest()
                self.error = None
                self.message = None
                return True
            except AccountError as exc:
                self.error = str(exc)
                return False

    def create_account(self, label: str, account_type: str = "account", api_key: str | None = None, external_update_callback=None) -> dict:
        self._ensure_account_changes_allowed()
        label = self._validate_label(label)
        account_type = str(account_type or "account").strip().lower()
        if account_type not in {"account", "api"}:
            raise AccountError("Account type must be account or api")
        if account_type == "api":
            api_key = str(api_key or "").strip()
            if not api_key:
                raise AccountError("API key is required")
            auth_data = json.dumps({"auth_mode": "apikey", "OPENAI_API_KEY": api_key}, indent=2).encode("utf-8") + b"\n"
        else:
            auth_data = None
        with self.lock:
            self.sync_config_from_disk()
            active = self._find(self.manifest.get("activeAccountId"))
            common_toml, _ = self._config_editor_parts(self._header_for(active))
            if active is not None and active.get("ready"):
                _, external_update = self.reconcile_active_from_live()
                if external_update is not None and external_update_callback is not None:
                    external_update_callback(external_update)
            old_live = self.auth_path.read_bytes() if self.auth_path.exists() else None
            old_config = self.config_path.read_bytes() if self.config_path.exists() else None
            old_manifest = json.loads(json.dumps(self.manifest))
            account_id = uuid.uuid4().hex
            account = self._new_record(account_id, label, auth_data is not None, auth_data)
            account["accountType"] = account_type
            if auth_data is not None and self._find_identity(account["identity"]) is not None:
                raise AccountError("This API key is already managed on this machine", 409)
            self.manifest["accounts"].append(account)
            self.manifest["activeAccountId"] = account_id
            self._record_activation(account_id)
            try:
                if auth_data is None:
                    if self.auth_path.exists():
                        self.auth_path.unlink()
                else:
                    atomic_write_bytes(self.auth_path, auth_data)
                    atomic_write_bytes(self._account_path(account_id), auth_data)
                self._apply_config_for(account, common_toml)
                self._save_manifest()
            except Exception as exc:
                self.manifest = old_manifest
                if old_live is not None:
                    atomic_write_bytes(self.auth_path, old_live)
                if old_config is not None:
                    atomic_write_bytes(self.config_path, old_config)
                raise AccountError(f"Could not prepare the new account login: {exc}", 500) from exc
            self.error = None
            self.message = None
            return self.status()

    def rename(self, account_id: str, label: str, update_local_data=None) -> dict:
        self._ensure_account_changes_allowed()
        label = self._validate_label(label)
        with self.lock:
            account = self._find(str(account_id or ""))
            if account is None:
                raise AccountError("Account not found", 404)
            old_label, old_updated_at = account["label"], account.get("updatedAt")
            account.update({"label": label, "updatedAt": timestamp()})
            for activation in self.manifest.get("activationHistory") or []:
                if activation.get("accountSlotId") == account["id"]:
                    activation["accountLabel"] = label
            rollback_local_data = None
            try:
                if update_local_data is not None:
                    rollback_local_data = update_local_data(account["id"], label)
                self._save_manifest()
            except Exception as exc:
                account.update({"label": old_label, "updatedAt": old_updated_at})
                for activation in self.manifest.get("activationHistory") or []:
                    if activation.get("accountSlotId") == account["id"]:
                        activation["accountLabel"] = old_label
                if rollback_local_data is not None:
                    rollback_local_data()
                raise AccountError(f"Could not rename the account: {exc}", 500) from exc
            self.error = None
            self.message = None
            return self.status()

    def delete(self, account_id: str) -> dict:
        self._ensure_account_changes_allowed()
        with self.lock:
            target = self._find(str(account_id or ""))
            if target is None:
                raise AccountError("Account not found", 404)
            if len(self.manifest["accounts"]) <= 1:
                raise AccountError("The only saved account cannot be deleted", 409)
            active = self.active_account()
            if target["id"] == active["id"]:
                raise AccountError("The active account cannot be deleted; switch to another account first", 409)
            old_live = self.auth_path.read_bytes() if self.auth_path.exists() else None
            old_manifest = json.loads(json.dumps(self.manifest))
            credential_path = self._account_path(target["id"])
            old_credential = credential_path.read_bytes() if credential_path.exists() else None
            try:
                self.manifest["accounts"] = [account for account in self.manifest["accounts"] if account["id"] != target["id"]]
                remove_directory(credential_path.parent)
                self._save_manifest()
            except Exception as exc:
                self.manifest = old_manifest
                if old_credential is not None:
                    atomic_write_bytes(credential_path, old_credential)
                if old_live is None:
                    if self.auth_path.exists():
                        self.auth_path.unlink()
                else:
                    atomic_write_bytes(self.auth_path, old_live)
                if isinstance(exc, AccountError):
                    raise
                raise AccountError(f"Could not delete the account; the previous account state was restored: {exc}", 500) from exc
            self.error = None
            self.message = None
            return self.status()

    def switch(self, account_id: str, external_update_callback=None) -> dict:
        self._ensure_account_changes_allowed()
        with self.lock:
            self.sync_config_from_disk()
            target = self._find(str(account_id or ""))
            if target is None:
                raise AccountError("Account not found", 404)
            active = self._find(self.manifest.get("activeAccountId"))
            common_toml, _ = self._config_editor_parts(self._header_for(active))
            merged_toml = self._compose_toml_parts(common_toml, self._header_for(target))
            if active is not None and target["id"] == active["id"]:
                if active.get("ready"):
                    _, external_update = self.reconcile_active_from_live()
                    if external_update is not None and external_update_callback is not None:
                        external_update_callback(external_update)
                else:
                    self.reconcile_pending_login()
                return self.status()
            update_session_providers = self._model_provider(self._compose_toml_parts(common_toml, self._header_for(active))) != self._model_provider(merged_toml)
            if active is None:
                old_live = self.auth_path.read_bytes() if self.auth_path.exists() else None
                old_config = self.config_path.read_bytes() if self.config_path.exists() else None
                old_manifest = json.loads(json.dumps(self.manifest))
                old_sessions = {}
                try:
                    if target.get("ready"):
                        data = self._account_path(target["id"]).read_bytes()
                        parse_auth_bytes(data)
                        atomic_write_bytes(self.auth_path, data)
                    elif self.auth_path.exists():
                        self.auth_path.unlink()
                    self.manifest["activeAccountId"] = target["id"]
                    self._record_activation(target["id"])
                    self._apply_config_for(target, common_toml)
                    if update_session_providers:
                        old_sessions = self._rewrite_recent_session_model_providers(merged_toml)
                    self._save_manifest()
                    return self.status()
                except Exception as exc:
                    self.manifest = old_manifest
                    if old_live is None:
                        if self.auth_path.exists():
                            self.auth_path.unlink()
                    else:
                        atomic_write_bytes(self.auth_path, old_live)
                    if old_config is not None:
                        atomic_write_bytes(self.config_path, old_config)
                    self._restore_session_files(old_sessions)
                    if isinstance(exc, AccountError):
                        raise
                    raise AccountError(f"Account switch failed and the previous login was restored: {exc}", 500) from exc
            if active.get("ready"):
                _, external_update = self.reconcile_active_from_live()
                if external_update is not None and external_update_callback is not None:
                    external_update_callback(external_update)
            old_live = self.auth_path.read_bytes() if self.auth_path.exists() else None
            old_config = self.config_path.read_bytes() if self.config_path.exists() else None
            old_manifest = json.loads(json.dumps(self.manifest))
            old_sessions = {}
            self.manifest["activeAccountId"] = target["id"]
            self._record_activation(target["id"])
            try:
                if target.get("ready"):
                    credential_path = self._account_path(target["id"])
                    if not credential_path.exists():
                        raise AccountError("Saved credentials for this account are missing", 409)
                    data = credential_path.read_bytes()
                    parse_auth_bytes(data)
                    atomic_write_bytes(self.auth_path, data)
                elif self.auth_path.exists():
                    self.auth_path.unlink()
                self._apply_config_for(target, common_toml)
                if update_session_providers:
                    old_sessions = self._rewrite_recent_session_model_providers(merged_toml)
                self._save_manifest()
            except Exception as exc:
                self.manifest = old_manifest
                if old_live is None:
                    if self.auth_path.exists():
                        self.auth_path.unlink()
                else:
                    atomic_write_bytes(self.auth_path, old_live)
                if old_config is not None:
                    atomic_write_bytes(self.config_path, old_config)
                self._restore_session_files(old_sessions)
                if isinstance(exc, AccountError):
                    raise
                raise AccountError(f"Account switch failed and the previous login was restored: {exc}", 500) from exc
            self.error = None
            self.message = None
            return self.status()

    def update_api_config(self, account_id: str, header_toml: str) -> dict:
        self._ensure_account_changes_allowed()
        if not isinstance(header_toml, str):
            raise AccountError("Header configuration must be TOML text")
        try:
            header_toml = self._format_toml(header_toml)
        except (tomlkit.exceptions.ParseError, tomllib.TOMLDecodeError) as exc:
            raise AccountError(f"Header configuration is not valid TOML: {exc}", 409) from exc
        with self.lock:
            account = self._find(str(account_id or ""))
            if account is None:
                raise AccountError("Account not found", 404)
            if account.get("accountType") != "api":
                raise AccountError("Headers can be configured only for API accounts", 409)
            active = self._find(self.manifest.get("activeAccountId"))
            common_toml, _ = self._config_editor_parts(header_toml if account is active else self._header_for(active))
            self._validate_config_parts(common_toml, (header_toml,))
            account["configHeaderToml"] = header_toml.strip() + "\n" if header_toml.strip() else ""
            account["configHeader"] = {}
            account["updatedAt"] = timestamp()
            if account["id"] == self.manifest.get("activeAccountId"):
                self._write_config(self._compose_toml_parts(common_toml, header_toml))
            self._save_manifest()
            return self.status()

    def update_common_account_header(self, header_toml: str) -> dict:
        if not isinstance(header_toml, str):
            raise AccountError("Common account header configuration must be TOML text")
        try:
            header_toml = self._format_toml(header_toml)
        except (tomlkit.exceptions.ParseError, tomllib.TOMLDecodeError) as exc:
            raise AccountError(f"Common account header configuration is not valid TOML: {exc}", 409) from exc
        with self.lock:
            active = self._find(self.manifest.get("activeAccountId"))
            common_toml, _ = self._config_editor_parts(self._header_for(active) if active and active.get("accountType") == "api" else header_toml)
            self._validate_config_parts(common_toml, (header_toml,))
            normalized = header_toml.strip() + "\n" if header_toml.strip() else ""
            self.manifest["commonAccountHeaderToml"] = normalized
            if active and active.get("accountType") != "api":
                self._write_config(self._compose_toml_parts(common_toml, normalized))
            self._save_manifest()
            return self.status()

    def update_common_config(self, common_toml: str) -> dict:
        if not isinstance(common_toml, str):
            raise AccountError("Common configuration must be TOML text")
        try:
            common_toml = self._format_toml(common_toml)
        except (tomlkit.exceptions.ParseError, tomllib.TOMLDecodeError) as exc:
            raise AccountError(f"Common configuration is not valid TOML: {exc}", 409) from exc
        with self.lock:
            active = self._find(self.manifest.get("activeAccountId"))
            is_api = bool(active and active.get("accountType") == "api")
            stored_header = (active or {}).get("configHeaderToml", "") if is_api else self.manifest.get("commonAccountHeaderToml", "")
            common, detected_header = self._config_editor_parts(stored_header)
            header_toml = stored_header or detected_header
            headers = self._stored_header_configs() + ([self.manifest.get("commonAccountHeaderToml", "")] if self.manifest.get("commonAccountHeaderToml", "") else [])
            if header_toml and header_toml not in headers:
                headers.append(header_toml)
            self._validate_config_parts(common_toml, headers)
            text = self._compose_toml_parts(common_toml, header_toml)
            self._write_config(text)
            return self.status()

    def status(self) -> dict:
        with self.lock:
            self.sync_config_from_disk()
            self.reconcile_pending_login()
            active_id = self.manifest["activeAccountId"]
            items = []
            for account in self.manifest["accounts"]:
                identity = account.get("identity") if isinstance(account.get("identity"), dict) else {}
                items.append({
                    "id": account["id"],
                    "label": account["label"],
                    "email": identity.get("email"),
                    "ready": bool(account.get("ready")),
                    "active": account["id"] == active_id,
                    "accountType": account.get("accountType") or (account.get("identity") or {}).get("accountType", "account"),
                    "isApiAccount": (account.get("accountType") or (account.get("identity") or {}).get("accountType")) == "api",
                    "sessionRefresh": normalize_session_refresh(account.get("sessionRefresh")),
                    "configHeader": dict(account.get("configHeader") or {}),
                    "configHeaderToml": account.get("configHeaderToml", ""),
                })
            active = self._find(active_id)
            return {
                "activeAccountId": active_id,
                "awaitingLogin": not bool(active and active.get("ready")),
                "error": self.error,
                "message": self.message,
                "items": items,
            }

    @staticmethod
    def _downloaded_cloud_identity(state: dict, data: bytes) -> tuple[bool, dict, str]:
        ready = state.get("ready", True) is not False
        identity = auth_identity(parse_auth_bytes(data)) if ready else {"accountId": None, "idTokenHash": None, "email": None, "accountType": "account"}
        account_type = state.get("accountType", identity.get("accountType", "account"))
        if ready and state.get("accountType") and state["accountType"] != identity.get("accountType"):
            raise AccountError("Cloud account type does not match its credential payload", 409)
        if not identity.get("accountId") and state.get("accountId"):
            identity["accountId"] = state["accountId"]
        if not identity.get("email") and state.get("email"):
            identity["email"] = state["email"]
        return ready, identity, account_type

    def _commit_downloaded_cloud_account(self, state: dict, data: bytes, identity: dict, ready: bool, account_type: str, account_id: str, error_action: str) -> None:
        with self.lock:
            if self._find(account_id) is not None:
                raise AccountError(f"{error_action} recovery blocked: the intended local credential slot is already in use", 409)
            if duplicate_key := self._find_cloud_key(state["accountKey"]):
                raise AccountError(f"{error_action} blocked: this cloud credential profile is already managed as {duplicate_key['label']}", 409)
            if account_type == "api" and (duplicate := self._find_identity(identity)):
                raise AccountError(f"This API account is already linked as {duplicate['label']}", 409)
            account = self._new_record(account_id, state.get("label") or "Cloud account", ready, data if ready else None)
            if identity.get("accountId") or identity.get("idTokenHash"):
                account["identity"] = identity
            account["accountType"] = account_type
            account["configHeader"] = dict(state.get("configHeader") or {})
            account["configHeaderToml"] = state.get("configHeaderToml", "")
            account["cloud"] = {"accountKey": state["accountKey"], "keyType": state.get("keyType", "opaque")}
            old_live = self.auth_path.read_bytes() if self.auth_path.exists() else None
            old_manifest = json.loads(json.dumps(self.manifest))
            try:
                if ready:
                    atomic_write_bytes(self._account_path(account_id), data)
                self.manifest["accounts"].append(account)
                if self.manifest.get("activeAccountId") is None:
                    self.manifest["activeAccountId"] = account_id
                    self._record_activation(account_id)
                    if ready:
                        atomic_write_bytes(self.auth_path, data)
                self._save_manifest()
            except Exception as exc:
                self.manifest = old_manifest
                shutil.rmtree(self._account_path(account_id).parent, ignore_errors=True)
                if old_live is None:
                    self.auth_path.unlink(missing_ok=True)
                else:
                    atomic_write_bytes(self.auth_path, old_live)
                raise AccountError(f"Cloud account {error_action.lower()} failed before the local credential profile was committed: {exc}", getattr(exc, "status", 500)) from exc

    def _link_downloaded_api_account(self, cloud, state: dict, data: bytes, etag: str, intended_account_id: str | None = None) -> dict:
        ready, identity, account_type = self._downloaded_cloud_identity(state, data)
        if account_type != "api" or identity.get("accountType") != "api" or not ready:
            raise AccountError("Only API accounts can be linked", 409)
        with self.lock:
            if duplicate := self._find_identity(identity):
                raise AccountError(f"This API account is already linked as {duplicate['label']}", 409)
            if duplicate_key := self._find_cloud_key(state["accountKey"]):
                raise AccountError(f"This API account is already linked as {duplicate_key['label']}", 409)
        cloud.cache_remote_account(state, etag, data)
        self._commit_downloaded_cloud_account(state, data, identity, ready, account_type, str(intended_account_id or uuid.uuid4().hex), "Link")
        self.error = None
        self.message = None
        return self.status()

    def link_cloud_account(self, cloud, account_key: str) -> dict:
        state, data, etag = cloud.bind_account(str(account_key or ""))
        return self._link_downloaded_api_account(cloud, state, data, etag)

    def bind_cloud_account(self, cloud, account_key: str, record_transition: bool = True, intended_account_id: str | None = None) -> dict:
        account_key = str(account_key or "")
        with self.lock:
            if duplicate_key := self._find_cloud_key(account_key):
                raise AccountError(f"Bind blocked: this cloud credential profile is already managed as {duplicate_key['label']}", 409)
        state, data, etag = cloud.bind_account(account_key)
        ready, identity, account_type = self._downloaded_cloud_identity(state, data)
        if account_type == "api":
            return self._link_downloaded_api_account(cloud, state, data, etag, intended_account_id)
        account_id = str(intended_account_id or uuid.uuid4().hex)
        with self.lock:
            if duplicate_key := self._find_cloud_key(account_key):
                raise AccountError(f"Bind blocked: this cloud credential profile is already managed as {duplicate_key['label']}", 409)
        if record_transition:
            cloud.begin_account_transition("bind", accountId=account_id, accountKey=account_key, revisionId=auth_fingerprint(data), etag=etag)
        try:
            self._commit_downloaded_cloud_account(state, data, identity, ready, account_type, account_id, "Bind")
        except Exception:
            if record_transition:
                cloud.clear_account_transition()
            raise
        try:
            cloud.delete_account_payloads(account_key, etag)
        except Exception as exc:
            raise AccountError(f"The credential profile is safely stored locally, but its cloud payload could not be removed; restart the monitor to retry cleanup: {exc}", getattr(exc, "status", 500)) from exc
        if record_transition:
            cloud.clear_account_transition()
        self.error = None
        self.message = None
        return self.status()

    def release_cloud_account(self, cloud, account_id: str) -> dict:
        with self.lock:
            target = self._find(str(account_id or ""))
            if target is None:
                raise AccountError("Local account not found", 404)
            if target.get("accountType") == "api":
                raise AccountError("API accounts must be shared instead of released", 409)
            if len(self.manifest["accounts"]) <= 1:
                raise AccountError("The only saved account cannot be released", 409)
            if target["id"] == self.manifest.get("activeAccountId"):
                raise AccountError("The active account cannot be released; switch to another account first", 409)
            stored_identity = target.get("identity") if isinstance(target.get("identity"), dict) else {}
            cloud_binding = target.get("cloud") or {}
            account_key = cloud_binding.get("accountKey")
            key_type = cloud_binding.get("keyType", "opaque") if account_key else "opaque"
            if not account_key:
                account_key = cloud.new_account_key()
            credential_path = self._account_path(target["id"])
            if not target.get("ready"):
                data, auth = b"{}", {}
            elif target["id"] == self.manifest.get("activeAccountId"):
                if not self.auth_path.exists():
                    raise AccountError(f"Account release refused: current auth.json for {target['label']} is missing", 409)
                data = self.auth_path.read_bytes()
                auth = parse_auth_bytes(data)
                self._validate_live_matches_saved(target, auth)
            else:
                data = credential_path.read_bytes()
                auth = parse_auth_bytes(data)
            cloud.begin_account_transition("release", accountId=target["id"], accountKey=account_key, revisionId=auth_fingerprint(data))
            try:
                identity = auth_identity(auth)
                cloud.release_account(
                    account_key, data, {"accountId": identity.get("accountId") or stored_identity.get("accountId"), "email": identity.get("email") or stored_identity.get("email")}, target["label"],
                    ready=bool(target.get("ready")), key_type=key_type, config_header=target.get("configHeader"), account_type=target.get("accountType", "account"), config_header_toml=target.get("configHeaderToml", ""),
                )
            except Exception as exc:
                cloud.clear_account_transition()
                raise AccountError(f"Account release failed; local credentials were unchanged: {exc}", getattr(exc, "status", 500)) from exc
            self.manifest["accounts"] = [account for account in self.manifest["accounts"] if account["id"] != target["id"]]
            shutil.rmtree(credential_path.parent, ignore_errors=True)
            self._save_manifest()
            cloud.clear_account_transition()
            return self.status()

    def share_cloud_account(self, cloud, account_id: str) -> dict:
        with self.lock:
            target = self._find(str(account_id or ""))
            if target is None:
                raise AccountError("Local account not found", 404)
            if target.get("accountType") != "api":
                raise AccountError("Only API accounts can be shared", 409)
            credential_path = self._account_path(target["id"])
            data = self.auth_path.read_bytes() if target["id"] == self.manifest.get("activeAccountId") else credential_path.read_bytes()
            auth = parse_auth_bytes(data)
            identity = auth_identity(auth)
            if identity.get("accountType") != "api" or not api_identity_id(identity):
                raise AccountError("The selected API account credential is incomplete", 409)
            if cloud.find_remote_api_accounts(api_identity_id(identity)):
                raise AccountError("This API account is already shared", 409)
            account_key = (target.get("cloud") or {}).get("accountKey") or cloud.api_account_key(identity)
            cloud.release_account(account_key, data, identity, target["label"], ready=bool(target.get("ready")), key_type=(target.get("cloud") or {}).get("keyType") or "api-identity", config_header=target.get("configHeader"), account_type="api", config_header_toml=target.get("configHeaderToml", ""), reject_existing=True)
            target["cloud"] = {"accountKey": account_key, "keyType": (target.get("cloud") or {}).get("keyType") or "api-identity"}
            self._save_manifest()
            return self.status()

    def finalize_recovered_release(self, account_id: str) -> None:
        with self.lock:
            target = self._find(str(account_id or ""))
            if target is None:
                return
            fallback = next((account for account in self.manifest["accounts"] if account["id"] != target["id"]), None)
            if self.manifest.get("activeAccountId") == target["id"]:
                self.manifest["activeAccountId"] = fallback["id"] if fallback else None
                if fallback:
                    self._record_activation(fallback["id"])
                if fallback and fallback.get("ready"):
                    atomic_write_bytes(self.auth_path, self._account_path(fallback["id"]).read_bytes())
                elif self.auth_path.exists():
                    self.auth_path.unlink()
            self.manifest["accounts"] = [account for account in self.manifest["accounts"] if account["id"] != target["id"]]
            shutil.rmtree(self._account_path(target["id"]).parent, ignore_errors=True)
            self._save_manifest()

    def rollback_recovered_release(self, account_id: str) -> None:
        with self.lock:
            target = self._find(str(account_id or ""))
            if target is None:
                return
            credential_path = self._account_path(target["id"])
            quarantine = credential_path.with_name("auth.release-quarantine.json")
            if quarantine.exists() and not credential_path.exists():
                os.replace(quarantine, credential_path)
            if self.manifest.get("activeAccountId") == target["id"] and credential_path.exists():
                atomic_write_bytes(self.auth_path, credential_path.read_bytes())
