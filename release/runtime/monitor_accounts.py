#!/usr/bin/env python3

import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from monitor_common import UNKNOWN_EVENT_ACCOUNT_ID, UNKNOWN_EVENT_ACCOUNT_LABEL, jwt_payload


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


def parse_auth_bytes(data: bytes) -> dict:
    try:
        auth = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AccountError("auth.json is not valid UTF-8 JSON") from exc
    if not isinstance(auth, dict):
        raise AccountError("auth.json must contain a JSON object")
    return auth


def auth_identity(auth: dict) -> dict:
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
    }


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
        self.root = Path(root) if root is not None else self.auth_path.parent / "usage-monitor-accounts"
        self.manifest_path = self.root / "accounts.json"
        self.lock = threading.RLock()
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
        identity = auth_identity(parse_auth_bytes(data)) if data is not None else {"accountId": None, "idTokenHash": None, "email": None}
        now = timestamp()
        return {
            "id": account_id,
            "label": label,
            "ready": ready,
            "createdAt": now,
            "updatedAt": now,
            "identity": identity,
            "fingerprint": auth_fingerprint(data) if data is not None else None,
            "cloud": {"state": "local-only", "accountKey": None, "boundMachineId": None},
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
            if not isinstance(manifest, dict) or manifest.get("version") not in {1, 2} or not isinstance(manifest.get("accounts"), list):
                raise AccountError("Unsupported or invalid account manifest", 500)
            changed = manifest.get("version") == 1
            if changed:
                manifest["version"] = 2
                manifest["cloudBindingEnabled"] = False
                for account in manifest["accounts"]:
                    account["cloud"] = {"state": "local-only", "accountKey": None, "boundMachineId": None}
            for account in manifest["accounts"]:
                credential_path = self._account_path(str(account.get("id") or ""))
                if not account.get("ready") or not credential_path.exists():
                    continue
                data = credential_path.read_bytes()
                identity = auth_identity(parse_auth_bytes(data))
                if account.get("identity") != identity or account.get("fingerprint") != auth_fingerprint(data):
                    account.update({"identity": identity, "fingerprint": auth_fingerprint(data)})
                    changed = True
            if changed:
                atomic_write_json(self.manifest_path, manifest)
            return manifest

        live = self.auth_path.read_bytes() if self.auth_path.exists() else None
        ready = False
        if live is not None:
            auth = parse_auth_bytes(live)
            tokens = auth.get("tokens") if isinstance(auth.get("tokens"), dict) else {}
            ready = isinstance(tokens.get("refresh_token"), str) and bool(tokens["refresh_token"].strip())
        if ready:
            atomic_write_bytes(self._account_path("ppl-pro"), live)
        manifest = {
            "version": 2,
            "activeAccountId": "ppl-pro" if live is not None else None,
            "cloudBindingEnabled": False,
            "accounts": [self._new_record("ppl-pro", "Current account", ready, live if ready else None)] if live is not None else [],
        }
        atomic_write_json(self.manifest_path, manifest)
        return manifest

    def _save_manifest(self) -> None:
        atomic_write_json(self.manifest_path, self.manifest)

    def _find(self, account_id: str) -> dict | None:
        return next((account for account in self.manifest["accounts"] if account.get("id") == account_id), None)

    def _find_identity(self, identity: dict, exclude_id: str | None = None) -> dict | None:
        return next((account for account in self.manifest["accounts"] if account.get("id") != exclude_id and same_auth_identity(account.get("identity"), identity)), None)

    def active_account(self) -> dict:
        account = self._find(self.manifest.get("activeAccountId"))
        if account is None:
            raise AccountError("The active account no longer exists", 500)
        return account

    def attribution_for_auth(self, auth: dict) -> dict:
        with self.lock:
            identity = auth_identity(auth)
            account = self._find_identity(identity)
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
        live_tokens = auth.get("tokens") if isinstance(auth.get("tokens"), dict) else {}
        if not isinstance(live_tokens.get("refresh_token"), str) or not live_tokens["refresh_token"].strip():
            return
        saved_tokens = saved_auth.get("tokens") if isinstance(saved_auth.get("tokens"), dict) else {}
        missing = [field for field in ("account_id",) if not live_tokens.get(field) or not saved_tokens.get(field)]
        if missing:
            raise AccountError(f"Account change refused: cannot verify {account['label']} because {', '.join(missing)} is missing from current or saved auth.json", 409)
        mismatched = [field for field in ("account_id",) if live_tokens[field] != saved_tokens[field]]
        if mismatched:
            raise AccountError(f"Account change refused: current auth.json does not match saved account {account['label']} ({' and '.join(mismatched)} differ)", 409)

    def _snapshot_live_into(self, account: dict, strict_identity: bool = True, require_saved_match: bool = False) -> bool:
        if not self.auth_path.exists():
            if require_saved_match:
                raise AccountError(f"Account change refused: current auth.json is missing, so saved account {account['label']} cannot be verified", 409)
            return False
        data = self.auth_path.read_bytes()
        auth = parse_auth_bytes(data)
        tokens = auth.get("tokens") if isinstance(auth.get("tokens"), dict) else {}
        if account.get("ready") and tokens and (not isinstance(tokens.get("refresh_token"), str) or not tokens["refresh_token"].strip()):
            return False
        if require_saved_match:
            self._validate_live_matches_saved(account, auth)
        identity = auth_identity(auth)
        stored_identity = account.get("identity") if isinstance(account.get("identity"), dict) else {}
        if strict_identity and account.get("ready") and (stored_identity.get("accountId") or stored_identity.get("idTokenHash")) and (identity.get("accountId") or identity.get("idTokenHash")) and not same_auth_identity(stored_identity, identity):
            raise AccountError("Live auth.json belongs to a different account; use New account before replacing the current login", 409)
        if duplicate := self._find_identity(identity, account["id"]):
            raise AccountError(f"Account change refused: this login is already managed as {duplicate['label']}", 409)
        fingerprint = auth_fingerprint(data)
        if account.get("ready") and account.get("fingerprint") == fingerprint and self._account_path(account["id"]).exists():
            return False
        atomic_write_bytes(self._account_path(account["id"]), data)
        account.update({"ready": True, "updatedAt": timestamp(), "identity": identity, "fingerprint": fingerprint})
        self._save_manifest()
        return True

    def sync_active_from_live(self) -> bool:
        with self.lock:
            if self.manifest.get("activeAccountId") is None:
                return False
            active = self.active_account()
            if not active.get("ready"):
                return self.reconcile_pending_login()
            return self._snapshot_live_into(active)

    def inactive_ready_credentials(self) -> list[dict]:
        with self.lock:
            active_id = self.manifest.get("activeAccountId")
            credentials = []
            for account in self.manifest["accounts"]:
                credential_path = self._account_path(account["id"])
                if account["id"] == active_id or not account.get("ready") or not credential_path.exists():
                    continue
                data = credential_path.read_bytes()
                credentials.append({"id": account["id"], "label": account["label"], "data": data, "fingerprint": auth_fingerprint(data)})
            return credentials

    def commit_polled_credentials(self, account_id: str, expected_fingerprint: str, data: bytes) -> bool:
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
            if not same_auth_identity(account.get("identity"), identity) or self._find_identity(identity, account_id):
                return False
            if account_id == self.manifest.get("activeAccountId"):
                if not self.auth_path.exists() or auth_fingerprint(self.auth_path.read_bytes()) != expected_fingerprint:
                    return False
                atomic_write_bytes(self.auth_path, data)
            atomic_write_bytes(credential_path, data)
            account.update({"updatedAt": timestamp(), "identity": identity, "fingerprint": fingerprint})
            self._save_manifest()
            return True

    def reconcile_pending_login(self) -> bool:
        with self.lock:
            if self.manifest.get("activeAccountId") is None:
                return False
            active = self.active_account()
            if active.get("ready") or not self.auth_path.exists():
                return False
            try:
                data = self.auth_path.read_bytes()
                auth = parse_auth_bytes(data)
                tokens = auth.get("tokens") if isinstance(auth.get("tokens"), dict) else {}
                if not isinstance(tokens.get("refresh_token"), str) or not tokens["refresh_token"].strip():
                    self.error = "Waiting for Codex login to create auth.json with a refresh token"
                    self.message = None
                    return False
                identity = auth_identity(auth)
                duplicate = self._find_identity(identity, active["id"])
                if duplicate:
                    credential_path = self._account_path(duplicate["id"])
                    old_credential = credential_path.read_bytes() if credential_path.exists() else None
                    old_manifest = json.loads(json.dumps(self.manifest))
                    try:
                        atomic_write_bytes(credential_path, data)
                        duplicate.update({"ready": True, "updatedAt": timestamp(), "identity": identity, "fingerprint": auth_fingerprint(data)})
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
                atomic_write_bytes(self._account_path(active["id"]), data)
                active.update({"ready": True, "updatedAt": timestamp(), "identity": identity, "fingerprint": auth_fingerprint(data)})
                self._save_manifest()
                self.error = None
                self.message = None
                return True
            except AccountError as exc:
                self.error = str(exc)
                return False

    def create_account(self, label: str) -> dict:
        label = self._validate_label(label)
        with self.lock:
            active = self._find(self.manifest.get("activeAccountId"))
            if active is not None and active.get("ready"):
                self._snapshot_live_into(active, require_saved_match=True)
            old_live = self.auth_path.read_bytes() if self.auth_path.exists() else None
            old_manifest = json.loads(json.dumps(self.manifest))
            account_id = uuid.uuid4().hex
            account = self._new_record(account_id, label, False)
            self.manifest["accounts"].append(account)
            self.manifest["activeAccountId"] = account_id
            try:
                if self.auth_path.exists():
                    self.auth_path.unlink()
                self._save_manifest()
            except Exception as exc:
                self.manifest = old_manifest
                if old_live is not None:
                    atomic_write_bytes(self.auth_path, old_live)
                raise AccountError(f"Could not prepare the new account login: {exc}", 500) from exc
            self.error = None
            self.message = None
            return self.status()

    def rename(self, account_id: str, label: str, update_local_data=None) -> dict:
        label = self._validate_label(label)
        with self.lock:
            account = self._find(str(account_id or ""))
            if account is None:
                raise AccountError("Account not found", 404)
            old_label, old_updated_at = account["label"], account.get("updatedAt")
            account.update({"label": label, "updatedAt": timestamp()})
            rollback_local_data = None
            try:
                if update_local_data is not None:
                    rollback_local_data = update_local_data(account["id"], label)
                self._save_manifest()
            except Exception as exc:
                account.update({"label": old_label, "updatedAt": old_updated_at})
                if rollback_local_data is not None:
                    rollback_local_data()
                raise AccountError(f"Could not rename the account: {exc}", 500) from exc
            self.error = None
            self.message = None
            return self.status()

    def delete(self, account_id: str) -> dict:
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
                if credential_path.exists():
                    credential_path.unlink()
                if credential_path.parent.exists():
                    credential_path.parent.rmdir()
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

    def switch(self, account_id: str) -> dict:
        with self.lock:
            target = self._find(str(account_id or ""))
            if target is None:
                raise AccountError("Account not found", 404)
            active = self._find(self.manifest.get("activeAccountId"))
            if active is None:
                if target.get("ready"):
                    data = self._account_path(target["id"]).read_bytes()
                    parse_auth_bytes(data)
                    atomic_write_bytes(self.auth_path, data)
                elif self.auth_path.exists():
                    self.auth_path.unlink()
                self.manifest["activeAccountId"] = target["id"]
                self._save_manifest()
                return self.status()
            if target["id"] == active["id"]:
                self.reconcile_pending_login()
                return self.status()
            if active.get("ready"):
                self._snapshot_live_into(active, require_saved_match=True)
            old_live = self.auth_path.read_bytes() if self.auth_path.exists() else None
            old_manifest = json.loads(json.dumps(self.manifest))
            self.manifest["activeAccountId"] = target["id"]
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
                self._save_manifest()
            except Exception as exc:
                self.manifest = old_manifest
                if old_live is None:
                    if self.auth_path.exists():
                        self.auth_path.unlink()
                else:
                    atomic_write_bytes(self.auth_path, old_live)
                if isinstance(exc, AccountError):
                    raise
                raise AccountError(f"Account switch failed and the previous login was restored: {exc}", 500) from exc
            self.error = None
            self.message = None
            return self.status()

    def status(self) -> dict:
        with self.lock:
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
                    "cloudState": (account.get("cloud") or {}).get("state", "local-only"),
                    "accountKey": (account.get("cloud") or {}).get("accountKey"),
                })
            active = self._find(active_id)
            return {
                "activeAccountId": active_id,
                "awaitingLogin": not bool(active and active.get("ready")),
                "cloudBindingEnabled": bool(self.manifest.get("cloudBindingEnabled")),
                "error": self.error,
                "message": self.message,
                "items": items,
            }

    def bind_cloud_account(self, cloud, account_key: str, record_transition: bool = True) -> dict:
        account_key = str(account_key or "")
        if record_transition:
            cloud.begin_account_transition("bind", accountKey=account_key)
        try:
            state, data, etag = cloud.bind_account(account_key)
        except Exception:
            if record_transition:
                cloud.clear_account_transition()
            raise
        ready = state.get("ready", True) is not False
        identity = auth_identity(parse_auth_bytes(data)) if ready else {"accountId": None, "idTokenHash": None, "email": None}
        if not identity.get("accountId") and state.get("accountId"):
            identity["accountId"] = state["accountId"]
        if not identity.get("email") and state.get("email"):
            identity["email"] = state["email"]
        with self.lock:
            duplicate = self._find_identity(identity)
        if duplicate is not None:
            if record_transition:
                cloud.clear_account_transition()
            raise AccountError(f"Bind blocked: this authenticated account is already managed as {duplicate['label']}. One authenticated account can have only one managed account on this machine.", 409)
        with self.lock:
            account_id = uuid.uuid4().hex
            account = self._new_record(account_id, state.get("label") or "Cloud account", ready, data if ready else None)
            account["cloud"] = {"state": "bound-local", "accountKey": state["accountKey"], "keyType": state.get("keyType", "identity"), "boundMachineId": cloud.machine_id}
            if ready:
                atomic_write_bytes(self._account_path(account_id), data)
            self.manifest["accounts"].append(account)
            self.manifest["cloudBindingEnabled"] = True
            if self.manifest.get("activeAccountId") is None:
                self.manifest["activeAccountId"] = account_id
                if ready:
                    atomic_write_bytes(self.auth_path, data)
            self._save_manifest()
            cloud.delete_account_payloads(account_key, etag)
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
            if len(self.manifest["accounts"]) <= 1:
                raise AccountError("The only saved account cannot be released", 409)
            if target["id"] == self.manifest.get("activeAccountId"):
                raise AccountError("The active account cannot be released; switch to another account first", 409)
            stored_identity = target.get("identity") if isinstance(target.get("identity"), dict) else {}
            cloud_binding = target.get("cloud") or {}
            account_key = cloud_binding.get("accountKey")
            key_type = cloud_binding.get("keyType", "identity") if account_key else "identity" if stored_identity.get("accountId") else "opaque"
            if not account_key:
                account_key = cloud.account_key(stored_identity["accountId"]) if stored_identity.get("accountId") else cloud.new_placeholder_account_key()
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
                    ready=bool(target.get("ready")), key_type=key_type,
                )
            except Exception as exc:
                cloud.clear_account_transition()
                raise AccountError(f"Account release failed; local credentials were unchanged: {exc}", getattr(exc, "status", 500)) from exc
            self.manifest["accounts"] = [account for account in self.manifest["accounts"] if account["id"] != target["id"]]
            shutil.rmtree(credential_path.parent, ignore_errors=True)
            self._save_manifest()
            cloud.clear_account_transition()
            return self.status()

    def finalize_recovered_release(self, account_id: str) -> None:
        with self.lock:
            target = self._find(str(account_id or ""))
            if target is None:
                return
            fallback = next((account for account in self.manifest["accounts"] if account["id"] != target["id"]), None)
            if self.manifest.get("activeAccountId") == target["id"]:
                self.manifest["activeAccountId"] = fallback["id"] if fallback else None
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
