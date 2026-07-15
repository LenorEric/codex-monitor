#!/usr/bin/env python3

import hashlib
import json
import os
import re
import shutil
import struct
import tempfile
import time
import uuid
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path, PurePosixPath

from monitor_accounts import atomic_write_json

MAX_SKILL_FILES = 10000
MAX_SKILL_BYTES = 512 * 1024 * 1024
MANIFEST_FULL_REHASH_SECONDS = 60 * 60
SAFE_SKILL_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
WINDOWS_RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL", *(f"COM{number}" for number in range(1, 10)), *(f"LPT{number}" for number in range(1, 10))}
WINDOWS_INVALID_CHARACTERS = set('<>:"/\\|?*')
PROJECTION_COPY_MARKER = ".codex-switch-managed-copy.json"


class SkillError(RuntimeError):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_name(name: str) -> str:
    name = str(name or "")
    if name != name.strip() or name.endswith(".") or not SAFE_SKILL_NAME.fullmatch(name) or name.split(".", 1)[0].upper() in WINDOWS_RESERVED_NAMES:
        raise SkillError("Invalid skill name")
    return name


def _portable_path(value: str) -> PurePosixPath:
    path = PurePosixPath(str(value or ""))
    if path.is_absolute() or path.as_posix() != value or not path.parts or any(
        part in {"", ".", ".."} or part.endswith((" ", ".")) or part.casefold() == PROJECTION_COPY_MARKER.casefold() or part.split(".", 1)[0].upper() in WINDOWS_RESERVED_NAMES
        or any(character in WINDOWS_INVALID_CHARACTERS or ord(character) < 32 for character in part) or len(part.encode("utf-16-le")) // 2 > 255 for part in path.parts
    ):
        raise SkillError("Skill path is not portable across Windows and Linux")
    return path


def _validate_portable_names(names) -> None:
    seen = {}
    for name in names:
        _safe_name(name)
        folded = name.casefold()
        if folded in seen and seen[folded] != name:
            raise SkillError(f"Skill names collide on a case-insensitive filesystem: {seen[folded]} and {name}")
        seen[folded] = name


def _validate_portable_tree_paths(paths) -> None:
    seen, full_paths = {}, set()
    for value in paths:
        path = _portable_path(value)
        if path.as_posix() in full_paths:
            raise SkillError(f"Skill snapshot contains duplicate path: {path.as_posix()}")
        full_paths.add(path.as_posix())
        for length in range(1, len(path.parts) + 1):
            partial = PurePosixPath(*path.parts[:length]).as_posix()
            folded = partial.casefold()
            if folded in seen and seen[folded] != partial:
                raise SkillError(f"Skill paths collide on a case-insensitive filesystem: {seen[folded]} and {partial}")
            seen[folded] = partial


def _create_directory_junction(source: Path, target: Path) -> None:
    import ctypes
    from ctypes import wintypes

    source_name = str(source.resolve(strict=True))
    if source_name.startswith("\\\\?\\UNC\\"):
        substitute_name = "\\??\\UNC\\" + source_name[8:]
    elif source_name.startswith("\\\\?\\"):
        substitute_name = "\\??\\" + source_name[4:]
    elif source_name.startswith("\\\\"):
        substitute_name = "\\??\\UNC\\" + source_name[2:]
    else:
        substitute_name = "\\??\\" + source_name
    substitute_bytes, print_bytes = substitute_name.encode("utf-16-le"), source_name.encode("utf-16-le")
    path_buffer = substitute_bytes + b"\0\0" + print_bytes + b"\0\0"
    reparse_data = struct.pack("<LHHHHHH", 0xA0000003, 8 + len(path_buffer), 0, 0, len(substitute_bytes), len(substitute_bytes) + 2, len(print_bytes)) + path_buffer
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.DeviceIoControl.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID]
    kernel32.DeviceIoControl.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    target.mkdir()
    handle = kernel32.CreateFileW(str(target), 0x40000000, 0, None, 3, 0x02200000, None)
    if handle == wintypes.HANDLE(-1).value:
        error = ctypes.get_last_error()
        target.rmdir()
        raise ctypes.WinError(error)
    try:
        buffer = ctypes.create_string_buffer(reparse_data)
        returned = wintypes.DWORD()
        if not kernel32.DeviceIoControl(handle, 0x000900A4, buffer, len(reparse_data), None, 0, ctypes.byref(returned), None):
            raise ctypes.WinError(ctypes.get_last_error())
    except Exception:
        kernel32.CloseHandle(handle)
        target.rmdir()
        raise
    kernel32.CloseHandle(handle)


def _is_link(path: Path) -> bool:
    return path.is_symlink() or bool(getattr(path, "is_junction", lambda: False)())


def _unlink_directory_link(path: Path) -> None:
    os.rmdir(path) if getattr(path, "is_junction", lambda: False)() else path.unlink()


def _replace_with_retry(source: Path, target: Path) -> None:
    for attempt in range(5):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt == 4 or os.name != "nt":
                raise
            time.sleep(0.02 * (attempt + 1))


def _tree_manifest(root: Path) -> list[dict]:
    root = root.resolve()
    paths = sorted(root.rglob("*"), key=lambda item: item.as_posix().lower())
    _validate_portable_tree_paths(path.relative_to(root).as_posix() for path in paths)
    rows, total = [], 0
    for path in paths:
        if _is_link(path):
            try:
                resolved = path.resolve(strict=True)
            except OSError as exc:
                raise SkillError(f"Broken link in skill: {path.relative_to(root)}") from exc
            try:
                resolved.relative_to(root)
            except ValueError as exc:
                raise SkillError(f"External link in skill: {path.relative_to(root)}") from exc
        if path.is_dir():
            continue
        if not path.is_file():
            raise SkillError(f"Unsupported entry in skill: {path.relative_to(root)}")
        data = path.read_bytes()
        total += len(data)
        if len(rows) >= MAX_SKILL_FILES or total > MAX_SKILL_BYTES:
            raise SkillError("Skill data exceeds the safety limit")
        rows.append({"path": path.relative_to(root).as_posix(), "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    if not any(row["path"] == "SKILL.md" for row in rows):
        raise SkillError("A managed skill must contain SKILL.md")
    return rows


def _copy_verified(source: Path, target: Path) -> list[dict]:
    expected = _tree_manifest(source)
    shutil.copytree(source, target, symlinks=False)
    if _tree_manifest(target) != expected:
        raise SkillError(f"Verification failed while copying {source.name}", 500)
    return expected


class SkillManager:
    def __init__(self, codex_home: Path, private_root: Path | None = None, gemini_skills: Path | None = None):
        self.codex_home = Path(codex_home)
        self.private_root = Path(private_root) if private_root is not None else Path.home() / ".codex-switch"
        self.skills_root = self.private_root / "skills"
        self.state_path = self.private_root / "skills.json"
        self.gemini_skills = Path(gemini_skills) if gemini_skills is not None else Path.home() / ".gemini" / "config" / "skills"
        self.paths = {"codex": self.codex_home / "skills", "gemini": self.gemini_skills}
        self.skills_root.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()
        self._scan_cache = None
        self._status_cache = None
        self._manifest_cache = {}
        self.content_revision = 0

    def _load_state(self) -> dict:
        if not self.state_path.exists():
            state = {"version": 1, "skills": {}, "deletions": {}}
            atomic_write_json(self.state_path, state)
            return state
        try:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SkillError(f"Cannot read skill assignment state: {exc}", 500) from exc
        if not isinstance(state, dict) or state.get("version") != 1 or not isinstance(state.get("skills"), dict):
            raise SkillError("Unsupported skill assignment state", 500)
        if not isinstance(state.get("deletions", {}), dict):
            raise SkillError("Unsupported skill deletion state", 500)
        if "deletions" not in state:
            state["deletions"] = {}
            atomic_write_json(self.state_path, state)
        return state

    def _save(self) -> None:
        atomic_write_json(self.state_path, self.state)

    def scan(self, refresh: bool = False) -> list[dict]:
        if self._scan_cache is not None and not refresh:
            return self._scan_cache
        found = {}
        for app in ("codex", "gemini"):
            root = self.paths[app]
            if not root.exists():
                continue
            for path in root.iterdir():
                if path.name.startswith(".") or path.name.endswith((".tmp", ".quarantine")) or _is_link(path) or self._projection(path.name, app).get("kind") == "copy" or not path.is_dir() or not (path / "SKILL.md").is_file():
                    continue
                name = _safe_name(path.name)
                try:
                    _tree_manifest(path)
                except SkillError as exc:
                    found.setdefault(name, {"name": name, "sources": [], "error": str(exc)})
                    continue
                found.setdefault(name, {"name": name, "sources": []})["sources"].append(app)
        self._scan_cache = [{**item, "authoritativeSource": "codex" if "codex" in item["sources"] else "gemini", "defaultAssignments": item["sources"]} for item in sorted(found.values(), key=lambda row: row["name"].lower())]
        return self._scan_cache

    def _invalidate_status(self, scan: bool = False, content: bool = False) -> None:
        self._status_cache = None
        if scan:
            self._scan_cache = None
        if content:
            self.content_revision += 1

    @staticmethod
    def _file_identity(path: Path) -> tuple:
        stat = path.stat()
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, getattr(stat, "st_ctime_ns", 0)

    def _tree_manifest_cached(self, root: Path) -> list[dict]:
        root = root.resolve()
        previous = self._manifest_cache.get(str(root), {})
        current, rows, total = {}, [], 0
        now = time.monotonic()
        paths = sorted(root.rglob("*"), key=lambda item: item.as_posix().lower())
        _validate_portable_tree_paths(path.relative_to(root).as_posix() for path in paths)
        for path in paths:
            if _is_link(path):
                try:
                    resolved = path.resolve(strict=True)
                except OSError as exc:
                    raise SkillError(f"Broken link in skill: {path.relative_to(root)}") from exc
                try:
                    resolved.relative_to(root)
                except ValueError as exc:
                    raise SkillError(f"External link in skill: {path.relative_to(root)}") from exc
            if path.is_dir():
                continue
            if not path.is_file():
                raise SkillError(f"Unsupported entry in skill: {path.relative_to(root)}")
            relative = path.relative_to(root).as_posix()
            identity = self._file_identity(path)
            cached = previous.get(relative)
            if cached and cached["identity"] == identity and now - cached["verifiedAt"] < MANIFEST_FULL_REHASH_SECONDS:
                sha256 = cached["sha256"]
                verified_at = cached["verifiedAt"]
            else:
                data = path.read_bytes()
                if self._file_identity(path) != identity:
                    raise SkillError(f"Skill changed while reading: {relative}", 409)
                sha256 = hashlib.sha256(data).hexdigest()
                verified_at = now
            total += identity[2]
            if len(rows) >= MAX_SKILL_FILES or total > MAX_SKILL_BYTES:
                raise SkillError("Skill data exceeds the safety limit")
            rows.append({"path": relative, "size": identity[2], "sha256": sha256})
            current[relative] = {"identity": identity, "sha256": sha256, "verifiedAt": verified_at}
        if not any(row["path"] == "SKILL.md" for row in rows):
            raise SkillError("A managed skill must contain SKILL.md")
        self._manifest_cache[str(root)] = current
        return rows

    def _snapshot_files(self, root: Path, manifest: list[dict]) -> dict[str, bytes]:
        root = root.resolve()
        cached = self._manifest_cache.get(str(root), {})
        files = {}
        for row in manifest:
            path = root / row["path"]
            if _is_link(path):
                try:
                    path.resolve(strict=True).relative_to(root)
                except (OSError, ValueError) as exc:
                    raise SkillError(f"Unsafe link in skill: {row['path']}", 409) from exc
            identity = self._file_identity(path)
            data = path.read_bytes()
            if self._file_identity(path) != identity or cached.get(row["path"], {}).get("identity") != identity or len(data) != row["size"] or hashlib.sha256(data).hexdigest() != row["sha256"]:
                raise SkillError(f"Skill changed while packaging: {row['path']}", 409)
            files[row["path"]] = data
        if self._tree_manifest_cached(root) != manifest:
            raise SkillError("Skill changed while packaging", 409)
        return files

    def _projection(self, name: str, app: str) -> dict:
        source, target = self.skills_root / name, self.paths[app] / name
        if not target.exists() and not target.is_symlink():
            return {"state": "missing"}
        if _is_link(target):
            try:
                return {"state": "linked" if target.resolve(strict=True) == source.resolve(strict=True) else "conflict", "target": str(target.resolve(strict=False))}
            except OSError:
                return {"state": "conflict", "target": "broken link"}
        token = self.state["skills"].get(name, {}).get(f"{app}CopyToken")
        try:
            marker = json.loads((target / PROJECTION_COPY_MARKER).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            marker = {}
        if token and marker.get("token") == token:
            return {"state": "linked", "target": str(source), "kind": "copy"}
        return {"state": "conflict", "target": str(target)}

    def _create_projection_copy(self, name: str, app: str) -> None:
        target = self.paths[app] / name
        record = self.state["skills"].setdefault(name, {"codex": False, "gemini": False, "managedAt": _timestamp()})
        token = uuid.uuid4().hex
        try:
            manifest = _copy_verified(self.skills_root / name, target)
            (target / PROJECTION_COPY_MARKER).write_text(json.dumps({"version": 1, "token": token}, separators=(",", ":")) + "\n", encoding="utf-8", newline="\n")
        except Exception:
            shutil.rmtree(target, ignore_errors=True)
            raise
        record[f"{app}CopyToken"] = token
        record[f"{app}CopyManifestSha256"] = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()

    def _remove_projection(self, name: str, app: str) -> None:
        target = self.paths[app] / name
        if self._projection(name, app).get("kind") == "copy":
            shutil.rmtree(target)
            self.state["skills"].get(name, {}).pop(f"{app}CopyToken", None)
            self.state["skills"].get(name, {}).pop(f"{app}CopyManifestSha256", None)
        else:
            _unlink_directory_link(target)

    def _refresh_projection_copy(self, name: str, app: str) -> None:
        record = self.state["skills"].get(name, {})
        if not record.get(f"{app}CopyToken") or self._projection(name, app).get("kind") != "copy":
            return
        manifest = self._tree_manifest_cached(self.skills_root / name)
        digest = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        if digest == record.get(f"{app}CopyManifestSha256"):
            return
        target = self.paths[app] / name
        stage = target.with_name(f".{name}.{uuid.uuid4().hex}.tmp")
        quarantine = target.with_name(f".{name}.{uuid.uuid4().hex}.quarantine")
        try:
            _copy_verified(self.skills_root / name, stage)
            (stage / PROJECTION_COPY_MARKER).write_text(json.dumps({"version": 1, "token": record[f"{app}CopyToken"]}, separators=(",", ":")) + "\n", encoding="utf-8", newline="\n")
            _replace_with_retry(target, quarantine)
            _replace_with_retry(stage, target)
            shutil.rmtree(quarantine, ignore_errors=True)
            record[f"{app}CopyManifestSha256"] = digest
            self._save()
        except Exception:
            shutil.rmtree(stage, ignore_errors=True)
            if quarantine.exists() and not target.exists():
                _replace_with_retry(quarantine, target)
            raise

    def _create_projection_link(self, name: str, app: str) -> None:
        target = self.paths[app] / name
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.symlink(self.skills_root / name, target, target_is_directory=True)
        except OSError as exc:
            if os.name != "nt":
                try:
                    self._create_projection_copy(name, app)
                except OSError as copy_exc:
                    raise SkillError(f"Could not create the {app} skill projection after symbolic-link failure: {copy_exc}", 409) from copy_exc
                return
            try:
                _create_directory_junction(self.skills_root / name, target)
            except OSError as junction_exc:
                raise SkillError(f"Could not create the {app} skill link: {junction_exc}", 409) from junction_exc

    def _replace_local_skill_with_link(self, name: str, app: str) -> None:
        target = self.paths[app] / name
        if _is_link(target) or not target.is_dir() or not (target / "SKILL.md").is_file():
            raise SkillError(f"Cannot assign {name} to {app}: {target} is unrelated", 409)
        _tree_manifest(target)
        quarantine = target.with_name(f".{name}.{uuid.uuid4().hex}.quarantine")
        os.replace(target, quarantine)
        try:
            self._create_projection_link(name, app)
        except Exception:
            if target.exists() or target.is_symlink():
                _unlink_directory_link(target) if _is_link(target) else shutil.rmtree(target)
            os.replace(quarantine, target)
            raise
        shutil.rmtree(quarantine)

    def assign(self, name: str, app: str, enabled: bool) -> dict:
        name = _safe_name(name)
        if app not in self.paths or not (self.skills_root / name / "SKILL.md").is_file():
            raise SkillError("Unknown managed skill or application", 404)
        record = self.state["skills"].setdefault(name, {"codex": False, "gemini": False, "managedAt": _timestamp()})
        projection = self._projection(name, app)
        target = self.paths[app] / name
        if enabled:
            if projection["state"] == "conflict":
                self._replace_local_skill_with_link(name, app)
            if projection["state"] == "missing":
                self._create_projection_link(name, app)
        elif projection["state"] == "linked":
            self._remove_projection(name, app)
        elif projection["state"] == "conflict":
            raise SkillError(f"Cannot remove {name} from {app}: {target} is unrelated", 409)
        record[app] = bool(enabled)
        record.pop(f"{app}Error", None)
        self._save()
        self._invalidate_status()
        return self.status()

    def manage(self, names: list[str]) -> dict:
        scanned = {row["name"]: row for row in self.scan(refresh=True)}
        results = []
        for raw_name in names:
            name = _safe_name(raw_name)
            _validate_portable_names([name, *(path.name for path in self.skills_root.iterdir() if not path.name.startswith("."))])
            item = scanned.get(name)
            if not item or item.get("error"):
                raise SkillError(item.get("error") if item else f"Unmanaged skill not found: {name}", 404)
            source = self.paths[item["authoritativeSource"]] / name
            final = self.skills_root / name
            if final.exists() or final.is_symlink():
                raise SkillError(f"Managed skill already exists: {name}", 409)
            stage = self.skills_root / f".{name}.{uuid.uuid4().hex}.tmp"
            quarantines = []
            try:
                manifest = _copy_verified(source, stage)
                for app in item["sources"]:
                    original = self.paths[app] / name
                    quarantine = original.with_name(f".{name}.{uuid.uuid4().hex}.quarantine")
                    os.replace(original, quarantine)
                    quarantines.append(quarantine)
                os.replace(stage, final)
                self.state["skills"][name] = {"codex": False, "gemini": False, "managedAt": _timestamp(), "sourceApps": item["sources"], "manifestSha256": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()}
                self.state["skills"][name]["clearedDeletions"] = [row["id"] for row in self.state["deletions"].get(name, [])]
                self._save()
                errors = []
                for app in item["defaultAssignments"]:
                    try:
                        self.assign(name, app, True)
                    except SkillError as exc:
                        self.state["skills"][name][f"{app}Error"] = str(exc)
                        errors.append(str(exc))
                self._save()
                for quarantine in quarantines:
                    shutil.rmtree(quarantine, ignore_errors=True)
                results.append({"name": name, "managed": True, "assignmentErrors": errors})
            except Exception:
                shutil.rmtree(stage, ignore_errors=True)
                if final.exists() or final.is_symlink():
                    shutil.rmtree(final, ignore_errors=True)
                for quarantine in quarantines:
                    app_root = next((root for root in self.paths.values() if quarantine.parent == root), None)
                    if app_root is not None and quarantine.exists() and not (app_root / name).exists():
                        os.replace(quarantine, app_root / name)
                raise
        self._invalidate_status(scan=True, content=bool(results))
        return {"results": results, **self.status()}

    def unmanage(self, name: str) -> dict:
        name = _safe_name(name)
        source = self.skills_root / name
        if not (source / "SKILL.md").is_file():
            raise SkillError(f"Managed skill not found: {name}", 404)
        projections = {app: self._projection(name, app) for app in self.paths}
        conflicts = [app for app, projection in projections.items() if projection["state"] == "conflict"]
        if conflicts:
            raise SkillError(f"Cannot unmanage {name}: unrelated path exists for {', '.join(conflicts)}", 409)
        stages = {}
        removed_links = []
        source_quarantine = self.skills_root / f".{name}.{uuid.uuid4().hex}.quarantine"
        original_record = dict(self.state["skills"][name]) if name in self.state["skills"] else None
        try:
            for app, projection in projections.items():
                if projection["state"] != "linked":
                    continue
                target = self.paths[app] / name
                stage = target.with_name(f".{name}.{uuid.uuid4().hex}.tmp")
                _copy_verified(source, stage)
                stages[app] = stage
            for app, stage in stages.items():
                target = self.paths[app] / name
                self._remove_projection(name, app)
                removed_links.append(app)
                _replace_with_retry(stage, target)
            os.replace(source, source_quarantine)
            self.state["skills"].pop(name, None)
            self.state["deletions"].setdefault(name, []).append({"id": uuid.uuid4().hex, "deletedAt": _timestamp()})
            self._save()
        except Exception:
            if source_quarantine.exists() and not source.exists():
                os.replace(source_quarantine, source)
            for app in reversed(removed_links):
                target = self.paths[app] / name
                if target.exists() and not _is_link(target):
                    shutil.rmtree(target)
                elif _is_link(target):
                    _unlink_directory_link(target)
                self._create_projection_link(name, app)
            if original_record is not None:
                self.state["skills"][name] = original_record
                self._save()
            for stage in stages.values():
                shutil.rmtree(stage, ignore_errors=True)
            raise
        shutil.rmtree(source_quarantine, ignore_errors=True)
        self._invalidate_status(scan=True, content=True)
        return {"name": name, "unmanaged": True, **self.status()}

    def reconcile(self) -> list[dict]:
        errors = []
        for path in self.skills_root.iterdir():
            if not path.name.startswith(".") and path.is_dir() and (path / "SKILL.md").is_file():
                self.state["skills"].setdefault(path.name, {"codex": False, "gemini": False, "managedAt": None})
        for name, record in list(self.state["skills"].items()):
            if not (self.skills_root / name / "SKILL.md").is_file():
                errors.append({"name": name, "error": "Managed source is missing"})
                continue
            for app in self.paths:
                try:
                    projection = self._projection(name, app)
                    self.assign(name, app, True if projection["state"] == "conflict" and not _is_link(self.paths[app] / name) and (self.paths[app] / name / "SKILL.md").is_file() else bool(record.get(app)))
                except SkillError as exc:
                    record[f"{app}Error"] = str(exc)
                    errors.append({"name": name, "app": app, "error": str(exc)})
        self._save()
        self._invalidate_status()
        return errors

    def status(self) -> dict:
        if self._status_cache is not None:
            return self._status_cache
        items = []
        for path in sorted(self.skills_root.iterdir(), key=lambda item: item.name.lower()):
            if path.name.startswith(".") or not path.is_dir() or not (path / "SKILL.md").is_file():
                continue
            record = self.state["skills"].setdefault(path.name, {"codex": False, "gemini": False, "managedAt": None})
            for app in self.paths:
                self._refresh_projection_copy(path.name, app)
            items.append({"name": path.name, "assignments": {app: bool(record.get(app)) for app in self.paths}, "projections": {app: self._projection(path.name, app) for app in self.paths}, "errors": {app: record.get(f"{app}Error") for app in self.paths if record.get(f"{app}Error")}})
        self._status_cache = {"version": 1, "privatePath": str(self.skills_root), "items": items}
        return self._status_cache

    def snapshot(self) -> bytes:
        self._invalidate_status()
        skills = {}
        files = {}
        for item in self.status()["items"]:
            skills[item["name"]] = self._tree_manifest_cached(self.skills_root / item["name"])
            files[item["name"]] = self._snapshot_files(self.skills_root / item["name"], skills[item["name"]])
        return self._build_snapshot(skills, files, self.state["deletions"], {name: record.get("clearedDeletions", []) for name, record in self.state["skills"].items() if name in skills})

    def skill_snapshots(self, names: set[str] | None = None) -> dict[str, bytes]:
        self._invalidate_status()
        current = {item["name"] for item in self.status()["items"]}
        available = current | set(self.state["deletions"])
        if names is not None:
            available &= names
        snapshots = {}
        for name in sorted(available):
            manifest = self._tree_manifest_cached(self.skills_root / name) if name in current else None
            snapshots[name] = self._build_snapshot(
                {name: manifest} if manifest is not None else {},
                {name: self._snapshot_files(self.skills_root / name, manifest)} if manifest is not None else {},
                {name: self.state["deletions"].get(name, [])},
                {name: self.state["skills"].get(name, {}).get("clearedDeletions", [])} if manifest is not None else {}
            )
        return snapshots

    def split_snapshot(self, data: bytes) -> dict[str, bytes]:
        manifest, files = self.inspect_snapshot(data)
        return {
            name: self._build_snapshot(
                {name: manifest["skills"][name]} if name in manifest["skills"] else {},
                {name: files[name]} if name in files else {},
                {name: manifest.get("deletions", {}).get(name, [])},
                {name: manifest.get("clearedDeletions", {}).get(name, [])} if name in manifest["skills"] else {}
            )
            for name in sorted(set(manifest["skills"]) | set(manifest.get("deletions", {})))
        }

    def combine_snapshots(self, snapshots: list[bytes]) -> bytes:
        skills, files, deletions, cleared = {}, {}, {}, {}
        for data in snapshots:
            manifest, package_files = self.inspect_snapshot(data)
            skills.update(manifest["skills"])
            files.update(package_files)
            deletions = self._merge_deletions(deletions, manifest.get("deletions", {}))
            for name, values in manifest.get("clearedDeletions", {}).items():
                cleared[name] = sorted(set(cleared.get(name, [])) | set(values))
        return self._build_snapshot(skills, files, deletions, cleared)

    def _build_snapshot(self, skills: dict, files: dict, deletions: dict, cleared_deletions: dict) -> bytes:
        _validate_portable_names(set(skills) | set(deletions) | set(cleared_deletions))
        buffer = BytesIO()
        manifest = {"version": 1, "createdAt": _timestamp(), "skills": skills, "deletions": deletions, "clearedDeletions": cleared_deletions}
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, rows in skills.items():
                for row in rows:
                    archive.writestr(f"skills/{name}/{row['path']}", files[name][row["path"]])
            archive.writestr("manifest.json", json.dumps(manifest, separators=(",", ":"), ensure_ascii=False).encode())
        return buffer.getvalue()

    def content_hash(self) -> str:
        return hashlib.sha256(json.dumps(self.content_hashes(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def content_hashes(self, names: set[str] | None = None) -> dict[str, str]:
        self._invalidate_status()
        current = {item["name"] for item in self.status()["items"]}
        available = current | set(self.state["deletions"])
        if names is not None:
            available &= names
        hashes = {}
        for name in sorted(available):
            content = {
                "skills": {name: self._tree_manifest_cached(self.skills_root / name)} if name in current else {},
                "deletions": {name: self.state["deletions"].get(name, [])},
                "clearedDeletions": {name: self.state["skills"].get(name, {}).get("clearedDeletions", [])} if name in current else {},
            }
            hashes[name] = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return hashes

    def snapshot_content_hash(self, data: bytes) -> str:
        return hashlib.sha256(json.dumps({name: self.skill_package_hash(package) for name, package in self.split_snapshot(data).items()}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def skill_package_hash(self, data: bytes) -> str:
        manifest, _ = self.inspect_snapshot(data)
        content = {"skills": manifest["skills"], "deletions": manifest.get("deletions", {}), "clearedDeletions": manifest.get("clearedDeletions", {})}
        return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def snapshot_merged_with_remote(self, remote_data: bytes, local_data: bytes) -> tuple[bytes, list[str], list[str]]:
        remote_manifest, remote_files = self.inspect_snapshot(remote_data)
        local_manifest, local_files = self.inspect_snapshot(local_data)
        deletions = self._merge_deletions(remote_manifest.get("deletions", {}), local_manifest.get("deletions", {}))
        cleared = {name: sorted(set(remote_manifest.get("clearedDeletions", {}).get(name, [])) | set(local_manifest.get("clearedDeletions", {}).get(name, []))) for name in remote_manifest["skills"].keys() | local_manifest["skills"].keys()}
        skills = {**remote_manifest["skills"], **local_manifest["skills"]}
        files = {**remote_files, **local_files}
        deleted = sorted(name for name in skills if self._active_deletion_ids(deletions, name) - set(cleared.get(name, [])))
        for name in deleted:
            skills.pop(name, None)
            files.pop(name, None)
        added = sorted(skills.keys() - remote_manifest["skills"].keys())
        updated = sorted(name for name in skills.keys() & remote_manifest["skills"].keys() if skills[name] != remote_manifest["skills"][name])
        removed = sorted(remote_manifest["skills"].keys() - skills.keys())
        data = self._build_snapshot(skills, files, deletions, {name: cleared[name] for name in skills if cleared.get(name)})
        self.inspect_snapshot(data)
        return data, added, updated, removed

    @staticmethod
    def _active_deletion_ids(deletions: dict, name: str) -> set[str]:
        return {row["id"] for row in deletions.get(name, [])}

    @staticmethod
    def _merge_deletions(*sources: dict) -> dict:
        merged = {}
        for source in sources:
            for name, rows in source.items():
                known = {row["id"] for row in merged.setdefault(name, [])}
                for row in rows:
                    if row["id"] not in known:
                        merged[name].append(row)
                        known.add(row["id"])
        return merged

    def inspect_snapshot(self, data: bytes) -> tuple[dict, dict[str, dict[str, bytes]]]:
        try:
            archive = zipfile.ZipFile(BytesIO(data))
            manifest = json.loads(archive.read("manifest.json"))
        except (zipfile.BadZipFile, KeyError, json.JSONDecodeError) as exc:
            raise SkillError("Invalid skill snapshot", 400) from exc
        if manifest.get("version") != 1 or not isinstance(manifest.get("skills"), dict):
            raise SkillError("Unsupported skill snapshot", 400)
        if not isinstance(manifest.get("deletions", {}), dict) or not isinstance(manifest.get("clearedDeletions", {}), dict):
            raise SkillError("Invalid skill deletion manifest")
        for name, rows in manifest.get("deletions", {}).items():
            _safe_name(name)
            if not isinstance(rows, list) or any(not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"] or not isinstance(row.get("deletedAt"), str) for row in rows) or len({row["id"] for row in rows}) != len(rows):
                raise SkillError("Invalid skill deletion manifest")
        for name, deletion_ids in manifest.get("clearedDeletions", {}).items():
            _safe_name(name)
            if name not in manifest["skills"] or not isinstance(deletion_ids, list) or any(not isinstance(value, str) for value in deletion_ids):
                raise SkillError("Invalid cleared skill deletions")
        files, total = {}, 0
        expected_paths = {"manifest.json"}
        if len(archive.namelist()) != len(set(archive.namelist())):
            raise SkillError("Skill snapshot contains duplicate paths")
        _validate_portable_names(set(manifest["skills"]) | set(manifest.get("deletions", {})) | set(manifest.get("clearedDeletions", {})))
        for name, rows in manifest["skills"].items():
            _safe_name(name)
            files[name] = {}
            if not isinstance(rows, list):
                raise SkillError("Invalid skill manifest")
            _validate_portable_tree_paths(str(row.get("path", "")) for row in rows if isinstance(row, dict))
            for row in rows:
                if not isinstance(row, dict):
                    raise SkillError("Invalid skill manifest")
                path = _portable_path(str(row.get("path", "")))
                archive_path = f"skills/{name}/{path.as_posix()}"
                expected_paths.add(archive_path)
                info = archive.getinfo(archive_path)
                if info.file_size > MAX_SKILL_BYTES or info.file_size > max(1024 * 1024, info.compress_size * 200):
                    raise SkillError("Skill snapshot exceeds the expansion safety limit")
                content = archive.read(archive_path)
                total += len(content)
                if len(expected_paths) > MAX_SKILL_FILES + 1 or total > MAX_SKILL_BYTES or len(content) != row.get("size") or hashlib.sha256(content).hexdigest() != row.get("sha256"):
                    raise SkillError("Skill snapshot verification failed")
                files[name][path.as_posix()] = content
            if "SKILL.md" not in files[name]:
                raise SkillError("Snapshot skill is missing SKILL.md")
        if set(archive.namelist()) != expected_paths:
            raise SkillError("Skill snapshot contains unexpected files")
        return manifest, files

    def merge(self, data: bytes) -> dict:
        manifest, files = self.inspect_snapshot(data)
        local = {item["name"]: _tree_manifest(self.skills_root / item["name"]) for item in self.status()["items"]}
        self.state["deletions"] = self._merge_deletions(self.state["deletions"], manifest.get("deletions", {}))
        remote_cleared = manifest.get("clearedDeletions", {})
        deletions = sorted(name for name in local if self._active_deletion_ids(self.state["deletions"], name) - set(self.state["skills"].get(name, {}).get("clearedDeletions", [])) - set(remote_cleared.get(name, [])))
        additions = sorted(manifest["skills"].keys() - local.keys())
        updates = sorted(name for name in manifest["skills"].keys() & local.keys() if manifest["skills"][name] != local[name])
        additions = [name for name in additions if name not in deletions]
        updates = [name for name in updates if name not in deletions]
        if not additions and not updates and not deletions:
            self._save()
            return {"added": [], "updated": [], "deleted": [], "projectionErrors": self.reconcile()}
        stage = self.private_root / f".skills.{uuid.uuid4().hex}.tmp"
        old = self.private_root / f".skills.{uuid.uuid4().hex}.old"
        try:
            shutil.copytree(self.skills_root, stage, symlinks=False)
            for name in deletions:
                shutil.rmtree(stage / name, ignore_errors=True)
            for name in additions + updates:
                target = stage / name
                if target.exists():
                    shutil.rmtree(target)
                for path, content in files[name].items():
                    output = target / Path(*PurePosixPath(path).parts)
                    output.parent.mkdir(parents=True, exist_ok=True)
                    output.write_bytes(content)
                _tree_manifest(target)
            _replace_with_retry(self.skills_root, old)
            _replace_with_retry(stage, self.skills_root)
            for name in deletions:
                for app in self.paths:
                    target = self.paths[app] / name
                    projection = self._projection(name, app)
                    if projection.get("kind") == "copy" or _is_link(target) and target.resolve(strict=False) == (self.skills_root / name).resolve(strict=False):
                        self._remove_projection(name, app)
                self.state["skills"].pop(name, None)
            for name in additions:
                self.state["skills"].setdefault(name, {"codex": False, "gemini": False, "managedAt": _timestamp(), "sourceApps": ["cloud"]})
            for name in additions + updates:
                if remote_cleared.get(name):
                    self.state["skills"][name]["clearedDeletions"] = remote_cleared[name]
            self._save()
            shutil.rmtree(old)
            errors = self.reconcile()
            self._invalidate_status(scan=True, content=True)
            return {"added": additions, "updated": updates, "deleted": deletions, "projectionErrors": errors}
        except Exception:
            shutil.rmtree(stage, ignore_errors=True)
            if old.exists() and not self.skills_root.exists():
                _replace_with_retry(old, self.skills_root)
            raise

    def restore(self, data: bytes) -> dict:
        _, files = self.inspect_snapshot(data)
        safety_root = self.private_root / "safety-backups"
        safety_root.mkdir(parents=True, exist_ok=True)
        safety_path = safety_root / f"skills-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"
        safety_path.write_bytes(self.snapshot())
        stage = self.private_root / f".skills.{uuid.uuid4().hex}.tmp"
        old = self.private_root / f".skills.{uuid.uuid4().hex}.old"
        try:
            for name, entries in files.items():
                for path, content in entries.items():
                    target = stage / name / Path(*PurePosixPath(path).parts)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(content)
            for name in files:
                _tree_manifest(stage / name)
            _replace_with_retry(self.skills_root, old)
            _replace_with_retry(stage, self.skills_root)
            surviving = set(files)
            for name in list(self.state["skills"]):
                if name not in surviving:
                    for app in self.paths:
                        target = self.paths[app] / name
                        projection = self._projection(name, app)
                        if projection.get("kind") == "copy" or _is_link(target) and target.resolve(strict=False) == (self.skills_root / name).resolve(strict=False):
                            self._remove_projection(name, app)
                    del self.state["skills"][name]
            for name in surviving:
                self.state["skills"].setdefault(name, {"codex": False, "gemini": False, "managedAt": _timestamp(), "sourceApps": ["cloud"]})
            self._save()
            shutil.rmtree(old)
            errors = self.reconcile()
            self._invalidate_status(scan=True, content=True)
            return {"restored": sorted(files), "safetyBackup": str(safety_path), "projectionErrors": errors}
        except Exception:
            shutil.rmtree(stage, ignore_errors=True)
            if old.exists() and not self.skills_root.exists():
                _replace_with_retry(old, self.skills_root)
            raise
