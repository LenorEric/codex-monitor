"""Transfer the local desktop sidebar when switching saved credentials."""

import hashlib
import json
import os
import re
import sqlite3
import subprocess
from contextlib import closing
from copy import deepcopy
from pathlib import Path

from monitor_common import atomic_write_bytes, jwt_payload


SCOPE = "electron-account-renderer-state-v1"
ATOMS = "electron-persisted-atom-state"
BRIDGE = "codex-monitor-sidebar-bridge-v1"
SCOPED_GLOBALS = (
    "open-in-target-preferences", "worktree-repository-preferences", "desktop-app-beta-preferences-by-account-id",
    "active-workspace-roots", "electron-saved-workspace-roots", "electron-workspace-root-labels", "local-projects",
    "app-server-project-id-by-legacy-project-id-by-host", "app-server-projects-migration-by-host", "app-server-pending-project-deletions-by-host",
    "selected-project", "project-writable-roots", "project-appearances", "project-files", "pinned-thread-ids",
    "app-server-migrated-pinned-thread-ids-by-host", "pinned-project-ids", "sidebar-project-thread-orders", "sidebar-thread-metadata",
    "thread-project-assignments", "thread-project-membership-host-ids", "thread-writable-roots", "thread-workspace-root-hints",
    "thread-projectless-output-directories", "projectless-thread-ids", "selected-remote-host-id", "remote-projects", "active-remote-project-id",
    "project-order", "connection-group-order", "remote-cwds-by-host-and-workspace", "host-id-remote-control-allowed",
    "added-remote-control-env-ids", "remote-project-connection-backfill-completed", "remote-connection-auto-connect-by-host-id", "queued-follow-ups",
)
MAP_KEYS = ("local-projects", "project-appearances", "thread-project-assignments", "thread-project-membership-host-ids", "sidebar-project-thread-orders")
LIST_KEYS = ("project-order", "pinned-project-ids", "pinned-thread-ids", "projectless-thread-ids")
PROJECT_MAP = "app-server-project-id-by-legacy-project-id-by-host"
MIGRATION = "app-server-projects-migration-by-host"
BACKING_KEYS = ("chatGptBacking", "chatGptBackingCreation", "retiredChatGptBacking")
ASSIGNMENTS = "thread-project-assignments"
HOSTS = "thread-project-membership-host-ids"
PROJECTLESS = "projectless-thread-ids"


class DesktopSidebarError(RuntimeError):
    pass


def _object(value):
    if not isinstance(value, dict):
        raise DesktopSidebarError("The desktop sidebar contains an invalid object.")
    return value


def _ids(value):
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise DesktopSidebarError("The desktop sidebar contains an invalid ID list.")
    return value


def _ordered(source, destination):
    return list(dict.fromkeys([*source, *destination]))


def _owner(auth):
    tokens = _object(auth.get("tokens"))
    token = tokens.get("access_token")
    claims = jwt_payload(token) if isinstance(token, str) else {}
    claims = _object(_object(claims).get("https://api.openai.com/auth"))
    account = claims.get("chatgpt_account_id") or claims.get("account_id")
    user = claims.get("user_id") or claims.get("chatgpt_user_id")
    if not all(isinstance(value, str) and value for value in (user, account)) or (tokens.get("account_id") or account) != account:
        raise DesktopSidebarError("The saved sign-in does not identify a desktop account and workspace.")
    return hashlib.sha256(json.dumps([user, account], ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


def _scope(value):
    scope = _object(value)
    owner = scope.get("owner")
    inactive = _object(scope.get("inactive"))
    if "owner" not in scope or any(not isinstance(key, str) or not re.fullmatch("[a-f0-9]{64}", key) for key in (*inactive, *([owner] if owner is not None else []))):
        raise DesktopSidebarError("The desktop sidebar contains an invalid account owner.")
    if owner is not None and owner in inactive:
        raise DesktopSidebarError("The desktop sidebar has two snapshots for the active account.")
    for snapshot in inactive.values():
        _object(_object(snapshot).get("globals"))
        _object(snapshot.get("atoms"))
    return scope


def _local_catalog(path):
    if not path.is_file():
        return set(), set(), set()
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as database:
        tables = {name for name, in database.execute("SELECT name FROM sqlite_schema WHERE type = 'table' AND name IN ('projects', 'threads')")}
        projects, backed_projects = set(), set()
        if "projects" in tables:
            for project_id, metadata in database.execute("SELECT id, metadata FROM projects"):
                metadata = _object(json.loads(metadata or "{}"))
                (backed_projects if any("codex." + key in metadata for key in BACKING_KEYS) else projects).add(project_id)
        threads = {thread_id for thread_id, in database.execute("SELECT id FROM threads")} if "threads" in tables else set()
        return projects, backed_projects, threads


def _local_layout(globals_, catalog):
    server_projects, backed_projects, local_threads = catalog
    layout = {}
    projects = _object(globals_.get("local-projects", {}))
    host_mappings = {host: _object(mapping) for host, mapping in _object(globals_.get(PROJECT_MAP, {})).items() if host.startswith("local:")}
    backed_ids = backed_projects | {
        key for mapping in host_mappings.values() for key, value in mapping.items() if isinstance(value, str) and value in backed_projects
    }
    local_projects = {}
    for project_id, project in projects.items():
        if project_id not in backed_ids and not any(key in _object(project) for key in BACKING_KEYS):
            _ids(project.get("rootPaths"))
            local_projects[project_id] = project
    project_ids = set(local_projects) | (server_projects - projects.keys())
    mappings = {
        host: {key: value for key, value in mapping.items() if key in local_projects and isinstance(value, str) and value in server_projects}
        for host, mapping in host_mappings.items()
    }
    layout[PROJECT_MAP] = {key: value for key, value in mappings.items() if value}
    layout["local-projects"] = local_projects
    layout["project-appearances"] = {key: value for key, value in _object(globals_.get("project-appearances", {})).items() if key in project_ids}
    hosts = _object(globals_.get(HOSTS, {}))
    assignments = _object(globals_.get(ASSIGNMENTS, {}))
    layout[ASSIGNMENTS] = {
        key: value for key, value in assignments.items()
        if _object(value).get("projectKind") == "local" and isinstance(value.get("projectId"), str)
        and value["projectId"] in project_ids and hosts.get(key, "local") == "local"
    }
    # Explicit local memberships are also evidence on older builds without a thread catalog.
    local_threads = (local_threads - assignments.keys()) | layout[ASSIGNMENTS].keys()
    local_threads = {key for key in local_threads if hosts.get(key, "local") == "local"}
    layout[HOSTS] = {key: value for key, value in hosts.items() if key in local_threads}
    for key in LIST_KEYS:
        allowed = project_ids if key in ("project-order", "pinned-project-ids") else local_threads
        layout[key] = [item for item in _ids(globals_.get(key, [])) if item in allowed]
    layout["sidebar-project-thread-orders"] = {
        key: {**_object(value), "threadIds": [item for item in _ids(value.get("threadIds")) if item in local_threads]}
        for key, value in _object(globals_.get("sidebar-project-thread-orders", {})).items() if key in project_ids
    }
    selected = _object(globals_.get("selected-project") or {})
    if selected.get("type") == "local" and isinstance(selected.get("projectId"), str) and selected["projectId"] in project_ids:
        layout["selected-project"] = selected
    return {key: value for key, value in layout.items() if value}


def _merge_layout(source, destination):
    merged = dict(destination)
    for key in MAP_KEYS:
        if key in source:
            merged[key] = {**_object(destination.get(key, {})), **source[key]}
    if "sidebar-project-thread-orders" in source:
        orders = _object(destination.get("sidebar-project-thread-orders", {}))
        for project_id, order in source["sidebar-project-thread-orders"].items():
            merged["sidebar-project-thread-orders"][project_id] = {
                **order, "threadIds": _ordered(order["threadIds"], _ids(_object(orders.get(project_id, {})).get("threadIds", []))),
            }
    for key in LIST_KEYS:
        if key in source:
            merged[key] = _ordered(source[key], _ids(destination.get(key, [])))
    if PROJECT_MAP in source:
        mappings = dict(_object(destination.get(PROJECT_MAP, {})))
        for host, mapping in source[PROJECT_MAP].items():
            mappings[host] = {**_object(mappings.get(host, {})), **mapping}
        merged[PROJECT_MAP] = mappings
    if "selected-project" in source:
        merged["selected-project"] = source["selected-project"]

    # An explicit assignment and an explicit projectless choice are mutually exclusive.
    source_assignments = source.get(ASSIGNMENTS, {})
    projectless = set(source.get(PROJECTLESS, [])) - source_assignments.keys()
    if projectless and ASSIGNMENTS in merged:
        merged[ASSIGNMENTS] = {key: value for key, value in _object(merged.get(ASSIGNMENTS, {})).items() if key not in projectless}
    if HOSTS in merged:
        merged[HOSTS] = {key: value for key, value in _object(merged[HOSTS]).items() if key not in (projectless | source_assignments.keys()) or key in source.get(HOSTS, {})}
    if PROJECTLESS in merged:
        merged[PROJECTLESS] = [key for key in _ids(merged[PROJECTLESS]) if key not in source_assignments]
    if MIGRATION in destination:
        previous = _object(destination.get(ASSIGNMENTS, {}))
        previous_projectless = set(_ids(destination.get(PROJECTLESS, [])))
        changed = sorted({key for key, value in source_assignments.items() if previous.get(key) != value} | (projectless - previous_projectless))
        if changed:
            migrations = dict(_object(destination[MIGRATION]))
            for host, migration in migrations.items():
                if host.startswith("local:") and _object(migration).get("version") == 1:
                    migrations[host] = {
                        **migration, "threadAssignmentsMigrated": False,
                        "pendingThreadAssignmentIds": _ordered(_ids(migration.get("pendingThreadAssignmentIds", [])), changed),
                    }
            merged[MIGRATION] = migrations
    return merged


def _account_globals(state, scope, owner):
    if scope["owner"] == owner:
        return {key: state[key] for key in SCOPED_GLOBALS if key in state}
    return scope["inactive"].get(owner, {}).get("globals")


def _put_account_globals(state, scope, owner, globals_):
    if scope["owner"] == owner:
        for key in SCOPED_GLOBALS:
            state.pop(key, None)
        state.update(globals_)
    else:
        snapshot = scope["inactive"].setdefault(owner, {"atoms": {}, "globals": {}})
        snapshot["globals"] = globals_


def _restore_parked(state, parked):
    parked = _object(parked)
    scope = deepcopy(_scope(parked.get(SCOPE)))
    if ATOMS in parked:
        _object(parked[ATOMS])
    for key in (*SCOPED_GLOBALS, ATOMS):
        state.pop(key, None)
        if key in parked:
            state[key] = parked[key]
    state[SCOPE] = scope
    return scope


def _desktop_running():
    if os.name == "nt":
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
             "$ErrorActionPreference = 'Stop'; @(Get-Process -Name ChatGPT,Codex -ErrorAction SilentlyContinue | "
             "Where-Object { $_.ProcessName -eq 'ChatGPT' -or $_.Path -like '*WindowsApps*OpenAI.Codex*\\app\\*' "
             "-or ($_.Path -and (Test-Path -LiteralPath (Join-Path (Split-Path $_.Path) 'resources/app.asar'))) }).Count"],
            capture_output=True, text=True, check=True, timeout=10, creationflags=subprocess.CREATE_NO_WINDOW,
        )
        return int(result.stdout.strip()) > 0
    for process in Path("/proc").iterdir():
        if not process.name.isdecimal():
            continue
        try:
            name = (process / "comm").read_text().strip().lower()
            if name in ("chatgpt", "codex-desktop", "chatgpt-desktop"):
                return True
            if name in ("codex", "electron"):
                executable = (process / "exe").readlink()
                if (executable.parent / "resources" / "app.asar").is_file():
                    return True
                if b"codex" in (process / "cmdline").read_bytes().lower() and name == "electron":
                    return True
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return False


def _require_closed_desktop():
    try:
        running = _desktop_running()
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        raise DesktopSidebarError("Could not verify that the ChatGPT desktop app is closed.") from exc
    if running:
        raise DesktopSidebarError("Fully quit the ChatGPT desktop app before switching accounts, then reopen it after the switch.")


class DesktopSidebarEdit:
    def __init__(self, path, original, updated):
        self.path = path
        self.original = original
        self.updated = updated

    def apply(self):
        _require_closed_desktop()
        if self.path.read_bytes() != self.original:
            raise DesktopSidebarError("The desktop sidebar changed during the switch. Quit the desktop and retry.")
        atomic_write_bytes(self.path, self.updated, private=True, replace_attempts=5)

    def restore(self):
        try:
            if self.path.read_bytes() != self.updated:
                return False
        except FileNotFoundError:
            return False
        atomic_write_bytes(self.path, self.original, private=True, replace_attempts=5)
        return True


def prepare_desktop_sidebar(auth_path, outgoing, incoming, *, outgoing_api, incoming_api):
    if outgoing_api and incoming_api:
        return None
    path = auth_path.with_name(".codex-global-state.json")
    if not path.is_file():
        return None
    try:
        original = path.read_bytes()
        original_state = _object(json.loads(original))
        state = dict(original_state)
        scope = _scope(state[SCOPE]) if SCOPE in state else None
        if scope is not None:
            # The prepared edit owns this copy until apply().
            scope = deepcopy(scope)
            state[SCOPE] = scope
        if ATOMS in state:
            _object(state[ATOMS])
        bridge = _object(state.get(BRIDGE, {}))
        parked = bridge.get("parked")
        if parked is not None and scope is not None:
            raise DesktopSidebarError("The desktop sidebar does not match the active login.")
        if outgoing_api and scope and scope["owner"] is not None and "apiLayout" not in bridge:
            # API credentials were selected without starting the desktop in API mode.
            return None
        if not scope and not parked and not incoming_api:
            return None

        catalog = _local_catalog(auth_path.with_name("state_5.sqlite"))
        if parked is not None:
            api_layout = _local_layout(state, catalog)
            scope = _restore_parked(state, parked)
            bridge = state[BRIDGE] = {"apiLayout": api_layout}
        if outgoing_api:
            if parked is None:
                api_layout = _local_layout(_object(bridge.get("apiLayout", {})), catalog)
                if scope["owner"] is None:
                    api_layout = _merge_layout(_local_layout(state, catalog), api_layout)
            owner = _owner(incoming)
            destination = _account_globals(state, scope, owner) or {}
            _put_account_globals(state, scope, owner, _merge_layout(api_layout, destination))
            state[BRIDGE] = {"apiLayout": api_layout}
        else:
            owner = _owner(outgoing)
            destination_owner = _owner(incoming) if not incoming_api else None
            if owner == destination_owner:
                return None
            if scope is None:
                scope = {"owner": owner, "inactive": {}}
            source = _account_globals(state, scope, owner)
            if source is None:
                raise DesktopSidebarError("Open the desktop with the current account once, then quit it and retry the switch.")
            local = _local_layout(source, catalog)
            if incoming_api:
                state[BRIDGE] = {"parked": {SCOPE: scope, **{key: state.pop(key) for key in (*SCOPED_GLOBALS, ATOMS) if key in state}}}
                state.pop(SCOPE, None)
                state.update(_merge_layout(local, _local_layout(_object(bridge.get("apiLayout", {})), catalog)))
            else:
                destination = _account_globals(state, scope, destination_owner) or {}
                _put_account_globals(state, scope, destination_owner, _merge_layout(local, destination))
        updated = json.dumps(state, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if state == original_state:
            return None
        return DesktopSidebarEdit(path, original, updated)
    except (ValueError, sqlite3.Error, OSError) as exc:
        raise DesktopSidebarError("Could not read the desktop sidebar or its local project index.") from exc
