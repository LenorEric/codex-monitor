import base64
import json
import shutil
import sqlite3
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from monitor_accounts import AccountError, AccountManager, atomic_write_bytes
from monitor_desktop_sidebar import ASSIGNMENTS, ATOMS, BRIDGE, HOSTS, PROJECT_MAP, PROJECTLESS, SCOPE, _owner, prepare_desktop_sidebar


def openai_auth(name):
    claims = {"https://api.openai.com/auth": {"user_id": f"user-{name}", "chatgpt_account_id": f"account-{name}"}}
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return {"tokens": {"account_id": f"account-{name}", "access_token": f"fixture.{payload}.fixture", "refresh_token": "fixture"}}


def project(project_id, name, **metadata):
    return {"id": project_id, "name": name, "rootPaths": [f"/fixture/{project_id}"], "createdAt": 1, "updatedAt": 1, **metadata}


def assign(project_id, kind="local"):
    return {"projectKind": kind, "projectId": project_id}


class DesktopSidebarTests(unittest.TestCase):
    def setUp(self):
        self.root = Path("tmp") / f"test-desktop-sidebar-{uuid.uuid4().hex}"
        self.root.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.root)
        self.auth = self.root / "auth.json"
        self.state = self.root / ".codex-global-state.json"
        self.db = self.root / "state_5.sqlite"
        self.a = openai_auth("a")
        self.b = openai_auth("b")
        self.api = {"OPENAI_API_KEY": "fixture-api-key"}
        self.owner_a, self.owner_b = _owner(self.a), _owner(self.b)
        self.auth.write_text(json.dumps(self.a), encoding="utf-8")
        self.manager = AccountManager(self.auth, root=self.root / "accounts")
        for slot, auth in (("b", self.b), ("api", self.api), ("api-2", {"OPENAI_API_KEY": "fixture-api-key-2"})):
            data = json.dumps(auth).encode()
            atomic_write_bytes(self.manager._account_path(slot), data)
            self.manager.manifest["accounts"].append(self.manager._new_record(slot, slot, True, data))
        self.manager._save_manifest()
        self.desktop = self.enterContext(patch("monitor_desktop_sidebar._desktop_running", return_value=False))
        with closing(sqlite3.connect(self.db)) as database, database:
            database.execute("CREATE TABLE projects (id TEXT PRIMARY KEY, metadata TEXT)")
            database.executemany("INSERT INTO projects VALUES (?, ?)", [("server-shared", "{}"), ("server-cloud", '{"codex.chatGptBacking":"fixture"}')])
            database.execute("CREATE TABLE threads (id TEXT PRIMARY KEY, archived INTEGER)")
            database.executemany("INSERT INTO threads VALUES (?, ?)", ((f"chat-{i}", i == 204) for i in range(205)))

    def initial_state(self):
        return {
            SCOPE: {"owner": self.owner_a, "inactive": {self.owner_b: {
                "atoms": {"account-private": "B"},
                "globals": {
                    "local-projects": {"b": project("b", "B"), "shared": project("shared", "Shared B", bOnly=True)},
                    "project-order": ["b", "shared"],
                    ASSIGNMENTS: {"chat-0": assign("b"), "b-chat": assign("b"), "chat-2": assign("b")},
                    PROJECTLESS: ["chat-1"], "remote-projects": {"keep": {"name": "B remote"}},
                },
            }}},
            ATOMS: {"chatgpt-sidebar-state-v2": {"private": "A"}},
            "local-projects": {
                "shared": project("shared", "Shared A"), "a": project("a", "A"),
                "cloud": project("cloud", "Cloud", chatGptBacking={"accountId": "A"}),
                "linked": project("linked", "Cloud linked by server"),
            },
            "project-order": ["a", "shared", "cloud", "linked"],
            ASSIGNMENTS: {"chat-0": assign("shared"), "chat-1": assign("a"), "cloud-chat": assign("cloud"), "linked-chat": assign("linked"), "remote-chat": assign("a")},
            HOSTS: {"chat-0": "local", "remote-chat": "remote-fixture"},
            PROJECT_MAP: {"local:fixture": {"shared": "server-shared", "linked": "server-cloud"}},
            PROJECTLESS: ["chat-2", *(f"chat-{i}" for i in range(3, 205)), "foreign-chat"],
            "remote-projects": {"private": {"name": "A remote"}},
            "unrelated": {"keep": True},
        }

    def save(self, state):
        self.state.write_text(json.dumps(state), encoding="utf-8")

    def load(self):
        return json.loads(self.state.read_bytes())

    def test_openai_round_trip_merges_all_local_memberships_by_id(self):
        original = self.initial_state()
        self.save(original)
        original_db = self.db.read_bytes()
        self.manager.switch("b")
        state = self.load()
        merged = state[SCOPE]["inactive"][self.owner_b]["globals"]
        self.assertEqual(merged["project-order"], ["a", "shared", "b"])
        self.assertEqual(merged["local-projects"]["shared"], project("shared", "Shared A"))
        self.assertEqual(merged[ASSIGNMENTS], {"chat-0": assign("shared"), "chat-1": assign("a"), "b-chat": assign("b")})
        self.assertEqual(len(merged[PROJECTLESS]), 203)
        self.assertIn("chat-204", merged[PROJECTLESS])
        self.assertEqual(merged[PROJECT_MAP], {"local:fixture": {"shared": "server-shared"}})
        self.assertEqual(state[ATOMS], original[ATOMS])
        self.assertEqual(merged["remote-projects"], {"keep": {"name": "B remote"}})
        self.manager.switch("ppl-pro")
        self.assertEqual(self.load()["project-order"], ["a", "shared", "b", "cloud", "linked"])
        self.assertEqual(self.load()[ASSIGNMENTS]["cloud-chat"], assign("cloud"))
        self.assertEqual(self.db.read_bytes(), original_db)

    def test_api_round_trip_parks_account_scope_and_keeps_api_edits(self):
        original = self.initial_state()
        self.save(original)
        archived = self.root / "archived_sessions"
        archived.mkdir()
        history = archived / "rollout.jsonl"
        history.write_bytes(b'{"model_provider":"openai"}\n')
        self.manager.switch("api")
        api_state = self.load()
        self.assertNotIn(SCOPE, api_state)
        self.assertNotIn(ATOMS, api_state)
        self.assertNotIn("remote-projects", api_state)
        self.assertEqual(api_state[BRIDGE]["parked"][SCOPE], original[SCOPE])
        self.assertEqual(api_state["project-order"], ["a", "shared"])
        self.assertEqual(set(api_state[ASSIGNMENTS]), {"chat-0", "chat-1"})
        api_state["local-projects"]["api-added"] = project("api-added", "Added in API mode")
        api_state["project-order"].insert(0, "api-added")
        self.save(api_state)
        self.manager.switch("api-2")
        self.assertEqual(self.load(), api_state)
        self.manager.switch("b")
        state = self.load()
        self.assertEqual(state[ATOMS], original[ATOMS])
        self.assertEqual(state[SCOPE]["owner"], self.owner_a)
        self.assertEqual(state[SCOPE]["inactive"][self.owner_b]["atoms"], {"account-private": "B"})
        self.assertEqual(state[SCOPE]["inactive"][self.owner_b]["globals"]["project-order"], ["api-added", "a", "shared", "b"])
        self.assertEqual(history.read_bytes(), b'{"model_provider":"openai"}\n')
        self.manager.switch("api")
        self.assertIn("api-added", self.load()["local-projects"])

    def test_running_desktop_and_malformed_state_leave_credentials_untouched(self):
        self.save(self.initial_state())
        original = self.state.read_bytes()
        self.desktop.return_value = True
        with self.assertRaisesRegex(AccountError, "Fully quit"):
            self.manager.switch("api")
        self.assertEqual(self.state.read_bytes(), original)
        self.assertEqual(json.loads(self.auth.read_bytes()), self.a)
        self.desktop.return_value = False
        self.state.write_bytes(b"{")
        with self.assertRaisesRegex(AccountError, "Could not read the desktop sidebar"):
            self.manager.switch("api")
        self.assertEqual(self.state.read_bytes(), b"{")
        self.assertEqual(json.loads(self.auth.read_bytes()), self.a)

    def test_failed_credential_write_rolls_back_only_the_staged_sidebar(self):
        self.save(self.initial_state())
        original = self.state.read_bytes()
        for newer in (None, b'{"newer":true}'):
            def fail_incoming(path, data):
                if path == self.auth and json.loads(data) == self.api:
                    if newer:
                        self.state.write_bytes(newer)
                    raise OSError("fixture credential failure")
                atomic_write_bytes(path, data)

            with patch("monitor_accounts.atomic_write_bytes", side_effect=fail_incoming), self.assertRaisesRegex(AccountError, "fixture credential failure"):
                self.manager.switch("api")
            self.assertEqual(self.state.read_bytes(), newer or original)
            self.assertEqual(json.loads(self.auth.read_bytes()), self.a)

    def test_retry_after_sidebar_was_staged_before_api_credentials(self):
        self.save(self.initial_state())
        edit = prepare_desktop_sidebar(self.auth, self.a, self.api, outgoing_api=False, incoming_api=True)
        edit.apply()
        staged = self.load()
        self.assertEqual(json.loads(self.auth.read_bytes()), self.a)
        self.manager.switch("api")
        self.assertEqual(self.load(), staged)
        self.assertEqual(json.loads(self.auth.read_bytes()), self.api)
