import base64
import hashlib
import json
import shutil
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from monitor_accounts import auth_identity
from monitor_cloud import CloudError, CloudManager, CryptoBox
from monitor_skills import SkillManager
from test_usage_v4 import MemoryWebDav


class CloudAccountTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / 'tmp' / ('account-test-' + uuid.uuid4().hex)
        self.root.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.root)
        self.cloud = CloudManager(self.root / 'private', SkillManager(self.root / 'codex', self.root / 'private', self.root / 'gemini'), None)
        self.addCleanup(self.cloud.operation_queue.close)
        self.client = MemoryWebDav()
        self.box = CryptoBox('', CryptoBox.descriptor(''))
        self.cloud._connection = lambda initialize=False: (self.client, self.box)
        self.auth = json.dumps({'auth_mode': 'apikey', 'OPENAI_API_KEY': 'sk-isolated-test'}).encode()
        self.identity = auth_identity(json.loads(self.auth))

    def publish(self):
        return self.cloud.release_account('account', self.auth, self.identity, 'API', account_type='api', config_header_toml='model = "test"\n')

    def test_publish_bind_list_and_delete_use_one_account_file(self):
        published = self.publish()
        self.assertEqual(set(self.client.files), {'accounts/account.enc'})
        self.assertNotIn('revisionId', published)
        with mock.patch.object(self.client, 'get', wraps=self.client.get) as get:
            state, auth, etag = self.cloud.bind_account('account')
        get.assert_called_once_with('accounts/account.enc')
        self.assertEqual(auth, self.auth)
        self.assertEqual(state['configHeaderToml'], 'model = "test"\n')
        self.assertNotIn('auth', state)
        self.assertEqual(self.cloud.list_accounts()[0]['authSha256'], hashlib.sha256(self.auth).hexdigest())
        self.cloud.delete_account_payloads('account', expected_etag=etag)
        self.assertEqual(self.client.files, {})
        self.assertTrue(self.cloud.delete_account_payloads('account')['alreadyDeleted'])

    def test_unchanged_release_is_idempotent_and_conflicts_preserve_original(self):
        self.publish()
        before = dict(self.client.files)
        self.assertFalse(self.publish()['changed'])
        changed = json.dumps({'auth_mode': 'apikey', 'OPENAI_API_KEY': 'sk-other'}).encode()
        with self.assertRaises(CloudError):
            self.cloud.release_account('account', changed, auth_identity(json.loads(changed)), 'API', account_type='api')
        with self.assertRaisesRegex(CloudError, 'already shared'):
            self.cloud.release_account('account', self.auth, self.identity, 'API', account_type='api', reject_existing=True)
        self.assertEqual(self.client.files, before)

    def test_remote_success_before_lost_response_recovers_without_extra_object(self):
        put = self.client.put
        def lose_response(*args, **kwargs):
            put(*args, **kwargs)
            raise CloudError('response lost', 502)
        with mock.patch.object(self.client, 'put', side_effect=lose_response), self.assertRaises(CloudError):
            self.publish()
        self.assertFalse(self.publish()['changed'])
        self.assertEqual(set(self.client.files), {'accounts/account.enc'})

    def test_invalid_credentials_checksum_and_identity_are_rejected(self):
        self.publish()
        payload = json.loads(self.client.get('accounts/account.enc')[0])
        for malformed in ([], payload | {'authSha256': '0' * 64}, payload | {'accountKey': 'other'}, payload | {'accountId': 'wrong'},
                payload | {'auth': base64.b64encode(b'broken json').decode(), 'authSize': 11, 'authSha256': hashlib.sha256(b'broken json').hexdigest()}):
            with self.subTest(malformed=type(malformed).__name__), self.assertRaises(CloudError):
                self.cloud._decode_account(self.box, 'account', json.dumps(malformed).encode())

    def test_changed_etag_prevents_delete_and_metadata_cache_excludes_auth(self):
        self.publish()
        before = dict(self.client.files)
        with self.assertRaisesRegex(CloudError, 'changed'):
            self.cloud.delete_account_payloads('account', expected_etag='outdated')
        self.assertEqual(self.client.files, before)
        saved = self.cloud.state_path.read_text(encoding='utf-8')
        self.assertNotIn(base64.b64encode(self.auth).decode(), saved)
        self.assertNotIn('sk-isolated-test', saved)
        self.assertFalse(hasattr(self.cloud, '_upload_revision'))
        self.assertFalse(hasattr(self.cloud, '_cleanup_account_revisions'))

    def test_empty_account_and_unsafe_paths(self):
        self.cloud.release_account('empty', b'{}', {}, 'Empty', ready=False)
        state, auth, _ = self.cloud.bind_account('empty')
        self.assertFalse(state['ready'])
        self.assertEqual(auth, b'{}')
        for key in ('../outside', 'nested/key', ''):
            with self.assertRaises(CloudError):
                self.cloud.bind_account(key)
