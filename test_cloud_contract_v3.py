import hashlib
import json
import threading
import unittest
import uuid
import shutil
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from monitor_cloud import CloudError, CloudManager, passphrase_hash
from monitor_usage_sync import content_hash


class CloudContractV3Tests(unittest.TestCase):
    @contextmanager
    def directory(self):
        path = Path(__file__).parent / f".cloud-v3-test-{uuid.uuid4().hex}"
        path.mkdir()
        try:
            yield path
        finally:
            shutil.rmtree(path, ignore_errors=True)

    @staticmethod
    def manager(root: Path, accounts=None) -> CloudManager:
        return CloudManager(root / "private", SimpleNamespace(), accounts)

    def test_usage_account_id_is_stable_across_passphrases_without_legacy_readers(self):
        with self.directory() as directory:
            accounts = SimpleNamespace(lock=threading.RLock(), manifest={"accounts": [{"id": "slot-a", "label": "A", "identity": {"accountId": "acct-a", "idTokenHash": "token-a"}}]})
            cloud = self.manager(directory, accounts)
            cloud._config["webdav"]["encryptionPassphraseHash"] = passphrase_hash("old secret", "https://example.test/dav", "user")
            stable = cloud.usage_account_id("slot-a")
            cloud._config["webdav"]["encryptionPassphraseHash"] = passphrase_hash("new secret", "https://example.test/dav", "user")
            cloud._usage_account_ids.clear()

            self.assertTrue(stable.startswith("v3:"))
            self.assertEqual(cloud.usage_account_id("slot-a"), stable)
            self.assertFalse(hasattr(cloud, "_legacy_usage_account_id"))
            self.assertFalse(hasattr(cloud, "usage_account_aliases"))



    def test_force_verification_uses_v4_publication(self):
        with self.directory() as directory:
            cloud = self.manager(directory)
            store = SimpleNamespace()
            cloud._usage_data = store
            with mock.patch.object(cloud, "_require_conditional_writes"), mock.patch.object(cloud, "_connection", return_value=(SimpleNamespace(transfers=[]), object())), mock.patch.object(cloud, "_v4_publish_retry", return_value={}) as publish:
                cloud._push_usage_data(True)

            publish.assert_called_once()
            self.assertTrue(publish.call_args.args[-1])





if __name__ == "__main__":
    unittest.main()
