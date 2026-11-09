import sqlite3
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import mock

import monitor_packages
from monitor_cloud import CloudError


class PackageSyncTests(unittest.TestCase):
    def test_fetch_skips_received_packages_and_imports_unseen_packages(self):
        db = sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        db.execute('CREATE TABLE package_receipts(destination TEXT, machine TEXT, package TEXT)')
        db.execute('INSERT INTO package_receipts VALUES(?,?,?)', ('destination', 'remote', 'seen'))

        @contextmanager
        def transaction():
            yield db

        store = SimpleNamespace(index=SimpleNamespace(transaction=transaction), machine_id='local', import_package=mock.Mock())
        manager = SimpleNamespace(_usage_data=store, _optional_remote_list=mock.Mock(side_effect=[['local', 'remote'], ['seen.enc', 'new.enc']]))
        with mock.patch.object(monitor_packages, 'destination', return_value='destination'), mock.patch.object(monitor_packages, '_descriptor', return_value={}) as descriptor, mock.patch.object(monitor_packages, 'records', return_value=[{'row': {}}]):
            result = monitor_packages.fetch(manager, object(), object())
        self.assertEqual(result['packagesDownloaded'], 1)
        self.assertEqual(descriptor.call_count, 1)
        store.import_package.assert_called_once_with('destination', 'remote', 'new', [{'row': {}}])

    def test_upload_still_runs_when_download_fails(self):
        with mock.patch.object(monitor_packages, 'fetch', side_effect=ValueError('offline')), mock.patch.object(monitor_packages, 'push', return_value={'packagesUploaded': 1}) as push:
            with self.assertRaisesRegex(CloudError, 'partially completed'):
                monitor_packages.sync(object(), object(), object())
        push.assert_called_once()


if __name__ == '__main__':
    unittest.main()
