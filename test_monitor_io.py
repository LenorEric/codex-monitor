import json
import shutil
import unittest
import uuid
from pathlib import Path
from unittest import mock

import monitor_common


class AtomicWriteTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parent / "tmp" / f"atomic-test-{uuid.uuid4().hex}"
        self.root.mkdir(parents=True)
        self.addCleanup(self.cleanup)
        self.path = self.root / "data.json"
        self.path.write_bytes(b"original")

    def cleanup(self):
        if not self.root.resolve().is_relative_to(Path(__file__).resolve().parent / "tmp"):
            raise RuntimeError("Unexpected test directory")
        shutil.rmtree(self.root)

    def test_json_serialization_preserves_existing_ascii_format(self):
        value = {"label": "中文", "nested": [1, True]}
        monitor_common.write_json(self.path, value)
        self.assertEqual(self.path.read_bytes(), (json.dumps(value, indent=2) + "\n").encode("utf-8"))

    def test_replace_failure_preserves_original_and_cleans_temporary(self):
        with mock.patch.object(monitor_common.os, "replace", side_effect=OSError("blocked")):
            with self.assertRaises(OSError):
                monitor_common.atomic_write_bytes(self.path, b"replacement")
        self.assertEqual(self.path.read_bytes(), b"original")
        self.assertEqual(list(self.root.iterdir()), [self.path])

    def test_fsync_failure_preserves_original_and_cleans_temporary(self):
        with mock.patch.object(monitor_common.os, "fsync", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                monitor_common.atomic_write_bytes(self.path, b"replacement")
        self.assertEqual(self.path.read_bytes(), b"original")
        self.assertEqual(list(self.root.iterdir()), [self.path])

    def test_interruption_preserves_original_and_cleans_temporary(self):
        with mock.patch.object(monitor_common.os, "replace", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                monitor_common.atomic_write_bytes(self.path, b"replacement")
        self.assertEqual(self.path.read_bytes(), b"original")
        self.assertEqual(list(self.root.iterdir()), [self.path])

    def test_private_windows_writer_retries_permission_errors(self):
        replace = monitor_common.os.replace
        attempts = []

        def transient_replace(source, target):
            attempts.append(source)
            if len(attempts) == 1:
                raise PermissionError("temporarily busy")
            replace(source, target)

        with mock.patch.object(monitor_common.os, "name", "nt"), mock.patch.object(monitor_common.os, "replace", side_effect=transient_replace), mock.patch.object(monitor_common.os, "chmod") as chmod, mock.patch.object(monitor_common.time, "sleep") as sleep:
            monitor_common.atomic_write_bytes(self.path, b"replacement", private=True, replace_attempts=5)
        self.assertEqual(self.path.read_bytes(), b"replacement")
        self.assertEqual(len(attempts), 2)
        chmod.assert_called_once_with(attempts[0], 0o600)
        sleep.assert_called_once_with(0.02)

    def test_default_writer_does_not_retry_permission_errors(self):
        with mock.patch.object(monitor_common.os, "replace", side_effect=PermissionError("busy")) as replace:
            with self.assertRaises(PermissionError):
                monitor_common.atomic_write_bytes(self.path, b"replacement")
        self.assertEqual(replace.call_count, 1)
        self.assertEqual(self.path.read_bytes(), b"original")


if __name__ == "__main__":
    unittest.main()
