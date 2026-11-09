import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import build_release


class ReleaseTests(unittest.TestCase):
    @patch("build_release.bump_package_version")
    @patch("build_release.subprocess.run")
    @patch("build_release.shutil.which", return_value="git")
    def test_clean_tree_skips_release(self, _which, run, bump):
        run.return_value = SimpleNamespace(returncode=0)
        build_release.main()
        bump.assert_not_called()
        self.assertEqual(run.call_count, 2)

    @patch("build_release.commit_release")
    @patch("build_release.build_release")
    @patch("build_release.bump_package_version", return_value="1.2.3")
    @patch("build_release.validate_runtime_files")
    @patch("build_release.validate_contract_metadata")
    @patch("build_release.subprocess.run")
    @patch("build_release.shutil.which", return_value="git")
    def test_staged_change_builds_release(self, _which, run, _metadata, _runtime, _bump, build, commit):
        run.side_effect = [SimpleNamespace(returncode=0), SimpleNamespace(returncode=1)]
        build_release.main()
        build.assert_called_once_with("1.2.3")
        commit.assert_called_once_with("1.2.3")

    @patch("build_release.build_release_zip")
    @patch("build_release.build_vsix")
    @patch("build_release.write_release_version")
    @patch("build_release.rebuild_runtime")
    def test_runtime_is_copied_before_packaging(self, rebuild, manifest, vsix, archive):
        calls = []
        rebuild.side_effect = lambda *_: calls.append("copy")
        manifest.side_effect = lambda *_: calls.append("manifest")
        vsix.side_effect = lambda *_: calls.append("vsix")
        archive.return_value = "archive"
        with patch("build_release.RELEASE_DIR", MagicMock()):
            build_release.build_release("1.2.3")
        self.assertEqual(calls, ["copy", "manifest", "vsix"])


if __name__ == "__main__":
    unittest.main()
