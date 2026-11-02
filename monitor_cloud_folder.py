"""Private filesystem transport for offline cloud migration and isolated fixtures.

Operate on a verified working copy, never on an actively synchronized folder.
"""

import hashlib
import os
from pathlib import Path
import shutil
import tempfile

from monitor_cloud import CloudError


class FolderCloudClient:
    def __init__(self, root):
        self.root = Path(root).resolve(strict=True)
        self.transfers, self.transfer_category = [], "metadata"

    def _path(self, name):
        if not isinstance(name, str) or "\\" in name or ":" in name or name.startswith("/") or ".." in name.split("/"):
            raise CloudError("Unsafe cloud folder path", 409)
        path = (self.root / name).resolve()
        if not path.is_relative_to(self.root):
            raise CloudError("Cloud folder path escapes its root", 409)
        return path

    @staticmethod
    def _etag(path):
        with path.open("rb") as source:
            return '"' + hashlib.file_digest(source, "sha256").hexdigest() + '"'

    def get(self, name):
        path = self._path(name)
        try:
            data = path.read_bytes()
        except FileNotFoundError as exc:
            raise CloudError("HTTP 404", 404, http_status=404) from exc
        self.transfers.append({"category": self.transfer_category, "attemptedUploadBytes": 0, "successfulUploadBytes": 0, "downloadBytes": len(data)})
        return data, '"' + hashlib.sha256(data).hexdigest() + '"'

    def get_file(self, name, target):
        path = self._path(name)
        try:
            with path.open("rb") as source:
                shutil.copyfileobj(source, target, 64 * 1024)
        except FileNotFoundError as exc:
            raise CloudError("HTTP 404", 404, http_status=404) from exc
        self.transfers.append({"category": self.transfer_category, "attemptedUploadBytes": 0, "successfulUploadBytes": 0, "downloadBytes": target.tell()})
        target.seek(0)

    def _check_write(self, path, etag, create):
        if create and path.exists() or etag and (not path.is_file() or self._etag(path) != etag):
            raise CloudError("HTTP 412", 409, http_status=412)
        path.parent.mkdir(parents=True, exist_ok=True)

    def put(self, name, data, etag=None, create=False):
        import io
        return self.put_file(name, io.BytesIO(data), etag=etag, create=create)

    def put_file(self, name, source, etag=None, create=False):
        path = self._path(name)
        self._check_write(path, etag, create)
        # NamedTemporaryFile's default ACL works on Windows and Unix; os.replace
        # publishes complete bytes without exposing a partially written object.
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".cloud-write-", delete=False) as target:
            temporary = Path(target.name)
            try:
                shutil.copyfileobj(source, target, 64 * 1024)
                target.flush()
                os.fsync(target.fileno())
            except BaseException:
                target.close()
                temporary.unlink()
                raise
        try:
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        self.transfers.append({"category": self.transfer_category, "attemptedUploadBytes": path.stat().st_size, "successfulUploadBytes": path.stat().st_size, "downloadBytes": 0})
        return self._etag(path)

    def ensure_directories(self, name):
        self._path(name).mkdir(parents=True, exist_ok=True)

    def list_details(self, name):
        path = self._path(name)
        try:
            return [{"name": item.name, "etag": self._etag(item) if item.is_file() else None} for item in sorted(path.iterdir())]
        except FileNotFoundError as exc:
            raise CloudError("HTTP 404", 404, http_status=404) from exc

    def list(self, name):
        return [item["name"] for item in self.list_details(name)]

    def delete(self, name, etag=None):
        path = self._path(name)
        if path == self.root:
            raise CloudError("Refusing to delete the cloud folder root", 409)
        if etag and path.is_file() and self._etag(path) != etag:
            raise CloudError("HTTP 412", 409, http_status=412)
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)
