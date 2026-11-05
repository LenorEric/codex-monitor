"""In-memory WebDAV fixtures for current package and account tests."""

from datetime import datetime
from monitor_cloud import CloudError


def moment(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


class MemoryWebDav:
    def __init__(self):
        self.files, self.revision, self.transfers, self.transfer_category = {}, 0, [], "metadata"

    def ensure_directories(self, path):
        pass

    def put(self, path, data, etag=None, create=False):
        self.transfers.append({"category": self.transfer_category, "attemptedUploadBytes": len(data), "successfulUploadBytes": len(data), "downloadBytes": 0})
        if create and path in self.files or etag and (path not in self.files or self.files[path][1] != etag):
            raise CloudError("HTTP 412", 409, http_status=412)
        self.revision += 1
        self.files[path] = data, f'"{self.revision}"'
        return self.files[path][1]

    def get(self, path):
        if path not in self.files:
            raise CloudError("HTTP 404", 502, http_status=404)
        data, etag = self.files[path]
        self.transfers.append({"category": self.transfer_category, "attemptedUploadBytes": 0, "successfulUploadBytes": 0, "downloadBytes": len(data)})
        return data, etag

    def list_details(self, path):
        prefix = path.rstrip("/") + "/"
        return [{"name": name[len(prefix):], "etag": etag} for name, (_, etag) in self.files.items() if name.startswith(prefix) and "/" not in name[len(prefix):]]

    def list(self, path):
        return [item["name"] for item in self.list_details(path)]

    def delete(self, path, etag=None):
        for name in list(self.files):
            if name == path or name.startswith(path.rstrip("/") + "/"):
                self.files.pop(name)


