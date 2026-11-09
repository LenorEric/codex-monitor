"""Shared settings and locking for session source operations."""

from functools import wraps

SOURCE_READ_BYTES = 1024 * 1024


def serialized(method):
    @wraps(method)
    def run(self, *args, **kwargs):
        with self.lock:
            return method(self, *args, **kwargs)
    return run


