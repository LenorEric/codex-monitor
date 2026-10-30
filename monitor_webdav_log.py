"""Bounded, local-only WebDAV diagnostics; never record request/response bodies."""

import json
from pathlib import Path
import re
import threading
import time
import urllib.parse


WEBDAV_LOG_MAX_BYTES = 5_000_000
_WRITE_LOCK = threading.Lock()


class WebDavLog:
    def __init__(self, path, max_bytes=WEBDAV_LOG_MAX_BYTES):
        self.path = Path(path)
        self.max_bytes = max_bytes
        self.context = threading.local()

    def write(self, event, *, secrets=(), **fields):
        def clean(value):
            if isinstance(value, dict):
                return {key: clean(item) for key, item in value.items()}
            if isinstance(value, (tuple, list)):
                return [clean(item) for item in value]
            if isinstance(value, str):
                for secret in sorted((item for item in secrets if isinstance(item, str) and item), key=len, reverse=True):
                    value = value.replace(secret, "[redacted]")
                value = re.sub(r"https?://[^\s<>\"']+", lambda match: self.safe_url(match[0]), value)
                return value[:2048]
            return value

        try:
            record = {"timestamp": time.time(), "event": event, "operationId": getattr(self.context, "operation_id", None), **clean(fields)}
            entry = (json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n").encode("utf-8")
            if len(entry) > self.max_bytes:
                return
            with _WRITE_LOCK:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.open("a+b") as stream:
                    stream.seek(0, 2)
                    if stream.tell() + len(entry) > self.max_bytes:
                        # Keep complete newest lines. No backup file can exceed the total budget.
                        stream.seek(max(0, stream.tell() - (self.max_bytes - len(entry))))
                        stream.readline()
                        retained = stream.read()
                        stream.seek(0)
                        stream.truncate()
                        stream.write(retained)
                    stream.write(entry)
        except (OSError, ValueError, TypeError):
            # Diagnostics must never change the outcome of a cloud operation.
            pass

    @staticmethod
    def safe_url(value):
        try:
            parsed = urllib.parse.urlsplit(value)
            return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc.rsplit("@", 1)[-1], parsed.path, "", ""))
        except ValueError:
            return "[invalid URL]"

    @staticmethod
    def response_headers(headers):
        # Explicit allowlist: cookies, authorization and lock tokens must never escape.
        allowed = {"retry-after", "server", "date", "content-type", "content-length", "x-request-id", "request-id", "x-correlation-id", "cf-ray"}
        return {key.lower(): str(value) for key, value in (headers or {}).items()
            if key.lower() in allowed or key.lower().startswith(("ratelimit-", "x-ratelimit-", "x-rate-limit-"))}
