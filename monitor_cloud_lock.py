"""Recoverable operation leases; native WebDAV locks cover metadata updates only."""

from contextlib import contextmanager
import hashlib
import json
import re
import threading
import time
import uuid
import xml.etree.ElementTree as ET


CLOUD_LOCK_PATH = "sync-lock.json"
CLOUD_LOCK_INIT_PATH = ".sync-lock-init"
LEASE_SECONDS = 300
RENEW_BEFORE_SECONDS = 60
MUTATING_METHODS = {"PUT", "DELETE", "MKCOL", "MOVE", "COPY", "PROPPATCH"}


def lock_error(message, http_status=None):
    # The transport owns the public error type; defer import to avoid a cycle.
    from monitor_cloud import CloudError
    return CloudError(message, 409, http_status=http_status, category="lock")


class CloudAccessLock:
    _verified = set()
    _verification_lock = threading.Lock()

    def __init__(self, client):
        self.client = client
        self.owner = uuid.uuid4().hex
        self.etag = None
        self.expires_at = 0
        self.deadline = 0
        self.writing = False
        self.claimed = False
        self.required = False
        self.failure = None
        self.mutex = threading.RLock()
        self.stop = threading.Event()
        self.worker = None
        self.operation_id = getattr(getattr(client.diagnostic_log, "context", None), "operation_id", None)

    def _log(self, event, **fields):
        if self.client.diagnostic_log:
            self.client.diagnostic_log.write(event, secrets=self.client._diagnostic_secrets, operationId=self.operation_id, owner=self.owner, expiresAt=self.expires_at, **fields)

    def _request(self, method, path=CLOUD_LOCK_PATH, data=None, headers=None, expected=(200, 201, 204)):
        try:
            return self.client.request(method, path, data, headers, expected, _coordination=True)
        except Exception as exc:
            if path == CLOUD_LOCK_PATH and getattr(exc, "http_status", None) in {412, 423}:
                exc.category = "lock"
            raise

    @staticmethod
    def parse(data):
        try:
            state = json.loads(data)
            if not isinstance(state, dict) or type(state.get("writing")) is not int or state["writing"] not in (0, 1):
                raise ValueError()
            if state == {"writing": 0}:
                return state
            if not isinstance(state.get("owner"), str) or not state["owner"] or set(state) - {"writing", "owner", "expiresAt"}:
                raise ValueError()
            if "expiresAt" in state and (type(state["expiresAt"]) is not int or state["expiresAt"] < 0):
                raise ValueError()
            return state
        except (ValueError, UnicodeDecodeError):
            raise lock_error("Invalid cloud lock status; preserve sync-lock.json and repair it before retrying") from None

    @staticmethod
    def eligible(state):
        if state.get("owner") and ("expiresAt" not in state or state["expiresAt"] > time.time()):
            raise lock_error("Cloud synchronization is busy or an interrupted operation needs recovery; sync-lock.json is owned by another operation", 423)

    def _read(self):
        body, etag, _ = self._request("GET")
        if not etag or etag.startswith("W/"):
            raise lock_error("Cloud lock requires strong ETags")
        return self.parse(body), etag

    def _ensure_file(self):
        try:
            return self._read()
        except Exception as exc:
            if getattr(exc, "http_status", None) != 404:
                raise
        try:
            self._request("MKCOL", CLOUD_LOCK_INIT_PATH, expected=(201,))
        except Exception as exc:
            if getattr(exc, "http_status", None) in {405, 409}:
                raise lock_error("Cloud lock file initialization is busy or needs recovery; retry after .sync-lock-init is released", 423) from exc
            raise
        try:
            try:
                return self._read()
            except Exception as exc:
                if getattr(exc, "http_status", None) != 404:
                    raise
            self._request("PUT", data=b'{"writing":0}', headers={"Content-Type": "application/json"})
            return self._read()
        finally:
            self._request("DELETE", CLOUD_LOCK_INIT_PATH, expected=(200, 204, 404))

    @contextmanager
    def _native(self):
        started = time.monotonic()
        token = None
        try:
            body, _, _ = self._request("LOCK", data=b'<D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>',
                headers={"Content-Type": "application/xml", "Depth": "0", "Timeout": "Second-60"}, expected=(200,))
            try:
                active = ET.fromstring(body).find(".//{DAV:}activelock")
                candidate = active.findtext("{DAV:}locktoken/{DAV:}href")
                if candidate and re.fullmatch(r"[A-Za-z][A-Za-z0-9+.-]*:[^\s<>]+", candidate):
                    token = candidate
                timeout = active.findtext("{DAV:}timeout")
                if active.find("{DAV:}lockscope/{DAV:}exclusive") is None or active.findtext("{DAV:}depth") not in {"0", "infinity"} or not token:
                    raise ValueError()
                if timeout != "Infinite" and (not isinstance(timeout, str) or not re.fullmatch(r"Second-[0-9]+", timeout) or int(timeout[7:]) <= 0):
                    raise ValueError()
                native_deadline = float("inf") if timeout == "Infinite" else started + int(timeout[7:])
            except (ET.ParseError, AttributeError, ValueError):
                raise lock_error("WebDAV must provide a valid exclusive native file lock") from None
            yield token, native_deadline
        finally:
            if token:
                self._request("UNLOCK", headers={"Lock-Token": f"<{token}>"}, expected=(204, 404, 409))

    def _write(self, state, etag, token, native_deadline):
        if time.monotonic() >= native_deadline:
            raise lock_error("Native cloud metadata lock expired before update")
        body = json.dumps(state, sort_keys=True, separators=(",", ":")).encode()
        _, new_etag, _ = self._request("PUT", data=body, headers={"Content-Type": "application/json", "If-Match": etag,
            "If": f"<{self.client._url(CLOUD_LOCK_PATH)}> (<{token}>)"})
        if time.monotonic() >= native_deadline:
            raise lock_error("Native cloud metadata lock expired during update")
        if not new_etag or new_etag.startswith("W/"):
            verified, new_etag = self._read()
            if verified != state:
                raise lock_error("Cloud lock update read-back verification failed")
        return new_etag

    def acquire(self):
        with self.mutex:
            if self.required:
                raise lock_error("Cloud lease instances cannot be reacquired")
            self.required = True
            self.eligible(self._ensure_file()[0])
            try:
                with self._native() as (token, native_deadline):
                    state, etag = self._read()
                    self.eligible(state)
                    started = time.monotonic()
                    expires_at = int(time.time()) + LEASE_SECONDS
                    self.etag = self._write({"writing": 0, "owner": self.owner, "expiresAt": expires_at}, etag, token, native_deadline)
                    self.expires_at = expires_at
                    self.deadline = started + max(0, expires_at - time.time())
                    self.claimed = True
                    self._log("lease_acquired", takeover=bool(state.get("owner")))
                self._verify_conditions()
                self.check()
            except Exception:
                if self.claimed:
                    self.release()
                raise
            self.worker = threading.Thread(target=self._renew_loop, name="cloud-lease-renewal", daemon=True)
            self.worker.start()

    def check(self):
        if self.failure:
            raise lock_error(f"Cloud lease is unusable: {self.failure}") from self.failure
        if not self.claimed or time.monotonic() >= self.deadline or time.time() >= self.expires_at:
            self.failure = lock_error("Cloud lease expired; operation stopped before further data access")
            raise self.failure

    def _owned(self, state):
        if state.get("owner") != self.owner or state.get("expiresAt", 0) <= time.time():
            raise lock_error("Cloud lease ownership changed or expired; operation stopped", 423)

    def _update(self, writing=None, release=False):
        self.check()
        with self._native() as (token, native_deadline):
            state, etag = self._read()
            self._owned(state)
            self.check()
            started = time.monotonic()
            expires_at = int(time.time()) + LEASE_SECONDS
            updated = {"writing": 0} if release else {"writing": int(self.writing if writing is None else writing), "owner": self.owner, "expiresAt": expires_at}
            self.etag = self._write(updated, etag, token, native_deadline)
            # A late successful response must not resurrect a locally expired operation.
            self.check()
            if not release:
                self.expires_at = expires_at
                self.deadline = started + max(0, expires_at - time.time())
                self.writing = bool(updated["writing"])

    def renew(self):
        with self.mutex:
            try:
                self._update()
                self._log("lease_renewed")
            except Exception as exc:
                self.failure = exc
                self.stop.set()
                self._log("lease_failed", error=str(exc))
                raise

    def _renew_loop(self):
        log = self.client.diagnostic_log
        if log:
            log.context.operation_id = self.operation_id
        while not self.stop.wait(max(0, min(self.deadline - time.monotonic(), self.expires_at - time.time()) - RENEW_BEFORE_SECONDS)):
            try:
                with self.mutex:
                    # Marking the first write may already have extended the scheduled lease.
                    if min(self.deadline - time.monotonic(), self.expires_at - time.time()) <= RENEW_BEFORE_SECONDS:
                        self.renew()
            except Exception:
                return

    def prepare(self, method):
        with self.mutex:
            self.check()
            if method in MUTATING_METHODS and not self.writing:
                try:
                    self._update(writing=True)
                except Exception as exc:
                    self.failure = exc
                    raise
            return self.etag

    def condition(self, etag):
        return f"<{self.client._url(CLOUD_LOCK_PATH)}> ([{etag}])"

    def retry_etag(self, previous):
        with self.mutex:
            self.check()
            try:
                state, etag = self._read()
                self._owned(state)
                if etag != self.etag:
                    raise lock_error("Cloud lease changed outside this operation", 423)
            except Exception as exc:
                self.failure = exc
                raise
            return self.etag if self.etag != previous else None

    def _verify_conditions(self):
        key = (self.client.base, self.client.root, hashlib.sha256(self.client.authorization.encode()).digest())
        with self._verification_lock:
            if key in self._verified:
                return
            path = ".lock-probe-" + uuid.uuid4().hex
            headers = {"If": self.condition(self.etag), "Content-Type": "application/octet-stream"}
            try:
                # A lost PUT response can still have created this unique probe.
                self._request("PUT", path, b"original", headers)
                for invalid in ({"If": self.condition('"invalid-' + uuid.uuid4().hex + '"')}, {"If-Match": '"invalid-' + uuid.uuid4().hex + '"'}):
                    _, _, status = self._request("PUT", path, b"rejected", headers | invalid, expected=(200, 201, 204, 412))
                    if status != 412:
                        raise lock_error("WebDAV must enforce tagged lease ETags and If-Match before cloud access")
                body, _, _ = self._request("GET", path, headers=headers)
                if body != b"original":
                    raise lock_error("WebDAV conditional probe was modified despite rejected writes")
            finally:
                self._request("DELETE", path, headers=headers, expected=(200, 204, 404))
            self._verified.add(key)

    def release(self):
        self.stop.set()
        if self.worker and self.worker is not threading.current_thread():
            self.worker.join()
        with self.mutex:
            if not self.claimed:
                return
            try:
                # A failed/expired lease must remain recoverable, never clear a successor.
                self._update(release=True)
                self._log("lease_released")
            finally:
                self.claimed = False
