"""In-memory, single-worker queue for logical WebDAV operations."""

from collections import deque
from copy import deepcopy
import threading
import time
import uuid

from monitor_common import now_iso as timestamp


class OperationSkipped(RuntimeError):
    status = 409


class OperationCancelled(OperationSkipped):
    pass


class CloudOperationQueue:
    def __init__(self, diagnostic_log=None):
        self.diagnostic_log = diagnostic_log
        self.session_id = uuid.uuid4().hex
        self.lock = threading.RLock()
        self.pending = deque()
        self.active = []
        self.completed = deque(maxlen=200)
        self.events = deque(maxlen=400)
        self.sequence = 0
        self.worker = None
        self.closed = False

    def in_worker(self):
        return threading.current_thread() is self.worker

    def _event(self, operation, phase):
        self.sequence += 1
        self.events.append({"id": self.sequence, "phase": phase, "operation": deepcopy(operation["public"])})
        if self.diagnostic_log:
            public = operation["public"]
            self.diagnostic_log.write("operation_" + phase, **operation["sanitize"]({
                "operationId": public["id"], "sessionId": self.session_id, "action": public["action"], "source": public["source"], "target": public["target"], "status": public["status"],
                "elapsedMs": round((time.monotonic() - operation["queued_monotonic"]) * 1000, 3),
                "error": {key: value for key, value in (public.get("error") or {}).items() if key != "details"},
                "partial": public["status"] == "failed" and operation.get("exception") is None,
                "failures": str((public.get("result") or {}).get("errors", ""))[:2048] if isinstance(public.get("result"), dict) else ""}))

    def submit(self, action, execute, *, target="", source="manual", validate=None, combine_key=None, context=None, sanitize=None):
        with self.lock:
            if self.closed:
                raise OperationCancelled("The monitor is stopping; the operation was not accepted")
            operation = {
                "public": {"id": uuid.uuid4().hex, "action": action, "target": target, "source": source, "status": "queued", "queuedAt": timestamp(), "context": context or {}},
                "execute": execute, "validate": validate, "combineKey": combine_key, "done": threading.Event(), "sanitize": sanitize or (lambda value: value),
                "queued_monotonic": time.monotonic(),
            }
            self.pending.append(operation)
            self._event(operation, "start")
            if self.worker is None:
                self.worker = threading.Thread(target=self._run, name="webdav-queue", daemon=True)
                self.worker.start()
            return operation

    def wait(self, operation):
        operation["done"].wait()
        if operation.get("exception") is not None:
            raise operation["exception"]
        return operation.get("result")

    def snapshot(self):
        with self.lock:
            return deepcopy({"sessionId": self.session_id, "lastEventId": self.sequence,
                "operations": [item["public"] for item in [*self.active, *self.pending] if item["public"]["status"] in {"queued", "running"}],
                "completed": list(self.completed), "events": list(self.events)})

    def _finish(self, operation, status, result=None, error=None):
        operation["result"], operation["exception"] = result, error
        operation["public"].update(status=status, finishedAt=timestamp())
        if error is not None:
            operation["public"]["error"] = operation["sanitize"]({"message": str(error), "status": getattr(error, "status", 500),
                "category": getattr(error, "category", None), "httpStatus": getattr(error, "http_status", None), "exceptionType": type(error).__name__,
                "details": getattr(error, "details", None), "decryptFailed": bool(getattr(error, "decrypt_failed", False))})
        else:
            operation["public"]["result"] = operation["sanitize"](deepcopy(result))
        self._event(operation, "end")
        self.completed.append(deepcopy(operation["public"]))
        # Do not retain request closures (which may contain passwords) in history.
        for key in ("execute", "validate", "sanitize"):
            operation.pop(key, None)
        operation["done"].set()

    def cancel(self, operation_id):
        with self.lock:
            for operation in self.pending:
                if operation["public"]["id"] == operation_id:
                    self.pending.remove(operation)
                    self._finish(operation, "cancelled", error=OperationCancelled("Removed from the queue before execution"))
                    return deepcopy(operation["public"])
            if any(item["public"]["id"] == operation_id and item["public"]["status"] == "running" for item in self.active):
                raise OperationSkipped("The operation has started and cannot be removed")
            for operation in self.completed:
                if operation["id"] == operation_id:
                    return deepcopy(operation)
            raise OperationSkipped("The operation no longer exists in this monitor session")

    def _run(self):
        while True:
            with self.lock:
                if not self.pending:
                    self.worker = None
                    return
                batch = [self.pending.popleft()]
                while batch[0]["combineKey"] is not None and self.pending and self.pending[0]["combineKey"] == batch[0]["combineKey"]:
                    batch.append(self.pending.popleft())
                self.active = batch
                for operation in batch:
                    operation["public"].update(status="running", startedAt=timestamp())
            valid = []
            for operation in batch:
                try:
                    if self.diagnostic_log:
                        self.diagnostic_log.context.operation_id = operation["public"]["id"]
                    if operation["validate"]:
                        operation["validate"]()
                    valid.append(operation)
                except Exception as exc:
                    with self.lock:
                        self._finish(operation, "skipped" if isinstance(exc, OperationSkipped) or getattr(exc, "status", None) in {400, 404, 409} else "failed", error=exc)
            if valid:
                try:
                    if self.diagnostic_log:
                        self.diagnostic_log.context.operation_id = valid[0]["public"]["id"]
                        self.diagnostic_log.write("operation_running", operationIds=[item["public"]["id"] for item in valid], action=valid[0]["public"]["action"])
                    result = valid[0]["execute"]()
                except Exception as exc:
                    with self.lock:
                        for operation in valid:
                            self._finish(operation, "skipped" if isinstance(exc, OperationSkipped) else "failed", error=exc)
                else:
                    with self.lock:
                        for operation in valid:
                            pending = isinstance(result, dict) and (bool(result.get("errors")) or isinstance(result.get("cloud"), dict) and result["cloud"].get("pending"))
                            self._finish(operation, "failed" if pending else "succeeded", result=result)
            with self.lock:
                self.active = []
            if self.diagnostic_log:
                self.diagnostic_log.context.operation_id = None

    def close(self):
        with self.lock:
            self.closed = True
            while self.pending:
                self._finish(self.pending.popleft(), "cancelled", error=OperationCancelled("Monitor shutdown cancelled the waiting operation"))
            worker = self.worker
        if worker is not None and worker is not threading.current_thread():
            worker.join()
