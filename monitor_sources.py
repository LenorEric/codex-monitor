"""Session file notification hints and serialized source operations."""

import threading
from functools import wraps
from pathlib import Path

SOURCE_READ_BYTES = 1024 * 1024
SOURCE_DISCOVERY_SECONDS = 15 * 60
SOURCE_CHECK_BATCH = 128

class SourceWatcher:
    """OS notifications are hints; persistent checkpoints and audits remain authoritative."""
    def __init__(self, home):
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer
        self.home, self.lock, self.pending, self.overflow = Path(home).resolve(), threading.Lock(), set(), True
        self.observer, self.roots = Observer(), set()
        watcher = self
        class Handler(FileSystemEventHandler):
            def on_any_event(self, event):
                if event.event_type not in {"created", "modified", "deleted", "moved"}:
                    return
                with watcher.lock:
                    for name in (event.src_path, getattr(event, "dest_path", "")):
                        if not name:
                            continue
                        path = Path(name).resolve()
                        if event.is_directory:
                            if path.parent == watcher.home and path.name in {"sessions", "archived_sessions"}:
                                watcher.overflow = True
                            continue
                        if path.suffix == ".jsonl" and any(path.is_relative_to(watcher.home / root) for root in ("sessions", "archived_sessions")):
                            if len(watcher.pending) < 4096:
                                watcher.pending.add(str(path))
                            else:
                                watcher.overflow = True
        self.handler = Handler()
        if self.home.exists():
            self.observer.schedule(self.handler, str(self.home), recursive=False)
        self.enroll()
        self.observer.start()

    def enroll(self):
        for name in ("sessions", "archived_sessions"):
            root = self.home / name
            if root.is_dir() and root not in self.roots:
                self.observer.schedule(self.handler, str(root), recursive=True)
                self.roots.add(root)

    def drain(self):
        self.enroll()
        with self.lock:
            paths = sorted(self.pending)[:SOURCE_CHECK_BATCH]
            self.pending.difference_update(paths)
            overflow, self.overflow = self.overflow, False
            return paths, overflow

    def stop(self):
        self.observer.stop()
        self.observer.join()


def serialized(method):
    @wraps(method)
    def run(self, *args, **kwargs):
        with self.lock:
            return method(self, *args, **kwargs)
    return run


