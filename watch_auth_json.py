import argparse
import time
from datetime import datetime
from pathlib import Path


DEFAULT_AUTH_PATH = Path.home() / ".codex" / "auth.json"
DEFAULT_AUTH_PATH_DISPLAY = "~/.codex/auth.json"


def read_snapshot(path):
    for _ in range(3):
        try:
            before = path.stat()
            content = path.read_bytes()
            after = path.stat()
        except (FileNotFoundError, PermissionError, OSError):
            return None
        if (before.st_mtime_ns, before.st_ctime_ns, before.st_size) == (after.st_mtime_ns, after.st_ctime_ns, after.st_size):
            return (after.st_mtime_ns, after.st_ctime_ns, after.st_size), content
    return None


def print_update(path, snapshot):
    print(f"\nUpdated: {datetime.fromtimestamp(snapshot[0][0] / 1_000_000_000).astimezone().isoformat(timespec='seconds')}")
    print(f"File: {path}")
    print("Content:")
    print(snapshot[1].decode("utf-8", errors="replace"))
    print("-" * 80, flush=True)


def watch(path, interval):
    previous = read_snapshot(path)
    print(f"Watching {path}")
    print("Waiting for modifications. Press Ctrl+C to stop.", flush=True)
    while True:
        time.sleep(interval)
        current = read_snapshot(path)
        if current is not None and current != previous:
            print_update(path, current)
        previous = current


def build_parser():
    parser = argparse.ArgumentParser(description="Print auth.json whenever it is modified.")
    parser.add_argument("path", nargs="?", type=Path, default=DEFAULT_AUTH_PATH, help=f"File to watch (default: {DEFAULT_AUTH_PATH_DISPLAY})".replace("%", "%%"))
    parser.add_argument("--interval", type=float, default=0.5, help="Polling interval in seconds (default: 0.5)")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    try:
        watch(args.path, args.interval)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
