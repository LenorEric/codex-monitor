#!/usr/bin/env python3
import sys

if sys.version_info < (3, 12):
    raise SystemExit("Codex Usage Monitor requires Python 3.12 or newer.")

from codex_monitor_daemon import *


if __name__ == "__main__":
    raise SystemExit(main())
