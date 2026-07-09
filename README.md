# Codex Usage Monitor

Codex Usage Monitor shows the authoritative 5-hour and 7-day usage reported by a manually started Python monitor in the VS Code status bar. Click the status-bar item to open **Codex Usage Details**, which uses the same API instance and history as the browser dashboard.

Start the monitor before using the extension:

```powershell
python monitor_codex_usage.py --dashboard
```

The Python server and extension use the fixed `http://127.0.0.1:8765` API endpoint. The extension never starts another Python process. The extension does not parse Codex session logs for rate-limit values, expose a manual refresh command, or contribute VS Code settings. Monitor behavior remains configured through the defaults in `monitor_codex_usage.py`.

## Requirements

- A manually running `python monitor_codex_usage.py --dashboard`.
- A working Codex login in the normal `CODEX_HOME`.
- The proxy requirements enforced by `monitor_common.py`.
