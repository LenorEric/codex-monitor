# Codex Usage Monitor runtime

This directory is the complete standalone monitor runtime. It contains no credentials, configuration, account data, or usage history.

Requirements:

- Python 3.12 or newer
- Codex already configured for the user who runs the monitor

Install dependencies and start the service from this directory:

```powershell
python -m pip install -r requirements.txt
python codex_monitor_daemon.py
```

Startup checks required Python libraries before loading the application. Missing or unloadable libraries are listed in the console with an installation command, and the program exits. Startup stages are printed before the dashboard URL: instance lock, update check, migrations, processing/backfill, server binding, store initialization/recovery, and background workers.

Use `python codex_monitor_daemon.py --dashboard` to open the dashboard automatically. The default server host is `0.0.0.0` on port 8765. Use `http://127.0.0.1:8765` locally or the machine's LAN/public IP remotely. The former `python monitor_codex_usage.py` command remains compatible.

Automatic updates are disabled by default. Enable them on the management Config page to check the published runtime before data migrations and dashboard startup, then every hour in the background. Startup waits for the initial check; a failed check is reported and startup continues. The console shows check results and download/verification progress. Verified updates replace this directory's packaged files and restart the same command with the same arguments; the updater installs `requirements.txt` with the running Python interpreter before restarting. A package-install failure restores runtime files and reports failure; pip changes to the Python environment are not rolled back.
Password-protected control requests are accepted through public addresses and reverse proxies without an origin restriction. Direct public-IP access uses unencrypted HTTP, so prefer a trusted VPN or an HTTPS reverse proxy.
Change the host to `127.0.0.1` in the management page and restart when LAN access is unnecessary. Install the matching VSIX from the parent release directory to connect VS Code to it.

Runtime state and sensitive account data are stored under `~/.codex-switch` and must be protected separately. They are deliberately not part of this release.

Token capture runs independently of remote quota acquisition. Persistent checkpoints resume after restart, and ordinary appends update only new facts and affected dashboard rows. Watchdog supplies OS file notifications; if it is unavailable, bounded polling and scheduled audits remain active. Install the updated `requirements.txt` after upgrading to enable notifications.

The local data contract is version 20; the consolidated cloud contract 4 converts reset timestamps and removes legacy token baselines, dependent events and retired quota compaction metadata. Ordered startup migration converts complete SQLite records to fixed local source identities while preserving v4 collection periods, peer records, and parser state. Migration 19 neither opens nor imports recorder logs or the legacy state file; its rollback backup covers only the database. SQLite then owns all active usage data. Python source ingestion tracks original Codex session files and writes the database directly; recorder JSONL is write-only backup output. Failed backup appends retry from a durable queue. Corrections append ordinary backup records. Database removals do not modify the logs or append deletion operations; explicit rebuilding uses current processing rules. Run `python codex_monitor_daemon.py --repair-processing` to rebuild derived state from SQLite and recheck original session files. An older runtime cannot write a newer local contract; restore a verified backup to roll back.

This runtime is distributed under the GNU General Public License version 3. See `LICENSE` in this directory.

Dashboard delivery uses one account-based gzip bulk containing both views, followed by sequenced WebSocket channels on port 8765. No HTTP protocol number is required. Cloud changes replace the complete bulk. See dashboard-delivery.md in the source repository.
