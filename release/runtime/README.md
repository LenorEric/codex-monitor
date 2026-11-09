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

The local data contract is 22 and the cloud contract is 5. Startup converts released Local/Merged history into identically shaped permanent canonical datasets. The same preparation processor serves both WebUI views. A durable local buffer publishes immutable packages 30 minutes after its first remaining arrival; manual Push flushes immediately. Downloads fetch unseen packages into memory and commit canonical results with package receipts. Canonical JSONL backups are retired. Optional `saveRawQuotaResponses` diagnostics, disabled by default, contain only quota API response bodies; `--raw-quota-log` changes their path. Existing JSONL files remain untouched. Local migration codecs remain available for sequential local upgrades; cloud migration code is removed and only contract 5 is supported.

This runtime is distributed under the GNU General Public License version 3. See `LICENSE` in this directory.

Dashboard delivery uses one account-based gzip bulk containing both views, followed by sequenced WebSocket channels on port 8765. No HTTP protocol number is required. Cloud imports use ordinary Merged deltas. Final token groups are immutable, and snapshots stream on demand from final delivery rows. See dashboard-delivery.md in the source repository.

Pending local contract 26 removes redundant identity/preparation storage and converts saved display results into final delivery rows and recoverable pending work. Canonical costs and upload/parser state are preserved without history replay. The delivery epoch changes once during conversion. Cloud contract 5 and client formats are unchanged. Freed SQLite pages remain reusable; automatic canonical compaction is disabled.
