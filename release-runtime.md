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

Use `python codex_monitor_daemon.py --dashboard` to open the dashboard automatically. The default server host is `0.0.0.0` on port 8765. Use `http://127.0.0.1:8765` locally or the machine's LAN/public IP remotely. The former `python monitor_codex_usage.py` command remains compatible.

Automatic updates are disabled by default. Enable them on the management Config page to check the published runtime in the background after startup and every hour. Verified updates replace this directory's packaged files and restart the same command with the same arguments; dependencies from `requirements.txt` are not installed automatically.
Password-protected control requests are accepted through public addresses and reverse proxies without an origin restriction. Direct public-IP access uses unencrypted HTTP, so prefer a trusted VPN or an HTTPS reverse proxy.
Change the host to `127.0.0.1` in the management page and restart when LAN access is unnecessary. Install the matching VSIX from the parent release directory to connect VS Code to it.

Runtime state and sensitive account data are stored under `~/.codex-switch` and must be protected separately. They are deliberately not part of this release.

Token capture runs independently of remote quota acquisition. Persistent checkpoints resume after restart, and ordinary appends update only new facts and affected dashboard rows. Watchdog supplies OS file notifications; if it is unavailable, bounded polling and scheduled audits remain active. Install the updated `requirements.txt` after upgrading to enable notifications.

The local data contract is version 8. Ordered startup migration preserves canonical JSONL histories and existing v4 collection periods and peer records. Bootstrap, repair, scheduled audits/retention, and retrospective quota corrections can still read their complete affected scope. Run `python codex_monitor_daemon.py --repair-processing` to rebuild derived checkpoints and dashboard rows while preserving canonical histories and cloud ownership metadata. An older runtime cannot write a newer local contract; restore a verified backup to roll back.

This runtime is distributed under the GNU General Public License version 3. See `LICENSE` in this directory.
