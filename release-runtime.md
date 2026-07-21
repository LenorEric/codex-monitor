# Codex Usage Monitor runtime

This directory is the complete standalone monitor runtime. It contains no credentials, configuration, account data, or usage history.

Requirements:

- Python 3.12 or newer
- Codex already configured for the user who runs the monitor

Install dependencies and start the service from this directory:

```powershell
python -m pip install -r requirements.txt
python monitor_codex_usage.py
```

Use `python monitor_codex_usage.py --dashboard` to open the dashboard automatically. The default server host is `0.0.0.0` on port 8765. Use `http://127.0.0.1:8765` locally or the machine's LAN/public IP remotely.
Password-protected control requests are accepted through public addresses and reverse proxies without an origin restriction. Direct public-IP access uses unencrypted HTTP, so prefer a trusted VPN or an HTTPS reverse proxy.
Change the host to `127.0.0.1` in the management page and restart when LAN access is unnecessary. Install the matching VSIX from the parent release directory to connect VS Code to it.

Runtime state and sensitive account data are stored under `~/.codex-switch` and must be protected separately. They are deliberately not part of this release.

This runtime is distributed under the GNU General Public License version 3. See `LICENSE` in this directory.
