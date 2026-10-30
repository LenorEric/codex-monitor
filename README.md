<div align="center">

# Codex Usage Monitor

**A local-first Codex quota, token, cost, account, skill, and encrypted-sync dashboard for VS Code.**

[![Version](https://img.shields.io/badge/version-1.5.13-4f8cff)](#quick-start)
[![Python](https://img.shields.io/badge/Python-3.12%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![VS Code](https://img.shields.io/badge/VS%20Code-1.96%2B-007ACC?logo=visualstudiocode&logoColor=white)](https://code.visualstudio.com/)
[![Platforms](https://img.shields.io/badge/platform-Windows%20%7C%20Linux-6b7280)](#requirements)
[![License](https://img.shields.io/badge/license-GPL--3.0-22c55e)](https://www.gnu.org/licenses/gpl-3.0.html)

English · <a href="./README.zh-CN.md">简体中文</a>

</div>

Codex Usage Monitor combines the authoritative 5-hour and 7-day limits reported by the Codex service with locally parsed session tokens and estimated cost.
A lightweight Python service owns the data, dashboard, account vault, managed skills, and optional encrypted WebDAV synchronization; the VS Code extension adds a status-bar summary and opens the same live dashboard.

> [!IMPORTANT]
> The extension does not start the Python monitor. Start `python codex_monitor_daemon.py` first and keep it running.

## Why use it?

| Capability | What you get |
| --- | --- |
| Live quota monitoring | Authoritative 5-hour and 7-day percentages, reset times, plan-aware history curves, and server-pushed live updates. |
| Token and cost analytics | Fresh input, cached input, cache writes, output, cache-hit rate, per-model totals, Standard/Fast attribution, and estimated cost. |
| Multiple Codex accounts | Safe local account switching, login-slot creation, rename/delete controls, identity validation, and per-account history attribution. |
| Skill management | Discover Codex and Gemini skills, move them into one private managed store, assign them with strict links or managed fallbacks, and synchronize changes. |
| Encrypted multi-machine sync | AES-256-GCM WebDAV packages for copied API accounts, moved OpenAI accounts, and incremental usage journals with verified writes. |
| Local-first privacy | Credentials and raw recorder data stay under `~/.codex-switch`; dashboard payloads redact secrets and cloud-downloaded history never contaminates local recorder files. |
| Resilient operation | Atomic local writes, bounded incremental session-log scanning, revision-keyed response caches, conditional cloud updates, rollback-aware key rotation, and extensive unit coverage. |

## How it fits together

```mermaid
flowchart LR
    C[Codex auth and session logs] --> M[Python monitor :8765]
    M --> D[Browser dashboard]
    M --> V[VS Code status bar and webview]
    M --> L[Local vault and history<br/>~/.codex-switch]
    M <--> W[Optional encrypted WebDAV]
```

The monitor is authoritative. The dashboard downloads one gzip-compressed `/api/dashboard` snapshot containing all retained Local and Merged history, grouped by account. `/api/dashboard/live` pushes sequenced changes over a WebSocket on the same port. Filters and time ranges operate locally. Changed cloud data triggers an atomic full snapshot refresh; reconnects replay retained updates or request a new snapshot. HTTP has no explicit protocol version. The retired `/api/series` and chunk endpoints return 410.

## Requirements

- Python 3.12 or newer on Windows and Linux.
- VS Code 1.96 or newer for the extension.
- A working Codex login in the normal `CODEX_HOME`.
- Port `8765` available.
- Python dependencies from `requirements.txt`—currently `cryptography>=46.0.0,<47`, `tomlkit>=0.13.3,<1`, and `watchdog>=6.0.0,<7`.
- Network/proxy access compatible with the environment variables recognized by Python and `monitor_common.py`.

## Quick start

### 1. Install the standalone runtime

Use the matching artifacts from `release/`:

1. Copy `release/runtime/` to a permanent location.
2. Open a terminal in that directory.
3. Install the dependency and start the monitor:

```console
python -m pip install -r requirements.txt
python codex_monitor_daemon.py --dashboard
```

Startup checks required Python libraries before loading the application. Missing or unloadable libraries are listed in the console with an installation command, and the program exits. Startup stages are printed before the dashboard URL: instance lock, update check, migrations, processing/backfill, server binding, store initialization/recovery, and background workers.

Without `--dashboard`, the service starts normally but does not open a browser:

```console
python codex_monitor_daemon.py
```

### 2. Install the VS Code extension

Install `release/codex-usage-monitor-1.5.13.vsix` from VS Code:

1. Open **Extensions**.
2. Select **Views and More Actions (…) → Install from VSIX…**.
3. Choose the VSIX and reload VS Code if requested.

The command line is also supported:

```console
code --install-extension release/codex-usage-monitor-1.5.13.vsix
```

### 3. Complete first-run setup

1. Keep the Python monitor running.
2. Open `http://127.0.0.1:8765` or click the Codex status-bar item.
3. Create a control password when prompted. Initial setup is accepted only from the same computer, and `123456` is rejected.
4. Use **Manage skills & accounts** for account, skill, WebDAV, server, and configuration operations.

You now have local quota, token, cost, model, and account history. WebDAV is optional.

## Using the dashboard

The top cards show the latest 5-hour and 7-day usage, reset times, plan, and active account. The charts and selectors provide:

- **Date / 5h / 24h / 7d / 30d / All** time ranges.
- Model filters for token and cost analysis.
- Account filters shared by quota, token, and cost views.
- **Local** and **Merged** quota, token, and cost datasets.
- Separate colored quota curves per selected account.
- Fresh input, cached input, output, cache write, cache-hit rate, and estimated cost. Session counts are no longer displayed.
- A full-width Cost vs Time chart with adaptive whole-minute/hour/day spending bars, a cumulative-cost toggle, 5h/7d usage overlays, and exact event-based account/model filters. Joint idle folding preserves intervals containing spending or usage changes. Older session-only cost baselines are excluded from this chart. Hover shows each model’s bar spending and cumulative cost through the pointer time, summed across selected accounts.

**Local** shows only quota, token, and cost data recorded by this machine. **Merged** extends each local account with synchronized records carrying the same private account identity.
Synchronized records that do not yet match a local account remain in the sync cache but are temporarily excluded; they are included automatically when the corresponding account is created or bound locally.
Merged datasets are materialized when local, synchronized, or account-identity data changes, so switching views only selects an existing dataset.

The extension contributes **Codex Usage Monitor: Show Details**. Opening it fetches the current dashboard, and invoking it again refreshes the same view. The extension shares one live connection, subscribes only to current state while the dashboard is closed, and relays history updates to its webview.

## Managing Codex accounts

The account vault lives in `~/.codex-switch/accounts`. On first startup, the current valid `auth.json` becomes **Current account**.

Codex exclusively owns refresh-token rotation for the active account. The monitor reloads and mirrors Codex's live `auth.json` before polling, uses its access token while valid, and pauses remote quota polling if Codex has not refreshed an expiring token yet. Ready inactive accounts are isolated from Codex and are polled every ten minutes; the monitor may rotate those credentials, reports failed inactive polls in the account selector, and never retries an ambiguous rotating-token exchange.

### Create and sign in to another account

1. Open **Manage skills & accounts**.
2. Choose **Create / login** and enter a local label.
3. The monitor securely saves the outgoing account and removes the live `auth.json`.
4. Run the normal Codex login in a new or restarted terminal.
5. The dashboard adopts the completed login automatically.

Restart existing Codex terminals after switching accounts because a running process may retain old credentials.

### Switch, rename, and delete

- **Switch** replaces only the live Codex `auth.json`; it never changes shared `config.toml`, sessions, prompts, MCP servers, or skills.
- **Rename** rewrites the local label across persisted monitor history.
- **Delete** is local-only and cannot delete the active account or the sole remaining local account.
- Before an outgoing signed-in account is saved, its live and vaulted `id_token` and `account_id` must match exactly.
- The same API key cannot occupy two ready local slots. Multiple normal OpenAI credential profiles may use the same authenticated account identity.

### Refresh after reset

Each normal account has a **Refresh after reset…** editor with independent 5h and 7d automation switches. The 5h automation can be limited to one or more daily time windows by painting the 24-hour bar in ten-minute steps or entering minute-precise start and end times; overlapping entries are merged and cross-midnight entries are split automatically. Leaving the list empty allows refresh all day.

The editor displays times in the browser's current time zone and shows the detected zone and UTC offset. On save, the browser converts each range to a fixed, timezone-free backend day position; the monitor stores neither the time zone nor its offset and compares those positions directly with the current timestamp. Any browser converts the saved positions through its own current offset for display. Consequently, switching users, travel, or daylight-saving changes can alter the displayed local hours without changing the saved execution windows. A 5h reset detected outside the allowed ranges remains queued until the next range opens, while 7d automation runs immediately after its reset.

### Transfer accounts between machines

Normal OpenAI accounts use move semantics:

- **Release** uploads and verifies the newest local account payload, then removes the local vault record.
- **Bind** downloads and integrity-checks a released payload, commits it locally, then removes and verifies removal of the cloud copy.

API accounts use copy semantics:

- **Share** uploads and verifies the API key, display name, and account-specific header settings while retaining the local copy.
- **Link** downloads and integrity-checks the API profile while retaining the cloud copy.
- API-key equality defines account equality. When the key already exists at the destination, the account is already shared or linked and no duplicate transfer action is shown.
- Later local name or header edits remain local; they do not create a second API account or automatically overwrite the other copy.

**Push** and **Fetch** never transfer account credentials. Local and cloud **Delete** actions remove only the selected copy, and Rename never mutates a cloud payload. At least one verified copy is preserved when a Bind or Release operation fails. Empty awaiting-login OpenAI slots can also be released and bound.

## Managing skills

The backend scans `CODEX_HOME/skills` and `~/.gemini/config/skills` for directories containing `SKILL.md`.

1. Open **Manage skills & accounts**.
2. Select **Scan skills** when you need a fresh discovery pass.
3. Select skills and choose **Manage selected**.
4. Assign each managed skill to Codex, Gemini, or both, and select **Shared** only for skills that should synchronize through WebDAV.

Managed content is moved into `~/.codex-switch/skills`.
The monitor creates strict per-skill symbolic links; Windows uses native directory junctions when symlinks are unavailable, while non-Windows systems use an ownership-marked managed copy as a fallback. Scanning preserves existing same-name target paths as unassigned conflicts; explicitly selecting **link to** replaces the conflicting path with the managed projection.

Newly managed skills are local-only by default. Existing managed skills retain their previous shared behavior when `skills.json` is upgraded. Cloud behavior is name-based for shared skills:

- **Push:** local same-name content wins, remote-only skills remain, and accounts are never included.
- **Fetch:** remote same-name shared content wins, local-only skills remain, and existing assignments are preserved.
- **Unshare / Unmanage:** publishes a durable tombstone so stale machines remove the shared skill instead of uploading it again. Unsharing keeps the managed source on the initiating machine; unmanaging keeps independent assigned copies.
- **Restore:** exact API restore creates a local safety ZIP before replacing the shared managed set; local-only managed skills remain untouched.

Changed shared skills and pending share-deletion tombstones receive independent two-minute stability windows. Stable content is uploaded automatically with unlimited retries, with 30 seconds between failures.
The five-second observer hashes incrementally and performs a bounded full verification; disabling `skillsAutoUpload` disables this observation.

## Automatic runtime updates

Enable **Automatically update Codex Monitor** on the management Config page to check the published `release/runtime` manifest before data migrations and dashboard startup, then every hour in the background. The initial check finishes before normal startup continues; a failed check is reported and startup continues. Console messages show the check result and download/verification progress. A newer verified runtime replaces only files named by the release manifest and then restarts the same entry command with the same arguments; failed checks leave the running installation unchanged. Before restarting, the updater installs the new requirements with the running Python interpreter. If package installation fails, runtime files are restored and the update reports failure; pip changes to the Python environment are not rolled back.

## WebDAV and encrypted synchronization

Open **Manage skills & accounts → Config file**. Configure the remote without manually editing secrets unless recovery requires it.

| Setting | Purpose |
| --- | --- |
| Enabled | Turns WebDAV-backed features on or off. |
| Base URL | HTTPS WebDAV endpoint. Plain HTTP is allowed only for literal loopback development URLs. |
| Username / password | WebDAV credentials. The login password must remain locally recoverable because it is sent to the server. |
| Remote root | Isolated directory used by this application. |
| Encryption passphrase | Optional second-layer AES-256-GCM encryption. Every machine must use the same normalized URL, username, and passphrase. |
| Skills auto upload | Watches stable managed-skill changes and uploads only changed packages. |
| Usage data auto sync | Synchronizes the encrypted incremental usage journal every 60 minutes. |
| Allow optimistic writes | Permits servers that ignore conditional writes; account exclusivity then becomes best-effort. |

Use **Test WebDAV** before Push. Jianguoyun/Nutstore users can use `https://dav.jianguoyun.com/dav/` with an application password.

The sync root has one global cloud data contract in `data-contract.json`. Every operation claims the root’s `sync-lock.json` using a native file lock and an atomic ETag-based owner claim. Its `writing` flag becomes 1 before writes and returns to 0 before unlock; readers claim the same file for consistency. Folder locking is unnecessary, and infinite file locks are supported. Older cloud contracts migrate automatically; newer versions block access until the local script is updated. Optimistic writes cannot bypass coordination; interrupted ownership requires explicit recovery. Update all participating clients. See [cloud contract and recovery details](cloud-data-contract.md).

Manual and automatic WebDAV operations share a server-side queue and run one at a time. The management page shows a spinner and the running-plus-waiting count in the bottom-right corner. Click it to inspect operations or remove waiting work.
Share/Unshare Skill and Unmanage apply their local changes when execution starts, so removing waiting work prevents those changes too. Each operation has one queued/start notification and one outcome notification, including skipped, cancelled, and partially completed work.
The queue survives page reloads and closed tabs, but waiting operations are discarded when the monitor stops. Targets and configuration are revalidated before execution. Identical adjacent sync requests and compatible automatic skill uploads can share work; other actions retain their order.

Queued management POSTs return HTTP `202` with `operationId` and `sessionId`. Authenticated clients read progress and recent results from `GET /api/manage/cloud/queue` and remove waiting work with `POST /api/manage/cloud/queue/cancel` and an `operationId` JSON field.

Manual **Push** always publishes managed-skill changes and local recorded usage data. Manual **Fetch** always refreshes remote-account metadata, merges managed-skill changes, and downloads recorded usage data from other machines. These manual transfers run even when their automatic options are disabled. Account credentials are transferred only through explicit **Share**, **Link**, **Release**, and **Bind** actions.

The encryption passphrase is converted with scrypt and immediately cleared from the staging field. Its deterministic salt is derived from the normalized WebDAV URL and username so another machine can derive the same key.
Changing an existing passphrase downloads, authenticates, re-encrypts, uploads, and verifies every known encrypted object; local configuration is committed only after the whole remote rotation succeeds, and partial remote writes are rolled back on failure.

If authentication works but decryption fails, the management page can reload local configuration and retry or overwrite the inaccessible cloud root from local data. Overwrite permanently removes cloud-only skills, released accounts, and usage history.

### Usage-data synchronization

Automatic usage synchronization supplements the directional manual Push and Fetch actions:

- It runs every 60 minutes when `usageDataAutoSync` is enabled. The one-hour interval is measured from the last attempted sync, so both successful and failed attempts delay the next attempt by one hour.
- It synchronizes quota history and every event-level token-ledger record. Cost/usage chart points and token-session summaries are rebuilt locally and are never synchronized.
- SQLite keeps raw local quota readings, including repeats and rejected readings. Cloud synchronization preserves the raw history;
  gaps over four hours remain separate and unmarked so charts can distinguish compaction from a real monitoring outage.
- It excludes credentials, skill contents, detailed sample logs, and runtime state.
- Each machine publishes a v4 encrypted head pointing to month catalogs, day manifests and immutable data partitions. Changed days are uploaded and verified before the head commits with a conditional ETag.
- Fetch compares remote day hashes with the SQLite cache and downloads missing or changed partitions. Removed machine origins are reconciled after reading the complete inventory.
- Usage account IDs are domain-separated hashes of account identity and stay unchanged when the encryption passphrase changes. Ordered migration converts old IDs before normal loading; runtime account matching uses current IDs only.
- Dashboard projections and prepared account snapshots share the typed SQLite store. SQLite facts and token/session totals remain authoritative; recorder files are write-only logs and obsolete JSON display caches are removed during migration.

Every 60 minutes, periodic Fetch also checks the authoritative skill index and refreshes the remote-account list. Strong head ETags skip unchanged usage inventories. Every 30 days a full fetch verifies active remote partitions. The first periodic Fetch runs immediately when no previous attempt is recorded or when that attempt is at least one hour old.

## Data and privacy

The canonical data root is `~/.codex-switch`:

| Path | Contents | Cloud synchronized? |
| --- | --- | --- |
| `config.json` | Server and auto-update settings, plaintext WebDAV login password, cookie secret, password verifier, and derived encryption key | No |
| `accounts/` | Sensitive Codex account vault and manifest | Explicit API Share/Link or OpenAI Release/Bind only |
| `skills/` | Private managed skill source | Optional encrypted packages |
| `usage_monitor_quota_readings.jsonl` | Write-only backup of raw quota readings | Raw SQLite records, encrypted |
| `usage_monitor_token_events.jsonl` | Write-only backup of usage events, stored costs, and corrections | Every record, encrypted |
| `usage_monitor_diagnostic_samples.jsonl` | Write-only diagnostic backup | Never |
| `usage_monitor_state.json` | Compatibility status output; SQLite owns restart state | Never |
| `webdav.log` | Local WebDAV operation and HTTP diagnostics, capped at 5 MB (5,000,000 bytes); oldest complete entries are removed when full | Never |
| `usage_monitor_sync_cache-v4.sqlite3` | Typed local/remote facts, incremental processing, dashboard projections and bulk/live interpretation data | Never uploaded as a database |

Each token event stores its cost calculated using the price effective at the event time. Stored costs remain authoritative after pricing changes; redundant price-epoch records and references are removed by the ordered data-contract migration after verifying totals. Existing session-only totals are preserved as schema 2 `sessionBaseline` records, which contribute to token summaries but not Cost vs Usage charts. Source high-water totals are retained to prevent historical usage from being replayed.

SQLite is authoritative for usage facts, token costs, parser checkpoints, synchronization ownership, and derived charts. Python ingestion reads original Codex session files and writes records directly to SQLite. The same transaction queues independent JSONL backup appends; failed log writes retry without blocking committed usage data. Startup, queries, synchronization, renaming, repair, and source reconciliation never read recorder JSONL. Contract 19 converts existing complete SQLite data to fixed local source identities and finalizes saved database processing state. It does not read or import recorder histories.

> [!WARNING]
> Protect the whole `~/.codex-switch` directory. Never commit it, place it in support bundles, log it, or share screenshots of its contents. Losing the encryption passphrase makes encrypted remote data unrecoverable.

For WebDAV failures, inspect `webdav.log` as JSON lines: Unix timestamps and operation IDs link queued/running/completed operations to individual requests, HTTP statuses, durations, byte counts, retries, and allowlisted response headers
(including `Retry-After`, rate-limit headers, and server request IDs). Credentials, authorization/cookie/lock headers, and request/response bodies are omitted. Treat remote paths and diagnostic error messages as private.
Logging is automatic; a full log drops its oldest entries, and a logging I/O failure does not interrupt synchronization.

Dashboard series and status are intentionally readable from the configured server address. Every account, skill, WebDAV, cloud, server, and configuration mutation requires the control password.
Control requests are accepted through public addresses and reverse proxies without an origin restriction so cloud deployments retain all dashboard features.

First-time control-password setup is loopback-only. If you do not need LAN or WAN access, change the server host to `127.0.0.1` and restart the monitor.
The default `0.0.0.0` listens on all interfaces and permits full password-protected dashboard use through the request's LAN or public IP address when the network routes port 8765 to the monitor.
Direct public-IP access uses unencrypted HTTP, so prefer a trusted VPN or an HTTPS reverse proxy rather than exposing port 8765 directly to the Internet.

## Command-line reference

```console
python codex_monitor_daemon.py --help
```

| Option | Description |
| --- | --- |
| `--dashboard` | Open the dashboard after the server starts. |
| `--codex-home PATH` | Use a different Codex home containing `sessions/` and usually `auth.json`. |
| `--auth PATH` | Override the live authentication file. |
| `--interval SECONDS` | Set the remote usage polling interval; default is 90 seconds. |
| `--timeout SECONDS` | Set the per-request timeout; default is 10 seconds. |
| `--quota-history PATH` | Override the write-only quota backup path. |
| `--token-ledger PATH` | Override the write-only token/cost backup path. |
| `--sample-log PATH` | Override the write-only diagnostic backup path. |
| `--sample-log-max-bytes N` | Deprecated compatibility option; write-only logs are never trimmed. |
| `--local-only` | Scan local session logs without calling ChatGPT usage endpoints. |
| `--no-token-scan` | Disable local session token scanning. |
| `--reencrypt-cloud` | Refresh nonces and verify all encrypted WebDAV payloads using the configured key, then exit. |
| `--retry-limit N` | Set bounded HTTP/dashboard retries; network outages continue retrying. |

## Troubleshooting

### The VS Code status bar cannot connect

- Confirm `python codex_monitor_daemon.py` is still running.
- Confirm `http://127.0.0.1:8765/api/status` opens locally.
- Check whether another process owns port `8765`.
- The extension always connects to `127.0.0.1:8765`, even when the server also listens on the LAN.

### The dashboard is waiting for login

- Complete the normal Codex login in a new or restarted terminal.
- Confirm the expected `CODEX_HOME/auth.json` exists.
- Do not manually copy credentials between managed account slots.

### A control password is reported as compromised

A nonempty `passwordHash` without its separate valid `passwordSalt` is rejected. Stop the monitor, remove only `control.passwordHash` from `~/.codex-switch/config.json`, restart, and create a new password locally. Do not reuse `123456`.

### WebDAV authentication works but encrypted data does not open

- Verify the normalized base URL, username, remote root, and passphrase match the other machine.
- Reload configuration and retry before choosing overwrite.
- Treat overwrite as destructive to cloud-only data.

### A skill cannot be assigned

- Inspect the target application's skill folder and the projection state shown in skill management.
- Selecting **link to** replaces any existing same-name target skill; scanning alone reports the conflict without replacing it or recording a projection error.
- On Windows, ensure the account can create a symlink or native junction at the target.

## Development

Run from the repository root:

```console
python -m pip install -r requirements.txt
python -m unittest discover -p "test_*.py"
npm run check
python codex_monitor_daemon.py --help
```

Build a reproducible deployment bundle:

```console
python build_release.py
```

or:

```console
npm run release
```

The builder increments the patch version in `package.json`, recreates generated files in `release/`, packages the pinned VSCE version, copies the standalone runtime and GPL license, removes obsolete versioned VSIX files, creates `release_pack/code-monitor-v<VERSION>.zip`, stages the complete working tree, and creates a Git commit named `v<VERSION>`. It runs only when explicitly invoked with one of the commands above.
Credentials, local history, caches, tests, reference sources, and development-only material are excluded.

### Repository layout

| Path | Responsibility |
| --- | --- |
| `codex_monitor_daemon.py` | Canonical CLI entry point, monitor startup, automatic update coordination, and restart. |
| `monitor_codex_usage.py` | Compatibility wrapper for the former entry command. |
| `monitor_auto_update.py` | Verified background runtime updater. |
| `monitor_migrations.py` | Local/cloud contract targets, ordered migrations, rollback recovery and old-format conversion. |
| `monitor_dashboard.py` | Polling loops, dashboard/API server, response caches, control authorization, and UI datasets. |
| `monitor_accounts.py` | Local credential vault, identity-safe switching, API copy transfer, and OpenAI move transfer. |
| `monitor_cloud.py` | Configuration, WebDAV, encryption, serialized cloud operations, packages, and usage journal. |
| `monitor_skills.py` | Skill discovery, managed storage, validation, assignments, and projections. |
| `monitor_tokens.py` | Incremental session-log parsing, token aggregation, Fast attribution, and cost calculation. |
| `monitor_token_ledger.py` | Current token/cost ledger and derived session totals; historical readers live in migration-only modules. |
| `monitor_events.py` / `monitor_quota.py` | Remote usage interpretation, reset handling, and delta validation. |
| `monitor_history.py` / `monitor_usage_sync.py` | Local persistence, compaction, provenance, synchronized cache, and merged datasets. |
| `extension.js` / `package.json` | Thin VS Code extension host and manifest. |
| `dashboard.html` / `management.html` | Local user interfaces. |
| `build_release.py` | Reproducible VSIX and standalone-runtime builder. |

## License

Codex Usage Monitor is free software licensed under the <a href="./LICENSE">GNU General Public License version 3</a>.

### Quota sampling and raw history

Quota JSONL is a write-only backup of extracted API readings, including repeated and rejected readings. SQLite owns the raw history, processed quota decisions, per-account/window decrease protection, and cloud publication inputs. Deleting, truncating, or editing recorder JSONL does not change active data. Database backups must preserve synchronization ownership as well as facts. Quota charts show exact backend values at every resolution, without rate filtering, rounding smoothing, or averaging.

Only `reset_at` determines resets; `reset_after_seconds` is ignored. Every decrease is rejected unless its reset timestamp moves forward and is strictly later than the last accepted sample time plus the window period (5h or 7d). A rejected decrease establishes a protection baseline: the next equal or higher reading is accepted; a lower reading is rejected and replaces the baseline. Accepted readings clear protection. Missing windows retain their state, plans establish fresh baselines, and 5h/7d windows are independent. Rejected points are never accepted retroactively.

Recorder logs are append-only, including diagnostics. Corrections append an ordinary updated record with the same identity. Removing a database record does not append an operation or alter earlier log entries. Batches begin with a newline to isolate torn tails without reading the log. A crash between fsync and queue acknowledgement may duplicate a backup delivery. Manual rebuilding from JSONL is controlled by external tools or people. The application provides no recovery reader, replay rules, or manual rebuild command. Normal startup and migration 19 never import these logs.
