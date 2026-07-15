# Codex Usage Monitor

Codex Usage Monitor shows the authoritative 5-hour and 7-day usage reported by a manually started Python monitor in the VS Code status bar. Click the status-bar item to open **Codex Usage Details**, which loads the current dashboard UI and data from the same Python server as the browser dashboard.
The dashboard also plots saved 5-hour and 7-day usage percentages over time for the accounts selected by the shared Accounts filter and Date/5h/24h/7d/30d/All range selector.

Start the monitor before using the extension:

```console
python monitor_codex_usage.py
```

## Deployment release

Run `python build_release.py` (or `npm run release`) to rebuild the self-contained `release/` directory. It contains the versioned VS Code extension package and a `runtime/` directory with the complete standalone Python monitor, dashboard assets, dependency list, and runtime instructions. Install the `.vsix`, copy `runtime/` to the target machine, run `python -m pip install -r requirements.txt` there, and then start `python monitor_codex_usage.py`.

The release builder recreates only generated files inside `release/`. Local credentials, account vaults, usage history, configuration, logs, caches, tests, reference projects, and development review files are excluded from both the release and the VS Code package.

The monitor serves the Python dashboard/API on fixed port 8765. Its bind IP is selected on the management page's **Config file** tab: `127.0.0.1` limits access to this computer, while `0.0.0.0` listens on every network interface. Restart the monitor after changing it. The extension always connects through `http://127.0.0.1:8765`, so either bind choice remains compatible. The default command does not open a browser; use `python monitor_codex_usage.py --dashboard` to open the dashboard in the default browser after starting the server. The extension fetches the current dashboard HTML whenever the details panel is opened or its command is invoked again, so dashboard UI changes do not require repackaging the extension. Its bundled dashboard remains available as a fallback when the live HTML cannot be fetched. The extension never starts another Python process, does not parse Codex session logs for rate-limit values, and contributes no VS Code settings. Monitor behavior remains configured through `~/.codex-switch/config.json` and the defaults in `monitor_codex_usage.py`.
The status bar and visible dashboard poll the lightweight `/api/status` endpoint every five seconds. Historical `/api/series` data is loaded when the dashboard opens and only reloaded when its revision changes; hidden dashboards skip polling,
concurrent requests are deduplicated, and unchanged series responses reuse a pre-serialized in-memory cache. `/api/series` remains available for older clients, and the extension falls back to it when connected to an older monitor without `/api/status`.

## Requirements

- Python 3.12 or newer on Windows and Linux.
- A manually running `python monitor_codex_usage.py`.
- A working Codex login in the normal `CODEX_HOME`.
- The proxy requirements enforced by `monitor_common.py`.
- Python dependencies installed with `python -m pip install -r requirements.txt`.

## Skills and encrypted backup

Open **Manage skills & accounts** from the dashboard. At startup the backend scans `CODEX_HOME/skills` and `~/.gemini/config/skills` for child directories containing `SKILL.md`.
The management page reuses that result until **Scan skills** explicitly refreshes it or a backend skill operation invalidates it.
Choosing **Manage selected** verifies and moves each skill into `~/.codex-switch/skills`, then creates strict per-skill directory symlinks for its default application. Skill names use a portable ASCII allowlist.
On Windows, a native directory junction is used when creating a symbolic link is not permitted. On non-Windows platforms, a monitor-owned directory copy is used when symbolic links are unavailable; it is refreshed from the private managed source and removed only when its opaque ownership marker matches local state. Projection creation never invokes a command shell.
A failed symlink leaves the verified private copy managed and reports a retryable projection error.
A managed skill takes precedence over a valid same-name local skill directory, replacing it with the managed link and marking that application assigned; unrelated files and links remain conflicts and are preserved.
Codex content wins when both application directories contain the same unmanaged skill name, and that skill defaults to both projections.

Assignments are stored in versioned `skills.json` and remain local to the machine. Each managed skill and its tombstone history use a separate encrypted cloud package; a small encrypted index identifies the current package for each skill. Packages contain no assignment state.
An explicit WebDAV Fetch merges remote additions and updates into the managed skill store without removing managed-only skills; remote content overwrites same-name managed content, and existing assignments are preserved.
Push merges the local managed set with the current remote snapshot without removing remote-only skills; local content overwrites same-name remote content.
Manual Push compares every local skill with the cloud and uploads separate packages only for changed skill names. If everything already matches, it performs no index rewrite and reports **Nothing to push**; otherwise the success notification's double-click detail lists added and updated skills. Accounts are never uploaded by Push.
Newly fetched skills remain unassigned unless a same-name local skill is replaced by the managed link. Fetch refreshes the released-account listing but does not download, restore, or upload account files; account transfer requires explicit **Bind** or **Release**.
The backend exact-restore operation remains available for API clients, creates a local safety ZIP, replaces the private set exactly, and preserves surviving assignments.

Edit `~/.codex-switch/config.json` to configure WebDAV, then restart the monitor; configuration, machine identity, and cloud state are loaded into backend memory at startup.
Enabled WebDAV endpoints must use HTTPS; plaintext HTTP is accepted only for literal localhost, `127.0.0.1`, or `::1` development endpoints.
The management page and every account, skill, and cloud control API require the control password. New and migrated configurations start with `123456`; change it before relying on the control lock. `control.password` is an import field: put a new password there and restart, after which it is cleared and replaced by a salted scrypt `control.passwordHash`. A signed `HttpOnly` cookie remembers successful unlocks for 30 days, including in the VS Code extension.
The WebDAV login `password` remains plaintext because it must be sent to the WebDAV server.
Authorization and account-identity headers are scoped to the original request and are never copied to redirects; authenticated cross-origin and HTTPS downgrade redirects are rejected.
WebDAV encryption uses only `encryptionPassphraseHash`; raw or protected passphrase fields are not supported. The hash is reproducible offline with scrypt `n=32768`, `r=8`, `p=1`, a 32-byte output,
and the fixed public salt `codex-switch-passphrase-v1`. Run `python -c "from getpass import getpass; from monitor_cloud import passphrase_hash; print(passphrase_hash(getpass('Passphrase: ')))"` from the repository,
then place the result in `webdav.encryptionPassphraseHash`. A remote root and every machine using it must use that exact hash.
Run `python monitor_codex_usage.py --reencrypt-cloud` to refresh nonces and verify every encrypted payload with the already configured key. This command does not rotate from a different old hash.
Backend operations keep the in-memory cloud state synchronized with atomic file writes.
A remote encryption descriptor is initialized only after an exact WebDAV 404, then read back and protected by verified `If-None-Match: *` behavior.
Network, authentication, and server failures never trigger descriptor replacement.
Jianguoyun/Nutstore uses `https://dav.jianguoyun.com/dav/` and an application password. API responses redact the password and encryption passphrase. Use **Test WebDAV** before Push.
Push intentionally overwrites same-name cloud skills and never uploads accounts. Conditional ETag writes protect skill-pointer merges and the explicit Bind/Release account moves when the server enforces them.
Servers such as Jianguoyun that ignore conditional writes can use `allowOptimisticWrites`, but account exclusivity is then best-effort rather than guaranteed.
Skill and account payloads use AES-256-GCM with a scrypt-derived key, authenticated manifests, SHA-256 hashes, sizes, and read-back verification.
Salted scrypt values prevent direct recovery of raw control passwords and encryption passphrases, but weak or reused passwords can still be found by offline guessing.
Use long, unique values; a stored derived cloud key also grants decryption access to someone who separately obtains the WebDAV ciphertext and credentials.

`config.json` intentionally stores the WebDAV login password and cookie-signing secret locally. The control password is stored only as a randomly salted verifier,
and the encryption credential only as the fixed-salt scrypt-derived key. The file is written with restrictive permissions where the platform supports them.
It is sensitive: never commit, sync, log, screenshot, or include `~/.codex-switch` in support bundles. Losing the passphrase makes remote backups unrecoverable.
Keep a separate protected recovery copy of the configuration and restart affected Codex terminals after an account switch or release because running processes may retain credentials.

Cloud traffic is event-driven. **Push** uploads managed skills only. Skills are name-merged with the current cloud snapshot, with local same-name content taking precedence and cloud-only names retained. Push, Fetch, bind, release, restore, and WebDAV test operations share one serialized backend operation queue so simultaneous requests cannot mutate cloud state concurrently. Rename and Delete are local-only account operations.
User-initiated Push, Fetch, Bind, Release, Restore, WebDAV Test, and cloud-backed Unmanage actions immediately show a persistent Started message, then replace it with a Finished or Error message when the operation returns.
Editing managed skill content starts that skill's own two-minute stability window. Once stable, only that skill package is uploaded; edits to another skill neither delay nor join the upload. Binding and releasing accounts do not schedule Push because those actions complete their own move directly.
The five-second change check compares canonical manifests without creating ZIP files. Unchanged files reuse identity, size, and nanosecond-time keyed hashes, with a full content rehash at least hourly.
ZIP construction and a fresh race-safe content verification happen only when a snapshot is actually needed. Disabling `skillsAutoUpload` also disables this periodic observation.
If a skill changes during its upload, its new version starts another two-minute stability window. Each stable skill version is attempted at most three times with 30 seconds between failures.
A final failure appears in the management page and marks the Push button, while successful automatic pushes remain silent.
Every five minutes the backend runs Fetch: an unchanged version-2 skill index compares its immutable package hashes with cached local content manifests and downloads only mismatched skill packages, while an unchanged legacy combined snapshot is skipped when its applied baseline still matches. The released cloud-account candidate list is refreshed even when no skill package is downloaded. Account files still move only through explicit Bind and Release. An explicit fetch, Push, or restore establishes the local skill baseline.

When WebDAV is enabled, encrypted usage-data synchronization is also enabled by default through `webdav.usageDataAutoSync`. It runs independently every 30 minutes and synchronizes only delta/cost intervals, quota history, and token-session history; detailed sample logs, runtime state, credentials, and skill data are excluded. Set `usageDataAutoSync` to `false` and restart to disable it without changing skill or account cloud behavior.

Usage traffic is incremental. Each machine publishes a small conditional-write pointer plus compressed, encrypted immutable chunks containing only new or changed locally originated records and retention tombstones. Other machines use pointer ETags and applied-head cursors to download only unseen chunks. The first publication is a live checkpoint, and a later checkpoint replaces a long revision tail only when it reduces bootstrap transfer by at least 25 percent. Quota synchronization run-length compacts idle periods per account: for an unchanged 5h/7d percentage, plan, and reset-cycle plateau it publishes only the first and latest observation, followed by the first changed observation. For example, five readings at 5 percent followed by 6 percent publish only A, E, and F; the complete A-F history remains local. Existing quota and token history is adopted on first sync; old compact cost pairs remain local because they do not contain enough interval metadata for safe cross-machine correlation.

Downloaded usage records never replace or append to the three local recorder files. They are retained by source machine in `usage_monitor_sync_cache.json`, and the dashboard derives a separate Merged dataset from that cache plus the untouched local data. Use the **Local** and **Merged** buttons to switch token and cost/delta views; Local is the default. The 5h and 7d Usage-vs-Time curves are always Merged because quota percentage is shared across machines. On the first startup with this separated layout, identifiable remote-origin rows written by older versions are moved from the recorder files into the cache and remote cursors are cleared once so authoritative checkpoints can rebuild any source revisions previously collapsed by merging.

Cross-machine cost ratios are derived from source intervals rather than by adding already-derived percentages. For a matching account, window, plan, and reset cycle, the monitor splits overlapping `startPercent → endPercent` intervals at every observed boundary, counts each percentage slice once, and adds each covering machine's proportional per-model cost. Thus two machines that each spend $1 while shared usage rises 1 percent produce $2 per percent even when their polling timestamps differ.
Routine OpenAI token refreshes update only the local vault. Account uploads occur for explicit **Push**, when Fetch confirms that this machine owns the binding, and when registration, binding, release, or interrupted-operation recovery requires verified cloud state.

## Account switching

The dashboard account selector switches only the live Codex `auth.json`. All accounts continue using the same `CODEX_HOME`, including its `config.toml`, MCP servers, sessions, conversation context, prompts, and skills.

On the first monitor startup, the current `auth.json` is saved as **Current account**. All monitor-owned saved data lives under `~/.codex-switch`: account credentials and the manifest are under `accounts`.
Delta history, compact per-account quota history, per-session token/cost history, the detailed sample log, runtime state, and the downloaded usage cache use `usage_monitor_history.jsonl`, `usage_monitor_quota_history.jsonl`, `usage_monitor_token_sessions.jsonl`, `usage_monitor_samples.jsonl`, `usage_monitor_state.json`, and `usage_monitor_sync_cache.json`. The first three files remain local recorder output; the cache is machine-local derived synchronization state. Per-session token counts remain raw counts; cost applies the recorded Fast service tier at 2x for GPT-5.4, 2.5x for GPT-5.5 and GPT-5.6, and a defensive 2x fallback for any other model unexpectedly reported as Fast.
The detailed sample log remains ordinary backward-compatible JSONL. It appends normally up to its configured cap, then atomically streams the newest complete rows into a replacement file targeted at 80% of the cap.
This provides hysteresis before another compaction is needed.
Startup idempotently seeds quota history from available raw readings retained in the detailed sample log, including original percentages that delta validation rejected, then appends each fresh reading without model separation.
Quota persistence occurs before trusted-baseline and local-cost delta processing, so those rules never suppress the percentage curve. The first startup after upgrading moves these files from their legacy repository/auth-adjacent locations when the destination does not already exist.
The `accounts/` directory contains refresh tokens and must be treated like `auth.json`: do not copy `.codex-switch` into a repository, logs, screenshots, or support bundles.

To add an account, open **Manage skills & accounts**, choose **Create / login**, enter a name, and confirm. The monitor atomically saves the current login and removes live `auth.json`; then run Codex login in a new or restarted terminal. The dashboard detects the next valid login automatically. Restart existing Codex terminals after switching accounts because a running process may retain its previous authentication state.

The monitor serializes account switching only with the credential-refresh section of polling. Ordinary percentage requests using a still-valid access token do not block account switching; if an account changes while such a request is in flight, its stale result is discarded. When OpenAI rotates a refresh token, the complete updated `auth.json` is mirrored into the active saved account before the usage request continues. Usage history remains shared, while account-switch boundaries reset delta baselines so usage from two accounts is not joined into one interval.

Before saving the outgoing account for a switch or new login, the monitor requires the live and saved `auth.json` copies to have identical `tokens.id_token` and `tokens.account_id` values. A mismatch or missing verification field refuses the account change without replacing either credential copy and displays a safe error in the dashboard. Managed labels identify local `auth.json` slots only: authenticated-account matching and usage-history merging use `tokens.account_id`, with a hash of `tokens.id_token` as the fallback. A machine cannot keep the same authenticated identity in two ready managed slots. Bind reports an error and leaves the released cloud payload untouched if that identity already exists locally. If a newly added empty slot detects a login already managed by another slot, the existing slot receives the new `auth.json`, the live file is removed, and the new slot stays empty for another login; the dashboard reports what happened.

Successful new-account preparation and account switches print a concise `Account event:` message to the monitor console. These messages contain escaped display labels only and never include credential contents.

Account cloud storage uses move semantics rather than backup semantics. **Bind** downloads and integrity-checks the encrypted account file without testing whether its token works, saves it in the local vault, and only then removes and verifies removal of the cloud copy. **Release** uploads and verifies the newest local account file before switching or removing the local vault record. Empty accounts are transferred as awaiting-login placeholders, so another machine can Bind, sign in, and Release them normally. A locally bound account therefore has no cloud copy. **Rename** and **Delete** affect only local records and never remove or rewrite cloud account payloads. If Bind cannot remove the cloud payload, the verified local copy is retained for recovery; if Release cannot verify its upload, the local vault, manifest, and live `auth.json` remain unchanged.

Delta history is attributed independently by both model and account. Account buttons in the dashboard filter and rebase the charts just like model buttons, and both filters can be combined. Existing delta rows are assigned to **Nomei Plus**. New rows record the matching saved account slot and label, or **Unknown** when the live credentials do not match a registered account. A saved account's current label is used while it exists; the recorded label remains available after deletion.

Token usage is recorded per Codex session using cumulative-to-delta parsing aligned with cc-switch. The dashboard summary between Reset Time vs Usage and Usage vs Time shows fresh input, cached input, output, cache write, cache hit rate, and estimated token cost for the selected time span, models, and accounts. Cache reads use `cached_tokens`; cache writes use `cache_write_tokens` and are charged at the model's cache-write input rate when applicable. A session keeps the account attribution assigned when it is first recorded.
Session logs are scanned incrementally in process memory. Unchanged files require metadata checks only, appended files are read from the previous byte offset, and only a truncated, replaced, or same-size rewritten file is rebuilt.
Partial JSONL records, replay boundaries, model/service-tier state, and cumulative token state are carried across polls; no cursor data is persisted.

Quota history is attributed by account only. The usage-over-time charts follow the shared Accounts filter, draw a separately colored curve for every selected account, and do not follow the model filter.
A curve always connects gaps of at most 30 minutes and always breaks gaps of at least two hours. For gaps over 30 minutes but under two hours, the inclusive absolute raw-usage thresholds are 10 percentage points for Plus and 2 points for Pro or Pro Lite on the 5h chart, and 5 points for Plus and 1 point for Pro or Pro Lite on the 7d chart. Unknown and mixed-plan readings use the conservative Pro threshold for that window.
Use `--quota-history` to override the quota-history path; `--compact-history-days` applies to both delta and quota history.

Access-token lifetime is estimated from the JWT `iat` and `exp` claims. The monitor refreshes once 30% of that lifetime remains; for example, a 10-day token is refreshed during its final 3 days. Tokens without a usable `iat` claim retain the 60-second fallback margin.
