const fs = require("fs");
const http = require("http");
const path = require("path");
const vscode = require("vscode");

const API_POLL_INTERVAL_MS = 5000;
const TOOLTIP_HOVER_DELAY_SECONDS = 3;
const DASHBOARD_URL = new URL("http://127.0.0.1:8765/");
const MANAGEMENT_URL = new URL("http://127.0.0.1:8765/manage");
const STATUS_API_URL = new URL("http://127.0.0.1:8765/api/status");
const SERIES_API_URL = new URL("http://127.0.0.1:8765/api/series");
const ACCOUNT_CREATE_URL = new URL("http://127.0.0.1:8765/api/accounts");
const ACCOUNT_SWITCH_URL = new URL("http://127.0.0.1:8765/api/accounts/switch");
const ACCOUNT_RENAME_URL = new URL("http://127.0.0.1:8765/api/accounts/rename");
const ACCOUNT_DELETE_URL = new URL("http://127.0.0.1:8765/api/accounts/delete");
const CONTROL_LOGIN_URL = new URL("http://127.0.0.1:8765/api/control/login");
const CONTROL_SETUP_URL = new URL("http://127.0.0.1:8765/api/control/setup");
const ACCOUNT_ACTION_TIMEOUT_MS = 300000;
const PAGE_ALLOWLIST = Object.freeze({ dashboard: { url: DASHBOARD_URL, asset: "dashboard.html", title: "Codex Usage Details" }, manage: { url: MANAGEMENT_URL, asset: "management.html", title: "Codex Monitor Management" } });
const MANAGEMENT_ACTION_ALLOWLIST = new Map([
    ["/api/control/login", { url: CONTROL_LOGIN_URL, method: "POST" }],
    ["/api/control/setup", { url: CONTROL_SETUP_URL, method: "POST" }],
    ["/api/manage/status", { url: new URL("http://127.0.0.1:8765/api/manage/status"), method: "GET" }],
    ["/api/manage/status?scan=1", { url: new URL("http://127.0.0.1:8765/api/manage/status?scan=1"), method: "GET" }],
    ["/api/manage/status?remote=1", { url: new URL("http://127.0.0.1:8765/api/manage/status?remote=1"), method: "GET" }],
    ["/api/manage/skills/manage", { url: new URL("http://127.0.0.1:8765/api/manage/skills/manage"), method: "POST" }],
    ["/api/manage/skills/unmanage", { url: new URL("http://127.0.0.1:8765/api/manage/skills/unmanage"), method: "POST" }],
    ["/api/manage/skills/assign", { url: new URL("http://127.0.0.1:8765/api/manage/skills/assign"), method: "POST" }],
    ["/api/manage/cloud/test", { url: new URL("http://127.0.0.1:8765/api/manage/cloud/test"), method: "POST" }],
    ["/api/manage/cloud/fetch", { url: new URL("http://127.0.0.1:8765/api/manage/cloud/fetch"), method: "POST" }],
    ["/api/manage/cloud/push", { url: new URL("http://127.0.0.1:8765/api/manage/cloud/push"), method: "POST" }],
    ["/api/manage/cloud/restore", { url: new URL("http://127.0.0.1:8765/api/manage/cloud/restore"), method: "POST" }],
    ["/api/manage/cloud/overwrite", { url: new URL("http://127.0.0.1:8765/api/manage/cloud/overwrite"), method: "POST" }],
    ["/api/manage/accounts/bind", { url: new URL("http://127.0.0.1:8765/api/manage/accounts/bind"), method: "POST" }],
    ["/api/manage/accounts/release", { url: new URL("http://127.0.0.1:8765/api/manage/accounts/release"), method: "POST" }],
    ["/api/manage/accounts/delete", { url: ACCOUNT_DELETE_URL, method: "POST" }],
    ["/api/manage/server", { url: new URL("http://127.0.0.1:8765/api/manage/server"), method: "POST" }],
    ["/api/manage/config", { url: new URL("http://127.0.0.1:8765/api/manage/config"), method: "POST" }],
    ["/api/manage/config/reload", { url: new URL("http://127.0.0.1:8765/api/manage/config/reload"), method: "POST" }],
    ["/api/accounts", { url: ACCOUNT_CREATE_URL, method: "POST" }],
    ["/api/accounts/switch", { url: ACCOUNT_SWITCH_URL, method: "POST" }],
    ["/api/accounts/rename", { url: ACCOUNT_RENAME_URL, method: "POST" }],
]);

class PythonMonitor {
    constructor(statusBar) {
        this.statusBar = statusBar;
        this.pendingStatusRequest = null;
        this.pendingSeriesRequests = new Map();
        this.pollTimer = null;
        this.lastTooltip = null;
    }

    start() {
        this.statusBar.text = "$(plug) Codex usage unavailable";
        this.statusBar.tooltip = "Start the monitor with: python monitor_codex_usage.py";
        this.statusBar.show();
        this.update();
        this.pollTimer = setInterval(() => this.update(), API_POLL_INTERVAL_MS);
    }

    async getStatus() {
        if (this.pendingStatusRequest) return this.pendingStatusRequest;
        this.pendingStatusRequest = requestJson(STATUS_API_URL).catch(error => {
            if (error.statusCode === 404) return this.getSeries();
            throw error;
        }).finally(() => {
            this.pendingStatusRequest = null;
        });
        return this.pendingStatusRequest;
    }

    async getSeries(view = "local") {
        view = view === "merged" ? "merged" : "local";
        if (this.pendingSeriesRequests.has(view)) return this.pendingSeriesRequests.get(view);
        const url = new URL(SERIES_API_URL);
        url.searchParams.set("view", view);
        const request = requestJson(url).finally(() => this.pendingSeriesRequests.delete(view));
        this.pendingSeriesRequests.set(view, request);
        return request;
    }

    async update() {
        try {
            const display = (await this.getStatus()).display || {};
            this.statusBar.text = `$(pulse) ${display.statusBarText || "Codex usage"}`;
            const tooltip = stableTooltip(display);
            if (tooltip !== this.lastTooltip) {
                this.lastTooltip = tooltip;
                this.statusBar.tooltip = tooltip;
            }
            this.statusBar.show();
        } catch (error) {
            this.statusBar.text = "$(plug) Codex usage unavailable";
            const tooltip = error.message || String(error);
            if (tooltip !== this.lastTooltip) {
                this.lastTooltip = tooltip;
                this.statusBar.tooltip = tooltip;
            }
            this.statusBar.show();
        }
    }

    dispose() {
        clearInterval(this.pollTimer);
    }
}

function secondsAgo(value) {
    const timestamp = Date.parse(value || "");
    return Number.isFinite(timestamp) ? `${Math.max(0, Math.floor((Date.now() - timestamp) / 1000) + TOOLTIP_HOVER_DELAY_SECONDS)}s ago` : "-";
}

function stableTooltip(display) {
    const windows = display.windows || {};
    return [
        "Codex Usage",
        `5h: ${windows["5h"]?.usageText || "-"} used, resets ${windows["5h"]?.resetText || "-"}`,
        `7d: ${windows["7d"]?.usageText || "-"} used, resets ${windows["7d"]?.resetText || "-"}`,
        `Last update ${secondsAgo(display.percentCheckedAt)}`,
    ].join("\n");
}

function requestJson(url, options = {}) {
    return new Promise((resolve, reject) => {
        const body = options.body === undefined ? undefined : JSON.stringify(options.body);
        const request = http.request(url, {
            method: options.method || "GET",
            headers: {
                Accept: "application/json",
                ...(options.cookie ? { Cookie: options.cookie } : {}),
                ...(body === undefined ? {} : { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) }),
            },
        }, response => {
            let body = "";
            response.setEncoding("utf8");
            response.on("data", chunk => {
                body += chunk;
            });
            response.on("end", () => {
                let payload;
                try {
                    const setCookie = response.headers["set-cookie"]?.[0]?.split(";", 1)[0];
                    if (setCookie) options.onSetCookie?.(setCookie);
                    payload = JSON.parse(body);
                } catch (error) {
                    const responseError = new Error(response.statusCode !== 200 ? `Python monitor returned HTTP ${response.statusCode}` : `Invalid Python monitor response: ${error.message}`);
                    responseError.statusCode = response.statusCode;
                    reject(responseError);
                    return;
                }
                if (response.statusCode !== 200) {
                    const error = new Error(payload.error || `Python monitor returned HTTP ${response.statusCode}`);
                    error.statusCode = response.statusCode;
                    error.details = payload.details;
                    error.decryptFailed = payload.decryptFailed;
                    reject(error);
                }
                else resolve(payload);
            });
        });
        request.setTimeout(options.timeoutMs ?? 10000, () => request.destroy(new Error("Python monitor request timed out")));
        request.on("error", error => reject(new Error(`Cannot connect to the manually started Python monitor: ${error.message}`)));
        if (body !== undefined) request.write(body);
        request.end();
    });
}

function requestText(url) {
    return new Promise((resolve, reject) => {
        const request = http.get(url, { headers: { Accept: "text/html" } }, response => {
            let body = "";
            response.setEncoding("utf8");
            response.on("data", chunk => {
                body += chunk;
            });
            response.on("end", () => response.statusCode === 200 ? resolve(body) : reject(new Error(`Python monitor returned HTTP ${response.statusCode}`)));
        });
        request.setTimeout(2000, () => request.destroy(new Error("Python dashboard request timed out")));
        request.on("error", reject);
    });
}

function addWebviewCsp(html, webview) {
    const csp = `<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'unsafe-inline'; img-src data:;">`;
    return html.replace('<meta charset="utf-8">', `<meta charset="utf-8">\n${csp}`);
}

async function detailsHtml(context, webview, page = "dashboard") {
    const definition = PAGE_ALLOWLIST[page];
    if (!definition) throw new Error("Unknown bundled page");
    try {
        return addWebviewCsp(await requestText(definition.url), webview);
    } catch {
        return addWebviewCsp(fs.readFileSync(path.join(context.extensionPath, definition.asset), "utf8"), webview);
    }
}

function activate(context) {
    const statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
    statusBar.command = "codexUsageMonitor.showDetails";
    const monitor = new PythonMonitor(statusBar);
    let panel;
    let currentPage = "dashboard";
    let controlCookie = context.globalState.get("codexUsageMonitor.controlCookie");
    const controlRequestJson = (url, options = {}) => requestJson(url, {
        ...options,
        cookie: controlCookie,
        onSetCookie: cookie => {
            controlCookie = cookie;
            context.globalState.update("codexUsageMonitor.controlCookie", cookie);
        },
    });
    context.subscriptions.push(
        statusBar,
        vscode.commands.registerCommand("codexUsageMonitor.showDetails", async () => {
            if (panel) {
                const target = panel;
                target.reveal();
                const html = await detailsHtml(context, target.webview, currentPage);
                if (panel === target) target.webview.html = html;
                return;
            }
            panel = vscode.window.createWebviewPanel("codexUsageDetails", "Codex Usage Details", vscode.ViewColumn.One, {
                enableScripts: true,
                retainContextWhenHidden: true,
            });
            panel.webview.onDidReceiveMessage(async message => {
                const target = panel;
                if (!target) return;
                if (message.type === "codexUsageNavigate") {
                    if (!PAGE_ALLOWLIST[message.page]) return;
                    currentPage = message.page;
                    target.title = PAGE_ALLOWLIST[currentPage].title;
                    target.webview.html = await detailsHtml(context, target.webview, currentPage);
                    return;
                }
                if (message.type === "codexUsageManageAction") {
                    try {
                        const action = MANAGEMENT_ACTION_ALLOWLIST.get(message.path);
                        if (!action) throw new Error("Management action is not allowed");
                        const payload = await controlRequestJson(action.url, { method: action.method, body: action.method === "POST" ? message.body || {} : undefined, timeoutMs: ACCOUNT_ACTION_TIMEOUT_MS });
                        target.webview.postMessage({ type: "codexUsageManageResult", requestId: message.requestId, payload });
                    } catch (error) {
                        target.webview.postMessage({ type: "codexUsageManageResult", requestId: message.requestId, error: error.message || String(error), details: error.details, decryptFailed: error.decryptFailed, status: error.statusCode });
                    }
                    return;
                }
                if (message.type === "codexUsageAccountAction") {
                    try {
                        const url = { create: ACCOUNT_CREATE_URL, switch: ACCOUNT_SWITCH_URL, rename: ACCOUNT_RENAME_URL, delete: ACCOUNT_DELETE_URL, login: CONTROL_LOGIN_URL, setup: CONTROL_SETUP_URL }[message.action];
                        if (!url) throw new Error("Unknown account action");
                        const payload = await controlRequestJson(url, { method: "POST", body: message.body || {}, timeoutMs: ACCOUNT_ACTION_TIMEOUT_MS });
                        target.webview.postMessage({ type: "codexUsageAccountAction", requestId: message.requestId, payload });
                    } catch (error) {
                        target.webview.postMessage({ type: "codexUsageAccountAction", requestId: message.requestId, error: error.message || String(error), details: error.details, status: error.statusCode });
                    }
                    return;
                }
                if (message.type === "getCodexUsageStatus") {
                    try {
                        target.webview.postMessage({ type: "codexUsageStatus", payload: await monitor.getStatus() });
                    } catch (error) {
                        target.webview.postMessage({ type: "codexUsageStatus", error: error.message || String(error) });
                    }
                    return;
                }
                if (message.type === "getCodexUsageSeries") {
                    const view = message.view === "merged" ? "merged" : "local";
                    try {
                        target.webview.postMessage({ type: "codexUsageSeries", view, payload: await monitor.getSeries(view) });
                    } catch (error) {
                        target.webview.postMessage({ type: "codexUsageSeries", view, error: error.message || String(error) });
                    }
                }
            });
            panel.onDidDispose(() => {
                panel = undefined;
            });
            const target = panel;
            const html = await detailsHtml(context, target.webview, currentPage);
            if (panel === target) target.webview.html = html;
        }),
        { dispose: () => monitor.dispose() },
    );
    monitor.start();
}

function deactivate() {}

module.exports = { activate, deactivate };
