const fs = require("fs");
const http = require("http");
const path = require("path");
const vscode = require("vscode");

const API_POLL_INTERVAL_MS = 5000;
const API_URL = new URL("http://127.0.0.1:8765/api/series");

class PythonMonitor {
    constructor(statusBar) {
        this.statusBar = statusBar;
        this.pendingRequest = null;
        this.pollTimer = null;
    }

    start() {
        this.statusBar.text = "$(plug) Codex usage unavailable";
        this.statusBar.tooltip = "Start the monitor with: python monitor_codex_usage.py --dashboard";
        this.statusBar.show();
        this.update();
        this.pollTimer = setInterval(() => this.update(), API_POLL_INTERVAL_MS);
    }

    async getSeries() {
        if (this.pendingRequest) return this.pendingRequest;
        this.pendingRequest = requestJson(API_URL).finally(() => {
            this.pendingRequest = null;
        });
        return this.pendingRequest;
    }

    async update() {
        try {
            const display = (await this.getSeries()).display || {};
            this.statusBar.text = `$(pulse) ${display.statusBarText || "Codex usage"}`;
            this.statusBar.tooltip = display.tooltip || "Codex usage details";
            this.statusBar.show();
        } catch (error) {
            this.statusBar.text = "$(plug) Codex usage unavailable";
            this.statusBar.tooltip = error.message || String(error);
            this.statusBar.show();
        }
    }

    dispose() {
        clearInterval(this.pollTimer);
    }
}

function requestJson(url) {
    return new Promise((resolve, reject) => {
        const request = http.get(url, { headers: { Accept: "application/json" } }, response => {
            let body = "";
            response.setEncoding("utf8");
            response.on("data", chunk => {
                body += chunk;
            });
            response.on("end", () => {
                if (response.statusCode !== 200) {
                    reject(new Error(`Python monitor returned HTTP ${response.statusCode}`));
                    return;
                }
                try {
                    resolve(JSON.parse(body));
                } catch (error) {
                    reject(new Error(`Invalid Python monitor response: ${error.message}`));
                }
            });
        });
        request.setTimeout(10000, () => request.destroy(new Error("Python monitor request timed out")));
        request.on("error", error => reject(new Error(`Cannot connect to the manually started Python monitor: ${error.message}`)));
    });
}

function detailsHtml(context, webview) {
    const csp = `<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'unsafe-inline'; img-src data:;">`;
    return fs.readFileSync(path.join(context.extensionPath, "dashboard.html"), "utf8").replace('<meta charset="utf-8">', `<meta charset="utf-8">\n${csp}`);
}

function activate(context) {
    const statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
    statusBar.command = "codexUsageMonitor.showDetails";
    const monitor = new PythonMonitor(statusBar);
    let panel;
    context.subscriptions.push(
        statusBar,
        vscode.commands.registerCommand("codexUsageMonitor.showDetails", () => {
            if (panel) {
                panel.reveal();
                return;
            }
            panel = vscode.window.createWebviewPanel("codexUsageDetails", "Codex Usage Details", vscode.ViewColumn.One, {
                enableScripts: true,
                retainContextWhenHidden: true,
            });
            panel.webview.html = detailsHtml(context, panel.webview);
            panel.webview.onDidReceiveMessage(async message => {
                if (message.type !== "getCodexUsageSeries") return;
                const target = panel;
                if (!target) return;
                try {
                    target.webview.postMessage({ type: "codexUsageSeries", payload: await monitor.getSeries() });
                } catch (error) {
                    target.webview.postMessage({ type: "codexUsageSeries", error: error.message || String(error) });
                }
            });
            panel.onDidDispose(() => {
                panel = undefined;
            });
        }),
        { dispose: () => monitor.dispose() },
    );
    monitor.start();
}

function deactivate() {}

module.exports = { activate, deactivate };
