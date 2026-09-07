const assert = require("node:assert/strict");
const fs = require("node:fs");
const test = require("node:test");
const vm = require("node:vm");

const html = fs.readFileSync(`${__dirname}/management.html`, "utf8");
const queueScript = html.slice(html.indexOf("const queuedPaths="), html.indexOf("</script>"));
const apiScript = html.split(/\r?\n/).find(line => line.startsWith("function api("));
const operationScript = html.slice(html.indexOf("function cloudOperationFor("), html.indexOf("function showCloudResult("));
const extension = fs.readFileSync(`${__dirname}/extension.js`, "utf8");
const routes = vm.runInNewContext(`${extension.slice(extension.indexOf("const API_POLL_INTERVAL_MS"), extension.indexOf("class PythonMonitor"))}\nMANAGEMENT_ACTION_ALLOWLIST`, { URL });
const flush = () => new Promise(resolve => setImmediate(resolve));

function harness({ bridge = false, storage = new Map(), snapshot = { sessionId: "session", lastEventId: 0, operations: [], completed: [], events: [] } } = {}) {
    const elements = new Map(), messages = [], requests = [], listeners = new Set();
    const element = id => {
        if (!elements.has(id)) {
            const classes = new Set();
            elements.set(id, { hidden: false, textContent: "", innerHTML: "", value: "", attributes: {}, setAttribute(key, value) { this.attributes[key] = value; },
                focus() {}, contains() { return false; }, classList: { add(value) { classes.add(value); }, remove(value) { classes.delete(value); }, contains(value) { return classes.has(value); } } });
        }
        return elements.get(id);
    };
    function publish(operation, phase) {
        snapshot.events.push({ id: ++snapshot.lastEventId, phase, operation: structuredClone(operation) });
    }
    function complete(operation, status = "succeeded", result = {}) {
        snapshot.operations = snapshot.operations.filter(item => item.id !== operation.id);
        Object.assign(operation, { status, result });
        snapshot.completed.push(structuredClone(operation));
        publish(operation, "end");
    }
    async function request(path, body) {
        requests.push({ path, body });
        if (path === "/api/manage/cloud/queue") return structuredClone(snapshot);
        if (path === "/api/manage/cloud/queue/cancel") {
            const operation = snapshot.operations.find(item => item.id === body.operationId);
            if (operation.status !== "queued") throw new Error("Already running");
            complete(operation, "cancelled");
            return structuredClone(operation);
        }
        const operation = { id: `operation-${requests.length}`, action: path.split("/").at(-1), source: "manual", status: "queued", target: body?.name || "", context: { path, body } };
        snapshot.operations.push(operation);
        publish(operation, "start");
        return { operationId: operation.id, sessionId: snapshot.sessionId, status: "queued" };
    }
    const context = vm.createContext({
        console, sequence: 0, setTimeout, clearTimeout, setInterval() {},
        document: { getElementById: element, addEventListener() {} },
        sessionStorage: { getItem: key => storage.get(key), setItem: (key, value) => storage.set(key, value) },
        addEventListener: (_, callback) => listeners.add(callback), removeEventListener: (_, callback) => listeners.delete(callback),
        fetch: async (path, options) => ({ ok: true, json: () => request(path, options.body ? JSON.parse(options.body) : undefined) }),
        vscode: bridge ? { postMessage(message) {
            assert.ok(routes.has(message.path), `Missing extension route: ${message.path}`);
            request(message.path, message.body).then(payload => {
                for (const listener of [...listeners]) listener({ data: { type: "codexUsageManageResult", requestId: message.requestId, payload } });
            });
        } } : null,
        escapeHtml: value => String(value ?? "").replaceAll("<", "&lt;").replaceAll(">", "&gt;"),
        titleStatus: value => value[0].toUpperCase() + value.slice(1),
        showMessage(title, summary, details, type) { const message = { title, summary, details, type, dismiss() { this.dismissed = true; } }; messages.push(message); return message; },
        showActionResult(path, body, result) { messages.push({ title: "Completed", path, body, result }); },
        showControlLogin() {}, showDecryptRecovery() {}, runLocal() {},
    });
    vm.runInContext(`${apiScript}\n${operationScript}\n${queueScript}`, context);
    return { context, elements, messages, requests, storage, snapshot, publish, complete, element, run: code => vm.runInContext(code, context) };
}

for (const bridge of [false, true]) {
    test(`${bridge ? "VS Code bridge" : "browser fetch"}: submitting during initial polling preserves notifications`, async () => {
        const view = harness({ bridge });
        await view.run('run("/api/manage/cloud/test",{})');
        await flush();
        assert.equal(view.messages.length, 1);
        view.complete(view.snapshot.operations[0]);
        await view.run("pollQueue()");
        assert.equal(view.messages.length, 2);
    });

    test(`${bridge ? "VS Code bridge" : "browser fetch"}: submissions remain available, queue opens, waiting work can be removed`, async () => {
        const view = harness({ bridge });
        await flush();
        assert.equal(view.element("webDavQueueButton").hidden, true);
        await view.run('run("/api/manage/cloud/test",{})');
        await view.run('run("/api/manage/cloud/push",{})');
        await flush();
        assert.equal(view.element("webDavQueueCount").textContent, "2");
        assert.equal(view.element("webDavQueueButton").hidden, false);
        assert.equal(view.messages.filter(message => message.title.endsWith("Queued")).length, 2);
        view.element("webDavQueueButton").onclick();
        assert.equal(view.element("webDavQueueModal").classList.contains("open"), true);
        assert.equal(view.element("webDavQueueButton").attributes["aria-expanded"], "true");
        const button = { dataset: { cancelOperation: view.snapshot.operations[1].id } };
        await view.element("webDavQueueRows").onclick({ target: { closest: () => button } });
        await flush();
        assert.equal(view.element("webDavQueueCount").textContent, "1");
        assert.equal(view.messages.filter(message => message.title.endsWith("Cancelled")).length, 1);
        view.snapshot.operations[0].status = "running";
        await view.run("pollQueue()");
        assert.doesNotMatch(view.element("webDavQueueRows").innerHTML, /data-cancel-operation/);
        view.complete(view.snapshot.operations[0]);
        await view.run("pollQueue()");
        assert.equal(view.element("webDavQueueButton").hidden, true);
        assert.equal(view.messages.length, 4);
    });
}

test("fast completion, repeated polling, page reload, and another tab do not duplicate lifecycle messages", async () => {
    const view = harness();
    await flush();
    const operation = { id: "fast", action: "test", source: "automatic", status: "queued" };
    view.publish(operation, "start");
    view.complete(operation);
    await view.run("pollQueue()");
    assert.equal(view.messages.length, 2);
    await view.run("pollQueue()");
    assert.equal(view.messages.length, 2);
    const reloaded = harness({ storage: view.storage, snapshot: view.snapshot });
    const otherTab = harness({ snapshot: view.snapshot });
    await flush();
    assert.equal(reloaded.messages.length, 0);
    assert.equal(otherTab.messages.length, 0);
    const next = { id: "next", action: "test", source: "automatic", status: "queued" };
    view.publish(next, "start");
    view.complete(next, "failed");
    await reloaded.run("pollQueue()");
    await otherTab.run("pollQueue()");
    assert.equal(reloaded.messages.length, 2);
    assert.equal(otherTab.messages.length, 2);
});

test("restart clears the old queue and partial completion is reported as failure", async () => {
    const view = harness();
    await flush();
    view.snapshot.sessionId = "new-session";
    await view.run("pollQueue()");
    assert.equal(view.messages[0].title, "WebDAV Queue Restarted");
    const operation = { id: "partial", action: "set_skill_shared", source: "manual", status: "queued" };
    view.publish(operation, "start");
    view.complete(operation, "failed", { cloud: { pending: true, error: "offline" } });
    await view.run("pollQueue()");
    assert.match(view.messages.at(-1).title, /Failed — Sync Pending/);
    assert.match(view.messages.at(-1).summary, /local change completed/);
});

test("queue targets are escaped and count includes running work", async () => {
    const view = harness({ snapshot: { sessionId: "session", lastEventId: 0, completed: [], events: [], operations: [
        { id: "running", action: "test", source: "manual", status: "running", target: "<script>unsafe</script>" },
    ] } });
    await flush();
    assert.equal(view.element("webDavQueueCount").textContent, "1");
    assert.doesNotMatch(view.element("webDavQueueRows").innerHTML, /<script>/);
    assert.match(view.element("webDavQueueRows").innerHTML, /&lt;script&gt;/);
});
