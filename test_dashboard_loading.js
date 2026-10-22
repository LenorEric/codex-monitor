const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const { test } = require("node:test");
const { performance } = require("node:perf_hooks");

const script = fs.readFileSync(`${__dirname}/dashboard.html`, "utf8").match(/<script>([\s\S]*?)<\/script>/)[1].split("restoreSelections();setupControls();setupDataView();")[0];
const flush = () => new Promise(resolve => setImmediate(resolve));
const frame = point => ({ quotaPoints: point ? [{ _key: point, timestamp: 1, usageAccountId: "a" }] : [], tokenSessions: [], costUsage: { fiveHour: [], sevenDay: [] }, historyStats: {} });
const manifest = (range, generation, chunks = []) => ({ protocolVersion: 4, view: "merged", range, generation, chunks, tokenSessions: [], rateRows: { fiveHour: [], sevenDay: [] }, builtAt: 1 });

function harness() {
    const elements = new Map(), noop = () => {};
    const element = id => {
        if (!elements.has(id)) elements.set(id, { value: "", textContent: "", style: {}, dataset: {}, width: 800, height: 300,
            classList: { add: noop, remove: noop, toggle: noop, contains: () => false }, replaceChildren: noop, append: noop, appendChild: noop, setAttribute: noop,
            getContext: () => ({ clearRect: noop }), getBoundingClientRect: () => ({ top: 0, bottom: 300, width: 800, height: 300 }) });
        return elements.get(id);
    };
    const context = vm.createContext({ console, performance, URLSearchParams, setTimeout, clearTimeout, requestAnimationFrame: () => 0, cancelAnimationFrame: noop, addEventListener: noop,
        innerHeight: 900, document: { hidden: false, getElementById: element, createElement: () => element(Symbol()), querySelectorAll: () => [] } });
    vm.runInContext(script, context);
    vm.runInContext("selected='Date';selectedDate='2030-01-01';drawAll=()=>{drawCount++};", context);
    context.drawCount = 0;
    return { context, elements, run: source => vm.runInContext(source, context) };
}

test("status renders while history and password setup are still pending", async () => {
    const h = harness();
    let finishHistory;
    h.context.requestStatus = async () => ({ payload: { controlPasswordConfigured: false }, etag: "status" });
    h.context.requestDashboard = () => new Promise(resolve => finishHistory = resolve);
    h.context.ensureControlPassword = () => new Promise(() => {});
    h.context.renderCurrentStatus = () => h.context.statusRendered = true;
    const history = h.run("load(true)");
    await h.run("pollStatus(true)");
    assert.equal(h.context.statusRendered, true);
    assert.equal(h.elements.get("historyStatus").textContent, "Loading selected history…");
    finishHistory({ payload: manifest("Date", "day"), etag: "day" });
    await history;
    assert.equal(h.context.drawCount, 1);
});

test("an obsolete day response cannot overwrite a newer preset selection", async () => {
    const h = harness();
    let finishDay;
    const requests = [];
    h.context.requestDashboard = scope => {
        requests.push(scope);
        if (scope.range === "Date") return new Promise(resolve => finishDay = resolve);
        return Promise.resolve({ payload: manifest("All", "overview") });
    };
    const day = h.run("load(true)");
    h.run("selected='All'");
    await h.run("load(true)");
    finishDay({ payload: manifest("Date", "obsolete", ["never-download"]) });
    await day;
    assert.equal(h.run("historyGeneration"), "overview");
    assert.equal(requests.length, 2);
    assert.equal(h.context.drawCount, 1);
});

test("changed scopes download only new chunks and unchanged manifests do not redraw", async () => {
    const h = harness(), chunks = [];
    let generation = 1;
    h.run("selected='All'");
    h.context.requestDashboard = async (scope, etag) => {
        if (scope.chunkId) { chunks.push(scope.chunkId); return { payload: frame(scope.chunkId) }; }
        if (etag === `g${generation}`) return { notModified: true, etag };
        return { payload: manifest("All", `g${generation}`, generation === 1 ? ["old", "shared"] : ["new", "shared"]), etag: `g${generation}` };
    };
    await h.run("load(true)");
    await h.run("load(true)");
    assert.equal(h.context.drawCount, 1);
    generation = 2;
    await h.run("load(true)");
    assert.deepEqual(chunks.sort(), ["new", "old", "shared"]);
    assert.equal(h.context.drawCount, 2);
    assert.equal(h.run("payload.quotaPoints.length"), 2);
});

test("prepared summaries preserve exact model-selected session counts", () => {
    const h = harness();
    h.context.sessions = [{ sessionCount: 3, byModel: { m: { usageTokens: { outputTokens: 10 } } } },
        { sessionCount: 2, byModel: { n: { usageTokens: { outputTokens: 20 } } } },
        { sessionCount: 4, byModel: { m: { usageTokens: { outputTokens: 30 } }, n: { usageTokens: { outputTokens: 40 } } } }];
    h.run("selectedModels=new Set(['m']);updateTokenSummary(sessions)");
    assert.equal(h.elements.get("tokenSessionCount").textContent, "7 sessions");
    assert.equal(h.elements.get("tokenOutput").textContent, "40");
    h.run("selectedModels=new Set(['m','n']);updateTokenSummary(sessions)");
    assert.equal(h.elements.get("tokenSessionCount").textContent, "9 sessions");
    assert.equal(h.elements.get("tokenOutput").textContent, "100");
});

test("a preparation error affects history only and the next refresh can recover", async () => {
    const h = harness();
    h.context.requestDashboard = async () => { throw Object.assign(new Error("Preparing"), { preparing: true }); };
    await h.run("load(true)");
    assert.match(h.elements.get("historyStatus").textContent, /background/);
    h.context.requestDashboard = async () => ({ payload: manifest("Date", "ready") });
    await h.run("load(true)");
    assert.equal(h.run("historyGeneration"), "ready");
});

test("VS Code bridge deduplicates identical scopes without combining unrelated requests", async () => {
    const extension = fs.readFileSync(`${__dirname}/extension.js`, "utf8");
    const source = extension.slice(extension.indexOf("const API_POLL_INTERVAL_MS"), extension.indexOf("function secondsAgo"));
    const calls = [];
    const context = vm.createContext({ URL, setInterval() {}, requestJson(url) { calls.push(url.toString()); return new Promise(() => {}); } });
    vm.runInContext(source + "\nmonitor=new PythonMonitor({});", context);
    vm.runInContext("monitor.getDashboard({view:'local',range:'All'});monitor.getDashboard({view:'local',range:'All'});monitor.getDashboard({view:'merged',range:'Date',start:0,end:86400});", context);
    assert.equal(calls.length, 2);
    assert.match(calls[0], /view=local/);
    assert.match(calls[1], /view=merged/);
    await flush();
});
