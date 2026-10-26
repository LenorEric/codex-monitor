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
            classList: { add: noop, remove: noop, toggle: noop, contains: () => false },
            replaceChildren(...children) { this.textContent = children.map(child => child.textContent || "").join(" "); },
            append(...children) { this.textContent += children.map(child => typeof child === "string" ? child : child.textContent || "").join(""); }, appendChild: noop, setAttribute: noop,
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

test("timestamp-only chart records use shared labels and refresh names independently of history", async () => {
    const h = harness();
    h.context.requestDashboard = async scope => scope.chunkId ? { payload: frame("point") } : {
        payload: { ...manifest("Date", "generation", ["chunk"]), accountLabels: { a: "Historical name" } }
    };
    await h.run("load(true)");
    assert.equal(h.run("eventTimestamp(payload.quotaPoints[0])"), 1);
    assert.equal(h.run("accountDisplayName('a')"), "Historical name");
    h.context.requestStatus = async () => ({ payload: { accounts: { usageGroups: [{ id: "a", label: "Latest slot" }] } } });
    h.context.renderCurrentStatus = () => {};
    h.context.ensureControlPassword = () => {};
    await h.run("pollStatus(true)");
    assert.equal(h.run("accountDisplayName('a')"), "Latest slot");
    assert.equal(h.context.drawCount, 2);
    await h.run("load(true)");
    assert.equal(h.context.drawCount, 2);
});

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

test("scope transitions wait for all chunks and preserve the previous chart state", async () => {
    const h = harness(), draws = [];
    let finishChunk;
    h.context.drawAll = animate => draws.push({ animate, point: h.run("payload.quotaPoints[0]?._key") });
    h.context.requestDashboard = async scope => ({ payload: scope.chunkId ? frame("old") : manifest("Date", "day", ["old"]) });
    await h.run("load(true)");
    h.run("selected='All';chartStates.scatter5h={lineLayer:'previous'};usageTimeChartStates.usageTime5h={labelLayer:'previous'}");
    h.context.requestDashboard = scope => scope.chunkId ? new Promise(resolve => finishChunk = resolve) :
        Promise.resolve({ payload: { ...manifest("All", "all", ["new"]), tokenSessions: [{ _key: "new-session" }] } });
    const pending = h.run("load(true)");
    await flush();
    assert.equal(draws.length, 1);
    assert.equal(h.run("payload.quotaPoints[0]._key"), "old");
    assert.equal(h.run("payload.tokenSessions.length"), 0);
    assert.equal(h.run("chartStates.scatter5h.lineLayer"), "previous");
    assert.equal(h.run("usageTimeChartStates.usageTime5h.labelLayer"), "previous");
    finishChunk({ payload: frame("new") });
    await pending;
    assert.deepEqual(draws, [{ animate: false, point: "old" }, { animate: true, point: "new" }]);
});

test("cached scope switches animate immediately and unchanged refreshes do not redraw", async () => {
    const h = harness(), draws = [];
    h.context.drawAll = animate => draws.push(animate);
    h.context.requestDashboard = async scope => ({ payload: manifest(scope.range, scope.range) });
    await h.run("load(true)");
    h.run("selected='All'");
    await h.run("load(true)");
    h.run("selected='Date'");
    h.context.requestDashboard = async () => ({ notModified: true });
    await h.run("load(true)");
    await h.run("load(true)");
    assert.deepEqual(draws, [false, true, true]);
});

test("a scope that is still preparing animates on retry once ready", async () => {
    const h = harness(), draws = [];
    h.context.drawAll = animate => draws.push(animate);
    h.context.requestDashboard = async () => ({ payload: manifest("Date", "day") });
    await h.run("load(true)");
    h.run("selected='All'");
    h.context.requestDashboard = async () => { throw Object.assign(new Error("Preparing"), { preparing: true }); };
    await h.run("load(true)");
    assert.deepEqual(draws, [false]);
    assert.equal(h.run("displayedScope"), h.run("new URLSearchParams({view:'merged',range:'Date',start:String(selectedDateBounds().startMs/1000),end:String(selectedDateBounds().endMs/1000)}).toString()"));
    h.context.requestDashboard = async () => ({ payload: manifest("All", "ready") });
    await h.run("load(true)");
    assert.deepEqual(draws, [false, true]);
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

test("cost time bins preserve hourly sums, gaps, exact filters and independent totals", () => {
    const h = harness();
    h.run(`selected='24h';payload={costTimeResolution:3600,costTimeBounds:[1800,10801],costTime:[
        {timestamp:1799,usageAccountId:'a',costByModelUsd:{m:100}},
        {timestamp:1800,usageAccountId:'a',costByModelUsd:{m:.1,n:2}},
        {timestamp:3601,usageAccountId:'a',costByModelUsd:{m:.2}},
        {timestamp:10800,usageAccountId:'b',costByModelUsd:{m:3}},
        {timestamp:10802,usageAccountId:'a',costByModelUsd:{m:100}}],
        costTimeTotals:[{usageAccountId:'a',costByModelUsd:{m:.3,n:2}},{usageAccountId:'b',costByModelUsd:{m:3}}]};`);
    const series = () => JSON.parse(h.run("JSON.stringify(costTimeSeries())"));
    assert.deepEqual(series().buckets.map(row => row.amount), [2.1, .2, 0, 3]);
    assert.equal(series().total, 5.3);
    assert.equal(series().buckets.at(-1).cumulative, 5.3);
    assert.equal(series().buckets[0].start, 1800);
    h.run("selectedModels=new Set(['m']);selectedAccounts=new Set(['a']);normalizedUsage=true;collapseUsageGaps=true;");
    assert.deepEqual(series().buckets.map(row => row.amount), [.1, .2, 0, 0]);
    assert.equal(series().total, .3);
    assert.equal(series().buckets.at(-1).cumulative, .3);
    h.run("payload.costTime=[]");
    assert.equal(series().total, .3);
    assert.equal(series().buckets.length, 0);
});

test("cost time retains zero events, uses daily bins and bounds All history", () => {
    const h = harness();
    h.run("selected='7d';payload={costTimeResolution:86400,costTimeBounds:[0,86401],costTime:[{timestamp:0,usageAccountId:'a',costByModelUsd:{m:0}},{timestamp:86401,usageAccountId:'a',costByModelUsd:{m:2}}]}");
    assert.equal(h.run("costTimeSeries().seconds"), 86400);
    assert.equal(h.run("costTimeSeries().buckets[0].amount"), 0);
    assert.equal(h.run("costTimeSeries().buckets[1].cumulative"), 2);
    h.run("selected='All';payload.costTimeBounds=[0,86400*10000];");
    assert.ok(h.run("costTimeSeries().buckets.length") <= 200);
    assert.equal(h.run("costTimeSeries().seconds%86400"), 0);
    assert.equal(h.run("costTimeMode"), "spending");
    h.run("setupCostTimeControls()");
    h.elements.get("costTimeCumulative").onclick();
    assert.equal(h.run("costTimeMode"), "cumulative");
    assert.equal(h.context.drawCount, 1);
});

test("cost time scoped chunks and exact totals are carried through dashboard loading", async () => {
    const h = harness();
    const cost = { _key: "cost", timestamp: 10, usageAccountId: "a", costByModelUsd: { m: 4 }, totalCostUsd: 4 };
    h.context.requestDashboard = async scope => ({ payload: scope.chunkId ? { ...frame(), costTime: [cost] } :
        { ...manifest("Date", "cost-day", ["cost-chunk"]), costTimeBounds: [0, 86400], costTimeTotals: [cost] } });
    await h.run("load(true)");
    assert.equal(h.run("payload.costTime.length"), 1);
    assert.equal(h.run("costTimeSeries().total"), 4);
    assert.throws(() => h.run("validateView({quotaPoints:[],tokenSessions:[],costUsage:{fiveHour:[],sevenDay:[]},costTime:[{}]})"), /Invalid cost time/);
});

test("cost time canvas renders bars, cumulative mode and interval tooltips", () => {
    const h = harness(), bars = [], lines = [], labels = [];
    const noop = () => {};
    h.context.devicePixelRatio = 1;
    h.context.getComputedStyle = () => ({ getPropertyValue: () => "#f3a43b" });
    h.elements.get("costTime") ?? h.run("document.getElementById('costTime')");
    h.elements.get("costTime").getContext = () => ({ clearRect: noop, setTransform: noop, measureText: () => ({ width: 30 }), beginPath: noop, stroke: noop,
        moveTo: noop, lineTo: (x, y) => lines.push([x, y]), fillRect: (...args) => bars.push(args), fillText: text => labels.push(text) });
    h.context.renderTip = (_tip, rows) => { h.context.tooltipRows = rows; };
    h.context.positionTip = noop;
    h.run("selected='24h';payload={costTimeResolution:3600,costTimeBounds:[0,7200],costTime:[{timestamp:1,usageAccountId:'a',costByModelUsd:{m:2}},{timestamp:3601,usageAccountId:'a',costByModelUsd:{m:3}}]};drawCostTimeChart()");
    assert.equal(bars.length, 2);
    assert.ok(bars[0][2] > 0 && bars[0][3] > 0);
    assert.match(h.elements.get("costTimeTotal").textContent, /5/);
    h.elements.get("costTime").onmousemove({ offsetX: 60, offsetY: 30 });
    assert.ok(h.context.tooltipRows.some(row => String(row).includes("Spending: $2")));
    h.run("costTimeMode='cumulative';drawCostTimeChart()");
    assert.equal(bars.length, 2);
    assert.ok(lines.length > 0);
    h.run("payload.costTime=[];drawCostTimeChart()");
    assert.ok(labels.includes("No timestamped cost events for the selected scope"));
});

test("cost intervals follow plot width and allow arbitrary integer minutes, hours and days", () => {
    const h = harness();
    h.run("payload={costTimeResolution:60,costTimeBounds:[0,18000],costTime:[{timestamp:0,usageAccountId:'a',costByModelUsd:{m:1}}]}");
    assert.equal(h.run("costTimeSeries(1300).seconds"), 7 * 60);
    assert.equal(h.run("costTimeSeries(1200).seconds"), 8 * 60);
    assert.equal(h.run("costTimeSeries(360).seconds"), 25 * 60);
    assert.equal(h.run("costTimeSeries(520).total"), 1);
    h.run("payload.costTimeBounds=[0,7*86400];payload.costTimeResolution=3600");
    assert.equal(h.run("costTimeSeries(1200).seconds"), 4 * 3600);
    h.run("payload.costTimeBounds=[0,30*86400]");
    assert.equal(h.run("costTimeSeries(1200).seconds"), 18 * 3600);
    h.run("payload.costTimeBounds=[0,700*86400];payload.costTimeResolution=86400");
    assert.equal(h.run("costTimeSeries(1200).seconds"), 18 * 86400);
    assert.equal(h.run("costTimeSeries(1200).buckets.length"), 39);
});

test("partial cost bar widths and hover boundaries follow actual durations", () => {
    const h = harness(), bars = [], noop = () => {};
    h.context.devicePixelRatio = 1;
    h.context.getComputedStyle = () => ({ getPropertyValue: () => "#f3a43b" });
    h.run("document.getElementById('costTime')");
    h.elements.get("costTime").getContext = () => ({ clearRect: noop, setTransform: noop, measureText: () => ({ width: 30 }), beginPath: noop, stroke: noop,
        moveTo: noop, lineTo: noop, fillRect: (...args) => bars.push(args), fillText: noop });
    h.context.renderTip = (_tip, rows) => { h.context.tooltipRows = rows; };
    h.context.positionTip = noop;
    h.run("selected='5h';payload={costTimeResolution:3600,costTimeBounds:[1800,9000],costTime:[{timestamp:1800,usageAccountId:'a',costByModelUsd:{m:1}},{timestamp:3600,usageAccountId:'a',costByModelUsd:{m:2}},{timestamp:9000,usageAccountId:'a',costByModelUsd:{m:3}}]};drawCostTimeChart()");
    assert.equal(bars.length, 3);
    assert.ok(Math.abs(bars[0][2] * 2 - bars[1][2]) < 1e-8);
    assert.ok(Math.abs(bars[0][2] - bars[2][2]) < 1e-8);
    assert.equal(h.run("costTimeSeries().buckets.at(-1).cumulative"), 6);
    h.elements.get("costTime").onmousemove({ offsetX: 260, offsetY: 30 });
    assert.ok(h.context.tooltipRows.includes("Spending: $2"));
});

function combinedCostHarness() {
    const h = harness();
    h.context.getComputedStyle = () => ({ getPropertyValue: name => ({ "--blue": "blue", "--green": "green", "--orange": "orange" }[name] || "gray") });
    h.run(`selected='24h';payload={costTimeBounds:[0,36000],costTimeTotals:[],quotaPoints:[
        {timestamp:0,usageAccountId:'a',fiveHour:{continuous:10,plan:'plus'},sevenDay:{continuous:1,plan:'plus'}},
        {timestamp:3600,usageAccountId:'a',fiveHour:{continuous:20,plan:'plus'},sevenDay:{continuous:2,plan:'plus'}},
        {timestamp:32400,usageAccountId:'a',fiveHour:{continuous:20,plan:'plus'},sevenDay:{continuous:2,plan:'plus'}},
        {timestamp:36000,usageAccountId:'a',fiveHour:{continuous:30,plan:'plus'},sevenDay:{continuous:3,plan:'plus'}}]};
        fixture={start:0,end:36000,buckets:[{start:0,end:3600,amount:1},{start:3600,end:32400,amount:0},{start:32400,end:36000,amount:2}]};updateAccountControls(payload.quotaPoints);`);
    return h;
}

test("combined chart collapses only zero spending with BOTH quota windows unchanged", () => {
    const h = combinedCostHarness();
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,36000)).length"), 1);
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,36000))[0].start"), 3600);
    assert.ok(h.run("costTimeFolds(fixture,costTimeUsageSeries(0,36000),0,1000,6).length") > 0);
    h.run("fixture.buckets[1].amount=1");
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,36000)).length"), 0);
    h.run("fixture.buckets[1].amount=0;payload.quotaPoints[2].sevenDay.continuous=2.1");
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,36000)).length"), 0);
    h.run("payload.quotaPoints[2].sevenDay.continuous=2;payload.quotaPoints[2].fiveHour.continuous=21");
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,36000)).length"), 0);
    h.run("payload.quotaPoints[2].fiveHour.continuous=20;collapseUsageGaps=false");
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,36000)).length"), 0);
    h.run("collapseUsageGaps=true;collapseUsageFlat=false");
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,36000)).length"), 1);
});

test("combined chart requires every account and does not treat missing quota as zero", () => {
    const h = combinedCostHarness();
    h.run("payload.quotaPoints.push(...payload.quotaPoints.map((point,index)=>({...point,usageAccountId:'b',fiveHour:{continuous:index,plan:'plus'}})));updateAccountControls(payload.quotaPoints)");
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,36000)).length"), 0);
    h.run("selectedAccounts=new Set(['a'])");
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,36000)).length"), 1);
    h.run("payload.quotaPoints[2].sevenDay.continuous=null;normalizedUsage=true");
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,36000)).length"), 0);
    assert.equal(h.run("costTimeUsageSeries(0,36000)[1].valueOf(payload.quotaPoints[2])"), null);
});

test("combined flat toggle and folded coordinates preserve active cost and time lookup", () => {
    const h = combinedCostHarness();
    h.run("fixture.end=10800;fixture.buckets=[{start:0,end:1800,amount:1},{start:1800,end:9000,amount:0},{start:9000,end:10800,amount:2}];payload.quotaPoints.forEach((point,index)=>point.timestamp=[0,1800,9000,10800][index]);");
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,10800))[0].kind"), "unchanged");
    h.run("collapseUsageFlat=false");
    assert.equal(h.run("costTimeFoldCandidates(fixture,costTimeUsageSeries(0,10800)).length"), 0);
    h.run("collapseUsageFlat=true;folds=costTimeFolds(fixture,costTimeUsageSeries(0,10800),0,1000,6);scale=usageTimeScale(0,10800,folds,0,1000)");
    assert.ok(h.run("folds.length") > 0);
    assert.ok(h.run("Math.abs(scale.timestampAt(scale.x(4500))-4500)") < 1e-6);
    assert.ok(h.run("scale.x(1800)-scale.x(0)") > 150);
});

test("combined canvas draws percentage axes, both usage lines and percentage tooltips", () => {
    const h = combinedCostHarness(), labels = [], strokes = [], noop = () => {};
    h.context.devicePixelRatio = 1;
    h.run("document.getElementById('costTime');payload.costTimeResolution=60;payload.costTime=[{timestamp:10,usageAccountId:'a',costByModelUsd:{m:1}}];collapseUsageGaps=false;collapseUsageFlat=false;");
    const ctx = { clearRect: noop, setTransform: noop, measureText: () => ({ width: 6 }), beginPath: noop, moveTo: noop, lineTo: noop, fillRect: noop,
        fillText: text => labels.push(text), stroke() { strokes.push({ color: this.strokeStyle, dash: [...(this.dash || [])] }); }, save: noop, restore: noop, rect: noop, clip: noop,
        setLineDash(value) { this.dash = value; } };
    h.elements.get("costTime").getContext = () => ctx;
    h.context.renderTip = (_tip, rows) => { h.context.tooltipRows = rows; };
    h.context.positionTip = noop;
    h.run("drawCostTimeChart()");
    assert.ok(labels.includes("100%"));
    assert.match(h.elements.get("costTimeLegend").textContent, /Spending.*5h.*7d/);
    assert.ok(strokes.some(stroke => stroke.color === "#4f8cff" && stroke.dash.join(",") === "6,4"));
    assert.ok(strokes.some(stroke => stroke.color === "#4f8cff" && stroke.dash.length === 0));
    h.elements.get("costTime").onmousemove({ offsetX: 55, offsetY: 30 });
    assert.ok(h.context.tooltipRows.some(row => typeof row === "string" && row.startsWith("5h ·")));
    assert.ok(h.context.tooltipRows.some(row => typeof row === "string" && row.startsWith("7d ·")));
    h.run("payload.costTime=[];drawCostTimeChart()");
    assert.doesNotMatch(h.elements.get("costTimeLegend").textContent, /Spending|Cumulative/);
    assert.match(h.elements.get("costTimeLegend").textContent, /5h.*7d/);
    assert.ok(!labels.includes("No timestamped cost events for the selected scope"));
    h.run("payload.quotaPoints.forEach(point=>point.sevenDay.continuous=null);drawCostTimeChart()");
    assert.match(h.elements.get("costTimeLegend").textContent, /5h/);
    assert.doesNotMatch(h.elements.get("costTimeLegend").textContent, /7d/);
    h.run("payload.quotaPoints=[];updateAccountControls([]);drawCostTimeChart()");
    assert.equal(h.elements.get("costTimeLegend").textContent, "");
});

test("combined legend series reuse scope accounts and respect selected account filters", () => {
    const h = combinedCostHarness();
    h.run("payload.quotaPoints.push(...payload.quotaPoints.map(point=>({...point,usageAccountId:'outside-scope'})))");
    assert.equal(h.run("costTimeUsageSeries(0,36000).some(item=>item.accountId==='outside-scope')"), false);
    h.run("updateAccountControls(payload.quotaPoints);selectedAccounts=new Set(['outside-scope'])");
    assert.equal(h.run("costTimeUsageSeries(0,36000).every(item=>item.accountId==='outside-scope')"), true);
    h.run("selectedAccounts.clear();updateAccountControls(payload.quotaPoints.filter(point=>point.usageAccountId==='a'))");
    assert.equal(h.run("costTimeUsageSeries(0,36000).every(item=>item.accountId==='a')"), true);
});

test("each account uses the same chart color for dashed 5h and solid 7d usage", () => {
    const h = combinedCostHarness();
    h.run("payload.quotaPoints.push(...payload.quotaPoints.map(point=>({...point,usageAccountId:'b'})));updateAccountControls(payload.quotaPoints);curveColors=new Map([['a','#aabbcc'],['b','#112233']])");
    const series = JSON.parse(h.run("JSON.stringify(costTimeUsageSeries(0,36000).map(({accountId,label,color,dash})=>({accountId,label,color,dash})))"));
    for (const [account, color] of [["a", "#aabbcc"], ["b", "#112233"]]) {
        assert.deepEqual(series.filter(item => item.accountId === account), [
            { accountId: account, label: "fiveHour", color, dash: [6, 4] },
            { accountId: account, label: "sevenDay", color, dash: [] },
        ]);
    }
});

test("combined usage preserves duplicate timestamp order and range edges", () => {
    const h = combinedCostHarness();
    h.run(`payload.quotaPoints=[
        {timestamp:10,usageAccountId:'a',fiveHour:{continuous:10},sevenDay:{continuous:10}},
        {timestamp:5,usageAccountId:'a',fiveHour:{continuous:5},sevenDay:{continuous:5}},
        {timestamp:10,usageAccountId:'a',fiveHour:{continuous:20},sevenDay:{continuous:20}},
        {timestamp:20,usageAccountId:'a',fiveHour:{continuous:30},sevenDay:{continuous:30}}];updateAccountControls(payload.quotaPoints)`);
    assert.deepEqual(JSON.parse(h.run("JSON.stringify(costTimeUsageSeries(10,10)[0].points.map(point=>[point.timestamp,point.fiveHour.continuous]))")), [[5, 5], [10, 10], [10, 20], [20, 30]]);
    assert.equal(h.run("costTimeUsageAt(costTimeUsageSeries(10,10)[0],10)"), 10);
    assert.equal(h.run("costTimeUsageAt(costTimeUsageSeries(10,10)[0],15)"), 25);
    assert.equal(h.run("costTimeUsageAt(costTimeUsageSeries(10,10)[0],4)"), null);
    assert.equal(h.run("costTimeUsageAt(costTimeUsageSeries(10,10)[0],21)"), null);
});

test("combined hover lookup keeps real gaps and honors precomputed compacted ranges", () => {
    const h = combinedCostHarness();
    h.run(`gapPoints=[
        {timestamp:0,fiveHour:{continuous:10}},
        {timestamp:20000,fiveHour:{continuous:30}}];
        gapItem={label:'fiveHour',points:gapPoints,valueOf:point=>point.fiveHour.continuous,compactedRanges:[]}`);
    assert.equal(h.run("costTimeUsageAt(gapItem,10000)"), null);
    h.run("gapPoints[1].overviewBucket={start:0,end:20000};gapItem.compactedRanges=compactedUsageRanges(gapPoints)");
    assert.equal(h.run("costTimeUsageAt(gapItem,10000)"), 20);
});
