const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const { test } = require("node:test");
const { performance } = require("node:perf_hooks");

const script = fs.readFileSync(`${__dirname}/dashboard.html`, "utf8").match(/<script>([\s\S]*?)<\/script>/)[1].split("restoreSelections();setupControls();setupDataView();")[0];
const flush = () => new Promise(resolve => setImmediate(resolve));

test("reset baselines stay in their window and sort before simultaneous observations", () => {
    const h = harness();
    h.run(`payload.quotaPoints=[
        {timestamp:0,usageAccountId:'a',fiveHour:{continuous:90},sevenDay:{continuous:10}},
        {timestamp:60,usageAccountId:'a',fiveHour:{continuous:10},sevenDay:{continuous:12}},
        {timestamp:60,usageAccountId:'a',resetWindow:'fiveHour',fiveHour:{continuous:0},sevenDay:{continuous:null}}];
        updateAccountControls(payload.quotaPoints)`);
    assert.deepEqual(JSON.parse(h.run("JSON.stringify(costTimeUsageSeries(0,60).map(item=>item.points.map(item.valueOf)))")), [[90, 0, 10], [10, 12]]);
    assert.equal(h.run("usageTimeAccountTotal(payload.quotaPoints,point=>point.fiveHour.continuous)"), 10);
    assert.equal(h.run("costTimeUsageSeries(61,62)[1].points[0].sevenDay.continuous"), 12);
});

function harness(overrides = {}) {
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
        innerHeight: 900, document: { hidden: false, getElementById: element, createElement: () => element(Symbol()), querySelectorAll: () => [] }, ...overrides });
    vm.runInContext(script, context);
    const drawAll = vm.runInContext("drawAll", context);
    vm.runInContext("selected='Date';selectedDate='2030-01-01';drawAll=()=>{drawCount++};connectLive=()=>{};", context);
    context.drawCount = 0;
    return { context, elements, drawAll, run: source => vm.runInContext(source, context) };
}

function offscreenChartHarness() {
    const scheduled = [], draws = [];
    const h = harness({ requestAnimationFrame: callback => scheduled.push(callback), setTimeout: callback => scheduled.push(callback),
        IntersectionObserver: class { observe() {} unobserve() {} } });
    h.context.drawAll = h.drawAll;
    h.context.css = () => "blue";
    h.context.drawUsageTimeChart = (id, period, points, animate) => draws.push({ id, range: h.run("selected"), animate });
    h.context.drawDeltaChart = (id, title, color, points, animate) => draws.push({ id, range: h.run("selected"), animate });
    h.context.drawCostTimeChart = animate => draws.push({ id: "costTime", range: h.run("selected"), animate });
    for (const id of ["usageTime5h", "usageTime7d", "scatter5h", "scatter7d", "costTime"]) {
        h.context.document.getElementById(id).getBoundingClientRect = () => ({ top: 1200, bottom: 1500, width: 800, height: 300 });
    }
    return { ...h, draws, drain: () => { while (scheduled.length) scheduled.shift()(); } };
}

test("a newer scope cancels queued chart draws and updates all charts offscreen", () => {
    const h = offscreenChartHarness();
    h.run("drawAll(true);selected='All';drawAll(true)");
    h.drain();
    assert.deepEqual(h.draws, ["usageTime5h", "usageTime7d", "scatter5h", "scatter7d", "costTime"].map(id => ({ id, range: "All", animate: true })));
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

test("chart accounts sort by current slot names and retain data order for duplicate names", () => {
    const h = combinedCostHarness();
    h.run(`payload.accounts={usageGroups:[{id:'z',label:'Slot 2'},{id:'a',label:'slot 2'},{id:'b',label:'Slot 10'}]};
        payload.accountLabels={z:'Old Z',a:'Old A',b:'Old B'};
        payload.quotaPoints=['b','z','a'].map(usageAccountId=>({timestamp:1,usageAccountId,accountLabel:'Historical',fiveHour:{continuous:1},sevenDay:{continuous:1}}));
        updateAccountControls(payload.quotaPoints)`);
    assert.deepEqual(JSON.parse(h.run("JSON.stringify(costTimeUsageSeries(0,2).filter(item=>item.label==='fiveHour').map(item=>item.accountId))")), ["z", "a", "b"]);
    h.run("payload.accounts.usageGroups[2].label='Slot 1'");
    assert.deepEqual(JSON.parse(h.run("JSON.stringify(costTimeUsageSeries(0,2).filter(item=>item.label==='fiveHour').map(item=>item.accountId))")), ["b", "z", "a"]);
    h.context.chart = (_id, series) => { h.context.scatterNames = series.map(item => item.name); };
    h.run("drawDeltaChart('scatter5h','',null,payload.quotaPoints.map(point=>({...point,deltaPercent:1,deltaCostUsd:1,costPer100PercentUsd:100})))");
    assert.deepEqual(Array.from(h.context.scatterNames), ["Slot 1", "Slot 2", "slot 2"]);
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


test("account switch renders cached usage and reset times before another status poll", async () => {
    const h = harness();
    h.context.showMessage = () => ({ dismiss() {} });
    h.context.load = () => {};
    h.context.authenticatedAccountAction = async () => ({ accounts: { activeAccountId: "b", items: [] } });
    h.run(`applyCurrentStatus({accounts:{activeAccountId:"a",items:[]},accountStatuses:{
        a:{lastSample:{windows:{"5h":{usedPercent:10,resetAt:1893456000}}}},
        b:{lastSample:{windows:{"5h":{usedPercent:70,resetAt:1893460000},"7d":{usedPercent:80,resetAt:1893888000}}}}
    }})`);
    assert.equal(h.elements.get("usage5hValue").textContent, "10.0%");
    await h.run('performAccountAction("switch",{accountId:"b"})');
    assert.equal(h.elements.get("usage5hValue").textContent, "70.0%");
    assert.equal(h.elements.get("usage7dValue").textContent, "80.0%");
    assert.ok(h.elements.get("time5hReset").textContent.includes(new Date(1893460000000).toLocaleString()));
    assert.equal(h.run("currentStatus.accounts.activeAccountId"), "b");
});



function bulkFixture(){
    const view=()=>({quota:{'5h':30000,'7d':70000},tokens:[['b',100,[[0,'default',10,5,2,1,100000000]]]],legacy:[],
        'usage:5h':[['q',0,100,10,20,1]],'usage:7d':[['w',0,100,20,1,1]],'chart:5h':[],'chart:7d':[]});
    return {epoch:'e',cursors:{local:0,merged:0,state:0},models:['model-x'],state:{accounts:{activeAccountId:'slot',items:[{id:'slot',usageAccountId:'a',ready:true}],usageGroups:[{id:'a',label:'Nomei'}]}},accounts:[{id:'a',local:view(),merged:view()}]};
}
test('bulk decoding preserves both quota windows and numeric times',()=>{
    const h=harness();h.context.bulkFixture=bulkFixture();h.run('acceptBulk(bulkFixture)');
    assert.equal(h.run('payload.costTime[0].totalCostUsd'),1);
    assert.equal(h.run('payload.tokenSessions[0].byModel["model-x"].usageTokens.freshInputTokens'),10);
    assert.equal(h.run('payload.quotaPoints.filter(p=>p.resetWindow==="fiveHour").length'),2);
    assert.equal(h.run('payload.quotaPoints.filter(p=>p.resetWindow==="sevenDay").length'),2);
    assert.equal(h.run('payload.quotaPoints[0].fiveHour.multiplier'),20);
    assert.equal(h.run('lastUpdateTimestamp'),100);
});
test('switching range and view does not fetch history',async()=>{
    const h=harness({fetch(){throw new Error('unexpected request')}});h.context.bulkFixture=bulkFixture();h.run('acceptBulk(bulkFixture);dataView="local";selected="All"');await h.run('load(true)');
    assert.equal(h.run('payload.tokenSessions.length'),1);
});
test('live token additions are idempotent and independent by view',()=>{
    const h=harness();h.context.bulkFixture=bulkFixture();h.run('acceptBulk(bulkFixture)');
    h.context.packet={epoch:'e',stream:'local',seq:1,channel:'tokens',account:'a',data:[['new',200,[['model-x','default',20,0,0,0,200000000]]]]};
    h.run('applyLive(packet);applyLive(packet)');assert.equal(h.run('payload.costTime.length'),1);
    h.run('dataView="local";showBulkView()');assert.equal(h.run('payload.costTime.length'),2);
    assert.equal(h.run('payload.costTime.reduce((sum,row)=>sum+row.totalCostUsd,0)'),3);
});
test('chart attribution updates never change token totals',()=>{
    const h=harness();h.context.bulkFixture=bulkFixture();h.run('acceptBulk(bulkFixture)');
    h.context.packet={epoch:'e',stream:'merged',seq:1,channel:'chart:5h',account:'a',data:{set:[['i',0,100,50,1,10,[['model-x',100000000,10]]]],remove:[]}};
    h.run('applyLive(packet)');assert.equal(h.run('payload.costTime[0].totalCostUsd'),1);assert.equal(h.run('payload.costUsage.fiveHour[0].totalCostUsd'),1);
});
test('cost popup sums selected accounts and stops cumulative at mouse time',()=>{
    const h=harness();h.run(`payload={costTimeBounds:[0,100],costTime:[{timestamp:10,usageAccountId:'a',costByModelUsd:{m:1}},{timestamp:40,usageAccountId:'b',costByModelUsd:{m:2}},{timestamp:80,usageAccountId:'a',costByModelUsd:{m:3}}]}`);
    assert.deepEqual(JSON.parse(h.run('JSON.stringify(costModelHover(50,100,60))')),[{model:'m',spending:3,cumulative:3}]);
    h.run('selectedAccounts=new Set(["a"])');assert.deepEqual(JSON.parse(h.run('JSON.stringify(costModelHover(50,100,90))')),[{model:'m',spending:3,cumulative:4}]);
});

test('obsolete epoch packets cannot replace active data',()=>{
 const h=harness();h.context.bulkFixture=bulkFixture();h.run('acceptBulk(bulkFixture)');
 h.context.packet={epoch:'old',stream:'state',seq:100,channel:'state',data:{accounts:{activeAccountId:'old'}}};
 h.run('applyLive(packet)');assert.equal(h.run('currentStatus.accounts.activeAccountId'),'slot');
});

function extensionHarness(requestJson,overrides={}){
 const source=fs.readFileSync(`${__dirname}/extension.js`,'utf8');
 const context=vm.createContext({URL,requestJson,stableTooltip:()=>'',setTimeout,clearTimeout,...overrides});
 vm.runInContext(source.slice(source.indexOf('const TOOLTIP_HOVER_DELAY_SECONDS'),source.indexOf('function secondsAgo'))+';monitor=new PythonMonitor({show(){}});',context);
 return context;
}
test('extension shares the initial bulk request',async()=>{
 let finish,calls=0;const context=extensionHarness(()=>{calls++;return new Promise(resolve=>finish=resolve)});
 const first=vm.runInContext('monitor.getDashboard()',context),second=vm.runInContext('monitor.getDashboard()',context);
 assert.equal(calls,1);finish({state:{accounts:{items:[]}}});await Promise.all([first,second]);assert.equal(calls,1);
});
test('a delayed HTTP status cannot overwrite newer streamed state',async()=>{
 let finish;const context=extensionHarness(()=>new Promise(resolve=>finish=resolve));
 const response=vm.runInContext('monitor.getStatusResponse()',context);
 vm.runInContext('monitor.cachedStatus={newest:true};monitor.streamStatusReceived=true;',context);
 finish({payload:{newest:false}});assert.equal((await response).payload.newest,true);
});

test('a new account initializes before its first token packet',()=>{
 const h=harness();h.context.bulkFixture=bulkFixture();h.run('acceptBulk(bulkFixture)');
 h.context.metadata={epoch:'e',stream:'merged',seq:1,channel:'account',account:'new',data:{label:'New account'}};
 h.context.event={epoch:'e',stream:'merged',seq:2,channel:'tokens',account:'new',data:[['event',200,[['model-y','default',1,0,0,0,1]]]]};
 h.run('applyLive(metadata);applyLive(event)');assert.equal(h.run('payload.accountLabels.new'),'New account');assert.equal(h.run('payload.costTime.length'),2);
});

test('extension keeps state-only subscription snapshots out of dashboard replay',()=>{
 const sockets=[];class Socket{constructor(){this.callbacks={};sockets.push(this)}on(name,callback){this.callbacks[name]=callback}}
 const context=extensionHarness(()=>{}, {WebSocket:Socket});context.forwarded=0;
 vm.runInContext('monitor.onDashboardPacket=()=>forwarded++;monitor.connectStream()',context);
 sockets[0].callbacks.message(Buffer.from(JSON.stringify({type:'state',epoch:'e',cursors:{state:1},data:{}})));
 assert.equal(context.forwarded,0);
});
