"""Account-oriented display rules, independent of SQL and network encoding."""

import bisect
import math
from collections import defaultdict

from monitor_common import parse_timestamp

MONEY = 100_000_000
WINDOWS = (("5h", "fiveHour"), ("7d", "sevenDay"))
PRO_CUTOVER = 1793232000  # 2026-10-29T00:00:00Z


def multiplier(plan, timestamp):
    return {"plus": 1, "pro_lite": 5, "pro": 20 if timestamp < PRO_CUTOVER else 10}.get(plan, 1)


def connected(previous, current, window):
    if previous[3] is None or current[3] is None:
        return False
    gap = current[1] - previous[2]
    if gap > 14400:
        return False
    if gap <= 1800:
        return True
    change = current[3] * current[4] - previous[3] * previous[4]
    return change > -3 and abs(change) / (gap / 60) < (.5 if window == "5h" else .1)


def usage_runs(points, window):
    """Internal rows append reset and plan; only the first six fields go on the wire."""
    name = dict(WINDOWS)[window]
    result = []
    for point in points:
        if point.get("resetWindow") not in (None, name):
            continue
        stamp = point.get("timestamp")
        if stamp is None:
            continue
        value = point.get(name) or {}
        percent = value.get("continuous")
        if percent is not None and not math.isfinite(percent):
            percent = None
        plan = value.get("plan", "plus")
        factor = value.get("multiplier", multiplier(plan, stamp))
        row = [str(point.get("_key") or f"{stamp}:{point.get('resetWindow', '')}"), stamp, stamp, percent, factor, 1,
            parse_timestamp(value.get("resetAt")), plan]
        previous = result[-1] if result else None
        if previous and percent is not None and previous[3:5] == row[3:5] and previous[6] == row[6] and not point.get("resetWindow"):
            previous[2] = stamp
            continue
        if previous and connected(previous, row, window):
            row[5] = 0
        result.append(row)
    return result


def valid_intervals(runs):
    result = []
    for previous, current in zip(runs, runs[1:]):
        if current[5] or previous[3] is None or current[3] is None or previous[7] != current[7] or previous[4] != current[4]:
            continue
        if current[1] <= previous[2] or current[3] <= previous[3]:
            continue
        result.append([f"{previous[0]}>{current[0]}", previous[2], current[1], (current[3] - previous[3]) * current[4], current[4]])
    return result


def nearest(intervals, stamp, ends=None):
    if not intervals:
        return None
    index = bisect.bisect_right(ends if ends is not None else [row[2] for row in intervals], stamp)
    candidates = range(max(0, index - 1), min(len(intervals), index + 1))
    return min(candidates, key=lambda i: (max(intervals[i][1] - stamp, stamp - intervals[i][2], 0), intervals[i][1]))


def weighted(rows):
    cost = sum(row[-1] for row in rows)
    return math.floor((sum(row[1] * row[-1] for row in rows) / cost if cost else sum(row[1] for row in rows) / len(rows)) + .5)


def aggregate_tokens(rows, intervals, api=False):
    boundaries = sorted({stamp for values in intervals.values() for row in values for stamp in row[1:3]})
    groups = defaultdict(list)
    ends = {window: [row[2] for row in values] for window, values in intervals.items()}
    for row in rows:
        key = math.floor(row[1] / 60) if api else (bisect.bisect_right(boundaries, row[1]), *[nearest(values, row[1], ends[window]) for window, values in intervals.items()]) if boundaries else row[0]
        groups[key].append(row)
    result = []
    for key, values in groups.items():
        models = defaultdict(lambda: [0] * 5)
        for row in values:
            for index, amount in enumerate(row[4:]):
                models[(row[2], row[3])][index] += amount
        result.append([f"bulk:{key}", weighted(values), [[model, tier, *amounts] for (model, tier), amounts in sorted(models.items())]])
    return sorted(result, key=lambda row: (row[1], row[0]))


def chart_rows(events, intervals):
    groups = defaultdict(list)
    ends = [row[2] for row in intervals]
    for row in events:
        index = nearest(intervals, row[1], ends)
        if index is not None:
            groups[index].append(row)
    result = []
    for index, interval in enumerate(intervals):
        values = groups[index]
        costs = defaultdict(int)
        for row in values:
            costs[row[2]] += row[-1]
        total = sum(costs.values())
        result.append([interval[0], interval[1], interval[2], weighted(values) if values else int(interval[2]), interval[4], interval[3],
            [[model, cost, interval[3] * cost / total if total else 0] for model, cost in sorted(costs.items())]])
    return result


def token_row(row):
    stamp = parse_timestamp(row.get("occurredAt"))
    if row.get("recordType") != "usage" or stamp is None:
        return None
    tokens = row.get("tokens") or {}
    return [str(row.get("eventId") or stamp), stamp, str(row.get("rawModel") or row.get("billingModel") or "unknown"), str(row.get("serviceTier") or "default"),
        *[int(tokens.get(key) or 0) for key in ("freshInputTokens", "cachedInputTokens", "outputTokens", "cacheWriteInputTokens")],
        round(float((row.get("cost") or {}).get("totalCostUsd") or 0) * MONEY)]
