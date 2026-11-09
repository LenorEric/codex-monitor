
import unittest

from monitor_interpretation import PRO_CUTOVER, aggregate_tokens, chart_rows, connected, multiplier, nearest, usage_runs, valid_intervals, weighted


def point(stamp, value, reset=100000, plan="plus", weekly=None):
    return {"timestamp": stamp, "fiveHour": {"continuous": value, "plan": plan, "resetAt": reset},
        "sevenDay": {"continuous": value if weekly is None else weekly, "plan": plan, "resetAt": reset}}


class InterpretationTests(unittest.TestCase):
    def test_same_time_values_sort_ascending_independently_for_each_window(self):
        points = [point(100, 30, weekly=10), point(100, 10, weekly=30), point(100, 20, weekly=20)]
        for window in ("5h", "7d"):
            self.assertEqual([row[3] for row in usage_runs(points, window)], [10, 20, 30])
            self.assertEqual([row[3] for row in usage_runs(list(reversed(points)), window)], [10, 20, 30])

    def test_multiplier_boundary(self):
        self.assertEqual(multiplier("pro", PRO_CUTOVER - 1), 20)
        self.assertEqual(multiplier("pro", PRO_CUTOVER), 10)
        self.assertEqual(multiplier("pro_lite", PRO_CUTOVER), 5)

    def test_only_consecutive_identical_values_and_resets_merge(self):
        rows = usage_runs([point(0, 10), point(90, 10), point(180, 11), point(270, 10), point(360, 10, 200000)], "5h")
        self.assertEqual([(r[1], r[2], r[3]) for r in rows], [(0, 90, 10), (180, 180, 11), (270, 270, 10), (360, 360, 10)])

    def test_invalid_samples_break_curves(self):
        rows = usage_runs([point(0, 10), point(90, None), point(180, 11)], "5h")
        self.assertEqual([row[5] for row in rows], [1, 1, 1])
        self.assertEqual(valid_intervals(rows), [])

    def test_continuity_boundaries(self):
        base = ["a", 0, 0, 10, 1]
        self.assertTrue(connected(base, ["b", 1800, 1800, 0, 1], "5h"))
        self.assertFalse(connected(base, ["b", 1801, 1801, 7, 1], "5h"))
        self.assertTrue(connected(base, ["b", 14400, 14400, 11, 1], "5h"))
        self.assertFalse(connected(base, ["b", 14401, 14401, 11, 1], "5h"))
        self.assertFalse(connected(base, ["b", 3600, 3600, 16, 1], "7d"))

    def test_reset_timestamp_tolerance(self):
        base = ["a", 0, 0, 10, 1, 0, 10000]
        for difference in (-15, -1, 0, 1, 15):
            self.assertTrue(connected(base, ["b", 90, 90, 10, 1, 0, 10000 + difference], "5h"))
        for reset in (None, 9984, 10016):
            self.assertFalse(connected(base, ["b", 90, 90, 10, 1, 0, reset], "5h"))

    def test_zero_usage_connects_across_reset_boundaries(self):
        for window in ("5h", "7d"):
            for values in ((0, 1), (1, 0), (0, 0)):
                with self.subTest(window=window, values=values):
                    rows = usage_runs([point(0, values[0], reset=10000), point(90, values[1], reset=20000)], window)
                    self.assertEqual([row[5] for row in rows], [1, 0])
            self.assertFalse(connected(["a", 0, 0, 0, 1, 0, 10000], ["b", 14401, 14401, 1, 1, 0, 20000], window))

    def test_interval_eligibility_preserves_plan_boundaries_and_normalized_delta(self):
        runs = usage_runs([point(0, 1, plan="pro"), point(100, 2, plan="pro"), point(200, 3, plan="plus"), point(300, 4, plan="plus")], "5h")
        self.assertEqual([row[3:] for row in valid_intervals(runs)], [[20, 20], [1, 1]])

    def test_union_of_window_boundaries(self):
        intervals = {"5h": [["a", 0, 3, 1, 1], ["b", 3, 7, 1, 1]], "7d": [["c", 2, 6, 1, 1]]}
        events = [[str(t), t, "m", "default", 1, 2, 3, 4, 10] for t in (.5, 1, 2.5, 4, 5, 6.5)]
        rows = aggregate_tokens(events, intervals)
        self.assertEqual(len(rows), 4)
        self.assertEqual(sum(model[-1] for row in rows for model in row[2]), 60)
        self.assertEqual(sum(model[2] for row in rows for model in row[2]), 6)

    def test_nearest_ties_choose_earlier_interval(self):
        intervals = [["a", 0, 10], ["b", 20, 30]]
        self.assertEqual(nearest(intervals, 15), 0)
        self.assertEqual(nearest(intervals, 16), 1)
        self.assertEqual(nearest(intervals, -100), 0)
        self.assertEqual(nearest(intervals, 100), 1)

    def test_nearest_limits_distance_to_bounds_inclusively(self):
        intervals = [["a", 1000, 2000], ["b", 4000, 5000]]
        for stamp, expected in ((399, None), (400, 0), (1500, 0), (2600, 0), (2601, None), (3000, None), (3399, None), (3400, 1), (5600, 1), (5601, None)):
            with self.subTest(stamp=stamp):
                self.assertEqual(nearest(intervals, stamp), expected)
                self.assertEqual(nearest(intervals, stamp, [2000, 5000]), expected)
        self.assertEqual(nearest([["long", 0, 10000]], 5000), 0)
        self.assertEqual(nearest([["a", 0, 100], ["b", 1300, 1400]], 700), 0)
        self.assertIsNone(nearest([], 0))

    def test_distant_costs_remain_in_token_totals_with_independent_windows(self):
        intervals = {"5h": [["a", 1000, 1100, 1, 1]], "7d": [["b", 0, 10000, 1, 1]]}
        events = [[str(t), t, "m", "default", 1, 0, 0, 0, 100] for t in (399, 400, 1050, 1700, 1701, 5000)]
        self.assertEqual(sum(model[1] for row in chart_rows(events, intervals["5h"]) for model in row[6]), 300)
        self.assertEqual(sum(model[1] for row in chart_rows(events, intervals["7d"]) for model in row[6]), 600)
        self.assertEqual(sum(model[-1] for row in aggregate_tokens(events, intervals) for model in row[2]), 600)

    def test_flat_and_outside_costs_are_never_lost(self):
        events = [[str(t), t, "m", "default", 1, 0, 0, 0, 100] for t in (-10, 5, 15, 25, 50)]
        intervals = [["a", 0, 10, 1, 1], ["b", 20, 30, 2, 1]]
        rows = chart_rows(events, intervals)
        self.assertEqual(sum(m[1] for row in rows for m in row[6]), 500)
        self.assertEqual([row[6][0][1] for row in rows], [300, 200])
        self.assertEqual(len(aggregate_tokens(events, {"5h": [], "7d": []})), 5)

    def test_api_minute_groups_and_zero_cost_average(self):
        events = [[str(t), t, "m", "default", 1, 0, 0, 0, 0] for t in (1, 59, 60)]
        rows = aggregate_tokens(events, {}, api=True)
        self.assertEqual([row[1] for row in rows], [30, 60])
        self.assertEqual(weighted([["a", 10, 1], ["b", 20, 3]]), 18)


if __name__ == "__main__":
    unittest.main()


if __name__ == "__main__":
    unittest.main()
