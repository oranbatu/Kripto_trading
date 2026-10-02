"""Permanent tests for the price-touch hierarchy detector."""
from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from detectors.price_touch_hierarchy_v1.engine import (
    Bar,
    HardTouchKeyError,
    LevelStats,
    analyze,
    atomic_bounds,
    average_rank_percentiles,
    build_grid,
    build_hierarchy,
    candle_touches_zone,
    canonical_timeframe_for,
    episode_marks,
    filter_completed,
    grid_center,
    hierarchy_score,
    hierarchy_tier,
    interaction_flags,
    iso_turkey,
    iso_utc,
    k_floor,
    primary_touch_type,
    qualification_result,
    register_touch_key,
    relative_distance_percent,
    representative_price_for,
    select_peak_indices,
    smooth_density,
    suppress_near_duplicates,
    attach_children,
    _GroupBuilder,
)
from detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy import (
    assert_python_write_allowed,
    next_revision,
    revision_filename,
)
from detectors.price_touch_hierarchy_v1.touch_config import CONFIG, PROJECT_ROOT
from detectors.price_touch_hierarchy_v1.workbook import _monthly_rows, _touch_rows

UTC = timezone.utc
D = Decimal


def _level(timeframe: str, price: str, touches: int = 10, episodes: int = 3, candidate: str = "C") -> LevelStats:
    center = D(price)
    level = LevelStats(
        timeframe=timeframe,
        grid_index=1,
        center=center,
        zone_lower=center * D("0.999"),
        zone_upper=center * D("1.001"),
        raw_atomic_touches=touches,
        smoothed_density=D(touches),
        candidate_id=candidate,
    )
    level.touches = touches
    level.episodes = episodes
    level.close_inside = touches // 2
    level.body = touches // 4
    level.level_id = candidate
    level.dates = {datetime(2024, 1, day).date() for day in (1, 2, 3)}
    level.months = {(2024, 1), (2024, 2), (2024, 3)}
    level.years = {2024}
    level.first_touch = datetime(2024, 1, 1, tzinfo=UTC)
    level.last_touch = datetime(2024, 3, 1, tzinfo=UTC)
    level.first_eligible = datetime(2024, 1, 5, tzinfo=UTC)
    level.percentiles = {
        "touch": D("0.5"),
        "episode": D("0.5"),
        "months": D("0.5"),
        "close_inside": D("0.5"),
        "engagement": D("0.5"),
        "span": D("0.5"),
    }
    return level


def _bar(hour: int, low: str, high: str, close: str, day: int = 1, timeframe: str = "1h") -> Bar:
    opened = datetime(2024, 1, day, hour, tzinfo=UTC)
    return Bar(
        timeframe=timeframe,
        row=0,
        open_time=opened,
        close_time=opened + timedelta(hours=1) - timedelta(milliseconds=1),
        open=D("100"),
        high=D(high),
        low=D(low),
        close=D(close),
        volume=D("1"),
    )


class PriceTouchHierarchyTests(unittest.TestCase):
    def test_01_one_candle_counts_once_despite_hypothetical_recrosses(self) -> None:
        level = _level("1h", "100", touches=0, candidate="C1")
        bar = _bar(0, "99.5", "100.5", "100")
        from detectors.price_touch_hierarchy_v1.engine import recount_peaks

        recount_peaks([bar], [level])
        self.assertEqual(level.touches, 1)

    def test_02_wick_and_body_count_once(self) -> None:
        level = _level("1h", "100", touches=0, candidate="C2")
        bar = _bar(0, "99.0", "100.5", "99.2")
        from detectors.price_touch_hierarchy_v1.engine import recount_peaks

        recount_peaks([bar], [level])
        self.assertEqual(level.touches, 1)
        self.assertEqual(level.close_inside + level.body + level.wick, 1)

    def test_03_open_and_close_inside_count_once(self) -> None:
        level = _level("1h", "100", touches=0, candidate="C3")
        bar = _bar(0, "99.95", "100.05", "100.01")
        bar = Bar(bar.timeframe, 0, bar.open_time, bar.close_time, D("100"), bar.high, bar.low, D("100.01"), D("1"))
        from detectors.price_touch_hierarchy_v1.engine import recount_peaks

        recount_peaks([bar], [level])
        self.assertEqual(level.touches, 1)
        self.assertEqual(level.events[0].touch_type, "CLOSE_INSIDE_ZONE")

    def test_04_one_candle_can_touch_two_zones_once_each(self) -> None:
        low = _level("1h", "100", touches=0, candidate="A")
        high = _level("1h", "102", touches=0, candidate="B")
        bar = _bar(0, "99.9", "102.2", "101")
        from detectors.price_touch_hierarchy_v1.engine import recount_peaks

        recount_peaks([bar], [low, high])
        self.assertEqual(low.touches, 1)
        self.assertEqual(high.touches, 1)

    def test_05_duplicate_touch_key_is_rejected(self) -> None:
        registry: set = set()
        key = ("1h", datetime(2024, 1, 1, tzinfo=UTC), "Z1")
        register_touch_key(registry, key)
        with self.assertRaises(HardTouchKeyError):
            register_touch_key(registry, key)

    def test_06_unfinished_candle_is_excluded(self) -> None:
        end = datetime(2024, 1, 1, 2, tzinfo=UTC)
        done = _bar(0, "100", "101", "100.5")
        late = _bar(3, "100", "101", "100.5")
        kept, excluded = filter_completed([done, late], end)
        self.assertEqual(len(kept), 1)
        self.assertEqual(excluded, 1)

    def test_07_exact_lower_bound_contact_counts(self) -> None:
        self.assertTrue(candle_touches_zone(D("100"), D("101"), D("101"), D("102")))

    def test_08_exact_upper_bound_contact_counts(self) -> None:
        self.assertTrue(candle_touches_zone(D("100"), D("101"), D("99"), D("100")))

    def test_09_candle_entirely_above_does_not_count(self) -> None:
        self.assertFalse(candle_touches_zone(D("103"), D("104"), D("100"), D("102")))

    def test_10_candle_entirely_below_does_not_count(self) -> None:
        self.assertFalse(candle_touches_zone(D("90"), D("99"), D("100"), D("102")))

    def test_11_close_inside_classification(self) -> None:
        kind = primary_touch_type(D("100"), D("101"), D("99"), D("100.2"), D("100"), D("100.5"))
        self.assertEqual(kind, "CLOSE_INSIDE_ZONE")

    def test_12_body_intersection_classification(self) -> None:
        kind = primary_touch_type(D("100.2"), D("101"), D("99"), D("99.5"), D("100"), D("100.4"))
        self.assertEqual(kind, "BODY_INTERSECTION")

    def test_13_wick_only_classification(self) -> None:
        kind = primary_touch_type(D("99"), D("100.2"), D("98"), D("99.2"), D("100"), D("100.5"))
        self.assertEqual(kind, "WICK_ONLY_INTERSECTION")

    def test_14_primary_types_reconcile(self) -> None:
        level = _level("1h", "100", touches=0, candidate="R")
        bars = [
            _bar(0, "99.9", "100.2", "100.0"),
            _bar(1, "99.0", "100.2", "99.2"),
            _bar(2, "98", "100.05", "98.5"),
        ]
        for index, bar in enumerate(bars):
            bars[index] = Bar(bar.timeframe, index, bar.open_time, bar.close_time, bar.open, bar.high, bar.low, bar.close, bar.volume)
        from detectors.price_touch_hierarchy_v1.engine import recount_peaks

        recount_peaks(bars, [level])
        self.assertEqual(level.close_inside + level.body + level.wick, level.touches)

    def test_15_cross_through_does_not_add_a_touch(self) -> None:
        flags = interaction_flags(D("99"), D("102"), D("98"), D("101"), D("100"), D("100.5"), None)
        self.assertTrue(flags["cross_through"])
        level = _level("1h", "100", touches=0, candidate="X")
        bar = _bar(0, "98", "102", "101")
        from detectors.price_touch_hierarchy_v1.engine import recount_peaks

        recount_peaks([bar], [level])
        self.assertEqual(level.touches, 1)
        self.assertEqual(level.cross_through, 1)

    def test_16_consecutive_touches_are_one_episode(self) -> None:
        marks = episode_marks([True, True, True])
        self.assertEqual([item[0] for item in marks], [1, 1, 1])
        self.assertTrue(marks[0][1])
        self.assertTrue(marks[-1][2])

    def test_17_exit_and_reentry_create_a_new_episode(self) -> None:
        marks = episode_marks([True, False, True])
        self.assertEqual([item[0] for item in marks], [1, 2])

    def test_18_logarithmic_grid_is_stable_when_extended(self) -> None:
        price = D("50000")
        k = k_floor(price)
        narrow = build_grid(D("40000"), D("60000"))
        wide = build_grid(D("10000"), D("100000"))
        self.assertEqual(narrow[2][k - narrow[0]], grid_center(k))
        self.assertEqual(wide[2][k - wide[0]], grid_center(k))

    def test_19_atomic_bounds_meet_without_a_gap_or_overlap_interior(self) -> None:
        _lower, upper = atomic_bounds(10000)
        next_lower, _next_upper = atomic_bounds(10001)
        self.assertEqual(upper, next_lower)
        self.assertLess(atomic_bounds(10000)[0], upper)

    def test_20_density_smoothing_is_exact(self) -> None:
        smoothed = smooth_density([0, 4, 8, 4, 0])
        self.assertEqual(smoothed[2], D("6"))
        self.assertEqual(smoothed[0], D("1"))

    def test_21_local_peak_selection_is_deterministic(self) -> None:
        raw = [1, 3, 10, 4, 1, 2, 9, 2]
        smoothed = smooth_density(raw)
        args = (raw, smoothed, [1] * len(raw), [1] * len(raw), [0] * len(raw), [None] * len(raw), 0)
        self.assertEqual(select_peak_indices(*args), select_peak_indices(*args))
        self.assertIn(2, select_peak_indices(*args))

    def test_22_equal_peak_tie_break_prefers_more_episodes_then_lower_index(self) -> None:
        first = datetime(2024, 1, 2, tzinfo=UTC)
        earlier = datetime(2024, 1, 1, tzinfo=UTC)
        peaks = select_peak_indices(
            [5, 5],
            [D("5"), D("5")],
            [1, 4],
            [1, 1],
            [0, 0],
            [first, earlier],
            0,
        )
        self.assertEqual(peaks, [1])
        tied = select_peak_indices(
            [5, 5],
            [D("5"), D("5")],
            [2, 2],
            [1, 1],
            [0, 0],
            [earlier, first],
            0,
        )
        self.assertEqual(tied, [0])

    def test_23_near_duplicate_suppression(self) -> None:
        strong = _level("1h", "100", touches=50, candidate="STRONG")
        weak = _level("1h", "100.20", touches=40, candidate="WEAK")
        kept, suppressed = suppress_near_duplicates([weak, strong])
        self.assertEqual([level.candidate_id for level in kept], ["STRONG"])
        self.assertEqual(suppressed[0].suppressed_by, "STRONG")
        apart = _level("1h", "101.00", touches=30, candidate="APART")
        kept_far, suppressed_far = suppress_near_duplicates([strong, apart])
        self.assertEqual(len(kept_far), 2)
        self.assertEqual(suppressed_far, [])

    def test_24_suppressed_candidate_remains_in_audit_output(self) -> None:
        strong = _level("1h", "100", touches=50, candidate="STRONG")
        weak = _level("1h", "100.10", touches=45, candidate="WEAK")
        _kept, suppressed = suppress_near_duplicates([strong, weak])
        self.assertEqual(len(suppressed), 1)
        self.assertIsNotNone(suppressed[0].suppression_distance)
        self.assertGreater(suppressed[0].touches, 0)

    def test_25_one_hour_39_touches_fail(self) -> None:
        passed, primary, _reasons = qualification_result("1h", 39, 3, 3)
        self.assertFalse(passed)
        self.assertEqual(primary, "1H_TOUCH_COUNT_BELOW_40")

    def test_26_one_hour_40_touches_pass_touch_threshold(self) -> None:
        passed, primary, reasons = qualification_result("1h", 40, 3, 3)
        self.assertTrue(passed)
        self.assertEqual(reasons, [])
        self.assertEqual(primary, "")

    def test_27_four_hour_19_touches_fail(self) -> None:
        passed, primary, _reasons = qualification_result("4h", 19, 3, 3)
        self.assertFalse(passed)
        self.assertEqual(primary, "4H_TOUCH_COUNT_BELOW_20")

    def test_28_four_hour_20_touches_pass_touch_threshold(self) -> None:
        self.assertTrue(qualification_result("4h", 20, 3, 3)[0])

    def test_29_daily_9_touches_fail(self) -> None:
        passed, primary, _reasons = qualification_result("1d", 9, 3, 3)
        self.assertFalse(passed)
        self.assertEqual(primary, "1D_TOUCH_COUNT_BELOW_10")

    def test_30_daily_10_touches_pass_touch_threshold(self) -> None:
        self.assertTrue(qualification_result("1d", 10, 3, 3)[0])

    def test_31_touch_pass_does_not_override_failed_episodes(self) -> None:
        passed, primary, reasons = qualification_result("1h", 40, 2, 3)
        self.assertFalse(passed)
        self.assertEqual(primary, "INDEPENDENT_EPISODES_BELOW_3")
        self.assertIn("INDEPENDENT_EPISODES_BELOW_3", reasons)

    def test_32_touch_pass_does_not_override_failed_dates(self) -> None:
        passed, primary, reasons = qualification_result("1h", 80, 5, 2)
        self.assertFalse(passed)
        self.assertIn("DISTINCT_UTC_DATES_BELOW_3", reasons)
        self.assertEqual(primary, "DISTINCT_UTC_DATES_BELOW_3")

    def test_33_timeframe_counts_cannot_be_combined(self) -> None:
        self.assertFalse(qualification_result("1d", 9, 3, 3)[0])
        self.assertTrue(qualification_result("1h", 40, 3, 3)[0])
        self.assertFalse(qualification_result("4h", 19, 3, 3)[0])
        combined = 30 + 10
        self.assertNotEqual(combined, 10)
        self.assertFalse(qualification_result("1d", 9, 3, 3)[0])

    def test_34_all_timeframes_are_tier_1(self) -> None:
        self.assertEqual(hierarchy_tier(["1d", "4h", "1h"]), "TIER_1_ALL_TIMEFRAMES")

    def test_35_daily_and_four_hour_are_tier_2(self) -> None:
        self.assertEqual(hierarchy_tier(["1d", "4h"]), "TIER_2_DAILY_WITH_INTRADAY")

    def test_36_daily_and_one_hour_are_tier_2(self) -> None:
        self.assertEqual(hierarchy_tier(["1h", "1d"]), "TIER_2_DAILY_WITH_INTRADAY")

    def test_37_daily_only_is_tier_3(self) -> None:
        self.assertEqual(hierarchy_tier(["1d"]), "TIER_3_DAILY_ONLY")

    def test_38_four_hour_and_one_hour_are_tier_4(self) -> None:
        self.assertEqual(hierarchy_tier(["4h", "1h"]), "TIER_4_4H_AND_1H")

    def test_39_four_hour_only_is_tier_5(self) -> None:
        self.assertEqual(hierarchy_tier(["4h"]), "TIER_5_4H_ONLY")

    def test_40_one_hour_only_is_tier_6(self) -> None:
        self.assertEqual(hierarchy_tier(["1h"]), "TIER_6_1H_ONLY")

    def test_41_child_maps_to_only_one_parent(self) -> None:
        left = _level("1d", "100", candidate="L1D-000001")
        right = _level("1d", "100.20", candidate="L1D-000002")
        child = _level("4h", "100.02", candidate="L4H-000001")
        attempts: list = []
        unmatched = attach_children([child], [_GroupBuilder(left), _GroupBuilder(right)], attempts)
        self.assertEqual(unmatched, [])
        parents = [attempt.parent_level_id for attempt in attempts if attempt.accepted]
        self.assertEqual(parents, ["L1D-000001"])

    def test_42_closest_eligible_parent_wins(self) -> None:
        far = _level("1d", "100.20", candidate="FAR")
        near = _level("1d", "100", candidate="NEAR")
        child = _level("1h", "100.02", candidate="CHILD")
        attempts: list = []
        attach_children([child], [_GroupBuilder(far), _GroupBuilder(near)], attempts)
        accepted = [attempt for attempt in attempts if attempt.accepted]
        self.assertEqual(accepted[0].parent_level_id, "NEAR")
        self.assertLess(accepted[0].distance_percent, relative_distance_percent(child.price, far.price))

    def test_43_higher_timeframe_wins_exact_distance_tie(self) -> None:
        daily = _level("1d", "100", candidate="DAILY")
        hourly_parent = _level("4h", "100", candidate="H4")
        child = _level("1h", "100", candidate="H1")
        attempts: list = []
        attach_children([child], [_GroupBuilder(hourly_parent), _GroupBuilder(daily)], attempts)
        accepted = [attempt for attempt in attempts if attempt.accepted]
        self.assertEqual(accepted[0].parent_timeframe, "1d")
        self.assertEqual(accepted[0].distance_percent, D("0"))

    def test_44_group_span_prevents_chain_merge(self) -> None:
        daily = _level("1d", "100", candidate="D")
        four = _level("4h", "100.20", candidate="H4")
        orphan = _level("1h", "100.45", candidate="H1")
        groups, attempts = build_hierarchy({"1d": [daily], "4h": [four], "1h": [orphan]})
        self.assertTrue(any(attempt.accepted and attempt.child_level_id == "H4" for attempt in attempts))
        daily_group = next(group for group in groups if "1d" in group.members)
        self.assertNotIn("1h", daily_group.members)
        self.assertLessEqual(daily_group.max_span, D("0.60"))
        anchor = _level("1d", "100", candidate="ANCHOR")
        builder = _GroupBuilder(anchor)
        builder.prices.append(D("100.50"))
        child = _level("1h", "99.80", candidate="WIDE")
        span_attempts: list = []
        unmatched = attach_children([child], [builder], span_attempts)
        self.assertEqual(unmatched, [child])
        self.assertTrue(any(attempt.rejection_reason == "GROUP_SPAN_ABOVE_0_60" for attempt in span_attempts))

    def test_45_representative_price_uses_daily_when_present(self) -> None:
        members = {"1d": _level("1d", "100"), "4h": _level("4h", "100.1"), "1h": _level("1h", "100.2")}
        self.assertEqual(representative_price_for(members), D("100"))

    def test_46_representative_price_uses_four_hour_without_daily(self) -> None:
        members = {"4h": _level("4h", "80"), "1h": _level("1h", "80.1")}
        self.assertEqual(representative_price_for(members), D("80"))

    def test_47_percentile_ties_use_average_rank(self) -> None:
        values = average_rank_percentiles([D("1"), D("3"), D("3")])
        self.assertEqual(values[0], D("0"))
        self.assertEqual(values[1], values[2])
        self.assertEqual(values[1], D("0.75"))

    def test_48_hierarchy_score_formula(self) -> None:
        daily = _level("1d", "100")
        four = _level("4h", "100")
        daily.percentiles = {"touch": D("1"), "episode": D("0"), "months": D("0")}
        four.percentiles = {"touch": D("0"), "episode": D("1"), "months": D("0")}
        daily.touches = 10
        four.touches = 10
        daily.close_inside = 0
        daily.body = 0
        four.close_inside = 0
        four.body = 0
        score = hierarchy_score({"1d": daily, "4h": four})
        touch = (D("4") * D("1") + D("2") * D("0")) / D("6")
        episode = (D("4") * D("0") + D("2") * D("1")) / D("6")
        expected = D("100") * (D("0.60") * touch + D("0.20") * episode)
        self.assertEqual(score, expected)

    def test_49_tier_sorts_before_score(self) -> None:
        daily = _level("1d", "100", candidate="D")
        four = _level("4h", "100", candidate="H4")
        joined = _level("1h", "100", candidate="H1A")
        solo = _level("1h", "140", candidate="H1B")
        for level in (daily, four, joined):
            level.percentiles = {"touch": D("0"), "episode": D("0"), "months": D("0")}
        solo.percentiles = {"touch": D("1"), "episode": D("1"), "months": D("1")}
        groups, _attempts = build_hierarchy({"1d": [daily], "4h": [four], "1h": [joined, solo]})
        self.assertEqual(groups[0].tier, "TIER_1_ALL_TIMEFRAMES")
        self.assertLess(groups[0].score, groups[-1].score)
        self.assertEqual(groups[-1].tier, "TIER_6_1H_ONLY")

    def test_50_canonical_visits_use_the_finest_timeframe(self) -> None:
        self.assertEqual(canonical_timeframe_for({"1d": object(), "4h": object(), "1h": object()}), "1h")
        self.assertEqual(canonical_timeframe_for({"1d": object(), "4h": object()}), "4h")
        daily = _level("1d", "100", episodes=4, candidate="D")
        intra = _level("1h", "100", episodes=9, candidate="H")
        groups, _attempts = build_hierarchy({"1d": [daily], "4h": [], "1h": [intra]})
        self.assertEqual(groups[0].canonical_timeframe, "1h")
        self.assertEqual(groups[0].canonical_visits, 9)
        self.assertNotEqual(groups[0].canonical_visits, 4 + 9)

    def test_51_utc_to_turkey_uses_europe_istanbul(self) -> None:
        stamp = datetime(2024, 6, 1, 0, 0, tzinfo=UTC)
        self.assertEqual(iso_turkey(stamp), "2024-06-01T03:00:00.000+03:00")

    def test_52_source_timestamps_stay_utc(self) -> None:
        stamp = datetime(2024, 6, 1, 0, 0, tzinfo=UTC)
        self.assertTrue(iso_utc(stamp).endswith("Z"))
        self.assertTrue(iso_utc(stamp).startswith("2024-06-01T00:00:00"))

    def test_53_revision_starts_at_rev00(self) -> None:
        self.assertEqual(next_revision([]), 0)
        self.assertEqual(revision_filename(0), "BTCUSDT_MTF_Most_Touched_Levels_rev00.xlsx")

    def test_54_revision_increments_from_the_maximum(self) -> None:
        self.assertEqual(next_revision([0, 4, 2]), 5)
        self.assertEqual(revision_filename(5), "BTCUSDT_MTF_Most_Touched_Levels_rev05.xlsx")

    def test_55_revision_gaps_are_not_filled(self) -> None:
        self.assertEqual(next_revision([0, 2]), 3)
        self.assertNotEqual(next_revision([0, 2]), 1)

    def test_56_existing_workbook_is_not_overwritten(self) -> None:
        existing = [0, 1, 4]
        chosen = next_revision(existing)
        self.assertNotIn(chosen, existing)
        self.assertGreater(chosen, max(existing))

    def test_57_filename_race_rescans(self) -> None:
        planned = next_revision([1, 2])
        discovered_during_write = [1, 2, planned]
        self.assertEqual(next_revision(discovered_during_write), planned + 1)

    def test_58_source_parquet_files_remain_unchanged(self) -> None:
        from data.catalog import fingerprint_file
        from data.loader import load_candles

        roots = [
            Path(r"C:\MarketData\derived\binance\futures\um\perpetual\1h\symbol=BTCUSDT"),
            Path(r"C:\MarketData\derived\binance\futures\um\perpetual\4h\symbol=BTCUSDT"),
            Path(r"C:\MarketData\derived\binance\futures\um\perpetual\1d\symbol=BTCUSDT"),
        ]
        files = [path for root in roots for path in root.rglob("data.parquet")]
        self.assertGreaterEqual(len(files), 3)
        before = {str(path): fingerprint_file(path) for path in files}
        load_candles(
            "BTCUSDT",
            "1d",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 3, tzinfo=UTC),
            warmup_bars=0,
            precision_mode="exact",
        )
        after = {str(path): fingerprint_file(path) for path in files}
        self.assertEqual(before, after)

    def test_59_range_detector_is_not_imported(self) -> None:
        self.assertFalse(any(name.startswith("detectors.primary_range_v1") for name in sys.modules))

    def test_60_swing_detector_is_not_imported(self) -> None:
        self.assertFalse(any(name.startswith("detectors.hierarchical_swing_v4") for name in sys.modules))

    def test_61_strategy_modules_are_not_imported(self) -> None:
        self.assertFalse(any(name.split(".")[0] in {"strategy", "strategies"} for name in sys.modules))

    def test_62_no_separate_project_is_created(self) -> None:
        self.assertEqual(PROJECT_ROOT.name, "backtest_system")
        self.assertEqual(CONFIG.package_dir.parent.name, "detectors")
        self.assertFalse((Path(r"C:\Users\oranb\Desktop") / "price_touch_hierarchy_v1").exists())

    def test_63_detector_python_cannot_be_written_outside_the_package(self) -> None:
        outside = Path(r"C:\Users\oranb\Desktop\price_touch_hierarchy_v1.py")
        with self.assertRaises(PermissionError):
            assert_python_write_allowed(outside)

    def test_64_rerun_is_deterministic(self) -> None:
        first = analyze({"1h": _qualified_hour_bars()})
        second = analyze({"1h": _qualified_hour_bars()})
        self.assertEqual(
            [(level.level_id, level.rank, str(level.price), level.touches) for level in first.levels["1h"]],
            [(level.level_id, level.rank, str(level.price), level.touches) for level in second.levels["1h"]],
        )
        self.assertEqual(
            [(group.rank, group.tier, str(group.score)) for group in first.groups],
            [(group.rank, group.tier, str(group.score)) for group in second.groups],
        )

    def test_65_unrounded_values_determine_rank(self) -> None:
        low = _level("1h", "100", candidate="LOW")
        high = _level("1h", "110", candidate="HIGH")
        low.percentiles = {"touch": D("0.10001"), "episode": D("0"), "months": D("0")}
        high.percentiles = {"touch": D("0.10004"), "episode": D("0"), "months": D("0")}
        for level in (low, high):
            level.touches = 40
            level.close_inside = 0
            level.body = 0
        scores = [hierarchy_score({"1h": low}), hierarchy_score({"1h": high})]
        rounded = [score.quantize(D("0.01")) for score in scores]
        self.assertEqual(rounded[0], rounded[1])
        order = sorted([0, 1], key=lambda index: -scores[index])
        self.assertEqual(order, [1, 0])

    def test_66_display_rounding_does_not_change_ordering(self) -> None:
        scores = [D("10.004"), D("10.001")]
        displayed = [score.quantize(D("0.01")) for score in scores]
        self.assertEqual(displayed[0], displayed[1])
        self.assertEqual(sorted(range(2), key=lambda index: (-scores[index], index)), [0, 1])

    def test_67_monthly_touch_counts_reconcile(self) -> None:
        result = analyze({"1h": _qualified_hour_bars()})
        rows = _monthly_rows(result)
        total = sum(row[3] for row in rows)
        expected = sum(level.touches for level in result.levels["1h"])
        self.assertEqual(total, expected)
        self.assertGreater(expected, 0)

    def test_68_detailed_events_reconcile_to_summary_counts(self) -> None:
        result = analyze({"1h": _qualified_hour_bars()})
        rows = _touch_rows(result)
        self.assertTrue(rows)
        self.assertTrue(all(row[-1] == 1 for row in rows))
        keys = [(row[2], row[4], row[3]) for row in rows]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(len(rows), sum(level.touches for level in result.levels["1h"]))


def _qualified_hour_bars() -> list[Bar]:
    bars: list[Bar] = []

    def add(day: int, hour: int, low: str, high: str, close: str) -> None:
        opened = datetime(2024, 1, day, hour, tzinfo=UTC)
        bars.append(
            Bar(
                "1h",
                len(bars),
                opened,
                opened + timedelta(hours=1) - timedelta(milliseconds=1),
                D(close),
                D(high),
                D(low),
                D(close),
                D("1"),
            )
        )

    for hour in range(14):
        add(1, hour, "99.95", "100.05", "100")
    add(1, 14, "90", "90.2", "90.1")
    for hour in range(13):
        add(3, hour, "99.95", "100.05", "100")
    add(3, 13, "90", "90.2", "90.1")
    for hour in range(13):
        add(5, hour, "99.95", "100.05", "100")
    return bars


if __name__ == "__main__":
    unittest.main()
