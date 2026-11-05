"""Permanent tests for the price-touch hierarchy detector."""
from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

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
    analytical_fingerprint,
    assert_python_write_allowed,
    batch_exit_code,
    log_path,
    main,
    next_revision,
    report_path,
    revision_filename,
    run_for_symbol_cli,
    scan_revisions,
    selected_symbols,
)
from detectors.price_touch_hierarchy_v1.touch_config import (
    ALGORITHM_VERSION,
    CONFIG,
    DATASET_END,
    EXCLUSIVE_LOAD_END,
    EXPECTED_LAST,
    EXPECTED_PARTITIONS,
    EXPECTED_ROWS,
    MIN_TOUCHES,
    PROJECT_ROOT,
    RUNNER_VERSION,
    SYMBOLS,
    SUPPORTED_SYMBOLS,
    PRICE_TOUCH_EXTENSION_SYMBOLS,
    TIMEFRAMES,
    require_price_touch_symbol,
    source_dataset,
    symbol_profile,
)
from detectors.price_touch_hierarchy_v1.workbook import validate_saved_workbook, write_workbook
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


class OctoberCoverageTests(unittest.TestCase):
    def test_69_coverage_constants_use_october_27(self) -> None:
        self.assertEqual(DATASET_END, datetime(2026, 10, 27, 23, 59, 59, 999000, tzinfo=UTC))
        self.assertEqual(EXCLUSIVE_LOAD_END, datetime(2026, 10, 28, tzinfo=UTC))
        self.assertEqual(EXPECTED_ROWS, {"1h": 24744, "4h": 6186, "1d": 1031})
        self.assertEqual(EXPECTED_PARTITIONS, {"1h": 34, "4h": 34, "1d": 3})
        self.assertEqual(EXPECTED_LAST["1h"], datetime(2026, 10, 27, 23, tzinfo=UTC))
        self.assertEqual(EXPECTED_LAST["4h"], datetime(2026, 10, 27, 20, tzinfo=UTC))
        self.assertEqual(EXPECTED_LAST["1d"], datetime(2026, 10, 27, tzinfo=UTC))

    def test_70_thresholds_and_timeframes_stay_unchanged(self) -> None:
        self.assertEqual(TIMEFRAMES, ("1h", "4h", "1d"))
        self.assertEqual(MIN_TOUCHES, {"1h": 40, "4h": 20, "1d": 10})

    def test_71_october_27_rows_are_loaded_and_october_28_is_excluded(self) -> None:
        from data.exceptions import EmptyRangeError
        from data.loader import load_candles

        for timeframe, rows in EXPECTED_ROWS.items():
            loaded = load_candles(
                "BTCUSDT",
                timeframe,
                datetime(2024, 1, 1, tzinfo=UTC),
                EXCLUSIVE_LOAD_END,
                warmup_bars=0,
                precision_mode="exact",
            )
            self.assertEqual(loaded.table.num_rows, rows)
            self.assertEqual(len(set(loaded.source_files)), EXPECTED_PARTITIONS[timeframe])
            last_open = loaded.table.column("open_time")[rows - 1].as_py()
            self.assertEqual(last_open, EXPECTED_LAST[timeframe])
            with self.assertRaises(EmptyRangeError):
                load_candles(
                    "BTCUSDT",
                    timeframe,
                    datetime(2026, 10, 28, tzinfo=UTC),
                    datetime(2026, 10, 29, tzinfo=UTC),
                    warmup_bars=0,
                    precision_mode="exact",
                )

    def test_72_september_15_and_16_and_the_month_boundary_remain_readable(self) -> None:
        from data.loader import load_candles

        september = load_candles(
            "BTCUSDT",
            "1h",
            datetime(2026, 9, 15, tzinfo=UTC),
            datetime(2026, 9, 17, tzinfo=UTC),
            warmup_bars=0,
            precision_mode="exact",
        )
        opens = [value.as_py() for value in september.table.column("open_time")]
        self.assertIn(datetime(2026, 9, 15, 23, tzinfo=UTC), opens)
        self.assertIn(datetime(2026, 9, 16, 0, tzinfo=UTC), opens)
        boundary = load_candles(
            "BTCUSDT",
            "4h",
            datetime(2026, 9, 30, 20, tzinfo=UTC),
            datetime(2026, 10, 1, 8, tzinfo=UTC),
            warmup_bars=0,
            precision_mode="exact",
        )
        names = " ".join(boundary.source_files).replace("\\", "/")
        self.assertIn("month=09", names)
        self.assertIn("month=10", names)
        self.assertEqual(boundary.table.num_rows, 3)

    def test_73_a_candle_closing_after_the_dataset_end_is_excluded(self) -> None:
        opened = datetime(2026, 10, 28, tzinfo=UTC)
        bar = Bar(
            "1d",
            0,
            opened,
            opened + timedelta(days=1) - timedelta(milliseconds=1),
            D("1"),
            D("2"),
            D("1"),
            D("1"),
            D("1"),
        )
        kept, excluded = filter_completed([bar], DATASET_END)
        self.assertEqual(kept, [])
        self.assertEqual(excluded, 1)


class MultiSymbolTests(unittest.TestCase):
    def test_74_symbol_universe_and_order(self) -> None:
        import config

        self.assertEqual(
            SYMBOLS,
            ("BTCUSDT", "ETHUSDT", "AVAXUSDT", "AAVEUSDT", "NEARUSDT", "LINKUSDT", "LTCUSDT", "OPUSDT", "DOTUSDT"),
        )
        self.assertEqual(tuple(config.ALLOWED_SYMBOLS), SYMBOLS)
        self.assertEqual(selected_symbols(["--all-symbols"]), list(SYMBOLS))
        self.assertEqual(selected_symbols([]), ["BTCUSDT"])
        for symbol in SYMBOLS:
            self.assertEqual(selected_symbols(["--symbol", symbol]), [symbol])

    def test_75_symbol_flags_reject_conflicts_and_unknown_symbols(self) -> None:
        self.assertEqual(main(["--symbol", "FOOUSDT"]), 2)
        self.assertEqual(main(["--symbol", "..\\BTCUSDT"]), 2)
        self.assertEqual(main(["--symbol", "XRP/USDT"]), 2)
        self.assertEqual(main(["--symbol", "ETHUSDT", "--all-symbols"]), 2)
        self.assertEqual(batch_exit_code(["ok", "ok", "ok"]), 0)
        self.assertEqual(batch_exit_code(["ok", "failed"]), 1)
        self.assertEqual(batch_exit_code([]), 1)

    def test_76_source_paths_partitions_and_timestamps(self) -> None:
        import pyarrow.parquet as pq

        for symbol in SYMBOLS:
            for timeframe, rows in EXPECTED_ROWS.items():
                folder = source_dataset(symbol)[timeframe]
                files = sorted(folder.rglob("data.parquet"))
                self.assertEqual(len(files), EXPECTED_PARTITIONS[timeframe], symbol + timeframe)
                counted = sum(pq.ParquetFile(path).metadata.num_rows for path in files)
                self.assertEqual(counted, rows, symbol + timeframe)
                last = pq.ParquetFile(files[-1]).read(columns=["open_time", "close_time"])
                self.assertEqual(last.column("open_time")[-1].as_py(), EXPECTED_LAST[timeframe])
                self.assertLess(last.column("close_time")[-1].as_py(), EXCLUSIVE_LOAD_END)

    def test_77_reports_and_revisions_do_not_cross_symbols(self) -> None:
        self.assertEqual(len({report_path(symbol) for symbol in SYMBOLS}), 9)
        self.assertEqual(len({log_path(symbol) for symbol in SYMBOLS}), 9)
        self.assertNotEqual(report_path("BTCUSDT"), report_path("ETHUSDT"))
        self.assertNotEqual(log_path("BTCUSDT"), log_path("ETHUSDT"))
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "BTCUSDT_MTF_Most_Touched_Levels_rev02.xlsx").write_bytes(b"btc")
            (root / "ETHUSDT_MTF_Most_Touched_Levels_rev00.xlsx").write_bytes(b"eth")
            (root / "btcusdt_MTF_Most_Touched_Levels_rev08.xlsx").write_bytes(b"case")
            (root / "~$BTCUSDT_MTF_Most_Touched_Levels_rev09.xlsx").write_bytes(b"lock")
            (root / "BTCUSDT_MTF_Most_Touched_Levels_rev07.xlsx.tmp").write_bytes(b"tmp")
            (root / "BTCUSDT_MTF_Most_Touched_Levels_rev06.xlsx.bak").write_bytes(b"bak")
            self.assertEqual(scan_revisions("BTCUSDT", root), [2, 8])
            self.assertEqual(scan_revisions("ETHUSDT", root), [0])
            self.assertEqual(next_revision(scan_revisions("ETHUSDT", root)), 1)
            self.assertEqual(next_revision(scan_revisions("BTCUSDT", root)), 9)
            (root / "BTCUSDT_MTF_Most_Touched_Levels_rev09.xlsx").write_bytes(b"new")
            self.assertEqual(next_revision(scan_revisions("BTCUSDT", root)), 10)
            self.assertEqual(revision_filename(100, "LINKUSDT"), "LINKUSDT_MTF_Most_Touched_Levels_rev100.xlsx")
            self.assertEqual(MIN_TOUCHES, {"1h": 40, "4h": 20, "1d": 10})
            self.assertEqual(RUNNER_VERSION, "UM_PERP_MTF_PRICE_TOUCH_HIERARCHY_V1")
            self.assertEqual(ALGORITHM_VERSION, "BTCUSDT_MTF_PRICE_TOUCH_HIERARCHY_V1")

    def test_78_symbol_state_and_workbook_identity_stay_separate(self) -> None:
        first = analyze({"1h": _qualified_hour_bars()})
        remembered = (first.levels["1h"][0].level_id, first.levels["1h"][0].touches, str(first.levels["1h"][0].price))
        shifted = []
        for bar in _qualified_hour_bars():
            shifted.append(Bar(bar.timeframe, bar.row, bar.open_time, bar.close_time, bar.open + D("1000"), bar.high + D("1000"), bar.low + D("1000"), bar.close + D("1000"), bar.volume))
        second = analyze({"1h": shifted})
        self.assertEqual((first.levels["1h"][0].level_id, first.levels["1h"][0].touches, str(first.levels["1h"][0].price)), remembered)
        self.assertNotEqual(first.levels["1h"][0].price, second.levels["1h"][0].price)
        self.assertIsNot(first, second)
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "ETHUSDT_MTF_Most_Touched_Levels_rev00.xlsx"
            context = {
                "symbol": "ETHUSDT",
                "revision_name": target.name,
                "diagnostic_rows": [
                    ["implementation", "symbol", "ETHUSDT"],
                    ["thresholds", "1h_minimum_completed_candle_touches", 40],
                    ["thresholds", "4h_minimum_completed_candle_touches", 20],
                    ["thresholds", "1d_minimum_completed_candle_touches", 10],
                    ["counts", "duplicate_touch_keys", 0],
                ],
            }
            write_workbook(target, second, context, "ETHUSDT synthetic workbook")
            values = validate_saved_workbook(target, "ETHUSDT")
            self.assertEqual(values["symbol"], "ETHUSDT")
            self.assertNotEqual(values["symbol"], "BTCUSDT")

    def test_79_btc_regression_fingerprints_remain_unchanged(self) -> None:
        from detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy import load_production

        loaded = load_production("BTCUSDT")
        september = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=UTC)
        historical = analyze(loaded["bars"], dataset_end=september)
        current = analyze(loaded["bars"], dataset_end=DATASET_END)
        repeated = analyze(loaded["bars"], dataset_end=DATASET_END)
        self.assertEqual(analytical_fingerprint(historical), "60d33c5db172914b952b0bbafff219ae21c71e932dd3bdc2cd67de8dd4ab8865")
        self.assertEqual(analytical_fingerprint(current), "72d563eef98a3d85ad777973f398a5da3a6d9ca120013d535a5f4517f05a6501")
        self.assertEqual(analytical_fingerprint(repeated), analytical_fingerprint(current))
        self.assertEqual(len(current.groups), 70)


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


WRAPPER_MODULES = {
    "BTCUSDT": "detectors.price_touch_hierarchy_v1.runners.run_btcusdt_price_touch_hierarchy",
    "ETHUSDT": "detectors.price_touch_hierarchy_v1.runners.run_ethusdt_price_touch_hierarchy",
    "AVAXUSDT": "detectors.price_touch_hierarchy_v1.runners.run_avaxusdt_price_touch_hierarchy",
    "AAVEUSDT": "detectors.price_touch_hierarchy_v1.runners.run_aaveusdt_price_touch_hierarchy",
    "NEARUSDT": "detectors.price_touch_hierarchy_v1.runners.run_nearusdt_price_touch_hierarchy",
    "LINKUSDT": "detectors.price_touch_hierarchy_v1.runners.run_linkusdt_price_touch_hierarchy",
    "LTCUSDT": "detectors.price_touch_hierarchy_v1.runners.run_ltcusdt_price_touch_hierarchy",
    "OPUSDT": "detectors.price_touch_hierarchy_v1.runners.run_opusdt_price_touch_hierarchy",
    "DOTUSDT": "detectors.price_touch_hierarchy_v1.runners.run_dotusdt_price_touch_hierarchy",
    "XRPUSDT": "detectors.price_touch_hierarchy_v1.runners.run_xrpusdt_price_touch_hierarchy",
    "DOGEUSDT": "detectors.price_touch_hierarchy_v1.runners.run_dogeusdt_price_touch_hierarchy",
    "ADAUSDT": "detectors.price_touch_hierarchy_v1.runners.run_adausdt_price_touch_hierarchy",
    "SOLUSDT": "detectors.price_touch_hierarchy_v1.runners.run_solusdt_price_touch_hierarchy",
}
DEBUG_CONFIGURATION_NAMES = {
    symbol: f"Debug {symbol} Price Touch Hierarchy" for symbol in WRAPPER_MODULES
}
PRESERVED_LAUNCH_NAMES = (
    "Run BTCUSDT 4H Range Detector",
    "Run 30m Special Swing Detector",
    "Run Price Touch Hierarchy Detector",
)


def _tracked_output_stats() -> dict[str, tuple[int, int]]:
    stats: dict[str, tuple[int, int]] = {}
    desktop = Path(r"C:\Users\oranb\Desktop")
    for symbol in SUPPORTED_SYMBOLS:
        for path in desktop.glob(f"{symbol}_MTF_Most_Touched_Levels_rev*.xlsx"):
            if path.name.startswith("~$"):
                continue
            item = path.stat()
            stats[str(path)] = (item.st_size, item.st_mtime_ns)
    for folder in (PROJECT_ROOT / "reports", PROJECT_ROOT / "logs"):
        if not folder.exists():
            continue
        for path in folder.glob("price_touch_hierarchy_*"):
            if not path.is_file():
                continue
            item = path.stat()
            stats[str(path)] = (item.st_size, item.st_mtime_ns)
    source = Path(r"C:\MarketData\derived\binance\futures\um\perpetual")
    for timeframe in TIMEFRAMES:
        for symbol in SUPPORTED_SYMBOLS:
            for path in (source / timeframe / f"symbol={symbol}").rglob("*.parquet"):
                item = path.stat()
                stats[str(path)] = (item.st_size, item.st_mtime_ns)
    return stats


class SymbolRunnerTests(unittest.TestCase):
    def test_80_exactly_thirteen_wrapper_modules_exist(self) -> None:
        folder = PROJECT_ROOT / "detectors" / "price_touch_hierarchy_v1" / "runners"
        found = sorted(path.name for path in folder.glob("run_*_price_touch_hierarchy.py"))
        expected = sorted(module.rsplit(".", 1)[-1] + ".py" for module in WRAPPER_MODULES.values())
        self.assertEqual(found, expected)
        self.assertEqual(len(found), 13)
        self.assertTrue((folder / "__init__.py").is_file())

    def test_81_each_wrapper_declares_its_fixed_symbol_and_shared_api(self) -> None:
        import importlib

        for symbol, module_name in WRAPPER_MODULES.items():
            source = (PROJECT_ROOT / Path(*module_name.split(".")).with_suffix(".py")).read_text(encoding="utf-8")
            tree = ast.parse(source)
            imports = [
                node for node in tree.body
                if isinstance(node, ast.ImportFrom) and node.module != "__future__"
            ]
            self.assertEqual(len(imports), 1)
            self.assertEqual(imports[0].module, "detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy")
            self.assertEqual([alias.name for alias in imports[0].names], ["run_for_symbol_cli"])
            functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
            self.assertEqual([node.name for node in functions], ["main"])
            self.assertEqual(functions[0].args.args, [])
            assigned = {
                node.targets[0].id: ast.literal_eval(node.value)
                for node in tree.body
                if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
            }
            self.assertEqual(assigned["SYMBOL"], symbol)
            module = importlib.import_module(module_name)
            self.assertEqual(module.SYMBOL, symbol)
            self.assertIs(module.run_for_symbol_cli, run_for_symbol_cli)

    def test_82_wrappers_do_not_duplicate_engine_logic_or_constants(self) -> None:
        package = PROJECT_ROOT / "detectors" / "price_touch_hierarchy_v1"
        analyze_defs = []
        for path in sorted(package.rglob("*.py")):
            if "tests" in path.parts:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            analyze_defs.extend(
                path.name
                for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef) and node.name == "analyze"
            )
        self.assertEqual(analyze_defs, ["engine.py"])
        forbidden = (
            "analyze(",
            "write_workbook",
            "MIN_TOUCHES",
            "hierarchy_score",
            "DATASET_END",
            "load_production",
            "scan_revisions",
            "GRID_ANCHOR",
            "FINAL_HALF_WIDTH",
            "representative_price",
        )
        for module_name in WRAPPER_MODULES.values():
            source = (PROJECT_ROOT / Path(*module_name.split(".")).with_suffix(".py")).read_text(encoding="utf-8")
            for token in forbidden:
                self.assertNotIn(token, source, module_name)

    def test_83_importing_wrappers_does_no_production_work(self) -> None:
        before = _tracked_output_stats()
        script = "import importlib; " + "; ".join(
            f"importlib.import_module({module!r})" for module in WRAPPER_MODULES.values()
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertNotIn("price_touch_hierarchy_v1 starting", completed.stdout)
        self.assertNotIn("SUMMARY_JSON", completed.stdout)
        self.assertNotIn("workbook", completed.stdout.lower())
        self.assertEqual(_tracked_output_stats(), before)

    def test_84_each_wrapper_calls_the_shared_api_once_for_its_symbol(self) -> None:
        import importlib

        for symbol, module_name in WRAPPER_MODULES.items():
            module = importlib.import_module(module_name)
            with patch.object(module, "run_for_symbol_cli", return_value=0) as shared:
                self.assertEqual(module.main(), 0)
            shared.assert_called_once_with(symbol)

    def test_85_a_wrapper_symbol_cannot_be_overridden(self) -> None:
        import detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy as runner
        import importlib

        with patch.object(runner, "main", return_value=0) as shared:
            with patch.object(sys, "argv", ["wrapper", "--symbol", "BTCUSDT", "--all-symbols"]):
                self.assertEqual(run_for_symbol_cli("ETHUSDT"), 0)
        shared.assert_called_once_with(["--symbol", "ETHUSDT"])
        module = importlib.import_module(WRAPPER_MODULES["AVAXUSDT"])
        with patch.object(module, "run_for_symbol_cli", return_value=0) as shared:
            with patch.object(sys, "argv", ["wrapper", "--symbol", "BTCUSDT", "--all-symbols"]):
                self.assertEqual(module.main(), 0)
        shared.assert_called_once_with("AVAXUSDT")
        with patch.object(runner, "main") as shared:
            self.assertEqual(run_for_symbol_cli("NOTUSDT"), 2)
        shared.assert_not_called()

    def test_86_shared_success_and_failure_exit_codes_pass_through(self) -> None:
        import detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy as runner
        import importlib

        module = importlib.import_module(WRAPPER_MODULES["LTCUSDT"])
        with patch.object(module, "run_for_symbol_cli", return_value=0):
            self.assertEqual(module.main(), 0)
        with patch.object(module, "run_for_symbol_cli", return_value=1):
            self.assertEqual(module.main(), 1)
        with patch.object(runner, "main", return_value=0):
            self.assertEqual(run_for_symbol_cli("BTCUSDT"), 0)
        with patch.object(runner, "main", return_value=4):
            self.assertEqual(run_for_symbol_cli("OPUSDT"), 4)

    def test_87_reports_logs_and_revisions_stay_symbol_specific(self) -> None:
        self.assertEqual(len({report_path(symbol) for symbol in SYMBOLS}), 9)
        self.assertEqual(len({log_path(symbol) for symbol in SYMBOLS}), 9)
        for symbol in SYMBOLS:
            self.assertTrue(report_path(symbol).name.endswith(f"price_touch_hierarchy_{symbol}.json"))
            self.assertTrue(log_path(symbol).name.endswith(f"price_touch_hierarchy_{symbol}.log"))
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "BTCUSDT_MTF_Most_Touched_Levels_rev03.xlsx").write_bytes(b"btc")
            (root / "ETHUSDT_MTF_Most_Touched_Levels_rev00.xlsx").write_bytes(b"eth")
            (root / "AVAXUSDT_MTF_Most_Touched_Levels_rev04.xlsx").write_bytes(b"avax")
            self.assertEqual(next_revision(scan_revisions("BTCUSDT", root)), 4)
            self.assertEqual(next_revision(scan_revisions("ETHUSDT", root)), 1)
            self.assertEqual(next_revision(scan_revisions("AVAXUSDT", root)), 5)
            self.assertEqual(scan_revisions("DOTUSDT", root), [])
            self.assertFalse(any(root.glob("*.tmp")))

    def test_88_btc_regression_fingerprints_remain_unchanged(self) -> None:
        from detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy import load_production

        loaded = load_production("BTCUSDT")
        september = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=UTC)
        historical = analyze(loaded["bars"], dataset_end=september)
        current = analyze(loaded["bars"], dataset_end=DATASET_END)
        self.assertEqual(
            analytical_fingerprint(historical),
            "60d33c5db172914b952b0bbafff219ae21c71e932dd3bdc2cd67de8dd4ab8865",
        )
        self.assertEqual(
            analytical_fingerprint(current),
            "72d563eef98a3d85ad777973f398a5da3a6d9ca120013d535a5f4517f05a6501",
        )
        self.assertFalse(list(Path(r"C:\Users\oranb\Desktop").glob("BTCUSDT_MTF_Most_Touched_Levels_rev04.xlsx")))

    def test_89_generic_runner_selection_remains_unchanged(self) -> None:
        import detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy as runner

        self.assertEqual(selected_symbols([]), ["BTCUSDT"])
        self.assertEqual(selected_symbols(["--symbol", "ETHUSDT"]), ["ETHUSDT"])
        self.assertEqual(selected_symbols(["--all-symbols"]), list(SYMBOLS))
        def summary_for(symbol: str) -> dict[str, str]:
            return {"status": "ok", "symbol": symbol}

        def guarded_main(arguments: list[str]):
            with patch.object(runner, "load_production", return_value={"source_paths": []}) as loaded, \
                 patch.object(runner, "run_for_symbol", side_effect=summary_for) as ran, \
                 patch.object(runner, "_write_batch_report") as batch, \
                 patch.object(runner, "_cleanup"), \
                 patch.object(runner, "_package_hashes", return_value={"same": "1"}), \
                 patch.object(runner, "_fingerprint_tree", return_value={"same": "1"}), \
                 patch.object(runner, "_stray_python_files", return_value=[]):
                code = runner.main(arguments)
            return code, loaded, ran, batch

        code, _loaded, ran, batch = guarded_main([])
        self.assertEqual(code, 0)
        ran.assert_called_once_with("BTCUSDT")
        batch.assert_not_called()
        code, _loaded, ran, batch = guarded_main(["--symbol", "NEARUSDT"])
        self.assertEqual(code, 0)
        ran.assert_called_once_with("NEARUSDT")
        batch.assert_not_called()
        code, loaded, ran, batch = guarded_main(["--all-symbols"])
        self.assertEqual(code, 0)
        self.assertEqual([call.args[0] for call in ran.call_args_list], list(SYMBOLS))
        self.assertEqual([call.args[0] for call in loaded.call_args_list], list(SYMBOLS))
        batch.assert_called_once()
        self.assertEqual(batch.call_args.args[0], list(SYMBOLS))

    def test_90_wrapper_modules_execute_without_publication(self) -> None:
        before = _tracked_output_stats()
        for symbol, module_name in WRAPPER_MODULES.items():
            with tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                marker = root / "called.txt"
                hook = root / "sitecustomize.py"
                hook.write_text(
                    "import detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy as runner\n"
                    "def fake(symbol):\n"
                    f"    open({str(marker)!r}, 'w', encoding='utf-8').write(symbol)\n"
                    "    return 0\n"
                    "runner.run_for_symbol_cli = fake\n",
                    encoding="utf-8",
                )
                env = os.environ.copy()
                env["PYTHONPATH"] = str(root) + os.pathsep + str(PROJECT_ROOT)
                completed = subprocess.run(
                    [sys.executable, "-m", module_name],
                    cwd=PROJECT_ROOT,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertEqual(marker.read_text(encoding="utf-8"), symbol)
                self.assertNotIn("--all-symbols", completed.stdout)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            hook = root / "sitecustomize.py"
            hook.write_text(
                "import detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy as runner\n"
                "def fake(symbol):\n"
                "    return 5\n"
                "runner.run_for_symbol_cli = fake\n",
                encoding="utf-8",
            )
            env = os.environ.copy()
            env["PYTHONPATH"] = str(root) + os.pathsep + str(PROJECT_ROOT)
            completed = subprocess.run(
                [sys.executable, "-m", WRAPPER_MODULES["DOTUSDT"]],
                cwd=PROJECT_ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 5, completed.stderr)
        self.assertEqual(_tracked_output_stats(), before)

    def test_91_launch_configurations_map_one_wrapper_each(self) -> None:
        launch = json.loads((PROJECT_ROOT / ".vscode" / "launch.json").read_text(encoding="utf-8"))
        settings = json.loads((PROJECT_ROOT / ".vscode" / "settings.json").read_text(encoding="utf-8"))
        configurations = launch["configurations"]
        names = [item["name"] for item in configurations]
        self.assertEqual(len(names), len(set(names)))
        for name in PRESERVED_LAUNCH_NAMES:
            self.assertIn(name, names)
        interpreter = settings["python.defaultInterpreterPath"].replace("/", "\\")
        self.assertIn(".venv\\Scripts\\python.exe", interpreter)
        for symbol, name in DEBUG_CONFIGURATION_NAMES.items():
            matches = [item for item in configurations if item["name"] == name]
            self.assertEqual(len(matches), 1)
            item = matches[0]
            self.assertEqual(item["type"], "debugpy")
            self.assertEqual(item["request"], "launch")
            self.assertEqual(item["module"], WRAPPER_MODULES[symbol])
            self.assertEqual(item["cwd"], "${workspaceFolder}")
            self.assertEqual(item["console"], "integratedTerminal")
            self.assertIs(item["justMyCode"], True)
            self.assertNotIn("stopOnEntry", item)
            self.assertNotIn("port", item)
            self.assertIn(".venv", item["python"].replace("\\", "/"))
            self.assertNotIn("program", item)
            rendered = json.dumps(item)
            self.assertNotIn("--all-symbols", rendered)
            self.assertNotIn("--symbol", rendered)
        for item in configurations:
            self.assertNotIn("--all-symbols", json.dumps(item.get("args", [])))
        gitignore = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertIn(".vscode/", gitignore)

    def test_92_runner_tests_leave_no_desktop_revision_or_temp_workbook(self) -> None:
        desktop = Path(r"C:\Users\oranb\Desktop")
        self.assertFalse(list(desktop.glob("BTCUSDT_MTF_Most_Touched_Levels_rev04.xlsx")))
        self.assertFalse(list(desktop.glob("~$*_MTF_Most_Touched_Levels_rev*.xlsx")))
        temporary = PROJECT_ROOT / "tmp" / "price_touch_hierarchy_v1"
        self.assertFalse(temporary.exists())


class DesktopOutputTests(unittest.TestCase):
    def test_93_known_folder_resolves_the_visible_desktop(self) -> None:
        from detectors.price_touch_hierarchy_v1.desktop_output import resolve_visible_desktop

        location = resolve_visible_desktop()
        self.assertEqual(location.method, "known_folder")
        self.assertTrue(location.path.is_absolute())
        self.assertTrue(location.path.is_dir())
        self.assertNotEqual(location.path.resolve(), PROJECT_ROOT.resolve())
        self.assertFalse(PROJECT_ROOT.resolve() in location.path.resolve().parents)

    def test_94_local_onedrive_unicode_and_environment_paths(self) -> None:
        from detectors.price_touch_hierarchy_v1.desktop_output import (
            DesktopResolutionError,
            KnownFolderUnavailable,
            resolve_visible_desktop,
        )

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            local = root / "Desktop"
            local.mkdir()
            onedrive = root / "OneDrive" / "Desktop"
            onedrive.mkdir(parents=True)
            spaced = root / "Visible Desktop" / "Masaüstü"
            spaced.parent.mkdir()
            expanded = root / "Expanded Desktop"
            expanded.mkdir()
            os.environ["PTH_DESKTOP_TEST"] = str(expanded)
            self.addCleanup(os.environ.pop, "PTH_DESKTOP_TEST", None)
            self.assertEqual(resolve_visible_desktop(known_folder=lambda: str(local)).path, local.resolve())
            self.assertEqual(
                resolve_visible_desktop(known_folder=lambda: str(onedrive)).method,
                "known_folder",
            )
            self.assertEqual(resolve_visible_desktop(known_folder=lambda: str(onedrive)).path, onedrive.resolve())
            created = resolve_visible_desktop(known_folder=lambda: str(spaced))
            self.assertTrue(created.path.is_dir())
            self.assertEqual(created.path, spaced.resolve())
            self.assertEqual(resolve_visible_desktop(known_folder=lambda: "%PTH_DESKTOP_TEST%").path, expanded.resolve())
            file_path = root / "not-a-directory"
            file_path.write_text("x", encoding="utf-8")
            with self.assertRaises(DesktopResolutionError):
                resolve_visible_desktop(known_folder=lambda: str(file_path))
            with self.assertRaises(DesktopResolutionError):
                resolve_visible_desktop(known_folder=lambda: str(PROJECT_ROOT))
            with self.assertRaises(DesktopResolutionError):
                resolve_visible_desktop(known_folder=lambda: str(PROJECT_ROOT / "reports"))

            def unavailable() -> str:
                raise KnownFolderUnavailable("api missing")

            registry_calls = []

            def registry() -> str:
                registry_calls.append("called")
                return str(onedrive)

            with self.assertRaises(DesktopResolutionError):
                resolve_visible_desktop(known_folder=lambda: str(PROJECT_ROOT), registry_desktop=registry)
            self.assertEqual(registry_calls, [])
            redirected = resolve_visible_desktop(known_folder=unavailable, registry_desktop=lambda: str(onedrive))
            self.assertEqual(redirected.method, "user_shell_folders")
            self.assertEqual(redirected.path, onedrive.resolve())
            (root / "home").mkdir()
            home = resolve_visible_desktop(
                known_folder=unavailable,
                registry_desktop=unavailable,
                home=root / "home",
            )
            self.assertEqual(home.method, "home_desktop")
            self.assertTrue((root / "home" / "Desktop").is_dir())

    def test_95_revision_allocation_scans_only_the_selected_directory(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "AVAXUSDT_MTF_Most_Touched_Levels_rev00.xlsx").write_bytes(b"old")
            (root / "AVAXUSDT_MTF_Most_Touched_Levels_rev02.xlsx").write_bytes(b"gap")
            (root / "LINKUSDT_MTF_Most_Touched_Levels_rev100.xlsx").write_bytes(b"high")
            (root / "btcusdt_mtf_most_touched_levels_rev07.xlsx").write_bytes(b"case")
            (root / "ETHUSDT_MTF_Most_Touched_Levels_rev04.xlsx").write_bytes(b"other")
            (root / "~$AVAXUSDT_MTF_Most_Touched_Levels_rev09.xlsx").write_bytes(b"lock")
            (root / "AVAXUSDT_MTF_Most_Touched_Levels_rev08.xlsx.tmp").write_bytes(b"tmp")
            (root / "AVAXUSDT_MTF_Most_Touched_Levels_rev06.xlsx.part").write_bytes(b"part")
            (root / "AVAXUSDT_MTF_Most_Touched_Levels_rev05.xlsx.bak").write_bytes(b"bak")
            (root / "notes.xlsx").write_bytes(b"unrelated")
            self.assertEqual(next_revision(scan_revisions("DOTUSDT", root)), 0)
            self.assertEqual(next_revision(scan_revisions("AVAXUSDT", root)), 3)
            self.assertEqual(scan_revisions("AVAXUSDT", root), [0, 2])
            self.assertEqual(next_revision(scan_revisions("LINKUSDT", root)), 101)
            self.assertEqual(revision_filename(101, "LINKUSDT"), "LINKUSDT_MTF_Most_Touched_Levels_rev101.xlsx")
            self.assertEqual(scan_revisions("BTCUSDT", root), [7])
            self.assertEqual(scan_revisions("ETHUSDT", root), [4])
            self.assertEqual((root / "AVAXUSDT_MTF_Most_Touched_Levels_rev00.xlsx").read_bytes(), b"old")

    def test_96_destination_that_appears_allocates_the_next_revision(self) -> None:
        import detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy as runner

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            desktop = root / "desktop"
            temporary = root / "tmp"
            desktop.mkdir()
            temporary.mkdir()
            original = desktop / "ETHUSDT_MTF_Most_Touched_Levels_rev00.xlsx"
            original.write_bytes(b"keep")
            context = {
                "symbol": "ETHUSDT",
                "existing_revisions": [0],
                "planned_revision": 1,
                "desktop_dir": desktop,
                "desktop_method": "known_folder",
                "temporary_dir": temporary,
            }

            def fake_write(path: Path, _result, _context, _readme) -> None:
                path.write_bytes(b"temporary")
                if path.name.endswith("rev01.xlsx"):
                    (desktop / "ETHUSDT_MTF_Most_Touched_Levels_rev01.xlsx").write_bytes(b"raced")

            with patch.object(runner, "write_workbook", fake_write), \
                 patch.object(runner, "_validate_workbook_container"), \
                 patch.object(runner, "_diagnostic_rows", return_value=[]):
                revision, final = runner._commit_workbook(None, context, "readme")
            self.assertEqual(revision, 2)
            self.assertEqual(final.name, "ETHUSDT_MTF_Most_Touched_Levels_rev02.xlsx")
            self.assertEqual(original.read_bytes(), b"keep")
            self.assertEqual((desktop / "ETHUSDT_MTF_Most_Touched_Levels_rev01.xlsx").read_bytes(), b"raced")
            self.assertFalse(list(temporary.glob("*.xlsx")))

    def test_97_wrappers_share_one_resolver_and_keep_fixed_symbols(self) -> None:
        import detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy as runner
        from detectors.price_touch_hierarchy_v1.desktop_output import DesktopLocation

        for symbol, module_name in WRAPPER_MODULES.items():
            source = (PROJECT_ROOT / Path(*module_name.split(".")).with_suffix(".py")).read_text(encoding="utf-8")
            self.assertNotIn("SHGetKnownFolderPath", source)
            self.assertNotIn("visible_desktop", source)
            self.assertNotIn("desktop_dir", source)
            self.assertIn(f'SYMBOL = "{symbol}"', source)
        self.assertEqual(selected_symbols([]), ["BTCUSDT"])
        self.assertEqual(selected_symbols(["--symbol", "AVAXUSDT"]), ["AVAXUSDT"])
        self.assertEqual(selected_symbols(["--all-symbols"]), list(SYMBOLS))
        with tempfile.TemporaryDirectory() as folder:
            desktop = Path(folder)
            (desktop / "AVAXUSDT_MTF_Most_Touched_Levels_rev01.xlsx").write_bytes(b"avax")
            (desktop / "BTCUSDT_MTF_Most_Touched_Levels_rev03.xlsx").write_bytes(b"btc")
            location = DesktopLocation(desktop, "known_folder")
            with patch.object(runner, "visible_desktop", return_value=location):
                self.assertEqual(runner.scan_revisions("AVAXUSDT"), [1])
                self.assertEqual(runner.scan_revisions("BTCUSDT"), [3])
                self.assertEqual(next_revision(runner.scan_revisions("AVAXUSDT")), 2)


class ExtensionSymbolTests(unittest.TestCase):
    def test_98_four_profiles_are_explicit_and_isolated(self) -> None:
        import config

        self.assertEqual(PRICE_TOUCH_EXTENSION_SYMBOLS, ("XRPUSDT", "DOGEUSDT", "ADAUSDT", "SOLUSDT"))
        self.assertEqual(SUPPORTED_SYMBOLS, SYMBOLS + PRICE_TOUCH_EXTENSION_SYMBOLS)
        self.assertEqual(len(set(SUPPORTED_SYMBOLS)), 13)
        self.assertTrue(set(PRICE_TOUCH_EXTENSION_SYMBOLS).isdisjoint(SYMBOLS))
        self.assertEqual(tuple(config.ALLOWED_SYMBOLS), SYMBOLS)
        self.assertEqual(selected_symbols(["--all-symbols"]), list(SYMBOLS))
        self.assertEqual(selected_symbols([]), ["BTCUSDT"])
        for symbol in PRICE_TOUCH_EXTENSION_SYMBOLS:
            self.assertEqual(selected_symbols(["--symbol", symbol]), [symbol])
            profile = symbol_profile(symbol)
            self.assertEqual(profile.tradingview_symbol, f"BINANCE:{symbol}.P")
            self.assertEqual(profile.workbook_family, f"{symbol}_MTF_Most_Touched_Levels_revNN.xlsx")
            self.assertEqual(profile.report_name, f"price_touch_hierarchy_{symbol}.json")
            self.assertEqual(profile.log_name, f"price_touch_hierarchy_{symbol}.log")
            self.assertEqual(profile.algorithm_version, ALGORITHM_VERSION)
            self.assertEqual(dict(profile.expected_rows), {"1h": 24744, "4h": 6186, "1d": 1031})
            self.assertEqual(dict(profile.expected_partitions), {"1h": 34, "4h": 34, "1d": 3})
            self.assertEqual(profile.first_open_utc, "2024-01-01T00:00:00Z")
            self.assertEqual(profile.last_close_utc, "2026-10-27T23:59:59.999Z")
            self.assertEqual(profile.exclusive_end_utc, "2026-10-28T00:00:00Z")
            sources = source_dataset(symbol)
            for timeframe, path in sources.items():
                text = str(path).replace("\\", "/")
                self.assertIn(f"/{timeframe}/symbol={symbol}", text)
                self.assertNotIn("symbol=BTCUSDT", text)
                self.assertEqual(path, Path(getattr(profile, f"source_{timeframe}")))
        for bad in ("", "btcusdt", "FOOUSDT", "XRP/USDT", "..\\SOLUSDT", "C:SOLUSDT", "SOL USDT"):
            with self.assertRaises(ValueError):
                require_price_touch_symbol(bad)
            with self.assertRaises(ValueError):
                source_dataset(bad)

    def test_99_extension_workbook_uses_the_shared_schema(self) -> None:
        import zipfile

        from openpyxl import load_workbook

        result = analyze({"1h": _qualified_hour_bars()})
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "XRPUSDT_MTF_Most_Touched_Levels_rev00.xlsx"
            context = {
                "symbol": "XRPUSDT",
                "revision_name": target.name,
                "diagnostic_rows": [
                    ["implementation", "symbol", "XRPUSDT"],
                    ["thresholds", "1h_minimum_completed_candle_touches", 40],
                    ["thresholds", "4h_minimum_completed_candle_touches", 20],
                    ["thresholds", "1d_minimum_completed_candle_touches", 10],
                    ["counts", "duplicate_touch_keys", 0],
                ],
            }
            write_workbook(target, result, context, "XRPUSDT synthetic workbook")
            validate_saved_workbook(target, "XRPUSDT")
            with zipfile.ZipFile(target) as archive:
                self.assertIsNone(archive.testzip())
                names = set(archive.namelist())
                self.assertNotIn("xl/vbaProject.bin", names)
            for kwargs in ({}, {"read_only": True}, {"data_only": True}):
                book = load_workbook(target, **kwargs)
                try:
                    self.assertEqual(tuple(book.sheetnames), (
                        "Executive Summary", "Hierarchical Ranking", "1D Levels", "4H Levels",
                        "1H Levels", "Cross-TF Mapping", "Top Level Touches", "Monthly Persistence",
                        "Suppressed Levels", "Rejected Candidates", "Parameters", "Diagnostics", "README",
                    ))
                finally:
                    book.close()
            book = load_workbook(target, data_only=True)
            try:
                sheet = book["Hierarchical Ranking"]
                headers = [cell.value for cell in next(sheet.iter_rows(min_row=1, max_row=1))]
                rank_at = headers.index("Overall Hierarchical Rank")
                price_at = headers.index("Representative Price")
                lower_at = headers.index("Consolidated Zone Lower")
                upper_at = headers.index("Consolidated Zone Upper")
                present_at = headers.index("Timeframes Present")
                ranks = []
                for row in sheet.iter_rows(min_row=2, values_only=True):
                    if row[rank_at] is None:
                        continue
                    ranks.append(int(row[rank_at]))
                    price = row[price_at]
                    self.assertIsInstance(price, (int, float))
                    self.assertLessEqual(float(row[lower_at]), float(price))
                    self.assertLessEqual(float(price), float(row[upper_at]))
                    present = str(row[present_at] or "")
                    self.assertGreaterEqual(int(row[headers.index("1H Completed-Candle Touches")] or 0), 0)
                self.assertEqual(ranks, list(range(1, len(ranks) + 1)))
            finally:
                book.close()


if __name__ == "__main__":
    unittest.main()
