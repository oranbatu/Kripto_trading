"""Required 2026 4h Swing Open/Close detector tests."""
from __future__ import annotations

import hashlib
import tempfile
import unittest
import zipfile
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from openpyxl import load_workbook

from detectors.swing_open_close_4h_with_1h_v1.engine import (
    COMPACT_THRESHOLD,
    ALTERNATIVE_DURATION_ABOVE,
    ALTERNATIVE_DURATION_BELOW,
    ALTERNATIVE_METHOD,
    ALTERNATIVE_NONE,
    CONFIRMED_ALTERNATIVE,
    CONFIRMED_COMPACT,
    CONFIRMED_STANDARD,
    CROSSED_RETURN,
    DERIVED,
    EXACT_RETURN,
    EXPIRED,
    REJECTED_CONFLICT,
    REJECTED_EXTREME_ON_CLOSE,
    REJECTED_EXTREME_ON_OPEN,
    REJECTED_NO_INTERIOR,
    REFERENCE_ABOVE_20,
    REJECTED_DURATION_GAP,
    REJECTED_WIDTH_250,
    REFERENCE_BOTH_WIDTHS,
    REFERENCE_OPEN_WIDTH,
    REJECTED_WIDTH_350,
    STANDARD_THRESHOLD,
    SWING_HIGH,
    SWING_LOW,
    TERMINAL,
    Bar,
    SwingError,
    analyze,
    audit_result,
    alternative_duration_rejection,
    both_widths_pass,
    duration_rejection,
    extreme_position_rejection,
    PRIMARY,
    iso_turkey,
    iso_utc,
    primary_sort_key,
)
from detectors.swing_open_close_4h_with_1h_v1.run_swing_open_close_4h_with_1h_detector import (
    load_production,
    next_revision,
    revision_filename,
    scan_revisions,
)
from detectors.swing_open_close_4h_with_1h_v1.swing_config import (
    ANALYSIS_END_CLOSE,
    ANALYSIS_END_OPEN,
    ANALYSIS_START,
    CONFIG,
    DETECTOR_VERSION,
    EXPECTED_MONTHLY,
    EXPECTED_ROWS,
    MAX_INTERIOR,
    PROJECT_ROOT,
    SOURCE_DATASET,
    WORKBOOK_SHEETS,
)
from detectors.swing_open_close_4h_with_1h_v1.workbook import validate_saved_workbook, write_workbook

UNIT_DIR = PROJECT_ROOT / "tmp" / "swing_open_close_4h_with_1h_unit"


def bar(row: int, open_: str, high: str, low: str, close: str) -> Bar:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=4 * row)
    return Bar(
        row=row,
        open_time=start,
        close_time=start + timedelta(hours=4) - timedelta(milliseconds=1),
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trades=3,
        taker_base=Decimal("4"),
        taker_quote=Decimal("400"),
    )


def attempt_of(result, open_row: int, direction: str):
    return next(item for item in result.attempts if item.open_row == open_row and item.direction == direction)


def highs(result):
    return [item for item in result.swings if item.direction == SWING_HIGH]


def lows(result):
    return [item for item in result.swings if item.direction == SWING_LOW]


def compact_high(extreme: str = "103.5", close: str = "100") -> list[Bar]:
    return [
        bar(0, "100", "101", "99", "100"),
        bar(1, "100", extreme, "99.5", "102"),
        bar(2, "102", "104", "98", close),
    ]


def compact_low(extreme: str = "96.5", close: str = "100") -> list[Bar]:
    return [
        bar(0, "100", "101", "99", "100"),
        bar(1, "100", "100.5", extreme, "98"),
        bar(2, "98", "101", "96", close),
    ]


def hold_until(last_row: int, extreme_high: str, final_close: str, extreme_row: int = 1) -> list[Bar]:
    rows = [bar(0, "100", "101", "99", "100")]
    for row in range(1, last_row):
        rows.append(bar(row, "101", extreme_high if row == extreme_row else "101.2", "99.5", "101"))
    rows.append(bar(last_row, "101", "102", "50", final_close))
    return rows


class SwingOpenClose4hTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        UNIT_DIR.mkdir(parents=True, exist_ok=True)
        cls.sample_bars = compact_high() + [
            bar(3, "100", "101", "99", "100"),
            bar(4, "100", "100.5", "96.5", "97"),
            bar(5, "97", "100", "96", "100"),
        ]
        cls.sample = analyze(cls.sample_bars)
        audit_result(cls.sample)
        cls.sample_path = UNIT_DIR / "sample.xlsx"
        context = {
            "provenance": {
                "rows": 6,
                "first_open_utc": "2026-01-01T00:00:00Z",
                "last_open_utc": "2026-01-01T20:00:00Z",
                "last_close_utc": "2026-01-01T23:59:59.999Z",
            },
            "monthly_counts": {"2026-01": 6},
            "diagnostic_rows": [["Section", "Name", "Value"], ["sample", "rows", 6]],
        }
        write_workbook(cls.sample_path, cls.sample, context, (CONFIG.package_dir / "README.md").read_text(encoding="utf-8"))
        validate_saved_workbook(cls.sample_path, 24)

    @classmethod
    def tearDownClass(cls) -> None:
        if UNIT_DIR.exists():
            for path in UNIT_DIR.glob("*"):
                path.unlink(missing_ok=True)

    def test_01_compact_high(self) -> None:
        result = analyze(compact_high())
        audit_result(result)
        swing = highs(result)[0]
        self.assertEqual(swing.interior_count, 1)
        self.assertEqual(swing.formation_class, "COMPACT")
        self.assertEqual(swing.duration_hours, Decimal("8"))
        self.assertEqual(swing.coverage_hours, Decimal("12"))
        self.assertEqual(swing.left_duration_hours, Decimal("4"))
        self.assertEqual(swing.right_duration_hours, Decimal("4"))

    def test_02_compact_low(self) -> None:
        result = analyze(compact_low())
        audit_result(result)
        self.assertEqual(lows(result)[0].extreme_price, Decimal("96.5"))

    def test_03_compact_five_interiors(self) -> None:
        result = analyze(hold_until(6, "103.5", "100"))
        audit_result(result)
        swing = next(item for item in highs(result) if item.open_row == 0)
        self.assertEqual(swing.interior_count, 5)
        self.assertEqual(swing.duration_hours, Decimal("24"))
        self.assertEqual(swing.coverage_hours, Decimal("28"))

    def test_04_compact_six_interiors(self) -> None:
        result = analyze(hold_until(7, "103.5", "100"))
        audit_result(result)
        swing = next(item for item in highs(result) if item.open_row == 0)
        self.assertEqual(swing.interior_count, 6)
        self.assertEqual(swing.formation_class, "COMPACT")
        self.assertEqual(swing.required_width, COMPACT_THRESHOLD)
        self.assertEqual(swing.duration_hours, Decimal("28"))
        self.assertEqual(swing.coverage_hours, Decimal("32"))

    def test_05_standard_twenty_interiors(self) -> None:
        result = analyze(hold_until(21, "102.5", "100"))
        audit_result(result)
        swing = next(item for item in highs(result) if item.open_row == 0)
        self.assertEqual(swing.interior_count, 20)
        self.assertEqual(swing.formation_class, "STANDARD")
        self.assertEqual(swing.required_width, STANDARD_THRESHOLD)
        self.assertEqual(swing.duration_hours, Decimal("84"))
        self.assertEqual(swing.coverage_hours, Decimal("88"))
        self.assertEqual(attempt_of(result, 0, SWING_HIGH).status, CONFIRMED_STANDARD)

    def test_06_zero_interior_rejected(self) -> None:
        self.assertEqual(duration_rejection(0), REJECTED_NO_INTERIOR)
        result = analyze([bar(0, "100", "110", "90", "100"), bar(1, "100", "110", "90", "100")])
        self.assertEqual(result.swings, [])
        self.assertTrue(all(item.status == TERMINAL for item in result.attempts))

    def test_07_twenty_one_interiors_rejected(self) -> None:
        self.assertEqual(duration_rejection(21), REFERENCE_ABOVE_20)
        self.assertEqual(duration_rejection(7), REJECTED_DURATION_GAP)
        self.assertIsNone(duration_rejection(8))
        self.assertIsNone(duration_rejection(20))
        rows = hold_until(22, "110", "100")
        result = analyze(rows)
        self.assertEqual(attempt_of(result, 0, SWING_HIGH).status, REFERENCE_ABOVE_20)
        self.assertEqual(attempt_of(result, 0, SWING_HIGH).interior_count, 21)
        self.assertFalse(any((item.interior_count or 0) > 20 for item in result.swings))
        self.assertFalse(any(item.interior_count == 7 for item in result.swings))

    def test_08_compact_exact_boundary_passes(self) -> None:
        result = analyze(compact_high("103.5", "100"))
        swing = highs(result)[0]
        self.assertEqual(swing.open_to_extreme_percent, Decimal("3.5"))
        self.assertGreaterEqual(swing.close_to_extreme_percent, COMPACT_THRESHOLD)
        audit_result(result)

    def test_09_compact_below_boundary_fails(self) -> None:
        result = analyze(compact_high("103.49999999", "100"))
        self.assertEqual(attempt_of(result, 0, SWING_HIGH).status, REFERENCE_BOTH_WIDTHS)
        five = analyze(hold_until(6, "102", "100"))
        self.assertEqual(attempt_of(five, 0, SWING_HIGH).status, REFERENCE_BOTH_WIDTHS)

    def test_10_standard_exact_boundary_passes(self) -> None:
        result = analyze(hold_until(9, "102", "100"))
        swing = next(item for item in highs(result) if item.open_row == 0)
        self.assertEqual(swing.interior_count, 8)
        self.assertEqual(swing.open_to_extreme_percent, Decimal("2"))
        self.assertEqual(swing.close_to_extreme_percent, Decimal("2"))
        self.assertEqual(swing.required_width, Decimal("2.00"))
        self.assertEqual(swing.duration_hours, Decimal("36"))
        self.assertEqual(swing.coverage_hours, Decimal("40"))
        audit_result(result)

    def test_11_standard_below_boundary_fails(self) -> None:
        result = analyze(hold_until(9, "101.99999999", "100"))
        self.assertEqual(attempt_of(result, 0, SWING_HIGH).status, REFERENCE_BOTH_WIDTHS)

    def test_12_both_widths_required(self) -> None:
        self.assertFalse(both_widths_pass(Decimal("4"), Decimal("3"), COMPACT_THRESHOLD))
        self.assertFalse(both_widths_pass(Decimal("3.50"), Decimal("3.49999999"), COMPACT_THRESHOLD))
        self.assertTrue(both_widths_pass(Decimal("2.00000000"), Decimal("2.00000000"), STANDARD_THRESHOLD))
        self.assertFalse(both_widths_pass(Decimal("2.00000000"), Decimal("1.99999999"), STANDARD_THRESHOLD))
        self.assertFalse(both_widths_pass(Decimal("1.99999999"), Decimal("2.00000000"), STANDARD_THRESHOLD))
        self.assertFalse(both_widths_pass(Decimal("1.99999999"), Decimal("1.99999999"), STANDARD_THRESHOLD))
        self.assertEqual(STANDARD_THRESHOLD, Decimal("2.00"))
        result = analyze(compact_high("103", "90"))
        attempt = attempt_of(result, 0, SWING_HIGH)
        self.assertEqual(attempt.status, REFERENCE_OPEN_WIDTH)
        self.assertGreaterEqual(attempt.close_to_extreme_percent, COMPACT_THRESHOLD)
        self.assertLess(attempt.open_to_extreme_percent, COMPACT_THRESHOLD)

    def test_13_high_uses_maximum_interior_high(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "110", "99", "101"),
            bar(2, "101", "105", "99", "108"),
            bar(3, "108", "109", "99", "100"),
        ])
        swing = next(item for item in highs(result) if item.open_row == 0)
        self.assertEqual(swing.extreme_price, Decimal("110"))
        self.assertEqual(swing.extreme_row, 1)

    def test_14_low_uses_minimum_interior_low(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "101", "90", "99"),
            bar(2, "99", "101", "95", "92"),
            bar(3, "92", "101", "91", "100"),
        ])
        swing = next(item for item in lows(result) if item.open_row == 0)
        self.assertEqual(swing.extreme_price, Decimal("90"))
        self.assertEqual(swing.extreme_row, 1)

    def test_15_extreme_need_not_be_central(self) -> None:
        result = analyze(hold_until(6, "110", "100", extreme_row=1))
        swing = next(item for item in highs(result) if item.open_row == 0 and item.detection_method == "REFERENCE_RETURN")
        self.assertEqual((swing.left_bars, swing.right_bars), (1, 5))
        self.assertFalse(swing.symmetric)

    def test_16_extreme_first_interior_accepted(self) -> None:
        result = analyze(hold_until(4, "110", "100", extreme_row=1))
        swing = next(item for item in highs(result) if item.open_row == 0)
        self.assertEqual(swing.extreme_row, swing.open_row + 1)

    def test_17_extreme_last_interior_accepted(self) -> None:
        result = analyze(hold_until(4, "110", "100", extreme_row=3))
        swing = next(item for item in highs(result) if item.open_row == 0)
        self.assertEqual(swing.extreme_row, swing.close_row - 1)

    def test_18_extreme_on_open_rejected(self) -> None:
        self.assertEqual(extreme_position_rejection(0, 0, 2), REJECTED_EXTREME_ON_OPEN)
        result = analyze(compact_high())
        swing = highs(result)[0]
        self.assertGreater(swing.extreme_row, swing.open_row)

    def test_19_extreme_on_close_rejected(self) -> None:
        self.assertEqual(extreme_position_rejection(0, 2, 2), REJECTED_EXTREME_ON_CLOSE)
        result = analyze(compact_high())
        self.assertLess(highs(result)[0].extreme_row, highs(result)[0].close_row)

    def test_20_equal_high_plateau(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "110", "99", "105"),
            bar(2, "105", "110", "99", "106"),
            bar(3, "106", "104", "99", "100"),
        ])
        swing = next(item for item in highs(result) if item.open_row == 0)
        self.assertEqual((swing.extreme_row, swing.plateau_start, swing.plateau_length), (2, 1, 2))

    def test_21_equal_low_plateau(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "101", "90", "95"),
            bar(2, "95", "101", "90", "94"),
            bar(3, "94", "101", "93", "100"),
        ])
        swing = next(item for item in lows(result) if item.open_row == 0)
        self.assertEqual(swing.extreme_row, 2)
        self.assertEqual(swing.plateau_length, 2)

    def test_22_exact_return_confirms_high(self) -> None:
        result = analyze(compact_high("110", "100"))
        self.assertEqual(highs(result)[0].completion_type, EXACT_RETURN)

    def test_23_cross_return_confirms_high(self) -> None:
        result = analyze(compact_high("110", "99"))
        self.assertEqual(highs(result)[0].completion_type, CROSSED_RETURN)

    def test_24_close_above_reference_does_not_confirm_high(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "110", "99", "105"),
            bar(2, "105", "111", "104", "106"),
        ])
        self.assertEqual(attempt_of(result, 0, SWING_HIGH).status, TERMINAL)
        self.assertFalse(any(item.open_row == 0 for item in highs(result)))

    def test_25_exact_return_confirms_low(self) -> None:
        result = analyze(compact_low("90", "100"))
        self.assertEqual(lows(result)[0].completion_type, EXACT_RETURN)

    def test_26_cross_return_confirms_low(self) -> None:
        result = analyze(compact_low("90", "101"))
        self.assertEqual(lows(result)[0].completion_type, CROSSED_RETURN)

    def test_27_close_below_reference_does_not_confirm_low(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "101", "90", "95"),
            bar(2, "95", "96", "89", "94"),
        ])
        self.assertEqual(attempt_of(result, 0, SWING_LOW).status, TERMINAL)

    def test_28_first_return_is_binding(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "101", "99", "101"),
            bar(2, "101", "102", "98", "99"),
            bar(3, "99", "150", "98", "140"),
            bar(4, "140", "151", "99", "100"),
        ])
        attempt = attempt_of(result, 0, SWING_HIGH)
        self.assertEqual(attempt.close_row, 2)
        self.assertEqual(attempt.status, REFERENCE_BOTH_WIDTHS)

    def test_29_failed_return_cannot_be_skipped(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "101", "99", "101"),
            bar(2, "101", "102", "98", "99"),
            bar(3, "99", "150", "98", "140"),
            bar(4, "140", "151", "99", "100"),
        ])
        self.assertFalse(any(item.open_row == 0 and item.direction == SWING_HIGH for item in result.swings))

    def test_30_no_return_expires(self) -> None:
        rows = [bar(0, "100", "101", "99", "100")]
        for row in range(1, 27):
            rows.append(bar(row, "101", "110" if row == 1 else "102", "99.5", "105"))
        result = analyze(rows)
        attempt = attempt_of(result, 0, SWING_HIGH)
        self.assertEqual(attempt.status, EXPIRED)
        self.assertEqual(attempt.interior_count, 25)
        self.assertEqual(attempt.actual_search_interior, 25)
        self.assertTrue(attempt.search_horizon_exhausted)

    def test_31_confirmation_at_4h_candle_close(self) -> None:
        rows = compact_high()
        result = analyze(rows)
        swing = highs(result)[0]
        expected = rows[swing.close_row].open_time + timedelta(hours=4) - timedelta(milliseconds=1)
        self.assertEqual(swing.confirmed_at, expected)
        self.assertNotEqual(swing.confirmed_at, rows[swing.extreme_row].close_time)

    def test_32_no_lookahead(self) -> None:
        base = analyze(compact_high())
        later = analyze(compact_high() + [bar(3, "100", "180", "20", "100")])
        original = highs(base)[0]
        same = next(item for item in highs(later) if item.open_row == 0)
        self.assertEqual((same.close_row, same.extreme_price, same.confirmed_at), (original.close_row, original.extreme_price, original.confirmed_at))

    def test_33_width_achievement_alone_does_not_confirm(self) -> None:
        rows = [bar(0, "100", "101", "99", "100")]
        for row in range(1, 27):
            rows.append(bar(row, "101", "110" if row == 1 else "102", "99.5", "105"))
        result = analyze(rows)
        attempt = attempt_of(result, 0, SWING_HIGH)
        self.assertEqual(attempt.width_achieved_row, 1)
        self.assertEqual(attempt.status, EXPIRED)
        self.assertFalse(any(item.open_row == 0 and item.detection_method == "REFERENCE_RETURN" for item in highs(result)))

    def test_34_directional_independence(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "101", "99", "101"),
            bar(2, "101", "102", "98", "99"),
            bar(3, "99", "101", "96", "97"),
            bar(4, "97", "102", "95", "101"),
        ])
        audit_result(result)
        self.assertEqual(attempt_of(result, 0, SWING_HIGH).status, REFERENCE_BOTH_WIDTHS)
        self.assertEqual(attempt_of(result, 0, SWING_LOW).status, CONFIRMED_COMPACT)

    def test_35_conflict_resolution(self) -> None:
        equal = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "110", "90", "100"),
            bar(2, "100", "101", "99", "100"),
        ])
        self.assertEqual(attempt_of(equal, 0, SWING_HIGH).status, CONFIRMED_COMPACT)
        self.assertEqual(attempt_of(equal, 0, SWING_LOW).status, REJECTED_CONFLICT)
        wider_low = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "104", "90", "100"),
            bar(2, "100", "101", "99", "100"),
        ])
        self.assertEqual(attempt_of(wider_low, 0, SWING_LOW).status, CONFIRMED_COMPACT)
        self.assertIn("LARGER_WIDTH", attempt_of(wider_low, 0, SWING_LOW).conflict_resolution)

    def test_36_overlaps_retained(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "100"),
            bar(1, "100", "110", "99.5", "105"),
            bar(2, "100", "101", "99", "100"),
            bar(3, "100", "110", "99.5", "105"),
            bar(4, "105", "106", "99", "100"),
        ])
        audit_result(result)
        owned = [item for item in highs(result) if item.open_row in (0, 2)]
        self.assertEqual(len(owned), 2)
        self.assertTrue(all(item.overlap_count > 0 for item in owned))

    def test_37_duplicate_keys_prohibited(self) -> None:
        result = analyze(compact_high())
        audit_result(result)
        keys = [(item.direction, item.open_row, item.extreme_row, item.close_row) for item in result.swings]
        self.assertEqual(len(keys), len(set(keys)))

    def test_38_ids_are_deterministic(self) -> None:
        first = analyze(compact_high())
        second = analyze(compact_high())
        self.assertEqual([item.swing_id for item in first.swings], [item.swing_id for item in second.swings])
        self.assertTrue(first.swings[0].swing_id.startswith("RAW-SH-"))
        self.assertTrue(first.primaries[0].primary_id.startswith("PRI-SWH4H-"))
        self.assertTrue(first.attempts[0].attempt_id.startswith("ATT4H-H-"))

    def test_39_utc_and_turkey_conversion(self) -> None:
        self.assertEqual(iso_turkey(ANALYSIS_START), "2026-01-01T03:00:00+03:00")
        self.assertEqual(iso_turkey(ANALYSIS_END_OPEN), "2026-09-15T23:00:00+03:00")
        self.assertEqual(iso_turkey(ANALYSIS_END_CLOSE), "2026-09-16T02:59:59.999+03:00")
        self.assertEqual(iso_utc(ANALYSIS_END_CLOSE), "2026-09-15T23:59:59.999Z")

    def test_40_only_2026_rows_used(self) -> None:
        loaded = load_production()
        self.assertTrue(all(item.open_time.year == 2026 for item in loaded["bars"]))
        self.assertTrue(all("year=2026" in path.replace("\\", "/").lower() for path in loaded["source_paths"]))
        self.assertFalse(any("year=2024" in path or "year=2025" in path for path in loaded["source_paths"]))
        result = analyze(compact_high())
        self.assertTrue(all(item.open_time.year == 2026 for item in result.swings))

    def test_41_pre_2026_context_rejected(self) -> None:
        early = bar(0, "100", "110", "90", "100")
        early.open_time = datetime(2025, 12, 31, 20, tzinfo=timezone.utc)
        early.close_time = early.open_time + timedelta(hours=4) - timedelta(milliseconds=1)
        with self.assertRaises(SwingError):
            analyze([early, bar(1, "100", "110", "90", "100")])

    def test_42_post_september_15_rejected(self) -> None:
        late = bar(0, "100", "110", "90", "100")
        late.open_time = datetime(2026, 9, 16, tzinfo=timezone.utc)
        late.close_time = late.open_time + timedelta(hours=4) - timedelta(milliseconds=1)
        with self.assertRaises(SwingError):
            analyze([late])

    def test_43_expected_row_count(self) -> None:
        loaded = load_production()
        self.assertEqual(len(loaded["bars"]), EXPECTED_ROWS)
        self.assertEqual(loaded["bars"][0].open_time, ANALYSIS_START)
        self.assertEqual(loaded["bars"][-1].open_time, ANALYSIS_END_OPEN)
        self.assertEqual(loaded["bars"][-1].close_time, ANALYSIS_END_CLOSE)

    def test_44_monthly_counts_reconcile(self) -> None:
        loaded = load_production()
        self.assertEqual(sum(EXPECTED_MONTHLY.values()), 1548)
        self.assertEqual(sum(loaded["monthly_counts"].values()), 1548)
        self.assertEqual(loaded["monthly_counts"]["2026-01"], 186)
        self.assertEqual(loaded["monthly_counts"]["2026-02"], 168)
        self.assertEqual(loaded["monthly_counts"]["2026-09"], 90)

    def test_45_forward_censoring(self) -> None:
        result = analyze(compact_high())
        swing = highs(result)[0]
        self.assertTrue(swing.forward_censored)
        self.assertEqual(swing.forward_censor_reason, "DATASET_END")

    def test_46_candidate_audit_maximum(self) -> None:
        self.assertEqual(EXPECTED_ROWS * 4, 6192)
        result = analyze(compact_high())
        self.assertLessEqual(len(result.attempts), 6192)
        self.assertEqual(len(result.attempts), len(result.bars) * 4)

    def test_47_dynamic_revision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "BTCUSDT_4H_Swing_Structure_rev00.xlsx").write_bytes(b"a")
            (root / "BTCUSDT_4H_Swing_Structure_rev02.xlsx").write_bytes(b"b")
            (root / "BTCUSDT_1H_Swing_Structure_rev09.xlsx").write_bytes(b"c")
            (root / "BTCUSDT_4H_Order_Blocks_rev00.xlsx").write_bytes(b"d")
            self.assertEqual(scan_revisions(root), [0, 2])
            self.assertEqual(next_revision(scan_revisions(root)), 3)

    def test_48_existing_workbook_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / revision_filename(0)
            target.write_bytes(b"keep")
            planned = root / revision_filename(next_revision(scan_revisions(root)))
            self.assertNotEqual(planned, target)
            self.assertEqual(target.read_bytes(), b"keep")

    def test_49_genuine_xlsx(self) -> None:
        self.assertEqual(self.sample_path.read_bytes()[:2], b"PK")
        self.assertTrue(zipfile.is_zipfile(self.sample_path))
        self.assertIsNone(zipfile.ZipFile(self.sample_path).testzip())

    def test_50_ooxml_validation(self) -> None:
        names = set(zipfile.ZipFile(self.sample_path).namelist())
        for member in ("[Content_Types].xml", "xl/workbook.xml", "xl/styles.xml", "xl/theme/theme1.xml"):
            self.assertIn(member, names)

    def test_51_normal_reopen(self) -> None:
        workbook = load_workbook(self.sample_path, read_only=False, data_only=False, keep_links=False)
        try:
            self.assertEqual(tuple(workbook.sheetnames), WORKBOOK_SHEETS)
            self.assertIsNone(workbook.vba_archive)
        finally:
            workbook.close()

    def test_52_streaming_reopen(self) -> None:
        workbook = load_workbook(self.sample_path, read_only=True, data_only=True)
        try:
            count = sum(1 for worksheet in workbook.worksheets for _row in worksheet.iter_rows())
            self.assertGreater(count, 13)
        finally:
            workbook.close()

    def test_53_thirteen_sheets(self) -> None:
        workbook = load_workbook(self.sample_path, read_only=True)
        try:
            self.assertEqual(workbook.sheetnames, list(WORKBOOK_SHEETS))
        finally:
            workbook.close()

    def test_54_large_sheets_use_plain_ranges(self) -> None:
        workbook = load_workbook(self.sample_path, read_only=False, data_only=False)
        try:
            for name in ("Formation Candles", "Candidate Audit", "Derived Same Extreme"):
                self.assertEqual(list(workbook[name].tables), [])
                self.assertIsNotNone(workbook[name].auto_filter.ref)
            self.assertEqual(workbook["Candidate Audit"].max_row - 1, 24)
            self.assertEqual(workbook["Alternative Primary"]["A2"].value, "NO_RESULTS")
            self.assertEqual(workbook["Standard Primary"]["A2"].value, "NO_RESULTS")
            headers = [cell.value for cell in workbook["Primary Swings"][1]]
            self.assertNotEqual(
                workbook["Primary Swings"].cell(2, headers.index("Swing Open UTC") + 1).value,
                workbook["Primary Swings"].cell(2, headers.index("Confirmed At UTC") + 1).value,
            )
        finally:
            workbook.close()

    def test_55_no_macros(self) -> None:
        self.assertNotIn("vba", " ".join(zipfile.ZipFile(self.sample_path).namelist()).lower())

    def test_56_no_external_links(self) -> None:
        self.assertNotIn("externalLink", " ".join(zipfile.ZipFile(self.sample_path).namelist()))

    def test_57_final_file_reopens(self) -> None:
        workbook = load_workbook(self.sample_path, read_only=False, data_only=False, keep_links=False)
        try:
            headers = [cell.value for cell in workbook["Parameters"][1]]
            name_index = headers.index("Parameter Name")
            value_index = headers.index("Effective Value")
            parameters = {row[name_index].value: row[value_index].value for row in workbook["Parameters"].iter_rows(min_row=2)}
            self.assertEqual(parameters["timeframe"], "4h")
            self.assertEqual(parameters["detector_version"], DETECTOR_VERSION)
            self.assertEqual(parameters["pre_2026_context_allowed"], "FALSE")
            self.assertEqual(parameters["expected_analysis_rows"], "1548")
            self.assertEqual(parameters["compact_maximum_interior_candles"], "6")
            self.assertEqual(parameters["standard_minimum_interior_candles"], "8")
            self.assertEqual(parameters["standard_minimum_width_percent"], "2.00")
            self.assertEqual(parameters["standard_boundary_pass"], "2.00")
            self.assertEqual(parameters["maximum_search_interior_candles"], "25")
            self.assertEqual(parameters["compact_minimum_width_percent"], "3.50")
            self.assertEqual(parameters["alternative_minimum_width_percent"], "3.50")
            self.assertEqual(parameters["duration_gap_interior_candles"], "7")
            self.assertEqual(parameters["duration_gap_rejection"], "REJECTED_DURATION_GAP_7_INTERIOR_BARS")
            self.assertEqual(parameters["primary_selection_first_rule"], "MINIMUM_TOTAL_FORMATION_CANDLES")
            self.assertEqual(parameters["distant_equal_prices_same_family"], "FALSE")
        finally:
            workbook.close()

    def test_58_source_files_unchanged(self) -> None:
        year = SOURCE_DATASET / "year=2026"
        before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in year.rglob("*.parquet")}
        analyze(compact_high())
        after = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in year.rglob("*.parquet")}
        self.assertEqual(before, after)
        self.assertGreater(len(before), 0)

    def test_59_existing_detectors_unchanged(self) -> None:
        roots = [
            PROJECT_ROOT / "detectors" / "swing_open_close_v1",
            PROJECT_ROOT / "detectors" / "order_block_4h_v1",
            PROJECT_ROOT / "detectors" / "open_liquidity_v1",
        ]
        before = _tree_hash(roots)
        analyze(compact_high())
        self.assertEqual(_tree_hash(roots), before)

    def test_60_no_separate_project(self) -> None:
        self.assertEqual(CONFIG.package_dir.parent.parent, PROJECT_ROOT)
        self.assertTrue(CONFIG.package_dir.name == "swing_open_close_4h_with_1h_v1")

    def test_61_no_python_file_outside_project(self) -> None:
        for path in CONFIG.package_dir.rglob("*.py"):
            self.assertTrue(str(path.resolve()).startswith(str(PROJECT_ROOT.resolve())))
        self.assertFalse((CONFIG.desktop_dir / "run_swing_open_close_4h_detector.py").exists())

    def test_62_two_run_determinism(self) -> None:
        first = analyze(hold_until(7, "102", "100"))
        second = analyze(hold_until(7, "102", "100"))
        self.assertEqual(
            [(item.swing_id, item.open_row, item.close_row, str(item.extreme_price)) for item in first.swings],
            [(item.swing_id, item.open_row, item.close_row, str(item.extreme_price)) for item in second.swings],
        )
        self.assertEqual([item.status for item in first.attempts], [item.status for item in second.attempts])

    def test_63_same_extreme_family_and_shortest_primary(self) -> None:
        result = analyze(_shared_extreme())
        audit_result(result)
        family = next(item for item in result.families if item.extreme_price == Decimal("120"))
        self.assertGreater(family.primary.family_size, 1)
        self.assertEqual(len([item for item in family.members if item.role == PRIMARY]), 1)
        self.assertEqual(family.primary.total_candles, min(item.total_candles for item in family.members))
        self.assertEqual(family.primary.formation_class, "COMPACT")
        self.assertTrue(any(item.formation_class == "STANDARD" and item.role == DERIVED for item in family.members))
        self.assertTrue(all(item.direction == SWING_HIGH for item in family.members))
        self.assertEqual(len(result.swings), len(result.primaries) + len(result.derived))
        self.assertTrue(all(item.role == PRIMARY for item in result.primaries))
        self.assertNotIn(family.primary.swing_id, {item.swing_id for item in result.derived})

    def test_64_distant_equal_prices_stay_separate(self) -> None:
        result = analyze(_distant_equal_highs())
        audit_result(result)
        families = [item for item in result.families if item.extreme_price == Decimal("110")]
        self.assertGreaterEqual(len(families), 2)
        self.assertEqual(len({item.event_id for item in families}), len(families))

    def test_65_display_rounding_does_not_merge_prices(self) -> None:
        result = analyze(_near_but_unequal_highs())
        prices = {item.extreme_price for item in result.swings}
        self.assertIn(Decimal("110"), prices)
        self.assertIn(Decimal("110.004"), prices)
        self.assertEqual(len([item for item in result.families if item.extreme_price in {Decimal("110"), Decimal("110.004")}]), 2)

    def test_66_exact_decimal_plateau_is_one_event(self) -> None:
        result = analyze([
            bar(0, "100", "101", "99", "101"),
            bar(1, "101", "120.000", "99", "110"),
            bar(2, "110", "120", "99", "112"),
            bar(3, "112", "113", "98", "100"),
        ])
        audit_result(result)
        grouped = [item for item in result.swings if item.extreme_price == Decimal("120")]
        self.assertGreaterEqual(len(grouped), 1)
        self.assertEqual(len({item.extreme_event_id for item in grouped}), 1)

    def test_67_completion_error_tie_break(self) -> None:
        result = analyze(_equal_length_error_pair())
        audit_result(result)
        family = next(item for item in result.families if item.extreme_price == Decimal("120") and item.primary.family_size > 1)
        self.assertEqual(family.selection_reason, "MINIMUM_ABSOLUTE_COMPLETION_ERROR_PERCENT")
        self.assertEqual(family.primary.close_price, family.primary.reference)
        self.assertTrue(any(item.derivation_reason == "EQUAL_MINIMUM_LENGTH_TIE_BREAK_LOSS" for item in family.members))

    def test_68_width_tie_break(self) -> None:
        result = analyze(_equal_error_width_pair())
        audit_result(result)
        family = next(item for item in result.families if item.extreme_price == Decimal("120") and item.primary.family_size > 1)
        self.assertEqual(family.selection_reason, "MAXIMUM_MINIMUM_OF_TWO_WIDTHS")
        self.assertEqual(family.primary.reference, Decimal("90"))

    def test_69_later_tie_breaks_use_sort_key(self) -> None:
        early = datetime(2026, 1, 2, tzinfo=timezone.utc)
        later = datetime(2026, 1, 3, tzinfo=timezone.utc)
        earlier_confirm = _key_swing("RAW-SWH4H-000002", 4, "0", "10", early, datetime(2026, 1, 1, tzinfo=timezone.utc))
        later_confirm = _key_swing("RAW-SWH4H-000001", 4, "0", "10", later, datetime(2026, 1, 1, tzinfo=timezone.utc))
        self.assertLess(primary_sort_key(earlier_confirm), primary_sort_key(later_confirm))
        earlier_open = _key_swing("RAW-SWH4H-000002", 4, "0", "10", early, datetime(2026, 1, 1, tzinfo=timezone.utc))
        later_open = _key_swing("RAW-SWH4H-000001", 4, "0", "10", early, datetime(2026, 1, 2, tzinfo=timezone.utc))
        self.assertLess(primary_sort_key(later_open), primary_sort_key(earlier_open))
        low_id = _key_swing("RAW-SWH4H-000001", 4, "0", "10", early, early)
        high_id = _key_swing("RAW-SWH4H-000002", 4, "0", "10", early, early)
        self.assertLess(primary_sort_key(low_id), primary_sort_key(high_id))

    def test_70_quality_cannot_override_shortest(self) -> None:
        result = analyze(_shared_extreme())
        family = next(item for item in result.families if item.extreme_price == Decimal("120"))
        shortest = min(family.members, key=lambda item: item.total_candles)
        self.assertEqual(family.primary.swing_id, shortest.swing_id)
        self.assertNotIn("QUALITY", family.selection_reason)
        longer = max(family.members, key=lambda item: (item.total_candles, item.quality_score))
        longer.quality_score = family.primary.quality_score + Decimal("50")
        self.assertEqual(min(family.members, key=primary_sort_key).swing_id, family.primary.swing_id)

    def test_71_principal_outputs_exclude_derived(self) -> None:
        result = analyze(_shared_extreme())
        audit_result(result)
        derived_ids = {item.swing_id for item in result.derived}
        self.assertTrue(derived_ids)
        self.assertTrue(derived_ids.isdisjoint({item.swing_id for item in result.primaries}))
        self.assertTrue(all(item.structure_label == "" for item in result.derived))
        self.assertTrue(all(item.structure_label for item in result.primaries))
        self.assertTrue(all(item.next_same_id is None or item.next_same_id.startswith("PRI-") for item in result.primaries))
        self.assertTrue(all(item.overlap_count == 0 for item in result.derived))
        path = UNIT_DIR / "primary_sample.xlsx"
        context = {
            "provenance": {"rows": len(result.bars), "first_open_utc": "", "last_open_utc": "", "last_close_utc": ""},
            "monthly_counts": {},
            "diagnostic_rows": [["Section", "Name", "Value"], ["sample", "rows", len(result.bars)]],
        }
        write_workbook(path, result, context, "Primary sample")
        workbook = load_workbook(path, read_only=False, data_only=False)
        try:
            primary_raw_ids = {row[1].value for row in workbook["Primary Swings"].iter_rows(min_row=2)}
            derived_sheet = {row[0].value for row in workbook["Derived Same Extreme"].iter_rows(min_row=2)}
            formation_ids = {row[1].value for row in workbook["Formation Candles"].iter_rows(min_row=2)}
            self.assertTrue(primary_raw_ids.isdisjoint(derived_ids))
            self.assertTrue(derived_ids.issubset(derived_sheet))
            self.assertTrue(derived_ids.issubset(formation_ids))
            self.assertEqual(list(workbook["Derived Same Extreme"].tables), [])
        finally:
            workbook.close()

    def test_72_different_types_do_not_share_a_family(self) -> None:
        result = analyze(compact_high("110", "100") + [bar(3, "100", "101", "90", "100")])
        # The appended bar keeps its own row from bar(); compact_high uses 0-2 and this uses row 3.
        directions = {item.direction for item in result.families}
        for family in result.families:
            self.assertEqual(len({member.direction for member in family.members}), 1)
        self.assertTrue(directions)

    def test_73_primary_ids_are_deterministic(self) -> None:
        first = analyze(_shared_extreme())
        second = analyze(_shared_extreme())
        self.assertEqual([item.primary_id for item in first.primaries], [item.primary_id for item in second.primaries])
        self.assertEqual([item.family_id for item in first.families], [item.family_id for item in second.families])
        self.assertEqual([item.swing_id for item in first.derived], [item.swing_id for item in second.derived])

    def test_74_seven_interior_gap_cannot_be_skipped(self) -> None:
        rows = [bar(0, "100", "101", "99", "101")]
        for row in range(1, 8):
            rows.append(bar(row, "101", "120" if row == 4 else "101.2", "99", "101"))
        rows.append(bar(8, "101", "121", "98", "100"))
        rows.append(bar(9, "100", "122", "98", "90"))
        result = analyze(rows)
        attempt = attempt_of(result, 0, SWING_HIGH)
        self.assertEqual(attempt.interior_count, 7)
        self.assertEqual(attempt.status, REJECTED_DURATION_GAP)
        self.assertEqual(attempt.formation_class, "INVALID_DURATION_GAP")
        self.assertEqual(attempt.applicable_minimum_width, "NOT_APPLICABLE")
        self.assertFalse(any(
            item.open_row == 0 and item.direction == SWING_HIGH and item.detection_method == "REFERENCE_RETURN"
            for item in result.swings
        ))

    def test_75_applicable_width_tracks_duration_class(self) -> None:
        compact = analyze(compact_high())
        standard = analyze(hold_until(9, "102.5", "100"))
        self.assertEqual(attempt_of(compact, 0, SWING_HIGH).applicable_minimum_width, "3.50000000")
        self.assertEqual(attempt_of(standard, 0, SWING_HIGH).applicable_minimum_width, "2.00000000")
        self.assertEqual(attempt_of(standard, 0, SWING_HIGH).applicable_rule_id, "STANDARD_8_TO_20_WIDTH_2_00")
        self.assertTrue(highs(compact)[0].both_widths_passed)

    def test_76_parameter_registry_matches_code_and_workbook(self) -> None:
        from detectors.swing_open_close_4h_with_1h_v1.swing_config import assert_parameter_registry, parameter_registry

        assert_parameter_registry()
        engine_source = (CONFIG.package_dir / "engine.py").read_text(encoding="utf-8")
        config_source = (CONFIG.package_dir / "swing_config.py").read_text(encoding="utf-8")
        self.assertIn('STANDARD_WIDTH = "2.00"', config_source)
        self.assertNotIn('STANDARD_WIDTH = "2.50"', config_source)
        self.assertNotIn("REJECTED_WIDTH_BELOW_2_00", engine_source)
        self.assertNotIn('Decimal("8.00")', engine_source)
        self.assertNotIn('Decimal("6.50")', engine_source)
        workbook = load_workbook(self.sample_path, read_only=True, data_only=True)
        try:
            rows = list(workbook["Parameters"].iter_rows(min_row=2, values_only=True))
            self.assertEqual([row[1] for row in rows], [item.name for item in parameter_registry()])
            self.assertEqual([row[2] for row in rows], [item.value for item in parameter_registry()])
            self.assertGreaterEqual(len(rows), 40)
            self.assertEqual(len(WORKBOOK_SHEETS), 15)
            self.assertIn("alternative_method_enabled", [row[1] for row in rows])
            self.assertIn("Alternative Primary", workbook.sheetnames)
        finally:
            workbook.close()

    def test_77_alternative_high_and_low_pass(self) -> None:
        high = analyze(_alternative_high(4))
        low = analyze(_alternative_low(4))
        high_swing = next(item for item in highs(high) if item.detection_method == ALTERNATIVE_METHOD)
        low_swing = next(item for item in lows(low) if item.detection_method == ALTERNATIVE_METHOD)
        self.assertEqual(high_swing.interior_count, 4)
        self.assertEqual(low_swing.interior_count, 4)
        self.assertTrue(high_swing.found_through_alternative)
        self.assertFalse(high_swing.reference_return_achieved)
        self.assertGreater(high_swing.close_price, high_swing.reference)
        self.assertLess(low_swing.close_price, low_swing.reference)
        self.assertGreaterEqual(high_swing.open_to_extreme_percent, COMPACT_THRESHOLD)
        self.assertGreaterEqual(high_swing.close_to_extreme_percent, COMPACT_THRESHOLD)
        audit_result(high)
        audit_result(low)

    def test_78_returned_close_is_not_alternative(self) -> None:
        high = analyze(compact_high("110", "99"))
        low = analyze(compact_low("90", "101"))
        self.assertFalse(any(item.detection_method == ALTERNATIVE_METHOD for item in high.swings))
        self.assertFalse(any(item.detection_method == ALTERNATIVE_METHOD for item in low.swings))
        self.assertTrue(all(not item.found_through_alternative for item in high.swings + low.swings))

    def test_79_alternative_duration_boundaries(self) -> None:
        self.assertIsNone(alternative_duration_rejection(4))
        self.assertIsNone(alternative_duration_rejection(10))
        self.assertEqual(alternative_duration_rejection(3), ALTERNATIVE_DURATION_BELOW)
        self.assertEqual(alternative_duration_rejection(11), ALTERNATIVE_DURATION_ABOVE)
        four = next(item for item in analyze(_alternative_high(4)).swings if item.detection_method == ALTERNATIVE_METHOD)
        ten = next(item for item in analyze(_alternative_high(10)).swings if item.open_row == 0 and item.detection_method == ALTERNATIVE_METHOD)
        self.assertEqual((four.interior_count, ten.interior_count), (4, 10))
        short = analyze(_alternative_high(3))
        self.assertEqual(_alt(short, 0, SWING_HIGH).status, TERMINAL)
        long = analyze(_alternative_high(11))
        self.assertEqual(_alt(long, 0, SWING_HIGH).status, ALTERNATIVE_NONE)
        self.assertFalse(any(item.detection_method == ALTERNATIVE_METHOD and item.open_row == 0 for item in long.swings))

    def test_80_seven_interiors_pass_only_on_alternative(self) -> None:
        result = analyze(_alternative_high(7))
        swing = next(item for item in highs(result) if item.open_row == 0 and item.detection_method == ALTERNATIVE_METHOD)
        self.assertEqual(swing.interior_count, 7)
        self.assertEqual(_alt(result, 0, SWING_HIGH).status, CONFIRMED_ALTERNATIVE)
        gap = analyze(hold_until(8, "110", "100", extreme_row=7))
        self.assertEqual(attempt_of(gap, 0, SWING_HIGH).status, REJECTED_DURATION_GAP)
        self.assertFalse(any(item.open_row == 0 and item.detection_method == ALTERNATIVE_METHOD for item in gap.swings))

    def test_81_alternative_widths_are_unrounded_and_joined(self) -> None:
        exact_close = Decimal("100.5")
        exact_extreme = exact_close * Decimal("1.035")
        exact = analyze(_alternative_width_close(exact_extreme, exact_close))
        swing = next(item for item in highs(exact) if item.detection_method == ALTERNATIVE_METHOD and item.open_row == 0)
        self.assertEqual(swing.close_to_extreme_percent, Decimal("3.5"))
        below_extreme = exact_close * Decimal("1.03499999")
        below = analyze(_alternative_width_close(below_extreme, exact_close))
        self.assertFalse(any(item.open_row == 0 and item.detection_method == ALTERNATIVE_METHOD for item in below.swings))
        self.assertFalse(both_widths_pass(Decimal("3.50"), Decimal("3.49999999"), COMPACT_THRESHOLD))

    def test_82_first_qualifying_alternative_close_is_binding(self) -> None:
        result = analyze(_binding_alternative())
        swing = next(item for item in highs(result) if item.open_row == 0 and item.detection_method == ALTERNATIVE_METHOD)
        self.assertEqual(swing.close_row, 6)
        self.assertEqual(swing.confirmed_at, result.bars[6].close_time)
        self.assertLess(swing.extreme_price, Decimal("140"))
        later_rows = _binding_alternative()
        later_rows.extend(bar(row, "130", "131", "109", "120") for row in range(8, 12))
        later_rows.append(bar(12, "120", "121", "90", "100"))
        later = analyze(later_rows)
        kept = next(item for item in highs(later) if item.open_row == 0 and item.detection_method == ALTERNATIVE_METHOD)
        self.assertEqual(kept.close_row, 6)
        self.assertEqual(kept.later_reference_return_row, 12)

    def test_83_methods_are_mutually_exclusive_and_cross_method_primary_is_shortest(self) -> None:
        result = analyze(_cross_method_family())
        audit_result(result)
        keys = [(item.direction, item.open_row, item.extreme_row, item.close_row) for item in result.swings]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertTrue(all(item.found_through_alternative for item in result.swings if item.detection_method == ALTERNATIVE_METHOD))
        self.assertTrue(all(not item.found_through_alternative and item.reference_return_achieved for item in result.swings if item.detection_method == "REFERENCE_RETURN"))
        family = next(item for item in result.families if item.cross_method)
        self.assertEqual(family.primary.detection_method, ALTERNATIVE_METHOD)
        self.assertLess(family.primary.total_candles, max(member.total_candles for member in family.members))
        self.assertEqual(sum(1 for item in family.members if item.role == PRIMARY), 1)
        self.assertEqual(len(result.swings), len(result.primaries) + len(result.derived))

    def test_84_search_horizon_is_25_and_does_not_expand_valid_durations(self) -> None:
        self.assertEqual(MAX_INTERIOR, 25)
        self.assertIsNone(duration_rejection(20))
        self.assertEqual(duration_rejection(21), REFERENCE_ABOVE_20)
        self.assertEqual(duration_rejection(25), REFERENCE_ABOVE_20)
        self.assertEqual(duration_rejection(26), REFERENCE_ABOVE_20)
        reached = analyze(_flat_until(26))
        reference = attempt_of(reached, 0, SWING_HIGH)
        self.assertEqual(reference.actual_search_interior, 25)
        self.assertEqual(reference.status, EXPIRED)
        self.assertTrue(reference.search_horizon_exhausted)
        self.assertEqual(reference.evaluations_beyond_horizon, 0)
        hidden = analyze(_return_at(27))
        hidden_reference = attempt_of(hidden, 0, SWING_HIGH)
        self.assertEqual(hidden_reference.actual_search_interior, 25)
        self.assertFalse(hidden_reference.reference_return_achieved)
        self.assertNotEqual(hidden_reference.close_row, 27)
        seen = analyze(_return_at(26))
        seen_reference = attempt_of(seen, 0, SWING_HIGH)
        self.assertEqual(seen_reference.interior_count, 25)
        self.assertEqual(seen_reference.status, REFERENCE_ABOVE_20)
        self.assertTrue(seen_reference.reference_binding_found)
        self.assertFalse(any(item.open_row == 0 and item.detection_method == "REFERENCE_RETURN" for item in seen.swings))
        censored = analyze(_flat_until(3))
        censored_reference = attempt_of(censored, 0, SWING_HIGH)
        self.assertTrue(censored_reference.search_censored)
        self.assertLess(censored_reference.maximum_observable_interior, 25)
        self.assertEqual(censored_reference.status, TERMINAL)
        binding = attempt_of(analyze(compact_high()), 0, SWING_HIGH)
        self.assertEqual(binding.actual_search_interior, 1)
        self.assertTrue(binding.reference_binding_found)
        self.assertFalse(binding.search_horizon_exhausted)
        alternative = _alt(analyze(_alternative_high(10)), 0, SWING_HIGH)
        self.assertEqual(alternative.interior_count, 10)
        self.assertLessEqual(alternative.actual_search_interior, 10)
        blocked = _alt(analyze(_alternative_high(11)), 0, SWING_HIGH)
        self.assertEqual(blocked.status, ALTERNATIVE_NONE)
        self.assertEqual(blocked.actual_search_interior, 10)
        self.assertTrue(blocked.alternative_window_exhausted)
        self.assertFalse(any(item.actual_search_interior > 25 or item.evaluations_beyond_horizon for item in reached.attempts))

    def test_85_standard_two_percent_boundary(self) -> None:
        low_rows = [bar(0, "100", "101", "99", "100")]
        for row in range(1, 9):
            low_rows.append(bar(row, "99", "100", "98" if row == 1 else "98.8", "99"))
        low_rows.append(bar(9, "99", "101", "98.5", "100"))
        low_result = analyze(low_rows)
        low_swing = next(item for item in lows(low_result) if item.open_row == 0 and item.detection_method == "REFERENCE_RETURN")
        self.assertEqual((low_swing.interior_count, low_swing.open_to_extreme_percent, low_swing.close_to_extreme_percent), (8, Decimal("2"), Decimal("2")))
        band = next(item for item in highs(analyze(hold_until(9, "102.25", "100"))) if item.open_row == 0 and item.detection_method == "REFERENCE_RETURN")
        self.assertEqual(band.formation_class, "STANDARD")
        self.assertGreater(band.open_to_extreme_percent, Decimal("2"))
        self.assertLess(band.open_to_extreme_percent, Decimal("2.5"))
        opened = attempt_of(analyze(hold_until(9, "101.5", "99")), 0, SWING_HIGH)
        self.assertEqual(opened.status, REFERENCE_OPEN_WIDTH)
        self.assertLess(opened.open_to_extreme_percent, STANDARD_THRESHOLD)
        self.assertGreaterEqual(opened.close_to_extreme_percent, STANDARD_THRESHOLD)
        self.assertEqual(opened.interior_count, 8)


def _flat_until(last_row: int) -> list[Bar]:
    rows = [bar(0, "100", "101", "99", "101")]
    for row in range(1, last_row + 1):
        rows.append(bar(row, "101", "102", "100.5", "101"))
    return rows


def _return_at(close_row: int) -> list[Bar]:
    rows = _flat_until(close_row - 1)
    rows.append(bar(close_row, "101", "102", "90", "100"))
    return rows


def _alt(result, open_row: int, direction: str):
    return next(item for item in result.attempts if item.open_row == open_row and item.direction == direction and item.detection_method == ALTERNATIVE_METHOD)


def _alternative_high(interior: int) -> list[Bar]:
    close_row = interior + 1
    rows = [bar(0, "100", "101", "99", "101")]
    for row in range(1, close_row):
        high = "110" if row == close_row - 1 else "101.2"
        rows.append(bar(row, "101", high, "100.4", "101"))
    rows.append(bar(close_row, "101", "107", "100.4", "105"))
    return rows


def _alternative_low(interior: int) -> list[Bar]:
    close_row = interior + 1
    rows = [bar(0, "100", "101", "99", "99")]
    for row in range(1, close_row):
        low = "90" if row == close_row - 1 else "98.8"
        rows.append(bar(row, "99", "99.5", low, "99"))
    rows.append(bar(close_row, "99", "99.5", "94", "95"))
    return rows


def _alternative_width_close(extreme: Decimal, close: Decimal) -> list[Bar]:
    rows = [bar(0, "100", "101", "99", "101")]
    for row in range(1, 4):
        rows.append(bar(row, "101", "101.2", "100.4", "101"))
    rows.append(bar(4, "101", format(extreme, "f"), "100.4", "101"))
    rows.append(bar(5, "101", format(max(extreme, close), "f"), "100.4", format(close, "f")))
    return rows


def _binding_alternative() -> list[Bar]:
    rows = [bar(0, "100", "101", "99", "101")]
    for row in range(1, 5):
        rows.append(bar(row, "101", "101.2", "100.4", "101"))
    rows.append(bar(5, "101", "120", "100.4", "115"))
    rows.append(bar(6, "115", "116", "109", "110"))
    rows.append(bar(7, "110", "140", "109", "130"))
    return rows


def _cross_method_family() -> list[Bar]:
    rows = [bar(0, "100", "101", "99", "101")]
    rows.append(bar(1, "101", "120", "100.4", "110"))
    for row in range(2, 12):
        rows.append(bar(row, "110", "112", "105", "110"))
    rows.append(bar(12, "110", "112", "98", "100"))
    return rows


def _shared_extreme() -> list[Bar]:
    rows = [bar(row, "100", "101.2", "99", "101") for row in range(8)]
    rows.append(bar(8, "101", "120", "99", "110"))
    rows.append(bar(9, "110", "111", "98", "100"))
    return rows


def _distant_equal_highs() -> list[Bar]:
    return [
        bar(0, "100", "101", "99", "101"),
        bar(1, "101", "110", "99", "105"),
        bar(2, "105", "106", "98", "100"),
        bar(3, "100", "101", "99", "101"),
        bar(4, "101", "110", "99", "105"),
        bar(5, "105", "106", "98", "100"),
    ]


def _near_but_unequal_highs() -> list[Bar]:
    return [
        bar(0, "100", "101", "99", "101"),
        bar(1, "101", "110", "99", "105"),
        bar(2, "105", "106", "98", "100"),
        bar(3, "100", "101", "99", "101"),
        bar(4, "101", "110.004", "99", "105"),
        bar(5, "105", "106", "98", "100"),
    ]


def _equal_length_error_pair() -> list[Bar]:
    return [
        bar(0, "100", "101", "99", "101"),
        bar(1, "90", "103", "89", "102"),
        bar(2, "102", "120", "99", "110"),
        bar(3, "110", "111", "94", "95"),
        bar(4, "95", "96", "88", "90"),
    ]


def _equal_error_width_pair() -> list[Bar]:
    return [
        bar(0, "100", "101", "99", "101"),
        bar(1, "90", "103", "89", "102"),
        bar(2, "102", "120", "99", "110"),
        bar(3, "110", "111", "94", "98"),
        bar(4, "98", "99", "88", "88.2"),
    ]


def _key_swing(swing_id: str, candles: int, error: str, width: str, confirmed, opened) -> SimpleNamespace:
    return SimpleNamespace(
        total_candles=candles,
        completion_difference_percent=Decimal(error),
        open_to_extreme_percent=Decimal(width),
        close_to_extreme_percent=Decimal(width),
        confirmed_at=confirmed,
        open_time=opened,
        swing_id=swing_id,
    )


def _tree_hash(roots: list[Path]) -> dict[str, str]:
    found = {}
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                found[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return found


if __name__ == "__main__":
    unittest.main()
