"""Permanent tests for the asymmetric hierarchical swing detector."""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]
PROJECT = PACKAGE.parents[1]
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

from detectors.hierarchical_swing_v4.engine import (
    Bar,
    InternalEngine,
    MajorEngine,
    Record,
    _BARS_FOR_PROMINENCE,
    allowed_central_positions,
    assign_causal_dispositions,
    classify_formation,
    enumerate_formations,
    iso_turkey,
    iso_utc,
    make_bars,
    run_detection,
)
from detectors.hierarchical_swing_v4.run_hierarchical_swing_detector import (
    assert_not_overwrite,
    assert_python_write_allowed,
    existing_revisions,
    load_production_bars,
    next_revision,
    revision_filename,
    source_partition_paths,
)
from detectors.hierarchical_swing_v4.swing_config import CONFIG, PACKAGE_DIR, PROJECT_ROOT
from detectors.hierarchical_swing_v4.workbook import validate_saved_workbook, write_workbook
from data.catalog import fingerprint_file

UTC = timezone.utc
D = Decimal


def _rows(count: int, price: str = "100") -> list[list[Decimal]]:
    base = D(price)
    return [[base, base + D("0.1"), base - D("0.1"), base] for _ in range(count)]


def _install_high(rows, start: int, length: int, extreme_pos: int, ref: Decimal, extreme: Decimal) -> None:
    lift = extreme - ref
    other = ref + (lift / D(4) if lift > 0 else D("0.01"))
    for offset in range(length):
        pos = offset + 1
        if pos == 1:
            rows[start + offset] = [ref, other, ref - D("0.05"), ref]
        elif pos == extreme_pos:
            rows[start + offset] = [ref, extreme, ref, extreme - D("0.01")]
        elif pos == length:
            rows[start + offset] = [ref, extreme - D("0.02"), ref - D("0.01"), ref]
        else:
            rows[start + offset] = [ref, other, ref - D("0.02"), ref]


def _install_low(rows, start: int, length: int, extreme_pos: int, ref: Decimal, extreme: Decimal) -> None:
    drop = ref - extreme
    other = ref - (drop / D(4) if drop > 0 else D("0.01"))
    for offset in range(length):
        pos = offset + 1
        if pos == 1:
            rows[start + offset] = [ref, ref + D("0.05"), other, ref]
        elif pos == extreme_pos:
            rows[start + offset] = [ref, ref, extreme, extreme + D("0.01")]
        elif pos == length:
            rows[start + offset] = [ref, ref + D("0.01"), extreme + D("0.02"), ref]
        else:
            rows[start + offset] = [ref, ref + D("0.02"), other, ref]


def _bars(rows, atr: str = "0.4") -> list[Bar]:
    bars = make_bars(rows, recompute_atr=False)
    for bar in bars:
        bar.atr = D(atr)
    return bars


def _qualified(kind: str, length: int, extreme_pos: int, ref: str, extreme: str):
    rows = _rows(length, ref)
    if kind == "HIGH":
        _install_high(rows, 0, length, extreme_pos, D(ref), D(extreme))
    else:
        _install_low(rows, 0, length, extreme_pos, D(ref), D(extreme))
    bars = _bars(rows)
    return classify_formation(bars, 0, length - 1, kind), bars


def _bar(row: int, close: str, when: datetime | None = None) -> Bar:
    opened = when or datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=row)
    price = D(close)
    return Bar(
        row=row,
        open_time=opened,
        close_time=opened + timedelta(hours=1) - timedelta(milliseconds=1),
        open=price,
        high=price,
        low=price,
        close=price,
        atr=D("1"),
    )


def _scenario():
    rows = _rows(80, "100")
    _install_low(rows, 8, 5, 3, D("100"), D("96"))
    _install_high(rows, 24, 5, 3, D("108"), D("110"))
    rows[28] = [D("109"), D("109.6"), D("107.8"), D("109.7")]
    rows[30] = [D("109.4"), D("109.5"), D("109.0"), D("109.4")]
    _install_low(rows, 44, 5, 3, D("106"), D("104"))
    rows[48] = [D("105.4"), D("106.2"), D("105.2"), D("105.5")]
    rows[55] = [D("105.8"), D("106.1"), D("105.6"), D("105.9")]
    return _bars(rows, "0.4")


class HierarchicalSwingDetectorTests(unittest.TestCase):
    def test_01_five_candle_minimum(self) -> None:
        record, _ = _qualified("HIGH", 5, 3, "100", "102")
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")
        self.assertEqual(record["total_bars"], 5)

    def test_02_ten_candle_maximum(self) -> None:
        record, _ = _qualified("HIGH", 10, 5, "100", "102")
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")
        self.assertEqual(record["total_bars"], 10)

    def test_03_three_interior_accepted(self) -> None:
        record, _ = _qualified("HIGH", 5, 3, "100", "102")
        self.assertEqual(record["interior_bars"], 3)
        self.assertTrue(record["structural_pass"])

    def test_04_eight_interior_accepted(self) -> None:
        record, _ = _qualified("LOW", 10, 5, "100", "98")
        self.assertEqual(record["interior_bars"], 8)
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")

    def test_05_below_five_rejected(self) -> None:
        bars = _bars(_rows(4))
        record = classify_formation(bars, 0, 3, "HIGH")
        self.assertEqual(record["primary_disposition"], "FORMATION_TOTAL_BARS_BELOW_5")

    def test_06_above_ten_rejected(self) -> None:
        bars = _bars(_rows(11))
        record = classify_formation(bars, 0, 10, "HIGH")
        self.assertEqual(record["primary_disposition"], "FORMATION_TOTAL_BARS_ABOVE_10")

    def test_07_asymmetric_spans_accepted(self) -> None:
        record, _ = _qualified("HIGH", 10, 4, "100", "102")
        self.assertEqual(record["left_bars"], 3)
        self.assertEqual(record["right_bars"], 6)
        self.assertFalse(record["symmetric"])
        self.assertEqual(record["pivot_span_mode"], "FORMATION_DERIVED_ASYMMETRIC")

    def test_08_ten_candle_extreme_position_4(self) -> None:
        record, _ = _qualified("HIGH", 10, 4, "100", "102")
        self.assertEqual(record["extreme_position"], 4)
        self.assertIn(4, allowed_central_positions(10))
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")

    def test_09_ten_candle_extreme_position_5(self) -> None:
        record, _ = _qualified("HIGH", 10, 5, "100", "102")
        self.assertEqual(record["extreme_position"], 5)
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")

    def test_10_ten_candle_extreme_position_6(self) -> None:
        record, _ = _qualified("HIGH", 10, 6, "100", "102")
        self.assertEqual(record["extreme_position"], 6)
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")

    def test_11_ten_candle_extreme_outside_central_three(self) -> None:
        early, _ = _qualified("HIGH", 10, 3, "100", "102")
        late, _ = _qualified("HIGH", 10, 7, "100", "102")
        self.assertEqual(early["primary_disposition"], "EXTREME_OUTSIDE_CENTRAL_THREE")
        self.assertEqual(late["primary_disposition"], "EXTREME_OUTSIDE_CENTRAL_THREE")

    def test_12_swing_high_outbound_boundary(self) -> None:
        record, _ = _qualified("HIGH", 5, 3, "100", "101")
        self.assertEqual(record["outbound_percent"], D("1"))
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")

    def test_13_swing_low_outbound_boundary(self) -> None:
        record, _ = _qualified("LOW", 5, 3, "100", "99")
        self.assertEqual(record["outbound_percent"], D("1"))
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")

    def test_14_exact_one_percent_passes(self) -> None:
        high, _ = _qualified("HIGH", 5, 3, "100", "101")
        low, _ = _qualified("LOW", 5, 3, "100", "99")
        self.assertGreaterEqual(high["outbound_percent"], D("1.00"))
        self.assertGreaterEqual(low["return_percent_reference_basis"], D("1.00"))
        self.assertEqual(high["primary_disposition"], "QUALIFIED_RAW_FORMATION")
        self.assertEqual(low["primary_disposition"], "QUALIFIED_RAW_FORMATION")

    def test_15_below_one_percent_fails(self) -> None:
        high, _ = _qualified("HIGH", 5, 3, "100", "100.99")
        low, _ = _qualified("LOW", 5, 3, "100", "99.01")
        self.assertEqual(high["primary_disposition"], "OUTBOUND_BELOW_1_PERCENT")
        self.assertEqual(low["primary_disposition"], "OUTBOUND_BELOW_1_PERCENT")
        self.assertLess(high["outbound_percent"], D("1"))

    def test_16_close_touch_exact_passes(self) -> None:
        rows = _rows(5, "100")
        _install_high(rows, 0, 5, 3, D("100"), D("102"))
        rows[4] = [D("100"), D("100"), D("100"), D("100")]
        record = classify_formation(_bars(rows), 0, 4, "HIGH")
        self.assertTrue(record["revisit_pass"])
        self.assertEqual(record["touch_price"], D("100"))
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")

    def test_17_close_missing_reference_rejected(self) -> None:
        rows = _rows(5, "100")
        _install_high(rows, 0, 5, 3, D("100"), D("102"))
        rows[4] = [D("100.4"), D("100.8"), D("100.2"), D("100.5")]
        record = classify_formation(_bars(rows), 0, 4, "HIGH")
        self.assertEqual(record["primary_disposition"], "CLOSE_CANDLE_DID_NOT_REVISIT_REFERENCE")

    def test_18_equal_high_plateau(self) -> None:
        rows = _rows(7, "100")
        _install_high(rows, 0, 7, 3, D("100"), D("103"))
        rows[2] = [D("100"), D("103"), D("100"), D("102")]
        rows[3] = [D("101"), D("103"), D("100.5"), D("102")]
        record = classify_formation(_bars(rows), 0, 6, "HIGH")
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")
        self.assertGreaterEqual(record["plateau_length"], 2)
        self.assertEqual(record["extreme_position"], 4)

    def test_19_equal_low_plateau(self) -> None:
        rows = _rows(7, "100")
        _install_low(rows, 0, 7, 3, D("100"), D("97"))
        rows[2] = [D("99"), D("100"), D("97"), D("98")]
        rows[3] = [D("98"), D("99.5"), D("97"), D("98")]
        record = classify_formation(_bars(rows), 0, 6, "LOW")
        self.assertEqual(record["primary_disposition"], "QUALIFIED_RAW_FORMATION")
        self.assertGreaterEqual(record["plateau_length"], 2)
        self.assertEqual(record["extreme_position"], 4)

    def test_20_representative_deduplication(self) -> None:
        rows = _rows(12, "100")
        _install_high(rows, 0, 10, 5, D("100"), D("105"))
        rows[1] = [D("103"), D("103.2"), D("102.5"), D("103")]
        bars = _bars(rows)
        formations, _ = enumerate_formations(bars)
        assign_causal_dispositions(bars, formations)
        groups: dict[tuple, list] = {}
        for item in formations:
            if item.get("extreme_row") is None or not item.get("structural_pass"):
                continue
            key = (item["formation_type"], item["extreme_row"], str(item["extreme_price"]), item["close_row"])
            groups.setdefault(key, []).append(item)
        multi = [group for group in groups.values() if len(group) >= 2]
        self.assertTrue(multi)
        for group in multi:
            self.assertLessEqual(sum(1 for item in group if item["emits_pivot"]), 1)
        duplicates = [item for item in formations if item["primary_disposition"] == "DUPLICATE_OF_REPRESENTATIVE_FORMATION"]
        qualified_ids = {
            item["formation_id"] for item in formations if item["primary_disposition"] == "QUALIFIED_RAW_FORMATION"
        }
        self.assertTrue(duplicates)
        self.assertTrue(qualified_ids)
        self.assertTrue(any(item["representative_formation_id"] in qualified_ids for item in duplicates))

    def test_21_confirmation_after_swing_close(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        self.assertGreater(len(result.pivots), 0)
        for pivot in result.pivots:
            self.assertEqual(pivot.confirm_time, result.bars[pivot.confirm_row].close_time)
            self.assertGreater(pivot.confirm_time, result.bars[pivot.extreme_row].open_time)
            self.assertGreaterEqual(pivot.confirm_row, pivot.extreme_row)

    def test_22_no_repainting(self) -> None:
        bars = _scenario()
        full = run_detection(bars, recompute_atr=False, verify_independence=False)
        swing = full.internal_swings[0]
        prefix = run_detection(bars[: swing.confirm_row + 1], recompute_atr=False, verify_independence=False)
        self.assertTrue(prefix.internal_swings)
        self.assertEqual(prefix.internal_swings[0].price, swing.price)
        self.assertEqual(prefix.internal_swings[0].confirm_row, swing.confirm_row)
        self.assertEqual(iso_utc(prefix.internal_swings[0].confirmed_at), iso_utc(swing.confirmed_at))

    def test_23_no_lookahead(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        for pivot in result.pivots:
            self.assertGreaterEqual(pivot.confirm_row, pivot.extreme_row)
        for swing in result.internal_swings:
            self.assertGreaterEqual(swing.confirm_row, swing.pivot.confirm_row)
            self.assertGreaterEqual(swing.confirmed_at, swing.raw_pivot_confirmed_at)
        for swing in result.major_swings:
            self.assertGreaterEqual(swing.confirmed_at, swing.internal_confirmed_at)

    def test_24_internal_bootstrap(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        self.assertIsNotNone(result.internal_seed)
        self.assertEqual(result.internal_seed.internal_role, "INTERNAL_BOOTSTRAP_SEED")
        self.assertTrue(result.internal_engine.bootstrap_record)
        self.assertGreaterEqual(len(result.internal_swings), 1)

    def test_25_major_bootstrap(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        self.assertIsNotNone(result.major_seed)
        self.assertEqual(result.major_seed.major_role, "MAJOR_BOOTSTRAP_SEED")
        self.assertFalse(result.major_seed.became_major_swing)
        self.assertGreaterEqual(len(result.major_swings), 1)

    def test_26_bootstrap_seeds_excluded(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        internal_pivots = {swing.pivot_id for swing in result.internal_swings}
        self.assertNotIn(result.internal_seed.pivot_id, internal_pivots)
        major_ids = {swing.internal_swing_id for swing in result.major_swings}
        self.assertNotIn(result.major_seed.internal_swing_id, major_ids)
        self.assertEqual(result.unique["unique_confirmed_internal_swings"], len(result.internal_swings))

    def test_27_reversal_evaluated_every_bar(self) -> None:
        bars = _scenario()
        result = run_detection(bars, recompute_atr=False, verify_independence=False)
        self.assertEqual(result.per_bar["internal_reversal_evaluations"], len(bars))
        self.assertEqual(result.per_bar["major_reversal_evaluations"], len(bars))
        self.assertEqual(result.per_bar["label"], "PER_BAR_DIAGNOSTIC_ONLY")

    def test_28_reversal_without_new_pivot(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        delayed = [swing for swing in result.internal_swings if swing.confirm_row > swing.pivot.confirm_row]
        self.assertTrue(delayed)
        released = {pivot.confirm_row for pivot in result.pivots}
        self.assertTrue(any(swing.confirm_row not in released or swing.confirm_row != swing.pivot.confirm_row for swing in delayed))

    def test_29_rejected_pivot_does_not_block(self) -> None:
        engine = InternalEngine()
        engine.bootstrap_done = True
        engine.search_direction = "LOW"
        engine.previous_opposite = Record(type="HIGH", price=D("110"), extreme_row=10, pivot_id="PREV")
        bad = Record(type="LOW", price=D("100"), extreme_row=14, atr=D("1"), pivot_id="BAD", formation_id="FB", confirm_row=16, confirm_time=_bar(16, "100").close_time, open_time=_bar(14, "100").open_time)
        good = Record(type="LOW", price=D("99"), extreme_row=20, atr=D("1"), pivot_id="GOOD", formation_id="FG", confirm_row=22, confirm_time=_bar(22, "100").close_time, open_time=_bar(20, "100").open_time)
        engine._consider(bad, _bar(16, "100"))
        engine._consider(good, _bar(22, "100"))
        self.assertEqual(bad.internal_disposition, "INTERNAL_SPACING_BELOW_6")
        self.assertIsNotNone(engine.active)
        self.assertEqual(engine.active.pivot_id, "GOOD")

    def test_30_internal_candidate_replacement(self) -> None:
        engine = InternalEngine()
        engine.bootstrap_done = True
        engine.search_direction = "HIGH"
        engine.previous_opposite = Record(type="LOW", price=D("90"), extreme_row=1, pivot_id="PREV")
        first = Record(type="HIGH", price=D("100"), extreme_row=10, atr=D("1"), pivot_id="H1", formation_id="F1", confirm_row=12, confirm_time=_bar(12, "100").close_time, open_time=_bar(10, "100").open_time)
        second = Record(type="HIGH", price=D("104"), extreme_row=14, atr=D("1"), pivot_id="H2", formation_id="F2", confirm_row=16, confirm_time=_bar(16, "100").close_time, open_time=_bar(14, "100").open_time)
        engine._consider(first, _bar(12, "103"))
        engine._consider(second, _bar(16, "103"))
        self.assertEqual(engine.active.price, D("104"))
        self.assertEqual(first.internal_disposition, "INTERNAL_REPLACED_BY_MORE_EXTREME")
        self.assertEqual(engine.replacements, 1)

    def test_31_major_candidate_replacement(self) -> None:
        bars = _bars(_rows(40, "100"), "1")
        global_bars = bars
        import detectors.hierarchical_swing_v4.engine as engine_module
        engine_module._BARS_FOR_PROMINENCE = global_bars
        engine = MajorEngine()
        engine.bootstrap_done = True
        engine.search_direction = "HIGH"
        engine.previous_opposite = Record(type="LOW", price=D("90"), extreme_row=1, internal_swing_id="PREV", confirm_row=2, sequence=1)
        first = self._major_source("HIGH", "120", 16, "IS-A")
        second = self._major_source("HIGH", "130", 28, "IS-B")
        engine._consider(first, bars[18])
        engine._consider(second, bars[30])
        self.assertIsNotNone(engine.active)
        self.assertEqual(engine.active.price, D("130"))
        self.assertEqual(first.major_non_promotion_reason, "MAJOR_REPLACED_BY_MORE_EXTREME")

    def test_32_replacement_recalculates_threshold(self) -> None:
        engine = InternalEngine()
        engine.bootstrap_done = True
        engine.search_direction = "HIGH"
        engine.previous_opposite = Record(type="LOW", price=D("90"), extreme_row=1, pivot_id="PREV")
        first = Record(type="HIGH", price=D("100"), extreme_row=10, atr=D("2"), pivot_id="H1", formation_id="F1", confirm_row=12, confirm_time=_bar(12, "100").close_time, open_time=_bar(10, "100").open_time)
        second = Record(type="HIGH", price=D("110"), extreme_row=16, atr=D("4"), pivot_id="H2", formation_id="F2", confirm_row=18, confirm_time=_bar(18, "100").close_time, open_time=_bar(16, "100").open_time)
        engine._consider(first, _bar(12, "100"))
        first_level = engine.active.reversal_level
        engine._consider(second, _bar(18, "100"))
        self.assertEqual(first_level, D("98"))
        self.assertEqual(engine.active.reversal_level, D("106"))
        self.assertNotEqual(engine.active.reversal_level, first_level)

    def test_33_internal_alternation(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        kinds = [swing.type for swing in result.internal_swings]
        self.assertTrue(kinds)
        self.assertTrue(all(left != right for left, right in zip(kinds, kinds[1:])))

    def test_34_major_alternation(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        kinds = [swing.type for swing in result.major_swings]
        self.assertTrue(kinds)
        self.assertTrue(all(left != right for left, right in zip(kinds, kinds[1:])))

    def test_35_major_displacement_and(self) -> None:
        import detectors.hierarchical_swing_v4.engine as engine_module
        engine_module._BARS_FOR_PROMINENCE = _bars(_rows(30, "10000"), "10")
        engine = MajorEngine()
        previous = Record(type="LOW", price=D("10000"), extreme_row=1)
        candidate = self._major_source("HIGH", "10040", 20, "IS-X")
        candidate.atr = D("10")
        reason = engine._failure(previous, candidate, 22)
        self.assertEqual(reason, "MAJOR_DISPLACEMENT_PERCENT_BELOW_1_50")
        wide = self._major_source("HIGH", "10200", 20, "IS-Y")
        wide.atr = D("10")
        self.assertIsNone(engine._failure(previous, wide, 22))

    def test_36_major_reversal_and(self) -> None:
        import detectors.hierarchical_swing_v4.engine as engine_module
        engine_module._BARS_FOR_PROMINENCE = _bars(_rows(5, "10000"), "100")
        engine = MajorEngine()
        source = self._major_source("HIGH", "10000", 1, "IS-R")
        source.atr = D("100")
        source.confirmed_at = _bar(1, "10000").close_time
        source.raw_pivot_confirmed_at = _bar(1, "10000").close_time
        engine.active = Record(
            candidate_id="MC-1",
            type="HIGH",
            price=D("10000"),
            atr=D("100"),
            reversal_level=D("9850"),
            displacement_atr_passed=True,
            displacement_percent_passed=True,
            spacing_passed=True,
            prominence_passed=True,
            activation_row=1,
            activation_time=_bar(1, "10000").close_time,
            extreme_row=1,
            formation_id="F",
            pivot_id="P",
            internal_swing_id="IS-R",
            source=source,
            previous=Record(type="LOW", price=D("9000"), extreme_row=0, confirm_row=0),
            displacement_price=D("1000"),
            displacement_atr=D("10"),
            displacement_percent=D("10"),
            spacing_bars=12,
            prominence=source.pivot.prominence,
            prominence_atr=D("2"),
            age=0,
            max_age=0,
            events=0,
            reversal_ever=False,
            closest_distance=None,
            closest_close=None,
            replacement_history="",
            hit_168=False,
            hit_720=False,
            hit_2160=False,
        )
        partial = _bar(2, "9920")
        self.assertFalse(engine._evaluate_active(partial))
        self.assertEqual(engine.confirmed, [])
        self.assertIsNotNone(engine.active)
        full = _bar(3, "9700")
        self.assertTrue(engine._evaluate_active(full))
        self.assertIsNone(engine.active)
        self.assertEqual(len(engine.confirmed), 1)

    def test_37_major_prominence_enforced(self) -> None:
        import detectors.hierarchical_swing_v4.engine as engine_module
        bars = _bars(_rows(30, "100"), "1")
        bars[20].high = D("110")
        bars[20].low = D("109.8")
        bars[20].close = D("110")
        engine_module._BARS_FOR_PROMINENCE = bars
        engine = MajorEngine()
        previous = Record(type="LOW", price=D("90"), extreme_row=1)
        candidate = self._major_source("HIGH", "110", 20, "IS-P")
        candidate.atr = D("1")
        candidate.extreme_row = 20
        reason = engine._failure(previous, candidate, 20)
        self.assertEqual(reason, "MAJOR_PROMINENCE_BELOW_1_25")

    def test_38_internal_spacing_enforcement(self) -> None:
        engine = InternalEngine()
        pivot = Record(type="LOW", price=D("90"), extreme_row=10, atr=D("1"))
        previous = Record(type="HIGH", price=D("100"), extreme_row=5)
        ok, reason = engine._eligibility(pivot, previous)
        self.assertFalse(ok)
        self.assertEqual(reason, "INTERNAL_SPACING_BELOW_6")

    def test_39_major_spacing_enforcement(self) -> None:
        import detectors.hierarchical_swing_v4.engine as engine_module
        engine_module._BARS_FOR_PROMINENCE = _bars(_rows(40, "100"), "1")
        engine = MajorEngine()
        previous = Record(type="HIGH", price=D("120"), extreme_row=10)
        candidate = self._major_source("LOW", "100", 21, "IS-S")
        candidate.atr = D("1")
        self.assertEqual(engine._failure(previous, candidate, 25), "MAJOR_SPACING_BELOW_12")

    def test_40_major_engine_cannot_affect_internal_output(self) -> None:
        bars = _scenario()
        enabled = run_detection(bars, enable_major=True, recompute_atr=False, verify_independence=True)
        disabled = run_detection(bars, enable_major=False, recompute_atr=False, verify_independence=False)
        self.assertTrue(enabled.independence_pass)
        left = [(swing.type, str(swing.price), swing.confirm_row) for swing in enabled.internal_swings]
        right = [(swing.type, str(swing.price), swing.confirm_row) for swing in disabled.internal_swings]
        self.assertEqual(left, right)

    def test_41_candidate_age_diagnostics(self) -> None:
        engine = InternalEngine()
        engine.bootstrap_done = True
        pivot = Record(type="HIGH", price=D("100"), extreme_row=5, atr=D("1"), pivot_id="P", formation_id="F", confirm_row=6, confirm_time=_bar(6, "100").close_time, open_time=_bar(5, "100").open_time)
        engine.active = Record(
            candidate_id="IC-1",
            type="HIGH",
            pivot=pivot,
            price=D("100"),
            atr=D("1"),
            reversal_level=D("90"),
            displacement_passed=True,
            spacing_passed=True,
            activation_row=6,
            extreme_row=5,
            age=0,
            max_age=0,
            events=0,
            reversal_ever=False,
            closest_distance=None,
            closest_close=None,
            hit_168=False,
            hit_720=False,
            hit_2160=False,
            formation_id="F",
            pivot_id="P",
        )
        for row in range(6, 200):
            engine.evaluate(_bar(row, "100"))
        self.assertIsNotNone(engine.active)
        self.assertGreater(engine.active.max_age, 168)
        self.assertEqual(engine.confirmed, [])
        self.assertGreaterEqual(engine.exceeded[168], 1)
        self.assertEqual(engine.ignored_valid, 0)

    def test_42_unique_counts_separated_from_per_bar_counts(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        self.assertEqual(result.per_bar["label"], "PER_BAR_DIAGNOSTIC_ONLY")
        self.assertGreater(result.per_bar["formation_checks"], result.unique["unique_qualified_formations"])
        self.assertGreater(result.per_bar["internal_reversal_evaluations"], result.unique["unique_internal_candidates"])
        self.assertNotEqual(result.per_bar["internal_reversal_evaluations"], result.unique["unique_confirmed_internal_swings"])

    def test_43_monthly_diagnostics_reconcile(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        self.assertEqual(sum(row["source_candles"] for row in result.monthly), len(result.bars))
        confirmed = sum(row["confirmed_internal_highs"] + row["confirmed_internal_lows"] for row in result.monthly)
        self.assertEqual(confirmed, len(result.internal_swings))
        majors = sum(row["confirmed_major_highs"] + row["confirmed_major_lows"] for row in result.monthly)
        self.assertEqual(majors, len(result.major_swings))

    def test_44_utc_turkey_conversion(self) -> None:
        opened = datetime(2026, 1, 1, tzinfo=UTC)
        closed = opened + timedelta(hours=1) - timedelta(milliseconds=1)
        self.assertEqual(iso_utc(opened), "2026-01-01T00:00:00Z")
        self.assertEqual(iso_turkey(opened), "2026-01-01T03:00:00.000+03:00")
        self.assertEqual(iso_turkey(closed), "2026-01-01T03:59:59.999+03:00")
        last_open = datetime(2026, 9, 15, 23, tzinfo=UTC)
        self.assertEqual(iso_turkey(last_open), "2026-09-16T02:00:00.000+03:00")

    def test_45_terminal_unresolved_candidate(self) -> None:
        engine = InternalEngine()
        engine.bootstrap_done = True
        pivot = Record(type="LOW", price=D("100"), extreme_row=4, atr=D("1"), pivot_id="P", formation_id="F", confirm_row=5, confirm_time=_bar(5, "100").close_time, open_time=_bar(4, "100").open_time)
        engine.active = Record(
            candidate_id="IC-9",
            type="LOW",
            pivot=pivot,
            price=D("100"),
            atr=D("1"),
            reversal_level=D("110"),
            displacement_passed=True,
            spacing_passed=True,
            activation_row=5,
            extreme_row=4,
            age=0,
            max_age=0,
            events=0,
            reversal_ever=False,
            closest_distance=None,
            closest_close=None,
            hit_168=False,
            hit_720=False,
            hit_2160=False,
            formation_id="F",
            pivot_id="P",
            unresolved=False,
            unresolved_reason="",
            replacement_history="",
        )
        engine.evaluate(_bar(6, "100"))
        engine.finalize()
        self.assertEqual(engine.confirmed, [])
        self.assertTrue(engine.active.unresolved)
        self.assertEqual(engine.active.unresolved_reason, "INTERNAL_REVERSAL_NOT_CONFIRMED_BEFORE_DATASET_END")

    def test_46_source_left_edge_censoring(self) -> None:
        result = run_detection(_bars(_rows(30)), recompute_atr=False, verify_independence=False)
        self.assertEqual(result.formation_stats["left_edge_censored"], 39)
        censored = [item for item in result.formations if item["primary_disposition"] == "LEFT_EDGE_CENSORED"]
        self.assertEqual(len(censored), 39)
        self.assertTrue(all(item["open_row"] < 0 for item in censored))
        self.assertTrue(all(item["reference_price"] is None for item in censored))
        self.assertEqual(result.bars[0].open_time, datetime(2026, 1, 1, tzinfo=UTC))

    def test_47_dataset_end_right_edge_censoring(self) -> None:
        result = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        self.assertEqual(result.formation_stats["right_edge_censored"], 39)
        censored = [item for item in result.formations if item["primary_disposition"] == "RIGHT_EDGE_CENSORED"]
        self.assertTrue(all(item["close_row"] >= len(result.bars) for item in censored))
        if result.major_swings:
            self.assertTrue(result.major_swings[-1].forward_data_censored)
            self.assertEqual(result.major_swings[-1].forward_censoring_reason, "DATASET_END")

    def test_48_dynamic_workbook_revision_and_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.assertEqual(next_revision(existing_revisions(root)), 0)
            (root / revision_filename(0)).write_bytes(b"x")
            (root / revision_filename(2)).write_bytes(b"y")
            (root / "BTCUSDT_1H_range_rev05.xlsx").write_bytes(b"z")
            found = existing_revisions(root)
            self.assertEqual(found, [0, 2])
            self.assertEqual(next_revision(found), 3)
            bars = _scenario()
            result = run_detection(bars, recompute_atr=False, verify_independence=False)
            context = {
                "revision_name": revision_filename(3),
                "diagnostic_rows": [["implementation", "source_rows", str(len(bars))], ["implementation", "workbook_revision", revision_filename(3)], ["PER_BAR_DIAGNOSTIC_ONLY", "per_bar_counter_label", "PER_BAR_DIAGNOSTIC_ONLY"], ["causality", "ignored_valid_reversal_count", "0"], ["causality", "state_transition_error_count", "0"]],
                "readme_lines": ["roundtrip"],
            }
            path = root / revision_filename(3)
            write_workbook(path, result, context)
            problems = validate_saved_workbook(path, result, context)
            self.assertEqual(problems, [])

    def test_49_existing_workbook_non_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / revision_filename(0)
            path.write_bytes(b"keep")
            with self.assertRaises(FileExistsError):
                assert_not_overwrite(path)
            self.assertEqual(next_revision(existing_revisions(Path(folder))), 1)
            self.assertFalse((Path(folder) / revision_filename(1)).exists())
            self.assertEqual(path.read_bytes(), b"keep")

    def test_50_project_path_enforcement(self) -> None:
        self.assertEqual(PROJECT_ROOT, Path(r"C:\Users\oranb\Desktop\backtest_system"))
        self.assertEqual(PACKAGE_DIR, PROJECT_ROOT / "detectors" / "hierarchical_swing_v4")
        with tempfile.TemporaryDirectory() as folder:
            env = os.environ.copy()
            env["PYTHONPATH"] = str(PROJECT_ROOT)
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            completed = subprocess.run(
                [sys.executable, "-c", "from detectors.hierarchical_swing_v4.swing_config import PROJECT_ROOT; print(PROJECT_ROOT)"],
                cwd=folder,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
        self.assertEqual(completed.stdout.strip(), str(PROJECT_ROOT))

    def test_51_detector_cannot_write_python_outside_package(self) -> None:
        with self.assertRaises(PermissionError):
            assert_python_write_allowed(Path(r"C:\Users\oranb\Desktop\hierarchical_swing_detector.py"))
        assert_python_write_allowed(PACKAGE_DIR / "run_hierarchical_swing_detector.py")

    def test_52_range_detector_is_not_imported(self) -> None:
        self.assertFalse(any("primary_range" in name for name in sys.modules))

    def test_53_strategy_modules_are_not_imported(self) -> None:
        self.assertFalse(any(name == "strategy" or name.startswith("strategy.") for name in sys.modules))

    def test_54_source_parquet_files_remain_unchanged(self) -> None:
        paths = source_partition_paths()
        before = {str(path): fingerprint_file(path) for path in paths}
        bars, provenance = load_production_bars()
        after = {str(path): fingerprint_file(path) for path in paths}
        self.assertEqual(before, after)
        self.assertEqual(len(bars), 6192)
        self.assertTrue(all("year=2026" in path.replace("/", "\\") for path in provenance["source_files"]))
        self.assertTrue(all("year=2024" not in path and "year=2025" not in path for path in provenance["source_files"]))

    def test_55_candidate_latch_regression(self) -> None:
        engine = InternalEngine()
        engine.bootstrap_done = True
        engine.search_direction = "LOW"
        pivot = Record(type="HIGH", price=D("110"), extreme_row=8, atr=D("2"), pivot_id="P", formation_id="F", confirm_row=10, confirm_time=_bar(10, "110").close_time, open_time=_bar(8, "110").open_time, internal_disposition="", internal_role="")
        previous = Record(type="LOW", price=D("100"), extreme_row=1, pivot_id="PREV")
        engine.previous_opposite = previous
        engine._activate(pivot, previous, _bar(10, "109"), "INTERNAL_CANDIDATE_ACTIVATED")
        self.assertIsNotNone(engine.active)
        confirmed = engine._evaluate_active(_bar(11, "107"))
        self.assertTrue(confirmed)
        self.assertIsNone(engine.active)
        self.assertEqual(len(engine.confirmed), 1)
        self.assertEqual(engine.ignored_valid, 0)
        self.assertEqual(engine.state_errors, 0)

    def test_56_engine_progresses_throughout_2026(self) -> None:
        bars, _ = load_production_bars()
        result = run_detection(bars, enable_major=True, recompute_atr=True, verify_independence=True)
        self.assertEqual(len(result.bars), 6192)
        self.assertEqual(result.per_bar["internal_reversal_evaluations"], 6192)
        self.assertEqual(result.per_bar["major_reversal_evaluations"], 6192)
        months = [row["utc_month"] for row in result.monthly]
        self.assertEqual(months, [f"2026-{month:02d}" for month in range(1, 10)])
        self.assertEqual(result.internal_engine.ignored_valid, 0)
        self.assertEqual(result.internal_engine.state_errors, 0)
        self.assertEqual(result.major_engine.state_errors, 0)
        self.assertTrue(result.independence_pass)
        self.assertEqual(iso_utc(result.bars[0].open_time), "2026-01-01T00:00:00Z")
        self.assertEqual(iso_utc(result.bars[-1].open_time), "2026-09-15T23:00:00Z")
        if result.internal_swings and not result.major_swings:
            self.assertTrue(all(swing.major_non_promotion_reason for swing in result.internal_swings))

    def test_57_rerun_does_not_rewrite_detector_source(self) -> None:
        files = [
            PACKAGE / "run_hierarchical_swing_detector.py",
            PACKAGE / "engine.py",
            PACKAGE / "workbook.py",
            PACKAGE / "swing_config.py",
            PACKAGE / "README.md",
            PACKAGE / "tests" / "test_hierarchical_swing_detector.py",
        ]
        before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
        first = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        second = run_detection(_scenario(), recompute_atr=False, verify_independence=False)
        after = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
        self.assertEqual(before, after)
        self.assertEqual(
            [(swing.type, str(swing.price), swing.confirm_row) for swing in first.internal_swings],
            [(swing.type, str(swing.price), swing.confirm_row) for swing in second.internal_swings],
        )
        self.assertEqual(
            [(swing.type, str(swing.price), swing.confirm_row) for swing in first.major_swings],
            [(swing.type, str(swing.price), swing.confirm_row) for swing in second.major_swings],
        )

    def test_58_future_workbook_runs_allocate_the_next_revision(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / revision_filename(0)).write_bytes(b"a")
            (root / revision_filename(4)).write_bytes(b"b")
            allocated = next_revision(existing_revisions(root))
            self.assertEqual(allocated, 5)
            self.assertEqual(revision_filename(allocated), "BTCUSDT_1H_swing_rev05.xlsx")
            (root / revision_filename(allocated)).write_bytes(b"c")
            self.assertEqual(next_revision(existing_revisions(root)), 6)
            self.assertEqual(revision_filename(100), "BTCUSDT_1H_swing_rev100.xlsx")

    def _major_source(self, kind: str, price: str, extreme_row: int, swing_id: str) -> Record:
        opened = _bar(extreme_row, price)
        pivot = Record(
            pivot_id="RP-" + swing_id,
            formation_id="FM-" + swing_id,
            type=kind,
            extreme_row=extreme_row,
            confirm_row=extreme_row + 2,
            price=D(price),
            atr=D("1"),
            open_time=opened.open_time,
            confirm_time=_bar(extreme_row + 2, price).close_time,
            prominence={"left_prominence": D("5"), "right_prominence": D("5"), "two_sided_prominence": D("5"), "max_two_sided_prominence": D("5"), "percentage_prominence": D("1"), "left_censored": False, "right_censored": False},
            prominence_atr_at_confirmation=D("5"),
        )
        return Record(
            internal_swing_id=swing_id,
            sequence=1,
            type=kind,
            price=D(price),
            atr=D("1"),
            extreme_row=extreme_row,
            confirm_row=extreme_row + 2,
            extreme_time=opened.open_time,
            confirmed_at=_bar(extreme_row + 2, price).close_time,
            raw_pivot_confirmed_at=_bar(extreme_row + 2, price).close_time,
            formation_id=pivot.formation_id,
            pivot_id=pivot.pivot_id,
            pivot=pivot,
            became_major_swing=False,
            major_swing_id="",
            major_non_promotion_reason="",
            major_role="",
        )


if __name__ == "__main__":
    unittest.main()
