"""Tests for the separate 30-minute special swing detector."""
from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from detectors.swing_open_close_30m_special_v1.config import (
    CONFIGURATION_VERSION,
    FIRST_CLOSE_OFFSET,
    FORMATION_CLASS,
    FORBIDDEN_TERMINALS,
    LAST_CLOSE_OFFSET,
    MAX_BOUNDARY,
    MAX_INTERIOR,
    MAX_TOTAL,
    MIN_BOUNDARY,
    MIN_INTERIOR,
    MIN_TOTAL,
    RESULT_COLUMNS,
    USER_PARAMETERS,
)
from detectors.swing_open_close_30m_special_v1.engine import (
    Bar,
    Swing,
    _primary_key,
    _suppress_overlap,
    analyze,
    body_penetrated,
    candle_direction,
    body_threshold,
    boundary_reason,
    duration_reason,
    penetration_fraction,
    select_interior_body_reference,
    signature,
    swing_open_body,
)
from detectors.swing_open_close_30m_special_v1.run_swing_open_close_30m_special_detector import allocate_revision
from detectors.swing_open_close_30m_special_v1.workbook import validate_workbook, write_workbook

UTC = timezone.utc
THIRD = Decimal(1) / Decimal(3)


def _bar(index: int, open_price: str, high: str, low: str, close: str) -> Bar:
    opened = datetime(2026, 7, 1, tzinfo=UTC) + timedelta(minutes=30 * index)
    return Bar(
        index,
        opened,
        opened + timedelta(minutes=30) - timedelta(milliseconds=1),
        Decimal(open_price),
        Decimal(high),
        Decimal(low),
        Decimal(close),
    )


def _quiet(count: int, price: str = "100") -> list[Bar]:
    return [_bar(index, price, "101", "99", price) for index in range(count)]


class DurationTests(unittest.TestCase):
    def test_duration_bounds(self) -> None:
        self.assertEqual(MIN_INTERIOR, 6)
        self.assertEqual(MAX_INTERIOR, 9)
        self.assertEqual(MIN_TOTAL, 8)
        self.assertEqual(MAX_TOTAL, 11)
        self.assertEqual(FIRST_CLOSE_OFFSET, 7)
        self.assertEqual(LAST_CLOSE_OFFSET, 10)
        self.assertIn("6_TO_9", CONFIGURATION_VERSION)
        self.assertNotIn("7_TO_10", CONFIGURATION_VERSION)
        self.assertEqual(duration_reason(5), "INTERIOR_COUNT_BELOW_6")
        for interior in (6, 7, 8, 9):
            self.assertIsNone(duration_reason(interior))
            self.assertGreaterEqual(interior + 2, 8)
            self.assertLessEqual(interior + 2, 11)
        self.assertEqual(duration_reason(10), "INTERIOR_COUNT_ABOVE_9")
        self.assertEqual(duration_reason(11), "INTERIOR_COUNT_ABOVE_9")
        self.assertIn("INTERIOR_COUNT_BELOW_7", FORBIDDEN_TERMINALS)
        self.assertIn("INTERIOR_COUNT_ABOVE_10", FORBIDDEN_TERMINALS)
        self.assertIn("SEARCH_NOT_YET_ELIGIBLE_BELOW_7_INTERIORS", FORBIDDEN_TERMINALS)
        self.assertIn("SEARCH_HORIZON_EXHAUSTED_AT_10_INTERIORS", FORBIDDEN_TERMINALS)

    def _series(self, last_row: int) -> list[Bar]:
        bars = _quiet(last_row + 1)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[3] = _bar(3, "100", "110", "99", "105")
        return bars

    def test_six_through_nine_confirm_and_five_or_ten_do_not(self) -> None:
        for interior in (6, 7, 8, 9):
            close_row = interior + 1
            bars = self._series(close_row)
            for index in range(7, close_row):
                bars[index] = _bar(index, "103", "103.2", "102.4", "102.5")
            bars[close_row] = _bar(close_row, "102.1", "102.2", "101.8", "102")
            result = analyze(bars)
            high = next(item for item in result.raw if item.open_row == 0 and item.direction == "SWING_HIGH")
            self.assertEqual(high.interior, interior)
            self.assertEqual(high.total, interior + 2)
            self.assertEqual(high.close_row, close_row)
            self.assertEqual(high.close_row - high.open_row, interior + 1)
            self.assertGreaterEqual(high.close_row - high.open_row, FIRST_CLOSE_OFFSET)
            self.assertLessEqual(high.close_row - high.open_row, LAST_CLOSE_OFFSET)
            self.assertEqual(high.penetration_fraction, THIRD)
            self.assertEqual(candle_direction(bars[0].open, bars[0].close), "BULLISH")
            self.assertEqual(candle_direction(bars[close_row].open, bars[close_row].close), "BEARISH")
        early = self._series(12)
        early[6] = _bar(6, "102", "102.2", "90", "90")
        for index in range(7, 11):
            early[index] = _bar(index, "103", "103.2", "102.4", "102.5")
        early[11] = _bar(11, "102", "102.2", "90", "90")
        blocked = analyze(early)
        record = next(item for item in blocked.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertEqual(record.terminal, "SEARCH_HORIZON_EXHAUSTED_AT_9_INTERIORS")
        self.assertEqual(record.evaluated_closes, 4)
        self.assertEqual(record.max_interior, 9)
        self.assertEqual(record.above_horizon, 0)
        self.assertFalse(record.directionally_bound)
        self.assertFalse(any(item.open_row == 0 and item.direction == "SWING_HIGH" for item in blocked.raw))
        self.assertEqual(blocked.counters["candidate_above_9_interiors_evaluated"], 0)
        self.assertEqual(blocked.counters["candidate_with_10_interiors_confirmed"], 0)
        self.assertEqual(blocked.counters["candidate_below_6_interiors_confirmed"], 0)
        self.assertNotIn("10", blocked.counters["raw_by_interior"])

    def test_pre_six_cross_cannot_bind_and_six_interior_close_is_first_eligible(self) -> None:
        bars = self._series(8)
        bars[6] = _bar(6, "102", "102.2", "90", "90")
        bars[7] = _bar(7, "102.1", "102.2", "101.8", "102")
        result = analyze(bars)
        high = next(item for item in result.raw if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertEqual(high.close_row, 7)
        self.assertEqual(high.interior, 6)
        self.assertEqual(high.total, 8)
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertEqual(record.evaluated_closes, 1)
        self.assertEqual(record.max_interior, 6)


class BodyTests(unittest.TestCase):
    def test_body_uses_open_and_close_only(self) -> None:
        low, high, size = swing_open_body(Decimal("103"), Decimal("100"))
        self.assertEqual((low, high, size), (Decimal("100"), Decimal("103"), Decimal("3")))
        threshold = body_threshold("SWING_HIGH", low, high, size)
        self.assertEqual(threshold, Decimal("102"))
        low_threshold = body_threshold("SWING_LOW", low, high, size)
        self.assertEqual(low_threshold, Decimal("101"))
        self.assertTrue(body_penetrated("SWING_HIGH", Decimal("102"), threshold))
        self.assertFalse(body_penetrated("SWING_HIGH", Decimal("102.00000001"), threshold))
        self.assertTrue(body_penetrated("SWING_LOW", Decimal("101"), low_threshold))
        self.assertEqual(penetration_fraction("SWING_HIGH", Decimal("102"), low, high, size), THIRD)

    def test_close_body_size_is_irrelevant_and_doji_fails(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "130", "90", "103")
        bars[4] = _bar(4, "100", "120", "99", "100")
        bars[8] = _bar(8, "102.1", "102.2", "101.8", "102")
        result = analyze(bars)
        confirmed = [item for item in result.raw if item.open_row == 0 and item.direction == "SWING_HIGH"]
        self.assertEqual(len(confirmed), 1)
        self.assertEqual(confirmed[0].close_row, 8)
        doji = _quiet(12)
        doji[0] = _bar(0, "100", "110", "90", "100")
        doji_result = analyze(doji)
        terminals = {item.terminal for item in doji_result.searches if item.open_row == 0}
        self.assertEqual(terminals, {"SWING_OPEN_BODY_ZERO"})

    def test_wick_only_does_not_bind_and_failed_binding_is_not_replaced(self) -> None:
        bars = _quiet(13)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[4] = _bar(4, "100", "100.2", "99", "100")
        bars[8] = _bar(8, "102", "102.2", "101", "101")
        bars[9] = _bar(9, "100", "140", "99", "101")
        result = analyze(bars)
        self.assertFalse(any(item.open_row == 0 and item.direction == "SWING_HIGH" for item in result.raw))
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertTrue(record.binding_failed)
        self.assertIn(record.terminal, {"OPEN_BOUNDARY_BELOW_0_90", "BOTH_BOUNDARIES_BELOW_MINIMUM", "CLOSE_BOUNDARY_BELOW_0_90"})


class BoundaryTests(unittest.TestCase):
    def test_minimum_is_inclusive_0_90_and_has_no_maximum(self) -> None:
        self.assertIsNone(MAX_BOUNDARY)
        self.assertIsNone(boundary_reason(Decimal("0.90"), Decimal("0.90")))
        self.assertEqual(boundary_reason(Decimal("0.89999999"), Decimal("0.89999999")), "BOTH_BOUNDARIES_BELOW_MINIMUM")
        self.assertEqual(boundary_reason(Decimal("0.80"), Decimal("0.80")), "BOTH_BOUNDARIES_BELOW_MINIMUM")
        self.assertIsNone(boundary_reason(Decimal("1.30"), Decimal("3.50")))
        self.assertIsNone(boundary_reason(Decimal("5"), Decimal("10")))
        self.assertEqual(boundary_reason(Decimal("0.89"), Decimal("2")), "OPEN_BOUNDARY_BELOW_0_90")
        self.assertTrue(FORBIDDEN_TERMINALS.isdisjoint({"OPEN_BOUNDARY_BELOW_0_90", "BOTH_BOUNDARIES_BELOW_MINIMUM"}))
        self.assertIn("BOUNDARY_ABOVE_MAXIMUM", FORBIDDEN_TERMINALS)


class StructureTests(unittest.TestCase):
    def test_primary_nested_and_duplicate_rules(self) -> None:
        def swing(raw: str, open_row: int, close_row: int, extreme: int, price: str) -> Swing:
            return Swing(
                raw, "", "SWING_HIGH", open_row, close_row, extreme, Decimal(price),
                extreme, extreme, f"SWING_HIGH|{price}|{extreme}|{extreme}", "",
                Decimal("100"), Decimal("100"), close_row - open_row - 1, close_row - open_row + 1,
                Decimal("1"), Decimal("1"), Decimal("0.1"), Decimal("120"), Decimal("90"),
                "PRIMARY", "PRIMARY", FORMATION_CLASS, Decimal("100"), Decimal("103"), Decimal("3"),
                Decimal("102"), THIRD,
            )
        raw = [
            swing("S30-RAW-000003", 0, 11, 6, "120"),
            swing("S30-RAW-000001", 1, 9, 4, "130"),
            swing("S30-RAW-000002", 2, 11, 7, "140"),
        ]
        _suppress_overlap(raw)
        by_open = {item.open_row: item.status for item in raw}
        self.assertEqual(by_open[1], "PRIMARY")
        self.assertEqual(by_open[0], "SUPPRESSED_LONGER_OVERLAPPING_STRUCTURE")
        self.assertEqual(by_open[2], "SUPPRESSED_NESTED_STRUCTURE")

    def test_same_extreme_family_keeps_shortest(self) -> None:
        bars = _quiet(20)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[1] = _bar(1, "100", "104", "99", "103")
        for index in (4, 5):
            bars[index] = _bar(index, "100", "110", "99", "100")
        bars[8] = _bar(8, "102", "102.2", "100", "101")
        bars[9] = _bar(9, "102", "102.2", "100", "101")
        result = analyze(bars)
        family = [item for item in result.raw if item.extreme_price == Decimal("110") and item.direction == "SWING_HIGH"]
        primaries = [item for item in family if item.status == "PRIMARY"]
        self.assertEqual(len(primaries), 1)
        self.assertEqual(primaries[0].interior, 6)
        self.assertEqual(primaries[0].open_row, 1)
        self.assertEqual(primaries[0].close_row, 8)
        self.assertTrue(all(item.status != "PRIMARY" or item is primaries[0] for item in family))


class WorkbookTests(unittest.TestCase):
    def test_workbook_contract_and_revision(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[4] = _bar(4, "100", "110", "99", "100")
        bars[8] = _bar(8, "102", "102.2", "100", "101")
        result = analyze(bars)
        self.assertGreaterEqual(len(result.displayed), 1)
        self.assertTrue(all(
            candle_direction(result.bars[item.open_row].open, result.bars[item.open_row].close) == "BULLISH"
            and candle_direction(result.bars[item.close_row].open, result.bars[item.close_row].close) == "BEARISH"
            for item in result.displayed if item.direction == "SWING_HIGH"
        ))
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "sample.xlsx"
            write_workbook(path, result)
            validation = validate_workbook(path, result)
            self.assertEqual(validation["sheets"], ["Special Swings", "Swing Highs", "Swing Lows", "Parameters"])
            self.assertEqual(dict(USER_PARAMETERS)["Minimum Boundary Percent"], "0.90%")
            self.assertEqual(dict(USER_PARAMETERS)["Minimum Interior Candles"], "6")
            self.assertEqual(dict(USER_PARAMETERS)["Maximum Interior Candles"], "9")
            self.assertEqual(dict(USER_PARAMETERS)["Minimum Total Formation Candles"], "8")
            self.assertEqual(dict(USER_PARAMETERS)["Maximum Total Formation Candles"], "11")
            self.assertEqual(dict(USER_PARAMETERS)["First Eligible Swing Close Offset"], "open_row + 7")
            self.assertEqual(dict(USER_PARAMETERS)["Final Eligible Swing Close Offset"], "open_row + 10")
            self.assertNotIn("7_TO_10", CONFIGURATION_VERSION)
            self.assertNotIn("7–10", " ".join(value for _, value in USER_PARAMETERS))
            self.assertEqual(dict(USER_PARAMETERS)["Swing High Reference Selection"], "Lowest Bearish Close With Immediate Bullish Interior Successor")
            self.assertEqual(dict(USER_PARAMETERS)["Swing Low Reference Selection"], "Highest Bullish Close With Immediate Bearish Interior Successor")
            self.assertEqual(dict(USER_PARAMETERS)["Pre-Close Interior Candle Eligible As Reference"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Reference Row Maximum"], "close_row - 2")
            self.assertEqual(dict(USER_PARAMETERS)["Reference OHLC Exported"], "Open, High, Low, Close")
            self.assertEqual(dict(USER_PARAMETERS)["Swing High Reference-To-Close Rule"], "Reference Close >= Swing Close Close")
            self.assertEqual(dict(USER_PARAMETERS)["Swing Low Reference-To-Close Rule"], "Reference Close <= Swing Close Close")
            self.assertEqual(dict(USER_PARAMETERS)["Exact Equality Passes"], "Yes")
            self.assertEqual(dict(USER_PARAMETERS)["Failed Compatibility Binds"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Search Continues After Compatibility Failure"], "Yes")
            self.assertEqual(dict(USER_PARAMETERS)["Search Extension Beyond Nine Interiors"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Reference Recalculated For Every Close"], "Yes")
            self.assertEqual(dict(USER_PARAMETERS)["Second-Best Reference May Replace Failed Min/Max"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Missing Reference Pair Rejects Swing"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Missing Pair Compatibility Status"], "Not Applicable")
            self.assertEqual(dict(USER_PARAMETERS)["Pair Is Primary Selection Input"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Swing High Reference Direction"], "Bearish")
            self.assertEqual(dict(USER_PARAMETERS)["Swing High Validation Direction"], "Bullish")
            self.assertEqual(dict(USER_PARAMETERS)["Swing Low Reference Direction"], "Bullish")
            self.assertEqual(dict(USER_PARAMETERS)["Swing Low Validation Direction"], "Bearish")
            self.assertEqual(dict(USER_PARAMETERS)["Reference-To-Validation Row Difference"], "1")
            self.assertEqual(dict(USER_PARAMETERS)["Final Interior Candle Eligible As Reference"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Swing Open Eligible In Reference Pair"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Swing Close Eligible In Reference Pair"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Reference Wicks Used"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Validation Candle Close Used For Selection"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Geometric Middle Required"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Missing Pair Rejects Swing"], "No")
            self.assertNotIn("Lowest Close Among Bearish Interior Candles", " ".join(value for _, value in USER_PARAMETERS))
            self.assertNotIn("0.80%", " ".join(value for _, value in USER_PARAMETERS))
            self.assertEqual(len(RESULT_COLUMNS), 59)
            self.assertEqual(RESULT_COLUMNS[7], "Swing Open Direction")
            self.assertEqual(RESULT_COLUMNS[14], "Swing Close Direction")
            self.assertEqual(RESULT_COLUMNS[19], "Earlier Swing Close Candidates Rejected By Reference Compatibility")
            self.assertEqual(RESULT_COLUMNS[24], "Interior Body Reference Pair Type")
            self.assertEqual(RESULT_COLUMNS[25], "Interior Body Reference Selection Rule")
            self.assertEqual(RESULT_COLUMNS[29], "Interior Body Reference Open")
            self.assertEqual(RESULT_COLUMNS[30], "Interior Body Reference High")
            self.assertEqual(RESULT_COLUMNS[31], "Interior Body Reference Low")
            self.assertEqual(RESULT_COLUMNS[32], "Interior Body Reference Close")
            self.assertEqual(RESULT_COLUMNS[40], "Interior Body Reference Is Immediately Before Swing Close")
            self.assertEqual(RESULT_COLUMNS[41], "Interior Body Reference Rows Before Swing Close")
            self.assertEqual(RESULT_COLUMNS[42], "Reference To Swing Close Compatibility Status")
            self.assertEqual(RESULT_COLUMNS[43], "Reference To Swing Close Margin")
            self.assertEqual(RESULT_COLUMNS[44], "Reference To Swing Close Margin Percent")
            self.assertEqual(RESULT_COLUMNS[45], "Reference To Validation Row Difference")
            self.assertEqual(dict(USER_PARAMETERS)["Swing High Swing Open Direction"], "Bullish")
            self.assertEqual(dict(USER_PARAMETERS)["Swing High Swing Close Direction"], "Bearish")
            self.assertEqual(dict(USER_PARAMETERS)["Swing Low Swing Open Direction"], "Bearish")
            self.assertEqual(dict(USER_PARAMETERS)["Swing Low Swing Close Direction"], "Bullish")
            self.assertEqual(dict(USER_PARAMETERS)["Wrong-Direction Threshold Crossing Binds"], "No")
            self.assertEqual(dict(USER_PARAMETERS)["Search Continues After Wrong Direction"], "Yes, Within 6–9 Interior Horizon")
            self.assertIn("Interior Body Reference Price", RESULT_COLUMNS)
            desktop = Path(folder)
            (desktop / "BTCUSDT_30M_Special_Swings_rev00.xlsx").write_bytes(b"x")
            (desktop / "BTCUSDT_30M_Special_Swings_rev02.xlsx").write_bytes(b"x")
            revision, target = allocate_revision(desktop)
            self.assertEqual(revision, 3)
            self.assertFalse(target.exists())


class BodyReferenceTests(unittest.TestCase):
    def _bars(self) -> list[Bar]:
        return [_bar(index, "100", "101", "99", "100") for index in range(9)]

    def test_swing_high_pair_rules(self) -> None:
        bars = self._bars()
        bars[0] = _bar(0, "80", "81", "70", "70")
        bars[1] = _bar(1, "100", "110", "90", "95")
        bars[2] = _bar(2, "94", "120", "93", "101")
        bars[3] = _bar(3, "100", "140", "80", "90")
        bars[4] = _bar(4, "90", "91", "70", "96")
        bars[5] = _bar(5, "80", "130", "70", "70")
        bars[6] = _bar(6, "70", "71", "69", "70")
        bars[7] = _bar(7, "60", "61", "40", "50")
        bars[8] = _bar(8, "50", "80", "49", "79")
        chosen = select_interior_body_reference(bars, 0, 8, "SWING_HIGH", 1, Decimal("140"))
        self.assertEqual(chosen["row"], 3)
        self.assertEqual(chosen["validation_row"], 4)
        self.assertEqual(chosen["row_difference"], 1)
        self.assertEqual(chosen["price"], Decimal("90"))
        self.assertEqual(chosen["direction"], "BEARISH")
        self.assertEqual(chosen["validation_direction"], "BULLISH")
        self.assertEqual(chosen["pair_type"], "BEARISH_TO_BULLISH_INTERIOR_PAIR")
        self.assertEqual(chosen["status"], "SWING_HIGH_BEARISH_TO_BULLISH_REFERENCE_PAIR_SELECTED")
        self.assertEqual(chosen["rule"], "MINIMUM_BEARISH_CLOSE_WITH_IMMEDIATE_BULLISH_INTERIOR_SUCCESSOR")
        self.assertNotEqual(chosen["price"], Decimal("70"))
        self.assertNotEqual(chosen["price"], Decimal("50"))
        self.assertNotEqual(chosen["price"], bars[4].close)
        self.assertGreater(chosen["successor_wrong_direction"], 0)
        self.assertGreater(chosen["successor_not_interior"], 0)
        self.assertNotEqual(chosen["position_fraction"], Decimal("0.5"))
        self.assertGreaterEqual(chosen["distance"], 0)

    def test_swing_low_pair_rules(self) -> None:
        bars = self._bars()
        bars[0] = _bar(0, "70", "80", "69", "79")
        bars[1] = _bar(1, "90", "130", "89", "101")
        bars[2] = _bar(2, "101", "102", "80", "95")
        bars[3] = _bar(3, "90", "140", "89", "110")
        bars[4] = _bar(4, "110", "111", "109", "111")
        bars[5] = _bar(5, "80", "81", "70", "96")
        bars[6] = _bar(6, "96", "97", "90", "90")
        bars[7] = _bar(7, "90", "120", "89", "119")
        bars[8] = _bar(8, "119", "120", "80", "80")
        chosen = select_interior_body_reference(bars, 0, 8, "SWING_LOW", 5, Decimal("70"))
        self.assertEqual(chosen["row"], 1)
        self.assertEqual(chosen["validation_row"], 2)
        self.assertEqual(chosen["price"], Decimal("101"))
        self.assertEqual(chosen["direction"], "BULLISH")
        self.assertEqual(chosen["validation_direction"], "BEARISH")
        self.assertEqual(chosen["pair_type"], "BULLISH_TO_BEARISH_INTERIOR_PAIR")
        self.assertEqual(chosen["status"], "SWING_LOW_BULLISH_TO_BEARISH_REFERENCE_PAIR_SELECTED")
        self.assertNotEqual(chosen["price"], Decimal("110"))
        self.assertNotEqual(chosen["price"], Decimal("119"))
        self.assertNotEqual(chosen["price"], bars[2].close)
        self.assertGreater(chosen["successor_wrong_direction"], 0)
        self.assertGreater(chosen["successor_not_interior"], 0)
        self.assertGreaterEqual(chosen["distance"], 0)

    def test_exact_close_tie_selects_the_last_eligible_pair(self) -> None:
        bars = self._bars()
        bars[1] = _bar(1, "100", "101", "90", "95")
        bars[2] = _bar(2, "95", "96", "94", "96")
        bars[4] = _bar(4, "100", "101", "88", "95")
        bars[5] = _bar(5, "95", "97", "94", "96")
        bars[6] = _bar(6, "99", "100", "70", "95")
        bars[7] = _bar(7, "95", "96", "94", "95")
        chosen = select_interior_body_reference(bars, 0, 8, "SWING_HIGH", 6, Decimal("101"))
        self.assertEqual(chosen["row"], 4)
        self.assertEqual(chosen["validation_row"], 5)
        self.assertEqual(chosen["tie_count"], 2)
        self.assertEqual(chosen["tie_first"], 1)
        self.assertEqual(chosen["tie_last"], 4)
        self.assertEqual(chosen["status"], "INTERIOR_BODY_REFERENCE_PAIR_EXACT_CLOSE_TIE")
        low = self._bars()
        low[1] = _bar(1, "90", "91", "89", "106")
        low[2] = _bar(2, "106", "107", "90", "100")
        low[4] = _bar(4, "90", "91", "89", "106")
        low[5] = _bar(5, "106", "107", "90", "100")
        low[6] = _bar(6, "90", "91", "89", "106")
        low_tie = select_interior_body_reference(low, 0, 8, "SWING_LOW", 2, Decimal("90"))
        self.assertEqual(low_tie["row"], 4)
        self.assertEqual(low_tie["validation_row"], 5)
        self.assertEqual(low_tie["direction"], "BULLISH")
        self.assertEqual(low_tie["tie_count"], 2)

    def test_display_rounding_does_not_create_a_tie(self) -> None:
        bars = self._bars()
        bars[1] = _bar(1, "102", "103", "100", "101.000000001")
        bars[2] = _bar(2, "101", "102", "100", "102")
        bars[4] = _bar(4, "102", "103", "90", "101.000000002")
        bars[5] = _bar(5, "101", "102", "100", "102")
        chosen = select_interior_body_reference(bars, 0, 8, "SWING_HIGH", 4, Decimal("103"))
        self.assertEqual(chosen["row"], 1)
        self.assertEqual(chosen["tie_count"], 1)
        self.assertEqual(chosen["price"], Decimal("101.000000001"))

    def test_missing_pair_does_not_reject_a_valid_swing(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "103", "104", "99.5", "100")
        for index in range(1, 8):
            bars[index] = _bar(index, "97", "100.2", "90" if index == 3 else "96", "96")
        bars[8] = _bar(8, "100.5", "102", "100", "101")
        result = analyze(bars)
        low = next(item for item in result.raw if item.open_row == 0 and item.direction == "SWING_LOW")
        self.assertEqual(low.body_reference_status, "NO_BULLISH_TO_BEARISH_INTERIOR_PAIR_FOR_SWING_LOW")
        self.assertIsNone(low.body_reference_price)
        self.assertIsNone(low.body_reference_validation_row)
        self.assertEqual(low.body_reference_tie_count, 0)
        self.assertEqual(low.body_reference_eligible_count, 0)
        self.assertEqual(low.extreme_price, Decimal("90"))
        self.assertIn(low.status, {"PRIMARY", "DERIVED_SAME_EXTREME"})
        displayed = [item for item in result.displayed if item.direction == "SWING_LOW"]
        self.assertTrue(displayed)
        self.assertTrue(all(
            item.body_reference_status == "NO_BULLISH_TO_BEARISH_INTERIOR_PAIR_FOR_SWING_LOW" for item in displayed
        ))
        self.assertEqual(result.counters["missing_pair_rejected_swing_count"], 0)
        self.assertEqual(low.compatibility_status, "NOT_APPLICABLE_NO_REFERENCE_PAIR")
        self.assertIsNone(low.reference_to_swing_close_margin)
        self.assertIsNone(low.reference_to_swing_close_margin_percent)

    def test_swing_low_reference_is_not_substituted_into_the_boundary(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "101", "99.5", "97")
        for index in range(1, 8):
            bars[index] = _bar(index, "100", "100.2", "99.8", "100")
        bars[1] = _bar(1, "90", "140", "99", "120")
        bars[3] = _bar(3, "94", "100.1", "90", "96")
        bars[5] = _bar(5, "96", "100.2", "95.5", "99")
        bars[6] = _bar(6, "99", "99.2", "94", "95")
        bars[8] = _bar(8, "100.4", "102", "100", "101")
        result = analyze(bars)
        low = next(item for item in result.raw if item.open_row == 0 and item.direction == "SWING_LOW")
        self.assertEqual(low.extreme_row, 3)
        self.assertEqual(low.extreme_price, Decimal("90"))
        self.assertEqual(low.body_reference_row, 5)
        self.assertEqual(low.body_reference_validation_row, 6)
        self.assertEqual(low.body_reference_price, Decimal("99"))
        self.assertNotEqual(low.body_reference_price, Decimal("120"))
        self.assertNotEqual(low.body_reference_price, Decimal("95"))
        self.assertEqual(low.body_reference_direction, "BULLISH")
        self.assertEqual(low.body_reference_validation_direction, "BEARISH")
        self.assertFalse(low.body_reference_matches_extremum)
        self.assertEqual(low.extremum_to_body_reference_distance, Decimal("9"))
        self.assertEqual(low.open_width_percent, Decimal("10"))
        self.assertNotEqual(low.open_width_percent, Decimal("1"))
        self.assertEqual(low.body_reference_bars_after_open, 5)
        self.assertEqual(low.body_reference_bars_before_close, 3)

    def test_confirmed_high_keeps_wick_extremum_and_can_match_it(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        for index in range(1, 8):
            bars[index] = _bar(index, "100", "100.2", "99.9", "100")
        bars[1] = _bar(1, "100", "101", "99.9", "105")
        bars[2] = _bar(2, "110", "111", "100", "100")
        bars[3] = _bar(3, "110", "112", "108", "109")
        bars[4] = _bar(4, "109", "111", "108", "110")
        bars[8] = _bar(8, "102.4", "102.6", "101.5", "102")
        result = analyze(bars)
        high = next(item for item in result.raw if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertEqual(high.extreme_row, 3)
        self.assertEqual(high.extreme_price, Decimal("112"))
        self.assertEqual(high.body_reference_row, 3)
        self.assertEqual(high.body_reference_validation_row, 4)
        self.assertEqual(high.body_reference_price, Decimal("109"))
        self.assertEqual(high.body_reference_direction, "BEARISH")
        self.assertEqual(high.body_reference_validation_direction, "BULLISH")
        self.assertTrue(high.body_reference_matches_extremum)
        self.assertFalse(high.body_reference_validation_matches_extremum)
        self.assertEqual(high.extremum_to_body_reference_distance, Decimal("3"))
        self.assertNotEqual(high.body_reference_price, Decimal("100"))
        self.assertNotEqual(high.body_reference_price, Decimal("105"))

    def test_primary_selection_ignores_body_reference_and_does_not_repaint(self) -> None:
        def swing(raw: str) -> Swing:
            return Swing(
                raw, "", "SWING_HIGH", 0, 8, 4, Decimal("110"),
                4, 4, "SWING_HIGH|110|4|4", "",
                Decimal("100"), Decimal("101"), 7, 9,
                Decimal("1"), Decimal("1"), Decimal("0.1"), Decimal("110"), Decimal("90"),
                "PRIMARY", "PRIMARY", FORMATION_CLASS, Decimal("100"), Decimal("103"), Decimal("3"),
                Decimal("102"), THIRD,
            )

        left = swing("S30-RAW-000001")
        right = swing("S30-RAW-000002")
        right.body_reference_row = 1
        right.body_reference_price = Decimal("1")
        right.body_reference_status = "NO_BEARISH_INTERIOR_CANDLE_FOR_SWING_HIGH"
        self.assertEqual(_primary_key(left)[:6], _primary_key(right)[:6])
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[4] = _bar(4, "100", "110", "99", "100")
        bars[8] = _bar(8, "102", "102.2", "100", "101")
        changed = list(bars)
        changed[11] = _bar(11, "100", "180", "90", "170")
        self.assertEqual(signature(analyze(bars)), signature(analyze(changed)))
        self.assertEqual(analyze(bars).counters["body_reference_repaint_count"], 0)

    def test_selected_reference_workbook_cells_match_the_source_candle(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "101", "99.5", "97")
        for index in range(1, 8):
            bars[index] = _bar(index, "100", "100.2", "99.8", "100")
        bars[3] = _bar(3, "94", "100.1", "90", "96")
        bars[4] = _bar(4, "96", "100.2", "95.5", "99")
        bars[5] = _bar(5, "99", "99.2", "94", "95")
        bars[7] = _bar(7, "97", "97.2", "94.8", "95")
        bars[8] = _bar(8, "100.4", "102", "100", "101")
        result = analyze(bars)
        self.assertTrue(any(item.body_reference_row is not None for item in result.displayed))
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "reference.xlsx"
            write_workbook(path, result)
            validate_workbook(path, result)


class DirectionTests(unittest.TestCase):
    def test_direction_uses_exact_open_and_close_only(self) -> None:
        self.assertEqual(candle_direction(Decimal("100"), Decimal("100.000000001")), "BULLISH")
        self.assertEqual(candle_direction(Decimal("100"), Decimal("99.999999999")), "BEARISH")
        self.assertEqual(candle_direction(Decimal("100"), Decimal("100")), "DOJI")
        bullish_wick = _bar(0, "100", "140", "99", "101")
        bearish_wick = _bar(1, "100", "140", "70", "99")
        self.assertEqual(candle_direction(bullish_wick.open, bullish_wick.close), "BULLISH")
        self.assertEqual(candle_direction(bearish_wick.open, bearish_wick.close), "BEARISH")

    def test_wrong_open_direction_never_seeds_and_doji_fails_both(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "103", "104", "99", "100")
        bearish_open = analyze(bars)
        high = next(item for item in bearish_open.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        low = next(item for item in bearish_open.searches if item.open_row == 0 and item.direction == "SWING_LOW")
        self.assertEqual(high.terminal, "SWING_HIGH_OPEN_NOT_BULLISH")
        self.assertTrue(low.open_seed)
        self.assertFalse(any(item.open_row == 0 and item.direction == "SWING_HIGH" for item in bearish_open.raw))
        bars[0] = _bar(0, "100", "104", "99", "103")
        bullish_open = analyze(bars)
        high = next(item for item in bullish_open.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        low = next(item for item in bullish_open.searches if item.open_row == 0 and item.direction == "SWING_LOW")
        self.assertTrue(high.open_seed)
        self.assertEqual(low.terminal, "SWING_LOW_OPEN_NOT_BEARISH")
        doji = _quiet(12)
        doji[0] = _bar(0, "100", "110", "90", "100")
        doji_result = analyze(doji)
        self.assertTrue(all(item.terminal == "SWING_OPEN_BODY_ZERO" for item in doji_result.searches if item.open_row == 0))

    def test_wrong_direction_threshold_crossing_does_not_bind_and_search_continues(self) -> None:
        bars = _quiet(13)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[4] = _bar(4, "100", "110", "99", "100")
        bars[8] = _bar(8, "100", "101", "99", "101")
        bars[9] = _bar(9, "102", "102.2", "100", "101")
        result = analyze(bars)
        high = next(item for item in result.raw if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertEqual(high.close_row, 9)
        self.assertEqual(candle_direction(bars[9].open, bars[9].close), "BEARISH")
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertGreaterEqual(record.wrong_direction_crossings, 1)
        self.assertTrue(record.directionally_bound)
        self.assertEqual(result.counters["wrong_direction_threshold_crossing_bound_count"], 0)

    def test_doji_and_bullish_close_cannot_confirm_swing_high(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[4] = _bar(4, "100", "110", "99", "100")
        bars[8] = _bar(8, "101", "102", "100", "101")
        bars[9] = _bar(9, "100", "102", "99", "101.5")
        bars[10] = _bar(10, "100", "101", "99", "100")
        bars[11] = _bar(11, "100", "101", "99", "100")
        result = analyze(bars)
        self.assertFalse(any(item.open_row == 0 and item.direction == "SWING_HIGH" for item in result.raw))
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertGreater(record.close_doji_crossings, 0)
        self.assertFalse(record.directionally_bound)

    def test_bearish_close_cannot_confirm_swing_low_and_later_bullish_close_binds(self) -> None:
        bars = _quiet(13)
        bars[0] = _bar(0, "103", "104", "99", "100")
        bars[4] = _bar(4, "100", "101", "90", "100")
        bars[8] = _bar(8, "102", "103", "99", "101")
        bars[9] = _bar(9, "100.5", "102", "100", "101")
        result = analyze(bars)
        low = next(item for item in result.raw if item.open_row == 0 and item.direction == "SWING_LOW")
        self.assertEqual(low.close_row, 9)
        self.assertEqual(candle_direction(bars[0].open, bars[0].close), "BEARISH")
        self.assertEqual(candle_direction(bars[9].open, bars[9].close), "BULLISH")
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_LOW")
        self.assertGreaterEqual(record.wrong_direction_crossings, 1)

    def test_failed_directional_binding_is_not_replaced(self) -> None:
        bars = _quiet(13)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[8] = _bar(8, "102", "102.2", "101", "101")
        bars[9] = _bar(9, "102", "140", "99", "101")
        result = analyze(bars)
        self.assertFalse(any(item.open_row == 0 and item.direction == "SWING_HIGH" for item in result.raw))
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertTrue(record.binding_failed)
        self.assertTrue(record.directionally_bound)
        self.assertEqual(record.evaluated_closes, 2)
        self.assertEqual(record.max_interior, 7)

    def test_direction_and_wick_rules_at_six_and_nine_interior_boundaries(self) -> None:
        six = _quiet(12)
        six[0] = _bar(0, "100", "104", "99", "103")
        six[3] = _bar(3, "100", "110", "99", "105")
        six[7] = _bar(7, "100", "101", "90", "101")
        six_result = analyze(six)
        six_record = next(item for item in six_result.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertGreaterEqual(six_record.wrong_direction_crossings, 1)
        self.assertFalse(any(item.open_row == 0 and item.direction == "SWING_HIGH" for item in six_result.raw))
        self.assertEqual(six_record.terminal, "SEARCH_HORIZON_EXHAUSTED_AT_9_INTERIORS")
        nine = _quiet(12)
        nine[0] = _bar(0, "100", "130", "90", "103")
        nine[4] = _bar(4, "100", "120", "99", "100")
        nine[7] = _bar(7, "103", "103.2", "90", "102.5")
        nine[10] = _bar(10, "102.1", "102.2", "101.8", "102")
        nine_result = analyze(nine)
        high = next(item for item in nine_result.raw if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertEqual(high.interior, 9)
        self.assertEqual(high.total, 11)
        self.assertEqual(high.close_row, 10)
        self.assertEqual(high.penetration_fraction, THIRD)
        self.assertEqual(candle_direction(nine[10].open, nine[10].close), "BEARISH")
        record = next(item for item in nine_result.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertGreaterEqual(record.wick_only, 1)
        self.assertEqual(nine_result.counters["wick_only_confirmations"], 0)
        low_bars = _quiet(12)
        low_bars[0] = _bar(0, "103", "104", "99", "100")
        low_bars[4] = _bar(4, "100", "101", "90", "100")
        low_bars[7] = _bar(7, "102", "103", "99", "101")
        low_bars[10] = _bar(10, "100.5", "102", "100", "101")
        low = next(item for item in analyze(low_bars).raw if item.open_row == 0 and item.direction == "SWING_LOW")
        self.assertEqual(low.interior, 9)
        self.assertEqual(low.close_row, 10)
        self.assertEqual(candle_direction(low_bars[0].open, low_bars[0].close), "BEARISH")
        self.assertEqual(candle_direction(low_bars[10].open, low_bars[10].close), "BULLISH")


class CompatibilityTests(unittest.TestCase):
    def test_swing_high_failure_continues_and_keeps_the_minimum_reference(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[1] = _bar(1, "100", "101", "89", "90")
        bars[2] = _bar(2, "90", "111", "89", "110")
        bars[3] = _bar(3, "112", "113", "109", "110")
        bars[4] = _bar(4, "110", "111", "109", "111")
        bars[7] = _bar(7, "102", "103", "99", "100")
        bars[8] = _bar(8, "100", "112", "99", "111")
        bars[9] = _bar(9, "100", "101", "88", "89")
        result = analyze(bars)
        high = next(item for item in result.raw if item.open_row == 0 and item.direction == "SWING_HIGH")
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertEqual(high.close_row, 9)
        self.assertEqual(high.interior, 8)
        self.assertLessEqual(high.interior, 9)
        self.assertEqual(high.body_reference_row, 1)
        self.assertEqual(high.body_reference_price, Decimal("90"))
        self.assertNotEqual(high.body_reference_price, Decimal("110"))
        self.assertNotEqual(high.body_reference_price, Decimal("100"))
        self.assertEqual(high.compatibility_status, "PASS")
        self.assertEqual(high.reference_to_swing_close_margin, Decimal("1"))
        self.assertGreater(high.reference_to_swing_close_margin, 0)
        self.assertEqual(high.earlier_compatibility_rejections, 1)
        self.assertTrue(high.final_reference_matches_provisional)
        self.assertLess(high.open_row, 7)
        self.assertLess(7, high.close_row)
        self.assertFalse(record.binding_failed)
        self.assertTrue(record.directionally_bound)
        self.assertEqual(record.compatibility_rejected, 1)
        self.assertEqual(record.reference_below_close, 1)
        self.assertEqual(record.reference_recalculations, 2)
        self.assertEqual(record.max_interior, 8)
        self.assertEqual(record.above_horizon, 0)
        self.assertEqual(record.compatibility_audit[0]["status"], "FAIL")
        self.assertEqual(record.compatibility_audit[0]["code"], "REFERENCE_CLOSE_BELOW_SWING_CLOSE_FOR_SWING_HIGH")
        self.assertEqual(record.compatibility_audit[0]["reference_price"], Decimal("90"))
        self.assertEqual(record.compatibility_audit[0]["eligible_count"], 2)
        self.assertEqual(record.compatibility_audit[1]["status"], "PASS")
        self.assertEqual(record.compatibility_audit[1]["eligible_count"], 3)
        self.assertGreater(record.compatibility_audit[1]["eligible_count"], record.compatibility_audit[0]["eligible_count"])
        self.assertEqual(record.compatibility_audit[1]["reference_row"], 1)
        self.assertNotEqual(record.compatibility_audit[0]["close_row"], record.compatibility_audit[1]["close_row"])
        self.assertEqual(signature(result), signature(analyze(bars)))
        self.assertEqual(result.counters["compatibility_failed_candidate_bound_count"], 0)
        self.assertEqual(result.counters["second_best_reference_substitution_count"], 0)
        self.assertEqual(result.counters["final_reference_mismatch_count"], 0)
        self.assertEqual(result.counters["search_beyond_nine_interiors_count"], 0)

    def test_swing_high_exact_equality_passes(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[1] = _bar(1, "102", "111", "100", "101")
        bars[2] = _bar(2, "101", "111", "100", "110")
        bars[7] = _bar(7, "102", "103", "100", "101")
        high = next(item for item in analyze(bars).raw if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertEqual(high.close_row, 7)
        self.assertEqual(high.body_reference_price, Decimal("101"))
        self.assertEqual(high.close_price, Decimal("101"))
        self.assertEqual(high.reference_to_swing_close_margin, Decimal("0"))
        self.assertEqual(high.compatibility_status, "PASS")
        self.assertEqual(high.earlier_compatibility_rejections, 0)

    def test_swing_high_rejections_stop_at_nine_interiors(self) -> None:
        bars = _quiet(14)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[1] = _bar(1, "100", "111", "89", "90")
        bars[2] = _bar(2, "90", "111", "89", "110")
        for index in range(7, 12):
            bars[index] = _bar(index, "102", "103", "99", "100")
        result = analyze(bars)
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertEqual(record.terminal, "SEARCH_HORIZON_EXHAUSTED_AT_9_INTERIORS")
        self.assertEqual(record.compatibility_rejected, 4)
        self.assertEqual(record.max_interior, 9)
        self.assertEqual(record.above_horizon, 0)
        self.assertFalse(record.directionally_bound)
        self.assertFalse(record.binding_failed)
        self.assertTrue(record.exhausted_after_only_compatibility_failures)
        self.assertFalse(any(item.open_row == 0 and item.direction == "SWING_HIGH" for item in result.raw))

    def test_swing_low_failure_continues_and_keeps_the_maximum_reference(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "103", "104", "99", "100")
        bars[1] = _bar(1, "100", "112", "99", "110")
        bars[2] = _bar(2, "110", "111", "89", "100")
        bars[3] = _bar(3, "100", "103", "99", "102")
        bars[4] = _bar(4, "102", "103", "98", "99")
        bars[7] = _bar(7, "100", "106", "99", "105")
        bars[8] = _bar(8, "105", "106", "99", "100")
        bars[9] = _bar(9, "100", "112", "99", "111")
        result = analyze(bars)
        low = next(item for item in result.raw if item.open_row == 0 and item.direction == "SWING_LOW")
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_LOW")
        self.assertEqual(low.close_row, 9)
        self.assertEqual(low.interior, 8)
        self.assertEqual(low.body_reference_row, 1)
        self.assertEqual(low.body_reference_price, Decimal("110"))
        self.assertNotEqual(low.body_reference_price, Decimal("102"))
        self.assertNotEqual(low.body_reference_price, Decimal("105"))
        self.assertEqual(low.compatibility_status, "PASS")
        self.assertEqual(low.reference_to_swing_close_margin, Decimal("1"))
        self.assertEqual(low.earlier_compatibility_rejections, 1)
        self.assertTrue(low.final_reference_matches_provisional)
        self.assertLess(0, 7)
        self.assertLess(7, low.close_row)
        self.assertFalse(record.binding_failed)
        self.assertEqual(record.compatibility_rejected, 1)
        self.assertEqual(record.reference_above_close, 1)
        self.assertEqual(record.reference_recalculations, 2)
        self.assertEqual(record.compatibility_audit[0]["code"], "REFERENCE_CLOSE_ABOVE_SWING_CLOSE_FOR_SWING_LOW")
        self.assertEqual(record.compatibility_audit[0]["reference_price"], Decimal("110"))
        self.assertEqual(record.compatibility_audit[0]["eligible_count"], 2)
        self.assertEqual(record.compatibility_audit[1]["status"], "PASS")
        self.assertEqual(record.compatibility_audit[1]["eligible_count"], 3)
        self.assertEqual(record.compatibility_audit[1]["reference_row"], 1)
        self.assertEqual(record.max_interior, 8)
        self.assertEqual(record.above_horizon, 0)

    def test_swing_low_exact_equality_passes(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "103", "104", "99", "100")
        bars[1] = _bar(1, "100", "112", "99", "101")
        bars[2] = _bar(2, "101", "102", "89", "100")
        bars[7] = _bar(7, "100", "102", "99", "101")
        low = next(item for item in analyze(bars).raw if item.open_row == 0 and item.direction == "SWING_LOW")
        self.assertEqual(low.close_row, 7)
        self.assertEqual(low.body_reference_price, Decimal("101"))
        self.assertEqual(low.close_price, Decimal("101"))
        self.assertEqual(low.reference_to_swing_close_margin, Decimal("0"))
        self.assertEqual(low.compatibility_status, "PASS")

    def test_swing_low_rejections_stop_at_nine_interiors(self) -> None:
        bars = _quiet(14)
        bars[0] = _bar(0, "103", "104", "99", "100")
        bars[1] = _bar(1, "100", "112", "99", "110")
        bars[2] = _bar(2, "110", "111", "89", "100")
        for index in range(7, 12):
            bars[index] = _bar(index, "100", "106", "99", "105")
        result = analyze(bars)
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_LOW")
        self.assertEqual(record.terminal, "SEARCH_HORIZON_EXHAUSTED_AT_9_INTERIORS")
        self.assertEqual(record.compatibility_rejected, 4)
        self.assertEqual(record.max_interior, 9)
        self.assertFalse(record.binding_failed)
        self.assertFalse(any(item.open_row == 0 and item.direction == "SWING_LOW" for item in result.raw))

    def test_compatible_boundary_failure_cannot_be_replaced(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[1] = _bar(1, "101.00000002", "101.00000002", "100.5", "101")
        bars[2] = _bar(2, "101", "101.00000002", "100.5", "101.00000001")
        bars[7] = _bar(7, "101.2", "130", "100", "101")
        bars[8] = _bar(8, "102", "102", "100", "90")
        result = analyze(bars)
        record = next(item for item in result.searches if item.open_row == 0 and item.direction == "SWING_HIGH")
        self.assertTrue(record.binding_failed)
        self.assertTrue(record.directionally_bound)
        self.assertEqual(record.compatibility_rejected, 0)
        self.assertEqual(record.evaluated_closes, 1)
        self.assertEqual(record.terminal, "CLOSE_BOUNDARY_BELOW_0_90")
        self.assertFalse(any(item.open_row == 0 and item.direction == "SWING_HIGH" for item in result.raw))

    def test_expanding_window_changes_the_selected_reference_before_binding(self) -> None:
        early = _quiet(9)
        early[1] = _bar(1, "102", "103", "100", "101")
        early[2] = _bar(2, "101", "102", "100", "102")
        early[6] = _bar(6, "100", "101", "80", "90")
        early[7] = _bar(7, "90", "91", "89", "95")
        first = select_interior_body_reference(early, 0, 7, "SWING_HIGH", 2, Decimal("103"))
        second = select_interior_body_reference(early, 0, 8, "SWING_HIGH", 2, Decimal("103"))
        self.assertEqual(first["row"], 1)
        self.assertEqual(first["price"], Decimal("101"))
        self.assertEqual(second["row"], 6)
        self.assertEqual(second["price"], Decimal("90"))
        self.assertEqual(second["validation_row"], 7)
        self.assertLessEqual(second["row"], 8 - 2)
        self.assertNotEqual(second["row"], 7)
        self.assertNotEqual(first["row"], second["row"])

    def test_pre_close_reference_is_excluded_for_both_directions(self) -> None:
        high_bars = [_bar(index, "100", "101", "99", "100") for index in range(9)]
        high_bars[2] = _bar(2, "100", "101", "90", "95")
        high_bars[3] = _bar(3, "95", "96", "94", "96")
        high_bars[7] = _bar(7, "90", "91", "70", "80")
        high = select_interior_body_reference(high_bars, 0, 8, "SWING_HIGH", 2, Decimal("101"))
        self.assertEqual(high["row"], 2)
        self.assertEqual(high["validation_row"], 3)
        self.assertEqual(high["price"], Decimal("95"))
        self.assertNotEqual(high["price"], Decimal("80"))
        self.assertNotEqual(high["row"], 7)
        self.assertLessEqual(high["row"], 6)
        self.assertGreater(high["pre_close_excluded"], 0)
        self.assertGreater(high["successor_is_swing_close"], 0)
        low_bars = [_bar(index, "100", "101", "99", "100") for index in range(9)]
        low_bars[2] = _bar(2, "90", "91", "89", "100")
        low_bars[3] = _bar(3, "100", "101", "90", "95")
        low_bars[7] = _bar(7, "100", "140", "99", "130")
        low = select_interior_body_reference(low_bars, 0, 8, "SWING_LOW", 3, Decimal("90"))
        self.assertEqual(low["row"], 2)
        self.assertEqual(low["validation_row"], 3)
        self.assertEqual(low["price"], Decimal("100"))
        self.assertNotEqual(low["price"], Decimal("130"))
        self.assertNotEqual(low["row"], 7)
        self.assertGreater(low["pre_close_excluded"], 0)
        allowed = [_bar(index, "100", "101", "99", "100") for index in range(9)]
        allowed[2] = _bar(2, "90", "91", "89", "100")
        allowed[3] = _bar(3, "100", "101", "90", "95")
        allowed[6] = _bar(6, "90", "121", "89", "120")
        allowed[7] = _bar(7, "120", "121", "100", "110")
        chosen = select_interior_body_reference(allowed, 0, 8, "SWING_LOW", 3, Decimal("90"))
        self.assertEqual(chosen["row"], 6)
        self.assertEqual(chosen["validation_row"], 7)
        self.assertEqual(chosen["price"], Decimal("120"))
        self.assertEqual(chosen["row"], 8 - 2)
        self.assertNotEqual(chosen["price"], allowed[6].high)

    def test_selected_reference_ohlc_matches_the_source_candle(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[1] = _bar(1, "110", "112", "108", "109")
        bars[2] = _bar(2, "109", "111", "108", "110")
        bars[8] = _bar(8, "102.4", "102.6", "101.5", "102")
        result = analyze(bars)
        high = next(item for item in result.raw if item.open_row == 0 and item.direction == "SWING_HIGH")
        candle = bars[high.body_reference_row]
        self.assertEqual(high.body_reference_row, 1)
        self.assertEqual(high.body_reference_validation_row, 2)
        self.assertEqual(high.body_reference_price, candle.close)
        self.assertNotEqual(high.body_reference_price, candle.low)
        self.assertNotEqual(high.body_reference_price, candle.high)
        self.assertEqual((candle.open, candle.high, candle.low, candle.close), (
            Decimal("110"), Decimal("112"), Decimal("108"), Decimal("109"),
        ))
        self.assertGreaterEqual(high.close_row - high.body_reference_row, 2)
        self.assertNotEqual(high.body_reference_row, high.close_row - 1)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "ohlc.xlsx"
            write_workbook(path, result)
            validate_workbook(path, result)


if __name__ == "__main__":
    unittest.main()
