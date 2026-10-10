"""Formula, timing, outcome, signal-source, statistics, overlap, and immutability tests."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from detectors.swing_open_close_30m_special_v1.engine import Bar, analyze
from strategies.take_position_1_to_2_v1.config import DETECTOR_PACKAGE, FINGERPRINT_PATH, ONE_TO_ONE_PACKAGE, PROJECT_ROOT
from strategies.take_position_1_to_2_v1.engine import (
    TakePositionError,
    annotate_overlap,
    assert_trade_invariants,
    build_trades,
    equivalent_signals,
    first_exit_index,
    grouped_summary,
    legacy_one_to_one_target,
    require_two_r,
    scan_trade,
    signal_rows,
    summarize,
    trade_geometry,
)
from strategies.take_position_1_to_2_v1.models import Minute, Trade
from strategies.take_position_1_to_2_v1.workbook import validate_workbook, write_workbook

UTC = timezone.utc
ONE = Decimal(1)
TWO = Decimal(2)


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


def _quiet(count: int) -> list[Bar]:
    return [_bar(index, "100", "101", "99", "100") for index in range(count)]


def _minute(opened: datetime, open_price: str, high: str, low: str, close: str) -> Minute:
    return Minute(opened, Decimal(open_price), Decimal(high), Decimal(low), Decimal(close))


def _both() -> list[Bar]:
    bars = _quiet(28)
    bars[0] = _bar(0, "100", "104", "99", "103")
    bars[1] = _bar(1, "110", "112", "108", "109")
    bars[2] = _bar(2, "109", "111", "108", "110")
    bars[8] = _bar(8, "102.4", "102.6", "101.5", "102")
    bars[12] = _bar(12, "103", "104", "99", "100")
    bars[13] = _bar(13, "100", "112", "95", "101")
    bars[14] = _bar(14, "101", "102", "90", "100")
    bars[19] = _bar(19, "100", "106", "99", "105")
    return bars


class FormulaTests(unittest.TestCase):
    def test_short_is_two_reward_units_and_rejects_the_old_target(self) -> None:
        geometry, reason = trade_geometry("SHORT", Decimal("100"), Decimal("105"), None)
        self.assertEqual(reason, "")
        stop, target, risk, reward = geometry
        self.assertEqual((stop, risk, reward, target), (Decimal("105"), Decimal("5"), Decimal("10"), Decimal("90")))
        self.assertEqual(reward / risk, TWO)
        self.assertEqual(target, Decimal("100") * 3 - stop * 2)
        self.assertEqual(legacy_one_to_one_target(Decimal("100"), Decimal("105")), Decimal("95"))
        self.assertNotEqual(target, Decimal("95"))
        with self.assertRaises(TakePositionError):
            require_two_r(Decimal("100"), Decimal("105"), Decimal("95"), Decimal("5"))
        rejected, code = trade_geometry("SHORT", Decimal("100"), Decimal("100"), None)
        self.assertIsNone(rejected)
        self.assertEqual(code, "INVALID_SHORT_STOP_NOT_ABOVE_ENTRY")
        rejected, code = trade_geometry("SHORT", Decimal("100"), Decimal("99"), None)
        self.assertEqual(code, "INVALID_SHORT_STOP_NOT_ABOVE_ENTRY")

    def test_long_is_two_reward_units_and_rejects_the_old_target(self) -> None:
        geometry, reason = trade_geometry("LONG", Decimal("100"), None, Decimal("95"))
        self.assertEqual(reason, "")
        stop, target, risk, reward = geometry
        self.assertEqual((stop, risk, reward, target), (Decimal("95"), Decimal("5"), Decimal("10"), Decimal("110")))
        self.assertEqual(reward / risk, TWO)
        self.assertEqual(target, Decimal("100") * 3 - stop * 2)
        self.assertEqual(legacy_one_to_one_target(Decimal("100"), Decimal("95")), Decimal("105"))
        self.assertNotEqual(target, Decimal("105"))
        with self.assertRaises(TakePositionError):
            require_two_r(Decimal("100"), Decimal("95"), Decimal("105"), Decimal("5"))
        rejected, code = trade_geometry("LONG", Decimal("100"), None, Decimal("100"))
        self.assertIsNone(rejected)
        self.assertEqual(code, "INVALID_LONG_STOP_NOT_BELOW_ENTRY")

    def test_a_one_risk_touch_does_not_take_profit(self) -> None:
        start = datetime(2026, 8, 1, tzinfo=UTC)
        signal = start - timedelta(milliseconds=1)
        candles = [
            _minute(start, "100", "101", "95", "96"),
            _minute(start + timedelta(minutes=1), "96", "97", "90", "91"),
        ]
        scanned = scan_trade("SHORT", signal, Decimal("100"), Decimal("105"), Decimal("90"), Decimal("5"), candles, [item.open_time for item in candles])
        self.assertEqual(scanned["outcome"], "WIN")
        self.assertEqual(scanned["realized"], TWO)
        self.assertEqual(scanned["exit_time"], start + timedelta(minutes=1))


class TimingTests(unittest.TestCase):
    def test_exit_scan_starts_after_confirmation_and_duration_is_floored(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[1] = _bar(1, "110", "112", "108", "109")
        bars[2] = _bar(2, "109", "111", "108", "110")
        bars[8] = _bar(8, "102.4", "102.6", "101.5", "102")
        result = analyze(bars)
        high = next(item for item in result.displayed if item.direction == "SWING_HIGH" and item.open_row == 0)
        signal = bars[high.close_row].close_time
        self.assertEqual(high.close_price, Decimal("102"))
        inside = signal.replace(second=0, microsecond=0)
        after = signal + timedelta(milliseconds=1)
        later = after + timedelta(minutes=30)
        minutes = [
            _minute(inside, "102", "200", "1", "102"),
            _minute(after, "102", "103", "101", "102"),
            _minute(later, "102", "103", "80", "90"),
        ]
        open_times = [item.open_time for item in minutes]
        self.assertEqual(first_exit_index(open_times, signal), 1)
        scanned = scan_trade("SHORT", signal, Decimal("102"), Decimal("111"), Decimal("84"), Decimal("9"), minutes, open_times)
        self.assertEqual(scanned["outcome"], "WIN")
        self.assertEqual(scanned["realized"], TWO)
        self.assertEqual(scanned["exit_time"], later)
        self.assertEqual(scanned["duration"], 30)
        censored = scan_trade("SHORT", signal, Decimal("102"), Decimal("111"), Decimal("84"), Decimal("9"), minutes[:2], open_times[:2])
        self.assertEqual(censored["outcome"], "OPEN_CENSORED")
        empty = scan_trade("SHORT", signal, Decimal("102"), Decimal("111"), Decimal("84"), Decimal("9"), minutes[:1], open_times[:1])
        self.assertEqual(empty["outcome"], "UNENTERED_CENSORED")


class OutcomeTests(unittest.TestCase):
    def _scan(self, direction: str, candles: list[Minute], stop: str, target: str) -> dict:
        signal = candles[0].open_time - timedelta(milliseconds=1)
        return scan_trade(direction, signal, Decimal("100"), Decimal(stop), Decimal(target), Decimal("5"), candles, [item.open_time for item in candles])

    def test_two_r_targets_resolve_once_and_do_not_repaint(self) -> None:
        start = datetime(2026, 8, 1, tzinfo=UTC)
        short_stop = self._scan("SHORT", [_minute(start, "100", "105", "99", "104")], "105", "90")
        self.assertEqual((short_stop["outcome"], short_stop["realized"]), ("LOSS", -ONE))
        short_target = self._scan("SHORT", [_minute(start, "100", "101", "90", "92")], "105", "90")
        self.assertEqual((short_target["outcome"], short_target["realized"]), ("WIN", TWO))
        long_stop = self._scan("LONG", [_minute(start, "100", "101", "95", "96")], "95", "110")
        self.assertEqual(long_stop["outcome"], "LOSS")
        self.assertEqual(long_stop["realized"], -ONE)
        long_target = self._scan("LONG", [_minute(start, "100", "110", "99", "108")], "95", "110")
        self.assertEqual((long_target["outcome"], long_target["realized"]), ("WIN", TWO))
        collision = self._scan("SHORT", [_minute(start, "100", "105", "90", "100")], "105", "90")
        self.assertEqual(collision["outcome"], "LOSS_AMBIGUOUS")
        self.assertEqual(collision["realized"], -ONE)
        self.assertTrue(collision["ambiguity"])
        stop_gap = self._scan("SHORT", [_minute(start, "106", "107", "104", "105")], "105", "90")
        self.assertEqual(stop_gap["fill"], Decimal("106"))
        self.assertEqual(stop_gap["realized"], Decimal("-1.2"))
        target_gap = self._scan("SHORT", [_minute(start, "89", "91", "88", "90")], "105", "90")
        self.assertEqual(target_gap["reason"], "TAKE_PROFIT_GAP")
        self.assertEqual(target_gap["fill"], Decimal("90"))
        self.assertEqual(target_gap["realized"], TWO)
        long_gap = self._scan("LONG", [_minute(start, "94", "96", "93", "95")], "95", "110")
        self.assertEqual(long_gap["realized"], Decimal("-1.2"))
        quiet = [_minute(start, "100", "101", "99", "100"), _minute(start + timedelta(minutes=1), "100", "101", "99", "100")]
        self.assertEqual(self._scan("LONG", quiet, "95", "110")["outcome"], "OPEN_CENSORED")
        first = self._scan(
            "SHORT",
            [_minute(start, "100", "105", "99", "104"), _minute(start + timedelta(minutes=1), "100", "101", "80", "90")],
            "105",
            "90",
        )
        self.assertEqual(first["outcome"], "LOSS")
        self.assertEqual(first["exit_time"], start)


class SignalSourceTests(unittest.TestCase):
    def test_only_primary_detector_values_receive_two_r_targets(self) -> None:
        bars = _both()
        result = analyze(bars)
        self.assertTrue(equivalent_signals(signal_rows(result), signal_rows(analyze(bars))))
        late = analyze(bars[12:])
        self.assertTrue(equivalent_signals(signal_rows(result, bars[12].open_time), signal_rows(late)))
        high = next(item for item in result.displayed if item.open_row == 0 and item.direction == "SWING_HIGH")
        low = next(item for item in result.displayed if item.open_row == 12 and item.direction == "SWING_LOW")
        self.assertEqual(high.peak_2_price, Decimal("111"))
        self.assertEqual(low.dip_2_price, Decimal("90"))
        signal = bars[8].close_time
        minutes = [
            _minute(bars[8].open_time, "102", "200", "1", "102"),
            _minute(signal + timedelta(milliseconds=1), "102", "103", "101", "102"),
            _minute(datetime(2026, 7, 1, 10, 30, tzinfo=UTC), "100", "101", "84", "90"),
            _minute(datetime(2026, 7, 1, 11, 0, tzinfo=UTC), "100", "101", "89", "96"),
        ]
        trades = build_trades(result, minutes)
        annotate_overlap(trades, minutes[-1].open_time)
        assert_trade_invariants(trades)
        traded = [trade.swing_id for trade in trades]
        self.assertEqual(len(traded), len(set(traded)))
        self.assertEqual(set(traded), {item.swing_id for item in result.displayed})
        derived = {item.swing_id for item in result.raw if item.status != "PRIMARY"}
        self.assertTrue(set(traded).isdisjoint(derived))
        short = next(trade for trade in trades if trade.swing_id == high.swing_id)
        long = next(trade for trade in trades if trade.swing_id == low.swing_id)
        self.assertEqual(short.direction, "SHORT")
        self.assertEqual(short.stop_price, Decimal("111"))
        self.assertEqual(short.risk_price, Decimal("9"))
        self.assertEqual(short.reward_price, Decimal("18"))
        self.assertEqual(short.target_price, Decimal("84"))
        self.assertEqual(short.outcome, "WIN")
        self.assertEqual(short.realized_r, TWO)
        self.assertEqual(long.direction, "LONG")
        self.assertEqual(long.stop_price, Decimal("90"))
        self.assertEqual(long.reward_price, Decimal("30"))
        self.assertEqual(long.target_price, Decimal("135"))
        self.assertIsNone(long.peak_2_high)
        self.assertIsNone(short.dip_2_low)

    def test_missing_peak_is_invalid(self) -> None:
        bars = _quiet(9)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[4] = _bar(4, "100", "110", "99", "100")
        bars[8] = _bar(8, "102", "102.2", "100", "101")
        result = analyze(bars)
        missing = next(item for item in result.displayed if item.open_row == 0 and item.direction == "SWING_HIGH")
        invalid = next(trade for trade in build_trades(result, []) if trade.swing_id == missing.swing_id)
        self.assertEqual(invalid.exit_reason, "MISSING_PEAK_2")
        self.assertIsNone(invalid.stop_price)

    def test_overlapping_signals_stay_independent(self) -> None:
        start = datetime(2026, 3, 1, tzinfo=UTC)
        later = start + timedelta(hours=2)
        short = Trade(
            "T30-000001", "S30-000001", "SWING_HIGH", "SHORT", start, start, Decimal("100"),
            Decimal("105"), Decimal("90"), Decimal("5"), Decimal("10"), Decimal("5"), Decimal("10"),
            Decimal("105"), None, "OPEN_CENSORED", "CENSORED", "DATASET_END",
        )
        long = Trade(
            "T30-000002", "S30-000002", "SWING_LOW", "LONG", later, later, Decimal("100"),
            Decimal("95"), Decimal("110"), Decimal("5"), Decimal("10"), Decimal("5"), Decimal("10"),
            None, Decimal("95"), "OPEN_CENSORED", "CENSORED", "DATASET_END",
        )
        stats = annotate_overlap([short, long], later + timedelta(hours=3))
        self.assertEqual(short.concurrent_at_entry, 1)
        self.assertEqual(long.concurrent_at_entry, 2)
        self.assertEqual(stats["maximum_simultaneously_open"], 2)
        self.assertEqual(stats["opposite_direction_overlap_count"], 2)
        self.assertEqual(short.outcome, "OPEN_CENSORED")
        self.assertEqual(long.outcome, "OPEN_CENSORED")


class StatisticsTests(unittest.TestCase):
    def _trade(self, direction: str, outcome: str, minutes: int | None, realized: str | None, swing: str) -> Trade:
        group = {"WIN": "WIN", "LOSS": "LOSS", "LOSS_AMBIGUOUS": "LOSS", "OPEN_CENSORED": "CENSORED", "INVALID": "INVALID"}[outcome]
        return Trade(
            trade_id=swing,
            swing_id=swing,
            swing_type="SWING_HIGH" if direction == "SHORT" else "SWING_LOW",
            direction=direction,
            signal_time=datetime(2026, 1, 1, tzinfo=UTC),
            entry_time=None if outcome == "INVALID" else datetime(2026, 1, 1, tzinfo=UTC),
            entry_price=None if outcome == "INVALID" else Decimal("100"),
            stop_price=None if outcome == "INVALID" else Decimal("105" if direction == "SHORT" else "95"),
            target_price=None if outcome == "INVALID" else Decimal("90" if direction == "SHORT" else "110"),
            risk_price=None if outcome == "INVALID" else Decimal("5"),
            reward_price=None if outcome == "INVALID" else Decimal("10"),
            risk_percent=None if outcome == "INVALID" else Decimal("5"),
            reward_percent=None if outcome == "INVALID" else Decimal("10"),
            peak_2_high=Decimal("105") if direction == "SHORT" and outcome != "INVALID" else None,
            dip_2_low=Decimal("95") if direction == "LONG" and outcome != "INVALID" else None,
            outcome=outcome,
            outcome_group=group,
            exit_reason=outcome,
            realized_r=None if realized is None else Decimal(realized),
            duration_minutes=minutes,
            intrabar_ambiguity=outcome == "LOSS_AMBIGUOUS",
        )

    def test_break_even_and_two_r_expectancy(self) -> None:
        trades = [self._trade("SHORT", "WIN", 10, "2", f"W{index}") for index in range(10)]
        trades.extend(self._trade("LONG", "LOSS", 30, "-1", f"L{index}") for index in range(20))
        trades.append(self._trade("LONG", "OPEN_CENSORED", 500, None, "OPEN"))
        trades.append(self._trade("SHORT", "INVALID", None, None, "BAD"))
        overall = summarize(trades)
        self.assertEqual(overall["resolved_trade_count"], 30)
        self.assertEqual(overall["win_rate_percent"], Decimal(100) / Decimal(3))
        self.assertEqual(overall["break_even_win_rate_percent"], Decimal(100) / Decimal(3))
        self.assertEqual(overall["win_rate_minus_break_even_percentage_points"], Decimal(0))
        self.assertEqual(overall["total_gross_r"], Decimal(0))
        self.assertEqual(overall["average_gross_r"], Decimal(0))
        self.assertEqual(overall["gross_expectancy_r"], Decimal(0))
        self.assertEqual(sum(trade.realized_r for trade in trades if trade.outcome == "WIN"), Decimal(20))
        self.assertEqual(sum(trade.realized_r for trade in trades if trade.outcome == "LOSS"), Decimal(-20))
        groups = grouped_summary(trades)
        self.assertEqual(
            groups["SWING HIGH SHORTS"]["resolved_trade_count"] + groups["SWING LOW LONGS"]["resolved_trade_count"],
            overall["resolved_trade_count"],
        )
        empty = summarize([])
        self.assertIsNone(empty["win_rate_percent"])
        self.assertEqual(empty["break_even_win_rate_percent"], Decimal(100) / Decimal(3))
        self.assertEqual(empty["total_gross_r"], Decimal(0))
        ambiguous = summarize([self._trade("SHORT", "LOSS_AMBIGUOUS", 12, "-1", "A")])
        self.assertEqual(ambiguous["loss_rate_percent"], Decimal(100))
        self.assertEqual(ambiguous["win_count"], 0)


class WorkbookTests(unittest.TestCase):
    def test_workbook_round_trip(self) -> None:
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[1] = _bar(1, "110", "112", "108", "109")
        bars[2] = _bar(2, "109", "111", "108", "110")
        bars[8] = _bar(8, "102.4", "102.6", "101.5", "102")
        result = analyze(bars)
        signal = result.bars[8].close_time
        minutes = [_minute(signal + timedelta(milliseconds=1), "102", "103", "80", "90")]
        trades = build_trades(result, minutes)
        annotate_overlap(trades, minutes[-1].open_time)
        summary = grouped_summary(trades)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "sample.xlsx"
            write_workbook(path, trades, summary, [("Detector immutability", "PASS")], "BTCUSDT_30M_SPECIAL_SWING_V1")
            checked = validate_workbook(path, trades, summary)
        self.assertFalse(checked["macros"])
        self.assertGreater(checked["rows"]["All Trades"], 0)


class ImmutabilityTests(unittest.TestCase):
    def test_detector_and_one_to_one_strategy_match_the_prework_fingerprint(self) -> None:
        recorded = json.loads(FINGERPRINT_PATH.read_text(encoding="utf-8"))
        self.assertTrue(recorded)
        for path_text, expected in recorded.items():
            path = Path(path_text)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertEqual(digest, expected["sha256"])
            self.assertEqual(path.stat().st_size, expected["size"])
            self.assertTrue(path.is_relative_to(DETECTOR_PACKAGE) or path.is_relative_to(ONE_TO_ONE_PACKAGE))
        self.assertTrue(ONE_TO_ONE_PACKAGE.is_dir())
        self.assertTrue((PROJECT_ROOT / "strategies" / "take_position_v1" / "engine.py").is_file())


if __name__ == "__main__":
    unittest.main()
