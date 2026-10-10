"""Required checks for the fixed 1:1.5 R take-position backtest."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from detectors.swing_open_close_30m_special_v1.engine import Bar, analyze, turkey_text
from strategies.take_position_1_to_1_5_v1.config import (
    DETECTOR_PACKAGE,
    FINGERPRINT_PATH,
    ONE_TO_ONE_PACKAGE,
    ONE_TO_TWO_PACKAGE,
)
from strategies.take_position_1_to_1_5_v1.engine import (
    TakePositionError,
    annotate_overlap,
    assert_trade_invariants,
    build_trades,
    configured_break_even,
    content_fingerprint,
    cumulative_r,
    equivalent_signals,
    first_exit_index,
    grouped_summary,
    one_to_one_point_five_target,
    one_to_one_target,
    one_to_two_target,
    require_one_point_five,
    scan_trade,
    signal_rows,
    summarize,
    trade_geometry,
)
from strategies.take_position_1_to_1_5_v1.models import Minute, Trade
from strategies.take_position_1_to_1_5_v1.workbook import revision_from_names, validate_workbook, write_workbook

UTC = timezone.utc
ONE = Decimal(1)
ONE_POINT_FIVE = Decimal("1.5")


def _bar(index: int, open_price: str, high: str, low: str, close: str) -> Bar:
    opened = datetime(2026, 7, 1, tzinfo=UTC) + timedelta(minutes=30 * index)
    return Bar(index, opened, opened + timedelta(minutes=30) - timedelta(milliseconds=1), Decimal(open_price), Decimal(high), Decimal(low), Decimal(close))


def _quiet(count: int) -> list[Bar]:
    return [_bar(index, "100", "101", "99", "100") for index in range(count)]


def _minute(opened: datetime, open_price: str, high: str, low: str, close: str) -> Minute:
    return Minute(opened, Decimal(open_price), Decimal(high), Decimal(low), Decimal(close))


def _scan(direction: str, candle: Minute, stop: str, target: str) -> dict:
    signal = candle.open_time - timedelta(milliseconds=1)
    return scan_trade(direction, signal, Decimal("100"), Decimal(stop), Decimal(target), Decimal("10"), [candle], [candle.open_time])


class RatioTests(unittest.TestCase):
    def test_short_and_long_targets_are_exactly_one_point_five(self) -> None:
        short, reason = trade_geometry("SHORT", Decimal("100"), Decimal("110"), None)
        self.assertEqual(reason, "")
        stop, target, risk, reward = short
        self.assertEqual((stop, risk, reward, target), (Decimal("110"), Decimal("10"), Decimal("15"), Decimal("85")))
        self.assertEqual(target, Decimal("100") - Decimal("1.5") * risk)
        self.assertEqual(target, one_to_one_point_five_target(Decimal("100"), Decimal("110")))
        self.assertEqual(reward / risk, ONE_POINT_FIVE)
        self.assertNotEqual(target, one_to_one_target(Decimal("100"), Decimal("110")))
        self.assertNotEqual(target, one_to_two_target(Decimal("100"), Decimal("110")))
        long, reason = trade_geometry("LONG", Decimal("100"), None, Decimal("90"))
        self.assertEqual(reason, "")
        stop, target, risk, reward = long
        self.assertEqual((stop, risk, reward, target), (Decimal("90"), Decimal("10"), Decimal("15"), Decimal("115")))
        self.assertEqual(target, Decimal("100") + Decimal("1.5") * risk)
        with self.assertRaises(TakePositionError):
            require_one_point_five(Decimal("100"), Decimal("110"), one_to_one_target(Decimal("100"), Decimal("110")), Decimal("10"))
        with self.assertRaises(TakePositionError):
            require_one_point_five(Decimal("100"), Decimal("110"), one_to_two_target(Decimal("100"), Decimal("110")), Decimal("10"))
        self.assertEqual(configured_break_even(), Decimal(40))
        self.assertEqual(Decimal(1) / (Decimal(1) + Decimal("1.5")), Decimal("0.4"))
        self.assertNotEqual(Decimal("0.4"), Decimal("0.5"))
        self.assertNotEqual(Decimal("0.4"), Decimal(1) / Decimal(3))

    def test_invalid_geometry_is_rejected(self) -> None:
        self.assertEqual(trade_geometry("SHORT", Decimal("100"), None, None)[1], "MISSING_PEAK_2")
        self.assertEqual(trade_geometry("LONG", Decimal("100"), None, None)[1], "MISSING_DIP_2")
        self.assertEqual(trade_geometry("SHORT", Decimal("100"), Decimal("100"), None)[1], "INVALID_SHORT_STOP_NOT_ABOVE_ENTRY")
        self.assertEqual(trade_geometry("SHORT", Decimal("100"), Decimal("99"), None)[1], "INVALID_SHORT_STOP_NOT_ABOVE_ENTRY")
        self.assertEqual(trade_geometry("LONG", Decimal("100"), None, Decimal("100"))[1], "INVALID_LONG_STOP_NOT_BELOW_ENTRY")
        self.assertEqual(trade_geometry("LONG", Decimal("100"), None, Decimal("101"))[1], "INVALID_LONG_STOP_NOT_BELOW_ENTRY")


class OutcomeTests(unittest.TestCase):
    def test_touches_gaps_and_censoring(self) -> None:
        start = datetime(2026, 8, 1, tzinfo=UTC)
        short_stop = _scan("SHORT", _minute(start, "100", "110", "99", "108"), "110", "85")
        self.assertEqual((short_stop["outcome"], short_stop["realized"]), ("STOP_LOSS", -ONE))
        short_target = _scan("SHORT", _minute(start, "100", "101", "85", "90"), "110", "85")
        self.assertEqual((short_target["outcome"], short_target["realized"]), ("TAKE_PROFIT", ONE_POINT_FIVE))
        long_stop = _scan("LONG", _minute(start, "100", "101", "90", "92"), "90", "115")
        self.assertEqual(long_stop["realized"], -ONE)
        long_target = _scan("LONG", _minute(start, "100", "115", "99", "112"), "90", "115")
        self.assertEqual(long_target["realized"], ONE_POINT_FIVE)
        both = _scan("SHORT", _minute(start, "100", "110", "85", "100"), "110", "85")
        self.assertEqual(both["outcome"], "STOP_LOSS")
        self.assertTrue(both["both"])
        self.assertEqual(both["realized"], -ONE)
        short_gap = _scan("SHORT", _minute(start, "112", "113", "111", "112"), "110", "85")
        self.assertEqual(short_gap["fill"], Decimal("112"))
        self.assertEqual(short_gap["realized"], Decimal("-1.2"))
        long_gap = _scan("LONG", _minute(start, "88", "89", "87", "88"), "90", "115")
        self.assertEqual(long_gap["fill"], Decimal("88"))
        self.assertLess(long_gap["realized"], -ONE)
        short_target_gap = _scan("SHORT", _minute(start, "80", "82", "79", "81"), "110", "85")
        self.assertEqual(short_target_gap["fill"], Decimal("85"))
        self.assertEqual(short_target_gap["realized"], ONE_POINT_FIVE)
        long_target_gap = _scan("LONG", _minute(start, "120", "121", "119", "120"), "90", "115")
        self.assertEqual(long_target_gap["realized"], ONE_POINT_FIVE)
        quiet = [_minute(start, "100", "101", "99", "100")]
        signal = start - timedelta(milliseconds=1)
        censored = scan_trade("LONG", signal, Decimal("100"), Decimal("90"), Decimal("115"), Decimal("10"), quiet, [start])
        self.assertEqual(censored["outcome"], "OPEN_AT_DATASET_END")
        self.assertFalse(censored["resolved"])
        inside = _minute(start, "100", "999", "1", "100")
        after = _minute(start + timedelta(minutes=1), "100", "101", "99", "100")
        self.assertEqual(first_exit_index([inside.open_time, after.open_time], inside.open_time + timedelta(milliseconds=1)), 1)


class SignalTests(unittest.TestCase):
    def test_detector_primary_maps_to_one_point_five_trades(self) -> None:
        bars = _quiet(28)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[1] = _bar(1, "110", "112", "108", "109")
        bars[2] = _bar(2, "109", "111", "108", "110")
        bars[8] = _bar(8, "102.4", "102.6", "101.5", "102")
        bars[12] = _bar(12, "103", "104", "99", "100")
        bars[13] = _bar(13, "100", "112", "95", "101")
        bars[14] = _bar(14, "101", "102", "90", "100")
        bars[19] = _bar(19, "100", "106", "99", "105")
        result = analyze(bars)
        self.assertTrue(equivalent_signals(signal_rows(result), signal_rows(analyze(list(bars)))))
        high = next(item for item in result.displayed if item.open_row == 0 and item.direction == "SWING_HIGH")
        low = next(item for item in result.displayed if item.open_row == 12 and item.direction == "SWING_LOW")
        signal = bars[8].close_time
        minutes = [
            _minute(bars[8].open_time, "102", "200", "1", "102"),
            _minute(signal + timedelta(milliseconds=1), "102", "103", "101", "102"),
            _minute(datetime(2026, 7, 1, 10, 30, tzinfo=UTC), "100", "101", "88.5", "95"),
        ]
        trades = build_trades(result, minutes)
        again = build_trades(analyze(bars), minutes)
        self.assertEqual(content_fingerprint(trades), content_fingerprint(again))
        annotate_overlap(trades, minutes[-1].open_time)
        assert_trade_invariants(trades)
        self.assertEqual([trade.trade_id for trade in trades], [f"T30-{index:06d}" for index in range(1, len(trades) + 1)])
        short = next(trade for trade in trades if trade.swing_id == high.swing_id)
        long = next(trade for trade in trades if trade.swing_id == low.swing_id)
        self.assertEqual(short.direction, "SHORT")
        self.assertEqual(short.entry_price, Decimal("102"))
        self.assertEqual(short.stop_price, high.peak_2_price)
        self.assertEqual(short.reward_distance, short.risk_price * ONE_POINT_FIVE)
        self.assertEqual(short.target_price, short.entry_price - short.reward_distance)
        self.assertEqual(short.outcome, "TAKE_PROFIT")
        self.assertEqual(short.realized_r, ONE_POINT_FIVE)
        self.assertGreater(short.first_eligible_time, short.signal_time)
        self.assertEqual(long.direction, "LONG")
        self.assertEqual(long.stop_price, low.dip_2_price)
        self.assertEqual(long.target_price, long.entry_price + long.reward_distance)
        missing_bars = _quiet(9)
        missing_bars[0] = _bar(0, "100", "104", "99", "103")
        missing_bars[4] = _bar(4, "100", "110", "99", "100")
        missing_bars[8] = _bar(8, "102", "102.2", "100", "101")
        missing = analyze(missing_bars)
        invalid = next(trade for trade in build_trades(missing, []) if trade.open_row == 0 and trade.direction == "SHORT")
        self.assertEqual(invalid.exit_reason, "MISSING_PEAK_2")
        self.assertFalse(invalid.resolved)

    def test_overlap_statistics_drawdown_and_timezone(self) -> None:
        start = datetime(2026, 3, 1, tzinfo=UTC)
        later = start + timedelta(hours=2)

        def trade(identifier: str, direction: str, outcome: str, realized: str | None, when: datetime) -> Trade:
            return Trade(
                identifier, identifier, "SWING_HIGH" if direction == "SHORT" else "SWING_LOW", direction, when, when, when,
                None if outcome == "INVALID" else when, None if outcome == "INVALID" else Decimal("100"),
                None if outcome == "INVALID" else Decimal("110" if direction == "SHORT" else "90"),
                None if outcome == "INVALID" else Decimal("85" if direction == "SHORT" else "115"),
                None if outcome == "INVALID" else Decimal("10"), None if outcome == "INVALID" else Decimal("15"),
                None if outcome == "INVALID" else Decimal("10"), None if outcome == "INVALID" else ONE_POINT_FIVE,
                Decimal("110") if direction == "SHORT" and outcome != "INVALID" else None,
                Decimal("90") if direction == "LONG" and outcome != "INVALID" else None,
                when, outcome, outcome, realized_r=None if realized is None else Decimal(realized),
                resolved=outcome in {"TAKE_PROFIT", "STOP_LOSS"}, duration_minutes=30,
            )

        trades = [
            trade("A", "SHORT", "TAKE_PROFIT", "1.5", start),
            trade("B", "LONG", "STOP_LOSS", "-1", later),
            trade("C", "LONG", "STOP_LOSS", "-1", later + timedelta(hours=1)),
            trade("D", "SHORT", "OPEN_AT_DATASET_END", None, later + timedelta(hours=3)),
            trade("E", "LONG", "INVALID", None, later),
        ]
        stats = annotate_overlap(trades, later + timedelta(hours=5))
        self.assertEqual(trades[0].concurrent_at_entry, 1)
        self.assertGreaterEqual(trades[1].concurrent_at_entry, 2)
        self.assertEqual(trades[0].outcome, "TAKE_PROFIT")
        self.assertEqual(trades[1].outcome, "STOP_LOSS")
        summary = summarize(trades)
        self.assertEqual(summary["resolved_trade_count"], 3)
        self.assertEqual(summary["unresolved_trade_count"], 1)
        self.assertEqual(summary["invalid_signal_count"], 1)
        self.assertEqual(summary["win_rate_percent"], Decimal(100) / Decimal(3))
        self.assertEqual(summary["break_even_win_rate_percent"], Decimal(40))
        self.assertEqual(summary["total_r"], Decimal("-0.5"))
        self.assertEqual(cumulative_r(trades)["maximum_drawdown_r"], Decimal(2))
        groups = grouped_summary(trades)
        self.assertEqual(groups["SHORT"]["take_profit_count"] + groups["LONG"]["take_profit_count"], summary["take_profit_count"])
        local = datetime(2026, 1, 15, 21, tzinfo=UTC).astimezone(ZoneInfo("Europe/Istanbul"))
        summer = datetime(2026, 7, 15, 21, tzinfo=UTC).astimezone(ZoneInfo("Europe/Istanbul"))
        self.assertEqual(local.utcoffset(), timedelta(hours=3))
        self.assertEqual(summer.utcoffset(), timedelta(hours=3))
        self.assertIn("+03:00", turkey_text(datetime(2026, 1, 15, 21, tzinfo=UTC)))
        self.assertGreaterEqual(stats["maximum_simultaneously_open"], 1)


class WorkbookAndImmutabilityTests(unittest.TestCase):
    def test_revision_workbook_and_protected_fingerprints(self) -> None:
        self.assertEqual(revision_from_names([]), 0)
        self.assertEqual(revision_from_names([
            "BTCUSDT_30M_Take_Position_1_TO_1_5R_rev00.xlsx",
            "BTCUSDT_30M_Take_Position_1_TO_1_5R_rev02.xlsx",
            "BTCUSDT_30M_Take_Position_1R_rev09.xlsx",
            "BTCUSDT_30M_Take_Position_1_TO_2R_rev00.xlsx",
        ]), 3)
        bars = _quiet(12)
        bars[0] = _bar(0, "100", "104", "99", "103")
        bars[1] = _bar(1, "110", "112", "108", "109")
        bars[2] = _bar(2, "109", "111", "108", "110")
        bars[8] = _bar(8, "102.4", "102.6", "101.5", "102")
        result = analyze(bars)
        signal = result.bars[8].close_time
        trades = build_trades(result, [_minute(signal + timedelta(milliseconds=1), "102", "103", "80", "90")])
        annotate_overlap(trades, signal + timedelta(minutes=1))
        summary = grouped_summary(trades)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "sample.xlsx"
            write_workbook(path, trades, summary, [], [("Reward multiple", "1.5")], "BTCUSDT_30M_SPECIAL_SWING_V1")
            checked = validate_workbook(path, trades)
        self.assertEqual(checked["sheets"][0], "Performance Summary")
        self.assertEqual(checked["sheets"][-1], "README")
        self.assertFalse(checked["macros"])
        self.assertFalse(checked["external_links"])
        recorded = json.loads(FINGERPRINT_PATH.read_text(encoding="utf-8"))
        for path_text, expected in recorded.items():
            path = Path(path_text)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), expected["sha256"])
            self.assertEqual(path.stat().st_size, expected["size"])
            self.assertTrue(path.is_relative_to(DETECTOR_PACKAGE) or path.is_relative_to(ONE_TO_ONE_PACKAGE) or path.is_relative_to(ONE_TO_TWO_PACKAGE))


if __name__ == "__main__":
    unittest.main()
