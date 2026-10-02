"""Single-timeframe range, half-open semantics, and projection tests."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import config
from data.exceptions import (
    ColumnProjectionError,
    EmptyRangeError,
    InvalidTimeRangeError,
    NaiveDatetimeError,
    UnsupportedSymbolError,
    UnsupportedTimeframeError,
)
from data.loader import load_candles
from data.timeutil import datetime_to_ms

UTC = timezone.utc
IST = ZoneInfo("Europe/Istanbul")
DAY = datetime(2026, 8, 10, tzinfo=UTC)
DAY_END = datetime(2026, 8, 11, tzinfo=UTC)
EXPECTED_DAY = {
    "1m": 1440,
    "5m": 288,
    "15m": 96,
    "30m": 48,
    "1h": 24,
    "4h": 6,
    "1d": 1,
}


class TestLoaderRange(unittest.TestCase):
    def test_one_complete_utc_day_all_timeframes(self) -> None:
        for tf, expected in EXPECTED_DAY.items():
            sl = load_candles("BTCUSDT", tf, DAY, DAY_END)
            self.assertEqual(sl.row_count, expected, tf)
            self.assertEqual(sl.trade_row_count, expected, tf)
            self.assertEqual(sl.trade_start_index, 0, tf)
            self.assertEqual(sl.symbol, "BTCUSDT")
            self.assertEqual(sl.timeframe, tf)
            first = sl.open_times_ms()[0]
            last = sl.open_times_ms()[-1]
            self.assertGreaterEqual(first, datetime_to_ms(DAY))
            self.assertLess(last, datetime_to_ms(DAY_END))
            if tf != "1d":
                spec = config.TIMEFRAME_SPECS[tf]
                self.assertEqual(last - first, spec.duration_ms * (expected - 1), tf)

    def test_half_open_excludes_end(self) -> None:
        sl = load_candles("ETHUSDT", "1h", DAY, DAY_END)
        self.assertEqual(sl.row_count, 24)
        last_open = sl.table.column("open_time")[-1].as_py()
        self.assertEqual(last_open, datetime(2026, 8, 10, 23, 0, tzinfo=UTC))
        next_hour = load_candles(
            "ETHUSDT", "1h",
            datetime(2026, 8, 10, 23, 0, tzinfo=UTC),
            datetime(2026, 8, 11, 1, 0, tzinfo=UTC),
        )
        self.assertEqual(next_hour.row_count, 2)

    def test_single_candle_range(self) -> None:
        sl = load_candles(
            "BTCUSDT", "5m",
            datetime(2026, 8, 10, 10, 30, tzinfo=UTC),
            datetime(2026, 8, 10, 10, 35, tzinfo=UTC),
        )
        self.assertEqual(sl.row_count, 1)
        self.assertEqual(sl.table.column("open_time")[0].as_py(), datetime(2026, 8, 10, 10, 30, tzinfo=UTC))
        self.assertEqual(int(sl.table.column("source_candle_count")[0].as_py()), 5)

    def test_cross_month_range(self) -> None:
        sl = load_candles(
            "LINKUSDT", "5m",
            datetime(2026, 8, 31, 22, 0, tzinfo=UTC),
            datetime(2026, 9, 1, 2, 0, tzinfo=UTC),
        )
        self.assertEqual(sl.row_count, 48)
        self.assertEqual(len(sl.source_files), 2)
        opens = sl.open_times_ms()
        self.assertEqual(opens[0], datetime_to_ms(datetime(2026, 8, 31, 22, 0, tzinfo=UTC)))
        self.assertEqual(opens[-1], datetime_to_ms(datetime(2026, 9, 1, 1, 55, tzinfo=UTC)))

    def test_cross_year_range(self) -> None:
        sl = load_candles(
            "AAVEUSDT", "1h",
            datetime(2024, 12, 31, 22, 0, tzinfo=UTC),
            datetime(2025, 1, 1, 2, 0, tzinfo=UTC),
        )
        self.assertEqual(sl.row_count, 4)
        self.assertEqual(len(sl.source_files), 2)

    def test_leap_day_2024(self) -> None:
        sl = load_candles(
            "BTCUSDT", "1h",
            datetime(2024, 2, 29, tzinfo=UTC),
            datetime(2024, 3, 1, tzinfo=UTC),
        )
        self.assertEqual(sl.row_count, 24)
        self.assertEqual(sl.table.column("open_time")[0].as_py(), datetime(2024, 2, 29, tzinfo=UTC))

    def test_final_available_day(self) -> None:
        sl = load_candles(
            "DOTUSDT", "5m",
            datetime(2026, 9, 15, tzinfo=UTC),
            datetime(2026, 9, 16, tzinfo=UTC),
        )
        self.assertEqual(sl.row_count, 288)
        self.assertEqual(
            sl.table.column("open_time")[-1].as_py(),
            datetime(2026, 9, 15, 23, 55, tzinfo=UTC),
        )
        daily = load_candles(
            "DOTUSDT", "1d",
            datetime(2026, 9, 15, tzinfo=UTC),
            datetime(2026, 9, 16, tzinfo=UTC),
        )
        self.assertEqual(daily.row_count, 1)

    def test_empty_out_of_range(self) -> None:
        with self.assertRaises(EmptyRangeError):
            load_candles(
                "BTCUSDT", "5m",
                datetime(2026, 9, 16, tzinfo=UTC),
                datetime(2026, 9, 17, tzinfo=UTC),
            )

    def test_start_equal_end(self) -> None:
        with self.assertRaises(InvalidTimeRangeError):
            load_candles("BTCUSDT", "5m", DAY, DAY)

    def test_end_before_start(self) -> None:
        with self.assertRaises(InvalidTimeRangeError):
            load_candles("BTCUSDT", "5m", DAY_END, DAY)

    def test_naive_datetime_rejected(self) -> None:
        naive = datetime(2026, 8, 10, 0, 0)
        with self.assertRaises(NaiveDatetimeError):
            load_candles("BTCUSDT", "5m", naive, DAY_END)
        with self.assertRaises(NaiveDatetimeError):
            load_candles("BTCUSDT", "5m", "2026-08-10T00:00:00", DAY_END)

    def test_aware_non_utc_normalized(self) -> None:
        start = datetime(2026, 8, 10, 3, 0, tzinfo=IST)  # 00:00 UTC
        end = datetime(2026, 8, 11, 3, 0, tzinfo=IST)  # 00:00 UTC next day
        sl = load_candles("BTCUSDT", "5m", start, end)
        self.assertEqual(sl.row_count, 288)
        self.assertEqual(sl.table.column("open_time")[0].as_py(), DAY)

    def test_column_projection(self) -> None:
        sl = load_candles("BTCUSDT", "15m", DAY, DAY_END, columns=["close"])
        names = sl.table.column_names
        self.assertIn("symbol", names)
        self.assertIn("open_time", names)
        self.assertIn("close_time", names)
        self.assertIn("close", names)
        self.assertNotIn("volume", names)
        self.assertNotIn("quote_asset_volume", names)
        self.assertEqual(sl.row_count, 96)

    def test_unknown_column_rejected(self) -> None:
        with self.assertRaises(ColumnProjectionError):
            load_candles("BTCUSDT", "5m", DAY, DAY_END, columns=["vwap"])

    def test_unsupported_symbol_and_timeframe(self) -> None:
        with self.assertRaises(UnsupportedSymbolError):
            load_candles("BTCUSDT.P", "5m", DAY, DAY_END)
        with self.assertRaises(UnsupportedTimeframeError):
            load_candles("BTCUSDT", "5min", DAY, DAY_END)

    def test_determinism(self) -> None:
        a = load_candles("LTCUSDT", "30m", DAY, DAY_END)
        b = load_candles("LTCUSDT", "30m", DAY, DAY_END)
        self.assertEqual(a.open_times_ms(), b.open_times_ms())
        self.assertEqual(dict(a.source_file_hashes), dict(b.source_file_hashes))
        self.assertEqual(a.table.column("close").to_pylist(), b.table.column("close").to_pylist())

    def test_1m_analytical_mapping(self) -> None:
        sl = load_candles("BTCUSDT", "1m", DAY, datetime(2026, 8, 10, 0, 5, tzinfo=UTC))
        self.assertEqual(sl.row_count, 5)
        self.assertIn("quote_asset_volume", sl.table.column_names)
        self.assertNotIn("quote_volume", sl.table.column_names)
        self.assertNotIn("ignore", sl.table.column_names)
        self.assertEqual(sl.table.column("source_candle_count").to_pylist(), [1] * 5)


if __name__ == "__main__":
    unittest.main()
