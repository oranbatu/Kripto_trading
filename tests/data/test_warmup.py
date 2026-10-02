"""Warm-up / lookback tests using exact prior completed candles."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone

from data.exceptions import InsufficientWarmupError, InvalidTimeRangeError
from data.loader import load_candles
from data.timeutil import datetime_to_ms

UTC = timezone.utc


class TestWarmup(unittest.TestCase):
    def test_exact_warmup_count_smoke(self) -> None:
        sl = load_candles(
            "ETHUSDT", "1h",
            datetime(2025, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 8, tzinfo=UTC),
            warmup_bars=100,
        )
        self.assertEqual(sl.warmup_bars_loaded, 100)
        self.assertEqual(sl.trade_row_count, 168)
        self.assertEqual(sl.row_count, 268)
        self.assertEqual(sl.trade_start_index, 100)
        start_ms = datetime_to_ms(datetime(2025, 1, 1, tzinfo=UTC))
        warmup_closes = sl.close_times_ms()[:100]
        self.assertTrue(all(c < start_ms for c in warmup_closes))
        trade_opens = sl.open_times_ms()[100:]
        self.assertTrue(all(o >= start_ms for o in trade_opens))

    def test_warmup_across_month_boundary(self) -> None:
        sl = load_candles(
            "BTCUSDT", "1h",
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
            warmup_bars=24,
        )
        self.assertEqual(sl.warmup_bars_loaded, 24)
        self.assertEqual(sl.open_times_ms()[0], datetime_to_ms(datetime(2026, 7, 31, tzinfo=UTC)))
        self.assertEqual(len(sl.source_files), 2)

    def test_warmup_across_year_boundary(self) -> None:
        sl = load_candles(
            "NEARUSDT", "4h",
            datetime(2025, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 2, tzinfo=UTC),
            warmup_bars=6,
        )
        self.assertEqual(sl.warmup_bars_loaded, 6)
        self.assertEqual(sl.open_times_ms()[0], datetime_to_ms(datetime(2024, 12, 31, tzinfo=UTC)))
        self.assertTrue(any("2024" in p for p in sl.source_files))
        self.assertTrue(any("2025" in p for p in sl.source_files))

    def test_warmup_across_leap_day(self) -> None:
        sl = load_candles(
            "OPUSDT", "1h",
            datetime(2024, 3, 1, tzinfo=UTC),
            datetime(2024, 3, 2, tzinfo=UTC),
            warmup_bars=48,
        )
        self.assertEqual(sl.warmup_bars_loaded, 48)
        self.assertEqual(sl.open_times_ms()[0], datetime_to_ms(datetime(2024, 2, 28, tzinfo=UTC)))
        leap = datetime_to_ms(datetime(2024, 2, 29, 12, 0, tzinfo=UTC))
        self.assertIn(leap, sl.open_times_ms()[:48])

    def test_warmup_yearly_1d_partitions(self) -> None:
        sl = load_candles(
            "AAVEUSDT", "1d",
            datetime(2025, 1, 10, tzinfo=UTC),
            datetime(2025, 1, 15, tzinfo=UTC),
            warmup_bars=20,
        )
        self.assertEqual(sl.warmup_bars_loaded, 20)
        self.assertEqual(sl.trade_row_count, 5)
        self.assertEqual(sl.open_times_ms()[0], datetime_to_ms(datetime(2024, 12, 21, tzinfo=UTC)))
        self.assertTrue(any("year=2024" in p.replace("\\", "/") for p in sl.source_files))
        self.assertTrue(any("year=2025" in p.replace("\\", "/") for p in sl.source_files))

    def test_insufficient_warmup_strict(self) -> None:
        with self.assertRaises(InsufficientWarmupError):
            load_candles(
                "BTCUSDT", "1d",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 5, tzinfo=UTC),
                warmup_bars=10,
                require_full_warmup=True,
            )

    def test_partial_warmup_nonstrict(self) -> None:
        sl = load_candles(
            "BTCUSDT", "1d",
            datetime(2024, 1, 3, tzinfo=UTC),
            datetime(2024, 1, 5, tzinfo=UTC),
            warmup_bars=10,
            require_full_warmup=False,
        )
        self.assertEqual(sl.warmup_bars_loaded, 2)
        self.assertEqual(sl.trade_row_count, 2)
        self.assertEqual(sl.validation_summary["warmup_shortage"], 8)

    def test_no_warmup_row_closes_at_or_after_start(self) -> None:
        sl = load_candles(
            "ETHUSDT", "15m",
            datetime(2026, 8, 10, tzinfo=UTC),
            datetime(2026, 8, 11, tzinfo=UTC),
            warmup_bars=4,
        )
        start_ms = datetime_to_ms(datetime(2026, 8, 10, tzinfo=UTC))
        for close_ms in sl.close_times_ms()[: sl.trade_start_index]:
            self.assertLess(close_ms, start_ms)

    def test_negative_warmup_rejected(self) -> None:
        with self.assertRaises(InvalidTimeRangeError):
            load_candles(
                "BTCUSDT", "5m",
                datetime(2026, 8, 10, tzinfo=UTC),
                datetime(2026, 8, 11, tzinfo=UTC),
                warmup_bars=-1,
            )
        with self.assertRaises(InvalidTimeRangeError):
            load_candles(
                "BTCUSDT", "5m",
                datetime(2026, 8, 10, tzinfo=UTC),
                datetime(2026, 8, 11, tzinfo=UTC),
                warmup_bars=True,  # type: ignore[arg-type]
            )


if __name__ == "__main__":
    unittest.main()
