"""Coverage through 2026-10-27 and the half-open boundary at 2026-10-28."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

import config
from data.catalog import DataCatalog, expected_partition_keys
from data.exceptions import EmptyRangeError
from data.loader import load_candles, load_multi_timeframe
from data.timeutil import datetime_to_ms, ms_to_datetime
from tests.helpers import fingerprint_tree

UTC = timezone.utc
DATASET_END = datetime(2026, 10, 28, tzinfo=UTC)
FINAL_CLOSE = datetime(2026, 10, 27, 23, 59, 59, 999000, tzinfo=UTC)
DAY_ROWS = {
    "1m": 1_440,
    "5m": 288,
    "15m": 96,
    "30m": 48,
    "1h": 24,
    "4h": 6,
    "1d": 1,
}
TWO_DAY_ROWS = {tf: count * 2 for tf, count in DAY_ROWS.items()}
LAST_OPENS = {
    "1m": datetime(2026, 10, 27, 23, 59, tzinfo=UTC),
    "5m": datetime(2026, 10, 27, 23, 55, tzinfo=UTC),
    "15m": datetime(2026, 10, 27, 23, 45, tzinfo=UTC),
    "30m": datetime(2026, 10, 27, 23, 30, tzinfo=UTC),
    "1h": datetime(2026, 10, 27, 23, 0, tzinfo=UTC),
    "4h": datetime(2026, 10, 27, 20, 0, tzinfo=UTC),
    "1d": datetime(2026, 10, 27, tzinfo=UTC),
}


def _opens(sl) -> list[datetime]:
    return sl.table.column("open_time").to_pylist()


def _assert_regular(test: unittest.TestCase, sl, timeframe: str) -> None:
    spec = config.TIMEFRAME_SPECS[timeframe]
    opens = sl.open_times_ms()
    test.assertEqual(len(opens), len(set(opens)), timeframe)
    for previous, current in zip(opens, opens[1:]):
        test.assertEqual(current - previous, spec.duration_ms, timeframe)
    symbols = set(sl.table.column("symbol").to_pylist())
    test.assertEqual(symbols, {sl.symbol}, timeframe)
    open_type = sl.table.schema.field("open_time").type
    close_type = sl.table.schema.field("close_time").type
    test.assertTrue(pa.types.is_timestamp(open_type), timeframe)
    test.assertEqual(str(open_type.tz), "UTC", timeframe)
    test.assertEqual(str(close_type.tz), "UTC", timeframe)
    test.assertEqual(_opens(sl)[0].utcoffset(), timedelta(0), timeframe)


class CoverageBoundaryTests(unittest.TestCase):
    def test_metadata_matches_verified_inventory(self) -> None:
        self.assertEqual(
            {tf: spec.expected_files for tf, spec in config.TIMEFRAME_SPECS.items()},
            {"1m": 306, "5m": 306, "15m": 306, "30m": 306, "1h": 306, "4h": 306, "1d": 27},
        )
        self.assertEqual(
            {tf: spec.expected_rows_per_symbol for tf, spec in config.TIMEFRAME_SPECS.items()},
            {"1m": 1_484_640, "5m": 296_928, "15m": 98_976, "30m": 49_488, "1h": 24_744, "4h": 6_186, "1d": 1_031},
        )
        for timeframe, spec in config.TIMEFRAME_SPECS.items():
            self.assertEqual(spec.first_open_utc, datetime(2024, 1, 1, tzinfo=UTC), timeframe)
            self.assertEqual(spec.last_open_utc, LAST_OPENS[timeframe], timeframe)
            exclusive = spec.last_open_utc + timedelta(milliseconds=spec.duration_ms)
            self.assertEqual(exclusive, DATASET_END, timeframe)
            self.assertEqual(spec.expected_rows_per_symbol * len(config.ALLOWED_SYMBOLS), spec.expected_total_rows)
            if timeframe == "1d":
                self.assertEqual(spec.partitioning, "symbol_year")
                self.assertEqual(spec.expected_rows_per_symbol, 366 + 365 + 300)
            else:
                self.assertEqual(spec.partitioning, "symbol_year_month")
                self.assertEqual(spec.expected_files, 34 * len(config.ALLOWED_SYMBOLS))

    def test_catalog_months_and_years_are_exact(self) -> None:
        catalog = DataCatalog()
        for timeframe in ("1m", "5m", "15m", "30m", "1h", "4h"):
            keys = expected_partition_keys(timeframe)
            self.assertEqual(len(keys), 306, timeframe)
            self.assertEqual(len(set(keys)), 306, timeframe)
            months = sorted({(year, month) for _symbol, year, month in keys})
            self.assertEqual(months[0], (2024, 1), timeframe)
            self.assertEqual(months[-1], (2026, 10), timeframe)
            self.assertEqual(len(months), 34, timeframe)
            info = catalog.get_dataset_info(timeframe, inspect_metadata=False)
            self.assertEqual(info.actual_files, 306, timeframe)
            self.assertFalse(info.missing_partitions, timeframe)
            self.assertFalse(info.extra_files, timeframe)
            for symbol in config.ALLOWED_SYMBOLS:
                symbol_keys = [key for key in keys if key[0] == symbol]
                self.assertEqual(len(symbol_keys), 34, (timeframe, symbol))
                parts = catalog.resolve_partitions(
                    symbol, timeframe,
                    datetime(2026, 10, 1, tzinfo=UTC),
                    DATASET_END,
                )
                self.assertEqual([(p.year, p.month) for p in parts], [(2026, 10)], (timeframe, symbol))
        daily = expected_partition_keys("1d")
        self.assertEqual(len(daily), 27)
        self.assertEqual(len(set(daily)), 27)
        self.assertEqual(sorted({year for _symbol, year, _month in daily}), [2024, 2025, 2026])
        info = catalog.get_dataset_info("1d", inspect_metadata=False)
        self.assertEqual(info.actual_files, 27)
        self.assertFalse(info.missing_partitions)
        self.assertFalse(info.extra_files)
        for symbol in config.ALLOWED_SYMBOLS:
            parts = catalog.resolve_partitions(symbol, "1d", datetime(2024, 1, 1, tzinfo=UTC), DATASET_END)
            self.assertEqual([p.year for p in parts], [2024, 2025, 2026], symbol)
            self.assertTrue(all(p.month is None for p in parts), symbol)

    def test_first_candle_is_unchanged(self) -> None:
        start = datetime(2024, 1, 1, tzinfo=UTC)
        for symbol in config.ALLOWED_SYMBOLS:
            for timeframe, spec in config.TIMEFRAME_SPECS.items():
                end = start + timedelta(milliseconds=spec.duration_ms)
                sl = load_candles(symbol, timeframe, start, end)
                self.assertEqual(sl.row_count, 1, (symbol, timeframe))
                self.assertEqual(_opens(sl)[0], start, (symbol, timeframe))

    def test_october_27_and_final_candle(self) -> None:
        start = datetime(2026, 10, 27, tzinfo=UTC)
        for symbol in config.ALLOWED_SYMBOLS:
            for timeframe, spec in config.TIMEFRAME_SPECS.items():
                sl = load_candles(symbol, timeframe, start, DATASET_END, precision_mode="exact")
                self.assertEqual(sl.row_count, DAY_ROWS[timeframe], (symbol, timeframe))
                self.assertEqual(_opens(sl)[0], start, (symbol, timeframe))
                self.assertEqual(_opens(sl)[-1], LAST_OPENS[timeframe], (symbol, timeframe))
                self.assertEqual(sl.table.column("close_time")[-1].as_py(), FINAL_CLOSE, (symbol, timeframe))
                _assert_regular(self, sl, timeframe)
                for name in config.DECIMAL_COLUMNS:
                    value = sl.table.column(name)[0].as_py()
                    self.assertIsInstance(value, Decimal, (symbol, timeframe, name))
                    self.assertEqual(sl.table.schema.field(name).type.scale, 8, name)
                counts = sl.table.column("source_candle_count").to_pylist()
                self.assertEqual(set(counts), {spec.source_candles_per_output}, (symbol, timeframe))

    def test_october_28_is_out_of_range(self) -> None:
        start = datetime(2026, 10, 28, tzinfo=UTC)
        end = datetime(2026, 10, 29, tzinfo=UTC)
        for symbol in config.ALLOWED_SYMBOLS:
            for timeframe in config.ALLOWED_TIMEFRAMES:
                with self.assertRaises(EmptyRangeError):
                    load_candles(symbol, timeframe, start, end)

    def test_end_exclusive_includes_october_27_only_through_midnight(self) -> None:
        for symbol in ("BTCUSDT", "ETHUSDT"):
            included = load_candles(
                symbol, "1m",
                datetime(2026, 10, 27, 23, 59, tzinfo=UTC),
                DATASET_END,
            )
            self.assertEqual(included.row_count, 1, symbol)
            self.assertEqual(_opens(included)[0], datetime(2026, 10, 27, 23, 59, tzinfo=UTC))
            with self.assertRaises(EmptyRangeError):
                load_candles(symbol, "1m", DATASET_END, datetime(2026, 10, 28, 0, 1, tzinfo=UTC))

    def test_september_15_through_17_is_continuous(self) -> None:
        start = datetime(2026, 9, 15, tzinfo=UTC)
        end = datetime(2026, 9, 17, tzinfo=UTC)
        boundary = datetime_to_ms(datetime(2026, 9, 16, tzinfo=UTC))
        for symbol in config.ALLOWED_SYMBOLS:
            for timeframe, spec in config.TIMEFRAME_SPECS.items():
                sl = load_candles(symbol, timeframe, start, end, precision_mode="exact")
                self.assertEqual(sl.row_count, TWO_DAY_ROWS[timeframe], (symbol, timeframe))
                self.assertEqual(_opens(sl)[0], start, (symbol, timeframe))
                self.assertIn(boundary, sl.open_times_ms(), (symbol, timeframe))
                _assert_regular(self, sl, timeframe)
                last_open = start + timedelta(milliseconds=spec.duration_ms * (sl.row_count - 1))
                self.assertEqual(_opens(sl)[-1], last_open, (symbol, timeframe))

    def test_september_october_partition_boundary(self) -> None:
        start = datetime(2026, 9, 30, 20, 0, tzinfo=UTC)
        end = datetime(2026, 10, 1, 4, 0, tzinfo=UTC)
        expected = {"1m": 480, "5m": 96, "15m": 32, "30m": 16, "1h": 8, "4h": 2}
        for symbol in config.ALLOWED_SYMBOLS:
            for timeframe, rows in expected.items():
                sl = load_candles(symbol, timeframe, start, end, precision_mode="exact")
                self.assertEqual(sl.row_count, rows, (symbol, timeframe))
                self.assertEqual(_opens(sl)[0], start, (symbol, timeframe))
                self.assertEqual(len(sl.source_files), 2, (symbol, timeframe))
                normalized = [path.replace("\\", "/") for path in sl.source_files]
                self.assertTrue(any("month=09" in path for path in normalized), (symbol, timeframe))
                self.assertTrue(any("month=10" in path for path in normalized), (symbol, timeframe))
                _assert_regular(self, sl, timeframe)

    def test_warmup_crosses_former_september_15_endpoint(self) -> None:
        trade_start = datetime(2026, 10, 1, tzinfo=UTC)
        sl = load_candles(
            "BTCUSDT", "1h",
            trade_start,
            datetime(2026, 10, 2, tzinfo=UTC),
            warmup_bars=400,
        )
        self.assertEqual(sl.warmup_bars_loaded, 400)
        self.assertEqual(sl.trade_start_index, 400)
        self.assertEqual(sl.trade_row_count, 24)
        self.assertEqual(sl.row_count, 424)
        opens = sl.open_times_ms()
        self.assertEqual(len(opens), len(set(opens)))
        last_warmup = datetime(2026, 9, 30, 23, 0, tzinfo=UTC)
        first_warmup = last_warmup - timedelta(hours=399)
        self.assertEqual(ms_to_datetime(opens[0]), first_warmup)
        self.assertEqual(ms_to_datetime(opens[399]), last_warmup)
        self.assertEqual(ms_to_datetime(opens[400]), trade_start)
        warmup_set = set(opens[:400])
        self.assertIn(datetime_to_ms(datetime(2026, 9, 15, 23, 0, tzinfo=UTC)), warmup_set)
        self.assertIn(datetime_to_ms(datetime(2026, 9, 16, tzinfo=UTC)), warmup_set)
        start_ms = datetime_to_ms(trade_start)
        self.assertTrue(all(close < start_ms for close in sl.close_times_ms()[:400]))
        self.assertTrue(all(open_ms >= start_ms for open_ms in opens[400:]))
        self.assertLess(opens[-1], datetime_to_ms(datetime(2026, 10, 2, tzinfo=UTC)))

    def test_october_27_no_lookahead(self) -> None:
        start = datetime(2026, 10, 27, tzinfo=UTC)
        data = load_multi_timeframe(
            "BTCUSDT",
            ["5m", "15m", "30m", "1h", "4h", "1d"],
            "5m",
            start,
            DATASET_END,
            decision_clock="close",
        )
        base = data.slices["5m"]
        for timeframe in ("15m", "30m", "1h", "4h", "1d"):
            higher = data.slices[timeframe]
            for row, decision in enumerate(base.close_times_ms()):
                index = data.available_index(timeframe, row)
                if index < 0:
                    continue
                self.assertLessEqual(higher.close_times_ms()[index], decision, timeframe)

        def mapped(timeframe: str, base_open: datetime) -> datetime | None:
            row = base.index_of_open(base_open)
            index = data.available_index(timeframe, row)
            if index < 0:
                return None
            return ms_to_datetime(data.slices[timeframe].open_times_ms()[index])

        at_1030 = datetime(2026, 10, 27, 10, 30, tzinfo=UTC)
        self.assertEqual(mapped("15m", at_1030), datetime(2026, 10, 27, 10, 15, tzinfo=UTC))
        self.assertNotEqual(mapped("15m", at_1030), at_1030)
        self.assertEqual(mapped("30m", at_1030), datetime(2026, 10, 27, 10, 0, tzinfo=UTC))
        self.assertEqual(mapped("1h", at_1030), datetime(2026, 10, 27, 9, 0, tzinfo=UTC))
        self.assertEqual(mapped("4h", at_1030), datetime(2026, 10, 27, 4, 0, tzinfo=UTC))
        self.assertEqual(mapped("1d", at_1030), datetime(2026, 10, 26, tzinfo=UTC))
        self.assertEqual(
            mapped("1d", datetime(2026, 10, 27, 23, 50, tzinfo=UTC)),
            datetime(2026, 10, 26, tzinfo=UTC),
        )
        self.assertEqual(
            mapped("1d", datetime(2026, 10, 27, 23, 55, tzinfo=UTC)),
            datetime(2026, 10, 27, tzinfo=UTC),
        )

    def test_analysis_mode_is_in_memory_and_reads_do_not_rewrite(self) -> None:
        start = datetime(2026, 10, 27, tzinfo=UTC)
        exact = load_candles("ETHUSDT", "5m", start, DATASET_END, precision_mode="exact")
        before = fingerprint_tree([Path(path) for path in exact.source_files])
        analysis = load_candles("ETHUSDT", "5m", start, DATASET_END, precision_mode="analysis")
        again = load_candles("ETHUSDT", "5m", start, DATASET_END, precision_mode="analysis")
        after = fingerprint_tree([Path(path) for path in exact.source_files])
        self.assertEqual(before, after)
        self.assertEqual(analysis.precision_mode, "analysis")
        self.assertEqual(analysis.table.column("close").to_pylist(), again.table.column("close").to_pylist())
        for name in config.DECIMAL_COLUMNS:
            self.assertTrue(pa.types.is_decimal(exact.table.schema.field(name).type), name)
            self.assertTrue(pa.types.is_floating(analysis.table.schema.field(name).type), name)
            self.assertEqual(analysis.table.schema.field(name).type.bit_width, 64, name)
            for stored, converted in zip(
                exact.table.column(name).to_pylist(),
                analysis.table.column(name).to_pylist(),
            ):
                expected = float(stored)
                abs_err = abs(expected - converted)
                scale = max(abs(expected), abs(converted), 1.0)
                self.assertTrue(
                    abs_err <= config.ANALYSIS_ABSOLUTE_TOLERANCE
                    or abs_err / scale <= config.ANALYSIS_RELATIVE_TOLERANCE,
                    f"{name} exact={stored} analysis={converted}",
                )


if __name__ == "__main__":
    unittest.main()
