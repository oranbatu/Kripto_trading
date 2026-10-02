"""Exact decimal128 versus in-memory analysis float64 tests."""
from __future__ import annotations

import math
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

import config
from data.loader import load_candles
from tests.helpers import fingerprint_tree

UTC = timezone.utc
START = datetime(2026, 8, 10, tzinfo=UTC)
END = datetime(2026, 8, 11, tzinfo=UTC)


class TestPrecision(unittest.TestCase):
    def test_exact_mode_keeps_decimal128(self) -> None:
        sl = load_candles("BTCUSDT", "5m", START, END, precision_mode="exact")
        for name in config.DECIMAL_COLUMNS:
            t = sl.table.schema.field(name).type
            self.assertTrue(pa.types.is_decimal(t), name)
            self.assertEqual(t.precision, 38, name)
            self.assertEqual(t.scale, 8, name)
            value = sl.table.column(name)[0].as_py()
            self.assertIsInstance(value, Decimal, name)

    def test_analysis_mode_float64_only_in_memory(self) -> None:
        exact = load_candles("BTCUSDT", "5m", START, END, precision_mode="exact")
        analysis = load_candles("BTCUSDT", "5m", START, END, precision_mode="analysis")
        self.assertEqual(analysis.precision_mode, "analysis")
        for name in config.DECIMAL_COLUMNS:
            t = analysis.table.schema.field(name).type
            self.assertTrue(pa.types.is_floating(t), name)
            self.assertEqual(t.bit_width, 64, name)
            for value in analysis.table.column(name).to_pylist():
                self.assertTrue(math.isfinite(value), name)
        self.assertEqual(analysis.table.schema.field("number_of_trades").type, pa.int64())
        self.assertTrue(pa.types.is_integer(analysis.table.schema.field("source_candle_count").type))
        self.assertTrue(pa.types.is_timestamp(analysis.table.schema.field("open_time").type))
        self.assertEqual(str(analysis.table.schema.field("open_time").type.tz), "UTC")
        # Exact table is a different object and still decimal after analysis conversion.
        self.assertTrue(pa.types.is_decimal(exact.table.schema.field("close").type))
        self.assertIsNot(exact.table, analysis.table)

    def test_exact_and_analysis_agree_within_tolerance(self) -> None:
        exact = load_candles("ETHUSDT", "15m", START, END, precision_mode="exact")
        analysis = load_candles("ETHUSDT", "15m", START, END, precision_mode="analysis")
        for name in ("open", "high", "low", "close", "volume"):
            for ex, an in zip(exact.table.column(name).to_pylist(), analysis.table.column(name).to_pylist()):
                converted = float(ex)
                abs_err = abs(converted - an)
                scale = max(abs(converted), abs(an), 1.0)
                self.assertTrue(
                    abs_err <= config.ANALYSIS_ABSOLUTE_TOLERANCE
                    or abs_err / scale <= config.ANALYSIS_RELATIVE_TOLERANCE,
                    f"{name} exact={ex} analysis={an} abs_err={abs_err}",
                )

    def test_analysis_does_not_mutate_source_files(self) -> None:
        sl = load_candles("BTCUSDT", "1h", START, END, precision_mode="exact")
        before = fingerprint_tree([Path(p) for p in sl.source_files])
        load_candles("BTCUSDT", "1h", START, END, precision_mode="analysis")
        after = fingerprint_tree([Path(p) for p in sl.source_files])
        self.assertEqual(before, after)

    def test_exact_table_not_mutated_by_later_analysis_call(self) -> None:
        exact = load_candles("LTCUSDT", "30m", START, END, precision_mode="exact")
        close_before = exact.table.column("close").to_pylist()
        load_candles("LTCUSDT", "30m", START, END, precision_mode="analysis")
        self.assertTrue(pa.types.is_decimal(exact.table.schema.field("close").type))
        self.assertEqual(exact.table.column("close").to_pylist(), close_before)


if __name__ == "__main__":
    unittest.main()
