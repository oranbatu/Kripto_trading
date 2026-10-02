"""Catalog discovery, inventory, and rejection tests."""
from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import config
from data.catalog import DataCatalog
from data.exceptions import PartitionNotFoundError, UnsupportedSymbolError, UnsupportedTimeframeError

UTC = timezone.utc


class TestCatalog(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cat = DataCatalog()

    def test_all_timeframes_discovered(self) -> None:
        for tf in config.ALLOWED_TIMEFRAMES:
            info = self.cat.get_dataset_info(tf, inspect_metadata=False)
            self.assertEqual(info.timeframe, tf)
            self.assertTrue(info.root.is_dir(), tf)
            self.assertEqual(info.actual_files, config.TIMEFRAME_SPECS[tf].expected_files, tf)
            self.assertEqual(info.expected_files, info.actual_files, tf)
            self.assertEqual(set(info.symbols), set(config.ALLOWED_SYMBOLS), tf)
            self.assertFalse(info.extra_files, tf)
            self.assertFalse(info.missing_partitions, tf)
            self.assertFalse(info.unexpected_symbols, tf)

    def test_expected_file_counts(self) -> None:
        expected = {"1m": 297, "5m": 297, "15m": 297, "30m": 297, "1h": 297, "4h": 297, "1d": 27}
        for tf, count in expected.items():
            info = self.cat.get_dataset_info(tf, inspect_metadata=False)
            self.assertEqual(info.actual_files, count)

    def test_nine_symbols(self) -> None:
        self.assertEqual(len(config.ALLOWED_SYMBOLS), 9)
        info = self.cat.get_dataset_info("5m", inspect_metadata=False)
        self.assertEqual(len(info.symbols), 9)

    def test_monthly_versus_yearly_routing(self) -> None:
        monthly = self.cat.resolve_partitions(
            "BTCUSDT", "1h",
            datetime(2026, 8, 10, tzinfo=UTC),
            datetime(2026, 8, 11, tzinfo=UTC),
        )
        self.assertEqual(len(monthly), 1)
        self.assertEqual(monthly[0].month, 8)
        self.assertTrue(str(monthly[0].path).endswith(r"symbol=BTCUSDT\year=2026\month=08\data.parquet")
                        or str(monthly[0].path).replace("\\", "/").endswith("symbol=BTCUSDT/year=2026/month=08/data.parquet"))
        yearly = self.cat.resolve_partitions(
            "ETHUSDT", "1d",
            datetime(2024, 12, 31, tzinfo=UTC),
            datetime(2025, 1, 2, tzinfo=UTC),
        )
        self.assertEqual([p.year for p in yearly], [2024, 2025])
        self.assertTrue(all(p.month is None for p in yearly))

    def test_metadata_sample_all_timeframes(self) -> None:
        for tf in config.ALLOWED_TIMEFRAMES:
            parts = self.cat.resolve_partitions(
                "BTCUSDT", tf,
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 2, tzinfo=UTC),
            )
            self.assertGreaterEqual(len(parts), 1, tf)
            self.assertEqual(parts[0].schema_version, config.TIMEFRAME_SPECS[tf].schema_version, tf)
            self.assertEqual(parts[0].interval_meta, tf, tf)
            self.assertEqual(parts[0].metadata.get("timezone") in (None, "UTC"), True, tf)

    def test_missing_partition_detection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cat = DataCatalog(Path(tmp))
            with self.assertRaises(PartitionNotFoundError) as ctx:
                cat.resolve_partitions(
                    "BTCUSDT", "1h",
                    datetime(2026, 8, 10, tzinfo=UTC),
                    datetime(2026, 8, 11, tzinfo=UTC),
                )
            self.assertIn("BTCUSDT", str(ctx.exception))
            self.assertIn("1h", str(ctx.exception))

    def test_unexpected_symbol_rejection(self) -> None:
        with self.assertRaises(UnsupportedSymbolError):
            self.cat.resolve_partitions(
                "SOLUSDT", "5m",
                datetime(2026, 8, 10, tzinfo=UTC),
                datetime(2026, 8, 11, tzinfo=UTC),
            )
        with self.assertRaises(UnsupportedSymbolError):
            self.cat.resolve_partitions(
                "btcusdt", "5m",
                datetime(2026, 8, 10, tzinfo=UTC),
                datetime(2026, 8, 11, tzinfo=UTC),
            )

    def test_unsupported_timeframe_rejection(self) -> None:
        with self.assertRaises(UnsupportedTimeframeError):
            self.cat.get_dataset_info("1H")
        with self.assertRaises(UnsupportedTimeframeError):
            self.cat.get_dataset_info("2h")
        with self.assertRaises(UnsupportedTimeframeError):
            self.cat.get_dataset_info("1w")

    def test_cross_month_uses_two_partitions(self) -> None:
        parts = self.cat.resolve_partitions(
            "BTCUSDT", "5m",
            datetime(2026, 8, 31, 22, 0, tzinfo=UTC),
            datetime(2026, 9, 1, 2, 0, tzinfo=UTC),
        )
        self.assertEqual([(p.year, p.month) for p in parts], [(2026, 8), (2026, 9)])
        self.assertLess(parts[0].start_utc, parts[1].start_utc)


if __name__ == "__main__":
    unittest.main()
