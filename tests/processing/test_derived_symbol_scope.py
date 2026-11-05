"""Symbol scope and synthetic aggregation for the four new USD-M symbols.

These tests use temporary directories only. They do not read or write production Parquet.
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import config
from processing.timeframes import (
    build_15m_parquet as b15,
    build_1d_parquet as b1d,
    build_1h_parquet as b1h,
    build_30m_parquet as b30,
    build_4h_parquet as b4h,
    build_5m_parquet as b5,
)
from processing.timeframes.symbol_scope import publication_symbols, validation_symbols

NEW = ("XRPUSDT", "DOGEUSDT", "ADAUSDT", "SOLUSDT")
MINUTE = 60_000


def _ms(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> int:
    return b15.ms_of(datetime(year, month, day, hour, minute, tzinfo=timezone.utc))


def _fold(rows):
    return (
        rows[0].open,
        max(row.high for row in rows),
        min(row.low for row in rows),
        rows[-1].close,
        sum((row.volume for row in rows), Decimal("0")),
        sum((row.quote_asset_volume for row in rows), Decimal("0")),
        sum(row.number_of_trades for row in rows),
        sum((row.taker_buy_base_asset_volume for row in rows), Decimal("0")),
        sum((row.taker_buy_quote_asset_volume for row in rows), Decimal("0")),
    )


class AllowlistTests(unittest.TestCase):
    def test_four_symbol_allowlist_and_nine_symbol_default(self):
        self.assertEqual(config.NEW_CANONICAL_1M_SYMBOLS, NEW)
        self.assertEqual(publication_symbols(config.ALLOWED_SYMBOLS), config.ALLOWED_SYMBOLS)
        self.assertTrue(set(NEW).isdisjoint(config.ALLOWED_SYMBOLS))
        argv = sys.argv
        try:
            sys.argv = ["builder", "--new-symbols-only"]
            selected = publication_symbols(config.ALLOWED_SYMBOLS)
            self.assertEqual(selected, NEW)
            self.assertEqual(validation_symbols("15m"), NEW)
        finally:
            sys.argv = argv

    def test_catalog_includes_thirteen_symbols_after_publication(self):
        for timeframe in ("1m", "5m", "15m", "30m", "1h", "4h", "1d"):
            self.assertEqual(config.symbols_for_timeframe(timeframe), config.CANONICAL_1M_SYMBOLS)
        self.assertEqual(len(config.ALLOWED_SYMBOLS), 9)
        self.assertIn("XRPUSDT", config.symbols_for_timeframe("30m"))

    def test_unsupported_symbol_and_path_traversal_are_rejected(self):
        with self.assertRaises(ValueError):
            config.require_canonical_symbols(("FOOUSDT",))
        with self.assertRaises(ValueError):
            config.require_canonical_symbols((r"XRPUSDT\..\BTCUSDT",))
        with self.assertRaises(ValueError):
            config.require_canonical_symbols(("XRPUSDT/../BTCUSDT",))
        with self.assertRaises(ValueError):
            publication_symbols(("BTCUSDT",))

    def test_scope_module_does_not_reference_detectors(self):
        text = Path("processing/timeframes/symbol_scope.py").read_text(encoding="utf-8")
        self.assertNotIn("detectors", text)
        self.assertNotIn("detector_control_center", text)


class DiscoveryTests(unittest.TestCase):
    def test_scoped_discovery_selects_only_requested_symbol(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "1m"
            output = root / "15m"
            for symbol in ("XRPUSDT", "BTCUSDT"):
                path = b15.hive_path(source, symbol, 2024, 1)
                path.parent.mkdir(parents=True)
                path.write_bytes(b"")
            original = b15.iter_year_months
            b15.iter_year_months = lambda _start, _end: [(2024, 1)]
            try:
                found = b15.discover_partitions(
                    source, output, symbols=("XRPUSDT",), require_expected_counts=False,
                )
            finally:
                b15.iter_year_months = original
            self.assertEqual([item.symbol for item in found], ["XRPUSDT"])
            self.assertEqual(found[0].output_path, b15.hive_path(output, "XRPUSDT", 2024, 1))
            self.assertNotIn("BTCUSDT", str(found[0].output_path))

    def test_unknown_hive_symbol_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "1m"
            output = root / "15m"
            good = b15.hive_path(source, "XRPUSDT", 2024, 1)
            good.parent.mkdir(parents=True)
            good.write_bytes(b"")
            bad = b15.hive_path(source, "NOTASYMBOL", 2024, 1)
            bad.parent.mkdir(parents=True)
            bad.write_bytes(b"")
            original = b15.iter_year_months
            b15.iter_year_months = lambda _start, _end: [(2024, 1)]
            try:
                with self.assertRaises(RuntimeError):
                    b15.discover_partitions(
                        source, output, symbols=("XRPUSDT",), require_expected_counts=False,
                    )
            finally:
                b15.iter_year_months = original

    def test_daily_output_is_yearly_and_intraday_output_is_monthly(self):
        self.assertEqual(
            b1d.hive_year_path(Path("derived"), "SOLUSDT", 2026).parts[-2:],
            ("year=2026", "data.parquet"),
        )
        monthly = b5.hive_month_path(Path("derived"), "ADAUSDT", 2026, 10)
        self.assertIn("month=10", monthly.parts)
        self.assertNotIn("month=10", b1d.hive_year_path(Path("derived"), "ADAUSDT", 2026).parts)


class AggregationTests(unittest.TestCase):
    def _write(self, path: Path, symbol: str, start: int, count: int, skip: int | None = None) -> None:
        b15._write_synthetic_1m(path, symbol, start, count, skip_open_ms=skip)

    def _assert_bucket(self, row, count: int) -> None:
        last_index = count - 1
        first = Decimal("100.00000000")
        last_open = first + Decimal(last_index) / Decimal("100000000")
        self.assertEqual(row.open, first)
        self.assertEqual(row.high, last_open + Decimal("1"))
        self.assertEqual(row.low, first)
        self.assertEqual(row.close, last_open + Decimal("0.50000000"))
        self.assertEqual(row.volume, Decimal("10.00000000") * count)
        self.assertEqual(row.quote_asset_volume, Decimal("100.00000000") * count)
        self.assertEqual(row.number_of_trades, 3 * count)
        self.assertEqual(row.taker_buy_base_asset_volume, Decimal("4.00000000") * count)
        self.assertEqual(row.taker_buy_quote_asset_volume, Decimal("40.00000000") * count)
        self.assertEqual(row.source_candle_count, count)
        self.assertGreaterEqual(row.high, row.open)
        self.assertGreaterEqual(row.high, row.close)
        self.assertGreaterEqual(row.high, row.low)
        self.assertLessEqual(row.low, row.open)
        self.assertLessEqual(row.low, row.close)
        self.assertEqual(row.close_ms, row.open_ms + count * MINUTE - 1)

    def test_each_timeframe_aggregates_one_complete_bucket(self):
        start = _ms(2024, 1, 1)
        cases = (
            (b5, 5, lambda path, out: b5.PartitionRef(
                "XRPUSDT", 2024, 1, path, path, path, path, path, path, out, start, start + 4 * MINUTE,
            )),
            (b15, 15, lambda path, out: b15.PartitionRef(
                "DOGEUSDT", 2024, 1, path, out, start, start + 14 * MINUTE,
            )),
            (b30, 30, lambda path, out: b30.PartitionRef(
                "ADAUSDT", 2024, 1, path, path, out, start, start + 29 * MINUTE,
            )),
            (b1h, 60, lambda path, out: b1h.PartitionRef(
                "SOLUSDT", 2024, 1, path, path, path, out, start, start + 59 * MINUTE,
            )),
            (b4h, 240, lambda path, out: b4h.PartitionRef(
                "XRPUSDT", 2024, 1, path, path, path, path, out, start, start + 239 * MINUTE,
            )),
            (b1d, 1440, lambda path, out: b1d.YearPartitionRef(
                "DOGEUSDT", 2024, (), out, start, start + 1439 * MINUTE,
            )),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for module, count, make_part in cases:
                symbol = make_part(root / "unused", root / "unused").symbol
                source = root / f"{module.OUTPUT_INTERVAL}.parquet"
                self._write(source, symbol, start, count)
                table = module.read_1m_table(source)
                part = make_part(source, root / f"{module.OUTPUT_INTERVAL}-out.parquet")
                work = module.aggregate_partition(part, table)
                self.assertEqual(len(work.rows), 1, module.OUTPUT_INTERVAL)
                self.assertEqual(work.incomplete_buckets, [], module.OUTPUT_INTERVAL)
                self._assert_bucket(work.rows[0], count)
                self.assertEqual(work.rows[0].open_ms, start)

    def test_leap_day_partial_october_and_october_28_exclusion(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            leap = _ms(2024, 2, 29)
            source = root / "leap.parquet"
            self._write(source, "XRPUSDT", leap, 15)
            part = b15.PartitionRef("XRPUSDT", 2024, 2, source, root / "leap-out.parquet", leap, leap + 14 * MINUTE)
            row = b15.aggregate_partition(part, b15.read_1m_table(source)).rows[0]
            self.assertEqual(b15.iso_of_ms(row.open_ms), "2024-02-29T00:00:00Z")
            october = _ms(2026, 10, 27, 23, 45)
            source = root / "october.parquet"
            self._write(source, "SOLUSDT", october, 15)
            part = b15.PartitionRef(
                "SOLUSDT", 2026, 10, source, root / "october-out.parquet", october, october + 14 * MINUTE,
            )
            row = b15.aggregate_partition(part, b15.read_1m_table(source)).rows[0]
            self.assertEqual(b15.iso_of_ms(row.open_ms), "2026-10-27T23:45:00Z")
            self.assertEqual(b15.iso_of_ms_us(row.close_ms), "2026-10-27T23:59:59.999Z")
            self.assertLess(row.open_ms, _ms(2026, 10, 28))
            self.assertEqual(b1d.EXPECTED_OUTPUT_ROWS_BY_YEAR, {2024: 366, 2025: 365, 2026: 300})
            self.assertEqual(27 * 96, 2592)
            self.assertEqual(27 * 288, 7776)

    def test_missing_duplicate_and_incomplete_buckets_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            start = _ms(2024, 3, 1)
            missing = root / "missing.parquet"
            self._write(missing, "ADAUSDT", start, 15, skip=_ms(2024, 3, 1, 0, 7))
            part = b15.PartitionRef("ADAUSDT", 2024, 3, missing, root / "missing-out.parquet", start, start + 14 * MINUTE)
            work = b15.aggregate_partition(part, b15.read_1m_table(missing))
            self.assertEqual(work.rows, [])
            self.assertTrue(work.incomplete_buckets)
            published = b15.process_partition(part)
            self.assertEqual(published["status"], "failed")
            self.assertFalse(part.output_path.exists())
            duplicate = root / "duplicate.parquet"
            self._write(duplicate, "ADAUSDT", start, 15)
            table = b15.read_1m_table(duplicate)
            doubled = b15.pa.concat_tables([table, table.slice(0, 1)])
            part = b15.PartitionRef("ADAUSDT", 2024, 3, duplicate, root / "duplicate-out.parquet", start, start + 14 * MINUTE)
            with self.assertRaises(ValueError):
                b15.aggregate_partition(part, doubled)

    def test_atomic_publication_skip_and_second_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            start = _ms(2024, 4, 1)
            source = root / "source.parquet"
            output = root / "symbol=DOGEUSDT" / "year=2024" / "month=04" / "data.parquet"
            self._write(source, "DOGEUSDT", start, 15)
            part = b15.PartitionRef("DOGEUSDT", 2024, 4, source, output, start, start + 14 * MINUTE)
            created = b15.process_partition(part)
            self.assertEqual(created["status"], "created")
            written = b15.pq.ParquetFile(output)
            try:
                self.assertEqual(written.metadata.row_group(0).column(0).compression, "ZSTD")
                metadata = {key.decode(): value.decode() for key, value in written.schema_arrow.metadata.items()}
                self.assertEqual(metadata["schema_version"], b15.SCHEMA_VERSION)
                self.assertEqual(metadata["source_canonical_sha256"], created["source_canonical_sha256"])
                self.assertEqual(str(written.schema_arrow.field("open").type), "decimal128(38, 8)")
            finally:
                written.close()
            first_stat = output.stat()
            again = b15.process_partition(part)
            self.assertEqual(again["status"], "skipped-as-already-equivalent")
            self.assertEqual(output.stat().st_mtime_ns, first_stat.st_mtime_ns)
            self.assertEqual(output.stat().st_size, first_stat.st_size)
            self.assertFalse(list(output.parent.glob("*.part")))
            output.write_bytes(b"not parquet")
            rebuilt = b15.process_partition(part)
            self.assertEqual(rebuilt["status"], "rebuilt", rebuilt.get("error"))
            checked = b15.pq.ParquetFile(output)
            try:
                self.assertGreater(checked.metadata.num_rows, 0)
            finally:
                checked.close()
            self.assertFalse(list(output.parent.glob("*.part")))

    def test_lower_timeframe_folds_exactly_into_higher_timeframe(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            start = _ms(2024, 1, 1)
            source = root / "source.parquet"
            self._write(source, "XRPUSDT", start, 30)
            table = b15.read_1m_table(source)
            five = b5.aggregate_partition(
                b5.PartitionRef("XRPUSDT", 2024, 1, source, source, source, source, source, source, root / "5.parquet", start, start + 29 * MINUTE),
                table,
            )
            fifteen = b15.aggregate_partition(
                b15.PartitionRef("XRPUSDT", 2024, 1, source, root / "15.parquet", start, start + 29 * MINUTE),
                table,
            )
            thirty = b30.aggregate_partition(
                b30.PartitionRef("XRPUSDT", 2024, 1, source, source, root / "30.parquet", start, start + 29 * MINUTE),
                table,
            )
            self.assertEqual(len(five.rows), 6)
            self.assertEqual(len(fifteen.rows), 2)
            self.assertEqual(len(thirty.rows), 1)
            self.assertEqual(_fold(five.rows[:3]), _fold(fifteen.rows[:1]))
            self.assertEqual(_fold(fifteen.rows), _fold(thirty.rows))
            self.assertEqual(_fold(five.rows), _fold(thirty.rows))


if __name__ == "__main__":
    unittest.main()
