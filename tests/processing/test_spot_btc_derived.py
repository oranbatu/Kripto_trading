"""Isolated tests for Spot BTC derived-timeframe aggregation."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pyarrow.parquet as pq

from ingestion.binance.spot_btc_pairs import SPOT_SYMBOLS, ms_of, reject_symbol
from processing.timeframes.build_spot_btc_derived import (
    AGGREGATION_RULES,
    DERIVED_ROOT,
    OUTPUT_COLUMNS,
    SOURCE_ROOT,
    TIMEFRAMES,
    DerivedLock,
    aggregate_block,
    assert_spot_scope,
    assert_under_spot,
    block_to_table,
    blocks_equal,
    derived_partition,
    fingerprint_block,
        fold_block,
        hierarchical_checks,
        output_schema,
    publish_equivalent,
    write_atomic,
)

PY = Path(r"C:\Users\oranb\Desktop\backtest_system\.venv\Scripts\python.exe")


def _minutes(start: int, count: int, *, high: int = 30000000, low: int = 10000000, volume: int = 1) -> dict:
    block = {key: [] for key in ("open_ms", "close_ms", "open", "high", "low", "close", "volume", "quote", "trades", "taker_base", "taker_quote")}
    for offset in range(count):
        open_ms = start + offset * 60_000
        block["open_ms"].append(open_ms)
        block["close_ms"].append(open_ms + 59_999)
        block["open"].append(20000000 + offset)
        block["high"].append(high + offset)
        block["low"].append(low)
        block["close"].append(25000000 + offset)
        block["volume"].append(volume)
        block["quote"].append(volume + 1)
        block["trades"].append(2)
        block["taker_base"].append(volume)
        block["taker_quote"].append(1)
    return block


class ScopeTests(unittest.TestCase):
    def test_allowlist_and_rejections(self):
        self.assertEqual(len(SPOT_SYMBOLS), 10)
        for symbol in SPOT_SYMBOLS:
            self.assertEqual(assert_spot_scope(symbol), symbol)
        for symbol in ("BTCBTC", "AAVEBTC", "OPBTC", "ETHUSDT", "ETHUSD", "ETHUSDC", "ETHFDUSD", "ETHUSD1", "ETHBNB", "BTCUSDT", "ETHBTC.P", "ethbtc", "ETH BTC", "../ETHBTC", "ETHBTC/SOLBTC"):
            with self.assertRaises(ValueError):
                reject_symbol(symbol)

    def test_roots_reject_futures(self):
        self.assertTrue(str(SOURCE_ROOT).endswith(r"processed\binance\spot\1m"))
        self.assertTrue(str(DERIVED_ROOT).endswith(r"derived\binance\spot"))
        root = Path(self._temp()) / "spot"
        allowed = root / "5m" / "data.parquet"
        allowed.parent.mkdir(parents=True)
        self.assertEqual(assert_under_spot(allowed, root), allowed.resolve())
        forbidden = root / "futures" / "data.parquet"
        forbidden.parent.mkdir()
        with self.assertRaises(ValueError):
            assert_under_spot(forbidden, root)

    def _temp(self) -> Path:
        path = Path(tempfile.mkdtemp(prefix="spot_derived_test_"))
        self.addCleanup(lambda: __import__("shutil").rmtree(path, ignore_errors=True))
        return path


class AggregationTests(unittest.TestCase):
    def _start(self, day: str) -> int:
        year, month, dom = (int(part) for part in day.split("-"))
        return ms_of(datetime(year, month, dom, tzinfo=timezone.utc))

    def test_bucket_alignment_and_ohlcv(self):
        start = self._start("2024-06-01")
        block = _minutes(start, 240, high=90000000, low=10000000, volume=1)
        block["high"][3] = 91000000
        block["low"][4] = 5000000
        five = aggregate_block(block, 5, TIMEFRAMES["5m"]["bucket_ms"])
        self.assertEqual(len(five["open_ms"]), 48)
        self.assertEqual([datetime.fromtimestamp(value / 1000, timezone.utc).minute for value in five["open_ms"][:12]], list(range(0, 60, 5)))
        self.assertEqual(five["open"][0], block["open"][0])
        self.assertEqual(five["close"][0], block["close"][4])
        self.assertEqual(five["high"][0], 91000000)
        self.assertEqual(five["low"][0], 5000000)
        self.assertEqual(five["volume"][0], 5)
        self.assertEqual(five["quote"][0], 10)
        self.assertEqual(five["trades"][0], 10)
        self.assertEqual(five["taker_base"][0], 5)
        self.assertEqual(five["taker_quote"][0], 5)
        fifteen = aggregate_block(block, 15, TIMEFRAMES["15m"]["bucket_ms"])
        thirty = aggregate_block(block, 30, TIMEFRAMES["30m"]["bucket_ms"])
        hour = aggregate_block(block, 60, TIMEFRAMES["1h"]["bucket_ms"])
        four = aggregate_block(block, 240, TIMEFRAMES["4h"]["bucket_ms"])
        self.assertEqual([datetime.fromtimestamp(value / 1000, timezone.utc).minute for value in fifteen["open_ms"]], [0, 15, 30, 45] * 4)
        self.assertEqual([datetime.fromtimestamp(value / 1000, timezone.utc).minute for value in thirty["open_ms"]], [0, 30] * 4)
        self.assertEqual([datetime.fromtimestamp(value / 1000, timezone.utc).minute for value in hour["open_ms"]], [0] * 4)
        self.assertEqual(datetime.fromtimestamp(four["open_ms"][0] / 1000, timezone.utc).hour, 0)
        folded = fold_block(five, 3)
        self.assertTrue(blocks_equal(folded, fifteen)[0])
        self.assertTrue(blocks_equal(fold_block(five, 12), hour)[0])
        self.assertTrue(blocks_equal(fold_block(hour, 4), four)[0])
        day = _minutes(start, 1440, high=90000000, low=10000000, volume=1)
        four_hours = aggregate_block(day, 240, TIMEFRAMES["4h"]["bucket_ms"])
        self.assertEqual([datetime.fromtimestamp(value / 1000, timezone.utc).hour for value in four_hours["open_ms"]], [0, 4, 8, 12, 16, 20])
        daily = aggregate_block(day, 1440, TIMEFRAMES["1d"]["bucket_ms"])
        self.assertEqual(datetime.fromtimestamp(daily["open_ms"][0] / 1000, timezone.utc).hour, 0)
        self.assertTrue(blocks_equal(fold_block(four_hours, 6), daily)[0])
        self.assertTrue(blocks_equal(fold_block(aggregate_block(day, 5, TIMEFRAMES["5m"]["bucket_ms"]), 288), daily)[0])

    def test_day_month_year_leap_and_october_edges(self):
        leap = aggregate_block(_minutes(self._start("2024-02-29"), 1440), 1440, TIMEFRAMES["1d"]["bucket_ms"])
        self.assertEqual(len(leap["open_ms"]), 1)
        self.assertEqual(leap["volume"][0], 1440)
        month_edge = aggregate_block(_minutes(ms_of(datetime(2024, 1, 31, 23, 55, tzinfo=timezone.utc)), 10), 5, TIMEFRAMES["5m"]["bucket_ms"])
        self.assertEqual(datetime.fromtimestamp(month_edge["open_ms"][1] / 1000, timezone.utc).day, 1)
        year_edge = aggregate_block(_minutes(ms_of(datetime(2024, 12, 31, 23, 55, tzinfo=timezone.utc)), 10), 5, TIMEFRAMES["5m"]["bucket_ms"])
        self.assertEqual(datetime.fromtimestamp(year_edge["open_ms"][1] / 1000, timezone.utc).year, 2025)
        october = aggregate_block(_minutes(self._start("2026-10-27"), 1440), 1440, TIMEFRAMES["1d"]["bucket_ms"])
        self.assertEqual(datetime.fromtimestamp(october["open_ms"][0] / 1000, timezone.utc).day, 27)
        with self.assertRaises(ValueError):
            aggregate_block(_minutes(ms_of(datetime(2026, 10, 27, 23, 55, tzinfo=timezone.utc)), 10), 5, TIMEFRAMES["5m"]["bucket_ms"])

    def test_rejects_incomplete_duplicate_gap_and_bad_ohlc(self):
        start = self._start("2024-06-01")
        with self.assertRaises(ValueError):
            aggregate_block(_minutes(start, 4), 5, TIMEFRAMES["5m"]["bucket_ms"])
        duplicate = _minutes(start, 5)
        duplicate["open_ms"][4] = duplicate["open_ms"][3]
        with self.assertRaises(ValueError):
            aggregate_block(duplicate, 5, TIMEFRAMES["5m"]["bucket_ms"])
        gap = _minutes(start, 5)
        gap["open_ms"][3] += 60_000
        with self.assertRaises(ValueError):
            aggregate_block(gap, 5, TIMEFRAMES["5m"]["bucket_ms"])
        bad = _minutes(start, 5)
        bad["high"][1] = 1
        with self.assertRaises(ValueError):
            aggregate_block(bad, 5, TIMEFRAMES["5m"]["bucket_ms"])
        swapped = _minutes(start, 5)
        swapped["open_ms"][1], swapped["open_ms"][2] = swapped["open_ms"][2], swapped["open_ms"][1]
        with self.assertRaises(ValueError):
            aggregate_block(swapped, 5, TIMEFRAMES["5m"]["bucket_ms"])
        malformed_close = _minutes(start, 5)
        malformed_close["close_ms"][2] += 1
        with self.assertRaises(ValueError):
            aggregate_block(malformed_close, 5, TIMEFRAMES["5m"]["bucket_ms"])

    def test_publication_skip_and_conflicts(self):
        root = Path(tempfile.mkdtemp(prefix="spot_derived_pub_"))
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        start = self._start("2024-06-01")
        block = aggregate_block(_minutes(start, 5, volume=1), 5, TIMEFRAMES["5m"]["bucket_ms"])
        schema = output_schema("5m")
        table = block_to_table("ETHBTC", block, 5, schema)
        final = derived_partition(root / "derived" / "binance" / "spot", "5m", "ETHBTC", 2024, 6)
        metadata = {
            "provider": "Binance", "market_type": "spot", "symbol": "ETHBTC", "base_asset": "ETH",
            "quote_asset": "BTC", "source_timeframe": "1m", "timeframe": "5m", "schema_version": "spot-5m-derived-v1",
            "coverage_start": "2024-06-01T00:00:00Z", "coverage_end": "2024-06-01T00:04:59.999Z",
            "exclusive_end": "2024-06-01T00:05:00Z", "source_partitions": "month=06", "source_fingerprints": "abc",
            "source_schema": "spot-1m-canonical-v1", "builder_version": "spot-btc-derived-v1",
            "publication_run_id": "run-a", "aggregation_rules": AGGREGATION_RULES, "expected_component_count": "5",
        }
        write_atomic(table, final, metadata)
        self.assertFalse(final.with_name("data.parquet.part").exists())
        self.assertEqual(tuple(pq.read_schema(final).names), OUTPUT_COLUMNS)
        self.assertEqual(pq.read_schema(final).metadata[b"schema_version"], b"spot-5m-derived-v1")
        self.assertEqual(pq.read_schema(final).metadata[b"market_type"], b"spot")
        self.assertIn("ZSTD", str(pq.ParquetFile(final).metadata.row_group(0).column(0).compression).upper())
        staged = root / "staged.parquet"
        write_atomic(table, staged, metadata)
        before = final.stat().st_mtime_ns
        self.assertEqual(publish_equivalent(staged, final, root / "quarantine"), "skipped-equivalent")
        self.assertEqual(final.stat().st_mtime_ns, before)
        changed = aggregate_block(_minutes(start, 5, volume=2), 5, TIMEFRAMES["5m"]["bucket_ms"])
        write_atomic(block_to_table("ETHBTC", changed, 5, schema), staged, metadata)
        with self.assertRaises(RuntimeError):
            publish_equivalent(staged, final, root / "quarantine")
        self.assertEqual(final.stat().st_mtime_ns, before)
        write_atomic(table, staged, {**metadata, "schema_version": "spot-5m-derived-v0"})
        with self.assertRaises(RuntimeError) as caught:
            publish_equivalent(staged, final, root / "quarantine")
        self.assertIn("metadata", str(caught.exception))
        self.assertEqual(final.stat().st_mtime_ns, before)
        self.assertEqual(fingerprint_block("ETHBTC", block), fingerprint_block("ETHBTC", block))
        self.assertTrue(str(derived_partition(root, "1d", "ETHBTC", 2024)).endswith(r"year=2024\data.parquet"))
        self.assertTrue(str(derived_partition(root, "5m", "ETHBTC", 2024, 6)).endswith(r"month=06\data.parquet"))

    def test_lock_and_import_have_no_publication(self):
        root = Path(tempfile.mkdtemp(prefix="spot_derived_lock_"))
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        path = root / "task.lock"
        path.write_text(json.dumps({"pid": 2**31 - 1, "started": "2024-01-01T00:00:00Z", "task": "binance_spot_btc_pairs_derived"}), encoding="utf-8")
        lock = DerivedLock(path)
        lock.acquire()
        with self.assertRaises(RuntimeError):
            DerivedLock(path).acquire()
        lock.release()
        script = (
            "import sys\n"
            f"sys.path.insert(0, r'{_PROJECT_ROOT}')\n"
            "import processing.timeframes.build_spot_btc_derived\n"
            "mods=[name for name in sys.modules if name.startswith('detectors') or name.startswith('tools.detector') or 'workbook' in name]\n"
            "print(','.join(mods))\n"
        )
        completed = subprocess.run([str(PY), "-c", script], capture_output=True, text=True, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "")

    def test_hierarchy_requires_spot_root(self):
        root = Path(tempfile.mkdtemp(prefix="spot_derived_hierarchy_"))
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        spot = root / "spot"
        spot.mkdir()
        hierarchical_checks(spot, ())
        with self.assertRaises(RuntimeError):
            hierarchical_checks(root, ("5m", "15m"))


if __name__ == "__main__":
    unittest.main()
