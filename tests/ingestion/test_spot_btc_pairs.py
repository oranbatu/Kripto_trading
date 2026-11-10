"""Isolated tests for the Binance Spot BTC-pair 1-minute ingestion workflow."""
from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from ingestion.binance.download_binance_spot_btc_1m import _publish_verified
from ingestion.binance.spot_btc_pairs import (
    EXCLUDED_SYMBOLS,
    EXPECTED_ARCHIVES_PER_SYMBOL,
    EXPECTED_DAILY_ARCHIVES,
    EXPECTED_MONTHLY_ARCHIVES,
    EXPECTED_ROWS_OCTOBER_2026,
    SPOT_SYMBOLS,
    Archive,
    TaskLock,
    assert_complete_minutes,
    base_asset,
    expected_partition_minutes,
    iter_csv_rows,
    ms_of,
    normalize_timestamps,
    parse_checksum_bytes,
    parse_decimal,
    parse_kline_row,
    partition_months,
    plan_archives,
    plan_symbol,
    reject_symbol,
    require_spot_symbols,
    validate_zip_members,
)
from processing.canonical.build_spot_btc_1m_parquet import (
    _accumulate_quality,
    _publish_staged,
    rows_to_table,
    write_parquet_atomic,
)

PY = Path(r"C:\Users\oranb\Desktop\backtest_system\.venv\Scripts\python.exe")


def _raw(open_ms: int, unit: str = "milliseconds", high: str = "0.03844000") -> list[str]:
    close_ms = open_ms + 59_999
    if unit == "microseconds":
        open_raw, close_raw = open_ms * 1000, close_ms * 1000 + 999
    else:
        open_raw, close_raw = open_ms, close_ms
    return [
        str(open_raw), "0.03842000", high, "0.03841000", "0.03843000", "1.25000000",
        str(close_raw), "0.04802500", "4", "0.50000000", "0.01921000", "0",
    ]


def _zip(path: Path, member: str, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as handle:
        handle.writestr(member, "".join(",".join(row) + "\n" for row in rows))


class SpotScopeTests(unittest.TestCase):
    def test_exact_allowlist(self):
        self.assertEqual(require_spot_symbols(SPOT_SYMBOLS), SPOT_SYMBOLS)
        self.assertEqual(len(SPOT_SYMBOLS), 10)
        for symbol in SPOT_SYMBOLS:
            self.assertEqual(base_asset(symbol) + "BTC", symbol)

    def test_rejects_excluded_and_unknown_symbols(self):
        for symbol in ("BTCBTC", "AAVEBTC", "OPBTC", "FOOBTC", "BTCUSDT", "ETHBTC.P"):
            with self.assertRaises(ValueError):
                reject_symbol(symbol)
        self.assertEqual(EXCLUDED_SYMBOLS, ("BTCBTC", "AAVEBTC", "OPBTC"))

    def test_rejects_path_traversal_and_malformed_symbols(self):
        for symbol in ("../ETHBTC", "ETHBTC/../../x", "ETH BTC", "ethbtc", "ETHBTC\\SOLBTC", "", "ETHBTC "):
            with self.assertRaises(ValueError):
                reject_symbol(symbol)

    def test_archive_plan_is_33_monthly_plus_27_daily(self):
        planned = plan_archives()
        self.assertEqual(len(planned), 600)
        self.assertTrue(all(item.symbol in SPOT_SYMBOLS for item in planned))
        self.assertFalse(any(item.symbol in EXCLUDED_SYMBOLS for item in planned))
        for symbol in SPOT_SYMBOLS:
            own = [item for item in planned if item.symbol == symbol]
            self.assertEqual(len(own), EXPECTED_ARCHIVES_PER_SYMBOL)
            self.assertEqual(sum(item.kind == "monthly" for item in own), EXPECTED_MONTHLY_ARCHIVES)
            self.assertEqual(sum(item.kind == "daily" for item in own), EXPECTED_DAILY_ARCHIVES)
            self.assertFalse(any(item.kind == "monthly" and item.period == "2026-10" for item in own))
            self.assertEqual([item.period for item in own if item.kind == "daily"], [f"2026-10-{day:02d}" for day in range(1, 28)])

    def test_urls_are_spot_not_futures(self):
        archive = plan_symbol("ETHBTC")[0]
        self.assertEqual(
            archive.zip_url,
            "https://data.binance.vision/data/spot/monthly/klines/ETHBTC/1m/ETHBTC-1m-2024-01.zip",
        )
        self.assertNotIn("futures", archive.zip_url)
        self.assertTrue(archive.checksum_url.endswith(".zip.CHECKSUM"))
        daily = [item for item in plan_symbol("SOLBTC") if item.period == "2026-10-27"][0]
        self.assertIn("/data/spot/daily/klines/SOLBTC/1m/SOLBTC-1m-2026-10-27.zip", daily.zip_url)

    def test_calendar_rules(self):
        self.assertEqual(expected_partition_minutes(2024, 2), 29 * 1440)
        self.assertEqual(expected_partition_minutes(2026, 10), EXPECTED_ROWS_OCTOBER_2026)
        self.assertEqual(len(partition_months()), 34)
        self.assertEqual(sum(expected_partition_minutes(year, month) for year, month in partition_months()), 1_484_640)
        periods = [item.period for item in plan_symbol("ETHBTC")]
        self.assertIn("2024-02", periods)
        self.assertIn("2026-10-27", periods)
        self.assertNotIn("2026-10-28", periods)
        self.assertNotIn("2026-10", [item.period for item in plan_symbol("ETHBTC") if item.kind == "monthly"])


class SpotValidationTests(unittest.TestCase):
    def test_checksum_success_and_failure(self):
        body = b"abc123".ljust(64, b"0")
        # 64 hex chars plus the official two-space filename form
        digest = "a" * 64
        good = f"{digest}  ETHBTC-1m-2024-01.zip\n".encode()
        self.assertEqual(parse_checksum_bytes(good, "ETHBTC-1m-2024-01.zip"), digest)
        bad = f"{'b' * 64}  OTHER.zip\n".encode()
        with self.assertRaises(ValueError):
            parse_checksum_bytes(bad, "ETHBTC-1m-2024-01.zip")
        self.assertNotEqual(hashlib.sha256(body).hexdigest(), digest)

    def test_corrupt_zip_and_unexpected_member(self):
        root = Path(self.id().replace(".", "_"))
        del root
        with self.subTest("corrupt"):
            path = Path(self._temp()) / "bad.zip"
            path.write_bytes(b"not a zip")
            archive = plan_symbol("ETHBTC")[0]
            with self.assertRaises(zipfile.BadZipFile):
                validate_zip_members(path, archive)
        with self.subTest("unexpected"):
            path = Path(self._temp()) / "extra.zip"
            archive = plan_symbol("ETHBTC")[0]
            with zipfile.ZipFile(path, "w") as handle:
                handle.writestr(archive.csv_name, "x")
                handle.writestr("../escape.csv", "x")
            with self.assertRaises(ValueError):
                validate_zip_members(path, archive)

    def _temp(self) -> Path:
        path = Path(tempfile.mkdtemp(prefix="spot_btc_test_"))
        self.addCleanup(lambda: __import__("shutil").rmtree(path, ignore_errors=True))
        return path

    def test_twelve_columns_header_and_decimal(self):
        open_ms = ms_of(datetime(2024, 6, 1, tzinfo=timezone.utc))
        parsed = parse_kline_row(_raw(open_ms), "milliseconds")
        self.assertEqual(len(_raw(open_ms)), 12)
        self.assertEqual(parsed["open"], Decimal("0.03842000"))
        self.assertEqual(parsed["quote_volume"], Decimal("0.04802500"))
        with self.assertRaises(ValueError):
            parse_decimal("0.038420001", "open")
        text = io.StringIO("open_time,open,high,low,close,volume,close_time,quote_asset_volume,number_of_trades,taker_buy_base_asset_volume,taker_buy_quote_asset_volume,ignore\n"
                           + ",".join(_raw(open_ms)) + "\n")
        self.assertEqual(list(iter_csv_rows(text, "milliseconds"))[0]["open_ms"], open_ms)
        bad = io.StringIO("not,a,header,row,and,not,twelve,fields,only,plus,extra,x\n")
        with self.assertRaises(ValueError):
            list(iter_csv_rows(bad, "milliseconds"))

    def test_timestamp_units_and_year_boundary(self):
        last_2024 = ms_of(datetime(2024, 12, 31, 23, 59, tzinfo=timezone.utc))
        first_2025 = ms_of(datetime(2025, 1, 1, tzinfo=timezone.utc))
        open_ms, close_ms = normalize_timestamps(last_2024, last_2024 + 59_999, "milliseconds")
        next_open, _next_close = normalize_timestamps(first_2025 * 1000, (first_2025 + 59_999) * 1000 + 999, "microseconds")
        self.assertEqual(next_open - open_ms, 60_000)
        self.assertEqual(close_ms, open_ms + 59_999)
        october_27 = ms_of(datetime(2026, 10, 27, 23, 59, tzinfo=timezone.utc))
        october_28 = ms_of(datetime(2026, 10, 28, tzinfo=timezone.utc))
        self.assertEqual(parse_kline_row(_raw(october_27, "microseconds"), "microseconds")["open_ms"], october_27)
        with self.assertRaises(ValueError):
            parse_kline_row(_raw(october_28, "microseconds"), "microseconds")
        with self.assertRaises(ValueError):
            normalize_timestamps(last_2024 * 1000, (last_2024 + 59_999) * 1000 + 999, "milliseconds")
        with self.assertRaises(ValueError):
            normalize_timestamps(first_2025, first_2025 + 59_999, "microseconds")

    def test_missing_duplicate_and_ohlc(self):
        start = ms_of(datetime(2024, 1, 1, tzinfo=timezone.utc))
        opens = [start, start + 60_000, start + 120_000]
        assert_complete_minutes(opens, start, start + 120_000, "sample")
        with self.assertRaises(ValueError):
            assert_complete_minutes([start, start + 120_000], start, start + 120_000, "missing")
        with self.assertRaises(ValueError):
            assert_complete_minutes([start, start, start + 60_000], start, start + 60_000, "duplicate")
        with self.assertRaises(ValueError):
            parse_kline_row(_raw(start, high="0.00000010"), "milliseconds")

    def test_atomic_publish_skip_and_conflict(self):
        root = self._temp()
        open_ms = ms_of(datetime(2024, 1, 1, tzinfo=timezone.utc))
        table = rows_to_table("ETHBTC", [parse_kline_row(_raw(open_ms), "milliseconds")])
        final = root / "processed" / "binance" / "spot" / "1m" / "symbol=ETHBTC" / "year=2024" / "month=01" / "data.parquet"
        write_parquet_atomic(table, final, {"provider": "Binance", "market_type": "spot", "schema_version": "spot-1m-canonical-v1"})
        self.assertFalse(final.with_name("data.parquet.part").exists())
        defects = {key: 0 for key in (
            "nulls", "negative_prices", "ohlc_violations", "negative_base_volume",
            "negative_quote_volume", "negative_trade_counts",
        )}
        _accumulate_quality(final, defects)
        self.assertEqual(defects["ohlc_violations"], 0)
        self.assertEqual(defects["negative_prices"], 0)
        staging = root / "staged"
        staged = staging / "processed" / "binance" / "spot" / "1m" / "symbol=ETHBTC" / "year=2024" / "month=01" / "data.parquet"
        staged.parent.mkdir(parents=True)
        staged.write_bytes(final.read_bytes())
        before = final.stat().st_mtime_ns
        result = _publish_staged(staging, root, root / "quarantine")
        self.assertEqual(result["skipped_equivalent_partitions"], 1)
        self.assertEqual(final.stat().st_mtime_ns, before)
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_bytes(b"different-parquet-bytes")
        with self.assertRaises(RuntimeError):
            _publish_staged(staging, root, root / "quarantine")
        self.assertEqual(final.stat().st_mtime_ns, before)
        self.assertTrue(any((root / "quarantine").iterdir()))

    def test_raw_equivalent_skip_and_conflict(self):
        root = self._temp()
        final = root / "ETHBTC-1m-2024-01.zip"
        final.write_bytes(b"official-zip")
        before = final.stat().st_mtime_ns
        same = root / "same.zip"
        same.write_bytes(b"official-zip")
        self.assertEqual(_publish_verified(same, final, root / "quarantine"), "skipped-equivalent")
        self.assertEqual(final.stat().st_mtime_ns, before)
        other = root / "other.zip"
        other.write_bytes(b"different-zip")
        with self.assertRaises(RuntimeError):
            _publish_verified(other, final, root / "quarantine")
        self.assertEqual(final.read_bytes(), b"official-zip")
        self.assertEqual(final.stat().st_mtime_ns, before)

    def test_lock_rejects_live_process_and_replaces_stale_lock(self):
        root = self._temp()
        lock_path = root / "task.lock"
        lock_path.write_text(json.dumps({"pid": 2**31 - 1, "started": "2024-01-01T00:00:00Z", "task": "binance_spot_btc_pairs_1m"}), encoding="utf-8")
        lock = TaskLock(lock_path)
        lock.acquire()
        self.assertEqual(json.loads(lock_path.read_text(encoding="utf-8"))["pid"], os.getpid())
        second = TaskLock(lock_path)
        with self.assertRaises(RuntimeError):
            second.acquire()
        lock.release()
        self.assertFalse(lock_path.exists())

    def test_import_has_no_side_effects(self):
        script = (
            "import sys\n"
            f"sys.path.insert(0, r'{_PROJECT_ROOT}')\n"
            "import ingestion.binance.spot_btc_pairs\n"
            "import ingestion.binance.download_binance_spot_btc_1m\n"
            "import processing.canonical.build_spot_btc_1m_parquet\n"
            "mods=[name for name in sys.modules if name.startswith('detectors') or name.startswith('tools.detector') or 'workbook' in name]\n"
            "print(','.join(mods))\n"
        )
        completed = subprocess.run([str(PY), "-c", script], capture_output=True, text=True, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "")
        refused = subprocess.run(
            [str(PY), "-m", "ingestion.binance.download_binance_spot_btc_1m", "ETHBTC"],
            cwd=_PROJECT_ROOT, capture_output=True, text=True, check=False,
        )
        self.assertNotEqual(refused.returncode, 0)


if __name__ == "__main__":
    unittest.main()
