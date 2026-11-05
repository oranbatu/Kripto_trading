"""Isolated tests for adding four canonical 1-minute symbols. No production download."""
from __future__ import annotations

import hashlib
import http.client
import io
import json
import tempfile
import time
import unittest
import zipfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import config
from data.catalog import expected_partition_keys
from ingestion.binance import download_binance_um_perp_1m as downloader
from processing.canonical import build_canonical_parquet as builder

NEW = ("XRPUSDT", "DOGEUSDT", "ADAUSDT", "SOLUSDT")
ORIGINAL = (
    "BTCUSDT", "ETHUSDT", "AVAXUSDT", "AAVEUSDT", "NEARUSDT",
    "LINKUSDT", "LTCUSDT", "OPUSDT", "DOTUSDT",
)
START = datetime(2024, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 10, 27, 23, 59, tzinfo=timezone.utc)


def _csv_row(open_ms: int, price: str = "1.25000000") -> str:
    close_ms = open_ms + 59_999
    return ",".join([
        str(open_ms), price, price, price, price, "10.5", str(close_ms),
        "13.125", "3", "4.5", "5.90625", "0",
    ])


def _zip_bytes(name: str, rows: list[str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, "\n".join(rows) + "\n")
    return buffer.getvalue()


class AllowlistTests(unittest.TestCase):
    def test_symbol_sets_and_rejections(self) -> None:
        self.assertEqual(config.NEW_CANONICAL_1M_SYMBOLS, NEW)
        self.assertEqual(config.CANONICAL_1M_SYMBOLS, ORIGINAL + NEW)
        self.assertEqual(config.ALLOWED_SYMBOLS, ORIGINAL)
        self.assertEqual(len(set(config.CANONICAL_1M_SYMBOLS)), 13)
        self.assertEqual(config.symbols_for_timeframe("1m"), config.CANONICAL_1M_SYMBOLS)
        self.assertEqual(config.symbols_for_timeframe("30m"), config.CANONICAL_1M_SYMBOLS)
        self.assertIn("XRPUSDT", {key[0] for key in expected_partition_keys("5m")})
        self.assertEqual(len(expected_partition_keys("1m")), 442)
        self.assertEqual(len(expected_partition_keys("30m")), 442)
        self.assertEqual(len(expected_partition_keys("5m")), 442)
        for bad in ("DOGE", r"..\XRPUSDT", "XRPUSDT/../BTCUSDT", "xrpusdt", "XRP USDT"):
            with self.assertRaises(ValueError):
                config.require_canonical_symbols((bad,))

    def test_archive_plan_counts_and_boundaries(self) -> None:
        total_monthly = total_daily = 0
        for symbol in NEW:
            planned = downloader.plan_symbol(symbol, START, END)
            monthly = [item for item in planned if item.kind == "monthly"]
            daily = [item for item in planned if item.kind == "daily"]
            self.assertEqual(len(monthly), 33, symbol)
            self.assertEqual(len(daily), 27, symbol)
            self.assertEqual(len(planned), 60, symbol)
            self.assertFalse(any(item.period == "2026-10" for item in monthly), symbol)
            self.assertFalse(any("2026-10-28" in item.period for item in planned), symbol)
            self.assertEqual(daily[0].period, "2026-10-01")
            self.assertEqual(daily[-1].period, "2026-10-27")
            self.assertTrue(all(item.zip_url.startswith("https://data.binance.vision/data/futures/um/") for item in planned))
            self.assertNotIn("/spot/", planned[0].zip_url)
            total_monthly += len(monthly)
            total_daily += len(daily)
        self.assertEqual(total_monthly, 132)
        self.assertEqual(total_daily, 108)
        self.assertEqual(total_monthly + total_daily, 240)
        feb_start, feb_next = downloader.month_bounds(2024, 2)
        self.assertEqual(int((feb_next - feb_start).total_seconds() // 60), 29 * 1440)
        self.assertEqual(366 * 1440, 527_040)
        self.assertEqual(365 * 1440, 525_600)
        self.assertEqual(300 * 1440, 432_000)
        self.assertEqual(527_040 + 525_600 + 432_000, 1_484_640)
        self.assertEqual(1_484_640 * 4, 5_938_560)
        self.assertEqual(27 * 1440, 38_880)


class DownloadSafetyTests(unittest.TestCase):
    def _job(self, root: Path, http) -> downloader.Job:
        return downloader.Job(root, "test-run", downloader.ms_of(START), downloader.ms_of(END), http, {})

    def test_checksum_corrupt_404_part_cleanup_skip_and_retry(self) -> None:
        symbol = "XRPUSDT"
        day = downloader.plan_symbol(symbol, START, END)
        daily = next(item for item in day if item.period == "2026-10-01")
        open_ms = daily.start_ms
        payload = _zip_bytes(daily.csv_name, [_csv_row(open_ms)])
        digest = hashlib.sha256(payload).hexdigest()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            http = mock.Mock()
            http.get_bytes.return_value = (200, f"{digest}  {daily.zip_name}\n".encode())
            def corrupt(url, dest):
                dest.write_bytes(b"corrupt")
                return 200, 7

            http.download.side_effect = corrupt
            archive, record = downloader.process_archive(self._job(root, http), daily)
            self.assertEqual(record["status"], "quarantined")
            self.assertFalse(daily.zip_path(root).exists())
            self.assertFalse(list((root / "raw").rglob("*.part")))

            def good(url, dest):
                dest.write_bytes(payload)
                return 200, len(payload)

            http.download.side_effect = good
            _archive, verified = downloader.process_archive(self._job(root, http), daily)
            self.assertEqual(verified["status"], "verified")
            before = daily.zip_path(root).stat().st_mtime_ns
            _archive, skipped = downloader.process_archive(self._job(root, http), daily)
            self.assertEqual(skipped["status"], "skipped-as-already-verified")
            self.assertEqual(daily.zip_path(root).stat().st_mtime_ns, before)

            missing = downloader.Archive(symbol, "daily", "2026-10-02", daily.start_ms, daily.end_ms)
            http.get_bytes.return_value = (404, b"")
            http.head_status.return_value = 404
            _archive, unavailable = downloader.process_archive(self._job(root, http), missing)
            self.assertEqual(unavailable["status"], "unavailable")
            self.assertFalse(missing.zip_path(root).exists())

        attempts = {"n": 0}

        def flaky():
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise downloader.TransientHttpError("temporary")
            return (200, b"ok")

        with mock.patch.object(downloader.time, "sleep"):
            self.assertEqual(downloader.Http()._with_retries(flaky, "unit"), (200, b"ok"))
        self.assertEqual(attempts["n"], 2)


class CanonicalPublicationTests(unittest.TestCase):
    def test_schema_precision_and_atomic_skip(self) -> None:
        self.assertEqual(builder.SCHEMA_VERSION, "um-perp-1m-canonical-v1")
        self.assertEqual(builder.PARQUET_COMPRESSION, "zstd")
        self.assertEqual(builder.DECIMAL_TYPE.precision, 38)
        self.assertEqual(builder.DECIMAL_TYPE.scale, 8)
        symbol = "SOLUSDT"
        start_ms = downloader.ms_of(datetime(2024, 1, 1, tzinfo=timezone.utc))
        rows = [
            builder.ParsedRow(
                start_ms, start_ms + 59_999, Decimal("1.23000000"), Decimal("1.25000000"),
                Decimal("1.20000000"), Decimal("1.24000000"), Decimal("10.50000000"),
                Decimal("12.91500000"), 4, Decimal("3.00000000"), Decimal("3.69000000"), 0,
            )
        ]
        partition = builder.Partition(symbol, 2024, 1, start_ms, start_ms, ())
        table, stats = builder.rows_to_table(partition, rows)
        self.assertEqual(table.schema.field("open").type, builder.DECIMAL_TYPE)
        self.assertEqual(table.column("open")[0].as_py(), Decimal("1.23000000"))
        self.assertEqual(stats.ohlc_violation_count, 0)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            final_root = root / "processed"
            with mock.patch.object(builder, "PROCESSED_ROOT", final_root), mock.patch.object(
                builder, "QUARANTINE_ROOT", root / "quarantine"
            ):
                archive_rows = list(iter([rows[0]]))
                part = builder.Partition(symbol, 2024, 1, start_ms, start_ms, ())
                with mock.patch.object(builder, "iter_partition_rows", return_value=archive_rows):
                    first = builder.process_partition(part, "unit", root / "tmp")
                    stamp = part.hive_path.stat()
                    time.sleep(0.01)
                    second = builder.process_partition(part, "unit-again", root / "tmp")
                self.assertEqual(first["status"], "verified-equivalent")
                self.assertEqual(second["status"], "skipped-as-already-equivalent")
                self.assertEqual(part.hive_path.stat().st_mtime_ns, stamp.st_mtime_ns)
                self.assertEqual(part.hive_path.stat().st_size, stamp.st_size)

    def test_four_symbol_plan_does_not_include_original_symbols(self) -> None:
        planned = []
        for symbol in NEW:
            planned.extend(downloader.plan_symbol(symbol, START, END))
        self.assertTrue(all(item.symbol in NEW for item in planned))
        self.assertFalse(any(item.symbol in ORIGINAL for item in planned))


def _good_inventory() -> dict:
    year_rows = {"2024": 527_040, "2025": 525_600, "2026": 432_000}
    per_symbol = {
        symbol: {
            "archives_consumed": 60,
            "monthly_archives": 33,
            "daily_archives": 27,
            "canonical_partitions": 34,
            "rows": 1_484_640,
            "year_rows": dict(year_rows),
            "october_2026_rows": 38_880,
        }
        for symbol in NEW
    }
    return {
        "source_archive_plan": {
            "monthly_archives": 132,
            "daily_archives": 108,
            "archives_consumed": 240,
            "october_monthly_archives": 0,
        },
        "per_symbol": per_symbol,
        "raw": {
            "zip_files": 915,
            "checksum_files": 915,
            "monthly_zip_files": 429,
            "daily_zip_files": 486,
            "consumed_production_plan": 780,
            "retained_historical_archives": 135,
        },
        "inventory_verified_archives": 240,
        "inventory_missing_archives": 0,
        "inventory_checksum_failures": 0,
        "canonical_1m": {
            "symbols": 13,
            "partitions": 442,
            "rows": 19_300_320,
            "symbol_names": list(ORIGINAL + NEW),
        },
        "fully_derived_symbol_count": 9,
        "fully_derived_symbols": list(ORIGINAL),
        "new_symbols_with_derived_timeframes": [],
    }


def _semantic(report: dict) -> dict:
    copy = json.loads(json.dumps(report))
    for key in ("run_id", "generated_at_utc", "run_started_utc", "duration_seconds"):
        copy.pop(key, None)
    return copy


def _stale_previous(symbols: tuple[str, ...]) -> dict:
    stale = downloader.STALE_COVERAGE_BOUNDARY
    return {
        "run_id": "20261029T195752Z",
        "requested_symbols": list(symbols),
        "required_range_utc": {"last_open_time": stale},
        "incremental_extension": {
            "applicable": True,
            "previous_last_open_time_utc": stale,
            "new_last_open_time_utc": stale,
            "new_periods_utc": ["2026-10-28"],
        },
        "previous_runs": [{"required_range_utc": {"last_open_time": stale}}],
    }


class ReportSemanticsTests(unittest.TestCase):
    def _report(self, mode: str, symbols: tuple[str, ...], previous: dict | None, planned=None):
        with tempfile.TemporaryDirectory() as folder:
            job = downloader.Job(Path(folder), "unit-run", downloader.ms_of(START), downloader.ms_of(END), None, {})
            validation = {symbol: {"validated": False, "reason": "unit"} for symbol in symbols}
            return downloader.build_report(
                job, datetime.now(timezone.utc), symbols, validation, planned or [], [], 0, False, previous, mode
            )

    def test_new_report_does_not_reuse_previous_dictionary(self) -> None:
        previous = _stale_previous(NEW)
        report = self._report(downloader.MODE_NEW_SYMBOLS, NEW, previous)
        previous["incremental_extension"]["injected"] = True
        previous["previous_runs"].append({"required_range_utc": {"last_open_time": downloader.STALE_COVERAGE_BOUNDARY}})
        self.assertNotIn("injected", json.dumps(report))
        self.assertEqual(report["previous_runs"], [])
        self.assertIsNot(report["incremental_extension"], previous["incremental_extension"])

    def test_new_symbols_mode_marks_extension_non_applicable_without_stale_timestamps(self) -> None:
        report = self._report(downloader.MODE_NEW_SYMBOLS, NEW, _stale_previous(NEW))
        extension = report["incremental_extension"]
        self.assertEqual(report["report_mode"], "NEW_SYMBOLS_ONLY")
        self.assertFalse(extension["applicable"])
        self.assertEqual(extension["reason"], "NEW_SYMBOLS_ONLY")
        self.assertEqual(extension["new_periods"], [])
        self.assertNotIn("previous_last_open_time_utc", extension)
        self.assertNotIn("new_last_open_time_utc", extension)
        self.assertNotIn(downloader.STALE_COVERAGE_BOUNDARY, json.dumps(report))
        self.assertEqual(report["symbol_scope"], list(NEW))
        self.assertEqual(report["coverage_last_open_utc"], "2026-10-27T23:59:00Z")
        self.assertEqual(report["coverage_last_close_utc"], "2026-10-27T23:59:59.999Z")
        self.assertEqual(report["coverage_end_exclusive_utc"], "2026-10-28T00:00:00Z")

    def test_true_incremental_uses_current_plan_and_zero_period_drops_unrelated_end(self) -> None:
        earlier = {
            "run_id": "earlier",
            "requested_symbols": list(ORIGINAL),
            "required_range_utc": {"last_open_time": "2026-09-30T23:59:00Z"},
        }
        planned = downloader.plan_symbol("BTCUSDT", START, END)
        extension, _runs = downloader.build_incremental_extension(
            earlier, planned, [], downloader.ms_of(END), ORIGINAL
        )
        self.assertTrue(extension["applicable"])
        self.assertEqual(extension["coverage_last_open_utc"], "2026-10-27T23:59:00Z")
        self.assertEqual(extension["new_periods"][0], "2026-10-01")
        self.assertEqual(extension["new_periods"][-1], "2026-10-27")
        self.assertEqual(len(extension["new_periods"]), 27)
        self.assertNotIn("2026-10", extension["new_periods"])
        stale = self._report(downloader.MODE_INCREMENTAL, ORIGINAL, _stale_previous(ORIGINAL), planned)
        self.assertFalse(stale["incremental_extension"]["applicable"])
        self.assertEqual(stale["incremental_extension"]["reason"], "PREVIOUS_RANGE_IS_NOT_A_STRICT_PREFIX")
        self.assertEqual(stale["incremental_extension"]["new_periods"], [])
        self.assertNotIn(downloader.STALE_COVERAGE_BOUNDARY, json.dumps(stale))
        btc = downloader.plan_symbol("BTCUSDT", START, END)
        self.assertEqual(sum(item.kind == "monthly" for item in btc), 33)
        self.assertEqual(sum(item.kind == "daily" for item in btc), 27)

    def test_read_only_counters_inventory_and_idempotent_semantics(self) -> None:
        inventory = _good_inventory()
        historical = downloader.historical_runs_from_previous({
            "run_id": "20261101T165204Z",
            "run_summary": {"verified": 240},
            "incremental_extension": {"previous_last_open_time_utc": downloader.STALE_COVERAGE_BOUNDARY},
            "previous_runs": [{"required_range_utc": {"last_open_time": downloader.STALE_COVERAGE_BOUNDARY}}],
        })
        started = datetime(2026, 11, 1, tzinfo=timezone.utc)
        first = downloader.assemble_read_only_report(inventory, historical, "run-a", "2026-11-01T17:00:00Z", started)
        inventory["raw"]["zip_files"] = 1
        inventory["per_symbol"]["XRPUSDT"]["rows"] = 1
        second = downloader.assemble_read_only_report(_good_inventory(), first["historical_runs"], "run-b", "2026-11-01T18:00:00Z", started)
        self.assertEqual(first["inventory"]["raw"]["zip_files"], 915)
        self.assertEqual(first["report_mode"], "READ_ONLY_VALIDATION")
        self.assertFalse(first["incremental_extension"]["applicable"])
        self.assertEqual(first["incremental_extension"]["reason"], "READ_ONLY_VALIDATION")
        self.assertEqual(first["current_run_downloaded"], 0)
        self.assertEqual(first["current_run_skipped"], 0)
        self.assertEqual(first["current_run_failed"], 0)
        self.assertEqual(first["inventory_verified_archives"], 240)
        self.assertEqual(first["inventory_missing_archives"], 0)
        self.assertEqual(first["inventory_checksum_failures"], 0)
        self.assertEqual(first["source_archive_plan"]["archives_consumed"], 240)
        self.assertEqual(first["canonical_1m_symbols"], 13)
        self.assertEqual(first["canonical_1m_partitions"], 442)
        self.assertEqual(first["canonical_1m_rows"], 19_300_320)
        self.assertEqual(first["fully_derived_symbol_count"], 9)
        self.assertEqual(sum(slot["rows"] for slot in first["inventory"]["per_symbol"].values()), 5_938_560)
        self.assertEqual(first["historical_runs"][0]["run_id"], "20261101T165204Z")
        self.assertEqual(first["historical_runs"][0]["verified"], 240)
        self.assertEqual(first["previous_runs"], [])
        self.assertNotIn(downloader.STALE_COVERAGE_BOUNDARY, json.dumps(first))
        self.assertEqual(first["validation_status"], "passed")
        self.assertEqual(_semantic(first), _semantic(second))
        self.assertEqual(downloader.production_inventory_errors(_good_inventory()), [])

    def test_atomic_publication_rejects_malformed_temporary_report(self) -> None:
        started = datetime(2026, 11, 1, tzinfo=timezone.utc)
        report = downloader.assemble_read_only_report(
            _good_inventory(), [], "run-a", "2026-11-01T17:00:00Z", started
        )
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "download_report.json"
            path.write_text('{"keep": true}\n', encoding="utf-8")
            downloader.write_report(path, report)
            self.assertFalse(path.with_name(path.name + ".part").exists())
            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(saved["report_mode"], "READ_ONLY_VALIDATION")
            broken = json.loads(json.dumps(report))
            broken["coverage_last_open_utc"] = downloader.STALE_COVERAGE_BOUNDARY
            with self.assertRaises(ValueError):
                downloader.write_report(path, broken)
            self.assertFalse(path.with_name(path.name + ".part").exists())
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["coverage_last_open_utc"], "2026-10-27T23:59:00Z")

    def test_report_only_makes_no_network_call_and_writes_no_market_data(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "raw").mkdir()
            (root / "processed").mkdir()
            before = {path.relative_to(root).as_posix() for path in root.rglob("*")}

            def refuse_network(*_args, **_kwargs):
                raise AssertionError("network")

            with mock.patch.object(http.client, "HTTPSConnection", refuse_network), mock.patch.object(
                downloader, "validate_symbols", refuse_network
            ), mock.patch.object(downloader, "Http", refuse_network):
                code = downloader.report_only(root, NEW)
            self.assertEqual(code, 1)
            created = [path for path in root.rglob("*") if path.suffix in {".zip", ".parquet"} or path.name.endswith(".CHECKSUM")]
            self.assertEqual(created, [])
            self.assertFalse((root / "reports" / "download_report.json").exists())
            self.assertEqual(before, {path.relative_to(root).as_posix() for path in root.rglob("*")})


if __name__ == "__main__":
    unittest.main()
