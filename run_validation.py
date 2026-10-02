#!/usr/bin/env python3
"""Run tests twice, smoke tests, immutability check, and write the validation report."""
from __future__ import annotations

import json
import logging
import sys
import time
import traceback
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

import config
from data.catalog import DataCatalog, fingerprint_file
from data.loader import load_candles, load_multi_timeframe
from data.timeutil import iso_z, ms_to_datetime
from tests.helpers import fingerprint_tree, list_source_files

UTC = timezone.utc
REPORT_PATH = PROJECT_ROOT / "reports" / "data_access_validation_report.json"
LOG_PATH = PROJECT_ROOT / "logs" / "data_access.log"


def _run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _configure_log() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("data_access")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fmt.converter = time.gmtime
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    return log


def _run_unittest() -> unittest.TestResult:
    loader = unittest.TestLoader()
    suite = loader.discover(
        start_dir=str(PROJECT_ROOT / "tests"),
        pattern="test_*.py",
        top_level_dir=str(PROJECT_ROOT),
    )
    runner = unittest.TextTestRunner(verbosity=2)
    return runner.run(suite)


def _mapped(data, tf: str, open_utc: datetime) -> dict:
    base = data.slices[data.base_timeframe]
    row = base.index_of_open(open_utc)
    idx = data.available_index(tf, row)
    if idx < 0:
        return {"base_row": row, "index": -1, "open_time_utc": None}
    return {
        "base_row": row,
        "index": idx,
        "open_time_utc": iso_z(ms_to_datetime(data.slices[tf].open_times_ms()[idx])),
    }


def smoke_tests(log: logging.Logger) -> dict:
    out: dict = {}
    sl = load_candles(
        "BTCUSDT", "5m",
        datetime(2026, 8, 10, tzinfo=UTC),
        datetime(2026, 8, 11, tzinfo=UTC),
    )
    out["single_timeframe"] = {
        "symbol": sl.symbol,
        "timeframe": sl.timeframe,
        "rows": sl.row_count,
        "expected_rows": 288,
        "ok": sl.row_count == 288,
        "first_open": iso_z(sl.loaded_start_utc),
        "last_open": iso_z(sl.loaded_end_utc),
        "precision_mode": sl.precision_mode,
        "source_files": list(sl.source_files),
    }
    log.info("smoke 5m BTCUSDT 2026-08-10 rows=%s", sl.row_count)

    wu = load_candles(
        "ETHUSDT", "1h",
        datetime(2025, 1, 1, tzinfo=UTC),
        datetime(2025, 1, 8, tzinfo=UTC),
        warmup_bars=100,
    )
    out["warmup"] = {
        "symbol": wu.symbol,
        "timeframe": wu.timeframe,
        "warmup_bars_loaded": wu.warmup_bars_loaded,
        "trade_rows": wu.trade_row_count,
        "total_rows": wu.row_count,
        "trade_start_index": wu.trade_start_index,
        "expected": {"warmup": 100, "trade": 168, "total": 268, "trade_start_index": 100},
        "ok": wu.warmup_bars_loaded == 100 and wu.trade_row_count == 168 and wu.row_count == 268,
    }
    log.info("smoke warmup ETHUSDT 1h total=%s warmup=%s trade=%s", wu.row_count, wu.warmup_bars_loaded, wu.trade_row_count)

    multi = load_multi_timeframe(
        "BTCUSDT",
        ["5m", "15m", "30m", "1h", "4h", "1d"],
        "5m",
        datetime(2026, 8, 10, tzinfo=UTC),
        datetime(2026, 8, 11, tzinfo=UTC),
        decision_clock="close",
    )
    probes = {
        "10:30": datetime(2026, 8, 10, 10, 30, tzinfo=UTC),
        "10:55": datetime(2026, 8, 10, 10, 55, tzinfo=UTC),
        "11:55": datetime(2026, 8, 10, 11, 55, tzinfo=UTC),
        "23:55": datetime(2026, 8, 10, 23, 55, tzinfo=UTC),
    }
    availability = {}
    for label, ts in probes.items():
        availability[label] = {
            "base_open_utc": iso_z(ts),
            "base_close_utc": iso_z(ms_to_datetime(multi.slices["5m"].close_times_ms()[multi.slices["5m"].index_of_open(ts)])),
            "15m": _mapped(multi, "15m", ts),
            "30m": _mapped(multi, "30m", ts),
            "1h": _mapped(multi, "1h", ts),
            "4h": _mapped(multi, "4h", ts),
            "1d": _mapped(multi, "1d", ts),
        }
        log.info("smoke availability %s %s", label, availability[label])
    out["multi_timeframe"] = {
        "symbol": multi.symbol,
        "base_timeframe": multi.base_timeframe,
        "decision_clock": multi.decision_clock,
        "base_rows": multi.slices["5m"].row_count,
        "availability": availability,
        "ok": multi.slices["5m"].row_count == 288,
    }

    exact = load_candles("BTCUSDT", "5m", datetime(2026, 8, 10, tzinfo=UTC), datetime(2026, 8, 11, tzinfo=UTC), precision_mode="exact")
    analysis = load_candles("BTCUSDT", "5m", datetime(2026, 8, 10, tzinfo=UTC), datetime(2026, 8, 11, tzinfo=UTC), precision_mode="analysis")
    out["precision"] = {
        "exact_close_type": str(exact.table.schema.field("close").type),
        "analysis_close_type": str(analysis.table.schema.field("close").type),
        "ok": pa.types.is_decimal(exact.table.schema.field("close").type)
        and pa.types.is_floating(analysis.table.schema.field("close").type),
    }
    return out


def _result_summary(result: unittest.TestResult) -> dict:
    return {
        "tests_run": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(result.skipped),
        "ok": result.wasSuccessful(),
        "failure_details": [f"{test.id()}: {err}" for test, err in result.failures + result.errors],
    }


def main() -> int:
    run_id = _run_id()
    log = _configure_log()
    log.info("run %s started", run_id)
    warnings: list[str] = []
    errors: list[str] = []
    started = time.time()

    source_files = list_source_files()
    before = fingerprint_tree(source_files)
    log.info("fingerprinted %s source files", len(before))

    catalog = DataCatalog()
    inventories = {}
    for tf in config.ALLOWED_TIMEFRAMES:
        info = catalog.get_dataset_info(tf, inspect_metadata=False)
        inventories[tf] = {
            "root": str(info.root),
            "actual_files": info.actual_files,
            "expected_files": info.expected_files,
            "symbols": list(info.symbols),
            "missing": list(info.missing_partitions),
            "extra": list(info.extra_files),
            "ok": info.actual_files == info.expected_files and not info.missing_partitions,
        }

    log.info("unittest pass 1")
    result1 = _run_unittest()
    log.info("unittest pass 2")
    result2 = _run_unittest()

    smoke = {}
    try:
        smoke = smoke_tests(log)
    except Exception as exc:
        errors.append(f"smoke tests failed: {exc}")
        log.exception("smoke tests failed")
        traceback.print_exc()

    after = fingerprint_tree(source_files)
    mutated = [path for path, meta in before.items() if after.get(path) != meta]
    missing = [path for path in before if path not in after]
    added = [path for path in after if path not in before]
    immutability_ok = not mutated and not missing and not added
    if not immutability_ok:
        errors.append(f"source mutation mutated={len(mutated)} missing={len(missing)} added={len(added)}")

    extra_in_source = []
    for tf in config.ALLOWED_TIMEFRAMES:
        root = config.dataset_root(tf)
        extra_in_source.extend(str(p) for p in root.rglob("*.part"))
        extra_in_source.extend(str(p) for p in root.rglob("*.tmp"))

    report = {
        "report_version": 1,
        "run_id": run_id,
        "python": sys.version.split()[0],
        "pyarrow": pa.__version__,
        "data_root": str(config.data_root()),
        "supported_symbols": list(config.ALLOWED_SYMBOLS),
        "supported_timeframes": list(config.ALLOWED_TIMEFRAMES),
        "half_open_convention": "[start_utc, end_utc)",
        "timezone": "UTC",
        "turkey_time": "display-only; never used for filtering or alignment",
        "dataset_inventories": inventories,
        "test_pass_1": _result_summary(result1),
        "test_pass_2": _result_summary(result2),
        "determinism_tests": {
            "second_unittest_pass_ok": result2.wasSuccessful(),
            "same_test_count": result1.testsRun == result2.testsRun,
            "ok": result1.wasSuccessful() and result2.wasSuccessful() and result1.testsRun == result2.testsRun,
        },
        "smoke_tests": smoke,
        "precision_mode_results": smoke.get("precision"),
        "warmup_results": smoke.get("warmup"),
        "no_lookahead_results": smoke.get("multi_timeframe"),
        "source_immutability": {
            "files_fingerprinted": len(before),
            "unchanged": immutability_ok,
            "mutated": mutated[:20],
            "missing": missing[:20],
            "added": added[:20],
            "leftover_part_or_tmp": extra_in_source,
        },
        "warnings": warnings,
        "errors": errors,
        "duration_seconds": round(time.time() - started, 1),
        "ok": (
            result1.wasSuccessful()
            and result2.wasSuccessful()
            and immutability_ok
            and not errors
            and (smoke.get("single_timeframe") or {}).get("ok")
            and (smoke.get("warmup") or {}).get("ok")
            and (smoke.get("multi_timeframe") or {}).get("ok")
            and (smoke.get("precision") or {}).get("ok")
        ),
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = REPORT_PATH.with_suffix(".json.part")
    tmp.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    tmp.replace(REPORT_PATH)
    log.info("report %s ok=%s", REPORT_PATH, report["ok"])
    if not report["ok"]:
        log.error("validation failed")
        return 1
    log.info("run %s finished with exit code 0", run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
