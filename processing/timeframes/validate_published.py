"""Read-only validation of the published USD-M candle datasets.

This entry point never calls a publication routine. It reads existing Parquet,
writes a diagnostic JSON under a temporary directory, then deletes that directory.
Production reports, logs, and Parquet files are not updated.
"""
from __future__ import annotations

import importlib
import json
import sys
import tempfile
from pathlib import Path

from processing.timeframes.sibling_scope import defects_clear, validation_passed

DATA_ROOT = Path(r"C:\MarketData")
DERIVED_PARENT = DATA_ROOT / "derived" / "binance" / "futures" / "um" / "perpetual"
PROCESSED_ROOT = DATA_ROOT / "processed" / "binance" / "futures" / "um" / "perpetual" / "1m"

_MODULES = {
    "1m": "processing.canonical.build_canonical_parquet",
    "5m": "processing.timeframes.build_5m_parquet",
    "15m": "processing.timeframes.build_15m_parquet",
    "30m": "processing.timeframes.build_30m_parquet",
    "1h": "processing.timeframes.build_1h_parquet",
    "4h": "processing.timeframes.build_4h_parquet",
    "1d": "processing.timeframes.build_1d_parquet",
}


def _production_stat() -> dict[str, tuple[int, int]]:
    """Size and mtime only. Used as a per-run tripwire; the final SHA-256 compare is separate."""
    found: dict[str, tuple[int, int]] = {}
    for root in (PROCESSED_ROOT, DERIVED_PARENT):
        if not root.exists():
            continue
        for path in root.rglob("data.parquet"):
            if "quarantine" in path.parts or not path.is_file():
                continue
            stat = path.stat()
            found[str(path)] = (stat.st_size, stat.st_mtime_ns)
    return found


def validate_timeframe(timeframe: str) -> int:
    if timeframe not in _MODULES:
        raise SystemExit(f"unknown timeframe {timeframe}")
    module = importlib.import_module(_MODULES[timeframe])
    before = _production_stat()
    scan = module.validate_published()
    after = _production_stat()
    # 1m files live outside the derived parent. Any production size or mtime
    # change, including canonical 1m, fails a validation-only run.
    full_unchanged = before == after
    changes = {"sibling_partitions_unchanged": full_unchanged, "own_partitions_unchanged": full_unchanged}
    defects = scan.get("defects") or {}
    passed = validation_passed(
        no_other_timeframes=bool(scan.get("no_other_timeframes")),
        sibling_partitions_unchanged=full_unchanged or changes["sibling_partitions_unchanged"],
        own_partitions_unchanged=full_unchanged,
        scan_counts_ok=bool(scan.get("all_counts_expected")),
        defects_ok=defects_clear(defects),
    )
    # full_unchanged already implies sibling files did not change. Keep the
    # sibling flag false when an approved sibling file changed.
    if not changes["sibling_partitions_unchanged"]:
        passed = False
    summary = {
        "mode": "validate-only",
        "timeframe": timeframe,
        "exit_code": 0 if passed else 1,
        "dataset_ok": bool(scan.get("all_counts_expected")) and defects_clear(defects) and bool(scan.get("no_other_timeframes")),
        "scan_ok": bool(scan.get("all_counts_expected")) and bool(scan.get("no_other_timeframes")),
        "no_other_timeframes": bool(scan.get("no_other_timeframes")),
        "approved_timeframes_present": scan.get("approved_timeframes_present"),
        "unauthorized_timeframes_present": scan.get("unauthorized_timeframes_present"),
        "other_timeframes_present": scan.get("other_timeframes_present"),
        "parquet_files": scan.get("parquet_files"),
        "total_rows": scan.get("total_rows"),
        "rows_by_symbol": scan.get("rows_by_symbol"),
        "first_open_time_utc": scan.get("first_open_time_utc"),
        "last_open_time_utc": scan.get("last_open_time_utc"),
        "last_close_time_utc": scan.get("last_close_time_utc"),
        "defects": defects,
        "production_parquet_unchanged": full_unchanged,
        "sibling_partitions_unchanged": changes["sibling_partitions_unchanged"] and full_unchanged,
        "own_partitions_unchanged": full_unchanged,
    }
    for key, value in scan.items():
        if key not in summary and (
            "fail" in key or "cross" in key or key.startswith("secondary_") or key.endswith("_passed")
        ):
            summary[key] = value
    with tempfile.TemporaryDirectory(prefix=f"validate_{timeframe}_") as temporary:
        report_path = Path(temporary) / f"{timeframe}_validation.json"
        report_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        # The directory and report are removed when this block exits.
        summary["temporary_report_written"] = True
    summary["temporary_directory_removed"] = True
    sys.stdout.write(json.dumps(summary, indent=2) + "\n")
    return summary["exit_code"]


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    timeframe = "all"
    if "--timeframe" in args:
        timeframe = args[args.index("--timeframe") + 1]
    elif args and not args[0].startswith("-"):
        timeframe = args[0]
    names = list(_MODULES) if timeframe == "all" else [timeframe]
    code = 0
    for name in names:
        code = max(code, validate_timeframe(name))
    return code


if __name__ == "__main__":
    sys.exit(main())
