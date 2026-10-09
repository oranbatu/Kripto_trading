"""Run the separate BTCUSDT 30-minute special swing detector."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import hashlib
import json
import logging
import os
import re
import traceback
from datetime import datetime, timezone
from decimal import Decimal

import config
from data.loader import load_candles
from data.timeutil import datetime_to_ms
from detectors.swing_open_close_30m_special_v1.config import (
    ANALYSIS_END_CLOSE,
    ANALYSIS_END_OPEN,
    ANALYSIS_START,
    CONFIGURATION_VERSION,
    DATASET,
    DESKTOP_DIR,
    DETECTOR_NAME,
    DETECTOR_VERSION,
    EXCLUSIVE_LOAD_END,
    EXPECTED_MONTHLY,
    EXPECTED_ROWS,
    LOG_PATH,
    PACKAGE_DIR,
    PROJECT_ROOT,
    REPORT_PATH,
    REVISION_PATTERN,
    STEP_MS,
    TIMEFRAME,
    TMP_DIR,
)
from detectors.swing_open_close_30m_special_v1.engine import Analysis, Bar, SpecialSwingError, analyze, signature
from detectors.swing_open_close_30m_special_v1.workbook import validate_workbook, write_workbook

# iso_utc is not in engine - I referenced it by mistake. I'll define locally if missing.
# Wait I didn't export iso_utc from engine. I need to fix the import.


def _decimal(value) -> Decimal:
    if value is None:
        raise SpecialSwingError("required value is null")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _iso(moment: datetime) -> str:
    value = moment.astimezone(timezone.utc)
    if value.microsecond == 0:
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    return value.strftime("%Y-%m-%dT%H:%M:%S") + f".{value.microsecond // 1000:03d}Z"


def fingerprint_file(path: Path) -> dict:
    data = path.read_bytes()
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def fingerprint_tree(root: Path) -> dict[str, dict]:
    found = {}
    if not root.exists():
        return found
    for path in root.rglob("*"):
        if not path.is_file() or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        found[str(path.resolve())] = fingerprint_file(path)
    return found


def protected_fingerprint() -> dict[str, dict]:
    found = {}
    detectors = PROJECT_ROOT / "detectors"
    for path in detectors.rglob("*"):
        if not path.is_file() or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        if PACKAGE_DIR in path.parents or path.parent == PACKAGE_DIR:
            continue
        found[str(path.resolve())] = fingerprint_file(path)
    return found


def allocate_revision(directory: Path) -> tuple[int, Path]:
    pattern = re.compile(REVISION_PATTERN, re.IGNORECASE)
    highest = -1
    if directory.exists():
        for path in directory.glob("*.xlsx"):
            match = pattern.match(path.name)
            if match:
                highest = max(highest, int(match.group(1)))
    revision = highest + 1
    return revision, directory / f"BTCUSDT_30M_Special_Swings_rev{revision:02d}.xlsx"


def load_production() -> dict:
    slice_ = load_candles(
        "BTCUSDT",
        TIMEFRAME,
        ANALYSIS_START,
        EXCLUSIVE_LOAD_END,
        warmup_bars=0,
        precision_mode=config.PRECISION_EXACT,
    )
    if slice_.symbol != "BTCUSDT" or slice_.timeframe != TIMEFRAME or slice_.precision_mode != config.PRECISION_EXACT:
        raise SpecialSwingError("dataset identity mismatch")
    if slice_.warmup_bars_loaded != 0:
        raise SpecialSwingError("warmup candles are not allowed")
    table = slice_.table
    fields = {
        name: [_decimal(value) for value in table.column(name).to_pylist()]
        for name in ("open", "high", "low", "close", "volume", "quote_asset_volume")
    }
    opens = table.column("open_time").to_pylist()
    closes = table.column("close_time").to_pylist()
    symbols = table.column("symbol").to_pylist()
    trades = table.column("number_of_trades").to_pylist()
    bars: list[Bar] = []
    for index in range(table.num_rows):
        if symbols[index] != "BTCUSDT":
            raise SpecialSwingError(f"unexpected symbol at row {index}")
        trade_count = trades[index]
        if trade_count is None or int(trade_count) < 0:
            raise SpecialSwingError("negative or null trade count")
        opened = opens[index]
        closed = closes[index]
        if opened.tzinfo is None or opened.utcoffset() != timezone.utc.utcoffset(opened):
            raise SpecialSwingError("timestamp is not UTC")
        bars.append(Bar(
            row=index,
            open_time=opened,
            close_time=closed,
            open=fields["open"][index],
            high=fields["high"][index],
            low=fields["low"][index],
            close=fields["close"][index],
            volume=fields["volume"][index],
            quote_volume=fields["quote_asset_volume"][index],
            trades=int(trade_count),
        ))
    _validate_frame(bars)
    sources = [Path(path) for path in slice_.source_files]
    for path in sources:
        text = str(path).replace("/", "\\").lower()
        if "perpetual\\30m\\symbol=btcusdt" not in text:
            raise SpecialSwingError(f"unexpected source path {path}")
        if not path.is_file():
            raise SpecialSwingError(f"source parquet is missing: {path}")
        with path.open("rb"):
            pass
    return {"bars": bars, "source_paths": sources}


def _validate_frame(bars: list[Bar]) -> None:
    if len(bars) != EXPECTED_ROWS:
        raise SpecialSwingError(f"rows {len(bars)} != {EXPECTED_ROWS}")
    if bars[0].open_time != ANALYSIS_START or bars[-1].open_time != ANALYSIS_END_OPEN:
        raise SpecialSwingError("analysis endpoints do not match")
    if bars[-1].close_time != ANALYSIS_END_CLOSE:
        raise SpecialSwingError("last close time mismatch")
    monthly: dict[tuple[int, int], int] = {}
    previous = None
    seen = set()
    for bar in bars:
        key = (bar.open_time.year, bar.open_time.month)
        monthly[key] = monthly.get(key, 0) + 1
        if bar.open_time in seen or (previous is not None and bar.open_time <= previous):
            raise SpecialSwingError("duplicate or unordered open time")
        seen.add(bar.open_time)
        if previous is not None and datetime_to_ms(bar.open_time) - datetime_to_ms(previous) != STEP_MS:
            raise SpecialSwingError(f"missing 30m timestamp at {_iso(bar.open_time)}")
        if datetime_to_ms(bar.close_time) != datetime_to_ms(bar.open_time) + STEP_MS - 1:
            raise SpecialSwingError(f"close time mismatch at {_iso(bar.open_time)}")
        if min(bar.volume, bar.quote_volume) < 0 or bar.trades < 0:
            raise SpecialSwingError("negative volume or trade count")
        if not (bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high and bar.low <= bar.high):
            raise SpecialSwingError(f"OHLC violation at {_iso(bar.open_time)}")
        previous = bar.open_time
    if monthly != EXPECTED_MONTHLY:
        raise SpecialSwingError(f"monthly counts {monthly}")


def _publish(result: Analysis, protected_before: dict, source_before: dict) -> Path:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    revision, final_path = allocate_revision(DESKTOP_DIR)
    if final_path.exists():
        raise SpecialSwingError(f"refusing to overwrite {final_path}")
    temporary = TMP_DIR / f"{final_path.stem}.tmp.xlsx"
    write_workbook(temporary, result)
    validate_workbook(temporary, result)
    revision, final_path = allocate_revision(DESKTOP_DIR)
    if final_path.exists():
        temporary.unlink(missing_ok=True)
        raise SpecialSwingError(f"refusing to overwrite {final_path}")
    os.replace(temporary, final_path)
    if protected_fingerprint() != protected_before:
        final_path.unlink(missing_ok=True)
        raise SpecialSwingError("protected detector files changed")
    if {path: fingerprint_file(Path(path)) for path in source_before} != source_before:
        final_path.unlink(missing_ok=True)
        raise SpecialSwingError("source parquet changed")
    return final_path


def _jsonable(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def main() -> int:
    protected_before = protected_fingerprint()
    loaded = load_production()
    source_before = {str(path.resolve()): fingerprint_file(path) for path in loaded["source_paths"]}
    result = analyze(loaded["bars"])
    again = analyze(loaded["bars"])
    if signature(result) != signature(again):
        raise SpecialSwingError("detection is not deterministic")
    final_path = _publish(result, protected_before, source_before)
    validation = validate_workbook(final_path, result)
    counters = result.counters
    payload = {
        "detector_name": DETECTOR_NAME,
        "detector_version": DETECTOR_VERSION,
        "configuration_version": CONFIGURATION_VERSION,
        "dataset": str(DATASET),
        "rows": len(loaded["bars"]),
        "monthly": {f"{year}-{month:02d}": count for (year, month), count in EXPECTED_MONTHLY.items()},
        "source_files": source_before,
        "counters": _jsonable(counters),
        "determinism": True,
        "workbook": str(final_path),
        "workbook_validation": validation,
        "protected_changed": 0,
        "source_changed": 0,
        "python": sys.version,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(message)s", force=True)
    logging.info("%s %s", DETECTOR_NAME, CONFIGURATION_VERSION)
    logging.info("rows %s workbook %s", len(loaded["bars"]), final_path)
    logging.info("primary %s raw %s", counters["displayed"], counters["raw"])
    print(f"WORKBOOK {final_path}")
    print(f"PRIMARY {counters['displayed']} HIGH {counters['swing_high']} LOW {counters['swing_low']} RAW {counters['raw']}")
    print(f"REPORT {REPORT_PATH}")
    print(f"LOG {LOG_PATH}")
    for path in TMP_DIR.glob("*.tmp.xlsx"):
        path.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
