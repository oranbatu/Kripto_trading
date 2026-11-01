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
import time
import traceback
from datetime import datetime, timezone
from decimal import Decimal

import config
from data.loader import load_candles
from data.timeutil import datetime_to_ms
from detectors.swing_open_close_30m_special_v1.config import (
    ALGORITHM_VERSION,
    ANALYSIS_END_CLOSE,
    ANALYSIS_END_OPEN,
    ANALYSIS_START,
    CONFIGURATION_VERSION,
    DETECTOR_NAME,
    DETECTOR_VERSION,
    EXCLUSIVE_LOAD_END,
    EXPECTED_MONTHLY,
    EXPECTED_ROWS,
    LOG_PATH,
    PACKAGE_DIR,
    PROJECT_ROOT,
    STEP_MS,
    SYMBOL,
    TIMEFRAME,
    TMP_DIR,
)
from detectors.swing_open_close_30m_special_v1.desktop_output import visible_desktop
from detectors.swing_open_close_30m_special_v1.engine import Analysis, Bar, SpecialSwingError, analyze, signature
from detectors.swing_open_close_30m_special_v1.symbols import SymbolProfile, parameters_for, require_profile
from detectors.swing_open_close_30m_special_v1.workbook import validate_workbook, write_workbook


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


def allocate_revision(directory: Path, symbol: str = SYMBOL) -> tuple[int, Path]:
    profile = require_profile(symbol)
    pattern = re.compile(rf"^{re.escape(profile.workbook_prefix)}_rev(\d{{2,}})\.xlsx$", re.IGNORECASE)
    highest = -1
    if directory.exists():
        for path in directory.glob("*.xlsx"):
            match = pattern.match(path.name)
            if match:
                highest = max(highest, int(match.group(1)))
    revision = highest + 1
    return revision, directory / f"{profile.workbook_prefix}_rev{revision:02d}.xlsx"


def load_production() -> dict:
    return load_window(
        start=ANALYSIS_START,
        exclusive_end=EXCLUSIVE_LOAD_END,
        end_open=ANALYSIS_END_OPEN,
        end_close=ANALYSIS_END_CLOSE,
        expected_rows=EXPECTED_ROWS,
        expected_monthly=EXPECTED_MONTHLY,
    )


def load_window(
    *,
    start: datetime,
    exclusive_end: datetime,
    end_open: datetime,
    end_close: datetime,
    expected_rows: int,
    expected_monthly: dict[tuple[int, int], int],
    symbol: str = SYMBOL,
) -> dict:
    profile = require_profile(symbol)
    slice_ = load_candles(
        profile.symbol,
        TIMEFRAME,
        start,
        exclusive_end,
        warmup_bars=0,
        precision_mode=config.PRECISION_EXACT,
    )
    if slice_.symbol != profile.symbol or slice_.timeframe != TIMEFRAME or slice_.precision_mode != config.PRECISION_EXACT:
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
        if symbols[index] != profile.symbol:
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
    _validate_frame(
        bars,
        start=start,
        exclusive_end=exclusive_end,
        end_open=end_open,
        end_close=end_close,
        expected_rows=expected_rows,
        expected_monthly=expected_monthly,
    )
    sources = [Path(path) for path in slice_.source_files]
    for path in sources:
        text = str(path).replace("/", "\\").lower()
        expected = str(profile.source_dir).replace("/", "\\").lower()
        if expected not in text or f"symbol={profile.symbol.lower()}" not in text:
            raise SpecialSwingError(f"unexpected source path {path}")
        if not path.is_file():
            raise SpecialSwingError(f"source parquet is missing: {path}")
        with path.open("rb"):
            pass
    return {"bars": bars, "source_paths": sources, "profile": profile}


def _validate_frame(
    bars: list[Bar],
    *,
    start: datetime,
    exclusive_end: datetime,
    end_open: datetime,
    end_close: datetime,
    expected_rows: int,
    expected_monthly: dict[tuple[int, int], int],
) -> None:
    if len(bars) != expected_rows:
        raise SpecialSwingError(f"rows {len(bars)} != {expected_rows}")
    if bars[0].open_time != start or bars[-1].open_time != end_open:
        raise SpecialSwingError("analysis endpoints do not match")
    if bars[-1].close_time != end_close:
        raise SpecialSwingError("last close time mismatch")
    if any(bar.open_time < start or bar.open_time >= exclusive_end for bar in bars):
        raise SpecialSwingError("candle opens outside the half-open analysis window")
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
    if monthly != expected_monthly:
        raise SpecialSwingError(f"monthly counts {monthly}")


def _publish(result: Analysis, protected_before: dict, source_before: dict, profile: SymbolProfile) -> Path:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    desktop = visible_desktop().path
    while True:
        revision, final_path = allocate_revision(desktop, profile.symbol)
        if final_path.exists():
            continue
        temporary = TMP_DIR / f"{final_path.stem}.tmp.xlsx"
        parameters = parameters_for(profile, revision)
        write_workbook(temporary, result, parameters)
        validate_workbook(temporary, result, parameters)
        confirmed_revision, confirmed_path = allocate_revision(desktop, profile.symbol)
        if confirmed_path != final_path or confirmed_path.exists() or confirmed_revision != revision:
            temporary.unlink(missing_ok=True)
            continue
        os.replace(temporary, final_path)
        break
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


def _cleanup_temporary_workbooks() -> None:
    if not TMP_DIR.exists():
        return
    for path in TMP_DIR.glob("*.tmp.xlsx"):
        path.unlink(missing_ok=True)


def _print_failure(exc: BaseException, profile: SymbolProfile | None = None) -> None:
    print("STATUS FAILURE")
    print(f"DETECTOR {DETECTOR_NAME}")
    print(f"DETECTOR_VERSION {DETECTOR_VERSION}")
    print(f"ALGORITHM_VERSION {ALGORITHM_VERSION}")
    print(f"CONFIGURATION_VERSION {CONFIGURATION_VERSION}")
    if profile is not None:
        print(f"SYMBOL {profile.symbol}")
        print(f"LOG {profile.log_path}")
    else:
        print(f"LOG {LOG_PATH}")
    print(f"ERROR {type(exc).__name__}: {exc}")


def analyze_symbol(symbol: str) -> dict:
    """Load and analyze one approved symbol without publishing a workbook."""
    profile = require_profile(symbol)
    loaded = load_production() if profile.symbol == SYMBOL else load_window(
        start=ANALYSIS_START,
        exclusive_end=EXCLUSIVE_LOAD_END,
        end_open=ANALYSIS_END_OPEN,
        end_close=ANALYSIS_END_CLOSE,
        expected_rows=EXPECTED_ROWS,
        expected_monthly=EXPECTED_MONTHLY,
        symbol=profile.symbol,
    )
    result = analyze(loaded["bars"])
    again = analyze(loaded["bars"])
    fingerprint = signature(result)
    if fingerprint != signature(again):
        raise SpecialSwingError("detection is not deterministic")
    return {
        "symbol": profile.symbol,
        "tradingview_symbol": profile.tradingview_symbol,
        "source": str(profile.source_dir),
        "partitions": [str(path) for path in loaded["source_paths"]],
        "rows": len(loaded["bars"]),
        "first_open": _iso(loaded["bars"][0].open_time),
        "last_open": _iso(loaded["bars"][-1].open_time),
        "last_close": _iso(loaded["bars"][-1].close_time),
        "validation": "passed",
        "swing_high": result.counters["swing_high"],
        "swing_low": result.counters["swing_low"],
        "retained": result.counters["displayed"],
        "fingerprint": fingerprint,
        "warnings": list(result.warnings),
    }


def _execute(profile: SymbolProfile) -> int:
    started = time.perf_counter()
    protected_before = protected_fingerprint()
    loaded = load_production() if profile.symbol == SYMBOL else load_window(
        start=ANALYSIS_START,
        exclusive_end=EXCLUSIVE_LOAD_END,
        end_open=ANALYSIS_END_OPEN,
        end_close=ANALYSIS_END_CLOSE,
        expected_rows=EXPECTED_ROWS,
        expected_monthly=EXPECTED_MONTHLY,
        symbol=profile.symbol,
    )
    source_before = {str(path.resolve()): fingerprint_file(path) for path in loaded["source_paths"]}
    result = analyze(loaded["bars"])
    again = analyze(loaded["bars"])
    if signature(result) != signature(again):
        raise SpecialSwingError("detection is not deterministic")
    final_path = _publish(result, protected_before, source_before, profile)
    parameters = parameters_for(profile, int(final_path.stem.rsplit("rev", 1)[-1]))
    validation = validate_workbook(final_path, result, parameters)
    counters = result.counters
    payload = {
        "detector_name": DETECTOR_NAME,
        "detector_version": DETECTOR_VERSION if profile.symbol == SYMBOL else ALGORITHM_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "legacy_btc_detector_version": DETECTOR_VERSION,
        "configuration_version": CONFIGURATION_VERSION,
        "symbol": profile.symbol,
        "tradingview_symbol": profile.tradingview_symbol,
        "dataset": str(profile.source_dir),
        "analysis_start": _iso(ANALYSIS_START),
        "analysis_end_open": _iso(ANALYSIS_END_OPEN),
        "analysis_end_close": _iso(ANALYSIS_END_CLOSE),
        "analysis_end_exclusive": _iso(EXCLUSIVE_LOAD_END),
        "expected_rows": EXPECTED_ROWS,
        "actual_rows": len(loaded["bars"]),
        "rows": len(loaded["bars"]),
        "monthly": {f"{year}-{month:02d}": count for (year, month), count in EXPECTED_MONTHLY.items()},
        "source_files": source_before,
        "counters": _jsonable(counters),
        "fingerprint": signature(result),
        "warnings": list(result.warnings),
        "determinism": True,
        "workbook": str(final_path),
        "workbook_validation": validation,
        "protected_changed": 0,
        "source_changed": 0,
        "python": sys.version,
    }
    profile.report_path.parent.mkdir(parents=True, exist_ok=True)
    profile.log_path.parent.mkdir(parents=True, exist_ok=True)
    profile.report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logging.basicConfig(filename=profile.log_path, level=logging.INFO, format="%(message)s", force=True)
    logging.info("%s %s %s", DETECTOR_NAME, profile.symbol, CONFIGURATION_VERSION)
    logging.info("rows %s workbook %s", len(loaded["bars"]), final_path)
    logging.info("primary %s raw %s", counters["displayed"], counters["raw"])
    elapsed = time.perf_counter() - started
    print(f"DETECTOR {DETECTOR_NAME}")
    print(f"DETECTOR_VERSION {DETECTOR_VERSION if profile.symbol == SYMBOL else ALGORITHM_VERSION}")
    print(f"ALGORITHM_VERSION {ALGORITHM_VERSION}")
    print(f"CONFIGURATION_VERSION {CONFIGURATION_VERSION}")
    print(f"SYMBOL {profile.symbol}")
    print(f"TRADINGVIEW {profile.tradingview_symbol}")
    print(f"TIMEFRAME {TIMEFRAME}")
    print(f"ANALYSIS {ANALYSIS_START.date().isoformat()} {ANALYSIS_END_OPEN.date().isoformat()}")
    print(f"ROWS {len(loaded['bars'])}")
    print(f"SWING_HIGH {counters['swing_high']}")
    print(f"SWING_LOW {counters['swing_low']}")
    print(f"RETAINED {counters['displayed']}")
    print(f"WORKBOOK {final_path}")
    print(f"REVISION {final_path.stem.rsplit('rev', 1)[-1]}")
    print(f"RUNTIME_SECONDS {elapsed:.3f}")
    print("STATUS SUCCESS")
    print(f"REPORT {profile.report_path}")
    print(f"LOG {profile.log_path}")
    _cleanup_temporary_workbooks()
    return 0


def run_for_symbol_cli(symbol: str) -> int:
    """Run one approved symbol. The symbol argument is not read from the process command line."""
    profile = None
    try:
        profile = require_profile(symbol)
        return _execute(profile)
    except Exception as exc:
        _print_failure(exc, profile)
        traceback.print_exc()
        _cleanup_temporary_workbooks()
        return 1


def main() -> int:
    return run_for_symbol_cli(SYMBOL)


def _run() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(_run())
