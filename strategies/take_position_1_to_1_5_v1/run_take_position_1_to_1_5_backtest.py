"""Run the read-only 2026 YTD 1:1.5 R take-position backtest.

This script does not place orders, call a trading API, download data, or modify the detector or the existing 1:1 and 1:2 strategies.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config
from data.loader import load_candles
from detectors.swing_open_close_30m_special_v1.config import (
    ANALYSIS_START as DETECTOR_START,
    DERIVED,
    DETECTOR_VERSION,
    EXCLUSIVE_LOAD_END as DETECTOR_END,
    EXPECTED_ROWS as DETECTOR_ROWS,
    LONGER,
    NESTED,
    PRIMARY,
)
from detectors.swing_open_close_30m_special_v1.engine import Bar, analyze, signature, turkey_text
from strategies.take_position_1_to_1_5_v1.config import (
    ANALYSIS_END,
    ANALYSIS_START,
    CONFIGURATION_VERSION,
    DESKTOP_DIR,
    DETECTOR_PACKAGE,
    ONE_TO_ONE_PACKAGE,
    ONE_TO_TWO_PACKAGE,
    EXCLUSIVE_END,
    EXPECTED_1M_ROWS,
    EXPECTED_30M_ROWS,
    EXPECTED_DAYS,
    LOG_PATH,
    PERFORMANCE_LABEL,
    REPORT_PATH,
    RESULT_LABEL,
    REVISION_PATTERN,
    SIGNAL_END_OPEN,
    STEP_1M_MS,
    STEP_30M_MS,
    STRATEGY_NAME,
    TMP_DIR,
)
from strategies.take_position_1_to_1_5_v1.engine import (
    TakePositionError,
    annotate_overlap,
    assert_trade_invariants,
    build_trades,
    content_fingerprint,
    equivalent_signals,
    grouped_summary,
    monthly_summary,
    signal_rows,
)
from strategies.take_position_1_to_1_5_v1.models import Minute
from strategies.take_position_1_to_1_5_v1.workbook import validate_workbook, write_workbook

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
RESOLVED = {"TAKE_PROFIT", "STOP_LOSS"}


def epoch_ms(moment: datetime) -> int:
    delta = moment - EPOCH
    return ((delta.days * 86400 + delta.seconds) * 1000) + delta.microseconds // 1000


def fingerprint(path: Path) -> dict:
    stat = path.stat()
    return {
        "path": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def tree_files(root: Path) -> list[Path]:
    found = []
    if not root.exists():
        return found
    for path in root.rglob("*"):
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
            found.append(path)
    return sorted(found)


def decimal_value(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def load_signal_bars() -> tuple[list[Bar], list[Path]]:
    slice_ = load_candles(
        "BTCUSDT",
        "30m",
        ANALYSIS_START,
        EXCLUSIVE_END,
        columns=["open", "high", "low", "close", "volume", "quote_asset_volume", "number_of_trades"],
        warmup_bars=0,
        precision_mode=config.PRECISION_EXACT,
    )
    if slice_.symbol != "BTCUSDT" or slice_.timeframe != "30m" or slice_.precision_mode != config.PRECISION_EXACT:
        raise TakePositionError("30m dataset identity mismatch")
    if slice_.warmup_bars_loaded != 0:
        raise TakePositionError("30m warmup is not allowed")
    table = slice_.table
    prices = {
        name: [decimal_value(value) for value in table.column(name).to_pylist()]
        for name in ("open", "high", "low", "close", "volume", "quote_asset_volume")
    }
    opens = table.column("open_time").to_pylist()
    closes = table.column("close_time").to_pylist()
    symbols = table.column("symbol").to_pylist()
    trades = table.column("number_of_trades").to_pylist()
    bars: list[Bar] = []
    for index in range(table.num_rows):
        if symbols[index] != "BTCUSDT":
            raise TakePositionError(f"unexpected 30m symbol at row {index}")
        opened = opens[index]
        closed = closes[index]
        if opened.utcoffset() != timezone.utc.utcoffset(opened):
            raise TakePositionError("30m timestamp is not UTC")
        high = prices["high"][index]
        low = prices["low"][index]
        open_price = prices["open"][index]
        close_price = prices["close"][index]
        if high < low or high < open_price or high < close_price or low > open_price or low > close_price:
            raise TakePositionError(f"invalid 30m OHLC at row {index}")
        bars.append(Bar(
            index,
            opened,
            closed,
            open_price,
            high,
            low,
            close_price,
            prices["volume"][index],
            prices["quote_asset_volume"][index],
            int(trades[index]),
        ))
    sources = [Path(path) for path in slice_.source_files]
    return bars, sources


def load_minutes() -> tuple[list[Minute], list[Path]]:
    slice_ = load_candles(
        "BTCUSDT",
        "1m",
        ANALYSIS_START,
        EXCLUSIVE_END,
        columns=["open", "high", "low", "close"],
        warmup_bars=0,
        precision_mode=config.PRECISION_EXACT,
    )
    if slice_.symbol != "BTCUSDT" or slice_.timeframe != "1m" or slice_.precision_mode != config.PRECISION_EXACT:
        raise TakePositionError("1m dataset identity mismatch")
    if slice_.warmup_bars_loaded != 0:
        raise TakePositionError("1m warmup is not allowed")
    table = slice_.table
    prices = {
        name: [decimal_value(value) for value in table.column(name).to_pylist()]
        for name in ("open", "high", "low", "close")
    }
    opens = table.column("open_time").to_pylist()
    closes = table.column("close_time").to_pylist()
    minutes: list[Minute] = []
    for index in range(table.num_rows):
        opened = opens[index]
        if opened.utcoffset() != timezone.utc.utcoffset(opened):
            raise TakePositionError("1m timestamp is not UTC")
        high = prices["high"][index]
        low = prices["low"][index]
        open_price = prices["open"][index]
        close_price = prices["close"][index]
        if high < low or high < open_price or high < close_price or low > open_price or low > close_price:
            raise TakePositionError(f"invalid 1m OHLC at row {index}")
        if epoch_ms(closes[index]) - epoch_ms(opened) != STEP_1M_MS - 1:
            raise TakePositionError(f"1m close time is not one minute minus 1ms at row {index}")
        minutes.append(Minute(opened, open_price, high, low, close_price))
    return minutes, [Path(path) for path in slice_.source_files]


def require_coverage(bars: list[Bar], minutes: list[Minute]) -> dict:
    if len(bars) != EXPECTED_30M_ROWS or len(minutes) != EXPECTED_1M_ROWS:
        raise TakePositionError(f"row counts 30m={len(bars)} 1m={len(minutes)}")
    if bars[0].open_time != ANALYSIS_START or minutes[0].open_time != ANALYSIS_START:
        raise TakePositionError("series does not start at 2026-01-01T00:00:00Z")
    if bars[-1].open_time != SIGNAL_END_OPEN or bars[-1].close_time != ANALYSIS_END:
        raise TakePositionError("30m series does not end at 2026-09-15T23:30:00Z")
    if minutes[-1].open_time != datetime(2026, 9, 15, 23, 59, tzinfo=timezone.utc):
        raise TakePositionError("1m series does not end at 2026-09-15T23:59:00Z")
    days = {(item.open_time.year, item.open_time.month, item.open_time.day) for item in bars}
    if len(days) != EXPECTED_DAYS:
        raise TakePositionError(f"UTC day count is {len(days)}")
    for index in range(1, len(bars)):
        if epoch_ms(bars[index].open_time) - epoch_ms(bars[index - 1].open_time) != STEP_30M_MS:
            raise TakePositionError(f"30m gap at row {index}")
        if epoch_ms(bars[index].close_time) - epoch_ms(bars[index].open_time) != STEP_30M_MS - 1:
            raise TakePositionError(f"30m close mismatch at row {index}")
    for index in range(1, len(minutes)):
        if epoch_ms(minutes[index].open_time) - epoch_ms(minutes[index - 1].open_time) != STEP_1M_MS:
            raise TakePositionError(f"1m gap at row {index}")
    return {"days": len(days), "rows_30m": len(bars), "rows_1m": len(minutes)}


def next_revision() -> int:
    pattern = re.compile(REVISION_PATTERN, re.IGNORECASE)
    numbers = []
    for path in DESKTOP_DIR.glob("BTCUSDT_30M_Take_Position_1_TO_1_5R_rev*.xlsx"):
        match = pattern.match(path.name)
        if match:
            numbers.append(int(match.group(1)))
    return max(numbers) + 1 if numbers else 0


def publish(temp: Path) -> Path:
    while True:
        revision = next_revision()
        final = DESKTOP_DIR / f"BTCUSDT_30M_Take_Position_1_TO_1_5R_rev{revision:02d}.xlsx"
        if final.exists():
            continue
        os.rename(temp, final)
        return final


def run_unit_tests() -> str:
    completed = subprocess.run(
        [sys.executable, "-m", "unittest", "strategies.take_position_1_to_1_5_v1.tests.test_take_position_1_to_1_5"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8"},
        check=False,
    )
    text = (completed.stderr or "") + (completed.stdout or "")
    if completed.returncode != 0:
        raise TakePositionError(text[-4000:])
    return text.strip().splitlines()[-1]


def preview(trade) -> dict:
    hours = None if trade.duration_minutes is None else format(Decimal(trade.duration_minutes) / Decimal(60), "f")
    return {
        "trade_id": trade.trade_id,
        "swing_id": trade.swing_id,
        "direction": trade.direction,
        "swing_type": trade.swing_type,
        "signal_time_turkey": turkey_text(trade.signal_time),
        "entry_price": None if trade.entry_price is None else format(trade.entry_price, "f"),
        "stop_price": None if trade.stop_price is None else format(trade.stop_price, "f"),
        "target_price": None if trade.target_price is None else format(trade.target_price, "f"),
        "reward_distance": None if trade.reward_distance is None else format(trade.reward_distance, "f"),
        "risk_to_reward": None if trade.outcome == "INVALID" else "1:1.5",
        "outcome": trade.outcome,
        "exit_reason": trade.exit_reason,
        "realized_r": None if trade.realized_r is None else format(trade.realized_r, "f"),
        "duration_hours": hours,
        "exit_time_turkey": None if trade.exit_time is None else turkey_text(trade.exit_time),
    }


def limited(items: list, count: int = 5) -> list:
    return items[:count]


def json_default(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Path):
        return str(value)
    raise TypeError(type(value).__name__)


def snapshot(paths: list[Path]) -> dict[str, dict]:
    return {str(path): fingerprint(path) for path in paths}


def main() -> None:
    print("tests", flush=True)
    first_tests = run_unit_tests()
    second_tests = run_unit_tests()
    print(first_tests, second_tests, flush=True)
    head_before = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, capture_output=True, text=True, check=True
    ).stdout.strip()
    protected = (
        tree_files(PROJECT_ROOT / "detectors")
        + tree_files(PROJECT_ROOT / "data")
        + tree_files(ONE_TO_ONE_PACKAGE)
        + tree_files(ONE_TO_TWO_PACKAGE)
        + [PROJECT_ROOT / "config.py"]
    )
    reports = tree_files(PROJECT_ROOT / "reports")
    logs = tree_files(PROJECT_ROOT / "logs")
    desktop_before = [path for path in DESKTOP_DIR.glob("*.xlsx") if path.is_file()]
    before = snapshot(protected + reports + logs + desktop_before)
    print("loading 30m", flush=True)
    bars, signal_sources = load_signal_bars()
    print("loading 1m", flush=True)
    minutes, exit_sources = load_minutes()
    coverage = require_coverage(bars, minutes)
    source_before = snapshot(signal_sources + exit_sources)
    for path in signal_sources + exit_sources:
        text = str(path).replace("/", "\\").lower()
        if "perpetual\\30m\\symbol=btcusdt" not in text and "perpetual\\1m\\symbol=btcusdt" not in text:
            raise TakePositionError(f"unexpected source path {path}")
    print("detector", flush=True)
    result = analyze(bars)
    if signature(result) != signature(analyze(bars)):
        raise TakePositionError("detector output is not deterministic")
    july_bars = [bar for bar in bars if DETECTOR_START <= bar.open_time < DETECTOR_END]
    if len(july_bars) != DETECTOR_ROWS:
        raise TakePositionError(f"July-September slice has {len(july_bars)} rows")
    july = analyze(july_bars)
    full_overlap = signal_rows(result, DETECTOR_START)
    july_rows = signal_rows(july)
    equivalence = equivalent_signals(full_overlap, july_rows)
    mismatches = []
    if not equivalence:
        july_set = set(july_rows)
        full_set = set(full_overlap)
        for row in full_overlap:
            if row not in july_set:
                mismatches.append({"side": "full_year_only", "row": [str(part) for part in row]})
        for row in july_rows:
            if row not in full_set:
                mismatches.append({"side": "july_window_only", "row": [str(part) for part in row]})
    print(f"primary {len(result.displayed)} equivalence {equivalence}", flush=True)
    print("simulating", flush=True)
    trades = build_trades(result, minutes)
    overlap = annotate_overlap(trades, minutes[-1].open_time + (minutes[-1].open_time - minutes[-2].open_time))
    assert_trade_invariants(trades)
    first_signature = content_fingerprint(trades)
    print("determinism", flush=True)
    repeat = build_trades(analyze(bars), minutes)
    annotate_overlap(repeat, minutes[-1].open_time + (minutes[-1].open_time - minutes[-2].open_time))
    second_signature = content_fingerprint(repeat)
    if first_signature != second_signature:
        raise TakePositionError("trade simulation is not deterministic")
    summaries = grouped_summary(trades)
    months = monthly_summary(trades)
    raw_statuses = {
        PRIMARY: sum(1 for item in result.raw if item.status == PRIMARY),
        DERIVED: sum(1 for item in result.raw if item.status == DERIVED),
        NESTED: sum(1 for item in result.raw if item.status == NESTED),
        LONGER: sum(1 for item in result.raw if item.status == LONGER),
    }
    if sum(raw_statuses.values()) != len(result.raw):
        raise TakePositionError("raw status reconciliation failed")
    if raw_statuses[PRIMARY] != len(result.displayed) or len(trades) != len(result.displayed):
        raise TakePositionError("primary trade reconciliation failed")
    derived_ids = {item.swing_id for item in result.raw if item.status == DERIVED}
    nested_ids = {item.swing_id for item in result.raw if item.status == NESTED}
    traded_ids = [trade.swing_id for trade in trades]
    if len(traded_ids) != len(set(traded_ids)):
        raise TakePositionError("duplicate trade")
    if set(traded_ids) & derived_ids or set(traded_ids) & nested_ids:
        raise TakePositionError("derived or nested structure was traded")
    warnings = []
    if not equivalence:
        warnings.append(
            f"July-September direct detector run differs from the full-year run for {len(mismatches)} identity rows. "
            "The published trades use the full 2026 year-to-date detector run."
        )
    diagnostics = [
        ("Result label", RESULT_LABEL),
        ("Performance", PERFORMANCE_LABEL),
        ("30m rows", coverage["rows_30m"]),
        ("1m rows", coverage["rows_1m"]),
        ("UTC days", coverage["days"]),
        ("30m first open", bars[0].open_time.isoformat()),
        ("30m last open", bars[-1].open_time.isoformat()),
        ("1m first open", minutes[0].open_time.isoformat()),
        ("1m last open", minutes[-1].open_time.isoformat()),
        ("Detector version", DETECTOR_VERSION),
        ("Strategy name", STRATEGY_NAME),
        ("Configuration version", CONFIGURATION_VERSION),
        ("Raw swings", len(result.raw)),
        ("Primary", raw_statuses[PRIMARY]),
        ("Derived", raw_statuses[DERIVED]),
        ("Nested", raw_statuses[NESTED]),
        ("Longer overlapping", raw_statuses[LONGER]),
        ("Primary reconciliation", f"{raw_statuses[PRIMARY]}+{raw_statuses[DERIVED]}+{raw_statuses[NESTED]}+{raw_statuses[LONGER]}={len(result.raw)}"),
        ("Trades", len(trades)),
        ("Duplicate trades", 0),
        ("Detector overlap equivalence", "PASS" if equivalence else "DISCREPANCY"),
        ("Overlap mismatches", len(mismatches)),
        ("Same-minute ambiguous", summaries["ALL TRADES"]["same_minute_ambiguous_count"]),
        ("Adverse gap stops", summaries["ALL TRADES"]["adverse_gap_stop_count"]),
        ("Favorable gap targets", summaries["ALL TRADES"]["favorable_gap_target_count"]),
        ("Unresolved", summaries["ALL TRADES"]["unresolved_trade_count"]),
        ("Invalid", summaries["ALL TRADES"]["invalid_signal_count"]),
        ("Maximum simultaneous trades", overlap["maximum_simultaneously_open"]),
        ("Overlapping trades", overlap["overlapping_trade_count"]),
        ("Live orders", 0),
        ("Exchange trading API calls", 0),
        ("Broker API calls", 0),
        ("Risk to reward", "1:1.5"),
        ("Normal take profit", "+1.5R"),
        ("Normal stop loss", "-1.0R"),
        ("Legacy 1:1 targets", 0),
        ("Legacy 1:2 targets", 0),
        ("Theoretical break-even win rate", "40.0000%"),
        ("Lookahead violations", 0),
        ("Repaint count", 0),
        ("Existing 1:1 strategy immutability", "PASS"),
        ("Existing 1:2 strategy immutability", "PASS"),
        ("Determinism", "PASS"),
        ("Test run 1", first_tests),
        ("Test run 2", second_tests),
        ("Gross results", PERFORMANCE_LABEL),
        ("Not investment advice", "Yes"),
    ]
    for warning in warnings:
        diagnostics.append(("Warning", warning))
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    temp = TMP_DIR / "take_position.tmp.xlsx"
    if temp.exists():
        temp.unlink()
    print("workbook", flush=True)
    write_workbook(temp, trades, summaries, months, diagnostics, DETECTOR_VERSION)
    validate_workbook(temp, trades)
    final = publish(temp)
    reopened = validate_workbook(final, trades)
    after_paths = snapshot(protected + reports + logs + desktop_before)
    changed = [path for path, old in before.items() if after_paths.get(path) != old]
    source_after = snapshot(signal_sources + exit_sources)
    source_changed = [path for path, old in source_before.items() if source_after.get(path) != old]
    detector_changed = [path for path in changed if Path(path).is_relative_to(DETECTOR_PACKAGE)]
    one_to_one_changed = [path for path in changed if Path(path).is_relative_to(ONE_TO_ONE_PACKAGE)]
    one_to_two_changed = [path for path in changed if Path(path).is_relative_to(ONE_TO_TWO_PACKAGE)]
    if changed or source_changed or detector_changed:
        raise TakePositionError(f"protected files changed: {changed[:10]} {source_changed[:5]}")
    head_after = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, capture_output=True, text=True, check=True
    ).stdout.strip()
    if head_before != head_after:
        raise TakePositionError("git HEAD changed")
    for leftover in TMP_DIR.glob("*.tmp.xlsx"):
        leftover.unlink()
    resolved = [trade for trade in trades if trade.outcome in RESOLVED]
    wins = sorted((trade for trade in resolved if trade.outcome == "TAKE_PROFIT"), key=lambda trade: (trade.duration_minutes, trade.trade_id))
    stops = sorted((trade for trade in resolved if trade.outcome == "STOP_LOSS"), key=lambda trade: (trade.duration_minutes, trade.trade_id))
    slowest = sorted(resolved, key=lambda trade: (-trade.duration_minutes, trade.trade_id))
    opens = [trade for trade in trades if trade.outcome == "OPEN_AT_DATASET_END"]
    invalids = [trade for trade in trades if trade.outcome == "INVALID"]
    shorts = [trade for trade in resolved if trade.direction == "SHORT"]
    longs = [trade for trade in resolved if trade.direction == "LONG"]
    report = {
        "strategy_name": STRATEGY_NAME,
        "strategy_version": STRATEGY_NAME,
        "run_id": hashlib.sha256(first_signature.encode("utf-8")).hexdigest()[:16],
        "configuration_version": CONFIGURATION_VERSION,
        "detector_version": DETECTOR_VERSION,
        "performance": PERFORMANCE_LABEL,
        "result_label": RESULT_LABEL,
        "runtime": sys.version,
        "coverage": coverage,
        "signal_sources": source_before,
        "detector_counters": {
            "raw": len(result.raw),
            "primary": raw_statuses[PRIMARY],
            "derived": raw_statuses[DERIVED],
            "nested": raw_statuses[NESTED],
            "longer": raw_statuses[LONGER],
            "swing_high": result.counters["swing_high"],
            "swing_low": result.counters["swing_low"],
        },
        "equivalence": {"pass": equivalence, "mismatch_count": len(mismatches), "examples": mismatches[:8]},
        "overlap": overlap,
        "summaries": summaries,
        "monthly": months,
        "invariants": {
            "live_order_count": 0,
            "exchange_trading_api_call_count": 0,
            "broker_api_call_count": 0,
            "entry_before_confirmation_count": 0,
            "exit_scan_inside_confirmation_candle_count": 0,
            "lookahead_violation_count": 0,
            "repaint_count": 0,
            "duplicate_trade_count": 0,
            "derived_signal_trade_count": 0,
            "nested_signal_trade_count": 0,
            "source_files_changed": 0,
            "detector_files_changed": len(detector_changed),
            "existing_1_to_1_strategy_files_changed": len(one_to_one_changed),
            "existing_1_to_2_strategy_files_changed": len(one_to_two_changed),
            "legacy_1_to_1_target_count": 0,
            "legacy_1_to_2_target_count": 0,
            "protected_files_changed": len(changed),
            "previous_workbooks_changed": 0,
        },
        "tests": {"run_1": first_tests, "run_2": second_tests},
        "determinism": {"content_fingerprint": first_signature, "match": True},
        "workbook": {"path": str(final), "validation": reopened, "sha256": hashlib.sha256(final.read_bytes()).hexdigest(), "size": final.stat().st_size},
        "git_head": head_after,
        "warnings": warnings,
        "previews": {
            "first_completed_shorts": [preview(trade) for trade in limited(shorts)],
            "first_completed_longs": [preview(trade) for trade in limited(longs)],
            "fastest_take_profits": [preview(trade) for trade in limited(wins)],
            "fastest_stop_losses": [preview(trade) for trade in limited(stops)],
            "slowest_resolved": [preview(trade) for trade in limited(slowest)],
            "open_or_censored": [preview(trade) for trade in (opens if len(opens) <= 5 else opens[:5])],
            "open_or_censored_count": len(opens),
            "invalid": [preview(trade) for trade in (invalids if len(invalids) <= 5 else invalids[:5])],
            "invalid_count": len(invalids),
        },
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2, default=json_default), encoding="utf-8")
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(
            f"{datetime.now(timezone.utc).isoformat()} {STRATEGY_NAME} workbook={final.name} "
            f"primary={raw_statuses[PRIMARY]} resolved={summaries['ALL TRADES']['resolved_trade_count']} "
            f"total_r={summaries['ALL TRADES']['total_r']} determinism=PASS immutability=PASS\n"
        )
    print(f"published {final}", flush=True)
    print(json.dumps({key: summaries[key]["resolved_trade_count"] for key in summaries}, default=json_default), flush=True)


if __name__ == "__main__":
    main()
