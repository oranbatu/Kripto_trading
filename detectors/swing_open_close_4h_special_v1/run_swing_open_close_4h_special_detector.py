"""Run the BTCUSDT 4h Special Swing detector."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import config
from data.loader import load_candles
from data.timeutil import datetime_to_ms
from detectors.swing_open_close_4h_special_v1.config import (
    ANALYSIS_END_CLOSE,
    ANALYSIS_END_OPEN,
    ANALYSIS_START,
    CONFIGURATION_VERSION,
    DATASET,
    DESKTOP_DIR,
    DETECTOR_VERSION,
    EXPECTED_MONTHLY,
    EXPECTED_ROWS,
    EXCLUSIVE_LOAD_END,
    LOG_PATH,
    MAPPING_ROOTS,
    MAX_INTERIOR,
    MIN_BOUNDARY,
    NORMAL_MIN_INTERIOR,
    NORMAL_STANDARD_ENABLED,
    NORMAL_USES_FULL_RETURN,
    NORMAL_USES_ONE_THIRD,
    STANDARD_CLASS,
    ZERO_CLASS,
    ZERO_EXCEPTION_ENABLED,
    ZERO_USES_FULL_RETURN,
    ZERO_USES_ONE_THIRD,
    PACKAGE_DIR,
    PROJECT_ROOT,
    PROTECTED_ROOT,
    REPORT_PATH,
    REVISION_PATTERN,
    STEP_MS,
    TIMEFRAME,
    TMP_DIR,
)
from detectors.swing_open_close_4h_special_v1.engine import (
    Analysis,
    Bar,
    SpecialSwingError,
    analyze,
    iso_utc,
    signature,
)
from detectors.swing_open_close_4h_special_v1.workbook import validate_workbook, write_workbook


def fingerprint_tree(root: Path) -> dict[str, tuple[str, int, int]]:
    found = {}
    if not root.exists():
        return found
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name != "__pycache__"]
        for name in filenames:
            if name.endswith(".pyc"):
                continue
            path = Path(dirpath) / name
            stat = path.stat()
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            found[str(path.resolve())] = (digest, stat.st_size, stat.st_mtime_ns)
    return found


def fingerprint_files(paths: list[str]) -> dict[str, tuple[str, int, int]]:
    found = {}
    for raw in paths:
        path = Path(raw)
        stat = path.stat()
        found[str(path.resolve())] = (
            hashlib.sha256(path.read_bytes()).hexdigest(),
            stat.st_size,
            stat.st_mtime_ns,
        )
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
    return revision, directory / f"BTCUSDT_4H_Special_Swings_rev{revision:02d}.xlsx"


def _decimal(value) -> Decimal:
    if value is None:
        raise SpecialSwingError("required value is null")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def load_production() -> dict:
    slice_ = load_candles(
        "BTCUSDT",
        TIMEFRAME,
        ANALYSIS_START,
        EXCLUSIVE_LOAD_END,
        warmup_bars=0,
        precision_mode=config.PRECISION_EXACT,
    )
    if slice_.warmup_bars_loaded != 0 or slice_.symbol != "BTCUSDT" or slice_.timeframe != TIMEFRAME:
        raise SpecialSwingError("dataset identity mismatch")
    if slice_.precision_mode != config.PRECISION_EXACT:
        raise SpecialSwingError("dataset is not exact precision")
    table = slice_.table
    fields = {
        name: [_decimal(value) for value in table.column(name).to_pylist()]
        for name in ("open", "high", "low", "close", "volume", "quote_asset_volume")
    }
    opens = table.column("open_time").to_pylist()
    closes = table.column("close_time").to_pylist()
    symbols = table.column("symbol").to_pylist()
    trades = table.column("number_of_trades").to_pylist()
    bars = []
    for index in range(table.num_rows):
        if symbols[index] != "BTCUSDT":
            raise SpecialSwingError(f"unexpected symbol at row {index}")
        trade_count = trades[index]
        if trade_count is None or int(trade_count) < 0:
            raise SpecialSwingError("negative or null trade count")
        opened = opens[index]
        closed = closes[index]
        if opened.tzinfo is None or opened.utcoffset() != timedelta(0):
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
    _validate_frame(bars, list(slice_.source_files))
    return {"bars": bars, "source_paths": list(slice_.source_files)}


def _validate_frame(bars: list[Bar], source_files: list[str]) -> None:
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
        if bar.open_time.year != 2026:
            raise SpecialSwingError("candle is outside 2026")
        key = (bar.open_time.year, bar.open_time.month)
        monthly[key] = monthly.get(key, 0) + 1
        if bar.open_time in seen or (previous is not None and bar.open_time <= previous):
            raise SpecialSwingError("duplicate or unordered open time")
        seen.add(bar.open_time)
        if previous is not None and datetime_to_ms(bar.open_time) - datetime_to_ms(previous) != STEP_MS:
            raise SpecialSwingError(f"missing 4h timestamp at {iso_utc(bar.open_time)}")
        if datetime_to_ms(bar.close_time) != datetime_to_ms(bar.open_time) + STEP_MS - 1:
            raise SpecialSwingError(f"close time mismatch at {iso_utc(bar.open_time)}")
        if min(bar.volume, bar.quote_volume) < 0:
            raise SpecialSwingError("negative volume")
        if not (bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high):
            raise SpecialSwingError(f"OHLC violation at {iso_utc(bar.open_time)}")
        previous = bar.open_time
    if monthly != EXPECTED_MONTHLY:
        raise SpecialSwingError(f"monthly counts {monthly}")
    for path in source_files:
        lowered = path.replace("\\", "/").lower()
        if not lowered.startswith("c:/marketdata/derived/binance/futures/um/perpetual/4h/") or "year=2026" not in lowered:
            raise SpecialSwingError(f"unexpected source {path}")
        if "year=2024" in lowered or "year=2025" in lowered:
            raise SpecialSwingError(f"pre-2026 source accessed {path}")


def _changed(before: dict, after: dict) -> int:
    return sum(1 for key, value in before.items() if after.get(key) != value) + sum(1 for key in after if key not in before)


def _check_results(result: Analysis) -> None:
    counts = result.counters
    if counts["alternative"] or counts["compact"] or counts["non_standard"]:
        raise SpecialSwingError("non-standard result was produced")
    third = Decimal(1) / Decimal(3)
    if (
        not ZERO_EXCEPTION_ENABLED
        or not NORMAL_STANDARD_ENABLED
        or not ZERO_USES_FULL_RETURN
        or ZERO_USES_ONE_THIRD
        or NORMAL_USES_FULL_RETURN
        or not NORMAL_USES_ONE_THIRD
        or NORMAL_MIN_INTERIOR != 1
        or MAX_INTERIOR != 5
    ):
        raise SpecialSwingError("formation configuration is not zero full-return plus standard one-third body")
    if counts["wick_only_confirmations"]:
        raise SpecialSwingError("wick-only penetration was confirmed")
    if counts["maximum_boundary_rejection_count"]:
        raise SpecialSwingError("a maximum boundary rejection was emitted")
    if counts["normal_min_interior"] != 1 or counts["normal_max_interior"] != 5 or counts["maximum_search_interior"] != 5:
        raise SpecialSwingError("search configuration is not 1-5 with horizon 5")
    minimum = Decimal(MIN_BOUNDARY)
    for item in result.raw:
        if item.formation_class == ZERO_CLASS:
            if item.interior != 0 or item.total != 2 or item.close_row != item.open_row + 1:
                raise SpecialSwingError("confirmed zero-interior shape is invalid")
            if item.completion_rule != "FULL_OPEN_REFERENCE_RETURN" or not item.full_open_return:
                raise SpecialSwingError("zero-interior confirmation is not a full Open return")
            returned = item.close_price <= item.reference if item.direction == "SWING_HIGH" else item.close_price >= item.reference
            if not returned:
                raise SpecialSwingError("zero-interior close did not reach Swing Open Open")
        elif item.formation_class == STANDARD_CLASS:
            if not (1 <= item.interior <= 5 and 3 <= item.total <= 7):
                raise SpecialSwingError("confirmed normal Standard duration is invalid")
            if not (item.open_row < item.extreme_row < item.close_row):
                raise SpecialSwingError("normal Standard extreme is not strictly interior")
            if item.completion_rule != "ONE_THIRD_BODY_PENETRATION" or item.penetration_fraction < third:
                raise SpecialSwingError("normal Standard penetration is below one-third")
        else:
            raise SpecialSwingError("unknown formation class")
        if item.open_width_percent < minimum or item.close_width_percent < minimum:
            raise SpecialSwingError("confirmed boundary is below 1.30")
    if counts["displayed"]:
        if counts["max_interior"] > 5:
            raise SpecialSwingError("displayed interior exceeds 5")
        if counts["min_open_boundary"] < minimum or counts["min_close_boundary"] < minimum:
            raise SpecialSwingError("a displayed boundary is below 1.30")
    if counts["largest_search_interior"] > 5 or counts["horizon_errors"]:
        raise SpecialSwingError("HARD_SEARCH_HORIZON_ERROR")


def publish(result: Analysis) -> Path:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    revision, final_path = allocate_revision(DESKTOP_DIR)
    temporary = TMP_DIR / f"{final_path.stem}.partial.xlsx"
    write_workbook(temporary, result)
    validate_workbook(temporary, result)
    revision, final_path = allocate_revision(DESKTOP_DIR)
    if temporary.stem != final_path.stem:
        temporary = TMP_DIR / f"{final_path.stem}.partial.xlsx"
        write_workbook(temporary, result)
        validate_workbook(temporary, result)
    if final_path.exists():
        raise SpecialSwingError(f"revision appeared before publish: {final_path}")
    os.replace(temporary, final_path)
    validate_workbook(final_path, result)
    return final_path


def _jsonable(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def write_report(result: Analysis, context: dict) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    technical = [
        {
            "swing_id": item.swing_id,
            "formation_class": item.formation_class,
            "completion_rule": item.completion_rule,
            "completion_target": item.completion_target,
            "body_low": item.body_low,
            "body_high": item.body_high,
            "body_size": item.body_size,
            "body_threshold": item.body_threshold,
            "penetration_price": item.penetration_price,
            "penetration_fraction": item.penetration_fraction,
            "penetration_percent": item.penetration_percent,
            "full_open_return": item.full_open_return,
            "extreme_source": item.extreme_source,
        }
        for item in result.displayed
    ]
    payload = {
        "detector_version": DETECTOR_VERSION,
        "configuration_version": CONFIGURATION_VERSION,
        "counters": _jsonable(result.counters),
        "displayed_body_penetration": _jsonable(technical),
        "context": context,
    }
    REPORT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger = logging.getLogger("special_4h_swing")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    logger.addHandler(handler)
    logger.info("detector %s", DETECTOR_VERSION)
    logger.info("raw %s displayed %s", result.counters["raw"], result.counters["displayed"])
    logger.info("workbook %s", context.get("workbook"))
    handler.close()


def _preview(result: Analysis, items=None) -> list[str]:
    lines = []
    chosen = result.displayed[:5] if items is None else items
    for item in chosen:
        opened = result.bars[item.open_row]
        closed = result.bars[item.close_row]
        extreme = result.bars[item.extreme_row]
        lines.append(
            " | ".join((
                item.swing_id,
                item.direction,
                item.formation_class,
                str(item.interior),
                str(item.total),
                turkey_text_safe(opened.open_time),
                str(opened.open),
                str(opened.high),
                str(opened.low),
                turkey_text_safe(closed.open_time),
                turkey_text_safe(closed.close_time),
                str(item.close_price),
                str(closed.high),
                str(closed.low),
                turkey_text_safe(extreme.open_time),
                "HIGH" if item.direction == "SWING_HIGH" else "LOW",
                str(item.extreme_price),
                str(item.structure_high),
                str(item.structure_low),
                str(item.open_width_percent),
                str(item.close_width_percent),
            ))
        )
    return lines


def turkey_text_safe(moment):
    from detectors.swing_open_close_4h_special_v1.engine import turkey_text
    return turkey_text(moment)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-run-1", default="")
    parser.add_argument("--test-run-2", default="")
    args = parser.parse_args()
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    protected_before = fingerprint_tree(PROTECTED_ROOT)
    mapping_before = {str(root): fingerprint_tree(root) for root in MAPPING_ROOTS}
    loaded = load_production()
    source_before = fingerprint_files(loaded["source_paths"])
    result = analyze(loaded["bars"])
    if signature(result) != signature(analyze(loaded["bars"])):
        raise SpecialSwingError("repeated execution changed the result")
    _check_results(result)
    final_path = publish(result)
    source_after = fingerprint_files(loaded["source_paths"])
    protected_after = fingerprint_tree(PROTECTED_ROOT)
    mapping_changed = sum(_changed(mapping_before[str(root)], fingerprint_tree(root)) for root in MAPPING_ROOTS)
    original_changed = _changed(protected_before, protected_after)
    source_changed = _changed(source_before, source_after)
    if original_changed or mapping_changed or source_changed:
        final_path.unlink(missing_ok=True)
        raise SpecialSwingError(
            f"immutability failed original={original_changed} mapping={mapping_changed} source={source_changed}"
        )
    context = {
        "workbook": str(final_path),
        "test_run_1": args.test_run_1,
        "test_run_2": args.test_run_2,
        "determinism": "IDENTICAL",
        "original_changed": original_changed,
        "source_changed": source_changed,
        "workbook_validation": "PASS",
    }
    write_report(result, context)
    print("WORKBOOK", final_path)
    print("REPORT", REPORT_PATH)
    print("LOG", LOG_PATH)
    print("ORIGINAL_4H_DETECTOR_CHANGED_FILE_COUNT", original_changed)
    print("ORIGINAL_4H_DETECTOR_IMMUTABILITY", "PASS" if original_changed == 0 else "FAIL")
    print("SOURCE_CHANGED", source_changed)
    print("MAPPING_CHANGED", mapping_changed)
    for key, value in result.counters.items():
        if key != "terminals":
            print(f"COUNT {key} {value}")
    for name, value in result.counters["terminals"].items():
        print(f"TERMINAL {name} {value}")
    for line in _preview(result):
        print("PREVIEW", line)
    zeros = [item for item in result.displayed if item.formation_class == ZERO_CLASS][:5]
    normals = [item for item in result.displayed if item.formation_class == STANDARD_CLASS][:5]
    for line in _preview(result, zeros):
        print("ZERO", line)
    for line in _preview(result, normals):
        print("STANDARD", line)
    partials = [item for item in result.displayed if not item.full_open_return]
    for line in _preview(result, [item for item in partials if item.formation_class == ZERO_CLASS][:5]):
        print("ZERO_PARTIAL", line)
    for line in _preview(result, [item for item in partials if item.formation_class == STANDARD_CLASS][:5]):
        print("STANDARD_PARTIAL", line)
    above = [
        item for item in result.displayed
        if item.open_width_percent > Decimal("3.50") or item.close_width_percent > Decimal("3.50")
    ][:5]
    for line in _preview(result, above):
        print("ABOVE_3_50", line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
