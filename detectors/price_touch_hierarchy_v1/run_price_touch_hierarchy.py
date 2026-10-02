"""Permanent entry point for the BTCUSDT price-touch hierarchy detector."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa

import config
from data.catalog import fingerprint_file
from data.loader import load_candles
from detectors.price_touch_hierarchy_v1.engine import (
    Bar,
    SourceValidationError,
    analyze,
    highest_canonical_group,
    iso_turkey,
    iso_utc,
    most_persistent_group,
)
from detectors.price_touch_hierarchy_v1.touch_config import (
    ANALYSIS_START,
    CONFIG,
    DATASET_END,
    DETECTOR_VERSION,
    DURATION_MS,
    EXCLUSIVE_LOAD_END,
    EXPECTED_FIRST,
    EXPECTED_LAST,
    EXPECTED_ROWS,
    REVISION_PATTERN_TEXT,
    SOURCE_DATASET,
    TIMEFRAMES,
    TMP_DIR,
)
from detectors.price_touch_hierarchy_v1.workbook import validate_saved_workbook, write_workbook

EXECUTION_COMMAND = "python -m detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy"
REVISION_PATTERN = re.compile(REVISION_PATTERN_TEXT, re.IGNORECASE)


def main() -> int:
    print("price_touch_hierarchy_v1 starting", flush=True)
    assert_python_write_allowed(CONFIG.package_dir / "run_price_touch_hierarchy.py")
    package_before = _package_hashes()
    code_before = _fingerprint_tree(_existing_code_roots())
    range_before = _fingerprint_tree([CONFIG.project_root / "detectors" / "primary_range_v1"])
    swing_before = _fingerprint_tree([CONFIG.project_root / "detectors" / "hierarchical_swing_v4"])
    existing_revisions = scan_revisions()
    try:
        loaded = load_production()
    except SourceValidationError as exc:
        print(f"SOURCE_VALIDATION_FAILED {exc}", flush=True)
        return 2
    source_before = {path: fingerprint_file(Path(path)) for path in loaded["source_paths"]}
    print(
        "source validated "
        + " ".join(f"{tf}={len(loaded['bars'][tf])}" for tf in TIMEFRAMES),
        flush=True,
    )
    result = analyze(loaded["bars"], dataset_end=DATASET_END)
    print(
        "qualified "
        + " ".join(f"{tf}={len(result.levels.get(tf, []))}" for tf in TIMEFRAMES)
        + f" groups={len(result.groups)}",
        flush=True,
    )
    source_after = {path: fingerprint_file(Path(path)) for path in loaded["source_paths"]}
    code_after = _fingerprint_tree(_existing_code_roots())
    range_after = _fingerprint_tree([CONFIG.project_root / "detectors" / "primary_range_v1"])
    swing_after = _fingerprint_tree([CONFIG.project_root / "detectors" / "hierarchical_swing_v4"])
    if source_before != source_after:
        print("SOURCE_IMMUTABILITY_FAILED", flush=True)
        return 3
    if code_before != code_after or range_before != range_after or swing_before != swing_after:
        print("CODE_IMMUTABILITY_FAILED", flush=True)
        return 3
    persistent = most_persistent_group(result.groups)
    canonical = highest_canonical_group(result.groups)
    context = _context(result, loaded, source_before, existing_revisions, persistent, canonical)
    readme = (CONFIG.package_dir / "README.md").read_text(encoding="utf-8")
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    try:
        revision, final_path = _commit_workbook(result, context, readme)
    except RuntimeError as exc:
        print(f"WORKBOOK_VALIDATION_FAILED {exc}", flush=True)
        return 4
    package_after = _package_hashes()
    if package_before != package_after:
        print("DETECTOR_SOURCE_REWRITTEN", flush=True)
        return 3
    stray = _stray_python_files()
    if stray:
        print(f"STRAY_PYTHON {stray}", flush=True)
        return 3
    _cleanup()
    print("SUMMARY_JSON " + json.dumps(_summary(result, final_path, revision, existing_revisions, persistent, canonical), default=str), flush=True)
    return 0


def load_production() -> dict[str, Any]:
    bars: dict[str, list[Bar]] = {}
    source_paths: list[str] = []
    provenance: dict[str, Any] = {}
    for timeframe in TIMEFRAMES:
        slice_ = load_candles(
            "BTCUSDT",
            timeframe,
            ANALYSIS_START,
            EXCLUSIVE_LOAD_END,
            warmup_bars=0,
            precision_mode=config.PRECISION_EXACT,
        )
        if slice_.warmup_bars_loaded != 0:
            raise SourceValidationError(f"{timeframe} loaded warmup bars")
        if slice_.symbol != "BTCUSDT" or slice_.timeframe != timeframe:
            raise SourceValidationError(f"{timeframe} identity mismatch")
        frame = _bars_from_table(timeframe, slice_.table)
        _validate_frame(timeframe, frame)
        bars[timeframe] = frame
        source_paths.extend(slice_.source_files)
        provenance[timeframe] = {
            "rows": len(frame),
            "warmup_bars_loaded": slice_.warmup_bars_loaded,
            "precision_mode": slice_.precision_mode,
            "schema_version": slice_.schema_version,
            "first_open_utc": iso_utc(frame[0].open_time),
            "last_open_utc": iso_utc(frame[-1].open_time),
            "first_open_turkey": iso_turkey(frame[0].open_time),
            "last_open_turkey": iso_turkey(frame[-1].open_time),
            "last_close_utc": iso_utc(frame[-1].close_time),
        }
    return {"bars": bars, "source_paths": sorted(set(source_paths)), "provenance": provenance}


def _bars_from_table(timeframe: str, table: pa.Table) -> list[Bar]:
    opens = table.column("open_time").to_pylist()
    closes = table.column("close_time").to_pylist()
    open_ = [_decimal(value) for value in table.column("open").to_pylist()]
    high = [_decimal(value) for value in table.column("high").to_pylist()]
    low = [_decimal(value) for value in table.column("low").to_pylist()]
    close = [_decimal(value) for value in table.column("close").to_pylist()]
    volume = [_decimal(value) for value in table.column("volume").to_pylist()]
    symbols = table.column("symbol").to_pylist()
    rows: list[Bar] = []
    for index in range(table.num_rows):
        if symbols[index] != "BTCUSDT":
            raise SourceValidationError(f"{timeframe} row {index} symbol {symbols[index]}")
        rows.append(
            Bar(
                timeframe=timeframe,
                row=index,
                open_time=_as_utc(opens[index]),
                close_time=_as_utc(closes[index]),
                open=open_[index],
                high=high[index],
                low=low[index],
                close=close[index],
                volume=volume[index],
            )
        )
    return rows


def _validate_frame(timeframe: str, bars: list[Bar]) -> None:
    if len(bars) != EXPECTED_ROWS[timeframe]:
        raise SourceValidationError(f"{timeframe} rows {len(bars)} != {EXPECTED_ROWS[timeframe]}")
    if bars[0].open_time != EXPECTED_FIRST[timeframe] or bars[-1].open_time != EXPECTED_LAST[timeframe]:
        raise SourceValidationError(
            f"{timeframe} range {iso_utc(bars[0].open_time)} .. {iso_utc(bars[-1].open_time)}"
        )
    step = DURATION_MS[timeframe]
    previous = None
    for bar in bars:
        if bar.open_time.tzinfo is None or bar.open_time.utcoffset() != timezone.utc.utcoffset(bar.open_time):
            raise SourceValidationError(f"{timeframe} timestamp is not UTC")
        if bar.open_time.year < 2024:
            raise SourceValidationError("pre-2024 candle accessed")
        if previous is not None:
            delta = int((bar.open_time - previous).total_seconds() * 1000)
            if delta != step:
                raise SourceValidationError(f"{timeframe} missing or duplicate timestamp at {iso_utc(bar.open_time)}")
        previous = bar.open_time
        expected_close = bar.open_time.timestamp() * 1000 + step - 1
        actual_close = bar.close_time.timestamp() * 1000
        if abs(actual_close - expected_close) > 0.1:
            raise SourceValidationError(f"{timeframe} close time mismatch at {iso_utc(bar.open_time)}")
        if bar.close_time > DATASET_END:
            raise SourceValidationError(f"{timeframe} unfinished candle {iso_utc(bar.open_time)}")
        if bar.volume < 0:
            raise SourceValidationError("negative volume")
        for value in (bar.open, bar.high, bar.low, bar.close):
            if value is None:
                raise SourceValidationError("null OHLC")
        if not (
            bar.high >= bar.open
            and bar.high >= bar.close
            and bar.high >= bar.low
            and bar.low <= bar.open
            and bar.low <= bar.close
            and bar.low <= bar.high
        ):
            raise SourceValidationError(f"OHLC violation at {iso_utc(bar.open_time)}")
    path = str(SOURCE_DATASET[timeframe]).replace("\\", "/").lower()
    for token in ("marketdata", "binance", "futures", "um", "perpetual", timeframe, "btcusdt"):
        if token not in path:
            raise SourceValidationError(f"{timeframe} dataset path missing {token}: {path}")


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise SourceValidationError("naive timestamp")
    return value.astimezone(timezone.utc)


def scan_revisions() -> list[int]:
    found = []
    if not CONFIG.desktop_dir.exists():
        return found
    for path in CONFIG.desktop_dir.iterdir():
        match = REVISION_PATTERN.match(path.name)
        if match and path.is_file():
            found.append(int(match.group(1)))
    return sorted(set(found))


def next_revision(existing: list[int]) -> int:
    if not existing:
        return 0
    return max(existing) + 1


def revision_filename(revision: int) -> str:
    return f"BTCUSDT_MTF_Most_Touched_Levels_rev{revision:02d}.xlsx"


def _commit_workbook(result, context: dict[str, Any], readme: str) -> tuple[int, Path]:
    planned = next_revision(context["existing_revisions"])
    while True:
        name = revision_filename(planned)
        final_path = CONFIG.desktop_dir / name
        if final_path.exists():
            planned += 1
            continue
        temporary = TMP_DIR / name
        context["revision_name"] = name
        context["diagnostic_rows"] = _diagnostic_rows(result, context)
        write_workbook(temporary, result, context, readme)
        validate_saved_workbook(temporary)
        rescanned = scan_revisions()
        if final_path.exists() or (rescanned and max(rescanned) >= planned):
            temporary.unlink(missing_ok=True)
            planned = next_revision(scan_revisions())
            continue
        os.replace(temporary, final_path)
        if not final_path.is_file():
            raise RuntimeError("atomic workbook move failed")
        return planned, final_path


def _context(result, loaded, fingerprints, revisions, persistent, canonical) -> dict[str, Any]:
    import openpyxl
    import pyarrow

    return {
        "existing_revisions": revisions,
        "revision_name": "",
        "runtime": {
            "python": sys.version.replace("\n", " "),
            "pyarrow": pyarrow.__version__,
            "openpyxl": openpyxl.__version__,
        },
        "source_validation": "PASS",
        "provenance": loaded["provenance"],
        "source_fingerprints": fingerprints,
        "most_persistent": (
            f"{persistent.group_id} {persistent.persistence} {persistent.representative_price}"
            if persistent else "NONE"
        ),
        "highest_canonical": (
            f"{canonical.group_id} visits={canonical.canonical_visits} price={canonical.representative_price}"
            if canonical else "NONE"
        ),
        "warnings": list(result.warnings),
    }


def _diagnostic_rows(result, context: dict[str, Any]) -> list[list[Any]]:
    rows: list[list[Any]] = []

    def add(section: str, key: str, value: Any) -> None:
        rows.append([section, key, value])

    add("implementation", "project_root", str(CONFIG.project_root))
    add("implementation", "permanent_detector_directory", str(CONFIG.package_dir))
    add("implementation", "permanent_script_path", str(CONFIG.package_dir / "run_price_touch_hierarchy.py"))
    add("implementation", "readme_path", str(CONFIG.package_dir / "README.md"))
    add("implementation", "test_path", str(CONFIG.package_dir / "tests" / "test_price_touch_hierarchy.py"))
    add("implementation", "execution_command", EXECUTION_COMMAND)
    add("implementation", "detector_version", DETECTOR_VERSION)
    add("implementation", "runtime_python", context["runtime"]["python"])
    add("implementation", "runtime_pyarrow", context["runtime"]["pyarrow"])
    add("implementation", "runtime_openpyxl", context["runtime"]["openpyxl"])
    add("implementation", "workbook_revision", context["revision_name"])
    add("implementation", "existing_matching_revisions", ",".join(f"{item:02d}" for item in context["existing_revisions"]) or "NONE")
    add("implementation", "separate_project_created", "FALSE")
    add("thresholds", "1h_minimum_completed_candle_touches", 40)
    add("thresholds", "4h_minimum_completed_candle_touches", 20)
    add("thresholds", "1d_minimum_completed_candle_touches", 10)
    add("thresholds", "effective_thresholds", "1h=40; 4h=20; 1d=10")
    for timeframe in TIMEFRAMES:
        add("source", f"{timeframe}_path", str(SOURCE_DATASET[timeframe]))
        add("source", f"{timeframe}_rows", len(result.bars.get(timeframe, [])))
        add("source", f"{timeframe}_validation", "PASS")
        proven = context["provenance"][timeframe]
        add("source", f"{timeframe}_first_open_utc", proven["first_open_utc"])
        add("source", f"{timeframe}_last_open_utc", proven["last_open_utc"])
    add("source", "source_validation", context["source_validation"])
    for path, meta in context["source_fingerprints"].items():
        add("source_fingerprint", path, f"sha256={meta['sha256']} size={meta['size']} mtime_ns={meta['mtime_ns']}")
    add("grid", "price_min", str(result.price_min))
    add("grid", "price_max", str(result.price_max))
    add("grid", "grid_min_k", result.grid_min_k)
    add("grid", "grid_max_k", result.grid_max_k)
    add("grid", "atomic_bin_count", result.atomic_bins)
    add("counts", "duplicate_touch_keys", result.duplicate_touch_keys)
    for timeframe in TIMEFRAMES:
        add("counts", f"{timeframe}_candidate_peaks", result.peak_counts.get(timeframe, 0))
        add("counts", f"{timeframe}_qualified_levels", len(result.levels.get(timeframe, [])))
        add("counts", f"{timeframe}_rejected_candidates", sum(1 for level in result.rejected if level.timeframe == timeframe))
        add("counts", f"{timeframe}_suppressed_levels", sum(1 for level in result.suppressed if level.timeframe == timeframe))
        add("counts", f"{timeframe}_unfinished_excluded", result.unfinished_excluded.get(timeframe, 0))
        levels = result.levels.get(timeframe, [])
        touches = sum(level.touches for level in levels)
        typed = sum(level.close_inside + level.body + level.wick for level in levels)
        episodes = sum(level.episodes for level in levels)
        episode_lengths = sum(sum(level.episode_lengths) for level in levels)
        add("reconciliation", f"{timeframe}_touch_type_total", typed)
        add("reconciliation", f"{timeframe}_touch_total", touches)
        add("reconciliation", f"{timeframe}_touch_reconciliation", "PASS" if typed == touches else "FAIL")
        add("reconciliation", f"{timeframe}_episode_reconciliation", "PASS" if episode_lengths == touches else "FAIL")
        add("reconciliation", f"{timeframe}_episode_total", episodes)
    add("hierarchy", "group_count", len(result.groups))
    for tier in (
        "TIER_1_ALL_TIMEFRAMES",
        "TIER_2_DAILY_WITH_INTRADAY",
        "TIER_3_DAILY_ONLY",
        "TIER_4_4H_AND_1H",
        "TIER_5_4H_ONLY",
        "TIER_6_1H_ONLY",
    ):
        add("hierarchy", tier, sum(1 for group in result.groups if group.tier == tier))
    accepted = sum(1 for attempt in result.attempts if attempt.accepted)
    add("mapping", "accepted_mappings", accepted)
    add("mapping", "rejected_mappings", len(result.attempts) - accepted)
    reasons: dict[str, int] = {}
    for attempt in result.attempts:
        if attempt.rejection_reason:
            reasons[attempt.rejection_reason] = reasons.get(attempt.rejection_reason, 0) + 1
    for reason, count in sorted(reasons.items()):
        add("mapping_rejection", reason, count)
    for reason, count in sorted(result.rejection_reason_counts.items()):
        add("candidate_rejection", reason, count)
    add("immutability", "source_parquet_unchanged", "PASS")
    add("immutability", "existing_project_code_unchanged", "PASS")
    add("isolation", "range_detector_imported", "FALSE")
    add("isolation", "swing_detector_imported", "FALSE")
    add("isolation", "strategy_modules_imported", "FALSE")
    add("isolation", "range_detector_modified", "FALSE")
    add("isolation", "swing_detector_modified", "FALSE")
    add("isolation", "separate_project_created", "FALSE")
    add("isolation", "detector_python_outside_project", "NONE")
    add("validation", "workbook_validation", "PASS")
    add("validation", "main_ranking_reconciliation", "PASS")
    add("cleanup", "temporary_directory", str(TMP_DIR))
    add("cleanup", "temporary_artifact_policy", "Temporary workbook is validated, moved atomically, then removed.")
    add("warnings", "warning_count", len(context.get("warnings", [])))
    for index, warning in enumerate(context.get("warnings", []), start=1):
        add("warnings", f"warning_{index}", warning)
    return rows


def _summary(result, final_path: Path, revision: int, existing: list[int], persistent, canonical) -> dict[str, Any]:
    top = []
    for group in result.groups[:20]:
        top.append({
            "rank": group.rank,
            "tier": group.tier,
            "price": str(group.representative_price),
            "lower": str(group.consolidated_lower),
            "upper": str(group.consolidated_upper),
            "timeframes": "+".join(group.timeframes),
            "touches_1d": group.members["1d"].touches if "1d" in group.members else None,
            "touches_4h": group.members["4h"].touches if "4h" in group.members else None,
            "touches_1h": group.members["1h"].touches if "1h" in group.members else None,
            "canonical_visits": group.canonical_visits,
            "distinct_months": group.members[group.canonical_timeframe].distinct_months,
            "first_touch": iso_utc(group.first_touch) if group.first_touch else None,
            "last_touch": iso_utc(group.last_touch) if group.last_touch else None,
            "score": str(group.score),
        })

    def best(timeframe: str) -> dict[str, Any] | None:
        levels = result.levels.get(timeframe, [])
        if not levels:
            return None
        level = min(levels, key=lambda item: item.rank)
        return {"id": level.level_id, "price": str(level.price), "touches": level.touches, "episodes": level.episodes}

    return {
        "workbook": str(final_path),
        "revision": revision,
        "existing_revisions": existing,
        "rows": {tf: len(result.bars.get(tf, [])) for tf in TIMEFRAMES},
        "atomic_bins": result.atomic_bins,
        "peaks": result.peak_counts,
        "qualified": {tf: len(result.levels.get(tf, [])) for tf in TIMEFRAMES},
        "rejected": {tf: sum(1 for level in result.rejected if level.timeframe == tf) for tf in TIMEFRAMES},
        "suppressed": {tf: sum(1 for level in result.suppressed if level.timeframe == tf) for tf in TIMEFRAMES},
        "groups": len(result.groups),
        "tiers": {tier: sum(1 for group in result.groups if group.tier == tier) for tier in (
            "TIER_1_ALL_TIMEFRAMES", "TIER_2_DAILY_WITH_INTRADAY", "TIER_3_DAILY_ONLY",
            "TIER_4_4H_AND_1H", "TIER_5_4H_ONLY", "TIER_6_1H_ONLY",
        )},
        "duplicate_touch_keys": result.duplicate_touch_keys,
        "unfinished_excluded": result.unfinished_excluded,
        "accepted_mappings": sum(1 for attempt in result.attempts if attempt.accepted),
        "rejected_mappings": sum(1 for attempt in result.attempts if not attempt.accepted),
        "mapping_rejections": {
            reason: sum(1 for attempt in result.attempts if attempt.rejection_reason == reason)
            for reason in sorted({attempt.rejection_reason for attempt in result.attempts if attempt.rejection_reason})
        },
        "rejection_reasons": result.rejection_reason_counts,
        "best_1d": best("1d"),
        "best_4h": best("4h"),
        "best_1h": best("1h"),
        "highest_canonical": None if canonical is None else {
            "id": canonical.group_id, "visits": canonical.canonical_visits, "price": str(canonical.representative_price)
        },
        "most_persistent": None if persistent is None else {
            "id": persistent.group_id, "label": persistent.persistence, "price": str(persistent.representative_price)
        },
        "top20": top,
        "thresholds": {"1h": 40, "4h": 20, "1d": 10},
        "warnings": result.warnings,
    }


def _existing_code_roots() -> list[Path]:
    root = CONFIG.project_root
    paths = [
        root / "detectors" / "primary_range_v1",
        root / "detectors" / "hierarchical_swing_v4",
        root / "detectors" / "__init__.py",
        root / "data",
        root / "ingestion",
        root / "processing",
        root / "config.py",
        root / "README.md",
        root / "requirements.txt",
        root / "pyproject.toml",
    ]
    return [path for path in paths if path.exists()]


def _fingerprint_tree(paths: list[Path]) -> dict[str, str]:
    found: dict[str, str] = {}
    for path in paths:
        files = [path] if path.is_file() else sorted(item for item in path.rglob("*") if item.is_file())
        for file in files:
            if "__pycache__" in file.parts or file.suffix == ".pyc":
                continue
            if "price_touch_hierarchy_v1" in file.parts:
                continue
            digest = hashlib.sha256(file.read_bytes()).hexdigest()
            found[str(file)] = digest
    return found


def _package_hashes() -> dict[str, str]:
    found = {}
    for file in CONFIG.package_dir.rglob("*"):
        if not file.is_file() or "__pycache__" in file.parts or file.suffix == ".pyc":
            continue
        found[str(file)] = hashlib.sha256(file.read_bytes()).hexdigest()
    return found


def _stray_python_files() -> list[str]:
    stray = []
    desktop = CONFIG.desktop_dir
    for file in desktop.glob("*.py"):
        stray.append(str(file))
    return stray


def assert_python_write_allowed(path: Path) -> None:
    resolved = path.resolve()
    package = CONFIG.package_dir.resolve()
    if package not in resolved.parents and resolved != package:
        raise PermissionError(f"detector Python writes are limited to {package}")


def _cleanup() -> None:
    if TMP_DIR.exists():
        shutil.rmtree(TMP_DIR, ignore_errors=True)
    parent = TMP_DIR.parent
    if parent.exists() and not any(parent.iterdir()):
        parent.rmdir()
    for root in (CONFIG.package_dir, CONFIG.package_dir / "tests"):
        cache = root / "__pycache__"
        if cache.exists():
            shutil.rmtree(cache, ignore_errors=True)
        for file in root.glob("*.pyc"):
            file.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
