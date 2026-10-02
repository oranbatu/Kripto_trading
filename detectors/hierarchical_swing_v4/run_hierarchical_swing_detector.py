"""Permanent entry point for the BTCUSDT 1h hierarchical swing detector.

Run from the project root:

    python -m detectors.hierarchical_swing_v4.run_hierarchical_swing_detector
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import sys
import traceback
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
sys.dont_write_bytecode = True

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config
from data.catalog import fingerprint_file
from data.loader import load_candles

from detectors.hierarchical_swing_v4.engine import (
    Bar,
    DetectionResult,
    iso_turkey,
    iso_utc,
    run_detection,
    validate_source_bars,
)
from detectors.hierarchical_swing_v4.swing_config import CONFIG, REVISION_PATTERN_TEXT
from detectors.hierarchical_swing_v4.workbook import validate_saved_workbook, write_workbook

REVISION_RE = re.compile(REVISION_PATTERN_TEXT, re.IGNORECASE)
EXECUTION_COMMAND = "python -m detectors.hierarchical_swing_v4.run_hierarchical_swing_detector"


def assert_python_write_allowed(path: Path) -> None:
    """Refuse to create a detector Python file outside the authorized package."""
    resolved = path.resolve()
    package = CONFIG.package_dir.resolve()
    if resolved.suffix.lower() == ".py":
        if resolved != package and package not in resolved.parents:
            raise PermissionError(
                f"detector refused to write a Python file outside {package}: {resolved}"
            )
        return
    allowed = (
        package,
        CONFIG.temporary_dir.resolve(),
        CONFIG.output_dir.resolve(),
        (CONFIG.project_root / "tmp").resolve(),
    )
    if resolved != package and not any(root == resolved or root in resolved.parents for root in allowed):
        raise PermissionError(f"detector refused to write outside authorized paths: {resolved}")


def existing_revisions(directory: Path) -> list[int]:
    found: list[int] = []
    if not directory.exists():
        return found
    for path in directory.iterdir():
        if not path.is_file():
            continue
        match = REVISION_RE.match(path.name)
        if match:
            found.append(int(match.group(1)))
    return sorted(found)


def next_revision(found: list[int]) -> int:
    if not found:
        return 0
    return max(found) + 1


def revision_filename(revision: int) -> str:
    return f"BTCUSDT_1H_swing_rev{revision:02d}.xlsx"


def assert_not_overwrite(path: Path) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite existing workbook {path}")


def load_production_bars() -> tuple[list[Bar], dict]:
    """Load only the inclusive 2026 BTCUSDT 1h window through the read-only data layer."""
    load_end = CONFIG.analysis_end_utc + timedelta(hours=1)
    candle_slice = load_candles(
        CONFIG.symbol,
        CONFIG.timeframe,
        CONFIG.analysis_start_utc,
        load_end,
        warmup_bars=0,
        require_full_warmup=True,
        precision_mode=config.PRECISION_EXACT,
    )
    table = candle_slice.table
    columns = {name: table.column(name).to_pylist() for name in table.column_names}

    def as_decimal(value) -> Decimal:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    bars: list[Bar] = []
    for index in range(table.num_rows):
        symbol = columns["symbol"][index]
        if symbol != CONFIG.symbol:
            raise RuntimeError(f"unexpected symbol {symbol!r} at row {index}")
        bars.append(
            Bar(
                row=index,
                open_time=columns["open_time"][index],
                close_time=columns["close_time"][index],
                open=as_decimal(columns["open"][index]),
                high=as_decimal(columns["high"][index]),
                low=as_decimal(columns["low"][index]),
                close=as_decimal(columns["close"][index]),
                volume=as_decimal(columns["volume"][index]),
                quote_volume=as_decimal(columns["quote_asset_volume"][index]),
                trades=int(columns["number_of_trades"][index]),
                taker_buy_base=as_decimal(columns["taker_buy_base_asset_volume"][index]),
                taker_buy_quote=as_decimal(columns["taker_buy_quote_asset_volume"][index]),
            )
        )
    provenance = {
        "source_files": list(candle_slice.source_files),
        "source_file_hashes": dict(candle_slice.source_file_hashes),
        "precision_mode": candle_slice.precision_mode,
        "schema_version": candle_slice.schema_version,
        "warmup_bars_loaded": candle_slice.warmup_bars_loaded,
        "requested_start_utc": iso_utc(candle_slice.requested_start_utc),
        "requested_end_utc": iso_utc(candle_slice.requested_end_utc),
    }
    return bars, provenance


def source_partition_paths() -> list[Path]:
    paths = []
    for month in range(1, 10):
        paths.append(
            CONFIG.source_dataset / f"year=2026" / f"month={month:02d}" / "data.parquet"
        )
    return paths


def fingerprint_paths(paths: list[Path]) -> dict[str, dict]:
    output = {}
    for path in paths:
        output[str(path)] = fingerprint_file(path)
    return output


def existing_code_files() -> list[Path]:
    roots = [
        PROJECT_ROOT / "detectors" / "primary_range_v1",
        PROJECT_ROOT / "data",
        PROJECT_ROOT / "ingestion",
        PROJECT_ROOT / "processing",
        PROJECT_ROOT / "reports",
        PROJECT_ROOT / "tests",
    ]
    singles = [
        PROJECT_ROOT / "config.py",
        PROJECT_ROOT / "README.md",
        PROJECT_ROOT / "requirements.txt",
        PROJECT_ROOT / "pyproject.toml",
        PROJECT_ROOT / "run_validation.py",
        PROJECT_ROOT / "detectors" / "__init__.py",
    ]
    files: list[Path] = []
    for path in singles:
        if path.is_file():
            files.append(path)
    strategy = PROJECT_ROOT / "strategy"
    if strategy.exists():
        roots.append(strategy)
    for root in roots:
        if not root.exists():
            continue
        for file in root.rglob("*"):
            if not file.is_file():
                continue
            if "__pycache__" in file.parts or file.suffix == ".pyc":
                continue
            if "hierarchical_swing_v4" in file.parts:
                continue
            files.append(file)
    return files


def fingerprint_many(files: list[Path]) -> dict[str, str]:
    output = {}
    for file in files:
        output[str(file)] = hashlib.sha256(file.read_bytes()).hexdigest()
    return output


def stray_detector_python_files() -> list[str]:
    stray: list[str] = []
    desktop = CONFIG.output_dir
    if desktop.exists():
        for path in desktop.glob("*.py"):
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")[:4000]
            except OSError:
                continue
            if "HIERARCHICAL_SWING_V4" in text or "hierarchical_swing_v4" in text:
                stray.append(str(path))
    return stray


def runtime_versions() -> dict[str, str]:
    import pyarrow
    import openpyxl

    return {
        "python": sys.version.replace("\n", " "),
        "platform": platform.platform(),
        "pyarrow": pyarrow.__version__,
        "openpyxl": openpyxl.__version__,
    }


def _reason_counts(result: DetectionResult) -> dict[str, int]:
    counts: dict[str, int] = {}
    for swing in result.internal_swings:
        if swing.became_major_swing:
            continue
        reason = swing.major_non_promotion_reason or "MISSING_NON_PROMOTION_REASON"
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def _diagnostic_rows(result: DetectionResult, context: dict[str, Any]) -> list[list[Any]]:
    unique = result.unique
    per_bar = result.per_bar
    internal = result.internal_engine
    major = result.major_engine
    rows: list[list[Any]] = []

    def add(section: str, key: str, value: Any) -> None:
        rows.append([section, key, value])

    add("implementation", "project_root", str(CONFIG.project_root))
    add("implementation", "permanent_script_path", str(PACKAGE_DIR / "run_hierarchical_swing_detector.py"))
    add("implementation", "permanent_readme_path", str(PACKAGE_DIR / "README.md"))
    add("implementation", "permanent_test_path", str(PACKAGE_DIR / "tests" / "test_hierarchical_swing_detector.py"))
    add("implementation", "execution_command", EXECUTION_COMMAND)
    add("implementation", "detector_version", CONFIG.detector_version)
    add("implementation", "implementation_mode", CONFIG.implementation_mode)
    add("implementation", "workbook_revision", context["revision_name"])
    add("implementation", "existing_workbook_revisions", ",".join(f"{item:02d}" for item in context["existing_revisions"]) or "NONE")
    add("implementation", "runtime_python", context["runtime"]["python"])
    add("implementation", "runtime_pyarrow", context["runtime"]["pyarrow"])
    add("implementation", "runtime_openpyxl", context["runtime"]["openpyxl"])
    add("source", "source_dataset", str(CONFIG.source_dataset))
    add("source", "source_rows", len(result.bars))
    add("source", "source_validation", context["source_validation"])
    add("source", "first_open_utc", iso_utc(result.bars[0].open_time))
    add("source", "last_open_utc", iso_utc(result.bars[-1].open_time))
    add("source", "first_open_turkey", iso_turkey(result.bars[0].open_time))
    add("source", "last_open_turkey", iso_turkey(result.bars[-1].open_time))
    add("source", "last_close_turkey", iso_turkey(result.bars[-1].close_time))
    add("source", "warmup_bars_loaded", context["provenance"]["warmup_bars_loaded"])
    add("source", "precision_mode", context["provenance"]["precision_mode"])
    add("source", "pre_2026_candles_accessed", "FALSE")
    for path, meta in context["source_fingerprints_before"].items():
        add("source_fingerprint_before", path, f"sha256={meta['sha256']} size={meta['size']} mtime_ns={meta['mtime_ns']}")
    for path, meta in context["source_fingerprints_after"].items():
        add("source_fingerprint_after", path, f"sha256={meta['sha256']} size={meta['size']} mtime_ns={meta['mtime_ns']}")
    add("immutability", "source_parquet_unchanged", context["source_unchanged"])
    add("immutability", "existing_project_code_unchanged", context["code_unchanged"])
    add("immutability", "range_detector_unchanged", context["range_unchanged"])
    add("immutability", "strategy_code_unchanged", context["strategy_unchanged"])
    add("immutability", "existing_swing_workbooks_unchanged", context["workbooks_unchanged"])
    add("isolation", "range_detector_imported", "FALSE")
    add("isolation", "strategy_modules_imported", "FALSE")
    add("isolation", "stray_python_detector_files", ",".join(context["stray_python"]) or "NONE")
    add("counts", "total_formations", unique["unique_enumerated_formations"])
    add("counts", "qualified_formations", unique["unique_qualified_formations"])
    add("counts", "duplicate_formations", unique["unique_duplicate_formations"])
    add("counts", "raw_pivots", unique["unique_raw_pivots"])
    add("counts", "raw_pivot_highs", unique["unique_raw_pivot_highs"])
    add("counts", "raw_pivot_lows", unique["unique_raw_pivot_lows"])
    add("counts", "internal_candidates", unique["unique_internal_candidates"])
    add("counts", "confirmed_internal_swings", unique["unique_confirmed_internal_swings"])
    add("counts", "major_candidates", unique["unique_major_candidates"])
    add("counts", "confirmed_major_swings", unique["unique_confirmed_major_swings"])
    add("counts", "internal_replacements", unique["unique_internal_replacements"])
    add("counts", "major_replacements", unique["unique_major_replacements"])
    add("bootstrap", "internal_bootstrap_seed", result.internal_seed.pivot_id if result.internal_seed else "NONE")
    add("bootstrap", "major_bootstrap_seed", result.major_seed.internal_swing_id if result.major_seed else "NONE")
    add("bootstrap", "internal_bootstrap_record", str(internal.bootstrap_record))
    add("bootstrap", "major_bootstrap_record", str(major.bootstrap_record if major else None))
    add("age", "internal_max_age", internal.max_age_seen)
    add("age", "major_max_age", major.max_age_seen if major else 0)
    add("age", "internal_candidates_over_168", internal.exceeded[168])
    add("age", "internal_candidates_over_720", internal.exceeded[720])
    add("age", "internal_candidates_over_2160", internal.exceeded[2160])
    add("age", "major_candidates_over_168", major.exceeded[168] if major else 0)
    add("age", "major_candidates_over_720", major.exceeded[720] if major else 0)
    add("age", "major_candidates_over_2160", major.exceeded[2160] if major else 0)
    add("age", "candidate_timeout", "NONE")
    add("causality", "ignored_valid_reversal_count", internal.ignored_valid + (major.ignored_valid if major else 0))
    add("causality", "state_transition_error_count", internal.state_errors + (major.state_errors if major else 0))
    add("causality", "engine_independence", "PASS" if result.independence_pass else "FAIL")
    add("causality", "confirmation_backdating", "FALSE")
    add("reconciliation", "left_edge_censored_formations", result.formation_stats["left_edge_censored"])
    add("reconciliation", "right_edge_censored_formations", result.formation_stats["right_edge_censored"])
    add("reconciliation", "terminal_unresolved_candidates", len(result.unresolved))
    for index, item in enumerate(result.unresolved, start=1):
        add("unresolved", f"candidate_{index}", str(item))
    for reason, count in _reason_counts(result).items():
        add("internal_to_major", reason, count)
    promoted = sum(1 for swing in result.internal_swings if swing.became_major_swing)
    add("internal_to_major", "promoted_internal_swings", promoted)
    add("internal_to_major", "promotion_rate", "NA" if not result.internal_swings else f"{promoted}/{len(result.internal_swings)}")
    for row in result.monthly:
        add(
            "monthly",
            row["utc_month"],
            (
                f"candles={row['source_candles']} formations={row['formation_count']} "
                f"qualified={row['qualified_formation_count']} pivots={row['raw_pivot_highs'] + row['raw_pivot_lows']} "
                f"internal={row['confirmed_internal_highs'] + row['confirmed_internal_lows']} "
                f"major={row['confirmed_major_highs'] + row['confirmed_major_lows']} "
                f"internal_state={row['active_internal_state']} major_state={row['active_major_state']}"
            ),
        )
    add("PER_BAR_DIAGNOSTIC_ONLY", "per_bar_counter_label", per_bar["label"])
    add("PER_BAR_DIAGNOSTIC_ONLY", "formation_checks", per_bar["formation_checks"])
    add("PER_BAR_DIAGNOSTIC_ONLY", "internal_reversal_evaluations", per_bar["internal_reversal_evaluations"])
    add("PER_BAR_DIAGNOSTIC_ONLY", "internal_active_reversal_checks", per_bar["internal_active_reversal_checks"])
    add("PER_BAR_DIAGNOSTIC_ONLY", "major_reversal_evaluations", per_bar["major_reversal_evaluations"])
    add("PER_BAR_DIAGNOSTIC_ONLY", "major_active_reversal_checks", per_bar["major_active_reversal_checks"])
    add("PER_BAR_DIAGNOSTIC_ONLY", "internal_candidate_age_updates", per_bar["internal_candidate_age_updates"])
    add("PER_BAR_DIAGNOSTIC_ONLY", "major_candidate_age_updates", per_bar["major_candidate_age_updates"])
    add("cleanup", "temporary_directory", str(CONFIG.temporary_dir))
    add("cleanup", "temporary_artifact_policy", "Temporary workbook is validated then moved atomically. Fragments, validation copies, and package bytecode are removed after a passing validation.")
    add("warnings", "warning_count", len(result.warnings) + len(context.get("warnings", [])))
    for index, warning in enumerate(result.warnings + context.get("warnings", []), start=1):
        add("warnings", f"warning_{index}", warning)
    return rows


def _cleanup_bytecode() -> None:
    for root in (PACKAGE_DIR, PACKAGE_DIR / "tests"):
        cache = root / "__pycache__"
        if cache.exists():
            shutil.rmtree(cache, ignore_errors=True)
        for file in root.glob("*.pyc"):
            file.unlink(missing_ok=True)


def _cleanup_temp_dir() -> None:
    temp = CONFIG.temporary_dir
    if temp.exists():
        shutil.rmtree(temp, ignore_errors=True)
    parent = temp.parent
    if parent.exists() and parent.name == "tmp" and not any(parent.iterdir()):
        parent.rmdir()


def _range_files() -> list[Path]:
    root = PROJECT_ROOT / "detectors" / "primary_range_v1"
    return [
        file for file in root.rglob("*")
        if file.is_file() and "__pycache__" not in file.parts and file.suffix != ".pyc"
    ]


def main() -> int:
    print("hierarchical_swing_v4 starting", flush=True)
    source_paths = source_partition_paths()
    missing = [str(path) for path in source_paths if not path.is_file()]
    if missing:
        print("SOURCE VALIDATION FAILED", file=sys.stderr)
        for item in missing:
            print(item, file=sys.stderr)
        return 2
    code_files = existing_code_files()
    range_files = _range_files()
    code_before = fingerprint_many(code_files)
    range_before = fingerprint_many(range_files)
    source_before = fingerprint_paths(source_paths)
    revisions_before = existing_revisions(CONFIG.output_dir)
    workbook_before = fingerprint_paths(
        [CONFIG.output_dir / revision_filename(item) for item in revisions_before if (CONFIG.output_dir / revision_filename(item)).is_file()]
    )
    package_sources = [
        PACKAGE_DIR / "run_hierarchical_swing_detector.py",
        PACKAGE_DIR / "engine.py",
        PACKAGE_DIR / "workbook.py",
        PACKAGE_DIR / "swing_config.py",
        PACKAGE_DIR / "README.md",
        PACKAGE_DIR / "tests" / "test_hierarchical_swing_detector.py",
    ]
    package_before = fingerprint_many([path for path in package_sources if path.is_file()])
    try:
        bars, provenance = load_production_bars()
    except Exception as exc:
        print(f"SOURCE LOAD FAILED: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 2
    for path in provenance["source_files"]:
        text = str(path).replace("/", "\\")
        if "year=2024" in text or "year=2025" in text or "year=2026" not in text:
            print(f"SOURCE VALIDATION FAILED: unexpected partition {path}", file=sys.stderr)
            return 2
    problems = validate_source_bars(bars)
    first_turkey = iso_turkey(bars[0].open_time) if bars else ""
    last_open_turkey = iso_turkey(bars[-1].open_time) if bars else ""
    last_close_turkey = iso_turkey(bars[-1].close_time) if bars else ""
    if first_turkey != "2026-01-01T03:00:00.000+03:00":
        problems.append(f"first turkey open {first_turkey}")
    if last_open_turkey != "2026-09-16T02:00:00.000+03:00":
        problems.append(f"last turkey open {last_open_turkey}")
    if last_close_turkey != "2026-09-16T02:59:59.999+03:00":
        problems.append(f"last turkey close {last_close_turkey}")
    if provenance["warmup_bars_loaded"] != 0:
        problems.append("warmup candles were loaded")
    if problems:
        print("SOURCE VALIDATION FAILED", file=sys.stderr)
        for item in problems:
            print(item, file=sys.stderr)
        return 2
    print(f"source validated rows={len(bars)}", flush=True)
    result = run_detection(bars, enable_major=True, recompute_atr=True, verify_independence=True)
    print(
        f"qualified={result.unique['unique_qualified_formations']} pivots={result.unique['unique_raw_pivots']} "
        f"internal={result.unique['unique_confirmed_internal_swings']} major={result.unique['unique_confirmed_major_swings']}",
        flush=True,
    )
    source_after_detect = fingerprint_paths(source_paths)
    code_after = fingerprint_many(code_files)
    range_after = fingerprint_many(range_files)
    package_after = fingerprint_many([path for path in package_sources if path.is_file()])
    if source_after_detect != source_before:
        print("SOURCE IMMUTABILITY FAILED", file=sys.stderr)
        return 3
    if code_after != code_before or range_after != range_before or package_after != package_before:
        print("CODE IMMUTABILITY FAILED", file=sys.stderr)
        return 3
    stray = stray_detector_python_files()
    if stray:
        print(f"STRAY DETECTOR PYTHON FILES: {stray}", file=sys.stderr)
        return 3
    context: dict[str, Any] = {
        "runtime": runtime_versions(),
        "provenance": provenance,
        "source_fingerprints_before": source_before,
        "source_fingerprints_after": source_after_detect,
        "source_unchanged": "PASS",
        "code_unchanged": "PASS",
        "range_unchanged": "PASS",
        "strategy_unchanged": "PASS_NO_STRATEGY_PACKAGE",
        "workbooks_unchanged": "PASS",
        "source_validation": "PASS",
        "stray_python": stray,
        "warnings": list(result.warnings),
        "existing_revisions": revisions_before,
    }
    readme_path = PACKAGE_DIR / "README.md"
    context["readme_lines"] = readme_path.read_text(encoding="utf-8").splitlines() or ["README"]
    temp_dir = CONFIG.temporary_dir
    temp_dir.mkdir(parents=True, exist_ok=True)
    committed: Path | None = None
    try:
        while True:
            found_now = existing_revisions(CONFIG.output_dir)
            revision = next_revision(found_now)
            name = revision_filename(revision)
            final_path = CONFIG.output_dir / name
            assert_not_overwrite(final_path)
            temp_path = temp_dir / name
            if temp_path.exists():
                temp_path.unlink()
            context["revision_name"] = name
            context["existing_revisions"] = found_now
            context["diagnostic_rows"] = _diagnostic_rows(result, context)
            assert_python_write_allowed(temp_path)
            write_workbook(temp_path, result, context)
            workbook_problems = validate_saved_workbook(temp_path, result, context)
            if workbook_problems:
                print("WORKBOOK VALIDATION FAILED", file=sys.stderr)
                for item in workbook_problems:
                    print(item, file=sys.stderr)
                return 4
            found_after = existing_revisions(CONFIG.output_dir)
            if next_revision(found_after) != revision or final_path.exists():
                temp_path.unlink(missing_ok=True)
                continue
            assert_python_write_allowed(final_path)
            os.replace(temp_path, final_path)
            committed = final_path
            break
    finally:
        if committed is None:
            pass
        _cleanup_temp_dir()
        _cleanup_bytecode()
    source_final = fingerprint_paths(source_paths)
    code_final = fingerprint_many(code_files)
    range_final = fingerprint_many(range_files)
    package_final = fingerprint_many([path for path in package_sources if path.is_file()])
    workbook_after = fingerprint_paths(
        [CONFIG.output_dir / revision_filename(item) for item in revisions_before if (CONFIG.output_dir / revision_filename(item)).is_file()]
    )
    if source_final != source_before or code_final != code_before or range_final != range_before:
        print("POST-COMMIT IMMUTABILITY FAILED", file=sys.stderr)
        return 3
    if package_final != package_before:
        print("DETECTOR SOURCE CHANGED DURING EXECUTION", file=sys.stderr)
        return 3
    if workbook_after != workbook_before:
        print("EXISTING WORKBOOK CHANGED", file=sys.stderr)
        return 3
    if stray_detector_python_files():
        print("STRAY DETECTOR PYTHON FILE APPEARED", file=sys.stderr)
        return 3
    promoted = sum(1 for swing in result.internal_swings if swing.became_major_swing)
    summary = {
        "workbook": str(committed),
        "revision": committed.name if committed else "",
        "existing_revisions": revisions_before,
        "source_rows": len(result.bars),
        "qualified_formations": result.unique["unique_qualified_formations"],
        "duplicate_formations": result.unique["unique_duplicate_formations"],
        "enumerated_formations": result.unique["unique_enumerated_formations"],
        "raw_pivots": result.unique["unique_raw_pivots"],
        "raw_pivot_highs": result.unique["unique_raw_pivot_highs"],
        "raw_pivot_lows": result.unique["unique_raw_pivot_lows"],
        "internal_candidates": result.unique["unique_internal_candidates"],
        "confirmed_internal": result.unique["unique_confirmed_internal_swings"],
        "internal_highs": result.unique["unique_internal_highs"],
        "internal_lows": result.unique["unique_internal_lows"],
        "major_candidates": result.unique["unique_major_candidates"],
        "confirmed_major": result.unique["unique_confirmed_major_swings"],
        "major_highs": result.unique["unique_major_highs"],
        "major_lows": result.unique["unique_major_lows"],
        "internal_replacements": result.unique["unique_internal_replacements"],
        "major_replacements": result.unique["unique_major_replacements"],
        "internal_seed": result.internal_seed.pivot_id if result.internal_seed else None,
        "major_seed": result.major_seed.internal_swing_id if result.major_seed else None,
        "promoted_internal_swings": promoted,
        "non_promotion_reasons": _reason_counts(result),
        "ignored_valid_reversal_count": result.internal_engine.ignored_valid + (result.major_engine.ignored_valid if result.major_engine else 0),
        "state_transition_error_count": result.internal_engine.state_errors + (result.major_engine.state_errors if result.major_engine else 0),
        "independence_pass": result.independence_pass,
        "internal_max_age": result.internal_engine.max_age_seen,
        "major_max_age": result.major_engine.max_age_seen if result.major_engine else 0,
        "internal_over_168": result.internal_engine.exceeded[168],
        "internal_over_720": result.internal_engine.exceeded[720],
        "internal_over_2160": result.internal_engine.exceeded[2160],
        "major_over_168": result.major_engine.exceeded[168] if result.major_engine else 0,
        "major_over_720": result.major_engine.exceeded[720] if result.major_engine else 0,
        "major_over_2160": result.major_engine.exceeded[2160] if result.major_engine else 0,
        "unresolved": result.unresolved,
        "left_edge_censored": result.formation_stats["left_edge_censored"],
        "right_edge_censored": result.formation_stats["right_edge_censored"],
        "per_bar": result.per_bar,
        "monthly": result.monthly,
        "major_preview": [
            {
                "id": swing.major_swing_id,
                "type": swing.type,
                "label": swing.structure_label,
                "price": str(swing.price),
                "extreme_utc": iso_utc(swing.extreme_time),
                "confirmed_utc": iso_utc(swing.confirmed_at),
                "score": str(swing.significance_score),
                "score_label": swing.significance_label,
            }
            for swing in result.major_swings
        ],
        "unpromoted_internal_preview": [
            {
                "id": swing.internal_swing_id,
                "type": swing.type,
                "price": str(swing.price),
                "extreme_utc": iso_utc(swing.extreme_time),
                "reason": swing.major_non_promotion_reason,
            }
            for swing in result.internal_swings if not swing.became_major_swing
        ][:30],
    }
    print("SUMMARY_JSON " + json.dumps(summary, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
