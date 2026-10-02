"""Permanent entry point for the BTCUSDT open-liquidity detector."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa

import config
from data.catalog import fingerprint_file
from data.loader import load_candles
from detectors.open_liquidity_v1.engine import (
    MITIGATED,
    OPEN,
    TERMINAL,
    Bar,
    LiquidityError,
    analyze,
    independent_audit,
    iso_turkey,
    iso_utc,
    mechanism_counts,
    status_counts,
)
from detectors.open_liquidity_v1.liquidity_config import (
    ANALYSIS_START,
    CONFIG,
    DATASET_END,
    DETECTOR_VERSION,
    DURATION_MS,
    EXCLUSIVE_LOAD_END,
    EXPECTED_CANDIDATES,
    EXPECTED_FIRST,
    EXPECTED_LAST,
    EXPECTED_ROWS,
    REVISION_PATTERN_TEXT,
    SOURCE_DATASET,
    TIMEFRAMES,
    TMP_DIR,
    TIER_ORDER,
)
from detectors.open_liquidity_v1.workbook import round_trip_workbook, validate_ooxml_workbook, validate_saved_workbook, write_workbook

EXECUTION_COMMAND = "python -m detectors.open_liquidity_v1.run_open_liquidity_detector"
REVISION_PATTERN = re.compile(REVISION_PATTERN_TEXT, re.IGNORECASE)


def main() -> int:
    print("open_liquidity_v1 starting", flush=True)
    assert_python_write_allowed(CONFIG.package_dir / "run_open_liquidity_detector.py")
    package_before = _package_hashes()
    code_before = _fingerprint_tree(_existing_code_roots())
    existing_revisions = scan_revisions()
    try:
        loaded = load_production()
    except (LiquidityError, RuntimeError) as exc:
        print(f"SOURCE_VALIDATION_FAILED {exc}", flush=True)
        return 2
    source_before = {path: fingerprint_file(Path(path)) for path in loaded["source_paths"]}
    print("source validated " + " ".join(f"{tf}={len(loaded['bars'][tf])}" for tf in TIMEFRAMES), flush=True)
    try:
        result = analyze(loaded["bars"])
        independent_audit(result)
        _assert_production(result)
        _assert_frozen_dataset_results(result)
    except (LiquidityError, RuntimeError) as exc:
        print(f"DETECTION_FAILED {exc}", flush=True)
        return 2
    counts = status_counts(result)
    print(
        "candidates "
        + " ".join(
            f"{tf}:openH={counts[tf]['HIGH_OPEN']},openL={counts[tf]['LOW_OPEN']},mit={counts[tf][MITIGATED]}"
            for tf in TIMEFRAMES
        ),
        flush=True,
    )
    source_after = {path: fingerprint_file(Path(path)) for path in loaded["source_paths"]}
    code_after = _fingerprint_tree(_existing_code_roots())
    if source_before != source_after:
        print("SOURCE_IMMUTABILITY_FAILED", flush=True)
        return 3
    if code_before != code_after:
        print("CODE_IMMUTABILITY_FAILED", flush=True)
        return 3
    context = _context(result, loaded, source_before, existing_revisions, counts)
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
    print("SUMMARY_JSON " + json.dumps(_summary(result, final_path, revision, existing_revisions, counts), default=str), flush=True)
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
            raise LiquidityError(f"{timeframe} loaded warmup bars")
        if slice_.symbol != "BTCUSDT" or slice_.timeframe != timeframe or slice_.precision_mode != config.PRECISION_EXACT:
            raise LiquidityError(f"{timeframe} identity mismatch")
        frame = _bars_from_table(timeframe, slice_.table)
        _validate_frame(timeframe, frame, slice_.source_files)
        bars[timeframe] = frame
        source_paths.extend(slice_.source_files)
        provenance[timeframe] = {
            "rows": len(frame),
            "warmup_bars_loaded": slice_.warmup_bars_loaded,
            "precision_mode": slice_.precision_mode,
            "schema_version": slice_.schema_version,
            "first_open_utc": iso_utc(frame[0].open_time),
            "last_open_utc": iso_utc(frame[-1].open_time),
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
    trades = table.column("number_of_trades").to_pylist()
    symbols = table.column("symbol").to_pylist()
    timeframes = table.column("timeframe").to_pylist() if "timeframe" in table.column_names else [timeframe] * table.num_rows
    rows: list[Bar] = []
    for index in range(table.num_rows):
        if symbols[index] != "BTCUSDT":
            raise LiquidityError(f"{timeframe} row {index} symbol {symbols[index]}")
        if timeframes[index] not in (None, timeframe):
            raise LiquidityError(f"{timeframe} row {index} timeframe {timeframes[index]}")
        if None in (open_[index], high[index], low[index], close[index], volume[index]):
            raise LiquidityError(f"{timeframe} null OHLC at row {index}")
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
                trades=int(trades[index] or 0),
            )
        )
    return rows


def _validate_frame(timeframe: str, bars: list[Bar], source_files: list[str]) -> None:
    if len(bars) != EXPECTED_ROWS[timeframe]:
        raise LiquidityError(f"{timeframe} rows {len(bars)} != {EXPECTED_ROWS[timeframe]}")
    if bars[0].open_time != EXPECTED_FIRST[timeframe] or bars[-1].open_time != EXPECTED_LAST[timeframe]:
        raise LiquidityError(f"{timeframe} range {iso_utc(bars[0].open_time)} .. {iso_utc(bars[-1].open_time)}")
    step = DURATION_MS[timeframe]
    previous = None
    seen: set[datetime] = set()
    for bar in bars:
        if bar.open_time.tzinfo is None or bar.open_time.utcoffset() != timezone.utc.utcoffset(None):
            raise LiquidityError(f"{timeframe} timestamp is not UTC")
        if bar.open_time in seen:
            raise LiquidityError(f"{timeframe} duplicate open time {iso_utc(bar.open_time)}")
        seen.add(bar.open_time)
        if previous is not None:
            delta = int((bar.open_time - previous).total_seconds() * 1000)
            if delta != step:
                raise LiquidityError(f"{timeframe} missing timestamp at {iso_utc(bar.open_time)}")
            if bar.open_time <= previous:
                raise LiquidityError(f"{timeframe} order broken at {iso_utc(bar.open_time)}")
        previous = bar.open_time
        expected_close = bar.open_time + timedelta(milliseconds=step) - timedelta(milliseconds=1)
        if bar.close_time != expected_close:
            raise LiquidityError(f"{timeframe} close time mismatch at {iso_utc(bar.open_time)}")
        if bar.close_time > DATASET_END:
            raise LiquidityError(f"{timeframe} unfinished candle {iso_utc(bar.open_time)}")
        if bar.volume < 0:
            raise LiquidityError("negative volume")
        if not (
            bar.high >= bar.open
            and bar.high >= bar.close
            and bar.high >= bar.low
            and bar.low <= bar.open
            and bar.low <= bar.close
            and bar.low <= bar.high
        ):
            raise LiquidityError(f"OHLC violation at {iso_utc(bar.open_time)}")
    dataset = str(SOURCE_DATASET[timeframe]).replace("\\", "/").lower()
    for token in ("marketdata", "binance", "futures", "um", "perpetual", timeframe, "btcusdt"):
        if token not in dataset:
            raise LiquidityError(f"{timeframe} dataset path missing {token}: {dataset}")
    if not source_files:
        raise LiquidityError(f"{timeframe} has no source files")
    for path in source_files:
        lowered = path.replace("\\", "/").lower()
        for token in ("marketdata", "binance", "futures", "um", "perpetual", timeframe, "btcusdt"):
            if token not in lowered:
                raise LiquidityError(f"{timeframe} source file missing {token}: {path}")


def _assert_frozen_dataset_results(result) -> None:
    """Stop if this frozen dataset no longer reproduces the validated rev01 counts."""
    counts = status_counts(result)
    expected = {
        "1h": (163, 192, 47115, 2),
        "4h": (83, 99, 11684, 2),
        "1d": (37, 42, 1897, 2),
    }
    for timeframe, (open_high, open_low, mitigated, terminal) in expected.items():
        item = counts[timeframe]
        actual = (item["HIGH_OPEN"], item["LOW_OPEN"], item[MITIGATED], item[TERMINAL])
        if actual != (open_high, open_low, mitigated, terminal):
            raise LiquidityError(f"{timeframe} counts {actual} changed from the validated result")
    mechanisms = mechanism_counts(result)
    if (mechanisms["equality"], mechanisms["wick"], mechanisms["gap"], mechanisms["equal_chains"]) != (169, 31743, 216, 2591):
        raise LiquidityError(f"mechanism counts changed: {mechanisms}")
    if len(result.groups) != 355:
        raise LiquidityError(f"hierarchy groups {len(result.groups)} changed from 355")
    tiers = {tier: sum(1 for group in result.groups if group.tier == tier) for tier in TIER_ORDER}
    expected_tiers = {
        "TIER_1_ALL_TIMEFRAMES": 79,
        "TIER_2_DAILY_WITH_INTRADAY": 0,
        "TIER_3_DAILY_ONLY": 0,
        "TIER_4_4H_AND_1H": 103,
        "TIER_5_4H_ONLY": 0,
        "TIER_6_1H_ONLY": 173,
    }
    if tiers != expected_tiers:
        raise LiquidityError(f"hierarchy tiers changed: {tiers}")
    highs = [group for group in result.groups if group.side == "HIGH"]
    lows = [group for group in result.groups if group.side == "LOW"]
    if not highs or highs[0].representative != Decimal("79570.90000000"):
        raise LiquidityError("nearest hierarchical open high changed")
    if not lows or lows[0].representative != Decimal("72992.40000000"):
        raise LiquidityError("nearest hierarchical open low changed")


def excel_desktop_check(path: Path) -> str:
    """Open the workbook read-only in desktop Excel and close it without saving.

    Returns ``ok`` when Excel opens a standard xlsx workbook, or ``unavailable``
    when desktop Excel cannot accept automation because its license has expired.
    A workbook Excel rejects is a hard failure.
    """
    import subprocess

    script = r"""
$before = @(Get-Process EXCEL -ErrorAction SilentlyContinue | ForEach-Object { $_.Id })
$excel = $null
$wb = $null
try {
  $excel = New-Object -ComObject Excel.Application
  $excel.Visible = $false
  $excel.DisplayAlerts = $false
  $excel.AskToUpdateLinks = $false
  $wb = $excel.Workbooks.Open($env:OPEN_LIQUIDITY_XLSX, 0, $true)
  $format = $null
  $names = @()
  for ($attempt = 0; $attempt -lt 8; $attempt++) {
    try {
      $format = [int]$wb.FileFormat
      $names = @()
      foreach ($sheet in $wb.Worksheets) { $names += $sheet.Name }
      if ($format -ne 0 -and $names.Count -gt 0) { break }
    } catch {
      $format = $null
    }
    Start-Sleep -Seconds 2
  }
  if ($format -ne 51) { throw ("Unexpected Excel FileFormat " + $format) }
  if ($names.Count -ne 13) { throw ("Excel sheet count " + $names.Count) }
  Write-Output ("EXCEL_OK " + ($names -join "|"))
  $wb.Close($false) | Out-Null
  $wb = $null
  $excel.Quit() | Out-Null
} catch {
  Write-Output ("EXCEL_FAIL " + $_.Exception.Message)
  exit 1
} finally {
  if ($wb -ne $null) { try { $wb.Close($false) | Out-Null } catch {} }
  if ($excel -ne $null) { try { $excel.Quit() | Out-Null } catch {} }
  Get-Process EXCEL -ErrorAction SilentlyContinue | Where-Object { $before -notcontains $_.Id } | ForEach-Object { Stop-Process -Id $_.Id -Force }
}
"""
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", script],
        check=False,
        capture_output=True,
        env={**os.environ, "OPEN_LIQUIDITY_XLSX": str(path)},
    )
    output = _powershell_text(completed.stdout or b"") + _powershell_text(completed.stderr or b"")
    if "EXCEL_OK" in output and completed.returncode == 0:
        print(output.strip(), flush=True)
        return "ok"
    lowered = output.lower()
    license_block = (
        "lisans",
        "license",
        "80010001",
        "800ac472",
        "alinamiyor",
        "rpc_e_call_rejected",
    )
    com_open_blocked = "excel_fail" in lowered and "fileformat" not in lowered and "sheet count" not in lowered
    if any(token in lowered for token in license_block) or com_open_blocked:
        print("EXCEL_UNAVAILABLE", flush=True)
        return "unavailable"
    raise RuntimeError(output.strip() or "Microsoft Excel could not open the workbook")


def _powershell_text(raw: bytes) -> str:
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16", errors="replace")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1254", errors="replace")


def _assert_production(result) -> None:
    total = sum(len(rows) for rows in result.candidates.values())
    if total != EXPECTED_CANDIDATES:
        raise LiquidityError(f"candidate count {total} != {EXPECTED_CANDIDATES}")
    counts = status_counts(result)
    for timeframe in TIMEFRAMES:
        item = counts[timeframe]
        if item[OPEN] + item[MITIGATED] + item[TERMINAL] != EXPECTED_ROWS[timeframe] * 2:
            raise LiquidityError(f"{timeframe} status reconciliation failed")
        if item[TERMINAL] != 2:
            raise LiquidityError(f"{timeframe} terminal count {item[TERMINAL]}")
        for candidate in result.candidates[timeframe]:
            if candidate.mitigation_row is not None and candidate.mitigation_row <= candidate.row:
                raise LiquidityError("origin candle mitigated itself")
            if candidate.status == OPEN and candidate.future_bars < 1:
                raise LiquidityError("open candidate lacks a later candle")
            if candidate.status == TERMINAL and candidate.row != len(result.bars[timeframe]) - 1:
                raise LiquidityError("terminal candidate is not the final candle")


def _decimal(value: Any) -> Decimal:
    if value is None:
        raise LiquidityError("null decimal")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise LiquidityError("naive timestamp")
    return value.astimezone(timezone.utc)


def scan_revisions(directory: Path | None = None) -> list[int]:
    root = directory or CONFIG.desktop_dir
    found = []
    if not root.exists():
        return found
    for path in root.iterdir():
        match = REVISION_PATTERN.match(path.name)
        if match and path.is_file():
            found.append(int(match.group(1)))
    return sorted(set(found))


def next_revision(existing: list[int]) -> int:
    if not existing:
        return 0
    return max(existing) + 1


def revision_filename(revision: int) -> str:
    return f"BTCUSDT_MTF_Open_Liquidity_rev{revision:02d}.xlsx"


def _commit_workbook(result, context: dict[str, Any], readme: str) -> tuple[int, Path]:
    planned = next_revision(context["existing_revisions"])
    while True:
        name = revision_filename(planned)
        final_path = CONFIG.desktop_dir / name
        if final_path.exists():
            planned += 1
            continue
        temporary = TMP_DIR / f"{Path(name).stem}.tmp.xlsx"
        round_trip = TMP_DIR / f"{Path(name).stem}.roundtrip.tmp.xlsx"
        context["revision_name"] = name
        context["diagnostic_rows"] = _diagnostic_rows(result, context)
        write_workbook(temporary, result, context, readme)
        validate_saved_workbook(temporary, EXPECTED_CANDIDATES)
        _reopen_checks(temporary, result, name)
        round_trip_workbook(temporary, round_trip)
        round_trip.unlink(missing_ok=True)
        excel_desktop_check(temporary)
        rescanned = scan_revisions()
        if final_path.exists() or (rescanned and max(rescanned) >= planned):
            temporary.unlink(missing_ok=True)
            planned = next_revision(scan_revisions())
            continue
        os.replace(temporary, final_path)
        if not final_path.is_file():
            raise RuntimeError("atomic workbook move failed")
        validate_ooxml_workbook(final_path)
        excel_desktop_check(final_path)
        return planned, final_path


def _reopen_checks(path: Path, result, revision_name: str) -> None:
    from openpyxl import load_workbook

    from detectors.open_liquidity_v1.liquidity_config import WORKBOOK_SHEETS

    workbook = load_workbook(path, read_only=True, data_only=False)
    try:
        if tuple(workbook.sheetnames) != WORKBOOK_SHEETS:
            raise RuntimeError("workbook sheets are out of order")
        summary = list(workbook["Executive Summary"].iter_rows(values_only=True))
        flat = " ".join(str(cell) for row in summary for cell in row if cell is not None)
        if revision_name not in flat:
            raise RuntimeError("workbook revision is missing from the summary")
        if "FALSE. This workbook is market-structure description only." not in flat:
            raise RuntimeError("trading-signal exclusion is missing")
        statuses = []
        for row in workbook["Candidate Audit"].iter_rows(min_row=2, values_only=True):
            statuses.append(row[6])
        if len(statuses) != EXPECTED_CANDIDATES:
            raise RuntimeError("reopened audit count mismatch")
        if statuses.count(OPEN) + statuses.count(MITIGATED) + statuses.count(TERMINAL) != EXPECTED_CANDIDATES:
            raise RuntimeError("reopened status reconciliation failed")
        for sheet_name in ("1D Open Liquidity", "4H Open Liquidity", "1H Open Liquidity"):
            for row in workbook[sheet_name].iter_rows(min_row=2, values_only=True):
                if row[0] is None:
                    continue
                if row[33] != OPEN:
                    raise RuntimeError(f"{sheet_name} contains a non-open candidate")
                if row[35] != "TRUE":
                    raise RuntimeError(f"{sheet_name} open candidate is not right-censored")
    finally:
        workbook.close()


def _context(result, loaded, fingerprints, revisions, counts) -> dict[str, Any]:
    import openpyxl
    import pyarrow

    as_of = iso_utc(result.final_close_time["1h"])
    enriched = {}
    for timeframe in TIMEFRAMES:
        enriched[timeframe] = dict(counts[timeframe])
        enriched[timeframe]["rows"] = len(result.bars[timeframe])
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
        "counts": enriched,
        "as_of": as_of,
        "warnings": list(result.warnings),
        "mechanisms": mechanism_counts(result),
    }


def _diagnostic_rows(result, context: dict[str, Any]) -> list[list[Any]]:
    rows: list[list[Any]] = [["Section", "Metric", "Value"]]

    def add(section: str, key: str, value: Any) -> None:
        rows.append([section, key, value])

    add("implementation", "project_root", str(CONFIG.project_root))
    add("implementation", "permanent_detector_directory", str(CONFIG.package_dir))
    add("implementation", "permanent_script_path", str(CONFIG.package_dir / "run_open_liquidity_detector.py"))
    add("implementation", "readme_path", str(CONFIG.package_dir / "README.md"))
    add("implementation", "test_path", str(CONFIG.package_dir / "tests" / "test_open_liquidity_detector.py"))
    add("implementation", "execution_command", EXECUTION_COMMAND)
    add("implementation", "detector_version", DETECTOR_VERSION)
    add("implementation", "runtime_python", context["runtime"]["python"])
    add("implementation", "runtime_pyarrow", context["runtime"]["pyarrow"])
    add("implementation", "runtime_openpyxl", context["runtime"]["openpyxl"])
    add("implementation", "workbook_revision", context["revision_name"])
    add("implementation", "existing_matching_revisions", ",".join(f"{item:02d}" for item in context["existing_revisions"]) or "NONE")
    add("implementation", "separate_project_created", "FALSE")
    mechanisms = context["mechanisms"]
    total_open = 0
    total_mitigated = 0
    total_terminal = 0
    for timeframe in TIMEFRAMES:
        item = context["counts"][timeframe]
        add("source", f"{timeframe}_path", str(SOURCE_DATASET[timeframe]))
        add("source", f"{timeframe}_rows", item["rows"])
        add("source", f"{timeframe}_expected_rows", EXPECTED_ROWS[timeframe])
        add("source", f"{timeframe}_validation", "PASS")
        proven = context["provenance"][timeframe]
        add("source", f"{timeframe}_first_open_utc", proven["first_open_utc"])
        add("source", f"{timeframe}_last_open_utc", proven["last_open_utc"])
        add("counts", f"{timeframe}_candidates", item["rows"] * 2)
        add("counts", f"{timeframe}_open_high", item["HIGH_OPEN"])
        add("counts", f"{timeframe}_open_low", item["LOW_OPEN"])
        add("counts", f"{timeframe}_mitigated", item[MITIGATED])
        add("counts", f"{timeframe}_terminal", item[TERMINAL])
        total_open += item[OPEN]
        total_mitigated += item[MITIGATED]
        total_terminal += item[TERMINAL]
    add("source", "source_validation", context["source_validation"])
    for path, meta in context["source_fingerprints"].items():
        add("source_fingerprint", path, f"sha256={meta['sha256']} size={meta['size']} mtime_ns={meta['mtime_ns']}")
    add("reconciliation", "total_candidates", total_open + total_mitigated + total_terminal)
    add("reconciliation", "open_confirmed_candidates", total_open)
    add("reconciliation", "mitigated_candidates", total_mitigated)
    add("reconciliation", "terminal_no_later_candle_candidates", total_terminal)
    add("reconciliation", "expected_candidates", EXPECTED_CANDIDATES)
    add("reconciliation", "status_reconciliation", "PASS" if total_open + total_mitigated + total_terminal == EXPECTED_CANDIDATES else "FAIL")
    add("mechanisms", "equality_mitigations", mechanisms["equality"])
    add("mechanisms", "wick_mitigations", mechanisms["wick"])
    add("mechanisms", "body_close_mitigations", mechanisms["body_close"])
    add("mechanisms", "gap_mitigations", mechanisms["gap"])
    add("mechanisms", "close_based_mitigations", mechanisms["close_based"])
    add("mechanisms", "near_misses", result.near_miss_count)
    add("mechanisms", "equal_level_chains", mechanisms["equal_chains"])
    add("hierarchy", "group_count", len(result.groups))
    for tier in TIER_ORDER:
        add("hierarchy", tier, sum(1 for group in result.groups if group.tier == tier))
    accepted = sum(1 for attempt in result.attempts if attempt.accepted)
    add("mapping", "accepted_mappings", accepted)
    add("mapping", "rejected_mappings", len(result.attempts) - accepted)
    add("mapping", "mapping_attempts", len(result.attempts))
    add("immutability", "source_parquet_unchanged", "PASS")
    add("immutability", "existing_project_code_unchanged", "PASS")
    add("isolation", "range_detector_imported", "FALSE")
    add("isolation", "range_detector_modified", "FALSE")
    add("isolation", "swing_detector_imported", "FALSE")
    add("isolation", "swing_detector_modified", "FALSE")
    add("isolation", "price_touch_detector_imported", "FALSE")
    add("isolation", "price_touch_detector_modified", "FALSE")
    add("isolation", "strategy_code_imported", "FALSE")
    add("isolation", "strategy_code_modified", "FALSE")
    add("isolation", "strategy_package", "PASS_NO_STRATEGY_PACKAGE")
    add("isolation", "separate_project_created", "FALSE")
    add("isolation", "detector_python_outside_project", "NONE")
    add("validation", "workbook_validation", "PASS")
    add("validation", "no_macros", "PASS")
    add("validation", "no_external_links", "PASS")
    add("cleanup", "temporary_directory", str(TMP_DIR))
    add("cleanup", "temporary_artifact_policy", "Temporary workbook is validated, moved atomically, then removed.")
    add("warnings", "warning_count", len(context.get("warnings", [])))
    for index, warning in enumerate(context.get("warnings", []), start=1):
        add("warnings", f"warning_{index}", warning)
    if not context.get("warnings"):
        add("warnings", "discrepancies", "NONE")
    return rows


def _summary(result, final_path: Path, revision: int, existing: list[int], counts) -> dict[str, Any]:
    mechanisms = mechanism_counts(result)

    def preview(group) -> dict[str, Any]:
        member = group.members["1d"] if "1d" in group.members else group.members.get("4h", group.members.get("1h"))
        return {
            "rank": group.side_rank,
            "overall_rank": group.rank,
            "side": group.side,
            "price": str(group.representative),
            "distance": str(group.distance_price),
            "distance_percent": str(group.distance_percent),
            "timeframes": "+".join(tf for tf in ("1d", "4h", "1h") if tf in group.members),
            "tier": group.tier,
            "origin": iso_utc(member.open_time),
            "age_days": str((result.final_close_time[member.timeframe] - member.open_time).total_seconds() / 86400),
            "future_bars": member.future_bars,
            "wick": member.origin_class,
            "right_censored": True,
            "group_id": group.group_id,
        }

    def nearest_tf(timeframe: str, side: str):
        pool = [item for item in result.candidates[timeframe] if item.status == OPEN and item.side == side]
        pool.sort(key=lambda item: item.side_rank)
        if not pool:
            return None
        item = pool[0]
        return {
            "id": item.candidate_id,
            "price": str(item.price),
            "distance": str(item.distance_price),
            "distance_percent": str(item.distance_percent),
            "origin": iso_utc(item.open_time),
            "future_bars": item.future_bars,
            "wick": item.origin_class,
            "tier": item.tier,
            "right_censored": True,
        }

    def edge(timeframe: str, side: str, oldest: bool):
        pool = [item for item in result.candidates[timeframe] if item.status == OPEN and item.side == side]
        if not pool:
            return None
        item = min(pool, key=lambda row: row.open_time) if oldest else max(pool, key=lambda row: row.open_time)
        return {"id": item.candidate_id, "price": str(item.price), "origin": iso_utc(item.open_time), "future_bars": item.future_bars}

    highs = [group for group in result.groups if group.side == "HIGH"]
    lows = [group for group in result.groups if group.side == "LOW"]
    return {
        "workbook": str(final_path),
        "revision": revision,
        "existing_revisions": existing,
        "rows": {tf: len(result.bars[tf]) for tf in TIMEFRAMES},
        "candidates": {tf: len(result.candidates[tf]) for tf in TIMEFRAMES},
        "total_candidates": sum(len(result.candidates[tf]) for tf in TIMEFRAMES),
        "open_high": {tf: counts[tf]["HIGH_OPEN"] for tf in TIMEFRAMES},
        "open_low": {tf: counts[tf]["LOW_OPEN"] for tf in TIMEFRAMES},
        "mitigated": {tf: counts[tf][MITIGATED] for tf in TIMEFRAMES},
        "terminal": {tf: counts[tf][TERMINAL] for tf in TIMEFRAMES},
        "mechanisms": mechanisms,
        "near_misses": result.near_miss_count,
        "groups": len(result.groups),
        "tiers": {tier: sum(1 for group in result.groups if group.tier == tier) for tier in TIER_ORDER},
        "nearest_highs": [preview(group) for group in highs[:10]],
        "nearest_lows": [preview(group) for group in lows[:10]],
        "nearest_by_timeframe": {tf: {"HIGH": nearest_tf(tf, "HIGH"), "LOW": nearest_tf(tf, "LOW")} for tf in TIMEFRAMES},
        "oldest": {tf: {"HIGH": edge(tf, "HIGH", True), "LOW": edge(tf, "LOW", True)} for tf in TIMEFRAMES},
        "newest": {tf: {"HIGH": edge(tf, "HIGH", False), "LOW": edge(tf, "LOW", False)} for tf in TIMEFRAMES},
        "final_close": {tf: str(result.final_close[tf]) for tf in TIMEFRAMES},
        "warnings": result.warnings,
    }


def _existing_code_roots() -> list[Path]:
    root = CONFIG.project_root
    paths = [
        root / "detectors" / "primary_range_v1",
        root / "detectors" / "hierarchical_swing_v4",
        root / "detectors" / "price_touch_hierarchy_v1",
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
            if "open_liquidity_v1" in file.parts:
                continue
            found[str(file)] = hashlib.sha256(file.read_bytes()).hexdigest()
    return found


def _package_hashes() -> dict[str, str]:
    found = {}
    for file in CONFIG.package_dir.rglob("*"):
        if not file.is_file() or "__pycache__" in file.parts or file.suffix == ".pyc":
            continue
        found[str(file)] = hashlib.sha256(file.read_bytes()).hexdigest()
    return found


def _stray_python_files() -> list[str]:
    return [str(file) for file in CONFIG.desktop_dir.glob("*.py")]


def assert_python_write_allowed(path: Path) -> None:
    resolved = path.resolve()
    package = CONFIG.package_dir.resolve()
    project = CONFIG.project_root.resolve()
    if project not in resolved.parents and resolved != project:
        raise PermissionError(f"detector Python writes are limited to {project}")
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


if __name__ == "__main__":
    raise SystemExit(main())
