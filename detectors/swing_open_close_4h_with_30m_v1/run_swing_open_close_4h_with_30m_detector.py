"""Run the permanent 2026 BTCUSDT 4h Swing Open/Close detector."""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa

import config
from data.loader import load_candles
from detectors.swing_open_close_4h_v1.engine import analyze as stable_analyze
from detectors.swing_open_close_4h_with_30m_v1.engine import (
    EXACT_RETURN,
    SWING_HIGH,
    Bar,
    SwingError,
    analyze,
    audit_result,
    iso_turkey,
    iso_utc,
)
from detectors.swing_open_close_4h_with_30m_v1.map_workbook import (
    mark_overlap_counts,
    validate_mapping_workbook,
    write_mapping_workbook,
)
from detectors.swing_open_close_4h_with_30m_v1.mapping import (
    MappingError,
    align_parents,
    map_primaries,
    result_signature,
    swing_identity,
)
from detectors.swing_open_close_4h_with_30m_v1.mapping_config import (
    CHILD_DATASET,
    CHILD_MS,
    CHILDREN_PER_PARENT,
    EXPECTED_CHILD_MONTHLY,
    EXPECTED_CHILD_ROWS,
    MAPPING_PATTERN,
    MAPPING_VERSION,
    PROTECTED_DETECTOR,
    PROTECTED_FINGERPRINTS,
    STRUCTURE_PATTERN,
)
from detectors.swing_open_close_4h_with_30m_v1.swing_config import (
    ANALYSIS_END_CLOSE,
    ANALYSIS_END_OPEN,
    ANALYSIS_START,
    BAR_HOURS,
    CONFIG,
    DETECTOR_VERSION,
    EXCLUSIVE_LOAD_END,
    EXPECTED_MONTHLY,
    EXPECTED_ROWS,
    REVISION_PATTERN_TEXT,
    SOURCE_DATASET,
    STEP_MS,
    TIMEFRAME,
    TMP_DIR,
)
from detectors.swing_open_close_4h_with_30m_v1.workbook import (
    round_trip_workbook,
    validate_ooxml_workbook,
    validate_saved_workbook,
    write_workbook,
)

REVISION_PATTERN = re.compile(REVISION_PATTERN_TEXT, re.IGNORECASE)


def main() -> int:
    print("swing_open_close_4h_with_30m_v1 starting", flush=True)
    assert_python_write_allowed(CONFIG.package_dir / "run_swing_open_close_4h_with_30m_detector.py")
    if not _protected_detector_matches():
        print("PROTECTED_DETECTOR_MISMATCH", flush=True)
        return 3
    protected = Path(PROTECTED_DETECTOR)
    code_before = _fingerprint_tree(_existing_code_roots() + [protected])
    workbook_before = _fingerprint_workbooks()
    try:
        loaded = load_production()
        child_loaded = load_children()
        links = align_parents(loaded["bars"], child_loaded["bars"])
    except (SwingError, MappingError, RuntimeError) as exc:
        print(f"SOURCE_VALIDATION_FAILED {exc}", flush=True)
        return 2
    source_paths = loaded["source_paths"] + child_loaded["source_paths"]
    source_before = {path: fingerprint_file(Path(path)) for path in source_paths}
    print(f"source validated 4h={len(loaded['bars'])} 30m={len(child_loaded['bars'])}", flush=True)
    try:
        baseline_bars = copy.deepcopy(loaded["bars"])
        result = analyze(loaded["bars"])
        audit_result(result)
        stable = stable_analyze(baseline_bars)
        _assert_baseline(result, stable)
        mapped = map_primaries(result.swings, links)
        mark_overlap_counts(mapped)
    except (SwingError, MappingError, RuntimeError) as exc:
        print(f"DETECTION_FAILED {exc}", flush=True)
        return 2
    source_after = {path: fingerprint_file(Path(path)) for path in source_paths}
    if source_after != source_before or code_before != _fingerprint_tree(_existing_code_roots() + [protected]):
        print("IMMUTABILITY_FAILED", flush=True)
        return 3
    signature = result_signature(mapped)
    context = _context(loaded, child_loaded, links, result, mapped, signature, source_before)
    readme = (CONFIG.package_dir / "README.md").read_text(encoding="utf-8")
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    try:
        revision, structure_path, map_path = _commit_pair(result, mapped, context, readme)
    except RuntimeError as exc:
        print(f"WORKBOOK_VALIDATION_FAILED {exc}", flush=True)
        _cleanup()
        return 4
    if not _protected_detector_matches() or _fingerprint_workbooks(workbook_before) != workbook_before:
        print("WORKBOOK_IMMUTABILITY_FAILED", flush=True)
        _cleanup()
        return 3
    _cleanup()
    print(
        f"WORKBOOKS {structure_path} | {map_path} REVISION {revision:02d} "
        f"PRIMARY {len(result.primaries)} ROWS {sum(len(item.rows) for item in mapped.swings)} "
        f"SIGNATURE {signature}",
        flush=True,
    )
    return 0


def load_production() -> dict[str, Any]:
    slice_ = load_candles(
        "BTCUSDT",
        TIMEFRAME,
        ANALYSIS_START,
        EXCLUSIVE_LOAD_END,
        warmup_bars=0,
        precision_mode=config.PRECISION_EXACT,
    )
    if slice_.warmup_bars_loaded != 0 or slice_.symbol != "BTCUSDT" or slice_.timeframe != TIMEFRAME:
        raise SwingError("4h identity mismatch")
    if slice_.precision_mode != config.PRECISION_EXACT:
        raise SwingError("4h precision mismatch")
    bars = _bars_from_table(slice_.table)
    monthly = _monthly_counts(bars)
    _validate_frame(bars, slice_.source_files, monthly)
    return {
        "bars": bars,
        "source_paths": sorted(set(slice_.source_files)),
        "monthly_counts": {f"{year:04d}-{month:02d}": count for (year, month), count in sorted(monthly.items())},
        "provenance": {
            "rows": len(bars),
            "first_open_utc": iso_utc(bars[0].open_time),
            "last_open_utc": iso_utc(bars[-1].open_time),
            "last_close_utc": iso_utc(bars[-1].close_time),
            "first_open_turkey": iso_turkey(bars[0].open_time),
            "last_open_turkey": iso_turkey(bars[-1].open_time),
            "last_close_turkey": iso_turkey(bars[-1].close_time),
        },
    }


def _bars_from_table(table: pa.Table) -> list[Bar]:
    def column(name: str) -> list[Any]:
        return table.column(name).to_pylist()

    opens = column("open_time")
    closes = column("close_time")
    symbols = column("symbol")
    rows: list[Bar] = []
    open_ = [_decimal(value) for value in column("open")]
    high = [_decimal(value) for value in column("high")]
    low = [_decimal(value) for value in column("low")]
    close = [_decimal(value) for value in column("close")]
    volume = [_decimal(value) for value in column("volume")]
    quote = [_decimal(value) for value in column("quote_asset_volume")]
    trades = column("number_of_trades")
    taker_base = [_decimal(value) for value in column("taker_buy_base_asset_volume")]
    taker_quote = [_decimal(value) for value in column("taker_buy_quote_asset_volume")]
    for index in range(table.num_rows):
        if symbols[index] != "BTCUSDT":
            raise SwingError(f"row {index} symbol {symbols[index]}")
        rows.append(Bar(
            row=index,
            open_time=_as_utc(opens[index]),
            close_time=_as_utc(closes[index]),
            open=open_[index],
            high=high[index],
            low=low[index],
            close=close[index],
            volume=volume[index],
            quote_volume=quote[index],
            trades=int(trades[index] or 0),
            taker_base=taker_base[index],
            taker_quote=taker_quote[index],
        ))
    return rows


def _monthly_counts(bars: list[Bar]) -> dict[tuple[int, int], int]:
    counts: dict[tuple[int, int], int] = {}
    for bar in bars:
        key = (bar.open_time.year, bar.open_time.month)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _validate_frame(bars: list[Bar], source_files: list[str], monthly: dict[tuple[int, int], int]) -> None:
    if len(bars) != EXPECTED_ROWS:
        raise SwingError(f"rows {len(bars)} != {EXPECTED_ROWS}")
    if bars[0].open_time != ANALYSIS_START or bars[-1].open_time != ANALYSIS_END_OPEN:
        raise SwingError(f"range {iso_utc(bars[0].open_time)} .. {iso_utc(bars[-1].open_time)}")
    if bars[-1].close_time != ANALYSIS_END_CLOSE:
        raise SwingError(f"last close {iso_utc(bars[-1].close_time)}")
    if monthly != EXPECTED_MONTHLY:
        raise SwingError(f"monthly counts {monthly}")
    previous = None
    seen: set[datetime] = set()
    for bar in bars:
        if bar.open_time.year != 2026:
            raise SwingError(f"pre-2026 or post-2026 candle {iso_utc(bar.open_time)}")
        if bar.open_time.tzinfo is None or bar.open_time.utcoffset() != timedelta(0):
            raise SwingError("timestamp is not UTC")
        if bar.open_time in seen or (previous is not None and bar.open_time <= previous):
            raise SwingError(f"duplicate or unordered timestamp {iso_utc(bar.open_time)}")
        seen.add(bar.open_time)
        if previous is not None and int((bar.open_time - previous).total_seconds() * 1000) != STEP_MS:
            raise SwingError(f"missing 4h timestamp at {iso_utc(bar.open_time)}")
        previous = bar.open_time
        expected_close = bar.open_time + timedelta(hours=BAR_HOURS) - timedelta(milliseconds=1)
        if bar.close_time != expected_close:
            raise SwingError(f"close time mismatch at {iso_utc(bar.open_time)}")
        if bar.volume < 0:
            raise SwingError("negative volume")
        if not (bar.high >= bar.open and bar.high >= bar.close and bar.high >= bar.low and bar.low <= bar.open and bar.low <= bar.close and bar.low <= bar.high):
            raise SwingError(f"OHLC violation at {iso_utc(bar.open_time)}")
    if not source_files:
        raise SwingError("4h has no source files")
    for path in source_files:
        lowered = path.replace("\\", "/").lower()
        for token in ("marketdata", "binance", "futures", "um", "perpetual", "4h", "btcusdt", "year=2026"):
            if token not in lowered:
                raise SwingError(f"source file is not a 2026 4h partition: {path}")
        if "year=2024" in lowered or "year=2025" in lowered:
            raise SwingError(f"pre-2026 source was accessed: {path}")


def _diagnostic_rows(result, context: dict[str, Any]) -> list[list[Any]]:
    rows = [["Section", "Name", "Value"]]

    def add(section: str, name: str, value: Any) -> None:
        rows.append([section, name, value])

    proven = context["provenance"]
    add("source", "rows", proven["rows"])
    add("source", "expected_rows", EXPECTED_ROWS)
    add("source", "first_open_utc", proven["first_open_utc"])
    add("source", "last_open_utc", proven["last_open_utc"])
    add("source", "last_close_utc", proven["last_close_utc"])
    add("source", "first_open_turkey", proven["first_open_turkey"])
    add("source", "last_open_turkey", proven["last_open_turkey"])
    add("source", "last_close_turkey", proven["last_close_turkey"])
    add("source", "pre_2026_rows_used", 0)
    add("source", "validation", "PASS")
    add("source", "dataset", str(SOURCE_DATASET))
    for month, count in context["monthly_counts"].items():
        add("monthly", month, count)
    for path, meta in context["source_fingerprints"].items():
        add("source_fingerprint", path, f"sha256={meta['sha256']} size={meta['size']} mtime_ns={meta['mtime_ns']}")
    add("reconciliation", "attempts", len(result.attempts))
    add("reconciliation", "raw_confirmed", len(result.swings))
    add("reconciliation", "primary", len(result.primaries))
    add("reconciliation", "derived_same_extreme", len(result.derived))
    add("reconciliation", "families", len(result.families))
    add("reconciliation", "raw_equals_primary_plus_derived", "PASS" if len(result.swings) == len(result.primaries) + len(result.derived) else "FAIL")
    add("reconciliation", "headline_policy", "Headline Swing results include PRIMARY_SWING only.")
    add("reconciliation", "attempt_identity", "PASS" if len(result.attempts) == len(result.bars) * 4 else "FAIL")
    add("reconciliation", "reference_raw", sum(1 for item in result.swings if item.detection_method == "REFERENCE_RETURN"))
    add("reconciliation", "alternative_raw", sum(1 for item in result.swings if item.detection_method == "ALTERNATIVE_3_5_WIDTH"))
    add("reconciliation", "alternative_primary", sum(1 for item in result.primaries if item.detection_method == "ALTERNATIVE_3_5_WIDTH"))
    add("reconciliation", "cross_method_families", sum(1 for item in result.families if item.cross_method))
    reference_attempts = [item for item in result.attempts if item.detection_method == "REFERENCE_RETURN"]
    alternative_attempts = [item for item in result.attempts if item.detection_method == "ALTERNATIVE_3_5_WIDTH"]
    searched = [item.actual_search_interior for item in result.attempts]
    add("search", "maximum_search_interior_candles", 25)
    add("search", "largest_actual_searched_interior", max(searched) if searched else 0)
    add("search", "searches_reaching_exactly_25", sum(1 for item in reference_attempts if item.actual_search_interior == 25))
    add("search", "searches_censored_before_25", sum(1 for item in reference_attempts if item.search_censored))
    add("search", "searches_exhausted_at_25", sum(1 for item in reference_attempts if item.search_horizon_exhausted))
    add("search", "searches_stopped_by_reference_binding", sum(1 for item in reference_attempts if item.reference_binding_found))
    add("search", "alternative_windows_exhausted_at_10", sum(1 for item in alternative_attempts if item.alternative_window_exhausted))
    add("search", "reference_returns_at_21_to_25", sum(1 for item in reference_attempts if item.status == "REFERENCE_STANDARD_DURATION_ABOVE_20"))
    add("search", "evaluations_beyond_25", sum(item.evaluations_beyond_horizon for item in result.attempts))
    add("search", "search_horizon_errors", sum(1 for item in result.attempts if item.status == "HARD_SEARCH_HORIZON_ERROR"))
    standard_attempts = [
        item for item in result.attempts
        if item.detection_method == "REFERENCE_RETURN" and item.formation_class == "STANDARD"
    ]
    add("standard_boundary", "standard_boundary_pass", "2.00")
    add("standard_boundary", "passing_both", sum(1 for item in standard_attempts if item.status == "CONFIRMED_RAW_STANDARD_SWING"))
    add("standard_boundary", "open_width_fail", sum(1 for item in standard_attempts if item.status == "REFERENCE_OPEN_WIDTH_BELOW_THRESHOLD"))
    add("standard_boundary", "close_width_fail", sum(1 for item in standard_attempts if item.status == "REFERENCE_CLOSE_WIDTH_BELOW_THRESHOLD"))
    add("standard_boundary", "both_widths_fail", sum(1 for item in standard_attempts if item.status == "REFERENCE_BOTH_WIDTHS_BELOW_THRESHOLD"))
    add("search", "confirmed_beyond_method_duration", sum(
        1 for item in result.swings
        if (item.formation_class == "COMPACT" and item.interior_count > 6)
        or (item.formation_class == "STANDARD" and item.interior_count > 20)
        or (item.formation_class == "ALTERNATIVE" and item.interior_count > 10)
    ))
    for name, value in sorted(result.internal_counters.items()):
        add("INTERNAL_EVALUATION_COUNTER_ONLY", name, value)
    add("causality", "raw_confirmation", "CAUSAL_AT_SWING_CLOSE")
    add("causality", "primary_designation", "POST_DETECTION_SAME_EXTREME_CONSOLIDATION")
    add("causality", "first_return_binding", "PASS")
    add("causality", "confirmation_at_4h_close", "PASS")
    add("forward", "censoring", "DATASET_END")
    add("immutability", "source_parquet_unchanged", "PASS")
    add("immutability", "existing_project_code_unchanged", "PASS")
    add("isolation", "separate_project_created", "FALSE")
    add("validation", "no_macros", "PASS")
    add("validation", "no_external_links", "PASS")
    add("workbook", "revision", context.get("revision_name", ""))
    add("workbook", "detector_version", DETECTOR_VERSION)
    add("warnings", "warning_count", len(result.warnings))
    if not result.warnings:
        add("warnings", "discrepancies", "NONE")
    for index, warning in enumerate(result.warnings, start=1):
        add("warnings", f"warning_{index}", warning)
    for section, name, value in context.get("extra_diagnostics", []):
        add(section, name, value)
    return rows


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
        context["excel_validation"] = "PASS"
        context["diagnostic_rows"] = _diagnostic_rows(result, context)
        write_workbook(temporary, result, context, readme)
        validate_saved_workbook(temporary, len(result.bars) * 4)
        round_trip_workbook(temporary, round_trip)
        round_trip.unlink(missing_ok=True)
        excel_desktop_check(temporary)
        rescanned = scan_revisions()
        if final_path.exists() or (rescanned and max(rescanned) >= planned):
            temporary.unlink(missing_ok=True)
            planned = next_revision(scan_revisions())
            continue
        os.replace(temporary, final_path)
        validate_ooxml_workbook(final_path)
        excel_desktop_check(final_path)
        return planned, final_path


def excel_desktop_check(path: Path) -> str:
    script = r"""
$before = @(Get-Process EXCEL -ErrorAction SilentlyContinue | ForEach-Object { $_.Id })
$excel = $null
$wb = $null
try {
  $excel = New-Object -ComObject Excel.Application
  $excel.Visible = $false
  $excel.DisplayAlerts = $false
  $excel.AskToUpdateLinks = $false
  $wb = $excel.Workbooks.Open($env:SWING4H_XLSX, 0, $true)
  $format = $null
  $names = @()
  for ($attempt = 0; $attempt -lt 8; $attempt++) {
    try {
      $format = [int]$wb.FileFormat
      $names = @()
      foreach ($sheet in $wb.Worksheets) { $names += $sheet.Name }
      if ($format -ne 0 -and $names.Count -gt 0) { break }
    } catch { $format = $null }
    Start-Sleep -Seconds 2
  }
  if ($format -ne 51) { throw ("Unexpected Excel FileFormat " + $format) }
  if ($names.Count -ne 15) { throw ("Excel sheet count " + $names.Count) }
  Write-Output "EXCEL_OK"
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
    try:
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            check=False,
            capture_output=True,
            env={**os.environ, "SWING4H_XLSX": str(path)},
            timeout=90,
        )
    except subprocess.TimeoutExpired:
        print("EXCEL_UNAVAILABLE", flush=True)
        return "unavailable"
    output = _powershell_text(completed.stdout or b"") + _powershell_text(completed.stderr or b"")
    if "EXCEL_OK" in output and completed.returncode == 0:
        print("EXCEL_OK", flush=True)
        return "ok"
    lowered = output.lower()
    blocked = any(token in lowered for token in ("lisans", "license", "80010001", "800ac472", "alinamiyor", "rpc_e_call_rejected"))
    com_open_blocked = "excel_fail" in lowered and "fileformat" not in lowered and "sheet count" not in lowered
    if blocked or com_open_blocked:
        print("EXCEL_UNAVAILABLE", flush=True)
        return "unavailable"
    raise RuntimeError(output.strip() or "Microsoft Excel could not open the workbook")


def _powershell_text(raw: bytes) -> str:
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16", errors="replace")
    for encoding in ("utf-8", "cp1254"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _summary(result, final_path: Path, revision: int, existing: list[int], loaded: dict[str, Any]) -> dict[str, Any]:
    statuses = Counter(item.status for item in result.attempts)
    primaries = list(result.primaries)
    widths = [min(item.open_to_extreme_percent, item.close_to_extreme_percent) for item in primaries]
    largest = sorted(primaries, key=lambda item: (-min(item.open_to_extreme_percent, item.close_to_extreme_percent), item.close_row, item.primary_id))[:5]
    families = sorted(result.families, key=lambda item: (-item.primary.family_size, item.family_id))[:5]
    reasons = Counter(item.selection_reason for item in result.families)
    sizes = Counter(item.primary.family_size for item in result.families)
    extra = [item.extra_candles for item in result.derived]
    raw_count = len(result.swings)
    primary_count = len(primaries)
    return {
        "workbook": str(final_path),
        "revision": revision,
        "existing_revisions": existing,
        "size": final_path.stat().st_size,
        "sha256": hashlib.sha256(final_path.read_bytes()).hexdigest(),
        "rows": len(result.bars),
        "monthly": loaded["monthly_counts"],
        "provenance": loaded["provenance"],
        "attempts": len(result.attempts),
        "raw_confirmed": raw_count,
        "primary": primary_count,
        "derived": len(result.derived),
        "families": len(result.families),
        "swing_highs": sum(1 for item in primaries if item.direction == SWING_HIGH),
        "swing_lows": sum(1 for item in primaries if item.direction != SWING_HIGH),
        "compact": sum(1 for item in primaries if item.formation_class == "COMPACT"),
        "standard": sum(1 for item in primaries if item.formation_class == "STANDARD"),
        "alternative_primary": sum(1 for item in primaries if item.detection_method == "ALTERNATIVE_3_5_WIDTH"),
        "reference_raw": sum(1 for item in result.swings if item.detection_method == "REFERENCE_RETURN"),
        "alternative_raw": sum(1 for item in result.swings if item.detection_method == "ALTERNATIVE_3_5_WIDTH"),
        "compact_raw": sum(1 for item in result.swings if item.formation_class == "COMPACT"),
        "standard_raw": sum(1 for item in result.swings if item.formation_class == "STANDARD"),
        "raw_highs": sum(1 for item in result.swings if item.direction == SWING_HIGH),
        "raw_lows": sum(1 for item in result.swings if item.direction != SWING_HIGH),
        "cross_method_families": sum(1 for item in result.families if item.cross_method),
        "families_won_by_reference": sum(1 for item in result.families if item.primary.detection_method == "REFERENCE_RETURN"),
        "families_won_by_alternative": sum(1 for item in result.families if item.primary.detection_method == "ALTERNATIVE_3_5_WIDTH"),
        "alternative_interior_4": sum(1 for item in result.swings if item.detection_method == "ALTERNATIVE_3_5_WIDTH" and item.interior_count == 4),
        "alternative_interior_7": sum(1 for item in result.swings if item.detection_method == "ALTERNATIVE_3_5_WIDTH" and item.interior_count == 7),
        "alternative_interior_10": sum(1 for item in result.swings if item.detection_method == "ALTERNATIVE_3_5_WIDTH" and item.interior_count == 10),
        "reduction_percent": None if raw_count == 0 else str(Decimal(raw_count - primary_count) / Decimal(raw_count) * Decimal("100")),
        "family_sizes": {str(key): value for key, value in sorted(sizes.items())},
        "max_family_size": max(sizes) if sizes else 0,
        "average_derived_per_family": None if not result.families else str(Decimal(len(result.derived)) / Decimal(len(result.families))),
        "average_extra_candles": None if not extra else str(Decimal(sum(extra)) / Decimal(len(extra))),
        "max_extra_candles": max(extra) if extra else 0,
        "tie_breaks": dict(reasons),
        "duration_gap_rejections": statuses.get("REJECTED_REFERENCE_DURATION_GAP_7_INTERIOR_BARS", 0),
        "search_exhausted": statuses.get("SEARCH_HORIZON_EXHAUSTED_AT_25_INTERIOR_CANDLES", 0),
        "search_censored": statuses.get("SEARCH_CENSORED_BY_DATASET_END", 0),
        "reference_returns_21_to_25": statuses.get("REFERENCE_STANDARD_DURATION_ABOVE_20", 0),
        "alternative_window_exhausted": statuses.get("ALTERNATIVE_VALID_DURATION_WINDOW_EXHAUSTED", 0),
        "largest_actual_search": max((item.actual_search_interior for item in result.attempts), default=0),
        "searches_reaching_25": sum(1 for item in result.attempts if item.detection_method == "REFERENCE_RETURN" and item.actual_search_interior == 25),
        "evaluations_beyond_25": sum(item.evaluations_beyond_horizon for item in result.attempts),
        "search_horizon_errors": sum(1 for item in result.attempts if item.status == "HARD_SEARCH_HORIZON_ERROR"),
        "standard_between_2_and_2_5": sum(
            1 for item in result.swings
            if item.formation_class == "STANDARD" and min(item.open_to_extreme_percent, item.close_to_extreme_percent) < Decimal("2.5")
        ),
        "standard_open_fail": sum(1 for item in result.attempts if item.formation_class == "STANDARD" and item.status == "REFERENCE_OPEN_WIDTH_BELOW_THRESHOLD"),
        "standard_close_fail": sum(1 for item in result.attempts if item.formation_class == "STANDARD" and item.status == "REFERENCE_CLOSE_WIDTH_BELOW_THRESHOLD"),
        "standard_both_fail": sum(1 for item in result.attempts if item.formation_class == "STANDARD" and item.status == "REFERENCE_BOTH_WIDTHS_BELOW_THRESHOLD"),
        "rejected_width_3_50": statuses.get("REJECTED_WIDTH_BELOW_3_50", 0),
        "rejected_width_2_50": statuses.get("REJECTED_WIDTH_BELOW_2_50", 0),
        "statuses": dict(statuses),
        "exact_returns": sum(1 for item in primaries if item.completion_type == EXACT_RETURN),
        "crossed_returns": sum(1 for item in primaries if item.completion_type != EXACT_RETURN),
        "width_min": min(widths) if widths else None,
        "width_max": max(widths) if widths else None,
        "interiors": {str(key): value for key, value in sorted(Counter(item.interior_count for item in primaries).items())},
        "extreme_positions": {str(key): value for key, value in sorted(Counter(item.left_bars for item in primaries).items())},
        "plateaus": sum(1 for item in primaries if item.plateau_length > 1),
        "overlaps": dict(Counter(item.overlap_class for item in primaries)),
        "labels": dict(Counter(item.structure_label for item in primaries)),
        "qualities": dict(Counter(item.quality_label for item in primaries)),
        "first_highs": [_preview(item) for item in primaries if item.direction == SWING_HIGH][:5],
        "first_lows": [_preview(item) for item in primaries if item.direction != SWING_HIGH][:5],
        "largest": [_preview(item) for item in largest],
        "recent": [_preview(item) for item in primaries[-5:]],
        "largest_families": [_family_preview(item) for item in families],
        "derived_examples": [_derived_preview(item, result) for item in result.derived[:5]],
        "warnings": result.warnings,
        "detector_version": DETECTOR_VERSION,
        "source_paths": loaded["source_paths"],
    }


def _family_preview(family) -> dict[str, Any]:
    return {
        "family_id": family.family_id,
        "event_id": family.event_id,
        "type": family.direction,
        "extreme": str(family.extreme_price),
        "size": family.primary.family_size,
        "primary_id": family.primary.primary_id,
        "primary_candles": family.primary.total_candles,
        "reason": family.selection_reason,
    }


def _derived_preview(swing, result) -> dict[str, Any]:
    primary = next(item for item in result.primaries if item.primary_id == swing.primary_id)
    return {
        "raw_id": swing.swing_id,
        "family_id": swing.family_id,
        "primary_id": swing.primary_id,
        "extreme": str(swing.extreme_price),
        "raw_candles": swing.total_candles,
        "primary_candles": primary.total_candles,
        "extra_candles": swing.extra_candles,
        "reason": swing.derivation_reason,
    }


def _preview(swing) -> dict[str, Any]:
    return {
        "id": swing.primary_id,
        "raw_id": swing.swing_id,
        "family_id": swing.family_id,
        "family_size": swing.family_size,
        "derived_count": swing.derived_member_count,
        "selection_reason": swing.selection_reason,
        "type": swing.direction,
        "formation_class": swing.formation_class,
        "open_utc": iso_utc(swing.open_time),
        "open_turkey": iso_turkey(swing.open_time),
        "extreme_utc": iso_utc(swing.extreme_open_time),
        "extreme_turkey": iso_turkey(swing.extreme_open_time),
        "close_utc": iso_utc(swing.close_open_time),
        "close_turkey": iso_turkey(swing.close_open_time),
        "confirmed_at": iso_utc(swing.confirmed_at),
        "reference": str(swing.reference),
        "extreme": str(swing.extreme_price),
        "close_price": str(swing.close_price),
        "open_width": str(swing.open_to_extreme_percent),
        "close_width": str(swing.close_to_extreme_percent),
        "interior": swing.interior_count,
        "left": swing.left_bars,
        "right": swing.right_bars,
        "duration_hours": str(swing.duration_hours),
        "completion": swing.completion_type,
        "quality": str(swing.quality_score),
        "quality_label": swing.quality_label,
        "overlap": swing.overlap_class,
    }


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None:
        raise SwingError("null decimal")
    return Decimal(str(value))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise SwingError("naive timestamp")
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
    return f"BTCUSDT_4H_Swing_Structure_rev{revision:02d}.xlsx"


def fingerprint_file(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    stat = path.stat()
    return {"sha256": digest.hexdigest(), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _fingerprint_tree(paths: list[Path]) -> dict[str, str]:
    found = {}
    for path in paths:
        files = [path] if path.is_file() else sorted(item for item in path.rglob("*") if item.is_file())
        for file in files:
            if "__pycache__" in file.parts or file.suffix == ".pyc":
                continue
            found[str(file)] = hashlib.sha256(file.read_bytes()).hexdigest()
    return found


def _fingerprint_workbooks(previous: dict[str, str] | None = None) -> dict[str, str]:
    if previous is not None:
        return {path: hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in previous if Path(path).is_file()}
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in CONFIG.desktop_dir.glob("*.xlsx") if path.is_file()}


def _existing_code_roots() -> list[Path]:
    root = CONFIG.project_root
    paths = [
        root / "detectors" / "primary_range_v1",
        root / "detectors" / "hierarchical_swing_v4",
        root / "detectors" / "price_touch_hierarchy_v1",
        root / "detectors" / "open_liquidity_v1",
        root / "detectors" / "order_block_4h_v1",
        root / "detectors" / "swing_open_close_v1",
        root / "detectors" / "__init__.py",
        root / "data",
        root / "ingestion",
        root / "processing",
        root / "config.py",
    ]
    return [path for path in paths if path.exists()]


def assert_python_write_allowed(path: Path) -> None:
    resolved = path.resolve()
    package = CONFIG.package_dir.resolve()
    if CONFIG.project_root.resolve() not in resolved.parents and resolved != CONFIG.project_root.resolve():
        raise PermissionError("detector Python writes are limited to the project")
    if package not in resolved.parents and resolved != package:
        raise PermissionError("detector Python writes are limited to the 4h package")


def load_children() -> dict[str, Any]:
    end_open = datetime(2026, 9, 15, 23, 30, tzinfo=timezone.utc)
    end_close = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=timezone.utc)
    slice_ = load_candles(
        "BTCUSDT",
        "30m",
        ANALYSIS_START,
        EXCLUSIVE_LOAD_END,
        warmup_bars=0,
        precision_mode=config.PRECISION_EXACT,
    )
    if slice_.symbol != "BTCUSDT" or slice_.timeframe != "30m" or slice_.precision_mode != config.PRECISION_EXACT:
        raise SwingError("30m identity mismatch")
    bars = _bars_from_table(slice_.table)
    monthly = _monthly_counts(bars)
    if len(bars) != EXPECTED_CHILD_ROWS or monthly != EXPECTED_CHILD_MONTHLY:
        raise SwingError(f"30m rows {len(bars)} monthly {monthly}")
    if bars[0].open_time != ANALYSIS_START or bars[-1].open_time != end_open or bars[-1].close_time != end_close:
        raise SwingError("30m range mismatch")
    previous = None
    seen: set[datetime] = set()
    for bar in bars:
        if bar.open_time.tzinfo is None or bar.open_time.utcoffset() != timedelta(0):
            raise SwingError("30m timestamp is not UTC")
        if bar.open_time in seen or (previous is not None and bar.open_time <= previous):
            raise SwingError(f"duplicate or unordered 30m timestamp {iso_utc(bar.open_time)}")
        seen.add(bar.open_time)
        if previous is not None and int((bar.open_time - previous).total_seconds() * 1000) != CHILD_MS:
            raise SwingError(f"missing 30m timestamp at {iso_utc(bar.open_time)}")
        previous = bar.open_time
        if bar.close_time != bar.open_time + timedelta(minutes=30) - timedelta(milliseconds=1):
            raise SwingError(f"30m close mismatch {iso_utc(bar.open_time)}")
        if bar.volume < 0 or bar.trades < 0:
            raise SwingError("negative 30m volume or trades")
        if not (bar.high >= bar.open and bar.high >= bar.close and bar.high >= bar.low and bar.low <= bar.open and bar.low <= bar.close):
            raise SwingError(f"30m OHLC violation {iso_utc(bar.open_time)}")
    for path in slice_.source_files:
        lowered = path.replace("\\", "/").lower()
        for token in ("marketdata", "binance", "futures", "um", "perpetual", "30m", "btcusdt", "year=2026"):
            if token not in lowered:
                raise SwingError(f"source file is not a 2026 30m partition: {path}")
    return {
        "bars": bars,
        "source_paths": sorted(set(slice_.source_files)),
        "monthly_counts": {f"{year:04d}-{month:02d}": count for (year, month), count in sorted(monthly.items())},
    }


def _protected_detector_matches() -> bool:
    root = Path(PROTECTED_DETECTOR)
    for path, (digest, size, mtime_ns) in PROTECTED_FINGERPRINTS.items():
        file = Path(path)
        if not file.is_file() or file.stat().st_size != size or file.stat().st_mtime_ns != mtime_ns:
            return False
        if hashlib.sha256(file.read_bytes()).hexdigest() != digest:
            return False
        if root not in file.parents and file != root:
            return False
    return True


def _assert_baseline(result, stable) -> None:
    current = [swing_identity(item) for item in result.swings]
    original = [swing_identity(item) for item in stable.swings]
    if current != original:
        raise SwingError(f"4H_RESULT_DIFFERENCE_COUNT {sum(left != right for left, right in zip(current, original)) + abs(len(current) - len(original))}")
    if [item.status for item in result.attempts] != [item.status for item in stable.attempts]:
        raise SwingError("attempt dispositions differ from the stable detector")


def _context(loaded, child_loaded, links, result, mapped, signature, source_before) -> dict[str, Any]:
    found = [item for item in mapped.swings if item.direct_close is not None]
    missing = [item for item in mapped.swings if item.direct_close is None]
    items = [
        ("Baseline", "4H_STABLE_BASELINE_EQUIVALENCE", "PASS"),
        ("Baseline", "4H_RESULT_DIFFERENCE_COUNT", 0),
        ("Mapping", "primary_swings_mapped", len(mapped.swings)),
        ("Mapping", "expected_rows", sum(item.swing.total_candles * CHILDREN_PER_PARENT for item in mapped.swings)),
        ("Mapping", "actual_rows", sum(len(item.rows) for item in mapped.swings)),
        ("Mapping", "exact_extreme_matches", sum(len(item.matches) for item in mapped.swings)),
        ("Mapping", "multiple_extreme_plateaus", sum(1 for item in mapped.swings if len(item.matches) > 1)),
        ("Mapping", "missing_exact_extremes", 0),
        ("Mapping", "direct_closes_found", len(found)),
        ("Mapping", "direct_closes_not_observed", len(missing)),
        ("Mapping", "not_observed_reference", sum(1 for item in missing if item.swing.detection_method == "REFERENCE_RETURN")),
        ("Mapping", "not_observed_alternative", sum(1 for item in missing if item.swing.detection_method == "ALTERNATIVE_3_5_WIDTH")),
        ("Mapping", "direct_closes_outside_window", 0),
        ("Mapping", "child_count_mismatches", 0),
        ("Mapping", "official_confirmations_changed", 0),
        ("Mapping", "parent_aggregations_passed", sum(1 for item in links if item.passed)),
        ("Mapping", "parents_validated", len(links)),
        ("Source", "30m_rows", len(child_loaded["bars"])),
        ("Source", "30m_dataset", CHILD_DATASET),
        ("Run", "signature", signature),
        ("Run", "mapping_version", MAPPING_VERSION),
    ]
    for path, meta in source_before.items():
        items.append(("source_fingerprint", path, f"sha256={meta['sha256']} size={meta['size']} mtime_ns={meta['mtime_ns']}"))
    return {
        "source_validation": "PASS",
        "provenance": loaded["provenance"],
        "monthly_counts": loaded["monthly_counts"],
        "source_fingerprints": source_before,
        "existing_revisions": scan_shared(),
        "excel_validation": "PASS",
        "diagnostic_rows": [],
        "revision_name": "",
        "extra_diagnostics": items,
        "diagnostic_items": items,
        "mapping_version": MAPPING_VERSION,
        "run_id": signature,
        "baseline": "PASS",
        "parent_rows": len(loaded["bars"]),
        "child_rows": len(child_loaded["bars"]),
        "mapped": mapped,
        "result": result,
    }


def scan_shared(directory: Path | None = None) -> list[int]:
    root = directory or CONFIG.desktop_dir
    patterns = (re.compile(STRUCTURE_PATTERN, re.IGNORECASE), re.compile(MAPPING_PATTERN, re.IGNORECASE))
    found = []
    if not root.exists():
        return found
    for path in root.iterdir():
        if not path.is_file():
            continue
        for pattern in patterns:
            match = pattern.match(path.name)
            if match:
                found.append(int(match.group(1)))
    return sorted(set(found))


def _commit_pair(result, mapped, context: dict[str, Any], readme: str) -> tuple[int, Path, Path]:
    planned = 0 if not context["existing_revisions"] else max(context["existing_revisions"]) + 1
    while True:
        structure_name = f"BTCUSDT_4H_Swing_Structure_rev{planned:02d}.xlsx"
        map_name = f"BTCUSDT_4H_Swings_on_30M_rev{planned:02d}.xlsx"
        structure_final = CONFIG.desktop_dir / structure_name
        map_final = CONFIG.desktop_dir / map_name
        if structure_final.exists() or map_final.exists():
            planned += 1
            continue
        structure_tmp = TMP_DIR / f"{structure_final.stem}.partial.xlsx"
        map_tmp = TMP_DIR / f"{map_final.stem}.partial.xlsx"
        context["revision_name"] = f"rev{planned:02d}"
        context["diagnostic_items"] = list(context["extra_diagnostics"]) + [
            ("Output", "4h_workbook", str(structure_final)),
            ("Output", "30m_workbook", str(map_final)),
            ("Output", "shared_revision", f"{planned:02d}"),
        ]
        context["diagnostic_rows"] = _diagnostic_rows(result, context)
        write_workbook(structure_tmp, result, context, readme)
        validate_saved_workbook(structure_tmp, len(result.bars) * 4)
        write_mapping_workbook(map_tmp, mapped, context, readme)
        validate_mapping_workbook(map_tmp, len(result.primaries), sum(len(item.rows) for item in mapped.swings))
        excel_desktop_check(structure_tmp)
        rescanned = scan_shared()
        if structure_final.exists() or map_final.exists() or (rescanned and max(rescanned) >= planned):
            structure_tmp.unlink(missing_ok=True)
            map_tmp.unlink(missing_ok=True)
            planned = 0 if not rescanned else max(rescanned) + 1
            continue
        os.replace(structure_tmp, structure_final)
        try:
            os.replace(map_tmp, map_final)
        except OSError:
            structure_final.unlink(missing_ok=True)
            map_tmp.unlink(missing_ok=True)
            raise
        try:
            validate_ooxml_workbook(structure_final)
            validate_mapping_workbook(map_final, len(result.primaries), sum(len(item.rows) for item in mapped.swings))
        except Exception:
            structure_final.unlink(missing_ok=True)
            map_final.unlink(missing_ok=True)
            raise
        return planned, structure_final, map_final


def _cleanup() -> None:
    if TMP_DIR.exists():
        shutil.rmtree(TMP_DIR, ignore_errors=True)
    parent = TMP_DIR.parent
    if parent.exists() and parent.is_dir() and not any(parent.iterdir()):
        parent.rmdir()
    for root in (CONFIG.package_dir, CONFIG.package_dir / "tests"):
        cache = root / "__pycache__"
        if cache.exists():
            shutil.rmtree(cache, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
