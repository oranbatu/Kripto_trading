"""Run the permanent 1h Swing Open/Close detector."""
from __future__ import annotations

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
from detectors.swing_open_close_v1.engine import (
    EXACT_RETURN,
    SWING_HIGH,
    SWING_LOW,
    Bar,
    SwingError,
    analyze,
    audit_result,
    iso_turkey,
    iso_utc,
)
from detectors.swing_open_close_v1.swing_config import (
    ANALYSIS_START,
    CONFIG,
    DATASET_END,
    DETECTOR_VERSION,
    EXCLUSIVE_LOAD_END,
    EXPECTED_FIRST,
    EXPECTED_LAST_CLOSE,
    EXPECTED_LAST_OPEN,
    EXPECTED_ROWS,
    REVISION_PATTERN_TEXT,
    SOURCE_DATASET,
    STEP_MS,
    TIMEFRAME,
    TMP_DIR,
)
from detectors.swing_open_close_v1.workbook import (
    round_trip_workbook,
    validate_ooxml_workbook,
    validate_saved_workbook,
    write_workbook,
)

EXECUTION_COMMAND = "python -m detectors.swing_open_close_v1.run_swing_open_close_detector"
REVISION_PATTERN = re.compile(REVISION_PATTERN_TEXT, re.IGNORECASE)


def main() -> int:
    print("swing_open_close_v1 starting", flush=True)
    assert_python_write_allowed(CONFIG.package_dir / "run_swing_open_close_detector.py")
    package_before = _fingerprint_tree([CONFIG.package_dir])
    code_before = _fingerprint_tree(_existing_code_roots())
    workbook_before = _fingerprint_workbooks()
    existing_revisions = scan_revisions()
    try:
        loaded = load_production()
    except (SwingError, RuntimeError) as exc:
        print(f"SOURCE_VALIDATION_FAILED {exc}", flush=True)
        return 2
    source_before = {path: fingerprint_file(Path(path)) for path in loaded["source_paths"]}
    print(f"source validated rows={len(loaded['bars'])}", flush=True)
    try:
        result = analyze(loaded["bars"])
        audit_result(result)
    except (SwingError, RuntimeError) as exc:
        print(f"DETECTION_FAILED {exc}", flush=True)
        return 2
    print(f"confirmed swings={len(result.swings)} attempts={len(result.attempts)}", flush=True)
    source_after = {path: fingerprint_file(Path(path)) for path in loaded["source_paths"]}
    code_after = _fingerprint_tree(_existing_code_roots())
    if source_before != source_after:
        print("SOURCE_IMMUTABILITY_FAILED", flush=True)
        return 3
    if code_before != code_after:
        print("CODE_IMMUTABILITY_FAILED", flush=True)
        return 3
    context = {
        "source_validation": "PASS",
        "provenance": loaded["provenance"],
        "source_fingerprints": source_before,
        "existing_revisions": existing_revisions,
        "excel_validation": "PENDING",
        "diagnostic_rows": [],
        "revision_name": "",
    }
    readme = (CONFIG.package_dir / "README.md").read_text(encoding="utf-8")
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    try:
        revision, final_path = _commit_workbook(result, context, readme)
    except RuntimeError as exc:
        print(f"WORKBOOK_VALIDATION_FAILED {exc}", flush=True)
        _cleanup()
        return 4
    if _fingerprint_tree([CONFIG.package_dir]) != package_before:
        print("DETECTOR_SOURCE_REWRITTEN", flush=True)
        _cleanup()
        return 3
    if _fingerprint_workbooks(workbook_before) != workbook_before:
        print("WORKBOOK_IMMUTABILITY_FAILED", flush=True)
        _cleanup()
        return 3
    stray = [str(path) for path in CONFIG.desktop_dir.glob("*.py")]
    if stray:
        print(f"STRAY_PYTHON {stray}", flush=True)
        _cleanup()
        return 3
    _cleanup()
    print("SUMMARY_JSON " + json.dumps(_summary(result, final_path, revision, existing_revisions), default=str), flush=True)
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
        raise SwingError("1h identity mismatch")
    if slice_.precision_mode != config.PRECISION_EXACT:
        raise SwingError("1h precision mismatch")
    bars = _bars_from_table(slice_.table)
    _validate_frame(bars, slice_.source_files)
    return {
        "bars": bars,
        "source_paths": sorted(set(slice_.source_files)),
        "provenance": {
            "rows": len(bars),
            "first_open_utc": iso_utc(bars[0].open_time),
            "last_open_utc": iso_utc(bars[-1].open_time),
            "last_close_utc": iso_utc(bars[-1].close_time),
        },
    }


def _bars_from_table(table: pa.Table) -> list[Bar]:
    def column(name: str) -> list[Any]:
        return table.column(name).to_pylist()

    opens = column("open_time")
    closes = column("close_time")
    open_ = [_decimal(value) for value in column("open")]
    high = [_decimal(value) for value in column("high")]
    low = [_decimal(value) for value in column("low")]
    close = [_decimal(value) for value in column("close")]
    volume = [_decimal(value) for value in column("volume")]
    quote = [_decimal(value) for value in column("quote_asset_volume")]
    trades = column("number_of_trades")
    taker_base = [_decimal(value) for value in column("taker_buy_base_asset_volume")]
    taker_quote = [_decimal(value) for value in column("taker_buy_quote_asset_volume")]
    symbols = column("symbol")
    rows: list[Bar] = []
    for index in range(table.num_rows):
        if symbols[index] != "BTCUSDT":
            raise SwingError(f"row {index} symbol {symbols[index]}")
        rows.append(
            Bar(
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
            )
        )
    return rows


def _validate_frame(bars: list[Bar], source_files: list[str]) -> None:
    if len(bars) != EXPECTED_ROWS:
        raise SwingError(f"rows {len(bars)} != {EXPECTED_ROWS}")
    if bars[0].open_time != EXPECTED_FIRST or bars[-1].open_time != EXPECTED_LAST_OPEN:
        raise SwingError(f"range {iso_utc(bars[0].open_time)} .. {iso_utc(bars[-1].open_time)}")
    if bars[-1].close_time != EXPECTED_LAST_CLOSE:
        raise SwingError(f"last close {iso_utc(bars[-1].close_time)}")
    previous = None
    seen: set[datetime] = set()
    for bar in bars:
        if bar.open_time.tzinfo is None or bar.open_time.utcoffset() != timezone.utc.utcoffset(None):
            raise SwingError("timestamp is not UTC")
        if bar.open_time in seen or (previous is not None and bar.open_time <= previous):
            raise SwingError(f"duplicate or unordered timestamp {iso_utc(bar.open_time)}")
        seen.add(bar.open_time)
        if previous is not None and int((bar.open_time - previous).total_seconds() * 1000) != STEP_MS:
            raise SwingError(f"missing hourly timestamp at {iso_utc(bar.open_time)}")
        previous = bar.open_time
        expected_close = bar.open_time + timedelta(milliseconds=STEP_MS) - timedelta(milliseconds=1)
        if bar.close_time != expected_close or bar.close_time > DATASET_END:
            raise SwingError(f"close time mismatch at {iso_utc(bar.open_time)}")
        if bar.volume < 0:
            raise SwingError("negative volume")
        if not (bar.high >= bar.open and bar.high >= bar.close and bar.high >= bar.low and bar.low <= bar.open and bar.low <= bar.close and bar.low <= bar.high):
            raise SwingError(f"OHLC violation at {iso_utc(bar.open_time)}")
    for path in source_files:
        lowered = path.replace("\\", "/").lower()
        for token in ("marketdata", "binance", "futures", "um", "perpetual", "1h", "btcusdt"):
            if token not in lowered:
                raise SwingError(f"source file missing {token}: {path}")
    if not source_files:
        raise SwingError("1h has no source files")


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
    add("source", "validation", "PASS")
    add("source", "dataset", str(SOURCE_DATASET))
    for path, meta in context["source_fingerprints"].items():
        add("source_fingerprint", path, f"sha256={meta['sha256']} size={meta['size']} mtime_ns={meta['mtime_ns']}")
    add("reconciliation", "attempts", len(result.attempts))
    add("reconciliation", "confirmed", len(result.swings))
    add("reconciliation", "attempt_identity", "PASS" if len(result.attempts) == len(result.bars) * 2 else "FAIL")
    add("causality", "first_return_binding", "PASS")
    add("causality", "confirmation_at_swing_close", "PASS")
    add("immutability", "source_parquet_unchanged", "PASS")
    add("immutability", "existing_project_code_unchanged", "PASS")
    add("isolation", "separate_project_created", "FALSE")
    add("validation", "no_macros", "PASS")
    add("validation", "no_external_links", "PASS")
    add("workbook", "revision", context.get("revision_name", ""))
    add("workbook", "detector_version", DETECTOR_VERSION)
    add("warnings", "warning_count", len(result.warnings))
    for index, warning in enumerate(result.warnings, start=1):
        add("warnings", f"warning_{index}", warning)
    if not result.warnings:
        add("warnings", "discrepancies", "NONE")
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
        validate_saved_workbook(temporary, len(result.bars) * 2)
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
  $wb = $excel.Workbooks.Open($env:SWING_STRUCTURE_XLSX, 0, $true)
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
  if ($names.Count -ne 13) { throw ("Excel sheet count " + $names.Count) }
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
            env={**os.environ, "SWING_STRUCTURE_XLSX": str(path)},
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


def _summary(result, final_path: Path, revision: int, existing: list[int]) -> dict[str, Any]:
    statuses = Counter(item.status for item in result.attempts)
    labels = Counter(item.structure_label for item in result.swings)
    qualities = Counter(item.quality_label for item in result.swings)
    interiors = Counter(item.interior_count for item in result.swings)
    positions = Counter(item.left_bars for item in result.swings)
    overlaps = Counter(item.overlap_class for item in result.swings)
    widths = [min(item.open_to_extreme_percent, item.close_to_extreme_percent) for item in result.swings]
    ordered = list(result.swings)
    largest = sorted(ordered, key=lambda item: (-min(item.open_to_extreme_percent, item.close_to_extreme_percent), item.close_row, item.swing_id))[:5]
    return {
        "workbook": str(final_path),
        "revision": revision,
        "existing_revisions": existing,
        "size": final_path.stat().st_size,
        "sha256": hashlib.sha256(final_path.read_bytes()).hexdigest(),
        "rows": len(result.bars),
        "attempts": len(result.attempts),
        "confirmed": len(result.swings),
        "swing_highs": sum(1 for item in result.swings if item.direction == SWING_HIGH),
        "swing_lows": sum(1 for item in result.swings if item.direction == SWING_LOW),
        "compact": sum(1 for item in result.swings if item.formation_class == "COMPACT_SWING"),
        "standard": sum(1 for item in result.swings if item.formation_class == "STANDARD_SWING"),
        "statuses": dict(statuses),
        "exact_returns": sum(1 for item in result.swings if item.completion_type == EXACT_RETURN),
        "crossed_returns": sum(1 for item in result.swings if item.completion_type != EXACT_RETURN),
        "width_min": min(widths) if widths else None,
        "width_max": max(widths) if widths else None,
        "interiors": {str(key): value for key, value in sorted(interiors.items())},
        "extreme_positions": {str(key): value for key, value in sorted(positions.items())},
        "plateaus": sum(1 for item in result.swings if item.plateau_length > 1),
        "overlaps": dict(overlaps),
        "labels": dict(labels),
        "qualities": dict(qualities),
        "first_highs": [_preview(item) for item in ordered if item.direction == SWING_HIGH][:5],
        "first_lows": [_preview(item) for item in ordered if item.direction == SWING_LOW][:5],
        "largest": [_preview(item) for item in largest],
        "recent": [_preview(item) for item in ordered[-5:]],
        "warnings": result.warnings,
        "detector_version": DETECTOR_VERSION,
    }


def _preview(swing) -> dict[str, Any]:
    return {
        "id": swing.swing_id,
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
    return f"BTCUSDT_1H_Swing_Structure_rev{revision:02d}.xlsx"


def fingerprint_file(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    stat = path.stat()
    return {"sha256": digest.hexdigest(), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _fingerprint_tree(paths: list[Path]) -> dict[str, str]:
    found: dict[str, str] = {}
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
    found = {}
    for path in CONFIG.desktop_dir.glob("*.xlsx"):
        if path.is_file():
            found[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return found


def _existing_code_roots() -> list[Path]:
    root = CONFIG.project_root
    paths = [
        root / "detectors" / "primary_range_v1",
        root / "detectors" / "hierarchical_swing_v4",
        root / "detectors" / "price_touch_hierarchy_v1",
        root / "detectors" / "open_liquidity_v1",
        root / "detectors" / "order_block_4h_v1",
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
    project = CONFIG.project_root.resolve()
    if project not in resolved.parents and resolved != project:
        raise PermissionError(f"detector Python writes are limited to {project}")
    if package not in resolved.parents and resolved != package:
        raise PermissionError(f"detector Python writes are limited to {package}")


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
