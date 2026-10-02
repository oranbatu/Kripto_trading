"""Frozen configuration for the hierarchical swing detector.

Thresholds and paths live here. Engines must not mutate this object.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parents[1]
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "hierarchical_swing_v4"

SOURCE_DATASET = Path(
    r"C:\MarketData\derived\binance\futures\um\perpetual\1h\symbol=BTCUSDT"
)

DETECTOR_VERSION = "BTCUSDT_1H_HIERARCHICAL_SWING_V4_ASYMMETRIC_FORMATION"
IMPLEMENTATION_MODE = "PERMANENT_IN_PROJECT"

ISTANBUL = ZoneInfo("Europe/Istanbul")
UTC = timezone.utc


def _utc(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class SwingConfig:
    source_dataset: Path = SOURCE_DATASET
    output_dir: Path = DESKTOP_DIR
    project_root: Path = PROJECT_ROOT
    package_dir: Path = PACKAGE_DIR
    temporary_dir: Path = TMP_DIR
    symbol: str = "BTCUSDT"
    timeframe: str = "1h"
    provider: str = "binance"
    market: str = "usd_m_perpetual"
    archive_namespace: str = "futures/um"
    contract_type: str = "PERPETUAL"
    tradingview_symbol: str = "BINANCE:BTCUSDT.P"
    precision_mode: str = "exact"
    analysis_start_utc: datetime = _utc(2026, 1, 1, 0)
    # Inclusive last open. The loader uses a half-open end one hour later.
    analysis_end_utc: datetime = _utc(2026, 9, 15, 23)
    expected_rows: int = 6192
    atr_period: int = 14
    minimum_total_formation_candles: int = 5
    maximum_total_formation_candles: int = 10
    minimum_interior_candles: int = 3
    maximum_interior_candles: int = 8
    pivot_span_mode: str = "FORMATION_DERIVED_ASYMMETRIC"
    fixed_pivot_left_bars: str = "NONE"
    fixed_pivot_right_bars: str = "NONE"
    extreme_position_rule: str = "CENTRAL_THREE_FORMATION_POSITIONS"
    formation_reference: str = "SWING_OPEN_CANDLE_CLOSE"
    minimum_outbound_percent: Decimal = Decimal("1.00")
    minimum_return_percent: Decimal = Decimal("1.00")
    formation_confirmation: str = "SWING_CLOSE_CANDLE_COMPLETION"
    internal_bootstrap_enabled: bool = True
    internal_minimum_displacement_atr: Decimal = Decimal("2.0")
    internal_reversal_atr: Decimal = Decimal("1.0")
    internal_minimum_spacing_bars: int = 6
    major_bootstrap_enabled: bool = True
    major_minimum_displacement_atr: Decimal = Decimal("3.0")
    major_minimum_displacement_percent: Decimal = Decimal("1.50")
    major_reversal_atr: Decimal = Decimal("1.50")
    major_reversal_percent: Decimal = Decimal("0.75")
    major_minimum_spacing_bars: int = 12
    major_minimum_prominence_atr: Decimal = Decimal("1.25")
    reversal_evaluation_frequency: str = "EVERY_COMPLETED_BAR"
    candidate_timeout: str = "NONE"
    equality_tolerance_atr: Decimal = Decimal("0.05")
    retest_tolerance_atr: Decimal = Decimal("0.25")
    detector_version: str = DETECTOR_VERSION
    implementation_mode: str = IMPLEMENTATION_MODE
    hour_ms: int = 3_600_000


CONFIG = SwingConfig()

EXPECTED_MONTHLY_ROWS = {
    "2026-01": 744,
    "2026-02": 672,
    "2026-03": 744,
    "2026-04": 720,
    "2026-05": 744,
    "2026-06": 720,
    "2026-07": 744,
    "2026-08": 744,
    "2026-09": 360,
}

MONTHS = tuple(EXPECTED_MONTHLY_ROWS)

REVISION_PATTERN_TEXT = r"^BTCUSDT_1H_swing_rev(\d{2,})\.xlsx$"
WORKBOOK_SHEETS = (
    "Swing Summary",
    "Major Swing Points",
    "Major Swing Highs",
    "Major Swing Lows",
    "Major Swing Legs",
    "Internal Swings",
    "Raw Pivot Candidates",
    "Raw Formations",
    "Formation Candles",
    "State Transitions",
    "Monthly Diagnostics",
    "Forward Evaluation",
    "Parameters",
    "Diagnostics",
    "README",
)
