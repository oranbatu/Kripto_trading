"""Frozen configuration for the 1h Swing Open/Close detector."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parents[1]
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "swing_open_close_v1"
SOURCE_DATASET = Path(r"C:\MarketData\derived\binance\futures\um\perpetual\1h\symbol=BTCUSDT")

DETECTOR_VERSION = "BTCUSDT_1H_SWING_OPEN_CLOSE_V1"
TIMEFRAME = "1h"
DISPLAY_TZ = ZoneInfo("Europe/Istanbul")
STEP_MS = 3_600_000
BAR_HOURS = 1

ANALYSIS_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DATASET_END = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=timezone.utc)
EXCLUSIVE_LOAD_END = datetime(2026, 9, 16, tzinfo=timezone.utc)
EXPECTED_ROWS = 23736
EXPECTED_FIRST = ANALYSIS_START
EXPECTED_LAST_OPEN = datetime(2026, 9, 15, 23, tzinfo=timezone.utc)
EXPECTED_LAST_CLOSE = DATASET_END

COMPACT_MIN_INTERIOR = 1
COMPACT_MAX_INTERIOR = 5
STANDARD_MIN_INTERIOR = 6
STANDARD_MAX_INTERIOR = 15
MAX_INTERIOR = 15
COMPACT_WIDTH = "3.50"
STANDARD_WIDTH = "2.00"
EQUALITY_TOLERANCE_PERCENT = "0.05"
DIRECTION_PRIORITY = "SWING_HIGH_THEN_SWING_LOW"

REVISION_PATTERN_TEXT = r"^BTCUSDT_1H_Swing_Structure_rev(\d{2,})\.xlsx$"
WORKBOOK_SHEETS = (
    "Executive Summary",
    "Confirmed Swings",
    "Swing Highs",
    "Swing Lows",
    "Compact Swings",
    "Standard Swings",
    "Formation Candles",
    "Candidate Audit",
    "Overlap Analysis",
    "Forward Evaluation",
    "Parameters",
    "Diagnostics",
    "README",
)
PLAIN_RANGE_SHEETS = frozenset({
    "Executive Summary",
    "Formation Candles",
    "Candidate Audit",
    "Diagnostics",
    "README",
})
TABLE_NAMES = {
    "Confirmed Swings": "tblConfirmedSwings",
    "Swing Highs": "tblSwingHighs",
    "Swing Lows": "tblSwingLows",
    "Compact Swings": "tblCompactSwings",
    "Standard Swings": "tblStandardSwings",
    "Overlap Analysis": "tblOverlapAnalysis",
    "Forward Evaluation": "tblForwardEvaluation",
    "Parameters": "tblParameters",
}


@dataclass(frozen=True, slots=True)
class SwingConfig:
    project_root: Path = PROJECT_ROOT
    package_dir: Path = PACKAGE_DIR
    desktop_dir: Path = DESKTOP_DIR
    temporary_dir: Path = TMP_DIR
    detector_version: str = DETECTOR_VERSION


CONFIG = SwingConfig()
