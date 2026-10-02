"""Frozen configuration for the price-touch hierarchy detector.

Thresholds and zone geometry are constants. They are not adjusted after a run.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal
from zoneinfo import ZoneInfo

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parents[1]
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "price_touch_hierarchy_v1"
SOURCE_ROOT = Path(r"C:\MarketData\derived\binance\futures\um\perpetual")

DETECTOR_VERSION = "BTCUSDT_MTF_PRICE_TOUCH_HIERARCHY_V1"
IMPLEMENTATION_MODE = "PERMANENT_SCRIPT_INSIDE_EXISTING_PROJECT"

TIMEFRAMES = ("1h", "4h", "1d")
TIMEFRAME_WEIGHT = {"1d": Decimal("4"), "4h": Decimal("2"), "1h": Decimal("1")}
MIN_TOUCHES = {"1h": 40, "4h": 20, "1d": 10}
MIN_EPISODES = {"1h": 3, "4h": 3, "1d": 3}
MIN_DISTINCT_DATES = {"1h": 3, "4h": 3, "1d": 3}
TOUCH_REJECTION = {
    "1h": "1H_TOUCH_COUNT_BELOW_40",
    "4h": "4H_TOUCH_COUNT_BELOW_20",
    "1d": "1D_TOUCH_COUNT_BELOW_10",
}

ANALYSIS_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DATASET_END = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=timezone.utc)
EXCLUSIVE_LOAD_END = datetime(2026, 9, 16, tzinfo=timezone.utc)

EXPECTED_ROWS = {"1h": 23736, "4h": 5934, "1d": 989}
EXPECTED_FIRST = {tf: ANALYSIS_START for tf in TIMEFRAMES}
EXPECTED_LAST = {
    "1h": datetime(2026, 9, 15, 23, tzinfo=timezone.utc),
    "4h": datetime(2026, 9, 15, 20, tzinfo=timezone.utc),
    "1d": datetime(2026, 9, 15, tzinfo=timezone.utc),
}
DURATION_MS = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}

SOURCE_DATASET = {
    "1h": SOURCE_ROOT / "1h" / "symbol=BTCUSDT",
    "4h": SOURCE_ROOT / "4h" / "symbol=BTCUSDT",
    "1d": SOURCE_ROOT / "1d" / "symbol=BTCUSDT",
}

GRID_ANCHOR = Decimal("1.00000000")
GRID_GROWTH = Decimal("1.001")
GRID_STEP_PERCENT = Decimal("0.10")
FINAL_HALF_WIDTH = Decimal("0.0010")
SAME_TF_SEPARATION = Decimal("0.50")
CROSS_TF_TOLERANCE = Decimal("0.30")
MAX_GROUP_SPAN = Decimal("0.60")
PEAK_RADIUS = 5
SMOOTHING = (Decimal("0.25"), Decimal("0.50"), Decimal("0.25"))
TOP_TOUCH_GROUPS = 100
DISPLAY_TZ = ZoneInfo("Europe/Istanbul")

REVISION_PATTERN_TEXT = r"^BTCUSDT_MTF_Most_Touched_Levels_rev(\d{2,})\.xlsx$"

WORKBOOK_SHEETS = (
    "Executive Summary",
    "Hierarchical Ranking",
    "1D Levels",
    "4H Levels",
    "1H Levels",
    "Cross-TF Mapping",
    "Top Level Touches",
    "Monthly Persistence",
    "Suppressed Levels",
    "Rejected Candidates",
    "Parameters",
    "Diagnostics",
    "README",
)

TIER_ORDER = (
    "TIER_1_ALL_TIMEFRAMES",
    "TIER_2_DAILY_WITH_INTRADAY",
    "TIER_3_DAILY_ONLY",
    "TIER_4_4H_AND_1H",
    "TIER_5_4H_ONLY",
    "TIER_6_1H_ONLY",
)
TIER_RANK = {name: index for index, name in enumerate(TIER_ORDER)}

SCORE_WEIGHTS = {
    "touch": Decimal("0.60"),
    "episode": Decimal("0.20"),
    "persistence": Decimal("0.15"),
    "engagement": Decimal("0.05"),
}


@dataclass(frozen=True, slots=True)
class TouchConfig:
    project_root: Path = PROJECT_ROOT
    package_dir: Path = PACKAGE_DIR
    desktop_dir: Path = DESKTOP_DIR
    temporary_dir: Path = TMP_DIR
    detector_version: str = DETECTOR_VERSION
    implementation_mode: str = IMPLEMENTATION_MODE


CONFIG = TouchConfig()
