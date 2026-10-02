"""Frozen configuration for the open-liquidity detector."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parents[1]
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "open_liquidity_v1"
SOURCE_ROOT = Path(r"C:\MarketData\derived\binance\futures\um\perpetual")

DETECTOR_VERSION = "BTCUSDT_MTF_OPEN_LIQUIDITY_V1"
IMPLEMENTATION_MODE = "PERMANENT_SCRIPT_INSIDE_EXISTING_PROJECT"
TIMEFRAMES = ("1h", "4h", "1d")
DISPLAY_TZ = ZoneInfo("Europe/Istanbul")

ANALYSIS_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DATASET_END = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=timezone.utc)
EXCLUSIVE_LOAD_END = datetime(2026, 9, 16, tzinfo=timezone.utc)
EXPECTED_ROWS = {"1h": 23736, "4h": 5934, "1d": 989}
EXPECTED_CANDIDATES = 61318
EXPECTED_FIRST = {tf: ANALYSIS_START for tf in TIMEFRAMES}
EXPECTED_LAST = {
    "1h": datetime(2026, 9, 15, 23, tzinfo=timezone.utc),
    "4h": datetime(2026, 9, 15, 20, tzinfo=timezone.utc),
    "1d": datetime(2026, 9, 15, tzinfo=timezone.utc),
}
DURATION_MS = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
SOURCE_DATASET = {tf: SOURCE_ROOT / tf / "symbol=BTCUSDT" for tf in TIMEFRAMES}
PREFIX = {"1h": "1H", "4h": "4H", "1d": "1D"}

MATCH_TOLERANCE = "0.10"
MAX_GROUP_SPAN = "0.20"
NEAR_MISS_PERCENT = "0.05"
REVISION_PATTERN_TEXT = r"^BTCUSDT_MTF_Open_Liquidity_rev(\d{2,})\.xlsx$"
WORKBOOK_SHEETS = (
    "Executive Summary",
    "Hierarchical Open",
    "1D Open Liquidity",
    "4H Open Liquidity",
    "1H Open Liquidity",
    "Nearest Above Below",
    "Candidate Audit",
    "Mitigation Events",
    "Cross-TF Mapping",
    "Monthly Origins",
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


@dataclass(frozen=True, slots=True)
class LiquidityConfig:
    project_root: Path = PROJECT_ROOT
    package_dir: Path = PACKAGE_DIR
    desktop_dir: Path = DESKTOP_DIR
    temporary_dir: Path = TMP_DIR
    detector_version: str = DETECTOR_VERSION


CONFIG = LiquidityConfig()
