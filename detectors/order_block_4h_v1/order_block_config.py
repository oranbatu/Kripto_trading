"""Frozen configuration for the 4h body-based Order Block detector."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parents[1]
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "order_block_4h_v1"
SOURCE_DATASET = Path(r"C:\MarketData\derived\binance\futures\um\perpetual\4h\symbol=BTCUSDT")

DETECTOR_VERSION = "BTCUSDT_4H_BODY_ORDER_BLOCK_V1"
TIMEFRAME = "4h"
DISPLAY_TZ = ZoneInfo("Europe/Istanbul")
BAR_HOURS = 4
STEP_MS = 14_400_000
LOOKBACK = 20
MINIMUM_DISPLACEMENT_PERCENT = "1.00"

ANALYSIS_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DATASET_END = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=timezone.utc)
EXCLUSIVE_LOAD_END = datetime(2026, 9, 16, tzinfo=timezone.utc)
EXPECTED_ROWS = 5934
EXPECTED_PAIRS = 5933
EXPECTED_FIRST = ANALYSIS_START
EXPECTED_LAST_OPEN = datetime(2026, 9, 15, 20, tzinfo=timezone.utc)
EXPECTED_LAST_CLOSE = DATASET_END

REVISION_PATTERN_TEXT = r"^BTCUSDT_4H_Order_Blocks_rev(\d{2,})\.xlsx$"
WORKBOOK_SHEETS = (
    "Executive Summary",
    "All Order Blocks",
    "Bullish Order Blocks",
    "Bearish Order Blocks",
    "Active Untouched",
    "Body Mitigated",
    "Invalidated",
    "Lifecycle Events",
    "Candidate Audit",
    "Overlap Analysis",
    "Parameters",
    "Diagnostics",
    "README",
)
PLAIN_RANGE_SHEETS = frozenset({"Executive Summary", "Diagnostics", "README"})
TABLE_NAMES = {
    "All Order Blocks": "tblAllOrderBlocks",
    "Bullish Order Blocks": "tblBullishOrderBlocks",
    "Bearish Order Blocks": "tblBearishOrderBlocks",
    "Active Untouched": "tblActiveUntouched",
    "Body Mitigated": "tblBodyMitigated",
    "Invalidated": "tblInvalidated",
    "Lifecycle Events": "tblLifecycleEvents",
    "Candidate Audit": "tblCandidateAudit",
    "Overlap Analysis": "tblOverlapAnalysis",
    "Parameters": "tblParameters",
}


@dataclass(frozen=True, slots=True)
class OrderBlockConfig:
    project_root: Path = PROJECT_ROOT
    package_dir: Path = PACKAGE_DIR
    desktop_dir: Path = DESKTOP_DIR
    temporary_dir: Path = TMP_DIR
    detector_version: str = DETECTOR_VERSION


CONFIG = OrderBlockConfig()
