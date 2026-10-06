"""Configuration for the 4h Special Swing detector."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIR = Path(__file__).resolve().parent
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "swing_open_close_4h_special_v1"
REPORT_PATH = PROJECT_ROOT / "reports" / "special_4h_swing_report.json"
LOG_PATH = PROJECT_ROOT / "logs" / "special_4h_swing.log"
DATASET = r"C:\MarketData\derived\binance\futures\um\perpetual\4h\symbol=BTCUSDT"

DETECTOR_VERSION = "BTCUSDT_4H_SPECIAL_SWING_V1"
CONFIGURATION_VERSION = "SPECIAL_ZERO_REFERENCE_PLUS_STANDARD_1_TO_5_ONE_THIRD_BODY_NO_MAX_BOUNDARY_V2"
DETECTION_METHOD = "STANDARD_REFERENCE_RETURN_SPECIAL"
TIMEFRAME = "4h"
DISPLAY_TZ = ZoneInfo("Europe/Istanbul")

ANALYSIS_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ANALYSIS_END_OPEN = datetime(2026, 9, 15, 20, tzinfo=timezone.utc)
ANALYSIS_END_CLOSE = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=timezone.utc)
EXCLUSIVE_LOAD_END = datetime(2026, 9, 16, tzinfo=timezone.utc)
EXPECTED_ROWS = 1548
EXPECTED_MONTHLY = {
    (2026, 1): 186,
    (2026, 2): 168,
    (2026, 3): 186,
    (2026, 4): 180,
    (2026, 5): 186,
    (2026, 6): 180,
    (2026, 7): 186,
    (2026, 8): 186,
    (2026, 9): 90,
}
STEP_MS = 14_400_000

STANDARD_ENABLED = True
NORMAL_STANDARD_ENABLED = True
ZERO_EXCEPTION_ENABLED = True
ZERO_USES_FULL_RETURN = True
ZERO_USES_ONE_THIRD = False
NORMAL_USES_FULL_RETURN = False
NORMAL_USES_ONE_THIRD = True
COMPACT_ENABLED = False
ALTERNATIVE_ENABLED = False
NORMAL_MIN_INTERIOR = 1
MAX_INTERIOR = 5
MIN_TOTAL = 2
MAX_TOTAL = 7
ZERO_CLASS = "ZERO_INTERIOR_TWO_CANDLE_STANDARD"
STANDARD_CLASS = "STANDARD_SPECIAL"
MIN_BOUNDARY = "1.30"
MAX_BOUNDARY = None

SWING_HIGH = "SWING_HIGH"
SWING_LOW = "SWING_LOW"
PRIMARY = "PRIMARY"
DERIVED = "DERIVED_SAME_EXTREME"
NESTED = "SUPPRESSED_NESTED_STRUCTURE"
LONGER = "SUPPRESSED_LONGER_OVERLAPPING_STRUCTURE"

REVISION_PATTERN = r"^BTCUSDT_4H_Special_Swings_rev(\d{2,})\.xlsx$"
SHEETS = ("Special Swings", "Swing Highs", "Swing Lows", "Parameters")
RESULT_COLUMNS = (
    "Swing ID",
    "Swing Type",
    "Formation Class",
    "Interior Candle Count",
    "Total Formation Candle Count",
    "Swing Open Open Time Turkey",
    "Swing Open Open Price",
    "Swing Open High",
    "Swing Open Low",
    "Swing Close Open Time Turkey",
    "Swing Close Close Time Turkey",
    "Swing Close Close Price",
    "Swing Close High",
    "Swing Close Low",
    "Extremum Time Turkey",
    "Extremum Type",
    "Extremum Price",
    "Structure High",
    "Structure Low",
    "Open Boundary Percent",
    "Close Boundary Percent",
)
PROTECTED_ROOT = PROJECT_ROOT / "detectors" / "swing_open_close_4h_v1"
MAPPING_ROOTS = (
    PROJECT_ROOT / "detectors" / "swing_open_close_4h_with_30m_v1",
    PROJECT_ROOT / "detectors" / "swing_open_close_4h_with_1h_v1",
    PROJECT_ROOT / "detectors" / "swing_open_close_4h_with_15m_v1",
)

USER_PARAMETERS = (
    ("Symbol", "BTCUSDT"),
    ("TradingView Symbol", "BINANCE:BTCUSDT.P"),
    ("Timeframe", "4h"),
    ("Analysis Period", "2026-01-01 through 2026-09-15"),
    ("Alternative Method", "Disabled"),
    ("Compact Method", "Disabled"),
    ("Zero-Interior Two-Candle Structures", "Enabled"),
    ("Zero-Interior Confirmation", "Full Return to Swing Open Open"),
    ("Zero-Interior One-Third Body Rule", "Disabled"),
    ("Zero-Interior Extreme Source", "Lowest Low or Highest High of Endpoint Candles"),
    ("Zero-Interior Opposite Directions Required", "Yes"),
    ("Normal Standard Interior Candles", "1–5"),
    ("Normal Standard Confirmation", "Minimum One-Third Swing Open Body Penetration"),
    ("Swing Open Wicks Used for Body Penetration", "No"),
    ("Required Body Penetration", "1/3"),
    ("Exact One-Third Penetration Passes", "Yes"),
    ("Maximum Search Interior Candles", "5"),
    ("Minimum Boundary Percent", "1.30%"),
    ("Maximum Boundary Percent", "None — No Upper Limit"),
    ("Both Minimum Boundaries Required", "Yes"),
    ("Boundary Above 3.50% Allowed", "Yes"),
    ("Primary Rule", "Minimum Valid Candle Count Wins"),
    ("Nested Structures Displayed", "No"),
    ("Derived Structures Displayed", "No"),
    ("Display Timezone", "Europe/Istanbul"),
    ("Detector Version", DETECTOR_VERSION),
    ("Configuration Version", CONFIGURATION_VERSION),
)
