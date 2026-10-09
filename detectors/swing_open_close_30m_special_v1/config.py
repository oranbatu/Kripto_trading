"""Configuration for the separate BTCUSDT 30-minute special swing detector."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIR = Path(__file__).resolve().parent
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "swing_open_close_30m_special_v1"
REPORT_PATH = PROJECT_ROOT / "reports" / "special_30m_swing_report.json"
LOG_PATH = PROJECT_ROOT / "logs" / "special_30m_swing.log"
DATASET = Path(r"C:\MarketData\derived\binance\futures\um\perpetual\30m\symbol=BTCUSDT")

DETECTOR_NAME = "BTCUSDT 30M Swing Special"
DETECTOR_VERSION = "BTCUSDT_30M_SPECIAL_SWING_V1"
CONFIGURATION_VERSION = "STANDARD_6_TO_9_DIRECTIONAL_REFERENCE_PAIR_NOT_PRE_CLOSE_COMPATIBILITY_V10"
FORMATION_CLASS = "STANDARD_30M_SPECIAL"
TIMEFRAME = "30m"
SYMBOL = "BTCUSDT"
DISPLAY_TZ = ZoneInfo("Europe/Istanbul")

ANALYSIS_START = datetime(2026, 7, 1, tzinfo=timezone.utc)
ANALYSIS_END_OPEN = datetime(2026, 9, 15, 23, 30, tzinfo=timezone.utc)
ANALYSIS_END_CLOSE = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=timezone.utc)
EXCLUSIVE_LOAD_END = datetime(2026, 9, 16, tzinfo=timezone.utc)
EXPECTED_ROWS = 3696
EXPECTED_MONTHLY = {(2026, 7): 1488, (2026, 8): 1488, (2026, 9): 720}
STEP_MS = 1_800_000

STANDARD_ENABLED = True
ZERO_INTERIOR_ENABLED = False
COMPACT_ENABLED = False
ALTERNATIVE_ENABLED = False
MIN_INTERIOR = 6
MAX_INTERIOR = 9
MIN_TOTAL = 8
MAX_TOTAL = 11
FIRST_CLOSE_OFFSET = 7
LAST_CLOSE_OFFSET = 10
MIN_BOUNDARY = Decimal("0.90")
MAX_BOUNDARY = None
REQUIRED_FRACTION = Decimal(1) / Decimal(3)

SWING_HIGH = "SWING_HIGH"
SWING_LOW = "SWING_LOW"
PRIMARY = "PRIMARY"
DERIVED = "DERIVED_SAME_EXTREME"
NESTED = "SUPPRESSED_NESTED_STRUCTURE"
LONGER = "SUPPRESSED_LONGER_OVERLAPPING_STRUCTURE"

REVISION_PATTERN = r"^BTCUSDT_30M_Special_Swings_rev(\d{2,})\.xlsx$"
SHEETS = ("Special Swings", "Swing Highs", "Swing Lows", "Parameters")
RESULT_COLUMNS = (
    "Swing ID",
    "Swing Type",
    "Formation Class",
    "Interior Candle Count",
    "Total Formation Candle Count",
    "Swing Open Open Time Turkey",
    "Swing Open Close Time Turkey",
    "Swing Open Direction",
    "Swing Open Open Price",
    "Swing Open High",
    "Swing Open Low",
    "Swing Open Close",
    "Swing Close Open Time Turkey",
    "Swing Close Close Time Turkey",
    "Swing Close Direction",
    "Swing Close Open",
    "Swing Close High",
    "Swing Close Low",
    "Swing Close Close",
    "Earlier Swing Close Candidates Rejected By Reference Compatibility",
    "Extremum Time Turkey",
    "Extremum Type",
    "Extremum Price",
    "Interior Body Reference Status",
    "Interior Body Reference Pair Type",
    "Interior Body Reference Selection Rule",
    "Eligible Interior Reference Pair Count",
    "Interior Body Reference Time Turkey",
    "Interior Body Reference Candle Direction",
    "Interior Body Reference Open",
    "Interior Body Reference High",
    "Interior Body Reference Low",
    "Interior Body Reference Close",
    "Interior Body Reference Price",
    "Interior Body Reference Validation Time Turkey",
    "Interior Body Reference Validation Direction",
    "Interior Body Reference Validation Open",
    "Interior Body Reference Validation High",
    "Interior Body Reference Validation Low",
    "Interior Body Reference Validation Close",
    "Interior Body Reference Is Immediately Before Swing Close",
    "Interior Body Reference Rows Before Swing Close",
    "Reference To Swing Close Compatibility Status",
    "Reference To Swing Close Margin",
    "Reference To Swing Close Margin Percent",
    "Reference To Validation Row Difference",
    "Reference To Validation Close Change",
    "Reference To Validation Close Change Percent",
    "Interior Body Reference Bars After Swing Open",
    "Interior Body Reference Position Fraction",
    "Interior Body Reference Validation Position Fraction",
    "Interior Body Reference Tie Count",
    "Interior Body Reference Matches Extremum Candle",
    "Validation Candle Matches Extremum Candle",
    "Extremum To Body Reference Distance",
    "Structure High",
    "Structure Low",
    "Open Boundary Percent",
    "Close Boundary Percent",
)

USER_PARAMETERS = (
    ("Symbol", "BTCUSDT"),
    ("TradingView Symbol", "BINANCE:BTCUSDT.P"),
    ("Market", "Binance USD-M Perpetual"),
    ("Timeframe", "30m"),
    ("Analysis Start", "2026-07-01"),
    ("Analysis End", "2026-09-15"),
    ("Standard Method", "Enabled"),
    ("Zero-Interior Method", "Disabled"),
    ("Alternative Method", "Disabled"),
    ("Compact Method", "Disabled"),
    ("Minimum Interior Candles", "6"),
    ("Maximum Interior Candles", "9"),
    ("Minimum Total Formation Candles", "8"),
    ("Maximum Total Formation Candles", "11"),
    ("First Eligible Swing Close Offset", "open_row + 7"),
    ("Final Eligible Swing Close Offset", "open_row + 10"),
    ("Bullish Candle Definition", "Close > Open"),
    ("Bearish Candle Definition", "Close < Open"),
    ("Doji Definition", "Close = Open"),
    ("Doji Allowed As Swing Open", "No"),
    ("Doji Allowed As Swing Close", "No"),
    ("Swing High Swing Open Direction", "Bullish"),
    ("Swing High Swing Close Direction", "Bearish"),
    ("Swing Low Swing Open Direction", "Bearish"),
    ("Swing Low Swing Close Direction", "Bullish"),
    ("Wrong-Direction Threshold Crossing Binds", "No"),
    ("Search Continues After Wrong Direction", "Yes, Within 6–9 Interior Horizon"),
    ("Confirmation Rule", "Required Direction, One-Third Body Penetration, And Reference Compatibility When A Pair Exists"),
    ("Required Body Penetration", "1/3"),
    ("Swing Open Wicks Used", "No"),
    ("Wick-Only Confirmation", "Not Allowed"),
    ("Full Return to Swing Open Open Required", "No"),
    ("First Eligible Directional Penetration Binding", "Yes, After Reference Compatibility"),
    ("Minimum Boundary Percent", "0.90%"),
    ("Maximum Boundary Percent", "None — No Upper Limit"),
    ("Both Minimum Boundaries Required", "Yes"),
    ("Interior Body Reference Enabled", "Yes"),
    ("Interior Body Reference Is Hard Filter", "No"),
    ("Both Reference Pair Candles Strictly Interior", "Yes"),
    ("Reference Pair Must Be Consecutive", "Yes"),
    ("Reference-To-Validation Row Difference", "1"),
    ("Swing Open Eligible In Reference Pair", "No"),
    ("Swing Close Eligible In Reference Pair", "No"),
    ("Final Interior Candle Eligible As Reference", "No"),
    ("Pre-Close Interior Candle Eligible As Reference", "No"),
    ("Swing Close Eligible As Validation Candle", "No"),
    ("Reference Row Maximum", "close_row - 2"),
    ("Swing High Reference Direction", "Bearish"),
    ("Swing High Validation Direction", "Bullish"),
    ("Swing High Reference Selection", "Lowest Bearish Close With Immediate Bullish Interior Successor"),
    ("Swing High Reference-To-Close Rule", "Reference Close >= Swing Close Close"),
    ("Swing Low Reference Direction", "Bullish"),
    ("Swing Low Validation Direction", "Bearish"),
    ("Swing Low Reference Selection", "Highest Bullish Close With Immediate Bearish Interior Successor"),
    ("Swing Low Reference-To-Close Rule", "Reference Close <= Swing Close Close"),
    ("Reference OHLC Exported", "Open, High, Low, Close"),
    ("Exact Equality Passes", "Yes"),
    ("Compatibility Failure Binds", "No"),
    ("Search Beyond Nine Interiors", "No"),
    ("Second-Best Reference Substitution", "No"),
    ("Missing Pair Compatibility", "Not Applicable"),
    ("Reference Wicks Used For Selection", "No"),
    ("Failed Compatibility Binds", "No"),
    ("Search Continues After Compatibility Failure", "Yes"),
    ("Search Extension Beyond Nine Interiors", "No"),
    ("Reference Recalculated For Every Close", "Yes"),
    ("Second-Best Reference May Replace Failed Min/Max", "No"),
    ("Missing Reference Pair Rejects Swing", "No"),
    ("Missing Pair Compatibility Status", "Not Applicable"),
    ("Reference Validation Offset", "1"),
    ("Pair Is Primary Selection Input", "No"),
    ("Reference Price Field", "Reference Candle Close"),
    ("Validation Candle Close Used For Selection", "No"),
    ("Reference Wicks Used", "No"),
    ("Doji Eligible In Reference Pair", "No"),
    ("Pair Tie Rule", "Last Chronological Eligible Pair"),
    ("Missing Pair Rejects Swing", "No"),
    ("Geometric Middle Required", "No"),
    ("Primary Rule", "Minimum Valid Candle Count Wins"),
    ("Nested Structures Displayed", "No"),
    ("Derived Structures Displayed", "No"),
    ("Display Timezone", "Europe/Istanbul"),
    ("Detector Version", DETECTOR_VERSION),
    ("Configuration Version", CONFIGURATION_VERSION),
)

FORBIDDEN_TERMINALS = frozenset({
    "INTERIOR_COUNT_BELOW_7",
    "INTERIOR_COUNT_ABOVE_10",
    "SEARCH_NOT_YET_ELIGIBLE_BELOW_7_INTERIORS",
    "SEARCH_HORIZON_EXHAUSTED_AT_10_INTERIORS",
    "INTERIOR_COUNT_ABOVE_20",
    "SEARCH_HORIZON_EXHAUSTED_AT_20_INTERIORS",
    "OPEN_BOUNDARY_BELOW_0_80",
    "CLOSE_BOUNDARY_BELOW_0_80",
    "OPEN_BOUNDARY_BELOW_1_30",
    "CLOSE_BOUNDARY_BELOW_1_30",
    "OPEN_BOUNDARY_ABOVE_3_50",
    "CLOSE_BOUNDARY_ABOVE_3_50",
    "BOUNDARY_ABOVE_MAXIMUM",
    "ZERO_INTERIOR_REFERENCE_RETURN_NOT_ACHIEVED",
})
