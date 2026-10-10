"""Configuration for the separate 1R take-position backtest."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIR = Path(__file__).resolve().parent
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "take_position_v1"
REPORT_PATH = PROJECT_ROOT / "reports" / "take_position_1r_report.json"
LOG_PATH = PROJECT_ROOT / "logs" / "take_position_1r.log"
DETECTOR_PACKAGE = PROJECT_ROOT / "detectors" / "swing_open_close_30m_special_v1"

STRATEGY_NAME = "BTCUSDT 30M Special Swing 1R Take Position Backtest"
STRATEGY_VERSION = "BTCUSDT_30M_SPECIAL_SWING_TAKE_POSITION_1R_V1"
CONFIGURATION_VERSION = "CONFIRMATION_CLOSE_ENTRY_PEAK2_DIP2_STOP_GROSS_1R_V1"
PERFORMANCE_LABEL = "GROSS BEFORE FEES, SLIPPAGE, AND FUNDING"
RESULT_LABEL = "2026 YTD Through September 15"

SYMBOL = "BTCUSDT"
SIGNAL_TIMEFRAME = "30m"
EXIT_TIMEFRAME = "1m"
ANALYSIS_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ANALYSIS_END = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=timezone.utc)
SIGNAL_END_OPEN = datetime(2026, 9, 15, 23, 30, tzinfo=timezone.utc)
EXCLUSIVE_END = datetime(2026, 9, 16, tzinfo=timezone.utc)
EXPECTED_DAYS = 258
EXPECTED_30M_ROWS = 12_384
EXPECTED_1M_ROWS = 371_520
STEP_30M_MS = 1_800_000
STEP_1M_MS = 60_000

ENTRY_EXECUTION_MODE = "CONFIRMATION_CANDLE_CLOSE"
COLLISION_POLICY = "STOP_LOSS_FIRST"
POSITION_POLICY = "INDEPENDENT_EVERY_ELIGIBLE_PRIMARY_SIGNAL"
TARGET_R = Decimal(1)
ENTRY_FEE = Decimal(0)
EXIT_FEE = Decimal(0)
SLIPPAGE = Decimal(0)
FUNDING = Decimal(0)

REVISION_PATTERN = r"^BTCUSDT_30M_Take_Position_1R_rev(\d{2,})\.xlsx$"
SHEETS = (
    "Performance Summary",
    "All Trades",
    "Short Trades",
    "Long Trades",
    "Open and Censored",
    "Invalid Signals",
    "Parameters",
    "Diagnostics",
    "README",
)
TRADE_COLUMNS = (
    "Trade ID",
    "Detector Swing ID",
    "Swing Type",
    "Trade Direction",
    "Signal Time UTC",
    "Signal Time Turkey",
    "Entry Time UTC",
    "Entry Time Turkey",
    "Entry Price",
    "Entry Execution Mode",
    "Stop Loss Price",
    "Take Profit Price",
    "Risk Price",
    "Risk Percent",
    "Target R",
    "Peak 2 High",
    "Dip 2 Low",
    "Outcome",
    "Outcome Group",
    "Exit Reason",
    "Exit Event Minute UTC",
    "Exit Event Minute Turkey",
    "Exit Fill Price",
    "Realized Gross R",
    "Duration Minutes",
    "Duration Hours",
    "Duration Days",
    "Equivalent 30m Bars",
    "Intrabar Ambiguity",
    "Stop Gap",
    "Take Profit Gap",
    "MFE Price",
    "MAE Price",
    "MFE R",
    "MAE R",
    "Forward Data Censored",
    "Censoring Reason",
    "Last Available Price",
    "Overlapping Trade",
    "Concurrent Trade Count At Entry",
    "Detector Version",
    "Strategy Version",
    "Configuration Version",
)
PARAMETERS = (
    ("Strategy Name", STRATEGY_NAME),
    ("Strategy Version", STRATEGY_VERSION),
    ("Configuration Version", CONFIGURATION_VERSION),
    ("Provider", "Binance"),
    ("Market", "USD-M Perpetual"),
    ("Symbol", SYMBOL),
    ("TradingView Symbol", "BINANCE:BTCUSDT.P"),
    ("Signal Timeframe", "30m"),
    ("Exit Evaluation Timeframe", "1m"),
    ("Display Timezone", "Europe/Istanbul"),
    ("Analysis Start", "2026-01-01"),
    ("Available Analysis End", "2026-09-15"),
    ("Calendar-Year Data Complete", "No"),
    ("Result Label", RESULT_LABEL),
    ("Signal Source", "Existing 30m Special Swing Detector"),
    ("Only Primary Signals", "Yes"),
    ("Derived Signals Traded", "No"),
    ("Nested Signals Traded", "No"),
    ("Entry Mode", "Confirmation Candle Close"),
    ("Exit Scan Starts", "Strictly After Confirmation"),
    ("Position Policy", POSITION_POLICY),
    ("Overlapping Trades Allowed", "Yes"),
    ("Swing High Direction", "Short"),
    ("Swing High Stop", "Peak 2 High"),
    ("Swing High Target", "Exact 1:1 R"),
    ("Swing Low Direction", "Long"),
    ("Swing Low Stop", "Dip 2 Low"),
    ("Swing Low Target", "Exact 1:1 R"),
    ("Same 1m Candle SL/TP Policy", "Stop Loss First"),
    ("Entry Fee", "0"),
    ("Exit Fee", "0"),
    ("Slippage", "0"),
    ("Funding", "0"),
    ("Performance Type", "Gross Before Costs"),
)
