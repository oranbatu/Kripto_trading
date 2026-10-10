"""Configuration for the separate 1:1.5 R take-position backtest."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIR = Path(__file__).resolve().parent
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "take_position_1_to_1_5_v1"
REPORT_PATH = PROJECT_ROOT / "reports" / "take_position_1_to_1_5r_report.json"
LOG_PATH = PROJECT_ROOT / "logs" / "take_position_1_to_1_5r.log"
DETECTOR_PACKAGE = PROJECT_ROOT / "detectors" / "swing_open_close_30m_special_v1"
ONE_TO_ONE_PACKAGE = PROJECT_ROOT / "strategies" / "take_position_v1"
ONE_TO_TWO_PACKAGE = PROJECT_ROOT / "strategies" / "take_position_1_to_2_v1"
FINGERPRINT_PATH = PROJECT_ROOT / "tmp" / "take_position_1_to_1_5_before.json"

STRATEGY_NAME = "BTCUSDT_30M_SPECIAL_SWING_TAKE_POSITION_1_TO_1_5R_V1"
CONFIGURATION_VERSION = "CONFIRMATION_CLOSE_ENTRY_PEAK2_DIP2_STOP_GROSS_1_TO_1_5R_V1"
RISK_REWARD_RATIO = "1:1.5"
REWARD_MULTIPLE = Decimal(3) / Decimal(2)
RISK_MULTIPLE = Decimal(1)
BREAK_EVEN_RATE = Decimal(1) / (Decimal(1) + REWARD_MULTIPLE)
BREAK_EVEN_PERCENT = Decimal(40)
PERFORMANCE_LABEL = "GROSS_BEFORE_FEES_FUNDING_AND_SLIPPAGE"
RESULT_LABEL = "2026 year-to-date through 2026-09-15"
DATA_RANGE = "2026-01-01 through 2026-09-15"
SAME_BAR_POLICY = "STOP_FIRST_CONSERVATIVE"
COLLISION_POLICY = SAME_BAR_POLICY
POSITION_POLICY = "INDEPENDENT_EVERY_ELIGIBLE_PRIMARY_SIGNAL"
ENTRY_EXECUTION_MODE = "CONFIRMATION_CANDLE_CLOSE"

SYMBOL = "BTCUSDT"
MARKET = "USD-M Perpetual"
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

REVISION_PATTERN = r"^BTCUSDT_30M_Take_Position_1_TO_1_5R_rev(\d{2,})\.xlsx$"
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
PARAMETERS = (
    ("Strategy Name", STRATEGY_NAME),
    ("Configuration Version", CONFIGURATION_VERSION),
    ("Symbol", SYMBOL),
    ("Market", MARKET),
    ("TradingView Symbol", "BINANCE:BTCUSDT.P"),
    ("Signal Timeframe", SIGNAL_TIMEFRAME),
    ("Exit Resolution", "canonical 1m"),
    ("Analysis Start", "2026-01-01T00:00:00Z"),
    ("Analysis End", "2026-09-15T23:59:59.999Z"),
    ("Result Label", RESULT_LABEL),
    ("risk_reward_ratio", "1:1.5"),
    ("risk_multiple", "1.0"),
    ("reward_multiple", "1.5"),
    ("short_stop", "Peak 2 High"),
    ("long_stop", "Dip 2 Low"),
    ("entry", "Swing Close Close"),
    ("short_take_profit", "entry - 1.5 × (stop - entry)"),
    ("long_take_profit", "entry + 1.5 × (entry - stop)"),
    ("normal_take_profit_r", "+1.5"),
    ("normal_stop_loss_r", "-1.0"),
    ("gross_break_even_win_rate", "40.0000%"),
    ("signal_time", "completed Swing Close close time"),
    ("first_exit_bar", "strictly after signal time"),
    ("same_bar_policy", SAME_BAR_POLICY),
    ("favorable_gap_target_cap", "+1.5R"),
    ("adverse_stop_gap", "actual worse fill allowed"),
    ("fees", "0"),
    ("slippage", "0"),
    ("funding", "0"),
    ("overlapping_trades", "allowed"),
    ("dataset_end_policy", "OPEN_AT_DATASET_END"),
    ("calculation_timezone", "UTC"),
    ("display_timezone", "Europe/Istanbul"),
    ("Performance Basis", PERFORMANCE_LABEL),
)
