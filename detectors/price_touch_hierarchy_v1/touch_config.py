"""Frozen configuration for the price-touch hierarchy detector.

Thresholds and zone geometry are constants. They are not adjusted after a run.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal
from zoneinfo import ZoneInfo

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parents[1]
# Historical literal only. Publication uses the Windows Known Folder Desktop,
# which may be this folder or a redirected folder such as OneDrive\Desktop.
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "price_touch_hierarchy_v1"
SOURCE_ROOT = Path(r"C:\MarketData\derived\binance\futures\um\perpetual")

ALGORITHM_VERSION = "BTCUSDT_MTF_PRICE_TOUCH_HIERARCHY_V1"
RUNNER_VERSION = "UM_PERP_MTF_PRICE_TOUCH_HIERARCHY_V1"
DETECTOR_VERSION = RUNNER_VERSION
IMPLEMENTATION_MODE = "PERMANENT_SCRIPT_INSIDE_EXISTING_PROJECT"

SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "AVAXUSDT",
    "AAVEUSDT",
    "NEARUSDT",
    "LINKUSDT",
    "LTCUSDT",
    "OPUSDT",
    "DOTUSDT",
)
# Wrapper and Control Center scope. --all-symbols stays on SYMBOLS above.
PRICE_TOUCH_EXTENSION_SYMBOLS = (
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "SOLUSDT",
)
SUPPORTED_SYMBOLS = SYMBOLS + PRICE_TOUCH_EXTENSION_SYMBOLS
DEFAULT_SYMBOL = "BTCUSDT"

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
DATASET_END = datetime(2026, 10, 27, 23, 59, 59, 999000, tzinfo=timezone.utc)
EXCLUSIVE_LOAD_END = datetime(2026, 10, 28, tzinfo=timezone.utc)

EXPECTED_ROWS = {"1h": 24744, "4h": 6186, "1d": 1031}
EXPECTED_PARTITIONS = {"1h": 34, "4h": 34, "1d": 3}
EXPECTED_FIRST = {tf: ANALYSIS_START for tf in TIMEFRAMES}
EXPECTED_LAST = {
    "1h": datetime(2026, 10, 27, 23, tzinfo=timezone.utc),
    "4h": datetime(2026, 10, 27, 20, tzinfo=timezone.utc),
    "1d": datetime(2026, 10, 27, tzinfo=timezone.utc),
}
DURATION_MS = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}


@dataclass(frozen=True, slots=True)
class SymbolProfile:
    symbol: str
    tradingview_symbol: str
    source_1h: str
    source_4h: str
    source_1d: str
    workbook_family: str
    report_name: str
    log_name: str
    algorithm_version: str
    runner_version: str
    expected_partitions: tuple[tuple[str, int], ...]
    expected_rows: tuple[tuple[str, int], ...]
    first_open_utc: str
    last_close_utc: str
    exclusive_end_utc: str


def symbol_profile(symbol: str) -> SymbolProfile:
    checked = require_price_touch_symbol(symbol)
    sources = source_dataset(checked)
    return SymbolProfile(
        symbol=checked,
        tradingview_symbol=tradingview_symbol(checked),
        source_1h=str(sources["1h"]),
        source_4h=str(sources["4h"]),
        source_1d=str(sources["1d"]),
        workbook_family=f"{checked}_MTF_Most_Touched_Levels_revNN.xlsx",
        report_name=f"price_touch_hierarchy_{checked}.json",
        log_name=f"price_touch_hierarchy_{checked}.log",
        algorithm_version=ALGORITHM_VERSION,
        runner_version=RUNNER_VERSION,
        expected_partitions=tuple(EXPECTED_PARTITIONS.items()),
        expected_rows=tuple(EXPECTED_ROWS.items()),
        first_open_utc="2024-01-01T00:00:00Z",
        last_close_utc="2026-10-27T23:59:59.999Z",
        exclusive_end_utc="2026-10-28T00:00:00Z",
    )


def require_price_touch_symbol(symbol: str) -> str:
    """Accept only an explicit Price Touch symbol. Reject path-like text."""
    if (
        type(symbol) is not str
        or symbol not in SUPPORTED_SYMBOLS
        or any(part in symbol for part in ("/", "\\", "..", ":", " "))
    ):
        raise ValueError(f"unsupported price-touch symbol {symbol!r}")
    return symbol


def source_dataset(symbol: str) -> dict[str, Path]:
    checked = require_price_touch_symbol(symbol)
    root = SOURCE_ROOT.resolve()
    datasets: dict[str, Path] = {}
    for timeframe in TIMEFRAMES:
        path = (root / timeframe / f"symbol={checked}").resolve()
        if path.parent.parent != root or path.name != f"symbol={checked}":
            raise ValueError(f"price-touch source escaped its dataset root: {path}")
        datasets[timeframe] = path
    return datasets


def tradingview_symbol(symbol: str) -> str:
    return f"BINANCE:{require_price_touch_symbol(symbol)}.P"


def revision_pattern_text(symbol: str) -> str:
    return rf"^{re.escape(symbol)}_MTF_Most_Touched_Levels_rev(\d{{2,}})\.xlsx$"


SOURCE_DATASET = source_dataset(DEFAULT_SYMBOL)
REPORTS_DIR = PROJECT_ROOT / "reports"
LOGS_DIR = PROJECT_ROOT / "logs"

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

REVISION_PATTERN_TEXT = revision_pattern_text(DEFAULT_SYMBOL)

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
