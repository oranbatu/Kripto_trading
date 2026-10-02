"""Explicit configuration for the read-only Binance USD-M perpetual data layer."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

DEFAULT_DATA_ROOT = Path(r"C:\MarketData")
ENV_DATA_ROOT = "MARKET_DATA_ROOT"

ALLOWED_SYMBOLS: tuple[str, ...] = (
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

ALLOWED_TIMEFRAMES: tuple[str, ...] = ("1m", "5m", "15m", "30m", "1h", "4h", "1d")

PROVIDER_CANONICAL = "binance"
MARKET_CANONICAL = "usd_m_perpetual"
ARCHIVE_NAMESPACE = "futures/um"
TIMEZONE_NAME = "UTC"
BUCKET_ALIGNMENT = "UTC"

ANALYTICAL_COLUMNS: tuple[str, ...] = (
    "symbol",
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "source_candle_count",
)

IDENTITY_COLUMNS: tuple[str, ...] = ("symbol", "open_time", "close_time")
OHLC_COLUMNS: tuple[str, ...] = ("open", "high", "low", "close")
VOLUME_COLUMNS: tuple[str, ...] = (
    "volume",
    "quote_asset_volume",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
)
DECIMAL_COLUMNS: tuple[str, ...] = OHLC_COLUMNS + VOLUME_COLUMNS

PRECISION_EXACT = "exact"
PRECISION_ANALYSIS = "analysis"
ALLOWED_PRECISION_MODES: tuple[str, ...] = (PRECISION_EXACT, PRECISION_ANALYSIS)
DEFAULT_PRECISION_MODE = PRECISION_EXACT

DECISION_CLOCK_OPEN = "open"
DECISION_CLOCK_CLOSE = "close"
ALLOWED_DECISION_CLOCKS: tuple[str, ...] = (DECISION_CLOCK_OPEN, DECISION_CLOCK_CLOSE)
DEFAULT_DECISION_CLOCK = DECISION_CLOCK_CLOSE

DECIMAL_PRECISION = 38
DECIMAL_SCALE = 8
UNAVAILABLE_INDEX = -1

# Analysis-mode values are the IEEE-754 binary64 conversion of stored
# decimal128(38, 8). They must remain finite. Strategy code must not use
# exact float equality; use tick size or a relative tolerance of at least 1e-10.
ANALYSIS_RELATIVE_TOLERANCE = 1e-10
ANALYSIS_ABSOLUTE_TOLERANCE = 1e-10

CANONICAL_1M_PHYSICAL_COLUMNS: tuple[str, ...] = (
    "symbol",
    "interval",
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
)

CANONICAL_TO_ANALYTICAL: Mapping[str, str] = {
    "quote_volume": "quote_asset_volume",
    "count": "number_of_trades",
    "taker_buy_volume": "taker_buy_base_asset_volume",
    "taker_buy_quote_volume": "taker_buy_quote_asset_volume",
}
ANALYTICAL_TO_CANONICAL: Mapping[str, str] = {v: k for k, v in CANONICAL_TO_ANALYTICAL.items()}


@dataclass(frozen=True)
class TimeframeSpec:
    timeframe: str
    duration_ms: int
    candles_per_utc_day: int
    partitioning: str  # "symbol_year_month" | "symbol_year"
    location: str  # "processed" | "derived"
    schema_version: str
    expected_files: int
    expected_rows_per_symbol: int
    expected_total_rows: int
    first_open_utc: datetime
    last_open_utc: datetime
    source_candles_per_output: int


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


TIMEFRAME_SPECS: Mapping[str, TimeframeSpec] = {
    "1m": TimeframeSpec(
        timeframe="1m",
        duration_ms=60_000,
        candles_per_utc_day=1_440,
        partitioning="symbol_year_month",
        location="processed",
        schema_version="um-perp-1m-canonical-v1",
        expected_files=297,
        expected_rows_per_symbol=1_424_160,
        expected_total_rows=12_817_440,
        first_open_utc=_utc(2024, 1, 1),
        last_open_utc=_utc(2026, 9, 15, 23, 59),
        source_candles_per_output=1,
    ),
    "5m": TimeframeSpec(
        timeframe="5m",
        duration_ms=300_000,
        candles_per_utc_day=288,
        partitioning="symbol_year_month",
        location="derived",
        schema_version="um-perp-5m-derived-v1",
        expected_files=297,
        expected_rows_per_symbol=284_832,
        expected_total_rows=2_563_488,
        first_open_utc=_utc(2024, 1, 1),
        last_open_utc=_utc(2026, 9, 15, 23, 55),
        source_candles_per_output=5,
    ),
    "15m": TimeframeSpec(
        timeframe="15m",
        duration_ms=900_000,
        candles_per_utc_day=96,
        partitioning="symbol_year_month",
        location="derived",
        schema_version="um-perp-15m-derived-v1",
        expected_files=297,
        expected_rows_per_symbol=94_944,
        expected_total_rows=854_496,
        first_open_utc=_utc(2024, 1, 1),
        last_open_utc=_utc(2026, 9, 15, 23, 45),
        source_candles_per_output=15,
    ),
    "30m": TimeframeSpec(
        timeframe="30m",
        duration_ms=1_800_000,
        candles_per_utc_day=48,
        partitioning="symbol_year_month",
        location="derived",
        schema_version="um-perp-30m-derived-v1",
        expected_files=297,
        expected_rows_per_symbol=47_472,
        expected_total_rows=427_248,
        first_open_utc=_utc(2024, 1, 1),
        last_open_utc=_utc(2026, 9, 15, 23, 30),
        source_candles_per_output=30,
    ),
    "1h": TimeframeSpec(
        timeframe="1h",
        duration_ms=3_600_000,
        candles_per_utc_day=24,
        partitioning="symbol_year_month",
        location="derived",
        schema_version="um-perp-1h-derived-v1",
        expected_files=297,
        expected_rows_per_symbol=23_736,
        expected_total_rows=213_624,
        first_open_utc=_utc(2024, 1, 1),
        last_open_utc=_utc(2026, 9, 15, 23, 0),
        source_candles_per_output=60,
    ),
    "4h": TimeframeSpec(
        timeframe="4h",
        duration_ms=14_400_000,
        candles_per_utc_day=6,
        partitioning="symbol_year_month",
        location="derived",
        schema_version="um-perp-4h-derived-v1",
        expected_files=297,
        expected_rows_per_symbol=5_934,
        expected_total_rows=53_406,
        first_open_utc=_utc(2024, 1, 1),
        last_open_utc=_utc(2026, 9, 15, 20, 0),
        source_candles_per_output=240,
    ),
    "1d": TimeframeSpec(
        timeframe="1d",
        duration_ms=86_400_000,
        candles_per_utc_day=1,
        partitioning="symbol_year",
        location="derived",
        schema_version="um-perp-1d-derived-v1",
        expected_files=27,
        expected_rows_per_symbol=989,
        expected_total_rows=8_901,
        first_open_utc=_utc(2024, 1, 1),
        last_open_utc=_utc(2026, 9, 15),
        source_candles_per_output=1440,
    ),
}

REPORT_FILES: Mapping[str, str] = {
    "1m": "canonical_build_report.json",
    "5m": "derived_5m_report.json",
    "15m": "derived_15m_report.json",
    "30m": "derived_30m_report.json",
    "1h": "derived_1h_report.json",
    "4h": "derived_4h_report.json",
    "1d": "derived_1d_report.json",
}

VALIDATE_ON_LOAD = True
VALIDATE_ALIGNMENT = True
VALIDATE_INVENTORY_ON_CATALOG = True


def data_root() -> Path:
    """Return MARKET_DATA_ROOT only when that environment variable is set."""
    raw = os.environ.get(ENV_DATA_ROOT)
    if raw is not None and raw != "":
        return Path(raw)
    return DEFAULT_DATA_ROOT


def processed_root(root: Path | None = None) -> Path:
    base = root if root is not None else data_root()
    return base / "processed" / "binance" / "futures" / "um" / "perpetual"


def derived_root(root: Path | None = None) -> Path:
    base = root if root is not None else data_root()
    return base / "derived" / "binance" / "futures" / "um" / "perpetual"


def reports_dir(root: Path | None = None) -> Path:
    base = root if root is not None else data_root()
    return base / "reports"


def dataset_root(timeframe: str, root: Path | None = None) -> Path:
    spec = TIMEFRAME_SPECS[timeframe]
    parent = processed_root(root) if spec.location == "processed" else derived_root(root)
    return parent / timeframe


def report_path(timeframe: str, root: Path | None = None) -> Path:
    return reports_dir(root) / REPORT_FILES[timeframe]
