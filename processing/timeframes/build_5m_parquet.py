#!/usr/bin/env python3
"""
Derive a UTC-aligned 5-minute Parquet dataset from the verified canonical
Binance USD-M PERPETUAL 1m Parquet dataset.

This script is a derivation processor only. It never downloads data, never
modifies raw / canonical 1m / derived 15m / 30m / 1h / 4h / 1d trees, never
creates another timeframe, and never implements strategy or backtest logic.

Each output candle is built from exactly five consecutive unique 1m candles in
a UTC five-minute bucket (minutes 00/05/10/.../55). Incomplete buckets are a
hard failure.

Existing 15m, 30m, 1h, 4h, and 1d datasets are used only as independent
upward cross-checks: generated 5m candles are aggregated into those timeframes
and compared with the verified files. 5m rows are never generated from them.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sys
import time
import traceback
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterator, Optional

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError:
    sys.stderr.write(
        "pyarrow is required. Use:\n"
        "  C:\\Users\\oranb\\Desktop\\backtest_system\\.venv\\Scripts\\python.exe "
        "-m processing.timeframes.build_5m_parquet\n"
    )
    raise

# =============================================================================
# CONFIGURATION
# =============================================================================
DATA_ROOT: Path = Path(r"C:\MarketData")
RAW_ROOT: Path = DATA_ROOT / "raw"
CANONICAL_1M_ROOT: Path = DATA_ROOT / "processed" / "binance" / "futures" / "um" / "perpetual" / "1m"
DERIVED_15M_ROOT: Path = DATA_ROOT / "derived" / "binance" / "futures" / "um" / "perpetual" / "15m"
DERIVED_30M_ROOT: Path = DATA_ROOT / "derived" / "binance" / "futures" / "um" / "perpetual" / "30m"
DERIVED_1H_ROOT: Path = DATA_ROOT / "derived" / "binance" / "futures" / "um" / "perpetual" / "1h"
DERIVED_4H_ROOT: Path = DATA_ROOT / "derived" / "binance" / "futures" / "um" / "perpetual" / "4h"
DERIVED_1D_ROOT: Path = DATA_ROOT / "derived" / "binance" / "futures" / "um" / "perpetual" / "1d"
DERIVED_5M_ROOT: Path = DATA_ROOT / "derived" / "binance" / "futures" / "um" / "perpetual" / "5m"
REPORTS_DIR: Path = DATA_ROOT / "reports"
LOGS_DIR: Path = DATA_ROOT / "logs"
TMP_ROOT: Path = DATA_ROOT / "tmp" / "derive_5m"

EXISTING_SCRIPTS: tuple[Path, ...] = (
    Path(r"C:\Users\oranb\Desktop\backtest_system\ingestion\binance\download_binance_um_perp_1m.py"),
    Path(r"C:\Users\oranb\Desktop\backtest_system\processing\canonical\build_canonical_parquet.py"),
    Path(r"C:\Users\oranb\Desktop\backtest_system\processing\timeframes\build_15m_parquet.py"),
    Path(r"C:\Users\oranb\Desktop\backtest_system\processing\timeframes\build_30m_parquet.py"),
    Path(r"C:\Users\oranb\Desktop\backtest_system\processing\timeframes\build_1h_parquet.py"),
    Path(r"C:\Users\oranb\Desktop\backtest_system\processing\timeframes\build_4h_parquet.py"),
    Path(r"C:\Users\oranb\Desktop\backtest_system\processing\timeframes\build_1d_parquet.py"),
)
EXISTING_REPORTS: tuple[Path, ...] = (
    REPORTS_DIR / "download_report.json",
    REPORTS_DIR / "canonical_build_report.json",
    REPORTS_DIR / "derived_15m_report.json",
    REPORTS_DIR / "derived_30m_report.json",
    REPORTS_DIR / "derived_1h_report.json",
    REPORTS_DIR / "derived_4h_report.json",
    REPORTS_DIR / "derived_1d_report.json",
)

SYMBOLS: tuple[str, ...] = (
    "BTCUSDT", "ETHUSDT", "AVAXUSDT", "AAVEUSDT", "NEARUSDT",
    "LINKUSDT", "LTCUSDT", "OPUSDT", "DOTUSDT",
)
SOURCE_INTERVAL = "1m"
OUTPUT_INTERVAL = "5m"
RANGE_START_UTC = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
RANGE_END_UTC = datetime(2026, 9, 15, 23, 59, tzinfo=timezone.utc)

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
SCHEMA_VERSION = "um-perp-5m-derived-v1"
AGGREGATION_RULES_VERSION = "um-perp-5m-agg-v1"

DECIMAL_PRECISION = 38
DECIMAL_SCALE = 8
SOURCE_CANDLES_PER_OUTPUT = 5
MINUTE_MS = 60_000
HOUR_MS = 60 * MINUTE_MS
DAY_MS = 24 * HOUR_MS
BUCKET_MS = 5 * MINUTE_MS  # 300_000
MS_15M = 15 * MINUTE_MS
MS_30M = 30 * MINUTE_MS
MS_1H = HOUR_MS
MS_4H = 4 * HOUR_MS
N_5M_PER_15M = 3
N_5M_PER_30M = 6
N_5M_PER_1H = 12
N_5M_PER_4H = 48
N_5M_PER_1D = 288

EXPECTED_SOURCE_PARTITIONS = 297
EXPECTED_MONTHLY_SECONDARY = 297
EXPECTED_1D_FILES = 27
EXPECTED_OUTPUT_PARTITIONS = 297
EXPECTED_SOURCE_ROWS_PER_SYMBOL = 1_424_160
EXPECTED_SOURCE_ROWS_TOTAL = 12_817_440
EXPECTED_UTC_DAYS = 989
EXPECTED_OUTPUT_ROWS_PER_SYMBOL = EXPECTED_UTC_DAYS * N_5M_PER_1D  # 284_832
EXPECTED_OUTPUT_ROWS_TOTAL = EXPECTED_OUTPUT_ROWS_PER_SYMBOL * 9  # 2_563_488
EXPECTED_SEPT_2026_OUTPUT_ROWS = 15 * N_5M_PER_1D  # 4_320
EXPECTED_15M_TOTAL = 854_496
EXPECTED_30M_TOTAL = 427_248
EXPECTED_1H_TOTAL = 213_624
EXPECTED_4H_TOTAL = 53_406
EXPECTED_1D_TOTAL = 8_901
EXPECTED_FIRST_OPEN = "2024-01-01T00:00:00Z"
EXPECTED_LAST_OPEN = "2026-09-15T23:55:00Z"
EXPECTED_LAST_CLOSE = "2026-09-15T23:59:59.999Z"
# =============================================================================

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
QUANT = Decimal("1").scaleb(-DECIMAL_SCALE)
SCALE_INT = 10 ** DECIMAL_SCALE
CHUNK_BYTES = 1024 * 1024
VALID_OPEN_MINUTES = frozenset(range(0, 60, 5))
OPEN_HOURS_4H = frozenset({0, 4, 8, 12, 16, 20})

DECIMAL_TYPE = pa.decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
TS_UTC = pa.timestamp("ms", tz="UTC")

SOURCE_REQUIRED_COLUMNS = (
    "symbol", "interval", "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume",
)
SECONDARY_REQUIRED_COLUMNS = (
    "symbol", "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume", "source_candle_count",
)

OUTPUT_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("open_time", TS_UTC, nullable=False),
        pa.field("open", DECIMAL_TYPE, nullable=False),
        pa.field("high", DECIMAL_TYPE, nullable=False),
        pa.field("low", DECIMAL_TYPE, nullable=False),
        pa.field("close", DECIMAL_TYPE, nullable=False),
        pa.field("volume", DECIMAL_TYPE, nullable=False),
        pa.field("close_time", TS_UTC, nullable=False),
        pa.field("quote_asset_volume", DECIMAL_TYPE, nullable=False),
        pa.field("number_of_trades", pa.int64(), nullable=False),
        pa.field("taker_buy_base_asset_volume", DECIMAL_TYPE, nullable=False),
        pa.field("taker_buy_quote_asset_volume", DECIMAL_TYPE, nullable=False),
        pa.field("source_candle_count", pa.int16(), nullable=False),
    ],
    metadata={
        b"schema_version": SCHEMA_VERSION.encode("utf-8"),
        b"provider": b"binance",
        b"market": b"usd_m_perpetual",
        b"archive_namespace": b"futures/um",
        b"interval": OUTPUT_INTERVAL.encode("utf-8"),
        b"primary_source_interval": SOURCE_INTERVAL.encode("utf-8"),
        b"validation_interval_1": b"15m",
        b"validation_interval_2": b"30m",
        b"validation_interval_3": b"1h",
        b"validation_interval_4": b"4h",
        b"validation_interval_5": b"1d",
        b"timezone": b"UTC",
        b"bucket_alignment": b"UTC",
        b"source_candles_per_output": b"5",
        b"aggregation_rules_version": AGGREGATION_RULES_VERSION.encode("utf-8"),
    },
)

COMPARE_FIELDS = (
    "open", "high", "low", "close", "volume", "quote_asset_volume",
    "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
    "open_ms", "close_ms",
)

READONLY_TREES = (
    RAW_ROOT, CANONICAL_1M_ROOT, DERIVED_15M_ROOT, DERIVED_30M_ROOT,
    DERIVED_1H_ROOT, DERIVED_4H_ROOT, DERIVED_1D_ROOT,
)
log = logging.getLogger("derived_5m")


# ----------------------------------------------------------------------------- helpers
def ms_of(dt: datetime) -> int:
    return (dt - EPOCH) // timedelta(milliseconds=1)


def iso_of_ms(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return (EPOCH + timedelta(milliseconds=ms)).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_of_ms_us(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    dt = EPOCH + timedelta(milliseconds=ms)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"


def month_bounds_ms(year: int, month: int) -> tuple[int, int]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    nxt = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return ms_of(start), ms_of(nxt) - MINUTE_MS


def iter_year_months(start: datetime, end: datetime) -> Iterator[tuple[int, int]]:
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def hive_month_path(root: Path, symbol: str, year: int, month: int) -> Path:
    return root / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}" / "data.parquet"


def hive_year_path(root: Path, symbol: str, year: int) -> Path:
    return root / f"symbol={symbol}" / f"year={year}" / "data.parquet"


def bucket_start_ms(open_ms: int) -> int:
    return open_ms - (open_ms % BUCKET_MS)


def utc_minute(open_ms: int) -> int:
    return (open_ms // MINUTE_MS) % 60


def utc_hour(open_ms: int) -> int:
    return (open_ms // HOUR_MS) % 24


def is_utc_5m_open(open_ms: int) -> bool:
    return open_ms % MINUTE_MS == 0 and utc_minute(open_ms) in VALID_OPEN_MINUTES


def assert_outside(path: Path, forbidden: Path, label: str) -> None:
    try:
        path.resolve().relative_to(forbidden.resolve())
    except ValueError:
        return
    raise RuntimeError(f"{label} {path} resolves inside read-only tree {forbidden}")


def assert_outside_all(path: Path, label: str) -> None:
    for forbidden in READONLY_TREES:
        assert_outside(path, forbidden, label)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(CHUNK_BYTES), b""):
            h.update(block)
    return h.hexdigest()


def fingerprint_tree(root: Path, patterns: tuple[str, ...]) -> dict[str, dict]:
    files: list[Path] = []
    for pat in patterns:
        files.extend(p for p in root.rglob(pat) if p.is_file())
    out: dict[str, dict] = {}
    for p in sorted(set(files)):
        st = p.stat()
        try:
            rel = p.resolve().relative_to(root.resolve()).as_posix()
        except ValueError:
            rel = str(p)
        out[rel] = {
            "relative_path": rel,
            "size_bytes": st.st_size,
            "mtime_ns": st.st_mtime_ns,
            "sha256": sha256_file(p),
        }
    return out


def fingerprint_paths(paths: tuple[Path, ...]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for p in paths:
        st = p.stat()
        out[str(p)] = {
            "path": str(p),
            "size_bytes": st.st_size,
            "mtime_ns": st.st_mtime_ns,
            "sha256": sha256_file(p),
        }
    return out


def compare_fingerprints(before: dict, after: dict) -> dict:
    mismatches = []
    for k in sorted(set(before) | set(after)):
        if before.get(k) != after.get(k):
            mismatches.append({"key": k, "before": before.get(k), "after": after.get(k)})
    return {
        "files_before": len(before),
        "files_after": len(after),
        "unchanged": not mismatches and before.keys() == after.keys(),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
    }


def to_dec128(value: Decimal, field: str) -> Decimal:
    if not isinstance(value, Decimal):
        try:
            value = Decimal(value)
        except (InvalidOperation, TypeError) as exc:
            raise ValueError(f"{field} is not a decimal: {value!r}") from exc
    if not value.is_finite():
        raise ValueError(f"{field}={value!r} is not finite")
    exp = value.as_tuple().exponent
    if isinstance(exp, int) and exp < -DECIMAL_SCALE:
        raise ValueError(
            f"{field}={value!r} has more than {DECIMAL_SCALE} decimal places; refusing to round"
        )
    q = value.quantize(QUANT)
    digits = len(q.as_tuple().digits)
    if digits > DECIMAL_PRECISION:
        raise ValueError(f"{field}={value!r} exceeds decimal128({DECIMAL_PRECISION},{DECIMAL_SCALE})")
    return q


def scaled_int(d: Decimal) -> int:
    return int((to_dec128(d, "scaled") * SCALE_INT).to_integral_value())


def fmt_val(v) -> str:
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)


# ----------------------------------------------------------------------------- data classes
@dataclass(frozen=True)
class PartitionRef:
    symbol: str
    year: int
    month: int
    source_path: Path
    path_15m: Path
    path_30m: Path
    path_1h: Path
    path_4h: Path
    path_1d: Path
    output_path: Path
    start_ms: int
    end_ms: int

    @property
    def expected_1m_rows(self) -> int:
        return (self.end_ms - self.start_ms) // MINUTE_MS + 1

    @property
    def expected_5m_rows(self) -> int:
        return self.expected_1m_rows // SOURCE_CANDLES_PER_OUTPUT

    @property
    def expected_15m_rows(self) -> int:
        return self.expected_1m_rows // 15

    @property
    def expected_30m_rows(self) -> int:
        return self.expected_1m_rows // 30

    @property
    def expected_1h_rows(self) -> int:
        return self.expected_1m_rows // 60

    @property
    def expected_4h_rows(self) -> int:
        return self.expected_1m_rows // 240

    @property
    def expected_1d_rows(self) -> int:
        return self.expected_1m_rows // 1440

    @property
    def expected_first_open_ms(self) -> int:
        return self.start_ms

    @property
    def expected_last_open_ms(self) -> int:
        return bucket_start_ms(self.end_ms)


@dataclass
class OutRow:
    symbol: str
    open_ms: int
    close_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_asset_volume: Decimal
    number_of_trades: int
    taker_buy_base_asset_volume: Decimal
    taker_buy_quote_asset_volume: Decimal
    source_candle_count: int

    def as_compare_dict(self) -> dict:
        return {
            "open_ms": self.open_ms,
            "close_ms": self.close_ms,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "quote_asset_volume": self.quote_asset_volume,
            "number_of_trades": self.number_of_trades,
            "taker_buy_base_asset_volume": self.taker_buy_base_asset_volume,
            "taker_buy_quote_asset_volume": self.taker_buy_quote_asset_volume,
        }


@dataclass
class PartitionWork:
    rows: list[OutRow]
    source_rows_read: int
    source_rows_consumed: int
    source_rows_omitted: int
    source_rows_reused: int
    complete_buckets: int
    incomplete_buckets: list[dict]
    content_fingerprint: str
    first_open_ms: Optional[int]
    last_open_ms: Optional[int]
    last_close_ms: Optional[int]
    ohlc_violation_count: int
    negative_volume_count: int
    null_count: int
    out_of_order_count: int
    duplicate_bucket_count: int
    missing_bucket_count: int
    rows_outside_range: int


# ----------------------------------------------------------------------------- discovery
def _unexpected_hive_files(root: Path) -> list[str]:
    extra: list[str] = []
    for p in root.rglob("data.parquet"):
        try:
            sym = next(x.split("=", 1)[1] for x in p.parts if x.startswith("symbol="))
        except StopIteration:
            extra.append(str(p))
            continue
        if sym not in SYMBOLS:
            extra.append(str(p))
    return extra


def discover_partitions(
    source_root: Path,
    root_15m: Path,
    root_30m: Path,
    root_1h: Path,
    root_4h: Path,
    root_1d: Path,
    output_root: Path,
    require_expected_counts: bool = True,
) -> list[PartitionRef]:
    rs, re_ = ms_of(RANGE_START_UTC), ms_of(RANGE_END_UTC)
    out: list[PartitionRef] = []
    missing: dict[str, list[str]] = {"1m": [], "15m": [], "30m": [], "1h": [], "4h": [], "1d": []}
    for symbol in SYMBOLS:
        for year, month in iter_year_months(RANGE_START_UTC, RANGE_END_UTC):
            m_first, m_last = month_bounds_ms(year, month)
            start_ms, end_ms = max(m_first, rs), min(m_last, re_)
            src = hive_month_path(source_root, symbol, year, month)
            p15 = hive_month_path(root_15m, symbol, year, month)
            p30 = hive_month_path(root_30m, symbol, year, month)
            p1h = hive_month_path(root_1h, symbol, year, month)
            p4h = hive_month_path(root_4h, symbol, year, month)
            p1d = hive_year_path(root_1d, symbol, year)
            if not src.is_file():
                missing["1m"].append(str(src))
                continue
            if not p15.is_file():
                missing["15m"].append(str(p15))
            if not p30.is_file():
                missing["30m"].append(str(p30))
            if not p1h.is_file():
                missing["1h"].append(str(p1h))
            if not p4h.is_file():
                missing["4h"].append(str(p4h))
            if not p1d.is_file():
                missing["1d"].append(str(p1d))
            out.append(PartitionRef(
                symbol, year, month, src, p15, p30, p1h, p4h, p1d,
                hive_month_path(output_root, symbol, year, month),
                start_ms, end_ms,
            ))
    for label, items in missing.items():
        if items:
            raise FileNotFoundError(f"{len(items)} {label} partitions missing, first: {items[:5]}")
    for label, root in (
        ("canonical 1m", source_root),
        ("derived 15m", root_15m),
        ("derived 30m", root_30m),
        ("derived 1h", root_1h),
        ("derived 4h", root_4h),
        ("derived 1d", root_1d),
    ):
        extra = _unexpected_hive_files(root)
        if extra:
            raise RuntimeError(f"unexpected {label} files: {extra[:10]}")
    if require_expected_counts:
        if len(out) != EXPECTED_SOURCE_PARTITIONS:
            raise RuntimeError(f"discovered {len(out)} source partitions, expected {EXPECTED_SOURCE_PARTITIONS}")
        for label, root, expected in (
            ("15m", root_15m, EXPECTED_MONTHLY_SECONDARY),
            ("30m", root_30m, EXPECTED_MONTHLY_SECONDARY),
            ("1h", root_1h, EXPECTED_MONTHLY_SECONDARY),
            ("4h", root_4h, EXPECTED_MONTHLY_SECONDARY),
            ("1d", root_1d, EXPECTED_1D_FILES),
        ):
            n = len(list(root.rglob("data.parquet")))
            if n != expected:
                raise RuntimeError(f"discovered {n} {label} files, expected {expected}")
    return out


# ----------------------------------------------------------------------------- reads
def _plain_string(table: pa.Table, name: str) -> pa.Table:
    idx = table.schema.get_field_index(name)
    col = table.column(name)
    if not pa.types.is_string(col.type):
        table = table.set_column(idx, name, pc.cast(col, pa.string()))
    return table


def read_1m_table(path: Path) -> pa.Table:
    table = pq.ParquetFile(path).read()
    missing = [c for c in SOURCE_REQUIRED_COLUMNS if c not in table.column_names]
    if missing:
        raise ValueError(f"{path} missing columns {missing}; have {table.column_names}")
    table = table.select(list(SOURCE_REQUIRED_COLUMNS))
    table = _plain_string(table, "symbol")
    table = _plain_string(table, "interval")
    if str(table.schema.field("open_time").type) != "timestamp[ms, tz=UTC]":
        raise ValueError(f"{path} open_time type {table.schema.field('open_time').type}")
    if str(table.schema.field("open").type) != "decimal128(38, 8)":
        raise ValueError(f"{path} open type {table.schema.field('open').type}")
    if str(table.schema.field("count").type) != "int64":
        raise ValueError(f"{path} count type {table.schema.field('count').type}")
    return table


def read_secondary_table(path: Path, label: str) -> pa.Table:
    table = pq.ParquetFile(path).read()
    missing = [c for c in SECONDARY_REQUIRED_COLUMNS if c not in table.column_names]
    if missing:
        raise ValueError(f"{path} missing {label} columns {missing}; have {table.column_names}")
    table = table.select(list(SECONDARY_REQUIRED_COLUMNS))
    table = _plain_string(table, "symbol")
    if str(table.schema.field("open_time").type) != "timestamp[ms, tz=UTC]":
        raise ValueError(f"{path} {label} open_time type {table.schema.field('open_time').type}")
    if str(table.schema.field("open").type) != "decimal128(38, 8)":
        raise ValueError(f"{path} {label} open type {table.schema.field('open').type}")
    if str(table.schema.field("number_of_trades").type) != "int64":
        raise ValueError(f"{path} {label} number_of_trades type {table.schema.field('number_of_trades').type}")
    if str(table.schema.field("source_candle_count").type) != "int16":
        raise ValueError(f"{path} {label} source_candle_count type {table.schema.field('source_candle_count').type}")
    return table


def _col_has_null(table: pa.Table, name: str) -> int:
    n = pc.sum(pc.cast(pc.is_null(table.column(name)), pa.int64())).as_py()
    return int(n or 0)


def payload_bytes(row: OutRow) -> bytes:
    return (
        f"{row.symbol}|{row.open_ms}|{scaled_int(row.open)}|{scaled_int(row.high)}|"
        f"{scaled_int(row.low)}|{scaled_int(row.close)}|{scaled_int(row.volume)}|{row.close_ms}|"
        f"{scaled_int(row.quote_asset_volume)}|{row.number_of_trades}|"
        f"{scaled_int(row.taker_buy_base_asset_volume)}|{scaled_int(row.taker_buy_quote_asset_volume)}|"
        f"{row.source_candle_count}\n"
    ).encode("ascii")


def fingerprint_rows(rows: list[OutRow]) -> str:
    h = hashlib.sha256()
    for row in rows:
        h.update(payload_bytes(row))
    return h.hexdigest()


def aggregate_partition(part: PartitionRef, table: pa.Table) -> PartitionWork:
    nulls = sum(_col_has_null(table, c) for c in SOURCE_REQUIRED_COLUMNS)
    if nulls:
        raise ValueError(f"{part.source_path} contains {nulls} null required values")

    n = table.num_rows
    open_ms = pc.cast(table.column("open_time"), pa.int64()).to_pylist()
    close_ms = pc.cast(table.column("close_time"), pa.int64()).to_pylist()
    symbols = table.column("symbol").to_pylist()
    intervals = table.column("interval").to_pylist()
    opens = table.column("open").to_pylist()
    highs = table.column("high").to_pylist()
    lows = table.column("low").to_pylist()
    closes = table.column("close").to_pylist()
    vols = table.column("volume").to_pylist()
    qvols = table.column("quote_volume").to_pylist()
    trades = table.column("count").to_pylist()
    tb_base = table.column("taker_buy_volume").to_pylist()
    tb_quote = table.column("taker_buy_quote_volume").to_pylist()

    if any(s != part.symbol for s in symbols):
        raise ValueError(f"{part.source_path} contains mixed/unexpected symbols")
    if any(i != SOURCE_INTERVAL for i in intervals):
        raise ValueError(f"{part.source_path} contains a non-1m interval")

    opens_sorted = sorted(open_ms)
    if len(set(opens_sorted)) != n:
        raise ValueError(f"{part.source_path} has duplicate 1m open times")

    index_by_open = {open_ms[i]: i for i in range(n)}
    consumed: dict[int, int] = {t: 0 for t in opens_sorted}

    t = bucket_start_ms(part.start_ms)
    if t != part.start_ms or not is_utc_5m_open(part.start_ms):
        raise ValueError(
            f"misaligned bucket: partition {part.symbol} {part.year}-{part.month:02d} "
            f"starts at {iso_of_ms(part.start_ms)}, which is not a valid UTC 5m open "
            f"(minute divisible by 5)"
        )
    last_bucket = bucket_start_ms(part.end_ms)
    if last_bucket + 4 * MINUTE_MS != part.end_ms:
        raise ValueError(
            f"partition {part.symbol} {part.year}-{part.month:02d} last 1m {iso_of_ms(part.end_ms)} "
            f"does not complete a 5m bucket"
        )
    expected_starts: list[int] = []
    while t <= last_bucket:
        expected_starts.append(t)
        t += BUCKET_MS

    incomplete: list[dict] = []
    rows: list[OutRow] = []
    ohlc_viol = 0
    neg_vol = 0

    for bstart in expected_starts:
        wanted = [bstart + k * MINUTE_MS for k in range(SOURCE_CANDLES_PER_OUTPUT)]
        idxs = []
        missing_ts = []
        for w in wanted:
            i = index_by_open.get(w)
            if i is None:
                missing_ts.append(iso_of_ms(w))
            else:
                idxs.append(i)
        if len(idxs) != SOURCE_CANDLES_PER_OUTPUT or missing_ts:
            incomplete.append({
                "symbol": part.symbol,
                "bucket_start_utc": iso_of_ms(bstart),
                "reason": f"incomplete bucket: have {len(idxs)}/5; missing {missing_ts[:8]}",
            })
            continue
        src_opens = [open_ms[i] for i in idxs]
        if len(set(src_opens)) != SOURCE_CANDLES_PER_OUTPUT:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "bucket open times are not unique"})
            continue
        if src_opens[0] != bstart or not is_utc_5m_open(src_opens[0]):
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": f"first open {iso_of_ms(src_opens[0])} is not a valid UTC 5m open"})
            continue
        if src_opens[-1] != bstart + 4 * MINUTE_MS:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": f"last source open {iso_of_ms(src_opens[-1])} != start+4m"})
            continue
        if any(src_opens[k + 1] - src_opens[k] != MINUTE_MS for k in range(4)):
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "source open times are not consecutive 60,000 ms steps"})
            continue
        if any(symbols[i] != part.symbol for i in idxs):
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "mixed symbols in bucket"})
            continue

        b_opens = [to_dec128(opens[i], "open") for i in idxs]
        b_highs = [to_dec128(highs[i], "high") for i in idxs]
        b_lows = [to_dec128(lows[i], "low") for i in idxs]
        b_closes = [to_dec128(closes[i], "close") for i in idxs]
        b_vols = [to_dec128(vols[i], "volume") for i in idxs]
        b_qvols = [to_dec128(qvols[i], "quote_volume") for i in idxs]
        b_tb = [to_dec128(tb_base[i], "taker_buy_volume") for i in idxs]
        b_tbq = [to_dec128(tb_quote[i], "taker_buy_quote_volume") for i in idxs]
        b_trades = [int(trades[i]) for i in idxs]
        b_close_ms = [int(close_ms[i]) for i in idxs]

        bucket_ok = True
        for o, h, l, c, v, q, tb, tbq, tr in zip(
            b_opens, b_highs, b_lows, b_closes, b_vols, b_qvols, b_tb, b_tbq, b_trades
        ):
            if not (h >= max(o, c) and l <= min(o, c) and h >= l):
                incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                                   "reason": "source 1m OHLC invariant failed"})
                bucket_ok = False
                break
            if v < 0 or q < 0 or tb < 0 or tbq < 0 or tr < 0:
                incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                                   "reason": "source 1m negative volume/trade count"})
                bucket_ok = False
                break
        if not bucket_ok:
            continue

        out = OutRow(
            symbol=part.symbol,
            open_ms=src_opens[0],
            close_ms=b_close_ms[-1],
            open=b_opens[0],
            high=to_dec128(max(b_highs), "high"),
            low=to_dec128(min(b_lows), "low"),
            close=b_closes[-1],
            volume=to_dec128(sum(b_vols, Decimal("0")), "volume"),
            quote_asset_volume=to_dec128(sum(b_qvols, Decimal("0")), "quote_asset_volume"),
            number_of_trades=sum(b_trades),
            taker_buy_base_asset_volume=to_dec128(sum(b_tb, Decimal("0")), "taker_buy_base_asset_volume"),
            taker_buy_quote_asset_volume=to_dec128(sum(b_tbq, Decimal("0")), "taker_buy_quote_asset_volume"),
            source_candle_count=SOURCE_CANDLES_PER_OUTPUT,
        )
        if not (out.high >= max(out.open, out.close) and out.low <= min(out.open, out.close) and out.high >= out.low):
            ohlc_viol += 1
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "aggregated 5m OHLC invariant failed"})
            continue
        if (
            out.volume < 0 or out.quote_asset_volume < 0 or out.taker_buy_base_asset_volume < 0
            or out.taker_buy_quote_asset_volume < 0 or out.number_of_trades < 0
        ):
            neg_vol += 1
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "aggregated 5m negative volume/trade count"})
            continue
        if not is_utc_5m_open(out.open_ms):
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "output open_time is not a valid UTC 5m boundary"})
            continue
        if out.open_ms < part.start_ms or out.open_ms > part.expected_last_open_ms:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "output open_time outside partition range"})
            continue
        if out.source_candle_count != SOURCE_CANDLES_PER_OUTPUT:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "source_candle_count != 5"})
            continue
        if out.close_ms != out.open_ms + BUCKET_MS - 1:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": f"close_time {iso_of_ms_us(out.close_ms)} != last-source close of bucket"})
            continue
        rows.append(out)
        for w in wanted:
            consumed[w] += 1

    omitted = sum(1 for t, c in consumed.items() if c == 0)
    reused = sum(1 for t, c in consumed.items() if c > 1)
    used_once = sum(1 for t, c in consumed.items() if c == 1)
    missing_buckets = len(expected_starts) - len(rows)
    dups = len(rows) - len({r.open_ms for r in rows})
    ooo = 0
    for a, b in zip(rows, rows[1:]):
        if b.open_ms <= a.open_ms or b.open_ms - a.open_ms != BUCKET_MS:
            ooo += 1
    cutoff = ms_of(datetime(2026, 9, 16, tzinfo=timezone.utc))
    outside = sum(
        1 for r in rows
        if r.open_ms < ms_of(RANGE_START_UTC) or r.open_ms > ms_of(RANGE_END_UTC) or r.open_ms >= cutoff
    )
    return PartitionWork(
        rows=rows,
        source_rows_read=n,
        source_rows_consumed=used_once,
        source_rows_omitted=omitted,
        source_rows_reused=reused,
        complete_buckets=len(rows),
        incomplete_buckets=incomplete,
        content_fingerprint=fingerprint_rows(rows),
        first_open_ms=rows[0].open_ms if rows else None,
        last_open_ms=rows[-1].open_ms if rows else None,
        last_close_ms=rows[-1].close_ms if rows else None,
        ohlc_violation_count=ohlc_viol,
        negative_volume_count=neg_vol,
        null_count=nulls,
        out_of_order_count=ooo,
        duplicate_bucket_count=dups,
        missing_bucket_count=max(0, missing_buckets),
        rows_outside_range=outside,
    )


def rows_to_table(rows: list[OutRow]) -> pa.Table:
    return pa.table(
        {
            "symbol": pa.array([r.symbol for r in rows], pa.string()),
            "open_time": pa.array([r.open_ms for r in rows], TS_UTC),
            "open": pa.array([r.open for r in rows], DECIMAL_TYPE),
            "high": pa.array([r.high for r in rows], DECIMAL_TYPE),
            "low": pa.array([r.low for r in rows], DECIMAL_TYPE),
            "close": pa.array([r.close for r in rows], DECIMAL_TYPE),
            "volume": pa.array([r.volume for r in rows], DECIMAL_TYPE),
            "close_time": pa.array([r.close_ms for r in rows], TS_UTC),
            "quote_asset_volume": pa.array([r.quote_asset_volume for r in rows], DECIMAL_TYPE),
            "number_of_trades": pa.array([r.number_of_trades for r in rows], pa.int64()),
            "taker_buy_base_asset_volume": pa.array([r.taker_buy_base_asset_volume for r in rows], DECIMAL_TYPE),
            "taker_buy_quote_asset_volume": pa.array([r.taker_buy_quote_asset_volume for r in rows], DECIMAL_TYPE),
            "source_candle_count": pa.array([r.source_candle_count for r in rows], pa.int16()),
        },
        schema=OUTPUT_SCHEMA,
    )


def table_to_rows(table: pa.Table) -> list[OutRow]:
    extras = [c for c in table.column_names if c not in OUTPUT_SCHEMA.names]
    if extras:
        table = table.drop(extras)
    missing = [c for c in OUTPUT_SCHEMA.names if c not in table.column_names]
    if missing:
        raise ValueError(f"output missing columns {missing}")
    table = table.select(list(OUTPUT_SCHEMA.names))
    open_ms = pc.cast(table.column("open_time"), pa.int64()).to_pylist()
    close_ms = pc.cast(table.column("close_time"), pa.int64()).to_pylist()
    rows = []
    cols = {name: table.column(name).to_pylist() for name in OUTPUT_SCHEMA.names if name not in ("open_time", "close_time")}
    for i in range(table.num_rows):
        rows.append(OutRow(
            symbol=str(cols["symbol"][i]),
            open_ms=int(open_ms[i]),
            close_ms=int(close_ms[i]),
            open=to_dec128(cols["open"][i], "open"),
            high=to_dec128(cols["high"][i], "high"),
            low=to_dec128(cols["low"][i], "low"),
            close=to_dec128(cols["close"][i], "close"),
            volume=to_dec128(cols["volume"][i], "volume"),
            quote_asset_volume=to_dec128(cols["quote_asset_volume"][i], "quote_asset_volume"),
            number_of_trades=int(cols["number_of_trades"][i]),
            taker_buy_base_asset_volume=to_dec128(cols["taker_buy_base_asset_volume"][i], "taker_buy_base"),
            taker_buy_quote_asset_volume=to_dec128(cols["taker_buy_quote_asset_volume"][i], "taker_buy_quote"),
            source_candle_count=int(cols["source_candle_count"][i]),
        ))
    return rows


def filter_rows_to_partition(rows: list[OutRow], part: PartitionRef) -> list[OutRow]:
    return [r for r in rows if part.start_ms <= r.open_ms <= part.end_ms]


def combine_5m_group(items: list[OutRow]) -> dict:
    return {
        "open_ms": items[0].open_ms,
        "close_ms": items[-1].close_ms,
        "open": items[0].open,
        "high": to_dec128(max(x.high for x in items), "high"),
        "low": to_dec128(min(x.low for x in items), "low"),
        "close": items[-1].close,
        "volume": to_dec128(sum((x.volume for x in items), Decimal("0")), "volume"),
        "quote_asset_volume": to_dec128(sum((x.quote_asset_volume for x in items), Decimal("0")), "quote"),
        "number_of_trades": sum(x.number_of_trades for x in items),
        "taker_buy_base_asset_volume": to_dec128(
            sum((x.taker_buy_base_asset_volume for x in items), Decimal("0")), "tb"
        ),
        "taker_buy_quote_asset_volume": to_dec128(
            sum((x.taker_buy_quote_asset_volume for x in items), Decimal("0")), "tbq"
        ),
    }


def upward_cross_check(
    part: PartitionRef,
    rows_5m: list[OutRow],
    higher_rows: list[OutRow],
    n_5m: int,
    label: str,
    expected_count: int,
    require_midnight: bool = False,
    require_4h_hours: bool = False,
) -> list[str]:
    errors: list[str] = []
    idx = {r.open_ms: r for r in rows_5m}
    used = {r.open_ms: 0 for r in rows_5m}
    if len(higher_rows) != expected_count:
        errors.append(
            f"{part.symbol} {part.year}-{part.month:02d}: {label} row count {len(higher_rows)} "
            f"!= expected {expected_count}"
        )
    for h in higher_rows:
        if h.symbol != part.symbol:
            errors.append(f"{part.symbol} {iso_of_ms(h.open_ms)}: {label} symbol {h.symbol} mismatch")
            continue
        if require_midnight and h.open_ms % DAY_MS != 0:
            errors.append(f"{part.symbol} {iso_of_ms(h.open_ms)}: {label} is not UTC midnight")
            continue
        if require_4h_hours and (h.open_ms % HOUR_MS != 0 or utc_hour(h.open_ms) not in OPEN_HOURS_4H):
            errors.append(f"{part.symbol} {iso_of_ms(h.open_ms)}: {label} is not a valid UTC 4h open")
            continue
        items: list[OutRow] = []
        missing = False
        for i in range(n_5m):
            ts = h.open_ms + i * BUCKET_MS
            item = idx.get(ts)
            if item is None:
                errors.append(
                    f"{part.symbol} {iso_of_ms(h.open_ms)}: missing 5m candle {iso_of_ms(ts)} "
                    f"required for {label}"
                )
                missing = True
                break
            items.append(item)
        if missing:
            if len(errors) >= 20:
                break
            continue
        for item in items:
            used[item.open_ms] += 1
        implied = combine_5m_group(items)
        expected = h.as_compare_dict()
        diffs = []
        for field in COMPARE_FIELDS:
            if expected[field] != implied[field]:
                diffs.append(
                    f"{field} expected={fmt_val(expected[field])} actual={fmt_val(implied[field])}"
                )
        if diffs:
            errors.append(
                f"{part.symbol} {iso_of_ms(h.open_ms)}: 5m-to-{label} disagreement: " + "; ".join(diffs)
            )
        if len(errors) >= 20:
            break
    unused = sum(1 for c in used.values() if c == 0)
    reused = sum(1 for c in used.values() if c != 1)
    if unused or reused:
        errors.append(
            f"{part.symbol} {part.year}-{part.month:02d}: generated 5m candles did not each "
            f"participate exactly once in {label} cross-check (unused={unused} not_once={reused})"
        )
    return errors[:20]


def validate_output_rows(part: PartitionRef, rows: list[OutRow], expected_fp: str) -> list[str]:
    errors: list[str] = []
    if fingerprint_rows(rows) != expected_fp:
        errors.append("content fingerprint mismatch vs expected aggregation")
    if len(rows) != part.expected_5m_rows:
        errors.append(f"row count {len(rows)} != expected {part.expected_5m_rows}")
    if len(rows) * SOURCE_CANDLES_PER_OUTPUT != part.expected_1m_rows:
        errors.append("output row count * 5 != expected source row count")
    seen = set()
    prev = None
    cutoff = ms_of(datetime(2026, 9, 16, tzinfo=timezone.utc))
    for r in rows:
        if r.symbol != part.symbol:
            errors.append(f"row symbol {r.symbol} != {part.symbol}")
        dt = EPOCH + timedelta(milliseconds=r.open_ms)
        if dt.year != part.year or dt.month != part.month:
            errors.append(f"open_time {iso_of_ms(r.open_ms)} not in {part.year}-{part.month:02d}")
        if not is_utc_5m_open(r.open_ms):
            errors.append(f"open_time {iso_of_ms(r.open_ms)} not a valid UTC 5m open")
        if dt.second != 0 or r.open_ms % 1000 != 0:
            errors.append(f"open_time {iso_of_ms(r.open_ms)} has non-zero second/millisecond")
        if r.source_candle_count != SOURCE_CANDLES_PER_OUTPUT:
            errors.append("source_candle_count != 5")
        if r.close_ms != r.open_ms + BUCKET_MS - 1:
            errors.append(f"close_time {iso_of_ms_us(r.close_ms)} incorrect for {iso_of_ms(r.open_ms)}")
        if r.open_ms in seen:
            errors.append(f"duplicate open_time {iso_of_ms(r.open_ms)}")
        seen.add(r.open_ms)
        if prev is not None:
            if r.open_ms <= prev.open_ms:
                errors.append("output not strictly chronological")
            elif r.open_ms - prev.open_ms != BUCKET_MS:
                errors.append(
                    f"gap/step {r.open_ms - prev.open_ms} ms between {iso_of_ms(prev.open_ms)} and {iso_of_ms(r.open_ms)}"
                )
        if not (r.high >= max(r.open, r.close) and r.low <= min(r.open, r.close) and r.high >= r.low):
            errors.append(f"OHLC violation at {iso_of_ms(r.open_ms)}")
        if (
            r.volume < 0 or r.quote_asset_volume < 0 or r.taker_buy_base_asset_volume < 0
            or r.taker_buy_quote_asset_volume < 0 or r.number_of_trades < 0
        ):
            errors.append(f"negative volume/trades at {iso_of_ms(r.open_ms)}")
        if r.open_ms >= cutoff:
            errors.append("row on or after 2026-09-16")
        prev = r
    if rows:
        if rows[0].open_ms != part.expected_first_open_ms:
            errors.append(f"first open {iso_of_ms(rows[0].open_ms)} != {iso_of_ms(part.expected_first_open_ms)}")
        if rows[-1].open_ms != part.expected_last_open_ms:
            errors.append(f"last open {iso_of_ms(rows[-1].open_ms)} != {iso_of_ms(part.expected_last_open_ms)}")
    return errors[:20]


def load_higher_for_partition(part: PartitionRef) -> tuple[list[OutRow], list[OutRow], list[OutRow], list[OutRow], list[OutRow]]:
    r15 = table_to_rows(read_secondary_table(part.path_15m, "15m"))
    r30 = table_to_rows(read_secondary_table(part.path_30m, "30m"))
    r1h = table_to_rows(read_secondary_table(part.path_1h, "1h"))
    r4h = table_to_rows(read_secondary_table(part.path_4h, "4h"))
    r1d_all = table_to_rows(read_secondary_table(part.path_1d, "1d"))
    r1d = filter_rows_to_partition(r1d_all, part)
    return r15, r30, r1h, r4h, r1d


def _htf_bundle(part: PartitionRef, rows_5m: list[OutRow], r15, r30, r1h, r4h, r1d) -> list[str]:
    errors: list[str] = []
    errors.extend(upward_cross_check(part, rows_5m, r15, N_5M_PER_15M, "15m", part.expected_15m_rows))
    errors.extend(upward_cross_check(part, rows_5m, r30, N_5M_PER_30M, "30m", part.expected_30m_rows))
    errors.extend(upward_cross_check(part, rows_5m, r1h, N_5M_PER_1H, "1h", part.expected_1h_rows))
    errors.extend(upward_cross_check(
        part, rows_5m, r4h, N_5M_PER_4H, "4h", part.expected_4h_rows, require_4h_hours=True
    ))
    errors.extend(upward_cross_check(
        part, rows_5m, r1d, N_5M_PER_1D, "1d", part.expected_1d_rows, require_midnight=True
    ))
    return errors


def provenance_meta(
    part: PartitionRef, work: PartitionWork,
    source_sha: str, sha_15m: str, sha_30m: str, sha_1h: str, sha_4h: str, sha_1d: str,
) -> dict[bytes, bytes]:
    return {
        b"schema_version": SCHEMA_VERSION.encode(),
        b"provider": b"binance",
        b"market": b"usd_m_perpetual",
        b"archive_namespace": b"futures/um",
        b"interval": OUTPUT_INTERVAL.encode(),
        b"primary_source_interval": SOURCE_INTERVAL.encode(),
        b"validation_interval_1": b"15m",
        b"validation_interval_2": b"30m",
        b"validation_interval_3": b"1h",
        b"validation_interval_4": b"4h",
        b"validation_interval_5": b"1d",
        b"timezone": b"UTC",
        b"bucket_alignment": b"UTC",
        b"source_candles_per_output": b"5",
        b"aggregation_rules_version": AGGREGATION_RULES_VERSION.encode(),
        b"symbol": part.symbol.encode(),
        b"year": str(part.year).encode(),
        b"month": f"{part.month:02d}".encode(),
        b"source_canonical_filename": part.source_path.name.encode(),
        b"source_canonical_path": str(part.source_path).encode(),
        b"source_canonical_sha256": source_sha.encode(),
        b"source_15m_filename": part.path_15m.name.encode(),
        b"source_15m_path": str(part.path_15m).encode(),
        b"source_15m_sha256": sha_15m.encode(),
        b"source_30m_filename": part.path_30m.name.encode(),
        b"source_30m_path": str(part.path_30m).encode(),
        b"source_30m_sha256": sha_30m.encode(),
        b"source_1h_filename": part.path_1h.name.encode(),
        b"source_1h_path": str(part.path_1h).encode(),
        b"source_1h_sha256": sha_1h.encode(),
        b"source_4h_filename": part.path_4h.name.encode(),
        b"source_4h_path": str(part.path_4h).encode(),
        b"source_4h_sha256": sha_4h.encode(),
        b"source_1d_filename": part.path_1d.name.encode(),
        b"source_1d_path": str(part.path_1d).encode(),
        b"source_1d_sha256": sha_1d.encode(),
        b"source_row_count": str(work.source_rows_read).encode(),
        b"output_row_count": str(len(work.rows)).encode(),
        b"expected_first_open_time": (iso_of_ms(part.expected_first_open_ms) or "").encode(),
        b"expected_last_open_time": (iso_of_ms(part.expected_last_open_ms) or "").encode(),
        b"content_fingerprint": work.content_fingerprint.encode(),
    }


def write_validated_parquet(
    part: PartitionRef, table: pa.Table, work: PartitionWork,
    source_sha: str, sha_15m: str, sha_30m: str, sha_1h: str, sha_4h: str, sha_1d: str,
    r15, r30, r1h, r4h, r1d,
) -> dict:
    dest = part.output_path
    assert_outside_all(dest, "5m output")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    if tmp.exists():
        tmp.unlink()
    schema = table.schema.with_metadata(
        {**(table.schema.metadata or {}), **provenance_meta(
            part, work, source_sha, sha_15m, sha_30m, sha_1h, sha_4h, sha_1d
        )}
    )
    table = table.cast(schema)
    pq.write_table(
        table, tmp, compression=PARQUET_COMPRESSION, compression_level=PARQUET_COMPRESSION_LEVEL,
        version="2.6", coerce_timestamps="ms", allow_truncated_timestamps=False,
        write_statistics=True, use_dictionary=["symbol"], row_group_size=max(len(work.rows), 1),
    )
    read_back = pq.ParquetFile(tmp).read()
    rb_rows = table_to_rows(read_back)
    rb_fp = fingerprint_rows(rb_rows)
    errors = validate_output_rows(part, rb_rows, work.content_fingerprint)
    if rb_fp != work.content_fingerprint:
        errors.append(f"read-back fingerprint {rb_fp} != expected {work.content_fingerprint}")
    errors.extend(_htf_bundle(part, rb_rows, r15, r30, r1h, r4h, r1d))
    md = {k.decode(): v.decode(errors="replace") for k, v in (read_back.schema.metadata or {}).items()}
    if md.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"provenance schema_version {md.get('schema_version')}")
    if md.get("source_canonical_sha256") != source_sha:
        errors.append("provenance source sha256 mismatch")
    if (
        md.get("source_15m_sha256") != sha_15m or md.get("source_30m_sha256") != sha_30m
        or md.get("source_1h_sha256") != sha_1h or md.get("source_4h_sha256") != sha_4h
        or md.get("source_1d_sha256") != sha_1d
    ):
        errors.append("provenance validation-source sha256 mismatch")
    if md.get("interval") != "5m" or md.get("primary_source_interval") != "1m":
        errors.append("provenance interval fields incorrect")
    if errors:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise ValueError("; ".join(errors))
    os.replace(tmp, dest)
    return {
        "file_size_bytes": dest.stat().st_size,
        "output_sha256": sha256_file(dest),
        "readback_fingerprint": rb_fp,
        "num_rows": len(rb_rows),
        "compression": PARQUET_COMPRESSION,
    }


def existing_is_current(
    part: PartitionRef, work: PartitionWork,
    source_sha: str, sha_15m: str, sha_30m: str, sha_1h: str, sha_4h: str, sha_1d: str,
    r15, r30, r1h, r4h, r1d,
) -> tuple[bool, list[str]]:
    dest = part.output_path
    if not dest.is_file():
        return False, ["missing"]
    reasons: list[str] = []
    try:
        table = pq.ParquetFile(dest).read()
        md = {k.decode(): v.decode(errors="replace") for k, v in (table.schema.metadata or {}).items()}
        if md.get("schema_version") != SCHEMA_VERSION:
            reasons.append("schema_version")
        if md.get("source_canonical_sha256") != source_sha:
            reasons.append("source_sha256")
        if md.get("source_15m_sha256") != sha_15m:
            reasons.append("source_15m_sha256")
        if md.get("source_30m_sha256") != sha_30m:
            reasons.append("source_30m_sha256")
        if md.get("source_1h_sha256") != sha_1h:
            reasons.append("source_1h_sha256")
        if md.get("source_4h_sha256") != sha_4h:
            reasons.append("source_4h_sha256")
        if md.get("source_1d_sha256") != sha_1d:
            reasons.append("source_1d_sha256")
        if md.get("interval") != "5m":
            reasons.append("interval")
        rows = table_to_rows(table)
        fp = fingerprint_rows(rows)
        if fp != work.content_fingerprint:
            reasons.append("content_fingerprint")
        if md.get("content_fingerprint") != work.content_fingerprint:
            reasons.append("recorded_fingerprint")
        reasons.extend(["row_validate:" + e for e in validate_output_rows(part, rows, work.content_fingerprint)])
        reasons.extend(["xcheck:" + e for e in _htf_bundle(part, rows, r15, r30, r1h, r4h, r1d)])
    except Exception as exc:
        reasons.append(f"{type(exc).__name__}: {exc}")
    return (not reasons), reasons


def _fail_rec(rec: dict, error: str, test_context: Optional[str]) -> dict:
    rec["status"] = "failed"
    rec["error"] = error
    if test_context:
        log.error(
            "INTENTIONAL SYNTHETIC TEST ERROR [%s]: %s %04d-%02d -- %s",
            test_context, rec["symbol"], rec["year"], rec["month"], error,
        )
    else:
        log.error("%s %04d-%02d: FAILED -- %s", rec["symbol"], rec["year"], rec["month"], error)
    return rec


def process_partition(part: PartitionRef, allow_incomplete: bool = False, test_context: Optional[str] = None) -> dict:
    rec = {
        "symbol": part.symbol, "year": part.year, "month": part.month,
        "source_path": str(part.source_path), "path_15m": str(part.path_15m),
        "path_30m": str(part.path_30m), "path_1h": str(part.path_1h),
        "path_4h": str(part.path_4h), "path_1d": str(part.path_1d),
        "output_path": str(part.output_path), "status": None, "error": None,
        "source_canonical_sha256": None, "source_15m_sha256": None,
        "source_30m_sha256": None, "source_1h_sha256": None,
        "source_4h_sha256": None, "source_1d_sha256": None,
        "output_parquet_sha256": None, "expected_fingerprint": None, "readback_fingerprint": None,
        "aggregation_equivalent": False, "secondary_15m_equivalent": False,
        "secondary_30m_equivalent": False, "secondary_1h_equivalent": False,
        "secondary_4h_equivalent": False, "secondary_1d_equivalent": False,
        "canonical_1m_checks_passed": 0, "canonical_1m_checks_total": 0,
        "secondary_15m_checks_passed": 0, "secondary_15m_checks_total": 0,
        "secondary_30m_checks_passed": 0, "secondary_30m_checks_total": 0,
        "secondary_1h_checks_passed": 0, "secondary_1h_checks_total": 0,
        "secondary_4h_checks_passed": 0, "secondary_4h_checks_total": 0,
        "secondary_1d_checks_passed": 0, "secondary_1d_checks_total": 0, "notes": [],
    }
    try:
        source_sha = sha256_file(part.source_path)
        rec["source_canonical_sha256"] = source_sha
        work = aggregate_partition(part, read_1m_table(part.source_path))
        rec.update({
            "source_rows_read": work.source_rows_read,
            "source_rows_consumed": work.source_rows_consumed,
            "source_rows_omitted": work.source_rows_omitted,
            "source_rows_reused": work.source_rows_reused,
            "complete_buckets": work.complete_buckets,
            "incomplete_bucket_count": len(work.incomplete_buckets),
            "incomplete_buckets": work.incomplete_buckets[:10],
            "output_row_count": len(work.rows),
            "expected_output_row_count": part.expected_5m_rows,
            "first_open_time_utc": iso_of_ms(work.first_open_ms),
            "last_open_time_utc": iso_of_ms(work.last_open_ms),
            "last_close_time_utc": iso_of_ms_us(work.last_close_ms),
            "missing_bucket_count": work.missing_bucket_count,
            "duplicate_bucket_count": work.duplicate_bucket_count,
            "out_of_order_count": work.out_of_order_count,
            "null_count": work.null_count,
            "ohlc_violation_count": work.ohlc_violation_count,
            "negative_volume_count": work.negative_volume_count,
            "rows_outside_range": work.rows_outside_range,
            "expected_fingerprint": work.content_fingerprint,
            "canonical_1m_checks_total": len(work.rows),
            "accounting_ok": (
                work.source_rows_consumed == work.source_rows_read
                and work.source_rows_omitted == 0 and work.source_rows_reused == 0
                and len(work.rows) * SOURCE_CANDLES_PER_OUTPUT == work.source_rows_read
            ),
        })
        hard_fail = (
            (work.incomplete_buckets and not allow_incomplete)
            or work.source_rows_omitted or work.source_rows_reused
            or work.missing_bucket_count or work.duplicate_bucket_count
            or work.out_of_order_count or work.ohlc_violation_count
            or work.negative_volume_count or work.null_count or work.rows_outside_range
            or (not allow_incomplete and len(work.rows) != part.expected_5m_rows)
            or (not allow_incomplete and len(work.rows) * SOURCE_CANDLES_PER_OUTPUT != work.source_rows_read)
        )
        if hard_fail:
            rec["canonical_1m_checks_passed"] = 0
            return _fail_rec(
                rec,
                (
                    f"incomplete={len(work.incomplete_buckets)} omitted={work.source_rows_omitted} "
                    f"reused={work.source_rows_reused} missing_buckets={work.missing_bucket_count} "
                    f"first_incomplete={work.incomplete_buckets[:1]}"
                ),
                test_context,
            )
        rec["canonical_1m_checks_passed"] = len(work.rows)
        rec["aggregation_equivalent"] = True

        for label, path in (
            ("15m", part.path_15m), ("30m", part.path_30m), ("1h", part.path_1h),
            ("4h", part.path_4h), ("1d", part.path_1d),
        ):
            if not path.is_file():
                return _fail_rec(rec, f"missing {label} validation partition {path}", test_context)
        sha_15m = sha256_file(part.path_15m)
        sha_30m = sha256_file(part.path_30m)
        sha_1h = sha256_file(part.path_1h)
        sha_4h = sha256_file(part.path_4h)
        sha_1d = sha256_file(part.path_1d)
        rec["source_15m_sha256"] = sha_15m
        rec["source_30m_sha256"] = sha_30m
        rec["source_1h_sha256"] = sha_1h
        rec["source_4h_sha256"] = sha_4h
        rec["source_1d_sha256"] = sha_1d
        r15, r30, r1h, r4h, r1d = load_higher_for_partition(part)
        rec["secondary_15m_checks_total"] = part.expected_15m_rows
        rec["secondary_30m_checks_total"] = part.expected_30m_rows
        rec["secondary_1h_checks_total"] = part.expected_1h_rows
        rec["secondary_4h_checks_total"] = part.expected_4h_rows
        rec["secondary_1d_checks_total"] = part.expected_1d_rows

        x15 = upward_cross_check(part, work.rows, r15, N_5M_PER_15M, "15m", part.expected_15m_rows)
        if x15:
            return _fail_rec(rec, "5m-to-15m cross-check disagreement (not repaired): " + " | ".join(x15[:5]), test_context)
        rec["secondary_15m_checks_passed"] = part.expected_15m_rows
        rec["secondary_15m_equivalent"] = True

        x30 = upward_cross_check(part, work.rows, r30, N_5M_PER_30M, "30m", part.expected_30m_rows)
        if x30:
            return _fail_rec(rec, "5m-to-30m cross-check disagreement (not repaired): " + " | ".join(x30[:5]), test_context)
        rec["secondary_30m_checks_passed"] = part.expected_30m_rows
        rec["secondary_30m_equivalent"] = True

        x1h = upward_cross_check(part, work.rows, r1h, N_5M_PER_1H, "1h", part.expected_1h_rows)
        if x1h:
            return _fail_rec(rec, "5m-to-1h cross-check disagreement (not repaired): " + " | ".join(x1h[:5]), test_context)
        rec["secondary_1h_checks_passed"] = part.expected_1h_rows
        rec["secondary_1h_equivalent"] = True

        x4h = upward_cross_check(part, work.rows, r4h, N_5M_PER_4H, "4h", part.expected_4h_rows, require_4h_hours=True)
        if x4h:
            return _fail_rec(rec, "5m-to-4h cross-check disagreement (not repaired): " + " | ".join(x4h[:5]), test_context)
        rec["secondary_4h_checks_passed"] = part.expected_4h_rows
        rec["secondary_4h_equivalent"] = True

        x1d = upward_cross_check(part, work.rows, r1d, N_5M_PER_1D, "1d", part.expected_1d_rows, require_midnight=True)
        if x1d:
            return _fail_rec(rec, "5m-to-1d cross-check disagreement (not repaired): " + " | ".join(x1d[:5]), test_context)
        rec["secondary_1d_checks_passed"] = part.expected_1d_rows
        rec["secondary_1d_equivalent"] = True

        skippable, reasons = existing_is_current(
            part, work, source_sha, sha_15m, sha_30m, sha_1h, sha_4h, sha_1d, r15, r30, r1h, r4h, r1d
        )
        if skippable:
            rec["status"] = "skipped-as-already-equivalent"
            rec["output_parquet_sha256"] = sha256_file(part.output_path)
            rec["readback_fingerprint"] = work.content_fingerprint
            rec["notes"].append("existing 5m partition fully equivalent; not rewritten")
            log.info("%s %04d-%02d: skip (equivalent, %d 5m rows)", part.symbol, part.year, part.month, len(work.rows))
            return rec

        rebuilt = part.output_path.exists()
        if rebuilt:
            rec["notes"].append(f"rebuilding because existing output is not current: {reasons[:8]}")
            log.warning("%s %04d-%02d: rebuilding (%s)", part.symbol, part.year, part.month, reasons[:6])
        meta = write_validated_parquet(
            part, rows_to_table(work.rows), work, source_sha, sha_15m, sha_30m, sha_1h, sha_4h, sha_1d,
            r15, r30, r1h, r4h, r1d,
        )
        rec["output_parquet_sha256"] = meta["output_sha256"]
        rec["readback_fingerprint"] = meta["readback_fingerprint"]
        rec["file_size_bytes"] = meta["file_size_bytes"]
        rec["status"] = "rebuilt" if rebuilt else "created"
        log.info(
            "%s %04d-%02d: %s %d 5m rows from %d 1m rows (15m %d, 30m %d, 1h %d, 4h %d, 1d %d) fp=%s",
            part.symbol, part.year, part.month, rec["status"], len(work.rows), work.source_rows_read,
            rec["secondary_15m_checks_passed"], rec["secondary_30m_checks_passed"],
            rec["secondary_1h_checks_passed"], rec["secondary_4h_checks_passed"],
            rec["secondary_1d_checks_passed"], work.content_fingerprint[:16],
        )
        return rec
    except Exception as exc:
        leftover = part.output_path.with_name(part.output_path.name + ".part")
        if leftover.exists():
            leftover.unlink()
        rec["notes"].append(traceback.format_exc(limit=4))
        return _fail_rec(rec, f"{type(exc).__name__}: {exc}", test_context)


# ----------------------------------------------------------------------------- independent scan
def independent_scan(partitions: list[PartitionRef], output_root: Path) -> dict:
    files = sorted(output_root.rglob("data.parquet")) if output_root.exists() else []
    parts_left = list(output_root.rglob("*.part")) if output_root.exists() else []
    parent = output_root.parent
    other_tf = [
        str(parent / tf) for tf in ("1w", "1W", "weekly", "1M", "monthly", "8h", "12h", "2h", "3d")
        if (parent / tf).exists()
    ]
    symbols = sorted({p.parent.parent.parent.name.split("=", 1)[1] for p in files}) if files else []
    rows_by_sym: dict[str, int] = {s: 0 for s in SYMBOLS}
    first_last: dict[str, list[int]] = {}
    last_close: dict[str, int] = {}
    eq_fail = s15 = s30 = s1h = s4h = s1d = align = src_count = continuity = 0
    c_pass = p15 = p30 = p1h = p4h = p1d = 0
    prev_last: dict[str, Optional[int]] = {s: None for s in SYMBOLS}
    cache_1d: dict[tuple[str, int], list[OutRow]] = {}
    for part in partitions:
        if not part.output_path.is_file():
            eq_fail += 1
            continue
        work = aggregate_partition(part, read_1m_table(part.source_path))
        out = table_to_rows(pq.ParquetFile(part.output_path).read())
        if fingerprint_rows(out) != work.content_fingerprint:
            eq_fail += 1
        else:
            c_pass += len(out)
        r15 = table_to_rows(read_secondary_table(part.path_15m, "15m"))
        r30 = table_to_rows(read_secondary_table(part.path_30m, "30m"))
        r1h = table_to_rows(read_secondary_table(part.path_1h, "1h"))
        r4h = table_to_rows(read_secondary_table(part.path_4h, "4h"))
        key = (part.symbol, part.year)
        if key not in cache_1d:
            cache_1d[key] = table_to_rows(read_secondary_table(part.path_1d, "1d"))
        r1d = filter_rows_to_partition(cache_1d[key], part)
        if upward_cross_check(part, out, r15, N_5M_PER_15M, "15m", part.expected_15m_rows):
            s15 += 1
        else:
            p15 += len(r15)
        if upward_cross_check(part, out, r30, N_5M_PER_30M, "30m", part.expected_30m_rows):
            s30 += 1
        else:
            p30 += len(r30)
        if upward_cross_check(part, out, r1h, N_5M_PER_1H, "1h", part.expected_1h_rows):
            s1h += 1
        else:
            p1h += len(r1h)
        if upward_cross_check(part, out, r4h, N_5M_PER_4H, "4h", part.expected_4h_rows, require_4h_hours=True):
            s4h += 1
        else:
            p4h += len(r4h)
        if upward_cross_check(part, out, r1d, N_5M_PER_1D, "1d", part.expected_1d_rows, require_midnight=True):
            s1d += 1
        else:
            p1d += len(r1d)
        for r in out:
            if not is_utc_5m_open(r.open_ms):
                align += 1
            if r.source_candle_count != SOURCE_CANDLES_PER_OUTPUT:
                src_count += 1
        rows_by_sym[part.symbol] = rows_by_sym.get(part.symbol, 0) + len(out)
        if out:
            sl = first_last.setdefault(part.symbol, [out[0].open_ms, out[-1].open_ms])
            sl[0] = min(sl[0], out[0].open_ms)
            sl[1] = max(sl[1], out[-1].open_ms)
            last_close[part.symbol] = max(last_close.get(part.symbol, 0), out[-1].close_ms)
            prev = prev_last[part.symbol]
            if prev is not None and out[0].open_ms - prev != BUCKET_MS:
                continuity += 1
            prev_last[part.symbol] = out[-1].open_ms
    total = sum(rows_by_sym.values())
    all_ok = (
        len(files) == EXPECTED_OUTPUT_PARTITIONS and symbols == sorted(SYMBOLS)
        and total == EXPECTED_OUTPUT_ROWS_TOTAL
        and all(v == EXPECTED_OUTPUT_ROWS_PER_SYMBOL for v in rows_by_sym.values())
        and eq_fail == s15 == s30 == s1h == s4h == s1d == align == src_count == continuity == 0
        and c_pass == EXPECTED_OUTPUT_ROWS_TOTAL
        and p15 == EXPECTED_15M_TOTAL and p30 == EXPECTED_30M_TOTAL
        and p1h == EXPECTED_1H_TOTAL and p4h == EXPECTED_4H_TOTAL and p1d == EXPECTED_1D_TOTAL
        and not other_tf and not parts_left
        and all(iso_of_ms(v[0]) == EXPECTED_FIRST_OPEN for v in first_last.values())
        and all(iso_of_ms(v[1]) == EXPECTED_LAST_OPEN for v in first_last.values())
        and all(iso_of_ms_us(v) == EXPECTED_LAST_CLOSE for v in last_close.values())
    )
    return {
        "parquet_files": len(files), "part_files": len(parts_left),
        "other_timeframes_present": other_tf, "symbols": symbols,
        "unexpected_symbols": [s for s in symbols if s not in SYMBOLS],
        "rows_by_symbol": rows_by_sym, "total_rows": total,
        "first_open_time_utc": {s: iso_of_ms(v[0]) for s, v in first_last.items()},
        "last_open_time_utc": {s: iso_of_ms(v[1]) for s, v in first_last.items()},
        "last_close_time_utc": {s: iso_of_ms_us(v) for s, v in last_close.items()},
        "canonical_1m_equivalence_failures": eq_fail,
        "secondary_15m_crosscheck_failures": s15,
        "secondary_30m_crosscheck_failures": s30,
        "secondary_1h_crosscheck_failures": s1h,
        "secondary_4h_crosscheck_failures": s4h,
        "secondary_1d_crosscheck_failures": s1d,
        "alignment_failures": align, "source_candle_count_failures": src_count,
        "month_year_boundary_continuity_failures": continuity,
        "canonical_1m_checks_passed": c_pass, "canonical_1m_checks_total": total,
        "secondary_15m_checks_passed": p15, "secondary_15m_checks_total": EXPECTED_15M_TOTAL,
        "secondary_30m_checks_passed": p30, "secondary_30m_checks_total": EXPECTED_30M_TOTAL,
        "secondary_1h_checks_passed": p1h, "secondary_1h_checks_total": EXPECTED_1H_TOTAL,
        "secondary_4h_checks_passed": p4h, "secondary_4h_checks_total": EXPECTED_4H_TOTAL,
        "secondary_1d_checks_passed": p1d, "secondary_1d_checks_total": EXPECTED_1D_TOTAL,
        "all_counts_expected": all_ok,
    }


# ----------------------------------------------------------------------------- tests
def _write_synthetic_1m(
    path: Path, symbol: str, start_ms: int, n_minutes: int,
    skip_open_ms: Optional[int] = None, duplicate_open_ms: Optional[int] = None,
) -> None:
    assert_outside_all(path, "synthetic 1m")
    path.parent.mkdir(parents=True, exist_ok=True)
    open_ms, close_ms, px = [], [], []
    for i in range(n_minutes):
        t = start_ms + i * MINUTE_MS
        if skip_open_ms is not None and t == skip_open_ms:
            continue
        open_ms.append(t)
        close_ms.append(t + 59_999)
        px.append(Decimal("100.00000000") + Decimal(i) / Decimal("100000000"))
        if duplicate_open_ms is not None and t == duplicate_open_ms:
            open_ms.append(t)
            close_ms.append(t + 59_999)
            px.append(Decimal("100.00000000") + Decimal(i) / Decimal("100000000"))
    n = len(open_ms)
    schema = pa.schema([
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("interval", pa.string(), nullable=False),
        pa.field("open_time", TS_UTC, nullable=False),
        pa.field("open", DECIMAL_TYPE, nullable=False),
        pa.field("high", DECIMAL_TYPE, nullable=False),
        pa.field("low", DECIMAL_TYPE, nullable=False),
        pa.field("close", DECIMAL_TYPE, nullable=False),
        pa.field("volume", DECIMAL_TYPE, nullable=False),
        pa.field("close_time", TS_UTC, nullable=False),
        pa.field("quote_volume", DECIMAL_TYPE, nullable=False),
        pa.field("count", pa.int64(), nullable=False),
        pa.field("taker_buy_volume", DECIMAL_TYPE, nullable=False),
        pa.field("taker_buy_quote_volume", DECIMAL_TYPE, nullable=False),
        pa.field("ignore", pa.int64(), nullable=False),
    ])
    one = Decimal("1.00000000")
    table = pa.table({
        "symbol": pa.array([symbol] * n, pa.string()),
        "interval": pa.array(["1m"] * n, pa.string()),
        "open_time": pa.array(open_ms, TS_UTC),
        "open": pa.array(px, DECIMAL_TYPE),
        "high": pa.array([p + one for p in px], DECIMAL_TYPE),
        "low": pa.array([p for p in px], DECIMAL_TYPE),
        "close": pa.array([p + Decimal("0.50000000") for p in px], DECIMAL_TYPE),
        "volume": pa.array([Decimal("10.00000000")] * n, DECIMAL_TYPE),
        "close_time": pa.array(close_ms, TS_UTC),
        "quote_volume": pa.array([Decimal("100.00000000")] * n, DECIMAL_TYPE),
        "count": pa.array([3] * n, pa.int64()),
        "taker_buy_volume": pa.array([Decimal("4.00000000")] * n, DECIMAL_TYPE),
        "taker_buy_quote_volume": pa.array([Decimal("40.00000000")] * n, DECIMAL_TYPE),
        "ignore": pa.array([0] * n, pa.int64()),
    }, schema=schema)
    pq.write_table(table, path, compression="zstd")


def _write_synthetic_derived_matching_1m(src_1m: Path, dest: Path, symbol: str, bucket_ms: int, n_source: int) -> None:
    assert_outside_all(dest, "synthetic derived")
    dest.parent.mkdir(parents=True, exist_ok=True)
    table = read_1m_table(src_1m)
    n = table.num_rows
    open_ms = pc.cast(table.column("open_time"), pa.int64()).to_pylist()
    close_ms = pc.cast(table.column("close_time"), pa.int64()).to_pylist()
    index_by_open = {open_ms[i]: i for i in range(n)}
    opens = table.column("open").to_pylist()
    highs = table.column("high").to_pylist()
    lows = table.column("low").to_pylist()
    closes = table.column("close").to_pylist()
    vols = table.column("volume").to_pylist()
    qvols = table.column("quote_volume").to_pylist()
    trades = table.column("count").to_pylist()
    tb_base = table.column("taker_buy_volume").to_pylist()
    tb_quote = table.column("taker_buy_quote_volume").to_pylist()
    starts = sorted({o - (o % bucket_ms) for o in open_ms})
    rows: list[OutRow] = []
    for bstart in starts:
        wanted = [bstart + k * MINUTE_MS for k in range(n_source)]
        idxs = [index_by_open[w] for w in wanted if w in index_by_open]
        if len(idxs) != n_source:
            continue
        b_highs = [to_dec128(highs[i], "high") for i in idxs]
        b_lows = [to_dec128(lows[i], "low") for i in idxs]
        b_vols = [to_dec128(vols[i], "volume") for i in idxs]
        b_qvols = [to_dec128(qvols[i], "quote") for i in idxs]
        b_tb = [to_dec128(tb_base[i], "tb") for i in idxs]
        b_tbq = [to_dec128(tb_quote[i], "tbq") for i in idxs]
        rows.append(OutRow(
            symbol=symbol, open_ms=bstart, close_ms=int(close_ms[idxs[-1]]),
            open=to_dec128(opens[idxs[0]], "open"), high=to_dec128(max(b_highs), "high"),
            low=to_dec128(min(b_lows), "low"), close=to_dec128(closes[idxs[-1]], "close"),
            volume=to_dec128(sum(b_vols, Decimal("0")), "volume"),
            quote_asset_volume=to_dec128(sum(b_qvols, Decimal("0")), "quote"),
            number_of_trades=sum(int(trades[i]) for i in idxs),
            taker_buy_base_asset_volume=to_dec128(sum(b_tb, Decimal("0")), "tb"),
            taker_buy_quote_asset_volume=to_dec128(sum(b_tbq, Decimal("0")), "tbq"),
            source_candle_count=n_source,
        ))
    pq.write_table(rows_to_table(rows), dest, compression="zstd")


def _mutate_high(path: Path, delta: Decimal) -> None:
    tbl = pq.ParquetFile(path).read()
    highs = tbl.column("high").to_pylist()
    highs[0] = to_dec128(Decimal(highs[0]) + delta, "high")
    arrays = {name: tbl.column(name) for name in tbl.column_names}
    arrays["high"] = pa.array(highs, DECIMAL_TYPE)
    pq.write_table(pa.table(arrays, schema=tbl.schema), path, compression="zstd")


def run_self_tests() -> None:
    test_root = TMP_ROOT / "self_tests"
    if test_root.exists():
        shutil.rmtree(test_root)
    test_root.mkdir(parents=True, exist_ok=True)
    src_root = test_root / "canonical_1m"
    root_15 = test_root / "derived_15m"
    root_30 = test_root / "derived_30m"
    root_1h = test_root / "derived_1h"
    root_4h = test_root / "derived_4h"
    root_1d = test_root / "derived_1d"
    dst_root = test_root / "derived_5m"
    log.info("self-tests starting under %s", test_root)

    def copy_real(symbol: str, year: int, month: int) -> PartitionRef:
        mapping = [
            (CANONICAL_1M_ROOT, src_root, True),
            (DERIVED_15M_ROOT, root_15, True),
            (DERIVED_30M_ROOT, root_30, True),
            (DERIVED_1H_ROOT, root_1h, True),
            (DERIVED_4H_ROOT, root_4h, True),
        ]
        copied = []
        for src_base, dst_base, monthly in mapping:
            src = hive_month_path(src_base, symbol, year, month)
            dst = hive_month_path(dst_base, symbol, year, month)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            copied.append(dst)
        src_1d = hive_year_path(DERIVED_1D_ROOT, symbol, year)
        dst_1d = hive_year_path(root_1d, symbol, year)
        dst_1d.parent.mkdir(parents=True, exist_ok=True)
        if not dst_1d.exists():
            shutil.copy2(src_1d, dst_1d)
        rs, re_ = ms_of(RANGE_START_UTC), ms_of(RANGE_END_UTC)
        m_first, m_last = month_bounds_ms(year, month)
        return PartitionRef(
            symbol, year, month, copied[0], copied[1], copied[2], copied[3], copied[4], dst_1d,
            hive_month_path(dst_root, symbol, year, month), max(m_first, rs), min(m_last, re_),
        )

    jan = copy_real("BTCUSDT", 2024, 1)
    rec = process_partition(jan)
    assert rec["status"] == "created", rec
    assert rec["output_row_count"] == 31 * 288, rec["output_row_count"]
    assert rec["source_rows_read"] == 31 * 1440
    assert rec["accounting_ok"] and rec["secondary_1d_equivalent"]
    jan_table = pq.ParquetFile(jan.output_path).read()
    assert jan_table.schema.names == list(OUTPUT_SCHEMA.names)
    assert str(jan_table.schema.field("open").type) == "decimal128(38, 8)"
    jan_rows = table_to_rows(jan_table)
    assert jan_rows[0].open_ms == ms_of(datetime(2024, 1, 1, tzinfo=timezone.utc))
    assert jan_rows[-1].open_ms == ms_of(datetime(2024, 1, 31, 23, 55, tzinfo=timezone.utc))
    assert all(is_utc_5m_open(r.open_ms) and r.source_candle_count == 5 for r in jan_rows)
    assert jan_rows[11].open_ms == ms_of(datetime(2024, 1, 1, 0, 55, tzinfo=timezone.utc))
    assert jan_rows[12].open_ms == ms_of(datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc))
    assert jan_rows[12].open_ms - jan_rows[11].open_ms == BUCKET_MS
    day0_last = jan_rows[287]
    day1_first = jan_rows[288]
    assert iso_of_ms(day0_last.open_ms) == "2024-01-01T23:55:00Z"
    assert iso_of_ms(day1_first.open_ms) == "2024-01-02T00:00:00Z"
    assert day1_first.open_ms - day0_last.open_ms == BUCKET_MS
    log.info("TEST OK: 31-day month + UTC hour/day-boundary continuity BTCUSDT 2024-01 -> %d 5m rows", rec["output_row_count"])

    feb = copy_real("BTCUSDT", 2024, 2)
    rec = process_partition(feb)
    assert rec["status"] == "created" and rec["output_row_count"] == 29 * 288, rec
    feb_rows = table_to_rows(pq.ParquetFile(feb.output_path).read())
    leap = next(r for r in feb_rows if iso_of_ms(r.open_ms) == "2024-02-29T00:00:00Z")
    assert leap.source_candle_count == 5
    log.info("TEST OK: leap February 2024 -> %d 5m rows including 2024-02-29", rec["output_row_count"])

    sept = copy_real("ETHUSDT", 2026, 9)
    rec = process_partition(sept)
    assert rec["status"] == "created", rec
    assert rec["output_row_count"] == EXPECTED_SEPT_2026_OUTPUT_ROWS
    assert rec["last_open_time_utc"] == EXPECTED_LAST_OPEN
    assert rec["last_close_time_utc"] == EXPECTED_LAST_CLOSE
    log.info("TEST OK: September 2026 partial month -> %d 5m rows last=%s", rec["output_row_count"], rec["last_open_time_utc"])

    jan_last = table_to_rows(pq.ParquetFile(jan.output_path).read())[-1]
    feb_first = table_to_rows(pq.ParquetFile(feb.output_path).read())[0]
    assert feb_first.open_ms - jan_last.open_ms == BUCKET_MS
    log.info("TEST OK: month-boundary continuity %s -> %s", iso_of_ms(jan_last.open_ms), iso_of_ms(feb_first.open_ms))

    dec = copy_real("BTCUSDT", 2024, 12)
    jan25 = copy_real("BTCUSDT", 2025, 1)
    assert process_partition(dec)["status"] == "created"
    assert process_partition(jan25)["status"] == "created"
    dec_last = table_to_rows(pq.ParquetFile(dec.output_path).read())[-1]
    jan25_first = table_to_rows(pq.ParquetFile(jan25.output_path).read())[0]
    assert jan25_first.open_ms - dec_last.open_ms == BUCKET_MS
    log.info("TEST OK: year-boundary continuity %s -> %s", iso_of_ms(dec_last.open_ms), iso_of_ms(jan25_first.open_ms))

    sha_before = sha256_file(jan.output_path)
    mtime_before = jan.output_path.stat().st_mtime_ns
    rec2 = process_partition(jan)
    assert rec2["status"] == "skipped-as-already-equivalent", rec2
    assert sha256_file(jan.output_path) == sha_before
    assert jan.output_path.stat().st_mtime_ns == mtime_before
    log.info("TEST OK: idempotent skip of valid existing partition")

    start = ms_of(datetime(2024, 3, 1, tzinfo=timezone.utc))

    def syn_part(tag: str, symbol: str, month: int, start_ms: int, end_ms: int) -> PartitionRef:
        return PartitionRef(
            symbol, 2024, month,
            hive_month_path(test_root / f"{tag}_src", symbol, 2024, month),
            hive_month_path(test_root / f"{tag}_15", symbol, 2024, month),
            hive_month_path(test_root / f"{tag}_30", symbol, 2024, month),
            hive_month_path(test_root / f"{tag}_1h", symbol, 2024, month),
            hive_month_path(test_root / f"{tag}_4h", symbol, 2024, month),
            hive_year_path(test_root / f"{tag}_1d", symbol, 2024),
            hive_month_path(test_root / f"{tag}_dst", symbol, 2024, month),
            start_ms, end_ms,
        )

    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: 4-row incomplete 5m bucket")
    syn = syn_part("inc", "BTCUSDT", 3, start, start + 4 * MINUTE_MS)
    _write_synthetic_1m(syn.source_path, "BTCUSDT", start, 5, skip_open_ms=start + 2 * MINUTE_MS)
    rec = process_partition(syn, test_context="4-row-bucket")
    assert rec["status"] == "failed" and rec["incomplete_bucket_count"] >= 1
    assert not syn.output_path.exists()
    log.info("TEST OK: 4-row bucket rejected (%s)", rec["error"])

    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: duplicate 1m timestamps")
    dup = syn_part("dup", "BTCUSDT", 3, start, start + 4 * MINUTE_MS)
    _write_synthetic_1m(dup.source_path, "BTCUSDT", start, 5, duplicate_open_ms=start + MINUTE_MS)
    rec = process_partition(dup, test_context="duplicate-timestamps")
    assert rec["status"] == "failed" and rec["error"] and "duplicate" in rec["error"].lower()
    log.info("TEST OK: duplicate timestamps rejected (%s)", rec["error"])

    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: invalid UTC minute")
    mis_start = start + MINUTE_MS
    mis = syn_part("mis", "ETHUSDT", 3, mis_start, mis_start + 4 * MINUTE_MS)
    _write_synthetic_1m(mis.source_path, "ETHUSDT", mis_start, 5)
    rec = process_partition(mis, test_context="invalid-minute")
    assert rec["status"] == "failed" and rec["error"] and "misaligned" in rec["error"].lower()
    log.info("TEST OK: invalid UTC minute rejected (%s)", rec["error"])

    def make_complete(tag: str, symbol: str, month: int, n_minutes: int = 2880) -> PartitionRef:
        st = ms_of(datetime(2024, month, 1, tzinfo=timezone.utc))
        part = syn_part(tag, symbol, month, st, st + (n_minutes - 1) * MINUTE_MS)
        _write_synthetic_1m(part.source_path, symbol, st, n_minutes)
        _write_synthetic_derived_matching_1m(part.source_path, part.path_15m, symbol, MS_15M, 15)
        _write_synthetic_derived_matching_1m(part.source_path, part.path_30m, symbol, MS_30M, 30)
        _write_synthetic_derived_matching_1m(part.source_path, part.path_1h, symbol, MS_1H, 60)
        _write_synthetic_derived_matching_1m(part.source_path, part.path_4h, symbol, MS_4H, 240)
        _write_synthetic_derived_matching_1m(part.source_path, part.path_1d, symbol, DAY_MS, 1440)
        return part

    syn2 = make_complete("fp", "ETHUSDT", 4)
    rec = process_partition(syn2)
    assert rec["status"] == "created" and rec["output_row_count"] == 576, rec
    good_fp = rec["expected_fingerprint"]
    tbl = pq.ParquetFile(syn2.output_path).read()
    closes = tbl.column("close").to_pylist()
    closes[0] = to_dec128(Decimal(closes[0]) + Decimal("1.00000000"), "close")
    arrays = {name: tbl.column(name) for name in tbl.column_names}
    arrays["close"] = pa.array(closes, DECIMAL_TYPE)
    pq.write_table(pa.table(arrays, schema=tbl.schema), syn2.output_path, compression="zstd")
    rec = process_partition(syn2)
    assert rec["status"] == "rebuilt" and rec["expected_fingerprint"] == good_fp == rec["readback_fingerprint"]
    log.info("TEST OK: fingerprint mismatch detected and partition rebuilt")

    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: 5m-to-15m discrepancy")
    d15 = make_complete("d15", "LINKUSDT", 5)
    assert process_partition(d15)["status"] == "created"
    _mutate_high(d15.path_15m, Decimal("9.00000000"))
    rec = process_partition(d15, test_context="5m-vs-15m-discrepancy")
    assert rec["status"] == "failed" and "15m" in rec["error"].lower()
    log.info("TEST OK: 5m-to-15m discrepancy detected (%s)", rec["error"])

    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: 5m-to-30m discrepancy")
    d30 = make_complete("d30", "AAVEUSDT", 6)
    assert process_partition(d30)["status"] == "created"
    _mutate_high(d30.path_30m, Decimal("7.00000000"))
    rec = process_partition(d30, test_context="5m-vs-30m-discrepancy")
    assert rec["status"] == "failed" and "30m" in rec["error"].lower()
    log.info("TEST OK: 5m-to-30m discrepancy detected (%s)", rec["error"])

    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: 5m-to-1h discrepancy")
    d1h = make_complete("d1h", "NEARUSDT", 7)
    assert process_partition(d1h)["status"] == "created"
    _mutate_high(d1h.path_1h, Decimal("5.00000000"))
    rec = process_partition(d1h, test_context="5m-vs-1h-discrepancy")
    assert rec["status"] == "failed" and "1h" in rec["error"].lower()
    log.info("TEST OK: 5m-to-1h discrepancy detected (%s)", rec["error"])

    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: 5m-to-4h discrepancy")
    d4h = make_complete("d4h", "LTCUSDT", 8)
    assert process_partition(d4h)["status"] == "created"
    _mutate_high(d4h.path_4h, Decimal("6.00000000"))
    rec = process_partition(d4h, test_context="5m-vs-4h-discrepancy")
    assert rec["status"] == "failed" and "4h" in rec["error"].lower()
    log.info("TEST OK: 5m-to-4h discrepancy detected (%s)", rec["error"])

    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: 5m-to-1d discrepancy")
    d1d = make_complete("d1d", "DOTUSDT", 8)
    assert process_partition(d1d)["status"] == "created"
    _mutate_high(d1d.path_1d, Decimal("4.00000000"))
    rec = process_partition(d1d, test_context="5m-vs-1d-discrepancy")
    assert rec["status"] == "failed" and "1d" in rec["error"].lower()
    log.info("TEST OK: 5m-to-1d discrepancy detected (%s)", rec["error"])

    shutil.rmtree(test_root)
    log.info("self-tests passed; temporary artifacts removed")


# ----------------------------------------------------------------------------- logging / report
def setup_logging(log_file: Path) -> None:
    assert_outside_all(log_file, "log")
    log_file.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)sZ %(levelname)-7s %(message)s", "%Y-%m-%dT%H:%M:%S")
    fmt.converter = time.gmtime
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fh = logging.FileHandler(log_file, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)


def write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    with open(part, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
        fh.write("\n")
    os.replace(part, path)


def load_previous_run_ids(path: Path) -> tuple[Optional[str], Optional[str]]:
    if not path.is_file():
        return None, None
    try:
        prev = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, None
    return prev.get("production_run_id") or prev.get("run_id"), prev.get("idempotence_run_id")


def summarize(records: list[dict]) -> dict:
    by_status = dict(Counter(r.get("status") for r in records))
    per_symbol: dict[str, dict] = {}
    for r in records:
        s = per_symbol.setdefault(r["symbol"], {"rows": 0, "source_rows": 0, "first": None, "last": None, "last_close": None})
        s["rows"] += r.get("output_row_count") or 0
        s["source_rows"] += r.get("source_rows_read") or 0
        fo, lo, lc = r.get("first_open_time_utc"), r.get("last_open_time_utc"), r.get("last_close_time_utc")
        if fo and (s["first"] is None or fo < s["first"]):
            s["first"] = fo
        if lo and (s["last"] is None or lo > s["last"]):
            s["last"] = lo
        if lc and (s["last_close"] is None or lc > s["last_close"]):
            s["last_close"] = lc
    return {
        "by_status": by_status,
        "source_rows_read": sum(r.get("source_rows_read") or 0 for r in records),
        "source_rows_consumed": sum(r.get("source_rows_consumed") or 0 for r in records),
        "source_rows_omitted": sum(r.get("source_rows_omitted") or 0 for r in records),
        "source_rows_reused": sum(r.get("source_rows_reused") or 0 for r in records),
        "complete_buckets": sum(r.get("complete_buckets") or 0 for r in records),
        "incomplete_buckets": sum(r.get("incomplete_bucket_count") or 0 for r in records),
        "output_rows": sum(r.get("output_row_count") or 0 for r in records),
        "fingerprint_matches": sum(
            1 for r in records
            if r.get("aggregation_equivalent") and r.get("expected_fingerprint") == r.get("readback_fingerprint")
        ),
        "canonical_1m_checks_passed": sum(r.get("canonical_1m_checks_passed") or 0 for r in records),
        "canonical_1m_checks_total": sum(r.get("canonical_1m_checks_total") or 0 for r in records),
        "secondary_15m_checks_passed": sum(r.get("secondary_15m_checks_passed") or 0 for r in records),
        "secondary_15m_checks_total": sum(r.get("secondary_15m_checks_total") or 0 for r in records),
        "secondary_30m_checks_passed": sum(r.get("secondary_30m_checks_passed") or 0 for r in records),
        "secondary_30m_checks_total": sum(r.get("secondary_30m_checks_total") or 0 for r in records),
        "secondary_1h_checks_passed": sum(r.get("secondary_1h_checks_passed") or 0 for r in records),
        "secondary_1h_checks_total": sum(r.get("secondary_1h_checks_total") or 0 for r in records),
        "secondary_4h_checks_passed": sum(r.get("secondary_4h_checks_passed") or 0 for r in records),
        "secondary_4h_checks_total": sum(r.get("secondary_4h_checks_total") or 0 for r in records),
        "secondary_1d_checks_passed": sum(r.get("secondary_1d_checks_passed") or 0 for r in records),
        "secondary_1d_checks_total": sum(r.get("secondary_1d_checks_total") or 0 for r in records),
        "failed": sum(1 for r in records if r.get("status") == "failed"),
        "per_symbol": per_symbol,
        "missing_bucket_count": sum(r.get("missing_bucket_count") or 0 for r in records),
        "duplicate_bucket_count": sum(r.get("duplicate_bucket_count") or 0 for r in records),
        "out_of_order_count": sum(r.get("out_of_order_count") or 0 for r in records),
        "null_count": sum(r.get("null_count") or 0 for r in records),
        "ohlc_violation_count": sum(r.get("ohlc_violation_count") or 0 for r in records),
        "negative_volume_count": sum(r.get("negative_volume_count") or 0 for r in records),
        "rows_outside_range": sum(r.get("rows_outside_range") or 0 for r in records),
    }


def print_plan(partitions: list[PartitionRef], already: int) -> None:
    for line in (
        "=" * 100,
        "5m PROCESSING PLAN -- derive UTC-aligned 5m from canonical 1m only",
        "NOT a downloader. NOT Turkey time. NOT generated from 15m/30m/1h/4h/1d.",
        f"Symbols ({len(SYMBOLS)}): {', '.join(SYMBOLS)}",
        f"Primary source (read-only): {CANONICAL_1M_ROOT}",
        f"  partitions={len(partitions)} expected={EXPECTED_SOURCE_PARTITIONS}  1m rows={EXPECTED_SOURCE_ROWS_TOTAL}",
        f"Upward 15m: {DERIVED_15M_ROOT}  -- 3 generated 5m candles per existing 15m",
        f"Upward 30m: {DERIVED_30M_ROOT}  -- 6 generated 5m candles per existing 30m",
        f"Upward 1h:  {DERIVED_1H_ROOT}  -- 12 generated 5m candles per existing 1h",
        f"Upward 4h:  {DERIVED_4H_ROOT}  -- 48 generated 5m candles per existing 4h",
        f"Upward 1d:  {DERIVED_1D_ROOT}  -- 288 generated 5m candles per existing UTC day",
        f"Output: {DERIVED_5M_ROOT}",
        "UTC buckets: minutes 00/05/10/.../55; exactly 5 consecutive unique 1m candles per output candle",
        f"Expected: {EXPECTED_OUTPUT_PARTITIONS} files, {EXPECTED_OUTPUT_ROWS_PER_SYMBOL} rows/symbol, "
        f"{EXPECTED_OUTPUT_ROWS_TOTAL} total",
        f"First open {EXPECTED_FIRST_OPEN}  last open {EXPECTED_LAST_OPEN}  last close {EXPECTED_LAST_CLOSE}",
        f"Accounting identity: {EXPECTED_OUTPUT_ROWS_TOTAL} x 5 = {EXPECTED_SOURCE_ROWS_TOTAL}",
        f"Existing output files: {already} (re-verified; skipped only if fully equivalent)",
        f"Schema {SCHEMA_VERSION}  decimal128({DECIMAL_PRECISION},{DECIMAL_SCALE})  compression {PARQUET_COMPRESSION}",
        "On 5m-vs-higher-TF disagreement: fail the 5m partition; never repair any dataset",
        "=" * 100,
    ):
        log.info(line)


def run_production(run_id: str, started: datetime) -> tuple[int, dict]:
    for forbidden in READONLY_TREES:
        assert_outside(DERIVED_5M_ROOT, forbidden, "derived 5m")
        assert_outside(TMP_ROOT, forbidden, "tmp")
    DERIVED_5M_ROOT.mkdir(parents=True, exist_ok=True)
    TMP_ROOT.mkdir(parents=True, exist_ok=True)

    partitions = discover_partitions(
        CANONICAL_1M_ROOT, DERIVED_15M_ROOT, DERIVED_30M_ROOT, DERIVED_1H_ROOT,
        DERIVED_4H_ROOT, DERIVED_1D_ROOT, DERIVED_5M_ROOT,
    )
    already = sum(1 for p in partitions if p.output_path.exists())
    print_plan(partitions, already)

    log.info("fingerprinting read-only raw + 1m + 15m + 30m + 1h + 4h + 1d + existing scripts/reports...")
    before_raw = fingerprint_tree(RAW_ROOT, ("*.zip", "*.zip.CHECKSUM"))
    before_1m = fingerprint_tree(CANONICAL_1M_ROOT, ("data.parquet",))
    before_15m = fingerprint_tree(DERIVED_15M_ROOT, ("data.parquet",))
    before_30m = fingerprint_tree(DERIVED_30M_ROOT, ("data.parquet",))
    before_1h = fingerprint_tree(DERIVED_1H_ROOT, ("data.parquet",))
    before_4h = fingerprint_tree(DERIVED_4H_ROOT, ("data.parquet",))
    before_1d = fingerprint_tree(DERIVED_1D_ROOT, ("data.parquet",))
    before_scripts = fingerprint_paths(EXISTING_SCRIPTS)
    before_reports = fingerprint_paths(EXISTING_REPORTS)
    log.info(
        "pre-run fingerprints: raw=%d 1m=%d 15m=%d 30m=%d 1h=%d 4h=%d 1d=%d scripts=%d reports=%d",
        len(before_raw), len(before_1m), len(before_15m), len(before_30m), len(before_1h),
        len(before_4h), len(before_1d), len(before_scripts), len(before_reports),
    )
    if (
        len(before_1m) != EXPECTED_SOURCE_PARTITIONS
        or len(before_15m) != EXPECTED_MONTHLY_SECONDARY
        or len(before_30m) != EXPECTED_MONTHLY_SECONDARY
        or len(before_1h) != EXPECTED_MONTHLY_SECONDARY
        or len(before_4h) != EXPECTED_MONTHLY_SECONDARY
        or len(before_1d) != EXPECTED_1D_FILES
    ):
        raise RuntimeError("source partition counts do not match expected values")

    records: list[dict] = []
    interrupted = False
    try:
        for i, part in enumerate(partitions, 1):
            records.append(process_partition(part))
            if i % 33 == 0:
                log.info("progress: %d/%d partitions", i, len(partitions))
    except KeyboardInterrupt:
        interrupted = True
        log.warning("interrupted")

    log.info("independent disk scan of derived 5m + full 1m/15m/30m/1h/4h/1d checks...")
    scan = independent_scan(partitions, DERIVED_5M_ROOT)
    log.info("re-fingerprinting read-only sources...")
    after_raw = fingerprint_tree(RAW_ROOT, ("*.zip", "*.zip.CHECKSUM"))
    after_1m = fingerprint_tree(CANONICAL_1M_ROOT, ("data.parquet",))
    after_15m = fingerprint_tree(DERIVED_15M_ROOT, ("data.parquet",))
    after_30m = fingerprint_tree(DERIVED_30M_ROOT, ("data.parquet",))
    after_1h = fingerprint_tree(DERIVED_1H_ROOT, ("data.parquet",))
    after_4h = fingerprint_tree(DERIVED_4H_ROOT, ("data.parquet",))
    after_1d = fingerprint_tree(DERIVED_1D_ROOT, ("data.parquet",))
    after_scripts = fingerprint_paths(EXISTING_SCRIPTS)
    after_reports = fingerprint_paths(EXISTING_REPORTS)
    immut_raw = compare_fingerprints(before_raw, after_raw)
    immut_1m = compare_fingerprints(before_1m, after_1m)
    immut_15m = compare_fingerprints(before_15m, after_15m)
    immut_30m = compare_fingerprints(before_30m, after_30m)
    immut_1h = compare_fingerprints(before_1h, after_1h)
    immut_4h = compare_fingerprints(before_4h, after_4h)
    immut_1d = compare_fingerprints(before_1d, after_1d)
    immut_scripts = compare_fingerprints(before_scripts, after_scripts)
    immut_reports = compare_fingerprints(before_reports, after_reports)
    if all(x["unchanged"] for x in (immut_raw, immut_1m, immut_15m, immut_30m, immut_1h, immut_4h, immut_1d)):
        log.info("raw, canonical 1m, and derived 15m/30m/1h/4h/1d files unchanged (sha256, size, mtime_ns)")
    else:
        log.error("SOURCE MUTATION")
    if immut_scripts["unchanged"] and immut_reports["unchanged"]:
        log.info("existing scripts and 1m/15m/30m/1h/4h/1d reports unchanged")
    else:
        log.error("SCRIPT/REPORT MUTATION")

    leftover = list(DERIVED_5M_ROOT.rglob("*.part")) if DERIVED_5M_ROOT.exists() else []
    for p in leftover:
        try:
            p.unlink()
        except OSError as exc:
            log.warning("could not remove %s: %s", p, exc)

    summ = summarize(records)
    report_path = REPORTS_DIR / "derived_5m_report.json"
    prev_prod, prev_idem = load_previous_run_ids(report_path)
    skipped_all = summ["by_status"] == {"skipped-as-already-equivalent": len(records)} and records
    production_run_id = (prev_prod or run_id) if skipped_all else run_id
    idempotence_run_id = run_id if skipped_all else prev_idem
    now = datetime.now(timezone.utc)
    dataset_validation = {
        "parquet_files_ok": scan["parquet_files"] == EXPECTED_OUTPUT_PARTITIONS,
        "symbols_ok": scan["symbols"] == sorted(SYMBOLS),
        "rows_per_symbol_ok": all(v == EXPECTED_OUTPUT_ROWS_PER_SYMBOL for v in scan["rows_by_symbol"].values()),
        "total_rows_ok": scan["total_rows"] == EXPECTED_OUTPUT_ROWS_TOTAL,
        "first_open_ok": all(v == EXPECTED_FIRST_OPEN for v in scan["first_open_time_utc"].values()),
        "last_open_ok": all(v == EXPECTED_LAST_OPEN for v in scan["last_open_time_utc"].values()),
        "last_close_ok": all(v == EXPECTED_LAST_CLOSE for v in scan["last_close_time_utc"].values()),
        "source_accounting_ok": (
            summ["source_rows_consumed"] == EXPECTED_SOURCE_ROWS_TOTAL
            and summ["source_rows_omitted"] == 0 and summ["source_rows_reused"] == 0
            and summ["output_rows"] * SOURCE_CANDLES_PER_OUTPUT == summ["source_rows_consumed"]
        ),
        "canonical_1m_equivalence_ok": summ["canonical_1m_checks_passed"] == EXPECTED_OUTPUT_ROWS_TOTAL and scan["canonical_1m_equivalence_failures"] == 0,
        "secondary_15m_equivalence_ok": summ["secondary_15m_checks_passed"] == EXPECTED_15M_TOTAL and scan["secondary_15m_crosscheck_failures"] == 0,
        "secondary_30m_equivalence_ok": summ["secondary_30m_checks_passed"] == EXPECTED_30M_TOTAL and scan["secondary_30m_crosscheck_failures"] == 0,
        "secondary_1h_equivalence_ok": summ["secondary_1h_checks_passed"] == EXPECTED_1H_TOTAL and scan["secondary_1h_crosscheck_failures"] == 0,
        "secondary_4h_equivalence_ok": summ["secondary_4h_checks_passed"] == EXPECTED_4H_TOTAL and scan["secondary_4h_crosscheck_failures"] == 0,
        "secondary_1d_equivalence_ok": summ["secondary_1d_checks_passed"] == EXPECTED_1D_TOTAL and scan["secondary_1d_crosscheck_failures"] == 0,
        "fingerprint_ok": summ["fingerprint_matches"] == len(records) == EXPECTED_OUTPUT_PARTITIONS,
        "zero_defect_counts_ok": (
            summ["missing_bucket_count"] == summ["duplicate_bucket_count"] == summ["incomplete_buckets"]
            == summ["out_of_order_count"] == summ["null_count"] == summ["ohlc_violation_count"]
            == summ["negative_volume_count"] == summ["rows_outside_range"] == 0
        ),
        "no_other_timeframes": not scan["other_timeframes_present"],
        "sources_unchanged": all(x["unchanged"] for x in (immut_raw, immut_1m, immut_15m, immut_30m, immut_1h, immut_4h, immut_1d)),
        "existing_scripts_unchanged": immut_scripts["unchanged"],
        "existing_reports_unchanged": immut_reports["unchanged"],
    }
    dataset_validation["all_passed"] = all(dataset_validation.values())

    report = {
        "report_version": 1,
        "schema_version": SCHEMA_VERSION,
        "aggregation_rules_version": AGGREGATION_RULES_VERSION,
        "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_id": run_id,
        "production_run_id": production_run_id,
        "idempotence_run_id": idempotence_run_id,
        "run_started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_seconds": round((now - started).total_seconds(), 1),
        "interrupted": interrupted,
        "python": sys.version.split()[0],
        "pyarrow": pa.__version__,
        "executable": sys.executable,
        "provider": "binance",
        "market": "usd_m_perpetual",
        "archive_namespace": "futures/um",
        "source_interval": SOURCE_INTERVAL,
        "validation_interval_1": "15m",
        "validation_interval_2": "30m",
        "validation_interval_3": "1h",
        "validation_interval_4": "4h",
        "validation_interval_5": "1d",
        "output_interval": OUTPUT_INTERVAL,
        "timezone": "UTC",
        "bucket_alignment": "UTC",
        "source_candles_per_output": SOURCE_CANDLES_PER_OUTPUT,
        "parquet_compression": PARQUET_COMPRESSION,
        "source_schema_columns": list(SOURCE_REQUIRED_COLUMNS),
        "secondary_schema_columns": list(SECONDARY_REQUIRED_COLUMNS),
        "output_schema_columns": list(OUTPUT_SCHEMA.names),
        "output_schema_types": {f.name: str(f.type) for f in OUTPUT_SCHEMA},
        "decimal": {"precision": DECIMAL_PRECISION, "scale": DECIMAL_SCALE},
        "required_range_utc": {
            "source_first_open_time": RANGE_START_UTC.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_last_open_time": RANGE_END_UTC.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "output_first_open_time_expected": EXPECTED_FIRST_OPEN,
            "output_last_open_time_expected": EXPECTED_LAST_OPEN,
            "output_last_close_time_expected": EXPECTED_LAST_CLOSE,
            "inclusive_source": True,
        },
        "data_root": str(DATA_ROOT),
        "source_root": str(CANONICAL_1M_ROOT),
        "secondary_15m_root": str(DERIVED_15M_ROOT),
        "secondary_30m_root": str(DERIVED_30M_ROOT),
        "secondary_1h_root": str(DERIVED_1H_ROOT),
        "secondary_4h_root": str(DERIVED_4H_ROOT),
        "secondary_1d_root": str(DERIVED_1D_ROOT),
        "output_root": str(DERIVED_5M_ROOT),
        "requested_symbols": list(SYMBOLS),
        "expected": {
            "source_partitions": EXPECTED_SOURCE_PARTITIONS,
            "monthly_secondary_partitions": EXPECTED_MONTHLY_SECONDARY,
            "daily_files": EXPECTED_1D_FILES,
            "output_partitions": EXPECTED_OUTPUT_PARTITIONS,
            "rows_per_symbol": EXPECTED_OUTPUT_ROWS_PER_SYMBOL,
            "total_rows": EXPECTED_OUTPUT_ROWS_TOTAL,
            "source_rows_per_symbol": EXPECTED_SOURCE_ROWS_PER_SYMBOL,
            "source_rows_total": EXPECTED_SOURCE_ROWS_TOTAL,
            "source_rows_consumed_identity": EXPECTED_OUTPUT_ROWS_TOTAL * SOURCE_CANDLES_PER_OUTPUT,
            "htf_15m_total": EXPECTED_15M_TOTAL,
            "htf_30m_total": EXPECTED_30M_TOTAL,
            "htf_1h_total": EXPECTED_1H_TOTAL,
            "htf_4h_total": EXPECTED_4H_TOTAL,
            "htf_1d_total": EXPECTED_1D_TOTAL,
        },
        "source_partitions_verified": {
            "canonical_1m": len(before_1m),
            "derived_15m": len(before_15m),
            "derived_30m": len(before_30m),
            "derived_1h": len(before_1h),
            "derived_4h": len(before_4h),
            "derived_1d": len(before_1d),
        },
        "run_summary": summ,
        "independent_scan": scan,
        "dataset_validation": dataset_validation,
        "raw_immutability": immut_raw,
        "canonical_1m_immutability": immut_1m,
        "derived_15m_immutability": immut_15m,
        "derived_30m_immutability": immut_30m,
        "derived_1h_immutability": immut_1h,
        "derived_4h_immutability": immut_4h,
        "derived_1d_immutability": immut_1d,
        "existing_scripts_immutability": immut_scripts,
        "existing_reports_immutability": immut_reports,
        "partitions": records,
        "warnings": [],
        "errors": [r for r in records if r.get("status") == "failed"],
    }
    write_report(report_path, report)

    log.info("-" * 100)
    log.info(
        "summary status=%s output_rows=%d source_consumed=%d omitted=%d reused=%d incomplete=%d "
        "fp_match=%d 1m_eq=%d/%d 15m=%d/%d 30m=%d/%d 1h=%d/%d 4h=%d/%d 1d=%d/%d files=%d "
        "scan_ok=%s dataset_ok=%s",
        summ["by_status"], summ["output_rows"], summ["source_rows_consumed"], summ["source_rows_omitted"],
        summ["source_rows_reused"], summ["incomplete_buckets"], summ["fingerprint_matches"],
        summ["canonical_1m_checks_passed"], summ["canonical_1m_checks_total"],
        summ["secondary_15m_checks_passed"], summ["secondary_15m_checks_total"],
        summ["secondary_30m_checks_passed"], summ["secondary_30m_checks_total"],
        summ["secondary_1h_checks_passed"], summ["secondary_1h_checks_total"],
        summ["secondary_4h_checks_passed"], summ["secondary_4h_checks_total"],
        summ["secondary_1d_checks_passed"], summ["secondary_1d_checks_total"],
        scan["parquet_files"], scan["all_counts_expected"], dataset_validation["all_passed"],
    )
    for s in SYMBOLS:
        ps = summ["per_symbol"].get(s, {})
        log.info("%-9s 5m_rows=%s source_1m=%s %s .. %s close_last=%s",
                 s, ps.get("rows"), ps.get("source_rows"), ps.get("first"), ps.get("last"), ps.get("last_close"))
    log.info("report: %s", report_path)
    log.info("log:    %s", LOGS_DIR / "derived_5m.log")

    bad = (
        interrupted or summ["failed"]
        or not all(x["unchanged"] for x in (immut_raw, immut_1m, immut_15m, immut_30m, immut_1h, immut_4h, immut_1d, immut_scripts, immut_reports))
        or not scan["all_counts_expected"] or not dataset_validation["all_passed"]
        or summ["output_rows"] != EXPECTED_OUTPUT_ROWS_TOTAL
        or summ["source_rows_omitted"] or summ["source_rows_reused"] or summ["incomplete_buckets"]
        or summ["fingerprint_matches"] != len(records)
        or any(ps.get("rows") != EXPECTED_OUTPUT_ROWS_PER_SYMBOL for ps in summ["per_symbol"].values())
        or leftover
    )
    return (1 if bad else 0), report


def main() -> int:
    started = datetime.now(timezone.utc)
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    for required in (
        DATA_ROOT, RAW_ROOT, CANONICAL_1M_ROOT, DERIVED_15M_ROOT, DERIVED_30M_ROOT,
        DERIVED_1H_ROOT, DERIVED_4H_ROOT, DERIVED_1D_ROOT,
    ):
        if not required.exists():
            sys.stderr.write(f"BLOCKER: {required} does not exist\n")
            return 2
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    setup_logging(LOGS_DIR / "derived_5m.log")
    log.info("run %s started", run_id)
    log.info("python %s", sys.version.replace("\n", " "))
    log.info("pyarrow %s executable %s", pa.__version__, sys.executable)
    try:
        run_self_tests()
    except Exception:
        log.exception("SELF-TESTS FAILED -- production derivation will not start")
        return 2
    code, _ = run_production(run_id, started)
    log.info("run %s finished with exit code %d", run_id, code)
    return code


if __name__ == "__main__":
    sys.exit(main())
