#!/usr/bin/env python3
"""
Derive a UTC-aligned 30-minute Parquet dataset from the verified canonical
Binance USD-M PERPETUAL 1m Parquet dataset.

This script is a derivation processor only. It never downloads data, never
modifies anything under DATA_ROOT/raw, the canonical 1m tree, or the derived
15m tree, never resamples to 1h/4h/1d, and never implements strategy or
backtest logic.

Each output candle is built from exactly 30 consecutive unique 1m candles in a
UTC half-hour bucket (minute 00 or 30). Incomplete buckets are a hard failure.

The verified derived 15m dataset is used only as a secondary independent
cross-check (exactly two consecutive 15m candles per 30m candle). 30m rows are
never generated from 15m. A 1m-vs-15m disagreement fails the partition.
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
        "-m processing.timeframes.build_30m_parquet\n"
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
REPORTS_DIR: Path = DATA_ROOT / "reports"
LOGS_DIR: Path = DATA_ROOT / "logs"
TMP_ROOT: Path = DATA_ROOT / "tmp" / "derive_30m"  # never under raw / canonical 1m / derived 15m

EXISTING_SCRIPTS: tuple[Path, ...] = (
    Path(r"C:\Users\oranb\Desktop\backtest_system\ingestion\binance\download_binance_um_perp_1m.py"),
    Path(r"C:\Users\oranb\Desktop\backtest_system\processing\canonical\build_canonical_parquet.py"),
    Path(r"C:\Users\oranb\Desktop\backtest_system\processing\timeframes\build_15m_parquet.py"),
)
EXISTING_REPORTS: tuple[Path, ...] = (
    REPORTS_DIR / "download_report.json",
    REPORTS_DIR / "canonical_build_report.json",
    REPORTS_DIR / "derived_15m_report.json",
)

SYMBOLS: tuple[str, ...] = (
    "BTCUSDT", "ETHUSDT", "AVAXUSDT", "AAVEUSDT", "NEARUSDT",
    "LINKUSDT", "LTCUSDT", "OPUSDT", "DOTUSDT",
)
SOURCE_INTERVAL = "1m"
SECONDARY_INTERVAL = "15m"
OUTPUT_INTERVAL = "30m"
RANGE_START_UTC = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
RANGE_END_UTC = datetime(2026, 9, 15, 23, 59, tzinfo=timezone.utc)

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
SCHEMA_VERSION = "um-perp-30m-derived-v1"
AGGREGATION_RULES_VERSION = "um-perp-30m-agg-v1"

DECIMAL_PRECISION = 38
DECIMAL_SCALE = 8
SOURCE_CANDLES_PER_OUTPUT = 30
SECONDARY_CANDLES_PER_OUTPUT = 2
SECONDARY_SOURCE_CANDLES = 15
BUCKET_MINUTES = 30
SECONDARY_BUCKET_MINUTES = 15
MINUTE_MS = 60_000
BUCKET_MS = BUCKET_MINUTES * MINUTE_MS  # 1_800_000
SECONDARY_BUCKET_MS = SECONDARY_BUCKET_MINUTES * MINUTE_MS  # 900_000

EXPECTED_SOURCE_PARTITIONS = 297
EXPECTED_SECONDARY_PARTITIONS = 297
EXPECTED_OUTPUT_PARTITIONS = 297
EXPECTED_SOURCE_ROWS_PER_SYMBOL = 1_424_160
EXPECTED_SOURCE_ROWS_TOTAL = 12_817_440
EXPECTED_UTC_DAYS = 989
EXPECTED_OUTPUT_ROWS_PER_SYMBOL = EXPECTED_UTC_DAYS * 48  # 47_472
EXPECTED_OUTPUT_ROWS_TOTAL = EXPECTED_OUTPUT_ROWS_PER_SYMBOL * 9  # 427_248
EXPECTED_SEPT_2026_OUTPUT_ROWS = 15 * 48  # 720
EXPECTED_FIRST_OPEN = "2024-01-01T00:00:00Z"
EXPECTED_LAST_OPEN = "2026-09-15T23:30:00Z"
EXPECTED_LAST_CLOSE = "2026-09-15T23:59:59.999Z"
# =============================================================================

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
QUANT = Decimal("1").scaleb(-DECIMAL_SCALE)
SCALE_INT = 10 ** DECIMAL_SCALE
CHUNK_BYTES = 1024 * 1024
VALID_OPEN_MINUTES = frozenset({0, 30})
SECONDARY_OPEN_MINUTES = frozenset({0, 15, 30, 45})

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
        b"source_interval": SOURCE_INTERVAL.encode("utf-8"),
        b"secondary_validation_interval": SECONDARY_INTERVAL.encode("utf-8"),
        b"timezone": b"UTC",
        b"bucket_alignment": b"UTC",
        b"source_candles_per_output": b"30",
        b"secondary_candles_per_output": b"2",
        b"aggregation_rules_version": AGGREGATION_RULES_VERSION.encode("utf-8"),
    },
)

log = logging.getLogger("derived_30m")


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


def hive_path(root: Path, symbol: str, year: int, month: int) -> Path:
    return root / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}" / "data.parquet"


def bucket_start_ms(open_ms: int) -> int:
    return open_ms - (open_ms % BUCKET_MS)


def utc_minute(open_ms: int) -> int:
    return (open_ms // MINUTE_MS) % 60


def assert_outside(path: Path, forbidden: Path, label: str) -> None:
    try:
        path.resolve().relative_to(forbidden.resolve())
    except ValueError:
        return
    raise RuntimeError(f"{label} {path} resolves inside read-only tree {forbidden}")


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


# ----------------------------------------------------------------------------- data classes
@dataclass(frozen=True)
class PartitionRef:
    symbol: str
    year: int
    month: int
    source_path: Path
    secondary_path: Path
    output_path: Path
    start_ms: int
    end_ms: int

    @property
    def expected_1m_rows(self) -> int:
        return (self.end_ms - self.start_ms) // MINUTE_MS + 1

    @property
    def expected_30m_rows(self) -> int:
        return self.expected_1m_rows // SOURCE_CANDLES_PER_OUTPUT

    @property
    def expected_15m_rows(self) -> int:
        return self.expected_1m_rows // SECONDARY_SOURCE_CANDLES

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
        parts = p.parts
        try:
            sym = next(x.split("=", 1)[1] for x in parts if x.startswith("symbol="))
        except StopIteration:
            extra.append(str(p))
            continue
        if sym not in SYMBOLS:
            extra.append(str(p))
    return extra


def discover_partitions(
    source_root: Path,
    secondary_root: Path,
    output_root: Path,
    require_expected_counts: bool = True,
) -> list[PartitionRef]:
    rs, re_ = ms_of(RANGE_START_UTC), ms_of(RANGE_END_UTC)
    out: list[PartitionRef] = []
    missing_1m: list[str] = []
    missing_15m: list[str] = []
    for symbol in SYMBOLS:
        for year, month in iter_year_months(RANGE_START_UTC, RANGE_END_UTC):
            m_first, m_last = month_bounds_ms(year, month)
            start_ms, end_ms = max(m_first, rs), min(m_last, re_)
            src = hive_path(source_root, symbol, year, month)
            sec = hive_path(secondary_root, symbol, year, month)
            if not src.is_file():
                missing_1m.append(str(src))
                continue
            if not sec.is_file():
                missing_15m.append(str(sec))
            out.append(PartitionRef(
                symbol, year, month, src, sec, hive_path(output_root, symbol, year, month),
                start_ms, end_ms,
            ))
    if missing_1m:
        raise FileNotFoundError(f"{len(missing_1m)} canonical 1m partitions missing, first: {missing_1m[:5]}")
    if missing_15m:
        raise FileNotFoundError(f"{len(missing_15m)} derived 15m partitions missing, first: {missing_15m[:5]}")
    extra_1m = _unexpected_hive_files(source_root)
    extra_15m = _unexpected_hive_files(secondary_root)
    if extra_1m:
        raise RuntimeError(f"unexpected canonical 1m files: {extra_1m[:10]}")
    if extra_15m:
        raise RuntimeError(f"unexpected derived 15m files: {extra_15m[:10]}")
    if require_expected_counts:
        if len(out) != EXPECTED_SOURCE_PARTITIONS:
            raise RuntimeError(f"discovered {len(out)} source partitions, expected {EXPECTED_SOURCE_PARTITIONS}")
        n15 = len(list(secondary_root.rglob("data.parquet")))
        if n15 != EXPECTED_SECONDARY_PARTITIONS:
            raise RuntimeError(f"discovered {n15} 15m partitions, expected {EXPECTED_SECONDARY_PARTITIONS}")
    return out


# ----------------------------------------------------------------------------- reads
def read_1m_table(path: Path) -> pa.Table:
    table = pq.ParquetFile(path).read()
    missing = [c for c in SOURCE_REQUIRED_COLUMNS if c not in table.column_names]
    if missing:
        raise ValueError(f"{path} missing columns {missing}; have {table.column_names}")
    table = table.select(list(SOURCE_REQUIRED_COLUMNS))
    if str(table.schema.field("open_time").type) != "timestamp[ms, tz=UTC]":
        raise ValueError(f"{path} open_time type {table.schema.field('open_time').type}")
    if str(table.schema.field("open").type) != "decimal128(38, 8)":
        raise ValueError(f"{path} open type {table.schema.field('open').type}")
    if str(table.schema.field("count").type) != "int64":
        raise ValueError(f"{path} count type {table.schema.field('count').type}")
    return table


def read_15m_table(path: Path) -> pa.Table:
    table = pq.ParquetFile(path).read()
    missing = [c for c in SECONDARY_REQUIRED_COLUMNS if c not in table.column_names]
    if missing:
        raise ValueError(f"{path} missing 15m columns {missing}; have {table.column_names}")
    table = table.select(list(SECONDARY_REQUIRED_COLUMNS))
    if str(table.schema.field("open_time").type) != "timestamp[ms, tz=UTC]":
        raise ValueError(f"{path} 15m open_time type {table.schema.field('open_time').type}")
    if str(table.schema.field("open").type) != "decimal128(38, 8)":
        raise ValueError(f"{path} 15m open type {table.schema.field('open').type}")
    if str(table.schema.field("number_of_trades").type) != "int64":
        raise ValueError(f"{path} 15m number_of_trades type {table.schema.field('number_of_trades').type}")
    if str(table.schema.field("source_candle_count").type) != "int16":
        raise ValueError(f"{path} 15m source_candle_count type {table.schema.field('source_candle_count').type}")
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
    if t != part.start_ms or utc_minute(part.start_ms) not in VALID_OPEN_MINUTES:
        raise ValueError(
            f"misaligned bucket: partition {part.symbol} {part.year}-{part.month:02d} "
            f"starts at {iso_of_ms(part.start_ms)}, which is not a UTC 30-minute boundary"
        )
    last_bucket = bucket_start_ms(part.end_ms)
    if last_bucket + 29 * MINUTE_MS != part.end_ms:
        raise ValueError(
            f"partition {part.symbol} {part.year}-{part.month:02d} last 1m {iso_of_ms(part.end_ms)} "
            f"does not complete a 30m bucket"
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
                "reason": f"incomplete bucket: have {len(idxs)}/30; missing {missing_ts[:8]}",
            })
            continue
        src_opens = [open_ms[i] for i in idxs]
        if len(set(src_opens)) != SOURCE_CANDLES_PER_OUTPUT:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "bucket open times are not unique"})
            continue
        if src_opens[0] != bstart or utc_minute(src_opens[0]) not in VALID_OPEN_MINUTES:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": f"first open {iso_of_ms(src_opens[0])} is not UTC 30-minute aligned"})
            continue
        if src_opens[-1] != bstart + 29 * MINUTE_MS:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": f"last source open {iso_of_ms(src_opens[-1])} != start+29m"})
            continue
        if any(src_opens[k + 1] - src_opens[k] != MINUTE_MS for k in range(29)):
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
                               "reason": "aggregated 30m OHLC invariant failed"})
            continue
        if (
            out.volume < 0 or out.quote_asset_volume < 0 or out.taker_buy_base_asset_volume < 0
            or out.taker_buy_quote_asset_volume < 0 or out.number_of_trades < 0
        ):
            neg_vol += 1
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "aggregated 30m negative volume/trade count"})
            continue
        if utc_minute(out.open_ms) not in VALID_OPEN_MINUTES or out.open_ms % MINUTE_MS != 0:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "output open_time is not a UTC 30-minute boundary"})
            continue
        if out.open_ms < part.start_ms or out.open_ms > part.expected_last_open_ms:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "output open_time outside partition range"})
            continue
        if out.source_candle_count != SOURCE_CANDLES_PER_OUTPUT:
            incomplete.append({"symbol": part.symbol, "bucket_start_utc": iso_of_ms(bstart),
                               "reason": "source_candle_count != 30"})
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

    fp = fingerprint_rows(rows)
    return PartitionWork(
        rows=rows,
        source_rows_read=n,
        source_rows_consumed=used_once,
        source_rows_omitted=omitted,
        source_rows_reused=reused,
        complete_buckets=len(rows),
        incomplete_buckets=incomplete,
        content_fingerprint=fp,
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


def _index_15m(table: pa.Table) -> dict[int, dict]:
    nulls = sum(_col_has_null(table, c) for c in SECONDARY_REQUIRED_COLUMNS)
    if nulls:
        raise ValueError(f"15m table contains {nulls} null required values")
    n = table.num_rows
    open_ms = pc.cast(table.column("open_time"), pa.int64()).to_pylist()
    if len(set(open_ms)) != n:
        raise ValueError("15m table has duplicate open times")
    close_ms = pc.cast(table.column("close_time"), pa.int64()).to_pylist()
    idx: dict[int, dict] = {}
    cols = {name: table.column(name).to_pylist() for name in SECONDARY_REQUIRED_COLUMNS if name not in ("open_time", "close_time")}
    for i in range(n):
        minute = utc_minute(open_ms[i])
        if minute not in SECONDARY_OPEN_MINUTES or open_ms[i] % MINUTE_MS != 0:
            raise ValueError(f"15m open_time {iso_of_ms(open_ms[i])} is not UTC quarter-hour aligned")
        idx[int(open_ms[i])] = {
            "symbol": str(cols["symbol"][i]),
            "open": to_dec128(cols["open"][i], "15m.open"),
            "high": to_dec128(cols["high"][i], "15m.high"),
            "low": to_dec128(cols["low"][i], "15m.low"),
            "close": to_dec128(cols["close"][i], "15m.close"),
            "volume": to_dec128(cols["volume"][i], "15m.volume"),
            "quote_asset_volume": to_dec128(cols["quote_asset_volume"][i], "15m.quote"),
            "number_of_trades": int(cols["number_of_trades"][i]),
            "taker_buy_base_asset_volume": to_dec128(cols["taker_buy_base_asset_volume"][i], "15m.tb"),
            "taker_buy_quote_asset_volume": to_dec128(cols["taker_buy_quote_asset_volume"][i], "15m.tbq"),
            "source_candle_count": int(cols["source_candle_count"][i]),
            "close_ms": int(close_ms[i]),
        }
    return idx


def cross_check_15m(part: PartitionRef, rows: list[OutRow], table_15m: pa.Table) -> list[str]:
    """Independent secondary test. Never used to generate 30m values."""
    errors: list[str] = []
    idx = _index_15m(table_15m)
    if table_15m.num_rows != part.expected_15m_rows:
        errors.append(
            f"{part.symbol} {part.year}-{part.month:02d}: 15m row count {table_15m.num_rows} "
            f"!= expected {part.expected_15m_rows}"
        )
    for r in rows:
        first_ms = r.open_ms
        second_ms = r.open_ms + SECONDARY_BUCKET_MS
        minute = utc_minute(r.open_ms)
        if minute == 0:
            expected_pair = (0, 15)
        elif minute == 30:
            expected_pair = (30, 45)
        else:
            errors.append(f"{part.symbol} {iso_of_ms(r.open_ms)}: 30m open minute {minute} not 00/30")
            continue
        a = idx.get(first_ms)
        b = idx.get(second_ms)
        if a is None or b is None:
            errors.append(
                f"{part.symbol} {iso_of_ms(r.open_ms)}: missing 15m pair "
                f"{iso_of_ms(first_ms)}/{iso_of_ms(second_ms)}"
            )
            continue
        if utc_minute(first_ms) != expected_pair[0] or utc_minute(second_ms) != expected_pair[1]:
            errors.append(f"{part.symbol} {iso_of_ms(r.open_ms)}: 15m pair minutes are not {expected_pair}")
            continue
        if a["symbol"] != part.symbol or b["symbol"] != part.symbol:
            errors.append(f"{part.symbol} {iso_of_ms(r.open_ms)}: 15m pair has mixed/unexpected symbols")
            continue
        if a["source_candle_count"] != SECONDARY_SOURCE_CANDLES or b["source_candle_count"] != SECONDARY_SOURCE_CANDLES:
            errors.append(
                f"{part.symbol} {iso_of_ms(r.open_ms)}: 15m source_candle_count "
                f"{a['source_candle_count']}+{b['source_candle_count']} != 15+15"
            )
            continue
        if a["source_candle_count"] + b["source_candle_count"] != SOURCE_CANDLES_PER_OUTPUT:
            errors.append(f"{part.symbol} {iso_of_ms(r.open_ms)}: two 15m rows do not represent 30 canonical 1m candles")
            continue

        implied_high = to_dec128(max(a["high"], b["high"]), "15m.high")
        implied_low = to_dec128(min(a["low"], b["low"]), "15m.low")
        implied_vol = to_dec128(a["volume"] + b["volume"], "15m.volume")
        implied_q = to_dec128(a["quote_asset_volume"] + b["quote_asset_volume"], "15m.quote")
        implied_tb = to_dec128(a["taker_buy_base_asset_volume"] + b["taker_buy_base_asset_volume"], "15m.tb")
        implied_tbq = to_dec128(a["taker_buy_quote_asset_volume"] + b["taker_buy_quote_asset_volume"], "15m.tbq")
        implied_trades = a["number_of_trades"] + b["number_of_trades"]

        diffs = []
        if r.open != a["open"]:
            diffs.append(f"open 1m={r.open} 15m={a['open']}")
        if r.high != implied_high:
            diffs.append(f"high 1m={r.high} 15m={implied_high}")
        if r.low != implied_low:
            diffs.append(f"low 1m={r.low} 15m={implied_low}")
        if r.close != b["close"]:
            diffs.append(f"close 1m={r.close} 15m={b['close']}")
        if r.volume != implied_vol:
            diffs.append(f"volume 1m={r.volume} 15m={implied_vol}")
        if r.quote_asset_volume != implied_q:
            diffs.append(f"quote 1m={r.quote_asset_volume} 15m={implied_q}")
        if r.number_of_trades != implied_trades:
            diffs.append(f"trades 1m={r.number_of_trades} 15m={implied_trades}")
        if r.taker_buy_base_asset_volume != implied_tb:
            diffs.append(f"tb_base 1m={r.taker_buy_base_asset_volume} 15m={implied_tb}")
        if r.taker_buy_quote_asset_volume != implied_tbq:
            diffs.append(f"tb_quote 1m={r.taker_buy_quote_asset_volume} 15m={implied_tbq}")
        if r.close_ms != b["close_ms"]:
            diffs.append(f"close_time 1m={iso_of_ms_us(r.close_ms)} 15m={iso_of_ms_us(b['close_ms'])}")
        if diffs:
            errors.append(
                f"{part.symbol} {iso_of_ms(r.open_ms)}: canonical 1m aggregation disagrees with 15m cross-check: "
                + "; ".join(diffs)
            )
        if len(errors) >= 20:
            break
    return errors[:20]


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


def validate_output_rows(part: PartitionRef, rows: list[OutRow], expected_fp: str) -> list[str]:
    errors: list[str] = []
    if fingerprint_rows(rows) != expected_fp:
        errors.append("content fingerprint mismatch vs expected aggregation")
    if len(rows) != part.expected_30m_rows:
        errors.append(f"row count {len(rows)} != expected {part.expected_30m_rows}")
    if len(rows) * SOURCE_CANDLES_PER_OUTPUT != part.expected_1m_rows:
        errors.append("output row count * 30 != expected source row count")
    seen = set()
    prev = None
    cutoff = ms_of(datetime(2026, 9, 16, tzinfo=timezone.utc))
    for r in rows:
        if r.symbol != part.symbol:
            errors.append(f"row symbol {r.symbol} != {part.symbol}")
        dt = EPOCH + timedelta(milliseconds=r.open_ms)
        if dt.year != part.year or dt.month != part.month:
            errors.append(f"open_time {iso_of_ms(r.open_ms)} not in {part.year}-{part.month:02d}")
        if utc_minute(r.open_ms) not in VALID_OPEN_MINUTES or r.open_ms % MINUTE_MS != 0:
            errors.append(f"open_time {iso_of_ms(r.open_ms)} not UTC 30-minute aligned")
        if r.source_candle_count != SOURCE_CANDLES_PER_OUTPUT:
            errors.append("source_candle_count != 30")
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


def provenance_meta(part: PartitionRef, work: PartitionWork, source_sha: str, secondary_sha: str) -> dict[bytes, bytes]:
    return {
        b"schema_version": SCHEMA_VERSION.encode(),
        b"provider": b"binance",
        b"market": b"usd_m_perpetual",
        b"archive_namespace": b"futures/um",
        b"interval": OUTPUT_INTERVAL.encode(),
        b"source_interval": SOURCE_INTERVAL.encode(),
        b"secondary_validation_interval": SECONDARY_INTERVAL.encode(),
        b"timezone": b"UTC",
        b"bucket_alignment": b"UTC",
        b"source_candles_per_output": b"30",
        b"secondary_candles_per_output": b"2",
        b"aggregation_rules_version": AGGREGATION_RULES_VERSION.encode(),
        b"symbol": part.symbol.encode(),
        b"year": str(part.year).encode(),
        b"month": f"{part.month:02d}".encode(),
        b"source_canonical_filename": part.source_path.name.encode(),
        b"source_canonical_path": str(part.source_path).encode(),
        b"source_canonical_sha256": source_sha.encode(),
        b"source_15m_filename": part.secondary_path.name.encode(),
        b"source_15m_path": str(part.secondary_path).encode(),
        b"source_15m_sha256": secondary_sha.encode(),
        b"source_row_count": str(work.source_rows_read).encode(),
        b"output_row_count": str(len(work.rows)).encode(),
        b"expected_first_open_time": (iso_of_ms(part.expected_first_open_ms) or "").encode(),
        b"expected_last_open_time": (iso_of_ms(part.expected_last_open_ms) or "").encode(),
        b"content_fingerprint": work.content_fingerprint.encode(),
    }


def write_validated_parquet(
    part: PartitionRef,
    table: pa.Table,
    work: PartitionWork,
    source_sha: str,
    secondary_sha: str,
    table_15m: pa.Table,
) -> dict:
    dest = part.output_path
    assert_outside(dest, RAW_ROOT, "30m output")
    assert_outside(dest, CANONICAL_1M_ROOT, "30m output")
    assert_outside(dest, DERIVED_15M_ROOT, "30m output")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    if tmp.exists():
        tmp.unlink()
    schema = table.schema.with_metadata({**(table.schema.metadata or {}), **provenance_meta(part, work, source_sha, secondary_sha)})
    table = table.cast(schema)
    pq.write_table(
        table,
        tmp,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        version="2.6",
        coerce_timestamps="ms",
        allow_truncated_timestamps=False,
        write_statistics=True,
        use_dictionary=["symbol"],
        row_group_size=max(len(work.rows), 1),
    )
    read_back = pq.ParquetFile(tmp).read()
    rb_rows = table_to_rows(read_back)
    rb_fp = fingerprint_rows(rb_rows)
    errors = validate_output_rows(part, rb_rows, work.content_fingerprint)
    if rb_fp != work.content_fingerprint:
        errors.append(f"read-back fingerprint {rb_fp} != expected {work.content_fingerprint}")
    errors.extend(cross_check_15m(part, rb_rows, table_15m))
    md = {k.decode(): v.decode(errors="replace") for k, v in (read_back.schema.metadata or {}).items()}
    if md.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"provenance schema_version {md.get('schema_version')}")
    if md.get("source_canonical_sha256") != source_sha:
        errors.append("provenance source sha256 mismatch")
    if md.get("source_15m_sha256") != secondary_sha:
        errors.append("provenance 15m sha256 mismatch")
    if md.get("interval") != "30m" or md.get("source_interval") != "1m":
        errors.append("provenance interval fields incorrect")
    if md.get("secondary_validation_interval") != "15m":
        errors.append("provenance secondary interval incorrect")
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
        "schema_names": list(OUTPUT_SCHEMA.names),
    }


def existing_is_current(
    part: PartitionRef,
    work: PartitionWork,
    source_sha: str,
    secondary_sha: str,
    table_15m: pa.Table,
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
        if md.get("source_15m_sha256") != secondary_sha:
            reasons.append("secondary_sha256")
        if md.get("interval") != "30m":
            reasons.append("interval")
        rows = table_to_rows(table)
        fp = fingerprint_rows(rows)
        if fp != work.content_fingerprint:
            reasons.append("content_fingerprint")
        if md.get("content_fingerprint") != work.content_fingerprint:
            reasons.append("recorded_fingerprint")
        reasons.extend(["row_validate:" + e for e in validate_output_rows(part, rows, work.content_fingerprint)])
        reasons.extend(["15m_crosscheck:" + e for e in cross_check_15m(part, rows, table_15m)])
    except Exception as exc:
        reasons.append(f"{type(exc).__name__}: {exc}")
    return (not reasons), reasons


def _fail_rec(rec: dict, error: str, test_context: Optional[str]) -> dict:
    rec["status"] = "failed"
    rec["error"] = error
    if test_context:
        log.error("INTENTIONAL SYNTHETIC TEST ERROR [%s]: %s %04d-%02d -- %s",
                  test_context, rec["symbol"], rec["year"], rec["month"], error)
    else:
        log.error("%s %04d-%02d: FAILED -- %s", rec["symbol"], rec["year"], rec["month"], error)
    return rec


def process_partition(part: PartitionRef, allow_incomplete: bool = False, test_context: Optional[str] = None) -> dict:
    rec = {
        "symbol": part.symbol,
        "year": part.year,
        "month": part.month,
        "source_path": str(part.source_path),
        "secondary_path": str(part.secondary_path),
        "output_path": str(part.output_path),
        "status": None,
        "error": None,
        "source_canonical_sha256": None,
        "source_15m_sha256": None,
        "output_parquet_sha256": None,
        "expected_fingerprint": None,
        "readback_fingerprint": None,
        "aggregation_equivalent": False,
        "secondary_15m_equivalent": False,
        "canonical_1m_checks_passed": 0,
        "canonical_1m_checks_total": 0,
        "secondary_15m_checks_passed": 0,
        "secondary_15m_checks_total": 0,
        "notes": [],
    }
    try:
        source_sha = sha256_file(part.source_path)
        rec["source_canonical_sha256"] = source_sha
        src_table = read_1m_table(part.source_path)
        work = aggregate_partition(part, src_table)
        rec.update({
            "source_rows_read": work.source_rows_read,
            "source_rows_consumed": work.source_rows_consumed,
            "source_rows_omitted": work.source_rows_omitted,
            "source_rows_reused": work.source_rows_reused,
            "complete_buckets": work.complete_buckets,
            "incomplete_bucket_count": len(work.incomplete_buckets),
            "incomplete_buckets": work.incomplete_buckets[:10],
            "output_row_count": len(work.rows),
            "expected_output_row_count": part.expected_30m_rows,
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
            "canonical_1m_checks_passed": len(work.rows) if not work.incomplete_buckets else 0,
            "accounting_ok": (
                work.source_rows_consumed == work.source_rows_read
                and work.source_rows_omitted == 0
                and work.source_rows_reused == 0
                and len(work.rows) * SOURCE_CANDLES_PER_OUTPUT == work.source_rows_read
            ),
        })
        hard_fail = (
            (work.incomplete_buckets and not allow_incomplete)
            or work.source_rows_omitted
            or work.source_rows_reused
            or work.missing_bucket_count
            or work.duplicate_bucket_count
            or work.out_of_order_count
            or work.ohlc_violation_count
            or work.negative_volume_count
            or work.null_count
            or work.rows_outside_range
            or (not allow_incomplete and len(work.rows) != part.expected_30m_rows)
            or (not allow_incomplete and len(work.rows) * SOURCE_CANDLES_PER_OUTPUT != work.source_rows_read)
        )
        if hard_fail:
            return _fail_rec(
                rec,
                (
                    f"incomplete={len(work.incomplete_buckets)} omitted={work.source_rows_omitted} "
                    f"reused={work.source_rows_reused} missing_buckets={work.missing_bucket_count} "
                    f"first_incomplete={work.incomplete_buckets[:1]}"
                ),
                test_context,
            )

        if not part.secondary_path.is_file():
            return _fail_rec(rec, f"missing 15m secondary partition {part.secondary_path}", test_context)
        secondary_sha = sha256_file(part.secondary_path)
        rec["source_15m_sha256"] = secondary_sha
        table_15m = read_15m_table(part.secondary_path)
        xerr = cross_check_15m(part, work.rows, table_15m)
        rec["secondary_15m_checks_total"] = len(work.rows)
        if xerr:
            rec["secondary_15m_checks_passed"] = 0
            rec["secondary_15m_equivalent"] = False
            return _fail_rec(
                rec,
                "canonical 1m vs 15m cross-check disagreement (not repaired): " + " | ".join(xerr[:5]),
                test_context,
            )
        rec["secondary_15m_checks_passed"] = len(work.rows)
        rec["secondary_15m_equivalent"] = True
        rec["canonical_1m_checks_passed"] = len(work.rows)
        rec["aggregation_equivalent"] = True

        skippable, reasons = existing_is_current(part, work, source_sha, secondary_sha, table_15m)
        if skippable:
            rec["status"] = "skipped-as-already-equivalent"
            rec["output_parquet_sha256"] = sha256_file(part.output_path)
            rec["readback_fingerprint"] = work.content_fingerprint
            rec["notes"].append("existing 30m partition fully equivalent to current 1m source and 15m cross-check; not rewritten")
            log.info("%s %04d-%02d: skip (equivalent, %d 30m rows)", part.symbol, part.year, part.month, len(work.rows))
            return rec

        rebuilt = part.output_path.exists()
        if rebuilt:
            rec["notes"].append(f"rebuilding because existing output is not current: {reasons[:8]}")
            log.warning("%s %04d-%02d: rebuilding (%s)", part.symbol, part.year, part.month, reasons[:6])

        table = rows_to_table(work.rows)
        meta = write_validated_parquet(part, table, work, source_sha, secondary_sha, table_15m)
        rec["output_parquet_sha256"] = meta["output_sha256"]
        rec["readback_fingerprint"] = meta["readback_fingerprint"]
        rec["file_size_bytes"] = meta["file_size_bytes"]
        rec["status"] = "rebuilt" if rebuilt else "created"
        log.info(
            "%s %04d-%02d: %s %d 30m rows from %d 1m rows (15m cross-check %d/%d) fp=%s",
            part.symbol, part.year, part.month, rec["status"], len(work.rows),
            work.source_rows_read, rec["secondary_15m_checks_passed"], rec["secondary_15m_checks_total"],
            work.content_fingerprint[:16],
        )
        return rec
    except Exception as exc:
        leftover = part.output_path.with_name(part.output_path.name + ".part")
        if leftover.exists():
            leftover.unlink()
        rec["notes"].append(traceback.format_exc(limit=4))
        return _fail_rec(rec, f"{type(exc).__name__}: {exc}", test_context)


# ----------------------------------------------------------------------------- independent scan
def independent_scan(
    source_root: Path,
    secondary_root: Path,
    output_root: Path,
    partitions: list[PartitionRef],
) -> dict:
    files = sorted(output_root.rglob("data.parquet")) if output_root.exists() else []
    parts_left = list(output_root.rglob("*.part")) if output_root.exists() else []
    other_tf = []
    for tf in ("1h", "4h", "1d", "daily"):
        p = output_root.parent / tf
        if p.exists():
            other_tf.append(str(p))
    symbols = sorted({p.parent.parent.parent.name.split("=", 1)[1] for p in files}) if files else []
    rows_by_sym: dict[str, int] = {s: 0 for s in SYMBOLS}
    first_last: dict[str, list[int]] = {}
    last_close: dict[str, int] = {}
    eq_fail = 0
    sec_fail = 0
    align_fail = 0
    src_count_fail = 0
    continuity_fail = 0
    canonical_passed = 0
    secondary_passed = 0
    prev_last: dict[str, Optional[int]] = {s: None for s in SYMBOLS}

    for part in partitions:
        if not part.output_path.is_file():
            eq_fail += 1
            continue
        src = read_1m_table(part.source_path)
        work = aggregate_partition(part, src)
        out = table_to_rows(pq.ParquetFile(part.output_path).read())
        if fingerprint_rows(out) != work.content_fingerprint:
            eq_fail += 1
        else:
            canonical_passed += len(out)
        sec = read_15m_table(part.secondary_path)
        xerr = cross_check_15m(part, out, sec)
        if xerr:
            sec_fail += 1
        else:
            secondary_passed += len(out)
        for r in out:
            if utc_minute(r.open_ms) not in VALID_OPEN_MINUTES or r.open_ms % MINUTE_MS != 0:
                align_fail += 1
            if r.source_candle_count != SOURCE_CANDLES_PER_OUTPUT:
                src_count_fail += 1
        rows_by_sym[part.symbol] = rows_by_sym.get(part.symbol, 0) + len(out)
        if out:
            sl = first_last.setdefault(part.symbol, [out[0].open_ms, out[-1].open_ms])
            sl[0] = min(sl[0], out[0].open_ms)
            sl[1] = max(sl[1], out[-1].open_ms)
            last_close[part.symbol] = max(last_close.get(part.symbol, 0), out[-1].close_ms)
            prev = prev_last[part.symbol]
            if prev is not None and out[0].open_ms - prev != BUCKET_MS:
                continuity_fail += 1
            prev_last[part.symbol] = out[-1].open_ms
    total = sum(rows_by_sym.values())
    all_ok = (
        len(files) == EXPECTED_OUTPUT_PARTITIONS
        and symbols == sorted(SYMBOLS)
        and total == EXPECTED_OUTPUT_ROWS_TOTAL
        and all(v == EXPECTED_OUTPUT_ROWS_PER_SYMBOL for v in rows_by_sym.values())
        and eq_fail == 0 and sec_fail == 0 and align_fail == 0 and src_count_fail == 0
        and continuity_fail == 0
        and canonical_passed == EXPECTED_OUTPUT_ROWS_TOTAL
        and secondary_passed == EXPECTED_OUTPUT_ROWS_TOTAL
        and not other_tf and not parts_left
        and all(iso_of_ms(v[0]) == EXPECTED_FIRST_OPEN for v in first_last.values())
        and all(iso_of_ms(v[1]) == EXPECTED_LAST_OPEN for v in first_last.values())
        and all(iso_of_ms_us(v) == EXPECTED_LAST_CLOSE for v in last_close.values())
    )
    return {
        "parquet_files": len(files),
        "part_files": len(parts_left),
        "other_timeframes_present": other_tf,
        "symbols": symbols,
        "unexpected_symbols": [s for s in symbols if s not in SYMBOLS],
        "rows_by_symbol": rows_by_sym,
        "total_rows": total,
        "first_open_time_utc": {s: iso_of_ms(v[0]) for s, v in first_last.items()},
        "last_open_time_utc": {s: iso_of_ms(v[1]) for s, v in first_last.items()},
        "last_close_time_utc": {s: iso_of_ms_us(v) for s, v in last_close.items()},
        "canonical_1m_equivalence_failures": eq_fail,
        "secondary_15m_crosscheck_failures": sec_fail,
        "alignment_failures": align_fail,
        "source_candle_count_failures": src_count_fail,
        "month_boundary_continuity_failures": continuity_fail,
        "canonical_1m_checks_passed": canonical_passed,
        "canonical_1m_checks_total": total,
        "secondary_15m_checks_passed": secondary_passed,
        "secondary_15m_checks_total": total,
        "all_counts_expected": all_ok,
    }


# ----------------------------------------------------------------------------- tests
def _write_synthetic_1m(
    path: Path,
    symbol: str,
    start_ms: int,
    n_minutes: int,
    skip_open_ms: Optional[int] = None,
    duplicate_open_ms: Optional[int] = None,
) -> None:
    assert_outside(path, CANONICAL_1M_ROOT, "synthetic 1m")
    assert_outside(path, RAW_ROOT, "synthetic 1m")
    assert_outside(path, DERIVED_15M_ROOT, "synthetic 1m")
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


def _write_synthetic_15m_matching_1m(src_1m: Path, dest_15m: Path, symbol: str) -> None:
    """Build matching 15m candles from a synthetic 1m file. Test-only; not used in production."""
    assert_outside(dest_15m, DERIVED_15M_ROOT, "synthetic 15m")
    assert_outside(dest_15m, CANONICAL_1M_ROOT, "synthetic 15m")
    assert_outside(dest_15m, RAW_ROOT, "synthetic 15m")
    dest_15m.parent.mkdir(parents=True, exist_ok=True)
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
    starts = sorted({o - (o % SECONDARY_BUCKET_MS) for o in open_ms})
    rows: list[OutRow] = []
    for bstart in starts:
        wanted = [bstart + k * MINUTE_MS for k in range(SECONDARY_SOURCE_CANDLES)]
        idxs = [index_by_open[w] for w in wanted if w in index_by_open]
        if len(idxs) != SECONDARY_SOURCE_CANDLES:
            continue
        b_highs = [to_dec128(highs[i], "high") for i in idxs]
        b_lows = [to_dec128(lows[i], "low") for i in idxs]
        b_vols = [to_dec128(vols[i], "volume") for i in idxs]
        b_qvols = [to_dec128(qvols[i], "quote") for i in idxs]
        b_tb = [to_dec128(tb_base[i], "tb") for i in idxs]
        b_tbq = [to_dec128(tb_quote[i], "tbq") for i in idxs]
        rows.append(OutRow(
            symbol=symbol,
            open_ms=bstart,
            close_ms=int(close_ms[idxs[-1]]),
            open=to_dec128(opens[idxs[0]], "open"),
            high=to_dec128(max(b_highs), "high"),
            low=to_dec128(min(b_lows), "low"),
            close=to_dec128(closes[idxs[-1]], "close"),
            volume=to_dec128(sum(b_vols, Decimal("0")), "volume"),
            quote_asset_volume=to_dec128(sum(b_qvols, Decimal("0")), "quote"),
            number_of_trades=sum(int(trades[i]) for i in idxs),
            taker_buy_base_asset_volume=to_dec128(sum(b_tb, Decimal("0")), "tb"),
            taker_buy_quote_asset_volume=to_dec128(sum(b_tbq, Decimal("0")), "tbq"),
            source_candle_count=SECONDARY_SOURCE_CANDLES,
        ))
    pq.write_table(rows_to_table(rows), dest_15m, compression="zstd")


def run_self_tests() -> None:
    test_root = TMP_ROOT / "self_tests"
    if test_root.exists():
        shutil.rmtree(test_root)
    test_root.mkdir(parents=True, exist_ok=True)
    src_root = test_root / "canonical_1m"
    sec_root = test_root / "derived_15m"
    dst_root = test_root / "derived_30m"
    log.info("self-tests starting under %s", test_root)

    def copy_real(symbol: str, year: int, month: int) -> PartitionRef:
        src = hive_path(CANONICAL_1M_ROOT, symbol, year, month)
        sec = hive_path(DERIVED_15M_ROOT, symbol, year, month)
        dst_src = hive_path(src_root, symbol, year, month)
        dst_sec = hive_path(sec_root, symbol, year, month)
        dst_src.parent.mkdir(parents=True, exist_ok=True)
        dst_sec.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst_src)
        shutil.copy2(sec, dst_sec)
        rs, re_ = ms_of(RANGE_START_UTC), ms_of(RANGE_END_UTC)
        m_first, m_last = month_bounds_ms(year, month)
        return PartitionRef(
            symbol, year, month, dst_src, dst_sec, hive_path(dst_root, symbol, year, month),
            max(m_first, rs), min(m_last, re_),
        )

    # 1. Normal 31-day month
    jan = copy_real("BTCUSDT", 2024, 1)
    rec = process_partition(jan)
    assert rec["status"] == "created", rec
    assert rec["output_row_count"] == 31 * 48 == 1488, rec["output_row_count"]
    assert rec["source_rows_read"] == 31 * 1440
    assert rec["accounting_ok"] and rec["aggregation_equivalent"] and rec["secondary_15m_equivalent"]
    assert rec["incomplete_bucket_count"] == 0
    assert rec["canonical_1m_checks_passed"] == rec["canonical_1m_checks_total"] == 1488
    assert rec["secondary_15m_checks_passed"] == rec["secondary_15m_checks_total"] == 1488
    jan_table = pq.ParquetFile(jan.output_path).read()
    assert jan_table.schema.names == list(OUTPUT_SCHEMA.names)
    assert str(jan_table.schema.field("open").type) == "decimal128(38, 8)"
    assert str(jan_table.schema.field("open_time").type) == "timestamp[ms, tz=UTC]"
    assert str(jan_table.schema.field("source_candle_count").type) == "int16"
    jan_rows = table_to_rows(jan_table)
    assert jan_rows[0].open_ms == ms_of(datetime(2024, 1, 1, tzinfo=timezone.utc))
    assert jan_rows[-1].open_ms == ms_of(datetime(2024, 1, 31, 23, 30, tzinfo=timezone.utc))
    assert all(utc_minute(r.open_ms) in VALID_OPEN_MINUTES for r in jan_rows)
    assert all(r.source_candle_count == 30 for r in jan_rows)
    log.info("TEST OK: 31-day month BTCUSDT 2024-01 -> %d 30m rows", rec["output_row_count"])

    # 2. February 2024 leap year
    feb = copy_real("BTCUSDT", 2024, 2)
    rec = process_partition(feb)
    assert rec["status"] == "created", rec
    assert rec["output_row_count"] == 29 * 48 == 1392, rec["output_row_count"]
    assert rec["source_rows_read"] == 29 * 1440
    assert rec["secondary_15m_equivalent"]
    log.info("TEST OK: leap February 2024 -> %d 30m rows", rec["output_row_count"])

    # 3. September 2026 partial 15-day month
    sept = copy_real("ETHUSDT", 2026, 9)
    rec = process_partition(sept)
    assert rec["status"] == "created", rec
    assert rec["output_row_count"] == EXPECTED_SEPT_2026_OUTPUT_ROWS, rec["output_row_count"]
    assert rec["last_open_time_utc"] == EXPECTED_LAST_OPEN
    assert rec["last_close_time_utc"] == EXPECTED_LAST_CLOSE
    assert rec["source_rows_read"] == 15 * 1440
    log.info("TEST OK: September 2026 partial month -> %d 30m rows last=%s", rec["output_row_count"], rec["last_open_time_utc"])

    # 4. Month-boundary continuity Jan last -> Feb first
    jan_last = table_to_rows(pq.ParquetFile(jan.output_path).read())[-1]
    feb_first = table_to_rows(pq.ParquetFile(feb.output_path).read())[0]
    assert feb_first.open_ms - jan_last.open_ms == BUCKET_MS, (iso_of_ms(jan_last.open_ms), iso_of_ms(feb_first.open_ms))
    log.info("TEST OK: month-boundary continuity %s -> %s", iso_of_ms(jan_last.open_ms), iso_of_ms(feb_first.open_ms))

    # 5. Existing valid output is skipped and not rewritten
    sha_before = sha256_file(jan.output_path)
    mtime_before = jan.output_path.stat().st_mtime_ns
    rec2 = process_partition(jan)
    assert rec2["status"] == "skipped-as-already-equivalent", rec2
    assert sha256_file(jan.output_path) == sha_before
    assert jan.output_path.stat().st_mtime_ns == mtime_before
    log.info("TEST OK: idempotent skip of valid existing partition")

    # 6. Incomplete 29-row bucket rejection
    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: 29-row incomplete bucket")
    syn_src = hive_path(test_root / "syn_src", "BTCUSDT", 2024, 3)
    syn_sec = hive_path(test_root / "syn_sec", "BTCUSDT", 2024, 3)
    syn_dst = hive_path(test_root / "syn_dst", "BTCUSDT", 2024, 3)
    start = ms_of(datetime(2024, 3, 1, tzinfo=timezone.utc))
    skip = start + 7 * MINUTE_MS
    _write_synthetic_1m(syn_src, "BTCUSDT", start, 60, skip_open_ms=skip)
    syn_part = PartitionRef("BTCUSDT", 2024, 3, syn_src, syn_sec, syn_dst, start, start + 59 * MINUTE_MS)
    rec = process_partition(syn_part, test_context="29-row-bucket")
    assert rec["status"] == "failed", rec
    assert rec["incomplete_bucket_count"] >= 1, rec
    assert not syn_dst.exists(), "failed incomplete partition must not emit data.parquet"
    log.info("TEST OK: 29-row bucket rejected (%s)", rec["error"])

    # 7. Duplicate timestamps
    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: duplicate 1m timestamps")
    dup_src = hive_path(test_root / "dup_src", "BTCUSDT", 2024, 3)
    dup_sec = hive_path(test_root / "dup_sec", "BTCUSDT", 2024, 3)
    dup_dst = hive_path(test_root / "dup_dst", "BTCUSDT", 2024, 3)
    _write_synthetic_1m(dup_src, "BTCUSDT", start, 60, duplicate_open_ms=start + 3 * MINUTE_MS)
    dup_part = PartitionRef("BTCUSDT", 2024, 3, dup_src, dup_sec, dup_dst, start, start + 59 * MINUTE_MS)
    rec = process_partition(dup_part, test_context="duplicate-timestamps")
    assert rec["status"] == "failed", rec
    assert rec["error"] and "duplicate" in rec["error"].lower(), rec
    assert not dup_dst.exists()
    log.info("TEST OK: duplicate timestamps rejected (%s)", rec["error"])

    # 8. Misaligned bucket (partition does not start on minute 00/30)
    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: misaligned 30m bucket")
    mis_src = hive_path(test_root / "mis_src", "ETHUSDT", 2024, 3)
    mis_sec = hive_path(test_root / "mis_sec", "ETHUSDT", 2024, 3)
    mis_dst = hive_path(test_root / "mis_dst", "ETHUSDT", 2024, 3)
    mis_start = start + 15 * MINUTE_MS
    _write_synthetic_1m(mis_src, "ETHUSDT", mis_start, 30)
    mis_part = PartitionRef("ETHUSDT", 2024, 3, mis_src, mis_sec, mis_dst, mis_start, mis_start + 29 * MINUTE_MS)
    rec = process_partition(mis_part, test_context="misaligned-bucket")
    assert rec["status"] == "failed", rec
    assert rec["error"] and "misaligned" in rec["error"].lower(), rec
    assert not mis_dst.exists()
    log.info("TEST OK: misaligned bucket rejected (%s)", rec["error"])

    # 9. Fingerprint-mismatch detection -> rebuild
    syn2_src = hive_path(test_root / "syn2_src", "ETHUSDT", 2024, 4)
    syn2_sec = hive_path(test_root / "syn2_sec", "ETHUSDT", 2024, 4)
    syn2_dst = hive_path(test_root / "syn2_dst", "ETHUSDT", 2024, 4)
    start = ms_of(datetime(2024, 4, 1, tzinfo=timezone.utc))
    _write_synthetic_1m(syn2_src, "ETHUSDT", start, 60)
    _write_synthetic_15m_matching_1m(syn2_src, syn2_sec, "ETHUSDT")
    syn2_part = PartitionRef("ETHUSDT", 2024, 4, syn2_src, syn2_sec, syn2_dst, start, start + 59 * MINUTE_MS)
    rec = process_partition(syn2_part)
    assert rec["status"] == "created" and rec["output_row_count"] == 2, rec
    good_fp = rec["expected_fingerprint"]
    tbl = pq.ParquetFile(syn2_dst).read()
    closes = tbl.column("close").to_pylist()
    closes[0] = to_dec128(Decimal(closes[0]) + Decimal("1.00000000"), "close")
    arrays = {name: tbl.column(name) for name in tbl.column_names}
    arrays["close"] = pa.array(closes, DECIMAL_TYPE)
    bad = pa.table(arrays, schema=tbl.schema)
    pq.write_table(bad, syn2_dst, compression="zstd")
    rec = process_partition(syn2_part)
    assert rec["status"] == "rebuilt", rec
    assert rec["expected_fingerprint"] == good_fp == rec["readback_fingerprint"]
    log.info("TEST OK: fingerprint mismatch detected and partition rebuilt")

    # 10. Deliberate 1m vs 15m discrepancy
    log.info("INTENTIONAL SYNTHETIC TEST ERROR expected below: 1m vs 15m discrepancy")
    disc_src = hive_path(test_root / "disc_src", "LINKUSDT", 2024, 5)
    disc_sec = hive_path(test_root / "disc_sec", "LINKUSDT", 2024, 5)
    disc_dst = hive_path(test_root / "disc_dst", "LINKUSDT", 2024, 5)
    start = ms_of(datetime(2024, 5, 1, tzinfo=timezone.utc))
    _write_synthetic_1m(disc_src, "LINKUSDT", start, 60)
    _write_synthetic_15m_matching_1m(disc_src, disc_sec, "LINKUSDT")
    disc_part = PartitionRef("LINKUSDT", 2024, 5, disc_src, disc_sec, disc_dst, start, start + 59 * MINUTE_MS)
    rec = process_partition(disc_part)
    assert rec["status"] == "created", rec
    tbl15 = pq.ParquetFile(disc_sec).read()
    highs = tbl15.column("high").to_pylist()
    highs[0] = to_dec128(Decimal(highs[0]) + Decimal("9.00000000"), "high")
    arrays15 = {name: tbl15.column(name) for name in tbl15.column_names}
    arrays15["high"] = pa.array(highs, DECIMAL_TYPE)
    pq.write_table(pa.table(arrays15, schema=tbl15.schema), disc_sec, compression="zstd")
    rec = process_partition(disc_part, test_context="1m-vs-15m-discrepancy")
    assert rec["status"] == "failed", rec
    assert rec["error"] and "disagreement" in rec["error"].lower(), rec
    log.info("TEST OK: 1m vs 15m discrepancy detected and partition failed (%s)", rec["error"])

    shutil.rmtree(test_root)
    log.info("self-tests passed; temporary artifacts removed")


# ----------------------------------------------------------------------------- logging / report
def setup_logging(log_file: Path) -> None:
    assert_outside(log_file, RAW_ROOT, "log")
    assert_outside(log_file, CANONICAL_1M_ROOT, "log")
    assert_outside(log_file, DERIVED_15M_ROOT, "log")
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


def summarize(records: list[dict]) -> dict:
    from collections import Counter
    by_status = dict(Counter(r.get("status") for r in records))
    src_read = sum(r.get("source_rows_read") or 0 for r in records)
    src_cons = sum(r.get("source_rows_consumed") or 0 for r in records)
    omitted = sum(r.get("source_rows_omitted") or 0 for r in records)
    reused = sum(r.get("source_rows_reused") or 0 for r in records)
    complete = sum(r.get("complete_buckets") or 0 for r in records)
    incomplete = sum(r.get("incomplete_bucket_count") or 0 for r in records)
    out_rows = sum(r.get("output_row_count") or 0 for r in records)
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
        "source_rows_read": src_read,
        "source_rows_consumed": src_cons,
        "source_rows_omitted": omitted,
        "source_rows_reused": reused,
        "complete_buckets": complete,
        "incomplete_buckets": incomplete,
        "output_rows": out_rows,
        "fingerprint_matches": sum(
            1 for r in records
            if r.get("aggregation_equivalent") and r.get("expected_fingerprint") == r.get("readback_fingerprint")
        ),
        "canonical_1m_checks_passed": sum(r.get("canonical_1m_checks_passed") or 0 for r in records),
        "canonical_1m_checks_total": sum(r.get("canonical_1m_checks_total") or 0 for r in records),
        "secondary_15m_checks_passed": sum(r.get("secondary_15m_checks_passed") or 0 for r in records),
        "secondary_15m_checks_total": sum(r.get("secondary_15m_checks_total") or 0 for r in records),
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
        "30m PROCESSING PLAN -- derive UTC-aligned 30m from canonical 1m only",
        "NOT a downloader. NOT 1h/4h/1d. NOT Turkey time. NOT generated from 15m.",
        f"Symbols ({len(SYMBOLS)}): {', '.join(SYMBOLS)}",
        f"Primary source (read-only): {CANONICAL_1M_ROOT}",
        f"  partitions={len(partitions)} expected={EXPECTED_SOURCE_PARTITIONS}  1m rows={EXPECTED_SOURCE_ROWS_TOTAL}",
        f"Secondary 15m (read-only cross-check): {DERIVED_15M_ROOT}",
        f"  partitions expected={EXPECTED_SECONDARY_PARTITIONS}; 2 consecutive 15m candles per 30m candle",
        f"Output: {DERIVED_30M_ROOT}",
        "UTC buckets: minutes 00 and 30; exactly 30 consecutive unique 1m candles per output candle",
        f"Expected: {EXPECTED_OUTPUT_PARTITIONS} files, {EXPECTED_OUTPUT_ROWS_PER_SYMBOL} rows/symbol, "
        f"{EXPECTED_OUTPUT_ROWS_TOTAL} total",
        f"First open {EXPECTED_FIRST_OPEN}  last open {EXPECTED_LAST_OPEN}  last close {EXPECTED_LAST_CLOSE}",
        f"Accounting identity: {EXPECTED_OUTPUT_ROWS_TOTAL} x 30 = {EXPECTED_SOURCE_ROWS_TOTAL}",
        f"Existing output files: {already} (re-verified; skipped only if fully equivalent)",
        f"Schema {SCHEMA_VERSION}  decimal128({DECIMAL_PRECISION},{DECIMAL_SCALE})  compression {PARQUET_COMPRESSION}",
        "On 1m vs 15m disagreement: fail the partition; never repair either dataset from the other",
        "=" * 100,
    ):
        log.info(line)


def run_production(run_id: str, started: datetime) -> tuple[int, dict]:
    assert_outside(DERIVED_30M_ROOT, RAW_ROOT, "derived 30m")
    assert_outside(DERIVED_30M_ROOT, CANONICAL_1M_ROOT, "derived 30m")
    assert_outside(DERIVED_30M_ROOT, DERIVED_15M_ROOT, "derived 30m")
    assert_outside(TMP_ROOT, RAW_ROOT, "tmp")
    assert_outside(TMP_ROOT, CANONICAL_1M_ROOT, "tmp")
    assert_outside(TMP_ROOT, DERIVED_15M_ROOT, "tmp")
    DERIVED_30M_ROOT.mkdir(parents=True, exist_ok=True)
    TMP_ROOT.mkdir(parents=True, exist_ok=True)

    partitions = discover_partitions(CANONICAL_1M_ROOT, DERIVED_15M_ROOT, DERIVED_30M_ROOT)
    already = sum(1 for p in partitions if p.output_path.exists())
    print_plan(partitions, already)

    log.info("fingerprinting read-only raw + canonical 1m + derived 15m + existing scripts/reports...")
    before_raw = fingerprint_tree(RAW_ROOT, ("*.zip", "*.zip.CHECKSUM"))
    before_1m = fingerprint_tree(CANONICAL_1M_ROOT, ("data.parquet",))
    before_15m = fingerprint_tree(DERIVED_15M_ROOT, ("data.parquet",))
    before_scripts = fingerprint_paths(EXISTING_SCRIPTS)
    before_reports = fingerprint_paths(EXISTING_REPORTS)
    log.info(
        "pre-run fingerprints: raw=%d canonical_1m=%d derived_15m=%d scripts=%d reports=%d",
        len(before_raw), len(before_1m), len(before_15m), len(before_scripts), len(before_reports),
    )

    records: list[dict] = []
    interrupted = False
    try:
        for i, part in enumerate(partitions, 1):
            rec = process_partition(part)
            records.append(rec)
            if i % 33 == 0:
                log.info("progress: %d/%d partitions", i, len(partitions))
    except KeyboardInterrupt:
        interrupted = True
        log.warning("interrupted")

    log.info("independent disk scan of derived 30m + full 1m re-aggregation + full 15m cross-check...")
    scan = independent_scan(CANONICAL_1M_ROOT, DERIVED_15M_ROOT, DERIVED_30M_ROOT, partitions)
    log.info("re-fingerprinting read-only sources...")
    after_raw = fingerprint_tree(RAW_ROOT, ("*.zip", "*.zip.CHECKSUM"))
    after_1m = fingerprint_tree(CANONICAL_1M_ROOT, ("data.parquet",))
    after_15m = fingerprint_tree(DERIVED_15M_ROOT, ("data.parquet",))
    after_scripts = fingerprint_paths(EXISTING_SCRIPTS)
    after_reports = fingerprint_paths(EXISTING_REPORTS)
    immut_raw = compare_fingerprints(before_raw, after_raw)
    immut_1m = compare_fingerprints(before_1m, after_1m)
    immut_15m = compare_fingerprints(before_15m, after_15m)
    immut_scripts = compare_fingerprints(before_scripts, after_scripts)
    immut_reports = compare_fingerprints(before_reports, after_reports)
    if immut_raw["unchanged"] and immut_1m["unchanged"] and immut_15m["unchanged"]:
        log.info("raw, canonical 1m, and derived 15m files unchanged (sha256, size, mtime_ns)")
    else:
        log.error(
            "SOURCE MUTATION raw=%s 1m=%s 15m=%s",
            immut_raw["mismatch_count"], immut_1m["mismatch_count"], immut_15m["mismatch_count"],
        )
    if immut_scripts["unchanged"] and immut_reports["unchanged"]:
        log.info("existing scripts and 1m/15m reports unchanged")
    else:
        log.error("SCRIPT/REPORT MUTATION scripts=%s reports=%s", immut_scripts["mismatch_count"], immut_reports["mismatch_count"])

    leftover = list(DERIVED_30M_ROOT.rglob("*.part")) if DERIVED_30M_ROOT.exists() else []
    for p in leftover:
        try:
            p.unlink()
            log.info("removed leftover %s", p)
        except OSError as exc:
            log.warning("could not remove %s: %s", p, exc)

    summ = summarize(records)
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
            and summ["source_rows_omitted"] == 0
            and summ["source_rows_reused"] == 0
            and summ["output_rows"] * SOURCE_CANDLES_PER_OUTPUT == summ["source_rows_consumed"]
        ),
        "canonical_1m_equivalence_ok": (
            summ["canonical_1m_checks_passed"] == EXPECTED_OUTPUT_ROWS_TOTAL
            and scan["canonical_1m_equivalence_failures"] == 0
        ),
        "secondary_15m_equivalence_ok": (
            summ["secondary_15m_checks_passed"] == EXPECTED_OUTPUT_ROWS_TOTAL
            and scan["secondary_15m_crosscheck_failures"] == 0
        ),
        "fingerprint_ok": summ["fingerprint_matches"] == len(records) == EXPECTED_OUTPUT_PARTITIONS,
        "zero_defect_counts_ok": (
            summ["missing_bucket_count"] == 0
            and summ["duplicate_bucket_count"] == 0
            and summ["incomplete_buckets"] == 0
            and summ["out_of_order_count"] == 0
            and summ["null_count"] == 0
            and summ["ohlc_violation_count"] == 0
            and summ["negative_volume_count"] == 0
            and summ["rows_outside_range"] == 0
        ),
        "no_other_timeframes": not scan["other_timeframes_present"],
        "sources_unchanged": immut_raw["unchanged"] and immut_1m["unchanged"] and immut_15m["unchanged"],
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
        "secondary_validation_interval": SECONDARY_INTERVAL,
        "output_interval": OUTPUT_INTERVAL,
        "timezone": "UTC",
        "bucket_alignment": "UTC",
        "source_candles_per_output": SOURCE_CANDLES_PER_OUTPUT,
        "secondary_candles_per_output": SECONDARY_CANDLES_PER_OUTPUT,
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
        "secondary_root": str(DERIVED_15M_ROOT),
        "output_root": str(DERIVED_30M_ROOT),
        "requested_symbols": list(SYMBOLS),
        "expected": {
            "source_partitions": EXPECTED_SOURCE_PARTITIONS,
            "secondary_partitions": EXPECTED_SECONDARY_PARTITIONS,
            "output_partitions": EXPECTED_OUTPUT_PARTITIONS,
            "rows_per_symbol": EXPECTED_OUTPUT_ROWS_PER_SYMBOL,
            "total_rows": EXPECTED_OUTPUT_ROWS_TOTAL,
            "source_rows_per_symbol": EXPECTED_SOURCE_ROWS_PER_SYMBOL,
            "source_rows_total": EXPECTED_SOURCE_ROWS_TOTAL,
            "source_rows_consumed_identity": EXPECTED_OUTPUT_ROWS_TOTAL * SOURCE_CANDLES_PER_OUTPUT,
        },
        "run_summary": summ,
        "independent_scan": scan,
        "dataset_validation": dataset_validation,
        "raw_immutability": immut_raw,
        "canonical_1m_immutability": immut_1m,
        "derived_15m_immutability": immut_15m,
        "existing_scripts_immutability": immut_scripts,
        "existing_reports_immutability": immut_reports,
        "partitions": records,
        "warnings": [],
        "errors": [r for r in records if r.get("status") == "failed"],
    }
    write_report(REPORTS_DIR / "derived_30m_report.json", report)

    log.info("-" * 100)
    log.info(
        "summary status=%s output_rows=%d source_consumed=%d omitted=%d reused=%d incomplete=%d "
        "fp_match=%d 1m_eq=%d/%d 15m_eq=%d/%d files=%d raw_unchanged=%s 1m_unchanged=%s "
        "15m_unchanged=%s scan_ok=%s dataset_ok=%s",
        summ["by_status"], summ["output_rows"], summ["source_rows_consumed"], summ["source_rows_omitted"],
        summ["source_rows_reused"], summ["incomplete_buckets"], summ["fingerprint_matches"],
        summ["canonical_1m_checks_passed"], summ["canonical_1m_checks_total"],
        summ["secondary_15m_checks_passed"], summ["secondary_15m_checks_total"],
        scan["parquet_files"], immut_raw["unchanged"], immut_1m["unchanged"], immut_15m["unchanged"],
        scan["all_counts_expected"], dataset_validation["all_passed"],
    )
    for s in SYMBOLS:
        ps = summ["per_symbol"].get(s, {})
        log.info(
            "%-9s 30m_rows=%s source_1m=%s %s .. %s close_last=%s",
            s, ps.get("rows"), ps.get("source_rows"), ps.get("first"), ps.get("last"), ps.get("last_close"),
        )
    log.info("report: %s", REPORTS_DIR / "derived_30m_report.json")
    log.info("log:    %s", LOGS_DIR / "derived_30m.log")

    bad = (
        interrupted
        or summ["failed"]
        or not immut_raw["unchanged"]
        or not immut_1m["unchanged"]
        or not immut_15m["unchanged"]
        or not immut_scripts["unchanged"]
        or not immut_reports["unchanged"]
        or not scan["all_counts_expected"]
        or not dataset_validation["all_passed"]
        or summ["output_rows"] != EXPECTED_OUTPUT_ROWS_TOTAL
        or summ["source_rows_omitted"]
        or summ["source_rows_reused"]
        or summ["incomplete_buckets"]
        or summ["fingerprint_matches"] != len(records)
        or any(ps.get("rows") != EXPECTED_OUTPUT_ROWS_PER_SYMBOL for ps in summ["per_symbol"].values())
        or leftover
    )
    return (1 if bad else 0), report


def main() -> int:
    started = datetime.now(timezone.utc)
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    for required in (DATA_ROOT, RAW_ROOT, CANONICAL_1M_ROOT, DERIVED_15M_ROOT):
        if not required.exists():
            sys.stderr.write(f"BLOCKER: {required} does not exist\n")
            return 2
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    setup_logging(LOGS_DIR / "derived_30m.log")
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
