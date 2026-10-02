#!/usr/bin/env python3
"""
Build a canonical, monthly-partitioned Parquet dataset from already-downloaded
and verified Binance USD-M USDT-margined PERPETUAL futures 1-minute kline ZIPs.

This script is a processor only. It never downloads data, never calls Binance
APIs, never resamples, and never modifies anything under DATA_ROOT/raw.

Raw ZIP archives remain the source of truth. CSV members are streamed from the
ZIPs; values are stored as UTC timestamps and decimal128. The decimal scale is
strictly sufficient for every fractional digit observed in the source CSVs; a
source value that would require rounding is a hard error.

Hive layout (one data.parquet per symbol-year-month):
  <DATA_ROOT>/processed/binance/futures/um/perpetual/1m/symbol=<SYM>/year=<YYYY>/month=<MM>/data.parquet
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import shutil
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
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
        "pyarrow is required. Use the project venv:\n"
        "  C:\\Users\\oranb\\Desktop\\backtest_system\\.venv\\Scripts\\python.exe "
        "-m processing.canonical.build_canonical_parquet\n"
    )
    raise

# =============================================================================
# CONFIGURATION -- the only place that defines WHAT is processed.
# =============================================================================
DATA_ROOT: Path = Path(r"C:\MarketData")
RAW_ROOT: Path = DATA_ROOT / "raw" / "binance" / "futures" / "um" / "perpetual"
PROCESSED_ROOT: Path = DATA_ROOT / "processed" / "binance" / "futures" / "um" / "perpetual" / "1m"
REPORTS_DIR: Path = DATA_ROOT / "reports"
LOGS_DIR: Path = DATA_ROOT / "logs"
TMP_ROOT: Path = DATA_ROOT / "tmp" / "canonical_build"          # never under raw/
QUARANTINE_ROOT: Path = DATA_ROOT / "quarantine" / "canonical"  # processed files only

SYMBOLS: tuple[str, ...] = (
    "BTCUSDT", "ETHUSDT", "AVAXUSDT", "AAVEUSDT", "NEARUSDT",
    "LINKUSDT", "LTCUSDT", "OPUSDT", "DOTUSDT",
)
INTERVAL = "1m"
RANGE_START_UTC = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
RANGE_END_UTC = datetime(2026, 9, 15, 23, 59, tzinfo=timezone.utc)

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
SCHEMA_VERSION = "um-perp-1m-canonical-v1"

DECIMAL_PRECISION = 38
DECIMAL_SCALE = 8          # observed source max: prices 7 dp, quote volumes 8 dp
FINGERPRINT_WORKERS = 8
EXPECTED_ZIP_COUNT = 423
EXPECTED_ROWS_PER_SYMBOL = 1_424_160
EXPECTED_ROWS_TOTAL = 12_817_440
EXPECTED_PARTITION_COUNT = 297   # 9 symbols * 33 months (2024-01 .. 2026-09)
# =============================================================================

MINUTE_MS = 60_000
KLINE_COLUMNS = 12
EXPECTED_HEADER = (
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore",
)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
QUANT = Decimal("1").scaleb(-DECIMAL_SCALE)          # 10^-8
SCALE_INT = 10 ** DECIMAL_SCALE
CHUNK_BYTES = 1024 * 1024

DECIMAL_TYPE = pa.decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
CANONICAL_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.dictionary(pa.int32(), pa.string()), nullable=False),
        pa.field("interval", pa.dictionary(pa.int32(), pa.string()), nullable=False),
        pa.field("open_time", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("open", DECIMAL_TYPE, nullable=False),
        pa.field("high", DECIMAL_TYPE, nullable=False),
        pa.field("low", DECIMAL_TYPE, nullable=False),
        pa.field("close", DECIMAL_TYPE, nullable=False),
        pa.field("volume", DECIMAL_TYPE, nullable=False),
        pa.field("close_time", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("quote_volume", DECIMAL_TYPE, nullable=False),
        pa.field("count", pa.int64(), nullable=False),
        pa.field("taker_buy_volume", DECIMAL_TYPE, nullable=False),
        pa.field("taker_buy_quote_volume", DECIMAL_TYPE, nullable=False),
        pa.field("ignore", pa.int64(), nullable=False),
    ],
    metadata={
        b"schema_version": SCHEMA_VERSION.encode("utf-8"),
        b"provider": b"Binance",
        b"market": b"USD-M USDT-margined PERPETUAL futures",
        b"archive_namespace": b"futures/um",
        b"interval": INTERVAL.encode("utf-8"),
        b"source": b"verified raw ZIP CSV members; raw archives are the source of truth",
    },
)

log = logging.getLogger("canonical_1m")


# ----------------------------------------------------------------------------- time / path helpers
def ms_of(dt: datetime) -> int:
    return (dt - EPOCH) // timedelta(milliseconds=1)


def iso_of_ms(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return (EPOCH + timedelta(milliseconds=ms)).strftime("%Y-%m-%dT%H:%M:%SZ")


def month_bounds_ms(year: int, month: int) -> tuple[int, int]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    nxt = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return ms_of(start), ms_of(nxt) - MINUTE_MS


def iter_year_months(start: datetime, end: datetime) -> Iterator[tuple[int, int]]:
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def assert_outside_raw(path: Path, raw_root: Path, label: str) -> None:
    try:
        path.resolve().relative_to(raw_root.resolve())
    except ValueError:
        return
    raise RuntimeError(f"{label} {path} resolves inside the read-only raw tree {raw_root}")


def hive_partition_dir(symbol: str, year: int, month: int) -> Path:
    return PROCESSED_ROOT / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}"


def hive_parquet_path(symbol: str, year: int, month: int) -> Path:
    return hive_partition_dir(symbol, year, month) / "data.parquet"


# ----------------------------------------------------------------------------- discovery
@dataclass(frozen=True)
class SourceArchive:
    symbol: str
    kind: str
    period: str
    zip_path: Path
    checksum_path: Path
    start_ms: int
    end_ms: int


@dataclass(frozen=True)
class Partition:
    symbol: str
    year: int
    month: int
    start_ms: int
    end_ms: int
    archives: tuple[SourceArchive, ...]

    @property
    def hive_path(self) -> Path:
        return hive_parquet_path(self.symbol, self.year, self.month)

    @property
    def expected_minutes(self) -> int:
        return (self.end_ms - self.start_ms) // MINUTE_MS + 1 if self.end_ms >= self.start_ms else 0


def list_raw_files(raw_root: Path) -> tuple[list[Path], list[Path]]:
    zips = sorted(p for p in raw_root.rglob("*.zip") if p.is_file())
    checksums = sorted(p for p in raw_root.rglob("*.zip.CHECKSUM") if p.is_file())
    return zips, checksums


def plan_from_disk(raw_root: Path, symbols: tuple[str, ...], range_start: datetime, range_end: datetime) -> tuple[list[Partition], dict]:
    """Build the active-coverage plan from files that actually exist. Monthly archives
    cover complete months; daily archives cover the partial month 2026-09-01..15.
    Never pairs a monthly archive with daily archives for the same minutes."""
    rs, re_ = ms_of(range_start), ms_of(range_end)
    partitions: list[Partition] = []
    missing: list[str] = []
    unused_note: list[str] = []
    planned_zips: set[Path] = set()

    for symbol in symbols:
        monthly_dir = raw_root / "monthly" / "klines" / symbol / INTERVAL
        daily_dir = raw_root / "daily" / "klines" / symbol / INTERVAL
        for year, month in iter_year_months(range_start, range_end):
            m_first, m_last = month_bounds_ms(year, month)
            p_start, p_end = max(m_first, rs), min(m_last, re_)
            archives: list[SourceArchive] = []
            complete_month = m_first >= rs and m_last <= re_
            if complete_month:
                period = f"{year:04d}-{month:02d}"
                zpath = monthly_dir / f"{symbol}-{INTERVAL}-{period}.zip"
                if not zpath.is_file():
                    missing.append(str(zpath))
                    continue
                archives.append(SourceArchive(
                    symbol, "monthly", period, zpath, zpath.with_name(zpath.name + ".CHECKSUM"),
                    m_first, m_last,
                ))
            else:
                day = datetime(year, month, 1, tzinfo=timezone.utc)
                month_next = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
                while day < month_next:
                    d_start = ms_of(day)
                    d_end = d_start + 1439 * MINUTE_MS
                    if d_start >= rs and d_end <= re_:
                        period = day.strftime("%Y-%m-%d")
                        zpath = daily_dir / f"{symbol}-{INTERVAL}-{period}.zip"
                        if not zpath.is_file():
                            missing.append(str(zpath))
                        else:
                            archives.append(SourceArchive(
                                symbol, "daily", period, zpath, zpath.with_name(zpath.name + ".CHECKSUM"),
                                d_start, d_end,
                            ))
                    day += timedelta(days=1)
            if not archives:
                continue
            for a in archives:
                planned_zips.add(a.zip_path.resolve())
                if not a.checksum_path.is_file():
                    missing.append(str(a.checksum_path))
            partitions.append(Partition(symbol, year, month, p_start, p_end, tuple(archives)))

    all_zips, all_checksums = list_raw_files(raw_root)
    for z in all_zips:
        if z.resolve() not in planned_zips:
            unused_note.append(str(z))
    inventory = {
        "zip_files_on_disk": len(all_zips),
        "checksum_files_on_disk": len(all_checksums),
        "monthly_zip_on_disk": sum(1 for p in all_zips if "monthly" in p.parts),
        "daily_zip_on_disk": sum(1 for p in all_zips if "daily" in p.parts),
        "interval_directories": sorted({p.parent.name for p in all_zips}),
        "spot_or_cm_paths": [str(p) for p in all_zips if "spot" in str(p).lower() or "\\cm\\" in str(p) or "/cm/" in str(p).replace("\\", "/")],
        "part_files_in_raw": [str(p) for p in raw_root.rglob("*.part")],
        "planned_zip_count": len(planned_zips),
        "missing_required_files": missing,
        "unplanned_zip_files": unused_note,
        "partition_count": len(partitions),
    }
    return partitions, inventory


# ----------------------------------------------------------------------------- raw immutability
def _fingerprint_one(path: Path, raw_root: Path) -> dict:
    st = path.stat()
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(CHUNK_BYTES), b""):
            h.update(block)
    try:
        rel = path.resolve().relative_to(raw_root.resolve()).as_posix()
    except ValueError:
        rel = str(path)
    return {
        "relative_path": rel,
        "size_bytes": st.st_size,
        "mtime_ns": st.st_mtime_ns,
        "sha256": h.hexdigest(),
    }


def fingerprint_raw(raw_root: Path) -> dict[str, dict]:
    zips, checksums = list_raw_files(raw_root)
    files = zips + checksums
    out: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=FINGERPRINT_WORKERS, thread_name_prefix="fp") as ex:
        for rec in ex.map(lambda p: _fingerprint_one(p, raw_root), files):
            out[rec["relative_path"]] = rec
    return out


def compare_fingerprints(before: dict[str, dict], after: dict[str, dict]) -> dict:
    mismatches = []
    all_keys = sorted(set(before) | set(after))
    for k in all_keys:
        b, a = before.get(k), after.get(k)
        if b != a:
            mismatches.append({"relative_path": k, "before": b, "after": a})
    return {
        "files_before": len(before),
        "files_after": len(after),
        "unchanged": len(mismatches) == 0 and before.keys() == after.keys(),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
    }


# ----------------------------------------------------------------------------- CSV streaming (read-only ZIP)
@dataclass
class ParsedRow:
    open_ms: int
    close_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    count: int
    taker_buy_volume: Decimal
    taker_buy_quote_volume: Decimal
    ignore: int


def parse_decimal(value: str, field: str) -> Decimal:
    try:
        d = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"unparseable {field}={value!r}") from exc
    if not d.is_finite():
        raise ValueError(f"{field}={value!r} is not a finite decimal")
    exp = d.as_tuple().exponent
    if isinstance(exp, int) and exp < -DECIMAL_SCALE:
        raise ValueError(f"{field}={value!r} has more than {DECIMAL_SCALE} decimal places; refusing to round")
    # Scale is known to be sufficient, so quantize only pads trailing zeros.
    return d.quantize(QUANT)


def parse_kline_row(row: list[str]) -> ParsedRow:
    if len(row) != KLINE_COLUMNS:
        raise ValueError(f"{len(row)} columns, expected {KLINE_COLUMNS}")
    open_ms = int(row[0])
    close_ms = int(row[6])
    if not (10**12 <= open_ms < 10**13):
        raise ValueError(f"open_time {open_ms} is not a millisecond epoch")
    if not (10**12 <= close_ms < 10**13):
        raise ValueError(f"close_time {close_ms} is not a millisecond epoch")
    return ParsedRow(
        open_ms=open_ms,
        close_ms=close_ms,
        open=parse_decimal(row[1], "open"),
        high=parse_decimal(row[2], "high"),
        low=parse_decimal(row[3], "low"),
        close=parse_decimal(row[4], "close"),
        volume=parse_decimal(row[5], "volume"),
        quote_volume=parse_decimal(row[7], "quote_volume"),
        count=int(row[8]),
        taker_buy_volume=parse_decimal(row[9], "taker_buy_volume"),
        taker_buy_quote_volume=parse_decimal(row[10], "taker_buy_quote_volume"),
        ignore=int(row[11]),
    )


def iter_archive_rows(archive: SourceArchive) -> Iterator[ParsedRow]:
    """Stream kline rows from the ZIP CSV member. The ZIP is opened read-only."""
    with zipfile.ZipFile(archive.zip_path, "r") as zf:
        expected = f"{archive.symbol}-{INTERVAL}-{archive.period}.csv"
        names = zf.namelist()
        if names != [expected]:
            raise ValueError(f"{archive.zip_path.name}: members {names!r}, expected exactly [{expected!r}]")
        with zf.open(expected, "r") as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", errors="strict", newline="")
            reader = csv.reader(text)
            header_checked = False
            for lineno, row in enumerate(reader, 1):
                if not row:
                    continue
                if not header_checked:
                    header_checked = True
                    if not row[0].strip().lstrip("-").isdigit():
                        if tuple(c.strip().lower() for c in row) != EXPECTED_HEADER:
                            raise ValueError(f"{archive.zip_path.name} line {lineno}: unexpected header {row!r}")
                        continue
                try:
                    yield parse_kline_row(row)
                except ValueError as exc:
                    raise ValueError(f"{archive.zip_path.name} line {lineno}: {exc}") from exc


def iter_partition_rows(partition: Partition) -> Iterator[ParsedRow]:
    for archive in partition.archives:
        yield from iter_archive_rows(archive)


# ----------------------------------------------------------------------------- payload hash (lossless numeric identity)
def scaled_int(d: Decimal) -> int:
    return int((d * SCALE_INT).to_integral_value())


def row_payload_bytes(symbol: str, row: ParsedRow) -> bytes:
    return (
        f"{symbol}|{row.open_ms}|{scaled_int(row.open)}|{scaled_int(row.high)}|{scaled_int(row.low)}|"
        f"{scaled_int(row.close)}|{scaled_int(row.volume)}|{row.close_ms}|{scaled_int(row.quote_volume)}|"
        f"{row.count}|{scaled_int(row.taker_buy_volume)}|{scaled_int(row.taker_buy_quote_volume)}|{row.ignore}\n"
    ).encode("ascii")


def parquet_row_payload_bytes(symbol: str, open_ms: int, close_ms: int, o, h, l, c, vol, qvol, count, tb, tbq, ign) -> bytes:
    return row_payload_bytes(symbol, ParsedRow(
        open_ms=int(open_ms), close_ms=int(close_ms),
        open=Decimal(str(o)), high=Decimal(str(h)), low=Decimal(str(l)), close=Decimal(str(c)),
        volume=Decimal(str(vol)), quote_volume=Decimal(str(qvol)), count=int(count),
        taker_buy_volume=Decimal(str(tb)), taker_buy_quote_volume=Decimal(str(tbq)), ignore=int(ign),
    ))


# ----------------------------------------------------------------------------- table build / quality
@dataclass
class PartitionStats:
    row_count: int = 0
    first_open_time_utc: Optional[str] = None
    last_open_time_utc: Optional[str] = None
    missing_minute_count: int = 0
    duplicate_minute_count: int = 0
    out_of_order_count: int = 0
    ohlc_violation_count: int = 0
    negative_volume_count: int = 0
    rows_outside_partition: int = 0
    misaligned_open_time_count: int = 0
    payload_sha256: Optional[str] = None
    content_checks_passed: bool = False
    anomalies: list = field(default_factory=list)


def rows_to_table(partition: Partition, rows: list[ParsedRow]) -> tuple[pa.Table, PartitionStats]:
    stats = PartitionStats(row_count=len(rows))
    seen: set[int] = set()
    prev: Optional[int] = None
    for row in rows:
        if prev is not None and row.open_ms < prev:
            stats.out_of_order_count += 1
        prev = row.open_ms
        if row.open_ms in seen:
            stats.duplicate_minute_count += 1
        else:
            seen.add(row.open_ms)
        if not (row.high >= max(row.open, row.close) and row.low <= min(row.open, row.close) and row.high >= row.low):
            stats.ohlc_violation_count += 1
        if row.volume < 0 or row.quote_volume < 0 or row.taker_buy_volume < 0 or row.taker_buy_quote_volume < 0 or row.count < 0:
            stats.negative_volume_count += 1
        if row.open_ms < partition.start_ms or row.open_ms > partition.end_ms:
            stats.rows_outside_partition += 1
        if row.open_ms % MINUTE_MS:
            stats.misaligned_open_time_count += 1
    present = sum(1 for t in seen if partition.start_ms <= t <= partition.end_ms)
    stats.missing_minute_count = partition.expected_minutes - present

    # Canonical order is chronological. Hash after this sort so Parquet read-back matches.
    ordered = sorted(rows, key=lambda r: (r.open_ms, r.close_ms))
    n = len(ordered)
    hasher = hashlib.sha256()
    open_ms: list[int] = []
    close_ms: list[int] = []
    opens: list[Decimal] = []
    highs: list[Decimal] = []
    lows: list[Decimal] = []
    closes: list[Decimal] = []
    vols: list[Decimal] = []
    qvols: list[Decimal] = []
    tbvs: list[Decimal] = []
    tbqs: list[Decimal] = []
    counts: list[int] = []
    ignores: list[int] = []
    for row in ordered:
        hasher.update(row_payload_bytes(partition.symbol, row))
        open_ms.append(row.open_ms)
        close_ms.append(row.close_ms)
        opens.append(row.open)
        highs.append(row.high)
        lows.append(row.low)
        closes.append(row.close)
        vols.append(row.volume)
        qvols.append(row.quote_volume)
        tbvs.append(row.taker_buy_volume)
        tbqs.append(row.taker_buy_quote_volume)
        counts.append(row.count)
        ignores.append(row.ignore)
    stats.payload_sha256 = hasher.hexdigest()

    table = pa.table(
        {
            "symbol": pa.array([partition.symbol] * n, pa.string()).dictionary_encode(),
            "interval": pa.array([INTERVAL] * n, pa.string()).dictionary_encode(),
            "open_time": pa.array(open_ms, type=pa.timestamp("ms", tz="UTC")),
            "open": pa.array(opens, type=DECIMAL_TYPE),
            "high": pa.array(highs, type=DECIMAL_TYPE),
            "low": pa.array(lows, type=DECIMAL_TYPE),
            "close": pa.array(closes, type=DECIMAL_TYPE),
            "volume": pa.array(vols, type=DECIMAL_TYPE),
            "close_time": pa.array(close_ms, type=pa.timestamp("ms", tz="UTC")),
            "quote_volume": pa.array(qvols, type=DECIMAL_TYPE),
            "count": pa.array(counts, type=pa.int64()),
            "taker_buy_volume": pa.array(tbvs, type=DECIMAL_TYPE),
            "taker_buy_quote_volume": pa.array(tbqs, type=DECIMAL_TYPE),
            "ignore": pa.array(ignores, type=pa.int64()),
        },
        schema=CANONICAL_SCHEMA,
    )
    if n:
        t_i64 = pc.cast(table.column("open_time"), pa.int64())
        stats.first_open_time_utc = iso_of_ms(int(pc.min(t_i64).as_py()))
        stats.last_open_time_utc = iso_of_ms(int(pc.max(t_i64).as_py()))
        if n >= 2:
            diffs = pc.subtract(t_i64.slice(1), t_i64.slice(0, n - 1))
            not_increasing = int(pc.sum(pc.less_equal(diffs, pa.scalar(0, type=pa.int64()))).as_py() or 0)
            if not_increasing:
                stats.anomalies.append(
                    f"after sort, {not_increasing} adjacent open_time pair(s) are not strictly increasing"
                )
    if stats.duplicate_minute_count:
        stats.anomalies.append(f"{stats.duplicate_minute_count} duplicate open time(s)")
    if stats.out_of_order_count:
        stats.anomalies.append(f"{stats.out_of_order_count} source row(s) not in chronological order (sorted in Parquet)")
    if stats.rows_outside_partition:
        stats.anomalies.append(f"{stats.rows_outside_partition} row(s) with open time outside the partition UTC month/range")
    if stats.misaligned_open_time_count:
        stats.anomalies.append(f"{stats.misaligned_open_time_count} open time(s) not aligned to a minute boundary")
    if stats.ohlc_violation_count:
        stats.anomalies.append(f"{stats.ohlc_violation_count} row(s) violate OHLC invariants")
    if stats.negative_volume_count:
        stats.anomalies.append(f"{stats.negative_volume_count} row(s) with negative volume/count")
    if stats.missing_minute_count:
        stats.anomalies.append(f"{stats.missing_minute_count} missing one-minute timestamp(s) in the partition window")
    stats.content_checks_passed = not stats.anomalies
    return table, stats


def table_payload_sha256(symbol: str, table: pa.Table) -> str:
    """Hash Parquet-decoded values with the same encoding used for the raw CSV stream."""
    hasher = hashlib.sha256()
    open_ms = pc.cast(table.column("open_time"), pa.int64()).to_pylist()
    close_ms = pc.cast(table.column("close_time"), pa.int64()).to_pylist()
    cols = {
        name: table.column(name).to_pylist()
        for name in (
            "open", "high", "low", "close", "volume",
            "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore",
        )
    }
    for i in range(table.num_rows):
        hasher.update(parquet_row_payload_bytes(
            symbol, int(open_ms[i]), int(close_ms[i]),
            cols["open"][i], cols["high"][i], cols["low"][i], cols["close"][i],
            cols["volume"][i], cols["quote_volume"][i], cols["count"][i],
            cols["taker_buy_volume"][i], cols["taker_buy_quote_volume"][i], cols["ignore"][i],
        ))
    return hasher.hexdigest()


def write_parquet_atomic(table: pa.Table, final: Path, tmp_dir: Path, extra_meta: dict[bytes, bytes]) -> dict:
    assert_outside_raw(final, RAW_ROOT, "parquet destination")
    assert_outside_raw(tmp_dir, RAW_ROOT, "parquet temp dir")
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    part = tmp_dir / f"{final.parent.parent.parent.name}_{final.parent.parent.name}_{final.parent.name}.parquet.part"
    if part.exists():
        part.unlink()
    schema = table.schema.with_metadata({**(table.schema.metadata or {}), **extra_meta})
    table = table.cast(schema)
    pq.write_table(
        table,
        part,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        version="2.6",
        coerce_timestamps="ms",
        allow_truncated_timestamps=False,
        write_statistics=True,
        use_dictionary=["symbol", "interval"],
    )
    os.replace(part, final)
    meta = pq.read_metadata(final)
    return {
        "file_size_bytes": final.stat().st_size,
        "num_rows": meta.num_rows,
        "num_row_groups": meta.num_row_groups,
        "num_columns": meta.num_columns,
        "serialized_size": meta.serialized_size,
        "compression": PARQUET_COMPRESSION,
        "created_by": meta.created_by,
        "schema_names": list(pq.read_schema(final).names),
    }


def read_partition_table(path: Path) -> pa.Table:
    """Read one data.parquet file only. Do not infer Hive partition keys from parent directories."""
    table = pq.ParquetFile(path).read()
    extras = [name for name in table.column_names if name not in CANONICAL_SCHEMA.names]
    if extras:
        table = table.drop(extras)
    missing = [name for name in CANONICAL_SCHEMA.names if name not in table.column_names]
    if missing:
        raise ValueError(f"{path} missing canonical columns {missing}; have {table.column_names}")
    table = table.select(list(CANONICAL_SCHEMA.names))
    return table.cast(CANONICAL_SCHEMA)


def quarantine_processed(path: Path, run_id: str, reason: str) -> Optional[str]:
    if not path.exists():
        return None
    dest = QUARANTINE_ROOT / run_id / path.parent.parent.parent.name / path.parent.parent.name / path.parent.name / path.name
    assert_outside_raw(dest, RAW_ROOT, "processed quarantine")
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest = dest.with_name(f"{dest.name}.{int(time.time() * 1000)}")
    shutil.move(str(path), str(dest))
    dest.with_name(dest.name + ".quarantine_reason.txt").write_text(
        f"{datetime.now(timezone.utc).isoformat()}\n{reason}\n", encoding="utf-8"
    )
    return str(dest)


# ----------------------------------------------------------------------------- partition processing
def process_partition(partition: Partition, run_id: str, tmp_dir: Path) -> dict:
    rec = {
        "symbol": partition.symbol,
        "interval": INTERVAL,
        "year": partition.year,
        "month": partition.month,
        "hive_path": str(partition.hive_path),
        "partition_start_utc": iso_of_ms(partition.start_ms),
        "partition_end_utc": iso_of_ms(partition.end_ms),
        "expected_minute_count": partition.expected_minutes,
        "source_archives": [
            {
                "kind": a.kind,
                "period": a.period,
                "zip_path": str(a.zip_path),
                "checksum_path": str(a.checksum_path),
                "zip_url_namespace": "futures/um",
            }
            for a in partition.archives
        ],
        "status": None,
        "error": None,
        "equivalent_to_raw": False,
        "raw_payload_sha256": None,
        "parquet_payload_sha256": None,
        "parquet_metadata": None,
        "quarantined_file": None,
        "notes": [],
    }
    try:
        rows = list(iter_partition_rows(partition))
        table, stats = rows_to_table(partition, rows)
        rec.update({
            "row_count": stats.row_count,
            "first_open_time_utc": stats.first_open_time_utc,
            "last_open_time_utc": stats.last_open_time_utc,
            "missing_minute_count": stats.missing_minute_count,
            "duplicate_minute_count": stats.duplicate_minute_count,
            "out_of_order_count": stats.out_of_order_count,
            "ohlc_violation_count": stats.ohlc_violation_count,
            "negative_volume_count": stats.negative_volume_count,
            "rows_outside_partition": stats.rows_outside_partition,
            "misaligned_open_time_count": stats.misaligned_open_time_count,
            "content_checks_passed": stats.content_checks_passed,
            "anomalies": stats.anomalies,
            "raw_payload_sha256": stats.payload_sha256,
        })
        extra_meta = {
            b"schema_version": SCHEMA_VERSION.encode("utf-8"),
            b"symbol": partition.symbol.encode("utf-8"),
            b"year": f"{partition.year}".encode("ascii"),
            b"month": f"{partition.month:02d}".encode("ascii"),
            b"raw_payload_sha256": (stats.payload_sha256 or "").encode("ascii"),
            b"run_id": run_id.encode("ascii"),
        }

        final = partition.hive_path
        if final.exists():
            existing = read_partition_table(final)
            existing_hash = table_payload_sha256(partition.symbol, existing)
            rec["parquet_payload_sha256"] = existing_hash
            rec["parquet_metadata"] = {
                "file_size_bytes": final.stat().st_size,
                "num_rows": existing.num_rows,
                "schema_names": existing.schema.names,
            }
            if existing.num_rows == stats.row_count and existing_hash == stats.payload_sha256:
                rec["equivalent_to_raw"] = True
                rec["status"] = "skipped-as-already-equivalent"
                rec["notes"].append("existing Parquet payload hash matches the raw ZIP CSV stream; not rewritten")
                log.info("%s %04d-%02d: skip (already equivalent, %d rows)", partition.symbol, partition.year, partition.month, stats.row_count)
                return rec
            reason = (
                f"existing Parquet is not equivalent to raw "
                f"(rows {existing.num_rows} vs {stats.row_count}, "
                f"hash {existing_hash} vs {stats.payload_sha256})"
            )
            rec["quarantined_file"] = quarantine_processed(final, run_id, reason)
            rec["notes"].append(reason + "; quarantined processed file and rebuilt from raw")
            log.warning("%s %04d-%02d: %s", partition.symbol, partition.year, partition.month, reason)

        meta = write_parquet_atomic(table, final, tmp_dir, extra_meta)
        written = read_partition_table(final)
        written_hash = table_payload_sha256(partition.symbol, written)
        rec["parquet_payload_sha256"] = written_hash
        rec["parquet_metadata"] = meta
        if written.num_rows != stats.row_count or written_hash != stats.payload_sha256:
            rec["status"] = "failed"
            rec["error"] = (
                f"read-back mismatch: rows {written.num_rows} vs {stats.row_count}, "
                f"hash {written_hash} vs {stats.payload_sha256}"
            )
            rec["equivalent_to_raw"] = False
            log.error("%s %04d-%02d: %s", partition.symbol, partition.year, partition.month, rec["error"])
            return rec
        rec["equivalent_to_raw"] = True
        rec["status"] = "verified-equivalent"
        log.info("%s %04d-%02d: wrote %d rows -> %s sha256=%s", partition.symbol, partition.year, partition.month,
                 stats.row_count, final, written_hash[:16])
        return rec
    except Exception as exc:
        rec["status"] = "failed"
        rec["error"] = f"{type(exc).__name__}: {exc}"
        log.error("%s %04d-%02d: FAILED -- %s", partition.symbol, partition.year, partition.month, rec["error"])
        return rec


# ----------------------------------------------------------------------------- dataset-level checks
def processed_inventory() -> dict:
    if not PROCESSED_ROOT.exists():
        return {"parquet_files": 0, "hive_partition_dirs": 0, "part_files": 0, "symbols": []}
    files = sorted(PROCESSED_ROOT.rglob("data.parquet"))
    parts = list(PROCESSED_ROOT.rglob("*.part"))
    symbols = sorted({p.parent.parent.parent.name.split("=", 1)[-1] for p in files})
    return {
        "parquet_files": len(files),
        "hive_partition_dirs": len(files),
        "part_files": len(parts),
        "symbols": symbols,
        "interval_leaf": PROCESSED_ROOT.name,
    }


def overall_equivalence(partition_records: list[dict]) -> dict:
    by_symbol: dict[str, dict] = {}
    for rec in partition_records:
        s = rec["symbol"]
        slot = by_symbol.setdefault(s, {
            "row_count": 0, "partitions": 0, "equivalent_partitions": 0, "failed_partitions": 0,
            "missing_minute_count": 0, "duplicate_minute_count": 0, "out_of_order_count": 0,
            "ohlc_violation_count": 0, "first_open_time_utc": None, "last_open_time_utc": None,
        })
        slot["partitions"] += 1
        slot["row_count"] += rec.get("row_count") or 0
        slot["missing_minute_count"] += rec.get("missing_minute_count") or 0
        slot["duplicate_minute_count"] += rec.get("duplicate_minute_count") or 0
        slot["out_of_order_count"] += rec.get("out_of_order_count") or 0
        slot["ohlc_violation_count"] += rec.get("ohlc_violation_count") or 0
        if rec.get("equivalent_to_raw"):
            slot["equivalent_partitions"] += 1
        if rec.get("status") == "failed":
            slot["failed_partitions"] += 1
        fo, lo = rec.get("first_open_time_utc"), rec.get("last_open_time_utc")
        if fo and (slot["first_open_time_utc"] is None or fo < slot["first_open_time_utc"]):
            slot["first_open_time_utc"] = fo
        if lo and (slot["last_open_time_utc"] is None or lo > slot["last_open_time_utc"]):
            slot["last_open_time_utc"] = lo
    total_rows = sum(v["row_count"] for v in by_symbol.values())
    all_eq = all(r.get("equivalent_to_raw") for r in partition_records) and partition_records
    return {
        "all_partitions_equivalent_to_raw": bool(all_eq),
        "partition_records": len(partition_records),
        "verified_or_skipped_equivalent": sum(1 for r in partition_records if r.get("status") in ("verified-equivalent", "skipped-as-already-equivalent")),
        "failed_partitions": sum(1 for r in partition_records if r.get("status") == "failed"),
        "total_parquet_rows": total_rows,
        "expected_total_rows": EXPECTED_ROWS_TOTAL,
        "row_count_matches_expected": total_rows == EXPECTED_ROWS_TOTAL,
        "per_symbol": by_symbol,
    }


# ----------------------------------------------------------------------------- logging / report
def setup_logging(log_file: Path) -> None:
    assert_outside_raw(log_file, RAW_ROOT, "log file")
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
    assert_outside_raw(path, RAW_ROOT, "report")
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    with open(part, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
        fh.write("\n")
    os.replace(part, path)


# ----------------------------------------------------------------------------- orchestration
def run() -> int:
    started = datetime.now(timezone.utc)
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    for required in (DATA_ROOT, RAW_ROOT):
        if not required.exists():
            sys.stderr.write(f"BLOCKER: {required} does not exist\n")
            return 2
    for p, label in ((PROCESSED_ROOT, "processed"), (REPORTS_DIR, "reports"), (LOGS_DIR, "logs"),
                     (TMP_ROOT, "tmp"), (QUARANTINE_ROOT, "quarantine")):
        assert_outside_raw(p, RAW_ROOT, label)
        p.mkdir(parents=True, exist_ok=True)
    setup_logging(LOGS_DIR / "canonical_build.log")
    log.info("run %s started", run_id)
    log.info("python %s", sys.version.replace("\n", " "))
    log.info("pyarrow %s executable %s", pa.__version__, sys.executable)
    log.info("schema_version %s compression %s/%s", SCHEMA_VERSION, PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL)

    # 1. inspect disk (do not trust the download report alone)
    partitions, inventory = plan_from_disk(RAW_ROOT, SYMBOLS, RANGE_START_UTC, RANGE_END_UTC)
    log.info("raw inventory: %s", {k: inventory[k] for k in (
        "zip_files_on_disk", "checksum_files_on_disk", "monthly_zip_on_disk", "daily_zip_on_disk",
        "planned_zip_count", "partition_count", "interval_directories")})
    if inventory["spot_or_cm_paths"] or inventory["part_files_in_raw"]:
        log.error("BLOCKER: raw tree contains spot/cm paths or .part files: %s %s",
                  inventory["spot_or_cm_paths"], inventory["part_files_in_raw"])
        return 2
    if inventory["missing_required_files"]:
        log.error("BLOCKER: missing %d required source files, first: %s",
                  len(inventory["missing_required_files"]), inventory["missing_required_files"][:5])
        return 2
    if inventory["unplanned_zip_files"]:
        log.warning("unplanned ZIP files on disk (not used): %s", inventory["unplanned_zip_files"])
    if inventory["zip_files_on_disk"] != EXPECTED_ZIP_COUNT or inventory["interval_directories"] != [INTERVAL]:
        log.error("BLOCKER: unexpected raw inventory (zips=%s intervals=%s)",
                  inventory["zip_files_on_disk"], inventory["interval_directories"])
        return 2
    if len(partitions) != EXPECTED_PARTITION_COUNT:
        log.error("BLOCKER: planned %d partitions, expected %d", len(partitions), EXPECTED_PARTITION_COUNT)
        return 2

    already = sum(1 for p in partitions if p.hive_path.exists())
    for line in (
        "=" * 100,
        "Canonical Parquet build from verified Binance USD-M PERPETUAL 1m raw ZIPs -- NOT Spot, NOT a downloader",
        f"Symbols ({len(SYMBOLS)}): {', '.join(SYMBOLS)}",
        f"Interval: {INTERVAL} only | schema {SCHEMA_VERSION} | pyarrow {pa.__version__}",
        f"UTC range: {RANGE_START_UTC.strftime('%Y-%m-%dT%H:%M:%SZ')} through {RANGE_END_UTC.strftime('%Y-%m-%dT%H:%M:%SZ')} inclusive",
        f"Raw (read-only): {RAW_ROOT}",
        f"Processed Hive root: {PROCESSED_ROOT}",
        f"Plan: {len(partitions)} partitions from {inventory['planned_zip_count']} ZIP archives "
        f"({inventory['monthly_zip_on_disk']} monthly + {inventory['daily_zip_on_disk']} daily); "
        f"{already} partitions already on disk will be re-verified and skipped if equivalent",
        f"Temp (outside raw): {TMP_ROOT}",
        "=" * 100,
    ):
        log.info(line)

    # 2. fingerprint raw BEFORE any processing
    log.info("fingerprinting %d raw ZIP + CHECKSUM files (sha256 + mtime_ns + size)...", inventory["zip_files_on_disk"] + inventory["checksum_files_on_disk"])
    before_fp = fingerprint_raw(RAW_ROOT)
    log.info("pre-run fingerprint captured for %d files", len(before_fp))

    # 3. process partitions sequentially (one month in memory at a time)
    records: list[dict] = []
    interrupted = False
    try:
        for i, part in enumerate(partitions, 1):
            rec = process_partition(part, run_id, TMP_ROOT)
            records.append(rec)
            if i % 33 == 0:
                log.info("progress: %d/%d partitions", i, len(partitions))
    except KeyboardInterrupt:
        interrupted = True
        log.warning("interrupted -- partitions already written remain; rerun is idempotent")

    # 4. fingerprint raw AFTER
    log.info("re-fingerprinting raw tree to confirm immutability...")
    after_fp = fingerprint_raw(RAW_ROOT)
    immut = compare_fingerprints(before_fp, after_fp)
    if not immut["unchanged"]:
        log.error("RAW TREE CHANGED during processing -- %d mismatch(es)", immut["mismatch_count"])
    else:
        log.info("raw ZIP and CHECKSUM files unchanged (sha256, size, mtime_ns)")

    # 5. leftover temp parts
    leftover_tmp = list(TMP_ROOT.glob("*.part"))
    leftover_proc = list(PROCESSED_ROOT.rglob("*.part")) if PROCESSED_ROOT.exists() else []
    for p in leftover_tmp + leftover_proc:
        try:
            p.unlink()
            log.info("removed leftover part file %s", p)
        except OSError as exc:
            log.warning("could not remove %s: %s", p, exc)

    eq = overall_equivalence(records)
    inv = processed_inventory()
    now = datetime.now(timezone.utc)
    report = {
        "report_version": 1,
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_id": run_id,
        "run_started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_seconds": round((now - started).total_seconds(), 1),
        "interrupted": interrupted,
        "python": sys.version.split()[0],
        "pyarrow": pa.__version__,
        "executable": sys.executable,
        "provider": "Binance",
        "market": "Binance USD-M futures (USDT-margined PERPETUAL contracts)",
        "archive_namespace": "futures/um",
        "interval": INTERVAL,
        "parquet_compression": PARQUET_COMPRESSION,
        "decimal": {"precision": DECIMAL_PRECISION, "scale": DECIMAL_SCALE, "rounding": "none-required; extra source digits are a hard error"},
        "canonical_columns": CANONICAL_SCHEMA.names,
        "required_range_utc": {
            "first_open_time": RANGE_START_UTC.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "last_open_time": RANGE_END_UTC.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "inclusive": True,
        },
        "data_root": str(DATA_ROOT),
        "raw_root": str(RAW_ROOT),
        "processed_root": str(PROCESSED_ROOT),
        "requested_symbols": list(SYMBOLS),
        "raw_inventory": inventory,
        "raw_immutability": immut,
        "equivalence": eq,
        "processed_inventory": inv,
        "partitions": records,
        "data_quality_note": (
            "Gaps, duplicates and OHLC anomalies are reported exactly as observed in the official "
            "raw ZIP CSV members; raw archives are the source of truth. "
            "Numeric values are stored as decimal128 with a scale strictly sufficient for the source CSVs; "
            "a source value with more fractional digits is a hard error. Equivalence is the SHA-256 of every "
            "row's scaled integer payload from the raw CSV stream versus the Parquet read-back."
        ),
    }
    report_path = REPORTS_DIR / "canonical_build_report.json"
    write_report(report_path, report)

    log.info("-" * 100)
    log.info("run summary: equivalent=%s rows=%d expected=%d failed=%d parquet_files=%d raw_unchanged=%s",
             eq["all_partitions_equivalent_to_raw"], eq["total_parquet_rows"], eq["expected_total_rows"],
             eq["failed_partitions"], inv["parquet_files"], immut["unchanged"])
    for sym in SYMBOLS:
        s = eq["per_symbol"].get(sym, {})
        log.info("%-9s rows=%s partitions=%s/%s equivalent missing=%s dup=%s ohlc=%s %s..%s",
                 sym, s.get("row_count"), s.get("equivalent_partitions"), s.get("partitions"),
                 s.get("missing_minute_count"), s.get("duplicate_minute_count"), s.get("ohlc_violation_count"),
                 s.get("first_open_time_utc"), s.get("last_open_time_utc"))
    log.info("report: %s", report_path)
    log.info("log:    %s", LOGS_DIR / "canonical_build.log")

    bad = (
        interrupted
        or not immut["unchanged"]
        or not eq["all_partitions_equivalent_to_raw"]
        or not eq["row_count_matches_expected"]
        or inv["parquet_files"] != EXPECTED_PARTITION_COUNT
        or eq["failed_partitions"]
        or any(s.get("row_count") != EXPECTED_ROWS_PER_SYMBOL for s in eq["per_symbol"].values())
    )
    exit_code = 1 if bad else 0
    log.info("run %s finished with exit code %d", run_id, exit_code)
    return exit_code


def main() -> int:
    return run()


if __name__ == "__main__":
    sys.exit(main())
