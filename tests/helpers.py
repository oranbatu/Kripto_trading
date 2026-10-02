"""Shared helpers for read-only tests. Never writes into real MarketData trees."""
from __future__ import annotations

import hashlib
import shutil
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import config
from data.loader import load_candles

UTC = timezone.utc
DEC = pa.decimal128(38, 8)
TS = pa.timestamp("ms", tz="UTC")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
TMP_ROOT = PROJECT_ROOT / "_tmp_tests"

DERIVED_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("open_time", TS, nullable=False),
        pa.field("open", DEC, nullable=False),
        pa.field("high", DEC, nullable=False),
        pa.field("low", DEC, nullable=False),
        pa.field("close", DEC, nullable=False),
        pa.field("volume", DEC, nullable=False),
        pa.field("close_time", TS, nullable=False),
        pa.field("quote_asset_volume", DEC, nullable=False),
        pa.field("number_of_trades", pa.int64(), nullable=False),
        pa.field("taker_buy_base_asset_volume", DEC, nullable=False),
        pa.field("taker_buy_quote_asset_volume", DEC, nullable=False),
        pa.field("source_candle_count", pa.int16(), nullable=False),
    ]
)


def utc(y, m, d, hh=0, mm=0, ss=0, ms=0) -> datetime:
    return datetime(y, m, d, hh, mm, ss, ms * 1000, tzinfo=UTC)


def d(value: str) -> Decimal:
    return Decimal(value)


def fingerprint_tree(paths: list[Path]) -> dict[str, dict]:
    out = {}
    for path in paths:
        if not path.is_file():
            continue
        h = hashlib.sha256()
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
        st = path.stat()
        out[str(path)] = {"sha256": h.hexdigest(), "size": st.st_size, "mtime_ns": st.st_mtime_ns}
    return out


def list_source_files(root: Path | None = None) -> list[Path]:
    root = root or config.data_root()
    files: list[Path] = []
    for tf in config.ALLOWED_TIMEFRAMES:
        files.extend(sorted(config.dataset_root(tf, root).rglob("data.parquet")))
    files.extend(sorted(config.reports_dir(root).glob("*.json")))
    return files


def write_derived_parquet(path: Path, table: pa.Table, metadata: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = {k.encode("utf-8"): v.encode("utf-8") for k, v in metadata.items()}
    schema = table.schema.with_metadata(encoded)
    table = table.cast(schema)
    pq.write_table(table, path, compression="zstd")


def hive_path(root: Path, timeframe: str, symbol: str, year: int, month: int | None) -> Path:
    base = config.dataset_root(timeframe, root)
    if month is None:
        return base / f"symbol={symbol}" / f"year={year}" / "data.parquet"
    return base / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}" / "data.parquet"


def copy_real_month(timeframe: str, symbol: str, year: int, month: int, dest_root: Path) -> Path:
    src = config.dataset_root(timeframe) / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}" / "data.parquet"
    dest = hive_path(dest_root, timeframe, symbol, year, month)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return dest


def mutate_high(path: Path, open_utc: datetime, sentinel: Decimal) -> None:
    table = pq.ParquetFile(path).read()
    opens = table.column("open_time").cast(pa.int64()).to_pylist()
    target = int((open_utc - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds() * 1000)
    # integer ms without float:
    delta = open_utc - datetime(1970, 1, 1, tzinfo=UTC)
    target = delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
    idx = opens.index(target)
    highs = table.column("high").to_pylist()
    highs[idx] = sentinel
    arrays = []
    for field in table.schema:
        if field.name == "high":
            arrays.append(pa.array(highs, type=field.type))
        else:
            arrays.append(table.column(field.name).combine_chunks())
    new = pa.Table.from_arrays(arrays, schema=table.schema)
    write_derived_parquet(path, new, {k.decode(): v.decode() for k, v in (table.schema.metadata or {}).items()})


def load_day(symbol: str, timeframe: str, day: datetime, **kwargs):
    end = utc(day.year, day.month, day.day) 
    # caller should pass full next day; helper:
    from datetime import timedelta
    start = day if day.tzinfo else utc(day.year, day.month, day.day)
    return load_candles(symbol, timeframe, start, start + timedelta(days=1), **kwargs)
