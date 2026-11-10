#!/usr/bin/env python3
"""Derive Spot BTC 5m, 15m, 30m, 1h, 4h and 1d candles from canonical 1m data.

The symbol list, source root and destination timeframes are fixed. This module
does not download data, does not write futures paths, and does not change the
USD-M derived builders. Importing it does not publish files.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.binance.spot_btc_pairs import (
    EXCLUDED_SYMBOLS,
    EXPECTED_PARTITIONS_PER_SYMBOL,
    EXPECTED_ROWS_PER_SYMBOL,
    EXPECTED_ROWS_TOTAL,
    MINUTE_MS,
    RANGE_EXCLUSIVE_END_MS,
    RANGE_LAST_OPEN_MS,
    RANGE_START_MS,
    SCHEMA_VERSION as SOURCE_SCHEMA,
    SPOT_SYMBOLS,
    base_asset,
    days_in_month,
    expected_partition_minutes,
    iso_close_of_ms,
    iso_of_ms,
    ms_of,
    partition_months,
    pid_alive,
    reject_symbol,
    require_spot_symbols,
)

DATA_ROOT = Path(r"C:\MarketData")
SOURCE_ROOT = DATA_ROOT / "processed" / "binance" / "spot" / "1m"
DERIVED_ROOT = DATA_ROOT / "derived" / "binance" / "spot"
REPORTS_DIR = DATA_ROOT / "reports"
LOGS_DIR = DATA_ROOT / "logs"
TASK_NAME = "binance_spot_btc_pairs_derived"
BUILDER_VERSION = "spot-btc-derived-v1"
AGGREGATION_RULES = (
    "open=first;high=max;low=min;close=last;volume=exact-sum;"
    "quote_asset_volume=exact-sum;number_of_trades=exact-sum;"
    "taker_buy_base_asset_volume=exact-sum;taker_buy_quote_asset_volume=exact-sum;"
    "incomplete-bucket=reject"
)
DECIMAL_SCALE = 8
SCALE = 10 ** DECIMAL_SCALE
CLOSE_OFFSET_MS = 59_999
DAY_MS = 24 * 60 * MINUTE_MS
DISK_MARGIN_BYTES = 2 * 1024 * 1024 * 1024

TIMEFRAMES: dict[str, dict] = {
    "5m": {"components": 5, "bucket_ms": 5 * MINUTE_MS, "partition": "month", "rows": 296_928, "october": 7_776, "last_open": "2026-10-27T23:55:00Z", "schema": "spot-5m-derived-v1"},
    "15m": {"components": 15, "bucket_ms": 15 * MINUTE_MS, "partition": "month", "rows": 98_976, "october": 2_592, "last_open": "2026-10-27T23:45:00Z", "schema": "spot-15m-derived-v1"},
    "30m": {"components": 30, "bucket_ms": 30 * MINUTE_MS, "partition": "month", "rows": 49_488, "october": 1_296, "last_open": "2026-10-27T23:30:00Z", "schema": "spot-30m-derived-v1"},
    "1h": {"components": 60, "bucket_ms": 60 * MINUTE_MS, "partition": "month", "rows": 24_744, "october": 648, "last_open": "2026-10-27T23:00:00Z", "schema": "spot-1h-derived-v1"},
    "4h": {"components": 240, "bucket_ms": 4 * 60 * MINUTE_MS, "partition": "month", "rows": 6_186, "october": 162, "last_open": "2026-10-27T20:00:00Z", "schema": "spot-4h-derived-v1"},
    "1d": {"components": 1_440, "bucket_ms": DAY_MS, "partition": "year", "rows": 1_031, "october": 27, "last_open": "2026-10-27T00:00:00Z", "schema": "spot-1d-derived-v1"},
}
TIMEFRAME_ORDER = ("5m", "15m", "30m", "1h", "4h", "1d")
HIERARCHY = (
    ("5m", "15m", 3), ("5m", "30m", 6), ("5m", "1h", 12), ("5m", "4h", 48), ("5m", "1d", 288),
    ("15m", "30m", 2), ("15m", "1h", 4), ("15m", "4h", 16), ("15m", "1d", 96),
    ("30m", "1h", 2), ("30m", "4h", 8), ("30m", "1d", 48),
    ("1h", "4h", 4), ("1h", "1d", 24),
    ("4h", "1d", 6),
)
REVIEWED_SOURCE_FINGERPRINTS = {
    "ETHBTC": "dac925f03931fd84c5eabb4a3e9d5cf62ec2cbb90463301a582f7f5172bd264e",
    "AVAXBTC": "7fd566ff54a3b77f212b65fa217b790b70c36a3cbe5c70c8e708cb0c442be5be",
    "NEARBTC": "631c5397fcdca86f43070e6d2ab7c76a9ce0e0096f38befb3ecb0067a252d456",
    "LINKBTC": "93ba3b4f717df75e06ed2281ccd0c967f64961e2ee00b07acb604ec81525bbcc",
    "LTCBTC": "5e1fcd496de297eb680fca6d792bda523be45fb1923ea26377470c6d59652c37",
    "DOTBTC": "4600840145673f3dbb0944cef27889ae8396af26046d953b8dc28fa78c171ec9",
    "XRPBTC": "3541573e4535c324d163161fa1ebffeb43b1c13b1989526c074e968d4a7e87fa",
    "DOGEBTC": "0a0eb3c9455d255afa9be83eebe8ffa3f56dc382a50185b1a728a91f05ef8d46",
    "ADABTC": "182a8430ccde2d2c6bbb2993cf97e72754621bbf9d88ec1674aa5da30ed0b895",
    "SOLBTC": "d2c4c19656ae4854c64b51c2eb8cc98cb487fe16c99b49f4d20807674cfd0e2e",
}
REJECTED_EXAMPLES = ("BTCBTC", "AAVEBTC", "OPBTC", "ETHUSDT", "ETHUSD", "ETHUSDC", "ETHFDUSD", "ETHUSD1", "ETHBNB", "BTCUSDT", "ETHBTC.P")
DECIMAL_TYPE = pa.decimal128(38, DECIMAL_SCALE)
TS_UTC = pa.timestamp("ms", tz="UTC")
OUTPUT_COLUMNS = (
    "symbol", "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume", "source_candle_count",
)
CRITICAL_METADATA = (
    "provider", "market_type", "symbol", "base_asset", "quote_asset", "source_timeframe",
    "timeframe", "schema_version", "coverage_start", "coverage_end", "exclusive_end",
    "source_partitions", "source_fingerprints", "source_schema", "builder_version",
    "aggregation_rules", "expected_component_count",
)
SUM_FIELDS = ("volume", "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume")
log = logging.getLogger("spot_btc_derived")


def output_schema(timeframe: str) -> pa.Schema:
    spec = TIMEFRAMES[timeframe]
    return pa.schema(
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
            b"schema_version": spec["schema"].encode("ascii"),
            b"provider": b"Binance",
            b"market_type": b"spot",
            b"quote_asset": b"BTC",
            b"source_timeframe": b"1m",
            b"timeframe": timeframe.encode("ascii"),
            b"builder_version": BUILDER_VERSION.encode("ascii"),
            b"aggregation_rules": AGGREGATION_RULES.encode("ascii"),
            b"expected_component_count": str(spec["components"]).encode("ascii"),
        },
    )


def assert_spot_scope(symbol: str) -> str:
    reject_symbol(symbol)
    if symbol in EXCLUDED_SYMBOLS or not symbol.endswith("BTC") or symbol == "BTCBTC":
        raise ValueError(f"rejected Spot symbol {symbol!r}")
    return symbol


def assert_under_spot(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    resolved.relative_to(root.resolve())
    parts = {part.lower() for part in resolved.parts}
    if "futures" in parts or "perpetual" in parts:
        raise ValueError(f"{resolved} is not a Spot destination")
    return resolved


def source_partition(root: Path, symbol: str, year: int, month: int) -> Path:
    return root / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}" / "data.parquet"


def derived_partition(spot_root: Path, timeframe: str, symbol: str, year: int, month: int | None = None) -> Path:
    """spot_root is the derived/binance/spot directory, not a timeframe directory."""
    base = spot_root / timeframe / f"symbol={symbol}" / f"year={year}"
    if TIMEFRAMES[timeframe]["partition"] == "year":
        return base / "data.parquet"
    if month is None:
        raise ValueError(f"{timeframe} requires a month")
    return base / f"month={month:02d}" / "data.parquet"


def unscaled(value: int) -> Decimal:
    return Decimal(value).scaleb(-DECIMAL_SCALE)


def scaled(value) -> int:
    number = Decimal(value)
    exponent = number.as_tuple().exponent
    if isinstance(exponent, int) and exponent < -DECIMAL_SCALE:
        raise ValueError(f"{value!r} has more than {DECIMAL_SCALE} decimal places")
    return int(number.scaleb(DECIMAL_SCALE))


def aggregate_block(block: dict, step: int, bucket_ms: int) -> dict:
    """Aggregate one continuous UTC minute block. Incomplete blocks raise."""
    opens = block["open_ms"]
    count = len(opens)
    if step <= 0 or count == 0 or count % step:
        raise ValueError(f"incomplete bucket: {count} source rows is not divisible by {step}")
    closes = block["close_ms"]
    fields = ("open", "high", "low", "close", "volume", "quote", "trades", "taker_base", "taker_quote")
    out = {name: [] for name in ("open_ms", "close_ms", *fields)}
    for start in range(0, count, step):
        stop = start + step
        open_ms = opens[start]
        if open_ms % bucket_ms:
            raise ValueError(f"{iso_of_ms(open_ms)} is not aligned to the {bucket_ms} ms bucket")
        if open_ms < RANGE_START_MS or open_ms > RANGE_LAST_OPEN_MS or open_ms >= RANGE_EXCLUSIVE_END_MS:
            raise ValueError(f"{iso_of_ms(open_ms)} is outside the Spot window")
        for offset in range(step):
            expected = open_ms + offset * MINUTE_MS
            seen = opens[start + offset]
            if seen != expected:
                kind = "duplicate" if offset and seen == opens[start + offset - 1] else "gap or out-of-order"
                raise ValueError(f"{kind} at {iso_of_ms(seen)}; expected {iso_of_ms(expected)}")
            if seen > RANGE_LAST_OPEN_MS or seen >= RANGE_EXCLUSIVE_END_MS:
                raise ValueError(f"{iso_of_ms(seen)} is outside the Spot window")
            if closes[start + offset] != seen + CLOSE_OFFSET_MS:
                raise ValueError(f"malformed close at {iso_of_ms(seen)}")
            if min(block["open"][start + offset], block["high"][start + offset], block["low"][start + offset], block["close"][start + offset]) <= 0:
                raise ValueError(f"non-positive price at {iso_of_ms(seen)}")
            high = block["high"][start + offset]
            low = block["low"][start + offset]
            price_open = block["open"][start + offset]
            price_close = block["close"][start + offset]
            if high < max(price_open, price_close, low) or low > min(price_open, price_close, high):
                raise ValueError(f"OHLC violation at {iso_of_ms(seen)}")
            if min(block["volume"][start + offset], block["quote"][start + offset], block["taker_base"][start + offset], block["taker_quote"][start + offset], block["trades"][start + offset]) < 0:
                raise ValueError(f"negative volume or trade count at {iso_of_ms(seen)}")
        out["open_ms"].append(open_ms)
        out["close_ms"].append(closes[stop - 1])
        out["open"].append(block["open"][start])
        out["high"].append(max(block["high"][start:stop]))
        out["low"].append(min(block["low"][start:stop]))
        out["close"].append(block["close"][stop - 1])
        out["volume"].append(sum(block["volume"][start:stop]))
        out["quote"].append(sum(block["quote"][start:stop]))
        out["trades"].append(sum(block["trades"][start:stop]))
        out["taker_base"].append(sum(block["taker_base"][start:stop]))
        out["taker_quote"].append(sum(block["taker_quote"][start:stop]))
        if out["close_ms"][-1] != open_ms + bucket_ms - 1:
            raise ValueError(f"bucket close is not the final millisecond of {iso_of_ms(open_ms)}")
        if out["high"][-1] < max(out["open"][-1], out["close"][-1], out["low"][-1]) or out["low"][-1] > min(out["open"][-1], out["close"][-1], out["high"][-1]):
            raise ValueError(f"aggregated OHLC violation at {iso_of_ms(open_ms)}")
    return out


def fold_block(child: dict, ratio: int) -> dict:
    step_ms = child["open_ms"][1] - child["open_ms"][0] if len(child["open_ms"]) > 1 else 0
    bucket_ms = step_ms * ratio
    renamed = {
        "open_ms": child["open_ms"],
        "close_ms": child["close_ms"],
        "open": child["open"],
        "high": child["high"],
        "low": child["low"],
        "close": child["close"],
        "volume": child["volume"],
        "quote": child["quote"],
        "trades": child["trades"],
        "taker_base": child["taker_base"],
        "taker_quote": child["taker_quote"],
    }
    # Child rows are already buckets. Reuse the continuity checker by presenting
    # them as synthetic minutes only when ratio grouping is exact and aligned.
    count = len(child["open_ms"])
    if count % ratio:
        raise ValueError(f"incomplete fold: {count} rows is not divisible by {ratio}")
    out = {name: [] for name in renamed}
    for start in range(0, count, ratio):
        stop = start + ratio
        for offset in range(1, ratio):
            if child["open_ms"][start + offset] != child["open_ms"][start] + offset * step_ms:
                raise ValueError("folded rows are not continuous")
        out["open_ms"].append(child["open_ms"][start])
        out["close_ms"].append(child["close_ms"][stop - 1])
        out["open"].append(child["open"][start])
        out["high"].append(max(child["high"][start:stop]))
        out["low"].append(min(child["low"][start:stop]))
        out["close"].append(child["close"][stop - 1])
        out["volume"].append(sum(child["volume"][start:stop]))
        out["quote"].append(sum(child["quote"][start:stop]))
        out["trades"].append(sum(child["trades"][start:stop]))
        out["taker_base"].append(sum(child["taker_base"][start:stop]))
        out["taker_quote"].append(sum(child["taker_quote"][start:stop]))
        if out["close_ms"][-1] != out["open_ms"][-1] + bucket_ms - 1:
            raise ValueError("folded close time is not the parent bucket end")
    return out


def blocks_equal(left: dict, right: dict) -> tuple[bool, str]:
    if len(left["open_ms"]) != len(right["open_ms"]):
        return False, f"row count {len(left['open_ms'])} != {len(right['open_ms'])}"
    keys = ("open_ms", "close_ms", "open", "high", "low", "close", "volume", "quote", "trades", "taker_base", "taker_quote")
    for index, open_ms in enumerate(left["open_ms"]):
        for key in keys:
            if left[key][index] != right[key][index]:
                return False, f"{iso_of_ms(open_ms)} field {key}: {left[key][index]} != {right[key][index]}"
    return True, ""


def fingerprint_block(symbol: str, block: dict) -> str:
    digest = hashlib.sha256()
    for index, open_ms in enumerate(block["open_ms"]):
        line = (
            f"{symbol}|{open_ms}|{block['close_ms'][index]}|{block['open'][index]}|{block['high'][index]}|"
            f"{block['low'][index]}|{block['close'][index]}|{block['volume'][index]}|{block['quote'][index]}|"
            f"{block['trades'][index]}|{block['taker_base'][index]}|{block['taker_quote'][index]}\n"
        )
        digest.update(line.encode("ascii"))
    return digest.hexdigest()


def block_to_table(symbol: str, block: dict, components: int, schema: pa.Schema) -> pa.Table:
    count = len(block["open_ms"])
    return pa.table(
        {
            "symbol": pa.array([symbol] * count, pa.string()),
            "open_time": pa.array(block["open_ms"], TS_UTC),
            "open": pa.array([unscaled(value) for value in block["open"]], DECIMAL_TYPE),
            "high": pa.array([unscaled(value) for value in block["high"]], DECIMAL_TYPE),
            "low": pa.array([unscaled(value) for value in block["low"]], DECIMAL_TYPE),
            "close": pa.array([unscaled(value) for value in block["close"]], DECIMAL_TYPE),
            "volume": pa.array([unscaled(value) for value in block["volume"]], DECIMAL_TYPE),
            "close_time": pa.array(block["close_ms"], TS_UTC),
            "quote_asset_volume": pa.array([unscaled(value) for value in block["quote"]], DECIMAL_TYPE),
            "number_of_trades": pa.array(block["trades"], pa.int64()),
            "taker_buy_base_asset_volume": pa.array([unscaled(value) for value in block["taker_base"]], DECIMAL_TYPE),
            "taker_buy_quote_asset_volume": pa.array([unscaled(value) for value in block["taker_quote"]], DECIMAL_TYPE),
            "source_candle_count": pa.array([components] * count, pa.int16()),
        },
        schema=schema,
    )


def read_parquet(path: Path) -> pa.Table:
    return pq.ParquetFile(path).read()


def table_to_block(path: Path) -> dict:
    table = read_parquet(path)
    for name in OUTPUT_COLUMNS:
        if table.column(name).null_count:
            raise RuntimeError(f"{path} contains nulls in {name}")
    return {
        "open_ms": [int(value) for value in table.column("open_time").cast(pa.int64()).to_pylist()],
        "close_ms": [int(value) for value in table.column("close_time").cast(pa.int64()).to_pylist()],
        "open": [scaled(value) for value in table.column("open").to_pylist()],
        "high": [scaled(value) for value in table.column("high").to_pylist()],
        "low": [scaled(value) for value in table.column("low").to_pylist()],
        "close": [scaled(value) for value in table.column("close").to_pylist()],
        "volume": [scaled(value) for value in table.column("volume").to_pylist()],
        "quote": [scaled(value) for value in table.column("quote_asset_volume").to_pylist()],
        "trades": [int(value) for value in table.column("number_of_trades").to_pylist()],
        "taker_base": [scaled(value) for value in table.column("taker_buy_base_asset_volume").to_pylist()],
        "taker_quote": [scaled(value) for value in table.column("taker_buy_quote_asset_volume").to_pylist()],
    }


def read_source_block(path: Path, symbol: str) -> tuple[dict, str]:
    table = read_parquet(path)
    metadata = table.schema.metadata or {}
    if metadata.get(b"market_type") != b"spot" or metadata.get(b"schema_version") != SOURCE_SCHEMA.encode("ascii"):
        raise RuntimeError(f"{path.name} is not {SOURCE_SCHEMA}")
    if metadata.get(b"quote_asset") != b"BTC" or metadata.get(b"symbol") != symbol.encode("ascii"):
        raise RuntimeError(f"{path.name} is not {symbol} quoted in BTC")
    symbols = set(table.column("symbol").to_pylist())
    intervals = set(table.column("interval").to_pylist())
    if symbols != {symbol} or intervals != {"1m"}:
        raise RuntimeError(f"{path.name} symbol/interval mismatch: {symbols} {intervals}")
    block = {
        "open_ms": [int(value) for value in table.column("open_time").cast(pa.int64()).to_pylist()],
        "close_ms": [int(value) for value in table.column("close_time").cast(pa.int64()).to_pylist()],
        "open": [scaled(value) for value in table.column("open").to_pylist()],
        "high": [scaled(value) for value in table.column("high").to_pylist()],
        "low": [scaled(value) for value in table.column("low").to_pylist()],
        "close": [scaled(value) for value in table.column("close").to_pylist()],
        "volume": [scaled(value) for value in table.column("volume").to_pylist()],
        "quote": [scaled(value) for value in table.column("quote_volume").to_pylist()],
        "trades": [int(value) for value in table.column("count").to_pylist()],
        "taker_base": [scaled(value) for value in table.column("taker_buy_volume").to_pylist()],
        "taker_quote": [scaled(value) for value in table.column("taker_buy_quote_volume").to_pylist()],
    }
    digest = hashlib.sha256()
    for index, open_ms in enumerate(block["open_ms"]):
        digest.update(
            f"{symbol}|{open_ms}|{block['open'][index]}|{block['high'][index]}|{block['low'][index]}|{block['close'][index]}|"
            f"{block['volume'][index]}|{block['close_ms'][index]}|{block['quote'][index]}|{block['trades'][index]}|"
            f"{block['taker_base'][index]}|{block['taker_quote'][index]}\n".encode("ascii")
        )
    return block, digest.hexdigest()


def concat_blocks(blocks: list[dict]) -> dict:
    keys = blocks[0].keys()
    return {key: [value for block in blocks for value in block[key]] for key in keys}


def write_atomic(table: pa.Table, final: Path, metadata: dict[str, str]) -> None:
    final.parent.mkdir(parents=True, exist_ok=True)
    part = final.with_name(final.name + ".part")
    if part.exists():
        part.unlink()
    extra = {key.encode("ascii"): value.encode("utf-8") for key, value in metadata.items()}
    schema = table.schema.with_metadata({**(table.schema.metadata or {}), **extra})
    pq.write_table(
        table.cast(schema), part, compression="zstd", compression_level=3, version="2.6",
        coerce_timestamps="ms", allow_truncated_timestamps=False, write_statistics=True,
    )
    os.replace(part, final)


def file_signature(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest(), stat.st_size, stat.st_mtime_ns


def critical_metadata(path: Path) -> dict[str, str]:
    metadata = pq.read_schema(path).metadata or {}
    return {key: metadata.get(key.encode("ascii"), b"").decode("utf-8") for key in CRITICAL_METADATA}


def publish_equivalent(staged: Path, final: Path, quarantine: Path) -> str:
    for candidate in (staged, final) if final.exists() else (staged,):
        if tuple(pq.read_schema(candidate).names) != OUTPUT_COLUMNS:
            raise RuntimeError(f"{candidate.name} column order is not the derived schema")
    if not final.exists():
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, final)
        return "created"
    before = file_signature(final)
    if read_parquet(final).column("source_candle_count").to_pylist() != read_parquet(staged).column("source_candle_count").to_pylist():
        raise RuntimeError(f"source candle count differs at {final}")
    same_rows, reason = blocks_equal(table_to_block(final), table_to_block(staged))
    same_meta = critical_metadata(final) == critical_metadata(staged)
    if same_rows and same_meta:
        staged.unlink()
        if file_signature(final) != before:
            raise RuntimeError(f"equivalent skip changed {final}")
        return "skipped-equivalent"
    quarantine.mkdir(parents=True, exist_ok=True)
    destination = quarantine / (str(final.name) + "." + hashlib.sha256(staged.read_bytes()).hexdigest()[:12])
    os.replace(staged, destination)
    kind = "metadata" if same_rows else "rows"
    raise RuntimeError(f"canonical conflict ({kind}) at {final}: {reason or 'critical metadata differs'}")


class DerivedLock:
    def __init__(self, path: Path):
        self.path = path
        self._held = False

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({
            "pid": os.getpid(),
            "started": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "task": TASK_NAME,
        }, indent=2)
        for _attempt in range(2):
            try:
                descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                existing = json.loads(self.path.read_text(encoding="utf-8"))
                if pid_alive(int(existing.get("pid", 0))):
                    raise RuntimeError(f"Spot derivation already running as pid {existing.get('pid')}")
                self.path.unlink()
                continue
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(payload)
            self._held = True
            return
        raise RuntimeError(f"could not acquire {self.path}")

    def release(self) -> None:
        if self._held and self.path.exists():
            self.path.unlink()
        self._held = False


def _setup_logging(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.INFO)
    log.handlers.clear()
    log.propagate = False
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    formatter.converter = time.gmtime
    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    log.addHandler(file_handler)
    log.addHandler(stream)


def source_preflight(source_root: Path) -> dict:
    from processing.canonical.build_spot_btc_1m_parquet import validate_published
    require_spot_symbols(SPOT_SYMBOLS)
    if source_root.resolve() != SOURCE_ROOT.resolve():
        raise RuntimeError("production preflight must use the approved Spot 1m root")
    result = validate_published(DATA_ROOT)
    mismatches = []
    for symbol, expected in REVIEWED_SOURCE_FINGERPRINTS.items():
        actual = result["per_symbol"][symbol]["content_fingerprint"]
        if actual != expected or actual != result["per_symbol"][symbol]["content_fingerprint_repeat"]:
            mismatches.append({"symbol": symbol, "expected": expected, "actual": actual})
    if mismatches or not result["passed"]:
        raise RuntimeError(f"canonical Spot 1m preflight failed: {mismatches or result['defects']}")
    if any(item["rows"] != EXPECTED_ROWS_PER_SYMBOL for item in result["per_symbol"].values()):
        raise RuntimeError("canonical Spot 1m row count changed")
    if result["rows_total"] != EXPECTED_ROWS_TOTAL or result["partitions_total"] != EXPECTED_PARTITIONS_PER_SYMBOL * 10:
        raise RuntimeError("canonical Spot 1m inventory changed")
    return result


def build_symbol_timeframe(source_root: Path, staging: Path, symbol: str, timeframe: str, run_id: str) -> dict:
    spec = TIMEFRAMES[timeframe]
    schema = output_schema(timeframe)
    yearly: dict[int, list[dict]] = {}
    yearly_names: dict[int, list[str]] = {}
    yearly_hashes: dict[int, list[str]] = {}
    partitions = []
    total = empty_block()
    for year, month in partition_months():
        path = source_partition(source_root, symbol, year, month)
        expected = expected_partition_minutes(year, month)
        block, source_hash = read_source_block(path, symbol)
        month_start = ms_of(datetime(year, month, 1, tzinfo=timezone.utc))
        if len(block["open_ms"]) != expected or block["open_ms"][0] != month_start:
            raise RuntimeError(f"{symbol} {year:04d}-{month:02d} has {len(block['open_ms'])} rows; expected {expected}")
        derived = aggregate_block(block, spec["components"], spec["bucket_ms"])
        _check_sums(block, derived, f"{symbol} {timeframe} {year:04d}-{month:02d}")
        logical = f"symbol={symbol}/year={year}/month={month:02d}/data.parquet"
        if spec["partition"] == "month":
            final = derived_partition(staging, timeframe, symbol, year, month)
            assert_under_spot(final, staging)
            _write_derived(final, symbol, timeframe, derived, schema, [logical], [source_hash], run_id)
            partitions.append(str(final))
        else:
            yearly.setdefault(year, []).append(derived)
            yearly_names.setdefault(year, []).append(logical)
            yearly_hashes.setdefault(year, []).append(source_hash)
        total = concat_blocks([total, derived]) if total["open_ms"] else derived
    if spec["partition"] == "year":
        for year, blocks in yearly.items():
            combined = concat_blocks(blocks)
            final = derived_partition(staging, timeframe, symbol, year)
            assert_under_spot(final, staging)
            _write_derived(final, symbol, timeframe, combined, schema, yearly_names[year], yearly_hashes[year], run_id)
            partitions.append(str(final))
    if len(total["open_ms"]) != spec["rows"]:
        raise RuntimeError(f"{symbol} {timeframe} rows {len(total['open_ms'])} != {spec['rows']}")
    if iso_of_ms(total["open_ms"][0]) != "2024-01-01T00:00:00Z" or iso_of_ms(total["open_ms"][-1]) != spec["last_open"]:
        raise RuntimeError(f"{symbol} {timeframe} coverage {iso_of_ms(total['open_ms'][0])}..{iso_of_ms(total['open_ms'][-1])}")
    if iso_close_of_ms(total["close_ms"][-1]) != "2026-10-27T23:59:59.999Z":
        raise RuntimeError(f"{symbol} {timeframe} last close {iso_close_of_ms(total['close_ms'][-1])}")
    first = fingerprint_block(symbol, total)
    second = fingerprint_block(symbol, total)
    if first != second:
        raise RuntimeError(f"{symbol} {timeframe} fingerprint changed")
    return {"symbol": symbol, "rows": len(total["open_ms"]), "fingerprint": first, "fingerprint_repeat": second, "partitions": len(partitions)}


def empty_block() -> dict:
    return {key: [] for key in ("open_ms", "close_ms", "open", "high", "low", "close", "volume", "quote", "trades", "taker_base", "taker_quote")}


def _write_derived(final: Path, symbol: str, timeframe: str, block: dict, schema: pa.Schema, names: list[str], hashes: list[str], run_id: str) -> None:
    spec = TIMEFRAMES[timeframe]
    write_atomic(block_to_table(symbol, block, spec["components"], schema), final, {
        "provider": "Binance",
        "market_type": "spot",
        "symbol": symbol,
        "base_asset": base_asset(symbol),
        "quote_asset": "BTC",
        "source_timeframe": "1m",
        "timeframe": timeframe,
        "schema_version": spec["schema"],
        "coverage_start": iso_of_ms(block["open_ms"][0]),
        "coverage_end": iso_close_of_ms(block["close_ms"][-1]),
        "exclusive_end": iso_of_ms(block["close_ms"][-1] + 1),
        "source_partitions": ",".join(names),
        "source_fingerprints": ",".join(hashes),
        "source_schema": SOURCE_SCHEMA,
        "builder_version": BUILDER_VERSION,
        "publication_run_id": run_id,
        "aggregation_rules": AGGREGATION_RULES,
        "expected_component_count": str(spec["components"]),
    })


def verify_timeframe(source_root: Path, staging: Path, timeframe: str) -> dict[str, str]:
    """Re-aggregate every source month and compare it with the staged or published rows."""
    spec = TIMEFRAMES[timeframe]
    fingerprints = {}
    for symbol in SPOT_SYMBOLS:
        rebuilt = empty_block()
        stored_all = empty_block()
        source_sums = {key: 0 for key in SUM_KEYS}
        derived_sums = {key: 0 for key in SUM_KEYS}
        year_source = {year: {key: 0 for key in SUM_KEYS} for year in (2024, 2025, 2026)}
        year_derived = {year: {key: 0 for key in SUM_KEYS} for year in (2024, 2025, 2026)}
        for year, month in partition_months():
            path = source_partition(source_root, symbol, year, month)
            block, _hash = read_source_block(path, symbol)
            derived = aggregate_block(block, spec["components"], spec["bucket_ms"])
            expected_buckets = expected_partition_minutes(year, month) // spec["components"]
            month_start = ms_of(datetime(year, month, 1, tzinfo=timezone.utc))
            if len(derived["open_ms"]) != expected_buckets or derived["open_ms"][0] != month_start:
                raise RuntimeError(f"{symbol} {timeframe} {year:04d}-{month:02d} bucket bounds differ")
            if derived["open_ms"][-1] != month_start + (expected_buckets - 1) * spec["bucket_ms"]:
                raise RuntimeError(f"{symbol} {timeframe} {year:04d}-{month:02d} last bucket differs")
            _check_sums(block, derived, f"{symbol} {timeframe} {year:04d}-{month:02d}")
            for key in SUM_KEYS:
                source_sums[key] += sum(block[key])
                derived_sums[key] += sum(derived[key])
                year_source[year][key] += sum(block[key])
                year_derived[year][key] += sum(derived[key])
            if spec["partition"] == "month":
                final = derived_partition(staging, timeframe, symbol, year, month)
                _audit_partition(final, timeframe, symbol)
                stored = table_to_block(final)
                same, reason = blocks_equal(derived, stored)
                if not same:
                    raise RuntimeError(f"{symbol} {timeframe} independent 1m cross-check failed: {reason}")
                stored_all = concat_blocks([stored_all, stored]) if stored_all["open_ms"] else stored
            rebuilt = concat_blocks([rebuilt, derived]) if rebuilt["open_ms"] else derived
        if spec["partition"] == "year":
            stored_all = empty_block()
            for year in (2024, 2025, 2026):
                final = derived_partition(staging, timeframe, symbol, year)
                _audit_partition(final, timeframe, symbol)
                part = table_to_block(final)
                expected_days = _expected_year_days(year)
                if len(part["open_ms"]) != expected_days or iso_of_ms(part["open_ms"][0]) != f"{year:04d}-01-01T00:00:00Z":
                    raise RuntimeError(f"{symbol} {timeframe} {year} yearly partition bounds differ")
                stored_all = concat_blocks([stored_all, part]) if stored_all["open_ms"] else part
            same, reason = blocks_equal(rebuilt, stored_all)
            if not same:
                raise RuntimeError(f"{symbol} {timeframe} independent 1m cross-check failed: {reason}")
        for year in (2024, 2025, 2026):
            _assert_sum_maps(year_source[year], year_derived[year], f"{symbol} {timeframe} {year}")
        _assert_sum_maps(source_sums, derived_sums, f"{symbol} {timeframe} full-window")
        _assert_shape(symbol, timeframe, stored_all)
        _check_october(symbol, timeframe, staging)
        first = fingerprint_block(symbol, stored_all)
        second = fingerprint_block(symbol, stored_all)
        if first != second:
            raise RuntimeError(f"{symbol} {timeframe} stored fingerprint changed")
        fingerprints[symbol] = first
    return fingerprints


SUM_KEYS = ("volume", "quote", "trades", "taker_base", "taker_quote")
BOUNDARY_OPENS = (
    "2024-02-29T00:00:00Z",
    "2024-12-31T00:00:00Z",
    "2025-01-01T00:00:00Z",
    "2025-12-31T00:00:00Z",
    "2026-01-01T00:00:00Z",
    "2026-10-01T00:00:00Z",
    "2026-10-27T00:00:00Z",
)


def _check_sums(source: dict, derived: dict, label: str) -> None:
    for key in SUM_KEYS:
        if sum(source[key]) != sum(derived[key]):
            raise RuntimeError(f"conservation failed for {label} {key}")


def _assert_sum_maps(source: dict, derived: dict, label: str) -> None:
    for key in SUM_KEYS:
        if source[key] != derived[key]:
            raise RuntimeError(f"conservation failed for {label} {key}")


def _expected_year_days(year: int) -> int:
    if year == 2026:
        return sum(days_in_month(2026, month) for month in range(1, 10)) + 27
    return sum(days_in_month(year, month) for month in range(1, 13))


def _audit_partition(path: Path, timeframe: str, symbol: str) -> None:
    spec = TIMEFRAMES[timeframe]
    schema = pq.read_schema(path)
    if tuple(schema.names) != OUTPUT_COLUMNS:
        raise RuntimeError(f"{path} column order differs")
    metadata = schema.metadata or {}
    expected = {
        b"provider": b"Binance",
        b"market_type": b"spot",
        b"symbol": symbol.encode("ascii"),
        b"base_asset": base_asset(symbol).encode("ascii"),
        b"quote_asset": b"BTC",
        b"source_timeframe": b"1m",
        b"timeframe": timeframe.encode("ascii"),
        b"schema_version": spec["schema"].encode("ascii"),
        b"source_schema": SOURCE_SCHEMA.encode("ascii"),
        b"builder_version": BUILDER_VERSION.encode("ascii"),
        b"aggregation_rules": AGGREGATION_RULES.encode("ascii"),
        b"expected_component_count": str(spec["components"]).encode("ascii"),
    }
    for key, value in expected.items():
        if metadata.get(key) != value:
            raise RuntimeError(f"{path} metadata {key.decode()} differs")
    source_names = metadata.get(b"source_partitions", b"").decode("utf-8")
    if f"symbol={symbol}/" not in source_names:
        raise RuntimeError(f"{path} does not cite its own source partitions")
    for other in SPOT_SYMBOLS:
        if other != symbol and f"symbol={other}" in source_names:
            raise RuntimeError(f"{path} cites {other}")
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_row_groups < 1 or "ZSTD" not in str(parquet.metadata.row_group(0).column(0).compression).upper():
        raise RuntimeError(f"{path} is not ZSTD")


def _assert_shape(symbol: str, timeframe: str, block: dict) -> None:
    spec = TIMEFRAMES[timeframe]
    opens = block["open_ms"]
    bucket = spec["bucket_ms"]
    if len(opens) != spec["rows"] or len(set(opens)) != len(opens):
        raise RuntimeError(f"{symbol} {timeframe} row or uniqueness check failed")
    if opens[0] != RANGE_START_MS or opens[0] % bucket:
        raise RuntimeError(f"{symbol} {timeframe} does not start on the UTC boundary")
    for previous, current in zip(opens, opens[1:]):
        if current - previous != bucket or current % bucket:
            raise RuntimeError(f"{symbol} {timeframe} spacing broken at {iso_of_ms(current)}")
        if current >= RANGE_EXCLUSIVE_END_MS:
            raise RuntimeError(f"{symbol} {timeframe} contains a bucket on or after the exclusive end")
    if iso_of_ms(opens[-1]) != spec["last_open"] or iso_close_of_ms(block["close_ms"][-1]) != "2026-10-27T23:59:59.999Z":
        raise RuntimeError(f"{symbol} {timeframe} final boundary differs")
    if block["close_ms"][-1] + 1 != RANGE_EXCLUSIVE_END_MS:
        raise RuntimeError(f"{symbol} {timeframe} exclusive end differs")
    present = set(opens)
    for stamp in BOUNDARY_OPENS:
        instant = ms_of(datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc))
        if instant not in present:
            raise RuntimeError(f"{symbol} {timeframe} is missing {stamp}")
    if timeframe == "4h":
        hours = {datetime.fromtimestamp(value / 1000, timezone.utc).hour for value in opens}
        if hours != {0, 4, 8, 12, 16, 20}:
            raise RuntimeError(f"{symbol} 4h bucket hours are {sorted(hours)}")
    for index, open_ms in enumerate(opens):
        if block["close_ms"][index] != open_ms + bucket - 1:
            raise RuntimeError(f"{symbol} {timeframe} close differs at {iso_of_ms(open_ms)}")
        prices = (block["open"][index], block["high"][index], block["low"][index], block["close"][index])
        if min(prices) <= 0 or block["high"][index] < max(prices) or block["low"][index] > min(prices):
            raise RuntimeError(f"{symbol} {timeframe} OHLC violation at {iso_of_ms(open_ms)}")
        amounts = (block["volume"][index], block["quote"][index], block["taker_base"][index], block["taker_quote"][index], block["trades"][index])
        if min(amounts) < 0:
            raise RuntimeError(f"{symbol} {timeframe} negative amount at {iso_of_ms(open_ms)}")


def _check_october(symbol: str, timeframe: str, root: Path) -> None:
    spec = TIMEFRAMES[timeframe]
    if spec["partition"] == "month":
        block = table_to_block(derived_partition(root, timeframe, symbol, 2026, 10))
    else:
        block = table_to_block(derived_partition(root, timeframe, symbol, 2026))
        start = ms_of(datetime(2026, 10, 1, tzinfo=timezone.utc))
        block = {key: [value for index, value in enumerate(values) if block["open_ms"][index] >= start] for key, values in block.items()}
    if len(block["open_ms"]) != spec["october"]:
        raise RuntimeError(f"{symbol} {timeframe} October 2026 rows {len(block['open_ms'])} != {spec['october']}")
    if iso_of_ms(block["open_ms"][-1]) != spec["last_open"]:
        raise RuntimeError(f"{symbol} {timeframe} October last open mismatch")
    if any(open_ms >= RANGE_EXCLUSIVE_END_MS for open_ms in block["open_ms"]):
        raise RuntimeError(f"{symbol} {timeframe} contains 28 October")


def hierarchical_checks(root: Path, ready: tuple[str, ...]) -> None:
    if root.name != "spot" or "futures" in {part.lower() for part in root.parts}:
        raise RuntimeError(f"hierarchical checks require the Spot derived root, got {root}")
    available = set(ready)
    cache: dict[tuple[str, str], dict] = {}

    def load(timeframe: str, symbol: str) -> dict:
        key = (timeframe, symbol)
        if key not in cache:
            cache[key] = _load_timeframe(root, timeframe, symbol)
        return cache[key]

    for child, parent, ratio in HIERARCHY:
        if child not in available or parent not in available:
            continue
        for symbol in SPOT_SYMBOLS:
            folded = fold_block(load(child, symbol), ratio)
            same, reason = blocks_equal(folded, load(parent, symbol))
            if not same:
                raise RuntimeError(f"{symbol} {child}->{parent} hierarchical check failed: {reason}")


def _load_timeframe(root: Path, timeframe: str, symbol: str) -> dict:
    blocks = []
    if TIMEFRAMES[timeframe]["partition"] == "year":
        for year in (2024, 2025, 2026):
            blocks.append(table_to_block(derived_partition(root, timeframe, symbol, year)))
    else:
        for year, month in partition_months():
            blocks.append(table_to_block(derived_partition(root, timeframe, symbol, year, month)))
    return concat_blocks(blocks)


def publish_tree(staging_spot: Path, spot_root: Path, quarantine: Path) -> dict:
    created = skipped = 0
    for staged in sorted(staging_spot.rglob("data.parquet")):
        relative = staged.relative_to(staging_spot)
        final = spot_root / relative
        assert_under_spot(final, spot_root)
        status = publish_equivalent(staged, final, quarantine)
        if status == "created":
            created += 1
        else:
            skipped += 1
    return {"created": created, "skipped_equivalent": skipped}


def run_all(data_root: Path = DATA_ROOT, source_root: Path = SOURCE_ROOT) -> int:
    require_spot_symbols(SPOT_SYMBOLS)
    if source_root.resolve() != SOURCE_ROOT.resolve():
        raise RuntimeError("refusing a non-approved Spot source root")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lock = DerivedLock(data_root / "tmp" / "locks" / f"{TASK_NAME}.lock")
    work = data_root / "tmp" / f"{TASK_NAME}_{run_id}"
    free = shutil.disk_usage(data_root).free
    if free < DISK_MARGIN_BYTES:
        raise RuntimeError(f"insufficient disk space: {free}")
    try:
        lock.acquire()
        preflight = source_preflight(source_root)
        log_path = data_root / "logs" / f"{TASK_NAME}.log"
        _setup_logging(log_path)
        source_bytes = sum(path.stat().st_size for path in source_root.rglob("data.parquet"))
        if free < source_bytes + DISK_MARGIN_BYTES:
            raise RuntimeError(f"insufficient disk space: free={free} source={source_bytes} margin={DISK_MARGIN_BYTES}")
        log.info(
            "source preflight passed rows=%s partitions=%s free=%s source_bytes=%s",
            preflight["rows_total"], preflight["partitions_total"], free, source_bytes,
        )
        ready: list[str] = []
        results = {}
        spot_root = data_root / "derived" / "binance" / "spot"
        for timeframe in TIMEFRAME_ORDER:
            staging = work / "staged" / "derived" / "binance" / "spot"
            _setup_logging(data_root / "logs" / f"binance_spot_btc_pairs_{timeframe}_build.log")
            built = []
            for symbol in SPOT_SYMBOLS:
                built.append(build_symbol_timeframe(source_root, staging, symbol, timeframe, run_id))
                log.info("staged %s %s rows=%s fingerprint=%s", symbol, timeframe, built[-1]["rows"], built[-1]["fingerprint"])
            verified = verify_timeframe(source_root, staging, timeframe)
            for item in built:
                if verified[item["symbol"]] != item["fingerprint"]:
                    raise RuntimeError(f"{item['symbol']} {timeframe} stored fingerprint differs from the staged aggregation")
            publication = publish_tree(staging, spot_root, work / "quarantine")
            expected_files = len(SPOT_SYMBOLS) * (3 if TIMEFRAMES[timeframe]["partition"] == "year" else EXPECTED_PARTITIONS_PER_SYMBOL)
            pattern = "symbol=*/year=*/data.parquet" if TIMEFRAMES[timeframe]["partition"] == "year" else "symbol=*/year=*/month=*/data.parquet"
            found = list((spot_root / timeframe).glob(pattern))
            if len(found) != expected_files or publication["created"] + publication["skipped_equivalent"] != expected_files:
                raise RuntimeError(
                    f"{timeframe} file gate failed: found={len(found)} created={publication['created']} "
                    f"skipped={publication['skipped_equivalent']} expected={expected_files}"
                )
            ready.append(timeframe)
            hierarchical_checks(spot_root, tuple(ready))
            results[timeframe] = {"built": built, "publication": publication}
            _write_timeframe_report(data_root, timeframe, run_id, built, publication)
            log.info("published %s created=%s skipped=%s files=%s", timeframe, publication["created"], publication["skipped_equivalent"], len(found))
        _write_validation_report(data_root, run_id, results, preflight, free, source_bytes)
        return 0
    finally:
        lock.release()
        if work.exists():
            shutil.rmtree(work, ignore_errors=True)


def _write_timeframe_report(data_root: Path, timeframe: str, run_id: str, built: list[dict], publication: dict) -> None:
    path = data_root / "reports" / f"binance_spot_btc_pairs_{timeframe}_build_report.json"
    previous = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"runs": []}
    runs = list(previous.get("runs") or [])
    runs.append({"run_id": run_id, "publication": publication})
    report = {
        "task": TASK_NAME,
        "market_type": "spot",
        "quote_asset": "BTC",
        "timeframe": timeframe,
        "symbols": list(SPOT_SYMBOLS),
        "excluded_symbols": list(EXCLUDED_SYMBOLS),
        "source_root": "processed/binance/spot/1m",
        "schema_version": TIMEFRAMES[timeframe]["schema"],
        "current_run": {"run_id": run_id, "publication": publication},
        "fingerprints": {item["symbol"]: item["fingerprint"] for item in built},
        "rows_per_symbol": {item["symbol"]: item["rows"] for item in built},
        "runs": runs,
    }
    _atomic_json(path, report)


def _write_validation_report(data_root: Path, run_id: str, results: dict, preflight: dict, free_bytes: int, source_bytes: int) -> None:
    path = data_root / "reports" / "binance_spot_btc_pairs_derived_validation_report.json"
    previous = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"runs": []}
    fingerprints = {
        f"{symbol}:{timeframe}": item["fingerprint"]
        for timeframe, payload in results.items()
        for item in payload["built"]
        for symbol in [item["symbol"]]
    }
    runs = list(previous.get("runs") or [])
    publication = {timeframe: payload["publication"] for timeframe, payload in results.items()}
    runs.append({"run_id": run_id, "publication": publication})
    report = {
        "task": TASK_NAME,
        "market_type": "spot",
        "quote_asset": "BTC",
        "symbols": list(SPOT_SYMBOLS),
        "excluded_symbols": list(EXCLUDED_SYMBOLS),
        "run_id": run_id,
        "mode": "DERIVED_VALIDATION",
        "source_rows_total": preflight["rows_total"],
        "source_partitions_total": preflight["partitions_total"],
        "source_fingerprints_matched": True,
        "free_bytes_before": free_bytes,
        "source_bytes": source_bytes,
        "timeframes": publication,
        "fingerprints": fingerprints,
        "downloaded_files": 0,
        "runs": runs,
    }
    _atomic_json(path, report)


def _atomic_json(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(path.suffix + ".part")
    part.write_text(json.dumps(report, indent=2), encoding="utf-8")
    os.replace(part, path)


def validate_only(data_root: Path = DATA_ROOT, source_root: Path = SOURCE_ROOT) -> dict:
    """Read derived Spot partitions. This function does not write."""
    require_spot_symbols(SPOT_SYMBOLS)
    derived_root = data_root / "derived" / "binance" / "spot"
    fingerprints = {}
    rows = {timeframe: TIMEFRAMES[timeframe]["rows"] * len(SPOT_SYMBOLS) for timeframe in TIMEFRAME_ORDER}
    partitions = {}
    for timeframe in TIMEFRAME_ORDER:
        verified = verify_timeframe(source_root, derived_root, timeframe)
        expected_files = len(SPOT_SYMBOLS) * (3 if TIMEFRAMES[timeframe]["partition"] == "year" else EXPECTED_PARTITIONS_PER_SYMBOL)
        pattern = "symbol=*/year=*/data.parquet" if TIMEFRAMES[timeframe]["partition"] == "year" else "symbol=*/year=*/month=*/data.parquet"
        found = list((derived_root / timeframe).glob(pattern))
        if len(found) != expected_files or len(verified) != len(SPOT_SYMBOLS):
            raise RuntimeError(f"{timeframe} read-only file gate failed: found={len(found)} expected={expected_files}")
        partitions[timeframe] = len(found)
        for symbol, digest in verified.items():
            fingerprints[f"{symbol}:{timeframe}"] = digest
    hierarchical_checks(derived_root, TIMEFRAME_ORDER)
    total_rows = sum(rows.values())
    total_partitions = sum(partitions.values())
    if total_rows != 4_773_530 or total_partitions != 1_730 or len(fingerprints) != 60:
        raise RuntimeError(f"read-only totals differ: rows={total_rows} partitions={total_partitions} fingerprints={len(fingerprints)}")
    report_path = data_root / "reports" / "binance_spot_btc_pairs_derived_validation_report.json"
    if report_path.exists():
        recorded = json.loads(report_path.read_text(encoding="utf-8")).get("fingerprints", {})
        if recorded != fingerprints:
            raise RuntimeError("read-only fingerprints differ from the publication report")
    return {
        "passed": True,
        "rows": rows,
        "rows_total": total_rows,
        "partitions": partitions,
        "partitions_total": total_partitions,
        "fingerprints": fingerprints,
        "downloaded_files": 0,
    }


def main() -> int:
    args = sys.argv[1:]
    if args == ["--validate-only"]:
        result = validate_only()
        json.dump({"passed": result["passed"], "rows_total": result["rows_total"], "partitions_total": result["partitions_total"]}, sys.stdout)
        sys.stdout.write("\n")
        return 0 if result["passed"] else 1
    if args == ["--all"]:
        return run_all()
    raise SystemExit("unsupported arguments")


if __name__ == "__main__":
    sys.exit(main())
