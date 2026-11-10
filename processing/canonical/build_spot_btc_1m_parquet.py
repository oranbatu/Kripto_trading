#!/usr/bin/env python3
"""Publish canonical Spot 1-minute Parquet for the ten approved BTC pairs.

The builder reads only verified Spot ZIPs. It never downloads, never fills a
missing minute, and never writes a futures, derived, detector, or workbook path.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import shutil
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ingestion.binance.spot_btc_pairs import (
    DECIMAL_SCALE,
    EXPECTED_PARTITIONS_PER_SYMBOL,
    EXPECTED_ROWS_2024,
    EXPECTED_ROWS_2025,
    EXPECTED_ROWS_2026,
    EXPECTED_ROWS_OCTOBER_2026,
    EXPECTED_ROWS_PER_SYMBOL,
    EXPECTED_ROWS_TOTAL,
    INTERVAL,
    RANGE_EXCLUSIVE_END_MS,
    RANGE_LAST_OPEN_MS,
    RANGE_START_MS,
    SCHEMA_VERSION,
    SPOT_SYMBOLS,
    TASK_NAME,
    Archive,
    TaskLock,
    assert_under,
    base_asset,
    expected_partition_minutes,
    file_signature,
    iso_close_of_ms,
    iso_of_ms,
    iter_csv_rows,
    ms_of,
    parse_checksum_bytes,
    partition_months,
    plan_archives,
    require_spot_symbols,
    sha256_file,
)

DATA_ROOT = Path(r"C:\MarketData")
REPORT_PATH = DATA_ROOT / "reports" / "binance_spot_btc_pairs_1m_canonical_report.json"
LOG_PATH = DATA_ROOT / "logs" / "binance_spot_btc_pairs_1m_canonical.log"
DECIMAL_TYPE = pa.decimal128(38, DECIMAL_SCALE)
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
        b"schema_version": SCHEMA_VERSION.encode("ascii"),
        b"provider": b"Binance",
        b"market_type": b"spot",
        b"quote_asset": b"BTC",
        b"interval": b"1m",
    },
)

log = logging.getLogger("binance_spot_btc_canonical")


def hive_path(root: Path, symbol: str, year: int, month: int) -> Path:
    return root / "processed" / "binance" / "spot" / "1m" / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}" / "data.parquet"


def _setup_logging(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.INFO)
    log.handlers.clear()
    log.propagate = False
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    log.addHandler(file_handler)
    log.addHandler(stream)


def _partition_bounds(year: int, month: int) -> tuple[int, int]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if (year, month) == (2026, 10):
        return ms_of(start), RANGE_LAST_OPEN_MS
    nxt = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return ms_of(start), ms_of(nxt) - 60_000


def _sources_for(archives: list[Archive], symbol: str, year: int, month: int) -> list[Archive]:
    if (year, month) == (2026, 10):
        return [item for item in archives if item.symbol == symbol and item.kind == "daily"]
    period = f"{year:04d}-{month:02d}"
    return [item for item in archives if item.symbol == symbol and item.kind == "monthly" and item.period == period]


def _read_archive_rows(root: Path, archive: Archive) -> list[dict]:
    zip_path = archive.zip_path(root)
    checksum_path = archive.checksum_path(root)
    digest = parse_checksum_bytes(checksum_path.read_bytes(), archive.zip_name)
    if sha256_file(zip_path) != digest:
        raise RuntimeError(f"raw checksum mismatch for {archive.zip_name}")
    with zipfile.ZipFile(zip_path) as handle:
        names = handle.namelist()
        if names != [archive.csv_name]:
            raise RuntimeError(f"unexpected members in {archive.zip_name}: {names!r}")
        with handle.open(archive.csv_name) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            return list(iter_csv_rows(text, archive.timestamp_unit))


def _payload(symbol: str, row: dict) -> bytes:
    scale = 10 ** DECIMAL_SCALE
    values = [
        symbol,
        str(row["open_ms"]),
        str(int(row["open"] * scale)),
        str(int(row["high"] * scale)),
        str(int(row["low"] * scale)),
        str(int(row["close"] * scale)),
        str(int(row["volume"] * scale)),
        str(row["close_ms"]),
        str(int(row["quote_volume"] * scale)),
        str(row["count"]),
        str(int(row["taker_buy_volume"] * scale)),
        str(int(row["taker_buy_quote_volume"] * scale)),
        str(row["ignore"]),
    ]
    return ("|".join(values) + "\n").encode("ascii")


def content_fingerprint(symbol: str, rows: list[dict]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(_payload(symbol, row))
    return digest.hexdigest()


def rows_to_table(symbol: str, rows: list[dict]) -> pa.Table:
    count = len(rows)
    def column(name: str):
        return [row[name] for row in rows]
    arrays = {
        "symbol": pa.array([symbol] * count, pa.string()).dictionary_encode(),
        "interval": pa.array([INTERVAL] * count, pa.string()).dictionary_encode(),
        "open_time": pa.array(column("open_ms"), type=pa.timestamp("ms", tz="UTC")),
        "open": pa.array(column("open"), type=DECIMAL_TYPE),
        "high": pa.array(column("high"), type=DECIMAL_TYPE),
        "low": pa.array(column("low"), type=DECIMAL_TYPE),
        "close": pa.array(column("close"), type=DECIMAL_TYPE),
        "volume": pa.array(column("volume"), type=DECIMAL_TYPE),
        "close_time": pa.array(column("close_ms"), type=pa.timestamp("ms", tz="UTC")),
        "quote_volume": pa.array(column("quote_volume"), type=DECIMAL_TYPE),
        "count": pa.array(column("count"), type=pa.int64()),
        "taker_buy_volume": pa.array(column("taker_buy_volume"), type=DECIMAL_TYPE),
        "taker_buy_quote_volume": pa.array(column("taker_buy_quote_volume"), type=DECIMAL_TYPE),
        "ignore": pa.array(column("ignore"), type=pa.int64()),
    }
    return pa.table(arrays, schema=CANONICAL_SCHEMA)


def write_parquet_atomic(table: pa.Table, final: Path, metadata: dict[str, str]) -> None:
    final.parent.mkdir(parents=True, exist_ok=True)
    part = final.with_name(final.name + ".part")
    if part.exists():
        part.unlink()
    extra = {key.encode("ascii"): value.encode("utf-8") for key, value in metadata.items()}
    schema = table.schema.with_metadata({**(table.schema.metadata or {}), **extra})
    pq.write_table(
        table.cast(schema),
        part,
        compression="zstd",
        compression_level=3,
        version="2.6",
        coerce_timestamps="ms",
        allow_truncated_timestamps=False,
        write_statistics=True,
        use_dictionary=["symbol", "interval"],
    )
    os.replace(part, final)


def _validate_month(symbol: str, rows: list[dict], year: int, month: int, units: set[str]) -> None:
    start_ms, end_ms = _partition_bounds(year, month)
    if any(row["open_ms"] < start_ms or row["open_ms"] > end_ms or row["open_ms"] >= RANGE_EXCLUSIVE_END_MS for row in rows):
        raise RuntimeError(f"{symbol} {year:04d}-{month:02d} contains an out-of-window row")
    opens = [row["open_ms"] for row in rows]
    if opens != sorted(opens):
        raise RuntimeError(f"{symbol} {year:04d}-{month:02d} is not strictly ordered")
    if len(opens) != len(set(opens)):
        raise RuntimeError(f"{symbol} {year:04d}-{month:02d} contains duplicate open times")
    expected = list(range(start_ms, end_ms + 1, 60_000))
    if opens != expected:
        missing = [iso_of_ms(stamp) for stamp in expected if stamp not in set(opens)]
        raise RuntimeError(
            f"{symbol} {year:04d}-{month:02d} missing {len(missing)} minute(s); first={missing[:3]}"
        )
    if any(row["timestamp_unit"] not in ("milliseconds", "microseconds") for row in rows):
        raise RuntimeError(f"{symbol} {year:04d}-{month:02d} has an unknown timestamp unit")
    units.update(row["timestamp_unit"] for row in rows)


def _update_fingerprint_from_parquet(digest: "hashlib._Hash", symbol: str, path: Path) -> None:
    table = pq.read_table(path)
    opens = table.column("open_time").cast(pa.int64()).to_pylist()
    closes = table.column("close_time").cast(pa.int64()).to_pylist()
    columns = {
        name: table.column(name).to_pylist()
        for name in (
            "open", "high", "low", "close", "volume", "quote_volume", "count",
            "taker_buy_volume", "taker_buy_quote_volume", "ignore",
        )
    }
    for index, open_ms in enumerate(opens):
        row = {
            "open_ms": int(open_ms),
            "close_ms": int(closes[index]),
            "open": Decimal(columns["open"][index]),
            "high": Decimal(columns["high"][index]),
            "low": Decimal(columns["low"][index]),
            "close": Decimal(columns["close"][index]),
            "volume": Decimal(columns["volume"][index]),
            "quote_volume": Decimal(columns["quote_volume"][index]),
            "count": int(columns["count"][index]),
            "taker_buy_volume": Decimal(columns["taker_buy_volume"][index]),
            "taker_buy_quote_volume": Decimal(columns["taker_buy_quote_volume"][index]),
            "ignore": int(columns["ignore"][index]),
        }
        digest.update(_payload(symbol, row))


def _update_fingerprint(digest: "hashlib._Hash", symbol: str, rows: list[dict]) -> None:
    for row in rows:
        digest.update(_payload(symbol, row))


def _build_symbol(root: Path, staging: Path, symbol: str, archives: list[Archive], run_id: str) -> dict:
    units: set[str] = set()
    partitions = []
    first_digest = hashlib.sha256()
    row_count = 0
    for year, month in partition_months():
        sources = _sources_for(archives, symbol, year, month)
        if (year, month) == (2026, 10):
            if len(sources) != 27 or any(item.kind != "daily" for item in sources):
                raise RuntimeError(f"{symbol} October 2026 does not use exactly 27 daily archives")
        elif len(sources) != 1:
            raise RuntimeError(f"{symbol} {year:04d}-{month:02d} does not map to one monthly archive")
        rows = []
        names = []
        digests = []
        for source in sources:
            rows.extend(_read_archive_rows(root, source))
            names.append(source.zip_name)
            digests.append(sha256_file(source.zip_path(root)))
        rows.sort(key=lambda item: item["open_ms"])
        _validate_month(symbol, rows, year, month, units)
        _update_fingerprint(first_digest, symbol, rows)
        row_count += len(rows)
        table = rows_to_table(symbol, rows)
        final = hive_path(staging, symbol, year, month)
        assert_under(final, staging.resolve())
        write_parquet_atomic(table, final, {
            "provider": "Binance",
            "market_type": "spot",
            "base_asset": base_asset(symbol),
            "quote_asset": "BTC",
            "symbol": symbol,
            "interval": "1m",
            "schema_version": SCHEMA_VERSION,
            "coverage_start": iso_of_ms(_partition_bounds(year, month)[0]),
            "coverage_end": iso_of_ms(_partition_bounds(year, month)[1]),
            "source_archives": ",".join(names),
            "source_sha256": ",".join(digests),
            "publication_run_id": run_id,
            "timestamp_units": ",".join(sorted({row["timestamp_unit"] for row in rows})),
        })
        partitions.append({"symbol": symbol, "year": year, "month": month, "rows": len(rows)})
        log.info("staged %s %04d-%02d rows=%d", symbol, year, month, len(rows))
    second_digest = hashlib.sha256()
    for year, month in partition_months():
        _update_fingerprint_from_parquet(second_digest, symbol, hive_path(staging, symbol, year, month))
    first = first_digest.hexdigest()
    second = second_digest.hexdigest()
    if first != second:
        raise RuntimeError(f"fingerprint mismatch for {symbol}")
    return {
        "symbol": symbol,
        "rows": row_count,
        "partitions": partitions,
        "timestamp_units": sorted(units),
        "content_fingerprint": first,
        "content_fingerprint_repeat": second,
    }


def _payload_sha(path: Path) -> str:
    symbol = next(part.split("=", 1)[1] for part in path.parts if part.startswith("symbol="))
    digest = hashlib.sha256()
    _update_fingerprint_from_parquet(digest, symbol, path)
    return digest.hexdigest()


def _publish_staged(staging: Path, root: Path, quarantine: Path) -> dict:
    created = skipped = 0
    staged_files = sorted(staging.rglob("data.parquet"))
    for staged in staged_files:
        relative = staged.relative_to(staging)
        final = root / relative
        assert_under(final, (root / "processed" / "binance" / "spot").resolve())
        if final.exists():
            before = file_signature(final)
            try:
                equivalent = _payload_sha(final) == _payload_sha(staged)
            except (OSError, ValueError, pa.ArrowException):
                equivalent = False
            if not equivalent:
                quarantine.mkdir(parents=True, exist_ok=True)
                destination = quarantine / (relative.as_posix().replace("/", "_") + "." + sha256_file(staged)[:12])
                os.replace(staged, destination)
                raise RuntimeError(f"canonical conflict at {final}")
            staged.unlink()
            after = file_signature(final)
            if after != before:
                raise RuntimeError(f"equivalent canonical skip changed {final}")
            skipped += 1
            continue
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, final)
        created += 1
    return {"created_partitions": created, "skipped_equivalent_partitions": skipped, "staged_partitions": len(staged_files)}


def run(root: Path = DATA_ROOT, report_path: Path = REPORT_PATH, log_path: Path = LOG_PATH) -> int:
    require_spot_symbols(SPOT_SYMBOLS)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    _setup_logging(log_path)
    lock = TaskLock(root / "tmp" / "locks" / f"{TASK_NAME}.lock")
    work = root / "tmp" / f"{TASK_NAME}_canonical_{run_id}"
    staging = work / "staged"
    quarantine = root / "tmp" / f"{TASK_NAME}_canonical_quarantine_{run_id}"
    archives = plan_archives()
    current = {"run_id": run_id, "mode": "CANONICAL_PUBLISH", "symbols": list(SPOT_SYMBOLS)}
    try:
        lock.acquire()
        built = []
        for symbol in SPOT_SYMBOLS:
            built.append(_build_symbol(root, staging, symbol, archives, run_id))
        _assert_inventory(built)
        staged_validation = validate_published(staging)
        if not staged_validation["passed"]:
            current["validation"] = staged_validation
            raise RuntimeError("staged Spot canonical validation failed; production partitions were not published")
        publication = _publish_staged(staging, root, quarantine)
        validation = validate_published(root)
        if not validation["passed"]:
            current["validation"] = validation
            raise RuntimeError("published Spot canonical validation failed")
        current.update({
            "finished": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "publication": publication,
            "symbols_built": built,
            "validation": validation,
        })
        _store(report_path, current)
        log.info("canonical publish created=%s skipped=%s", publication["created_partitions"], publication["skipped_equivalent_partitions"])
        return 0
    except Exception as exc:
        current["mode"] = "FAILED"
        current["error"] = str(exc)
        log.error("canonical build failed: %s", exc)
        _store(report_path, current)
        return 1
    finally:
        lock.release()
        if work.exists():
            shutil.rmtree(work, ignore_errors=True)
        if current.get("mode") != "FAILED" and quarantine.exists():
            shutil.rmtree(quarantine, ignore_errors=True)


def _assert_inventory(built: list[dict]) -> None:
    if len(built) != 10:
        raise RuntimeError("canonical build did not cover ten symbols")
    total = 0
    for item in built:
        if item["rows"] != EXPECTED_ROWS_PER_SYMBOL or len(item["partitions"]) != EXPECTED_PARTITIONS_PER_SYMBOL:
            raise RuntimeError(f"{item['symbol']} inventory {item['rows']} rows / {len(item['partitions'])} partitions")
        if item["content_fingerprint"] != item["content_fingerprint_repeat"]:
            raise RuntimeError(f"{item['symbol']} fingerprint changed between calculations")
        total += item["rows"]
    if total != EXPECTED_ROWS_TOTAL:
        raise RuntimeError(f"combined rows {total} != {EXPECTED_ROWS_TOTAL}")


def _reject_nonspec_schema(path: Path) -> None:
    schema = pq.read_schema(path)
    metadata = schema.metadata or {}
    if metadata.get(b"market_type") != b"spot" or metadata.get(b"schema_version") != SCHEMA_VERSION.encode("ascii"):
        raise RuntimeError(f"{path.name} is not {SCHEMA_VERSION}")
    if b"USD-M" in metadata.get(b"market", b"") or b"perpetual" in metadata.get(b"market_type", b""):
        raise RuntimeError(f"{path.name} claims to be perpetual futures data")
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_row_groups:
        compression = str(parquet.metadata.row_group(0).column(0).compression).upper()
        if "ZSTD" not in compression:
            raise RuntimeError(f"{path.name} compression is {compression}")


def _violation_count(mask) -> int:
    value = pc.sum(pc.cast(mask, pa.int64())).as_py()
    return 0 if value is None else int(value)


def _accumulate_quality(path: Path, defects: dict) -> None:
    table = pq.read_table(path)
    for name in table.column_names:
        defects["nulls"] += table.column(name).null_count
    zero = pa.scalar(Decimal(0), DECIMAL_TYPE)
    defects["negative_prices"] += sum(
        _violation_count(pc.less_equal(table.column(name), zero))
        for name in ("open", "high", "low", "close")
    )
    defects["ohlc_violations"] += _violation_count(pc.less(table.column("high"), table.column("open")))
    defects["ohlc_violations"] += _violation_count(pc.less(table.column("high"), table.column("close")))
    defects["ohlc_violations"] += _violation_count(pc.less(table.column("high"), table.column("low")))
    defects["ohlc_violations"] += _violation_count(pc.greater(table.column("low"), table.column("open")))
    defects["ohlc_violations"] += _violation_count(pc.greater(table.column("low"), table.column("close")))
    defects["negative_base_volume"] += _violation_count(pc.less(table.column("volume"), zero))
    defects["negative_base_volume"] += _violation_count(pc.less(table.column("taker_buy_volume"), zero))
    defects["negative_quote_volume"] += _violation_count(pc.less(table.column("quote_volume"), zero))
    defects["negative_quote_volume"] += _violation_count(pc.less(table.column("taker_buy_quote_volume"), zero))
    defects["negative_trade_counts"] += _violation_count(pc.less(table.column("count"), 0))


def validate_published(root: Path = DATA_ROOT) -> dict:
    """Read published Spot partitions. This function does not write."""
    require_spot_symbols(SPOT_SYMBOLS)
    per_symbol = {}
    total = 0
    partition_count = 0
    defects = {
        "missing_minutes": 0,
        "duplicate_open_times": 0,
        "out_of_order_rows": 0,
        "rows_outside_range": 0,
        "nulls": 0,
        "malformed_timestamps": 0,
        "ohlc_violations": 0,
        "negative_prices": 0,
        "negative_base_volume": 0,
        "negative_quote_volume": 0,
        "negative_trade_counts": 0,
    }
    october_absent_28 = True
    october_complete = True
    leap_day_present = True
    transition_ok = True
    start_2025 = ms_of(datetime(2025, 1, 1, tzinfo=timezone.utc))
    start_2026 = ms_of(datetime(2026, 1, 1, tzinfo=timezone.utc))
    october_start = ms_of(datetime(2026, 10, 1, tzinfo=timezone.utc))
    leap_ms = ms_of(datetime(2024, 2, 29, tzinfo=timezone.utc))
    october_28_ms = ms_of(datetime(2026, 10, 28, tzinfo=timezone.utc))
    for symbol in SPOT_SYMBOLS:
        rows = 0
        years = {2024: 0, 2025: 0, 2026: 0}
        october = 0
        partitions = 0
        first = None
        last = None
        last_close = None
        units = set()
        previous = None
        first_fp = hashlib.sha256()
        second_fp = hashlib.sha256()
        dec_last = jan_first = None
        symbol_leap = False
        for year, month in partition_months():
            path = hive_path(root, symbol, year, month)
            if not path.exists():
                defects["missing_minutes"] += expected_partition_minutes(year, month)
                leap_day_present = False
                continue
            partitions += 1
            partition_count += 1
            _reject_nonspec_schema(path)
            _accumulate_quality(path, defects)
            _update_fingerprint_from_parquet(first_fp, symbol, path)
            _update_fingerprint_from_parquet(second_fp, symbol, path)
            table = pq.read_table(path, columns=["open_time", "close_time"])
            meta_units = pq.read_schema(path).metadata.get(b"timestamp_units")
            if meta_units:
                units.update(item for item in meta_units.decode("ascii").split(",") if item)
            opens = [int(item) for item in table.column("open_time").cast(pa.int64()).to_pylist()]
            closes = [int(item) for item in table.column("close_time").cast(pa.int64()).to_pylist()]
            if (year, month) == (2024, 12) and opens:
                dec_last = opens[-1]
            if (year, month) == (2025, 1) and opens:
                jan_first = opens[0]
            if leap_ms in opens:
                symbol_leap = True
            if october_28_ms in opens:
                october_absent_28 = False
            for open_ms, close_ms in zip(opens, closes):
                if previous is not None:
                    if open_ms == previous:
                        defects["duplicate_open_times"] += 1
                    elif open_ms < previous:
                        defects["out_of_order_rows"] += 1
                    elif open_ms != previous + 60_000:
                        defects["missing_minutes"] += (open_ms - previous) // 60_000 - 1
                previous = open_ms
                if open_ms < RANGE_START_MS or open_ms > RANGE_LAST_OPEN_MS or open_ms >= RANGE_EXCLUSIVE_END_MS:
                    defects["rows_outside_range"] += 1
                if close_ms != open_ms + 59_999 or open_ms % 60_000:
                    defects["malformed_timestamps"] += 1
                if open_ms < start_2025:
                    years[2024] += 1
                elif open_ms < start_2026:
                    years[2025] += 1
                else:
                    years[2026] += 1
                if october_start <= open_ms <= RANGE_LAST_OPEN_MS:
                    october += 1
            rows += len(opens)
            if opens:
                first = opens[0] if first is None else first
                last = opens[-1]
                last_close = closes[-1]
        if not symbol_leap:
            leap_day_present = False
        if dec_last is None or jan_first is None or jan_first - dec_last != 60_000:
            transition_ok = False
        if last != RANGE_LAST_OPEN_MS:
            october_complete = False
        per_symbol[symbol] = {
            "rows": rows,
            "partitions": partitions,
            "rows_2024": years[2024],
            "rows_2025": years[2025],
            "rows_2026": years[2026],
            "october_2026_rows": october,
            "first_open": iso_of_ms(first) if first is not None else None,
            "last_open": iso_of_ms(last) if last is not None else None,
            "last_close": iso_close_of_ms(last_close) if last_close is not None else None,
            "content_fingerprint": first_fp.hexdigest(),
            "content_fingerprint_repeat": second_fp.hexdigest(),
            "timestamp_units": sorted(units),
        }
        total += rows
        if years[2024] != EXPECTED_ROWS_2024 or years[2025] != EXPECTED_ROWS_2025 or years[2026] != EXPECTED_ROWS_2026:
            defects["missing_minutes"] += 1
        if october != EXPECTED_ROWS_OCTOBER_2026:
            defects["missing_minutes"] += 1
    # Explicit boundary probes use ETHBTC only as a representative of the shared calendar.
    eth_feb = hive_path(root, "ETHBTC", 2024, 2)
    eth_dec = hive_path(root, "ETHBTC", 2024, 12)
    eth_jan = hive_path(root, "ETHBTC", 2025, 1)
    eth_oct = hive_path(root, "ETHBTC", 2026, 10)
    if eth_feb.exists():
        opens = pq.read_table(eth_feb, columns=["open_time"]).column("open_time").cast(pa.int64()).to_pylist()
        leap_day_present = leap_day_present and ms_of(datetime(2024, 2, 29, tzinfo=timezone.utc)) in set(map(int, opens))
    else:
        leap_day_present = False
    if eth_dec.exists() and eth_jan.exists():
        dec_last = int(pq.read_table(eth_dec, columns=["open_time"]).column("open_time").cast(pa.int64()).to_pylist()[-1])
        jan_first = int(pq.read_table(eth_jan, columns=["open_time"]).column("open_time").cast(pa.int64()).to_pylist()[0])
        transition_ok = transition_ok and jan_first - dec_last == 60_000 and dec_last < ms_of(datetime(2025, 1, 1, tzinfo=timezone.utc)) <= jan_first
    else:
        transition_ok = False
    if eth_oct.exists():
        opens = set(map(int, pq.read_table(eth_oct, columns=["open_time"]).column("open_time").cast(pa.int64()).to_pylist()))
        october_absent_28 = october_absent_28 and ms_of(datetime(2026, 10, 28, tzinfo=timezone.utc)) not in opens
        october_complete = october_complete and ms_of(datetime(2026, 10, 27, 23, 59, tzinfo=timezone.utc)) in opens
    else:
        october_absent_28 = False
        october_complete = False
    passed = (
        total == EXPECTED_ROWS_TOTAL
        and partition_count == EXPECTED_PARTITIONS_PER_SYMBOL * len(SPOT_SYMBOLS)
        and all(item["rows"] == EXPECTED_ROWS_PER_SYMBOL for item in per_symbol.values())
        and all(item["first_open"] == "2024-01-01T00:00:00Z" for item in per_symbol.values())
        and all(item["last_open"] == "2026-10-27T23:59:00Z" for item in per_symbol.values())
        and all(item["last_close"] == "2026-10-27T23:59:59.999Z" for item in per_symbol.values())
        and all(item["content_fingerprint"] == item["content_fingerprint_repeat"] for item in per_symbol.values())
        and all(value == 0 for value in defects.values())
        and leap_day_present
        and transition_ok
        and october_absent_28
        and october_complete
    )
    return {
        "mode": "READ_ONLY_VALIDATION",
        "passed": passed,
        "rows_total": total,
        "partitions_total": partition_count,
        "exclusive_end": "2026-10-28T00:00:00Z",
        "per_symbol": per_symbol,
        "defects": defects,
        "leap_day_2024_02_29_present": leap_day_present,
        "timestamp_unit_transition_ok": transition_ok,
        "october_27_complete": october_complete,
        "october_28_absent": october_absent_28,
        "downloaded_files": 0,
    }


def _store(path: Path, current: dict) -> None:
    previous = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"runs": []}
    runs = list(previous.get("runs") or [])
    runs.append({"run_id": current.get("run_id"), "mode": current.get("mode"), "error": current.get("error")})
    report = {
        "task": TASK_NAME,
        "market": "spot",
        "interval": "1m",
        "schema_version": SCHEMA_VERSION,
        "symbols": list(SPOT_SYMBOLS),
        "excluded_symbols": ["BTCBTC", "AAVEBTC", "OPBTC"],
        "current_run": {
            "run_id": current.get("run_id"),
            "mode": current.get("mode"),
            "publication": current.get("publication"),
            "error": current.get("error"),
        },
        "final_inventory": current.get("validation"),
        "content_fingerprints": {
            item["symbol"]: item["content_fingerprint"]
            for item in current.get("symbols_built") or []
        },
        "timestamp_units": {
            item["symbol"]: item["timestamp_units"]
            for item in current.get("symbols_built") or []
        },
        "runs": runs,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(path.suffix + ".part")
    part.write_text(json.dumps(report, indent=2), encoding="utf-8")
    os.replace(part, path)


def main() -> int:
    if len(sys.argv) == 2 and sys.argv[1] == "--validate-only":
        result = validate_published()
        json.dump({"passed": result["passed"], "rows_total": result["rows_total"], "partitions_total": result["partitions_total"]}, sys.stdout)
        sys.stdout.write("\n")
        return 0 if result["passed"] else 1
    if len(sys.argv) > 1:
        raise SystemExit("unsupported arguments")
    return run()


if __name__ == "__main__":
    sys.exit(main())
