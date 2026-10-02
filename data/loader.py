"""Read-only single- and multi-timeframe candle loader."""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import config
from data.alignment import build_availability
from data.catalog import DataCatalog, PartitionRef, require_symbol, require_timeframe
from data.exceptions import (
    EmptyRangeError,
    InsufficientWarmupError,
    InvalidTimeRangeError,
    SchemaMismatchError,
    UnsupportedTimeframeError,
)
from data.models import CandleSlice, MultiTimeframeData
from data.timeutil import datetime_to_ms, iso_z, ms_to_datetime, require_aware_utc, require_half_open_range
from data.validation import (
    exact_analysis_agree,
    output_columns,
    validate_analysis_finite,
    validate_chronology,
    validate_decimal_types,
    validate_non_negative,
    validate_ohlc,
    validate_requested_columns,
    validate_spacing,
    validate_symbol_column,
    validate_timestamp_types,
    validate_trade_range,
    validate_warmup,
)

log = logging.getLogger("data_access")

_TS_UTC = pa.timestamp("ms", tz="UTC")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _require_precision(mode: str) -> str:
    if mode not in config.ALLOWED_PRECISION_MODES:
        raise InvalidTimeRangeError(
            f"precision_mode must be one of {config.ALLOWED_PRECISION_MODES}, got {mode!r}"
        )
    return mode


def _require_clock(clock: str) -> str:
    if clock not in config.ALLOWED_DECISION_CLOCKS:
        raise UnsupportedTimeframeError(
            f"decision_clock must be one of {config.ALLOWED_DECISION_CLOCKS}, got {clock!r}"
        )
    return clock


def _require_warmup(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidTimeRangeError(f"warmup_bars must be a non-negative int, got {value!r}")
    return value


def _physical_columns(timeframe: str, analytical: Iterable[str]) -> list[str]:
    out: list[str] = []
    for name in analytical:
        if timeframe == "1m":
            if name == "source_candle_count":
                continue
            out.append(config.ANALYTICAL_TO_CANONICAL.get(name, name))
        else:
            out.append(name)
    return list(dict.fromkeys(out))


def _to_analytical(table: pa.Table, timeframe: str, wanted: Sequence[str]) -> pa.Table:
    if table.num_rows == 0:
        schema_fields = []
        for name in wanted:
            schema_fields.append(_analytical_field(name))
        return pa.table({f.name: pa.array([], type=f.type) for f in schema_fields}, schema=pa.schema(schema_fields))
    arrays: list[pa.Array] = []
    fields: list[pa.Field] = []
    n = table.num_rows
    names = set(table.column_names)
    for name in wanted:
        if timeframe == "1m" and name == "source_candle_count":
            arrays.append(pa.nulls(n, type=pa.int16()).fill_null(1))
            fields.append(pa.field(name, pa.int16(), nullable=False))
            continue
        physical = config.ANALYTICAL_TO_CANONICAL.get(name, name) if timeframe == "1m" else name
        if physical not in names:
            raise SchemaMismatchError(f"physical column {physical} missing while mapping {name}")
        col = table.column(physical)
        if name == "symbol" and not pa.types.is_string(col.type):
            col = pc.cast(col, pa.string())
        if name == "number_of_trades" and not pa.types.is_int64(col.type):
            col = pc.cast(col, pa.int64())
        arrays.append(col.combine_chunks())
        fields.append(pa.field(name, arrays[-1].type, nullable=False))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _analytical_field(name: str) -> pa.Field:
    ts = _TS_UTC
    dec = pa.decimal128(config.DECIMAL_PRECISION, config.DECIMAL_SCALE)
    mapping = {
        "symbol": pa.string(),
        "open_time": ts,
        "close_time": ts,
        "open": dec,
        "high": dec,
        "low": dec,
        "close": dec,
        "volume": dec,
        "quote_asset_volume": dec,
        "taker_buy_base_asset_volume": dec,
        "taker_buy_quote_asset_volume": dec,
        "number_of_trades": pa.int64(),
        "source_candle_count": pa.int16(),
    }
    return pa.field(name, mapping[name], nullable=False)


def _read_partition(
    path: Path,
    physical_cols: Sequence[str],
    start: datetime,
    end: datetime,
) -> pa.Table:
    """Read one Parquet file. Do not use directory-level Hive inference.

    pq.read_table() on a Hive path merges partition fields with file columns and
    breaks on canonical 1m dictionary-encoded `symbol`. Scan the file itself,
    push a time filter through the dataset scanner, then drop extras.
    """
    import pyarrow.dataset as ds

    available = set(pq.ParquetFile(path).schema_arrow.names)
    cols = [c for c in physical_cols if c in available]
    dataset = ds.dataset(path, format="parquet", partitioning=None)
    start_scalar = pa.scalar(start, type=_TS_UTC)
    end_scalar = pa.scalar(end, type=_TS_UTC)
    filt = (ds.field("open_time") >= start_scalar) & (ds.field("open_time") < end_scalar)
    table = dataset.to_table(columns=cols, filter=filt)
    extras = [name for name in table.column_names if name not in cols]
    if extras:
        table = table.drop(extras)
    return table


def _to_analysis_mode(table: pa.Table) -> pa.Table:
    fields: list[pa.Field] = []
    cols: list[pa.Array] = []
    for field in table.schema:
        col = table.column(field.name)
        if field.name in config.DECIMAL_COLUMNS:
            cols.append(pc.cast(col, pa.float64()).combine_chunks())
            fields.append(pa.field(field.name, pa.float64(), nullable=False))
        else:
            cols.append(col.combine_chunks())
            fields.append(field)
    return pa.Table.from_arrays(cols, schema=pa.schema(fields))


def _collect_warmup_opens(
    catalog: DataCatalog,
    symbol: str,
    timeframe: str,
    start: datetime,
    warmup_bars: int,
    *,
    require_full: bool,
) -> tuple[datetime | None, int, list[int]]:
    if warmup_bars == 0:
        return None, 0, []
    spec = config.TIMEFRAME_SPECS[timeframe]
    parts = [p for p in catalog.list_partitions(timeframe, symbol=symbol) if p.start_utc < start]
    parts.sort(key=lambda p: p.start_utc)
    start_ms = datetime_to_ms(start)
    collected_ms: list[int] = []
    for part in reversed(parts):
        raw = _read_partition(part.path, ["open_time", "close_time"], part.start_utc, start)
        if raw.num_rows == 0:
            continue
        opens = raw.column("open_time").cast(pa.int64())
        closes = raw.column("close_time").cast(pa.int64())
        mask = pc.less(closes, start_ms)
        raw = raw.filter(mask)
        if raw.num_rows == 0:
            continue
        chunk = raw.column("open_time").cast(pa.int64()).to_pylist()
        collected_ms = chunk + collected_ms
        if len(collected_ms) >= warmup_bars:
            collected_ms = collected_ms[-warmup_bars:]
            break
    loaded = len(collected_ms)
    if loaded < warmup_bars and require_full:
        raise InsufficientWarmupError(
            f"symbol={symbol} timeframe={timeframe} start={iso_z(start)} "
            f"requested {warmup_bars} completed warmup bars with close_time < start, found {loaded}"
        )
    if not collected_ms:
        return None, 0, []
    return ms_to_datetime(collected_ms[0]), loaded, collected_ms


def load_candles(
    symbol: str,
    timeframe: str,
    start_utc,
    end_utc,
    *,
    columns: list[str] | None = None,
    warmup_bars: int = 0,
    require_full_warmup: bool = True,
    precision_mode: str = config.DEFAULT_PRECISION_MODE,
    data_root: Path | None = None,
    catalog: DataCatalog | None = None,
    allow_empty: bool = False,
) -> CandleSlice:
    symbol = require_symbol(symbol)
    timeframe = require_timeframe(timeframe)
    start = require_aware_utc(start_utc, name="start_utc")
    end = require_aware_utc(end_utc, name="end_utc")
    require_half_open_range(start, end)
    warmup_bars = _require_warmup(warmup_bars)
    precision_mode = _require_precision(precision_mode)
    requested_cols = validate_requested_columns(columns)
    wanted = output_columns(requested_cols)
    spec = config.TIMEFRAME_SPECS[timeframe]
    cat = catalog if catalog is not None else DataCatalog(data_root)

    read_analytical = list(dict.fromkeys(
        list(wanted)
        + list(config.IDENTITY_COLUMNS)
        + list(config.OHLC_COLUMNS)
        + ["volume", "number_of_trades"]
    ))
    physical = _physical_columns(timeframe, read_analytical)

    warmup_first, warmup_loaded, warmup_opens = _collect_warmup_opens(
        cat, symbol, timeframe, start, warmup_bars, require_full=require_full_warmup
    )
    load_start = warmup_first if warmup_first is not None else start
    partitions = cat.resolve_partitions(symbol, timeframe, load_start, end)
    hashes: dict[str, str] = {}
    pieces: list[pa.Table] = []
    for part in partitions:
        hashes[str(part.path)] = _sha256_file(part.path)
        piece = _read_partition(part.path, physical, load_start, end)
        if piece.num_rows:
            pieces.append(piece)
    if not pieces:
        if allow_empty:
            empty = _empty_analytical(wanted)
            return _empty_slice(
                symbol, timeframe, start, end, warmup_bars, warmup_loaded, precision_mode, hashes, spec, empty
            )
        raise EmptyRangeError(
            f"No {timeframe} candles for {symbol} in [{iso_z(start)}, {iso_z(end)})"
        )
    raw = pa.concat_tables(pieces, promote_options="default")
    analytical = _to_analytical(raw, timeframe, read_analytical)
    validate_timestamp_types(analytical, symbol, timeframe, start, end)
    validate_symbol_column(analytical, symbol, timeframe, start, end)
    if precision_mode == config.PRECISION_EXACT:
        validate_decimal_types(analytical, symbol, timeframe, start, end)

    start_ms = datetime_to_ms(start)
    end_ms = datetime_to_ms(end)
    opens = analytical.column("open_time").cast(pa.int64())
    closes = analytical.column("close_time").cast(pa.int64())
    trade_mask = pc.and_(pc.greater_equal(opens, start_ms), pc.less(opens, end_ms))
    warmup_mask = pc.less(closes, start_ms)
    trade = analytical.filter(trade_mask)
    warmup = analytical.filter(warmup_mask)
    if warmup_bars:
        if warmup.num_rows > warmup_loaded:
            warmup = warmup.slice(warmup.num_rows - warmup_loaded)
        warmup_loaded = warmup.num_rows
    else:
        warmup = warmup.slice(0, 0)
        warmup_loaded = 0

    if trade.num_rows == 0 and not allow_empty:
        raise EmptyRangeError(
            f"No {timeframe} candles for {symbol} in trading range [{iso_z(start)}, {iso_z(end)})"
        )
    if warmup_bars and require_full_warmup and warmup_loaded < warmup_bars:
        raise InsufficientWarmupError(
            f"symbol={symbol} timeframe={timeframe} requested {warmup_bars} warmup bars, loaded {warmup_loaded}"
        )

    if warmup.num_rows:
        validate_chronology(warmup, symbol, timeframe, start, end)
        validate_spacing(warmup, symbol, timeframe, start, end, spec.duration_ms)
    if trade.num_rows:
        validate_chronology(trade, symbol, timeframe, start, end)
        validate_spacing(trade, symbol, timeframe, start, end, spec.duration_ms)

    combined = pa.concat_tables([warmup, trade]) if warmup.num_rows else trade
    validate_chronology(combined, symbol, timeframe, start, end)
    validate_ohlc(combined, symbol, timeframe, start, end)
    validate_non_negative(combined, symbol, timeframe, start, end)
    trade_start_index = warmup.num_rows
    validate_warmup(
        combined,
        trade_start_index,
        start,
        symbol=symbol,
        timeframe=timeframe,
        end=end,
        requested=warmup_bars,
        loaded=warmup_loaded,
        require_full=require_full_warmup,
    )
    validate_trade_range(combined, trade_start_index, start, end, symbol=symbol, timeframe=timeframe)

    returned = combined.select([name for name in wanted if name in combined.column_names])
    exact_table = returned
    if precision_mode == config.PRECISION_ANALYSIS:
        returned = _to_analysis_mode(exact_table)
        validate_analysis_finite(returned, symbol, timeframe, start, end)
        exact_analysis_agree(exact_table, returned)

    loaded_start = None
    loaded_end = None
    if returned.num_rows:
        loaded_start = ms_to_datetime(returned.column("open_time").cast(pa.int64())[0].as_py())
        loaded_end = ms_to_datetime(returned.column("open_time").cast(pa.int64())[-1].as_py())

    summary = {
        "rows": returned.num_rows,
        "trade_rows": trade.num_rows,
        "warmup_bars_requested": warmup_bars,
        "warmup_bars_loaded": warmup_loaded,
        "warmup_shortage": max(0, warmup_bars - warmup_loaded),
        "source_files": list(hashes),
        "precision_mode": precision_mode,
        "schema_version": spec.schema_version,
        "duration_ms": spec.duration_ms,
        "half_open": "[start_utc, end_utc)",
        "ok": True,
    }
    log.info(
        "loaded %s %s rows=%d warmup=%d trade=%d files=%d range=[%s, %s)",
        symbol, timeframe, returned.num_rows, warmup_loaded, trade.num_rows, len(hashes),
        iso_z(start), iso_z(end),
    )
    return CandleSlice(
        symbol=symbol,
        timeframe=timeframe,
        table=returned,
        requested_start_utc=start,
        requested_end_utc=end,
        loaded_start_utc=loaded_start,
        loaded_end_utc=loaded_end,
        warmup_bars_requested=warmup_bars,
        warmup_bars_loaded=warmup_loaded,
        trade_start_index=trade_start_index,
        precision_mode=precision_mode,
        source_files=tuple(hashes.keys()),
        source_file_hashes=hashes,
        validation_summary=summary,
        schema_version=spec.schema_version,
    )


def _empty_analytical(wanted: Sequence[str]) -> pa.Table:
    schema = pa.schema([_analytical_field(name) for name in wanted])
    return pa.Table.from_pylist([], schema=schema)


def _empty_slice(
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    warmup_bars: int,
    warmup_loaded: int,
    precision_mode: str,
    hashes: dict[str, str],
    spec: config.TimeframeSpec,
    table: pa.Table,
) -> CandleSlice:
    return CandleSlice(
        symbol=symbol,
        timeframe=timeframe,
        table=table,
        requested_start_utc=start,
        requested_end_utc=end,
        loaded_start_utc=None,
        loaded_end_utc=None,
        warmup_bars_requested=warmup_bars,
        warmup_bars_loaded=warmup_loaded,
        trade_start_index=0,
        precision_mode=precision_mode,
        source_files=tuple(hashes.keys()),
        source_file_hashes=hashes,
        validation_summary={"rows": 0, "ok": True, "empty": True},
        schema_version=spec.schema_version,
    )


def load_multi_timeframe(
    symbol: str,
    timeframes: list[str],
    base_timeframe: str,
    start_utc,
    end_utc,
    *,
    warmup_bars: dict[str, int] | None = None,
    precision_mode: str = config.DEFAULT_PRECISION_MODE,
    decision_clock: str = config.DEFAULT_DECISION_CLOCK,
    require_full_warmup: bool = True,
    data_root: Path | None = None,
    catalog: DataCatalog | None = None,
) -> MultiTimeframeData:
    symbol = require_symbol(symbol)
    if not timeframes:
        raise UnsupportedTimeframeError("timeframes must be a non-empty list")
    if len(timeframes) != len(set(timeframes)):
        raise UnsupportedTimeframeError(f"duplicate timeframe names in {timeframes}")
    for tf in timeframes:
        require_timeframe(tf)
    base_timeframe = require_timeframe(base_timeframe)
    if base_timeframe not in timeframes:
        raise UnsupportedTimeframeError(f"base_timeframe {base_timeframe!r} is not in {timeframes}")
    base_ms = config.TIMEFRAME_SPECS[base_timeframe].duration_ms
    for tf in timeframes:
        if config.TIMEFRAME_SPECS[tf].duration_ms < base_ms:
            raise UnsupportedTimeframeError(
                f"timeframe {tf} is higher frequency than base {base_timeframe} and cannot be "
                "aligned as a higher timeframe. Load it separately."
            )
    start = require_aware_utc(start_utc, name="start_utc")
    end = require_aware_utc(end_utc, name="end_utc")
    require_half_open_range(start, end)
    precision_mode = _require_precision(precision_mode)
    decision_clock = _require_clock(decision_clock)
    warmup_map = dict(warmup_bars or {})
    unknown = [k for k in warmup_map if k not in timeframes]
    if unknown:
        raise UnsupportedTimeframeError(f"warmup_bars keys not in timeframes: {unknown}")
    for value in warmup_map.values():
        _require_warmup(value)
    cat = catalog if catalog is not None else DataCatalog(data_root)

    base_slice = load_candles(
        symbol,
        base_timeframe,
        start,
        end,
        warmup_bars=warmup_map.get(base_timeframe, 0),
        require_full_warmup=require_full_warmup,
        precision_mode=precision_mode,
        catalog=cat,
    )
    slices: dict[str, CandleSlice] = {base_timeframe: base_slice}
    availability: dict[str, tuple[int, ...]] = {}
    hashes: dict[str, str] = dict(base_slice.source_file_hashes)
    files: list[str] = list(base_slice.source_files)

    htf_anchor = base_slice.loaded_start_utc or start
    for tf in timeframes:
        if tf == base_timeframe:
            continue
        user_wu = warmup_map.get(tf, 0)
        lookback_n = max(user_wu, 1)
        wu_first, _wu_loaded, _wu_opens = _collect_warmup_opens(
            cat, symbol, tf, htf_anchor, lookback_n, require_full=False
        )
        htf_load_start = wu_first if wu_first is not None else htf_anchor
        htf_slice = load_candles(
            symbol,
            tf,
            htf_load_start,
            end,
            warmup_bars=0,
            require_full_warmup=False,
            precision_mode=precision_mode,
            catalog=cat,
        )
        slices[tf] = htf_slice
        hashes.update(htf_slice.source_file_hashes)
        files.extend(path for path in htf_slice.source_files if path not in files)
        availability[tf] = build_availability(
            base_slice, htf_slice, decision_clock=decision_clock, start=start, end=end
        )

    summary = {
        "base_timeframe": base_timeframe,
        "decision_clock": decision_clock,
        "base_rows": base_slice.row_count,
        "availability_timeframes": list(availability),
        "ok": True,
    }
    return MultiTimeframeData(
        symbol=symbol,
        base_timeframe=base_timeframe,
        timeframes=tuple(timeframes),
        slices=slices,
        decision_clock=decision_clock,
        availability=availability,
        requested_start_utc=start,
        requested_end_utc=end,
        validation_summary=summary,
        source_files=tuple(files),
        source_file_hashes=hashes,
    )
