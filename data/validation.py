"""Reusable validation for loaded candle tables and alignment maps."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

import pyarrow as pa
import pyarrow.compute as pc

import config
from data.exceptions import (
    ColumnProjectionError,
    DataGapError,
    DuplicateTimestampError,
    InsufficientWarmupError,
    InvalidTimeRangeError,
    LookaheadViolationError,
    OutOfOrderDataError,
    SchemaMismatchError,
)
from data.timeutil import datetime_to_ms, iso_z, ms_to_datetime


def analytical_schema() -> pa.Schema:
    ts = pa.timestamp("ms", tz="UTC")
    dec = pa.decimal128(config.DECIMAL_PRECISION, config.DECIMAL_SCALE)
    return pa.schema(
        [
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("open_time", ts, nullable=False),
            pa.field("open", dec, nullable=False),
            pa.field("high", dec, nullable=False),
            pa.field("low", dec, nullable=False),
            pa.field("close", dec, nullable=False),
            pa.field("volume", dec, nullable=False),
            pa.field("close_time", ts, nullable=False),
            pa.field("quote_asset_volume", dec, nullable=False),
            pa.field("number_of_trades", pa.int64(), nullable=False),
            pa.field("taker_buy_base_asset_volume", dec, nullable=False),
            pa.field("taker_buy_quote_asset_volume", dec, nullable=False),
            pa.field("source_candle_count", pa.int16(), nullable=False),
        ]
    )


def _ctx(symbol: str, timeframe: str, start: datetime, end: datetime, extra: str = "") -> str:
    base = f"symbol={symbol} timeframe={timeframe} range=[{iso_z(start)}, {iso_z(end)})"
    return f"{base} {extra}".strip()


def validate_requested_columns(columns: Sequence[str] | None) -> tuple[str, ...] | None:
    if columns is None:
        return None
    unknown = [c for c in columns if c not in config.ANALYTICAL_COLUMNS]
    if unknown:
        raise ColumnProjectionError(
            f"Unknown analytical columns {unknown}. Allowed: {list(config.ANALYTICAL_COLUMNS)}"
        )
    seen: list[str] = []
    for name in columns:
        if name not in seen:
            seen.append(name)
    return tuple(seen)


def output_columns(requested: Sequence[str] | None) -> tuple[str, ...]:
    if requested is None:
        return config.ANALYTICAL_COLUMNS
    ordered: list[str] = []
    for name in config.IDENTITY_COLUMNS:
        if name not in ordered:
            ordered.append(name)
    for name in config.ANALYTICAL_COLUMNS:
        if name in requested and name not in ordered:
            ordered.append(name)
    return tuple(ordered)


def validate_symbol_column(table: pa.Table, symbol: str, timeframe: str, start: datetime, end: datetime) -> None:
    if table.num_rows == 0:
        return
    values = pc.unique(table.column("symbol")).to_pylist()
    if values != [symbol]:
        raise SchemaMismatchError(
            f"{_ctx(symbol, timeframe, start, end)} symbol column values {values}"
        )


def validate_timestamp_types(table: pa.Table, symbol: str, timeframe: str, start: datetime, end: datetime) -> None:
    for name in ("open_time", "close_time"):
        if name not in table.column_names:
            raise SchemaMismatchError(f"{_ctx(symbol, timeframe, start, end)} missing {name}")
        field = table.schema.field(name)
        t = field.type
        if not pa.types.is_timestamp(t) or t.unit != "ms" or str(t.tz) not in ("UTC", "utc"):
            raise SchemaMismatchError(
                f"{_ctx(symbol, timeframe, start, end)} {name} type {t} is not timestamp[ms, tz=UTC]"
            )


def validate_decimal_types(table: pa.Table, symbol: str, timeframe: str, start: datetime, end: datetime) -> None:
    for name in config.DECIMAL_COLUMNS:
        if name not in table.column_names:
            continue
        t = table.schema.field(name).type
        if not pa.types.is_decimal(t) or t.precision != config.DECIMAL_PRECISION or t.scale != config.DECIMAL_SCALE:
            raise SchemaMismatchError(
                f"{_ctx(symbol, timeframe, start, end)} {name} type {t} is not decimal128(38, 8)"
            )


def validate_chronology(
    table: pa.Table,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    *,
    source_file: str | None = None,
) -> dict[str, int]:
    if table.num_rows == 0:
        return {"rows": 0, "duplicates": 0, "out_of_order": 0, "gaps": 0}
    opens = table.column("open_time").cast(pa.int64())
    n = table.num_rows
    if n == 1:
        return {"rows": 1, "duplicates": 0, "out_of_order": 0, "gaps": 0}
    prev = opens.slice(0, n - 1)
    nxt = opens.slice(1)
    diff = pc.subtract(nxt, prev)
    zero = pc.sum(pc.equal(diff, 0)).as_py() or 0
    negative = pc.sum(pc.less(diff, 0)).as_py() or 0
    loc = f" file={source_file}" if source_file else ""
    if zero:
        raise DuplicateTimestampError(
            f"{_ctx(symbol, timeframe, start, end)} duplicate open_time count={zero}{loc}"
        )
    if negative:
        raise OutOfOrderDataError(
            f"{_ctx(symbol, timeframe, start, end)} out-of-order open_time count={negative}{loc}"
        )
    return {"rows": n, "duplicates": 0, "out_of_order": 0}


def validate_spacing(
    table: pa.Table,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    duration_ms: int,
    *,
    source_file: str | None = None,
) -> int:
    n = table.num_rows
    if n <= 1:
        return 0
    opens = table.column("open_time").cast(pa.int64())
    diff = pc.subtract(opens.slice(1), opens.slice(0, n - 1))
    bad_mask = pc.not_equal(diff, duration_ms)
    bad = pc.sum(bad_mask.cast(pa.int64())).as_py() or 0
    if bad:
        first = None
        flags = bad_mask.to_pylist()
        diffs = diff.to_pylist()
        opens_list = opens.to_pylist()
        for i, flag in enumerate(flags):
            if flag:
                first = (i, opens_list[i], opens_list[i + 1], diffs[i])
                break
        loc = f" file={source_file}" if source_file else ""
        raise DataGapError(
            f"{_ctx(symbol, timeframe, start, end)} unexpected spacing "
            f"expected_delta_ms={duration_ms} first={first}{loc}"
        )
    return 0


def validate_ohlc(table: pa.Table, symbol: str, timeframe: str, start: datetime, end: datetime) -> int:
    needed = ("open", "high", "low", "close")
    if any(name not in table.column_names for name in needed):
        return 0
    high, low_, open_, close = (table.column(n) for n in ("high", "low", "open", "close"))
    bad = pc.or_(
        pc.or_(pc.less(high, open_), pc.less(high, close)),
        pc.or_(pc.greater(low_, open_), pc.or_(pc.greater(low_, close), pc.less(high, low_))),
    )
    count = pc.sum(bad.cast(pa.int64())).as_py() or 0
    if count:
        idx = bad.to_pylist().index(True)
        open_ms = table.column("open_time").cast(pa.int64())[idx].as_py()
        raise SchemaMismatchError(
            f"{_ctx(symbol, timeframe, start, end)} OHLC invariant failed at row={idx} "
            f"open_time={iso_z(ms_to_datetime(open_ms))}"
        )
    return 0


def validate_non_negative(
    table: pa.Table, symbol: str, timeframe: str, start: datetime, end: datetime
) -> int:
    count = 0
    for name in config.VOLUME_COLUMNS:
        if name not in table.column_names:
            continue
        bad = pc.less(table.column(name), 0)
        n = pc.sum(bad.cast(pa.int64())).as_py() or 0
        if n:
            idx = bad.to_pylist().index(True)
            raise SchemaMismatchError(
                f"{_ctx(symbol, timeframe, start, end)} negative {name} at row={idx}"
            )
        count += n
    if "number_of_trades" in table.column_names:
        bad = pc.less(table.column("number_of_trades"), 0)
        n = pc.sum(bad.cast(pa.int64())).as_py() or 0
        if n:
            raise SchemaMismatchError(
                f"{_ctx(symbol, timeframe, start, end)} negative number_of_trades"
            )
        count += n
    return count


def validate_warmup(
    table: pa.Table,
    trade_start_index: int,
    start: datetime,
    *,
    symbol: str,
    timeframe: str,
    end: datetime,
    requested: int,
    loaded: int,
    require_full: bool,
) -> None:
    if requested < 0:
        raise InvalidTimeRangeError("warmup_bars must be >= 0")
    if require_full and loaded < requested:
        raise InsufficientWarmupError(
            f"{_ctx(symbol, timeframe, start, end)} requested {requested} warmup bars, loaded {loaded}"
        )
    if trade_start_index != loaded:
        raise SchemaMismatchError(
            f"{_ctx(symbol, timeframe, start, end)} trade_start_index={trade_start_index} != warmup_loaded={loaded}"
        )
    if loaded == 0:
        return
    start_ms = datetime_to_ms(start)
    closes = table.column("close_time").cast(pa.int64()).slice(0, loaded)
    if pc.sum(pc.greater_equal(closes, start_ms).cast(pa.int64())).as_py():
        raise SchemaMismatchError(
            f"{_ctx(symbol, timeframe, start, end)} a warmup row has close_time >= start_utc"
        )


def validate_trade_range(table: pa.Table, trade_start_index: int, start: datetime, end: datetime,
                         *, symbol: str, timeframe: str) -> None:
    trade = table.slice(trade_start_index)
    if trade.num_rows == 0:
        return
    start_ms = datetime_to_ms(start)
    end_ms = datetime_to_ms(end)
    opens = trade.column("open_time").cast(pa.int64())
    if pc.sum(pc.less(opens, start_ms).cast(pa.int64())).as_py():
        raise SchemaMismatchError(f"{_ctx(symbol, timeframe, start, end)} trade row open_time < start_utc")
    if pc.sum(pc.greater_equal(opens, end_ms).cast(pa.int64())).as_py():
        raise SchemaMismatchError(f"{_ctx(symbol, timeframe, start, end)} trade row open_time >= end_utc")


def validate_analysis_finite(table: pa.Table, symbol: str, timeframe: str, start: datetime, end: datetime) -> None:
    import math

    for name in config.DECIMAL_COLUMNS:
        if name not in table.column_names:
            continue
        t = table.schema.field(name).type
        if not pa.types.is_floating(t):
            raise SchemaMismatchError(
                f"{_ctx(symbol, timeframe, start, end)} analysis mode {name} type {t} is not float64"
            )
        values = table.column(name).to_pylist()
        for i, value in enumerate(values):
            if value is None or not math.isfinite(value):
                raise SchemaMismatchError(
                    f"{_ctx(symbol, timeframe, start, end)} non-finite {name} at row={i} value={value!r}"
                )


def exact_analysis_agree(exact_table: pa.Table, analysis_table: pa.Table) -> None:
    import math
    from decimal import Decimal

    if exact_table.num_rows != analysis_table.num_rows:
        raise SchemaMismatchError("exact/analysis row counts differ")
    for name in config.DECIMAL_COLUMNS:
        if name not in exact_table.column_names or name not in analysis_table.column_names:
            continue
        casted = pc.cast(exact_table.column(name), pa.float64())
        analysis_col = analysis_table.column(name)
        mismatch = pc.invert(pc.equal(casted, analysis_col))
        if pc.sum(mismatch.cast(pa.int64())).as_py():
            idx = mismatch.to_pylist().index(True)
            raise SchemaMismatchError(
                f"{name} row={idx} analysis value is not the PyArrow float64 cast of exact decimal128"
            )
        exact_vals = exact_table.column(name).to_pylist()
        analysis_vals = analysis_col.to_pylist()
        for i, (ex, an) in enumerate(zip(exact_vals, analysis_vals)):
            converted = float(ex if isinstance(ex, Decimal) else Decimal(str(ex)))
            if not math.isfinite(converted) or not math.isfinite(an):
                raise SchemaMismatchError(f"{name} row={i} non-finite after conversion")
            abs_err = abs(converted - an)
            scale = max(abs(converted), abs(an), 1.0)
            if abs_err > config.ANALYSIS_ABSOLUTE_TOLERANCE and abs_err / scale > config.ANALYSIS_RELATIVE_TOLERANCE:
                raise SchemaMismatchError(
                    f"{name} row={i} exact={ex} analysis={an} abs_err={abs_err}"
                )


def validate_alignment_map(
    *,
    symbol: str,
    base_timeframe: str,
    higher_timeframe: str,
    decision_clock: str,
    decision_ms: Sequence[int],
    htf_close_ms: Sequence[int],
    htf_open_ms: Sequence[int],
    indices: Sequence[int],
    start: datetime,
    end: datetime,
) -> None:
    n_base = len(decision_ms)
    n_htf = len(htf_close_ms)
    if len(indices) != n_base:
        raise LookaheadViolationError(
            f"{_ctx(symbol, base_timeframe, start, end)} availability length {len(indices)} != base rows {n_base}"
        )
    last = config.UNAVAILABLE_INDEX
    for i, idx in enumerate(indices):
        if idx < config.UNAVAILABLE_INDEX or idx >= n_htf:
            raise LookaheadViolationError(
                f"{_ctx(symbol, base_timeframe, start, end)} {higher_timeframe} mapped index {idx} "
                f"out of range at base_row={i}"
            )
        if idx < last:
            raise LookaheadViolationError(
                f"{_ctx(symbol, base_timeframe, start, end)} {higher_timeframe} mapped indices decreased "
                f"at base_row={i}: {last} -> {idx}"
            )
        if idx != config.UNAVAILABLE_INDEX:
            if htf_close_ms[idx] > decision_ms[i]:
                raise LookaheadViolationError(
                    f"{_ctx(symbol, base_timeframe, start, end)} lookahead: {higher_timeframe} row={idx} "
                    f"close_time={iso_z(ms_to_datetime(htf_close_ms[idx]))} > "
                    f"decision_time={iso_z(ms_to_datetime(decision_ms[i]))} "
                    f"decision_clock={decision_clock}"
                )
            nxt = idx + 1
            if nxt < n_htf and htf_close_ms[nxt] <= decision_ms[i]:
                raise LookaheadViolationError(
                    f"{_ctx(symbol, base_timeframe, start, end)} skipped available {higher_timeframe} "
                    f"row={nxt} at base_row={i}"
                )
        last = idx
