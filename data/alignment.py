"""No-look-ahead as-of alignment using higher-timeframe close_time."""
from __future__ import annotations

from datetime import datetime
from typing import Sequence

import config
from data.exceptions import LookaheadViolationError, UnsupportedTimeframeError
from data.models import CandleSlice, MultiTimeframeData
from data.timeutil import datetime_to_ms
from data.validation import validate_alignment_map


def asof_indices(htf_close_ms: Sequence[int], decision_ms: Sequence[int]) -> tuple[int, ...]:
    """Return the last HTF row whose close_time <= each decision_time, else -1.

    Both inputs must be strictly non-decreasing. Two-pointer scan is O(n+m).
    """
    n = len(htf_close_ms)
    j = 0
    out: list[int] = []
    for decision in decision_ms:
        while j < n and htf_close_ms[j] <= decision:
            j += 1
        out.append(j - 1 if j else config.UNAVAILABLE_INDEX)
    return tuple(out)


def decision_times_ms(base: CandleSlice, decision_clock: str) -> list[int]:
    if decision_clock == config.DECISION_CLOCK_OPEN:
        return base.open_times_ms()
    if decision_clock == config.DECISION_CLOCK_CLOSE:
        return base.close_times_ms()
    raise UnsupportedTimeframeError(
        f"Unsupported decision_clock {decision_clock!r}. Allowed: {config.ALLOWED_DECISION_CLOCKS}"
    )


def build_availability(
    base: CandleSlice,
    higher: CandleSlice,
    *,
    decision_clock: str,
    start: datetime,
    end: datetime,
) -> tuple[int, ...]:
    decision_ms = decision_times_ms(base, decision_clock)
    htf_close = higher.close_times_ms()
    htf_open = higher.open_times_ms()
    indices = asof_indices(htf_close, decision_ms)
    validate_alignment_map(
        symbol=base.symbol,
        base_timeframe=base.timeframe,
        higher_timeframe=higher.timeframe,
        decision_clock=decision_clock,
        decision_ms=decision_ms,
        htf_close_ms=htf_close,
        htf_open_ms=htf_open,
        indices=indices,
        start=start,
        end=end,
    )
    return indices


def validate_alignment(data: MultiTimeframeData) -> None:
    base = data.slices[data.base_timeframe]
    for timeframe, indices in data.availability.items():
        if timeframe == data.base_timeframe:
            continue
        higher = data.slices[timeframe]
        validate_alignment_map(
            symbol=data.symbol,
            base_timeframe=data.base_timeframe,
            higher_timeframe=timeframe,
            decision_clock=data.decision_clock,
            decision_ms=decision_times_ms(base, data.decision_clock),
            htf_close_ms=higher.close_times_ms(),
            htf_open_ms=higher.open_times_ms(),
            indices=indices,
            start=data.requested_start_utc,
            end=data.requested_end_utc,
        )


def assert_unavailable_open(
    data: MultiTimeframeData,
    timeframe: str,
    base_open_utc: datetime,
    forbidden_open_utc: datetime,
) -> None:
    base = data.slices[data.base_timeframe]
    row = base.index_of_open(base_open_utc)
    idx = data.available_index(timeframe, row)
    higher = data.slices[timeframe]
    forbidden = datetime_to_ms(forbidden_open_utc)
    if idx != config.UNAVAILABLE_INDEX:
        mapped_open = higher.open_times_ms()[idx]
        if mapped_open == forbidden:
            raise LookaheadViolationError(
                f"{timeframe} candle opening {forbidden_open_utc.isoformat()} was visible to "
                f"base {data.base_timeframe} {base_open_utc.isoformat()} under {data.decision_clock}"
            )
        if mapped_open > forbidden:
            raise LookaheadViolationError(
                f"{timeframe} mapped open {mapped_open} is later than forbidden {forbidden}"
            )
