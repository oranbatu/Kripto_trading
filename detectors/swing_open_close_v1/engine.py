"""Causal Swing Open, interior extreme, and Swing Close detection.

The extreme is the maximum interior High or the minimum interior Low.
Confirmation uses the Swing Close candle's close against the Swing Open
candle's open. The first return close for a direction is binding.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from statistics import pstdev
from typing import Sequence

from detectors.swing_open_close_v1.swing_config import (
    COMPACT_MAX_INTERIOR,
    COMPACT_MIN_INTERIOR,
    COMPACT_WIDTH,
    DIRECTION_PRIORITY,
    DISPLAY_TZ,
    EQUALITY_TOLERANCE_PERCENT,
    MAX_INTERIOR,
    STANDARD_MAX_INTERIOR,
    STANDARD_MIN_INTERIOR,
    STANDARD_WIDTH,
)

D0 = Decimal("0")
D1 = Decimal("1")
D2 = Decimal("2")
D100 = Decimal("100")
COMPACT_THRESHOLD = Decimal(COMPACT_WIDTH)
STANDARD_THRESHOLD = Decimal(STANDARD_WIDTH)
EQUALITY_TOLERANCE = Decimal(EQUALITY_TOLERANCE_PERCENT)

SWING_HIGH = "SWING_HIGH"
SWING_LOW = "SWING_LOW"
COMPACT_SWING = "COMPACT_SWING"
STANDARD_SWING = "STANDARD_SWING"

CONFIRMED_COMPACT = "CONFIRMED_COMPACT_SWING"
CONFIRMED_STANDARD = "CONFIRMED_STANDARD_SWING"
REJECTED_WIDTH_350 = "REJECTED_WIDTH_BELOW_3_50"
REJECTED_WIDTH_200 = "REJECTED_WIDTH_BELOW_2_00"
REJECTED_NO_INTERIOR = "REJECTED_NO_INTERIOR_EXTREME"
REJECTED_EXTREME_ON_OPEN = "REJECTED_EXTREME_ON_OPEN_CANDLE"
REJECTED_EXTREME_ON_CLOSE = "REJECTED_EXTREME_ON_CLOSE_CANDLE"
REJECTED_CONFLICT = "REJECTED_DIRECTION_CONFLICT"
EXPIRED = "EXPIRED_NO_RETURN_WITHIN_MAX_DURATION"
TERMINAL = "TERMINAL_DATASET_END"
CONFIRMED_STATUSES = {CONFIRMED_COMPACT, CONFIRMED_STANDARD}

EXACT_RETURN = "EXACT_REFERENCE_CLOSE"
CROSSED_RETURN = "CROSSED_REFERENCE_CLOSE"
STANDALONE = "STANDALONE"
OVERLAPPING = "OVERLAPPING"
CONTAINS_OTHER = "CONTAINS_OTHER_SWING"
CONTAINED_BY = "CONTAINED_BY_OTHER_SWING"


class SwingError(RuntimeError):
    """The detector stopped because an input or result broke a hard rule."""


@dataclass(slots=True)
class Bar:
    row: int
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = D0
    quote_volume: Decimal = D0
    trades: int = 0
    taker_base: Decimal = D0
    taker_quote: Decimal = D0


@dataclass(slots=True)
class Attempt:
    attempt_id: str
    direction: str
    open_row: int
    open_time: datetime
    reference: Decimal
    status: str
    swing_id: str | None = None
    formation_class: str | None = None
    close_row: int | None = None
    close_time: datetime | None = None
    close_price: Decimal | None = None
    extreme_row: int | None = None
    extreme_time: datetime | None = None
    extreme_price: Decimal | None = None
    extreme_rows: tuple[int, ...] = ()
    plateau_start: int | None = None
    plateau_end: int | None = None
    plateau_length: int = 0
    interior_count: int | None = None
    required_width: Decimal | None = None
    open_to_extreme_percent: Decimal | None = None
    close_to_extreme_percent: Decimal | None = None
    open_to_extreme_price: Decimal | None = None
    close_to_extreme_price: Decimal | None = None
    width_achieved_row: int | None = None
    width_achieved_time: datetime | None = None
    completion_type: str | None = None
    conflict: bool = False
    conflict_resolution: str | None = None
    terminal: bool = False


@dataclass(slots=True)
class Swing:
    swing_id: str
    sequence: int
    direction: str
    formation_class: str
    structure_label: str
    open_row: int
    extreme_row: int
    close_row: int
    open_time: datetime
    open_close_time: datetime
    extreme_open_time: datetime
    extreme_close_time: datetime
    close_open_time: datetime
    open_candle_high: Decimal
    open_candle_low: Decimal
    open_candle_close: Decimal
    extreme_candle_open: Decimal
    extreme_candle_high: Decimal
    extreme_candle_low: Decimal
    extreme_candle_close: Decimal
    close_candle_open: Decimal
    close_candle_high: Decimal
    close_candle_low: Decimal
    reference: Decimal
    extreme_price: Decimal
    close_price: Decimal
    open_to_extreme_price: Decimal
    open_to_extreme_percent: Decimal
    close_to_extreme_price: Decimal
    close_to_extreme_percent: Decimal
    required_width: Decimal
    completion_type: str
    completion_difference_price: Decimal
    completion_difference_percent: Decimal
    overshoot_price: Decimal
    overshoot_percent: Decimal
    interior_count: int
    total_candles: int
    left_bars: int
    right_bars: int
    extreme_position: int
    interior_position: int
    symmetric: bool
    asymmetry_difference: int
    asymmetry_ratio: Decimal
    plateau_start: int
    plateau_end: int
    plateau_length: int
    equal_extreme_rows: tuple[int, ...]
    equal_extreme_count: int
    duration_hours: Decimal
    duration_days: Decimal
    width_achieved_row: int
    confirmed_at: datetime
    quality_score: Decimal
    quality_label: str
    path_length: Decimal
    net_close_movement: Decimal
    adverse_before: Decimal
    adverse_after: Decimal
    earlier_reference_returns: int
    near_reference_before_close: int
    direction_changes: int
    close_volatility: Decimal
    extreme_prominence: Decimal
    reference_to_extreme_efficiency: Decimal
    extreme_to_close_efficiency: Decimal
    bullish_candles: int
    bearish_candles: int
    doji_candles: int
    up_closes: int
    down_closes: int
    base_volume: Decimal
    quote_volume: Decimal
    trades: int
    taker_base: Decimal
    taker_quote: Decimal
    average_volume: Decimal
    median_volume: Decimal
    max_volume_row: int
    extreme_volume_ratio: Decimal | None
    close_volume_ratio: Decimal | None
    overlap_class: str = STANDALONE
    overlap_count: int = 0
    same_type_overlap: int = 0
    opposite_type_overlap: int = 0
    contained_count: int = 0
    parent_ids: str = ""
    child_ids: str = ""
    shared_candles: int = 0
    shared_percent: Decimal = D0
    alternating_view: str = ""
    next_same_id: str | None = None
    next_opposite_id: str | None = None
    bars_to_next: int | None = None
    favorable_excursion: Decimal | None = None
    adverse_excursion: Decimal | None = None
    first_break_row: int | None = None
    first_close_beyond_row: int | None = None
    retest_extreme_row: int | None = None
    retest_reference_row: int | None = None
    forward_censored: bool = True
    forward_censor_reason: str = "DATASET_END"


@dataclass(slots=True)
class AnalysisResult:
    bars: list[Bar]
    attempts: list[Attempt]
    swings: list[Swing]
    warnings: list[str] = field(default_factory=list)


def iso_utc(dt: datetime) -> str:
    value = dt.astimezone(timezone.utc)
    if value.microsecond == 0:
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    millis = value.microsecond // 1000
    return value.strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}Z"


def iso_turkey(dt: datetime) -> str:
    value = dt.astimezone(DISPLAY_TZ)
    offset = value.strftime("%z")
    offset = offset[:3] + ":" + offset[3:]
    if value.microsecond == 0:
        return value.strftime("%Y-%m-%dT%H:%M:%S") + offset
    millis = value.microsecond // 1000
    return value.strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}" + offset


def duration_rejection(interior_count: int) -> str | None:
    """Return a rejection for a duration the scanner is not allowed to confirm."""
    if interior_count < 1:
        return REJECTED_NO_INTERIOR
    if interior_count > MAX_INTERIOR:
        return EXPIRED
    return None


def extreme_position_rejection(open_row: int, extreme_row: int, close_row: int) -> str | None:
    if extreme_row <= open_row:
        return REJECTED_EXTREME_ON_OPEN
    if extreme_row >= close_row:
        return REJECTED_EXTREME_ON_CLOSE
    if close_row - open_row - 1 < 1:
        return REJECTED_NO_INTERIOR
    return None


def both_widths_pass(open_percent: Decimal, close_percent: Decimal, threshold: Decimal) -> bool:
    return open_percent >= threshold and close_percent >= threshold


def formation_class(interior_count: int) -> tuple[str, Decimal] | None:
    if COMPACT_MIN_INTERIOR <= interior_count <= COMPACT_MAX_INTERIOR:
        return COMPACT_SWING, COMPACT_THRESHOLD
    if STANDARD_MIN_INTERIOR <= interior_count <= STANDARD_MAX_INTERIOR:
        return STANDARD_SWING, STANDARD_THRESHOLD
    return None


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    if value < low:
        return low
    if value > high:
        return high
    return value


def _open_width(direction: str, reference: Decimal, extreme: Decimal) -> tuple[Decimal, Decimal]:
    if direction == SWING_HIGH:
        price = extreme - reference
    else:
        price = reference - extreme
    return price, price / reference * D100


def _close_width(direction: str, close_price: Decimal, extreme: Decimal) -> tuple[Decimal, Decimal]:
    if close_price == 0:
        raise SwingError("swing close price must be non-zero")
    if direction == SWING_HIGH:
        price = extreme - close_price
    else:
        price = close_price - extreme
    return price, price / close_price * D100


def _plateau(rows: Sequence[int]) -> tuple[int, int, int]:
    end = rows[-1]
    start = end
    present = set(rows)
    while start - 1 in present:
        start -= 1
    return start, end, end - start + 1


def _first_width_row(
    bars: Sequence[Bar],
    open_row: int,
    close_row: int,
    direction: str,
    reference: Decimal,
    threshold: Decimal,
) -> int | None:
    running: Decimal | None = None
    for row in range(open_row + 1, close_row):
        price = bars[row].high if direction == SWING_HIGH else bars[row].low
        if running is None:
            running = price
        elif direction == SWING_HIGH:
            running = price if price > running else running
        else:
            running = price if price < running else running
        _price, percent = _open_width(direction, reference, running)
        if percent >= threshold:
            return row
    return None


def _returned(direction: str, close_price: Decimal, reference: Decimal) -> bool:
    if direction == SWING_HIGH:
        return close_price <= reference
    return close_price >= reference


def _completion_type(direction: str, close_price: Decimal, reference: Decimal) -> str:
    if close_price == reference:
        return EXACT_RETURN
    return CROSSED_RETURN


def _scan_direction(bars: Sequence[Bar], open_row: int, direction: str, attempt_id: str) -> Attempt:
    origin = bars[open_row]
    reference = origin.open
    if reference <= 0:
        raise SwingError("swing reference open must be positive")
    attempt = Attempt(
        attempt_id=attempt_id,
        direction=direction,
        open_row=open_row,
        open_time=origin.open_time,
        reference=reference,
        status=TERMINAL,
        terminal=True,
    )
    last_row = len(bars) - 1
    if open_row + 2 > last_row:
        return attempt
    max_close_row = open_row + MAX_INTERIOR + 1
    limit = min(max_close_row, last_row)
    full_window = last_row >= max_close_row
    extreme_price: Decimal | None = None
    extreme_rows: list[int] = []
    for close_row in range(open_row + 2, limit + 1):
        interior_row = close_row - 1
        price = bars[interior_row].high if direction == SWING_HIGH else bars[interior_row].low
        if extreme_price is None or (direction == SWING_HIGH and price > extreme_price) or (
            direction == SWING_LOW and price < extreme_price
        ):
            extreme_price = price
            extreme_rows = [interior_row]
        elif price == extreme_price:
            extreme_rows.append(interior_row)
        close_price = bars[close_row].close
        if not _returned(direction, close_price, reference):
            continue
        interior = close_row - open_row - 1
        classified = formation_class(interior)
        if classified is None or extreme_price is None:
            attempt.status = REJECTED_NO_INTERIOR if interior < 1 else EXPIRED
            return attempt
        formation, threshold = classified
        representative = extreme_rows[-1]
        position = extreme_position_rejection(open_row, representative, close_row)
        open_price, open_percent = _open_width(direction, reference, extreme_price)
        close_move, close_percent = _close_width(direction, close_price, extreme_price)
        plateau_start, plateau_end, plateau_length = _plateau(extreme_rows)
        achieved = _first_width_row(bars, open_row, close_row, direction, reference, threshold)
        attempt.close_row = close_row
        attempt.close_time = bars[close_row].close_time
        attempt.close_price = close_price
        attempt.extreme_row = representative
        attempt.extreme_time = bars[representative].open_time
        attempt.extreme_price = extreme_price
        attempt.extreme_rows = tuple(extreme_rows)
        attempt.plateau_start = plateau_start
        attempt.plateau_end = plateau_end
        attempt.plateau_length = plateau_length
        attempt.interior_count = interior
        attempt.formation_class = formation
        attempt.required_width = threshold
        attempt.open_to_extreme_price = open_price
        attempt.open_to_extreme_percent = open_percent
        attempt.close_to_extreme_price = close_move
        attempt.close_to_extreme_percent = close_percent
        attempt.width_achieved_row = achieved
        attempt.width_achieved_time = None if achieved is None else bars[achieved].close_time
        attempt.completion_type = _completion_type(direction, close_price, reference)
        attempt.terminal = False
        if position is not None:
            attempt.status = position
            return attempt
        if both_widths_pass(open_percent, close_percent, threshold):
            attempt.status = CONFIRMED_COMPACT if formation == COMPACT_SWING else CONFIRMED_STANDARD
        else:
            attempt.status = REJECTED_WIDTH_350 if formation == COMPACT_SWING else REJECTED_WIDTH_200
        return attempt
    attempt.extreme_price = extreme_price
    attempt.extreme_rows = tuple(extreme_rows)
    if extreme_rows:
        attempt.extreme_row = extreme_rows[-1]
        attempt.extreme_time = bars[attempt.extreme_row].open_time
        plateau_start, plateau_end, plateau_length = _plateau(extreme_rows)
        attempt.plateau_start = plateau_start
        attempt.plateau_end = plateau_end
        attempt.plateau_length = plateau_length
        attempt.interior_count = limit - open_row - 1
        _price, attempt.open_to_extreme_percent = _open_width(direction, reference, extreme_price)
        if full_window:
            attempt.width_achieved_row = _first_width_row(
                bars, open_row, limit, direction, reference, STANDARD_THRESHOLD
            )
            if attempt.width_achieved_row is not None:
                attempt.width_achieved_time = bars[attempt.width_achieved_row].close_time
    if full_window:
        attempt.status = EXPIRED
        attempt.terminal = False
    else:
        attempt.status = TERMINAL
        attempt.terminal = True
    return attempt


def _binding_width(attempt: Attempt) -> Decimal:
    if attempt.open_to_extreme_percent is None or attempt.close_to_extreme_percent is None:
        return D0
    return min(attempt.open_to_extreme_percent, attempt.close_to_extreme_percent)


def _resolve_conflict(high: Attempt, low: Attempt) -> tuple[Attempt, Attempt, str]:
    high_achieved = high.width_achieved_row
    low_achieved = low.width_achieved_row
    if high_achieved is not None and low_achieved is not None and high_achieved != low_achieved:
        winner = high if high_achieved < low_achieved else low
        reason = "EARLIER_WIDTH_ACHIEVEMENT"
    elif high.close_row is not None and low.close_row is not None and high.close_row != low.close_row:
        winner = high if high.close_row < low.close_row else low
        reason = "EARLIER_RETURN_CONFIRMATION"
    else:
        high_width = _binding_width(high)
        low_width = _binding_width(low)
        if high_width != low_width:
            winner = high if high_width > low_width else low
            reason = "LARGER_WIDTH"
        else:
            winner = high
            reason = DIRECTION_PRIORITY
    loser = low if winner is high else high
    winner.conflict = True
    loser.conflict = True
    winner.conflict_resolution = f"WINNER_{reason}"
    loser.conflict_resolution = f"LOSER_{reason}"
    loser.status = REJECTED_CONFLICT
    loser.swing_id = None
    return winner, loser, reason


def analyze(bars: Sequence[Bar]) -> AnalysisResult:
    if not bars:
        raise SwingError("at least one completed candle is required")
    for index, item in enumerate(bars):
        if item.row != index:
            raise SwingError("bar rows must equal their positions")
    attempts: list[Attempt] = []
    high_count = 0
    low_count = 0
    confirmed_attempts: list[Attempt] = []
    for open_row in range(len(bars)):
        high_count += 1
        low_count += 1
        high = _scan_direction(bars, open_row, SWING_HIGH, f"ATT-H-{high_count:06d}")
        low = _scan_direction(bars, open_row, SWING_LOW, f"ATT-L-{low_count:06d}")
        if (
            high.status in CONFIRMED_STATUSES
            and low.status in CONFIRMED_STATUSES
            and high.close_row == low.close_row
        ):
            _resolve_conflict(high, low)
        attempts.extend((high, low))
        if high.status in CONFIRMED_STATUSES:
            confirmed_attempts.append(high)
        if low.status in CONFIRMED_STATUSES:
            confirmed_attempts.append(low)
    confirmed_attempts.sort(key=lambda item: (item.close_row if item.close_row is not None else 0, item.open_row, item.direction))
    swings = _build_swings(bars, confirmed_attempts)
    _assign_labels(swings)
    _assign_alternating(swings)
    warnings = _assign_overlaps(swings)
    _assign_forward(bars, swings)
    result = AnalysisResult(bars=list(bars), attempts=attempts, swings=swings, warnings=warnings)
    return result


def _build_swings(bars: Sequence[Bar], attempts: Sequence[Attempt]) -> list[Swing]:
    swings: list[Swing] = []
    high_sequence = 0
    low_sequence = 0
    for attempt in attempts:
        if attempt.direction == SWING_HIGH:
            high_sequence += 1
            sequence = high_sequence
            swing_id = f"SWH-{sequence:06d}"
        else:
            low_sequence += 1
            sequence = low_sequence
            swing_id = f"SWL-{sequence:06d}"
        attempt.swing_id = swing_id
        swings.append(_swing_from_attempt(bars, attempt, swing_id, sequence))
    return swings


def _swing_from_attempt(bars: Sequence[Bar], attempt: Attempt, swing_id: str, sequence: int) -> Swing:
    if attempt.close_row is None or attempt.extreme_row is None or attempt.extreme_price is None or attempt.close_price is None:
        raise SwingError(f"{swing_id} is missing a completed interval")
    if attempt.formation_class is None or attempt.required_width is None:
        raise SwingError(f"{swing_id} is missing its formation class")
    origin = bars[attempt.open_row]
    extreme = bars[attempt.extreme_row]
    close = bars[attempt.close_row]
    window = list(bars[attempt.open_row:attempt.close_row + 1])
    closes = [item.close for item in window]
    left = attempt.extreme_row - attempt.open_row
    right = attempt.close_row - attempt.extreme_row
    difference = attempt.close_price - attempt.reference
    if attempt.direction == SWING_HIGH:
        overshoot = attempt.reference - attempt.close_price
    else:
        overshoot = attempt.close_price - attempt.reference
    if overshoot < 0:
        overshoot = D0
    achieved = attempt.width_achieved_row if attempt.width_achieved_row is not None else attempt.extreme_row
    path_to_extreme = _path_length(closes[: left + 1])
    path_to_close = _path_length(closes[left:])
    efficiency_out = _efficiency(abs(attempt.extreme_price - attempt.reference), path_to_extreme)
    efficiency_back = _efficiency(abs(attempt.close_price - attempt.extreme_price), path_to_close)
    balance = Decimal(min(left, right)) / Decimal(max(left, right))
    binding = min(attempt.open_to_extreme_percent or D0, attempt.close_to_extreme_percent or D0)
    if attempt.formation_class == COMPACT_SWING:
        width_component = _clamp((binding - COMPACT_THRESHOLD) / Decimal("6.50"), D0, D1)
    else:
        width_component = _clamp((binding - STANDARD_THRESHOLD) / Decimal("8.00"), D0, D1)
    return_error = abs(difference) / attempt.reference * D100
    precision = D1 - _clamp(return_error / D1, D0, D1)
    path_component = (efficiency_out + efficiency_back) / D2
    quality = D100 * (Decimal("0.45") * width_component + Decimal("0.25") * precision + Decimal("0.20") * path_component + Decimal("0.10") * balance)
    volumes = [item.volume for item in window]
    average_volume = sum(volumes, D0) / Decimal(len(volumes))
    median_volume = _median(volumes)
    max_volume_row = max(window, key=lambda item: (item.volume, item.row)).row
    extreme_ratio = None if average_volume == 0 else extreme.volume / average_volume
    close_ratio = None if average_volume == 0 else close.volume / average_volume
    prominence = _prominence(bars, attempt)
    return Swing(
        swing_id=swing_id,
        sequence=sequence,
        direction=attempt.direction,
        formation_class=attempt.formation_class,
        structure_label="",
        open_row=attempt.open_row,
        extreme_row=attempt.extreme_row,
        close_row=attempt.close_row,
        open_time=origin.open_time,
        open_close_time=origin.close_time,
        extreme_open_time=extreme.open_time,
        extreme_close_time=extreme.close_time,
        close_open_time=close.open_time,
        open_candle_high=origin.high,
        open_candle_low=origin.low,
        open_candle_close=origin.close,
        extreme_candle_open=extreme.open,
        extreme_candle_high=extreme.high,
        extreme_candle_low=extreme.low,
        extreme_candle_close=extreme.close,
        close_candle_open=close.open,
        close_candle_high=close.high,
        close_candle_low=close.low,
        reference=attempt.reference,
        extreme_price=attempt.extreme_price,
        close_price=attempt.close_price,
        open_to_extreme_price=attempt.open_to_extreme_price or D0,
        open_to_extreme_percent=attempt.open_to_extreme_percent or D0,
        close_to_extreme_price=attempt.close_to_extreme_price or D0,
        close_to_extreme_percent=attempt.close_to_extreme_percent or D0,
        required_width=attempt.required_width,
        completion_type=attempt.completion_type or "",
        completion_difference_price=difference,
        completion_difference_percent=difference / attempt.reference * D100,
        overshoot_price=overshoot,
        overshoot_percent=overshoot / attempt.reference * D100,
        interior_count=attempt.interior_count or 0,
        total_candles=attempt.interior_count + 2 if attempt.interior_count is not None else 0,
        left_bars=left,
        right_bars=right,
        extreme_position=left,
        interior_position=left,
        symmetric=left == right,
        asymmetry_difference=abs(left - right),
        asymmetry_ratio=balance,
        plateau_start=attempt.plateau_start or attempt.extreme_row,
        plateau_end=attempt.plateau_end or attempt.extreme_row,
        plateau_length=attempt.plateau_length,
        equal_extreme_rows=attempt.extreme_rows,
        equal_extreme_count=len(attempt.extreme_rows),
        duration_hours=Decimal(attempt.close_row - attempt.open_row),
        duration_days=Decimal(attempt.close_row - attempt.open_row) / Decimal("24"),
        width_achieved_row=achieved,
        confirmed_at=close.close_time,
        quality_score=quality,
        quality_label=_quality_label(quality),
        path_length=_path_length(closes),
        net_close_movement=close.close - origin.close,
        adverse_before=_adverse(attempt.direction, attempt.reference, closes[: left + 1], before=True),
        adverse_after=_adverse(attempt.direction, extreme.close, closes[left:], before=False),
        earlier_reference_returns=_earlier_returns(bars, attempt),
        near_reference_before_close=_near_reference(bars, attempt),
        direction_changes=_direction_changes(closes),
        close_volatility=_volatility(closes),
        extreme_prominence=prominence,
        reference_to_extreme_efficiency=efficiency_out,
        extreme_to_close_efficiency=efficiency_back,
        bullish_candles=sum(1 for item in window if item.close > item.open),
        bearish_candles=sum(1 for item in window if item.close < item.open),
        doji_candles=sum(1 for item in window if item.close == item.open),
        up_closes=sum(1 for previous, item in zip(window, window[1:]) if item.close > previous.close),
        down_closes=sum(1 for previous, item in zip(window, window[1:]) if item.close < previous.close),
        base_volume=sum((item.volume for item in window), D0),
        quote_volume=sum((item.quote_volume for item in window), D0),
        trades=sum(item.trades for item in window),
        taker_base=sum((item.taker_base for item in window), D0),
        taker_quote=sum((item.taker_quote for item in window), D0),
        average_volume=average_volume,
        median_volume=median_volume,
        max_volume_row=max_volume_row,
        extreme_volume_ratio=extreme_ratio,
        close_volume_ratio=close_ratio,
    )


def _path_length(closes: Sequence[Decimal]) -> Decimal:
    total = D0
    for previous, current in zip(closes, closes[1:]):
        total += abs(current - previous)
    return total


def _efficiency(net: Decimal, path: Decimal) -> Decimal:
    if path == 0:
        return D1 if net == 0 else D0
    return _clamp(net / path, D0, D1)


def _median(values: Sequence[Decimal]) -> Decimal:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / D2


def _adverse(direction: str, anchor: Decimal, closes: Sequence[Decimal], before: bool) -> Decimal:
    if not closes:
        return D0
    if direction == SWING_HIGH:
        return max(D0, anchor - min(closes))
    return max(D0, max(closes) - anchor)


def _earlier_returns(bars: Sequence[Bar], attempt: Attempt) -> int:
    count = 0
    for row in range(attempt.open_row + 2, attempt.close_row or attempt.open_row):
        if _returned(attempt.direction, bars[row].close, attempt.reference):
            count += 1
    return count


def _near_reference(bars: Sequence[Bar], attempt: Attempt) -> int:
    count = 0
    for row in range(attempt.open_row + 1, attempt.close_row or attempt.open_row):
        if _returned(attempt.direction, bars[row].close, attempt.reference):
            continue
        distance = abs(bars[row].close - attempt.reference) / attempt.reference * D100
        if distance <= EQUALITY_TOLERANCE:
            count += 1
    return count


def _direction_changes(closes: Sequence[Decimal]) -> int:
    signs: list[int] = []
    for previous, current in zip(closes, closes[1:]):
        if current > previous:
            signs.append(1)
        elif current < previous:
            signs.append(-1)
    changes = 0
    for previous, current in zip(signs, signs[1:]):
        if previous != current:
            changes += 1
    return changes


def _volatility(closes: Sequence[Decimal]) -> Decimal:
    returns: list[float] = []
    for previous, current in zip(closes, closes[1:]):
        if previous == 0:
            continue
        returns.append(float((current - previous) / previous))
    if len(returns) < 2:
        return D0
    return Decimal(str(pstdev(returns))) * D100


def _prominence(bars: Sequence[Bar], attempt: Attempt) -> Decimal:
    values = []
    for row in range(attempt.open_row + 1, attempt.close_row or attempt.open_row):
        values.append(bars[row].high if attempt.direction == SWING_HIGH else bars[row].low)
    unique = sorted(set(values), reverse=attempt.direction == SWING_HIGH)
    if len(unique) < 2 or attempt.extreme_price is None:
        return D0
    return abs(attempt.extreme_price - unique[1]) / attempt.reference * D100


def _quality_label(score: Decimal) -> str:
    if score >= Decimal("85"):
        return "EXCEPTIONAL"
    if score >= Decimal("70"):
        return "HIGH_QUALITY"
    if score >= Decimal("50"):
        return "SIGNIFICANT"
    if score >= Decimal("30"):
        return "MODERATE"
    return "BASIC_CONFIRMED"


def _assign_labels(swings: list[Swing]) -> None:
    previous_high: Decimal | None = None
    previous_low: Decimal | None = None
    for swing in swings:
        if swing.direction == SWING_HIGH:
            swing.structure_label = _label("HIGH", swing.extreme_price, previous_high)
            previous_high = swing.extreme_price
        else:
            swing.structure_label = _label("LOW", swing.extreme_price, previous_low)
            previous_low = swing.extreme_price


def _label(kind: str, price: Decimal, previous: Decimal | None) -> str:
    if previous is None:
        return "FIRST_SWING_HIGH" if kind == "HIGH" else "FIRST_SWING_LOW"
    difference = abs(price - previous) / previous * D100
    if difference <= EQUALITY_TOLERANCE:
        return "EH" if kind == "HIGH" else "EL"
    if kind == "HIGH":
        return "HH" if price > previous else "LH"
    return "HL" if price > previous else "LL"


def _assign_alternating(swings: list[Swing]) -> None:
    last_included: str | None = None
    for swing in swings:
        if last_included is None or swing.direction != last_included:
            swing.alternating_view = "INCLUDED"
            last_included = swing.direction
        else:
            swing.alternating_view = "SKIPPED_NON_ALTERNATING"


def _assign_overlaps(swings: list[Swing]) -> list[str]:
    warnings: list[str] = []
    for left in swings:
        parents: list[str] = []
        children: list[str] = []
        max_shared = 0
        for right in swings:
            if left.swing_id == right.swing_id:
                continue
            shared = _shared_candles(left, right)
            if shared <= 0:
                continue
            left.overlap_count += 1
            if left.direction == right.direction:
                left.same_type_overlap += 1
            else:
                left.opposite_type_overlap += 1
            max_shared = max(max_shared, shared)
            if _strictly_contains(right, left):
                parents.append(right.swing_id)
            if _strictly_contains(left, right):
                children.append(right.swing_id)
                left.contained_count += 1
        left.shared_candles = max_shared
        left.shared_percent = D0 if left.total_candles == 0 else Decimal(max_shared) / Decimal(left.total_candles) * D100
        left.parent_ids = _join_ids(parents)
        left.child_ids = _join_ids(children)
        if len(left.parent_ids) >= 32767 or len(left.child_ids) >= 32767:
            warnings.append(f"{left.swing_id} overlap id list was truncated")
        if parents:
            left.overlap_class = CONTAINED_BY
        elif children:
            left.overlap_class = CONTAINS_OTHER
        elif left.overlap_count:
            left.overlap_class = OVERLAPPING
        else:
            left.overlap_class = STANDALONE
    return warnings


def _shared_candles(left: Swing, right: Swing) -> int:
    start = max(left.open_row, right.open_row)
    end = min(left.close_row, right.close_row)
    return max(0, end - start + 1)


def _strictly_contains(parent: Swing, child: Swing) -> bool:
    covers = parent.open_row <= child.open_row and parent.close_row >= child.close_row
    longer = (parent.close_row - parent.open_row) > (child.close_row - child.open_row)
    return covers and longer


def _join_ids(ids: Sequence[str]) -> str:
    text = ",".join(ids)
    if len(text) <= 32767:
        return text
    return text[:32750] + ",TRUNCATED"


def _assign_forward(bars: Sequence[Bar], swings: list[Swing]) -> None:
    if not bars:
        return
    suffix_high = [D0] * len(bars)
    suffix_low = [D0] * len(bars)
    suffix_high[-1] = bars[-1].high
    suffix_low[-1] = bars[-1].low
    for index in range(len(bars) - 2, -1, -1):
        suffix_high[index] = bars[index].high if bars[index].high > suffix_high[index + 1] else suffix_high[index + 1]
        suffix_low[index] = bars[index].low if bars[index].low < suffix_low[index + 1] else suffix_low[index + 1]
    for index, swing in enumerate(swings):
        later = swings[index + 1:]
        same = next((item for item in later if item.direction == swing.direction), None)
        opposite = next((item for item in later if item.direction != swing.direction), None)
        nxt = same if same is not None else opposite
        if same is not None and opposite is not None and opposite.close_row < same.close_row:
            nxt = opposite
        elif same is not None:
            nxt = same
        swing.next_same_id = None if same is None else same.swing_id
        swing.next_opposite_id = None if opposite is None else opposite.swing_id
        if nxt is not None:
            swing.bars_to_next = nxt.close_row - swing.close_row
        if swing.close_row + 1 < len(bars):
            if swing.direction == SWING_HIGH:
                swing.favorable_excursion = max(D0, suffix_high[swing.close_row + 1] - swing.extreme_price)
                swing.adverse_excursion = max(D0, swing.extreme_price - suffix_low[swing.close_row + 1])
            else:
                swing.favorable_excursion = max(D0, swing.extreme_price - suffix_low[swing.close_row + 1])
                swing.adverse_excursion = max(D0, suffix_high[swing.close_row + 1] - swing.extreme_price)
            _first_forward_events(bars, swing)
        swing.forward_censored = True
        swing.forward_censor_reason = "DATASET_END"


def _first_forward_events(bars: Sequence[Bar], swing: Swing) -> None:
    for bar in bars[swing.close_row + 1:]:
        if swing.direction == SWING_HIGH:
            broke = bar.high > swing.extreme_price
            closed_beyond = bar.close > swing.extreme_price
        else:
            broke = bar.low < swing.extreme_price
            closed_beyond = bar.close < swing.extreme_price
        retest_extreme = bar.low <= swing.extreme_price <= bar.high
        retest_reference = bar.low <= swing.reference <= bar.high
        if broke and swing.first_break_row is None:
            swing.first_break_row = bar.row
        if closed_beyond and swing.first_close_beyond_row is None:
            swing.first_close_beyond_row = bar.row
        if retest_extreme and swing.retest_extreme_row is None:
            swing.retest_extreme_row = bar.row
        if retest_reference and swing.retest_reference_row is None:
            swing.retest_reference_row = bar.row
        if (
            swing.first_break_row is not None
            and swing.first_close_beyond_row is not None
            and swing.retest_extreme_row is not None
            and swing.retest_reference_row is not None
        ):
            return


def audit_result(result: AnalysisResult) -> None:
    if len(result.attempts) != len(result.bars) * 2:
        raise SwingError("attempt count is not one row per open and direction")
    seen_attempts = {(item.open_row, item.direction) for item in result.attempts}
    if len(seen_attempts) != len(result.attempts):
        raise SwingError("duplicate directional attempts")
    confirmed = [item for item in result.attempts if item.status in CONFIRMED_STATUSES]
    if len(confirmed) != len(result.swings):
        raise SwingError("confirmed attempts and swings differ")
    keys: set[tuple[str, int, int, int]] = set()
    ids: set[str] = set()
    for swing in result.swings:
        if swing.swing_id in ids:
            raise SwingError(f"duplicate swing id {swing.swing_id}")
        ids.add(swing.swing_id)
        key = (swing.direction, swing.open_row, swing.extreme_row, swing.close_row)
        if key in keys:
            raise SwingError(f"duplicate confirmed key {key}")
        keys.add(key)
        _audit_swing(result.bars, swing)
    high_ids = [swing.swing_id for swing in result.swings if swing.direction == SWING_HIGH]
    low_ids = [swing.swing_id for swing in result.swings if swing.direction == SWING_LOW]
    if high_ids != [f"SWH-{index:06d}" for index in range(1, len(high_ids) + 1)]:
        raise SwingError("swing high ids are not sequential")
    if low_ids != [f"SWL-{index:06d}" for index in range(1, len(low_ids) + 1)]:
        raise SwingError("swing low ids are not sequential")


def _audit_swing(bars: Sequence[Bar], swing: Swing) -> None:
    if not (swing.open_row < swing.extreme_row < swing.close_row):
        raise SwingError(f"{swing.swing_id} extreme is not interior")
    interior = swing.close_row - swing.open_row - 1
    if swing.interior_count != interior:
        raise SwingError(f"{swing.swing_id} interior count mismatch")
    classified = formation_class(interior)
    if classified is None or classified[0] != swing.formation_class:
        raise SwingError(f"{swing.swing_id} formation class mismatch")
    _name, threshold = classified
    if swing.direction == SWING_HIGH:
        extreme = max(bars[row].high for row in range(swing.open_row + 1, swing.close_row))
        if bars[swing.close_row].close > swing.reference:
            raise SwingError(f"{swing.swing_id} high close did not return")
    else:
        extreme = min(bars[row].low for row in range(swing.open_row + 1, swing.close_row))
        if bars[swing.close_row].close < swing.reference:
            raise SwingError(f"{swing.swing_id} low close did not return")
    if extreme != swing.extreme_price:
        raise SwingError(f"{swing.swing_id} extreme price is not the interior extreme")
    if bars[swing.extreme_row].high != extreme and swing.direction == SWING_HIGH:
        raise SwingError(f"{swing.swing_id} representative candle does not contain the high")
    if bars[swing.extreme_row].low != extreme and swing.direction == SWING_LOW:
        raise SwingError(f"{swing.swing_id} representative candle does not contain the low")
    open_price, open_percent = _open_width(swing.direction, swing.reference, extreme)
    close_price, close_percent = _close_width(swing.direction, bars[swing.close_row].close, extreme)
    if open_percent != swing.open_to_extreme_percent or close_percent != swing.close_to_extreme_percent:
        raise SwingError(f"{swing.swing_id} widths were not recomputed from stored prices")
    if not both_widths_pass(open_percent, close_percent, threshold):
        raise SwingError(f"{swing.swing_id} failed a width rule")
    for row in range(swing.open_row + 2, swing.close_row):
        if _returned(swing.direction, bars[row].close, swing.reference):
            raise SwingError(f"{swing.swing_id} used a later close after an earlier return")
    if swing.confirmed_at != bars[swing.close_row].close_time:
        raise SwingError(f"{swing.swing_id} confirmation is not the swing close completion")
    if swing.reference != bars[swing.open_row].open:
        raise SwingError(f"{swing.swing_id} reference is not the swing open")
    if swing.earlier_reference_returns != 0:
        raise SwingError(f"{swing.swing_id} has an earlier reference return")
