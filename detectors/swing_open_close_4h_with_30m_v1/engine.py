"""2026 BTCUSDT 4h Swing Open, interior extreme, and Swing Close detector."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from statistics import pstdev
from typing import Sequence

from detectors.swing_open_close_4h_with_30m_v1.swing_config import (
    ALTERNATIVE_MAX_INTERIOR,
    ALTERNATIVE_MIN_INTERIOR,
    ANALYSIS_END_OPEN,
    ANALYSIS_START,
    ATR_PERIOD,
    BAR_HOURS,
    COMPACT_MAX_INTERIOR,
    COMPACT_MIN_INTERIOR,
    COMPACT_WIDTH,
    DIRECTION_PRIORITY,
    DISPLAY_TZ,
    DURATION_GAP_INTERIOR,
    EQUALITY_TOLERANCE_PERCENT,
    HOURS_PER_DAY,
    MAX_INTERIOR,
    SEARCH_ONLY_MIN_INTERIOR,
    PROMINENCE_SENTINEL,
    QUALITY_COMPACT_WIDTH_SPAN,
    QUALITY_EXCEPTIONAL_MIN,
    QUALITY_HIGH_MIN,
    QUALITY_MODERATE_MIN,
    QUALITY_RETURN_SCALE,
    QUALITY_SIGNIFICANT_MIN,
    QUALITY_STANDARD_WIDTH_SPAN,
    QUALITY_WEIGHT_BALANCE,
    QUALITY_WEIGHT_PATH,
    QUALITY_WEIGHT_PRECISION,
    QUALITY_WEIGHT_WIDTH,
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
QUALITY_COMPACT_SPAN = Decimal(QUALITY_COMPACT_WIDTH_SPAN)
QUALITY_STANDARD_SPAN = Decimal(QUALITY_STANDARD_WIDTH_SPAN)
QUALITY_W_WIDTH = Decimal(QUALITY_WEIGHT_WIDTH)
QUALITY_W_PRECISION = Decimal(QUALITY_WEIGHT_PRECISION)
QUALITY_W_PATH = Decimal(QUALITY_WEIGHT_PATH)
QUALITY_W_BALANCE = Decimal(QUALITY_WEIGHT_BALANCE)
QUALITY_PRECISION_SCALE = Decimal(QUALITY_RETURN_SCALE)
PROMINENCE_FLOOR = Decimal(PROMINENCE_SENTINEL)
BAR_SPAN = timedelta(hours=BAR_HOURS) - timedelta(milliseconds=1)

SWING_HIGH = "SWING_HIGH"
SWING_LOW = "SWING_LOW"
COMPACT_SWING = "COMPACT"
STANDARD_SWING = "STANDARD"
ALTERNATIVE_CLASS = "ALTERNATIVE"
REFERENCE_METHOD = "REFERENCE_RETURN"
ALTERNATIVE_METHOD = "ALTERNATIVE_3_5_WIDTH"
INVALID_DURATION_GAP = "INVALID_DURATION_GAP"
CONFIRMED_COMPACT = "CONFIRMED_RAW_COMPACT_SWING"
CONFIRMED_STANDARD = "CONFIRMED_RAW_STANDARD_SWING"
CONFIRMED_ALTERNATIVE = "CONFIRMED_RAW_ALTERNATIVE_SWING"
PRIMARY = "PRIMARY_SWING"
DERIVED = "DERIVED_SAME_EXTREME"
LONGER_SAME_EXTREME = "LONGER_FORMATION_FOR_SAME_EXTREME"
EQUAL_LENGTH_LOSS = "EQUAL_MINIMUM_LENGTH_TIE_BREAK_LOSS"
SELECTION_REASONS = (
    "MINIMUM_TOTAL_FORMATION_CANDLES",
    "MINIMUM_ABSOLUTE_COMPLETION_ERROR_PERCENT",
    "MAXIMUM_MINIMUM_OF_TWO_WIDTHS",
    "EARLIEST_CONFIRMATION",
    "LATEST_SWING_OPEN",
    "LOWEST_STABLE_RAW_SWING_ID",
)
REJECTED_WIDTH_350 = "REJECTED_WIDTH_BELOW_3_50"
REJECTED_WIDTH_250 = "REJECTED_WIDTH_BELOW_2_50"
REFERENCE_OPEN_WIDTH = "REFERENCE_OPEN_WIDTH_BELOW_THRESHOLD"
REFERENCE_CLOSE_WIDTH = "REFERENCE_CLOSE_WIDTH_BELOW_THRESHOLD"
REFERENCE_BOTH_WIDTHS = "REFERENCE_BOTH_WIDTHS_BELOW_THRESHOLD"
REJECTED_DURATION_GAP = "REJECTED_REFERENCE_DURATION_GAP_7_INTERIOR_BARS"
ALTERNATIVE_DURATION_BELOW = "ALTERNATIVE_DURATION_BELOW_4"
ALTERNATIVE_DURATION_ABOVE = "ALTERNATIVE_DURATION_ABOVE_10"
ALTERNATIVE_OPEN_WIDTH = "ALTERNATIVE_OPEN_WIDTH_BELOW_3_5"
ALTERNATIVE_CLOSE_WIDTH = "ALTERNATIVE_CLOSE_WIDTH_BELOW_3_5"
ALTERNATIVE_BOTH_WIDTHS = "ALTERNATIVE_BOTH_WIDTHS_BELOW_3_5"
ALTERNATIVE_REFERENCE_ACHIEVED = "ALTERNATIVE_REFERENCE_RETURN_ALREADY_ACHIEVED"
ALTERNATIVE_WRONG_SIDE = "ALTERNATIVE_WRONG_SIDE_OF_REFERENCE"
ALTERNATIVE_NONE = "ALTERNATIVE_VALID_DURATION_WINDOW_EXHAUSTED"
REFERENCE_ABOVE_20 = "REFERENCE_STANDARD_DURATION_ABOVE_20"
HARD_SEARCH_HORIZON_ERROR = "HARD_SEARCH_HORIZON_ERROR"
REJECTED_NO_INTERIOR = "REJECTED_NO_INTERIOR_EXTREME"
REJECTED_EXTREME_ON_OPEN = "REJECTED_EXTREME_ON_OPEN_CANDLE"
REJECTED_EXTREME_ON_CLOSE = "REJECTED_EXTREME_ON_CLOSE_CANDLE"
REJECTED_CONFLICT = "REJECTED_DIRECTION_CONFLICT"
EXPIRED = "SEARCH_HORIZON_EXHAUSTED_AT_25_INTERIOR_CANDLES"
TERMINAL = "SEARCH_CENSORED_BY_DATASET_END"
CONFIRMED_STATUSES = {CONFIRMED_COMPACT, CONFIRMED_STANDARD, CONFIRMED_ALTERNATIVE}
EXACT_RETURN = "EXACT_REFERENCE_CLOSE"
CROSSED_RETURN = "CROSSED_REFERENCE_CLOSE"
STANDALONE = "STANDALONE"
OVERLAPPING = "OVERLAPPING"
CONTAINS_OTHER = "CONTAINS_OTHER_PRIMARY"
CONTAINED_BY = "CONTAINED_BY_OTHER_PRIMARY"


class SwingError(RuntimeError):
    """The 4h swing window or a formation rule failed."""


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
    atr: Decimal | None = None
    atr_available: bool = False


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
    applicable_minimum_width: str | None = None
    applicable_rule_id: str | None = None
    open_width_pass: bool | None = None
    close_width_pass: bool | None = None
    both_widths_passed: bool | None = None
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
    family_id: str | None = None
    primary_swing_id: str | None = None
    consolidation_status: str | None = None
    derivation_reason: str | None = None
    detection_method: str = REFERENCE_METHOD
    found_through_alternative: bool = False
    reference_return_required: bool = True
    reference_return_achieved: bool = False
    maximum_search_interior: int = MAX_INTERIOR
    actual_search_interior: int = 0
    search_horizon_end_row: int | None = None
    search_horizon_end_time: datetime | None = None
    search_horizon_exhausted: bool = False
    search_censored: bool = False
    maximum_observable_interior: int = 0
    missing_search_candles: int = 0
    last_available_time: datetime | None = None
    search_terminal_reason: str = ""
    reference_binding_found: bool = False
    reference_binding_interior: int | None = None
    alternative_window_entered: bool = False
    alternative_window_exhausted: bool = False
    alternative_binding_found: bool = False
    evaluations_within_horizon: int = 0
    evaluations_beyond_horizon: int = 0


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
    left_duration_hours: Decimal
    right_duration_hours: Decimal
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
    coverage_hours: Decimal
    duration_days: Decimal
    width_achieved_row: int
    width_achieved_time: datetime | None
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
    open_body: Decimal
    extreme_body: Decimal
    close_body: Decimal
    open_upper_wick: Decimal
    open_lower_wick: Decimal
    extreme_upper_wick: Decimal
    extreme_lower_wick: Decimal
    close_upper_wick: Decimal
    close_lower_wick: Decimal
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
    inside_2026_window: bool = True
    role: str = ""
    family_id: str = ""
    extreme_event_id: str = ""
    family_size: int = 0
    derived_member_count: int = 0
    primary_id: str = ""
    primary_sequence: int = 0
    primary_rank: int = 0
    selection_reason: str = ""
    tie_break_reason: str = ""
    derivation_reason: str = ""
    primary_known_at: datetime | None = None
    event_run_start: int = 0
    event_run_end: int = 0
    extra_candles: int = 0
    compared_total_candles: int = 0
    compared_interior_count: int = 0
    open_row_delta: int = 0
    close_row_delta: int = 0
    duration_delta_hours: Decimal = D0
    width_delta: Decimal = D0
    return_error_delta: Decimal = D0
    applicable_rule_id: str = ""
    open_width_pass: bool = False
    close_width_pass: bool = False
    both_widths_passed: bool = False
    detection_method: str = REFERENCE_METHOD
    found_through_alternative: bool = False
    reference_return_required: bool = True
    reference_return_achieved: bool = False
    later_reference_return_row: int | None = None
    maximum_search_interior: int = MAX_INTERIOR
    actual_search_interior: int = 0
    search_horizon_end_row: int | None = None
    search_horizon_exhausted: bool = False
    search_censored: bool = False
    maximum_observable_interior: int = 0
    missing_search_candles: int = 0
    search_terminal_reason: str = ""
    reference_binding_found: bool = False
    reference_binding_interior: int | None = None
    alternative_window_entered: bool = False
    alternative_window_exhausted: bool = False
    alternative_binding_found: bool = False
    evaluations_within_horizon: int = 0
    evaluations_beyond_horizon: int = 0


@dataclass(slots=True)
class Family:
    family_id: str
    event_id: str
    direction: str
    extreme_price: Decimal
    run_start: int
    run_end: int
    known_at: datetime
    selection_reason: str
    members: list[Swing]
    primary: Swing
    reference_member_count: int = 0
    alternative_member_count: int = 0
    cross_method: bool = False


@dataclass(slots=True)
class AnalysisResult:
    bars: list[Bar]
    attempts: list[Attempt]
    swings: list[Swing]
    primaries: list[Swing] = field(default_factory=list)
    derived: list[Swing] = field(default_factory=list)
    families: list[Family] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    internal_counters: dict[str, int] = field(default_factory=dict)


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
    if interior_count < COMPACT_MIN_INTERIOR:
        return REJECTED_NO_INTERIOR
    if interior_count == DURATION_GAP_INTERIOR:
        return REJECTED_DURATION_GAP
    if interior_count > STANDARD_MAX_INTERIOR:
        return REFERENCE_ABOVE_20
    return None


def applicable_width_text(interior_count: int) -> str:
    if COMPACT_MIN_INTERIOR <= interior_count <= COMPACT_MAX_INTERIOR:
        return format(COMPACT_THRESHOLD, ".8f")
    if interior_count == DURATION_GAP_INTERIOR:
        return "NOT_APPLICABLE"
    if STANDARD_MIN_INTERIOR <= interior_count <= STANDARD_MAX_INTERIOR:
        return format(STANDARD_THRESHOLD, ".8f")
    return "NOT_APPLICABLE"


def applicable_rule_id(interior_count: int) -> str:
    if COMPACT_MIN_INTERIOR <= interior_count <= COMPACT_MAX_INTERIOR:
        return "COMPACT_1_TO_6_WIDTH_3_50"
    if interior_count == DURATION_GAP_INTERIOR:
        return "DURATION_GAP_7_INVALID"
    if STANDARD_MIN_INTERIOR <= interior_count <= STANDARD_MAX_INTERIOR:
        return "STANDARD_8_TO_20_WIDTH_2_00"
    if SEARCH_ONLY_MIN_INTERIOR <= interior_count <= MAX_INTERIOR:
        return "REFERENCE_SEARCH_ONLY_21_TO_25"
    return "BEYOND_SEARCH_HORIZON"


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


def assert_2026_window(bars: Sequence[Bar]) -> None:
    """Reject any candle outside the authorized 2026 4h window."""
    for bar in bars:
        if bar.open_time < ANALYSIS_START or bar.open_time > ANALYSIS_END_OPEN:
            raise SwingError(f"candle outside the 2026 window: {iso_utc(bar.open_time)}")
        if bar.close_time != bar.open_time + BAR_SPAN:
            raise SwingError(f"4h close time mismatch at {iso_utc(bar.open_time)}")
        if bar.close_time > ANALYSIS_END_OPEN + BAR_SPAN:
            raise SwingError(f"close extends beyond the 2026 window: {iso_utc(bar.close_time)}")


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    if value < low:
        return low
    if value > high:
        return high
    return value


def _open_width(direction: str, reference: Decimal, extreme: Decimal) -> tuple[Decimal, Decimal]:
    price = extreme - reference if direction == SWING_HIGH else reference - extreme
    return price, price / reference * D100


def _close_width(direction: str, close_price: Decimal, extreme: Decimal) -> tuple[Decimal, Decimal]:
    if close_price == 0:
        raise SwingError("swing close price must be non-zero")
    price = extreme - close_price if direction == SWING_HIGH else close_price - extreme
    return price, price / close_price * D100


def _plateau(rows: Sequence[int]) -> tuple[int, int, int]:
    end = rows[-1]
    start = end
    for row in reversed(rows[:-1]):
        if row == start - 1:
            start = row
        else:
            break
    return start, end, end - start + 1


def _first_width_row(bars, open_row, close_row, direction, reference, threshold) -> int | None:
    running = None
    for row in range(open_row + 1, close_row):
        price = bars[row].high if direction == SWING_HIGH else bars[row].low
        if running is None or (direction == SWING_HIGH and price > running) or (direction == SWING_LOW and price < running):
            running = price
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


def _observable_interior(bars: Sequence[Bar], open_row: int) -> int:
    return max(0, len(bars) - 1 - open_row - 1)


def _stamp_search(attempt: Attempt, bars: Sequence[Bar], inspected: int, method_limit: int) -> None:
    observable = _observable_interior(bars, attempt.open_row)
    end_row = attempt.open_row + MAX_INTERIOR + 1
    attempt.maximum_search_interior = MAX_INTERIOR
    attempt.maximum_observable_interior = observable
    attempt.missing_search_candles = max(0, MAX_INTERIOR - observable)
    attempt.search_horizon_end_row = end_row
    attempt.last_available_time = bars[-1].close_time if bars else None
    if 0 <= end_row < len(bars):
        attempt.search_horizon_end_time = bars[end_row].open_time
    attempt.evaluations_within_horizon = inspected
    if attempt.evaluations_beyond_horizon or (attempt.actual_search_interior > MAX_INTERIOR):
        attempt.status = HARD_SEARCH_HORIZON_ERROR
        attempt.search_terminal_reason = HARD_SEARCH_HORIZON_ERROR
        raise SwingError(HARD_SEARCH_HORIZON_ERROR)
    if attempt.status == CONFIRMED_ALTERNATIVE:
        attempt.alternative_binding_found = True
    if attempt.detection_method == REFERENCE_METHOD and attempt.reference_return_achieved and attempt.interior_count is not None:
        attempt.actual_search_interior = attempt.interior_count
        attempt.reference_binding_found = True
        attempt.reference_binding_interior = attempt.interior_count
    elif attempt.alternative_binding_found and attempt.interior_count is not None:
        attempt.actual_search_interior = attempt.interior_count
    elif attempt.status == EXPIRED:
        attempt.actual_search_interior = MAX_INTERIOR
        attempt.search_horizon_exhausted = True
    elif attempt.status == ALTERNATIVE_NONE:
        attempt.actual_search_interior = ALTERNATIVE_MAX_INTERIOR
        attempt.alternative_window_entered = True
        attempt.alternative_window_exhausted = True
    else:
        reached = min(method_limit, observable) if inspected else 0
        attempt.actual_search_interior = reached
        attempt.search_censored = True
    if attempt.detection_method == ALTERNATIVE_METHOD and attempt.actual_search_interior >= ALTERNATIVE_MIN_INTERIOR:
        attempt.alternative_window_entered = True
    attempt.search_terminal_reason = attempt.status
    if attempt.actual_search_interior > MAX_INTERIOR:
        attempt.status = HARD_SEARCH_HORIZON_ERROR
        raise SwingError(HARD_SEARCH_HORIZON_ERROR)


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
    inspected = 0
    if open_row + 2 > last_row:
        _stamp_search(attempt, bars, inspected, MAX_INTERIOR)
        return attempt
    max_close_row = open_row + MAX_INTERIOR + 1
    limit = min(max_close_row, last_row)
    full_window = last_row >= max_close_row
    extreme_price = None
    extreme_rows: list[int] = []
    for close_row in range(open_row + 2, limit + 1):
        interior = close_row - open_row - 1
        inspected += 1
        if interior > MAX_INTERIOR:
            attempt.evaluations_beyond_horizon += 1
            attempt.status = HARD_SEARCH_HORIZON_ERROR
            _stamp_search(attempt, bars, inspected, MAX_INTERIOR)
            return attempt
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
        if extreme_price is None:
            attempt.status = REJECTED_NO_INTERIOR
            _stamp_search(attempt, bars, inspected, MAX_INTERIOR)
            return attempt
        representative = extreme_rows[-1]
        position = extreme_position_rejection(open_row, representative, close_row)
        open_price, open_percent = _open_width(direction, reference, extreme_price)
        close_move, close_percent = _close_width(direction, close_price, extreme_price)
        plateau_start, plateau_end, plateau_length = _plateau(extreme_rows)
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
        attempt.open_to_extreme_price = open_price
        attempt.open_to_extreme_percent = open_percent
        attempt.close_to_extreme_price = close_move
        attempt.close_to_extreme_percent = close_percent
        attempt.completion_type = _completion_type(direction, close_price, reference)
        attempt.applicable_minimum_width = applicable_width_text(interior)
        attempt.applicable_rule_id = applicable_rule_id(interior)
        attempt.terminal = False
        attempt.reference_return_achieved = True
        if interior == DURATION_GAP_INTERIOR:
            attempt.formation_class = INVALID_DURATION_GAP
            attempt.required_width = None
            attempt.open_width_pass = None
            attempt.close_width_pass = None
            attempt.both_widths_passed = False
            attempt.status = REJECTED_DURATION_GAP
            _stamp_search(attempt, bars, inspected, MAX_INTERIOR)
            return attempt
        classified = formation_class(interior)
        if classified is None:
            attempt.status = REFERENCE_ABOVE_20 if interior > STANDARD_MAX_INTERIOR else REJECTED_NO_INTERIOR
            _stamp_search(attempt, bars, inspected, MAX_INTERIOR)
            return attempt
        formation, threshold = classified
        achieved = _first_width_row(bars, open_row, close_row, direction, reference, threshold)
        attempt.formation_class = formation
        attempt.required_width = threshold
        attempt.width_achieved_row = achieved
        attempt.width_achieved_time = None if achieved is None else bars[achieved].close_time
        attempt.open_width_pass = open_percent >= threshold
        attempt.close_width_pass = close_percent >= threshold
        passed = both_widths_pass(open_percent, close_percent, threshold)
        attempt.both_widths_passed = passed
        if position is not None:
            attempt.status = position
            _stamp_search(attempt, bars, inspected, MAX_INTERIOR)
            return attempt
        if passed:
            attempt.status = CONFIRMED_COMPACT if formation == COMPACT_SWING else CONFIRMED_STANDARD
        else:
            attempt.status = _reference_width_rejection(bool(attempt.open_width_pass), bool(attempt.close_width_pass))
        _stamp_search(attempt, bars, inspected, MAX_INTERIOR)
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
    _stamp_search(attempt, bars, inspected, MAX_INTERIOR)
    return attempt


def _reference_width_rejection(open_pass: bool, close_pass: bool) -> str:
    if not open_pass and not close_pass:
        return REFERENCE_BOTH_WIDTHS
    if not open_pass:
        return REFERENCE_OPEN_WIDTH
    return REFERENCE_CLOSE_WIDTH


def alternative_duration_rejection(interior_count: int) -> str | None:
    if interior_count < ALTERNATIVE_MIN_INTERIOR:
        return ALTERNATIVE_DURATION_BELOW
    if interior_count > ALTERNATIVE_MAX_INTERIOR:
        return ALTERNATIVE_DURATION_ABOVE
    return None


def _alternative_width_rejection(open_pass: bool, close_pass: bool) -> str:
    if not open_pass and not close_pass:
        return ALTERNATIVE_BOTH_WIDTHS
    if not open_pass:
        return ALTERNATIVE_OPEN_WIDTH
    return ALTERNATIVE_CLOSE_WIDTH


def _count(counters: dict[str, int], name: str) -> None:
    counters[name] = counters.get(name, 0) + 1


def _scan_alternative(bars: Sequence[Bar], open_row: int, direction: str, attempt_id: str, counters: dict[str, int]) -> Attempt:
    origin = bars[open_row]
    reference = origin.open
    attempt = Attempt(
        attempt_id=attempt_id,
        direction=direction,
        open_row=open_row,
        open_time=origin.open_time,
        reference=reference,
        status=TERMINAL,
        terminal=True,
        detection_method=ALTERNATIVE_METHOD,
        found_through_alternative=True,
        reference_return_required=False,
        reference_return_achieved=False,
    )
    last_row = len(bars) - 1
    inspected = 0
    first_close = open_row + ALTERNATIVE_MIN_INTERIOR + 1
    last_close = open_row + ALTERNATIVE_MAX_INTERIOR + 1
    if first_close > last_row:
        _count(counters, "INTERNAL_EVALUATION_COUNTER_ONLY:ALTERNATIVE_WINDOW_NOT_REACHED")
        _stamp_search(attempt, bars, inspected, ALTERNATIVE_MAX_INTERIOR)
        return attempt
    limit = min(last_close, last_row)
    full_window = last_row >= last_close
    extreme_price = None
    extreme_rows: list[int] = []
    for close_row in range(open_row + 2, limit + 1):
        interior = close_row - open_row - 1
        inspected += 1
        if interior > ALTERNATIVE_MAX_INTERIOR or interior > MAX_INTERIOR:
            attempt.evaluations_beyond_horizon += 1
            attempt.status = HARD_SEARCH_HORIZON_ERROR
            _stamp_search(attempt, bars, inspected, ALTERNATIVE_MAX_INTERIOR)
            return attempt
        interior_row = close_row - 1
        price = bars[interior_row].high if direction == SWING_HIGH else bars[interior_row].low
        if extreme_price is None or (direction == SWING_HIGH and price > extreme_price) or (
            direction == SWING_LOW and price < extreme_price
        ):
            extreme_price = price
            extreme_rows = [interior_row]
        elif price == extreme_price:
            extreme_rows.append(interior_row)
        if interior < ALTERNATIVE_MIN_INTERIOR:
            _count(counters, "INTERNAL_EVALUATION_COUNTER_ONLY:ALTERNATIVE_DURATION_BELOW_4")
            continue
        close_price = bars[close_row].close
        if _returned(direction, close_price, reference):
            _count(counters, "INTERNAL_EVALUATION_COUNTER_ONLY:ALTERNATIVE_REFERENCE_RETURN_ALREADY_ACHIEVED")
            continue
        if extreme_price is None:
            continue
        open_price, open_percent = _open_width(direction, reference, extreme_price)
        close_move, close_percent = _close_width(direction, close_price, extreme_price)
        open_pass = open_percent >= COMPACT_THRESHOLD
        close_pass = close_percent >= COMPACT_THRESHOLD
        if not (open_pass and close_pass):
            _count(counters, "INTERNAL_EVALUATION_COUNTER_ONLY:" + _alternative_width_rejection(open_pass, close_pass))
            continue
        representative = extreme_rows[-1]
        plateau_start, plateau_end, plateau_length = _plateau(extreme_rows)
        achieved = _first_width_row(bars, open_row, close_row, direction, reference, COMPACT_THRESHOLD)
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
        attempt.formation_class = ALTERNATIVE_CLASS
        attempt.required_width = COMPACT_THRESHOLD
        attempt.applicable_minimum_width = format(COMPACT_THRESHOLD, ".8f")
        attempt.applicable_rule_id = "ALTERNATIVE_4_TO_10_WIDTH_3_50"
        attempt.open_to_extreme_price = open_price
        attempt.open_to_extreme_percent = open_percent
        attempt.close_to_extreme_price = close_move
        attempt.close_to_extreme_percent = close_percent
        attempt.width_achieved_row = achieved
        attempt.width_achieved_time = None if achieved is None else bars[achieved].close_time
        attempt.open_width_pass = True
        attempt.close_width_pass = True
        attempt.both_widths_passed = True
        attempt.completion_type = "NON_RETURN_CLOSE"
        attempt.terminal = False
        attempt.status = CONFIRMED_ALTERNATIVE
        _count(counters, "INTERNAL_EVALUATION_COUNTER_ONLY:ALTERNATIVE_QUALIFIED")
        _stamp_search(attempt, bars, inspected, ALTERNATIVE_MAX_INTERIOR)
        return attempt
    if full_window:
        attempt.status = ALTERNATIVE_NONE
        attempt.terminal = False
        attempt.formation_class = ALTERNATIVE_CLASS
        attempt.applicable_minimum_width = format(COMPACT_THRESHOLD, ".8f")
        attempt.applicable_rule_id = "ALTERNATIVE_4_TO_10_WIDTH_3_50"
    _stamp_search(attempt, bars, inspected, ALTERNATIVE_MAX_INTERIOR)
    return attempt


def _binding_width(attempt: Attempt) -> Decimal:
    if attempt.open_to_extreme_percent is None or attempt.close_to_extreme_percent is None:
        return D0
    return min(attempt.open_to_extreme_percent, attempt.close_to_extreme_percent)


def _resolve_conflict(high: Attempt, low: Attempt) -> None:
    if high.width_achieved_row is not None and low.width_achieved_row is not None and high.width_achieved_row != low.width_achieved_row:
        winner = high if high.width_achieved_row < low.width_achieved_row else low
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


def primary_sort_key(swing: Swing) -> tuple:
    """Sort key for shortest-formation primary selection. Lower sorts first."""
    binding = min(swing.open_to_extreme_percent, swing.close_to_extreme_percent)
    return (
        swing.total_candles,
        abs(swing.completion_difference_percent),
        -binding,
        swing.confirmed_at,
        -int(swing.open_time.timestamp()),
        swing.swing_id,
    )


def _deciding_reason(ordered: Sequence[Swing]) -> str:
    if len(ordered) == 1:
        return "SOLE_FAMILY_MEMBER"
    left = primary_sort_key(ordered[0])
    right = primary_sort_key(ordered[1])
    for index, (winner_value, other_value) in enumerate(zip(left, right)):
        if winner_value != other_value:
            return SELECTION_REASONS[index]
    return SELECTION_REASONS[-1]


def _price_run(bars: Sequence[Bar], row: int, direction: str, price: Decimal) -> tuple[int, int]:
    start = row
    end = row

    def matches(index: int) -> bool:
        candle = bars[index]
        return (candle.high if direction == SWING_HIGH else candle.low) == price

    while start > 0 and matches(start - 1):
        start -= 1
    while end + 1 < len(bars) and matches(end + 1):
        end += 1
    return start, end


def _consolidate(bars: Sequence[Bar], swings: list[Swing], attempts: Sequence[Attempt]) -> list[Family]:
    grouped: dict[tuple, list[Swing]] = {}
    for swing in swings:
        start, end = _price_run(bars, swing.extreme_row, swing.direction, swing.extreme_price)
        swing.event_run_start = start
        swing.event_run_end = end
        grouped.setdefault((swing.direction, swing.extreme_price, start), []).append(swing)
    ordered_keys = sorted(grouped, key=lambda key: (key[2], str(key[1]), key[0]))
    event_ids: dict[tuple, str] = {}
    high_events = 0
    low_events = 0
    for key in ordered_keys:
        if key[0] == SWING_HIGH:
            high_events += 1
            event_ids[key] = f"EXH4H-{high_events:06d}"
        else:
            low_events += 1
            event_ids[key] = f"EXL4H-{low_events:06d}"
    families: list[Family] = []
    high_families = 0
    low_families = 0
    for key in ordered_keys:
        members = grouped[key]
        if key[0] == SWING_HIGH:
            high_families += 1
            family_id = f"FAM-H-{high_families:06d}"
        else:
            low_families += 1
            family_id = f"FAM-L-{low_families:06d}"
        run_end = max(item.event_run_end for item in members)
        horizon = min(run_end + MAX_INTERIOR, len(bars) - 1)
        known_at = bars[horizon].close_time
        ordered = sorted(members, key=primary_sort_key)
        reason = _deciding_reason(ordered)
        winner = ordered[0]
        for rank, member in enumerate(ordered, start=1):
            member.family_id = family_id
            member.extreme_event_id = event_ids[key]
            member.family_size = len(members)
            member.derived_member_count = len(members) - 1
            member.primary_rank = rank
            member.primary_known_at = known_at
            member.selection_reason = reason
            member.event_run_start = key[2]
            member.event_run_end = run_end
        reference_count = sum(1 for item in ordered if item.detection_method == REFERENCE_METHOD)
        alternative_count = sum(1 for item in ordered if item.detection_method == ALTERNATIVE_METHOD)
        families.append(Family(
            family_id=family_id,
            event_id=event_ids[key],
            direction=key[0],
            extreme_price=key[1],
            run_start=key[2],
            run_end=run_end,
            known_at=known_at,
            selection_reason=reason,
            members=ordered,
            primary=winner,
            reference_member_count=reference_count,
            alternative_member_count=alternative_count,
            cross_method=reference_count > 0 and alternative_count > 0,
        ))
    winners = sorted((family.primary for family in families), key=lambda item: (item.confirmed_at, item.open_row, item.swing_id))
    high_primary = 0
    low_primary = 0
    for swing in winners:
        if swing.direction == SWING_HIGH:
            high_primary += 1
            swing.primary_id = f"PRI-SWH4H-{high_primary:06d}"
            swing.primary_sequence = high_primary
        else:
            low_primary += 1
            swing.primary_id = f"PRI-SWL4H-{low_primary:06d}"
            swing.primary_sequence = low_primary
        swing.role = PRIMARY
        swing.tie_break_reason = "NONE" if swing.family_size == 1 else swing.selection_reason
    for family in families:
        primary = family.primary
        for member in family.members:
            member.primary_id = primary.primary_id
            member.compared_total_candles = primary.total_candles
            member.compared_interior_count = primary.interior_count
            if member.role == PRIMARY:
                continue
            member.role = DERIVED
            member.extra_candles = member.total_candles - primary.total_candles
            member.open_row_delta = member.open_row - primary.open_row
            member.close_row_delta = member.close_row - primary.close_row
            member.duration_delta_hours = member.duration_hours - primary.duration_hours
            member.width_delta = min(member.open_to_extreme_percent, member.close_to_extreme_percent) - min(primary.open_to_extreme_percent, primary.close_to_extreme_percent)
            member.return_error_delta = abs(member.completion_difference_percent) - abs(primary.completion_difference_percent)
            if member.total_candles > primary.total_candles:
                member.derivation_reason = LONGER_SAME_EXTREME
            else:
                member.derivation_reason = EQUAL_LENGTH_LOSS
            member.tie_break_reason = family.selection_reason
    by_raw = {swing.swing_id: swing for swing in swings}
    for attempt in attempts:
        swing = by_raw.get(attempt.swing_id or "")
        if swing is None:
            continue
        attempt.family_id = swing.family_id
        attempt.primary_swing_id = swing.primary_id
        attempt.consolidation_status = swing.role
        attempt.derivation_reason = swing.derivation_reason or None
    return families


def _assign_atr(bars: Sequence[Bar]) -> None:
    period = ATR_PERIOD
    tr_values: list[Decimal] = []
    atr = None
    for index, bar in enumerate(bars):
        bar.atr = None
        bar.atr_available = False
        if index == 0:
            continue
        previous = bars[index - 1].close
        true_range = max(bar.high - bar.low, abs(bar.high - previous), abs(bar.low - previous))
        tr_values.append(true_range)
        if len(tr_values) < period:
            continue
        if atr is None:
            atr = sum(tr_values, D0) / Decimal(period)
        else:
            atr = (atr * Decimal(period - 1) + true_range) / Decimal(period)
        bar.atr = atr
        bar.atr_available = True


def analyze(bars: Sequence[Bar]) -> AnalysisResult:
    if not bars:
        raise SwingError("at least one completed 4h candle is required")
    assert_2026_window(bars)
    for index, item in enumerate(bars):
        if item.row != index:
            raise SwingError("bar rows must equal their positions")
    _assign_atr(bars)
    attempts: list[Attempt] = []
    counters: dict[str, int] = {}
    high_count = 0
    low_count = 0
    confirmed_attempts: list[Attempt] = []
    for open_row in range(len(bars)):
        high_count += 1
        low_count += 1
        high = _scan_direction(bars, open_row, SWING_HIGH, f"ATT4H-H-R-{high_count:06d}")
        low = _scan_direction(bars, open_row, SWING_LOW, f"ATT4H-L-R-{low_count:06d}")
        alt_high = _scan_alternative(bars, open_row, SWING_HIGH, f"ATT4H-H-A-{high_count:06d}", counters)
        alt_low = _scan_alternative(bars, open_row, SWING_LOW, f"ATT4H-L-A-{low_count:06d}", counters)
        if high.status in CONFIRMED_STATUSES and low.status in CONFIRMED_STATUSES and high.close_row == low.close_row:
            _resolve_conflict(high, low)
        if alt_high.status in CONFIRMED_STATUSES and alt_low.status in CONFIRMED_STATUSES and alt_high.close_row == alt_low.close_row:
            _resolve_conflict(alt_high, alt_low)
        batch = (high, low, alt_high, alt_low)
        attempts.extend(batch)
        for item in batch:
            if item.status in CONFIRMED_STATUSES:
                confirmed_attempts.append(item)
    confirmed_attempts.sort(key=lambda item: (item.close_row or 0, item.open_row, item.direction))
    swings = _build_swings(bars, confirmed_attempts)
    families = _consolidate(bars, swings, attempts)
    primaries = [item for item in swings if item.role == PRIMARY]
    primaries.sort(key=lambda item: (item.confirmed_at, item.open_row, item.swing_id))
    derived = [item for item in swings if item.role == DERIVED]
    _assign_labels(primaries)
    _assign_alternating(primaries)
    warnings = _assign_overlaps(primaries)
    _assign_forward(bars, primaries)
    for swing in swings:
        for row in range(swing.close_row + 1, len(bars)):
            if _returned(swing.direction, bars[row].close, swing.reference):
                swing.later_reference_return_row = row
                break
    return AnalysisResult(
        bars=list(bars),
        attempts=attempts,
        swings=swings,
        primaries=primaries,
        derived=derived,
        families=families,
        warnings=warnings,
        internal_counters=counters,
    )


def _build_swings(bars: Sequence[Bar], attempts: Sequence[Attempt]) -> list[Swing]:
    swings: list[Swing] = []
    high_sequence = 0
    low_sequence = 0
    for attempt in attempts:
        if attempt.direction == SWING_HIGH:
            high_sequence += 1
            sequence = high_sequence
            swing_id = f"RAW-SH-{sequence:06d}"
        else:
            low_sequence += 1
            sequence = low_sequence
            swing_id = f"RAW-SL-{sequence:06d}"
        attempt.swing_id = swing_id
        swings.append(_swing_from_attempt(bars, attempt, swing_id, sequence))
    return swings


def _body(candle: Bar) -> Decimal:
    return candle.close - candle.open


def _upper_wick(candle: Bar) -> Decimal:
    return candle.high - max(candle.open, candle.close)


def _lower_wick(candle: Bar) -> Decimal:
    return min(candle.open, candle.close) - candle.low


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
    if attempt.formation_class in {COMPACT_SWING, ALTERNATIVE_CLASS}:
        width_component = _clamp((binding - COMPACT_THRESHOLD) / QUALITY_COMPACT_SPAN, D0, D1)
    else:
        width_component = _clamp((binding - STANDARD_THRESHOLD) / QUALITY_STANDARD_SPAN, D0, D1)
    return_error = abs(difference) / attempt.reference * D100
    precision = D1 - _clamp(return_error / QUALITY_PRECISION_SCALE, D0, D1)
    path_component = (efficiency_out + efficiency_back) / D2
    quality = D100 * (
        QUALITY_W_WIDTH * width_component
        + QUALITY_W_PRECISION * precision
        + QUALITY_W_PATH * path_component
        + QUALITY_W_BALANCE * balance
    )
    volumes = [item.volume for item in window]
    average_volume = sum(volumes, D0) / Decimal(len(volumes))
    open_to_close_bars = attempt.close_row - attempt.open_row
    duration_hours = Decimal(open_to_close_bars * BAR_HOURS)
    total_candles = attempt.interior_count + 2
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
        total_candles=total_candles,
        left_bars=left,
        right_bars=right,
        left_duration_hours=Decimal(left * BAR_HOURS),
        right_duration_hours=Decimal(right * BAR_HOURS),
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
        duration_hours=duration_hours,
        coverage_hours=Decimal(total_candles * BAR_HOURS),
        duration_days=duration_hours / Decimal(HOURS_PER_DAY),
        width_achieved_row=achieved,
        width_achieved_time=attempt.width_achieved_time,
        confirmed_at=close.close_time,
        quality_score=quality,
        quality_label=_quality_label(quality),
        path_length=_path_length(closes),
        net_close_movement=close.close - origin.close,
        adverse_before=_adverse(attempt.direction, attempt.reference, closes[: left + 1]),
        adverse_after=_adverse(attempt.direction, extreme.close, closes[left:]),
        earlier_reference_returns=_earlier_returns(bars, attempt),
        near_reference_before_close=_near_reference(bars, attempt),
        direction_changes=_direction_changes(closes),
        close_volatility=_volatility(closes),
        extreme_prominence=_prominence(bars, attempt),
        reference_to_extreme_efficiency=efficiency_out,
        extreme_to_close_efficiency=efficiency_back,
        bullish_candles=sum(1 for item in window if item.close > item.open),
        bearish_candles=sum(1 for item in window if item.close < item.open),
        doji_candles=sum(1 for item in window if item.close == item.open),
        up_closes=sum(1 for previous, item in zip(window, window[1:]) if item.close > previous.close),
        down_closes=sum(1 for previous, item in zip(window, window[1:]) if item.close < previous.close),
        open_body=_body(origin),
        extreme_body=_body(extreme),
        close_body=_body(close),
        open_upper_wick=_upper_wick(origin),
        open_lower_wick=_lower_wick(origin),
        extreme_upper_wick=_upper_wick(extreme),
        extreme_lower_wick=_lower_wick(extreme),
        close_upper_wick=_upper_wick(close),
        close_lower_wick=_lower_wick(close),
        base_volume=sum((item.volume for item in window), D0),
        quote_volume=sum((item.quote_volume for item in window), D0),
        trades=sum(item.trades for item in window),
        taker_base=sum((item.taker_base for item in window), D0),
        taker_quote=sum((item.taker_quote for item in window), D0),
        average_volume=average_volume,
        median_volume=_median(volumes),
        max_volume_row=max(window, key=lambda item: (item.volume, item.row)).row,
        extreme_volume_ratio=None if average_volume == 0 else extreme.volume / average_volume,
        close_volume_ratio=None if average_volume == 0 else close.volume / average_volume,
        applicable_rule_id=attempt.applicable_rule_id or "",
        open_width_pass=bool(attempt.open_width_pass),
        close_width_pass=bool(attempt.close_width_pass),
        both_widths_passed=bool(attempt.both_widths_passed),
        detection_method=attempt.detection_method,
        found_through_alternative=attempt.found_through_alternative,
        reference_return_required=attempt.reference_return_required,
        reference_return_achieved=attempt.reference_return_achieved,
        maximum_search_interior=attempt.maximum_search_interior,
        actual_search_interior=attempt.actual_search_interior,
        search_horizon_end_row=attempt.search_horizon_end_row,
        search_horizon_exhausted=attempt.search_horizon_exhausted,
        search_censored=attempt.search_censored,
        maximum_observable_interior=attempt.maximum_observable_interior,
        missing_search_candles=attempt.missing_search_candles,
        search_terminal_reason=attempt.search_terminal_reason,
        reference_binding_found=attempt.reference_binding_found,
        reference_binding_interior=attempt.reference_binding_interior,
        alternative_window_entered=attempt.alternative_window_entered,
        alternative_window_exhausted=attempt.alternative_window_exhausted,
        alternative_binding_found=attempt.alternative_binding_found,
        evaluations_within_horizon=attempt.evaluations_within_horizon,
        evaluations_beyond_horizon=attempt.evaluations_beyond_horizon,
    )


def _path_length(closes: Sequence[Decimal]) -> Decimal:
    return sum((abs(current - previous) for previous, current in zip(closes, closes[1:])), D0)


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


def _adverse(direction: str, anchor: Decimal, closes: Sequence[Decimal]) -> Decimal:
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
    for row in range(attempt.open_row + 2, attempt.close_row or attempt.open_row):
        if _returned(attempt.direction, bars[row].close, attempt.reference):
            continue
        distance = abs(bars[row].close - attempt.reference) / attempt.reference * D100
        if distance <= EQUALITY_TOLERANCE:
            count += 1
    return count


def _direction_changes(closes: Sequence[Decimal]) -> int:
    signs = []
    for previous, current in zip(closes, closes[1:]):
        if current > previous:
            signs.append(1)
        elif current < previous:
            signs.append(-1)
    return sum(1 for previous, current in zip(signs, signs[1:]) if previous != current)


def _volatility(closes: Sequence[Decimal]) -> Decimal:
    returns = []
    for previous, current in zip(closes, closes[1:]):
        if previous == 0:
            continue
        returns.append(float((current - previous) / previous))
    if len(returns) < 2:
        return D0
    return Decimal(str(pstdev(returns))) * D100


def _prominence(bars: Sequence[Bar], attempt: Attempt) -> Decimal:
    values = [
        bars[row].high if attempt.direction == SWING_HIGH else bars[row].low
        for row in range(attempt.open_row + 1, attempt.close_row or attempt.open_row)
    ]
    unique = sorted(set(values), reverse=attempt.direction == SWING_HIGH)
    if len(unique) < 2 or attempt.extreme_price is None:
        return D0
    return abs(attempt.extreme_price - unique[1]) / attempt.reference * D100


def _quality_label(score: Decimal) -> str:
    if score >= Decimal(QUALITY_EXCEPTIONAL_MIN):
        return "EXCEPTIONAL"
    if score >= Decimal(QUALITY_HIGH_MIN):
        return "HIGH_QUALITY"
    if score >= Decimal(QUALITY_SIGNIFICANT_MIN):
        return "SIGNIFICANT"
    if score >= Decimal(QUALITY_MODERATE_MIN):
        return "MODERATE"
    return "BASIC_CONFIRMED"


def _assign_labels(swings: list[Swing]) -> None:
    previous_high = None
    previous_low = None
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
    last = None
    for swing in swings:
        if last is None or swing.direction != last:
            swing.alternating_view = "INCLUDED"
            last = swing.direction
        else:
            swing.alternating_view = "SKIPPED_NON_ALTERNATING"


def _assign_overlaps(swings: list[Swing]) -> list[str]:
    warnings = []
    for left in swings:
        parents = []
        children = []
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
                parents.append(right.primary_id)
            if _strictly_contains(left, right):
                children.append(right.primary_id)
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
    return 0 if end < start else end - start + 1


def _strictly_contains(parent: Swing, child: Swing) -> bool:
    return parent.open_row <= child.open_row and parent.close_row >= child.close_row and (
        parent.close_row - parent.open_row > child.close_row - child.open_row
    )


def _join_ids(ids: Sequence[str]) -> str:
    text = ",".join(ids)
    if len(text) <= 32767:
        return text
    return text[:32760] + ",TRUNCATED"


def _assign_forward(bars: Sequence[Bar], swings: list[Swing]) -> None:
    if not bars:
        return
    suffix_high = [D0] * (len(bars) + 1)
    suffix_low = [PROMINENCE_FLOOR] * (len(bars) + 1)
    for row in range(len(bars) - 1, -1, -1):
        suffix_high[row] = max(bars[row].high, suffix_high[row + 1])
        suffix_low[row] = min(bars[row].low, suffix_low[row + 1])
    for index, swing in enumerate(swings):
        for later in swings[index + 1:]:
            if swing.next_same_id is None and later.direction == swing.direction:
                swing.next_same_id = later.primary_id
            if swing.next_opposite_id is None and later.direction != swing.direction:
                swing.next_opposite_id = later.primary_id
            if swing.next_same_id and swing.next_opposite_id:
                break
        next_row = None
        if swing.next_same_id or swing.next_opposite_id:
            candidates = [item.close_row for item in swings[index + 1:] if item.primary_id in {swing.next_same_id, swing.next_opposite_id}]
            next_row = min(candidates) if candidates else None
        if next_row is not None:
            swing.bars_to_next = next_row - swing.close_row
        if swing.close_row + 1 < len(bars):
            if swing.direction == SWING_HIGH:
                swing.favorable_excursion = suffix_high[swing.close_row + 1] - swing.close_price
                swing.adverse_excursion = swing.close_price - suffix_low[swing.close_row + 1]
            else:
                swing.favorable_excursion = swing.close_price - suffix_low[swing.close_row + 1]
                swing.adverse_excursion = suffix_high[swing.close_row + 1] - swing.close_price
        _first_forward_events(bars, swing)
        swing.forward_censored = True
        swing.forward_censor_reason = "DATASET_END"


def _first_forward_events(bars: Sequence[Bar], swing: Swing) -> None:
    for row in range(swing.close_row + 1, len(bars)):
        candle = bars[row]
        if swing.direction == SWING_HIGH:
            if swing.first_break_row is None and candle.high > swing.extreme_price:
                swing.first_break_row = row
            if swing.first_close_beyond_row is None and candle.close > swing.extreme_price:
                swing.first_close_beyond_row = row
            if swing.retest_extreme_row is None and candle.low <= swing.extreme_price <= candle.high:
                swing.retest_extreme_row = row
        else:
            if swing.first_break_row is None and candle.low < swing.extreme_price:
                swing.first_break_row = row
            if swing.first_close_beyond_row is None and candle.close < swing.extreme_price:
                swing.first_close_beyond_row = row
            if swing.retest_extreme_row is None and candle.low <= swing.extreme_price <= candle.high:
                swing.retest_extreme_row = row
        if swing.retest_reference_row is None and candle.low <= swing.reference <= candle.high:
            swing.retest_reference_row = row
        if all(value is not None for value in (swing.first_break_row, swing.first_close_beyond_row, swing.retest_extreme_row, swing.retest_reference_row)):
            return


def audit_result(result: AnalysisResult) -> None:
    if len(result.attempts) != len(result.bars) * 4:
        raise SwingError("attempt count is not one row per open, direction, and method")
    if len({(item.open_row, item.direction, item.detection_method) for item in result.attempts}) != len(result.attempts):
        raise SwingError("duplicate directional attempts")
    confirmed = [item for item in result.attempts if item.status in CONFIRMED_STATUSES]
    if len(confirmed) != len(result.swings):
        raise SwingError("confirmed attempts and swings differ")
    keys = set()
    ids = set()
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
    if high_ids != [f"RAW-SH-{index:06d}" for index in range(1, len(high_ids) + 1)]:
        raise SwingError("raw swing high ids are not sequential")
    if low_ids != [f"RAW-SL-{index:06d}" for index in range(1, len(low_ids) + 1)]:
        raise SwingError("raw swing low ids are not sequential")
    keys = [(item.direction, item.open_row, item.extreme_row, item.close_row) for item in result.swings]
    if len(keys) != len(set(keys)):
        raise SwingError("the same exact candidate was emitted twice")
    _audit_families(result)
    for bar in result.bars:
        if bar.open_time < ANALYSIS_START or bar.open_time > ANALYSIS_END_OPEN:
            raise SwingError("analysis used a candle outside 2026")


def _audit_families(result: AnalysisResult) -> None:
    if len(result.primaries) + len(result.derived) != len(result.swings):
        raise SwingError("raw swings do not equal primary plus derived")
    if len(result.families) != len(result.primaries):
        raise SwingError("family count does not equal primary count")
    seen = set()
    primary_high = [item.primary_id for item in result.primaries if item.direction == SWING_HIGH]
    primary_low = [item.primary_id for item in result.primaries if item.direction == SWING_LOW]
    if primary_high != [f"PRI-SWH4H-{index:06d}" for index in range(1, len(primary_high) + 1)]:
        raise SwingError("primary high ids are not sequential")
    if primary_low != [f"PRI-SWL4H-{index:06d}" for index in range(1, len(primary_low) + 1)]:
        raise SwingError("primary low ids are not sequential")
    for family in result.families:
        if len({item.direction for item in family.members}) != 1:
            raise SwingError(f"{family.family_id} mixes swing types")
        primaries = [item for item in family.members if item.role == PRIMARY]
        if len(primaries) != 1 or primaries[0].swing_id != family.primary.swing_id:
            raise SwingError(f"{family.family_id} does not have exactly one primary")
        shortest = min(item.total_candles for item in family.members)
        if family.primary.total_candles != shortest:
            raise SwingError(f"{family.family_id} primary is not the shortest formation")
        for member in family.members:
            if member.swing_id in seen:
                raise SwingError(f"{member.swing_id} belongs to more than one family")
            seen.add(member.swing_id)
            if member.extreme_price != family.extreme_price or member.direction != family.direction:
                raise SwingError(f"{member.swing_id} family key mismatch")
            if member.role == DERIVED and member.total_candles < family.primary.total_candles:
                raise SwingError(f"{member.swing_id} derived swing is shorter than its primary")
    if seen != {item.swing_id for item in result.swings}:
        raise SwingError("a raw swing is missing from the family reconciliation")


def _audit_alternative(bars: Sequence[Bar], swing: Swing) -> None:
    interior = swing.close_row - swing.open_row - 1
    if swing.formation_class != ALTERNATIVE_CLASS or not swing.found_through_alternative:
        raise SwingError(f"{swing.swing_id} alternative classification is inconsistent")
    if alternative_duration_rejection(interior) is not None or swing.interior_count != interior:
        raise SwingError(f"{swing.swing_id} alternative duration is outside 4 through 10")
    if not (swing.open_row < swing.extreme_row < swing.close_row):
        raise SwingError(f"{swing.swing_id} extreme is not interior")
    if _returned(swing.direction, bars[swing.close_row].close, swing.reference):
        raise SwingError(f"{swing.swing_id} alternative close reached the reference")
    if swing.required_width != COMPACT_THRESHOLD:
        raise SwingError(f"{swing.swing_id} alternative width threshold is not 3.50")
    _price, open_percent = _open_width(swing.direction, swing.reference, swing.extreme_price)
    _move, close_percent = _close_width(swing.direction, bars[swing.close_row].close, swing.extreme_price)
    if not both_widths_pass(open_percent, close_percent, COMPACT_THRESHOLD):
        raise SwingError(f"{swing.swing_id} alternative widths failed")
    for row in range(swing.open_row + ALTERNATIVE_MIN_INTERIOR + 1, swing.close_row):
        if _returned(swing.direction, bars[row].close, swing.reference):
            continue
        earlier_extreme = max(bars[index].high for index in range(swing.open_row + 1, row)) if swing.direction == SWING_HIGH else min(bars[index].low for index in range(swing.open_row + 1, row))
        earlier_open, earlier_open_percent = _open_width(swing.direction, swing.reference, earlier_extreme)
        earlier_close, earlier_close_percent = _close_width(swing.direction, bars[row].close, earlier_extreme)
        if both_widths_pass(earlier_open_percent, earlier_close_percent, COMPACT_THRESHOLD):
            raise SwingError(f"{swing.swing_id} skipped an earlier qualifying alternative close")
    expected_confirm = bars[swing.close_row].open_time + BAR_SPAN
    if swing.confirmed_at != bars[swing.close_row].close_time or swing.confirmed_at != expected_confirm:
        raise SwingError(f"{swing.swing_id} alternative confirmation is not the completed close")


def _audit_swing(bars: Sequence[Bar], swing: Swing) -> None:
    if swing.detection_method == ALTERNATIVE_METHOD:
        _audit_alternative(bars, swing)
        return
    if swing.detection_method != REFERENCE_METHOD or swing.found_through_alternative:
        raise SwingError(f"{swing.swing_id} reference method flag is inconsistent")
    if not swing.reference_return_achieved:
        raise SwingError(f"{swing.swing_id} reference swing did not achieve the return")
    if not (swing.open_row < swing.extreme_row < swing.close_row):
        raise SwingError(f"{swing.swing_id} extreme is not interior")
    if swing.open_time < ANALYSIS_START or swing.extreme_open_time < ANALYSIS_START:
        raise SwingError(f"{swing.swing_id} begins before 2026")
    if swing.close_open_time > ANALYSIS_END_OPEN:
        raise SwingError(f"{swing.swing_id} closes after the available 2026 data")
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
        contains = bars[swing.extreme_row].high == extreme
    else:
        extreme = min(bars[row].low for row in range(swing.open_row + 1, swing.close_row))
        if bars[swing.close_row].close < swing.reference:
            raise SwingError(f"{swing.swing_id} low close did not return")
        contains = bars[swing.extreme_row].low == extreme
    if extreme != swing.extreme_price or not contains:
        raise SwingError(f"{swing.swing_id} extreme is not the representative interior extreme")
    if swing.extreme_row != max(row for row in range(swing.open_row + 1, swing.close_row) if (bars[row].high if swing.direction == SWING_HIGH else bars[row].low) == extreme):
        raise SwingError(f"{swing.swing_id} representative extreme is not the last occurrence")
    _price, open_percent = _open_width(swing.direction, swing.reference, extreme)
    _move, close_percent = _close_width(swing.direction, bars[swing.close_row].close, extreme)
    if open_percent != swing.open_to_extreme_percent or close_percent != swing.close_to_extreme_percent:
        raise SwingError(f"{swing.swing_id} widths were not recomputed from stored prices")
    if not both_widths_pass(open_percent, close_percent, threshold):
        raise SwingError(f"{swing.swing_id} failed a width rule")
    for row in range(swing.open_row + 2, swing.close_row):
        if _returned(swing.direction, bars[row].close, swing.reference):
            raise SwingError(f"{swing.swing_id} used a later close after an earlier return")
    expected_confirm = bars[swing.close_row].open_time + BAR_SPAN
    if swing.confirmed_at != bars[swing.close_row].close_time or swing.confirmed_at != expected_confirm:
        raise SwingError(f"{swing.swing_id} confirmation is not the 4h swing close completion")
    if swing.reference != bars[swing.open_row].open:
        raise SwingError(f"{swing.swing_id} reference is not the swing open")
    if swing.duration_hours != Decimal((swing.close_row - swing.open_row) * BAR_HOURS):
        raise SwingError(f"{swing.swing_id} duration hours mismatch")
    if swing.earlier_reference_returns != 0:
        raise SwingError(f"{swing.swing_id} has an earlier reference return")
