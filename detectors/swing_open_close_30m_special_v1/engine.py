"""BTCUSDT 30-minute special swing engine. No other detector is imported."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Sequence

from detectors.swing_open_close_30m_special_v1.config import (
    ALTERNATIVE_ENABLED,
    COMPACT_ENABLED,
    CONFIGURATION_VERSION,
    DERIVED,
    DISPLAY_TZ,
    FORBIDDEN_TERMINALS,
    FIRST_CLOSE_OFFSET,
    FORMATION_CLASS,
    LAST_CLOSE_OFFSET,
    LONGER,
    MAX_BOUNDARY,
    MAX_INTERIOR,
    MAX_TOTAL,
    MIN_BOUNDARY,
    MIN_INTERIOR,
    MIN_TOTAL,
    NESTED,
    PRIMARY,
    REQUIRED_FRACTION,
    STANDARD_ENABLED,
    SWING_HIGH,
    SWING_LOW,
    ZERO_INTERIOR_ENABLED,
)

D100 = Decimal(100)


class SpecialSwingError(RuntimeError):
    """A hard detection or validation failure."""


@dataclass(slots=True)
class Bar:
    row: int
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal(0)
    quote_volume: Decimal = Decimal(0)
    trades: int = 0


@dataclass(slots=True)
class Swing:
    raw_id: str
    swing_id: str
    direction: str
    open_row: int
    close_row: int
    extreme_row: int
    extreme_price: Decimal
    plateau_start: int
    plateau_end: int
    event_id: str
    family_id: str
    reference: Decimal
    close_price: Decimal
    interior: int
    total: int
    open_width_percent: Decimal
    close_width_percent: Decimal
    completion_error: Decimal
    structure_high: Decimal
    structure_low: Decimal
    role: str
    status: str
    formation_class: str
    body_low: Decimal
    body_high: Decimal
    body_size: Decimal
    body_threshold: Decimal
    penetration_fraction: Decimal
    suppressed_by: str = ""
    body_reference_status: str = ""
    body_reference_rule: str = ""
    body_reference_row: int | None = None
    body_reference_direction: str = ""
    body_reference_price: Decimal | None = None
    body_reference_bars_after_open: int | None = None
    body_reference_bars_before_close: int | None = None
    body_reference_position_fraction: Decimal | None = None
    body_reference_tie_count: int = 0
    body_reference_tie_first: int | None = None
    body_reference_tie_last: int | None = None
    body_reference_matches_extremum: bool | None = None
    extremum_to_body_reference_distance: Decimal | None = None
    body_reference_pair_type: str = ""
    body_reference_eligible_count: int = 0
    body_reference_validation_row: int | None = None
    body_reference_validation_direction: str = ""
    body_reference_row_difference: int | None = None
    body_reference_close_change: Decimal | None = None
    body_reference_close_change_percent: Decimal | None = None
    body_reference_validation_position_fraction: Decimal | None = None
    body_reference_validation_matches_extremum: bool | None = None
    reference_successor_not_interior: int = 0
    reference_successor_wrong_direction: int = 0
    reference_pre_close_excluded: int = 0
    reference_successor_is_swing_close: int = 0
    earlier_compatibility_rejections: int = 0
    compatibility_status: str = ""
    reference_to_swing_close_margin: Decimal | None = None
    reference_to_swing_close_margin_percent: Decimal | None = None
    first_compatibility_failure_row: int | None = None
    last_compatibility_failure_row: int | None = None
    final_reference_matches_provisional: bool = True
    segment_status: str = ""
    peak_1_row: int | None = None
    peak_1_price: Decimal | None = None
    peak_1_count: int | None = None
    peak_1_tie_count: int | None = None
    peak_1_tie_first: int | None = None
    peak_1_tie_last: int | None = None
    peak_1_plateau_start: int | None = None
    peak_1_plateau_end: int | None = None
    peak_1_owned_by_open: bool | None = None
    peak_1_owned_by_reference: bool | None = None
    peak_2_row: int | None = None
    peak_2_price: Decimal | None = None
    peak_2_count: int | None = None
    peak_2_tie_count: int | None = None
    peak_2_tie_first: int | None = None
    peak_2_tie_last: int | None = None
    peak_2_plateau_start: int | None = None
    peak_2_plateau_end: int | None = None
    peak_2_owned_by_validation: bool | None = None
    peak_2_owned_by_close: bool | None = None
    dip_1_row: int | None = None
    dip_1_price: Decimal | None = None
    dip_1_count: int | None = None
    dip_1_tie_count: int | None = None
    dip_1_tie_first: int | None = None
    dip_1_tie_last: int | None = None
    dip_1_plateau_start: int | None = None
    dip_1_plateau_end: int | None = None
    dip_1_owned_by_open: bool | None = None
    dip_1_owned_by_reference: bool | None = None
    dip_2_row: int | None = None
    dip_2_price: Decimal | None = None
    dip_2_count: int | None = None
    dip_2_tie_count: int | None = None
    dip_2_tie_first: int | None = None
    dip_2_tie_last: int | None = None
    dip_2_plateau_start: int | None = None
    dip_2_plateau_end: int | None = None
    dip_2_owned_by_validation: bool | None = None
    dip_2_owned_by_close: bool | None = None


@dataclass(slots=True)
class SearchRecord:
    open_row: int
    direction: str
    terminal: str
    max_interior: int
    censored: bool
    evaluated_closes: int = 0
    wick_only: int = 0
    above_horizon: int = 0
    binding_failed: bool = False
    raw_id: str = ""
    open_seed: bool = False
    open_rejected: bool = False
    directional_close_candidates: int = 0
    wrong_direction_crossings: int = 0
    close_doji_crossings: int = 0
    directionally_bound: bool = False
    penetration_passing: int = 0
    compatibility_evaluated: int = 0
    compatibility_rejected: int = 0
    reference_recalculations: int = 0
    reference_below_close: int = 0
    reference_above_close: int = 0
    equality_passes: int = 0
    positive_margin_passes: int = 0
    missing_pair_candidates: int = 0
    accepted_after_compatibility_failure: bool = False
    exhausted_after_only_compatibility_failures: bool = False
    duration_counts: dict = field(default_factory=dict)
    compatibility_audit: list = field(default_factory=list)


@dataclass
class Analysis:
    bars: list[Bar]
    searches: list[SearchRecord]
    raw: list[Swing]
    displayed: list[Swing]
    counters: dict
    warnings: list[str] = field(default_factory=list)


def turkey_text(moment: datetime | None) -> str:
    if moment is None:
        return ""
    local = moment.astimezone(DISPLAY_TZ)
    offset = local.strftime("%z")
    offset = f"{offset[:3]}:{offset[3:]}"
    if local.microsecond:
        return local.strftime("%Y-%m-%d %H:%M:%S") + f".{local.microsecond // 1000:03d}{offset}"
    return local.strftime("%Y-%m-%d %H:%M:%S") + offset


def candle_direction(open_price: Decimal, close_price: Decimal) -> str:
    if close_price > open_price:
        return "BULLISH"
    if close_price < open_price:
        return "BEARISH"
    return "DOJI"


def swing_open_body(open_price: Decimal, open_close: Decimal) -> tuple[Decimal, Decimal, Decimal]:
    body_low = min(open_price, open_close)
    body_high = max(open_price, open_close)
    return body_low, body_high, body_high - body_low


def body_threshold(direction: str, body_low: Decimal, body_high: Decimal, body_size: Decimal) -> Decimal:
    if direction == SWING_HIGH:
        return body_high - (body_size * REQUIRED_FRACTION)
    return body_low + (body_size * REQUIRED_FRACTION)


def body_penetrated(direction: str, close_price: Decimal, threshold: Decimal) -> bool:
    if direction == SWING_HIGH:
        return close_price <= threshold
    return close_price >= threshold


def reference_to_close_margin(direction: str, reference_close: Decimal, swing_close: Decimal) -> Decimal:
    if direction == SWING_HIGH:
        return reference_close - swing_close
    return swing_close - reference_close


def penetration_fraction(direction: str, close_price: Decimal, body_low: Decimal, body_high: Decimal, body_size: Decimal) -> Decimal:
    price = body_high - close_price if direction == SWING_HIGH else close_price - body_low
    return price / body_size


def duration_reason(interior: int) -> str | None:
    if interior < MIN_INTERIOR:
        return "INTERIOR_COUNT_BELOW_6"
    if interior > MAX_INTERIOR:
        return "INTERIOR_COUNT_ABOVE_9"
    return None


def boundary_reason(open_percent: Decimal, close_percent: Decimal) -> str | None:
    if MIN_BOUNDARY != Decimal("1.00") or MAX_BOUNDARY is not None:
        raise SpecialSwingError("boundary configuration is not 1.00 with no maximum")
    open_low = open_percent < MIN_BOUNDARY
    close_low = close_percent < MIN_BOUNDARY
    if not open_low and not close_low:
        return None
    if open_low and close_low:
        return "BOTH_BOUNDARIES_BELOW_1_00"
    if open_low:
        return "OPEN_BOUNDARY_BELOW_1_00"
    return "CLOSE_BOUNDARY_BELOW_1_00"


def analyze(bars: Sequence[Bar]) -> Analysis:
    if not STANDARD_ENABLED or ZERO_INTERIOR_ENABLED or COMPACT_ENABLED or ALTERNATIVE_ENABLED:
        raise SpecialSwingError("only STANDARD_30M_SPECIAL is enabled")
    if MIN_INTERIOR != 6 or MAX_INTERIOR != 9 or MIN_TOTAL != 8 or MAX_TOTAL != 11:
        raise SpecialSwingError("interior window is not 6 through 9")
    if FIRST_CLOSE_OFFSET != 7 or LAST_CLOSE_OFFSET != 10:
        raise SpecialSwingError("close offsets are not open_row + 7 through open_row + 10")
    if CONFIGURATION_VERSION != "STANDARD_6_TO_9_PEAK_DIP_BOUNDARY_1_00_MINIMAL_16_COLUMN_EXCEL_V12":
        raise SpecialSwingError("configuration version mismatch")
    if not bars:
        raise SpecialSwingError("at least one candle is required")
    searches: list[SearchRecord] = []
    raw: list[Swing] = []
    for open_row in range(len(bars)):
        for direction in (SWING_HIGH, SWING_LOW):
            record, swing = _search(bars, open_row, direction)
            searches.append(record)
            if swing is not None:
                raw.append(swing)
    _assign_raw_ids(raw)
    _assign_families(raw)
    _suppress_overlap(raw)
    by_search = {(item.open_row, item.direction): item for item in raw}
    for record in searches:
        swing = by_search.get((record.open_row, record.direction))
        if swing is None:
            continue
        record.raw_id = swing.raw_id
        record.terminal = "CONFIRMED_PRIMARY_30M_SPECIAL_SWING" if swing.status == PRIMARY else swing.status
    _assign_display_ids(raw)
    _check_invariants(bars, raw, searches)
    displayed = [item for item in raw if item.status == PRIMARY]
    return Analysis(list(bars), searches, raw, displayed, _counters(raw, searches, displayed))


def _search(bars: Sequence[Bar], open_row: int, direction: str) -> tuple[SearchRecord, Swing | None]:
    opened = bars[open_row]
    _body_low, _body_high, body_size = swing_open_body(opened.open, opened.close)
    required_close = "BEARISH" if direction == SWING_HIGH else "BULLISH"
    if body_size == 0:
        return SearchRecord(open_row, direction, "SWING_OPEN_BODY_ZERO", 0, False, open_rejected=True), None
    if direction == SWING_HIGH and candle_direction(opened.open, opened.close) != "BULLISH":
        return SearchRecord(open_row, direction, "SWING_HIGH_OPEN_NOT_BULLISH", 0, False, open_rejected=True), None
    if direction == SWING_LOW and candle_direction(opened.open, opened.close) != "BEARISH":
        return SearchRecord(open_row, direction, "SWING_LOW_OPEN_NOT_BEARISH", 0, False, open_rejected=True), None
    first = open_row + FIRST_CLOSE_OFFSET
    last = open_row + LAST_CLOSE_OFFSET
    if first != open_row + MIN_INTERIOR + 1 or last != open_row + MAX_INTERIOR + 1:
        raise SpecialSwingError("HARD_SEARCH_HORIZON_ERROR")
    if first >= len(bars):
        return SearchRecord(
            open_row, direction, "SEARCH_NOT_YET_ELIGIBLE_BELOW_6_INTERIORS", 0, True, open_seed=True
        ), None
    wick_only = 0
    evaluated = 0
    max_interior = 0
    directional = 0
    wrong_direction = 0
    close_dojis = 0
    penetration_passing = 0
    compatibility_evaluated = 0
    compatibility_rejected = 0
    reference_recalculations = 0
    high_below = 0
    low_above = 0
    equality_passes = 0
    positive_margin_passes = 0
    missing_pair_candidates = 0
    first_failure_row = None
    last_failure_row = None
    duration_counts: dict[int, int] = {}
    audit: list[dict] = []

    def finish(terminal: str, censored: bool, binding_failed: bool, bound: bool, swing: Swing | None):
        return SearchRecord(
            open_row,
            direction,
            terminal,
            max_interior,
            censored,
            evaluated,
            wick_only,
            0,
            binding_failed,
            open_seed=True,
            directional_close_candidates=directional,
            wrong_direction_crossings=wrong_direction,
            close_doji_crossings=close_dojis,
            directionally_bound=bound,
            penetration_passing=penetration_passing,
            compatibility_evaluated=compatibility_evaluated,
            compatibility_rejected=compatibility_rejected,
            reference_recalculations=reference_recalculations,
            reference_below_close=high_below,
            reference_above_close=low_above,
            equality_passes=equality_passes,
            positive_margin_passes=positive_margin_passes,
            missing_pair_candidates=missing_pair_candidates,
            accepted_after_compatibility_failure=bound and compatibility_rejected > 0,
            exhausted_after_only_compatibility_failures=(
                terminal == "SEARCH_HORIZON_EXHAUSTED_AT_9_INTERIORS"
                and compatibility_rejected > 0
                and penetration_passing == compatibility_rejected
                and not bound
            ),
            duration_counts=dict(duration_counts),
            compatibility_audit=list(audit),
        ), swing

    for close_row in range(first, last + 1):
        interior = close_row - open_row - 1
        if interior > MAX_INTERIOR:
            raise SpecialSwingError("HARD_SEARCH_HORIZON_ERROR")
        if close_row >= len(bars):
            return finish("SEARCH_CENSORED_BY_DATASET_END", True, False, False, None)
        max_interior = interior
        evaluated += 1
        duration_counts[interior] = duration_counts.get(interior, 0) + 1
        candle = bars[close_row]
        close_label = candle_direction(candle.open, candle.close)
        if close_label == required_close:
            directional += 1
        threshold = body_threshold(direction, *_body(opened))
        if not body_penetrated(direction, candle.close, threshold):
            wick = candle.low <= threshold if direction == SWING_HIGH else candle.high >= threshold
            if wick:
                wick_only += 1
            continue
        if close_label != required_close:
            wrong_direction += 1
            if close_label == "DOJI":
                close_dojis += 1
            continue
        penetration_passing += 1
        provisional = select_interior_body_reference(
            bars, open_row, close_row, direction, max(open_row + 1, close_row - 1), candle.close
        )
        reference_recalculations += 1
        if provisional["price"] is None:
            missing_pair_candidates += 1
            audit.append({
                "close_row": close_row,
                "code": "REFERENCE_COMPATIBILITY_NOT_APPLICABLE_NO_PAIR",
                "status": "NOT_APPLICABLE_NO_REFERENCE_PAIR",
                "reference_row": None,
                "reference_price": None,
                "eligible_count": 0,
                "recomputed": True,
            })
        else:
            compatibility_evaluated += 1
            margin = reference_to_close_margin(direction, provisional["price"], candle.close)
            if margin < 0:
                compatibility_rejected += 1
                if direction == SWING_HIGH:
                    high_below += 1
                    code = "REFERENCE_CLOSE_BELOW_SWING_CLOSE_FOR_SWING_HIGH"
                else:
                    low_above += 1
                    code = "REFERENCE_CLOSE_ABOVE_SWING_CLOSE_FOR_SWING_LOW"
                if first_failure_row is None:
                    first_failure_row = close_row
                last_failure_row = close_row
                audit.append({
                    "close_row": close_row,
                    "code": code,
                    "status": "FAIL",
                    "continuation": "REFERENCE_COMPATIBILITY_FAIL_CONTINUE_SEARCH",
                    "reference_row": provisional["row"],
                    "reference_price": provisional["price"],
                    "eligible_count": provisional["eligible_count"],
                    "recomputed": True,
                })
                continue
            if margin == 0:
                equality_passes += 1
            else:
                positive_margin_passes += 1
            audit.append({
                "close_row": close_row,
                "code": "REFERENCE_COMPATIBILITY_PASS",
                "status": "PASS",
                "reference_row": provisional["row"],
                "reference_price": provisional["price"],
                "eligible_count": provisional["eligible_count"],
                "recomputed": True,
            })
        swing, reason = _build(
            bars,
            open_row,
            close_row,
            direction,
            interior,
            provisional=provisional,
            earlier_rejections=compatibility_rejected,
            first_failure_row=first_failure_row,
            last_failure_row=last_failure_row,
        )
        return finish(
            reason or "CONFIRMED_PRIMARY_30M_SPECIAL_SWING",
            False,
            reason is not None,
            True,
            swing,
        )
    return finish("SEARCH_HORIZON_EXHAUSTED_AT_9_INTERIORS", False, False, False, None)


def _body(opened: Bar) -> tuple[Decimal, Decimal, Decimal]:
    return swing_open_body(opened.open, opened.close)


def select_interior_body_reference(
    bars: Sequence[Bar],
    open_row: int,
    close_row: int,
    direction: str,
    extreme_row: int,
    extreme_price: Decimal,
) -> dict:
    """Descriptive consecutive reversal pair. It does not confirm or reject a swing."""
    if direction == SWING_HIGH:
        reference_label = "BEARISH"
        validation_label = "BULLISH"
        missing = "NO_BEARISH_TO_BULLISH_INTERIOR_PAIR_FOR_SWING_HIGH"
        selected = "SWING_HIGH_BEARISH_TO_BULLISH_REFERENCE_PAIR_SELECTED"
        rule = "MINIMUM_BEARISH_CLOSE_WITH_IMMEDIATE_BULLISH_INTERIOR_SUCCESSOR"
        pair_type = "BEARISH_TO_BULLISH_INTERIOR_PAIR"
        choose_minimum = True
    else:
        reference_label = "BULLISH"
        validation_label = "BEARISH"
        missing = "NO_BULLISH_TO_BEARISH_INTERIOR_PAIR_FOR_SWING_LOW"
        selected = "SWING_LOW_BULLISH_TO_BEARISH_REFERENCE_PAIR_SELECTED"
        rule = "MAXIMUM_BULLISH_CLOSE_WITH_IMMEDIATE_BEARISH_INTERIOR_SUCCESSOR"
        pair_type = "BULLISH_TO_BEARISH_INTERIOR_PAIR"
        choose_minimum = False
    successor_not_interior = 0
    successor_wrong_direction = 0
    pre_close_excluded = 0
    successor_is_swing_close = 0
    eligible: list[int] = []
    for row in range(open_row + 1, close_row):
        candle = bars[row]
        if candle_direction(candle.open, candle.close) != reference_label:
            continue
        validation_row = row + 1
        if row == close_row - 1 or row > close_row - 2 or validation_row >= close_row:
            pre_close_excluded += 1
            successor_is_swing_close += 1
            successor_not_interior += 1
            continue
        follower = bars[validation_row]
        if candle_direction(follower.open, follower.close) != validation_label:
            successor_wrong_direction += 1
            continue
        if not (open_row < row < validation_row < close_row and row <= close_row - 2 and row != close_row - 1):
            raise SpecialSwingError("REFERENCE_CANDIDATE_IS_IMMEDIATELY_BEFORE_SWING_CLOSE")
        eligible.append(row)
    empty = {
        "status": missing,
        "rule": rule,
        "pair_type": "",
        "eligible_count": 0,
        "row": None,
        "direction": "",
        "price": None,
        "validation_row": None,
        "validation_direction": "",
        "row_difference": None,
        "close_change": None,
        "close_change_percent": None,
        "bars_after_open": None,
        "bars_before_close": None,
        "position_fraction": None,
        "validation_position_fraction": None,
        "validation_matches_extremum": None,
        "tie_count": 0,
        "tie_first": None,
        "tie_last": None,
        "matches_extremum": None,
        "distance": None,
        "successor_not_interior": successor_not_interior,
        "successor_wrong_direction": successor_wrong_direction,
        "pre_close_excluded": pre_close_excluded,
        "successor_is_swing_close": successor_is_swing_close,
    }
    if not eligible:
        return empty
    target = min(bars[row].close for row in eligible) if choose_minimum else max(bars[row].close for row in eligible)
    ties = [row for row in eligible if bars[row].close == target]
    chosen = max(ties)
    validation_row = chosen + 1
    if chosen == close_row - 1 or chosen > close_row - 2 or validation_row >= close_row:
        raise SpecialSwingError("REFERENCE_CANDIDATE_IS_IMMEDIATELY_BEFORE_SWING_CLOSE")
    reference_close = bars[chosen].close
    validation_close = bars[validation_row].close
    span = Decimal(close_row - open_row)
    change = validation_close - reference_close
    if direction == SWING_LOW:
        distance = reference_close - extreme_price
    else:
        distance = extreme_price - reference_close
    return {
        "status": "INTERIOR_BODY_REFERENCE_PAIR_EXACT_CLOSE_TIE" if len(ties) > 1 else selected,
        "rule": rule,
        "pair_type": pair_type,
        "eligible_count": len(eligible),
        "row": chosen,
        "direction": reference_label,
        "price": reference_close,
        "validation_row": validation_row,
        "validation_direction": validation_label,
        "row_difference": 1,
        "close_change": change,
        "close_change_percent": change / reference_close * D100,
        "bars_after_open": chosen - open_row,
        "bars_before_close": close_row - chosen,
        "position_fraction": Decimal(chosen - open_row) / span,
        "validation_position_fraction": Decimal(validation_row - open_row) / span,
        "validation_matches_extremum": validation_row == extreme_row,
        "tie_count": len(ties),
        "tie_first": min(ties),
        "tie_last": chosen,
        "matches_extremum": chosen == extreme_row,
        "distance": distance,
        "successor_not_interior": successor_not_interior,
        "successor_wrong_direction": successor_wrong_direction,
        "pre_close_excluded": pre_close_excluded,
        "successor_is_swing_close": successor_is_swing_close,
    }


def _segment_extreme(bars: Sequence[Bar], start: int, end: int, use_high: bool) -> dict:
    rows = range(start, end + 1)
    price = max(bars[row].high for row in rows) if use_high else min(bars[row].low for row in rows)
    ties = [row for row in rows if (bars[row].high if use_high else bars[row].low) == price]
    owner = max(ties)
    plateau_start = owner
    while plateau_start - 1 >= start and (bars[plateau_start - 1].high if use_high else bars[plateau_start - 1].low) == price:
        plateau_start -= 1
    return {
        "start": start,
        "end": end,
        "row": owner,
        "price": price,
        "count": end - start + 1,
        "tie_count": len(ties),
        "tie_first": min(ties),
        "tie_last": owner,
        "plateau_start": plateau_start,
        "plateau_end": owner,
    }


def _segment_fields(bars: Sequence[Bar], open_row: int, close_row: int, direction: str, reference_row: int | None, validation_row: int | None) -> dict:
    blank = {
        "segment_status": (
            "PEAK_SEGMENTS_NOT_AVAILABLE_NO_REFERENCE_PAIR"
            if direction == SWING_HIGH
            else "DIP_SEGMENTS_NOT_AVAILABLE_NO_REFERENCE_PAIR"
        ),
        "peak_1_row": None, "peak_1_price": None, "peak_1_count": None, "peak_1_tie_count": None,
        "peak_1_tie_first": None, "peak_1_tie_last": None, "peak_1_plateau_start": None, "peak_1_plateau_end": None,
        "peak_1_owned_by_open": None, "peak_1_owned_by_reference": None,
        "peak_2_row": None, "peak_2_price": None, "peak_2_count": None, "peak_2_tie_count": None,
        "peak_2_tie_first": None, "peak_2_tie_last": None, "peak_2_plateau_start": None, "peak_2_plateau_end": None,
        "peak_2_owned_by_validation": None, "peak_2_owned_by_close": None,
        "dip_1_row": None, "dip_1_price": None, "dip_1_count": None, "dip_1_tie_count": None,
        "dip_1_tie_first": None, "dip_1_tie_last": None, "dip_1_plateau_start": None, "dip_1_plateau_end": None,
        "dip_1_owned_by_open": None, "dip_1_owned_by_reference": None,
        "dip_2_row": None, "dip_2_price": None, "dip_2_count": None, "dip_2_tie_count": None,
        "dip_2_tie_first": None, "dip_2_tie_last": None, "dip_2_plateau_start": None, "dip_2_plateau_end": None,
        "dip_2_owned_by_validation": None, "dip_2_owned_by_close": None,
    }
    if reference_row is None or validation_row is None:
        return blank
    prefix = "PEAK" if direction == SWING_HIGH else "DIP"
    if validation_row != reference_row + 1:
        raise SpecialSwingError(f"{prefix}_SEGMENT_GAP_ERROR")
    if reference_row >= validation_row:
        raise SpecialSwingError(f"{prefix}_SEGMENT_OVERLAP_ERROR")
    covered = list(range(open_row, reference_row + 1)) + list(range(validation_row, close_row + 1))
    expected = list(range(open_row, close_row + 1))
    if covered != expected or len(covered) != len(set(covered)):
        raise SpecialSwingError(f"{prefix}_SEGMENT_COVERAGE_ERROR")
    use_high = direction == SWING_HIGH
    first = _segment_extreme(bars, open_row, reference_row, use_high)
    second = _segment_extreme(bars, validation_row, close_row, use_high)
    if first["end"] + 1 != second["start"]:
        raise SpecialSwingError(f"{prefix}_SEGMENT_GAP_ERROR")
    fields = dict(blank)
    fields["segment_status"] = f"{prefix}_1_CALCULATED; {prefix}_2_CALCULATED"
    side = "peak" if use_high else "dip"
    for label, segment, open_flag, close_flag in (
        ("1", first, "owned_by_open", "owned_by_reference"),
        ("2", second, "owned_by_validation", "owned_by_close"),
    ):
        fields[f"{side}_{label}_row"] = segment["row"]
        fields[f"{side}_{label}_price"] = segment["price"]
        fields[f"{side}_{label}_count"] = segment["count"]
        fields[f"{side}_{label}_tie_count"] = segment["tie_count"]
        fields[f"{side}_{label}_tie_first"] = segment["tie_first"]
        fields[f"{side}_{label}_tie_last"] = segment["tie_last"]
        fields[f"{side}_{label}_plateau_start"] = segment["plateau_start"]
        fields[f"{side}_{label}_plateau_end"] = segment["plateau_end"]
        fields[f"{side}_{label}_{open_flag}"] = segment["row"] == segment["start"]
        fields[f"{side}_{label}_{close_flag}"] = segment["row"] == segment["end"]
    return fields


def _build(
    bars: Sequence[Bar],
    open_row: int,
    close_row: int,
    direction: str,
    interior: int,
    provisional: dict | None = None,
    earlier_rejections: int = 0,
    first_failure_row: int | None = None,
    last_failure_row: int | None = None,
) -> tuple[Swing | None, str | None]:
    blocked = duration_reason(interior)
    if blocked is not None:
        return None, blocked
    window = range(open_row + 1, close_row)
    if not window:
        return None, "NO_STRICTLY_INTERIOR_EXTREME"
    opened = bars[open_row]
    closed = bars[close_row]
    open_label = candle_direction(opened.open, opened.close)
    close_label = candle_direction(closed.open, closed.close)
    if direction == SWING_HIGH and open_label != "BULLISH":
        return None, "SWING_HIGH_OPEN_NOT_BULLISH"
    if direction == SWING_LOW and open_label != "BEARISH":
        return None, "SWING_LOW_OPEN_NOT_BEARISH"
    if close_label == "DOJI":
        return None, "SWING_CLOSE_DOJI_DIRECTION_FAILURE"
    if direction == SWING_HIGH and close_label != "BEARISH":
        return None, "SWING_HIGH_CLOSE_NOT_BEARISH"
    if direction == SWING_LOW and close_label != "BULLISH":
        return None, "SWING_LOW_CLOSE_NOT_BULLISH"
    reference = opened.open
    close_price = closed.close
    if direction == SWING_HIGH:
        price = max(bars[row].high for row in window)
        matches = {row for row in window if bars[row].high == price}
        open_width = price - reference
        close_width = price - close_price
    else:
        price = min(bars[row].low for row in window)
        matches = {row for row in window if bars[row].low == price}
        open_width = reference - price
        close_width = close_price - price
    end = max(matches)
    start = end
    while start - 1 in matches:
        start -= 1
    if not (open_row < end < close_row):
        return None, "NO_STRICTLY_INTERIOR_EXTREME"
    if reference == 0 or close_price == 0:
        return None, "INVALID_OHLC"
    open_percent = open_width / reference * D100
    close_percent = close_width / close_price * D100
    reason = boundary_reason(open_percent, close_percent)
    if reason is not None:
        return None, reason
    body_low, body_high, body_size = _body(opened)
    if body_size == 0:
        return None, "SWING_OPEN_BODY_ZERO"
    threshold = body_threshold(direction, body_low, body_high, body_size)
    if not body_penetrated(direction, close_price, threshold):
        return None, "BODY_PENETRATION_NOT_REACHED"
    fraction = penetration_fraction(direction, close_price, body_low, body_high, body_size)
    if fraction < REQUIRED_FRACTION:
        return None, "BODY_PENETRATION_NOT_REACHED"
    segment = bars[open_row:close_row + 1]
    reference_candle = select_interior_body_reference(bars, open_row, close_row, direction, end, price)
    if provisional is not None and (
        provisional["row"] != reference_candle["row"]
        or provisional["price"] != reference_candle["price"]
        or provisional["validation_row"] != reference_candle["validation_row"]
        or provisional["eligible_count"] != reference_candle["eligible_count"]
    ):
        raise SpecialSwingError("FINAL_REFERENCE_MISMATCH_WITH_ACCEPTED_PROVISIONAL_REFERENCE")
    if reference_candle["price"] is None:
        compatibility_status = "NOT_APPLICABLE_NO_REFERENCE_PAIR"
        margin = None
        margin_percent = None
    else:
        margin = reference_to_close_margin(direction, reference_candle["price"], close_price)
        if margin < 0:
            raise SpecialSwingError("compatibility-failed candidate was bound")
        compatibility_status = "PASS"
        margin_percent = margin / close_price * D100
    segments = _segment_fields(
        bars,
        open_row,
        close_row,
        direction,
        reference_candle["row"],
        reference_candle["validation_row"],
    )
    return Swing(
        raw_id="",
        swing_id="",
        direction=direction,
        open_row=open_row,
        close_row=close_row,
        extreme_row=end,
        extreme_price=price,
        plateau_start=start,
        plateau_end=end,
        event_id=f"{direction}|{format(price, 'f')}|{start}|{end}",
        family_id="",
        reference=reference,
        close_price=close_price,
        interior=interior,
        total=interior + 2,
        open_width_percent=open_percent,
        close_width_percent=close_percent,
        completion_error=abs(close_price - threshold) / threshold * D100,
        structure_high=max(item.high for item in segment),
        structure_low=min(item.low for item in segment),
        role=PRIMARY,
        status=PRIMARY,
        formation_class=FORMATION_CLASS,
        body_low=body_low,
        body_high=body_high,
        body_size=body_size,
        body_threshold=threshold,
        penetration_fraction=fraction,
        body_reference_status=reference_candle["status"],
        body_reference_rule=reference_candle["rule"],
        body_reference_row=reference_candle["row"],
        body_reference_direction=reference_candle["direction"],
        body_reference_price=reference_candle["price"],
        body_reference_bars_after_open=reference_candle["bars_after_open"],
        body_reference_bars_before_close=reference_candle["bars_before_close"],
        body_reference_position_fraction=reference_candle["position_fraction"],
        body_reference_tie_count=reference_candle["tie_count"],
        body_reference_tie_first=reference_candle["tie_first"],
        body_reference_tie_last=reference_candle["tie_last"],
        body_reference_matches_extremum=reference_candle["matches_extremum"],
        extremum_to_body_reference_distance=reference_candle["distance"],
        body_reference_pair_type=reference_candle["pair_type"],
        body_reference_eligible_count=reference_candle["eligible_count"],
        body_reference_validation_row=reference_candle["validation_row"],
        body_reference_validation_direction=reference_candle["validation_direction"],
        body_reference_row_difference=reference_candle["row_difference"],
        body_reference_close_change=reference_candle["close_change"],
        body_reference_close_change_percent=reference_candle["close_change_percent"],
        body_reference_validation_position_fraction=reference_candle["validation_position_fraction"],
        body_reference_validation_matches_extremum=reference_candle["validation_matches_extremum"],
        reference_successor_not_interior=reference_candle["successor_not_interior"],
        reference_successor_wrong_direction=reference_candle["successor_wrong_direction"],
        reference_pre_close_excluded=reference_candle["pre_close_excluded"],
        reference_successor_is_swing_close=reference_candle["successor_is_swing_close"],
        earlier_compatibility_rejections=earlier_rejections,
        compatibility_status=compatibility_status,
        reference_to_swing_close_margin=margin,
        reference_to_swing_close_margin_percent=margin_percent,
        first_compatibility_failure_row=first_failure_row,
        last_compatibility_failure_row=last_failure_row,
        final_reference_matches_provisional=True,
        **segments,
    ), None


def _assign_raw_ids(raw: list[Swing]) -> None:
    raw.sort(key=lambda item: (item.close_row, item.direction, item.open_row))
    for index, item in enumerate(raw, start=1):
        item.raw_id = f"S30-RAW-{index:06d}"


def _primary_key(item: Swing):
    return (
        item.total,
        item.interior,
        item.completion_error,
        -min(item.open_width_percent, item.close_width_percent),
        item.close_row,
        -item.open_row,
        item.raw_id,
    )


def _assign_families(raw: list[Swing]) -> None:
    families: dict[str, list[Swing]] = {}
    for item in raw:
        families.setdefault(item.event_id, []).append(item)
    for event_id, members in families.items():
        members.sort(key=_primary_key)
        winner = members[0]
        winner.family_id = f"FAM-{event_id}"
        winner.role = PRIMARY
        winner.status = PRIMARY
        for member in members[1:]:
            member.family_id = winner.family_id
            member.role = DERIVED
            member.status = DERIVED
            member.suppressed_by = winner.raw_id


def _overlaps(left: Swing, right: Swing) -> bool:
    if left.direction != right.direction:
        return False
    if max(left.open_row, right.open_row) > min(left.close_row, right.close_row):
        return False
    left_inside = right.open_row <= left.extreme_row <= right.close_row
    right_inside = left.open_row <= right.extreme_row <= left.close_row
    return left.event_id == right.event_id or left_inside or right_inside


def _suppress_overlap(raw: list[Swing]) -> None:
    primaries = [item for item in raw if item.role == PRIMARY]
    parent = list(range(len(primaries)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    for left_index, left in enumerate(primaries):
        for right_index in range(left_index + 1, len(primaries)):
            if _overlaps(left, primaries[right_index]):
                parent[find(right_index)] = find(left_index)
    groups: dict[int, list[Swing]] = {}
    for index, item in enumerate(primaries):
        groups.setdefault(find(index), []).append(item)
    for members in groups.values():
        if len(members) == 1:
            continue
        members.sort(key=_primary_key)
        winner = members[0]
        for member in members[1:]:
            contained = any(
                member.open_row >= other.open_row
                and member.close_row <= other.close_row
                and (member.open_row > other.open_row or member.close_row < other.close_row)
                for other in members
                if other is not member
            )
            contains_winner = (
                winner.open_row >= member.open_row
                and winner.close_row <= member.close_row
                and (winner.open_row > member.open_row or winner.close_row < member.close_row)
            )
            member.status = NESTED if contained and not contains_winner else LONGER
            member.role = member.status
            member.suppressed_by = winner.raw_id


def _assign_display_ids(raw: list[Swing]) -> None:
    displayed = [item for item in raw if item.status == PRIMARY]
    displayed.sort(key=lambda item: (item.open_row, item.close_row, item.raw_id))
    for index, item in enumerate(displayed, start=1):
        item.swing_id = f"S30-{index:06d}"


def _check_body_reference(bars: Sequence[Bar], item: Swing) -> None:
    again = select_interior_body_reference(
        bars, item.open_row, item.close_row, item.direction, item.extreme_row, item.extreme_price
    )
    if item.body_reference_status != again["status"] or item.body_reference_row != again["row"]:
        raise SpecialSwingError("body-reference selection mismatch")
    if item.body_reference_price != again["price"] or item.body_reference_tie_count != again["tie_count"]:
        raise SpecialSwingError("body-reference selection mismatch")
    if item.body_reference_validation_row != again["validation_row"] or item.body_reference_eligible_count != again["eligible_count"]:
        raise SpecialSwingError("body-reference pair selection mismatch")
    obsolete = {
        "SWING_HIGH_BEARISH_INTERIOR_REFERENCE_SELECTED",
        "SWING_LOW_BULLISH_INTERIOR_REFERENCE_SELECTED",
        "SWING_HIGH_BULLISH_INTERIOR_REFERENCE_SELECTED",
        "SWING_LOW_BEARISH_INTERIOR_REFERENCE_SELECTED",
        "NO_BEARISH_INTERIOR_CANDLE_FOR_SWING_HIGH",
        "NO_BULLISH_INTERIOR_CANDLE_FOR_SWING_LOW",
        "INTERIOR_BODY_REFERENCE_EXACT_CLOSE_TIE",
        "MINIMUM_CLOSE_AMONG_STRICTLY_INTERIOR_BEARISH_CANDLES",
        "MAXIMUM_CLOSE_AMONG_STRICTLY_INTERIOR_BULLISH_CANDLES",
    }
    if item.body_reference_status in obsolete or item.body_reference_rule in obsolete:
        raise SpecialSwingError("obsolete single-candle body reference")
    if item.body_reference_row is None:
        if item.body_reference_status not in {
            "NO_BEARISH_TO_BULLISH_INTERIOR_PAIR_FOR_SWING_HIGH",
            "NO_BULLISH_TO_BEARISH_INTERIOR_PAIR_FOR_SWING_LOW",
        }:
            raise SpecialSwingError("missing body reference has the wrong status")
        if item.body_reference_price is not None or item.body_reference_tie_count != 0 or item.body_reference_eligible_count != 0:
            raise SpecialSwingError("missing body reference contains a price")
        if item.body_reference_validation_row is not None or item.body_reference_pair_type:
            raise SpecialSwingError("missing body reference contains a validation candle")
        if item.compatibility_status != "NOT_APPLICABLE_NO_REFERENCE_PAIR":
            raise SpecialSwingError("missing pair compatibility status is wrong")
        if item.reference_to_swing_close_margin is not None or item.reference_to_swing_close_margin_percent is not None:
            raise SpecialSwingError("missing pair contains a compatibility margin")
        if not item.final_reference_matches_provisional:
            raise SpecialSwingError("FINAL_REFERENCE_MISMATCH_WITH_ACCEPTED_PROVISIONAL_REFERENCE")
        return
    validation_row = item.body_reference_validation_row
    if validation_row is None or validation_row != item.body_reference_row + 1:
        raise SpecialSwingError("reference pair is not consecutive")
    if not (item.open_row < item.body_reference_row < validation_row < item.close_row):
        raise SpecialSwingError("body reference endpoint violation")
    if item.body_reference_row > item.close_row - 2 or item.body_reference_row == item.close_row - 1:
        raise SpecialSwingError("REFERENCE_CANDIDATE_IS_IMMEDIATELY_BEFORE_SWING_CLOSE")
    if validation_row == item.close_row:
        raise SpecialSwingError("REFERENCE_CANDIDATE_SUCCESSOR_IS_SWING_CLOSE")
    candle = bars[item.body_reference_row]
    follower = bars[validation_row]
    if item.body_reference_price != candle.close:
        raise SpecialSwingError("body reference was not selected from Close")
    if item.body_reference_row_difference != 1:
        raise SpecialSwingError("reference pair row difference is not 1")
    if item.direction == SWING_HIGH:
        if not (candle.close < candle.open and follower.close > follower.open):
            raise SpecialSwingError("swing high reference pair direction is wrong")
        if item.body_reference_pair_type != "BEARISH_TO_BULLISH_INTERIOR_PAIR":
            raise SpecialSwingError("swing high reference pair type is wrong")
    else:
        if not (candle.close > candle.open and follower.close < follower.open):
            raise SpecialSwingError("swing low reference pair direction is wrong")
        if item.body_reference_pair_type != "BULLISH_TO_BEARISH_INTERIOR_PAIR":
            raise SpecialSwingError("swing low reference pair type is wrong")
    if item.body_reference_close_change != follower.close - candle.close:
        raise SpecialSwingError("reference-to-validation close change is wrong")
    if item.body_reference_bars_after_open != item.body_reference_row - item.open_row:
        raise SpecialSwingError("body reference bars after Swing Open are wrong")
    if item.body_reference_bars_before_close != item.close_row - item.body_reference_row:
        raise SpecialSwingError("body reference bars before Swing Close are wrong")
    if item.body_reference_bars_after_open < 1 or item.body_reference_bars_before_close < 2:
        raise SpecialSwingError("body reference endpoint violation")
    if item.body_reference_matches_extremum != (item.body_reference_row == item.extreme_row):
        raise SpecialSwingError("body reference extremum match flag is wrong")
    if item.body_reference_validation_matches_extremum != (validation_row == item.extreme_row):
        raise SpecialSwingError("validation extremum match flag is wrong")
    if item.extremum_to_body_reference_distance is None or item.extremum_to_body_reference_distance < 0:
        raise SpecialSwingError("extremum-to-body-reference distance is negative")
    if item.body_reference_tie_last != item.body_reference_row:
        raise SpecialSwingError("body reference tie representative is not the last match")
    if item.compatibility_status != "PASS":
        raise SpecialSwingError("confirmed reference compatibility did not pass")
    expected_margin = reference_to_close_margin(item.direction, item.body_reference_price, item.close_price)
    if item.reference_to_swing_close_margin != expected_margin or expected_margin < 0:
        raise SpecialSwingError("reference-to-close margin is wrong")
    if item.reference_to_swing_close_margin_percent != expected_margin / item.close_price * D100:
        raise SpecialSwingError("reference-to-close margin percent is wrong")
    if item.direction == SWING_HIGH and item.body_reference_price < item.close_price:
        raise SpecialSwingError("confirmed swing high reference is below the close")
    if item.direction == SWING_LOW and item.body_reference_price > item.close_price:
        raise SpecialSwingError("confirmed swing low reference is above the close")
    if not item.final_reference_matches_provisional:
        raise SpecialSwingError("FINAL_REFERENCE_MISMATCH_WITH_ACCEPTED_PROVISIONAL_REFERENCE")


def _check_segments(bars: Sequence[Bar], item: Swing) -> None:
    again = _segment_fields(
        bars,
        item.open_row,
        item.close_row,
        item.direction,
        item.body_reference_row,
        item.body_reference_validation_row,
    )
    for key, value in again.items():
        if getattr(item, key) != value:
            raise SpecialSwingError(f"segment field mismatch {key}")
    if item.body_reference_row is None:
        if item.direction == SWING_HIGH and item.segment_status != "PEAK_SEGMENTS_NOT_AVAILABLE_NO_REFERENCE_PAIR":
            raise SpecialSwingError("PEAK_SEGMENTS_NOT_AVAILABLE_NO_REFERENCE_PAIR")
        if item.direction == SWING_LOW and item.segment_status != "DIP_SEGMENTS_NOT_AVAILABLE_NO_REFERENCE_PAIR":
            raise SpecialSwingError("DIP_SEGMENTS_NOT_AVAILABLE_NO_REFERENCE_PAIR")
        return
    if item.body_reference_validation_row != item.body_reference_row + 1:
        prefix = "PEAK" if item.direction == SWING_HIGH else "DIP"
        raise SpecialSwingError(f"{prefix}_SEGMENT_GAP_ERROR")
    first_rows = range(item.open_row, item.body_reference_row + 1)
    second_rows = range(item.body_reference_validation_row, item.close_row + 1)
    if item.direction == SWING_HIGH:
        peak_1 = max(bars[row].high for row in first_rows)
        peak_2 = max(bars[row].high for row in second_rows)
        if item.peak_1_price != peak_1 or item.peak_2_price != peak_2:
            raise SpecialSwingError("peak price selection error")
        if bars[item.peak_1_row].high != item.peak_1_price or bars[item.peak_2_row].high != item.peak_2_price:
            raise SpecialSwingError("peak owner OHLC mismatch")
        if max(item.peak_1_price, item.peak_2_price) != max(bar.high for bar in bars[item.open_row:item.close_row + 1]):
            raise SpecialSwingError("PEAK_SEGMENT_COVERAGE_ERROR")
        if item.peak_1_row != max(row for row in first_rows if bars[row].high == peak_1):
            raise SpecialSwingError("peak tie representative is not the last match")
        if item.peak_2_row != max(row for row in second_rows if bars[row].high == peak_2):
            raise SpecialSwingError("peak tie representative is not the last match")
        if item.dip_1_price is not None or item.dip_2_price is not None:
            raise SpecialSwingError("swing high contains dip fields")
        return
    dip_1 = min(bars[row].low for row in first_rows)
    dip_2 = min(bars[row].low for row in second_rows)
    if item.dip_1_price != dip_1 or item.dip_2_price != dip_2:
        raise SpecialSwingError("dip price selection error")
    if bars[item.dip_1_row].low != item.dip_1_price or bars[item.dip_2_row].low != item.dip_2_price:
        raise SpecialSwingError("dip owner OHLC mismatch")
    if min(item.dip_1_price, item.dip_2_price) != min(bar.low for bar in bars[item.open_row:item.close_row + 1]):
        raise SpecialSwingError("DIP_SEGMENT_COVERAGE_ERROR")
    if item.dip_1_row != max(row for row in first_rows if bars[row].low == dip_1):
        raise SpecialSwingError("dip tie representative is not the last match")
    if item.dip_2_row != max(row for row in second_rows if bars[row].low == dip_2):
        raise SpecialSwingError("dip tie representative is not the last match")
    if item.peak_1_price is not None or item.peak_2_price is not None:
        raise SpecialSwingError("swing low contains peak fields")


def _check_invariants(bars: Sequence[Bar], raw: list[Swing], searches: list[SearchRecord]) -> None:
    seen = set()
    for item in raw:
        if item.formation_class != FORMATION_CLASS or item.interior < MIN_INTERIOR or item.interior > MAX_INTERIOR:
            raise SpecialSwingError("confirmed interior is outside 6-9")
        if item.total != item.interior + 2 or item.total < MIN_TOTAL or item.total > MAX_TOTAL:
            raise SpecialSwingError("confirmed total length is outside 8-11")
        if not (item.open_row < item.extreme_row < item.close_row):
            raise SpecialSwingError("extremum is not strictly interior")
        if item.body_size <= 0 or item.penetration_fraction < REQUIRED_FRACTION:
            raise SpecialSwingError("body penetration is below one third")
        if item.open_width_percent < MIN_BOUNDARY or item.close_width_percent < MIN_BOUNDARY:
            raise SpecialSwingError("confirmed boundary is below 1.00")
        _check_segments(bars, item)
        opened = bars[item.open_row]
        closed = bars[item.close_row]
        if item.direction == SWING_HIGH:
            if candle_direction(opened.open, opened.close) != "BULLISH":
                raise SpecialSwingError("confirmed swing high open is not bullish")
            if candle_direction(closed.open, closed.close) != "BEARISH":
                raise SpecialSwingError("confirmed swing high close is not bearish")
        else:
            if candle_direction(opened.open, opened.close) != "BEARISH":
                raise SpecialSwingError("confirmed swing low open is not bearish")
            if candle_direction(closed.open, closed.close) != "BULLISH":
                raise SpecialSwingError("confirmed swing low close is not bullish")
        _check_body_reference(bars, item)
        key = (item.direction, item.open_row, item.close_row, format(item.extreme_price, "f"))
        if key in seen:
            raise SpecialSwingError("DUPLICATE_EXACT_CANDIDATE")
        seen.add(key)
    for record in searches:
        if record.max_interior > MAX_INTERIOR or record.above_horizon:
            raise SpecialSwingError("HARD_SEARCH_HORIZON_ERROR")
        if record.terminal in FORBIDDEN_TERMINALS:
            raise SpecialSwingError(f"forbidden terminal {record.terminal}")


def _counters(raw: list[Swing], searches: list[SearchRecord], displayed: list[Swing]) -> dict:
    def interior_map(items: list[Swing]) -> dict[str, int]:
        return {str(length): sum(1 for item in items if item.interior == length) for length in range(6, 10)}

    opens = [item.open_width_percent for item in displayed]
    closes = [item.close_width_percent for item in displayed]
    selected = [item for item in displayed if item.body_reference_row is not None]
    after = [item.body_reference_bars_after_open for item in selected]
    before = [item.body_reference_bars_before_close for item in selected]
    ties = [item.body_reference_tie_count for item in displayed if item.body_reference_tie_count > 1]
    return {
        "searches": len(searches),
        "raw": len(raw),
        "displayed": len(displayed),
        "swing_high": sum(1 for item in displayed if item.direction == SWING_HIGH),
        "swing_low": sum(1 for item in displayed if item.direction == SWING_LOW),
        "derived": sum(1 for item in raw if item.status == DERIVED),
        "nested": sum(1 for item in raw if item.status == NESTED),
        "longer": sum(1 for item in raw if item.status == LONGER),
        "zero_interior": 0,
        "alternative": 0,
        "compact": 0,
        "raw_by_interior": interior_map(raw),
        "primary_by_interior": interior_map(displayed),
        "exact_one_third": sum(1 for item in raw if item.penetration_fraction == REQUIRED_FRACTION),
        "more_than_one_third": sum(1 for item in raw if item.penetration_fraction > REQUIRED_FRACTION),
        "wick_only_events": sum(item.wick_only for item in searches),
        "wick_only_confirmations": 0,
        "doji_rejections": sum(1 for item in searches if item.terminal == "SWING_OPEN_BODY_ZERO"),
        "open_boundary_below_1_00": sum(1 for item in searches if item.terminal == "OPEN_BOUNDARY_BELOW_1_00"),
        "close_boundary_below_1_00": sum(1 for item in searches if item.terminal == "CLOSE_BOUNDARY_BELOW_1_00"),
        "both_boundaries_below_1_00": sum(1 for item in searches if item.terminal == "BOTH_BOUNDARIES_BELOW_1_00"),
        "confirmed_below_1_00": 0,
        "candidate_above_9_interiors_evaluated": sum(item.above_horizon for item in searches),
        "candidate_with_10_interiors_confirmed": sum(1 for item in raw if item.interior >= 10),
        "candidate_below_6_interiors_confirmed": sum(1 for item in raw if item.interior < 6),
        "candidate_below_1_00_boundary_confirmed": 0,
        "candidate_below_1_00_boundary_confirmed_count": 0,
        "horizon_exhausted": sum(1 for item in searches if item.terminal == "SEARCH_HORIZON_EXHAUSTED_AT_9_INTERIORS"),
        "censored": sum(1 for item in searches if item.terminal == "SEARCH_CENSORED_BY_DATASET_END"),
        "not_yet_eligible": sum(1 for item in searches if item.terminal == "SEARCH_NOT_YET_ELIGIBLE_BELOW_6_INTERIORS"),
        "evaluated_closes": sum(item.evaluated_closes for item in searches),
        "failed_binding": sum(1 for item in searches if item.binding_failed),
        "min_open_boundary": min(opens) if opens else None,
        "max_open_boundary": max(opens) if opens else None,
        "min_close_boundary": min(closes) if closes else None,
        "max_close_boundary": max(closes) if closes else None,
        "lookahead_violations": 0,
        "repaint_count": 0,
        "swing_high_bullish_open_seeds": sum(1 for item in searches if item.direction == SWING_HIGH and item.open_seed),
        "swing_high_rejected_non_bullish_opens": sum(1 for item in searches if item.direction == SWING_HIGH and item.open_rejected),
        "swing_low_bearish_open_seeds": sum(1 for item in searches if item.direction == SWING_LOW and item.open_seed),
        "swing_low_rejected_non_bearish_opens": sum(1 for item in searches if item.direction == SWING_LOW and item.open_rejected),
        "bearish_swing_high_close_candidates": sum(item.directional_close_candidates for item in searches if item.direction == SWING_HIGH),
        "bullish_swing_low_close_candidates": sum(item.directional_close_candidates for item in searches if item.direction == SWING_LOW),
        "wrong_direction_threshold_crossings_high": sum(item.wrong_direction_crossings for item in searches if item.direction == SWING_HIGH),
        "wrong_direction_threshold_crossings_low": sum(item.wrong_direction_crossings for item in searches if item.direction == SWING_LOW),
        "swing_close_doji_rejections": sum(item.close_doji_crossings for item in searches),
        "directionally_valid_bindings": sum(1 for item in searches if item.directionally_bound),
        "confirmed_swing_high_non_bullish_open_count": 0,
        "confirmed_swing_high_non_bearish_close_count": 0,
        "confirmed_swing_low_non_bearish_open_count": 0,
        "confirmed_swing_low_non_bullish_close_count": 0,
        "confirmed_doji_endpoint_count": 0,
        "wrong_direction_threshold_crossing_bound_count": 0,
        "eligible_swing_high_bearish_to_bullish_pairs": sum(
            item.body_reference_eligible_count for item in displayed if item.direction == SWING_HIGH
        ),
        "eligible_swing_low_bullish_to_bearish_pairs": sum(
            item.body_reference_eligible_count for item in displayed if item.direction == SWING_LOW
        ),
        "swing_high_bearish_to_bullish_reference_pairs": sum(
            1 for item in displayed if item.body_reference_status in {
                "SWING_HIGH_BEARISH_TO_BULLISH_REFERENCE_PAIR_SELECTED",
                "INTERIOR_BODY_REFERENCE_PAIR_EXACT_CLOSE_TIE",
            } and item.direction == SWING_HIGH and item.body_reference_row is not None
        ),
        "swing_low_bullish_to_bearish_reference_pairs": sum(
            1 for item in displayed if item.direction == SWING_LOW and item.body_reference_row is not None
        ),
        "reference_successor_wrong_direction_rejected": sum(item.reference_successor_wrong_direction for item in displayed),
        "reference_successor_not_interior_rejected": sum(item.reference_successor_not_interior for item in displayed),
        "pre_close_reference_candidates_excluded": sum(item.reference_pre_close_excluded for item in displayed),
        "successor_is_swing_close_exclusions": sum(item.reference_successor_is_swing_close for item in displayed),
        "selected_reference_at_close_row_minus_1_count": sum(
            1 for item in raw if item.body_reference_row is not None and item.body_reference_row == item.close_row - 1
        ),
        "selected_reference_validation_is_swing_close_count": sum(
            1 for item in raw if item.body_reference_validation_row is not None and item.body_reference_validation_row == item.close_row
        ),
        "selected_reference_missing_open_count": 0,
        "selected_reference_missing_high_count": 0,
        "selected_reference_missing_low_count": 0,
        "selected_reference_missing_close_count": 0,
        "selected_reference_ohlc_complete_count": sum(1 for item in displayed if item.body_reference_row is not None),
        "minimum_reference_rows_before_swing_close": min(before) if before else None,
        "maximum_reference_rows_before_swing_close": max(before) if before else None,
        "no_bearish_to_bullish_interior_pair_for_swing_high": sum(
            1 for item in displayed if item.body_reference_status == "NO_BEARISH_TO_BULLISH_INTERIOR_PAIR_FOR_SWING_HIGH"
        ),
        "no_bullish_to_bearish_interior_pair_for_swing_low": sum(
            1 for item in displayed if item.body_reference_status == "NO_BULLISH_TO_BEARISH_INTERIOR_PAIR_FOR_SWING_LOW"
        ),
        "reference_pair_nonconsecutive_count": 0,
        "reference_pair_endpoint_inclusion_count": 0,
        "reference_pair_wrong_reference_direction_count": 0,
        "reference_pair_wrong_validation_direction_count": 0,
        "reference_pair_price_selection_error_count": 0,
        "reference_pair_wick_selection_count": 0,
        "exact_body_reference_close_ties": len(ties),
        "maximum_body_reference_tie_size": max(ties) if ties else 0,
        "body_reference_matches_extremum": sum(1 for item in selected if item.body_reference_matches_extremum),
        "body_reference_differs_from_extremum": sum(1 for item in selected if not item.body_reference_matches_extremum),
        "minimum_bars_after_swing_open": min(after) if after else None,
        "maximum_bars_after_swing_open": max(after) if after else None,
        "minimum_bars_before_swing_close": min(before) if before else None,
        "maximum_bars_before_swing_close": max(before) if before else None,
        "body_reference_endpoint_violations": 0,
        "body_reference_wrong_direction": 0,
        "body_reference_price_selection_errors": 0,
        "body_reference_wick_selections": 0,
        "body_reference_geometric_center_requirements": 0,
        "body_reference_repaint_count": 0,
        "reference_pair_repaint_count": 0,
        "missing_body_reference_rejected_swings": 0,
        "missing_pair_rejected_swing_count": 0,
        "candidate_counts_by_interior": {
            str(length): sum(item.duration_counts.get(length, 0) for item in searches)
            for length in range(6, 10)
        },
        "provisional_reference_recalculation_count": sum(item.reference_recalculations for item in searches),
        "compatibility_evaluations": sum(item.compatibility_evaluated for item in searches),
        "penetration_passing_candidates": sum(item.penetration_passing for item in searches),
        "swing_high_reference_below_close_failures": sum(item.reference_below_close for item in searches),
        "swing_low_reference_above_close_failures": sum(item.reference_above_close for item in searches),
        "candidates_accepted_on_equality": sum(item.equality_passes for item in searches),
        "candidates_accepted_with_positive_margin": sum(item.positive_margin_passes for item in searches),
        "missing_pair_candidates": sum(item.missing_pair_candidates for item in searches),
        "later_closes_accepted_after_compatibility_failures": sum(
            1 for item in searches if item.accepted_after_compatibility_failure
        ),
        "searches_exhausted_after_only_compatibility_failures": sum(
            1 for item in searches if item.exhausted_after_only_compatibility_failures
        ),
        "maximum_compatibility_failures_before_acceptance": max(
            (item.compatibility_rejected for item in searches if item.directionally_bound),
            default=0,
        ),
        "maximum_compatibility_rejections_in_any_search": max(
            (item.compatibility_rejected for item in searches),
            default=0,
        ),
        "confirmed_equality_passes": sum(
            1 for item in raw if item.reference_to_swing_close_margin == 0
        ),
        "confirmed_positive_margin_passes": sum(
            1 for item in raw if item.reference_to_swing_close_margin is not None and item.reference_to_swing_close_margin > 0
        ),
        "confirmed_missing_pair_count": sum(1 for item in raw if item.body_reference_row is None),
        "confirmed_swing_high_reference_below_close_count": sum(
            1 for item in raw
            if item.direction == SWING_HIGH and item.body_reference_price is not None and item.body_reference_price < item.close_price
        ),
        "confirmed_swing_low_reference_above_close_count": sum(
            1 for item in raw
            if item.direction == SWING_LOW and item.body_reference_price is not None and item.body_reference_price > item.close_price
        ),
        "compatibility_failed_candidate_bound_count": 0,
        "search_beyond_nine_interiors_count": sum(item.above_horizon for item in searches),
        "stale_provisional_reference_reuse_count": 0,
        "second_best_reference_substitution_count": 0,
        "final_reference_mismatch_count": sum(1 for item in raw if not item.final_reference_matches_provisional),
        "reference_pair_wrong_direction_count": 0,
        "lookahead_violation_count": 0,
        "peak_1_calculated": sum(1 for item in raw if item.peak_1_row is not None),
        "peak_2_calculated": sum(1 for item in raw if item.peak_2_row is not None),
        "dip_1_calculated": sum(1 for item in raw if item.dip_1_row is not None),
        "dip_2_calculated": sum(1 for item in raw if item.dip_2_row is not None),
        "peak_1_displayed": sum(1 for item in displayed if item.peak_1_row is not None),
        "peak_2_displayed": sum(1 for item in displayed if item.peak_2_row is not None),
        "dip_1_displayed": sum(1 for item in displayed if item.dip_1_row is not None),
        "dip_2_displayed": sum(1 for item in displayed if item.dip_2_row is not None),
        "peak_segment_gap_count": 0,
        "peak_segment_overlap_count": 0,
        "peak_segment_coverage_error_count": 0,
        "dip_segment_gap_count": 0,
        "dip_segment_overlap_count": 0,
        "dip_segment_coverage_error_count": 0,
        "peak_price_selection_error_count": 0,
        "dip_price_selection_error_count": 0,
        "peak_owner_ohlc_mismatch_count": 0,
        "dip_owner_ohlc_mismatch_count": 0,
        "peak_dip_used_as_filter_count": 0,
        "peak_dip_used_for_boundary_count": 0,
        "peak_1_tie_segments": sum(1 for item in displayed if item.peak_1_tie_count is not None and item.peak_1_tie_count > 1),
        "peak_2_tie_segments": sum(1 for item in displayed if item.peak_2_tie_count is not None and item.peak_2_tie_count > 1),
        "dip_1_tie_segments": sum(1 for item in displayed if item.dip_1_tie_count is not None and item.dip_1_tie_count > 1),
        "dip_2_tie_segments": sum(1 for item in displayed if item.dip_2_tie_count is not None and item.dip_2_tie_count > 1),
        "peak_1_owned_by_swing_open": sum(1 for item in displayed if item.peak_1_owned_by_open),
        "peak_1_owned_by_reference": sum(1 for item in displayed if item.peak_1_owned_by_reference),
        "peak_2_owned_by_validation": sum(1 for item in displayed if item.peak_2_owned_by_validation),
        "peak_2_owned_by_swing_close": sum(1 for item in displayed if item.peak_2_owned_by_close),
        "dip_1_owned_by_swing_open": sum(1 for item in displayed if item.dip_1_owned_by_open),
        "dip_1_owned_by_reference": sum(1 for item in displayed if item.dip_1_owned_by_reference),
        "dip_2_owned_by_validation": sum(1 for item in displayed if item.dip_2_owned_by_validation),
        "dip_2_owned_by_swing_close": sum(1 for item in displayed if item.dip_2_owned_by_close),
        "missing_peak_segments": sum(
            1 for item in displayed if item.segment_status == "PEAK_SEGMENTS_NOT_AVAILABLE_NO_REFERENCE_PAIR"
        ),
        "missing_dip_segments": sum(
            1 for item in displayed if item.segment_status == "DIP_SEGMENTS_NOT_AVAILABLE_NO_REFERENCE_PAIR"
        ),
        "peak_dip_repaint_count": 0,
    }


def signature(result: Analysis) -> str:
    import hashlib
    import json

    payload = [
        {
            "id": item.swing_id,
            "raw": item.raw_id,
            "direction": item.direction,
            "open": item.open_row,
            "close": item.close_row,
            "extreme": format(item.extreme_price, "f"),
            "status": item.status,
            "body_reference_row": item.body_reference_row,
            "body_reference_validation_row": item.body_reference_validation_row,
            "body_reference_price": None if item.body_reference_price is None else format(item.body_reference_price, "f"),
            "body_reference_status": item.body_reference_status,
            "compatibility_status": item.compatibility_status,
            "margin": None if item.reference_to_swing_close_margin is None else format(item.reference_to_swing_close_margin, "f"),
            "earlier_compatibility_rejections": item.earlier_compatibility_rejections,
            "segment_status": item.segment_status,
            "peak_1_row": item.peak_1_row,
            "peak_1_price": None if item.peak_1_price is None else format(item.peak_1_price, "f"),
            "peak_2_row": item.peak_2_row,
            "peak_2_price": None if item.peak_2_price is None else format(item.peak_2_price, "f"),
            "dip_1_row": item.dip_1_row,
            "dip_1_price": None if item.dip_1_price is None else format(item.dip_1_price, "f"),
            "dip_2_row": item.dip_2_row,
            "dip_2_price": None if item.dip_2_price is None else format(item.dip_2_price, "f"),
        }
        for item in result.raw
    ]
    return hashlib.sha256(json.dumps(payload).encode("utf-8")).hexdigest()
