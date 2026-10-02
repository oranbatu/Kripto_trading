"""Causal body-based Order Block detection.

Qualification, zone construction, mitigation, and invalidation read candle
open and close only. High and low are copied for audit and for a separate
wick-contact diagnostic. They never change a disposition, a zone, a quality
label, or a lifecycle status.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Sequence

from detectors.order_block_4h_v1.order_block_config import BAR_HOURS, DISPLAY_TZ, LOOKBACK

D0 = Decimal("0")
D1 = Decimal("1")
D2 = Decimal("2")
D3 = Decimal("3")
D4 = Decimal("4")
D20 = Decimal("20")
D24 = Decimal("24")
D100 = Decimal("100")
MINIMUM_DISPLACEMENT = D1

QUALIFIED_BULLISH = "QUALIFIED_BULLISH_ORDER_BLOCK"
QUALIFIED_BEARISH = "QUALIFIED_BEARISH_ORDER_BLOCK"
REJECTED_SAME_DIRECTION = "REJECTED_SAME_DIRECTION"
REJECTED_ORIGIN_DOJI = "REJECTED_ORIGIN_DOJI"
REJECTED_IMPULSE_DOJI = "REJECTED_IMPULSE_DOJI"
REJECTED_BULLISH_BELOW = "REJECTED_BULLISH_DISPLACEMENT_BELOW_1_PERCENT"
REJECTED_BEARISH_BELOW = "REJECTED_BEARISH_DISPLACEMENT_BELOW_1_PERCENT"
REJECTED_DIRECTION_MISMATCH = "REJECTED_DIRECTION_MISMATCH"
DISPOSITIONS = (
    QUALIFIED_BULLISH,
    QUALIFIED_BEARISH,
    REJECTED_SAME_DIRECTION,
    REJECTED_ORIGIN_DOJI,
    REJECTED_IMPULSE_DOJI,
    REJECTED_BULLISH_BELOW,
    REJECTED_BEARISH_BELOW,
    REJECTED_DIRECTION_MISMATCH,
)

ACTIVE_UNTOUCHED = "ACTIVE_UNTOUCHED"
ACTIVE_BODY_MITIGATED = "ACTIVE_BODY_MITIGATED"
INVALIDATED = "INVALIDATED_BY_BODY_CLOSE"
TERMINAL = "TERMINAL_AT_DATASET_END"
STATUSES = (ACTIVE_UNTOUCHED, ACTIVE_BODY_MITIGATED, INVALIDATED, TERMINAL)

QUALIFICATION_PRICE_MODE = "OPEN_CLOSE_ONLY"
ZONE_DEFINITION = "ORIGIN_CANDLE_BODY_ONLY"
STRUCTURE_METRIC = "CLOSE_BASED_DIAGNOSTIC_ONLY"
FORWARD_METRIC_CLASS = "EX_POST_FORWARD_METRIC"


class OrderBlockError(RuntimeError):
    """The detector stopped because an input or result violated a hard rule."""


@dataclass(slots=True)
class Bar:
    row: int
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trades: int


@dataclass(slots=True)
class PairRecord:
    pair_id: str
    origin_row: int
    impulse_row: int
    origin_open_time: datetime
    impulse_open_time: datetime
    impulse_close_time: datetime
    origin_open: Decimal
    origin_close: Decimal
    impulse_open: Decimal
    impulse_close: Decimal
    origin_direction: str
    impulse_direction: str
    bullish_displacement_percent: Decimal
    bearish_displacement_percent: Decimal
    bullish_qualification: bool
    bearish_qualification: bool
    disposition: str
    order_block_id: str | None


@dataclass(slots=True)
class LifecycleEvent:
    order_block_id: str
    direction: str
    event_type: str
    row: int
    open_time: datetime
    close_time: datetime
    open: Decimal
    close: Decimal
    bars_from_confirmation: int
    detail: str


@dataclass(slots=True)
class OrderBlock:
    order_block_id: str
    direction: str
    sequence: int
    origin_row: int
    impulse_row: int
    origin_open_time: datetime
    origin_close_time: datetime
    impulse_open_time: datetime
    impulse_close_time: datetime
    confirmed_at: datetime
    origin_open: Decimal
    origin_close: Decimal
    origin_high: Decimal
    origin_low: Decimal
    impulse_open: Decimal
    impulse_close: Decimal
    impulse_high: Decimal
    impulse_low: Decimal
    origin_direction: str
    impulse_direction: str
    origin_body_price: Decimal
    origin_body_percent: Decimal
    impulse_body_price: Decimal
    impulse_body_percent: Decimal
    impulse_to_origin_body_ratio: Decimal
    displacement_price: Decimal
    displacement_percent: Decimal
    minimum_displacement_percent: Decimal
    displacement_pass: bool
    zone_lower: Decimal
    zone_upper: Decimal
    zone_midpoint: Decimal
    zone_width_price: Decimal
    zone_width_percent: Decimal
    rolling_median_body: Decimal | None
    rolling_mean_body: Decimal | None
    rolling_median_body_percent: Decimal | None
    rolling_mean_body_percent: Decimal | None
    impulse_body_to_median_ratio: Decimal | None
    displacement_to_median_body_ratio: Decimal | None
    close_structure_break: str
    prior_close_extreme: Decimal | None
    break_distance: Decimal | None
    break_percent: Decimal | None
    quality_score: Decimal
    quality_label: str
    status: str
    ever_body_touched: bool
    ever_midpoint: bool
    ever_full_traversal: bool
    ever_invalidated: bool
    wick_only_contact: bool
    ever_gap: bool
    first_event: str | None
    latest_event: str | None
    first_touch_row: int | None
    first_touch_open_time: datetime | None
    first_touch_close_time: datetime | None
    first_touch_open: Decimal | None
    first_touch_close: Decimal | None
    bars_to_touch: int | None
    hours_to_touch: Decimal | None
    days_to_touch: Decimal | None
    body_overlap_price: Decimal | None
    body_overlap_percent: Decimal | None
    retrace_percent_raw: Decimal | None
    retrace_percent_display: Decimal | None
    body_beyond_far_edge: bool | None
    midpoint_on_first_touch: bool | None
    full_traversal_on_first_touch: bool | None
    bars_to_midpoint: int | None
    bars_to_invalidation: int | None
    first_invalidation_row: int | None
    first_invalidation_open_time: datetime | None
    first_invalidation_close_time: datetime | None
    first_invalidation_open: Decimal | None
    first_invalidation_close: Decimal | None
    body_revisit_count: int
    touch_episode_count: int
    max_close_excursion_before: Decimal | None
    max_close_excursion_after: Decimal | None
    final_close: Decimal
    final_distance_price: Decimal
    final_distance_percent: Decimal
    final_distance_side: str
    right_censored: bool
    censoring_reason: str
    open_at_dataset_end: bool
    overlap_bullish: int = 0
    overlap_bearish: int = 0
    same_direction_overlap: int = 0
    opposite_direction_overlap: int = 0
    max_overlap_price: Decimal = field(default_factory=lambda: D0)
    max_overlap_percent: Decimal = field(default_factory=lambda: D0)
    cluster_id: str = ""


@dataclass(slots=True)
class AnalysisResult:
    bars: list[Bar]
    pairs: list[PairRecord]
    blocks: list[OrderBlock]
    events: list[LifecycleEvent]
    final_close: Decimal
    final_close_time: datetime


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


def candle_direction(open_: Decimal, close: Decimal) -> str:
    if close < open_:
        return "BEARISH"
    if close > open_:
        return "BULLISH"
    return "DOJI"


def classify_pair(
    origin_open: Decimal,
    origin_close: Decimal,
    impulse_open: Decimal,
    impulse_close: Decimal,
) -> tuple[str, Decimal, Decimal]:
    """Return disposition and both displacement percentages.

    The signature intentionally omits high and low.
    """
    if origin_open <= 0 or impulse_open <= 0:
        raise OrderBlockError("open must be positive to compute a body percentage")
    bullish_percent = (impulse_close - origin_open) / origin_open * D100
    bearish_percent = (origin_open - impulse_close) / origin_open * D100
    origin_dir = candle_direction(origin_open, origin_close)
    impulse_dir = candle_direction(impulse_open, impulse_close)
    if origin_dir == "DOJI":
        disposition = REJECTED_ORIGIN_DOJI
    elif impulse_dir == "DOJI":
        disposition = REJECTED_IMPULSE_DOJI
    elif origin_dir == impulse_dir:
        disposition = REJECTED_SAME_DIRECTION
    elif origin_dir == "BEARISH" and impulse_dir == "BULLISH":
        disposition = QUALIFIED_BULLISH if bullish_percent >= MINIMUM_DISPLACEMENT else REJECTED_BULLISH_BELOW
    elif origin_dir == "BULLISH" and impulse_dir == "BEARISH":
        disposition = QUALIFIED_BEARISH if bearish_percent >= MINIMUM_DISPLACEMENT else REJECTED_BEARISH_BELOW
    else:
        disposition = REJECTED_DIRECTION_MISMATCH
    return disposition, bullish_percent, bearish_percent


def _median(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / D2


def _mean(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, D0) / Decimal(len(values))


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    if value < low:
        return low
    if value > high:
        return high
    return value


def _quality_label(score: Decimal) -> str:
    if score >= Decimal("85"):
        return "EXCEPTIONAL_BODY_DISPLACEMENT"
    if score >= Decimal("70"):
        return "STRONG_BODY_DISPLACEMENT"
    if score >= Decimal("50"):
        return "MODERATE_BODY_DISPLACEMENT"
    if score >= Decimal("30"):
        return "BASIC_QUALIFIED"
    return "MINIMUM_QUALIFIED"


def _body_bounds(open_: Decimal, close: Decimal) -> tuple[Decimal, Decimal]:
    return min(open_, close), max(open_, close)


def _body_touches(open_: Decimal, close: Decimal, zone_lower: Decimal, zone_upper: Decimal) -> bool:
    body_low, body_high = _body_bounds(open_, close)
    return body_high >= zone_lower and body_low <= zone_upper


def _wick_touches(high: Decimal, low: Decimal, zone_lower: Decimal, zone_upper: Decimal) -> bool:
    return high >= zone_lower and low <= zone_upper


def _away(direction: str, close: Decimal, zone_lower: Decimal, zone_upper: Decimal) -> Decimal:
    if direction == "BULLISH":
        return close - zone_upper if close > zone_upper else D0
    return zone_lower - close if close < zone_lower else D0


def _zone_distance(close: Decimal, zone_lower: Decimal, zone_upper: Decimal) -> tuple[Decimal, str]:
    if zone_lower <= close <= zone_upper:
        return D0, "INSIDE"
    if close > zone_upper:
        return close - zone_upper, "ABOVE"
    return zone_lower - close, "BELOW"


def _ratio(numerator: Decimal, denominator: Decimal | None) -> Decimal | None:
    if denominator is None or denominator == 0:
        return None
    return numerator / denominator


def analyze(bars: Sequence[Bar]) -> AnalysisResult:
    if len(bars) < 2:
        raise OrderBlockError("at least two completed candles are required")
    pairs: list[PairRecord] = []
    blocks: list[OrderBlock] = []
    events: list[LifecycleEvent] = []
    bull_sequence = 0
    bear_sequence = 0
    final_close = bars[-1].close
    for index in range(len(bars) - 1):
        origin = bars[index]
        impulse = bars[index + 1]
        if origin.row != index or impulse.row != index + 1:
            raise OrderBlockError("order block pairs must be adjacent rows")
        disposition, bullish_percent, bearish_percent = classify_pair(
            origin.open, origin.close, impulse.open, impulse.close
        )
        order_block_id = None
        if disposition == QUALIFIED_BULLISH:
            bull_sequence += 1
            order_block_id = f"OB4H-BULL-{bull_sequence:06d}"
            blocks.append(_build_block(bars, origin, impulse, "BULLISH", bull_sequence, order_block_id, bullish_percent, events))
        elif disposition == QUALIFIED_BEARISH:
            bear_sequence += 1
            order_block_id = f"OB4H-BEAR-{bear_sequence:06d}"
            blocks.append(_build_block(bars, origin, impulse, "BEARISH", bear_sequence, order_block_id, bearish_percent, events))
        pairs.append(
            PairRecord(
                pair_id=f"CP4H-{index + 1:06d}",
                origin_row=origin.row,
                impulse_row=impulse.row,
                origin_open_time=origin.open_time,
                impulse_open_time=impulse.open_time,
                impulse_close_time=impulse.close_time,
                origin_open=origin.open,
                origin_close=origin.close,
                impulse_open=impulse.open,
                impulse_close=impulse.close,
                origin_direction=candle_direction(origin.open, origin.close),
                impulse_direction=candle_direction(impulse.open, impulse.close),
                bullish_displacement_percent=bullish_percent,
                bearish_displacement_percent=bearish_percent,
                bullish_qualification=disposition == QUALIFIED_BULLISH,
                bearish_qualification=disposition == QUALIFIED_BEARISH,
                disposition=disposition,
                order_block_id=order_block_id,
            )
        )
    _assign_overlaps(blocks)
    return AnalysisResult(
        bars=list(bars),
        pairs=pairs,
        blocks=blocks,
        events=events,
        final_close=final_close,
        final_close_time=bars[-1].close_time,
    )


def _build_block(
    bars: Sequence[Bar],
    origin: Bar,
    impulse: Bar,
    direction: str,
    sequence: int,
    order_block_id: str,
    displacement_percent: Decimal,
    events: list[LifecycleEvent],
) -> OrderBlock:
    zone_lower, zone_upper = _body_bounds(origin.open, origin.close)
    zone_width = zone_upper - zone_lower
    origin_body = abs(origin.close - origin.open)
    impulse_body = abs(impulse.close - impulse.open)
    if direction == "BULLISH":
        displacement_price = impulse.close - origin.open
    else:
        displacement_price = origin.open - impulse.close
    preceding = list(bars[max(0, impulse.row - LOOKBACK):impulse.row])
    bodies = [abs(bar.close - bar.open) for bar in preceding]
    body_percents = [abs(bar.close - bar.open) / bar.open * D100 for bar in preceding]
    median_body = _median(bodies)
    mean_body = _mean(bodies)
    median_body_percent = _median(body_percents)
    mean_body_percent = _mean(body_percents)
    impulse_to_median = _ratio(impulse_body, median_body)
    displacement_to_median = _ratio(displacement_price, median_body)
    structure, prior_extreme, break_distance, break_percent = _close_structure(direction, impulse, preceding)
    dominance = impulse_body / origin_body
    displacement_component = _clamp((displacement_percent - MINIMUM_DISPLACEMENT) / D2, D0, D1)
    impulse_component = _clamp((impulse_to_median or D0) / D3, D0, D1)
    dominance_component = _clamp(dominance / D3, D0, D1)
    structure_component = D1 if structure == "TRUE" else D0
    quality_score = D100 * (
        Decimal("0.50") * displacement_component
        + Decimal("0.25") * impulse_component
        + Decimal("0.15") * dominance_component
        + Decimal("0.10") * structure_component
    )
    lifecycle = _lifecycle(bars, direction, impulse.row, zone_lower, zone_upper, order_block_id, events)
    distance, side = _zone_distance(bars[-1].close, zone_lower, zone_upper)
    distance_percent = distance / bars[-1].close * D100 if bars[-1].close else D0
    invalidated = lifecycle["ever_invalidated"]
    return OrderBlock(
        order_block_id=order_block_id,
        direction=direction,
        sequence=sequence,
        origin_row=origin.row,
        impulse_row=impulse.row,
        origin_open_time=origin.open_time,
        origin_close_time=origin.close_time,
        impulse_open_time=impulse.open_time,
        impulse_close_time=impulse.close_time,
        confirmed_at=impulse.close_time,
        origin_open=origin.open,
        origin_close=origin.close,
        origin_high=origin.high,
        origin_low=origin.low,
        impulse_open=impulse.open,
        impulse_close=impulse.close,
        impulse_high=impulse.high,
        impulse_low=impulse.low,
        origin_direction=candle_direction(origin.open, origin.close),
        impulse_direction=candle_direction(impulse.open, impulse.close),
        origin_body_price=origin_body,
        origin_body_percent=origin_body / origin.open * D100,
        impulse_body_price=impulse_body,
        impulse_body_percent=impulse_body / impulse.open * D100,
        impulse_to_origin_body_ratio=dominance,
        displacement_price=displacement_price,
        displacement_percent=displacement_percent,
        minimum_displacement_percent=MINIMUM_DISPLACEMENT,
        displacement_pass=True,
        zone_lower=zone_lower,
        zone_upper=zone_upper,
        zone_midpoint=(zone_lower + zone_upper) / D2,
        zone_width_price=zone_width,
        zone_width_percent=zone_width / origin.open * D100,
        rolling_median_body=median_body,
        rolling_mean_body=mean_body,
        rolling_median_body_percent=median_body_percent,
        rolling_mean_body_percent=mean_body_percent,
        impulse_body_to_median_ratio=impulse_to_median,
        displacement_to_median_body_ratio=displacement_to_median,
        close_structure_break=structure,
        prior_close_extreme=prior_extreme,
        break_distance=break_distance,
        break_percent=break_percent,
        quality_score=quality_score,
        quality_label=_quality_label(quality_score),
        status=lifecycle["status"],
        ever_body_touched=lifecycle["ever_body_touched"],
        ever_midpoint=lifecycle["ever_midpoint"],
        ever_full_traversal=lifecycle["ever_full_traversal"],
        ever_invalidated=invalidated,
        wick_only_contact=lifecycle["wick_only_contact"],
        ever_gap=lifecycle["ever_gap"],
        first_event=lifecycle["first_event"],
        latest_event=lifecycle["latest_event"],
        first_touch_row=lifecycle["first_touch_row"],
        first_touch_open_time=lifecycle["first_touch_open_time"],
        first_touch_close_time=lifecycle["first_touch_close_time"],
        first_touch_open=lifecycle["first_touch_open"],
        first_touch_close=lifecycle["first_touch_close"],
        bars_to_touch=lifecycle["bars_to_touch"],
        hours_to_touch=lifecycle["hours_to_touch"],
        days_to_touch=lifecycle["days_to_touch"],
        body_overlap_price=lifecycle["body_overlap_price"],
        body_overlap_percent=lifecycle["body_overlap_percent"],
        retrace_percent_raw=lifecycle["retrace_percent_raw"],
        retrace_percent_display=lifecycle["retrace_percent_display"],
        body_beyond_far_edge=lifecycle["body_beyond_far_edge"],
        midpoint_on_first_touch=lifecycle["midpoint_on_first_touch"],
        full_traversal_on_first_touch=lifecycle["full_traversal_on_first_touch"],
        bars_to_midpoint=lifecycle["bars_to_midpoint"],
        bars_to_invalidation=lifecycle["bars_to_invalidation"],
        first_invalidation_row=lifecycle["first_invalidation_row"],
        first_invalidation_open_time=lifecycle["first_invalidation_open_time"],
        first_invalidation_close_time=lifecycle["first_invalidation_close_time"],
        first_invalidation_open=lifecycle["first_invalidation_open"],
        first_invalidation_close=lifecycle["first_invalidation_close"],
        body_revisit_count=lifecycle["body_revisit_count"],
        touch_episode_count=lifecycle["touch_episode_count"],
        max_close_excursion_before=lifecycle["max_close_excursion_before"],
        max_close_excursion_after=lifecycle["max_close_excursion_after"],
        final_close=bars[-1].close,
        final_distance_price=distance,
        final_distance_percent=distance_percent,
        final_distance_side=side,
        right_censored=not invalidated,
        censoring_reason="DATASET_END" if not invalidated else "NOT_APPLICABLE",
        open_at_dataset_end=not invalidated,
    )


def _close_structure(direction: str, impulse: Bar, preceding: Sequence[Bar]) -> tuple[str, Decimal | None, Decimal | None, Decimal | None]:
    if len(preceding) < LOOKBACK:
        return "INSUFFICIENT_HISTORY", None, None, None
    window = preceding[-LOOKBACK:]
    if direction == "BULLISH":
        extreme = max(bar.close for bar in window)
        broken = impulse.close > extreme
        distance = impulse.close - extreme
    else:
        extreme = min(bar.close for bar in window)
        broken = impulse.close < extreme
        distance = extreme - impulse.close
    percent = distance / extreme * D100 if extreme else None
    return ("TRUE" if broken else "FALSE"), extreme, distance, percent


def _lifecycle(
    bars: Sequence[Bar],
    direction: str,
    impulse_row: int,
    zone_lower: Decimal,
    zone_upper: Decimal,
    order_block_id: str,
    events: list[LifecycleEvent],
) -> dict[str, object]:
    state: dict[str, object] = {
        "status": TERMINAL,
        "ever_body_touched": False,
        "ever_midpoint": False,
        "ever_full_traversal": False,
        "ever_invalidated": False,
        "wick_only_contact": False,
        "ever_gap": False,
        "first_event": None,
        "latest_event": None,
        "first_touch_row": None,
        "first_touch_open_time": None,
        "first_touch_close_time": None,
        "first_touch_open": None,
        "first_touch_close": None,
        "bars_to_touch": None,
        "hours_to_touch": None,
        "days_to_touch": None,
        "body_overlap_price": None,
        "body_overlap_percent": None,
        "retrace_percent_raw": None,
        "retrace_percent_display": None,
        "body_beyond_far_edge": None,
        "midpoint_on_first_touch": None,
        "full_traversal_on_first_touch": None,
        "bars_to_midpoint": None,
        "bars_to_invalidation": None,
        "first_invalidation_row": None,
        "first_invalidation_open_time": None,
        "first_invalidation_close_time": None,
        "first_invalidation_open": None,
        "first_invalidation_close": None,
        "body_revisit_count": 0,
        "touch_episode_count": 0,
        "max_close_excursion_before": None,
        "max_close_excursion_after": None,
    }
    later = [bar for bar in bars if bar.row > impulse_row]
    if not later:
        return state
    width = zone_upper - zone_lower
    midpoint = (zone_lower + zone_upper) / D2
    in_episode = False
    before_values: list[Decimal] = []
    after_values: list[Decimal] = []
    seen_touch = False
    for bar in later:
        bars_after = bar.row - impulse_row
        body_low, body_high = _body_bounds(bar.open, bar.close)
        touched = body_high >= zone_lower and body_low <= zone_upper
        gap = (direction == "BULLISH" and bar.open < zone_lower) or (direction == "BEARISH" and bar.open > zone_upper)
        invalidated = (direction == "BULLISH" and bar.close < zone_lower) or (direction == "BEARISH" and bar.close > zone_upper)
        midpoint_hit = body_low <= midpoint <= body_high
        full_hit = body_low <= zone_lower and body_high >= zone_upper
        wick_only = (not touched) and _wick_touches(bar.high, bar.low, zone_lower, zone_upper)
        if not seen_touch:
            before_values.append(_away(direction, bar.close, zone_lower, zone_upper))
        else:
            after_values.append(_away(direction, bar.close, zone_lower, zone_upper))
        if gap and not state["ever_gap"]:
            state["ever_gap"] = True
            _remember(state, events, order_block_id, direction, "GAP_OPEN_BEYOND_BODY_ZONE", bar, bars_after, "open beyond the body zone")
        if touched:
            state["body_revisit_count"] = int(state["body_revisit_count"]) + 1
            if not in_episode:
                state["touch_episode_count"] = int(state["touch_episode_count"]) + 1
                in_episode = True
            if not state["ever_body_touched"]:
                state["ever_body_touched"] = True
                seen_touch = True
                before_values.pop()
                raw, display, beyond = _retrace(direction, body_low, body_high, zone_lower, zone_upper, width)
                overlap = min(body_high, zone_upper) - max(body_low, zone_lower)
                state.update(
                    {
                        "first_touch_row": bar.row,
                        "first_touch_open_time": bar.open_time,
                        "first_touch_close_time": bar.close_time,
                        "first_touch_open": bar.open,
                        "first_touch_close": bar.close,
                        "bars_to_touch": bars_after,
                        "hours_to_touch": Decimal(bars_after * BAR_HOURS),
                        "days_to_touch": Decimal(bars_after * BAR_HOURS) / D24,
                        "body_overlap_price": overlap,
                        "body_overlap_percent": overlap / width * D100,
                        "retrace_percent_raw": raw,
                        "retrace_percent_display": display,
                        "body_beyond_far_edge": beyond,
                        "midpoint_on_first_touch": midpoint_hit,
                        "full_traversal_on_first_touch": full_hit,
                    }
                )
                _remember(state, events, order_block_id, direction, "BODY_ZONE_TOUCH", bar, bars_after, "first body intersection")
        else:
            in_episode = False
        if midpoint_hit and not state["ever_midpoint"]:
            state["ever_midpoint"] = True
            state["bars_to_midpoint"] = bars_after
            _remember(state, events, order_block_id, direction, "MIDPOINT_REACHED", bar, bars_after, "body covers the zone midpoint")
        if full_hit and not state["ever_full_traversal"]:
            state["ever_full_traversal"] = True
            _remember(state, events, order_block_id, direction, "FULL_ZONE_TRAVERSED", bar, bars_after, "body covers the entire zone")
        if invalidated and not state["ever_invalidated"]:
            state["ever_invalidated"] = True
            state["bars_to_invalidation"] = bars_after
            state["first_invalidation_row"] = bar.row
            state["first_invalidation_open_time"] = bar.open_time
            state["first_invalidation_close_time"] = bar.close_time
            state["first_invalidation_open"] = bar.open
            state["first_invalidation_close"] = bar.close
            _remember(state, events, order_block_id, direction, "INVALIDATED_BY_BODY_CLOSE", bar, bars_after, "close strictly through the far body edge")
        if wick_only and not state["wick_only_contact"]:
            state["wick_only_contact"] = True
            _remember(state, events, order_block_id, direction, "WICK_ONLY_CONTACT_DIAGNOSTIC", bar, bars_after, "wick intersects the zone and the body does not")
    if before_values:
        state["max_close_excursion_before"] = max(before_values)
    if after_values:
        state["max_close_excursion_after"] = max(after_values)
    if state["ever_invalidated"]:
        state["status"] = INVALIDATED
    elif state["ever_body_touched"]:
        state["status"] = ACTIVE_BODY_MITIGATED
    else:
        state["status"] = ACTIVE_UNTOUCHED
    return state


def _retrace(
    direction: str,
    body_low: Decimal,
    body_high: Decimal,
    zone_lower: Decimal,
    zone_upper: Decimal,
    width: Decimal,
) -> tuple[Decimal, Decimal, bool]:
    if direction == "BULLISH":
        raw = (zone_upper - body_low) / width * D100
        beyond = body_low < zone_lower
    else:
        raw = (body_high - zone_lower) / width * D100
        beyond = body_high > zone_upper
    return raw, _clamp(raw, D0, D100), beyond


def _remember(
    state: dict[str, object],
    events: list[LifecycleEvent],
    order_block_id: str,
    direction: str,
    event_type: str,
    bar: Bar,
    bars_after: int,
    detail: str,
) -> None:
    if state["first_event"] is None:
        state["first_event"] = event_type
    state["latest_event"] = event_type
    events.append(
        LifecycleEvent(
            order_block_id=order_block_id,
            direction=direction,
            event_type=event_type,
            row=bar.row,
            open_time=bar.open_time,
            close_time=bar.close_time,
            open=bar.open,
            close=bar.close,
            bars_from_confirmation=bars_after,
            detail=detail,
        )
    )


def _assign_overlaps(blocks: list[OrderBlock]) -> None:
    parent = list(range(len(blocks)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        root_left, root_right = find(left), find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    for left_index, left in enumerate(blocks):
        for right_index in range(left_index + 1, len(blocks)):
            right = blocks[right_index]
            if left.zone_lower > right.zone_upper or right.zone_lower > left.zone_upper:
                continue
            overlap = min(left.zone_upper, right.zone_upper) - max(left.zone_lower, right.zone_lower)
            _record_overlap(left, right, overlap)
            _record_overlap(right, left, overlap)
            union(left_index, right_index)
    groups: dict[int, list[OrderBlock]] = defaultdict(list)
    for index, block in enumerate(blocks):
        groups[find(index)].append(block)
    ordered = sorted(groups.values(), key=lambda items: (min(item.impulse_row for item in items), items[0].order_block_id))
    for number, items in enumerate(ordered, start=1):
        cluster_id = f"OC4H-{number:06d}"
        for block in items:
            block.cluster_id = cluster_id


def _record_overlap(block: OrderBlock, other: OrderBlock, overlap: Decimal) -> None:
    if other.direction == "BULLISH":
        block.overlap_bullish += 1
    else:
        block.overlap_bearish += 1
    if other.direction == block.direction:
        block.same_direction_overlap += 1
    else:
        block.opposite_direction_overlap += 1
    percent = overlap / block.zone_width_price * D100
    if overlap > block.max_overlap_price:
        block.max_overlap_price = overlap
        block.max_overlap_percent = percent


def disposition_counts(result: AnalysisResult) -> dict[str, int]:
    counts = {name: 0 for name in DISPOSITIONS}
    for pair in result.pairs:
        counts[pair.disposition] += 1
    return counts


def status_counts(result: AnalysisResult) -> dict[str, int]:
    counts = {name: 0 for name in STATUSES}
    for block in result.blocks:
        counts[block.status] += 1
    return counts


def _median_values(values: Sequence[Decimal]) -> Decimal | None:
    return _median(values)


def _mean_values(values: Sequence[Decimal]) -> Decimal | None:
    return _mean(values)


def zone_distance(close: Decimal, zone_lower: Decimal, zone_upper: Decimal) -> tuple[Decimal, str]:
    return _zone_distance(close, zone_lower, zone_upper)


def audit_result(result: AnalysisResult) -> None:
    """Recompute qualification from open and close and check bookkeeping."""
    if len(result.pairs) != len(result.bars) - 1:
        raise OrderBlockError(f"pair count {len(result.pairs)} != {len(result.bars) - 1}")
    counts = disposition_counts(result)
    if sum(counts.values()) != len(result.pairs):
        raise OrderBlockError("pair dispositions do not reconcile")
    qualified = [pair for pair in result.pairs if pair.disposition in {QUALIFIED_BULLISH, QUALIFIED_BEARISH}]
    if len(qualified) != len(result.blocks):
        raise OrderBlockError("qualified pairs and order blocks differ")
    keys: set[tuple[int, int, str]] = set()
    ids: set[str] = set()
    bull_ids = [block.order_block_id for block in result.blocks if block.direction == "BULLISH"]
    bear_ids = [block.order_block_id for block in result.blocks if block.direction == "BEARISH"]
    if bull_ids != [f"OB4H-BULL-{index:06d}" for index in range(1, len(bull_ids) + 1)]:
        raise OrderBlockError("bullish ids are not sequential")
    if bear_ids != [f"OB4H-BEAR-{index:06d}" for index in range(1, len(bear_ids) + 1)]:
        raise OrderBlockError("bearish ids are not sequential")
    blocks_by_id = {block.order_block_id: block for block in result.blocks}
    if len(blocks_by_id) != len(result.blocks):
        raise OrderBlockError("duplicate order block ids")
    for pair in result.pairs:
        origin = result.bars[pair.origin_row]
        impulse = result.bars[pair.impulse_row]
        if impulse.row != origin.row + 1:
            raise OrderBlockError("stored pair is not adjacent")
        disposition, bullish_percent, bearish_percent = classify_pair(origin.open, origin.close, impulse.open, impulse.close)
        if disposition != pair.disposition or bullish_percent != pair.bullish_displacement_percent or bearish_percent != pair.bearish_displacement_percent:
            raise OrderBlockError(f"pair {pair.pair_id} does not match open/close qualification")
        if pair.bullish_qualification and pair.bearish_qualification:
            raise OrderBlockError("a pair qualified in both directions")
        if pair.disposition not in DISPOSITIONS:
            raise OrderBlockError(f"unknown disposition {pair.disposition}")
        block = blocks_by_id.get(pair.order_block_id or "")
        if pair.disposition in {QUALIFIED_BULLISH, QUALIFIED_BEARISH}:
            if block is None:
                raise OrderBlockError(f"missing block for {pair.order_block_id}")
            _audit_block(block, origin, impulse, result)
            key = (block.origin_row, block.impulse_row, block.direction)
            if key in keys:
                raise OrderBlockError(f"duplicate qualified key {key}")
            keys.add(key)
            ids.add(block.order_block_id)
        elif pair.order_block_id is not None:
            raise OrderBlockError("rejected pair has an order block id")
    if counts[QUALIFIED_BULLISH] + counts[QUALIFIED_BEARISH] + sum(counts[name] for name in DISPOSITIONS if name.startswith("REJECTED")) != len(result.pairs):
        raise OrderBlockError("qualified and rejected counts do not cover every pair")
    if sum(status_counts(result).values()) != len(result.blocks):
        raise OrderBlockError("lifecycle statuses do not cover every order block")


def _audit_block(block: OrderBlock, origin: Bar, impulse: Bar, result: AnalysisResult) -> None:
    zone_lower, zone_upper = _body_bounds(origin.open, origin.close)
    if block.zone_lower != zone_lower or block.zone_upper != zone_upper:
        raise OrderBlockError(f"{block.order_block_id} zone does not use the origin body")
    if block.confirmed_at != impulse.close_time:
        raise OrderBlockError(f"{block.order_block_id} confirmation is not the impulse close")
    if block.displacement_percent < MINIMUM_DISPLACEMENT:
        raise OrderBlockError(f"{block.order_block_id} displacement is below 1 percent")
    if block.direction == "BULLISH":
        expected = (impulse.close - origin.open) / origin.open * D100
        if candle_direction(origin.open, origin.close) != "BEARISH" or candle_direction(impulse.open, impulse.close) != "BULLISH":
            raise OrderBlockError(f"{block.order_block_id} breaks the bullish direction rule")
    else:
        expected = (origin.open - impulse.close) / origin.open * D100
        if candle_direction(origin.open, origin.close) != "BULLISH" or candle_direction(impulse.open, impulse.close) != "BEARISH":
            raise OrderBlockError(f"{block.order_block_id} breaks the bearish direction rule")
    if expected != block.displacement_percent:
        raise OrderBlockError(f"{block.order_block_id} displacement was not recomputed from open and close")
    if block.first_touch_row is not None and block.first_touch_row <= block.impulse_row:
        raise OrderBlockError(f"{block.order_block_id} was touched at or before confirmation")
    if block.first_invalidation_row is not None and block.first_invalidation_row <= block.impulse_row:
        raise OrderBlockError(f"{block.order_block_id} was invalidated at or before confirmation")
    has_later = block.impulse_row < len(result.bars) - 1
    if not has_later and block.status != TERMINAL:
        raise OrderBlockError(f"{block.order_block_id} without a later candle is not terminal")
    if has_later and block.status == TERMINAL:
        raise OrderBlockError(f"{block.order_block_id} is terminal despite a later candle")
    if block.ever_invalidated and block.status != INVALIDATED:
        raise OrderBlockError(f"{block.order_block_id} invalidation was not the final status")
    if block.status == ACTIVE_BODY_MITIGATED and not block.ever_body_touched:
        raise OrderBlockError(f"{block.order_block_id} is mitigated without a body touch")
    if block.status == ACTIVE_UNTOUCHED and (block.ever_body_touched or block.ever_invalidated):
        raise OrderBlockError(f"{block.order_block_id} is untouched after a body event")
    if block.quality_label not in {
        "EXCEPTIONAL_BODY_DISPLACEMENT",
        "STRONG_BODY_DISPLACEMENT",
        "MODERATE_BODY_DISPLACEMENT",
        "BASIC_QUALIFIED",
        "MINIMUM_QUALIFIED",
    }:
        raise OrderBlockError(f"{block.order_block_id} has no quality label")
