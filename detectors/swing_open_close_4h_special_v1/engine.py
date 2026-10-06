"""Causal 4h Special Swing detector."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Sequence

from detectors.swing_open_close_4h_special_v1.config import (
    ALTERNATIVE_ENABLED,
    COMPACT_ENABLED,
    DERIVED,
    DISPLAY_TZ,
    LONGER,
    MAX_INTERIOR,
    MIN_BOUNDARY,
    NESTED,
    NORMAL_MIN_INTERIOR,
    NORMAL_STANDARD_ENABLED,
    NORMAL_USES_FULL_RETURN,
    NORMAL_USES_ONE_THIRD,
    PRIMARY,
    STANDARD_CLASS,
    STANDARD_ENABLED,
    SWING_HIGH,
    SWING_LOW,
    ZERO_CLASS,
    ZERO_EXCEPTION_ENABLED,
    ZERO_USES_FULL_RETURN,
    ZERO_USES_ONE_THIRD,
)

D0 = Decimal("0")
D100 = Decimal("100")
MIN_B = Decimal(MIN_BOUNDARY)
THIRD = Decimal(1) / Decimal(3)
FORBIDDEN_BOUNDARY_REASONS = {
    "OPEN_BOUNDARY_ABOVE_3_50",
    "CLOSE_BOUNDARY_ABOVE_3_50",
    "BOTH_BOUNDARIES_ABOVE_MAXIMUM",
    "BOUNDARIES_OUTSIDE_ALLOWED_RANGE",
}


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
    volume: Decimal = D0
    quote_volume: Decimal = D0
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
    open_width_price: Decimal
    open_width_percent: Decimal
    close_width_price: Decimal
    close_width_percent: Decimal
    completion_error: Decimal
    structure_high: Decimal
    structure_low: Decimal
    role: str
    status: str
    formation_class: str = ""
    zero_exception: bool = False
    extreme_source: str = ""
    zero_plateau: bool = False
    zero_match_count: int = 0
    completion_rule: str = ""
    completion_target: Decimal = D0
    body_low: Decimal = D0
    body_high: Decimal = D0
    body_size: Decimal = D0
    body_threshold: Decimal = D0
    penetration_price: Decimal = D0
    penetration_fraction: Decimal = D0
    penetration_percent: Decimal = D0
    full_open_return: bool = False
    suppressed_by: str = ""


@dataclass(slots=True)
class SearchRecord:
    open_row: int
    direction: str
    terminal: str
    max_interior: int
    censored: bool
    raw_id: str = ""
    saw_zero: bool = False
    binding_interior: int | None = None
    wick_only: int = 0


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


def iso_utc(moment: datetime | None) -> str:
    if moment is None:
        return ""
    value = moment.astimezone(timezone.utc)
    if value.microsecond == 0:
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    return value.strftime("%Y-%m-%dT%H:%M:%S") + f".{value.microsecond // 1000:03d}Z"


def swing_open_body(open_price: Decimal, open_close: Decimal) -> tuple[Decimal, Decimal, Decimal]:
    body_low = min(open_price, open_close)
    body_high = max(open_price, open_close)
    return body_low, body_high, body_high - body_low


def body_threshold(direction: str, body_low: Decimal, body_high: Decimal, body_size: Decimal) -> Decimal:
    if direction == SWING_HIGH:
        return body_high - (body_size * THIRD)
    return body_low + (body_size * THIRD)


def body_penetrated(direction: str, close_price: Decimal, threshold: Decimal) -> bool:
    if direction == SWING_HIGH:
        return close_price <= threshold
    return close_price >= threshold


def penetration_values(
    direction: str,
    close_price: Decimal,
    body_low: Decimal,
    body_high: Decimal,
    body_size: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    price = body_high - close_price if direction == SWING_HIGH else close_price - body_low
    fraction = price / body_size
    return price, fraction, fraction * D100


def wick_only_reason(direction: str, high: Decimal, low: Decimal, close_price: Decimal, threshold: Decimal) -> str | None:
    if body_penetrated(direction, close_price, threshold):
        return None
    wick = low <= threshold if direction == SWING_HIGH else high >= threshold
    if wick:
        return "NORMAL_STANDARD_WICK_ONLY_BODY_PENETRATION"
    return "NORMAL_STANDARD_BODY_PENETRATION_NOT_REACHED"


def duration_reason(interior: int) -> str | None:
    if interior > MAX_INTERIOR:
        return "STANDARD_DURATION_ABOVE_5"
    return None


def zero_adjacency_reason(interior: int, close_row: int, open_row: int) -> str | None:
    if interior != 0 or close_row != open_row + 1:
        return "ZERO_INTERIOR_ENDPOINTS_NOT_ADJACENT"
    return None


def zero_return_reason(direction: str, reference: Decimal, close_price: Decimal) -> str | None:
    returned = close_price <= reference if direction == SWING_HIGH else close_price >= reference
    if not returned:
        return "ZERO_INTERIOR_REFERENCE_RETURN_NOT_ACHIEVED"
    return None


def zero_direction_reason(
    direction: str,
    open_price: Decimal,
    open_close: Decimal,
    close_open: Decimal,
    close_price: Decimal,
) -> str | None:
    if open_close == open_price or close_price == close_open:
        return "ZERO_INTERIOR_DOJI_NOT_ALLOWED"
    if direction == SWING_LOW:
        if open_close >= open_price:
            return "ZERO_INTERIOR_SWING_LOW_OPEN_NOT_BEARISH"
        if close_price <= close_open:
            return "ZERO_INTERIOR_SWING_LOW_CLOSE_NOT_BULLISH"
        return None
    if open_close <= open_price:
        return "ZERO_INTERIOR_SWING_HIGH_OPEN_NOT_BULLISH"
    if close_price >= close_open:
        return "ZERO_INTERIOR_SWING_HIGH_CLOSE_NOT_BEARISH"
    return None


def boundary_reason(open_percent: Decimal, close_percent: Decimal) -> str | None:
    open_low = open_percent < MIN_B
    close_low = close_percent < MIN_B
    if not open_low and not close_low:
        return None
    if open_low and close_low:
        return "BOTH_BOUNDARIES_BELOW_MINIMUM"
    if open_low:
        return "OPEN_BOUNDARY_BELOW_1_30"
    return "CLOSE_BOUNDARY_BELOW_1_30"


def analyze(bars: Sequence[Bar]) -> Analysis:
    if (
        not STANDARD_ENABLED
        or not NORMAL_STANDARD_ENABLED
        or COMPACT_ENABLED
        or ALTERNATIVE_ENABLED
        or not ZERO_EXCEPTION_ENABLED
        or not ZERO_USES_FULL_RETURN
        or ZERO_USES_ONE_THIRD
        or NORMAL_USES_FULL_RETURN
        or not NORMAL_USES_ONE_THIRD
    ):
        raise SpecialSwingError("Special detector flags are not zero full-return plus standard one-third body")
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
        record.terminal = "CONFIRMED_PRIMARY_SPECIAL_SWING" if swing.status == PRIMARY else swing.status
    _assign_display_ids(raw)
    _link_suppressed(raw)
    _check_invariants(raw, searches)
    displayed = [item for item in raw if item.status == PRIMARY]
    return Analysis(list(bars), searches, raw, displayed, _counters(raw, searches, displayed))


def _search(bars: Sequence[Bar], open_row: int, direction: str) -> tuple[SearchRecord, Swing | None]:
    opened = bars[open_row]
    reference = opened.open
    body_low, body_high, body_size = swing_open_body(opened.open, opened.close)
    max_interior = 0
    saw_zero = False
    wick_only = 0
    last_available = len(bars) - 1

    def finish(terminal: str, censored: bool, binding: int | None) -> SearchRecord:
        return SearchRecord(open_row, direction, terminal, max_interior, censored, "", saw_zero, binding, wick_only)

    adjacent = open_row + 1
    if adjacent > last_available:
        return finish("SEARCH_CENSORED_BY_DATASET_END", True, None), None
    saw_zero = True
    max_interior = 0
    adjacent_close = bars[adjacent].close
    full_return = adjacent_close <= reference if direction == SWING_HIGH else adjacent_close >= reference
    if full_return:
        swing, reason = _build_zero(bars, open_row, adjacent, direction, reference, adjacent_close)
        if reason is not None:
            return finish(reason, False, 0), None
        return finish("CONFIRMED_PRIMARY_SPECIAL_SWING", False, 0), swing
    if body_size == 0:
        return finish("NORMAL_STANDARD_SWING_OPEN_BODY_ZERO", False, None), None
    threshold = body_threshold(direction, body_low, body_high, body_size)
    for close_row in range(open_row + 2, open_row + MAX_INTERIOR + 2):
        if close_row > last_available:
            return finish("SEARCH_CENSORED_BY_DATASET_END", True, None), None
        interior = close_row - open_row - 1
        if interior > MAX_INTERIOR:
            raise SpecialSwingError("HARD_SEARCH_HORIZON_ERROR")
        max_interior = interior
        candle = bars[close_row]
        if not body_penetrated(direction, candle.close, threshold):
            if wick_only_reason(direction, candle.high, candle.low, candle.close, threshold) == "NORMAL_STANDARD_WICK_ONLY_BODY_PENETRATION":
                wick_only += 1
            continue
        swing, reason = _build(bars, open_row, close_row, direction, reference, candle.close, interior)
        if reason is not None:
            return finish(reason, False, interior), None
        return finish("CONFIRMED_PRIMARY_SPECIAL_SWING", False, interior), swing
    return finish("SEARCH_HORIZON_EXHAUSTED_AT_5_INTERIOR_CANDLES", False, None), None


def _build(
    bars: Sequence[Bar],
    open_row: int,
    close_row: int,
    direction: str,
    reference: Decimal,
    close_price: Decimal,
    interior: int,
) -> tuple[Swing | None, str | None]:
    blocked = duration_reason(interior)
    if blocked is not None:
        return None, blocked
    window = range(open_row + 1, close_row)
    if not window:
        return None, "NO_STRICTLY_INTERIOR_EXTREME"
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
    open_percent = open_width / reference * D100
    close_percent = close_width / close_price * D100
    reason = boundary_reason(open_percent, close_percent)
    if reason is not None:
        return None, reason
    opened = bars[open_row]
    body_low, body_high, body_size = swing_open_body(opened.open, opened.close)
    if body_size == 0:
        return None, "NORMAL_STANDARD_SWING_OPEN_BODY_ZERO"
    threshold = body_threshold(direction, body_low, body_high, body_size)
    if not body_penetrated(direction, close_price, threshold):
        return None, "NORMAL_STANDARD_BODY_PENETRATION_NOT_REACHED"
    pen_price, pen_fraction, pen_percent = penetration_values(direction, close_price, body_low, body_high, body_size)
    if pen_fraction < THIRD:
        return None, "NORMAL_STANDARD_BODY_PENETRATION_NOT_REACHED"
    segment = bars[open_row:close_row + 1]
    full_return = close_price <= reference if direction == SWING_HIGH else close_price >= reference
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
        event_id=f"{direction}|{price}|{start}|{end}",
        family_id="",
        reference=reference,
        close_price=close_price,
        interior=interior,
        total=interior + 2,
        open_width_price=open_width,
        open_width_percent=open_percent,
        close_width_price=close_width,
        close_width_percent=close_percent,
        completion_error=abs(close_price - threshold) / threshold * D100,
        structure_high=max(item.high for item in segment),
        structure_low=min(item.low for item in segment),
        role=PRIMARY,
        status=PRIMARY,
        formation_class=STANDARD_CLASS,
        zero_exception=False,
        extreme_source="STRICTLY_INTERIOR_CANDLE_ONLY",
        zero_plateau=False,
        zero_match_count=end - start + 1,
        completion_rule="ONE_THIRD_BODY_PENETRATION",
        completion_target=threshold,
        body_low=body_low,
        body_high=body_high,
        body_size=body_size,
        body_threshold=threshold,
        penetration_price=pen_price,
        penetration_fraction=pen_fraction,
        penetration_percent=pen_percent,
        full_open_return=full_return,
    ), None


def _build_zero(
    bars: Sequence[Bar],
    open_row: int,
    close_row: int,
    direction: str,
    reference: Decimal,
    close_price: Decimal,
) -> tuple[Swing | None, str | None]:
    if close_row != open_row + 1:
        return None, "ZERO_INTERIOR_ENDPOINTS_NOT_ADJACENT"
    opened = bars[open_row]
    closed = bars[close_row]
    missed = zero_return_reason(direction, reference, close_price)
    if missed is not None:
        return None, missed
    direction_reason = zero_direction_reason(direction, opened.open, opened.close, closed.open, close_price)
    if direction_reason is not None:
        return None, direction_reason
    if direction == SWING_HIGH:
        price = max(opened.high, closed.high)
        open_match = opened.high == price
        close_match = closed.high == price
        open_width = price - reference
        close_width = price - close_price
    else:
        price = min(opened.low, closed.low)
        open_match = opened.low == price
        close_match = closed.low == price
        open_width = reference - price
        close_width = close_price - price
    if open_match and close_match:
        extreme_row = close_row
        plateau = True
        match_count = 2
        start, end = open_row, close_row
    elif close_match:
        extreme_row = close_row
        plateau = False
        match_count = 1
        start = end = close_row
    else:
        extreme_row = open_row
        plateau = False
        match_count = 1
        start = end = open_row
    open_percent = open_width / reference * D100
    close_percent = close_width / close_price * D100
    reason = boundary_reason(open_percent, close_percent)
    if reason is not None:
        return None, reason
    segment = (opened, closed)
    return Swing(
        raw_id="",
        swing_id="",
        direction=direction,
        open_row=open_row,
        close_row=close_row,
        extreme_row=extreme_row,
        extreme_price=price,
        plateau_start=start,
        plateau_end=end,
        event_id=f"{direction}|{price}|{start}|{end}",
        family_id="",
        reference=reference,
        close_price=close_price,
        interior=0,
        total=2,
        open_width_price=open_width,
        open_width_percent=open_percent,
        close_width_price=close_width,
        close_width_percent=close_percent,
        completion_error=abs(close_price - reference) / reference * D100,
        structure_high=max(opened.high, closed.high),
        structure_low=min(opened.low, closed.low),
        role=PRIMARY,
        status=PRIMARY,
        formation_class=ZERO_CLASS,
        zero_exception=True,
        extreme_source="SWING_OPEN_OR_SWING_CLOSE_ENDPOINT",
        zero_plateau=plateau,
        zero_match_count=match_count,
        completion_rule="FULL_OPEN_REFERENCE_RETURN",
        completion_target=reference,
        full_open_return=True,
    ), None


def _assign_raw_ids(raw: list[Swing]) -> None:
    raw.sort(key=lambda item: (item.close_row, item.direction, item.open_row))
    for index, item in enumerate(raw, start=1):
        item.raw_id = f"SPS-RAW-{index:06d}"


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


def _link_suppressed(raw: list[Swing]) -> None:
    published = {item.raw_id: item.swing_id for item in raw if item.swing_id}
    for item in raw:
        if item.suppressed_by in published:
            item.suppressed_by = published[item.suppressed_by]


def _assign_display_ids(raw: list[Swing]) -> None:
    displayed = [item for item in raw if item.status == PRIMARY]
    displayed.sort(key=lambda item: (item.open_row, item.close_row, item.raw_id))
    for index, item in enumerate(displayed, start=1):
        item.swing_id = f"SPS-{index:06d}"


def _check_invariants(raw: list[Swing], searches: list[SearchRecord]) -> None:
    seen = set()
    for item in raw:
        if item.formation_class == ZERO_CLASS:
            if item.interior != 0 or item.total != 2 or item.close_row != item.open_row + 1:
                raise SpecialSwingError("zero-interior formation shape is invalid")
            if item.extreme_row not in (item.open_row, item.close_row) or not item.zero_exception:
                raise SpecialSwingError("zero-interior extreme is not an endpoint")
            if item.completion_rule != "FULL_OPEN_REFERENCE_RETURN" or not item.full_open_return:
                raise SpecialSwingError("zero-interior confirmation is not a full Open return")
            returned = item.close_price <= item.reference if item.direction == SWING_HIGH else item.close_price >= item.reference
            if not returned:
                raise SpecialSwingError("zero-interior close did not reach Swing Open Open")
        elif item.formation_class == STANDARD_CLASS:
            if item.interior < NORMAL_MIN_INTERIOR or item.interior > MAX_INTERIOR:
                raise SpecialSwingError("normal Standard interior is outside 1-5")
            if not (item.open_row < item.extreme_row < item.close_row) or item.zero_exception:
                raise SpecialSwingError("normal Standard extreme is not strictly interior")
            if item.body_size <= 0 or item.penetration_fraction < THIRD or item.completion_rule != "ONE_THIRD_BODY_PENETRATION":
                raise SpecialSwingError("normal Standard body penetration is below one-third")
        else:
            raise SpecialSwingError("unknown formation class")
        if item.open_width_percent < MIN_B or item.close_width_percent < MIN_B:
            raise SpecialSwingError("confirmed boundary is below 1.30")
        key = (item.direction, item.open_row, item.close_row, str(item.extreme_price))
        if key in seen:
            raise SpecialSwingError("DUPLICATE_EXACT_CANDIDATE")
        seen.add(key)
    for record in searches:
        if record.max_interior > MAX_INTERIOR:
            raise SpecialSwingError("HARD_SEARCH_HORIZON_ERROR")
        if record.terminal in FORBIDDEN_BOUNDARY_REASONS:
            raise SpecialSwingError("maximum boundary rejection was emitted")


def _counters(raw: list[Swing], searches: list[SearchRecord], displayed: list[Swing]) -> dict:
    opens = [item.open_width_percent for item in displayed]
    closes = [item.close_width_percent for item in displayed]
    interiors = [item.interior for item in displayed]
    return {
        "searches": len(searches),
        "raw": len(raw),
        "displayed": len(displayed),
        "swing_high": sum(1 for item in displayed if item.direction == SWING_HIGH),
        "swing_low": sum(1 for item in displayed if item.direction == SWING_LOW),
        "derived": sum(1 for item in raw if item.status == DERIVED),
        "nested": sum(1 for item in raw if item.status == NESTED),
        "longer": sum(1 for item in raw if item.status == LONGER),
        "alternative": 0,
        "compact": 0,
        "non_standard": 0,
        "min_interior": min(interiors) if interiors else None,
        "max_interior": max(interiors) if interiors else None,
        "min_open_boundary": min(opens) if opens else None,
        "max_open_boundary": max(opens) if opens else None,
        "min_close_boundary": min(closes) if closes else None,
        "max_close_boundary": max(closes) if closes else None,
        "normal_min_interior": NORMAL_MIN_INTERIOR,
        "normal_max_interior": MAX_INTERIOR,
        "maximum_search_interior": MAX_INTERIOR,
        "zero_exception_enabled": ZERO_EXCEPTION_ENABLED,
        "zero_interior_candidates": sum(1 for item in searches if item.binding_interior == 0),
        "zero_interior_confirmed": sum(1 for item in raw if item.formation_class == ZERO_CLASS),
        "zero_interior_high": sum(1 for item in raw if item.formation_class == ZERO_CLASS and item.direction == SWING_HIGH),
        "zero_interior_low": sum(1 for item in raw if item.formation_class == ZERO_CLASS and item.direction == SWING_LOW),
        "zero_interior_primary": sum(1 for item in displayed if item.formation_class == ZERO_CLASS),
        "normal_standard_candidates": sum(1 for item in searches if item.binding_interior is not None and item.binding_interior >= 1),
        "normal_standard_confirmed": sum(1 for item in raw if item.formation_class == STANDARD_CLASS),
        "normal_standard_high": sum(1 for item in raw if item.formation_class == STANDARD_CLASS and item.direction == SWING_HIGH),
        "normal_standard_low": sum(1 for item in raw if item.formation_class == STANDARD_CLASS and item.direction == SWING_LOW),
        "exact_one_third_passes": sum(1 for item in raw if item.formation_class == STANDARD_CLASS and item.penetration_fraction == THIRD),
        "one_third_without_full_open_return": sum(
            1 for item in raw if item.formation_class == STANDARD_CLASS and not item.full_open_return
        ),
        "primary_without_full_open_return": sum(
            1 for item in displayed if item.formation_class == STANDARD_CLASS and not item.full_open_return
        ),
        "normal_full_open_return": sum(1 for item in raw if item.formation_class == STANDARD_CLASS and item.full_open_return),
        "zero_full_open_return": sum(1 for item in raw if item.formation_class == ZERO_CLASS and item.full_open_return),
        "doji_swing_open_rejections": sum(1 for item in searches if item.terminal == "NORMAL_STANDARD_SWING_OPEN_BODY_ZERO"),
        "insufficient_body_penetration": sum(
            1 for item in searches if item.terminal == "SEARCH_HORIZON_EXHAUSTED_AT_5_INTERIOR_CANDLES"
        ),
        "confirmed_open_above_3_50": sum(1 for item in raw if item.open_width_percent > Decimal("3.50")),
        "confirmed_close_above_3_50": sum(1 for item in raw if item.close_width_percent > Decimal("3.50")),
        "confirmed_either_above_3_50": sum(
            1 for item in raw if item.open_width_percent > Decimal("3.50") or item.close_width_percent > Decimal("3.50")
        ),
        "primary_either_above_3_50": sum(
            1 for item in displayed if item.open_width_percent > Decimal("3.50") or item.close_width_percent > Decimal("3.50")
        ),
        "maximum_boundary_rejection_count": sum(1 for item in searches if item.terminal in FORBIDDEN_BOUNDARY_REASONS),
        "maximum_boundary_limit": "NONE",
        "wick_only_events": sum(item.wick_only for item in searches),
        "wick_only_confirmations": 0,
        "zero_uses_full_return": ZERO_USES_FULL_RETURN,
        "zero_uses_one_third": ZERO_USES_ONE_THIRD,
        "normal_uses_full_return": NORMAL_USES_FULL_RETURN,
        "normal_uses_one_third": NORMAL_USES_ONE_THIRD,
        "required_body_fraction": format(THIRD, "f"),
        "largest_search_interior": max((item.max_interior for item in searches), default=0),
        "body_penetration_not_reached": sum(
            1 for item in searches if item.terminal == "SEARCH_HORIZON_EXHAUSTED_AT_5_INTERIOR_CANDLES"
        ),
        "horizon_exhausted": sum(
            1 for item in searches if item.terminal == "SEARCH_HORIZON_EXHAUSTED_AT_5_INTERIOR_CANDLES"
        ),
        "censored": sum(1 for item in searches if item.censored),
        "horizon_errors": 0,
        "terminals": {name: sum(1 for item in searches if item.terminal == name) for name in sorted({item.terminal for item in searches})},
    }


def signature(result: Analysis) -> str:
    rows = []
    for item in result.displayed:
        rows.append("|".join((
            item.swing_id,
            item.direction,
            item.formation_class,
            str(item.open_row),
            str(item.close_row),
            str(item.extreme_price),
            str(item.open_width_percent),
            str(item.close_width_percent),
        )))
    return hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()
