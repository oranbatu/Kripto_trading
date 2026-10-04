"""Exact 15m decomposition of authoritative 4h swings."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import pstdev
from typing import Sequence

from data.timeutil import datetime_to_ms

from detectors.swing_open_close_4h_with_15m_v1.engine import (
    PRIMARY,
    SWING_HIGH,
    Bar,
    Swing,
    iso_turkey,
    iso_utc,
)
from detectors.swing_open_close_4h_with_15m_v1.mapping_config import (
    CHILDREN_PER_PARENT,
    FOUND,
    MAPPED,
    NOT_OBSERVED,
)

D0 = Decimal("0")
D100 = Decimal("100")
CHILD_SPAN = timedelta(minutes=15) - timedelta(milliseconds=1)
CHILD_STEP = timedelta(minutes=15)


def extreme_plateaus(matches: Sequence[Bar]) -> list[tuple[Bar, ...]]:
    """Group exact extreme matches into consecutive 15m plateaus."""
    groups: list[list[Bar]] = []
    for match in matches:
        if groups and match.open_time == groups[-1][-1].open_time + CHILD_STEP:
            groups[-1].append(match)
        else:
            groups.append([match])
    return [tuple(group) for group in groups]


class MappingError(RuntimeError):
    """A 15m alignment, aggregation, or extreme match failed."""


@dataclass(slots=True)
class ParentLink:
    parent: Bar
    children: tuple[Bar, ...]
    open_equal: bool
    high_equal: bool
    low_equal: bool
    close_equal: bool
    volume_equal: bool
    quote_equal: bool
    trades_equal: bool
    taker_base_equal: bool
    taker_quote_equal: bool

    @property
    def passed(self) -> bool:
        return all((
            self.open_equal, self.high_equal, self.low_equal, self.close_equal,
            self.volume_equal, self.quote_equal, self.trades_equal,
            self.taker_base_equal, self.taker_quote_equal,
            len(self.children) == CHILDREN_PER_PARENT,
        ))


@dataclass(slots=True)
class MappedSwing:
    swing: Swing
    rows: list[dict]
    direct_open: Bar
    matches: list[Bar]
    representative: Bar
    direct_close: Bar | None
    direct_close_state: str
    wick_rejected: int
    final_child: Bar
    first_close_child: Bar
    precedes_confirmation: bool
    inside_official_close_block: bool
    minutes_before_confirmation: Decimal | None
    child_position: int | None
    status: str
    metrics: dict


@dataclass(slots=True)
class MapResult:
    links: list[ParentLink]
    swings: list[MappedSwing]
    failures: list[str] = field(default_factory=list)


def align_parents(parents: Sequence[Bar], children: Sequence[Bar]) -> list[ParentLink]:
    """Require every 4h candle to equal sixteen exact 15m children."""
    if len(children) != len(parents) * CHILDREN_PER_PARENT:
        raise MappingError(f"child count {len(children)} != {len(parents)} * {CHILDREN_PER_PARENT}")
    by_open = {}
    for child in children:
        if child.open_time in by_open:
            raise MappingError(f"duplicate 15m open {iso_utc(child.open_time)}")
        by_open[child.open_time] = child
    links: list[ParentLink] = []
    assigned: set[datetime] = set()
    for parent in parents:
        block = []
        for offset in range(CHILDREN_PER_PARENT):
            opened = parent.open_time + CHILD_STEP * offset
            child = by_open.get(opened)
            if child is None:
                raise MappingError(f"missing 15m child {iso_utc(opened)} for {iso_utc(parent.open_time)}")
            if child.open_time.minute not in (0, 15, 30, 45) or child.open_time.second != 0 or child.open_time.microsecond != 0:
                raise MappingError(f"15m open is not 15-minute aligned {iso_utc(child.open_time)}")
            if child.open_time in assigned:
                raise MappingError(f"child assigned twice {iso_utc(child.open_time)}")
            assigned.add(child.open_time)
            if offset and child.open_time != block[-1].open_time + CHILD_STEP:
                raise MappingError(f"15m gap at {iso_utc(child.open_time)}")
            if child.close_time != child.open_time + CHILD_SPAN:
                raise MappingError(f"15m close mismatch {iso_utc(child.open_time)}")
            block.append(child)
        if block[0].open_time != parent.open_time or block[-1].close_time != parent.close_time:
            raise MappingError(f"parent bounds mismatch {iso_utc(parent.open_time)}")
        link = ParentLink(
            parent=parent,
            children=tuple(block),
            open_equal=parent.open == block[0].open,
            high_equal=parent.high == max(item.high for item in block),
            low_equal=parent.low == min(item.low for item in block),
            close_equal=parent.close == block[-1].close,
            volume_equal=parent.volume == sum((item.volume for item in block), D0),
            quote_equal=parent.quote_volume == sum((item.quote_volume for item in block), D0),
            trades_equal=parent.trades == sum(item.trades for item in block),
            taker_base_equal=parent.taker_base == sum((item.taker_base for item in block), D0),
            taker_quote_equal=parent.taker_quote == sum((item.taker_quote for item in block), D0),
        )
        if not link.passed:
            raise MappingError(f"parent aggregation failed at {iso_utc(parent.open_time)}")
        links.append(link)
    if len(assigned) != len(children):
            raise MappingError("a 15m candle was not assigned to a parent")
    return links


def map_primaries(swings: Sequence[Swing], links: Sequence[ParentLink]) -> MapResult:
    primaries = [item for item in swings if item.role == PRIMARY]
    mapped: list[MappedSwing] = []
    failures: list[str] = []
    for swing in primaries:
        try:
            mapped.append(_map_one(swing, links))
        except MappingError as exc:
            failures.append(f"{swing.primary_id}: {exc}")
    if failures:
        raise MappingError("; ".join(failures))
    return MapResult(links=list(links), swings=mapped, failures=failures)


def result_signature(mapped: MapResult) -> str:
    import hashlib
    parts = []
    for item in mapped.swings:
        close = "" if item.direct_close is None else iso_utc(item.direct_close.close_time)
        parts.append("|".join((
            item.swing.primary_id,
            item.swing.direction,
            item.swing.detection_method,
            iso_utc(item.direct_open.open_time),
            iso_utc(item.representative.open_time),
            str(item.swing.extreme_price),
            item.direct_close_state,
            close,
            str(len(item.rows)),
            iso_utc(item.swing.confirmed_at),
        )))
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def swing_identity(swing: Swing) -> tuple:
    return (
        swing.swing_id,
        swing.primary_id,
        swing.role,
        swing.direction,
        swing.detection_method,
        swing.formation_class,
        swing.open_row,
        iso_utc(swing.open_time),
        swing.extreme_row,
        str(swing.extreme_price),
        swing.extreme_event_id,
        swing.close_row,
        iso_utc(swing.confirmed_at),
        swing.interior_count,
        swing.total_candles,
        str(swing.open_to_extreme_percent),
        str(swing.close_to_extreme_percent),
        swing.family_id,
        swing.search_terminal_reason,
    )


def _map_one(swing: Swing, links: Sequence[ParentLink]) -> MappedSwing:
    if swing.role != PRIMARY:
        raise MappingError("only primary swings are mapped")
    expected = swing.total_candles * CHILDREN_PER_PARENT
    block_links = list(links[swing.open_row:swing.close_row + 1])
    if len(block_links) != swing.total_candles:
        raise MappingError("formation parent span does not match total candles")
    children: list[Bar] = []
    for link in block_links:
        if not link.passed:
            raise MappingError("parent aggregation failed")
        children.extend(link.children)
    if len(children) != expected:
        raise MappingError(f"mapped count {len(children)} != {expected}")
    direct_open = children[0]
    if direct_open.open_time != swing.open_time or direct_open.open != swing.reference:
        raise MappingError("15m swing open does not match the 4h reference")
    plateau_links = list(links[swing.plateau_start:swing.plateau_end + 1])
    matches: list[Bar] = []
    for link in plateau_links:
        for child in link.children:
            price = child.high if swing.direction == SWING_HIGH else child.low
            if price == swing.extreme_price:
                matches.append(child)
    if not matches:
        raise MappingError("MAPPED_15M_EXTREME_NOT_FOUND")
    representative = matches[-1]
    final_child = block_links[-1].children[-1]
    first_close_child = block_links[-1].children[0]
    if final_child.close != swing.close_price:
        raise MappingError("final 15m child close does not equal the 4h close")
    direct_close, state, wick_rejected, flags = _direct_close(swing, children, representative)
    inside = False
    position = None
    precedes = False
    minutes = None
    if direct_close is not None:
        parent_index = _parent_index(direct_close, block_links)
        inside = parent_index == swing.close_row
        position = _child_offset(direct_close, links[parent_index]) + 1
        precedes = direct_close.close_time < swing.confirmed_at
        minutes = Decimal(datetime_to_ms(swing.confirmed_at) - datetime_to_ms(direct_close.close_time)) / Decimal(60000)
        if direct_close.close_time > swing.confirmed_at or direct_close.close_time <= representative.close_time:
            raise MappingError("DIRECT_15M_CLOSE_OUTSIDE_OFFICIAL_WINDOW")
    metrics = _metrics(swing, children, representative, direct_close)
    rows = []
    for sequence, child in enumerate(children, start=1):
        parent_index = _parent_index(child, block_links)
        link = links[parent_index]
        offset = _child_offset(child, link)
        exact = child in matches
        representative_flag = child.open_time == representative.open_time
        is_direct_close = direct_close is not None and child.open_time == direct_close.open_time
        is_final = child.open_time == final_child.open_time
        is_open = sequence == 1
        kind = _parent_kind(parent_index, swing)
        search_eligible, condition_passed = flags.get(child.open_time, (False, False))
        role, combined = _roles(
            is_open, kind, exact, representative_flag, is_direct_close, is_final, swing.direction,
        )
        rows.append({
            "child": child,
            "sequence": sequence,
            "parent": link.parent,
            "parent_index": parent_index,
            "offset": offset + 1,
            "kind": kind,
            "role": role,
            "combined": combined,
            "exact": exact,
            "representative": representative_flag,
            "is_open": is_open,
            "is_direct_close": is_direct_close,
            "is_final": is_final,
            "search_eligible": search_eligible,
            "condition_passed": condition_passed,
            "before_extreme": child.close_time <= representative.close_time,
            "after_extreme": child.open_time > representative.open_time,
            "after_direct": direct_close is not None and child.open_time > direct_close.open_time,
            "swing_status": MAPPED,
        })
    return MappedSwing(
        swing=swing,
        rows=rows,
        direct_open=direct_open,
        matches=matches,
        representative=representative,
        direct_close=direct_close,
        direct_close_state=state,
        wick_rejected=wick_rejected,
        final_child=final_child,
        first_close_child=first_close_child,
        precedes_confirmation=precedes,
        inside_official_close_block=inside,
        minutes_before_confirmation=minutes,
        child_position=position,
        status=MAPPED,
        metrics=metrics,
    )


def _direct_close(swing: Swing, children: Sequence[Bar], representative: Bar):
    confirmed_ms = datetime_to_ms(swing.confirmed_at)
    extreme_ms = datetime_to_ms(representative.close_time)
    found = None
    wick = 0
    flags: dict[datetime, tuple[bool, bool]] = {}
    for child in children:
        close_ms = datetime_to_ms(child.close_time)
        eligible = extreme_ms < close_ms <= confirmed_ms
        passed = eligible and _close_passed(swing, child.close)
        if eligible and _wick_only(swing, child):
            wick += 1
        flags[child.open_time] = (eligible, passed)
        if passed and found is None:
            found = child
    state = FOUND if found is not None else NOT_OBSERVED
    return found, state, wick, flags


def _close_passed(swing: Swing, close: Decimal) -> bool:
    if swing.direction == SWING_HIGH:
        return close <= swing.reference
    return close >= swing.reference


def _wick_only(swing: Swing, child: Bar) -> bool:
    touched = child.low <= swing.reference <= child.high
    return touched and not _close_passed(swing, child.close)


def _parent_index(child: Bar, block_links: Sequence[ParentLink]) -> int:
    for link in block_links:
        if any(item.open_time == child.open_time for item in link.children):
            return link.parent.row
    raise MappingError(f"child has no parent {iso_utc(child.open_time)}")


def _child_offset(child: Bar, link: ParentLink) -> int:
    for index, item in enumerate(link.children):
        if item.open_time == child.open_time:
            return index
    raise MappingError("child missing from parent block")


def _parent_kind(parent_index: int, swing: Swing) -> str:
    if parent_index == swing.open_row:
        return "open"
    if swing.plateau_start <= parent_index <= swing.plateau_end:
        return "extreme"
    if parent_index == swing.close_row:
        return "close"
    return "interior"


def _roles(is_open, kind, exact, representative, is_direct, is_final, direction) -> tuple[str, str]:
    names = []
    if is_open:
        names.append("SWING_OPEN_15M")
    if kind == "open" and not is_open:
        names.append("OPEN_BLOCK_CHILD")
    if exact and not representative:
        names.append("EXTREME_MATCH_15M")
    if representative:
        names.append("REPRESENTATIVE_EXTREME_15M")
    if is_direct:
        names.append("DIRECT_SWING_CLOSE_15M")
    if kind == "close" and not is_final:
        names.append("OFFICIAL_4H_CLOSE_BLOCK_CHILD")
    if is_final:
        names.append("FINAL_OFFICIAL_4H_CLOSE_CHILD")
    if kind == "interior" and not names:
        names.append("INTERIOR_15M")
    if not names:
        names.append("INTERIOR_15M")
    priority = (
        "SWING_OPEN_15M",
        "REPRESENTATIVE_EXTREME_15M",
        "DIRECT_SWING_CLOSE_15M",
        "FINAL_OFFICIAL_4H_CLOSE_CHILD",
        "EXTREME_MATCH_15M",
        "OPEN_BLOCK_CHILD",
        "OFFICIAL_4H_CLOSE_BLOCK_CHILD",
        "INTERIOR_15M",
    )
    primary = next(name for name in priority if name in names)
    if direction == SWING_HIGH and primary == "REPRESENTATIVE_EXTREME_15M":
        primary = "REPRESENTATIVE_EXTREME_15M"
    return primary, " | ".join(names)


def _metrics(swing: Swing, children: Sequence[Bar], representative: Bar, direct_close: Bar | None) -> dict:
    closes = [item.close for item in children]
    net = closes[-1] - closes[0]
    path = D0
    bullish = bearish = unchanged = 0
    for previous, current in zip(closes, closes[1:]):
        path += abs(current - previous)
    for item in children:
        if item.close > item.open:
            bullish += 1
        elif item.close < item.open:
            bearish += 1
        else:
            unchanged += 1
    returns = []
    for previous, current in zip(closes, closes[1:]):
        if previous != 0:
            returns.append(float((current - previous) / previous))
    volatility = Decimal(str(pstdev(returns))) if len(returns) > 1 else D0
    pullback = D0
    peak = closes[0]
    trough = closes[0]
    for price in closes:
        peak = max(peak, price)
        trough = min(trough, price)
        if swing.direction == SWING_HIGH:
            pullback = max(pullback, peak - price)
        else:
            pullback = max(pullback, price - trough)
    volume = sum((item.volume for item in children), D0)
    quote = sum((item.quote_volume for item in children), D0)
    trades = sum(item.trades for item in children)
    taker = sum((item.taker_base for item in children), D0)
    weighted = sum(((item.high + item.low + item.close) / Decimal(3) * item.volume for item in children), D0)
    vwap = None if volume == 0 else weighted / volume
    extreme_index = next(index for index, item in enumerate(children) if item.open_time == representative.open_time)
    direct_index = None if direct_close is None else next(index for index, item in enumerate(children) if item.open_time == direct_close.open_time)
    after_extreme = None if direct_index is None else direct_index - extreme_index
    to_confirmation = None
    if direct_close is not None:
        to_confirmation = sum(1 for item in children if representative.close_time < item.close_time <= swing.confirmed_at and item.open_time > direct_close.open_time)
    return {
        "path": path,
        "net": net,
        "efficiency": D0 if path == 0 else abs(net) / path,
        "bullish": bullish,
        "bearish": bearish,
        "unchanged": unchanged,
        "pullback": pullback,
        "volatility": volatility,
        "volume": volume,
        "quote": quote,
        "trades": trades,
        "taker_ratio": None if volume == 0 else taker / volume,
        "vwap": vwap,
        "candles_to_extreme": extreme_index,
        "candles_extreme_to_direct": after_extreme,
        "candles_direct_to_confirmation": to_confirmation,
    }


def flag(value: bool) -> str:
    return "TRUE" if value else "FALSE"


def price_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def turkey(value: datetime | None) -> str | None:
    return None if value is None else iso_turkey(value)


def utc(value: datetime | None) -> str | None:
    return None if value is None else iso_utc(value)
