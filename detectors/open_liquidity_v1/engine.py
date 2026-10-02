"""Exact completed-candle open-liquidity detection.

A later wick that reaches a level closes it. Equality counts. The origin
candle cannot close its own extreme. The final candle has no later
observation, so its extremes are terminal rather than proven open.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Sequence
from zoneinfo import ZoneInfo

from detectors.open_liquidity_v1.liquidity_config import PREFIX, TIER_ORDER

D0 = Decimal("0")
D13 = Decimal("13")
D14 = Decimal("14")
D100 = Decimal("100")
NEAR = Decimal("0.05")
MATCH = Decimal("0.10")
SPAN = Decimal("0.20")
HOURS_PER_BAR = {"1h": Decimal("1"), "4h": Decimal("4"), "1d": Decimal("24")}
TURKEY = ZoneInfo("Europe/Istanbul")
TIER_RANK = {name: index for index, name in enumerate(TIER_ORDER)}
OPEN = "OPEN_CONFIRMED_BY_LATER_OBSERVATION"
MITIGATED = "MITIGATED"
TERMINAL = "TERMINAL_NO_LATER_CANDLE"


class LiquidityError(RuntimeError):
    """Raised when liquidity status or reconciliation is inconsistent."""


@dataclass(frozen=True, slots=True)
class Bar:
    timeframe: str
    row: int
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = D0
    trades: int = 0


@dataclass
class Candidate:
    candidate_id: str
    timeframe: str
    side: str
    row: int
    open_time: datetime
    close_time: datetime
    price: Decimal
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trades: int
    atr: Decimal | None
    upper_wick: Decimal
    lower_wick: Decimal
    body: Decimal
    wick_to_body: Decimal | None
    origin_class: str
    status: str = ""
    touch_status: str = ""
    close_status: str = ""
    mitigation_row: int | None = None
    close_mitigation_row: int | None = None
    mechanism: str = ""
    exact_flag: bool = False
    gap_flag: bool = False
    wick_flag: bool = False
    body_flag: bool = False
    close_flag: bool = False
    penetration: Decimal = D0
    penetration_percent: Decimal = D0
    near_miss: bool = False
    chain_id: str = ""
    chain_sequence: int = 1
    chain_length: int = 1
    previous_equal_id: str = ""
    next_equal_id: str = ""
    chain_survivor_id: str = ""
    future_bars: int = 0
    distance_price: Decimal = D0
    distance_percent: Decimal = D0
    distance_atr: Decimal | None = None
    contradiction: bool = False
    timeframe_rank: int = 0
    side_rank: int = 0
    nearest_rank: int = 0
    oldest_rank: int = 0
    newest_rank: int = 0
    wick_rank: int = 0
    observation_rank: int = 0
    group_id: str = ""
    tier: str = ""
    warning: str = ""


@dataclass
class MappingAttempt:
    attempt_id: str
    side: str
    parent_timeframe: str
    parent_id: str
    parent_price: Decimal
    child_timeframe: str
    child_id: str
    child_price: Decimal
    distance_percent: Decimal
    proposed_span: Decimal
    tolerance_pass: bool
    span_pass: bool
    accepted: bool
    selected_parent: str
    rejection_reason: str
    tie_break: str


@dataclass
class LiquidityGroup:
    group_id: str
    side: str
    members: dict[str, Candidate]
    tier: str
    representative: Decimal
    distance_price: Decimal
    distance_percent: Decimal
    max_span: Decimal
    oldest: datetime
    newest: datetime
    observation_hours: Decimal
    warning: str
    rank: int = 0
    side_rank: int = 0


@dataclass
class AnalysisResult:
    bars: dict[str, list[Bar]]
    candidates: dict[str, list[Candidate]]
    groups: list[LiquidityGroup]
    attempts: list[MappingAttempt]
    final_close: dict[str, Decimal]
    final_close_time: dict[str, datetime]
    final_atr: dict[str, Decimal | None]
    near_miss_count: int
    warnings: list[str] = field(default_factory=list)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def iso_turkey(dt: datetime) -> str:
    return dt.astimezone(TURKEY).isoformat(timespec="milliseconds")


def wilder_atr(bars: Sequence[Bar]) -> list[Decimal | None]:
    atr: list[Decimal | None] = [None] * len(bars)
    if not bars:
        return atr
    trs: list[Decimal] = []
    previous: Decimal | None = None
    for bar in bars:
        if previous is None:
            tr = bar.high - bar.low
        else:
            tr = max(bar.high - bar.low, abs(bar.high - previous), abs(bar.low - previous))
        trs.append(tr)
        previous = bar.close
    if len(trs) < 14:
        return atr
    seed = sum(trs[:14], D0) / D14
    atr[13] = seed
    current = seed
    for index in range(14, len(trs)):
        current = (current * D13 + trs[index]) / D14
        atr[index] = current
    return atr


def origin_classification(side: str, open_: Decimal, high: Decimal, low: Decimal, close: Decimal) -> str:
    if side == "HIGH":
        return "UPPER_WICK_EXTREME" if high > max(open_, close) else "BODY_AT_HIGH"
    return "LOWER_WICK_EXTREME" if low < min(open_, close) else "BODY_AT_LOW"


def relative_distance_percent(left: Decimal, right: Decimal) -> Decimal:
    return abs(left - right) / ((left + right) / 2) * D100


def hierarchy_tier(timeframes: set[str] | Sequence[str]) -> str:
    present = set(timeframes)
    has_d, has_4, has_1 = "1d" in present, "4h" in present, "1h" in present
    if has_d and has_4 and has_1:
        return "TIER_1_ALL_TIMEFRAMES"
    if has_d and (has_4 or has_1):
        return "TIER_2_DAILY_WITH_INTRADAY"
    if has_d:
        return "TIER_3_DAILY_ONLY"
    if has_4 and has_1:
        return "TIER_4_4H_AND_1H"
    if has_4:
        return "TIER_5_4H_ONLY"
    if has_1:
        return "TIER_6_1H_ONLY"
    raise LiquidityError("a hierarchy group requires a timeframe")


def representative_price(members: dict[str, Candidate]) -> Decimal:
    if "1d" in members:
        return members["1d"].price
    if "4h" in members:
        return members["4h"].price
    return members["1h"].price


def observation_warning(future_bars: int, candle_count: int) -> str:
    """Share of the timeframe sample that sits strictly after the origin.

    VERY_LOW: under 1% of candles remain after the origin.
    LOW: 1% to under 5%.
    MODERATE: 5% to under 20%.
    HIGH: 20% or more.
    """
    if candle_count <= 0:
        return "VERY_LOW_FUTURE_OBSERVATION"
    share = Decimal(future_bars) / Decimal(candle_count)
    if share < Decimal("0.01"):
        return "VERY_LOW_FUTURE_OBSERVATION"
    if share < Decimal("0.05"):
        return "LOW_FUTURE_OBSERVATION"
    if share < Decimal("0.20"):
        return "MODERATE_FUTURE_OBSERVATION"
    return "HIGH_FUTURE_OBSERVATION"


def _sparse_extreme(values: Sequence[Decimal], high: bool) -> list[list[Decimal]]:
    rows = [list(values)]
    span = 1
    while span * 2 <= len(values):
        previous = rows[-1]
        limit = len(values) - span * 2 + 1
        nxt = []
        for index in range(limit):
            left = previous[index]
            right = previous[index + span]
            if high:
                nxt.append(left if left >= right else right)
            else:
                nxt.append(left if left <= right else right)
        rows.append(nxt)
        span *= 2
    return rows


def _query(table: Sequence[Sequence[Decimal]], left: int, right: int, high: bool) -> Decimal:
    length = right - left + 1
    level = length.bit_length() - 1
    span = 1 << level
    first = table[level][left]
    second = table[level][right - span + 1]
    if high:
        return first if first >= second else second
    return first if first <= second else second


def suffix_extremes(values: Sequence[Decimal], high: bool) -> list[Decimal | None]:
    """Extreme of every strictly later value. The final row is None."""
    result: list[Decimal | None] = [None] * len(values)
    running: Decimal | None = None
    for index in range(len(values) - 1, -1, -1):
        result[index] = running
        current = values[index]
        if running is None:
            running = current
        elif high:
            running = current if current > running else running
        else:
            running = current if current < running else running
    return result


def first_reach(table: Sequence[Sequence[Decimal]], start: int, end: int, price: Decimal, high: bool) -> int | None:
    if start > end:
        return None
    lo, hi = start, end
    found = None
    while lo <= hi:
        mid = (lo + hi) // 2
        extreme = _query(table, start, mid, high)
        reached = extreme >= price if high else extreme <= price
        if reached:
            found = mid
            hi = mid - 1
        else:
            lo = mid + 1
    return found


def brute_suffix(values: Sequence[Decimal], high: bool) -> list[Decimal | None]:
    result: list[Decimal | None] = [None] * len(values)
    for index in range(len(values)):
        later = values[index + 1 :]
        if not later:
            continue
        result[index] = max(later) if high else min(later)
    return result


def brute_first(values: Sequence[Decimal], start: int, price: Decimal, high: bool) -> int | None:
    for index in range(start, len(values)):
        if values[index] >= price if high else values[index] <= price:
            return index
    return None


def classify_mechanism(side: str, price: Decimal, bar: Bar) -> tuple[str, bool, bool, bool, bool, bool]:
    if side == "HIGH":
        gap = bar.open >= price
        exact = bar.high == price
        close_beyond = bar.close >= price
        body = max(bar.open, bar.close) >= price
        wick = bar.high > price and max(bar.open, bar.close) < price
        _penetration = bar.high - price
    else:
        gap = bar.open <= price
        exact = bar.low == price
        close_beyond = bar.close <= price
        body = min(bar.open, bar.close) <= price
        wick = bar.low < price and min(bar.open, bar.close) > price
        _penetration = price - bar.low
    del _penetration
    if gap:
        mechanism = "GAP_OPEN_AT_OR_BEYOND"
    elif exact:
        mechanism = "EXACT_EXTREME_TOUCH"
    elif close_beyond:
        mechanism = "CLOSE_AT_OR_BEYOND"
    elif body:
        mechanism = "BODY_REACH"
    else:
        mechanism = "WICK_REACH"
    return mechanism, exact, gap, wick, body, close_beyond


def _candidate(bar: Bar, side: str, atr: Decimal | None) -> Candidate:
    prefix = PREFIX[bar.timeframe]
    letter = "H" if side == "HIGH" else "L"
    price = bar.high if side == "HIGH" else bar.low
    upper = bar.high - max(bar.open, bar.close)
    lower = min(bar.open, bar.close) - bar.low
    body = abs(bar.close - bar.open)
    wick = upper if side == "HIGH" else lower
    return Candidate(
        candidate_id=f"LQ-{prefix}-{letter}-{bar.row + 1:06d}",
        timeframe=bar.timeframe,
        side=side,
        row=bar.row,
        open_time=bar.open_time,
        close_time=bar.close_time,
        price=price,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        trades=bar.trades,
        atr=atr,
        upper_wick=upper,
        lower_wick=lower,
        body=body,
        wick_to_body=(wick / body) if body > 0 else None,
        origin_class=origin_classification(side, bar.open, bar.high, bar.low, bar.close),
    )


def _is_near_miss(side: str, price: Decimal, extreme: Decimal | None) -> bool:
    if extreme is None or price <= 0:
        return False
    if side == "HIGH":
        if extreme >= price:
            return False
        return (price - extreme) / price * D100 <= NEAR
    if extreme <= price:
        return False
    return (extreme - price) / price * D100 <= NEAR


def detect_timeframe(bars: Sequence[Bar]) -> list[Candidate]:
    if not bars:
        return []
    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]
    closes = [bar.close for bar in bars]
    atrs = wilder_atr(bars)
    high_table = _sparse_extreme(highs, True)
    low_table = _sparse_extreme(lows, False)
    close_max = _sparse_extreme(closes, True)
    close_min = _sparse_extreme(closes, False)
    future_high = suffix_extremes(highs, True)
    future_low = suffix_extremes(lows, False)
    last = len(bars) - 1
    candidates: list[Candidate] = []
    for bar in bars:
        for side in ("HIGH", "LOW"):
            candidate = _candidate(bar, side, atrs[bar.row])
            candidate.future_bars = last - bar.row
            if candidate.future_bars == 0:
                candidate.status = TERMINAL
                candidate.touch_status = TERMINAL
                candidate.close_status = TERMINAL
                candidates.append(candidate)
                continue
            if side == "HIGH":
                reached = future_high[bar.row] is not None and future_high[bar.row] >= candidate.price
                candidate.near_miss = (not reached) and _is_near_miss(side, candidate.price, future_high[bar.row])
                touch_row = first_reach(high_table, bar.row + 1, last, candidate.price, True) if reached else None
                close_row = first_reach(close_max, bar.row + 1, last, candidate.price, True)
            else:
                reached = future_low[bar.row] is not None and future_low[bar.row] <= candidate.price
                candidate.near_miss = (not reached) and _is_near_miss(side, candidate.price, future_low[bar.row])
                touch_row = first_reach(low_table, bar.row + 1, last, candidate.price, False) if reached else None
                close_row = first_reach(close_min, bar.row + 1, last, candidate.price, False)
            if reached:
                if touch_row is None or touch_row <= bar.row:
                    raise LiquidityError(f"{candidate.candidate_id} mitigation is not strictly later")
                later = bars[touch_row]
                mechanism, exact, gap, wick, body, close_beyond = classify_mechanism(side, candidate.price, later)
                candidate.status = MITIGATED
                candidate.touch_status = MITIGATED
                candidate.mitigation_row = touch_row
                candidate.mechanism = mechanism
                candidate.exact_flag = exact
                candidate.gap_flag = gap
                candidate.wick_flag = wick
                candidate.body_flag = body
                candidate.close_flag = close_beyond
                candidate.penetration = later.high - candidate.price if side == "HIGH" else candidate.price - later.low
                candidate.penetration_percent = candidate.penetration / candidate.price * D100
            else:
                candidate.status = OPEN
                candidate.touch_status = OPEN
            if close_row is None:
                candidate.close_status = "NOT_CLOSE_MITIGATED"
            else:
                candidate.close_status = "CLOSE_MITIGATED"
                candidate.close_mitigation_row = close_row
            candidates.append(candidate)
    _assign_equal_chains(candidates)
    return candidates


def _assign_equal_chains(candidates: Sequence[Candidate]) -> None:
    grouped: dict[tuple[str, Decimal], list[Candidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[(candidate.side, candidate.price)].append(candidate)
    chain_number = {"HIGH": 0, "LOW": 0}
    for (side, _price), chain in grouped.items():
        chain.sort(key=lambda item: item.row)
        chain_number[side] += 1
        prefix = PREFIX[chain[0].timeframe]
        chain_id = f"CH-{prefix}-{side[0]}-{chain_number[side]:06d}"
        survivor = chain[-1].candidate_id if chain[-1].status == OPEN else ""
        for sequence, candidate in enumerate(chain, start=1):
            candidate.chain_id = chain_id
            candidate.chain_sequence = sequence
            candidate.chain_length = len(chain)
            candidate.previous_equal_id = chain[sequence - 2].candidate_id if sequence > 1 else ""
            candidate.next_equal_id = chain[sequence].candidate_id if sequence < len(chain) else ""
            candidate.chain_survivor_id = survivor


def _distance(side: str, price: Decimal, reference: Decimal, atr: Decimal | None) -> tuple[Decimal, Decimal, Decimal | None, bool]:
    if side == "HIGH":
        delta = price - reference
    else:
        delta = reference - price
    percent = delta / reference * D100 if reference else D0
    distance_atr = delta / atr if atr not in (None, D0) else None
    return delta, percent, distance_atr, delta <= 0


def rank_open_candidates(
    candidates: Sequence[Candidate],
    reference: Decimal,
    final_atr: Decimal | None,
    candle_count: int,
) -> list[Candidate]:
    opens = [candidate for candidate in candidates if candidate.status == OPEN]
    for candidate in opens:
        delta, percent, distance_atr, contradiction = _distance(candidate.side, candidate.price, reference, final_atr)
        candidate.distance_price = delta
        candidate.distance_percent = percent
        candidate.distance_atr = distance_atr
        candidate.contradiction = contradiction
        candidate.warning = observation_warning(candidate.future_bars, candle_count)
    for side in ("HIGH", "LOW"):
        sided = [candidate for candidate in opens if candidate.side == side]
        sided.sort(key=_nearest_key)
        for rank, candidate in enumerate(sided, start=1):
            candidate.side_rank = rank
            candidate.timeframe_rank = rank
            candidate.nearest_rank = rank
        by_age = sorted(sided, key=lambda candidate: (candidate.open_time, candidate.candidate_id))
        for rank, candidate in enumerate(by_age, start=1):
            candidate.oldest_rank = rank
        for rank, candidate in enumerate(reversed(by_age), start=1):
            candidate.newest_rank = rank
        by_wick = sorted(sided, key=_wick_key)
        for rank, candidate in enumerate(by_wick, start=1):
            candidate.wick_rank = rank
        by_obs = sorted(sided, key=lambda candidate: (-candidate.future_bars, candidate.candidate_id))
        for rank, candidate in enumerate(by_obs, start=1):
            candidate.observation_rank = rank
    return opens


def _wick_atr(candidate: Candidate) -> Decimal:
    wick = candidate.upper_wick if candidate.side == "HIGH" else candidate.lower_wick
    if candidate.atr in (None, D0):
        return D0
    return wick / candidate.atr


def _nearest_key(candidate: Candidate) -> tuple:
    return (
        candidate.distance_price,
        -candidate.future_bars,
        -candidate.future_bars,
        -_wick_atr(candidate),
        -candidate.open_time.timestamp(),
        candidate.candidate_id,
    )


def _wick_key(candidate: Candidate) -> tuple:
    return (-_wick_atr(candidate), candidate.candidate_id)


class _GroupBuilder:
    def __init__(self, anchor: Candidate) -> None:
        self.anchor = anchor
        self.members = {anchor.timeframe: anchor}
        self.prices = [anchor.price]

    def span_with(self, child: Candidate) -> Decimal:
        prices = self.prices + [child.price]
        return relative_distance_percent(min(prices), max(prices))

    def add(self, child: Candidate) -> None:
        self.members[child.timeframe] = child
        self.prices.append(child.price)


def _parent_key(parent: Candidate, distance: Decimal) -> tuple:
    order = {"1d": 0, "4h": 1, "1h": 2}[parent.timeframe]
    return (distance, order, -parent.future_bars, parent.open_time, parent.candidate_id)


def attach_children(
    children: Sequence[Candidate],
    parents: Sequence[_GroupBuilder],
    attempts: list[MappingAttempt],
) -> list[Candidate]:
    """Give each parent at most one child of a timeframe, choosing the nearest pair first.

    A child that loses a parent because that timeframe slot is already filled stays
    available for another parent. A child that is never selected stays unmatched.
    """
    pairs: list[tuple[_GroupBuilder, Candidate, Decimal, Decimal]] = []
    ordered_children = sorted(children, key=lambda item: (item.price, item.candidate_id))
    for child in ordered_children:
        for parent in parents:
            if parent.anchor.side != child.side:
                attempts.append(
                    _attempt(parent.anchor, child, D0, D0, False, False, False, "", "HIGH_LOW_SIDE_MISMATCH", "REJECTED_SIDE")
                )
                continue
            distance = relative_distance_percent(child.price, parent.anchor.price)
            if distance > MATCH:
                attempts.append(
                    _attempt(
                        parent.anchor,
                        child,
                        distance,
                        D0,
                        False,
                        False,
                        False,
                        "",
                        "RELATIVE_DISTANCE_ABOVE_0_10",
                        "REJECTED_DISTANCE",
                    )
                )
                continue
            proposed = parent.span_with(child)
            if proposed > SPAN:
                attempts.append(
                    _attempt(parent.anchor, child, distance, proposed, True, False, False, "", "GROUP_SPAN_ABOVE_0_20", "REJECTED_SPAN")
                )
                continue
            pairs.append((parent, child, distance, proposed))
    pairs.sort(key=lambda item: _parent_key(item[0].anchor, item[2]) + (item[1].candidate_id,))
    assigned: dict[str, str] = {}
    for parent, child, distance, proposed in pairs:
        if child.candidate_id in assigned:
            attempts.append(
                _attempt(
                    parent.anchor,
                    child,
                    distance,
                    proposed,
                    True,
                    True,
                    False,
                    assigned[child.candidate_id],
                    "LOST_TIE_BREAK",
                    "LOST_TIE_BREAK",
                )
            )
            continue
        if child.timeframe in parent.members:
            attempts.append(
                _attempt(
                    parent.anchor,
                    child,
                    distance,
                    proposed,
                    True,
                    True,
                    False,
                    "",
                    "PARENT_TIMEFRAME_ALREADY_FILLED",
                    "PARENT_KEPT_NEARER_CHILD",
                )
            )
            continue
        parent.add(child)
        assigned[child.candidate_id] = parent.anchor.candidate_id
        attempts.append(
            _attempt(parent.anchor, child, distance, proposed, True, True, True, parent.anchor.candidate_id, "", "SELECTED")
        )
    return [child for child in ordered_children if child.candidate_id not in assigned]


def _attempt(
    parent: Candidate,
    child: Candidate,
    distance: Decimal,
    proposed: Decimal,
    tolerance: bool,
    span_ok: bool,
    accepted: bool,
    selected: str,
    reason: str,
    tie: str,
) -> MappingAttempt:
    return MappingAttempt(
        attempt_id="",
        side=child.side,
        parent_timeframe=parent.timeframe,
        parent_id=parent.candidate_id,
        parent_price=parent.price,
        child_timeframe=child.timeframe,
        child_id=child.candidate_id,
        child_price=child.price,
        distance_percent=distance,
        proposed_span=proposed,
        tolerance_pass=tolerance,
        span_pass=span_ok,
        accepted=accepted,
        selected_parent=selected,
        rejection_reason=reason,
        tie_break=tie,
    )


def _warning_severity(label: str) -> int:
    order = {
        "VERY_LOW_FUTURE_OBSERVATION": 0,
        "LOW_FUTURE_OBSERVATION": 1,
        "MODERATE_FUTURE_OBSERVATION": 2,
        "HIGH_FUTURE_OBSERVATION": 3,
    }
    return order.get(label, 9)


def build_hierarchy(
    opens: dict[str, list[Candidate]],
    reference: Decimal,
) -> tuple[list[LiquidityGroup], list[MappingAttempt]]:
    attempts: list[MappingAttempt] = []
    groups: list[LiquidityGroup] = []
    for side in ("HIGH", "LOW"):
        daily = [_GroupBuilder(item) for item in opens.get("1d", []) if item.side == side]
        four_open = [item for item in opens.get("4h", []) if item.side == side]
        hour_open = [item for item in opens.get("1h", []) if item.side == side]
        unmatched_4h = attach_children(four_open, daily, attempts)
        unmatched_1h = attach_children(hour_open, daily, attempts)
        four_groups = [_GroupBuilder(item) for item in unmatched_4h]
        still_1h = attach_children(unmatched_1h, four_groups, attempts)
        hour_groups = [_GroupBuilder(item) for item in still_1h]
        for builder in daily + four_groups + hour_groups:
            members = builder.members
            tier = hierarchy_tier(members)
            price = representative_price(members)
            delta = price - reference if side == "HIGH" else reference - price
            percent = delta / reference * D100 if reference else D0
            span = D0 if len(builder.prices) == 1 else relative_distance_percent(min(builder.prices), max(builder.prices))
            origins = [item.open_time for item in members.values()]
            observation = max(Decimal(item.future_bars) * HOURS_PER_BAR[item.timeframe] for item in members.values())
            groups.append(
                LiquidityGroup(
                    group_id="",
                    side=side,
                    members=members,
                    tier=tier,
                    representative=price,
                    distance_price=delta,
                    distance_percent=percent,
                    max_span=span,
                    oldest=min(origins),
                    newest=max(origins),
                    observation_hours=observation,
                    warning=min((member.warning for member in members.values()), key=_warning_severity),
                )
            )
    for side in ("HIGH", "LOW"):
        sided = [group for group in groups if group.side == side]
        sided.sort(
            key=lambda group: (
                TIER_RANK[group.tier],
                group.distance_price,
                -group.observation_hours,
                group.oldest,
                group.representative,
            )
        )
        for rank, group in enumerate(sided, start=1):
            group.side_rank = rank
            group.rank = rank
            group.group_id = f"HG-{side[0]}-{rank:06d}"
            for member in group.members.values():
                member.group_id = group.group_id
                member.tier = group.tier
    for index, attempt in enumerate(attempts, start=1):
        attempt.attempt_id = f"MA-{index:06d}"
    groups.sort(key=lambda group: (0 if group.side == "HIGH" else 1, group.side_rank))
    for index, group in enumerate(groups, start=1):
        group.rank = index
    return groups, attempts


def monthly_summary(result: AnalysisResult) -> list[dict[str, object]]:
    """UTC-month origin counts. Open rate uses candidates created in that month."""
    rows: list[dict[str, object]] = []
    for timeframe in ("1d", "4h", "1h"):
        buckets: dict[str, dict[str, object]] = {}
        for bar in result.bars.get(timeframe, []):
            key = bar.open_time.strftime("%Y-%m")
            bucket = buckets.setdefault(key, _empty_month(timeframe, key))
            bucket["candles"] = int(bucket["candles"]) + 1
        for candidate in result.candidates.get(timeframe, []):
            key = candidate.open_time.strftime("%Y-%m")
            bucket = buckets.setdefault(key, _empty_month(timeframe, key))
            if candidate.side == "HIGH":
                bucket["high_created"] = int(bucket["high_created"]) + 1
            else:
                bucket["low_created"] = int(bucket["low_created"]) + 1
            if candidate.status == OPEN:
                side_key = "high_open" if candidate.side == "HIGH" else "low_open"
                bucket[side_key] = int(bucket[side_key]) + 1
                oldest_key = "oldest_high" if candidate.side == "HIGH" else "oldest_low"
                current = bucket[oldest_key]
                if current is None or candidate.open_time < current:
                    bucket[oldest_key] = candidate.open_time
            elif candidate.status == MITIGATED:
                side_key = "high_mitigated" if candidate.side == "HIGH" else "low_mitigated"
                bucket[side_key] = int(bucket[side_key]) + 1
                bars = candidate.mitigation_row - candidate.row if candidate.mitigation_row is not None else 0
                bucket["mitigation_bars"].append(bars)  # type: ignore[union-attr]
            elif candidate.status == TERMINAL:
                bucket["terminal"] = int(bucket["terminal"]) + 1
        for key in sorted(buckets):
            bucket = buckets[key]
            samples = list(bucket["mitigation_bars"])  # type: ignore[arg-type]
            created = int(bucket["high_created"]) + int(bucket["low_created"])
            opened = int(bucket["high_open"]) + int(bucket["low_open"])
            mitigated = int(bucket["high_mitigated"]) + int(bucket["low_mitigated"])
            bucket["median_bars"] = _median(samples)
            bucket["max_bars"] = max(samples) if samples else None
            bucket["open_rate"] = (Decimal(opened) / Decimal(created)) if created else D0
            bucket["mitigation_rate"] = (Decimal(mitigated) / Decimal(created)) if created else D0
            rows.append(bucket)
    return rows


def _empty_month(timeframe: str, month: str) -> dict[str, object]:
    return {
        "timeframe": timeframe,
        "month": month,
        "candles": 0,
        "high_created": 0,
        "low_created": 0,
        "high_open": 0,
        "low_open": 0,
        "high_mitigated": 0,
        "low_mitigated": 0,
        "terminal": 0,
        "mitigation_bars": [],
        "median_bars": None,
        "max_bars": None,
        "open_rate": D0,
        "mitigation_rate": D0,
        "oldest_high": None,
        "oldest_low": None,
    }


def _median(values: list[int]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return Decimal(ordered[mid])
    return (Decimal(ordered[mid - 1]) + Decimal(ordered[mid])) / 2


def independent_audit(result: AnalysisResult, stride: int = 100) -> None:
    """Recompute a deterministic sample by forward scan and compare with the result."""
    for timeframe, bars in result.bars.items():
        by_row_side = {(item.row, item.side): item for item in result.candidates[timeframe]}
        last = len(bars) - 1
        for row in range(len(bars)):
            if row % stride != 0 and row != last and by_row_side[(row, "HIGH")].status != OPEN and by_row_side[(row, "LOW")].status != OPEN:
                continue
            for side in ("HIGH", "LOW"):
                candidate = by_row_side[(row, side)]
                found = None
                for later in range(row + 1, len(bars)):
                    reached = bars[later].high >= candidate.price if side == "HIGH" else bars[later].low <= candidate.price
                    if reached:
                        found = later
                        break
                if found is None:
                    expected = TERMINAL if row == last else OPEN
                    if candidate.status != expected or candidate.mitigation_row is not None:
                        raise LiquidityError(f"audit mismatch {candidate.candidate_id}")
                else:
                    if candidate.status != MITIGATED or candidate.mitigation_row != found:
                        raise LiquidityError(f"audit mitigation mismatch {candidate.candidate_id}")


def analyze(bars_by_tf: dict[str, Sequence[Bar]]) -> AnalysisResult:
    candidates: dict[str, list[Candidate]] = {}
    opens: dict[str, list[Candidate]] = {}
    final_close: dict[str, Decimal] = {}
    final_close_time: dict[str, datetime] = {}
    final_atr: dict[str, Decimal | None] = {}
    stored: dict[str, list[Bar]] = {}
    for timeframe in ("1d", "4h", "1h"):
        bars = list(bars_by_tf.get(timeframe, []))
        normalized: list[Bar] = []
        for index, bar in enumerate(bars):
            normalized.append(
                Bar(
                    timeframe,
                    index,
                    bar.open_time,
                    bar.close_time,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.trades,
                )
            )
        stored[timeframe] = normalized
        found = detect_timeframe(normalized)
        candidates[timeframe] = found
        if normalized:
            atrs = wilder_atr(normalized)
            final_close[timeframe] = normalized[-1].close
            final_close_time[timeframe] = normalized[-1].close_time
            final_atr[timeframe] = atrs[-1]
            opens[timeframe] = rank_open_candidates(found, normalized[-1].close, atrs[-1], len(normalized))
        else:
            opens[timeframe] = []
    reference = final_close.get("1h", final_close.get("4h", final_close.get("1d", D0)))
    groups, attempts = build_hierarchy(opens, reference)
    result = AnalysisResult(
        bars=stored,
        candidates=candidates,
        groups=groups,
        attempts=attempts,
        final_close=final_close,
        final_close_time=final_close_time,
        final_atr=final_atr,
        near_miss_count=sum(1 for rows in candidates.values() for item in rows if item.near_miss),
    )
    validate_result(result)
    return result


def validate_result(result: AnalysisResult) -> None:
    seen: set[str] = set()
    for timeframe, rows in result.candidates.items():
        bars = result.bars[timeframe]
        if len(rows) != len(bars) * 2:
            raise LiquidityError(f"{timeframe} candidate count {len(rows)} != {len(bars) * 2}")
        counts = {OPEN: 0, MITIGATED: 0, TERMINAL: 0}
        for candidate in rows:
            if candidate.status not in counts:
                raise LiquidityError(f"unknown status {candidate.status}")
            counts[candidate.status] += 1
            if candidate.mitigation_row is not None and candidate.mitigation_row <= candidate.row:
                raise LiquidityError("origin candle mitigated itself")
            if candidate.status == OPEN and candidate.future_bars < 1:
                raise LiquidityError("open candidate has no later candle")
            if candidate.status == OPEN and candidate.contradiction:
                raise LiquidityError(f"{candidate.candidate_id} open liquidity is on the wrong side of the final close")
        if counts[OPEN] + counts[MITIGATED] + counts[TERMINAL] != len(rows):
            raise LiquidityError("status reconciliation failed")
        if counts[TERMINAL] != (2 if bars else 0):
            raise LiquidityError(f"{timeframe} terminal count {counts[TERMINAL]}")
    for group in result.groups:
        if group.max_span > SPAN:
            raise LiquidityError("group span exceeds 0.20%")
        if hierarchy_tier(group.members) != group.tier:
            raise LiquidityError("tier mismatch")
        for member in group.members.values():
            if member.candidate_id in seen:
                raise LiquidityError(f"{member.candidate_id} has two parents")
            seen.add(member.candidate_id)
            if member.status != OPEN:
                raise LiquidityError("a closed candidate entered a hierarchy group")
            if member.side != group.side:
                raise LiquidityError("high and low were grouped")
    for rows in result.candidates.values():
        for candidate in rows:
            if candidate.status == OPEN and not candidate.group_id:
                raise LiquidityError(f"{candidate.candidate_id} is open and has no hierarchy group")


def status_counts(result: AnalysisResult) -> dict[str, dict[str, int]]:
    output: dict[str, dict[str, int]] = {}
    for timeframe, rows in result.candidates.items():
        output[timeframe] = {OPEN: 0, MITIGATED: 0, TERMINAL: 0, "HIGH_OPEN": 0, "LOW_OPEN": 0}
        for candidate in rows:
            output[timeframe][candidate.status] += 1
            if candidate.status == OPEN:
                output[timeframe][f"{candidate.side}_OPEN"] += 1
    return output


def mechanism_counts(result: AnalysisResult) -> dict[str, int]:
    counts = {"equality": 0, "wick": 0, "body_close": 0, "gap": 0, "close_based": 0, "equal_chains": 0}
    chains: set[str] = set()
    for rows in result.candidates.values():
        for candidate in rows:
            if candidate.exact_flag:
                counts["equality"] += 1
            if candidate.mechanism == "WICK_REACH":
                counts["wick"] += 1
            if candidate.mechanism in {"BODY_REACH", "CLOSE_AT_OR_BEYOND"}:
                counts["body_close"] += 1
            if candidate.mechanism == "GAP_OPEN_AT_OR_BEYOND":
                counts["gap"] += 1
            if candidate.close_status == "CLOSE_MITIGATED":
                counts["close_based"] += 1
            if candidate.chain_length > 1:
                chains.add(candidate.chain_id)
    counts["equal_chains"] = len(chains)
    return counts
