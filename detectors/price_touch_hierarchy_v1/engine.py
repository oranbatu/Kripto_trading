"""Deterministic completed-candle price-touch hierarchy.

One completed candle contributes at most one touch to one zone.
Intrabar path is never inferred. Timeframe touch counts are never added
together to pass a qualification threshold.
"""
from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

from detectors.price_touch_hierarchy_v1.touch_config import (
    CONFIG,
    CROSS_TF_TOLERANCE,
    DATASET_END,
    DURATION_MS,
    FINAL_HALF_WIDTH,
    GRID_ANCHOR,
    GRID_GROWTH,
    MAX_GROUP_SPAN,
    MIN_DISTINCT_DATES,
    MIN_EPISODES,
    MIN_TOUCHES,
    PEAK_RADIUS,
    SAME_TF_SEPARATION,
    SCORE_WEIGHTS,
    SMOOTHING,
    TIER_RANK,
    TIMEFRAME_WEIGHT,
    TOUCH_REJECTION,
)

D0 = Decimal("0")
D1 = Decimal("1")
D100 = Decimal("100")
LN_G = GRID_GROWTH.ln()
SQRT_G = GRID_GROWTH.sqrt()
ONE_MINUS = D1 - FINAL_HALF_WIDTH
ONE_PLUS = D1 + FINAL_HALF_WIDTH
TURKEY = ZoneInfo("Europe/Istanbul")


class HardTouchKeyError(RuntimeError):
    """Raised when a candle would touch the same zone more than once."""


class SourceValidationError(RuntimeError):
    """Raised when a timeframe inventory or OHLC invariant fails."""


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


@dataclass
class TouchEvent:
    timeframe: str
    level_id: str
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    zone_lower: Decimal
    zone_upper: Decimal
    touch_type: str
    open_inside: bool
    close_inside: bool
    high_inside: bool
    low_inside: bool
    cross_through: bool
    from_above: bool
    from_below: bool
    bullish: bool
    bearish: bool
    doji: bool
    episode_id: int
    episode_start: bool
    episode_end: bool
    contribution: int = 1


@dataclass
class LevelStats:
    timeframe: str
    grid_index: int
    center: Decimal
    zone_lower: Decimal
    zone_upper: Decimal
    raw_atomic_touches: int
    smoothed_density: Decimal
    touches: int = 0
    close_inside: int = 0
    body: int = 0
    wick: int = 0
    cross_through: int = 0
    bullish: int = 0
    bearish: int = 0
    doji: int = 0
    from_above: int = 0
    from_below: int = 0
    high_inside: int = 0
    low_inside: int = 0
    open_inside: int = 0
    episodes: int = 0
    longest_episode: int = 0
    median_episode_length: Decimal = D0
    dates: set[datetime.date] = field(default_factory=set)  # type: ignore[name-defined]
    weeks: set[tuple[int, int]] = field(default_factory=set)
    months: set[tuple[int, int]] = field(default_factory=set)
    years: set[int] = field(default_factory=set)
    first_touch: datetime | None = None
    last_touch: datetime | None = None
    first_close: datetime | None = None
    last_close: datetime | None = None
    gaps: list[Decimal] = field(default_factory=list)
    month_counts: dict[str, int] = field(default_factory=dict)
    year_counts: dict[int, int] = field(default_factory=dict)
    quarter_counts: dict[str, int] = field(default_factory=dict)
    events: list[TouchEvent] = field(default_factory=list)
    episode_lengths: list[int] = field(default_factory=list)
    first_eligible: datetime | None = None
    touches_at_eligibility: int = 0
    episodes_at_eligibility: int = 0
    dates_at_eligibility: int = 0
    candidate_id: str = ""
    level_id: str = ""
    suppressed_by: str = ""
    suppression_distance: Decimal | None = None
    suppressed_neighbor_count: int = 0
    group_id: str = ""
    rank: int = 0
    would_be_rank: int = 0
    percentiles: dict[str, Decimal] = field(default_factory=dict)

    @property
    def price(self) -> Decimal:
        return self.center

    @property
    def distinct_dates(self) -> int:
        return len(self.dates)

    @property
    def distinct_weeks(self) -> int:
        return len(self.weeks)

    @property
    def distinct_months(self) -> int:
        return len(self.months)

    @property
    def distinct_years(self) -> int:
        return len(self.years)

    @property
    def span_seconds(self) -> Decimal:
        if self.first_touch is None or self.last_touch is None:
            return D0
        return Decimal(str((self.last_touch - self.first_touch).total_seconds()))

    @property
    def engagement_ratio(self) -> Decimal:
        if self.touches <= 0:
            return D0
        return Decimal(self.close_inside + self.body) / Decimal(self.touches)


@dataclass
class MappingAttempt:
    attempt_id: str
    parent_timeframe: str
    parent_level_id: str
    parent_price: Decimal
    child_timeframe: str
    child_level_id: str
    child_price: Decimal
    distance_percent: Decimal
    zone_overlap: bool
    proposed_span_percent: Decimal
    tolerance_pass: bool
    span_pass: bool
    accepted: bool
    rejection_reason: str
    selected_parent: str
    tie_break: str


@dataclass
class HierarchyGroup:
    group_id: str
    members: dict[str, LevelStats]
    tier: str
    score: Decimal
    representative_price: Decimal
    consolidated_lower: Decimal
    consolidated_upper: Decimal
    max_child_distance: Decimal
    max_span: Decimal
    canonical_visits: int
    canonical_timeframe: str
    rank: int = 0
    persistence: str = ""
    first_touch: datetime | None = None
    last_touch: datetime | None = None
    first_eligible: datetime | None = None

    @property
    def timeframes(self) -> tuple[str, ...]:
        return tuple(tf for tf in ("1d", "4h", "1h") if tf in self.members)


@dataclass
class AnalysisResult:
    bars: dict[str, list[Bar]]
    grid_min_k: int
    grid_max_k: int
    atomic_bins: int
    price_min: Decimal
    price_max: Decimal
    levels: dict[str, list[LevelStats]]
    suppressed: list[LevelStats]
    rejected: list[LevelStats]
    groups: list[HierarchyGroup]
    attempts: list[MappingAttempt]
    unfinished_excluded: dict[str, int]
    duplicate_touch_keys: int
    warnings: list[str]
    peak_counts: dict[str, int]
    rejection_reason_counts: dict[str, int]


def iso_utc(dt: datetime) -> str:
    stamp = dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    return stamp.replace("+00:00", "Z")


def iso_turkey(dt: datetime) -> str:
    return dt.astimezone(TURKEY).isoformat(timespec="milliseconds")


def candle_touches_zone(low: Decimal, high: Decimal, lower: Decimal, upper: Decimal) -> bool:
    return high >= lower and low <= upper


def primary_touch_type(
    open_: Decimal, high: Decimal, low: Decimal, close: Decimal, lower: Decimal, upper: Decimal
) -> str:
    if lower <= close <= upper:
        return "CLOSE_INSIDE_ZONE"
    body_low = open_ if open_ <= close else close
    body_high = close if open_ <= close else open_
    if max(body_low, lower) <= min(body_high, upper):
        return "BODY_INTERSECTION"
    return "WICK_ONLY_INTERSECTION"


def interaction_flags(
    open_: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    lower: Decimal,
    upper: Decimal,
    previous_close: Decimal | None,
) -> dict[str, bool]:
    return {
        "open_inside": lower <= open_ <= upper,
        "close_inside": lower <= close <= upper,
        "high_inside": lower <= high <= upper,
        "low_inside": lower <= low <= upper,
        "cross_through": low <= lower and high >= upper,
        "from_above": previous_close is not None and previous_close > upper,
        "from_below": previous_close is not None and previous_close < lower,
        "bullish": close > open_,
        "bearish": close < open_,
        "doji": close == open_,
    }


def grid_center(k: int) -> Decimal:
    return GRID_ANCHOR * (GRID_GROWTH ** k)


def atomic_bounds(k: int) -> tuple[Decimal, Decimal]:
    previous = grid_center(k - 1)
    center = grid_center(k)
    nxt = grid_center(k + 1)
    return (previous * center).sqrt(), (center * nxt).sqrt()


def k_floor(price: Decimal) -> int:
    return int((price.ln() / LN_G).to_integral_value(rounding=ROUND_FLOOR))


def k_ceil(price: Decimal) -> int:
    return int((price.ln() / LN_G).to_integral_value(rounding=ROUND_CEILING))


def build_grid(min_low: Decimal, max_high: Decimal) -> tuple[int, int, list[Decimal], list[Decimal], list[Decimal]]:
    """Return k range and parallel center/lower/upper arrays.

    The anchor stays at 1 USDT. Extending the dataset changes only the covered
    index range, never the center that belongs to an existing index.
    """
    if min_low <= 0 or max_high <= 0:
        raise SourceValidationError("price grid requires positive prices")
    k_min = k_floor(min_low) - 2
    k_max = k_ceil(max_high) + 2
    span = [grid_center(k) for k in range(k_min - 1, k_max + 2)]
    centers = span[1:-1]
    lowers = [(span[index] * span[index + 1]).sqrt() for index in range(len(span) - 2)]
    uppers = [(span[index + 1] * span[index + 2]).sqrt() for index in range(len(span) - 2)]
    return k_min, k_max, centers, lowers, uppers


def relative_distance_percent(left: Decimal, right: Decimal) -> Decimal:
    if left <= 0 or right <= 0:
        raise SourceValidationError("relative distance requires positive prices")
    return (abs(left - right) / ((left + right) / 2)) * D100


def final_zone(center: Decimal) -> tuple[Decimal, Decimal]:
    return center * ONE_MINUS, center * ONE_PLUS


def smooth_density(raw: Sequence[int]) -> list[Decimal]:
    n = len(raw)
    left_w, mid_w, right_w = SMOOTHING
    out: list[Decimal] = []
    for index in range(n):
        left = Decimal(raw[index - 1]) if index > 0 else D0
        right = Decimal(raw[index + 1]) if index + 1 < n else D0
        out.append(left_w * left + mid_w * Decimal(raw[index]) + right_w * right)
    return out


def _peak_tie_key(raw: int, episodes: int, months: int, close_inside: int, first: datetime | None, k: int) -> tuple:
    first_ord = first.timestamp() if first is not None else 10**18
    return (-raw, -episodes, -months, -close_inside, first_ord, k)


def select_peak_indices(
    raw: Sequence[int],
    smoothed: Sequence[Decimal],
    episodes: Sequence[int],
    months: Sequence[int],
    close_inside: Sequence[int],
    first_touch: Sequence[datetime | None],
    k_min: int,
) -> list[int]:
    chosen: list[int] = []
    n = len(raw)
    for index in range(n):
        if raw[index] <= 0:
            continue
        lo = max(0, index - PEAK_RADIUS)
        hi = min(n, index + PEAK_RADIUS + 1)
        window_max = max(smoothed[lo:hi])
        if smoothed[index] < window_max:
            continue
        contenders = [
            other
            for other in range(lo, hi)
            if raw[other] > 0 and smoothed[other] == smoothed[index]
        ]
        winner = min(
            contenders,
            key=lambda other: _peak_tie_key(
                raw[other],
                episodes[other],
                months[other],
                close_inside[other],
                first_touch[other],
                k_min + other,
            ),
        )
        if winner == index:
            chosen.append(index)
    return chosen


def episode_marks(touched: Sequence[bool]) -> list[tuple[int, bool, bool]]:
    """Return (episode_id, is_start, is_end) for each touched position.

    Untouched positions are omitted. Consecutive touches share one episode.
    A new episode starts only when the immediately previous candle did not touch.
    """
    marks: list[tuple[int, bool, bool]] = []
    episode = 0
    previous = False
    for flag in touched:
        if not flag:
            if previous and marks:
                episode_id, is_start, _is_end = marks[-1]
                marks[-1] = (episode_id, is_start, True)
            previous = False
            continue
        if not previous:
            episode += 1
            marks.append((episode, True, False))
        else:
            marks.append((episode, False, False))
        previous = True
    if previous and marks:
        episode_id, is_start, _is_end = marks[-1]
        marks[-1] = (episode_id, is_start, True)
    return marks


def qualification_result(timeframe: str, touches: int, episodes: int, distinct_dates: int) -> tuple[bool, str, list[str]]:
    reasons: list[str] = []
    if touches < MIN_TOUCHES[timeframe]:
        reasons.append(TOUCH_REJECTION[timeframe])
    if episodes < MIN_EPISODES[timeframe]:
        reasons.append("INDEPENDENT_EPISODES_BELOW_3")
    if distinct_dates < MIN_DISTINCT_DATES[timeframe]:
        reasons.append("DISTINCT_UTC_DATES_BELOW_3")
    if not reasons:
        return True, "", []
    return False, reasons[0], reasons


def hierarchy_tier(timeframes: Iterable[str]) -> str:
    present = set(timeframes)
    has_d = "1d" in present
    has_4 = "4h" in present
    has_1 = "1h" in present
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
    raise ValueError("hierarchy tier requires at least one timeframe")


def representative_price_for(members: dict[str, LevelStats]) -> Decimal:
    if "1d" in members:
        return members["1d"].price
    if "4h" in members:
        return members["4h"].price
    return members["1h"].price


def canonical_timeframe_for(members: dict[str, LevelStats]) -> str:
    if "1h" in members:
        return "1h"
    if "4h" in members:
        return "4h"
    return "1d"


def average_rank_percentiles(values: Sequence[Decimal]) -> list[Decimal]:
    n = len(values)
    if n == 0:
        return []
    if n == 1:
        return [D1]
    order = sorted(range(n), key=lambda index: values[index])
    result = [D0] * n
    cursor = 0
    while cursor < n:
        end = cursor
        while end + 1 < n and values[order[end + 1]] == values[order[cursor]]:
            end += 1
        average_rank = Decimal(cursor + 1 + end + 1) / 2
        percentile = (average_rank - 1) / Decimal(n - 1)
        for slot in range(cursor, end + 1):
            result[order[slot]] = percentile
        cursor = end + 1
    return result


def weighted_mean(parts: Sequence[tuple[Decimal, Decimal]]) -> Decimal:
    weight = sum((item[0] for item in parts), D0)
    if weight == 0:
        return D0
    return sum((item[0] * item[1] for item in parts), D0) / weight


def hierarchy_score(members: dict[str, LevelStats]) -> Decimal:
    touch_parts = []
    episode_parts = []
    month_parts = []
    engage_parts = []
    for timeframe, level in members.items():
        weight = TIMEFRAME_WEIGHT[timeframe]
        touch_parts.append((weight, level.percentiles["touch"]))
        episode_parts.append((weight, level.percentiles["episode"]))
        month_parts.append((weight, level.percentiles["months"]))
        engage_parts.append((weight, level.engagement_ratio))
    return D100 * (
        SCORE_WEIGHTS["touch"] * weighted_mean(touch_parts)
        + SCORE_WEIGHTS["episode"] * weighted_mean(episode_parts)
        + SCORE_WEIGHTS["persistence"] * weighted_mean(month_parts)
        + SCORE_WEIGHTS["engagement"] * weighted_mean(engage_parts)
    )


def persistence_label(distinct_months: int, span_days: Decimal, most_active_share: Decimal) -> str:
    """Descriptive only. This label never overrides tier or touch rank.

    RECENT_OR_EMERGING: two or fewer active months, or lifespan under 60 days.
    FREQUENT_BUT_CONCENTRATED: otherwise, most-active month holds at least 45%.
    FREQUENT_AND_PERSISTENT: at least six months and most-active share at most 35%.
    INFREQUENT_BUT_PERSISTENT: every remaining qualified level.
    """
    if distinct_months <= 2 or span_days < 60:
        return "RECENT_OR_EMERGING"
    if most_active_share >= Decimal("0.45"):
        return "FREQUENT_BUT_CONCENTRATED"
    if distinct_months >= 6 and most_active_share <= Decimal("0.35"):
        return "FREQUENT_AND_PERSISTENT"
    return "INFREQUENT_BUT_PERSISTENT"


def register_touch_key(registry: set[tuple], key: tuple) -> None:
    if key in registry:
        raise HardTouchKeyError(f"duplicate touch key {key}")
    registry.add(key)


def _month_key(moment: datetime) -> str:
    stamp = moment.astimezone(timezone.utc)
    return f"{stamp.year:04d}-{stamp.month:02d}"


def _quarter_key(moment: datetime) -> str:
    stamp = moment.astimezone(timezone.utc)
    quarter = (stamp.month - 1) // 3 + 1
    return f"{stamp.year:04d}-Q{quarter}"


def _iso_week(moment: datetime) -> tuple[int, int]:
    stamp = moment.astimezone(timezone.utc)
    iso = stamp.isocalendar()
    return int(iso.year), int(iso.week)


def expected_close(open_time: datetime, timeframe: str) -> datetime:
    return open_time + timedelta(milliseconds=DURATION_MS[timeframe]) - timedelta(milliseconds=1)


def filter_completed(bars: Sequence[Bar], dataset_end: datetime) -> tuple[list[Bar], int]:
    kept: list[Bar] = []
    excluded = 0
    for bar in bars:
        if bar.close_time <= dataset_end:
            kept.append(bar)
        else:
            excluded += 1
    return kept, excluded


def _atomic_pass(
    bars: Sequence[Bar],
    k_min: int,
    k_max: int,
    lowers: Sequence[Decimal],
    uppers: Sequence[Decimal],
) -> tuple[list[int], list[int], list[int], list[int], list[datetime | None]]:
    width = k_max - k_min + 1
    raw = [0] * width
    episodes = [0] * width
    close_inside = [0] * width
    months_sets: list[set[tuple[int, int]]] = [set() for _ in range(width)]
    first: list[datetime | None] = [None] * width
    last_row = [-2] * width
    for bar in bars:
        start = max(k_min, k_ceil(bar.low / SQRT_G) - 1)
        stop = min(k_max, k_floor(bar.high * SQRT_G) + 1)
        for k in range(start, stop + 1):
            offset = k - k_min
            if not candle_touches_zone(bar.low, bar.high, lowers[offset], uppers[offset]):
                continue
            raw[offset] += 1
            if last_row[offset] != bar.row - 1:
                episodes[offset] += 1
            last_row[offset] = bar.row
            kind = primary_touch_type(bar.open, bar.high, bar.low, bar.close, lowers[offset], uppers[offset])
            if kind == "CLOSE_INSIDE_ZONE":
                close_inside[offset] += 1
            opened = bar.open_time.astimezone(timezone.utc)
            months_sets[offset].add((opened.year, opened.month))
            if first[offset] is None:
                first[offset] = bar.open_time
    month_counts = [len(item) for item in months_sets]
    return raw, episodes, close_inside, month_counts, first


def _consume_bar(level: LevelStats, bar: Bar, previous_close: Decimal | None, registry: set[tuple]) -> None:
    key = (bar.timeframe, bar.open_time, level.candidate_id)
    register_touch_key(registry, key)
    kind = primary_touch_type(bar.open, bar.high, bar.low, bar.close, level.zone_lower, level.zone_upper)
    flags = interaction_flags(bar.open, bar.high, bar.low, bar.close, level.zone_lower, level.zone_upper, previous_close)
    level.touches += 1
    if kind == "CLOSE_INSIDE_ZONE":
        level.close_inside += 1
    elif kind == "BODY_INTERSECTION":
        level.body += 1
    else:
        level.wick += 1
    level.cross_through += int(flags["cross_through"])
    level.bullish += int(flags["bullish"])
    level.bearish += int(flags["bearish"])
    level.doji += int(flags["doji"])
    level.from_above += int(flags["from_above"])
    level.from_below += int(flags["from_below"])
    level.high_inside += int(flags["high_inside"])
    level.low_inside += int(flags["low_inside"])
    level.open_inside += int(flags["open_inside"])
    opened = bar.open_time.astimezone(timezone.utc)
    level.dates.add(opened.date())
    level.weeks.add(_iso_week(bar.open_time))
    level.months.add((opened.year, opened.month))
    level.years.add(opened.year)
    month = _month_key(bar.open_time)
    level.month_counts[month] = level.month_counts.get(month, 0) + 1
    level.year_counts[opened.year] = level.year_counts.get(opened.year, 0) + 1
    quarter = _quarter_key(bar.open_time)
    level.quarter_counts[quarter] = level.quarter_counts.get(quarter, 0) + 1
    if level.first_touch is None:
        level.first_touch = bar.open_time
        level.first_close = bar.close_time
    elif level.last_touch is not None:
        gap_hours = Decimal(str((bar.open_time - level.last_touch).total_seconds())) / Decimal(3600)
        level.gaps.append(gap_hours)
    level.last_touch = bar.open_time
    level.last_close = bar.close_time
    event = TouchEvent(
        timeframe=bar.timeframe,
        level_id=level.candidate_id,
        open_time=bar.open_time,
        close_time=bar.close_time,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        zone_lower=level.zone_lower,
        zone_upper=level.zone_upper,
        touch_type=kind,
        open_inside=flags["open_inside"],
        close_inside=flags["close_inside"],
        high_inside=flags["high_inside"],
        low_inside=flags["low_inside"],
        cross_through=flags["cross_through"],
        from_above=flags["from_above"],
        from_below=flags["from_below"],
        bullish=flags["bullish"],
        bearish=flags["bearish"],
        doji=flags["doji"],
        episode_id=0,
        episode_start=False,
        episode_end=False,
    )
    level.events.append(event)


def _finalize_episodes(level: LevelStats, touched_rows: set[int], row_count: int) -> None:
    mask = [index in touched_rows for index in range(row_count)]
    marks = episode_marks(mask)
    if len(marks) != len(level.events):
        raise HardTouchKeyError("episode marks do not reconcile to touch events")
    lengths: dict[int, int] = defaultdict(int)
    for event, (episode_id, is_start, is_end) in zip(level.events, marks):
        event.episode_id = episode_id
        event.episode_start = is_start
        event.episode_end = is_end
        lengths[episode_id] += 1
    level.episodes = len(lengths)
    level.episode_lengths = [lengths[key] for key in sorted(lengths)]
    level.longest_episode = max(level.episode_lengths) if level.episode_lengths else 0
    ordered = sorted(level.episode_lengths)
    if ordered:
        mid = len(ordered) // 2
        if len(ordered) % 2:
            level.median_episode_length = Decimal(ordered[mid])
        else:
            level.median_episode_length = (Decimal(ordered[mid - 1]) + Decimal(ordered[mid])) / 2
    _mark_eligibility(level)


def _mark_eligibility(level: LevelStats) -> None:
    need_touches = MIN_TOUCHES[level.timeframe]
    need_episodes = MIN_EPISODES[level.timeframe]
    need_dates = MIN_DISTINCT_DATES[level.timeframe]
    seen_dates: set = set()
    episodes = 0
    for index, event in enumerate(level.events, start=1):
        if event.episode_start:
            episodes += 1
        seen_dates.add(event.open_time.astimezone(timezone.utc).date())
        if (
            level.first_eligible is None
            and index >= need_touches
            and episodes >= need_episodes
            and len(seen_dates) >= need_dates
        ):
            level.first_eligible = event.close_time
            level.touches_at_eligibility = index
            level.episodes_at_eligibility = episodes
            level.dates_at_eligibility = len(seen_dates)


def recount_peaks(bars: Sequence[Bar], levels: list[LevelStats]) -> None:
    if not levels or not bars:
        return
    ordered = sorted(range(len(levels)), key=lambda index: levels[index].zone_lower)
    lowers = [levels[index].zone_lower for index in ordered]
    touched: dict[int, set[int]] = {index: set() for index in range(len(levels))}
    registry: set[tuple] = set()
    previous_close: Decimal | None = None
    for bar in bars:
        cursor = bisect_right(lowers, bar.high) - 1
        while cursor >= 0 and levels[ordered[cursor]].zone_upper >= bar.low:
            level_index = ordered[cursor]
            level = levels[level_index]
            if candle_touches_zone(bar.low, bar.high, level.zone_lower, level.zone_upper):
                _consume_bar(level, bar, previous_close, registry)
                touched[level_index].add(bar.row)
            cursor -= 1
        previous_close = bar.close
    for index, level in enumerate(levels):
        _finalize_episodes(level, touched[index], len(bars))
        if level.close_inside + level.body + level.wick != level.touches:
            raise HardTouchKeyError("primary touch types do not reconcile")
        if sum(level.episode_lengths) != level.touches:
            raise HardTouchKeyError("episode lengths do not reconcile to touches")


def suppress_near_duplicates(levels: Sequence[LevelStats]) -> tuple[list[LevelStats], list[LevelStats]]:
    ordered = sorted(
        levels,
        key=lambda level: (
            -level.touches,
            -level.episodes,
            -level.distinct_months,
            -(level.close_inside + level.body),
            level.price,
            level.candidate_id,
        ),
    )
    accepted: list[LevelStats] = []
    suppressed: list[LevelStats] = []
    for level in ordered:
        winner = None
        distance = None
        for chosen in accepted:
            gap = relative_distance_percent(level.price, chosen.price)
            if gap < SAME_TF_SEPARATION:
                winner = chosen
                distance = gap
                break
        if winner is None:
            accepted.append(level)
            continue
        level.suppressed_by = winner.candidate_id
        level.suppression_distance = distance
        winner.suppressed_neighbor_count += 1
        suppressed.append(level)
    return accepted, suppressed


def _assign_percentiles(levels: list[LevelStats]) -> None:
    if not levels:
        return
    metrics = {
        "touch": [Decimal(level.touches) for level in levels],
        "episode": [Decimal(level.episodes) for level in levels],
        "months": [Decimal(level.distinct_months) for level in levels],
        "close_inside": [Decimal(level.close_inside) for level in levels],
        "engagement": [level.engagement_ratio for level in levels],
        "span": [level.span_seconds for level in levels],
    }
    ranked = {name: average_rank_percentiles(values) for name, values in metrics.items()}
    for index, level in enumerate(levels):
        level.percentiles = {name: ranked[name][index] for name in ranked}


def _rank_key(level: LevelStats) -> tuple:
    return (
        -level.touches,
        -level.episodes,
        -level.distinct_months,
        -level.distinct_years,
        -level.close_inside,
        -level.body,
        -level.span_seconds,
        level.price,
        level.level_id,
    )


def _zones_overlap(left: LevelStats, right: LevelStats) -> bool:
    return left.zone_lower <= right.zone_upper and right.zone_lower <= left.zone_upper


def _group_span(prices: Sequence[Decimal]) -> Decimal:
    if len(prices) <= 1:
        return D0
    return relative_distance_percent(min(prices), max(prices))


def _parent_sort_key(parent: LevelStats, distance: Decimal) -> tuple:
    timeframe_rank = {"1d": 0, "4h": 1, "1h": 2}[parent.timeframe]
    return (
        timeframe_rank,
        distance,
        -parent.percentiles.get("touch", D0),
        -parent.percentiles.get("episode", D0),
        parent.level_id,
    )


class _GroupBuilder:
    def __init__(self, anchor: LevelStats) -> None:
        self.anchor = anchor
        self.members: dict[str, LevelStats] = {anchor.timeframe: anchor}
        self.prices: list[Decimal] = [anchor.price]

    def span_with(self, child: LevelStats) -> Decimal:
        return _group_span(self.prices + [child.price])

    def add(self, child: LevelStats) -> None:
        self.members[child.timeframe] = child
        self.prices.append(child.price)


def attach_children(
    children: Sequence[LevelStats],
    parents: Sequence[_GroupBuilder],
    attempts: list[MappingAttempt],
) -> list[LevelStats]:
    unmatched: list[LevelStats] = []
    for child in sorted(children, key=lambda level: (level.price, level.level_id)):
        matches: list[tuple[_GroupBuilder, Decimal, bool]] = []
        for parent in parents:
            distance = relative_distance_percent(child.price, parent.anchor.price)
            overlap = _zones_overlap(child, parent.anchor)
            if overlap or distance <= CROSS_TF_TOLERANCE:
                matches.append((parent, distance, overlap))
        matches.sort(key=lambda item: _parent_sort_key(item[0].anchor, item[1]))
        eligible: list[tuple[_GroupBuilder, Decimal, bool, Decimal]] = []
        for parent, distance, overlap in matches:
            proposed = parent.span_with(child)
            span_ok = proposed <= MAX_GROUP_SPAN
            if not span_ok:
                attempts.append(
                    _attempt(parent.anchor, child, distance, overlap, proposed, True, False, "GROUP_SPAN_ABOVE_0_60", "", "REJECTED_SPAN")
                )
                continue
            eligible.append((parent, distance, overlap, proposed))
        if not eligible:
            unmatched.append(child)
            continue
        winner = eligible[0]
        attempts.append(
            _attempt(winner[0].anchor, child, winner[1], winner[2], winner[3], True, True, "", winner[0].anchor.level_id, "SELECTED")
        )
        for loser in eligible[1:]:
            attempts.append(
                _attempt(
                    loser[0].anchor,
                    child,
                    loser[1],
                    loser[2],
                    loser[3],
                    True,
                    False,
                    "LOST_TIE_BREAK",
                    winner[0].anchor.level_id,
                    "LOST_TIE_BREAK",
                )
            )
        winner[0].add(child)
    for index, attempt in enumerate(attempts, start=1):
        if not attempt.attempt_id:
            attempt.attempt_id = f"MA-{index:06d}"
    return unmatched


def _attempt(
    parent: LevelStats,
    child: LevelStats,
    distance: Decimal,
    overlap: bool,
    proposed: Decimal,
    tolerance_pass: bool,
    accepted: bool,
    reason: str,
    selected: str,
    tie: str,
) -> MappingAttempt:
    return MappingAttempt(
        attempt_id="",
        parent_timeframe=parent.timeframe,
        parent_level_id=parent.level_id,
        parent_price=parent.price,
        child_timeframe=child.timeframe,
        child_level_id=child.level_id,
        child_price=child.price,
        distance_percent=distance,
        zone_overlap=overlap,
        proposed_span_percent=proposed,
        tolerance_pass=tolerance_pass,
        span_pass=proposed <= MAX_GROUP_SPAN,
        accepted=accepted,
        rejection_reason=reason,
        selected_parent=selected,
        tie_break=tie,
    )


def build_hierarchy(levels: dict[str, list[LevelStats]]) -> tuple[list[HierarchyGroup], list[MappingAttempt]]:
    attempts: list[MappingAttempt] = []
    daily = [_GroupBuilder(level) for level in levels.get("1d", [])]
    unmatched_4h = attach_children(levels.get("4h", []), daily, attempts)
    unmatched_1h = attach_children(levels.get("1h", []), daily, attempts)
    four = [_GroupBuilder(level) for level in unmatched_4h]
    still_1h = attach_children(unmatched_1h, four, attempts)
    hourly = [_GroupBuilder(level) for level in still_1h]
    builders = daily + four + hourly
    groups: list[HierarchyGroup] = []
    for builder in builders:
        members = builder.members
        tier = hierarchy_tier(members)
        price = representative_price_for(members)
        lower = min(level.zone_lower for level in members.values())
        upper = max(level.zone_upper for level in members.values())
        child_distances = [
            relative_distance_percent(level.price, price)
            for timeframe, level in members.items()
            if level.price != price
        ]
        canonical_tf = canonical_timeframe_for(members)
        firsts = [level.first_touch for level in members.values() if level.first_touch]
        lasts = [level.last_touch for level in members.values() if level.last_touch]
        eligibles = [level.first_eligible for level in members.values() if level.first_eligible]
        groups.append(
            HierarchyGroup(
                group_id="",
                members=members,
                tier=tier,
                score=hierarchy_score(members),
                representative_price=price,
                consolidated_lower=lower,
                consolidated_upper=upper,
                max_child_distance=max(child_distances) if child_distances else D0,
                max_span=_group_span([level.price for level in members.values()]),
                canonical_visits=members[canonical_tf].episodes,
                canonical_timeframe=canonical_tf,
                first_touch=min(firsts) if firsts else None,
                last_touch=max(lasts) if lasts else None,
                first_eligible=max(eligibles) if eligibles else None,
            )
        )
    groups.sort(
        key=lambda group: (
            TIER_RANK[group.tier],
            -group.score,
            -group.canonical_visits,
            group.representative_price,
            group.members[canonical_timeframe_for(group.members)].level_id,
        )
    )
    for index, group in enumerate(groups, start=1):
        group.rank = index
        group.group_id = f"HG-{index:06d}"
        group.persistence = _group_persistence(group)
        for level in group.members.values():
            level.group_id = group.group_id
    for index, attempt in enumerate(attempts, start=1):
        attempt.attempt_id = f"MA-{index:06d}"
    return groups, attempts


def _group_persistence(group: HierarchyGroup) -> str:
    level = group.members[group.canonical_timeframe]
    share = D0
    if level.touches and level.month_counts:
        share = Decimal(max(level.month_counts.values())) / Decimal(level.touches)
    span_days = level.span_seconds / Decimal(86400)
    return persistence_label(level.distinct_months, span_days, share)


def _most_active_share(level: LevelStats) -> Decimal:
    if not level.touches or not level.month_counts:
        return D0
    return Decimal(max(level.month_counts.values())) / Decimal(level.touches)


def level_persistence(level: LevelStats) -> str:
    return persistence_label(level.distinct_months, level.span_seconds / Decimal(86400), _most_active_share(level))


def directional_balance(bullish: int, bearish: int) -> Decimal:
    return D1 - (Decimal(abs(bullish - bearish)) / Decimal(max(1, bullish + bearish)))


def analyze(bars_by_tf: dict[str, Sequence[Bar]], dataset_end: datetime = DATASET_END) -> AnalysisResult:
    completed: dict[str, list[Bar]] = {}
    excluded: dict[str, int] = {}
    for timeframe, bars in bars_by_tf.items():
        kept, dropped = filter_completed(bars, dataset_end)
        renumbered = []
        for index, bar in enumerate(kept):
            renumbered.append(
                Bar(
                    timeframe=timeframe,
                    row=index,
                    open_time=bar.open_time,
                    close_time=bar.close_time,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                )
            )
        completed[timeframe] = renumbered
        excluded[timeframe] = dropped
    lows = [bar.low for bars in completed.values() for bar in bars]
    highs = [bar.high for bars in completed.values() for bar in bars]
    if not lows:
        raise SourceValidationError("no completed candles were supplied")
    price_min = min(lows)
    price_max = max(highs)
    k_min, k_max, centers, lowers, uppers = build_grid(price_min, price_max)
    qualified: dict[str, list[LevelStats]] = {}
    suppressed_all: list[LevelStats] = []
    rejected_all: list[LevelStats] = []
    peak_counts: dict[str, int] = {}
    prefix = {"1h": "1H", "4h": "4H", "1d": "1D"}
    for timeframe in ("1d", "4h", "1h"):
        bars = completed.get(timeframe, [])
        raw, episodes, close_inside, month_counts, first = _atomic_pass(bars, k_min, k_max, lowers, uppers)
        smoothed = smooth_density(raw)
        peaks = select_peak_indices(raw, smoothed, episodes, month_counts, close_inside, first, k_min)
        peak_counts[timeframe] = len(peaks)
        candidates: list[LevelStats] = []
        for ordinal, offset in enumerate(sorted(peaks), start=1):
            k = k_min + offset
            lower, upper = final_zone(centers[offset])
            candidates.append(
                LevelStats(
                    timeframe=timeframe,
                    grid_index=k,
                    center=centers[offset],
                    zone_lower=lower,
                    zone_upper=upper,
                    raw_atomic_touches=raw[offset],
                    smoothed_density=smoothed[offset],
                    candidate_id=f"C{prefix[timeframe]}-{ordinal:06d}",
                )
            )
        recount_peaks(bars, candidates)
        kept, suppressed = suppress_near_duplicates(candidates)
        for level in suppressed:
            passed, _primary, reasons = qualification_result(
                timeframe, level.touches, level.episodes, level.distinct_dates
            )
            if "SUPPRESSED_AS_NEAR_DUPLICATE" not in reasons:
                reasons.append("SUPPRESSED_AS_NEAR_DUPLICATE")
            level.would_be_rank = 0
            suppressed_all.append(level)
            rejected_all.append(level)
            if passed:
                level.percentiles = {}
        threshold_pool = []
        for level in candidates:
            passed, _primary, _reasons = qualification_result(
                timeframe, level.touches, level.episodes, level.distinct_dates
            )
            if passed:
                threshold_pool.append(level)
        threshold_pool.sort(key=_rank_key)
        for rank, level in enumerate(threshold_pool, start=1):
            level.would_be_rank = rank
        accepted = [level for level in kept if qualification_result(timeframe, level.touches, level.episodes, level.distinct_dates)[0]]
        accepted.sort(key=lambda level: (level.price, level.grid_index))
        for ordinal, level in enumerate(accepted, start=1):
            level.level_id = f"L{prefix[timeframe]}-{ordinal:06d}"
            for event in level.events:
                event.level_id = level.level_id
        accepted.sort(key=_rank_key)
        for rank, level in enumerate(accepted, start=1):
            level.rank = rank
        _assign_percentiles(accepted)
        qualified[timeframe] = accepted
        accepted_ids = {id(level) for level in accepted}
        for level in kept:
            passed, _primary, _reasons = qualification_result(
                timeframe, level.touches, level.episodes, level.distinct_dates
            )
            if passed:
                continue
            level.would_be_rank = 0
            rejected_all.append(level)
        for level in candidates:
            if id(level) not in accepted_ids:
                level.events.clear()
    rejected_by_tf: dict[str, list[LevelStats]] = defaultdict(list)
    for level in rejected_all:
        rejected_by_tf[level.timeframe].append(level)
    for timeframe, rows in rejected_by_tf.items():
        rows.sort(key=_rank_key)
        for rank, level in enumerate(rows, start=1):
            if level.suppressed_by and level.would_be_rank:
                continue
            level.would_be_rank = rank
    groups, attempts = build_hierarchy(qualified)
    result = AnalysisResult(
        bars={tf: list(rows) for tf, rows in completed.items()},
        grid_min_k=k_min,
        grid_max_k=k_max,
        atomic_bins=k_max - k_min + 1,
        price_min=price_min,
        price_max=price_max,
        levels=qualified,
        suppressed=suppressed_all,
        rejected=rejected_all,
        groups=groups,
        attempts=attempts,
        unfinished_excluded=excluded,
        duplicate_touch_keys=0,
        warnings=[],
        peak_counts=peak_counts,
        rejection_reason_counts=_rejection_counts(rejected_all),
    )
    validate_analysis(result)
    return result


def rejection_reasons(level: LevelStats) -> tuple[str, list[str]]:
    passed, primary, reasons = qualification_result(
        level.timeframe, level.touches, level.episodes, level.distinct_dates
    )
    if level.suppressed_by:
        if passed:
            primary = "SUPPRESSED_AS_NEAR_DUPLICATE"
        if "SUPPRESSED_AS_NEAR_DUPLICATE" not in reasons:
            reasons.append("SUPPRESSED_AS_NEAR_DUPLICATE")
    return primary, reasons


def _rejection_counts(levels: Sequence[LevelStats]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for level in levels:
        primary, _reasons = rejection_reasons(level)
        if primary:
            counts[primary] += 1
    return dict(counts)


def validate_analysis(result: AnalysisResult) -> None:
    seen_children: set[str] = set()
    for group in result.groups:
        if group.max_span > MAX_GROUP_SPAN:
            raise HardTouchKeyError(f"{group.group_id} span {group.max_span} exceeds 0.60")
        if TIER_RANK[group.tier] != TIER_RANK[hierarchy_tier(group.timeframes)]:
            raise HardTouchKeyError("hierarchy tier does not match members")
        canonical = group.members[group.canonical_timeframe].episodes
        if group.canonical_visits != canonical:
            raise HardTouchKeyError("canonical visits are not the finest timeframe episode count")
        summed = sum(level.episodes for level in group.members.values())
        if len(group.members) > 1 and group.canonical_visits == summed and all(
            level.episodes > 0 for level in group.members.values()
        ):
            # Equal sums can happen. The stored canonical value must still be the finest series,
            # which was checked above. Do not replace it with the sum.
            pass
        for timeframe, level in group.members.items():
            if level.level_id in seen_children:
                raise HardTouchKeyError(f"{level.level_id} belongs to more than one group")
            seen_children.add(level.level_id)
            passed, _primary, _reasons = qualification_result(
                timeframe, level.touches, level.episodes, level.distinct_dates
            )
            if not passed:
                raise HardTouchKeyError(f"{level.level_id} failed qualification")
            if level.touches < MIN_TOUCHES[timeframe]:
                raise HardTouchKeyError("qualified level is below the touch threshold")
    for index in range(1, len(result.groups)):
        prev = result.groups[index - 1]
        cur = result.groups[index]
        if TIER_RANK[prev.tier] > TIER_RANK[cur.tier]:
            raise HardTouchKeyError("tier order is not ahead of score")
        if prev.tier == cur.tier and prev.score < cur.score:
            raise HardTouchKeyError("within-tier score order is wrong")
    for timeframe, levels in result.levels.items():
        for level in levels:
            if level.close_inside + level.body + level.wick != level.touches:
                raise HardTouchKeyError("touch type reconciliation failed")
            if sum(level.month_counts.values()) != level.touches:
                raise HardTouchKeyError("monthly touches do not reconcile")
            keys = [(event.timeframe, event.open_time, event.level_id) for event in level.events]
            if len(keys) != len(set(keys)):
                raise HardTouchKeyError("duplicate touch key inside a level")
            if any(event.contribution != 1 for event in level.events):
                raise HardTouchKeyError("touch contribution is not 1")
        ranks = [level.rank for level in levels]
        if ranks != list(range(1, len(levels) + 1)):
            ordered = sorted(levels, key=lambda level: level.rank)
            if [level.rank for level in ordered] != list(range(1, len(levels) + 1)):
                raise HardTouchKeyError("timeframe ranks are not contiguous")


def gap_median(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return D0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def gap_mean(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return D0
    return sum(values, D0) / Decimal(len(values))


def most_persistent_group(groups: Sequence[HierarchyGroup]) -> HierarchyGroup | None:
    if not groups:
        return None
    label_rank = {
        "FREQUENT_AND_PERSISTENT": 0,
        "INFREQUENT_BUT_PERSISTENT": 1,
        "FREQUENT_BUT_CONCENTRATED": 2,
        "RECENT_OR_EMERGING": 3,
    }

    def key(group: HierarchyGroup) -> tuple:
        level = group.members[group.canonical_timeframe]
        return (
            label_rank.get(group.persistence, 9),
            -level.distinct_months,
            -level.span_seconds,
            -group.canonical_visits,
            group.representative_price,
        )

    return min(groups, key=key)


def highest_canonical_group(groups: Sequence[HierarchyGroup]) -> HierarchyGroup | None:
    if not groups:
        return None
    return min(groups, key=lambda group: (-group.canonical_visits, group.rank))
