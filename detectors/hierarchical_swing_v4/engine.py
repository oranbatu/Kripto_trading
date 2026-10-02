"""Causal asymmetric-formation hierarchical swing engine.

RAW_FORMATION and RAW_PIVOT are derived from five-to-ten-candle structures.
INTERNAL_SWING and MAJOR_SWING are separate alternating state machines.
The major engine never mutates internal confirmation results.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext
from statistics import pstdev
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from detectors.hierarchical_swing_v4.swing_config import CONFIG, EXPECTED_MONTHLY_ROWS, MONTHS

getcontext().prec = 50

UTC = timezone.utc
ISTANBUL = ZoneInfo("Europe/Istanbul")
D0 = Decimal("0")
D1 = Decimal("1")
D100 = Decimal("100")
HOUR = timedelta(hours=1)
MILLISECOND = timedelta(milliseconds=1)


class HardInternalStateMachineError(RuntimeError):
    """Raised when an internal candidate stays active after a valid reversal."""


class HardMajorStateMachineError(RuntimeError):
    """Raised when a major candidate stays active after a valid reversal."""


def iso_utc(dt: datetime | None) -> str:
    if dt is None:
        return ""
    value = dt.astimezone(UTC)
    millis = value.microsecond // 1000
    if millis:
        return value.strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}Z"
    return value.strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def iso_turkey(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone(ISTANBUL).isoformat(timespec="milliseconds")


def month_key(dt: datetime) -> str:
    value = dt.astimezone(UTC)
    return f"{value.year:04d}-{value.month:02d}"


def close_time_of(open_time: datetime) -> datetime:
    return open_time.astimezone(UTC) + HOUR - MILLISECOND


def percent(numerator: Decimal, denominator: Decimal) -> Decimal:
    return (numerator / denominator) * D100


def clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    if value < low:
        return low
    if value > high:
        return high
    return value


def allowed_central_positions(total_bars: int) -> tuple[int, ...]:
    """One-based central-three positions. Even lengths use the lower center."""
    if total_bars % 2 == 1:
        center = (total_bars + 1) // 2
    else:
        center = total_bars // 2
    return (center - 1, center, center + 1)


def centrality_distance(total_bars: int, position: int) -> Decimal:
    if total_bars % 2 == 1:
        center = Decimal(total_bars + 1) / Decimal(2)
    else:
        center = (Decimal(total_bars) / Decimal(2)) + Decimal("0.5")
    return abs(Decimal(position) - center)


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
    taker_buy_base: Decimal = D0
    taker_buy_quote: Decimal = D0
    tr: Decimal | None = None
    atr: Decimal | None = None

    @property
    def typical(self) -> Decimal:
        return (self.high + self.low + self.close) / Decimal(3)


def compute_atr(bars: Sequence[Bar], period: int = CONFIG.atr_period) -> None:
    """Causal Wilder ATR. The seed is the SMA of the first `period` true ranges."""
    if period < 1:
        raise ValueError("ATR period must be positive")
    prev_close: Decimal | None = None
    trs: list[Decimal] = []
    atr: Decimal | None = None
    for index, bar in enumerate(bars):
        if prev_close is None:
            true_range = bar.high - bar.low
        else:
            true_range = max(
                bar.high - bar.low,
                abs(bar.high - prev_close),
                abs(bar.low - prev_close),
            )
        bar.tr = true_range
        trs.append(true_range)
        if index == period - 1:
            atr = sum(trs[:period]) / Decimal(period)
        elif index >= period and atr is not None:
            atr = ((atr * Decimal(period - 1)) + true_range) / Decimal(period)
        bar.atr = atr if index >= period - 1 else None
        prev_close = bar.close


def make_bars(
    rows: Sequence[Sequence[Any]],
    *,
    start: datetime | None = None,
    recompute_atr: bool = True,
) -> list[Bar]:
    """Build UTC hourly bars from OHLC tuples. Used by tests and synthetic cases."""
    origin = start or datetime(2026, 1, 1, tzinfo=UTC)
    bars: list[Bar] = []
    for index, row in enumerate(rows):
        open_time = origin + timedelta(hours=index)
        volume = Decimal(row[4]) if len(row) > 4 else Decimal("1")
        quote = Decimal(row[5]) if len(row) > 5 else volume
        trades = int(row[6]) if len(row) > 6 else 1
        taker_base = Decimal(row[7]) if len(row) > 7 else volume / Decimal(2)
        taker_quote = Decimal(row[8]) if len(row) > 8 else quote / Decimal(2)
        bars.append(
            Bar(
                row=index,
                open_time=open_time,
                close_time=close_time_of(open_time),
                open=Decimal(row[0]),
                high=Decimal(row[1]),
                low=Decimal(row[2]),
                close=Decimal(row[3]),
                volume=volume,
                quote_volume=quote,
                trades=trades,
                taker_buy_base=taker_base,
                taker_buy_quote=taker_quote,
            )
        )
    if recompute_atr:
        compute_atr(bars)
    return bars


def _ohlc(bar: Bar) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    return (bar.open, bar.high, bar.low, bar.close)


def _rep_rank(formation: dict[str, Any]) -> tuple:
    return (
        formation["close_row"],
        -formation["outbound_percent"],
        -formation["return_percent_reference_basis"],
        formation["centrality_distance"],
        formation["confirmation_delay"],
        formation["total_bars"],
        formation["open_row"],
        formation["formation_id"],
    )


def _base_formation(
    *,
    formation_id: str,
    formation_type: str,
    open_row: int | None,
    close_row: int | None,
    disposition: str,
    specific: str,
) -> dict[str, Any]:
    return {
        "formation_id": formation_id,
        "formation_type": formation_type,
        "open_row": open_row,
        "extreme_row": None,
        "close_row": close_row,
        "total_bars": None,
        "interior_bars": None,
        "left_bars": None,
        "right_bars": None,
        "asymmetry_difference": None,
        "asymmetry_ratio": None,
        "symmetric": None,
        "extreme_position": None,
        "allowed_positions": "",
        "central_pass": False,
        "reference_price": None,
        "extreme_price": None,
        "outbound_price": None,
        "outbound_percent": D0,
        "return_price": None,
        "return_percent": D0,
        "return_percent_reference_basis": D0,
        "touch_price": None,
        "revisit_pass": False,
        "plateau_start_row": None,
        "plateau_end_row": None,
        "plateau_length": 0,
        "centrality_distance": Decimal("999"),
        "confirmation_delay": 10**9,
        "primary_disposition": disposition,
        "specific_reason": specific,
        "qualification_status": "REJECTED",
        "structural_pass": False,
        "emits_pivot": False,
        "representative_formation_id": formation_id,
        "duplicate_count": 0,
        "duplicate_ids": [],
        "dedup_reason": "",
        "edge": False,
        "detector_version": CONFIG.detector_version,
        "pivot_span_mode": CONFIG.pivot_span_mode,
    }


def classify_formation(bars: Sequence[Bar], start: int, end: int, kind: str) -> dict[str, Any]:
    """Classify one window. `kind` is HIGH or LOW. Length failures are explicit."""
    kind = kind.upper()
    if kind not in ("HIGH", "LOW"):
        raise ValueError(f"formation kind must be HIGH or LOW, got {kind!r}")
    total = end - start + 1
    formation_id = f"PENDING-{start}-{end}-{kind}"
    prefix = "HIGH" if kind == "HIGH" else "LOW"
    record = _base_formation(
        formation_id=formation_id,
        formation_type=kind,
        open_row=start,
        close_row=end,
        disposition="",
        specific="",
    )
    record["total_bars"] = total
    record["interior_bars"] = total - 2

    def reject(disposition: str, specific: str) -> dict[str, Any]:
        record["primary_disposition"] = disposition
        record["specific_reason"] = specific
        record["qualification_status"] = "REJECTED"
        record["structural_pass"] = False
        record["emits_pivot"] = False
        return record

    if total < CONFIG.minimum_total_formation_candles:
        return reject("FORMATION_TOTAL_BARS_BELOW_5", "FORMATION_TOTAL_BARS_BELOW_5")
    if total > CONFIG.maximum_total_formation_candles:
        return reject("FORMATION_TOTAL_BARS_ABOVE_10", "FORMATION_TOTAL_BARS_ABOVE_10")
    interior = total - 2
    if interior < CONFIG.minimum_interior_candles:
        return reject("FORMATION_INTERIOR_BARS_BELOW_3", "FORMATION_INTERIOR_BARS_BELOW_3")
    if interior > CONFIG.maximum_interior_candles:
        return reject("FORMATION_INTERIOR_BARS_ABOVE_8", "FORMATION_INTERIOR_BARS_ABOVE_8")
    if start < 0 or end >= len(bars) or start > end:
        return reject("OTHER_EXPLICIT_REASON", "FORMATION_WINDOW_OUTSIDE_DATASET")

    window = bars[start : end + 1]
    allowed = allowed_central_positions(total)
    record["allowed_positions"] = ",".join(str(item) for item in allowed)
    reference = window[0].close
    record["reference_price"] = reference
    if reference <= 0:
        return reject("OTHER_EXPLICIT_REASON", f"{prefix}_NON_POSITIVE_REFERENCE")

    if kind == "HIGH":
        prices = [bar.high for bar in window]
    else:
        prices = [bar.low for bar in window]
    extreme_price = max(prices) if kind == "HIGH" else min(prices)
    members = [start + offset for offset, price in enumerate(prices) if price == extreme_price]
    contiguous = all(members[i + 1] == members[i] + 1 for i in range(len(members) - 1))
    record["extreme_price"] = extreme_price
    record["plateau_start_row"] = members[0]
    record["plateau_end_row"] = members[-1]
    record["plateau_length"] = len(members)
    if not contiguous:
        record["central_pass"] = False
        return reject("NOT_TRUE_FORMATION_EXTREME", f"{prefix}_EQUAL_PLATEAU_NOT_REPRESENTATIVE")

    def position_of(row: int) -> int:
        return row - start + 1

    in_central = [row for row in members if position_of(row) in allowed]
    if not in_central:
        record["central_pass"] = False
        record["extreme_position"] = position_of(members[-1])
        if len(members) > 1:
            return reject("PLATEAU_REPRESENTATIVE_FAILURE", f"{prefix}_EQUAL_PLATEAU_NOT_REPRESENTATIVE")
        return reject("EXTREME_OUTSIDE_CENTRAL_THREE", f"{prefix}_EXTREME_OUTSIDE_CENTRAL_THREE")

    last_member = members[-1]
    if position_of(last_member) in allowed:
        extreme_row = last_member
    else:
        extreme_row = max(in_central)
    extreme_position = position_of(extreme_row)
    record["extreme_row"] = extreme_row
    record["extreme_position"] = extreme_position
    record["central_pass"] = True
    record["left_bars"] = extreme_row - start
    record["right_bars"] = end - extreme_row
    record["asymmetry_difference"] = abs(record["left_bars"] - record["right_bars"])
    record["asymmetry_ratio"] = Decimal(record["left_bars"]) / Decimal(record["right_bars"])
    record["symmetric"] = record["left_bars"] == record["right_bars"]
    record["centrality_distance"] = centrality_distance(total, extreme_position)
    record["confirmation_delay"] = end - extreme_row

    chosen_price = bars[extreme_row].high if kind == "HIGH" else bars[extreme_row].low
    if chosen_price != extreme_price:
        return reject("NOT_TRUE_FORMATION_EXTREME", f"{prefix}_EXTREME_NOT_FORMATION_{'MAXIMUM' if kind == 'HIGH' else 'MINIMUM'}")

    if kind == "HIGH":
        outbound_price = extreme_price - reference
    else:
        outbound_price = reference - extreme_price
    record["outbound_price"] = outbound_price
    record["outbound_percent"] = percent(outbound_price, reference)
    if record["outbound_percent"] < CONFIG.minimum_outbound_percent:
        return reject("OUTBOUND_BELOW_1_PERCENT", f"{prefix}_OUTBOUND_BELOW_1_PERCENT")

    close_bar = bars[end]
    revisit = close_bar.low <= reference <= close_bar.high
    record["revisit_pass"] = revisit
    if not revisit:
        return reject(
            "CLOSE_CANDLE_DID_NOT_REVISIT_REFERENCE",
            f"{prefix}_CLOSE_CANDLE_DID_NOT_REVISIT_OPEN_REFERENCE",
        )

    # The close candle's range contains the swing-open close, so the deterministic
    # reference-touch price is that swing-open close itself.
    touch = reference
    record["touch_price"] = touch
    if kind == "HIGH":
        return_price = extreme_price - touch
        record["return_percent"] = percent(return_price, extreme_price)
    else:
        return_price = touch - extreme_price
        record["return_percent"] = percent(return_price, extreme_price)
    record["return_price"] = return_price
    record["return_percent_reference_basis"] = percent(return_price, reference)
    # The qualifying return is measured against the swing-open close ("the relevant
    # reference basis"). Exact 1.00% outbound therefore passes when the close
    # candle revisits that reference. The extreme-basis figure is retained.
    if record["return_percent_reference_basis"] < CONFIG.minimum_return_percent:
        return reject("OTHER_EXPLICIT_REASON", f"{prefix}_RETURN_BELOW_1_PERCENT")

    record["primary_disposition"] = "QUALIFIED_RAW_FORMATION"
    record["specific_reason"] = "QUALIFIED_RAW_FORMATION"
    record["qualification_status"] = "QUALIFIED"
    record["structural_pass"] = True
    record["emits_pivot"] = True
    return record


def _mark_duplicate(formation: dict[str, Any], representative: dict[str, Any], reason: str) -> None:
    formation["primary_disposition"] = "DUPLICATE_OF_REPRESENTATIVE_FORMATION"
    formation["specific_reason"] = reason
    formation["qualification_status"] = "DUPLICATE"
    formation["emits_pivot"] = False
    formation["representative_formation_id"] = representative["formation_id"]
    formation["dedup_reason"] = reason
    representative["duplicate_count"] = int(representative.get("duplicate_count", 0)) + 1
    representative.setdefault("duplicate_ids", []).append(formation["formation_id"])


def _plateau_connected(
    bars: Sequence[Bar],
    left: int,
    right: int,
    price: Decimal,
    kind: str,
    limit_row: int,
) -> bool:
    lo, hi = (left, right) if left <= right else (right, left)
    if hi > limit_row or lo < 0:
        return False
    for row in range(lo, hi + 1):
        observed = bars[row].high if kind == "HIGH" else bars[row].low
        if observed != price:
            return False
    return True


def enumerate_formations(bars: Sequence[Bar]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Enumerate every in-range 5–10 candle window and both edge-censored windows."""
    formations: list[dict[str, Any]] = []
    n = len(bars)
    sequence = 0
    checks = 0
    for end in range(n):
        for length in range(CONFIG.minimum_total_formation_candles, CONFIG.maximum_total_formation_candles + 1):
            start = end - (length - 1)
            if start < 0:
                continue
            for kind in ("HIGH", "LOW"):
                checks += 1
                record = classify_formation(bars, start, end, kind)
                sequence += 1
                record["formation_id"] = f"FM-{sequence:06d}"
                record["representative_formation_id"] = record["formation_id"]
                formations.append(record)

    left_seq = 0
    for length in range(CONFIG.minimum_total_formation_candles, CONFIG.maximum_total_formation_candles + 1):
        for start in range(-(length - 1), 0):
            end = start + length - 1
            if end < 0 or end >= n:
                continue
            left_seq += 1
            record = _base_formation(
                formation_id=f"FM-L-{left_seq:04d}",
                formation_type="CENSORED",
                open_row=start,
                close_row=end,
                disposition="LEFT_EDGE_CENSORED",
                specific="LEFT_EDGE_CENSORED",
            )
            record["edge"] = True
            record["total_bars"] = length
            record["interior_bars"] = length - 2
            record["representative_formation_id"] = record["formation_id"]
            formations.append(record)

    right_seq = 0
    for length in range(CONFIG.minimum_total_formation_candles, CONFIG.maximum_total_formation_candles + 1):
        for extra in range(1, length):
            end = n - 1 + extra
            start = end - (length - 1)
            if start < 0 or start >= n:
                continue
            right_seq += 1
            record = _base_formation(
                formation_id=f"FM-R-{right_seq:04d}",
                formation_type="CENSORED",
                open_row=start,
                close_row=end,
                disposition="RIGHT_EDGE_CENSORED",
                specific="RIGHT_EDGE_CENSORED",
            )
            record["edge"] = True
            record["total_bars"] = length
            record["interior_bars"] = length - 2
            record["representative_formation_id"] = record["formation_id"]
            formations.append(record)

    stats = {
        "formation_checks": checks,
        "left_edge_censored": left_seq,
        "right_edge_censored": right_seq,
        "enumerated": len(formations),
    }
    return formations, stats


def assign_causal_dispositions(
    bars: Sequence[Bar], formations: Sequence[dict[str, Any]]
) -> dict[int, list[str]]:
    """Deduplicate qualified formations in confirmation order and build the release plan."""
    by_close: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for formation in formations:
        if formation.get("edge"):
            continue
        by_close[int(formation["close_row"])].append(formation)

    emitted: list[dict[str, Any]] = []
    plan: dict[int, list[str]] = defaultdict(list)
    by_id = {formation["formation_id"]: formation for formation in formations}
    for row in range(len(bars)):
        batch = [item for item in by_close.get(row, []) if item["structural_pass"]]
        groups: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
        for formation in batch:
            key = (
                formation["formation_type"],
                formation["extreme_row"],
                formation["extreme_price"],
                formation["close_row"],
            )
            groups[key].append(formation)
        representatives: list[dict[str, Any]] = []
        for members in groups.values():
            members.sort(key=_rep_rank)
            best = members[0]
            for other in members[1:]:
                _mark_duplicate(other, best, "SAME_TYPE_EXTREME_PRICE_AND_CONFIRMATION")
            representatives.append(best)
        representatives.sort(key=_rep_rank)
        accepted: list[dict[str, Any]] = []
        for formation in representatives:
            prior = None
            for existing in emitted:
                if _plateau_connected(
                    bars,
                    int(formation["extreme_row"]),
                    int(existing["extreme_row"]),
                    formation["extreme_price"],
                    formation["formation_type"],
                    row,
                ):
                    prior = existing
                    break
            if prior is None:
                for existing in accepted:
                    if _plateau_connected(
                        bars,
                        int(formation["extreme_row"]),
                        int(existing["extreme_row"]),
                        formation["extreme_price"],
                        formation["formation_type"],
                        row,
                    ):
                        prior = existing
                        break
            if prior is not None:
                children = list(formation.get("duplicate_ids") or [])
                _mark_duplicate(formation, prior, "SAME_CONTINUOUS_PLATEAU_EVENT")
                for child_id in children:
                    child = by_id[child_id]
                    child["representative_formation_id"] = prior["formation_id"]
                    child["dedup_reason"] = "SAME_CONTINUOUS_PLATEAU_EVENT"
                    prior["duplicate_count"] = int(prior.get("duplicate_count", 0)) + 1
                    prior.setdefault("duplicate_ids", []).append(child_id)
                formation["duplicate_ids"] = []
                formation["duplicate_count"] = 0
                continue
            formation["primary_disposition"] = "QUALIFIED_RAW_FORMATION"
            formation["specific_reason"] = "QUALIFIED_RAW_FORMATION"
            formation["qualification_status"] = "QUALIFIED"
            formation["emits_pivot"] = True
            formation["representative_formation_id"] = formation["formation_id"]
            accepted.append(formation)
            emitted.append(formation)
        accepted.sort(key=lambda item: (item["extreme_row"], item["formation_type"], item["formation_id"]))
        plan[row] = [item["formation_id"] for item in accepted]
    return plan


def prominence(
    bars: Sequence[Bar],
    *,
    kind: str,
    extreme_row: int,
    price: Decimal,
    eval_row: int,
) -> dict[str, Any]:
    """Topographic two-sided prominence using only bars through `eval_row`.

    A high walks outward until a strictly higher high and measures down to the
    minimum low. A low walks outward until a strictly lower low and measures up
    to the maximum high. A side with no walked bars is zero and censored.
    """
    left_extreme = False
    right_extreme = False
    if kind == "HIGH":
        left_ref: Decimal | None = None
        row = extreme_row - 1
        while row >= 0:
            if bars[row].high > price:
                break
            if left_ref is None or bars[row].low < left_ref:
                left_ref = bars[row].low
            row -= 1
        if extreme_row == 0 or left_ref is None:
            left_extreme = True
        left_value = (price - left_ref) if left_ref is not None else D0
        right_ref: Decimal | None = None
        row = extreme_row + 1
        while row <= eval_row:
            if bars[row].high > price:
                break
            if right_ref is None or bars[row].low < right_ref:
                right_ref = bars[row].low
            row += 1
        if extreme_row >= eval_row or right_ref is None:
            right_extreme = True
        right_value = (price - right_ref) if right_ref is not None else D0
    else:
        left_ref = None
        row = extreme_row - 1
        while row >= 0:
            if bars[row].low < price:
                break
            if left_ref is None or bars[row].high > left_ref:
                left_ref = bars[row].high
            row -= 1
        if extreme_row == 0 or left_ref is None:
            left_extreme = True
        left_value = (left_ref - price) if left_ref is not None else D0
        right_ref = None
        row = extreme_row + 1
        while row <= eval_row:
            if bars[row].low < price:
                break
            if right_ref is None or bars[row].high > right_ref:
                right_ref = bars[row].high
            row += 1
        if extreme_row >= eval_row or right_ref is None:
            right_extreme = True
        right_value = (right_ref - price) if right_ref is not None else D0
    two_sided = left_value if left_value <= right_value else right_value
    larger = left_value if left_value >= right_value else right_value
    return {
        "left_prominence": left_value,
        "right_prominence": right_value,
        "two_sided_prominence": two_sided,
        "max_two_sided_prominence": larger,
        "percentage_prominence": percent(two_sided, price) if price > 0 else D0,
        "left_censored": left_extreme,
        "right_censored": right_extreme,
    }


def _atr_for_extreme(bars: Sequence[Bar], extreme_row: int, confirm_row: int) -> Decimal | None:
    extreme_atr = bars[extreme_row].atr
    if extreme_atr is not None and extreme_atr > 0:
        return extreme_atr
    confirm_atr = bars[confirm_row].atr
    if confirm_atr is not None and confirm_atr > 0:
        return confirm_atr
    return None


class Record:
    """Mutable analysis record. Fields are added by the engine that owns them."""

    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


def make_pivot(formation: dict[str, Any], bars: Sequence[Bar], sequence: int) -> Record:
    extreme_row = int(formation["extreme_row"])
    confirm_row = int(formation["close_row"])
    extreme = bars[extreme_row]
    confirm = bars[confirm_row]
    opener = bars[int(formation["open_row"])]
    kind = formation["formation_type"]
    price = extreme.high if kind == "HIGH" else extreme.low
    prom = prominence(
        bars,
        kind=kind,
        extreme_row=extreme_row,
        price=price,
        eval_row=confirm_row,
    )
    atr = _atr_for_extreme(bars, extreme_row, confirm_row)
    prom_atr = (prom["two_sided_prominence"] / atr) if atr else None
    return Record(
        pivot_id=f"RP-{sequence:06d}",
        formation_id=formation["formation_id"],
        type=kind,
        extreme_row=extreme_row,
        confirm_row=confirm_row,
        open_row=int(formation["open_row"]),
        price=price,
        atr=atr,
        atr_at_extreme=extreme.atr,
        atr_at_open=opener.atr,
        atr_at_close=confirm.atr,
        atr_at_confirmation=confirm.atr,
        open_time=extreme.open_time,
        confirm_time=confirm.close_time,
        formation_open_time=opener.open_time,
        formation_close_time=confirm.close_time,
        outbound_percent=formation["outbound_percent"],
        return_percent=formation["return_percent"],
        return_percent_reference_basis=formation["return_percent_reference_basis"],
        left_bars=formation["left_bars"],
        right_bars=formation["right_bars"],
        symmetric=formation["symmetric"],
        total_bars=formation["total_bars"],
        prominence=prom,
        prominence_atr_at_confirmation=prom_atr,
        internal_disposition="",
        internal_role="",
        candidate_id="",
    )


def _directional_delta(kind: str, price: Decimal, previous_price: Decimal) -> Decimal:
    if kind == "HIGH":
        return price - previous_price
    return previous_price - price


class InternalEngine:
    def __init__(self) -> None:
        self.seen: list[Record] = []
        self.bootstrap_done = False
        self.seed: Record | None = None
        self.previous_opposite: Record | None = None
        self.search_direction: str | None = None
        self.active: Record | None = None
        self.held: list[Record] = []
        self.confirmed: list[Record] = []
        self.candidates: list[Record] = []
        self.transitions: list[dict[str, Any]] = []
        self.rejections: list[Record] = []
        self.replacements = 0
        self.reversal_evaluations = 0
        self.active_reversal_checks = 0
        self.age_updates = 0
        self.ignored_valid = 0
        self.state_errors = 0
        self.candidate_seq = 0
        self.confirmed_this_bar: list[Record] = []
        self.month_end_state: dict[str, str] = {}
        self.max_age_by_month: dict[str, int] = {}
        self.exceeded = {168: 0, 720: 0, 2160: 0}
        self.max_age_seen = 0
        self.bootstrap_record: dict[str, Any] | None = None
        self._transition_seq = 0

    def consume_new_pivots(self, pivots: Sequence[Record], bar: Bar) -> None:
        ordered = sorted(pivots, key=lambda item: (item.extreme_row, item.type, item.pivot_id))
        for pivot in ordered:
            self._on_pivot(pivot, bar)

    def evaluate(self, bar: Bar) -> None:
        self.reversal_evaluations += 1
        self.confirmed_this_bar = []
        safety = 0
        while safety < 64:
            safety += 1
            if self.active is None:
                if not self._flush_held(bar):
                    break
            if self.active is None:
                break
            confirmed = self._evaluate_active(bar)
            if not confirmed:
                break
        else:
            self.state_errors += 1
            raise HardInternalStateMachineError("internal reversal chain exceeded the safety bound")
        self._capture_month(bar)

    def finalize(self) -> None:
        if self.active is not None:
            self.active.unresolved = True
            self.active.unresolved_reason = "INTERNAL_REVERSAL_NOT_CONFIRMED_BEFORE_DATASET_END"
        for pivot in self.held:
            pivot.internal_disposition = "INTERNAL_OPPOSITE_PIVOT_WHILE_CANDIDATE_ACTIVE"
            pivot.internal_role = "HELD_UNRESOLVED"

    def _on_pivot(self, pivot: Record, bar: Bar) -> None:
        self.seen.append(pivot)
        if not self.bootstrap_done:
            self._try_bootstrap(pivot, bar)
            return
        self._consider(pivot, bar)

    def _try_bootstrap(self, pivot: Record, bar: Bar) -> None:
        if pivot.atr is None or pivot.atr <= 0:
            pivot.internal_disposition = "INTERNAL_ATR_UNAVAILABLE"
            pivot.internal_role = "UNUSABLE"
            return
        best: Record | None = None
        best_rank: tuple | None = None
        best_disp = D0
        for earlier in self.seen[:-1]:
            if earlier.type == pivot.type:
                continue
            separation = pivot.extreme_row - earlier.extreme_row
            if separation < CONFIG.internal_minimum_spacing_bars:
                continue
            delta = _directional_delta(pivot.type, pivot.price, earlier.price)
            if delta <= 0 or delta < CONFIG.internal_minimum_displacement_atr * pivot.atr:
                continue
            disp_atr = delta / pivot.atr
            rank = (earlier.extreme_row, -disp_atr, earlier.pivot_id, pivot.pivot_id)
            if best is None or rank < best_rank:
                best = earlier
                best_rank = rank
                best_disp = disp_atr
        if best is None:
            pivot.internal_disposition = "INTERNAL_NOT_IN_BOOTSTRAP_PAIR"
            pivot.internal_role = "PRE_BOOTSTRAP"
            return
        self.bootstrap_done = True
        self.seed = best
        best.internal_role = "INTERNAL_BOOTSTRAP_SEED"
        best.internal_disposition = "INTERNAL_BOOTSTRAP_SEED"
        self.previous_opposite = best
        self.search_direction = pivot.type
        for other in self.seen:
            if other is best or other is pivot:
                continue
            if other.internal_role != "INTERNAL_BOOTSTRAP_SEED":
                other.internal_disposition = "INTERNAL_NOT_SELECTED_FOR_BOOTSTRAP"
                other.internal_role = "NOT_SELECTED"
        self.bootstrap_record = {
            "seed_pivot_id": best.pivot_id,
            "candidate_pivot_id": pivot.pivot_id,
            "displacement_atr": best_disp,
            "seed_row": best.extreme_row,
            "candidate_row": pivot.extreme_row,
            "bar": bar.row,
        }
        self._transition(
            bar,
            "BOOTSTRAP_PENDING",
            "INTERNAL_BOOTSTRAP_SEED",
            "INTERNAL_BOOTSTRAP_SEARCH",
            "Earlier raw pivot initializes internal direction only",
            pivot=best,
        )
        self._activate(pivot, best, bar, "INTERNAL_BOOTSTRAP_CANDIDATE")

    def _consider(self, pivot: Record, bar: Bar) -> None:
        if pivot.atr is None or pivot.atr <= 0:
            self._reject(pivot, bar, "INTERNAL_ATR_UNAVAILABLE")
            return
        if self.active is not None and pivot.type == self.active.type:
            self._consider_replacement(pivot, bar)
            return
        if self.active is not None and pivot.type != self.active.type:
            pivot.internal_disposition = "HELD_PENDING_OPPOSITE_RESOLUTION"
            pivot.internal_role = "HELD"
            self.held.append(pivot)
            self._transition(
                bar,
                self._state(),
                self._state(),
                "HOLD_OPPOSITE_PIVOT",
                "Opposite raw pivot is held so it cannot clear the active candidate",
                pivot=pivot,
            )
            return
        if self.search_direction is not None and pivot.type != self.search_direction:
            self._reject(pivot, bar, "INTERNAL_WRONG_DIRECTION")
            return
        self._try_activate(pivot, bar, "INTERNAL_CANDIDATE_ACTIVATED")

    def _consider_replacement(self, pivot: Record, bar: Bar) -> None:
        eligible, reason = self._eligibility(pivot, self.previous_opposite)
        if not eligible:
            self._reject(pivot, bar, reason)
            return
        more = (pivot.type == "HIGH" and pivot.price > self.active.price) or (
            pivot.type == "LOW" and pivot.price < self.active.price
        )
        if not more:
            self._reject(pivot, bar, "INTERNAL_NOT_MORE_EXTREME")
            return
        previous = self.active
        previous.pivot.internal_disposition = "INTERNAL_REPLACED_BY_MORE_EXTREME"
        previous.pivot.internal_role = "REPLACED_CANDIDATE"
        previous.replaced = True
        self.replacements += 1
        self._activate(pivot, self.previous_opposite, bar, "INTERNAL_CANDIDATE_REPLACED", replaces=previous)

    def _try_activate(self, pivot: Record, bar: Bar, trigger: str) -> bool:
        eligible, reason = self._eligibility(pivot, self.previous_opposite)
        if not eligible:
            self._reject(pivot, bar, reason)
            return False
        self._activate(pivot, self.previous_opposite, bar, trigger)
        return True

    def _eligibility(self, pivot: Record, previous: Record | None) -> tuple[bool, str]:
        if previous is None:
            return False, "INTERNAL_MISSING_PREVIOUS_OPPOSITE"
        if previous.type == pivot.type:
            return False, "INTERNAL_PREVIOUS_NOT_OPPOSITE"
        if pivot.atr is None or pivot.atr <= 0:
            return False, "INTERNAL_ATR_UNAVAILABLE"
        separation = pivot.extreme_row - previous.extreme_row
        if separation < CONFIG.internal_minimum_spacing_bars:
            return False, "INTERNAL_SPACING_BELOW_6"
        delta = _directional_delta(pivot.type, pivot.price, previous.price)
        if delta <= 0:
            return False, "INTERNAL_DISPLACEMENT_WRONG_DIRECTION"
        if delta < CONFIG.internal_minimum_displacement_atr * pivot.atr:
            return False, "INTERNAL_DISPLACEMENT_ATR_BELOW_2"
        return True, ""

    def _activate(
        self,
        pivot: Record,
        previous: Record | None,
        bar: Bar,
        trigger: str,
        replaces: Record | None = None,
    ) -> None:
        self.candidate_seq += 1
        delta = _directional_delta(pivot.type, pivot.price, previous.price) if previous else D0
        threshold = (
            pivot.price - CONFIG.internal_reversal_atr * pivot.atr
            if pivot.type == "HIGH"
            else pivot.price + CONFIG.internal_reversal_atr * pivot.atr
        )
        candidate = Record(
            candidate_id=f"IC-{self.candidate_seq:06d}",
            level="INTERNAL_SWING",
            type=pivot.type,
            pivot=pivot,
            formation_id=pivot.formation_id,
            pivot_id=pivot.pivot_id,
            price=pivot.price,
            atr=pivot.atr,
            extreme_row=pivot.extreme_row,
            confirm_row=pivot.confirm_row,
            activation_row=bar.row,
            activation_time=bar.close_time,
            previous=previous,
            displacement_price=delta,
            displacement_atr=(delta / pivot.atr) if pivot.atr else D0,
            displacement_passed=True,
            spacing_bars=pivot.extreme_row - previous.extreme_row if previous else 0,
            spacing_passed=True,
            prominence_passed=True,
            reversal_level=threshold,
            reversal_passed=False,
            reversal_ever=False,
            first_cross_time=None,
            closest_close=None,
            closest_distance=None,
            age=0,
            max_age=0,
            events=0,
            replaced=False,
            unresolved=False,
            unresolved_reason="",
            replacement_of=replaces.candidate_id if replaces else "",
            replacement_history=(replaces.replacement_history + "," if replaces else "")
            + (replaces.candidate_id if replaces else ""),
            hit_168=False,
            hit_720=False,
            hit_2160=False,
            trigger=trigger,
        )
        candidate.replacement_history = candidate.replacement_history.strip(",")
        pivot.internal_disposition = "INTERNAL_ACTIVE_CANDIDATE"
        pivot.internal_role = "CANDIDATE"
        pivot.candidate_id = candidate.candidate_id
        self.active = candidate
        self.candidates.append(candidate)
        self._transition(
            bar,
            "NO_ACTIVE_CANDIDATE" if replaces is None else replaces.candidate_id,
            candidate.candidate_id,
            trigger,
            "Internal candidate activated" if replaces is None else "More extreme internal candidate replaced the active candidate",
            pivot=pivot,
            candidate=candidate,
            replacement=replaces is not None,
        )

    def _flush_held(self, bar: Bar) -> bool:
        if self.active is not None or not self.held:
            return self.active is not None
        remaining: list[Record] = []
        activated = False
        for pivot in self.held:
            if activated:
                remaining.append(pivot)
                continue
            if self.search_direction is not None and pivot.type != self.search_direction:
                self._reject(pivot, bar, "INTERNAL_WRONG_DIRECTION")
                continue
            if self._try_activate(pivot, bar, "INTERNAL_CANDIDATE_ACTIVATED_FROM_HOLD"):
                activated = True
                continue
        self.held = remaining
        return activated

    def _evaluate_active(self, bar: Bar) -> bool:
        candidate = self.active
        if candidate is None:
            return False
        self.active_reversal_checks += 1
        candidate.events += 1
        candidate.age = bar.row - candidate.activation_row
        self.age_updates += 1
        self._note_age(candidate, bar)
        close = bar.close
        if candidate.type == "HIGH":
            distance = candidate.price - close
            crossed = close <= candidate.reversal_level
        else:
            distance = close - candidate.price
            crossed = close >= candidate.reversal_level
        gap = abs(close - candidate.reversal_level)
        if candidate.closest_distance is None or gap < candidate.closest_distance:
            candidate.closest_distance = gap
            candidate.closest_close = close
        if not crossed:
            return False
        if not candidate.reversal_ever:
            candidate.reversal_ever = True
            candidate.first_cross_time = bar.close_time
        requirements = candidate.displacement_passed and candidate.spacing_passed
        if not requirements:
            return False
        reversal_atr = distance / candidate.atr
        self._confirm(candidate, bar, distance, reversal_atr)
        if self.active is not None:
            self.ignored_valid += 1
            self.state_errors += 1
            raise HardInternalStateMachineError(
                "HARD_INTERNAL_STATE_MACHINE_ERROR: candidate remained active after a valid reversal"
            )
        return True

    def _confirm(self, candidate: Record, bar: Bar, distance: Decimal, reversal_atr: Decimal) -> None:
        sequence = len(self.confirmed) + 1
        swing = Record(
            internal_swing_id=f"IS-{sequence:06d}",
            sequence=sequence,
            level="INTERNAL_SWING",
            record_class="NORMAL_CONFIRMED",
            type=candidate.type,
            structure_label="",
            formation_id=candidate.formation_id,
            pivot_id=candidate.pivot_id,
            candidate_id=candidate.candidate_id,
            price=candidate.price,
            atr=candidate.atr,
            extreme_row=candidate.extreme_row,
            extreme_time=candidate.pivot.open_time,
            raw_pivot_confirmed_at=candidate.pivot.confirm_time,
            activated_at=candidate.activation_time,
            activation_row=candidate.activation_row,
            reversal_confirmed_at=bar.close_time,
            confirmed_at=bar.close_time,
            confirm_row=bar.row,
            age=candidate.age,
            max_age=candidate.max_age,
            replacement_history=candidate.replacement_history,
            displacement_price=candidate.displacement_price,
            displacement_atr=candidate.displacement_atr,
            displacement_passed=True,
            spacing_bars=candidate.spacing_bars,
            spacing_passed=True,
            reversal_price=distance,
            reversal_atr=reversal_atr,
            reversal_percent=percent(distance, candidate.price),
            reversal_passed=True,
            reversal_level=candidate.reversal_level,
            previous=candidate.previous,
            pivot=candidate.pivot,
            candidate=candidate,
            events=candidate.events,
            became_major_swing=False,
            major_swing_id="",
            major_non_promotion_reason="",
            major_role="",
        )
        earliest = max(
            swing.raw_pivot_confirmed_at,
            swing.activated_at,
            swing.reversal_confirmed_at,
        )
        if swing.confirmed_at < earliest:
            self.state_errors += 1
            raise HardInternalStateMachineError("internal confirmation precedes its causal inputs")
        candidate.pivot.internal_disposition = "INTERNAL_CONFIRMED"
        candidate.pivot.internal_role = "CONFIRMED_INTERNAL"
        candidate.reversal_passed = True
        self.confirmed.append(swing)
        self.confirmed_this_bar.append(swing)
        self.active = None
        self.previous_opposite = swing
        self.search_direction = "LOW" if swing.type == "HIGH" else "HIGH"
        self._transition(
            bar,
            candidate.candidate_id,
            swing.internal_swing_id,
            "INTERNAL_REVERSAL_CONFIRMED",
            "Close crossed the internal reversal threshold with displacement and spacing already passed",
            pivot=candidate.pivot,
            candidate=candidate,
            internal=swing,
            confirmation=True,
        )

    def _reject(self, pivot: Record, bar: Bar, reason: str) -> None:
        pivot.internal_disposition = reason
        pivot.internal_role = "REJECTED"
        self.rejections.append(pivot)
        self._transition(
            bar,
            self._state(),
            self._state(),
            "INTERNAL_PIVOT_REJECTED",
            reason,
            pivot=pivot,
        )

    def _note_age(self, candidate: Record, bar: Bar) -> None:
        if candidate.age > candidate.max_age:
            candidate.max_age = candidate.age
        if candidate.age > self.max_age_seen:
            self.max_age_seen = candidate.age
        month = month_key(bar.open_time)
        self.max_age_by_month[month] = max(self.max_age_by_month.get(month, 0), candidate.age)
        for threshold in (168, 720, 2160):
            if candidate.age > threshold and not getattr(candidate, f"hit_{threshold}"):
                setattr(candidate, f"hit_{threshold}", True)
                self.exceeded[threshold] += 1

    def _capture_month(self, bar: Bar) -> None:
        self.month_end_state[month_key(bar.open_time)] = self._state()

    def _state(self) -> str:
        if self.active is not None:
            return f"ACTIVE_{self.active.type}:{self.active.candidate_id}"
        if not self.bootstrap_done:
            return "BOOTSTRAP_PENDING"
        return "NO_ACTIVE_CANDIDATE"

    def _transition(
        self,
        bar: Bar,
        previous: str,
        new: str,
        trigger: str,
        reason: str,
        *,
        pivot: Record | None = None,
        candidate: Record | None = None,
        internal: Record | None = None,
        replacement: bool = False,
        confirmation: bool = False,
    ) -> None:
        self._transition_seq += 1
        self.transitions.append(
            {
                "engine": "INTERNAL",
                "seq": self._transition_seq,
                "event_time": bar.close_time,
                "row": bar.row,
                "previous_state": previous,
                "new_state": new,
                "trigger": trigger,
                "formation_id": getattr(pivot, "formation_id", "") if pivot else "",
                "raw_pivot_id": getattr(pivot, "pivot_id", "") if pivot else "",
                "internal_swing_id": getattr(internal, "internal_swing_id", "") if internal else "",
                "major_swing_id": "",
                "candidate_id": getattr(candidate, "candidate_id", "") if candidate else "",
                "candidate_type": getattr(candidate, "type", getattr(pivot, "type", "")) if (candidate or pivot) else "",
                "candidate_price": getattr(candidate, "price", getattr(pivot, "price", "")) if (candidate or pivot) else "",
                "displacement_status": getattr(candidate, "displacement_passed", ""),
                "spacing_status": getattr(candidate, "spacing_passed", ""),
                "prominence_status": getattr(candidate, "prominence_passed", ""),
                "reversal_status": confirmation,
                "replacement_event": replacement,
                "confirmation_event": confirmation,
                "reason": reason,
            }
        )


class MajorEngine:
    def __init__(self) -> None:
        self.seen: list[Record] = []
        self.bootstrap_done = False
        self.seed: Record | None = None
        self.previous_opposite: Record | None = None
        self.search_direction: str | None = None
        self.active: Record | None = None
        self.held: list[Record] = []
        self.confirmed: list[Record] = []
        self.candidates: list[Record] = []
        self.transitions: list[dict[str, Any]] = []
        self.non_promotions: list[tuple[str, str]] = []
        self.replacements = 0
        self.reversal_evaluations = 0
        self.active_reversal_checks = 0
        self.age_updates = 0
        self.ignored_valid = 0
        self.state_errors = 0
        self.candidate_seq = 0
        self.month_end_state: dict[str, str] = {}
        self.max_age_by_month: dict[str, int] = {}
        self.exceeded = {168: 0, 720: 0, 2160: 0}
        self.max_age_seen = 0
        self.bootstrap_record: dict[str, Any] | None = None
        self._transition_seq = 0

    def consume(self, swing: Record, bar: Bar) -> None:
        self._on_swing(swing, bar)

    def evaluate(self, bar: Bar) -> None:
        self.reversal_evaluations += 1
        safety = 0
        while safety < 64:
            safety += 1
            if self.active is None:
                if not self._flush_held(bar):
                    break
            if self.active is None:
                break
            confirmed = self._evaluate_active(bar)
            if not confirmed:
                break
        else:
            self.state_errors += 1
            raise HardMajorStateMachineError("major reversal chain exceeded the safety bound")
        self._capture_month(bar)

    def finalize(self) -> None:
        if self.active is not None:
            self.active.unresolved = True
            self.active.unresolved_reason = "MAJOR_REVERSAL_NOT_CONFIRMED_BEFORE_DATASET_END"
            source = self.active.source
            if not source.became_major_swing:
                source.major_non_promotion_reason = "MAJOR_REVERSAL_NOT_CONFIRMED_BEFORE_DATASET_END"
                source.major_role = "UNRESOLVED_CANDIDATE"
        for swing in self.held:
            if not swing.became_major_swing and swing.major_role != "MAJOR_BOOTSTRAP_SEED":
                swing.major_non_promotion_reason = "MAJOR_OPPOSITE_INTERNAL_WHILE_CANDIDATE_ACTIVE"
                swing.major_role = "HELD_UNRESOLVED"
                self.non_promotions.append((swing.internal_swing_id, swing.major_non_promotion_reason))

    def _on_swing(self, swing: Record, bar: Bar) -> None:
        self.seen.append(swing)
        if not self.bootstrap_done:
            self._try_bootstrap(swing, bar)
            return
        self._consider(swing, bar)

    def _try_bootstrap(self, swing: Record, bar: Bar) -> None:
        best: Record | None = None
        best_rank: tuple | None = None
        best_disp = D0
        for earlier in self.seen[:-1]:
            failure = self._failure(earlier, swing, bar.row)
            if failure is not None:
                continue
            delta = _directional_delta(swing.type, swing.price, earlier.price)
            disp_atr = delta / swing.atr
            rank = (earlier.confirm_row, earlier.sequence, -disp_atr, earlier.internal_swing_id)
            if best is None or rank < best_rank:
                best = earlier
                best_rank = rank
                best_disp = disp_atr
        if best is None:
            swing.major_non_promotion_reason = self._explain(swing, bar.row)
            swing.major_role = "NOT_PROMOTED"
            self.non_promotions.append((swing.internal_swing_id, swing.major_non_promotion_reason))
            return
        self.bootstrap_done = True
        self.seed = best
        best.major_role = "MAJOR_BOOTSTRAP_SEED"
        best.major_non_promotion_reason = "MAJOR_BOOTSTRAP_SEED"
        best.became_major_swing = False
        self.previous_opposite = best
        self.search_direction = swing.type
        for other in self.seen:
            if other is best or other is swing:
                continue
            if other.major_role != "MAJOR_BOOTSTRAP_SEED":
                if not other.major_non_promotion_reason:
                    other.major_non_promotion_reason = "MAJOR_NOT_SELECTED_FOR_BOOTSTRAP"
                other.major_role = "NOT_SELECTED"
        self.bootstrap_record = {
            "seed_internal_id": best.internal_swing_id,
            "candidate_internal_id": swing.internal_swing_id,
            "displacement_atr": best_disp,
            "bar": bar.row,
        }
        self._transition(
            bar,
            "BOOTSTRAP_PENDING",
            "MAJOR_BOOTSTRAP_SEED",
            "MAJOR_BOOTSTRAP_SEARCH",
            "Earlier internal swing initializes major direction only",
            internal=best,
        )
        self._activate(swing, best, bar, "MAJOR_BOOTSTRAP_CANDIDATE")

    def _explain(self, swing: Record, eval_row: int) -> str:
        opposites = [earlier for earlier in self.seen[:-1] if earlier.type != swing.type]
        if not opposites:
            return "MAJOR_NO_EARLIER_OPPOSITE_INTERNAL"
        return self._failure(opposites[0], swing, eval_row) or "MAJOR_BOOTSTRAP_PAIR_NOT_FOUND"

    def _failure(self, previous: Record, swing: Record, eval_row: int) -> str | None:
        if swing.atr is None or swing.atr <= 0:
            return "MAJOR_ATR_UNAVAILABLE"
        if previous.price is None or previous.price <= 0:
            return "MAJOR_PREVIOUS_PRICE_UNAVAILABLE"
        separation = swing.extreme_row - previous.extreme_row
        if separation < CONFIG.major_minimum_spacing_bars:
            return "MAJOR_SPACING_BELOW_12"
        delta = _directional_delta(swing.type, swing.price, previous.price)
        if delta <= 0:
            return "MAJOR_DISPLACEMENT_WRONG_DIRECTION"
        disp_atr = delta / swing.atr
        disp_pct = percent(delta, previous.price)
        if disp_atr < CONFIG.major_minimum_displacement_atr:
            return "MAJOR_DISPLACEMENT_ATR_BELOW_3"
        if disp_pct < CONFIG.major_minimum_displacement_percent:
            return "MAJOR_DISPLACEMENT_PERCENT_BELOW_1_50"
        prom = _prominence_from_context(swing, eval_row)
        prom_atr = None if prom is None or swing.atr is None else prom["two_sided_prominence"] / swing.atr
        if prom_atr is None or prom_atr < CONFIG.major_minimum_prominence_atr:
            return "MAJOR_PROMINENCE_BELOW_1_25"
        return None

    def _consider(self, swing: Record, bar: Bar) -> None:
        if self.active is not None and swing.type == self.active.type:
            self._consider_replacement(swing, bar)
            return
        if self.active is not None and swing.type != self.active.type:
            swing.major_role = "HELD"
            swing.major_non_promotion_reason = "HELD_PENDING_OPPOSITE_RESOLUTION"
            self.held.append(swing)
            self._transition(
                bar,
                self._state(),
                self._state(),
                "HOLD_OPPOSITE_INTERNAL",
                "Opposite internal swing is held and does not clear the active major candidate",
                internal=swing,
            )
            return
        if self.search_direction is not None and swing.type != self.search_direction:
            self._reject(swing, bar, "MAJOR_WRONG_DIRECTION")
            return
        self._try_activate(swing, bar, "MAJOR_CANDIDATE_ACTIVATED")

    def _consider_replacement(self, swing: Record, bar: Bar) -> None:
        failure = self._failure(self.previous_opposite, swing, bar.row)
        if failure is not None:
            self._reject(swing, bar, failure)
            return
        more = (swing.type == "HIGH" and swing.price > self.active.price) or (
            swing.type == "LOW" and swing.price < self.active.price
        )
        if not more:
            self._reject(swing, bar, "MAJOR_NOT_MORE_EXTREME_THAN_ACTIVE_CANDIDATE")
            return
        previous = self.active
        previous.replaced = True
        previous.source.major_non_promotion_reason = "MAJOR_REPLACED_BY_MORE_EXTREME"
        previous.source.major_role = "REPLACED_CANDIDATE"
        previous.source.became_major_swing = False
        self.replacements += 1
        self.non_promotions.append((previous.source.internal_swing_id, "MAJOR_REPLACED_BY_MORE_EXTREME"))
        self._activate(swing, self.previous_opposite, bar, "MAJOR_CANDIDATE_REPLACED", replaces=previous)

    def _try_activate(self, swing: Record, bar: Bar, trigger: str) -> bool:
        failure = self._failure(self.previous_opposite, swing, bar.row)
        if failure is not None:
            self._reject(swing, bar, failure)
            return False
        self._activate(swing, self.previous_opposite, bar, trigger)
        return True

    def _activate(
        self,
        swing: Record,
        previous: Record | None,
        bar: Bar,
        trigger: str,
        replaces: Record | None = None,
    ) -> None:
        self.candidate_seq += 1
        delta = _directional_delta(swing.type, swing.price, previous.price)
        disp_atr = delta / swing.atr
        disp_pct = percent(delta, previous.price)
        prom = _prominence_from_context(swing, bar.row)
        prom_atr = prom["two_sided_prominence"] / swing.atr
        atr_reversal = CONFIG.major_reversal_atr * swing.atr
        pct_reversal = (CONFIG.major_reversal_percent / D100) * swing.price
        required = atr_reversal if atr_reversal >= pct_reversal else pct_reversal
        threshold = swing.price - required if swing.type == "HIGH" else swing.price + required
        candidate = Record(
            candidate_id=f"MC-{self.candidate_seq:06d}",
            level="MAJOR_SWING",
            type=swing.type,
            source=swing,
            formation_id=swing.formation_id,
            pivot_id=swing.pivot_id,
            internal_swing_id=swing.internal_swing_id,
            price=swing.price,
            atr=swing.atr,
            extreme_row=swing.extreme_row,
            activation_row=bar.row,
            activation_time=bar.close_time,
            previous=previous,
            displacement_price=delta,
            displacement_atr=disp_atr,
            displacement_percent=disp_pct,
            displacement_atr_passed=disp_atr >= CONFIG.major_minimum_displacement_atr,
            displacement_percent_passed=disp_pct >= CONFIG.major_minimum_displacement_percent,
            spacing_bars=swing.extreme_row - previous.extreme_row,
            spacing_passed=True,
            prominence=prom,
            prominence_atr=prom_atr,
            prominence_passed=prom_atr >= CONFIG.major_minimum_prominence_atr,
            required_reversal=required,
            reversal_level=threshold,
            reversal_atr_passed=False,
            reversal_percent_passed=False,
            reversal_ever=False,
            first_cross_time=None,
            closest_close=None,
            closest_distance=None,
            age=0,
            max_age=0,
            events=0,
            replaced=False,
            unresolved=False,
            unresolved_reason="",
            replacement_of=replaces.candidate_id if replaces else "",
            replacement_history=((replaces.replacement_history + ",") if replaces and replaces.replacement_history else "")
            + (replaces.candidate_id if replaces else ""),
            hit_168=False,
            hit_720=False,
            hit_2160=False,
        )
        candidate.replacement_history = candidate.replacement_history.strip(",")
        swing.major_role = "CANDIDATE"
        swing.major_non_promotion_reason = ""
        swing.major_candidate_id = candidate.candidate_id
        self.active = candidate
        self.candidates.append(candidate)
        self._transition(
            bar,
            "NO_ACTIVE_CANDIDATE" if replaces is None else replaces.candidate_id,
            candidate.candidate_id,
            trigger,
            "Major candidate activated" if replaces is None else "More extreme major candidate replaced the active candidate",
            internal=swing,
            candidate=candidate,
            replacement=replaces is not None,
        )

    def _flush_held(self, bar: Bar) -> bool:
        if self.active is not None or not self.held:
            return self.active is not None
        remaining: list[Record] = []
        activated = False
        for swing in self.held:
            if activated:
                remaining.append(swing)
                continue
            if self.search_direction is not None and swing.type != self.search_direction:
                self._reject(swing, bar, "MAJOR_WRONG_DIRECTION")
                continue
            if self._try_activate(swing, bar, "MAJOR_CANDIDATE_ACTIVATED_FROM_HOLD"):
                activated = True
                continue
        self.held = remaining
        return activated

    def _evaluate_active(self, bar: Bar) -> bool:
        candidate = self.active
        if candidate is None:
            return False
        self.active_reversal_checks += 1
        candidate.events += 1
        candidate.age = bar.row - candidate.activation_row
        self.age_updates += 1
        self._note_age(candidate, bar)
        close = bar.close
        if candidate.type == "HIGH":
            distance = candidate.price - close
        else:
            distance = close - candidate.price
        reversal_atr = distance / candidate.atr
        reversal_percent = percent(distance, candidate.price)
        passed_atr = reversal_atr >= CONFIG.major_reversal_atr
        passed_pct = reversal_percent >= CONFIG.major_reversal_percent
        crossed = passed_atr and passed_pct
        gap = abs(close - candidate.reversal_level)
        if candidate.closest_distance is None or gap < candidate.closest_distance:
            candidate.closest_distance = gap
            candidate.closest_close = close
        if not crossed:
            return False
        if not candidate.reversal_ever:
            candidate.reversal_ever = True
            candidate.first_cross_time = bar.close_time
        requirements = (
            candidate.displacement_atr_passed
            and candidate.displacement_percent_passed
            and candidate.spacing_passed
            and candidate.prominence_passed
        )
        if not requirements:
            return False
        candidate.reversal_atr_passed = True
        candidate.reversal_percent_passed = True
        self._confirm(candidate, bar, distance, reversal_atr, reversal_percent)
        if self.active is not None:
            self.ignored_valid += 1
            self.state_errors += 1
            raise HardMajorStateMachineError(
                "HARD_MAJOR_STATE_MACHINE_ERROR: candidate remained active after a valid reversal"
            )
        return True

    def _confirm(
        self,
        candidate: Record,
        bar: Bar,
        distance: Decimal,
        reversal_atr: Decimal,
        reversal_percent: Decimal,
    ) -> None:
        sequence = len(self.confirmed) + 1
        source = candidate.source
        swing = Record(
            major_swing_id=f"MS-{sequence:06d}",
            sequence=sequence,
            level="MAJOR_SWING",
            record_class="NORMAL_CONFIRMED",
            type=candidate.type,
            structure_label="",
            formation_id=candidate.formation_id,
            pivot_id=candidate.pivot_id,
            internal_swing_id=candidate.internal_swing_id,
            candidate_id=candidate.candidate_id,
            price=candidate.price,
            atr=candidate.atr,
            extreme_row=candidate.extreme_row,
            extreme_time=source.extreme_time,
            formation_confirmed_at=source.raw_pivot_confirmed_at,
            internal_confirmed_at=source.confirmed_at,
            activated_at=candidate.activation_time,
            activation_row=candidate.activation_row,
            reversal_confirmed_at=bar.close_time,
            confirmed_at=bar.close_time,
            confirm_row=bar.row,
            age=candidate.age,
            max_age=candidate.max_age,
            replacement_history=candidate.replacement_history,
            previous=candidate.previous,
            displacement_price=candidate.displacement_price,
            displacement_atr=candidate.displacement_atr,
            displacement_percent=candidate.displacement_percent,
            displacement_atr_passed=True,
            displacement_percent_passed=True,
            reversal_price=distance,
            reversal_atr=reversal_atr,
            reversal_percent=reversal_percent,
            reversal_atr_passed=True,
            reversal_percent_passed=True,
            spacing_bars=candidate.spacing_bars,
            spacing_passed=True,
            prominence=candidate.prominence,
            prominence_atr=candidate.prominence_atr,
            prominence_passed=True,
            source_internal=source,
            pivot=source.pivot,
            inside_window=True,
            hierarchy_level="MAJOR_SWING",
            events=candidate.events,
        )
        earliest = max(
            swing.internal_confirmed_at,
            swing.activated_at,
            swing.reversal_confirmed_at,
        )
        if swing.confirmed_at < earliest:
            self.state_errors += 1
            raise HardMajorStateMachineError("major confirmation precedes its causal inputs")
        source.became_major_swing = True
        source.major_swing_id = swing.major_swing_id
        source.major_non_promotion_reason = ""
        source.major_role = "PROMOTED_MAJOR"
        self.confirmed.append(swing)
        self.active = None
        self.previous_opposite = swing
        self.search_direction = "LOW" if swing.type == "HIGH" else "HIGH"
        self._transition(
            bar,
            candidate.candidate_id,
            swing.major_swing_id,
            "MAJOR_REVERSAL_CONFIRMED",
            "Close crossed both major reversal thresholds with displacement, spacing, and prominence already passed",
            internal=source,
            candidate=candidate,
            major=swing,
            confirmation=True,
        )

    def _reject(self, swing: Record, bar: Bar, reason: str) -> None:
        swing.major_non_promotion_reason = reason
        swing.major_role = "NOT_PROMOTED"
        swing.became_major_swing = False
        self.non_promotions.append((swing.internal_swing_id, reason))
        self._transition(
            bar,
            self._state(),
            self._state(),
            "MAJOR_NON_PROMOTION",
            reason,
            internal=swing,
        )

    def _note_age(self, candidate: Record, bar: Bar) -> None:
        if candidate.age > candidate.max_age:
            candidate.max_age = candidate.age
        if candidate.age > self.max_age_seen:
            self.max_age_seen = candidate.age
        month = month_key(bar.open_time)
        self.max_age_by_month[month] = max(self.max_age_by_month.get(month, 0), candidate.age)
        for threshold in (168, 720, 2160):
            if candidate.age > threshold and not getattr(candidate, f"hit_{threshold}"):
                setattr(candidate, f"hit_{threshold}", True)
                self.exceeded[threshold] += 1

    def _capture_month(self, bar: Bar) -> None:
        self.month_end_state[month_key(bar.open_time)] = self._state()

    def _state(self) -> str:
        if self.active is not None:
            return f"ACTIVE_{self.active.type}:{self.active.candidate_id}"
        if not self.bootstrap_done:
            return "BOOTSTRAP_PENDING"
        return "NO_ACTIVE_CANDIDATE"

    def _transition(
        self,
        bar: Bar,
        previous: str,
        new: str,
        trigger: str,
        reason: str,
        *,
        internal: Record | None = None,
        candidate: Record | None = None,
        major: Record | None = None,
        replacement: bool = False,
        confirmation: bool = False,
    ) -> None:
        self._transition_seq += 1
        self.transitions.append(
            {
                "engine": "MAJOR",
                "seq": self._transition_seq,
                "event_time": bar.close_time,
                "row": bar.row,
                "previous_state": previous,
                "new_state": new,
                "trigger": trigger,
                "formation_id": getattr(internal, "formation_id", "") if internal else "",
                "raw_pivot_id": getattr(internal, "pivot_id", "") if internal else "",
                "internal_swing_id": getattr(internal, "internal_swing_id", "") if internal else "",
                "major_swing_id": getattr(major, "major_swing_id", "") if major else "",
                "candidate_id": getattr(candidate, "candidate_id", "") if candidate else "",
                "candidate_type": getattr(candidate, "type", getattr(internal, "type", "")) if (candidate or internal) else "",
                "candidate_price": getattr(candidate, "price", getattr(internal, "price", "")) if (candidate or internal) else "",
                "displacement_status": getattr(candidate, "displacement_atr_passed", ""),
                "spacing_status": getattr(candidate, "spacing_passed", ""),
                "prominence_status": getattr(candidate, "prominence_passed", ""),
                "reversal_status": confirmation,
                "replacement_event": replacement,
                "confirmation_event": confirmation,
                "reason": reason,
            }
        )


_BARS_FOR_PROMINENCE: list[Bar] = []


def _prominence_from_context(swing: Record, eval_row: int) -> dict[str, Any]:
    bars = _BARS_FOR_PROMINENCE
    return prominence(
        bars,
        kind=swing.type,
        extreme_row=swing.extreme_row,
        price=swing.price,
        eval_row=eval_row,
    )


def _core_signature(swings: Sequence[Record]) -> tuple:
    return tuple(
        (
            swing.sequence,
            swing.type,
            str(swing.price),
            swing.extreme_row,
            swing.confirm_row,
            swing.formation_id,
            swing.pivot_id,
            iso_utc(swing.confirmed_at),
        )
        for swing in swings
    )


def _replay(
    bars: Sequence[Bar],
    formations_by_id: dict[str, dict[str, Any]],
    plan: dict[int, list[str]],
    *,
    enable_major: bool,
) -> tuple[InternalEngine, MajorEngine | None, list[Record]]:
    global _BARS_FOR_PROMINENCE
    _BARS_FOR_PROMINENCE = list(bars)
    internal = InternalEngine()
    major = MajorEngine() if enable_major else None
    pivots: list[Record] = []
    sequence = 0
    for bar in bars:
        batch: list[Record] = []
        for formation_id in plan.get(bar.row, []):
            sequence += 1
            pivot = make_pivot(formations_by_id[formation_id], bars, sequence)
            batch.append(pivot)
        pivots.extend(batch)
        internal.consume_new_pivots(batch, bar)
        internal.evaluate(bar)
        if major is not None:
            for swing in internal.confirmed_this_bar:
                major.consume(swing, bar)
            major.evaluate(bar)
    internal.finalize()
    if major is not None:
        major.finalize()
    return internal, major, pivots


def _label_internal(swings: Sequence[Record]) -> None:
    last_high: Record | None = None
    last_low: Record | None = None
    for swing in swings:
        tolerance = CONFIG.equality_tolerance_atr * swing.atr
        if swing.type == "HIGH":
            if last_high is None:
                swing.structure_label = "FIRST_INTERNAL_HIGH"
            elif swing.price > last_high.price + tolerance:
                swing.structure_label = "HH"
            elif swing.price < last_high.price - tolerance:
                swing.structure_label = "LH"
            else:
                swing.structure_label = "EH"
            last_high = swing
        else:
            if last_low is None:
                swing.structure_label = "FIRST_INTERNAL_LOW"
            elif swing.price > last_low.price + tolerance:
                swing.structure_label = "HL"
            elif swing.price < last_low.price - tolerance:
                swing.structure_label = "LL"
            else:
                swing.structure_label = "EL"
            last_low = swing


def _label_major(swings: Sequence[Record]) -> None:
    last_high: Record | None = None
    last_low: Record | None = None
    for swing in swings:
        tolerance = CONFIG.equality_tolerance_atr * swing.atr
        if swing.type == "HIGH":
            if last_high is None:
                swing.structure_label = "FIRST_MAJOR_HIGH"
            elif swing.price > last_high.price + tolerance:
                swing.structure_label = "HH"
            elif swing.price < last_high.price - tolerance:
                swing.structure_label = "LH"
            else:
                swing.structure_label = "EH"
            last_high = swing
        else:
            if last_low is None:
                swing.structure_label = "FIRST_MAJOR_LOW"
            elif swing.price > last_low.price + tolerance:
                swing.structure_label = "HL"
            elif swing.price < last_low.price - tolerance:
                swing.structure_label = "LL"
            else:
                swing.structure_label = "EL"
            last_low = swing


def _leg_bounds(swing: Record, seed: Record | None) -> tuple[Record | None, str]:
    previous = swing.previous
    if previous is None:
        return None, "MISSING"
    if seed is not None and previous is seed:
        return previous, "BOOTSTRAP_SEED"
    return previous, "CONFIRMED_OPPOSITE_SWING"


def _median(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / Decimal(2)


def _attach_leg_metrics(
    swing: Record,
    bars: Sequence[Bar],
    *,
    seed: Record | None,
    formations: Sequence[dict[str, Any]],
    pivots: Sequence[Record],
    contained_internals: Sequence[Record],
    kind: str,
) -> None:
    previous, previous_class = _leg_bounds(swing, seed)
    swing.leg_start_class = previous_class
    if previous is None:
        swing.leg_start_row = None
        return
    start_row = int(previous.extreme_row)
    end_row = int(swing.extreme_row)
    start = bars[start_row]
    end = bars[end_row]
    swing.leg_start_row = start_row
    swing.leg_end_row = end_row
    swing.leg_start_price = previous.price
    swing.leg_end_price = swing.price
    swing.leg_start_open_time = start.open_time
    swing.leg_start_close_time = start.close_time
    swing.leg_end_open_time = end.open_time
    swing.leg_end_close_time = end.close_time
    swing.leg_start_ohlc = _ohlc(start)
    swing.leg_end_ohlc = _ohlc(end)
    amplitude = abs(swing.price - previous.price)
    swing.absolute_amplitude = amplitude
    swing.amplitude_percent_from_start = percent(amplitude, previous.price) if previous.price else D0
    high_price = swing.price if swing.price >= previous.price else previous.price
    low_price = swing.price if swing.price <= previous.price else previous.price
    swing.peak_to_trough_percent = percent(high_price - low_price, high_price) if high_price else D0
    swing.trough_to_peak_percent = percent(high_price - low_price, low_price) if low_price else D0
    midpoint = (high_price + low_price) / Decimal(2)
    swing.symmetric_width_percent = percent(high_price - low_price, midpoint) if midpoint else D0
    start_atr = start.atr
    end_atr = swing.atr
    swing.amplitude_atr_at_start = (amplitude / start_atr) if start_atr else None
    swing.amplitude_atr_at_end = (amplitude / end_atr) if end_atr else None
    leg_atrs = [bars[row].atr for row in range(start_row, end_row + 1) if bars[row].atr is not None]
    median_atr = _median([value for value in leg_atrs if value is not None])
    swing.amplitude_atr_median = (amplitude / median_atr) if median_atr else None
    swing.median_leg_atr = median_atr
    duration = end_row - start_row
    swing.duration_bars = duration
    swing.duration_hours = Decimal(duration)
    swing.duration_days = Decimal(duration) / Decimal(24)
    prev_confirm_row = getattr(previous, "confirm_row", None)
    if prev_confirm_row is None:
        prev_confirm_row = getattr(previous, "extreme_row", start_row)
    swing.confirmation_to_confirmation_bars = swing.confirm_row - int(prev_confirm_row)
    swing.price_change_per_bar = ((swing.price - previous.price) / Decimal(duration)) if duration else D0
    swing.percent_change_per_bar = (
        percent(swing.price - previous.price, previous.price) / Decimal(duration) if duration and previous.price else D0
    )
    if start_atr is not None and end_atr is not None and duration:
        swing.atr_change_per_bar = (end_atr - start_atr) / Decimal(duration)
    else:
        swing.atr_change_per_bar = None
    path = D0
    for row in range(start_row + 1, end_row + 1):
        path += abs(bars[row].close - bars[row - 1].close)
    net = bars[end_row].close - bars[start_row].close
    swing.path_length = path
    swing.net_close_change = net
    swing.leg_efficiency = (abs(net) / path) if path > 0 else D0
    bullish = bearish = unchanged = 0
    total_volume = D0
    quote_volume = D0
    typical_volume = D0
    pullback = D0
    peak = bars[start_row].high
    trough = bars[start_row].low
    log_returns: list[float] = []
    up_leg = swing.price >= previous.price
    for row in range(start_row, end_row + 1):
        bar = bars[row]
        if bar.close > bar.open:
            bullish += 1
        elif bar.close < bar.open:
            bearish += 1
        else:
            unchanged += 1
        total_volume += bar.volume
        quote_volume += bar.quote_volume
        typical_volume += bar.typical * bar.volume
        if up_leg:
            if bar.high > peak:
                peak = bar.high
            adverse = peak - bar.low
        else:
            if bar.low < trough:
                trough = bar.low
            adverse = bar.high - trough
        if adverse > pullback:
            pullback = adverse
        if row > start_row and bars[row - 1].close > 0 and bar.close > 0:
            log_returns.append(float(bar.close / bars[row - 1].close))
    swing.bullish_candles = bullish
    swing.bearish_candles = bearish
    swing.unchanged_candles = unchanged
    swing.internal_pullback = pullback
    if len(log_returns) >= 2:
        # pstdev of log price relatives. Descriptive only.
        logs = [math.log(value) for value in log_returns]
        swing.realized_volatility = Decimal(str(pstdev(logs)))
    else:
        swing.realized_volatility = None
    swing.total_leg_volume = total_volume
    swing.total_leg_quote_volume = quote_volume
    swing.vwap = (typical_volume / total_volume) if total_volume > 0 else None
    extreme = bars[end_row]
    body_high = extreme.open if extreme.open >= extreme.close else extreme.close
    body_low = extreme.open if extreme.open <= extreme.close else extreme.close
    swing.extreme_body = abs(extreme.close - extreme.open)
    swing.extreme_upper_wick = extreme.high - body_high
    swing.extreme_lower_wick = body_low - extreme.low
    span = extreme.high - extreme.low
    swing.close_location = ((extreme.close - extreme.low) / span) if span > 0 else Decimal("0.5")
    swing.base_volume = extreme.volume
    swing.quote_volume = extreme.quote_volume
    swing.trade_count = extreme.trades
    swing.taker_buy_ratio = (extreme.taker_buy_base / extreme.volume) if extreme.volume > 0 else None
    trail_start = end_row - 50
    if trail_start >= 0:
        trail = [bars[row].volume for row in range(trail_start, end_row)]
        trail_mean = sum(trail) / Decimal(len(trail)) if trail else D0
        swing.trailing_50_mean_volume = trail_mean
        swing.pivot_volume_to_trailing_50_mean = (extreme.volume / trail_mean) if trail_mean > 0 else None
    else:
        swing.trailing_50_mean_volume = None
        swing.pivot_volume_to_trailing_50_mean = None
    swing.raw_formations_inside_leg = sum(
        1
        for formation in formations
        if formation.get("emits_pivot")
        and formation.get("extreme_row") is not None
        and start_row < int(formation["extreme_row"]) < end_row
    )
    swing.raw_pivots_inside_leg = sum(
        1 for pivot in pivots if start_row < pivot.extreme_row < end_row
    )
    if kind == "MAJOR":
        swing.internal_swings_inside_leg = sum(
            1
            for internal in contained_internals
            if start_row < internal.extreme_row < end_row and internal.internal_swing_id != swing.internal_swing_id
        )
    swing.metric_class_geometry = "CAUSAL_AT_INTERNAL_CONFIRMATION" if kind == "INTERNAL" else "CAUSAL_AT_MAJOR_CONFIRMATION"
    swing.metric_class_path = swing.metric_class_geometry
    swing.metric_class_forward = "EX_POST_FORWARD_METRIC"


def _score_major(swing: Record) -> None:
    amplitude = swing.amplitude_atr_median or D0
    prominence_atr = swing.prominence_atr or D0
    reversal_atr = swing.reversal_atr or D0
    efficiency = swing.leg_efficiency or D0
    volume_ratio = swing.pivot_volume_to_trailing_50_mean or D0
    amplitude_component = clamp((amplitude - Decimal("3.0")) / Decimal("7.0"), D0, D1)
    prominence_component = clamp((prominence_atr - Decimal("1.25")) / Decimal("3.75"), D0, D1)
    reversal_component = clamp((reversal_atr - Decimal("1.50")) / Decimal("2.50"), D0, D1)
    efficiency_component = clamp(efficiency, D0, D1)
    volume_component = clamp((volume_ratio - D1) / Decimal("2.0"), D0, D1)
    score = D100 * (
        Decimal("0.30") * amplitude_component
        + Decimal("0.25") * prominence_component
        + Decimal("0.20") * reversal_component
        + Decimal("0.15") * efficiency_component
        + Decimal("0.10") * volume_component
    )
    swing.score_amplitude_component = amplitude_component
    swing.score_prominence_component = prominence_component
    swing.score_reversal_component = reversal_component
    swing.score_efficiency_component = efficiency_component
    swing.score_volume_component = volume_component
    swing.significance_score = score
    if score >= 85:
        swing.significance_label = "Exceptional"
    elif score >= 70:
        swing.significance_label = "Major"
    elif score >= 50:
        swing.significance_label = "Significant"
    elif score >= 30:
        swing.significance_label = "Moderate"
    else:
        swing.significance_label = "Minor Score"


def _attach_forward(swings: Sequence[Record], bars: Sequence[Bar]) -> None:
    last_row = len(bars) - 1
    for index, swing in enumerate(swings):
        nxt = swings[index + 1] if index + 1 < len(swings) else None
        swing.next_major_swing_id = nxt.major_swing_id if nxt else ""
        swing.bars_to_next_pivot = (nxt.extreme_row - swing.extreme_row) if nxt else None
        swing.bars_to_next_confirmation = (nxt.confirm_row - swing.confirm_row) if nxt else None
        window_end = nxt.extreme_row if nxt else last_row
        if window_end < swing.extreme_row:
            window_end = swing.extreme_row
        tolerance = CONFIG.retest_tolerance_atr * swing.atr
        favorable = D0
        adverse = D0
        retests = 0
        first_beyond = None
        for row in range(swing.extreme_row + 1, window_end + 1):
            bar = bars[row]
            if swing.type == "HIGH":
                favorable = max(favorable, swing.price - bar.low)
                adverse = max(adverse, bar.high - swing.price)
                beyond = bar.close > swing.price
            else:
                favorable = max(favorable, bar.high - swing.price)
                adverse = max(adverse, swing.price - bar.low)
                beyond = bar.close < swing.price
            if row > swing.confirm_row and bar.low <= swing.price + tolerance and bar.high >= swing.price - tolerance:
                retests += 1
            if beyond and first_beyond is None:
                first_beyond = bar
        swing.max_favorable_excursion = favorable
        swing.max_adverse_excursion = adverse
        swing.retest_count = retests
        swing.first_close_beyond_time = first_beyond.open_time if first_beyond else None
        swing.first_close_beyond_price = first_beyond.close if first_beyond else None
        censored = nxt is None or window_end >= last_row and nxt is None
        swing.forward_data_censored = nxt is None
        swing.forward_censoring_reason = "DATASET_END" if nxt is None else ""
        swing.forward_metric_class = "EX_POST_FORWARD_METRIC" if nxt is not None else "EX_POST_FORWARD_METRIC"
        if nxt is None:
            swing.forward_metric_class = "EX_POST_FORWARD_METRIC"


def _rejection_counts(formations: Sequence[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for formation in formations:
        if formation["primary_disposition"] != "QUALIFIED_RAW_FORMATION":
            counts[formation["primary_disposition"]] += 1
    return dict(counts)


def _build_monthly(
    bars: Sequence[Bar],
    formations: Sequence[dict[str, Any]],
    pivots: Sequence[Record],
    internal: InternalEngine,
    major: MajorEngine | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    present_months = []
    for key in MONTHS:
        if any(month_key(bar.open_time) == key for bar in bars):
            present_months.append(key)
    if not present_months:
        present_months = sorted({month_key(bar.open_time) for bar in bars})
    for key in present_months:
        month_formations = [
            formation
            for formation in formations
            if not formation.get("edge") and formation.get("close_row") is not None and 0 <= int(formation["close_row"]) < len(bars)
            and month_key(bars[int(formation["close_row"])].open_time) == key
        ]
        edge_formations = [
            formation
            for formation in formations
            if formation.get("edge") and formation["primary_disposition"].startswith("LEFT") and key == present_months[0]
            or formation.get("edge") and formation["primary_disposition"].startswith("RIGHT") and key == present_months[-1]
        ]
        # The expression above is operator-precedence sensitive. Recompute explicitly.
        edge_formations = []
        if key == present_months[0]:
            edge_formations.extend(formation for formation in formations if formation.get("primary_disposition") == "LEFT_EDGE_CENSORED")
        if key == present_months[-1]:
            edge_formations.extend(formation for formation in formations if formation.get("primary_disposition") == "RIGHT_EDGE_CENSORED")
        combined = month_formations + edge_formations
        rejected: dict[str, int] = defaultdict(int)
        qualified = 0
        for formation in combined:
            if formation["primary_disposition"] == "QUALIFIED_RAW_FORMATION":
                qualified += 1
            else:
                rejected[formation["primary_disposition"]] += 1
        month_pivots = [pivot for pivot in pivots if month_key(pivot.confirm_time) == key]
        activations = [candidate for candidate in internal.candidates if month_key(candidate.activation_time) == key and not candidate.replacement_of]
        replacements = [candidate for candidate in internal.candidates if month_key(candidate.activation_time) == key and candidate.replacement_of]
        confirmed = [swing for swing in internal.confirmed if month_key(swing.confirmed_at) == key]
        rejected_internal = [
            pivot for pivot in pivots if pivot.internal_role == "REJECTED" and month_key(pivot.confirm_time) == key
        ]
        major_activations = []
        major_replacements = []
        major_confirmed = []
        major_non = []
        if major is not None:
            major_activations = [
                candidate for candidate in major.candidates if month_key(candidate.activation_time) == key and not candidate.replacement_of
            ]
            major_replacements = [
                candidate for candidate in major.candidates if month_key(candidate.activation_time) == key and candidate.replacement_of
            ]
            major_confirmed = [swing for swing in major.confirmed if month_key(swing.confirmed_at) == key]
            major_non = [reason for swing_id, reason in major.non_promotions]
        max_age = internal.max_age_by_month.get(key, 0)
        if major is not None:
            max_age = max(max_age, major.max_age_by_month.get(key, 0))
        rows.append(
            {
                "utc_month": key,
                "source_candles": sum(1 for bar in bars if month_key(bar.open_time) == key),
                "formation_count": len(combined),
                "qualified_formation_count": qualified,
                "rejected_formation_count": sum(rejected.values()),
                "rejection_breakdown": "; ".join(f"{name}={count}" for name, count in sorted(rejected.items())),
                "raw_pivot_highs": sum(1 for pivot in month_pivots if pivot.type == "HIGH"),
                "raw_pivot_lows": sum(1 for pivot in month_pivots if pivot.type == "LOW"),
                "new_internal_high_candidates": sum(1 for candidate in activations if candidate.type == "HIGH"),
                "new_internal_low_candidates": sum(1 for candidate in activations if candidate.type == "LOW"),
                "confirmed_internal_highs": sum(1 for swing in confirmed if swing.type == "HIGH"),
                "confirmed_internal_lows": sum(1 for swing in confirmed if swing.type == "LOW"),
                "replaced_internal_candidates": len(replacements),
                "rejected_internal_candidates": len(rejected_internal),
                "active_internal_state": internal.month_end_state.get(key, "NO_BARS"),
                "new_major_high_candidates": sum(1 for candidate in major_activations if candidate.type == "HIGH"),
                "new_major_low_candidates": sum(1 for candidate in major_activations if candidate.type == "LOW"),
                "confirmed_major_highs": sum(1 for swing in major_confirmed if swing.type == "HIGH"),
                "confirmed_major_lows": sum(1 for swing in major_confirmed if swing.type == "LOW"),
                "replaced_major_candidates": len(major_replacements),
                "major_non_promotion_count": len([item for item in (major.non_promotions if major else []) if True]),
                "active_major_state": major.month_end_state.get(key, "MAJOR_DISABLED") if major else "MAJOR_DISABLED",
                "maximum_candidate_age": max_age,
                "ignored_valid_reversal_count": internal.ignored_valid + (major.ignored_valid if major else 0),
                "state_transition_errors": internal.state_errors + (major.state_errors if major else 0),
            }
        )
        # Major non-promotion counts are lifetime totals repeated per month unless
        # we cannot time them. Replace the lifetime figure with a month-local count
        # using transition timestamps below.
        _ = major_non
    if major is not None:
        for row in rows:
            key = row["utc_month"]
            row["major_non_promotion_count"] = sum(
                1
                for event in major.transitions
                if event["trigger"] == "MAJOR_NON_PROMOTION" and month_key(event["event_time"]) == key
            )
    return rows


def _unresolved_rows(internal: InternalEngine, major: MajorEngine | None, bars: Sequence[Bar]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add(level: str, candidate: Record) -> None:
        last = bars[-1]
        if candidate.type == "HIGH":
            missing = last.close - candidate.reversal_level
        else:
            missing = candidate.reversal_level - last.close
        if missing < 0:
            missing = D0
        atr = candidate.atr or D1
        rows.append(
            {
                "engine_level": level,
                "candidate_id": candidate.candidate_id,
                "candidate_type": candidate.type,
                "formation_id": candidate.formation_id,
                "raw_pivot_id": candidate.pivot_id,
                "internal_swing_id": getattr(candidate, "internal_swing_id", ""),
                "extreme_time_utc": iso_utc(bars[candidate.extreme_row].open_time),
                "extreme_time_turkey": iso_turkey(bars[candidate.extreme_row].open_time),
                "candidate_price": candidate.price,
                "candidate_atr": candidate.atr,
                "required_reversal": getattr(candidate, "required_reversal", abs(candidate.price - candidate.reversal_level)),
                "last_close": last.close,
                "missing_reversal_price": missing,
                "missing_reversal_atr": missing / atr,
                "missing_reversal_percent": percent(missing, candidate.price) if candidate.price else D0,
                "candidate_age": candidate.age,
                "last_replacement": candidate.replacement_history,
                "reason_unresolved": candidate.unresolved_reason,
            }
        )

    if internal.active is not None and internal.active.unresolved:
        add("INTERNAL_SWING", internal.active)
    if major is not None and major.active is not None and major.active.unresolved:
        add("MAJOR_SWING", major.active)
    return rows


@dataclass
class DetectionResult:
    bars: list[Bar]
    formations: list[dict[str, Any]]
    pivots: list[Record]
    internal_swings: list[Record]
    internal_seed: Record | None
    major_swings: list[Record]
    major_seed: Record | None
    transitions: list[dict[str, Any]]
    unresolved: list[dict[str, Any]]
    monthly: list[dict[str, Any]]
    formation_stats: dict[str, int]
    internal_engine: InternalEngine
    major_engine: MajorEngine | None
    independence_pass: bool
    warnings: list[str] = field(default_factory=list)

    @property
    def unique(self) -> dict[str, int]:
        dispositions = _rejection_counts(self.formations)
        return {
            "unique_enumerated_formations": len(self.formations),
            "unique_qualified_formations": sum(
                1 for formation in self.formations if formation["primary_disposition"] == "QUALIFIED_RAW_FORMATION"
            ),
            "unique_duplicate_formations": dispositions.get("DUPLICATE_OF_REPRESENTATIVE_FORMATION", 0),
            "unique_raw_pivots": len(self.pivots),
            "unique_raw_pivot_highs": sum(1 for pivot in self.pivots if pivot.type == "HIGH"),
            "unique_raw_pivot_lows": sum(1 for pivot in self.pivots if pivot.type == "LOW"),
            "unique_internal_candidates": len(self.internal_engine.candidates),
            "unique_confirmed_internal_swings": len(self.internal_swings),
            "unique_internal_highs": sum(1 for swing in self.internal_swings if swing.type == "HIGH"),
            "unique_internal_lows": sum(1 for swing in self.internal_swings if swing.type == "LOW"),
            "unique_major_candidates": len(self.major_engine.candidates) if self.major_engine else 0,
            "unique_confirmed_major_swings": len(self.major_swings),
            "unique_major_highs": sum(1 for swing in self.major_swings if swing.type == "HIGH"),
            "unique_major_lows": sum(1 for swing in self.major_swings if swing.type == "LOW"),
            "unique_internal_replacements": self.internal_engine.replacements,
            "unique_major_replacements": self.major_engine.replacements if self.major_engine else 0,
            "unique_rejection_reasons": len(dispositions),
        }

    @property
    def per_bar(self) -> dict[str, int]:
        major_evals = self.major_engine.reversal_evaluations if self.major_engine else 0
        major_age = self.major_engine.age_updates if self.major_engine else 0
        return {
            "label": "PER_BAR_DIAGNOSTIC_ONLY",
            "formation_checks": self.formation_stats["formation_checks"],
            "internal_reversal_evaluations": self.internal_engine.reversal_evaluations,
            "internal_active_reversal_checks": self.internal_engine.active_reversal_checks,
            "major_reversal_evaluations": major_evals,
            "major_active_reversal_checks": self.major_engine.active_reversal_checks if self.major_engine else 0,
            "internal_candidate_age_updates": self.internal_engine.age_updates,
            "major_candidate_age_updates": major_age,
        }


def run_detection(
    bars: Sequence[Bar],
    *,
    enable_major: bool = True,
    recompute_atr: bool = True,
    verify_independence: bool = True,
) -> DetectionResult:
    """Run the full causal detector. Formations are released only at swing-close completion."""
    if recompute_atr:
        compute_atr(bars)
    global _BARS_FOR_PROMINENCE
    _BARS_FOR_PROMINENCE = list(bars)
    formations, stats = enumerate_formations(bars)
    plan = assign_causal_dispositions(bars, formations)
    by_id = {formation["formation_id"]: formation for formation in formations}
    internal, major, pivots = _replay(bars, by_id, plan, enable_major=enable_major)
    independence_pass = True
    warnings: list[str] = []
    if verify_independence and enable_major:
        internal_off, _, _ = _replay(bars, by_id, plan, enable_major=False)
        independence_pass = _core_signature(internal.confirmed) == _core_signature(internal_off.confirmed) and (
            (internal.seed.pivot_id if internal.seed else None) == (internal_off.seed.pivot_id if internal_off.seed else None)
        )
        if not independence_pass:
            raise HardInternalStateMachineError("major engine changed internal swing output")
    _label_internal(internal.confirmed)
    if major is not None:
        _label_major(major.confirmed)
    for swing in internal.confirmed:
        _attach_leg_metrics(
            swing,
            bars,
            seed=internal.seed,
            formations=formations,
            pivots=pivots,
            contained_internals=internal.confirmed,
            kind="INTERNAL",
        )
    if major is not None:
        for swing in major.confirmed:
            _attach_leg_metrics(
                swing,
                bars,
                seed=major.seed,
                formations=formations,
                pivots=pivots,
                contained_internals=internal.confirmed,
                kind="MAJOR",
            )
            _score_major(swing)
        _attach_forward(major.confirmed, bars)
    transitions = list(internal.transitions)
    if major is not None:
        transitions.extend(major.transitions)
    transitions.sort(key=lambda item: (item["row"], 0 if item["engine"] == "INTERNAL" else 1, item["seq"]))
    result = DetectionResult(
        bars=list(bars),
        formations=list(formations),
        pivots=pivots,
        internal_swings=list(internal.confirmed),
        internal_seed=internal.seed,
        major_swings=list(major.confirmed) if major else [],
        major_seed=major.seed if major else None,
        transitions=transitions,
        unresolved=_unresolved_rows(internal, major, bars),
        monthly=_build_monthly(bars, formations, pivots, internal, major),
        formation_stats=stats,
        internal_engine=internal,
        major_engine=major,
        independence_pass=independence_pass,
        warnings=warnings,
    )
    problems = validate_result_invariants(result)
    if problems:
        raise RuntimeError("detection invariant failure: " + "; ".join(problems[:8]))
    return result


def validate_result_invariants(result: DetectionResult) -> list[str]:
    problems: list[str] = []
    qualified_ids = {
        formation["formation_id"]
        for formation in result.formations
        if formation["primary_disposition"] == "QUALIFIED_RAW_FORMATION"
    }
    for formation in result.formations:
        if formation.get("edge"):
            continue
        total = formation.get("total_bars")
        if formation["primary_disposition"] == "QUALIFIED_RAW_FORMATION":
            if total is None or not (5 <= int(total) <= 10):
                problems.append(f"{formation['formation_id']} length {total}")
            interior = formation.get("interior_bars")
            if interior is None or not (3 <= int(interior) <= 8):
                problems.append(f"{formation['formation_id']} interior {interior}")
            if not formation.get("central_pass"):
                problems.append(f"{formation['formation_id']} central failure")
            if formation["outbound_percent"] < CONFIG.minimum_outbound_percent:
                problems.append(f"{formation['formation_id']} outbound")
            if formation["return_percent_reference_basis"] < CONFIG.minimum_return_percent:
                problems.append(f"{formation['formation_id']} return")
            if not formation.get("revisit_pass"):
                problems.append(f"{formation['formation_id']} revisit")
            if formation["left_bars"] is None or formation["right_bars"] is None:
                problems.append(f"{formation['formation_id']} spans missing")
    for pivot in result.pivots:
        if pivot.formation_id not in qualified_ids:
            problems.append(f"{pivot.pivot_id} missing qualified formation")
        if pivot.confirm_row < pivot.extreme_row:
            problems.append(f"{pivot.pivot_id} confirmation before extreme")
    internal_ids = {swing.internal_swing_id for swing in result.internal_swings}
    pivot_ids = {pivot.pivot_id for pivot in result.pivots}
    for swing in result.internal_swings:
        if swing.pivot_id not in pivot_ids:
            problems.append(f"{swing.internal_swing_id} missing pivot")
        if swing.record_class != "NORMAL_CONFIRMED":
            problems.append(f"{swing.internal_swing_id} is not a normal internal")
        if swing.confirmed_at < swing.raw_pivot_confirmed_at:
            problems.append(f"{swing.internal_swing_id} repaint")
        if swing.confirm_row < swing.extreme_row:
            problems.append(f"{swing.internal_swing_id} look-ahead")
    for index in range(1, len(result.internal_swings)):
        if result.internal_swings[index].type == result.internal_swings[index - 1].type:
            problems.append("internal alternation broken")
            break
    if result.internal_seed is not None and any(
        swing.pivot_id == result.internal_seed.pivot_id for swing in result.internal_swings
    ):
        problems.append("internal bootstrap seed counted as a normal swing")
    for swing in result.major_swings:
        if swing.internal_swing_id not in internal_ids:
            problems.append(f"{swing.major_swing_id} missing internal")
        if swing.hierarchy_level != "MAJOR_SWING":
            problems.append(f"{swing.major_swing_id} hierarchy")
        if swing.confirmed_at < swing.internal_confirmed_at:
            problems.append(f"{swing.major_swing_id} repaint")
    for index in range(1, len(result.major_swings)):
        if result.major_swings[index].type == result.major_swings[index - 1].type:
            problems.append("major alternation broken")
            break
    if result.major_seed is not None and any(
        swing.internal_swing_id == result.major_seed.internal_swing_id for swing in result.major_swings
    ):
        problems.append("major bootstrap seed counted as a normal major swing")
    if result.internal_engine.ignored_valid or (result.major_engine and result.major_engine.ignored_valid):
        problems.append("ignored valid reversal")
    if result.internal_engine.state_errors or (result.major_engine and result.major_engine.state_errors):
        problems.append("state transition error")
    if result.internal_engine.active is not None and result.internal_engine.active.reversal_ever:
        if result.internal_engine.active.displacement_passed and result.internal_engine.active.spacing_passed:
            # A crossed reversal that still meets requirements must not remain active.
            last_close = result.bars[-1].close
            level = result.internal_engine.active.reversal_level
            kind = result.internal_engine.active.type
            still_across = last_close <= level if kind == "HIGH" else last_close >= level
            if still_across:
                problems.append("internal candidate latched after reversal")
    if not result.independence_pass:
        problems.append("engine independence failed")
    if result.per_bar["label"] != "PER_BAR_DIAGNOSTIC_ONLY":
        problems.append("per-bar label missing")
    month_sources = sum(row["source_candles"] for row in result.monthly)
    if month_sources != len(result.bars):
        problems.append("monthly source candles do not reconcile")
    confirmed_internal = sum(row["confirmed_internal_highs"] + row["confirmed_internal_lows"] for row in result.monthly)
    if confirmed_internal != len(result.internal_swings):
        problems.append("monthly internal confirmations do not reconcile")
    confirmed_major = sum(row["confirmed_major_highs"] + row["confirmed_major_lows"] for row in result.monthly)
    if confirmed_major != len(result.major_swings):
        problems.append("monthly major confirmations do not reconcile")
    return problems


def validate_source_bars(bars: Sequence[Bar]) -> list[str]:
    """Production source checks. Returns a list of failures; empty means valid."""
    problems: list[str] = []
    if len(bars) != CONFIG.expected_rows:
        problems.append(f"row count {len(bars)} != {CONFIG.expected_rows}")
        return problems
    first = bars[0].open_time.astimezone(UTC)
    last = bars[-1].open_time.astimezone(UTC)
    if first != CONFIG.analysis_start_utc:
        problems.append(f"first open {iso_utc(first)}")
    if last != CONFIG.analysis_end_utc:
        problems.append(f"last open {iso_utc(last)}")
    counts: dict[str, int] = defaultdict(int)
    seen: set[datetime] = set()
    previous: datetime | None = None
    for bar in bars:
        opened = bar.open_time.astimezone(UTC)
        if opened.tzinfo is None or opened.utcoffset() != timedelta(0):
            problems.append(f"naive or non-UTC open at row {bar.row}")
            break
        if opened in seen:
            problems.append(f"duplicate open {iso_utc(opened)}")
            break
        seen.add(opened)
        if previous is not None and opened - previous != HOUR:
            problems.append(f"missing or irregular hour before {iso_utc(opened)}")
            break
        previous = opened
        if bar.open is None or bar.high is None or bar.low is None or bar.close is None:
            problems.append(f"null OHLC at row {bar.row}")
            break
        if bar.high < bar.low or bar.high < bar.open or bar.high < bar.close or bar.low > bar.open or bar.low > bar.close:
            problems.append(f"OHLC inconsistency at row {bar.row}")
            break
        if bar.volume < 0 or bar.quote_volume < 0 or bar.taker_buy_base < 0 or bar.taker_buy_quote < 0:
            problems.append(f"negative volume at row {bar.row}")
            break
        counts[month_key(opened)] += 1
        if opened.year != 2026:
            problems.append(f"non-2026 candle {iso_utc(opened)}")
            break
    for key, expected in EXPECTED_MONTHLY_ROWS.items():
        if counts.get(key, 0) != expected:
            problems.append(f"{key} count {counts.get(key, 0)} != {expected}")
    expected_close = close_time_of(bars[-1].open_time)
    if bars[-1].close_time.astimezone(UTC) != expected_close.astimezone(UTC):
        problems.append("last close time is not open + 1 hour - 1 millisecond")
    return problems
