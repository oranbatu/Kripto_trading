"""1:2 R trade simulation. Swing signals come only from the existing detector engine."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from statistics import median

from detectors.swing_open_close_30m_special_v1.config import DETECTOR_VERSION, SWING_HIGH, SWING_LOW
from detectors.swing_open_close_30m_special_v1.engine import Analysis, analyze
from strategies.take_position_1_to_2_v1.config import (
    COLLISION_POLICY,
    CONFIGURATION_VERSION,
    ENTRY_EXECUTION_MODE,
    STRATEGY_VERSION,
    TARGET_R,
)
from strategies.take_position_1_to_2_v1.models import Minute, Trade

D100 = Decimal(100)
ONE = Decimal(1)
TWO = Decimal(2)
BREAK_EVEN = D100 / Decimal(3)


class TakePositionError(RuntimeError):
    """A hard backtest or reconciliation failure."""


def signal_rows(result: Analysis, not_before: datetime | None = None) -> list[tuple]:
    """Identity rows used to compare two detector runs. Display IDs are omitted."""
    rows = []
    for item in result.displayed:
        opened = result.bars[item.open_row]
        if not_before is not None and opened.open_time < not_before:
            continue
        closed = result.bars[item.close_row]
        rows.append((
            opened.open_time,
            closed.open_time,
            closed.close_time,
            item.direction,
            item.close_price,
            item.peak_2_price,
            item.dip_2_price,
            item.status,
        ))
    return rows


def equivalent_signals(left: list[tuple], right: list[tuple]) -> bool:
    return left == right


def legacy_one_to_one_target(entry: Decimal, stop: Decimal) -> Decimal:
    """The retired 1:1 formula, retained only so this package can reject it."""
    return (entry * 2) - stop


def require_two_r(entry: Decimal, stop: Decimal, target: Decimal, risk: Decimal) -> Decimal:
    reward = abs(target - entry)
    if risk <= 0 or reward != risk * TWO or reward / risk != TWO:
        raise TakePositionError("target is not exact 2R")
    if target == legacy_one_to_one_target(entry, stop):
        raise TakePositionError("legacy 1:1 target was used")
    if target != (entry * 3) - (stop * 2):
        raise TakePositionError("target does not match 3*entry - 2*stop")
    return reward


def trade_geometry(direction: str, entry: Decimal, peak_2: Decimal | None, dip_2: Decimal | None):
    """Return stop, target, risk, and reward, or an invalid-reason code."""
    if direction == "SHORT":
        if peak_2 is None:
            return None, "MISSING_PEAK_2"
        if peak_2 <= entry:
            return None, "INVALID_SHORT_STOP_NOT_ABOVE_ENTRY"
        risk = peak_2 - entry
        reward = risk * TWO
        target = entry - reward
        if risk <= 0 or not (target < entry < peak_2):
            return None, "INVALID_SHORT_STOP_NOT_ABOVE_ENTRY"
        require_two_r(entry, peak_2, target, risk)
        return (peak_2, target, risk, reward), ""
    if dip_2 is None:
        return None, "MISSING_DIP_2"
    if dip_2 >= entry:
        return None, "INVALID_LONG_STOP_NOT_BELOW_ENTRY"
    risk = entry - dip_2
    reward = risk * TWO
    target = entry + reward
    if risk <= 0 or not (dip_2 < entry < target):
        return None, "INVALID_LONG_STOP_NOT_BELOW_ENTRY"
    require_two_r(entry, dip_2, target, risk)
    return (dip_2, target, risk, reward), ""


def first_exit_index(open_times: list[datetime], signal_time: datetime) -> int:
    low = 0
    high = len(open_times)
    while low < high:
        mid = (low + high) // 2
        if open_times[mid] <= signal_time:
            low = mid + 1
        else:
            high = mid
    return low


def _duration_minutes(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() // 60)


def _excursions(direction: str, entry: Decimal, risk: Decimal, window: list[Minute]):
    if not window:
        return None, None, None, None
    highest = max(item.high for item in window)
    lowest = min(item.low for item in window)
    if direction == "SHORT":
        favorable = entry - lowest
        adverse = highest - entry
    else:
        favorable = highest - entry
        adverse = entry - lowest
    return favorable, adverse, favorable / risk, adverse / risk


def _remaining(direction: str, last: Decimal, stop: Decimal, target: Decimal) -> tuple[Decimal, Decimal]:
    if direction == "SHORT":
        return stop - last, last - target
    return last - stop, target - last


def _resolve_bar(direction: str, entry: Decimal, stop: Decimal, target: Decimal, risk: Decimal, candle: Minute) -> dict | None:
    if direction == "SHORT":
        if candle.open >= stop:
            fill = max(stop, candle.open)
            return {
                "outcome": "LOSS",
                "group": "LOSS",
                "reason": "STOP_LOSS_GAP",
                "fill": fill,
                "realized": (entry - fill) / risk,
                "ambiguity": False,
                "stop_gap": True,
                "target_gap": False,
            }
        if candle.open <= target:
            return {
                "outcome": "WIN",
                "group": "WIN",
                "reason": "TAKE_PROFIT_GAP",
                "fill": target,
                "realized": TWO,
                "ambiguity": False,
                "stop_gap": False,
                "target_gap": True,
            }
        stop_hit = candle.high >= stop
        target_hit = candle.low <= target
    else:
        if candle.open <= stop:
            fill = min(stop, candle.open)
            return {
                "outcome": "LOSS",
                "group": "LOSS",
                "reason": "STOP_LOSS_GAP",
                "fill": fill,
                "realized": (fill - entry) / risk,
                "ambiguity": False,
                "stop_gap": True,
                "target_gap": False,
            }
        if candle.open >= target:
            return {
                "outcome": "WIN",
                "group": "WIN",
                "reason": "TAKE_PROFIT_GAP",
                "fill": target,
                "realized": TWO,
                "ambiguity": False,
                "stop_gap": False,
                "target_gap": True,
            }
        stop_hit = candle.low <= stop
        target_hit = candle.high >= target
    if stop_hit and target_hit:
        if COLLISION_POLICY != "STOP_LOSS_FIRST":
            raise TakePositionError("unexpected same-minute collision policy")
        return {
            "outcome": "LOSS_AMBIGUOUS",
            "group": "LOSS",
            "reason": "AMBIGUOUS_BOTH_TOUCHED_SAME_1M",
            "fill": stop,
            "realized": -ONE,
            "ambiguity": True,
            "stop_gap": False,
            "target_gap": False,
        }
    if stop_hit:
        return {
            "outcome": "LOSS",
            "group": "LOSS",
            "reason": "STOP_LOSS",
            "fill": stop,
            "realized": -ONE,
            "ambiguity": False,
            "stop_gap": False,
            "target_gap": False,
        }
    if target_hit:
        return {
            "outcome": "WIN",
            "group": "WIN",
            "reason": "TAKE_PROFIT",
            "fill": target,
            "realized": TWO,
            "ambiguity": False,
            "stop_gap": False,
            "target_gap": False,
        }
    return None


def scan_trade(direction: str, signal_time: datetime, entry: Decimal, stop: Decimal, target: Decimal, risk: Decimal, minutes: list[Minute], open_times: list[datetime]) -> dict:
    start = first_exit_index(open_times, signal_time)
    if start >= len(minutes):
        return {"outcome": "UNENTERED_CENSORED", "group": "CENSORED", "reason": "NO_POST_CONFIRMATION_DATA", "censored": True}
    for index in range(start, len(minutes)):
        if minutes[index].open_time <= signal_time:
            raise TakePositionError("exit scan entered the confirmation candle")
        resolved = _resolve_bar(direction, entry, stop, target, risk, minutes[index])
        if resolved is None:
            continue
        window = minutes[start:index + 1]
        favorable, adverse, favorable_r, adverse_r = _excursions(direction, entry, risk, window)
        resolved.update({
            "exit_time": minutes[index].open_time,
            "censored": False,
            "mfe_price": favorable,
            "mae_price": adverse,
            "mfe_r": favorable_r,
            "mae_r": adverse_r,
            "duration": _duration_minutes(signal_time, minutes[index].open_time),
        })
        return resolved
    last = minutes[-1]
    favorable, adverse, favorable_r, adverse_r = _excursions(direction, entry, risk, minutes[start:])
    stop_left, target_left = _remaining(direction, last.close, stop, target)
    return {
        "outcome": "OPEN_CENSORED",
        "group": "CENSORED",
        "reason": "DATASET_END",
        "censored": True,
        "exit_time": None,
        "fill": None,
        "realized": None,
        "ambiguity": False,
        "stop_gap": False,
        "target_gap": False,
        "last_price": last.close,
        "last_time": last.open_time,
        "distance_to_stop": stop_left,
        "distance_to_target": target_left,
        "mfe_price": favorable,
        "mae_price": adverse,
        "mfe_r": favorable_r,
        "mae_r": adverse_r,
        "duration": _duration_minutes(signal_time, last.open_time),
    }


def build_trades(result: Analysis, minutes: list[Minute]) -> list[Trade]:
    """One independent trade candidate per confirmed Primary swing."""
    open_times = [item.open_time for item in minutes]
    for index in range(1, len(open_times)):
        if open_times[index] <= open_times[index - 1]:
            raise TakePositionError("1m candles are not strictly chronological")
    seen = set()
    trades: list[Trade] = []
    ordered = sorted(
        result.displayed,
        key=lambda item: (
            result.bars[item.close_row].close_time,
            result.bars[item.open_row].open_time,
            item.direction,
            item.swing_id,
        ),
    )
    for item in ordered:
        if item.status != "PRIMARY":
            raise TakePositionError("displayed detector result is not Primary")
        opened = result.bars[item.open_row]
        closed = result.bars[item.close_row]
        signal_time = closed.close_time
        identity = (item.swing_id, item.direction, opened.open_time, signal_time)
        if identity in seen:
            raise TakePositionError("duplicate trade")
        seen.add(identity)
        entry = item.close_price
        if entry != closed.close:
            raise TakePositionError("entry price is not Swing Close Close")
        if item.direction == SWING_HIGH:
            direction = "SHORT"
        elif item.direction == SWING_LOW:
            direction = "LONG"
        else:
            raise TakePositionError(f"unexpected swing type {item.direction}")
        peak = item.peak_2_price if direction == "SHORT" else None
        dip = item.dip_2_price if direction == "LONG" else None
        geometry, reason = trade_geometry(direction, entry, item.peak_2_price, item.dip_2_price)
        if geometry is None:
            trades.append(Trade(
                trade_id="",
                swing_id=item.swing_id,
                swing_type=item.direction,
                direction=direction,
                signal_time=signal_time,
                entry_time=None,
                entry_price=None,
                stop_price=None,
                target_price=None,
                risk_price=None,
                reward_price=None,
                risk_percent=None,
                reward_percent=None,
                peak_2_high=peak,
                dip_2_low=dip,
                outcome="INVALID",
                outcome_group="INVALID",
                exit_reason=reason,
                open_row=item.open_row,
                close_row=item.close_row,
            ))
            continue
        stop, target, risk, reward = geometry
        scanned = scan_trade(direction, signal_time, entry, stop, target, risk, minutes, open_times)
        entered = scanned["outcome"] != "UNENTERED_CENSORED"
        trades.append(Trade(
            trade_id="",
            swing_id=item.swing_id,
            swing_type=item.direction,
            direction=direction,
            signal_time=signal_time,
            entry_time=signal_time if entered else None,
            entry_price=entry,
            stop_price=stop,
            target_price=target,
            risk_price=risk,
            reward_price=reward,
            risk_percent=risk / entry * D100,
            reward_percent=reward / entry * D100,
            peak_2_high=peak,
            dip_2_low=dip,
            outcome=scanned["outcome"],
            outcome_group=scanned["group"],
            exit_reason=scanned["reason"],
            exit_time=scanned.get("exit_time"),
            exit_price=scanned.get("fill"),
            realized_r=scanned.get("realized"),
            duration_minutes=scanned.get("duration"),
            intrabar_ambiguity=bool(scanned.get("ambiguity")),
            stop_gap=bool(scanned.get("stop_gap")),
            take_profit_gap=bool(scanned.get("target_gap")),
            mfe_price=scanned.get("mfe_price"),
            mae_price=scanned.get("mae_price"),
            mfe_r=scanned.get("mfe_r"),
            mae_r=scanned.get("mae_r"),
            censored=bool(scanned.get("censored")),
            censoring_reason=scanned["reason"] if scanned.get("censored") else "",
            last_price=scanned.get("last_price"),
            last_available_time=scanned.get("last_time"),
            distance_to_stop=scanned.get("distance_to_stop"),
            distance_to_target=scanned.get("distance_to_target"),
            open_row=item.open_row,
            close_row=item.close_row,
        ))
    for index, trade in enumerate(trades, start=1):
        trade.trade_id = f"T30-{index:06d}"
    return trades


def annotate_overlap(trades: list[Trade], dataset_end: datetime) -> dict:
    intervals = []
    for trade in trades:
        if trade.entry_time is None:
            continue
        end = trade.exit_time if trade.exit_time is not None else dataset_end
        intervals.append((trade, trade.entry_time, end))
    maximum = 0
    running = 0
    events = []
    for _trade, start, end in intervals:
        events.append((start, 1))
        events.append((end, -1))
    for _moment, delta in sorted(events, key=lambda item: (item[0], item[1])):
        running += delta
        maximum = max(maximum, running)
    same = 0
    opposite = 0
    overlapping = 0
    for trade, start, end in intervals:
        others = [
            other for other, other_start, other_end in intervals
            if other is not trade and other_start < end and other_end > start
        ]
        active_at_entry = [
            other for other, other_start, other_end in intervals
            if other is not trade and other_start <= start < other_end
        ]
        trade.concurrent_at_entry = len(active_at_entry) + 1
        trade.overlapping = bool(others)
        if others:
            overlapping += 1
            if any(other.direction == trade.direction for other in others):
                same += 1
            if any(other.direction != trade.direction for other in others):
                opposite += 1
    return {
        "overlapping_trade_count": overlapping,
        "maximum_simultaneously_open": maximum,
        "same_direction_overlap_count": same,
        "opposite_direction_overlap_count": opposite,
    }


def _average(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal(0)) / Decimal(len(values))


def _median(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(median(values))


def _hours(trade: Trade) -> Decimal | None:
    if trade.duration_minutes is None:
        return None
    return Decimal(trade.duration_minutes) / Decimal(60)


def summarize(trades: list[Trade]) -> dict:
    resolved = [trade for trade in trades if trade.outcome in {"WIN", "LOSS", "LOSS_AMBIGUOUS"}]
    wins = [trade for trade in resolved if trade.outcome == "WIN"]
    losses = [trade for trade in resolved if trade.outcome == "LOSS"]
    ambiguous = [trade for trade in resolved if trade.outcome == "LOSS_AMBIGUOUS"]
    loss_family = losses + ambiguous
    valid = [trade for trade in trades if trade.outcome != "INVALID"]
    entered = [trade for trade in valid if trade.outcome != "UNENTERED_CENSORED"]
    hours = [value for value in (_hours(trade) for trade in resolved) if value is not None]
    take_profit_hours = [value for value in (_hours(trade) for trade in wins) if value is not None]
    stop_hours = [value for value in (_hours(trade) for trade in loss_family) if value is not None]
    risks = [trade.risk_percent for trade in entered if trade.risk_percent is not None]
    rewards = [trade.reward_percent for trade in entered if trade.reward_percent is not None]
    realized = [trade.realized_r for trade in resolved if trade.realized_r is not None]
    count = Decimal(len(resolved))
    win_rate = None if not resolved else Decimal(len(wins)) / count * D100

    return {
        "eligible_signal_count": len(trades),
        "valid_trade_count": len(valid),
        "invalid_trade_count": sum(1 for trade in trades if trade.outcome == "INVALID"),
        "entered_trade_count": len(entered),
        "resolved_trade_count": len(resolved),
        "win_count": len(wins),
        "loss_count": len(losses),
        "ambiguous_loss_count": len(ambiguous),
        "open_censored_count": sum(1 for trade in trades if trade.outcome == "OPEN_CENSORED"),
        "unentered_censored_count": sum(1 for trade in trades if trade.outcome == "UNENTERED_CENSORED"),
        "win_rate_percent": win_rate,
        "loss_rate_percent": None if not resolved else Decimal(len(losses) + len(ambiguous)) / count * D100,
        "break_even_win_rate_percent": BREAK_EVEN,
        "win_rate_minus_break_even_percentage_points": None if win_rate is None else win_rate - BREAK_EVEN,
        "total_gross_r": sum(realized, Decimal(0)) if realized else Decimal(0),
        "average_gross_r": _average(realized),
        "median_gross_r": _median(realized),
        "gross_expectancy_r": _average(realized),
        "average_risk_percent": _average(risks),
        "median_risk_percent": _median(risks),
        "minimum_risk_percent": min(risks) if risks else None,
        "maximum_risk_percent": max(risks) if risks else None,
        "average_reward_percent": _average(rewards),
        "average_hours_to_exit": _average(hours),
        "median_hours_to_exit": _median(hours),
        "minimum_hours_to_exit": min(hours) if hours else None,
        "maximum_hours_to_exit": max(hours) if hours else None,
        "average_hours_to_take_profit": _average(take_profit_hours),
        "median_hours_to_take_profit": _median(take_profit_hours),
        "average_hours_to_stop_loss": _average(stop_hours),
        "median_hours_to_stop_loss": _median(stop_hours),
        "fastest_take_profit_hours": min(take_profit_hours) if take_profit_hours else None,
        "slowest_take_profit_hours": max(take_profit_hours) if take_profit_hours else None,
        "fastest_stop_loss_hours": min(stop_hours) if stop_hours else None,
        "slowest_stop_loss_hours": max(stop_hours) if stop_hours else None,
        "same_1m_collision_count": sum(1 for trade in trades if trade.intrabar_ambiguity),
        "stop_gap_count": sum(1 for trade in trades if trade.stop_gap),
        "take_profit_gap_count": sum(1 for trade in trades if trade.take_profit_gap),
        "overlap_count": sum(1 for trade in trades if trade.overlapping),
    }


def grouped_summary(trades: list[Trade]) -> dict[str, dict]:
    return {
        "ALL TRADES": summarize(trades),
        "SWING HIGH SHORTS": summarize([trade for trade in trades if trade.direction == "SHORT"]),
        "SWING LOW LONGS": summarize([trade for trade in trades if trade.direction == "LONG"]),
    }


def assert_trade_invariants(trades: list[Trade]) -> None:
    seen = set()
    for trade in trades:
        identity = (trade.swing_id, trade.swing_type, trade.signal_time)
        if identity in seen:
            raise TakePositionError("duplicate_trade_count")
        seen.add(identity)
        if trade.outcome == "INVALID":
            if trade.entry_price is not None:
                raise TakePositionError("invalid signal was entered")
            continue
        if trade.entry_time is not None and trade.entry_time != trade.signal_time:
            raise TakePositionError("entry is not the confirmation close")
        if trade.exit_time is not None and trade.exit_time <= trade.signal_time:
            raise TakePositionError("exit scan included the confirmation candle")
        if trade.risk_price is None or trade.risk_price <= 0 or trade.reward_price != trade.risk_price * TWO:
            raise TakePositionError("reward is not 2R")
        if trade.target_price == legacy_one_to_one_target(trade.entry_price, trade.stop_price):
            raise TakePositionError("legacy 1:1 target was used")
        if trade.direction == "SHORT":
            if trade.stop_price != trade.peak_2_high or trade.dip_2_low is not None:
                raise TakePositionError("short stop is not Peak 2 High")
            if trade.risk_price != trade.stop_price - trade.entry_price:
                raise TakePositionError("short risk is wrong")
            if trade.target_price != trade.entry_price - trade.reward_price:
                raise TakePositionError("short target is wrong")
        else:
            if trade.stop_price != trade.dip_2_low or trade.peak_2_high is not None:
                raise TakePositionError("long stop is not Dip 2 Low")
            if trade.risk_price != trade.entry_price - trade.stop_price:
                raise TakePositionError("long risk is wrong")
            if trade.target_price != trade.entry_price + trade.reward_price:
                raise TakePositionError("long target is wrong")
        if abs(trade.target_price - trade.entry_price) / trade.risk_price != TWO:
            raise TakePositionError("risk-to-reward is not 1:2")
        if trade.outcome == "WIN" and trade.realized_r != TWO:
            raise TakePositionError("winning fill is not +2R")
        if trade.outcome in {"LOSS", "LOSS_AMBIGUOUS"} and not trade.stop_gap and trade.realized_r != -ONE:
            raise TakePositionError("normal loss is not -1R")
        if trade.stop_gap and trade.realized_r is not None and trade.realized_r > -ONE:
            raise TakePositionError("stop gap was capped inside -1R")
    if TARGET_R != TWO:
        raise TakePositionError("configured target is not 2R")
    if DETECTOR_VERSION != "BTCUSDT_30M_SPECIAL_SWING_V1":
        raise TakePositionError("detector version string changed")
    if STRATEGY_VERSION != "BTCUSDT_30M_SPECIAL_SWING_TAKE_POSITION_1_TO_2R_V1":
        raise TakePositionError("strategy version mismatch")
    if CONFIGURATION_VERSION != "CONFIRMATION_CLOSE_ENTRY_PEAK2_DIP2_STOP_GROSS_1_TO_2R_V1":
        raise TakePositionError("configuration version mismatch")
    if ENTRY_EXECUTION_MODE != "CONFIRMATION_CANDLE_CLOSE":
        raise TakePositionError("entry mode mismatch")


def trade_signature(trades: list[Trade]) -> str:
    import hashlib
    import json

    payload = [
        {
            "id": trade.trade_id,
            "swing": trade.swing_id,
            "direction": trade.direction,
            "signal": trade.signal_time.isoformat(),
            "outcome": trade.outcome,
            "entry": None if trade.entry_price is None else format(trade.entry_price, "f"),
            "stop": None if trade.stop_price is None else format(trade.stop_price, "f"),
            "target": None if trade.target_price is None else format(trade.target_price, "f"),
            "reward": None if trade.reward_price is None else format(trade.reward_price, "f"),
            "r": None if trade.realized_r is None else format(trade.realized_r, "f"),
            "exit": None if trade.exit_time is None else trade.exit_time.isoformat(),
        }
        for trade in trades
    ]
    return hashlib.sha256(json.dumps(payload).encode("utf-8")).hexdigest()


__all__ = ["analyze", "assert_trade_invariants", "build_trades"]
