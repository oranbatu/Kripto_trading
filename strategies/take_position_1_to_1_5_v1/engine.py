"""1:1.5 R trade simulation. Signals come only from the existing detector engine."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from statistics import median

from detectors.swing_open_close_30m_special_v1.config import DETECTOR_VERSION, SWING_HIGH, SWING_LOW
from detectors.swing_open_close_30m_special_v1.engine import Analysis, analyze
from strategies.take_position_1_to_1_5_v1.config import (
    BREAK_EVEN_PERCENT,
    BREAK_EVEN_RATE,
    CONFIGURATION_VERSION,
    REWARD_MULTIPLE,
    RISK_MULTIPLE,
    RISK_REWARD_RATIO,
    SAME_BAR_POLICY,
    STRATEGY_NAME,
)
from strategies.take_position_1_to_1_5_v1.models import Minute, Trade

D100 = Decimal(100)
ONE = Decimal(1)
REWARD = REWARD_MULTIPLE
ONE_POINT_FIVE = REWARD
TWO_POINT_FIVE = Decimal(5) / Decimal(2)
RESOLVED_OUTCOMES = {"TAKE_PROFIT", "STOP_LOSS"}


class TakePositionError(RuntimeError):
    """A hard backtest or reconciliation failure."""


def signal_rows(result: Analysis, not_before: datetime | None = None) -> list[tuple]:
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


def one_to_one_target(entry: Decimal, stop: Decimal) -> Decimal:
    return (entry * 2) - stop


def one_to_two_target(entry: Decimal, stop: Decimal) -> Decimal:
    return (entry * 3) - (stop * 2)


def one_to_one_point_five_target(entry: Decimal, stop: Decimal) -> Decimal:
    return (TWO_POINT_FIVE * entry) - (ONE_POINT_FIVE * stop)


def require_one_point_five(entry: Decimal, stop: Decimal, target: Decimal, risk: Decimal) -> Decimal:
    reward = abs(target - entry)
    if risk <= 0 or reward != risk * ONE_POINT_FIVE or reward / risk != ONE_POINT_FIVE:
        raise TakePositionError("target is not exact 1.5R")
    if target == one_to_one_target(entry, stop):
        raise TakePositionError("legacy 1:1 target was used")
    if target == one_to_two_target(entry, stop):
        raise TakePositionError("legacy 1:2 target was used")
    if target != one_to_one_point_five_target(entry, stop):
        raise TakePositionError("target does not match 2.5*entry - 1.5*stop")
    return reward


def configured_break_even() -> Decimal:
    """Return the 40% gross break-even rate and reject the 1:1 and 1:2 thresholds."""
    if REWARD != Decimal("1.5") or RISK_MULTIPLE != ONE:
        raise TakePositionError("reward multiple is not 1.5")
    if BREAK_EVEN_RATE != Decimal("0.4") or BREAK_EVEN_PERCENT != Decimal(40):
        raise TakePositionError("break-even is not 40 percent")
    if BREAK_EVEN_RATE == Decimal("0.5") or BREAK_EVEN_RATE == Decimal(1) / Decimal(3):
        raise TakePositionError("break-even collapsed to a 1:1 or 1:2 threshold")
    if Decimal(1) / (Decimal(1) + ONE) == BREAK_EVEN_RATE:
        raise TakePositionError("50 percent break-even was accepted")
    if Decimal(1) / (Decimal(1) + Decimal(2)) == BREAK_EVEN_RATE:
        raise TakePositionError("33.333 percent break-even was accepted")
    return BREAK_EVEN_PERCENT


def trade_geometry(direction: str, entry: Decimal, peak_2: Decimal | None, dip_2: Decimal | None):
    if direction == "SHORT":
        if peak_2 is None:
            return None, "MISSING_PEAK_2"
        if peak_2 <= entry:
            return None, "INVALID_SHORT_STOP_NOT_ABOVE_ENTRY"
        risk = peak_2 - entry
        if risk <= 0:
            return None, "ZERO_OR_NEGATIVE_RISK"
        reward = risk * ONE_POINT_FIVE
        target = entry - reward
        if not (target < entry < peak_2):
            return None, "INVALID_SHORT_TARGET"
        require_one_point_five(entry, peak_2, target, risk)
        return (peak_2, target, risk, reward), ""
    if dip_2 is None:
        return None, "MISSING_DIP_2"
    if dip_2 >= entry:
        return None, "INVALID_LONG_STOP_NOT_BELOW_ENTRY"
    risk = entry - dip_2
    if risk <= 0:
        return None, "ZERO_OR_NEGATIVE_RISK"
    reward = risk * ONE_POINT_FIVE
    target = entry + reward
    if not (dip_2 < entry < target):
        return None, "INVALID_LONG_TARGET"
    require_one_point_five(entry, dip_2, target, risk)
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


def _price_change(direction: str, entry: Decimal, price: Decimal) -> Decimal:
    if direction == "SHORT":
        return entry - price
    return price - entry


def _resolve_bar(direction: str, entry: Decimal, stop: Decimal, target: Decimal, risk: Decimal, candle: Minute) -> dict | None:
    if direction == "SHORT":
        if candle.open >= stop:
            fill = candle.open
            return _exit("STOP_LOSS", "STOP_LOSS_GAP", fill, _price_change(direction, entry, fill) / risk, -ONE, False, "STOP_LOSS_GAP", fill != stop)
        if candle.open <= target:
            return _exit("TAKE_PROFIT", "TAKE_PROFIT_GAP", target, ONE_POINT_FIVE, ONE_POINT_FIVE, False, "TAKE_PROFIT_GAP", False)
        stop_hit = candle.high >= stop
        target_hit = candle.low <= target
    else:
        if candle.open <= stop:
            fill = candle.open
            return _exit("STOP_LOSS", "STOP_LOSS_GAP", fill, _price_change(direction, entry, fill) / risk, -ONE, False, "STOP_LOSS_GAP", fill != stop)
        if candle.open >= target:
            return _exit("TAKE_PROFIT", "TAKE_PROFIT_GAP", target, ONE_POINT_FIVE, ONE_POINT_FIVE, False, "TAKE_PROFIT_GAP", False)
        stop_hit = candle.low <= stop
        target_hit = candle.high >= target
    if stop_hit and target_hit:
        if SAME_BAR_POLICY != "STOP_FIRST_CONSERVATIVE":
            raise TakePositionError("unexpected same-minute policy")
        return _exit("STOP_LOSS", "AMBIGUOUS_BOTH_TOUCHED_SAME_1M", stop, -ONE, -ONE, True, "", False)
    if stop_hit:
        return _exit("STOP_LOSS", "STOP_LOSS", stop, -ONE, -ONE, False, "", False)
    if target_hit:
        return _exit("TAKE_PROFIT", "TAKE_PROFIT", target, ONE_POINT_FIVE, ONE_POINT_FIVE, False, "", False)
    return None


def _exit(outcome: str, reason: str, fill: Decimal, realized: Decimal, intended: Decimal, both: bool, gap_type: str, beyond: bool) -> dict:
    return {
        "outcome": outcome,
        "reason": reason,
        "fill": fill,
        "realized": realized,
        "intended": intended,
        "both": both,
        "gap_type": gap_type,
        "gap_fill": bool(gap_type),
        "beyond": beyond,
        "resolved": True,
        "censored": False,
    }


def scan_trade(direction: str, signal_time: datetime, entry: Decimal, stop: Decimal, target: Decimal, risk: Decimal, minutes: list[Minute], open_times: list[datetime]) -> dict:
    start = first_exit_index(open_times, signal_time)
    if start >= len(minutes):
        return {"outcome": "OPEN_AT_DATASET_END", "reason": "NO_POST_CONFIRMATION_DATA", "resolved": False, "censored": True}
    first_time = minutes[start].open_time
    if first_time <= signal_time:
        raise TakePositionError("exit scan entered the confirmation candle")
    for index in range(start, len(minutes)):
        if minutes[index].open_time <= signal_time:
            raise TakePositionError("exit scan entered the confirmation candle")
        resolved = _resolve_bar(direction, entry, stop, target, risk, minutes[index])
        if resolved is None:
            continue
        window = minutes[start:index + 1]
        favorable, adverse, favorable_r, adverse_r = _excursions(direction, entry, risk, window)
        change = _price_change(direction, entry, resolved["fill"])
        resolved.update({
            "exit_time": minutes[index].open_time,
            "first_time": first_time,
            "mfe_price": favorable,
            "mae_price": adverse,
            "mfe_r": favorable_r,
            "mae_r": adverse_r,
            "duration": _duration_minutes(signal_time, minutes[index].open_time),
            "bars": index - start + 1,
            "gross_percent": change / entry * D100,
        })
        return resolved
    last = minutes[-1]
    favorable, adverse, favorable_r, adverse_r = _excursions(direction, entry, risk, minutes[start:])
    change = _price_change(direction, entry, last.close)
    return {
        "outcome": "OPEN_AT_DATASET_END",
        "reason": "DATASET_END",
        "resolved": False,
        "censored": True,
        "first_time": first_time,
        "last_price": last.close,
        "last_time": last.open_time,
        "unrealized_change": change,
        "unrealized_r": change / risk,
        "gross_percent": change / entry * D100,
        "mfe_price": favorable,
        "mae_price": adverse,
        "mfe_r": favorable_r,
        "mae_r": adverse_r,
        "duration": _duration_minutes(signal_time, last.open_time),
        "bars": len(minutes) - start,
    }


def build_trades(result: Analysis, minutes: list[Minute]) -> list[Trade]:
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
        reference_row = item.peak_2_row if direction == "SHORT" else item.dip_2_row
        reference_time = None if reference_row is None else result.bars[reference_row].open_time
        peak = item.peak_2_price if direction == "SHORT" else None
        dip = item.dip_2_price if direction == "LONG" else None
        geometry, reason = trade_geometry(direction, entry, item.peak_2_price, item.dip_2_price)
        base = dict(
            trade_id="",
            swing_id=item.swing_id,
            swing_type=item.direction,
            direction=direction,
            signal_time=signal_time,
            swing_open_time=opened.open_time,
            swing_close_open_time=closed.open_time,
            peak_2_high=peak,
            dip_2_low=dip,
            stop_reference_time=reference_time,
            open_row=item.open_row,
            close_row=item.close_row,
        )
        if geometry is None:
            trades.append(Trade(
                entry_time=None,
                entry_price=None,
                stop_price=None,
                target_price=None,
                risk_price=None,
                reward_distance=None,
                risk_percent=None,
                reward_ratio=None,
                outcome="INVALID",
                exit_reason=reason,
                candidate_entry=entry,
                candidate_stop=item.peak_2_price if direction == "SHORT" else item.dip_2_price,
                **base,
            ))
            continue
        stop, target, risk, reward = geometry
        scanned = scan_trade(direction, signal_time, entry, stop, target, risk, minutes, open_times)
        entered = scanned["reason"] != "NO_POST_CONFIRMATION_DATA"
        trades.append(Trade(
            entry_time=signal_time if entered else None,
            entry_price=entry,
            stop_price=stop,
            target_price=target,
            risk_price=risk,
            reward_distance=reward,
            risk_percent=risk / entry * D100,
            reward_ratio=reward / risk,
            outcome=scanned["outcome"],
            exit_reason=scanned["reason"],
            candidate_entry=entry,
            candidate_stop=stop,
            exit_time=scanned.get("exit_time"),
            exit_price=scanned.get("fill"),
            intended_r=scanned.get("intended"),
            realized_r=scanned.get("realized"),
            unrealized_r=scanned.get("unrealized_r"),
            unrealized_change=scanned.get("unrealized_change"),
            gross_percent=scanned.get("gross_percent"),
            duration_minutes=scanned.get("duration"),
            bars_to_exit=scanned.get("bars"),
            first_eligible_time=scanned.get("first_time"),
            same_minute_both=bool(scanned.get("both")),
            gap_type=scanned.get("gap_type", ""),
            gap_fill=bool(scanned.get("gap_fill")),
            slippage_beyond_stop=bool(scanned.get("beyond")),
            mfe_price=scanned.get("mfe_price"),
            mae_price=scanned.get("mae_price"),
            mfe_r=scanned.get("mfe_r"),
            mae_r=scanned.get("mae_r"),
            resolved=bool(scanned.get("resolved")),
            censored=bool(scanned.get("censored")),
            censoring_reason=scanned["reason"] if scanned.get("censored") else "",
            last_price=scanned.get("last_price"),
            last_time=scanned.get("last_time"),
            **base,
        ))
    for index, trade in enumerate(trades, start=1):
        trade.trade_id = f"T30-{index:06d}"
    return trades


def annotate_overlap(trades: list[Trade], dataset_end: datetime) -> dict:
    intervals = []
    for trade in trades:
        if trade.entry_time is None or trade.outcome == "INVALID":
            continue
        end = trade.exit_time if trade.exit_time is not None else dataset_end
        intervals.append((trade, trade.entry_time, end))
    maximum = 0
    running = 0
    events = [(start, 1) for _trade, start, end in intervals] + [(end, -1) for _trade, start, end in intervals]
    for _moment, delta in sorted(events, key=lambda item: (item[0], item[1])):
        running += delta
        maximum = max(maximum, running)
    overlapping = 0
    for trade, start, end in intervals:
        others = [other for other, other_start, other_end in intervals if other is not trade and other_start < end and other_end > start]
        active = [other for other, other_start, other_end in intervals if other is not trade and other_start <= start < other_end]
        trade.concurrent_at_entry = len(active) + 1
        trade.overlapping = bool(others)
        overlapping += int(bool(others))
    return {"overlapping_trade_count": overlapping, "maximum_simultaneously_open": maximum}


def _average(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal(0)) / Decimal(len(values))


def _median(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(median(values))


def cumulative_r(trades: list[Trade]) -> dict:
    ordered = sorted(
        (trade for trade in trades if trade.resolved and trade.realized_r is not None),
        key=lambda trade: (trade.entry_time, trade.swing_id, trade.trade_id),
    )
    total = Decimal(0)
    peak = Decimal(0)
    maximum_drawdown = Decimal(0)
    curve = []
    for trade in ordered:
        total += trade.realized_r
        if total > peak:
            peak = total
        drawdown = peak - total
        if drawdown > maximum_drawdown:
            maximum_drawdown = drawdown
        curve.append(total)
    return {"total_r": total, "maximum_drawdown_r": maximum_drawdown, "points": len(curve)}


def profit_factor(realized: list[Decimal]) -> Decimal | None:
    positive = sum((value for value in realized if value > 0), Decimal(0))
    negative = sum((value for value in realized if value < 0), Decimal(0))
    if negative == 0:
        return None
    return positive / abs(negative)


def summarize(trades: list[Trade]) -> dict:
    configured_break_even()
    valid = [trade for trade in trades if trade.outcome != "INVALID"]
    resolved = [trade for trade in valid if trade.resolved]
    wins = [trade for trade in resolved if trade.outcome == "TAKE_PROFIT"]
    losses = [trade for trade in resolved if trade.outcome == "STOP_LOSS"]
    unresolved = [trade for trade in valid if not trade.resolved]
    realized = [trade.realized_r for trade in resolved if trade.realized_r is not None]
    win_r = [trade.realized_r for trade in wins if trade.realized_r is not None]
    loss_r = [trade.realized_r for trade in losses if trade.realized_r is not None]
    count = Decimal(len(resolved))
    win_rate = None if not resolved else Decimal(len(wins)) / count
    loss_rate = None if not resolved else Decimal(len(losses)) / count
    average_win = _average(win_r)
    average_loss = _average(loss_r)
    expectancy = None
    if win_rate is not None and loss_rate is not None and average_win is not None and average_loss is not None:
        expectancy = (win_rate * average_win) + (loss_rate * average_loss)
    elif win_rate is not None and average_win is not None and not losses:
        expectancy = win_rate * average_win
    elif loss_rate is not None and average_loss is not None and not wins:
        expectancy = loss_rate * average_loss
    win_minutes = [Decimal(trade.duration_minutes) for trade in wins if trade.duration_minutes is not None]
    loss_minutes = [Decimal(trade.duration_minutes) for trade in losses if trade.duration_minutes is not None]
    all_minutes = [Decimal(trade.duration_minutes) for trade in resolved if trade.duration_minutes is not None]
    curve = cumulative_r(trades)
    win_percent = None if win_rate is None else win_rate * D100
    return {
        "eligible_signal_count": len(trades),
        "valid_trade_count": len(valid),
        "invalid_signal_count": sum(1 for trade in trades if trade.outcome == "INVALID"),
        "resolved_trade_count": len(resolved),
        "unresolved_trade_count": len(unresolved),
        "take_profit_count": len(wins),
        "stop_loss_count": len(losses),
        "same_minute_ambiguous_count": sum(1 for trade in trades if trade.same_minute_both),
        "adverse_gap_stop_count": sum(1 for trade in trades if trade.gap_type == "STOP_LOSS_GAP"),
        "favorable_gap_target_count": sum(1 for trade in trades if trade.gap_type == "TAKE_PROFIT_GAP"),
        "win_rate": win_rate,
        "win_rate_percent": win_percent,
        "loss_rate_percent": None if loss_rate is None else loss_rate * D100,
        "break_even_win_rate_percent": BREAK_EVEN_PERCENT,
        "win_rate_edge_percentage_points": None if win_percent is None else win_percent - BREAK_EVEN_PERCENT,
        "average_win_r": average_win,
        "average_loss_r": average_loss,
        "median_win_r": _median(win_r),
        "median_loss_r": _median(loss_r),
        "average_r": _average(realized),
        "median_r": _median(realized),
        "total_r": curve["total_r"],
        "maximum_drawdown_r": curve["maximum_drawdown_r"],
        "profit_factor": profit_factor(realized),
        "expectancy_r": expectancy,
        "ideal_expectancy_r": None if win_rate is None else (TWO_POINT_FIVE * win_rate) - ONE,
        "average_minutes_to_take_profit": _average(win_minutes),
        "median_minutes_to_take_profit": _median(win_minutes),
        "average_minutes_to_stop_loss": _average(loss_minutes),
        "median_minutes_to_stop_loss": _median(loss_minutes),
        "minimum_minutes_to_resolution": min(all_minutes) if all_minutes else None,
        "maximum_minutes_to_resolution": max(all_minutes) if all_minutes else None,
        "average_mfe_r": _average([trade.mfe_r for trade in valid if trade.mfe_r is not None]),
        "average_mae_r": _average([trade.mae_r for trade in valid if trade.mae_r is not None]),
    }


def monthly_summary(trades: list[Trade]) -> list[dict]:
    groups: dict[str, list[Trade]] = {}
    for trade in trades:
        if trade.outcome == "INVALID" or trade.entry_time is None:
            continue
        key = trade.entry_time.strftime("%Y-%m")
        groups.setdefault(key, []).append(trade)
    rows = []
    for key in sorted(groups):
        selected = groups[key]
        summary = summarize(selected)
        rows.append({
            "month": key,
            "trades": summary["valid_trade_count"],
            "take_profit_count": summary["take_profit_count"],
            "stop_loss_count": summary["stop_loss_count"],
            "unresolved_trade_count": summary["unresolved_trade_count"],
            "win_rate_percent": summary["win_rate_percent"],
            "total_r": summary["total_r"],
        })
    return rows


def grouped_summary(trades: list[Trade]) -> dict[str, dict]:
    return {
        "ALL TRADES": summarize(trades),
        "SHORT": summarize([trade for trade in trades if trade.direction == "SHORT"]),
        "LONG": summarize([trade for trade in trades if trade.direction == "LONG"]),
    }


def assert_trade_invariants(trades: list[Trade]) -> None:
    configured_break_even()
    seen = set()
    valid = [trade for trade in trades if trade.outcome != "INVALID"]
    resolved = [trade for trade in valid if trade.resolved]
    if len(valid) + sum(1 for trade in trades if trade.outcome == "INVALID") != len(trades):
        raise TakePositionError("eligible reconciliation failed")
    if len(resolved) + sum(1 for trade in valid if not trade.resolved) != len(valid):
        raise TakePositionError("valid reconciliation failed")
    if sum(1 for trade in resolved if trade.outcome == "TAKE_PROFIT") + sum(1 for trade in resolved if trade.outcome == "STOP_LOSS") != len(resolved):
        raise TakePositionError("resolved reconciliation failed")
    if sum(1 for trade in valid if trade.direction == "SHORT") + sum(1 for trade in valid if trade.direction == "LONG") != len(valid):
        raise TakePositionError("direction reconciliation failed")
    for trade in trades:
        identity = (trade.swing_id, trade.swing_type, trade.signal_time)
        if identity in seen:
            raise TakePositionError("duplicate trade")
        seen.add(identity)
        if trade.outcome == "INVALID":
            if trade.entry_time is not None or trade.resolved:
                raise TakePositionError("invalid signal was entered")
            continue
        if trade.entry_time is not None and trade.entry_time != trade.signal_time:
            raise TakePositionError("entry is not the confirmation close")
        if trade.exit_time is not None and trade.exit_time <= trade.signal_time:
            raise TakePositionError("exit scan included the confirmation candle")
        if trade.first_eligible_time is not None and trade.first_eligible_time <= trade.signal_time:
            raise TakePositionError("first exit bar is not after confirmation")
        if trade.risk_price is None or trade.risk_price <= 0 or trade.reward_distance != trade.risk_price * ONE_POINT_FIVE:
            raise TakePositionError("reward is not 1.5R")
        if trade.reward_ratio != ONE_POINT_FIVE:
            raise TakePositionError("verified ratio is not 1.5")
        if trade.target_price in {one_to_one_target(trade.entry_price, trade.stop_price), one_to_two_target(trade.entry_price, trade.stop_price)}:
            raise TakePositionError("legacy target formula was used")
        if trade.direction == "SHORT":
            if trade.stop_price != trade.peak_2_high or trade.dip_2_low is not None:
                raise TakePositionError("short stop is not Peak 2 High")
            if trade.target_price != trade.entry_price - trade.reward_distance:
                raise TakePositionError("short target is wrong")
        else:
            if trade.stop_price != trade.dip_2_low or trade.peak_2_high is not None:
                raise TakePositionError("long stop is not Dip 2 Low")
            if trade.target_price != trade.entry_price + trade.reward_distance:
                raise TakePositionError("long target is wrong")
        if trade.outcome == "TAKE_PROFIT" and trade.realized_r != ONE_POINT_FIVE:
            raise TakePositionError("take profit is not +1.5R")
        if trade.outcome == "STOP_LOSS" and not trade.gap_fill and trade.realized_r != -ONE:
            raise TakePositionError("normal stop is not -1R")
        if trade.gap_type == "STOP_LOSS_GAP" and trade.realized_r is not None and trade.realized_r > -ONE:
            raise TakePositionError("adverse gap was capped inside -1R")
        if trade.outcome == "TAKE_PROFIT" and trade.realized_r > ONE_POINT_FIVE:
            raise TakePositionError("favorable result exceeded +1.5R")
        if not trade.resolved and trade.outcome != "OPEN_AT_DATASET_END":
            raise TakePositionError("unresolved trade was classified")
    if DETECTOR_VERSION != "BTCUSDT_30M_SPECIAL_SWING_V1":
        raise TakePositionError("detector version string changed")
    if STRATEGY_NAME != "BTCUSDT_30M_SPECIAL_SWING_TAKE_POSITION_1_TO_1_5R_V1":
        raise TakePositionError("strategy name mismatch")
    if CONFIGURATION_VERSION != "CONFIRMATION_CLOSE_ENTRY_PEAK2_DIP2_STOP_GROSS_1_TO_1_5R_V1":
        raise TakePositionError("configuration version mismatch")
    if RISK_REWARD_RATIO != "1:1.5":
        raise TakePositionError("risk-reward label mismatch")


def content_fingerprint(trades: list[Trade]) -> str:
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
            "exit": None if trade.exit_time is None else trade.exit_time.isoformat(),
            "fill": None if trade.exit_price is None else format(trade.exit_price, "f"),
            "r": None if trade.realized_r is None else format(trade.realized_r, "f"),
        }
        for trade in trades
    ]
    return hashlib.sha256(json.dumps(payload).encode("utf-8")).hexdigest()


__all__ = ["analyze", "assert_trade_invariants", "build_trades", "content_fingerprint"]
