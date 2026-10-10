"""Trade records for the 1:1.5 R take-position backtest."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(slots=True)
class Minute:
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


@dataclass(slots=True)
class Trade:
    trade_id: str
    swing_id: str
    swing_type: str
    direction: str
    signal_time: datetime
    swing_open_time: datetime
    swing_close_open_time: datetime
    entry_time: datetime | None
    entry_price: Decimal | None
    stop_price: Decimal | None
    target_price: Decimal | None
    risk_price: Decimal | None
    reward_distance: Decimal | None
    risk_percent: Decimal | None
    reward_ratio: Decimal | None
    peak_2_high: Decimal | None
    dip_2_low: Decimal | None
    stop_reference_time: datetime | None
    outcome: str
    exit_reason: str
    candidate_entry: Decimal | None = None
    candidate_stop: Decimal | None = None
    exit_time: datetime | None = None
    exit_price: Decimal | None = None
    intended_r: Decimal | None = None
    realized_r: Decimal | None = None
    unrealized_r: Decimal | None = None
    unrealized_change: Decimal | None = None
    gross_percent: Decimal | None = None
    duration_minutes: int | None = None
    bars_to_exit: int | None = None
    first_eligible_time: datetime | None = None
    same_minute_both: bool = False
    gap_type: str = ""
    gap_fill: bool = False
    slippage_beyond_stop: bool = False
    mfe_price: Decimal | None = None
    mae_price: Decimal | None = None
    mfe_r: Decimal | None = None
    mae_r: Decimal | None = None
    resolved: bool = False
    censored: bool = False
    censoring_reason: str = ""
    last_price: Decimal | None = None
    last_time: datetime | None = None
    overlapping: bool = False
    concurrent_at_entry: int = 0
    open_row: int = 0
    close_row: int = 0
