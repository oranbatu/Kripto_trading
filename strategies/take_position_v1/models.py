"""Trade records for the 1R take-position backtest."""
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
    entry_time: datetime | None
    entry_price: Decimal | None
    stop_price: Decimal | None
    target_price: Decimal | None
    risk_price: Decimal | None
    risk_percent: Decimal | None
    peak_2_high: Decimal | None
    dip_2_low: Decimal | None
    outcome: str
    outcome_group: str
    exit_reason: str
    exit_time: datetime | None = None
    exit_price: Decimal | None = None
    realized_r: Decimal | None = None
    duration_minutes: int | None = None
    intrabar_ambiguity: bool = False
    stop_gap: bool = False
    take_profit_gap: bool = False
    mfe_price: Decimal | None = None
    mae_price: Decimal | None = None
    mfe_r: Decimal | None = None
    mae_r: Decimal | None = None
    censored: bool = False
    censoring_reason: str = ""
    last_price: Decimal | None = None
    last_available_time: datetime | None = None
    distance_to_stop: Decimal | None = None
    distance_to_target: Decimal | None = None
    overlapping: bool = False
    concurrent_at_entry: int = 0
    open_row: int = 0
    close_row: int = 0
