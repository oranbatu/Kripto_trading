"""Immutable in-memory models. These never write back to Parquet."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any, Mapping

import pyarrow as pa

import config
from data.timeutil import datetime_to_ms, iso_z, ms_to_datetime


def _proxy(data: Mapping[str, Any] | None) -> MappingProxyType[str, Any]:
    return MappingProxyType(dict(data or {}))


@dataclass(frozen=True, slots=True)
class CandleSlice:
    symbol: str
    timeframe: str
    table: pa.Table
    requested_start_utc: datetime
    requested_end_utc: datetime
    loaded_start_utc: datetime | None
    loaded_end_utc: datetime | None
    warmup_bars_requested: int
    warmup_bars_loaded: int
    trade_start_index: int
    precision_mode: str
    source_files: tuple[str, ...]
    source_file_hashes: Mapping[str, str]
    validation_summary: Mapping[str, Any]
    schema_version: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_file_hashes", _proxy(self.source_file_hashes))
        object.__setattr__(self, "validation_summary", _proxy(self.validation_summary))

    @property
    def row_count(self) -> int:
        return self.table.num_rows

    @property
    def trade_row_count(self) -> int:
        return max(0, self.row_count - self.trade_start_index)

    def warmup_mask(self) -> pa.BooleanArray:
        n = self.row_count
        k = self.trade_start_index
        return pa.array([i < k for i in range(n)], type=pa.bool_())

    def trade_table(self) -> pa.Table:
        return self.table.slice(self.trade_start_index)

    def warmup_table(self) -> pa.Table:
        return self.table.slice(0, self.trade_start_index)

    def open_times_ms(self) -> list[int]:
        return self.table.column("open_time").cast(pa.int64()).to_pylist()

    def close_times_ms(self) -> list[int]:
        return self.table.column("close_time").cast(pa.int64()).to_pylist()

    def index_of_open(self, open_utc: datetime) -> int:
        target = datetime_to_ms(open_utc)
        opens = self.open_times_ms()
        try:
            return opens.index(target)
        except ValueError as exc:
            raise KeyError(f"{self.symbol} {self.timeframe} has no candle opening at {iso_z(open_utc)}") from exc

    def provenance(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "schema_version": self.schema_version,
            "precision_mode": self.precision_mode,
            "requested_start_utc": iso_z(self.requested_start_utc),
            "requested_end_utc": iso_z(self.requested_end_utc),
            "loaded_start_utc": iso_z(self.loaded_start_utc) if self.loaded_start_utc else None,
            "loaded_end_utc": iso_z(self.loaded_end_utc) if self.loaded_end_utc else None,
            "row_count": self.row_count,
            "warmup_bars_requested": self.warmup_bars_requested,
            "warmup_bars_loaded": self.warmup_bars_loaded,
            "trade_start_index": self.trade_start_index,
            "source_files": list(self.source_files),
            "source_file_hashes": dict(self.source_file_hashes),
        }


@dataclass(frozen=True, slots=True)
class MultiTimeframeData:
    symbol: str
    base_timeframe: str
    timeframes: tuple[str, ...]
    slices: Mapping[str, CandleSlice]
    decision_clock: str
    availability: Mapping[str, tuple[int, ...]]
    requested_start_utc: datetime
    requested_end_utc: datetime
    validation_summary: Mapping[str, Any]
    source_files: tuple[str, ...]
    source_file_hashes: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "slices", _proxy(self.slices))
        object.__setattr__(self, "availability", _proxy(self.availability))
        object.__setattr__(self, "validation_summary", _proxy(self.validation_summary))
        object.__setattr__(self, "source_file_hashes", _proxy(self.source_file_hashes))

    def slice_for(self, timeframe: str) -> CandleSlice:
        return self.slices[timeframe]

    def available_index(self, timeframe: str, base_row: int) -> int:
        return self.availability[timeframe][base_row]

    def available_open_utc(self, timeframe: str, base_row: int) -> datetime | None:
        idx = self.available_index(timeframe, base_row)
        if idx == config.UNAVAILABLE_INDEX:
            return None
        ms = self.slices[timeframe].open_times_ms()[idx]
        return ms_to_datetime(ms)
