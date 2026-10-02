"""Explicit exceptions for the read-only market-data access layer."""
from __future__ import annotations


class MarketDataError(Exception):
    """Base error for catalog, load, validation, and alignment failures."""


class UnsupportedSymbolError(MarketDataError):
    """The requested symbol is not in the supported USD-M perpetual set."""


class UnsupportedTimeframeError(MarketDataError):
    """The requested timeframe is not a supported verified dataset."""


class InvalidTimeRangeError(MarketDataError):
    """Start/end are missing, inverted, empty, or otherwise unusable."""


class NaiveDatetimeError(InvalidTimeRangeError):
    """A datetime lacked timezone information and was not interpreted locally."""


class PartitionNotFoundError(MarketDataError):
    """A required Hive partition file is missing."""


class SchemaMismatchError(MarketDataError):
    """On-disk schema, metadata, or requested columns do not match expectations."""


class DataGapError(MarketDataError):
    """Loaded candles contain an unexpected internal timestamp gap."""


class DuplicateTimestampError(MarketDataError):
    """Duplicate open_time values were found in a loaded interval."""


class OutOfOrderDataError(MarketDataError):
    """Rows are not strictly increasing by open_time."""


class InsufficientWarmupError(MarketDataError):
    """require_full_warmup=True but fewer prior completed candles exist."""


class LookaheadViolationError(MarketDataError):
    """An availability mapping would expose an unfinished higher-timeframe candle."""


class SourceMutationError(MarketDataError):
    """A source Parquet file or report changed SHA-256, size, or mtime_ns."""


class EmptyRangeError(InvalidTimeRangeError):
    """The half-open range contains no candles for the requested symbol/timeframe."""


class ColumnProjectionError(SchemaMismatchError):
    """A requested column is not part of the analytical candle interface."""
