"""Read-only Hive catalog for verified Binance USD-M perpetual Parquet datasets."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

import config
from data.exceptions import (
    PartitionNotFoundError,
    SchemaMismatchError,
    UnsupportedSymbolError,
    UnsupportedTimeframeError,
)
from data.timeutil import datetime_to_ms, iso_z, require_aware_utc, require_half_open_range

_HIVE_SYMBOL = re.compile(r"^symbol=([A-Z0-9]+)$")
_HIVE_YEAR = re.compile(r"^year=(\d{4})$")
_HIVE_MONTH = re.compile(r"^month=(\d{2})$")


@dataclass(frozen=True, slots=True)
class PartitionRef:
    symbol: str
    timeframe: str
    year: int
    month: int | None
    path: Path
    start_utc: datetime
    end_utc: datetime  # exclusive partition coverage
    schema_version: str | None = None
    interval_meta: str | None = None
    metadata: Mapping[str, str] | None = None

    def covers_open_ms(self, open_ms: int) -> bool:
        return datetime_to_ms(self.start_utc) <= open_ms < datetime_to_ms(self.end_utc)


@dataclass(frozen=True, slots=True)
class DatasetInfo:
    timeframe: str
    root: Path
    partitioning: str
    schema_version: str
    expected_files: int
    actual_files: int
    symbols: tuple[str, ...]
    partitions: tuple[PartitionRef, ...]
    extra_files: tuple[str, ...]
    missing_partitions: tuple[str, ...]
    unexpected_symbols: tuple[str, ...]
    metadata_ok: bool
    notes: tuple[str, ...]


def require_symbol(symbol: str) -> str:
    if not isinstance(symbol, str) or symbol not in config.ALLOWED_SYMBOLS:
        raise UnsupportedSymbolError(
            f"Unsupported symbol {symbol!r}. Allowed: {', '.join(config.ALLOWED_SYMBOLS)}"
        )
    return symbol


def require_timeframe(timeframe: str) -> str:
    if not isinstance(timeframe, str) or timeframe not in config.ALLOWED_TIMEFRAMES:
        raise UnsupportedTimeframeError(
            f"Unsupported timeframe {timeframe!r}. Allowed: {', '.join(config.ALLOWED_TIMEFRAMES)}"
        )
    return timeframe


def _decode_meta(schema: pa.Schema) -> dict[str, str]:
    out: dict[str, str] = {}
    if not schema.metadata:
        return out
    for key, value in schema.metadata.items():
        k = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        v = value.decode("utf-8") if isinstance(value, bytes) else str(value)
        out[k] = v
    return out


def _month_start(year: int, month: int) -> datetime:
    return datetime(year, month, 1, tzinfo=timezone.utc)


def _next_month(year: int, month: int) -> datetime:
    if month == 12:
        return datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    return datetime(year, month + 1, 1, tzinfo=timezone.utc)


def _year_start(year: int) -> datetime:
    return datetime(year, 1, 1, tzinfo=timezone.utc)


def expected_partition_keys(timeframe: str) -> list[tuple[str, int, int | None]]:
    spec = config.TIMEFRAME_SPECS[timeframe]
    keys: list[tuple[str, int, int | None]] = []
    if spec.partitioning == "symbol_year_month":
        start = spec.first_open_utc
        last = spec.last_open_utc
        year, month = start.year, start.month
        while (year, month) <= (last.year, last.month):
            for symbol in config.ALLOWED_SYMBOLS:
                keys.append((symbol, year, month))
            if month == 12:
                year, month = year + 1, 1
            else:
                month += 1
    else:
        for year in range(spec.first_open_utc.year, spec.last_open_utc.year + 1):
            for symbol in config.ALLOWED_SYMBOLS:
                keys.append((symbol, year, None))
    return keys


class DataCatalog:
    def __init__(self, data_root: Path | None = None) -> None:
        self.data_root = Path(data_root) if data_root is not None else config.data_root()

    def dataset_root(self, timeframe: str) -> Path:
        require_timeframe(timeframe)
        return config.dataset_root(timeframe, self.data_root)

    def report_path(self, timeframe: str) -> Path:
        require_timeframe(timeframe)
        return config.report_path(timeframe, self.data_root)

    def get_dataset_info(self, timeframe: str, *, inspect_metadata: bool = True) -> DatasetInfo:
        timeframe = require_timeframe(timeframe)
        spec = config.TIMEFRAME_SPECS[timeframe]
        root = self.dataset_root(timeframe)
        partitions, extra, unexpected = self._discover(timeframe, root, inspect_metadata=inspect_metadata)
        expected = expected_partition_keys(timeframe)
        have = {(p.symbol, p.year, p.month) for p in partitions}
        missing = [
            self._hive_rel(timeframe, symbol, year, month)
            for symbol, year, month in expected
            if (symbol, year, month) not in have
        ]
        symbols = tuple(sorted({p.symbol for p in partitions}))
        metadata_ok = True
        notes: list[str] = []
        if inspect_metadata:
            for part in partitions:
                if part.schema_version != spec.schema_version:
                    metadata_ok = False
                    notes.append(
                        f"{part.path}: schema_version={part.schema_version!r} expected {spec.schema_version}"
                    )
                if part.interval_meta not in (None, timeframe):
                    metadata_ok = False
                    notes.append(f"{part.path}: interval metadata {part.interval_meta!r}")
        return DatasetInfo(
            timeframe=timeframe,
            root=root,
            partitioning=spec.partitioning,
            schema_version=spec.schema_version,
            expected_files=spec.expected_files,
            actual_files=len(partitions),
            symbols=symbols,
            partitions=tuple(partitions),
            extra_files=tuple(extra),
            missing_partitions=tuple(missing),
            unexpected_symbols=tuple(sorted(unexpected)),
            metadata_ok=metadata_ok and not extra and not missing and not unexpected,
            notes=tuple(notes),
        )

    def list_partitions(self, timeframe: str, *, symbol: str | None = None) -> tuple[PartitionRef, ...]:
        info = self.get_dataset_info(timeframe, inspect_metadata=False)
        parts = info.partitions
        if symbol is not None:
            symbol = require_symbol(symbol)
            parts = tuple(p for p in parts if p.symbol == symbol)
        return parts

    def resolve_partitions(
        self,
        symbol: str,
        timeframe: str,
        start_utc: datetime | str,
        end_utc: datetime | str,
        *,
        inspect_metadata: bool = True,
    ) -> tuple[PartitionRef, ...]:
        symbol = require_symbol(symbol)
        timeframe = require_timeframe(timeframe)
        start = require_aware_utc(start_utc, name="start_utc")
        end = require_aware_utc(end_utc, name="end_utc")
        require_half_open_range(start, end)
        spec = config.TIMEFRAME_SPECS[timeframe]
        needed = self._needed_keys(spec, symbol, start, end)
        resolved: list[PartitionRef] = []
        for key in needed:
            ref = self._partition_ref(timeframe, *key, inspect_metadata=inspect_metadata)
            if not ref.path.is_file():
                raise PartitionNotFoundError(
                    f"Missing {timeframe} partition for {symbol} {self._hive_rel(timeframe, *key)} "
                    f"required for [{iso_z(start)}, {iso_z(end)}) path={ref.path}"
                )
            if inspect_metadata:
                self._validate_partition_identity(ref, symbol, timeframe)
            resolved.append(ref)
        resolved.sort(key=lambda p: (p.start_utc, p.symbol, p.path.as_posix()))
        if len(resolved) >= 2:
            for prev, cur in zip(resolved, resolved[1:]):
                if cur.start_utc < prev.start_utc:
                    raise SchemaMismatchError(
                        f"Partition chronological order invalid: {prev.path} then {cur.path}"
                    )
        return tuple(resolved)

    def _discover(
        self,
        timeframe: str,
        root: Path,
        *,
        inspect_metadata: bool,
    ) -> tuple[list[PartitionRef], list[str], set[str]]:
        spec = config.TIMEFRAME_SPECS[timeframe]
        extra: list[str] = []
        unexpected_symbols: set[str] = set()
        partitions: list[PartitionRef] = []
        if not root.is_dir():
            return [], extra, unexpected_symbols
        for path in root.rglob("*"):
            if path.is_dir():
                continue
            rel = path.relative_to(root).as_posix()
            if path.name != "data.parquet":
                extra.append(rel)
                continue
            parsed = self._parse_hive_path(timeframe, path, root)
            if parsed is None:
                extra.append(rel)
                continue
            symbol, year, month = parsed
            if symbol not in config.ALLOWED_SYMBOLS:
                unexpected_symbols.add(symbol)
                extra.append(rel)
                continue
            ref = self._partition_ref(timeframe, symbol, year, month, inspect_metadata=inspect_metadata)
            if inspect_metadata:
                try:
                    self._validate_partition_identity(ref, symbol, timeframe)
                except SchemaMismatchError:
                    extra.append(rel)
                    continue
            partitions.append(ref)
        partitions.sort(key=lambda p: (p.symbol, p.start_utc, p.path.as_posix()))
        seen: set[tuple[str, int, int | None]] = set()
        unique: list[PartitionRef] = []
        for part in partitions:
            key = (part.symbol, part.year, part.month)
            if key in seen:
                extra.append(str(part.path))
                continue
            seen.add(key)
            unique.append(part)
        return unique, extra, unexpected_symbols

    def _parse_hive_path(
        self, timeframe: str, path: Path, root: Path
    ) -> tuple[str, int, int | None] | None:
        spec = config.TIMEFRAME_SPECS[timeframe]
        rel = path.relative_to(root)
        parts = rel.parts
        if spec.partitioning == "symbol_year_month":
            if len(parts) != 4 or parts[-1] != "data.parquet":
                return None
            sm, ym, mm, _ = parts
            sm_m = _HIVE_SYMBOL.fullmatch(sm)
            ym_m = _HIVE_YEAR.fullmatch(ym)
            mm_m = _HIVE_MONTH.fullmatch(mm)
            if not (sm_m and ym_m and mm_m):
                return None
            return sm_m.group(1), int(ym_m.group(1)), int(mm_m.group(1))
        if len(parts) != 3 or parts[-1] != "data.parquet":
            return None
        sm, ym, _ = parts
        sm_m = _HIVE_SYMBOL.fullmatch(sm)
        ym_m = _HIVE_YEAR.fullmatch(ym)
        if not (sm_m and ym_m):
            return None
        return sm_m.group(1), int(ym_m.group(1)), None

    def _hive_rel(self, timeframe: str, symbol: str, year: int, month: int | None) -> str:
        spec = config.TIMEFRAME_SPECS[timeframe]
        if spec.partitioning == "symbol_year_month":
            return f"symbol={symbol}/year={year}/month={month:02d}/data.parquet"
        return f"symbol={symbol}/year={year}/data.parquet"

    def _partition_path(self, timeframe: str, symbol: str, year: int, month: int | None) -> Path:
        return self.dataset_root(timeframe) / self._hive_rel(timeframe, symbol, year, month)

    def _partition_ref(
        self,
        timeframe: str,
        symbol: str,
        year: int,
        month: int | None,
        *,
        inspect_metadata: bool,
    ) -> PartitionRef:
        spec = config.TIMEFRAME_SPECS[timeframe]
        path = self._partition_path(timeframe, symbol, year, month)
        if spec.partitioning == "symbol_year_month":
            assert month is not None
            start = _month_start(year, month)
            end = _next_month(year, month)
        else:
            start = _year_start(year)
            end = _year_start(year + 1)
        schema_version = None
        interval_meta = None
        metadata: dict[str, str] | None = None
        if inspect_metadata and path.is_file():
            pf = pq.ParquetFile(path)
            metadata = _decode_meta(pf.schema_arrow)
            schema_version = metadata.get("schema_version")
            interval_meta = metadata.get("interval")
        return PartitionRef(
            symbol=symbol,
            timeframe=timeframe,
            year=year,
            month=month,
            path=path,
            start_utc=start,
            end_utc=end,
            schema_version=schema_version,
            interval_meta=interval_meta,
            metadata=metadata,
        )

    def _validate_partition_identity(self, ref: PartitionRef, symbol: str, timeframe: str) -> None:
        spec = config.TIMEFRAME_SPECS[timeframe]
        meta = dict(ref.metadata or {})
        if meta.get("symbol") not in (None, symbol):
            raise SchemaMismatchError(
                f"Partition metadata symbol={meta.get('symbol')!r} != path symbol={symbol} file={ref.path}"
            )
        if meta.get("interval") not in (None, timeframe):
            raise SchemaMismatchError(
                f"Partition interval metadata {meta.get('interval')!r} != {timeframe} file={ref.path}"
            )
        if meta.get("year") not in (None, str(ref.year)):
            raise SchemaMismatchError(
                f"Partition metadata year={meta.get('year')!r} != {ref.year} file={ref.path}"
            )
        if spec.partitioning == "symbol_year_month":
            expected_month = f"{ref.month:02d}"
            if meta.get("month") not in (None, expected_month, str(ref.month)):
                raise SchemaMismatchError(
                    f"Partition metadata month={meta.get('month')!r} != {expected_month} file={ref.path}"
                )
        if meta.get("schema_version") not in (None, spec.schema_version):
            raise SchemaMismatchError(
                f"schema_version {meta.get('schema_version')!r} != {spec.schema_version} file={ref.path}"
            )
        if meta.get("timezone") not in (None, "UTC"):
            raise SchemaMismatchError(f"timezone metadata {meta.get('timezone')!r} file={ref.path}")
        if meta.get("bucket_alignment") not in (None, "UTC"):
            raise SchemaMismatchError(
                f"bucket_alignment metadata {meta.get('bucket_alignment')!r} file={ref.path}"
            )
        provider = (meta.get("provider") or "").lower()
        if provider and provider != "binance":
            raise SchemaMismatchError(f"unexpected provider {meta.get('provider')!r} file={ref.path}")
        market = (meta.get("market") or "").lower()
        if market and ("usd" not in market and "perpetual" not in market):
            raise SchemaMismatchError(f"unexpected market {meta.get('market')!r} file={ref.path}")

    def _needed_keys(
        self,
        spec: config.TimeframeSpec,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> list[tuple[str, int, int | None]]:
        last_open_exclusive_limit = end
        keys: list[tuple[str, int, int | None]] = []
        if spec.partitioning == "symbol_year_month":
            year, month = start.year, start.month
            end_year, end_month = end.year, end.month
            if end.month == 1 and end.day == 1 and end.hour == 0 and end.minute == 0 and end.second == 0 and end.microsecond == 0:
                # Range ending exactly on a month boundary still only needs the previous month
                # when end is exclusive and start is before that boundary.
                pass
            cursor = datetime(year, month, 1, tzinfo=timezone.utc)
            stop = datetime(end_year, end_month, 1, tzinfo=timezone.utc)
            if end != stop:
                # end is inside end_month, include it
                pass
            else:
                # end is exactly month start: last included open is previous month
                if start < end:
                    stop = stop
            while cursor < end:
                keys.append((symbol, cursor.year, cursor.month))
                cursor = _next_month(cursor.year, cursor.month)
            return keys
        year = start.year
        while datetime(year, 1, 1, tzinfo=timezone.utc) < end:
            keys.append((symbol, year, None))
            year += 1
        return keys


def fingerprint_file(path: Path) -> dict[str, Any]:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    st = path.stat()
    return {
        "path": str(path),
        "sha256": h.hexdigest(),
        "size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
    }
