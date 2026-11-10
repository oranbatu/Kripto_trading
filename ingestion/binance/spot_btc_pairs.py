"""Binance Spot BTC-quoted 1-minute archive planning and validation.

This module is Spot-only. It does not read or write USD-M perpetual paths and
it performs no network or filesystem work on import.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Optional

SPOT_SYMBOLS: tuple[str, ...] = (
    "ETHBTC",
    "AVAXBTC",
    "NEARBTC",
    "LINKBTC",
    "LTCBTC",
    "DOTBTC",
    "XRPBTC",
    "DOGEBTC",
    "ADABTC",
    "SOLBTC",
)
EXCLUDED_SYMBOLS: tuple[str, ...] = ("BTCBTC", "AAVEBTC", "OPBTC")
INTERVAL = "1m"
QUOTE_ASSET = "BTC"
ARCHIVE_BASE_URL = "https://data.binance.vision/data/spot"
SCHEMA_VERSION = "spot-1m-canonical-v1"
TASK_NAME = "binance_spot_btc_pairs_1m"

RANGE_START_UTC = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
RANGE_LAST_OPEN_UTC = datetime(2026, 10, 27, 23, 59, tzinfo=timezone.utc)
RANGE_EXCLUSIVE_END_UTC = datetime(2026, 10, 28, 0, 0, tzinfo=timezone.utc)
UNIT_SWITCH_UTC = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MINUTE_MS = 60_000
CLOSE_OFFSET_MS = 59_999
KLINE_COLUMNS = 12
EXPECTED_HEADER = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
)
DECIMAL_SCALE = 8
QUANT = Decimal(1).scaleb(-DECIMAL_SCALE)

EXPECTED_ROWS_2024 = 527_040
EXPECTED_ROWS_2025 = 525_600
EXPECTED_ROWS_2026 = 432_000
EXPECTED_ROWS_OCTOBER_2026 = 38_880
EXPECTED_ROWS_PER_SYMBOL = 1_484_640
EXPECTED_ROWS_TOTAL = 14_846_400
EXPECTED_MONTHLY_ARCHIVES = 33
EXPECTED_DAILY_ARCHIVES = 27
EXPECTED_ARCHIVES_PER_SYMBOL = 60
EXPECTED_PARTITIONS_PER_SYMBOL = 34
MONTHLY_START = (2024, 1)
MONTHLY_END = (2026, 9)

_SYMBOL_RE = re.compile(r"^[A-Z0-9]+BTC$")
_UNSAFE = ("/", "\\", "..", ":", " ", "\x00")


def ms_of(value: datetime) -> int:
    return (value - EPOCH) // timedelta(milliseconds=1)


def iso_of_ms(ms: int) -> str:
    stamp = EPOCH + timedelta(milliseconds=ms)
    return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_close_of_ms(close_ms: int) -> str:
    stamp = EPOCH + timedelta(milliseconds=close_ms)
    return stamp.strftime("%Y-%m-%dT%H:%M:%S.") + f"{stamp.microsecond // 1000:03d}Z"


RANGE_START_MS = ms_of(RANGE_START_UTC)
RANGE_LAST_OPEN_MS = ms_of(RANGE_LAST_OPEN_UTC)
RANGE_EXCLUSIVE_END_MS = ms_of(RANGE_EXCLUSIVE_END_UTC)
UNIT_SWITCH_MS = ms_of(UNIT_SWITCH_UTC)


def base_asset(symbol: str) -> str:
    if not symbol.endswith(QUOTE_ASSET) or symbol == "BTCBTC":
        raise ValueError(f"{symbol} is not a BTC-quoted Spot symbol")
    return symbol[: -len(QUOTE_ASSET)]


def require_spot_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    """Accept only the exact ten-symbol tuple, in that order."""
    found = tuple(symbols)
    if found != SPOT_SYMBOLS:
        rejected = [item for item in found if item not in SPOT_SYMBOLS]
        raise ValueError(
            "Spot scope must be exactly "
            f"{','.join(SPOT_SYMBOLS)}; rejected {rejected or found!r}"
        )
    for symbol in found:
        if type(symbol) is not str or symbol != symbol.strip() or symbol.lower() == symbol:
            raise ValueError(f"malformed Spot symbol {symbol!r}")
        if any(token in symbol for token in _UNSAFE) or not _SYMBOL_RE.fullmatch(symbol):
            raise ValueError(f"malformed Spot symbol {symbol!r}")
        if symbol in EXCLUDED_SYMBOLS:
            raise ValueError(f"excluded Spot symbol {symbol}")
        if base_asset(symbol) + QUOTE_ASSET != symbol:
            raise ValueError(f"quote asset is not BTC for {symbol}")
    return found


def reject_symbol(symbol: object) -> None:
    """Raise for any symbol outside the approved ten, including excluded names."""
    if type(symbol) is not str or any(token in symbol for token in _UNSAFE):
        raise ValueError(f"malformed Spot symbol {symbol!r}")
    if symbol in EXCLUDED_SYMBOLS or symbol not in SPOT_SYMBOLS:
        raise ValueError(f"rejected Spot symbol {symbol!r}")
    require_spot_symbols(SPOT_SYMBOLS)


@dataclass(frozen=True)
class Archive:
    symbol: str
    kind: str
    period: str
    start_ms: int
    end_ms: int

    @property
    def zip_name(self) -> str:
        return f"{self.symbol}-{INTERVAL}-{self.period}.zip"

    @property
    def csv_name(self) -> str:
        return f"{self.symbol}-{INTERVAL}-{self.period}.csv"

    @property
    def zip_url(self) -> str:
        return (
            f"{ARCHIVE_BASE_URL}/{self.kind}/klines/{self.symbol}/{INTERVAL}/{self.zip_name}"
        )

    @property
    def checksum_url(self) -> str:
        return self.zip_url + ".CHECKSUM"

    @property
    def timestamp_unit(self) -> str:
        return "milliseconds" if self.start_ms < UNIT_SWITCH_MS else "microseconds"

    def zip_path(self, root: Path) -> Path:
        return (
            root / "raw" / "binance" / "spot" / self.kind / "klines"
            / self.symbol / INTERVAL / self.zip_name
        )

    def checksum_path(self, root: Path) -> Path:
        return self.zip_path(root).with_name(self.zip_name + ".CHECKSUM")


def _month_iter(start: tuple[int, int], end: tuple[int, int]):
    year, month = start
    while (year, month) <= end:
        yield year, month
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


def plan_symbol(symbol: str) -> list[Archive]:
    reject_symbol(symbol)
    archives: list[Archive] = []
    for year, month in _month_iter(MONTHLY_START, MONTHLY_END):
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        nxt = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
        start_ms = ms_of(start)
        end_ms = ms_of(nxt) - MINUTE_MS
        archives.append(Archive(symbol, "monthly", f"{year:04d}-{month:02d}", start_ms, end_ms))
    for day in range(1, 28):
        start = datetime(2026, 10, day, tzinfo=timezone.utc)
        start_ms = ms_of(start)
        archives.append(Archive(
            symbol, "daily", f"2026-10-{day:02d}", start_ms, start_ms + 1439 * MINUTE_MS,
        ))
    return archives


def plan_archives(symbols: Iterable[str] = SPOT_SYMBOLS) -> list[Archive]:
    require_spot_symbols(symbols)
    planned: list[Archive] = []
    for symbol in SPOT_SYMBOLS:
        planned.extend(plan_symbol(symbol))
    if any(item.symbol in EXCLUDED_SYMBOLS for item in planned):
        raise RuntimeError("excluded symbol entered the Spot archive plan")
    if any(item.kind == "monthly" and item.period == "2026-10" for item in planned):
        raise RuntimeError("October 2026 monthly archive entered the Spot plan")
    return planned


def partition_months() -> list[tuple[int, int]]:
    return list(_month_iter((2024, 1), (2026, 10)))


def days_in_month(year: int, month: int) -> int:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    nxt = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return (nxt - start).days


def expected_partition_minutes(year: int, month: int) -> int:
    if (year, month) < (2024, 1) or (year, month) > (2026, 10):
        raise ValueError(f"partition {year:04d}-{month:02d} is outside the Spot window")
    if (year, month) == (2026, 10):
        return EXPECTED_ROWS_OCTOBER_2026
    return days_in_month(year, month) * 1440


def assert_under(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    resolved.relative_to(root.resolve())
    text = str(resolved)
    if "futures" in text.split("\\") or "/futures/" in text.replace("\\", "/"):
        raise ValueError(f"{resolved} resolves under a futures path")
    return resolved


def parse_checksum_bytes(body: bytes, expected_zip_name: str) -> str:
    if len(body) > 4096:
        raise ValueError(f".CHECKSUM body is {len(body)} bytes")
    text = body.decode("utf-8").lstrip("\ufeff")
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        digest = parts[0].lower()
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError(f"malformed .CHECKSUM line: {line[:120]!r}")
        if len(parts) != 2:
            raise ValueError(f".CHECKSUM line must name the ZIP: {line[:120]!r}")
        named = parts[1].lstrip("*").replace("\\", "/").rsplit("/", 1)[-1]
        if named != expected_zip_name:
            raise ValueError(f".CHECKSUM names {named!r} but {expected_zip_name!r} was expected")
        return digest
    raise ValueError(".CHECKSUM file contains no digest line")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def normalize_timestamps(raw_open: int, raw_close: int, unit: str) -> tuple[int, int]:
    """Convert one official Spot pair to canonical millisecond timestamps.

    Archives before 2025-01-01 are milliseconds. Archives from 2025-01-01 onward
    are microseconds. Open times divide cleanly by 1000; close times keep the
    official trailing 999 microseconds and become open + 59.999 seconds.
    """
    if unit == "milliseconds":
        if not (10**12 <= raw_open < 10**13 and 10**12 <= raw_close < 10**13):
            raise ValueError(f"expected millisecond timestamps, got open={raw_open} close={raw_close}")
        open_ms, close_ms = raw_open, raw_close
    elif unit == "microseconds":
        if not (10**15 <= raw_open < 10**16 and 10**15 <= raw_close < 10**16):
            raise ValueError(f"expected microsecond timestamps, got open={raw_open} close={raw_close}")
        if raw_open % 1000 != 0:
            raise ValueError(f"microsecond open_time {raw_open} is not aligned to a millisecond")
        if raw_close % 1000 != 999:
            raise ValueError(f"microsecond close_time {raw_close} does not end in 999")
        open_ms, close_ms = raw_open // 1000, raw_close // 1000
    else:
        raise ValueError(f"unknown timestamp unit {unit!r}")
    if open_ms % MINUTE_MS != 0:
        raise ValueError(f"open_time {open_ms} is not aligned to a UTC minute")
    if close_ms != open_ms + CLOSE_OFFSET_MS:
        raise ValueError(f"close_time {close_ms} is not open_time plus 59.999 seconds")
    return open_ms, close_ms


def parse_decimal(value: str, field: str) -> Decimal:
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"unparseable {field}={value!r}") from exc
    if not number.is_finite():
        raise ValueError(f"{field}={value!r} is not finite")
    exponent = number.as_tuple().exponent
    if isinstance(exponent, int) and exponent < -DECIMAL_SCALE:
        raise ValueError(f"{field}={value!r} has more than {DECIMAL_SCALE} decimal places")
    return number.quantize(QUANT)


def ohlc_violation(open_: Decimal, high: Decimal, low: Decimal, close: Decimal,
                   volume: Decimal, quote_volume: Decimal, trades: int,
                   taker_base: Decimal, taker_quote: Decimal) -> Optional[str]:
    if min(open_, high, low, close) <= 0:
        return "price is not strictly positive"
    if high < max(open_, close, low) or low > min(open_, close, high):
        return "OHLC relationship is invalid"
    if min(volume, quote_volume, taker_base, taker_quote) < 0 or trades < 0:
        return "volume or trade count is negative"
    return None


def parse_kline_row(row: list[str], unit: str) -> dict:
    if len(row) != KLINE_COLUMNS:
        raise ValueError(f"{len(row)} columns, expected {KLINE_COLUMNS}")
    open_ms, close_ms = normalize_timestamps(int(row[0]), int(row[6]), unit)
    parsed = {
        "open_ms": open_ms,
        "close_ms": close_ms,
        "open": parse_decimal(row[1], "open"),
        "high": parse_decimal(row[2], "high"),
        "low": parse_decimal(row[3], "low"),
        "close": parse_decimal(row[4], "close"),
        "volume": parse_decimal(row[5], "volume"),
        "quote_volume": parse_decimal(row[7], "quote_asset_volume"),
        "count": int(row[8]),
        "taker_buy_volume": parse_decimal(row[9], "taker_buy_base_asset_volume"),
        "taker_buy_quote_volume": parse_decimal(row[10], "taker_buy_quote_asset_volume"),
        "ignore": int(row[11]),
        "timestamp_unit": unit,
    }
    reason = ohlc_violation(
        parsed["open"], parsed["high"], parsed["low"], parsed["close"],
        parsed["volume"], parsed["quote_volume"], parsed["count"],
        parsed["taker_buy_volume"], parsed["taker_buy_quote_volume"],
    )
    if reason:
        raise ValueError(reason)
    if open_ms < RANGE_START_MS or open_ms > RANGE_LAST_OPEN_MS:
        raise ValueError(f"open_time {iso_of_ms(open_ms)} is outside the requested window")
    return parsed


def iter_csv_rows(text, unit: str):
    import csv
    reader = csv.reader(text)
    saw_row = False
    for lineno, row in enumerate(reader, 1):
        if not row or not any(cell.strip() for cell in row):
            continue
        if not saw_row:
            saw_row = True
            cells = tuple(cell.strip() for cell in row)
            if cells == EXPECTED_HEADER:
                continue
            if not row[0].strip().lstrip("-").isdigit():
                raise ValueError(f"line {lineno}: refusing to discard non-header row {row!r}")
        try:
            yield parse_kline_row(row, unit)
        except (ValueError, OverflowError) as exc:
            raise ValueError(f"line {lineno}: {exc}") from exc


def validate_zip_members(path: Path, archive: Archive) -> None:
    with zipfile.ZipFile(path) as handle:
        bad = handle.testzip()
        if bad is not None:
            raise ValueError(f"ZIP CRC failed for member {bad!r}")
        names = handle.namelist()
    if names != [archive.csv_name]:
        raise ValueError(f"unexpected ZIP members {names!r}; expected [{archive.csv_name!r}]")
    for name in names:
        if name.startswith(("/", "\\")) or ".." in name.replace("\\", "/").split("/"):
            raise ValueError(f"unsafe ZIP member {name!r}")


def missing_open_times(opens: Iterable[int], start_ms: int, end_ms: int) -> list[int]:
    present = set(opens)
    return [stamp for stamp in range(start_ms, end_ms + 1, MINUTE_MS) if stamp not in present]


def assert_complete_minutes(opens: Iterable[int], start_ms: int, end_ms: int, label: str) -> None:
    """Require every UTC minute exactly once. Missing minutes are never filled."""
    previous: Optional[int] = None
    count = 0
    for stamp in opens:
        if previous is None:
            if stamp != start_ms:
                raise ValueError(f"{label} starts at {iso_of_ms(stamp)}; expected {iso_of_ms(start_ms)}")
        elif stamp == previous:
            raise ValueError(f"{label} duplicate open time {iso_of_ms(stamp)}")
        elif stamp != previous + MINUTE_MS:
            if stamp < previous:
                raise ValueError(f"{label} out-of-order open time {iso_of_ms(stamp)}")
            raise ValueError(
                f"{label} missing minute after {iso_of_ms(previous)}; next is {iso_of_ms(stamp)}"
            )
        if stamp > end_ms or stamp >= RANGE_EXCLUSIVE_END_MS:
            raise ValueError(f"{label} open time {iso_of_ms(stamp)} is outside the requested window")
        previous = stamp
        count += 1
    expected = (end_ms - start_ms) // MINUTE_MS + 1
    if count != expected or previous != end_ms:
        raise ValueError(f"{label} has {count} rows; expected {expected} ending {iso_of_ms(end_ms)}")


def duplicate_open_times(opens: Iterable[int]) -> list[int]:
    seen: set[int] = set()
    duplicates: list[int] = []
    for stamp in opens:
        if stamp in seen:
            duplicates.append(stamp)
        seen.add(stamp)
    return duplicates


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes
        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


class TaskLock:
    """Single-task lock. A live PID is refused; a dead PID is replaced."""

    def __init__(self, path: Path):
        self.path = path
        self._held = False

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            try:
                payload = json.loads(self.path.read_text(encoding="utf-8"))
                pid = int(payload.get("pid", 0))
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"unreadable lock {self.path}: {exc}") from exc
            if pid_alive(pid):
                raise RuntimeError(f"Spot task already running as pid {pid}")
            self.path.unlink()
        body = {
            "pid": os.getpid(),
            "started": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "task": TASK_NAME,
        }
        self.path.write_text(json.dumps(body, indent=2), encoding="utf-8")
        self._held = True

    def release(self) -> None:
        if self._held and self.path.exists():
            self.path.unlink()
        self._held = False


def file_signature(path: Path) -> dict:
    stat = path.stat()
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }
