"""UTC time helpers. Naive datetimes are never interpreted in local time."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from data.exceptions import InvalidTimeRangeError, NaiveDatetimeError

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
UTC = timezone.utc


def require_aware_utc(value: Any, *, name: str) -> datetime:
    if value is None:
        raise InvalidTimeRangeError(f"{name} is required")
    if isinstance(value, datetime):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise NaiveDatetimeError(
                f"{name} is a naive datetime; pass an aware UTC datetime. "
                "Windows local time is never used."
            )
        return value.astimezone(UTC)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise InvalidTimeRangeError(f"{name} is an empty timestamp string")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise InvalidTimeRangeError(f"{name} is not a parseable timestamp: {value!r}") from exc
        if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
            raise NaiveDatetimeError(
                f"{name}={value!r} has no timezone; naive strings are rejected"
            )
        return parsed.astimezone(UTC)
    raise InvalidTimeRangeError(f"{name} must be datetime or ISO-8601 string, got {type(value).__name__}")


def require_half_open_range(start: datetime, end: datetime) -> None:
    if end <= start:
        raise InvalidTimeRangeError(
            f"end_utc must be strictly after start_utc for half-open [start, end); "
            f"got start={start.isoformat()} end={end.isoformat()}"
        )


def datetime_to_ms(value: datetime) -> int:
    aware = value.astimezone(UTC)
    delta = aware - EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def ms_to_datetime(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=int(ms))


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S") + (
        f".{value.microsecond:06d}" if value.microsecond else ""
    ).rstrip("0").rstrip(".") + "Z"
