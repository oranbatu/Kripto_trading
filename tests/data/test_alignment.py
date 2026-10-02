"""Multi-timeframe close_time alignment tests, including calendar boundaries."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone

import config
from data.exceptions import UnsupportedTimeframeError
from data.loader import load_multi_timeframe
from data.timeutil import datetime_to_ms, ms_to_datetime

UTC = timezone.utc


def _mapped_open(data, tf: str, base_open: datetime) -> datetime | None:
    base = data.slices[data.base_timeframe]
    row = base.index_of_open(base_open)
    idx = data.available_index(tf, row)
    if idx == config.UNAVAILABLE_INDEX:
        return None
    return ms_to_datetime(data.slices[tf].open_times_ms()[idx])


class TestAlignment(unittest.TestCase):
    def test_base_5m_close_clock_intraday(self) -> None:
        data = load_multi_timeframe(
            "BTCUSDT",
            ["5m", "15m", "30m", "1h", "4h", "1d"],
            "5m",
            datetime(2026, 8, 10, tzinfo=UTC),
            datetime(2026, 8, 11, tzinfo=UTC),
            decision_clock="close",
        )
        t1030 = datetime(2026, 8, 10, 10, 30, tzinfo=UTC)
        self.assertEqual(_mapped_open(data, "15m", t1030), datetime(2026, 8, 10, 10, 15, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "30m", t1030), datetime(2026, 8, 10, 10, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1h", t1030), datetime(2026, 8, 10, 9, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "4h", t1030), datetime(2026, 8, 10, 4, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1d", t1030), datetime(2026, 8, 9, tzinfo=UTC))

        t1055 = datetime(2026, 8, 10, 10, 55, tzinfo=UTC)
        self.assertEqual(_mapped_open(data, "15m", t1055), datetime(2026, 8, 10, 10, 45, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "30m", t1055), datetime(2026, 8, 10, 10, 30, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1h", t1055), datetime(2026, 8, 10, 10, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "4h", t1055), datetime(2026, 8, 10, 4, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1d", t1055), datetime(2026, 8, 9, tzinfo=UTC))

        t1155 = datetime(2026, 8, 10, 11, 55, tzinfo=UTC)
        self.assertEqual(_mapped_open(data, "4h", t1155), datetime(2026, 8, 10, 8, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1d", t1155), datetime(2026, 8, 9, tzinfo=UTC))

        t2355 = datetime(2026, 8, 10, 23, 55, tzinfo=UTC)
        self.assertEqual(_mapped_open(data, "15m", t2355), datetime(2026, 8, 10, 23, 45, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "30m", t2355), datetime(2026, 8, 10, 23, 30, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1h", t2355), datetime(2026, 8, 10, 23, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "4h", t2355), datetime(2026, 8, 10, 20, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1d", t2355), datetime(2026, 8, 10, tzinfo=UTC))

    def test_decision_clock_open(self) -> None:
        data = load_multi_timeframe(
            "ETHUSDT",
            ["5m", "15m", "1h", "4h"],
            "5m",
            datetime(2026, 8, 10, tzinfo=UTC),
            datetime(2026, 8, 11, tzinfo=UTC),
            decision_clock="open",
        )
        t1030 = datetime(2026, 8, 10, 10, 30, tzinfo=UTC)
        self.assertEqual(_mapped_open(data, "15m", t1030), datetime(2026, 8, 10, 10, 15, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1h", t1030), datetime(2026, 8, 10, 9, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "4h", t1030), datetime(2026, 8, 10, 4, 0, tzinfo=UTC))

    def test_base_1m_with_all_higher(self) -> None:
        data = load_multi_timeframe(
            "BTCUSDT",
            ["1m", "5m", "15m", "30m", "1h", "4h", "1d"],
            "1m",
            datetime(2026, 8, 10, 10, 0, tzinfo=UTC),
            datetime(2026, 8, 10, 11, 0, tzinfo=UTC),
            decision_clock="close",
        )
        t1030 = datetime(2026, 8, 10, 10, 30, tzinfo=UTC)
        self.assertEqual(_mapped_open(data, "5m", t1030), datetime(2026, 8, 10, 10, 25, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "15m", t1030), datetime(2026, 8, 10, 10, 15, tzinfo=UTC))

    def test_base_15m_with_higher(self) -> None:
        data = load_multi_timeframe(
            "LINKUSDT",
            ["15m", "30m", "1h", "4h", "1d"],
            "15m",
            datetime(2026, 8, 10, tzinfo=UTC),
            datetime(2026, 8, 11, tzinfo=UTC),
            decision_clock="close",
        )
        t1030 = datetime(2026, 8, 10, 10, 30, tzinfo=UTC)
        self.assertEqual(_mapped_open(data, "30m", t1030), datetime(2026, 8, 10, 10, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1h", t1030), datetime(2026, 8, 10, 9, 0, tzinfo=UTC))

    def test_indices_monotonic_and_close_time_rule(self) -> None:
        data = load_multi_timeframe(
            "AAVEUSDT",
            ["5m", "1h", "1d"],
            "5m",
            datetime(2026, 8, 10, tzinfo=UTC),
            datetime(2026, 8, 11, tzinfo=UTC),
            decision_clock="close",
        )
        base = data.slices["5m"]
        decisions = base.close_times_ms()
        for tf, indices in data.availability.items():
            last = -1
            htf_close = data.slices[tf].close_times_ms()
            for i, idx in enumerate(indices):
                self.assertGreaterEqual(idx, last)
                if idx >= 0:
                    self.assertLessEqual(htf_close[idx], decisions[i])
                    if idx + 1 < len(htf_close):
                        self.assertGreater(htf_close[idx + 1], decisions[i])
                last = idx

    def test_hour_boundary(self) -> None:
        data = load_multi_timeframe(
            "BTCUSDT", ["5m", "1h"], "5m",
            datetime(2026, 8, 10, 9, 50, tzinfo=UTC),
            datetime(2026, 8, 10, 10, 10, tzinfo=UTC),
            decision_clock="close",
        )
        before = datetime(2026, 8, 10, 9, 55, tzinfo=UTC)
        after = datetime(2026, 8, 10, 10, 0, tzinfo=UTC)
        self.assertEqual(_mapped_open(data, "1h", before), datetime(2026, 8, 10, 9, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1h", after), datetime(2026, 8, 10, 9, 0, tzinfo=UTC))
        closed = datetime(2026, 8, 10, 10, 55, tzinfo=UTC)
        data2 = load_multi_timeframe(
            "BTCUSDT", ["5m", "1h"], "5m",
            datetime(2026, 8, 10, 10, 50, tzinfo=UTC),
            datetime(2026, 8, 10, 11, 5, tzinfo=UTC),
            decision_clock="close",
        )
        self.assertEqual(_mapped_open(data2, "1h", closed), datetime(2026, 8, 10, 10, 0, tzinfo=UTC))

    def test_four_hour_boundary(self) -> None:
        data = load_multi_timeframe(
            "BTCUSDT", ["5m", "4h"], "5m",
            datetime(2026, 8, 10, 11, 50, tzinfo=UTC),
            datetime(2026, 8, 10, 12, 5, tzinfo=UTC),
            decision_clock="close",
        )
        still_open = datetime(2026, 8, 10, 11, 50, tzinfo=UTC)
        just_closed = datetime(2026, 8, 10, 11, 55, tzinfo=UTC)
        self.assertEqual(_mapped_open(data, "4h", still_open), datetime(2026, 8, 10, 4, 0, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "4h", just_closed), datetime(2026, 8, 10, 8, 0, tzinfo=UTC))

    def test_utc_day_boundary(self) -> None:
        data = load_multi_timeframe(
            "BTCUSDT", ["5m", "1d"], "5m",
            datetime(2026, 8, 10, 23, 50, tzinfo=UTC),
            datetime(2026, 8, 11, 0, 10, tzinfo=UTC),
            decision_clock="close",
        )
        last = datetime(2026, 8, 10, 23, 55, tzinfo=UTC)
        first_next = datetime(2026, 8, 11, 0, 0, tzinfo=UTC)
        self.assertEqual(_mapped_open(data, "1d", last), datetime(2026, 8, 10, tzinfo=UTC))
        self.assertEqual(_mapped_open(data, "1d", first_next), datetime(2026, 8, 10, tzinfo=UTC))

    def test_month_and_year_and_leap(self) -> None:
        month = load_multi_timeframe(
            "DOTUSDT", ["1h", "1d"], "1h",
            datetime(2026, 8, 31, 20, 0, tzinfo=UTC),
            datetime(2026, 9, 1, 4, 0, tzinfo=UTC),
            decision_clock="close",
        )
        self.assertEqual(
            _mapped_open(month, "1d", datetime(2026, 8, 31, 23, 0, tzinfo=UTC)),
            datetime(2026, 8, 31, tzinfo=UTC),
        )
        year = load_multi_timeframe(
            "DOTUSDT", ["1h", "1d"], "1h",
            datetime(2024, 12, 31, 20, 0, tzinfo=UTC),
            datetime(2025, 1, 1, 4, 0, tzinfo=UTC),
            decision_clock="close",
        )
        self.assertEqual(
            _mapped_open(year, "1d", datetime(2024, 12, 31, 23, 0, tzinfo=UTC)),
            datetime(2024, 12, 31, tzinfo=UTC),
        )
        leap = load_multi_timeframe(
            "DOTUSDT", ["1h", "1d"], "1h",
            datetime(2024, 2, 29, tzinfo=UTC),
            datetime(2024, 3, 1, tzinfo=UTC),
            decision_clock="close",
        )
        self.assertEqual(
            _mapped_open(leap, "1d", datetime(2024, 2, 29, 12, 0, tzinfo=UTC)),
            datetime(2024, 2, 28, tzinfo=UTC),
        )

    def test_reject_higher_frequency_as_htf(self) -> None:
        with self.assertRaises(UnsupportedTimeframeError):
            load_multi_timeframe(
                "BTCUSDT", ["1h", "5m"], "1h",
                datetime(2026, 8, 10, tzinfo=UTC),
                datetime(2026, 8, 11, tzinfo=UTC),
            )

    def test_repeat_alignment_deterministic(self) -> None:
        args = ("BTCUSDT", ["5m", "15m", "1h"], "5m", datetime(2026, 8, 10, tzinfo=UTC), datetime(2026, 8, 11, tzinfo=UTC))
        a = load_multi_timeframe(*args, decision_clock="close")
        b = load_multi_timeframe(*args, decision_clock="close")
        self.assertEqual(dict(a.availability), dict(b.availability))
        self.assertEqual(dict(a.source_file_hashes), dict(b.source_file_hashes))


if __name__ == "__main__":
    unittest.main()
