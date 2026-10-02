"""No-look-ahead tests with real data and synthetic future sentinels."""
from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

import config
from data.alignment import validate_alignment_map
from data.catalog import DataCatalog
from data.exceptions import LookaheadViolationError
from data.loader import load_candles, load_multi_timeframe
from data.timeutil import datetime_to_ms, ms_to_datetime
from tests.helpers import copy_real_month, mutate_high

UTC = timezone.utc
SENTINEL = Decimal("99999999.01000000")


class TestNoLookahead(unittest.TestCase):
    def setUp(self) -> None:
        self.data = load_multi_timeframe(
            "BTCUSDT",
            ["5m", "15m", "30m", "1h", "4h", "1d"],
            "5m",
            datetime(2026, 8, 10, tzinfo=UTC),
            datetime(2026, 8, 11, tzinfo=UTC),
            decision_clock="close",
        )

    def _open(self, tf: str, base_open: datetime) -> datetime | None:
        row = self.data.slices["5m"].index_of_open(base_open)
        idx = self.data.available_index(tf, row)
        if idx < 0:
            return None
        return ms_to_datetime(self.data.slices[tf].open_times_ms()[idx])

    def test_unfinished_15m_unavailable_at_1030(self) -> None:
        t = datetime(2026, 8, 10, 10, 30, tzinfo=UTC)
        self.assertEqual(self._open("15m", t), datetime(2026, 8, 10, 10, 15, tzinfo=UTC))
        self.assertNotEqual(self._open("15m", t), datetime(2026, 8, 10, 10, 30, tzinfo=UTC))

    def test_unfinished_30m_unavailable_at_1030(self) -> None:
        t = datetime(2026, 8, 10, 10, 30, tzinfo=UTC)
        self.assertEqual(self._open("30m", t), datetime(2026, 8, 10, 10, 0, tzinfo=UTC))

    def test_unfinished_1h_unavailable_at_1030(self) -> None:
        t = datetime(2026, 8, 10, 10, 30, tzinfo=UTC)
        self.assertEqual(self._open("1h", t), datetime(2026, 8, 10, 9, 0, tzinfo=UTC))

    def test_unfinished_4h_unavailable_until_close(self) -> None:
        t = datetime(2026, 8, 10, 10, 30, tzinfo=UTC)
        self.assertEqual(self._open("4h", t), datetime(2026, 8, 10, 4, 0, tzinfo=UTC))
        self.assertEqual(
            self._open("4h", datetime(2026, 8, 10, 10, 55, tzinfo=UTC)),
            datetime(2026, 8, 10, 4, 0, tzinfo=UTC),
        )
        self.assertEqual(
            self._open("4h", datetime(2026, 8, 10, 11, 55, tzinfo=UTC)),
            datetime(2026, 8, 10, 8, 0, tzinfo=UTC),
        )

    def test_current_daily_unavailable_before_final_close(self) -> None:
        self.assertEqual(
            self._open("1d", datetime(2026, 8, 10, 10, 30, tzinfo=UTC)),
            datetime(2026, 8, 9, tzinfo=UTC),
        )
        self.assertEqual(
            self._open("1d", datetime(2026, 8, 10, 23, 50, tzinfo=UTC)),
            datetime(2026, 8, 9, tzinfo=UTC),
        )
        self.assertEqual(
            self._open("1d", datetime(2026, 8, 10, 23, 55, tzinfo=UTC)),
            datetime(2026, 8, 10, tzinfo=UTC),
        )

    def test_synthetic_future_sentinel_does_not_leak(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            copy_real_month("5m", "BTCUSDT", 2026, 8, root)
            path_15 = copy_real_month("15m", "BTCUSDT", 2026, 8, root)
            mutate_high(path_15, datetime(2026, 8, 10, 10, 30, tzinfo=UTC), SENTINEL)
            cat = DataCatalog(root)
            data = load_multi_timeframe(
                "BTCUSDT", ["5m", "15m"], "5m",
                datetime(2026, 8, 10, tzinfo=UTC),
                datetime(2026, 8, 11, tzinfo=UTC),
                decision_clock="close",
                catalog=cat,
            )
            t1030 = datetime(2026, 8, 10, 10, 30, tzinfo=UTC)
            row = data.slices["5m"].index_of_open(t1030)
            idx = data.available_index("15m", row)
            highs = data.slices["15m"].table.column("high").to_pylist()
            mapped_high = highs[idx]
            self.assertNotEqual(mapped_high, SENTINEL)
            mapped_open = ms_to_datetime(data.slices["15m"].open_times_ms()[idx])
            self.assertEqual(mapped_open, datetime(2026, 8, 10, 10, 15, tzinfo=UTC))
            # After the 10:30 15m candle closes, the sentinel may appear.
            t1045 = datetime(2026, 8, 10, 10, 45, tzinfo=UTC)
            row2 = data.slices["5m"].index_of_open(t1045)
            idx2 = data.available_index("15m", row2)
            self.assertEqual(highs[idx2], SENTINEL)

    def test_invalid_availability_mapping_raises(self) -> None:
        base = load_candles(
            "BTCUSDT", "5m",
            datetime(2026, 8, 10, 10, 30, tzinfo=UTC),
            datetime(2026, 8, 10, 10, 40, tzinfo=UTC),
        )
        htf = load_candles(
            "BTCUSDT", "15m",
            datetime(2026, 8, 10, 10, 0, tzinfo=UTC),
            datetime(2026, 8, 10, 11, 0, tzinfo=UTC),
        )
        # Deliberately point the 10:30 5m close at the unfinished 10:30 15m candle.
        forbidden = htf.index_of_open(datetime(2026, 8, 10, 10, 30, tzinfo=UTC))
        with self.assertRaises(LookaheadViolationError):
            validate_alignment_map(
                symbol="BTCUSDT",
                base_timeframe="5m",
                higher_timeframe="15m",
                decision_clock="close",
                decision_ms=base.close_times_ms(),
                htf_close_ms=htf.close_times_ms(),
                htf_open_ms=htf.open_times_ms(),
                indices=(forbidden,),
                start=datetime(2026, 8, 10, 10, 30, tzinfo=UTC),
                end=datetime(2026, 8, 10, 10, 40, tzinfo=UTC),
            )


if __name__ == "__main__":
    unittest.main()
