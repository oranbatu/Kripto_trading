"""Source immutability: SHA-256, size, mtime_ns, and no files created in source trees."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path

import config
from data.loader import load_candles, load_multi_timeframe
from tests.helpers import fingerprint_tree, list_source_files

UTC = timezone.utc


class TestImmutability(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.files = list_source_files()
        cls.before = fingerprint_tree(cls.files)
        cls.source_dirs = [
            config.dataset_root(tf)
            for tf in config.ALLOWED_TIMEFRAMES
        ] + [config.reports_dir()]

    def test_loads_do_not_change_source_fingerprints(self) -> None:
        load_candles(
            "BTCUSDT", "5m",
            datetime(2026, 8, 10, tzinfo=UTC),
            datetime(2026, 8, 11, tzinfo=UTC),
        )
        load_candles(
            "ETHUSDT", "1h",
            datetime(2025, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 8, tzinfo=UTC),
            warmup_bars=100,
        )
        load_multi_timeframe(
            "BTCUSDT",
            ["5m", "15m", "1h", "1d"],
            "5m",
            datetime(2026, 8, 10, tzinfo=UTC),
            datetime(2026, 8, 11, tzinfo=UTC),
            decision_clock="close",
        )
        after = fingerprint_tree(self.files)
        self.assertEqual(self.before, after)

    def test_no_new_files_in_source_directories(self) -> None:
        before_set = {str(p) for p in self.files}
        extras = []
        for directory in self.source_dirs:
            for path in directory.rglob("*"):
                if not path.is_file():
                    continue
                if str(path) not in before_set and path.suffix.lower() in {".parquet", ".part", ".tmp", ".json"}:
                    extras.append(str(path))
        self.assertEqual(extras, [])

    def test_existing_reports_unchanged(self) -> None:
        reports = sorted(config.reports_dir().glob("*.json"))
        self.assertGreaterEqual(len(reports), 7)
        after = fingerprint_tree(reports)
        before = {k: v for k, v in self.before.items() if k in after}
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
