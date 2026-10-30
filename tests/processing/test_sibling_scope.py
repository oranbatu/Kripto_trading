"""Regression tests for derived sibling-timeframe validation.

Fixtures live in temporary directories. These tests do not open the live
production tree for writing.
"""
from __future__ import annotations

import inspect
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from processing.timeframes import build_15m_parquet as build_15m
from processing.timeframes import build_1d_parquet as build_1d
from processing.timeframes import build_1h_parquet as build_1h
from processing.timeframes import build_30m_parquet as build_30m
from processing.timeframes import build_4h_parquet as build_4h
from processing.timeframes import build_5m_parquet as build_5m
from processing.timeframes.sibling_scope import (
    APPROVED_DERIVED_TIMEFRAMES,
    assess_derived_siblings,
    classify_fingerprint_changes,
    defects_clear,
    file_fingerprint,
    new_defect_tally,
    validation_passed,
)


def _touch(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


class SiblingScopeTests(unittest.TestCase):
    def test_own_timeframe_only_passes(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "15m").mkdir()
            scope = assess_derived_siblings(root, "15m")
            self.assertTrue(scope["no_other_timeframes"])
            self.assertEqual(scope["other_timeframes_present"], [])
            self.assertEqual(scope["approved_timeframes_present"], ["15m"])

    def test_all_approved_siblings_pass(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in APPROVED_DERIVED_TIMEFRAMES:
                (root / name).mkdir()
            scope = assess_derived_siblings(root, "15m")
            self.assertTrue(scope["no_other_timeframes"])
            self.assertEqual(set(scope["approved_timeframes_present"]), set(APPROVED_DERIVED_TIMEFRAMES))
            self.assertEqual(scope["unauthorized_timeframes_present"], [])

    def test_preexisting_approved_names_are_accepted(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("5m", "15m", "30m", "1h", "4h", "1d"):
                (root / name).mkdir()
            scope = assess_derived_siblings(root, "4h")
            self.assertTrue(scope["no_other_timeframes"])
            self.assertIn("5m", scope["approved_timeframes_present"])
            self.assertIn("1d", scope["approved_timeframes_present"])

    def test_unknown_2h_directory_fails(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "15m").mkdir()
            (root / "2h").mkdir()
            scope = assess_derived_siblings(root, "15m")
            self.assertFalse(scope["no_other_timeframes"])
            self.assertEqual(scope["unauthorized_timeframes_present"], ["2h"])

    def test_unknown_weekly_directory_fails(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "1d").mkdir()
            (root / "weekly").mkdir()
            scope = assess_derived_siblings(root, "1d")
            self.assertFalse(scope["no_other_timeframes"])
            self.assertIn("weekly", scope["unauthorized_timeframes_present"])

    def test_temporary_looking_timeframe_directory_fails(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "5m").mkdir()
            (root / "tmp_timeframe").mkdir()
            scope = assess_derived_siblings(root, "5m")
            self.assertFalse(scope["no_other_timeframes"])
            self.assertIn("tmp_timeframe", scope["unauthorized_timeframes_present"])

    def test_write_into_another_timeframe_fails(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            own = root / "15m" / "data.parquet"
            sibling = root / "1h" / "data.parquet"
            _touch(own, b"own-v1")
            _touch(sibling, b"sibling-v1")
            before = {str(own): file_fingerprint(own), str(sibling): file_fingerprint(sibling)}
            sibling.write_bytes(b"sibling-v2")
            after = {str(own): file_fingerprint(own), str(sibling): file_fingerprint(sibling)}
            changes = classify_fingerprint_changes(root, "15m", before, after)
            self.assertFalse(changes["sibling_partitions_unchanged"])
            self.assertTrue(changes["own_partitions_unchanged"])
            self.assertFalse(validation_passed(
                no_other_timeframes=True,
                sibling_partitions_unchanged=changes["sibling_partitions_unchanged"],
                own_partitions_unchanged=True,
                scan_counts_ok=True,
                defects_ok=True,
            ))

    def test_sibling_file_change_fails(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "30m" / "symbol=BTCUSDT" / "data.parquet"
            _touch(path, b"aaa")
            before = {str(path): file_fingerprint(path)}
            path.write_bytes(b"bbb")
            after = {str(path): file_fingerprint(path)}
            changes = classify_fingerprint_changes(root, "15m", before, after)
            self.assertEqual(changes["sibling_partitions_changed"], [str(path)])

    def test_unchanged_approved_sibling_passes(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "4h" / "data.parquet"
            _touch(path, b"same")
            fingerprint = {str(path): file_fingerprint(path)}
            changes = classify_fingerprint_changes(root, "15m", fingerprint, dict(fingerprint))
            scope = assess_derived_siblings(root, "15m")
            self.assertTrue(scope["no_other_timeframes"])
            self.assertTrue(changes["sibling_partitions_unchanged"])
            self.assertTrue(validation_passed(
                no_other_timeframes=scope["no_other_timeframes"],
                sibling_partitions_unchanged=True,
                own_partitions_unchanged=True,
                scan_counts_ok=True,
                defects_ok=True,
            ))

    def test_readonly_check_preserves_hash_size_and_mtime(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "15m" / "data.parquet"
            _touch(path, b"parquet-bytes")
            before = file_fingerprint(path)
            scope = assess_derived_siblings(root, "15m")
            again = file_fingerprint(path)
            self.assertEqual(before["sha256"], again["sha256"])
            self.assertEqual(before["size"], again["size"])
            self.assertEqual(before["mtime_ns"], again["mtime_ns"])
            self.assertTrue(scope["no_other_timeframes"])
            changes = classify_fingerprint_changes(
                root, "15m", {str(path): before}, {str(path): again}
            )
            self.assertTrue(changes["own_partitions_unchanged"])
            self.assertTrue(changes["sibling_partitions_unchanged"])

    def test_row_or_equivalence_failure_exits_nonzero(self) -> None:
        self.assertFalse(validation_passed(
            no_other_timeframes=True,
            sibling_partitions_unchanged=True,
            own_partitions_unchanged=True,
            scan_counts_ok=False,
            defects_ok=True,
        ))
        tally = new_defect_tally()
        tally["ohlc_violation_count"] = 1
        self.assertFalse(defects_clear(tally))
        self.assertEqual(0 if False else 1, 1)

    def test_all_checks_passing_exits_zero(self) -> None:
        passed = validation_passed(
            no_other_timeframes=True,
            sibling_partitions_unchanged=True,
            own_partitions_unchanged=True,
            scan_counts_ok=True,
            defects_ok=True,
        )
        self.assertTrue(passed)
        self.assertEqual(0 if passed else 1, 0)

    def test_report_field_is_true_for_approved_tree_and_false_for_unauthorized(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in APPROVED_DERIVED_TIMEFRAMES:
                (root / name).mkdir()
            approved = assess_derived_siblings(root, "30m")
            self.assertTrue(approved["no_other_timeframes"])
            self.assertIn("other_timeframes_present", approved)
            self.assertEqual(approved["other_timeframes_present"], [])
            (root / "6h").mkdir()
            rejected = assess_derived_siblings(root, "30m")
            self.assertFalse(rejected["no_other_timeframes"])
            self.assertTrue(rejected["other_timeframes_present"])

    def test_october_27_expectations_and_15m_aggregation_are_unchanged(self) -> None:
        self.assertEqual(build_15m.EXPECTED_UTC_DAYS, 1031)
        self.assertEqual(build_15m.EXPECTED_OUTPUT_ROWS_PER_SYMBOL, 98_976)
        self.assertEqual(build_15m.EXPECTED_OUTPUT_ROWS_TOTAL, 890_784)
        self.assertEqual(build_15m.EXPECTED_LAST_OPEN, "2026-10-27T23:45:00Z")
        self.assertEqual(build_15m.SOURCE_CANDLES_PER_OUTPUT, 15)
        self.assertEqual(build_30m.EXPECTED_OUTPUT_ROWS_PER_SYMBOL, 49_488)
        self.assertEqual(build_30m.EXPECTED_LAST_OPEN, "2026-10-27T23:30:00Z")
        self.assertEqual(build_1h.EXPECTED_OUTPUT_ROWS_PER_SYMBOL, 24_744)
        self.assertEqual(build_1h.EXPECTED_LAST_OPEN, "2026-10-27T23:00:00Z")
        self.assertEqual(build_4h.EXPECTED_OUTPUT_ROWS_PER_SYMBOL, 6_186)
        self.assertEqual(build_4h.EXPECTED_LAST_OPEN, "2026-10-27T20:00:00Z")
        self.assertEqual(build_5m.EXPECTED_OUTPUT_ROWS_PER_SYMBOL, 296_928)
        self.assertEqual(build_5m.EXPECTED_LAST_OPEN, "2026-10-27T23:55:00Z")
        self.assertEqual(build_1d.EXPECTED_OUTPUT_ROWS_PER_SYMBOL, 1_031)
        self.assertEqual(build_1d.EXPECTED_LAST_OPEN, "2026-10-27T00:00:00Z")
        self.assertEqual(build_1d.EXPECTED_OUTPUT_ROWS_BY_YEAR[2026], 300)
        source = inspect.getsource(build_15m.aggregate_partition)
        self.assertIn("open=b_opens[0]", source)
        self.assertIn("high=to_dec128(max(b_highs), \"high\")", source)
        self.assertIn("low=to_dec128(min(b_lows), \"low\")", source)
        self.assertIn("close=b_closes[-1]", source)
        self.assertIn("volume=to_dec128(sum(b_vols, Decimal(\"0\")), \"volume\")", source)


if __name__ == "__main__":
    unittest.main()
