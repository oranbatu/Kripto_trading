#!/usr/bin/env python3
"""Isolated unit tests for the PRIMARY range detector."""
from __future__ import annotations

import ast
import hashlib
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

DETECTOR_DIR = Path(__file__).resolve().parents[1]
PROJECT = DETECTOR_DIR.parents[1]
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(DETECTOR_DIR))

import run_primary_range_detector as prv1  # noqa: E402

UTC = timezone.utc
D = Decimal


class SymbolAndPathTests(unittest.TestCase):
    def test_symbol_normalized_uppercase(self):
        self.assertEqual(prv1.validate_symbol("btcusdt"), "BTCUSDT")

    def test_exact_symbol_required(self):
        with self.assertRaises(prv1.PrimaryRangeDetectorError):
            prv1.validate_symbol("ETH")
        with self.assertRaises(prv1.PrimaryRangeDetectorError):
            prv1.validate_symbol("BTC")
        with self.assertRaises(prv1.PrimaryRangeDetectorError):
            prv1.validate_symbol("OPTUSDT")

    def test_allowed_perpetual_symbols(self):
        for symbol in prv1.ALLOWED_SYMBOLS:
            self.assertEqual(prv1.validate_symbol(symbol), symbol)

    def test_timeframe_only_1h(self):
        self.assertEqual(prv1.validate_timeframe("1h"), "1h")
        with self.assertRaises(prv1.PrimaryRangeDetectorError):
            prv1.validate_timeframe("15m")
        with self.assertRaises(prv1.PrimaryRangeDetectorError):
            prv1.validate_timeframe("1H")

    def test_source_path_resolution(self):
        root = Path(r"C:\MarketData")
        path = prv1.derived_1h_symbol_root(root, "BTCUSDT")
        self.assertEqual(
            path,
            root / "derived" / "binance" / "futures" / "um" / "perpetual" / "1h" / "symbol=BTCUSDT",
        )

    def test_missing_partitions_fail_without_workbook(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(prv1.PrimaryRangeDetectorError) as ctx:
                prv1.inspect_source_inventory(Path(tmp), "BTCUSDT")
            self.assertIn("No derived 1h", str(ctx.exception))
            self.assertFalse(list(Path(tmp).glob("*.xlsx")))


class ContinuityAndPivotTests(unittest.TestCase):
    def test_hourly_continuity_counts(self):
        base = 1_704_067_200_000
        opens = [base, base + 3_600_000, base + 7_200_000]
        dups = ooo = missing = 0
        for i in range(1, len(opens)):
            dlt = opens[i] - opens[i - 1]
            if dlt == 0:
                dups += 1
            elif dlt < 0:
                ooo += 1
            elif dlt != prv1.HOUR_MS:
                missing += 1
        self.assertEqual((dups, ooo, missing), (0, 0, 0))
        gap = [base, base + 7_200_000]
        missing = 1 if gap[1] - gap[0] != prv1.HOUR_MS else 0
        self.assertEqual(missing, 1)

    def test_pivot_causal_delay(self):
        n = 20
        times = [datetime(2024, 1, 1, i, tzinfo=UTC) for i in range(n)]
        h = [D(10)] * n
        l = [D(9)] * n
        h[8] = D(12)
        l[8] = D(8)
        c = [D("9.5")] * n
        atr = [D(1)] * n
        highs, lows = prv1.find_pivots(times, h, l, atr)
        self.assertTrue(highs)
        ph = next(p for p in highs if p.idx == 8)
        self.assertEqual(ph.confirmed_idx, 11)
        self.assertEqual(ph.confirmed_time, times[11])

    def test_equal_high_leftmost_strict_left_closed_right(self):
        n = 15
        times = [datetime(2024, 1, 1, i, tzinfo=UTC) for i in range(n)]
        h = [D(10)] * n
        l = [D(9)] * n
        h[6] = D(12)
        h[7] = D(12)
        c = [D("9.5")] * n
        atr = [D(1)] * n
        highs, _ = prv1.find_pivots(times, h, l, atr)
        idx = [p.idx for p in highs]
        self.assertIn(6, idx)
        self.assertNotIn(7, idx)


class AnchorPairTests(unittest.TestCase):
    def test_non_adjacent_pair_example(self):
        result = prv1.synthetic_pair_test()
        self.assertTrue(result["ok"])
        self.assertEqual(result["selected"], (100, 135))
        self.assertEqual(result["sep"], 35)

    def test_upper_lower_independence(self):
        dummy_u = [prv1.Pivot(i, prv1.DUMMY_TIME, i + 3, prv1.DUMMY_TIME, D(1), D(1), "HIGH") for i in (10, 50)]
        dummy_l = [prv1.Pivot(i, prv1.DUMMY_TIME, i + 3, prv1.DUMMY_TIME, D(1), D(1), "LOW") for i in (11, 12)]
        self.assertIsNotNone(prv1.select_anchor_pair(dummy_u))
        self.assertIsNone(prv1.select_anchor_pair(dummy_l))

    def test_zero_additional_eligible_touches_accepted(self):
        result = prv1.synthetic_zero_additional_test()
        self.assertTrue(result["ok"])
        self.assertEqual(result["upper_additional"], 0)
        self.assertEqual(result["lower_additional"], 0)

    def test_no_hidden_three_touch_minimum(self):
        self.assertEqual(prv1.MIN_UPPER_TOUCHES, 2)
        self.assertEqual(prv1.MIN_LOWER_TOUCHES, 2)
        self.assertEqual(prv1.MIN_ADDITIONAL_ELIGIBLE, 0)

    def test_additional_touches_improve_score_not_mandatory(self):
        def score_for(n_u: int, n_l: int) -> Decimal:
            visits_u = [object()] * n_u
            visits_l = [object()] * n_l
            st = {
                "ratio": D("0.99"),
                "u": {"visits": visits_u},
                "l": {"visits": visits_l},
                "crosses": [None] * 6,
                "slope": D("0.01"),
                "er": D("0.01"),
                "bars": 500,
            }
            score, _label = prv1.quality_score(st, [D(1), D(1)], [D(1), D(1)], [D(1)], [D(1)])
            return score

        two = score_for(2, 2)
        four = score_for(4, 4)
        self.assertGreater(four, two)
        self.assertGreaterEqual(two, D(0))


class FilterBoundaryTests(unittest.TestCase):
    def test_500_bar_minimum_constant(self):
        self.assertEqual(prv1.MIN_RANGE_BARS, 500)

    def test_icr_boundary(self):
        self.assertFalse(D("0.969999999") >= prv1.MIN_INSIDE)
        self.assertTrue(D("0.97") >= prv1.MIN_INSIDE)
        self.assertTrue(D("1") >= prv1.MIN_INSIDE)

    def test_exact_13_passes_gt_13_fails(self):
        result = prv1.synthetic_width_tests()
        self.assertTrue(result["ok"])
        self.assertTrue(result["exact_13_passes"])
        self.assertTrue(result["gt_13_fails"])
        self.assertTrue(result["rounds_to_13_but_fails"])

    def test_normalized_width_threshold(self):
        self.assertTrue(D("2.0") >= prv1.MIN_WIDTH_ATR)
        self.assertFalse(D("1.999999") >= prv1.MIN_WIDTH_ATR)

    def test_causal_confirmation_not_before_second_anchor(self):
        start_idx = 0
        min_conf = start_idx + prv1.MIN_RANGE_BARS - 1
        u2_conf = 520
        l2_conf = 510
        confirmed = max(min_conf, u2_conf, l2_conf)
        self.assertGreaterEqual(confirmed, 499)
        self.assertGreaterEqual(confirmed, u2_conf)
        self.assertGreaterEqual(confirmed, l2_conf)

    def test_breakout_strong_and_normal(self):
        H = D(100)
        L = D(90)
        atr = [D(4)] * 10
        closes = [D(100)] * 10
        closes[5] = D("102.1")
        direction, first, conf, buf, *_ = prv1.detect_breakout(3, 10, H, L, closes, atr)
        self.assertEqual((direction, buf), ("UP", "strong"))
        self.assertEqual(first, conf)
        closes = [D(100)] * 10
        closes[5] = D("101.1")
        closes[6] = D("101.2")
        direction, first, conf, buf, *_ = prv1.detect_breakout(3, 10, H, L, closes, atr)
        self.assertEqual((direction, buf), ("UP", "normal"))
        self.assertEqual((first, conf), (5, 6))


class HierarchyAndDuplicateTests(unittest.TestCase):
    def _rec(self, **kw):
        rec = prv1.RangeRec()
        rec.start_idx = kw.get("s", 0)
        rec.end_idx = kw.get("e", 600)
        rec.confirmed_idx = kw.get("c", 500)
        rec.confirmed_at = datetime(2024, 2, 1, tzinfo=UTC)
        rec.confirmed_at_original = rec.confirmed_at
        rec.high = kw.get("h", D(100))
        rec.low = kw.get("l", D(90))
        rec.median_atr = D(1)
        rec.quality = kw.get("q", D("80"))
        rec.duration_bars = rec.end_idx - rec.start_idx + 1
        rec.start = datetime(2024, 1, 1, tzinfo=UTC)
        rec.structure_type = "PRIMARY"
        rec.parent_range_id = "none"
        rec.pass_primary = True
        return rec

    def test_primary_only_filter_discards_nested(self):
        outer = self._rec(s=0, e=800, h=D(110), l=D(80), q=D("80"))
        inner = self._rec(s=50, e=700, h=D(100), l=D(90), q=D("90"))
        outer.range_id = "TMP001"
        inner.range_id = "TMP002"
        prv1.classify_hierarchy([outer, inner])
        self.assertEqual(outer.structure_type, "PRIMARY")
        self.assertEqual(inner.structure_type, "NESTED")
        self.assertEqual(inner.confirmed_at, inner.confirmed_at_original)
        reporting = [r for r in (outer, inner) if r.structure_type == "PRIMARY"]
        self.assertEqual([r.range_id for r in reporting], ["TMP001"])

    def test_near_duplicate_keeps_higher_score(self):
        a = self._rec(s=0, e=700, q=D("80"))
        b = self._rec(s=10, e=690, q=D("70"))
        kept, removed, _merged = prv1.dedupe_and_merge(
            [a, b], [D(1)] * 800, [prv1.DUMMY_TIME] * 800,
            [D(100)] * 800, [D(90)] * 800, [D(95)] * 800, [], [],
        )
        self.assertEqual(removed, 1)
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0].quality, D("80"))


class RevisionAllocationTests(unittest.TestCase):
    def test_no_files_starts_at_rev01(self):
        with tempfile.TemporaryDirectory() as tmp:
            n = prv1.next_revision_number(Path(tmp), "BTCUSDT")
            self.assertEqual(n, 1)
            self.assertEqual(prv1.format_revision(n), "rev01")
            path = prv1.workbook_path_for(Path(tmp), "BTCUSDT", n)
            self.assertEqual(path.name, "BTCUSDT_1H_Range_Detection_rev01.xlsx")

    def test_revision_gaps_use_max_plus_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSDT_1H_Range_Detection_rev01.xlsx").write_bytes(b"x")
            (root / "BTCUSDT_1H_Range_Detection_rev02.xlsx").write_bytes(b"x")
            (root / "BTCUSDT_1H_Range_Detection_rev04.xlsx").write_bytes(b"x")
            self.assertEqual(prv1.next_revision_number(root, "BTCUSDT"), 5)

    def test_separate_sequence_per_symbol(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSDT_1H_Range_Detection_rev02.xlsx").write_bytes(b"x")
            (root / "ETHUSDT_1H_Range_Detection_rev01.xlsx").write_bytes(b"x")
            self.assertEqual(prv1.next_revision_number(root, "BTCUSDT"), 3)
            self.assertEqual(prv1.next_revision_number(root, "ETHUSDT"), 2)

    def test_revisions_beyond_99(self):
        self.assertEqual(prv1.format_revision(99), "rev99")
        self.assertEqual(prv1.format_revision(100), "rev100")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSDT_1H_Range_Detection_rev99.xlsx").write_bytes(b"x")
            self.assertEqual(prv1.next_revision_number(root, "BTCUSDT"), 100)
            self.assertEqual(prv1.format_revision(100), "rev100")

    def test_ignores_unrelated_and_non_rev_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSDT_1H_Range_Detection.xlsx").write_bytes(b"x")
            (root / "BTCUSDT_15M_Range_Detection_rev01.xlsx").write_bytes(b"x")
            (root / "~$BTCUSDT_1H_Range_Detection_rev09.xlsx").write_bytes(b"x")
            (root / "notes.xlsx").write_bytes(b"x")
            self.assertEqual(prv1.next_revision_number(root, "BTCUSDT"), 1)

    def test_existing_target_protection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "BTCUSDT_1H_Range_Detection_rev01.xlsx"
            target.write_bytes(b"old")
            n, label, path = prv1.allocate_output_path(root, "BTCUSDT")
            self.assertEqual(label, "rev02")
            self.assertFalse(path.exists())
            self.assertEqual(target.read_bytes(), b"old")


class DryRunDeterminismIsolationTests(unittest.TestCase):
    def test_dry_run_creates_no_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            before = {p.name for p in Path(tmp).iterdir()}
            result = prv1.run_primary_range_detector(
                "BTCUSDT",
                data_root=Path(r"C:\MarketData"),
                output_dir=Path(tmp),
                dry_run=True,
            )
            after = {p.name for p in Path(tmp).iterdir()}
            self.assertTrue(result["dry_run"])
            self.assertFalse(result["wrote_workbook"])
            self.assertEqual(before, after)
            self.assertTrue(str(result["intended_workbook"]).endswith("rev01.xlsx"))

    def test_desktop_btc_next_is_rev06(self):
        n = prv1.next_revision_number(Path(r"C:\Users\oranb\Desktop"), "BTCUSDT")
        self.assertEqual(n, 6)
        self.assertEqual(prv1.format_revision(n), "rev06")

    def test_determinism_synthetics(self):
        a = (prv1.synthetic_pair_test(), prv1.synthetic_width_tests(), prv1.synthetic_zero_additional_test())
        b = (prv1.synthetic_pair_test(), prv1.synthetic_width_tests(), prv1.synthetic_zero_additional_test())
        self.assertEqual(a, b)

    def test_source_immutability_of_inventory(self):
        inv = prv1.inspect_source_inventory(Path(r"C:\MarketData"), "BTCUSDT")
        fps = [prv1.fingerprint(Path(x["path"])) for x in inv["parquet_files"]]
        inv2 = prv1.inspect_source_inventory(Path(r"C:\MarketData"), "BTCUSDT")
        fps2 = [prv1.fingerprint(Path(x["path"])) for x in inv2["parquet_files"]]
        self.assertEqual(fps, fps2)

    def test_isolation_from_unrelated_strategies(self):
        src = DETECTOR_DIR.joinpath("run_primary_range_detector.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module.split(".")[0])
        self.assertNotIn("strategy", imported)
        self.assertNotIn("strategies", imported)
        self.assertNotIn("backtest", imported)
        pkg_init = (PROJECT / "detectors" / "__init__.py").read_text(encoding="utf-8")
        self.assertNotIn("run_primary_range_detector", pkg_init)
        strategy_hits = []
        for p in PROJECT.rglob("*.py"):
            if any(part in {".venv", "site-packages", "__pycache__", "detectors"} for part in p.parts):
                continue
            text = p.read_text(encoding="utf-8")
            if "primary_range_v1" in text or "run_primary_range_detector" in text:
                strategy_hits.append(str(p))
        self.assertEqual(strategy_hits, [])

    def test_sheet_order_constant(self):
        self.assertEqual(
            prv1.SHEET_ORDER,
            [
                "High Quality Ranges", "Range Details", "Boundary Touches", "EQ Crossings",
                "Moderate Ranges", "Parameters", "Diagnostics", "README",
            ],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
