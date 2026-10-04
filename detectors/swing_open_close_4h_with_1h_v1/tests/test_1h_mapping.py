"""1h mapping tests for the combined 4h swing detector."""
from __future__ import annotations

import copy
import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from detectors.swing_open_close_4h_with_1h_v1.engine import SWING_HIGH, SWING_LOW, Bar, analyze
from detectors.swing_open_close_4h_with_1h_v1.map_workbook import _thirty_index
from detectors.swing_open_close_4h_with_1h_v1.mapping import (
    MappingError,
    _map_one,
    align_parents,
    extreme_plateaus,
    map_primaries,
    result_signature,
)
from detectors.swing_open_close_4h_with_1h_v1.mapping_config import NOT_OBSERVED, PROTECTED_FINGERPRINTS
from detectors.swing_open_close_4h_with_1h_v1.run_swing_open_close_4h_with_1h_detector import scan_shared
from detectors.swing_open_close_4h_v1.engine import analyze as stable_analyze


def _child(opened: datetime, row: int, open_, high, low, close) -> Bar:
    return Bar(
        row=row,
        open_time=opened,
        close_time=opened + timedelta(hours=1) - timedelta(milliseconds=1),
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("1"),
        quote_volume=Decimal("2"),
        trades=1,
        taker_base=Decimal("0.4"),
        taker_quote=Decimal("0.8"),
    )


def _parent(start: datetime, row: int, specs: list[tuple[str, str, str, str]]) -> tuple[Bar, list[Bar]]:
    children = []
    for offset, values in enumerate(specs):
        children.append(_child(start + timedelta(hours=offset), row * 4 + offset, *values))
    parent = Bar(
        row=row,
        open_time=start,
        close_time=start + timedelta(hours=4) - timedelta(milliseconds=1),
        open=children[0].open,
        high=max(item.high for item in children),
        low=min(item.low for item in children),
        close=children[-1].close,
        volume=sum((item.volume for item in children), Decimal("0")),
        quote_volume=sum((item.quote_volume for item in children), Decimal("0")),
        trades=sum(item.trades for item in children),
        taker_base=sum((item.taker_base for item in children), Decimal("0")),
        taker_quote=sum((item.taker_quote for item in children), Decimal("0")),
    )
    return parent, children


def _flat(price: str = "100") -> list[tuple[str, str, str, str]]:
    return [(price, "101", "99", price) for _ in range(4)]


def _series(start: datetime, specs: list[list[tuple[str, str, str, str]]]):
    parents = []
    children = []
    for row, block in enumerate(specs):
        parent, kids = _parent(start + timedelta(hours=4 * row), row, block)
        parents.append(parent)
        children.extend(kids)
    return parents, children


def _high_series():
    start = datetime(2026, 1, 5, 4, tzinfo=timezone.utc)
    specs = [_flat(), _flat(), _flat()]
    specs[1][3] = ("105", "110", "104", "106")
    specs[2][0] = ("106", "107", "99", "104")
    specs[2][1] = ("104", "105", "99", "100")
    specs[2][3] = ("101", "102", "97", "98")
    return _series(start, specs)


def _alternative_series():
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    specs = [_flat("104") for _ in range(6)]
    specs[0] = _flat("100")
    specs[1][3] = ("105", "110", "104", "106")
    return _series(start, specs)


class MappingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        parents, children = _high_series()
        cls.parents = parents
        cls.children = children
        cls.links = align_parents(parents, children)
        cls.result = analyze(parents)
        cls.mapped = map_primaries(cls.result.swings, cls.links)

    def test_protected_packages_are_unchanged(self):
        for path, (digest, size, mtime_ns) in PROTECTED_FINGERPRINTS.items():
            file = Path(path)
            stat = file.stat()
            self.assertEqual(stat.st_size, size)
            self.assertEqual(stat.st_mtime_ns, mtime_ns)
            self.assertEqual(hashlib.sha256(file.read_bytes()).hexdigest(), digest)
            self.assertTrue(
                "swing_open_close_4h_v1" in str(file) or "swing_open_close_4h_with_30m_v1" in str(file)
            )

    def test_four_children_and_exact_ohlc_sums(self):
        self.assertEqual(len(self.links), 3)
        for link in self.links:
            self.assertEqual(len(link.children), 4)
            opens = [item.open_time for item in link.children]
            self.assertEqual(opens, [link.parent.open_time + timedelta(hours=offset) for offset in range(4)])
            self.assertEqual([item.minute for item in opens], [0, 0, 0, 0])
            self.assertEqual([offset + 1 for offset in range(4)], [1, 2, 3, 4])
            self.assertEqual(link.parent.open, link.children[0].open)
            self.assertEqual(link.parent.high, max(item.high for item in link.children))
            self.assertEqual(link.parent.low, min(item.low for item in link.children))
            self.assertEqual(link.parent.close, link.children[-1].close)
            self.assertEqual(link.parent.volume, sum(item.volume for item in link.children))
            self.assertEqual(link.parent.quote_volume, sum(item.quote_volume for item in link.children))
            self.assertEqual(link.parent.trades, sum(item.trades for item in link.children))
            self.assertEqual(link.parent.taker_base, sum(item.taker_base for item in link.children))
            self.assertEqual(link.parent.taker_quote, sum(item.taker_quote for item in link.children))
            self.assertEqual(link.children[-1].close_time, link.parent.close_time)
            self.assertTrue(link.passed)

    def test_swing_maps_to_formation_times_four(self):
        swing = next(item for item in self.mapped.swings if item.swing.direction == SWING_HIGH)
        self.assertEqual(len(swing.rows), swing.swing.total_candles * 4)
        self.assertEqual(swing.direct_open.open_time, swing.swing.open_time)
        self.assertEqual(swing.direct_open.open, swing.swing.reference)
        self.assertEqual(swing.rows[0]["offset"], 1)
        self.assertEqual([row["offset"] for row in swing.rows[:4]], [1, 2, 3, 4])

    def test_direct_close_rules(self):
        swing = next(item for item in self.mapped.swings if item.swing.direction == SWING_HIGH)
        self.assertEqual(swing.swing.direction, SWING_HIGH)
        self.assertEqual(swing.representative.high, Decimal("110"))
        self.assertEqual(swing.direct_close.close, Decimal("100"))
        self.assertNotEqual(swing.direct_close.open_time, swing.final_child.open_time)
        self.assertEqual(swing.final_child.close, Decimal("98"))
        final = next(row for row in swing.rows if row["is_final"])
        self.assertFalse(final["is_direct_close"])
        self.assertGreater(swing.wick_rejected, 0)
        self.assertTrue(swing.precedes_confirmation)
        self.assertTrue(swing.inside_official_close_block)
        self.assertEqual(swing.swing.confirmed_at, self.parents[2].close_time)
        self.assertTrue(all(row["child"].close_time <= swing.swing.confirmed_at for row in swing.rows if row["search_eligible"]))
        self.assertEqual(sum(1 for row in swing.rows if row["is_direct_close"]), 1)
        later = [row for row in swing.rows if row["condition_passed"]]
        self.assertGreater(len(later), 1)
        self.assertEqual(later[0]["child"].close, Decimal("100"))
        self.assertGreater(swing.direct_close.close_time, swing.representative.close_time)

    def test_missing_extreme_is_a_hard_failure(self):
        swing = next(item for item in self.result.primaries if item.direction == SWING_HIGH)
        original = swing.extreme_price
        swing.extreme_price = Decimal("999")
        with self.assertRaises(MappingError):
            _map_one(swing, self.links)
        swing.extreme_price = original

    def test_alternative_without_return_is_not_observed(self):
        parents, children = _alternative_series()
        links = align_parents(parents, children)
        result = analyze(parents)
        mapped = map_primaries(result.swings, links)
        alternative = [item for item in mapped.swings if item.swing.detection_method == "ALTERNATIVE_3_5_WIDTH"]
        self.assertTrue(alternative)
        self.assertIsNone(alternative[0].direct_close)
        self.assertEqual(alternative[0].direct_close_state, NOT_OBSERVED)
        self.assertEqual(alternative[0].swing.confirmed_at, alternative[0].final_child.close_time)

    def test_low_close_uses_greater_or_equal_and_plateau_last_match(self):
        start = datetime(2026, 4, 1, tzinfo=timezone.utc)
        specs = [_flat() for _ in range(3)]
        specs[1][1] = ("95", "100", "90", "96")
        specs[1][2] = ("95", "100", "90", "96")
        specs[2][0] = ("96", "101", "95", "99")
        specs[2][2] = ("99", "101", "98", "100")
        specs[2][3] = ("100", "103", "99", "102")
        parents, children = _series(start, specs)
        mapped = map_primaries(analyze(parents).swings, align_parents(parents, children))
        low = next(item for item in mapped.swings if item.swing.direction == SWING_LOW)
        self.assertEqual(low.representative.low, Decimal("90"))
        self.assertEqual(low.representative.open_time, start + timedelta(hours=6))
        groups = extreme_plateaus(low.matches)
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]), 2)
        self.assertEqual(low.direct_close.close, Decimal("100"))
        self.assertNotEqual(low.direct_close.open_time, low.final_child.open_time)
        self.assertTrue(low.precedes_confirmation)
        self.assertEqual(low.swing.confirmed_at, parents[-1].close_time)

    def test_separated_extreme_matches_stay_separate(self):
        start = datetime(2026, 4, 8, tzinfo=timezone.utc)
        specs = [_flat() for _ in range(3)]
        specs[1][0] = ("95", "100", "90", "96")
        specs[1][2] = ("95", "100", "90", "96")
        specs[2][2] = ("99", "101", "98", "100")
        specs[2][3] = ("100", "103", "99", "102")
        parents, children = _series(start, specs)
        mapped = map_primaries(analyze(parents).swings, align_parents(parents, children))
        low = next(item for item in mapped.swings if item.swing.direction == SWING_LOW)
        groups = extreme_plateaus(low.matches)
        self.assertEqual([len(group) for group in groups], [1, 1])
        self.assertEqual(low.representative.open_time, groups[-1][-1].open_time)
        self.assertEqual(low.representative.open_time, start + timedelta(hours=6))

    def test_overlap_keeps_distinct_swing_rows(self):
        start = datetime(2026, 5, 1, tzinfo=timezone.utc)
        specs = [_flat() for _ in range(4)]
        specs[1][3] = ("105", "110", "104", "106")
        specs[2][1] = ("99", "100", "90", "97")
        specs[2][3] = ("97", "99", "96", "98")
        specs[3][3] = ("100", "106", "99", "105")
        parents, children = _series(start, specs)
        mapped = map_primaries(analyze(parents).swings, align_parents(parents, children))
        keys = [(item.swing.primary_id, row["child"].open_time) for item in mapped.swings for row in item.rows]
        self.assertEqual(len(keys), len(set(keys)))
        stamps = {}
        for item in mapped.swings:
            for row in item.rows:
                stamps.setdefault(row["child"].open_time, set()).add(item.swing.primary_id)
        self.assertTrue(any(len(ids) > 1 for ids in stamps.values()))

    def test_turkey_conversion_and_index_widths(self):
        swing = next(item for item in self.mapped.swings if item.swing.direction == SWING_HIGH)
        text = __import__(
            "detectors.swing_open_close_4h_with_1h_v1.engine", fromlist=["iso_turkey"]
        ).iso_turkey(swing.direct_open.close_time)
        self.assertTrue(text.endswith("+03:00"))
        self.assertNotEqual(swing.direct_open.close_time, self.parents[0].close_time)
        rows = _thirty_index(self.mapped, {"mapping_version": "V", "run_id": "R", "revision_name": "rev00"})
        self.assertEqual(len(rows[0]), len(rows[1]))

    def test_revision_ignores_30m_workbooks(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "BTCUSDT_4H_Swing_Structure_rev02.xlsx").write_bytes(b"PK")
            (root / "BTCUSDT_4H_Swings_on_1H_rev05.xlsx").write_bytes(b"PK")
            (root / "BTCUSDT_4H_Swings_on_30M_rev09.xlsx").write_bytes(b"PK")
            found = scan_shared(root)
            self.assertEqual(found, [2, 5])
            self.assertEqual(max(found) + 1, 6)
            self.assertTrue((root / "BTCUSDT_4H_Swings_on_1H_rev05.xlsx").exists())

    def test_new_detector_matches_stable_baseline_on_synthetic_bars(self):
        stable = stable_analyze(copy.deepcopy(self.parents))
        self.assertEqual(
            [(item.primary_id, item.open_row, item.close_row, str(item.extreme_price)) for item in self.result.primaries],
            [(item.primary_id, item.open_row, item.close_row, str(item.extreme_price)) for item in stable.primaries],
        )

    def test_repeated_mapping_is_identical(self):
        again = map_primaries(analyze(copy.deepcopy(self.parents)).swings, self.links)
        self.assertEqual(result_signature(self.mapped), result_signature(again))

    def test_duplicate_child_is_rejected(self):
        parents, children = _high_series()
        children.append(children[0])
        with self.assertRaises(MappingError):
            align_parents(parents, children)


if __name__ == "__main__":
    unittest.main()
