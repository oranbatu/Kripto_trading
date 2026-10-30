"""Derived-tree sibling scope for USD-M timeframe builders.

``other_timeframes_present`` and ``no_other_timeframes`` keep their historical
report names. Their meaning is the allowlist check below, not "any other
timeframe directory exists".

Approved derived directories may already exist beside the builder's own output:

    5m, 15m, 30m, 1h, 4h, 1d

Those names are expected and do not set ``no_other_timeframes`` to false.
Any other immediate child directory (``2h``, ``weekly``, ``tmp_timeframe``,
and the rest) is unauthorized and does.

A second check compares file fingerprints. Validation-only mode must leave
every production Parquet file unchanged. A publication may replace only the
builder's own timeframe. A new or modified file under any other directory
fails, including a write into an approved sibling.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

APPROVED_DERIVED_TIMEFRAMES: tuple[str, ...] = ("5m", "15m", "30m", "1h", "4h", "1d")
APPROVED_DERIVED_TIMEFRAME_SET = frozenset(APPROVED_DERIVED_TIMEFRAMES)

DEFECT_FIELDS: tuple[str, ...] = (
    "source_rows_omitted",
    "source_rows_reused",
    "missing_bucket_count",
    "duplicate_bucket_count",
    "incomplete_bucket_count",
    "out_of_order_count",
    "null_count",
    "ohlc_violation_count",
    "negative_volume_count",
    "rows_outside_range",
)


def new_defect_tally() -> dict[str, int]:
    return {name: 0 for name in DEFECT_FIELDS}


def add_work_defects(tally: dict[str, int], work) -> None:
    tally["source_rows_omitted"] += int(work.source_rows_omitted)
    tally["source_rows_reused"] += int(work.source_rows_reused)
    tally["missing_bucket_count"] += int(work.missing_bucket_count)
    tally["duplicate_bucket_count"] += int(work.duplicate_bucket_count)
    tally["incomplete_bucket_count"] += len(work.incomplete_buckets)
    tally["out_of_order_count"] += int(work.out_of_order_count)
    tally["null_count"] += int(work.null_count)
    tally["ohlc_violation_count"] += int(work.ohlc_violation_count)
    tally["negative_volume_count"] += int(work.negative_volume_count)
    tally["rows_outside_range"] += int(work.rows_outside_range)


def defects_clear(tally: dict[str, int]) -> bool:
    return all(int(tally.get(name, 0)) == 0 for name in DEFECT_FIELDS)


def directory_scope(parent: Path) -> dict:
    """Classify immediate child directories against the approved timeframe allowlist.

    ``other_timeframes_present`` lists unauthorized directories only.
    ``no_other_timeframes`` is true when every child name is on the allowlist.
    """
    names: list[str] = []
    if parent.is_dir():
        names = sorted(path.name for path in parent.iterdir() if path.is_dir())
    unauthorized = [name for name in names if name not in APPROVED_DERIVED_TIMEFRAME_SET]
    return {
        "approved_timeframes_present": [name for name in names if name in APPROVED_DERIVED_TIMEFRAME_SET],
        "unauthorized_timeframes_present": unauthorized,
        "other_timeframes_present": [str(parent / name) for name in unauthorized],
        "no_other_timeframes": not unauthorized,
    }


def assess_derived_siblings(parent: Path, own_interval: str) -> dict:
    """Same directory allowlist, plus the builder's own timeframe name."""
    if own_interval not in APPROVED_DERIVED_TIMEFRAME_SET:
        raise ValueError(f"{own_interval} is not an approved derived timeframe")
    scope = directory_scope(parent)
    scope["own_interval"] = own_interval
    return scope


def file_fingerprint(path: Path) -> dict:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    stat = path.stat()
    return {"sha256": hasher.hexdigest(), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def fingerprint_tree(root: Path, pattern: str = "data.parquet") -> dict[str, dict]:
    found: dict[str, dict] = {}
    if not root.exists():
        return found
    for path in sorted(root.rglob(pattern)):
        if path.is_file() and "quarantine" not in path.parts:
            found[str(path)] = file_fingerprint(path)
    return found


def _timeframe_of(parent: Path, file_path: Path) -> str | None:
    try:
        relative = file_path.resolve().relative_to(parent.resolve())
    except ValueError:
        return None
    return relative.parts[0] if relative.parts else None


def classify_fingerprint_changes(
    parent: Path,
    own_interval: str,
    before: dict[str, dict],
    after: dict[str, dict],
) -> dict:
    """Split fingerprint differences into the builder's own tree and everything else."""
    own_changed: list[str] = []
    sibling_changed: list[str] = []
    for key in sorted(set(before) | set(after)):
        if before.get(key) == after.get(key):
            continue
        timeframe = _timeframe_of(parent, Path(key))
        if timeframe == own_interval:
            own_changed.append(key)
        else:
            sibling_changed.append(key)
    return {
        "own_partitions_changed": own_changed,
        "sibling_partitions_changed": sibling_changed,
        "own_partitions_unchanged": not own_changed,
        "sibling_partitions_unchanged": not sibling_changed,
    }


def validation_passed(
    *,
    no_other_timeframes: bool,
    sibling_partitions_unchanged: bool,
    own_partitions_unchanged: bool,
    scan_counts_ok: bool,
    defects_ok: bool,
) -> bool:
    return bool(
        no_other_timeframes
        and sibling_partitions_unchanged
        and own_partitions_unchanged
        and scan_counts_ok
        and defects_ok
    )
