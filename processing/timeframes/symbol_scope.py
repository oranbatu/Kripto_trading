"""Symbol selection for derived USD-M publication.

The default write set remains the original nine symbols. ``--new-symbols-only``
selects the four canonical symbols that were added later. Read-only validation
follows the shared catalog unless that same flag narrows it.
"""
from __future__ import annotations

import sys
from pathlib import Path

import config

NEW_SYMBOLS: tuple[str, ...] = config.NEW_CANONICAL_1M_SYMBOLS


def publication_symbols(default: tuple[str, ...]) -> tuple[str, ...]:
    """Symbols a builder may write. The default path does not expand to new symbols."""
    if "--new-symbols-only" in sys.argv:
        return config.require_canonical_symbols(tuple(NEW_SYMBOLS))
    if tuple(default) != tuple(config.ALLOWED_SYMBOLS):
        raise ValueError("derived publication default must remain the original nine symbols")
    return tuple(default)


def validation_symbols(timeframe: str) -> tuple[str, ...]:
    """Symbols a read-only validator checks. New-symbol runs stay on the four."""
    if "--new-symbols-only" in sys.argv:
        return config.require_canonical_symbols(tuple(NEW_SYMBOLS))
    return tuple(config.symbols_for_timeframe(timeframe))


def recognized_symbols() -> frozenset[str]:
    """Canonical symbols that may exist beside the symbols selected for this run."""
    return frozenset(config.CANONICAL_1M_SYMBOLS)


def hive_symbol(path: Path) -> str | None:
    for part in path.parts:
        if part.startswith("symbol="):
            return part.split("=", 1)[1]
    return None


def selected_files(files: list[Path], selected: tuple[str, ...]) -> list[Path]:
    chosen = set(selected)
    return [path for path in files if hive_symbol(path) in chosen]


def unknown_symbols(disk_symbols: list[str] | set[str]) -> list[str]:
    recognized = recognized_symbols()
    return sorted(symbol for symbol in disk_symbols if symbol not in recognized)


def scale_from_nine(total_for_nine: int, selected_count: int) -> int:
    """Scale a nine-symbol inventory total to the number of symbols in this run."""
    if total_for_nine % 9:
        raise ValueError(f"nine-symbol total {total_for_nine} is not divisible by 9")
    return (total_for_nine // 9) * selected_count
