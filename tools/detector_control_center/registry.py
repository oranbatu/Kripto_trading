"""Explicit registry of production detector tasks.

Entries are written by hand from the official runners. The control center
does not discover modules by scanning the filesystem.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALL_CATEGORIES = "All Detectors"


@dataclass(frozen=True, slots=True)
class DetectorTask:
    id: str
    display_name: str
    category: str
    description: str
    module: str
    args: tuple[str, ...]
    symbol: str
    timeframes: tuple[str, ...]
    workbook_family: str
    production: bool
    enabled: bool
    disabled_reason: str = ""

    def source_file(self) -> Path | None:
        if not self.module:
            return None
        return PROJECT_ROOT.joinpath(*self.module.split(".")).with_suffix(".py")


def _price(symbol: str) -> DetectorTask:
    lowered = symbol.lower()
    return DetectorTask(
        id=f"price_touch_{lowered}",
        display_name=f"{symbol} — Price Touch Hierarchy",
        category="Price Touch Hierarchy",
        description="Fixed-symbol wrapper. Publishes the next Most Touched Levels workbook.",
        module=f"detectors.price_touch_hierarchy_v1.runners.run_{lowered}_price_touch_hierarchy",
        args=(),
        symbol=symbol,
        timeframes=("1h", "4h", "1d"),
        workbook_family=f"{symbol}_MTF_Most_Touched_Levels_revNN.xlsx",
        production=True,
        enabled=True,
    )


TASKS: tuple[DetectorTask, ...] = (
    _price("BTCUSDT"),
    _price("ETHUSDT"),
    _price("AVAXUSDT"),
    _price("AAVEUSDT"),
    _price("NEARUSDT"),
    _price("LINKUSDT"),
    _price("LTCUSDT"),
    _price("OPUSDT"),
    _price("DOTUSDT"),
    DetectorTask(
        id="swing_30m_special",
        display_name="BTCUSDT — 30m Special Swing",
        category="Swing Detectors",
        description="No arguments. Publishes the next 30m special-swing workbook.",
        module="detectors.swing_open_close_30m_special_v1.run_swing_open_close_30m_special_detector",
        args=(),
        symbol="BTCUSDT",
        timeframes=("30m",),
        workbook_family="BTCUSDT_30M_Special_Swings_revNN.xlsx",
        production=True,
        enabled=True,
    ),
    DetectorTask(
        id="swing_4h_special",
        display_name="BTCUSDT — 4h Special Swing",
        category="Swing Detectors",
        description="No arguments. Optional test flags are not used.",
        module="detectors.swing_open_close_4h_special_v1.run_swing_open_close_4h_special_detector",
        args=(),
        symbol="BTCUSDT",
        timeframes=("4h",),
        workbook_family="BTCUSDT_4H_Special_Swings_revNN.xlsx",
        production=True,
        enabled=True,
    ),
    DetectorTask(
        id="hierarchical_swing_v4",
        display_name="BTCUSDT — 1h Hierarchical Swing",
        category="Swing Detectors",
        description="No arguments. Publishes the next 1h hierarchical-swing workbook.",
        module="detectors.hierarchical_swing_v4.run_hierarchical_swing_detector",
        args=(),
        symbol="BTCUSDT",
        timeframes=("1h",),
        workbook_family="BTCUSDT_1H_swing_revNN.xlsx",
        production=True,
        enabled=True,
    ),
    DetectorTask(
        id="open_liquidity_v1",
        display_name="BTCUSDT — Open Liquidity",
        category="Liquidity Detectors",
        description="No arguments. Publishes the next multi-timeframe open-liquidity workbook.",
        module="detectors.open_liquidity_v1.run_open_liquidity_detector",
        args=(),
        symbol="BTCUSDT",
        timeframes=("1h", "4h", "1d"),
        workbook_family="BTCUSDT_MTF_Open_Liquidity_revNN.xlsx",
        production=True,
        enabled=True,
    ),
    DetectorTask(
        id="primary_range_btcusdt",
        display_name="BTCUSDT — 1h Primary Range",
        category="Range Detectors",
        description="Official runner requires --symbol. This entry passes BTCUSDT only.",
        module="detectors.primary_range_v1.run_primary_range_detector",
        args=("--symbol", "BTCUSDT"),
        symbol="BTCUSDT",
        timeframes=("1h",),
        workbook_family="BTCUSDT_1H_Range_Detection_revNN.xlsx",
        production=True,
        enabled=True,
    ),
    DetectorTask(
        id="order_block_4h",
        display_name="BTCUSDT — 4h Order Block",
        category="Order Block Detectors",
        description="No arguments. Publishes the next 4h order-block workbook.",
        module="detectors.order_block_4h_v1.run_order_block_detector",
        args=(),
        symbol="BTCUSDT",
        timeframes=("4h",),
        workbook_family="BTCUSDT_4H_Order_Blocks_revNN.xlsx",
        production=True,
        enabled=True,
    ),
    DetectorTask(
        id="range_detector_4h_v1",
        display_name="BTCUSDT — 4h Range Detector",
        category="Range Detectors",
        description="Legacy package kept for reference.",
        module="",
        args=(),
        symbol="BTCUSDT",
        timeframes=("4h",),
        workbook_family="",
        production=False,
        enabled=False,
        disabled_reason="Bytecode only. No Python source runner exists, so this entry cannot be launched.",
    ),
)


def categories() -> tuple[str, ...]:
    found: list[str] = []
    for task in TASKS:
        if task.category not in found:
            found.append(task.category)
    return tuple(found)


def task_by_id(task_id: str) -> DetectorTask:
    for task in TASKS:
        if task.id == task_id:
            return task
    raise KeyError(task_id)


def enabled_tasks() -> tuple[DetectorTask, ...]:
    return tuple(task for task in TASKS if task.enabled)


def module_source_exists(task: DetectorTask) -> bool:
    path = task.source_file()
    return path is not None and path.is_file()
