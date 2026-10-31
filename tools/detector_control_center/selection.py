"""Checkbox selection rules for the detector control center."""
from __future__ import annotations

from tools.detector_control_center.registry import ALL_CATEGORIES, TASKS, DetectorTask


class SelectionState:
    def __init__(self, tasks: tuple[DetectorTask, ...] = TASKS) -> None:
        self.tasks = tasks
        self.selected: set[str] = set()
        self.category = ALL_CATEGORIES
        self.query = ""

    def visible(self) -> tuple[DetectorTask, ...]:
        query = self.query.casefold().strip()
        rows: list[DetectorTask] = []
        for task in self.tasks:
            if self.category != ALL_CATEGORIES and task.category != self.category:
                continue
            if query:
                haystack = " ".join(
                    (
                        task.display_name,
                        task.symbol,
                        task.category,
                        task.description,
                        task.workbook_family,
                        " ".join(task.timeframes),
                        task.disabled_reason,
                    )
                ).casefold()
                if query not in haystack:
                    continue
            rows.append(task)
        return tuple(rows)

    def set_selected(self, task_id: str, selected: bool) -> None:
        task = self._require(task_id)
        if not task.enabled:
            self.selected.discard(task_id)
            return
        if selected:
            self.selected.add(task_id)
        else:
            self.selected.discard(task_id)

    def select_visible(self) -> None:
        for task in self.visible():
            if task.enabled:
                self.selected.add(task.id)

    def clear(self) -> None:
        self.selected.clear()

    def ordered_ids(self) -> tuple[str, ...]:
        return tuple(task.id for task in self.tasks if task.id in self.selected and task.enabled)

    def ordered_tasks(self) -> tuple[DetectorTask, ...]:
        return tuple(task for task in self.tasks if task.id in self.selected and task.enabled)

    def count(self) -> int:
        return len(self.ordered_ids())

    def _require(self, task_id: str) -> DetectorTask:
        for task in self.tasks:
            if task.id == task_id:
                return task
        raise KeyError(task_id)
