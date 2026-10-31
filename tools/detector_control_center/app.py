"""Official entry point for the Detector Control Center.

Run from the project root:

    .venv\\Scripts\\python.exe -m tools.detector_control_center.app
"""
from __future__ import annotations

import argparse
from pathlib import Path

from tools.detector_control_center.executor import (
    DEFAULT_LOCK,
    DEFAULT_LOG_DIR,
    InterpreterError,
    SessionBusy,
    SessionLock,
    assert_project_interpreter,
    final_exit_code,
    run_tasks,
)
from tools.detector_control_center.registry import DetectorTask
from tools.detector_control_center.ui import ControlCenterWindow, show_control_center


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="detector_control_center")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the selected module commands and do not publish workbooks.",
    )
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="Open the window, measure it, and close without running a detector.",
    )
    return parser.parse_args(argv)


def main(
    argv: list[str] | None = None,
    *,
    lock_path: Path | None = None,
    opener=None,
    runner=None,
    log_dir: Path | None = None,
) -> int:
    args = parse_args(argv)
    try:
        assert_project_interpreter()
    except InterpreterError as exc:
        print(f"DETECTOR_CONTROL_CENTER_ERROR {exc}", flush=True)
        return 2
    lock = SessionLock(DEFAULT_LOCK if lock_path is None else lock_path)
    try:
        with lock:
            if args.self_check:
                return run_self_check()
            show = show_control_center if opener is None else opener
            selected = show()
            if not selected:
                print("Detector Control Center cancelled. No detector was started.", flush=True)
                return 0
            tasks = _unique_tasks(selected)
            if args.dry_run:
                print("Detector Control Center dry-run. No detector was started.", flush=True)
                for index, task in enumerate(tasks, start=1):
                    print(f"{index} {task.display_name} module={task.module} args={list(task.args)}", flush=True)
                return 0
            results = run_tasks(
                tasks,
                runner=runner,
                log_dir=DEFAULT_LOG_DIR if log_dir is None else log_dir,
            )
    except SessionBusy as exc:
        print(f"DETECTOR_CONTROL_CENTER_BUSY {exc}", flush=True)
        return 2
    return final_exit_code(results)


def run_self_check() -> int:
    window = ControlCenterWindow()
    window.root.update_idletasks()
    geometry = window.root.geometry()
    width, height, origin_x, origin_y = _parse_geometry(geometry)
    screen_w = window.root.winfo_screenwidth()
    screen_h = window.root.winfo_screenheight()
    expected_x = max(0, (screen_w - width) // 2)
    expected_y = max(0, (screen_h - height) // 2)
    print(f"SELF_CHECK geometry {geometry}", flush=True)
    print(f"SELF_CHECK size {width}x{height}", flush=True)
    print(f"SELF_CHECK screen {screen_w}x{screen_h}", flush=True)
    print(f"SELF_CHECK centered {origin_x == expected_x and origin_y == expected_y}", flush=True)
    print(f"SELF_CHECK tasks {len(window.state.tasks)}", flush=True)
    window.category.set("Swing Detectors")
    window._on_category()
    print(f"SELF_CHECK swing_visible {len(window.state.visible())}", flush=True)
    window.category.set("All Detectors")
    window._on_category()
    window.search.set("avax")
    print(f"SELF_CHECK search_visible {len(window.state.visible())}", flush=True)
    window.search.set("")
    window.refresh()
    window.state.set_selected("price_touch_btcusdt", True)
    window.state.set_selected("price_touch_ethusdt", True)
    window.refresh()
    print(f"SELF_CHECK selected {window.state.count()}", flush=True)
    print(f"SELF_CHECK run_label {window.run_button.cget('text')}", flush=True)
    window.submit()
    chosen = tuple(task.id for task in (window.result or ()))
    print(f"SELF_CHECK submitted {chosen}", flush=True)
    print("SELF_CHECK closed without starting a detector.", flush=True)
    return 0


def _parse_geometry(geometry: str) -> tuple[int, int, int, int]:
    size, _sep, origin = geometry.partition("+")
    width, height = size.split("x")
    origin_x, origin_y = origin.split("+")
    return int(width), int(height), int(origin_x), int(origin_y)


def _unique_tasks(selected) -> list[DetectorTask]:
    found: list[DetectorTask] = []
    seen: set[str] = set()
    for task in selected:
        if task.id in seen:
            continue
        seen.add(task.id)
        found.append(task)
    return found


if __name__ == "__main__":
    raise SystemExit(main())
