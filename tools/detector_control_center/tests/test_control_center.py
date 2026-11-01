"""Tests for the detector control center. These tests do not publish workbooks."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from tools.detector_control_center.app import main
from tools.detector_control_center.executor import (
    EXPECTED_PYTHON,
    InterpreterError,
    SessionBusy,
    SessionLock,
    assert_project_interpreter,
    command_for,
    final_exit_code,
    format_summary,
    run_tasks,
)
from tools.detector_control_center.registry import PROJECT_ROOT, TASKS, enabled_tasks, task_by_id
from tools.detector_control_center.selection import SelectionState
from tools.detector_control_center.ui import MIN_HEIGHT, MIN_WIDTH, WINDOW_HEIGHT, WINDOW_WIDTH, ControlCenterWindow


class RegistryTests(unittest.TestCase):
    def test_registry_is_unique_and_stable(self) -> None:
        ids = [task.id for task in TASKS]
        self.assertEqual(ids, list(dict.fromkeys(ids)))
        self.assertEqual(ids, [task.id for task in TASKS])

    def test_price_touch_entries_are_fixed_symbol_modules(self) -> None:
        symbols = ("BTCUSDT", "ETHUSDT", "AVAXUSDT", "AAVEUSDT", "NEARUSDT", "LINKUSDT", "LTCUSDT", "OPUSDT", "DOTUSDT")
        for symbol in symbols:
            task = task_by_id(f"price_touch_{symbol.lower()}")
            self.assertEqual(task.symbol, symbol)
            self.assertTrue(task.enabled)
            self.assertEqual(
                task.module,
                f"detectors.price_touch_hierarchy_v1.runners.run_{symbol.lower()}_price_touch_hierarchy",
            )
            self.assertNotIn("--all-symbols", task.args)
            self.assertNotIn("--symbol", task.args)
            self.assertTrue(task.source_file().is_file())
            text = task.source_file().read_text(encoding="utf-8")
            self.assertIn('if __name__ == "__main__":', text)
            self.assertIn(f'SYMBOL = "{symbol}"', text)

    def test_special_swing_30m_entries_are_fixed_symbol_modules(self) -> None:
        symbols = ("BTCUSDT", "ETHUSDT", "AVAXUSDT", "AAVEUSDT", "NEARUSDT", "LINKUSDT", "LTCUSDT", "OPUSDT", "DOTUSDT")
        tasks = [task for task in TASKS if task.timeframes == ("30m",) and task.category == "Swing Detectors"]
        self.assertEqual([task.symbol for task in tasks], list(symbols))
        self.assertEqual(len({task.id for task in tasks}), 9)
        self.assertNotIn("swing_30m_special", {task.id for task in TASKS})
        for task in tasks:
            self.assertEqual(task.args, ())
            self.assertTrue(task.production and task.enabled)
            self.assertEqual(
                task.module,
                f"detectors.swing_open_close_30m_special_v1.runners.run_{task.symbol.lower()}_special_swing",
            )
            self.assertNotIn("--all-symbols", task.args)
            self.assertTrue(task.source_file().is_file())
        state = SelectionState(tasks)
        state.set_selected("swing_30m_dotusdt", True)
        state.set_selected("swing_30m_btcusdt", True)
        self.assertEqual(state.ordered_ids(), ("swing_30m_btcusdt", "swing_30m_dotusdt"))

    def test_bytecode_only_range_detector_is_disabled(self) -> None:
        task = task_by_id("range_detector_4h_v1")
        self.assertFalse(task.enabled)
        self.assertIn("Bytecode only", task.disabled_reason)
        folder = PROJECT_ROOT / "detectors" / "range_detector_4h_v1"
        sources = [path for path in folder.rglob("*.py") if "__pycache__" not in path.parts]
        self.assertEqual(sources, [])

    def test_enabled_runners_exist_and_other_modules_do_not_use_shell_strings(self) -> None:
        for task in enabled_tasks():
            self.assertTrue(task.source_file().is_file(), task.id)
            text = task.source_file().read_text(encoding="utf-8")
            self.assertIn('if __name__ == "__main__":', text)
        primary = task_by_id("primary_range_btcusdt")
        self.assertEqual(primary.args, ("--symbol", "BTCUSDT"))


class SelectionTests(unittest.TestCase):
    def test_filter_search_and_hidden_selection(self) -> None:
        state = SelectionState()
        state.set_selected("price_touch_btcusdt", True)
        state.category = "Liquidity Detectors"
        self.assertEqual([task.id for task in state.visible()], ["open_liquidity_v1"])
        self.assertEqual(state.ordered_ids(), ("price_touch_btcusdt",))
        state.query = "avax"
        state.category = "All Detectors"
        self.assertEqual([task.id for task in state.visible()], ["price_touch_avaxusdt", "swing_30m_avaxusdt"])
        state.select_visible()
        state.query = ""
        state.clear()
        self.assertEqual(state.ordered_ids(), ())

    def test_select_visible_skips_disabled_and_preserves_registry_order(self) -> None:
        state = SelectionState()
        state.set_selected("price_touch_dotusdt", True)
        state.set_selected("price_touch_btcusdt", True)
        state.set_selected("range_detector_4h_v1", True)
        self.assertEqual(state.ordered_ids(), ("price_touch_btcusdt", "price_touch_dotusdt"))
        state.category = "Range Detectors"
        state.select_visible()
        self.assertIn("primary_range_btcusdt", state.selected)
        self.assertNotIn("range_detector_4h_v1", state.selected)

    def test_duplicate_ids_are_not_repeated(self) -> None:
        state = SelectionState()
        state.set_selected("swing_30m_btcusdt", True)
        state.set_selected("swing_30m_btcusdt", True)
        self.assertEqual(state.ordered_ids(), ("swing_30m_btcusdt",))


class ExecutionTests(unittest.TestCase):
    def test_command_uses_module_execution_and_project_python(self) -> None:
        task = task_by_id("price_touch_avaxusdt")
        command = command_for(task)
        self.assertEqual(Path(command[0]).resolve(), EXPECTED_PYTHON.resolve())
        self.assertEqual(command[1:3], ["-m", task.module])
        self.assertNotIn("--all-symbols", command)

    def test_invalid_interpreter_is_rejected(self) -> None:
        with self.assertRaises(InterpreterError):
            assert_project_interpreter(Path(r"C:\Windows\py.exe"))

    def test_sequential_success_failure_and_summary(self) -> None:
        codes = iter((1, 0))

        def fake(command, cwd, shell):
            self.assertFalse(shell)
            self.assertEqual(Path(cwd), PROJECT_ROOT)
            self.assertEqual(command[1], "-m")

            class Completed:
                returncode = next(codes)

            return Completed()

        first = task_by_id("price_touch_ethusdt")
        second = task_by_id("price_touch_btcusdt")
        with tempfile.TemporaryDirectory() as folder:
            results = run_tasks([first, second, first], runner=fake, log_dir=Path(folder))
            self.assertEqual(len(list(Path(folder).glob("*.json"))), 1)
        self.assertEqual([item.task.id for item in results], ["price_touch_ethusdt", "price_touch_btcusdt"])
        self.assertEqual([item.status for item in results], ["FAILED", "SUCCESS"])
        self.assertEqual(final_exit_code(results), 1)
        text = format_summary(results, 1.5)
        self.assertIn("failed 1", text)
        self.assertIn("succeeded 1", text)
        self.assertIn("overall FAILED", text)

    def test_missing_module_does_not_stop_the_next_task(self) -> None:
        missing = replace(task_by_id("price_touch_btcusdt"), module="detectors.missing_runner")
        called = []

        def fake(command, cwd, shell):
            called.append(command)
            class Completed:
                returncode = 0
            return Completed()

        results = run_tasks([missing, task_by_id("price_touch_ethusdt")], runner=fake, log_dir=None)
        self.assertEqual(results[0].status, "FAILED")
        self.assertEqual(results[1].status, "SUCCESS")
        self.assertEqual(len(called), 1)

    def test_disabled_task_is_skipped(self) -> None:
        results = run_tasks([task_by_id("range_detector_4h_v1")], runner=lambda *args, **kwargs: None, log_dir=None)
        self.assertEqual(results[0].status, "SKIPPED")
        self.assertEqual(final_exit_code(results), 0)

    def test_lock_rejects_live_session_recovers_stale_lock_and_cleans_exceptions(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "session.lock"
            path.write_text(json.dumps({"pid": 4321}), encoding="utf-8")
            with self.assertRaises(SessionBusy):
                with SessionLock(path, alive=lambda pid: pid == 4321):
                    pass
            path.write_text(json.dumps({"pid": 4321}), encoding="utf-8")
            with SessionLock(path, alive=lambda _pid: False):
                self.assertTrue(path.exists())
            self.assertFalse(path.exists())
            path.write_text("{", encoding="utf-8")
            try:
                with SessionLock(path, alive=lambda _pid: False):
                    raise RuntimeError("boom")
            except RuntimeError:
                pass
            self.assertFalse(path.exists())

    def test_cancel_and_import_do_not_run_detectors(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            lock = Path(folder) / "session.lock"
            code = main([], lock_path=lock, opener=lambda: None, runner=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("ran")))
            self.assertEqual(code, 0)
            self.assertFalse(lock.exists())
        self.assertFalse(any(name.startswith("detectors.") for name in sys.modules))


class WindowTests(unittest.TestCase):
    def test_window_size_selection_and_disabled_reason(self) -> None:
        window = ControlCenterWindow()
        try:
            window.root.update_idletasks()
            self.assertTrue(window.root.geometry().startswith(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}"))
            self.assertGreaterEqual(WINDOW_WIDTH, 700)
            self.assertLessEqual(WINDOW_WIDTH, 900)
            self.assertGreaterEqual(WINDOW_HEIGHT, 520)
            self.assertLessEqual(WINDOW_HEIGHT, 720)
            self.assertEqual(window.root.minsize(), (MIN_WIDTH, MIN_HEIGHT))
            self.assertIn("disabled", window.run_button.state())
            window.state.category = "Range Detectors"
            window.refresh()
            labels = [child.cget("text") for child in _labels(window.list_frame)]
            self.assertTrue(any("Bytecode only" in text for text in labels))
            window.state.category = "Price Touch Hierarchy"
            window.state.query = ""
            window.refresh()
            window.select_visible()
            self.assertEqual(window.state.count(), 9)
            self.assertNotIn("disabled", window.run_button.state())
            self.assertIn("RUN 9", window.run_button.cget("text"))
            window.clear()
            self.assertEqual(window.state.count(), 0)
            window.cancel()
            self.assertIsNone(window.result)
        finally:
            if not window.closed:
                window.cancel()


class LaunchTests(unittest.TestCase):
    def test_launch_configuration(self) -> None:
        payload = json.loads((PROJECT_ROOT / ".vscode" / "launch.json").read_text(encoding="utf-8"))
        first = payload["configurations"][0]
        self.assertEqual(first["name"], "Detector Control Center")
        self.assertEqual(first["type"], "debugpy")
        self.assertEqual(first["module"], "tools.detector_control_center.app")
        self.assertEqual(first["console"], "integratedTerminal")
        self.assertIn(".venv", first["python"].replace("\\", "/"))
        names = [item["name"] for item in payload["configurations"]]
        self.assertIn("Run 30m Special Swing Detector", names)
        self.assertIn("Run Price Touch Hierarchy Detector", names)
        self.assertEqual(len(names), len(set(names)))


def _labels(widget):
    found = []
    for child in widget.winfo_children():
        if child.winfo_class() == "Label":
            found.append(child)
        found.extend(_labels(child))
    return found


if __name__ == "__main__":
    unittest.main()
