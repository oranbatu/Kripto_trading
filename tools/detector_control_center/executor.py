"""Sequential production execution and the single-session lock."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from tools.detector_control_center.registry import PROJECT_ROOT, DetectorTask

EXPECTED_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
DEFAULT_LOCK = PROJECT_ROOT / "tmp" / "detector_control_center" / "session.lock"
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs" / "detector_control_center"


class InterpreterError(RuntimeError):
    """Raised when this process is not the project virtual environment."""


class SessionBusy(RuntimeError):
    """Raised when another control-center session already holds the lock."""


@dataclass(frozen=True, slots=True)
class TaskResult:
    task: DetectorTask
    status: str
    exit_code: int | None
    runtime_seconds: float
    started_at: str
    finished_at: str
    detail: str = ""


def assert_project_interpreter(executable: Path | None = None) -> Path:
    actual = Path(executable or sys.executable).resolve()
    expected = EXPECTED_PYTHON.resolve()
    if actual != expected:
        raise InterpreterError(
            f"Detector Control Center must run with {expected}. This process is {actual}."
        )
    return actual


def command_for(task: DetectorTask, executable: Path | None = None) -> list[str]:
    python = str(assert_project_interpreter(executable))
    return [python, "-m", task.module, *task.args]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name != "nt":
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True
    import ctypes

    access = 0x1000
    handle = ctypes.windll.kernel32.OpenProcess(access, False, int(pid))
    if not handle:
        return False
    ctypes.windll.kernel32.CloseHandle(handle)
    return True


class SessionLock:
    def __init__(self, path: Path, alive: Callable[[int], bool] | None = None) -> None:
        self.path = path
        self.alive = pid_is_alive if alive is None else alive
        self._held = False

    def __enter__(self) -> "SessionLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._acquire()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.release()

    def _acquire(self) -> None:
        if self.path.exists():
            self._recover_if_stale()
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            descriptor = os.open(self.path, flags)
        except FileExistsError as exc:
            raise SessionBusy(
                f"Another Detector Control Center session is active. Lock: {self.path}"
            ) from exc
        payload = {
            "pid": os.getpid(),
            "started_at": _utc_now(),
            "executable": sys.executable,
        }
        os.write(descriptor, json.dumps(payload, indent=2).encode("utf-8"))
        os.close(descriptor)
        self._held = True

    def _recover_if_stale(self) -> None:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            pid = int(payload.get("pid", 0))
        except (OSError, ValueError, TypeError):
            pid = 0
        if pid and self.alive(pid) and pid != os.getpid():
            raise SessionBusy(
                f"Another Detector Control Center session is active (pid {pid})."
            )
        self.path.unlink(missing_ok=True)

    def release(self) -> None:
        if self._held:
            self.path.unlink(missing_ok=True)
            self._held = False


def format_summary(results: list[TaskResult], total_runtime: float) -> str:
    lines = [
        "",
        "DETECTOR CONTROL CENTER SUMMARY",
        f"{'No':<4} {'Detector':<42} {'Symbol':<10} {'Status':<8} {'Exit':<6} {'Seconds':>8}",
    ]
    succeeded = failed = skipped = 0
    for index, result in enumerate(results, start=1):
        if result.status == "SUCCESS":
            succeeded += 1
        elif result.status == "SKIPPED":
            skipped += 1
        else:
            failed += 1
        exit_text = "" if result.exit_code is None else str(result.exit_code)
        lines.append(
            f"{index:<4} {result.task.display_name[:42]:<42} {result.task.symbol:<10} "
            f"{result.status:<8} {exit_text:<6} {result.runtime_seconds:>8.3f}"
        )
    lines.extend(
        [
            f"selected {len(results)}",
            f"succeeded {succeeded}",
            f"failed {failed}",
            f"skipped {skipped}",
            f"total_runtime_seconds {total_runtime:.3f}",
            f"overall {'SUCCESS' if failed == 0 else 'FAILED'}",
        ]
    )
    return "\n".join(lines)


def final_exit_code(results: list[TaskResult]) -> int:
    return 0 if all(result.status in {"SUCCESS", "SKIPPED"} for result in results) else 1


def run_tasks(
    tasks: list[DetectorTask] | tuple[DetectorTask, ...],
    *,
    runner: Callable[..., subprocess.CompletedProcess] | None = None,
    log_dir: Path | None = None,
    dry_run: bool = False,
) -> list[TaskResult]:
    """Run tasks in registry order. Detector output is not captured."""
    invoke = subprocess.run if runner is None else runner
    seen: set[str] = set()
    ordered: list[DetectorTask] = []
    for task in tasks:
        if task.id in seen:
            continue
        seen.add(task.id)
        ordered.append(task)
    results: list[TaskResult] = []
    started_all = datetime.now(timezone.utc)
    for index, task in enumerate(ordered, start=1):
        started = datetime.now(timezone.utc)
        started_text = started.isoformat(timespec="seconds").replace("+00:00", "Z")
        print("", flush=True)
        print(f"CONTROL {index}/{len(ordered)} {task.display_name}", flush=True)
        print(f"symbol {task.symbol}", flush=True)
        print(f"module {task.module}", flush=True)
        print(f"args {list(task.args)}", flush=True)
        print(f"started {started_text}", flush=True)
        if not task.enabled:
            finished = datetime.now(timezone.utc)
            results.append(
                TaskResult(task, "SKIPPED", None, 0.0, started_text, _stamp(finished), task.disabled_reason)
            )
            print(f"status SKIPPED {task.disabled_reason}", flush=True)
            continue
        source = task.source_file()
        if source is None or not source.is_file():
            finished = datetime.now(timezone.utc)
            detail = f"module source is missing: {source}"
            results.append(TaskResult(task, "FAILED", None, 0.0, started_text, _stamp(finished), detail))
            print(f"status FAILED {detail}", flush=True)
            continue
        command = command_for(task)
        if dry_run:
            print("dry_run " + " ".join(command), flush=True)
            finished = datetime.now(timezone.utc)
            results.append(TaskResult(task, "SUCCESS", 0, 0.0, started_text, _stamp(finished), "dry-run"))
            continue
        completed = invoke(command, cwd=PROJECT_ROOT, shell=False)
        finished = datetime.now(timezone.utc)
        runtime = (finished - started).total_seconds()
        status = "SUCCESS" if completed.returncode == 0 else "FAILED"
        results.append(TaskResult(task, status, completed.returncode, runtime, started_text, _stamp(finished)))
        print(f"finished {_stamp(finished)}", flush=True)
        print(f"runtime_seconds {runtime:.3f}", flush=True)
        print(f"exit_code {completed.returncode}", flush=True)
        print(f"status {status}", flush=True)
    total_runtime = (datetime.now(timezone.utc) - started_all).total_seconds()
    summary = format_summary(results, total_runtime)
    print(summary, flush=True)
    if log_dir is not None and not dry_run:
        _write_log(log_dir, results, total_runtime)
    return results


def _stamp(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds").replace("+00:00", "Z")


def _write_log(folder: Path, results: list[TaskResult], total_runtime: float) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    payload = {
        "controller": "detector_control_center",
        "finished_at": _utc_now(),
        "total_runtime_seconds": round(total_runtime, 3),
        "tasks": [
            {
                "id": result.task.id,
                "module": result.task.module,
                "args": list(result.task.args),
                "status": result.status,
                "exit_code": result.exit_code,
                "runtime_seconds": round(result.runtime_seconds, 3),
                "started_at": result.started_at,
                "finished_at": result.finished_at,
            }
            for result in results
        ],
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (folder / f"run_{stamp}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
