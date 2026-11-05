# Detector Control Center

This window launches the project's verified production detectors. It does not contain detector calculations, and it does not choose workbook revisions. Each official runner still does that work.

## Open it in Cursor

1. Open this project folder in Cursor.
2. Open Run and Debug.
3. Select **Detector Control Center** once.
4. Choose Run > Run Without Debugging, or press Ctrl+F5.

Cursor keeps using that launch configuration until another one is selected. The entry is first in the local `launch.json`. `.vscode` stays ignored by Git.

The launch uses the project interpreter directly:

`C:\Users\oranb\Desktop\backtest_system\.venv\Scripts\python.exe`

PowerShell activation is not required. `Activate.ps1` is not used.

The same command from the project root is:

```text
.venv\Scripts\python.exe -m tools.detector_control_center.app
```

## Choose detectors

Nothing is selected when the window opens. Tick one or more checkboxes. The category list and the search box only change what is visible. A detector that is hidden by a filter stays selected. Select All Visible affects the rows you can see. Clear Selection clears every selection, including hidden ones.

RUN shows the selected count. It stays disabled until at least one detector is selected. RUN closes the window and starts the selected detectors one after another, in registry order. Their terminal output stays visible. A failure is recorded and the next selected detector still runs. The final summary lists status, exit code, and runtime.

Cancel, Escape, or closing the window runs nothing and creates no workbook.

A real RUN is a production run. Each selected detector may publish its next Excel revision.

`--dry-run` prints the module commands and does not start them. It is not the normal Cursor action.

## Unavailable detectors

`range_detector_4h_v1` is shown with its checkbox disabled. The package has bytecode only and no Python source runner.

The generic price-touch command is not a checkbox. Thirteen symbol wrappers are separate entries, and none of them passes `--all-symbols`. `--all-symbols` on the generic runner still means the original nine symbols only.

The 30-minute special swing detector is also nine separate Swing Detectors entries. Each one runs only its fixed symbol through the shared detector. Selecting several of them runs those wrappers one after another in registry order. There is no all-symbol swing command.

Strategies are not listed.

## Add a future detector

Add one frozen `DetectorTask` to `registry.py` only after the official runner source exists, the module can be started with `python -m`, and import does not start production. Set `enabled=False` and a `disabled_reason` when the runner is missing. Do not point the registry at a file discovered by a folder scan.
