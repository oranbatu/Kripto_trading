# BTCUSDT 30M Special Swing 1R Take Position Backtest

Historical simulation of fixed 1:1 R trades taken from confirmed Primary swings of the existing 30-minute special detector.

The detector package is imported and called. It is not copied, reimplemented, or modified. No detector workbook is parsed and no live, broker, or exchange order is created.

Entry is the completed Swing Close close. A swing high is short with its stop at Peak 2 High. A swing low is long with its stop at Dip 2 Low. The target is exactly one risk unit away. Later 1-minute candles decide which level is reached first. A same-minute collision is a stop loss. Results are gross, before fees, slippage, and funding.

```text
.\.venv\Scripts\python.exe -m unittest strategies.take_position_v1.tests.test_take_position_backtest
.\.venv\Scripts\python.exe strategies\take_position_v1\run_take_position_backtest.py
```
