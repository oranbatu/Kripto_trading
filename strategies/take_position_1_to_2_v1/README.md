# BTCUSDT 30M Special Swing 1:2 R Take Position Backtest

Historical simulation of fixed 1:2 risk-to-reward trades taken from confirmed Primary swings of the existing 30-minute special detector.

A normal stop is -1R and a normal take profit is +2R. The detector package and the existing 1:1 strategy are imported or left untouched. They are not modified. No live, broker, or exchange order is created.

Entry is the completed Swing Close close. A swing high is short with its stop at Peak 2 High. A swing low is long with its stop at Dip 2 Low. The target is exactly two risk units away. Later 1-minute candles decide which level is reached first. Results are gross, before fees, slippage, and funding.

```text
.\.venv\Scripts\python.exe -m unittest strategies.take_position_1_to_2_v1.tests.test_take_position_1_to_2_backtest
.\.venv\Scripts\python.exe strategies\take_position_1_to_2_v1\run_take_position_1_to_2_backtest.py
```
