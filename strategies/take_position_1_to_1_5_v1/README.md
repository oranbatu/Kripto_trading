# BTCUSDT 30M Special Swing 1:1.5 R Take Position Backtest

Historical simulation of fixed 1:1.5 risk-to-reward trades from confirmed Primary swings of the existing 30-minute special detector.

A normal stop is -1R and a normal take profit is +1.5R. The gross theoretical break-even win rate is 40%. The detector and the existing 1:1 and 1:2 strategy packages are not modified. No live, broker, or exchange order is created.

```text
.\.venv\Scripts\python.exe -m unittest strategies.take_position_1_to_1_5_v1.tests.test_take_position_1_to_1_5
.\.venv\Scripts\python.exe strategies\take_position_1_to_1_5_v1\run_take_position_1_to_1_5_backtest.py
```
