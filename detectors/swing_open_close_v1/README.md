# BTCUSDT 1h Swing Open/Close V1

This detector finds Swing Open, interior extreme, and Swing Close formations on the stored Binance USD-M perpetual BTCUSDT 1h dataset.

The reference price is the Swing Open candle's open. A Swing High uses the highest interior High and is confirmed only when a later completed close is less than or equal to that reference. A Swing Low uses the lowest interior Low and is confirmed only when a later completed close is greater than or equal to that reference. The extreme candle must sit strictly between the open and the close.

One to five interior candles is a compact swing and both width measurements must be at least 3.50%. Six to fifteen interior candles is a standard swing and both width measurements must be at least 2.00%. The first close that returns to the reference is binding. A later close is not substituted when that first return fails the width rule.

Candle color, symmetry, and a fixed left/right pivot count are not required. Overlapping swings are retained. This detector does not create orders or profitability statistics.

Run it from the project root:

```text
cd C:\Users\oranb\Desktop\backtest_system
python -m detectors.swing_open_close_v1.run_swing_open_close_detector
```

The command writes the next `BTCUSDT_1H_Swing_Structure_revNN.xlsx` workbook on the Desktop and leaves earlier revisions in place.
