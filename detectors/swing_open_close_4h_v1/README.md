# BTCUSDT 4h Swing Open/Close, 2026

This detector finds Swing Open, interior extreme, and Swing Close formations on the stored Binance USD-M perpetual BTCUSDT 4h dataset.

The analysis window is calendar year 2026 only:

```text
2026-01-01T00:00:00Z through 2026-09-15T20:00:00Z
```

That window is 1,548 completed 4h candles. Candles from 2024 and 2025 are not read for context, extremes, confirmation, path calculation, or forward evaluation.

The reference price is the Swing Open candle's open. A Swing High uses the highest interior High and is confirmed only when a later completed 4h close is less than or equal to that reference. A Swing Low uses the lowest interior Low and is confirmed only when a later completed 4h close is greater than or equal to that reference. The extreme candle must sit strictly between the open and the close.

One to six interior candles is a compact swing and both width measurements must be at least 3.50%. Exactly seven interior candles are a duration gap and are rejected. Eight to twenty interior candles is a standard swing and both width measurements must be at least 2.00%. On a 4h chart, compact open-to-close duration is 8 through 28 hours, and standard open-to-close duration is 36 through 84 hours. The detector searches at most 25 interior candles after each Swing Open. That horizon is a search limit, not a valid duration. Compact remains 1–6 interiors at 3.50%, seven interiors remain invalid for reference return, Standard remains 8–20 interiors at 2.00%, and the alternative method remains 4–10 interiors at 3.50%. A reference return at 21–25 interiors is recorded and rejected. The alternative route stops after 10 interiors even though the reference search may continue. A close with 26 or more interiors is not inspected. If the dataset ends before 25 interiors can be seen, the search is censored. The first close that returns to the reference is binding, including a return that lands on the seven-candle gap or on the 21–25 search-only range. A second method, ALTERNATIVE_3_5_WIDTH, confirms a swing when the close stays beyond the reference, the interior count is 4 through 10, and both widths are at least 3.50%. Seven interior candles can pass only on that alternative path. A close that reaches the reference is never classified as alternative. After raw confirmation, swings that share the same type, the same unrounded extreme price, and the same continuous extreme event are one family. The shortest valid formation in that family is the Primary Swing. Longer formations of that same extreme are retained as derived records and are excluded from the principal worksheets.

Run it from the project root:

```text
cd C:\Users\oranb\Desktop\backtest_system
python -m detectors.swing_open_close_4h_v1.run_swing_open_close_4h_detector
```

The command writes the next `BTCUSDT_4H_Swing_Structure_revNN.xlsx` workbook on the Desktop.
