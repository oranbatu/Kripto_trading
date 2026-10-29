# BTCUSDT 30M Swing Special

This detector reads verified BTCUSDT USD-M perpetual 30-minute candles from 2026-07-01 through 2026-09-15.

A structure needs 6 to 9 strictly interior candles, a total length of 8 to 11 candles. The first eligible Swing Close is `open_row + 7` and the last is `open_row + 10`. A Swing High must open on a bullish candle and close on a bearish candle. A Swing Low must open on a bearish candle and close on a bullish candle. A doji fails either role. The Swing Close is the first completed candle, at or after the six-interior minimum, that has the required direction and whose close penetrates the Swing Open real body by an inclusive ratio from 1/3 through 5/3. A close below 1/3 or above 5/3 does not bind, and the search continues inside the same horizon. The body is the interval between the Swing Open open and close. Only the Swing Close close sets the penetration position. Wicks are not used, and the Swing Close candle's own body size is not the test.

Both the open-to-extreme and close-to-extreme boundaries must be at least 1.00%. Exact 1.00% passes. A Swing High boundary runs from the Swing Open open to the highest High in the strictly interior window. A Swing Low boundary runs from the Swing Open open to the lowest Low in that same window. Those wick extremes are used for this boundary and for the existing Peak and Dip calculations. They are not used for body penetration. There is no maximum boundary, and the obsolete 0.90% minimum is not used. Zero-interior, compact, and alternative methods are disabled. Only the shortest valid Primary structure for an event is written to Excel. Each result sheet has exactly 16 columns: swing type, the Swing Open and Swing Close open times, the Interior Body Reference time and OHLC, and Peak 1, Peak 2, Dip 1, and Dip 2. Timestamps on the result sheets are Europe/Istanbul candle open times.

Each confirmed structure also records one descriptive interior reversal pair. A Swing High selects the strictly interior bearish candle with the lowest Close that is immediately followed by a bullish interior candle. A Swing Low selects the strictly interior bullish candle with the highest Close that is immediately followed by a bearish interior candle. The workbook shows that candle's exact Open, High, Low, and Close. The reported reference price is the Close. Exact Close ties keep the last eligible pair. Swing Open, Swing Close, the interior candle immediately before Swing Close, dojis, non-consecutive candles, and wicks cannot form the pair. A missing pair leaves those cells blank and does not reject the swing.

When a pair exists, it is recalculated for every provisional Swing Close. A Swing High candidate binds only when that reference Low is greater than or equal to the candidate Close. A Swing Low candidate binds only when that reference High is less than or equal to the candidate Close. Exact equality passes. This wick comparison replaces the previous reference-Close comparison; the two are not both applied. A failure does not freeze the search: later closes are examined through nine interiors, and the minimum or maximum reference is not replaced by a second-best pair. The pair still does not change the boundary test or Primary selection.

After a Swing High is confirmed, Peak 1 is the maximum exact High from Swing Open through the Interior Body Reference candle, inclusive. Peak 2 is the maximum exact High from the bullish validation candle through Swing Close, inclusive. After a Swing Low is confirmed, Dip 1 is the minimum exact Low from Swing Open through the reference candle, and Dip 2 is the minimum exact Low from the bearish validation candle through Swing Close. The two segments are consecutive, do not overlap, and cover every candle from Swing Open through Swing Close once. Exact price ties keep the last candle. These fields are descriptive: they do not confirm the swing, choose the reference, calculate boundaries, or select Primary. A missing pair leaves Peak and Dip cells blank.

Run from the project root:

```text
.venv\Scripts\python.exe detectors\swing_open_close_30m_special_v1\run_swing_open_close_30m_special_detector.py
```

No arguments are required. That command uses the production defaults: BTCUSDT, the 30-minute timeframe, the configured analysis period, and the existing Parquet source. The runner allocates the next workbook revision and does not overwrite an existing workbook. The integrated terminal prints the detector version, counts, workbook path, runtime, and status. The JSON report is `reports\special_30m_swing_report.json` and the log is `logs\special_30m_swing.log`.

To run it in Cursor:

1. Open `detectors\swing_open_close_30m_special_v1\run_swing_open_close_30m_special_detector.py`.
2. Select `Run > Run Without Debugging`.
3. If Cursor asks once, select `Run 30m Special Swing Detector`.
4. Wait for the integrated terminal summary.
5. Open the newly generated revisioned Excel workbook at the path printed in that summary.

The same no-argument production run is what Cursor's Python `Run Python File` action executes. The launch configuration uses the project `.venv` and the project root as its working directory.
