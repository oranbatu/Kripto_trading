# BTCUSDT 30M Swing Special

This detector reads verified BTCUSDT USD-M perpetual 30-minute candles from 2026-07-01 through 2026-09-15.

A structure needs 6 to 9 strictly interior candles, a total length of 8 to 11 candles. The first eligible Swing Close is `open_row + 7` and the last is `open_row + 10`. A Swing High must open on a bullish candle and close on a bearish candle. A Swing Low must open on a bearish candle and close on a bullish candle. A doji fails either role. The Swing Close is the first completed candle, at or after the six-interior minimum, that has the required direction and whose close enters the Swing Open real body by at least one third. A wrong-direction candle that crosses the price threshold does not bind, and the search continues inside the same horizon. The body is the interval between the Swing Open open and close. Wicks are not used, and the Swing Close candle's own body size is not the test.

Both the open-to-extreme and close-to-extreme boundaries must be at least 1.00%. Exact 1.00% passes. There is no maximum boundary, and the obsolete 0.90% minimum is not used. Zero-interior, compact, and alternative methods are disabled. Only the shortest valid Primary structure for an event is written to Excel. Each result sheet has exactly 16 columns: swing type, the Swing Open and Swing Close open times, the Interior Body Reference time and OHLC, and Peak 1, Peak 2, Dip 1, and Dip 2. Timestamps on the result sheets are Europe/Istanbul candle open times.

Each confirmed structure also records one descriptive interior reversal pair. A Swing High selects the strictly interior bearish candle with the lowest Close that is immediately followed by a bullish interior candle. A Swing Low selects the strictly interior bullish candle with the highest Close that is immediately followed by a bearish interior candle. The workbook shows that candle's exact Open, High, Low, and Close. The reported reference price is the Close. Exact Close ties keep the last eligible pair. Swing Open, Swing Close, the interior candle immediately before Swing Close, dojis, non-consecutive candles, and wicks cannot form the pair. A missing pair leaves those cells blank and does not reject the swing.

When a pair exists, it is recalculated for every provisional Swing Close. A Swing High candidate binds only when that reference Close is greater than or equal to the candidate Close. A Swing Low candidate binds only when that reference Close is less than or equal to the candidate Close. Exact equality passes. A failure does not freeze the search: later closes are examined through nine interiors, and the minimum or maximum reference is not replaced by a second-best pair. The pair still does not change the boundary test or Primary selection.

After a Swing High is confirmed, Peak 1 is the maximum exact High from Swing Open through the Interior Body Reference candle, inclusive. Peak 2 is the maximum exact High from the bullish validation candle through Swing Close, inclusive. After a Swing Low is confirmed, Dip 1 is the minimum exact Low from Swing Open through the reference candle, and Dip 2 is the minimum exact Low from the bearish validation candle through Swing Close. The two segments are consecutive, do not overlap, and cover every candle from Swing Open through Swing Close once. Exact price ties keep the last candle. These fields are descriptive: they do not confirm the swing, choose the reference, calculate boundaries, or select Primary. A missing pair leaves Peak and Dip cells blank.

Run from the project root:

```text
.venv\Scripts\python.exe detectors\swing_open_close_30m_special_v1\run_swing_open_close_30m_special_detector.py
```
