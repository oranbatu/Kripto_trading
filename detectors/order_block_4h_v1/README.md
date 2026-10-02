# BTCUSDT 4h Body Order Block V1

This detector finds adjacent two-candle Order Blocks on the stored Binance USD-M perpetual BTCUSDT 4h dataset.

Qualification uses candle open and close only. A Bullish Order Block is a bearish candle immediately followed by a bullish candle whose close is at least 1.00% above the bearish candle's open. A Bearish Order Block is the opposite pair, using the same 1.00% body displacement from the bullish candle's open down to the bearish candle's close. The boundary value 1.00% qualifies. Anything below it does not.

The zone is the origin candle body: the minimum and maximum of that candle's open and close. High, low, wick length, ATR, fair value gaps, and wick-based breaks of structure do not decide qualification, zone bounds, or the primary lifecycle status.

The structure is confirmed when the impulse candle closes. Lifecycle checks start on the next completed candle. A later candle body that intersects the zone is a body mitigation. A later close strictly through the far side of the body zone invalidates the block. Exact equality with the far edge is a full touch, not a strict invalidation. Wick-only contact is recorded as a diagnostic and does not change the status.

This is market-structure detection. It does not create orders, entries, exits, or profitability statistics.

Run it from the project root:

```text
cd C:\Users\oranb\Desktop\backtest_system
python -m detectors.order_block_4h_v1.run_order_block_detector
```

The command writes the next `BTCUSDT_4H_Order_Blocks_revNN.xlsx` workbook on the Desktop and leaves earlier revisions in place.
