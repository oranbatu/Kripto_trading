# BTCUSDT 4h swings with exact 30m mapping

This detector finds Swing Open, interior extreme, and Swing Close formations on the stored BTCUSDT 4h candles. The 4h rules are the same rules as `swing_open_close_4h_v1`.

The 30m candles do not create swings. They decompose each Primary 4h swing into the eight 30m children of every 4h candle in that swing.

Official confirmation remains the close of the authoritative 4h Swing Close candle. A direct 30m Swing Close is the first completed 30m candle after the representative exact extreme whose close reaches the original 4h reference. Exact equality passes. A wick that only touches the reference does not pass. The first passing close is binding. An Alternative swing may have no such close, recorded as `DIRECT_30M_SWING_CLOSE_NOT_OBSERVED`.

`30M Swing Open Close Time Turkey` is the Europe/Istanbul close of the first 30m child of the 4h Swing Open candle. `30M Swing Close Turkey` is the Turkey close of the direct structural 30m Swing Close candle. Those are different fields.

Run from the project root:

```text
C:\Users\oranb\Desktop\backtest_system\.venv\Scripts\python.exe -m detectors.swing_open_close_4h_with_30m_v1.run_swing_open_close_4h_with_30m_detector
```

This is not investment advice.
