# BTCUSDT 4h swings with exact 15m mapping

Swings are detected only on the verified BTCUSDT 4h dataset. The 15m dataset does not independently create swings and cannot change Swing Open selection, the authoritative extreme price, Swing Close selection, the detection method, the formation class, same-extreme families, Primary selection, or the official 4h confirmation time.

The 15m candles decompose each Primary 4h swing into the sixteen 15m children of every 4h candle in that swing. One complete 4h parent maps to children opening every 15 minutes from T+00:00 through T+03:45.

Official confirmation remains the close of the authoritative 4h Swing Close candle. A direct 15m Swing Close is the first completed 15m candle after the representative exact extreme whose close reaches the original 4h reference. Exact equality passes. A wick that only touches the reference does not pass. The first passing close is binding. Search stays inside the original 4h swing interval. An Alternative swing may have no such close, recorded as `DIRECT_15M_SWING_CLOSE_NOT_OBSERVED`. That absence is not fabricated and does not invalidate the 4h swing.

`15M Swing Open Close Time Turkey` is the Europe/Istanbul close of the first 15m child of the 4h Swing Open candle. `15M Swing Close Turkey` is the Turkey close of the direct structural 15m Swing Close candle. Those are different fields. The final 15m child of the official 4h close block is stored separately and is not automatically the direct Swing Close.

Authoritative parameters remain the 4h parameters, including a 25-interior search horizon, Compact 1–6 at 3.50%, Standard 8–20 at 2.00%, and Alternative 4–10 at 3.50%.

Run:

```text
C:\Users\oranb\Desktop\backtest_system\.venv\Scripts\python.exe -m detectors.swing_open_close_4h_with_15m_v1.run_swing_open_close_4h_with_15m_detector
```
