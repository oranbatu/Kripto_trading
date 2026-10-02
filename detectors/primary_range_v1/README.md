# PRIMARY range detector (1h USD-M perpetual)

Isolated market-structure detector for horizontal PRIMARY ranges on verified
Binance USD-M USDT-margined perpetual `1h` Parquet data.

This is **not** a trading strategy. It does not create signals, orders,
positions, or performance statistics. It is not auto-registered. Unrelated
future strategies must not import this package unless range detection is
explicitly requested.

## Invoke

```powershell
python C:\Users\oranb\Desktop\backtest_system\detectors\primary_range_v1\run_primary_range_detector.py --symbol BTCUSDT
```

Another symbol, same script, no code edits:

```powershell
python C:\Users\oranb\Desktop\backtest_system\detectors\primary_range_v1\run_primary_range_detector.py --symbol ETHUSDT
```

Optional arguments: `--data-root`, `--output-dir`, `--timeframe` (must be `1h`), `--dry-run`.

Pass the **exact** perpetual symbol (`BTCUSDT`, not `BTC` or `ETH`).

## Versions

| Concept | Value | Meaning |
| --- | --- | --- |
| Detector version | `UM_PERP_1H_PRIMARY_RANGE_V1` | Algorithm identity |
| Configuration version | `PRIMARY_RANGE_CONFIG_REV05` | Hard filters and PRIMARY-only reporting |
| Output revision | `revNN` allocated per symbol | Desktop workbook counter, not an algorithm change |

Existing desktop files such as `BTCUSDT_1H_Range_Detection_rev05.xlsx` are never overwritten. The next BTCUSDT file is `rev06` if `rev05` is the highest matching revision.

## Source

Reads only:

`C:\MarketData\derived\binance\futures\um\perpetual\1h\symbol=<SYMBOL>`

The detector discovers the actual first/last timestamps and row count from the partitions. It does not resample, download, or modify Parquet.

## Hard filters (CONFIG REV05)

- 500 completed `1h` bars
- Two upper anchors and two lower anchors, independent
- Any same-side pair with row-index separation ≥ 30 (need not be adjacent)
- Additional eligible touches are descriptive only and may be zero
- Inside Close Ratio ≥ 0.97 (unrounded)
- Range Width Percent ≤ 13.0 (unrounded, structural bounds)
- Normalized width ≥ 2.0 ATR
- EQ crossings ≥ 2, efficiency ≤ 0.30, normalized slope ≤ 0.35
- Reported structures are PRIMARY only (`Parent Range ID = none`)
- Quality score ≥ 60

Hierarchy classification is `POST_DETECTION`. Causal `confirmed_at` is never rewritten. The PRIMARY label is not a live trading signal.

## Tests

```powershell
python -m unittest C:\Users\oranb\Desktop\backtest_system\detectors\primary_range_v1\tests\test_primary_range_detector.py
```

## Isolation

All detector code lives under `backtest_system\detectors\primary_range_v1\`.
It does not modify the data-access layer, downloader, Parquet builders, or
future strategy modules.
