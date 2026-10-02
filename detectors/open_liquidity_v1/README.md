# BTCUSDT multi-timeframe open liquidity

This detector finds completed-candle highs and lows that no later completed candle on the same timeframe has reached. It is a market-structure description. It does not create entries, exits, orders, stops, targets, position sizes, or performance statistics.

## Open High liquidity

A completed candle's high stays open only when every strictly later completed candle has a high strictly below that price.

```text
OPEN_HIGH = no later completed candle has later_high >= origin_high
```

## Open Low liquidity

A completed candle's low stays open only when every strictly later completed candle has a low strictly above that price.

```text
OPEN_LOW = no later completed candle has later_low <= origin_low
```

## Why equality closes liquidity

An exact retest means the price was reached. `later_high == origin_high` closes the older high. `later_low == origin_low` closes the older low. The later candle still creates its own candidate, and that new candidate is judged only by candles after it. In a chain of equal highs or equal lows, only the latest candidate can remain open.

## Why a wick closes liquidity

The primary rule uses the completed high-low range. A wick that touches the level is enough. The candle does not need to close beyond the level, and the body does not need to cross it.

## Touch status and close status

Touch status asks whether a later high or low reached the price. Close status asks whether a later close reached the price. Close status is diagnostic. A wick that tags an old high and closes back below it is touch-mitigated and not close-mitigated, so it is not open liquidity.

## The origin candle

A candle's high and low become available at that candle's close. The origin candle is not a later candle, so it cannot mitigate itself. Only a candle with a strictly later open time is eligible.

## Gaps

If a later candle opens at or beyond the level, the level has been traded through. That first event is `GAP_OPEN_AT_OR_BEYOND` and the candidate is mitigated.

## The final candle

The last completed candle has no later completed candle. Its high and low are `TERMINAL_NO_LATER_CANDLE`. They stay in the audit and are excluded from the main open-liquidity sheets, because absence of a later candle is not proof that the price will remain untouched.

## Dataset-end censoring

An open point means the price was not reached through the final stored candle. `Right Censored = TRUE` and `Censoring Reason = DATASET_END`. Future observation bars count the completed candles after the origin. When new candles are appended, a point that is open today can become mitigated. Open status is a statement about the stored sample, not a permanent property of the price.

Observation warnings use the share of that timeframe's candles that sit after the origin: under 1% is very low, under 5% is low, under 20% is moderate, and 20% or more is high. A recent open point is kept.

## Cross-timeframe hierarchy

Each timeframe is detected on its own candles. Another timeframe never decides whether a candidate is open. After that, nearby open points of the same side are grouped. A high is never grouped with a low. The match tolerance is 0.10% of the midpoint price, and a group may not span more than 0.20%. Daily opens are parents first, then unmatched 4h opens, then remaining 1h opens. The representative price is the highest timeframe present, not an average.

## Run it again

From the project root:

```text
cd C:\Users\oranb\Desktop\backtest_system
python -m detectors.open_liquidity_v1.run_open_liquidity_detector
```

The script scans the Desktop for `BTCUSDT_MTF_Open_Liquidity_revNN.xlsx`. The first run uses `rev00`. Later runs use one more than the highest existing revision. Gaps are left empty. An existing workbook is never overwritten. The file is written under `tmp\open_liquidity_v1`, reopened, validated, and then moved onto the Desktop. Temporary files are removed after that move.

Detector version: `BTCUSDT_MTF_OPEN_LIQUIDITY_V1`.
