"""Completed-candle price-touch hierarchy for BTCUSDT perpetual futures.

This detector measures where completed 1h, 4h, and 1d candles repeatedly
intersect narrow price zones. It is a historical interaction study. It does
not create entries, exits, orders, or performance statistics.
"""

# Price touch

A completed candle touches a zone when its high-low interval intersects that
zone: `high >= zone_lower` and `low <= zone_upper`. A wick, a body, a close
inside the zone, and a full pass through the zone are all touches. A bounce
or rejection is not required.

# Narrow zones

Exact prices are too brittle for continuous BTC quotations. Each reported
level is a representative logarithmic-grid price with a fixed final zone of
±0.10% (`P * 0.999` through `P * 1.001`). The width is the same percentage for
every candidate. It is not widened after seeing results.

The grid is `center(k) = 1.00000000 * 1.001 ** k`, anchored at 1 USDT so a
longer dataset does not move existing centers. Atomic bins use geometric
midpoints between neighboring centers.

# One candle, one touch

For each timeframe, candle, and zone the key `(timeframe, candle_open_time, zone_id)`
is unique. One candle adds at most one total touch to that zone. Several
intrabar crossings, wick plus body, or an open and close both inside the zone
still count once. OHLC data does not reveal the intrabar path, so that path is
never simulated. A candle may touch several different zones, once each.

# Touches and episodes

Total completed-candle touches are the primary statistic. Independent touch
episodes are secondary: consecutive touching candles are one episode, and a
new episode starts only when the previous completed candle did not touch the
zone. Episode count does not replace touch count.

# Qualification

Thresholds are hard and are applied inside one timeframe only:

- 1h: at least 40 completed-candle touches, 3 episodes, and 3 distinct UTC dates
- 4h: at least 20 completed-candle touches, 3 episodes, and 3 distinct UTC dates
- 1d: at least 10 completed-candle touches, 3 episodes, and 3 distinct UTC dates

All three requirements use AND. Raw 1h, 4h, and 1d counts are never added to
pass a threshold. A failed timeframe stays failed.

# Hierarchy

Qualified levels are matched across timeframes when final zones overlap or the
relative center distance is at most 0.30%. A child joins at most one parent.
The group's center span must stay at or below 0.60%. Daily levels are parents
first, then unmatched 4h levels, then leftover 1h levels.

The representative price is the 1d price when a daily level is present,
otherwise the 4h price, otherwise the 1h price. It is not an average.

Tiers, in rank order:

- TIER_1_ALL_TIMEFRAMES: 1d + 4h + 1h
- TIER_2_DAILY_WITH_INTRADAY: 1d + 4h, or 1d + 1h
- TIER_3_DAILY_ONLY: 1d only
- TIER_4_4H_AND_1H: 4h + 1h
- TIER_5_4H_ONLY: 4h only
- TIER_6_1H_ONLY: 1h only

Tier sorts ahead of the numeric score. The score ranks groups only inside one
tier. Weights are 1d=4, 4h=2, 1h=1. The score is
`100 * (0.60*touch percentile + 0.20*episode percentile + 0.15*month percentile + 0.05*close/body engagement)`.
It does not use future reaction or profitability.

Canonical independent market visits use the finest timeframe present in the
group. They are not the sum of 1h, 4h, and 1d episodes.

# Historical rank versus first eligibility

The workbook rank is `EX_POST_HISTORICAL_STRUCTURE`. It uses the full sample.
A level's first eligible time is the close of the candle on which that
timeframe first reached its touch, episode, and distinct-date minimums
together. That clock is not the first touch, and the final rank was not known
then.

# Persistence labels

These labels do not change rank:

- RECENT_OR_EMERGING: two or fewer active months, or lifespan under 60 days
- FREQUENT_BUT_CONCENTRATED: otherwise, most-active month holds at least 45% of touches
- FREQUENT_AND_PERSISTENT: at least six months and most-active share at most 35%
- INFREQUENT_BUT_PERSISTENT: every other qualified level

# How to run

From the existing project root:

```text
cd C:\Users\oranb\Desktop\backtest_system
python -m detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy
```

This package lives inside `backtest_system`. It is not a separate project.
It does not import or modify the range detector, the swing detector, strategy
code, ingestion, or Parquet builders.

The workbook is `BTCUSDT_MTF_Most_Touched_Levels_revNN.xlsx` on the Desktop.
The scan uses `^BTCUSDT_MTF_Most_Touched_Levels_rev(\d{2,})\.xlsx$`. The first
run is `rev00`. Later runs use maximum existing revision + 1, do not fill
gaps, and do not overwrite an existing file. Range and swing workbooks are ignored.

Tests:

```text
python -m unittest detectors.price_touch_hierarchy_v1.tests.test_price_touch_hierarchy
```
