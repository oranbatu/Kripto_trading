# Hierarchical Swing Detector V4

Permanent, read-only, asymmetric hierarchical swing detector for Binance USD-M perpetual `BTCUSDT` `1h` candles. It is an exploratory market-structure audit. It does not create orders, entries, exits, or profitability statistics.

## What it does

The detector separates four objects:

- `RAW_FORMATION`: a complete 5–10 candle candidate that passes the structural rules.
- `RAW_PIVOT`: the causally confirmed extreme candle of a representative qualified formation.
- `INTERNAL_SWING`: an alternating swing that passes the internal displacement, spacing, and close-based reversal rules.
- `MAJOR_SWING`: a stricter alternating swing that also passes percentage displacement, two-sided prominence, and the joined ATR/percentage reversal rule.

Only confirmed `MAJOR_SWING` rows appear on the main result sheets. Internal swings, raw pivots, and raw formations stay on their own audit sheets. Bootstrap seeds initialize direction and are not counted as ordinary swings.

## Market and timeframe

- Provider: Binance
- Market: USD-M `futures/um`
- Contract: USDT-margined `PERPETUAL`
- Symbol: `BTCUSDT` (`BINANCE:BTCUSDT.P`)
- Timeframe: `1h`
- Computation timezone: UTC
- Display timezone: `Europe/Istanbul` via `ZoneInfo("Europe/Istanbul")`
- Precision: exact stored decimal precision
- Window: `2026-01-01T00:00:00Z` through `2026-09-15T23:00:00Z`, both opens inclusive
- Expected rows: 6,192

The loader is called with a half-open end of `2026-09-16T00:00:00Z` so the final 23:00 UTC candle is included. No pre-2026 candle is requested and warmup is zero. A formation that would need candles before 1 January 2026 or after the dataset end is recorded as `LEFT_EDGE_CENSORED` or `RIGHT_EDGE_CENSORED` and is not padded.

## Source path

```text
C:\MarketData\derived\binance\futures\um\perpetual\1h\symbol=BTCUSDT
```

The detector reads that dataset through the project's read-only `data.loader.load_candles` API. It does not download, resample, or modify Parquet files.

## Execution

From the project root:

```text
cd C:\Users\oranb\Desktop\backtest_system
python -m detectors.hierarchical_swing_v4.run_hierarchical_swing_detector
```

Paths are resolved from the module file, not from the current working directory.

## Workbook naming

Desktop files matching `^BTCUSDT_1H_swing_rev(\d{2,})\.xlsx$` are scanned. The first run uses `BTCUSDT_1H_swing_rev00.xlsx`. Later runs use maximum existing revision + 1, with at least two digits, without filling gaps and without overwriting. The file is written under `tmp\hierarchical_swing_v4`, reopened, validated, then moved onto the Desktop.

## Formation model

A formation is `Swing Open + interior candles + Swing Close`. Total length is 5–10, so the interior count is 3–8. Every such window is evaluated for both a high and a low. The extreme must sit in the central three one-based positions:

- odd `N`: center `(N+1)/2`, positions `center-1, center, center+1`
- even `N`: lower center `N/2`, positions `lower_center-1, lower_center, lower_center+1`

Examples: `N=5 -> 2,3,4`, `N=6 -> 2,3,4`, `N=7 -> 3,4,5`, `N=8 -> 3,4,5`, `N=9 -> 4,5,6`, `N=10 -> 4,5,6`.

Left and right spans are `extreme_row - open_row` and `close_row - extreme_row`. They are formation-derived and may differ. There is no fixed symmetric pivot span.

The swing-open close is the reference. A high must print at least 1.00% above that close. A low must print at least 1.00% below it. The swing-close candle's full range must touch or cross the reference. The deterministic touch price is the swing-open close itself once that revisit is true. The qualifying return is the same distance divided by the swing-open close, so an exact 1.00% outbound move passes when the close candle revisits the reference. The extreme-basis return `(distance / extreme price)` is stored and is not a second, tighter gate.

Equal extreme prices must form one contiguous plateau. The representative candle is the last plateau candle when it lies in the central three; otherwise it is the latest plateau candle inside those positions. If none do, the formation is rejected. One continuous plateau emits one raw pivot.

Overlapping qualified formations that share type, extreme candle, extreme price, and confirmation candle are deduplicated. The representative prefers earlier confirmation, then stronger outbound, stronger reference-basis return, better centrality, shorter confirmation delay, shorter length, earlier open, and a stable id. Later formations of the same continuous plateau are duplicates of the earlier causal confirmation. Duplicate ids are retained.

A formation is confirmed only when the swing-close candle is complete. The raw pivot becomes available at that same close time. The extreme candle is never treated as confirmed earlier.

## ATR

True range is `max(high-low, abs(high-previous close), abs(low-previous close))`. The first candle uses `high-low`. Wilder ATR(14) is seeded with the SMA of the first 14 true ranges and then uses `(previous ATR * 13 + current TR) / 14`. No future bar enters the calculation.

## Internal and major hierarchy

Internal bootstrap (`INTERNAL_BOOTSTRAP_SEARCH`) selects the earliest opposite raw-pivot pair with displacement of at least 2.0 ATR at the later pivot and at least 6 bars between extremes. The earlier pivot is `INTERNAL_BOOTSTRAP_SEED`. The later pivot is the first normal internal candidate and still needs a 1.0 ATR close reversal.

After that, internal swings alternate. A high candidate requires `candidate high - previous low >= 2 ATR` and `row separation >= 6`, and it confirms when a completed close is at or below `candidate high - 1 ATR`. Lows mirror this. A more extreme eligible pivot may replace an active candidate and the reversal threshold is recalculated. A rejected pivot does not clear state. Reversal is evaluated on every completed bar, including bars with no new pivot.

Major bootstrap (`MAJOR_BOOTSTRAP_SEARCH`) uses confirmed internal swings only. The pair must satisfy displacement of at least 3.0 ATR and 1.50%, spacing of at least 12 bars, and two-sided prominence of at least 1.25 ATR. The earlier internal swing is `MAJOR_BOOTSTRAP_SEED`. The later one is the first major candidate. Major reversal distance must satisfy both `>= 1.50 ATR` and `>= 0.75%` of the candidate price. Those tests are joined by AND, as are the two displacement tests and the prominence test.

The major engine cannot reject, delay, relabel, or otherwise change an internal confirmation. Internal output is checked with the major engine disabled.

Candidate age is monitored. Age never confirms or expires a candidate. There is no timeout. A candidate that has crossed a fully qualified reversal and is still active raises `HARD_INTERNAL_STATE_MACHINE_ERROR` or `HARD_MAJOR_STATE_MACHINE_ERROR`.

## Prominence

For a high, the walk moves outward and stops before a strictly higher high. Prominence is the distance from the pivot high down to the minimum low on that walk. For a low, the walk stops before a strictly lower low and prominence is the distance up to the maximum high. Two-sided prominence is the minimum of the left and right sides. The right walk stops at the current completed evaluation bar.

## Causality and time

Confirmation timestamps are the close time of the bar that completes the relevant event. Internal confirmation is not earlier than the raw-pivot confirmation, displacement, spacing, and reversal times. Major confirmation is not earlier than the internal confirmation and the major requirement times. Turkey timestamps are converted with `ZoneInfo("Europe/Istanbul")`. One-hour close time is `open + 1 hour - 1 millisecond` in UTC.

Forward excursion, retests, and the next major swing are labeled `EX_POST_FORWARD_METRIC` or `KNOWN_AFTER_NEXT_SWING`. They are not used to confirm a swing. The last major swing is forward-censored with reason `DATASET_END`.

The descriptive significance score is not a filter. A confirmed major swing is kept regardless of its score.

## Workbook contents

Sheets, in order:

1. Swing Summary
2. Major Swing Points
3. Major Swing Highs
4. Major Swing Lows
5. Major Swing Legs
6. Internal Swings
7. Raw Pivot Candidates
8. Raw Formations
9. Formation Candles
10. State Transitions
11. Monthly Diagnostics
12. Forward Evaluation
13. Parameters
14. Diagnostics
15. README

Per-bar evaluation counters are labeled `PER_BAR_DIAGNOSTIC_ONLY` and are not candidate counts.

## Safety and immutability

Source Parquet files are fingerprinted before and after the run. Existing range-detector, data-access, ingestion, processing, report, and strategy files are fingerprinted and must stay unchanged. The detector does not import or execute the range detector and does not import a strategy package. It does not write a Python file outside `detectors\hierarchical_swing_v4`. Temporary workbook fragments and package bytecode are removed after validation.

## Isolation

This package is not registered as a strategy and is not part of `primary_range_v1`. Importing `detectors` does not run this detector. Running this module does not run range detection, ingestion, or Parquet builders.

## Tests

Permanent tests live in `detectors\hierarchical_swing_v4\tests\test_hierarchical_swing_detector.py`. From the project root:

```text
python -m unittest detectors.hierarchical_swing_v4.tests.test_hierarchical_swing_detector
```
