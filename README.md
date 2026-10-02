# backtest_system

Unified Python project for Binance USD-M USDT-margined perpetual futures candles.

This project consolidates previously separate download, Parquet-processing, data-access, and detector scripts into one root. Scripts remain independently executable. There is no `run_all` command, scheduler, or workflow orchestrator.

Source code lives here. Market data remains at `C:\MarketData` and is not part of this project.

## Directory tree

```text
C:\Users\oranb\Desktop\backtest_system\
├── README.md
├── requirements.txt
├── pyproject.toml
├── config.py
├── run_validation.py
├── ingestion\
│   └── binance\
│       └── download_binance_um_perp_1m.py
├── processing\
│   ├── canonical\
│   │   └── build_canonical_parquet.py
│   └── timeframes\
│       ├── build_5m_parquet.py
│       ├── build_15m_parquet.py
│       ├── build_30m_parquet.py
│       ├── build_1h_parquet.py
│       ├── build_4h_parquet.py
│       └── build_1d_parquet.py
├── data\
├── detectors\
│   └── primary_range_v1\
├── tests\
│   ├── data\
│   ├── ingestion\
│   └── processing\
├── reports\
└── logs\
```

## Responsibilities

| Branch | Responsibility |
| --- | --- |
| `ingestion/` | Online data acquisition only |
| `processing/canonical/` | Raw archive to canonical 1m Parquet |
| `processing/timeframes/` | Derived timeframe construction |
| `data/` | Read-only catalog, loading, alignment, time, models, and validation |
| `detectors/` | Independent market-structure detectors |
| `tests/` | Project-level tests grouped by responsibility |

## Market data location

Default data root: `C:\MarketData`.

Do not move market data into this source tree. Download, processing, and detector scripts keep their existing `C:\MarketData` output and input paths. The data-access layer still honors `MARKET_DATA_ROOT` when that environment variable is set. The downloader still honors `DATA_ROOT` when that environment variable is set.

| Timeframe | Root | Partitioning | Files | Rows/symbol |
| --- | --- | --- | ---: | ---: |
| `1m` | `processed/binance/futures/um/perpetual/1m` | symbol/year/month | 297 | 1,424,160 |
| `5m` | `derived/binance/futures/um/perpetual/5m` | symbol/year/month | 297 | 284,832 |
| `15m` | `derived/.../15m` | symbol/year/month | 297 | 94,944 |
| `30m` | `derived/.../30m` | symbol/year/month | 297 | 47,472 |
| `1h` | `derived/.../1h` | symbol/year/month | 297 | 23,736 |
| `4h` | `derived/.../4h` | symbol/year/month | 297 | 5,934 |
| `1d` | `derived/.../1d` | symbol/year | 27 | 989 |

Inclusive stored UTC range: first open `2024-01-01T00:00:00Z`. Last opens depend on timeframe (for example `1m` last open `2026-09-15T23:59:00Z`, `5m` last open `2026-09-15T23:55:00Z`).

Source Parquet files and existing validation reports under `C:\MarketData` are immutable from the data-access layer. The loader never writes into those trees, never resamples, and never repairs gaps.

## Interpreter and dependencies

Use the project interpreter:

```text
C:\Users\oranb\Desktop\backtest_system\.venv\Scripts\python.exe
```

Pinned in `requirements.txt`: CPython 3.11, `pyarrow==21.0.0`, `openpyxl==3.1.5`, `tzdata==2026.4`.

The downloader uses the standard library only.

Merged from:

- `backtest_system\requirements.txt`
- `candle_processing\requirements.txt`
- `openpyxl==3.1.5` already present in this project's validated venv (range detector)
- `candle_download` had no dependency file

## How to run existing scripts

Run each script independently from the project root. These commands are documented only; they are not a combined workflow.

```powershell
cd C:\Users\oranb\Desktop\backtest_system
```

Downloader:

```powershell
python -m ingestion.binance.download_binance_um_perp_1m
```

Canonical 1m Parquet:

```powershell
python -m processing.canonical.build_canonical_parquet
```

Derived timeframes:

```powershell
python -m processing.timeframes.build_5m_parquet
python -m processing.timeframes.build_15m_parquet
python -m processing.timeframes.build_30m_parquet
python -m processing.timeframes.build_1h_parquet
python -m processing.timeframes.build_4h_parquet
python -m processing.timeframes.build_1d_parquet
```

PRIMARY range detector:

```powershell
python -m detectors.primary_range_v1.run_primary_range_detector --symbol BTCUSDT
```

The detector file path remains valid:

```powershell
python C:\Users\oranb\Desktop\backtest_system\detectors\primary_range_v1\run_primary_range_detector.py --symbol BTCUSDT
```

Optional detector arguments: `--data-root`, `--output-dir`, `--timeframe` (must be `1h`), `--dry-run`.

## How to run existing tests

Data-access tests:

```powershell
C:\Users\oranb\Desktop\backtest_system\.venv\Scripts\python.exe -m unittest discover -s tests -t . -v
```

PRIMARY range detector tests:

```powershell
C:\Users\oranb\Desktop\backtest_system\.venv\Scripts\python.exe -m unittest detectors.primary_range_v1.tests.test_primary_range_detector -v
```

The existing data-access validation runner still exists. It runs tests twice, smoke tests, and writes `reports\data_access_validation_report.json` plus `logs\data_access.log`:

```powershell
C:\Users\oranb\Desktop\backtest_system\.venv\Scripts\python.exe run_validation.py
```

## Independent execution

Each downloader, processor, detector, and test module is invoked on its own. This project does not provide a command that runs the full pipeline.

## Data-access layer

Read-only catalog, loader, validator, warm-up, and **no-look-ahead** multi-timeframe alignment for the verified Parquet datasets under `C:\MarketData`.

This layer does **not** contain strategies, indicators, signals, orders, fills, positions, portfolio accounting, performance metrics, or a backtest engine. It only loads candles and tells you which higher-timeframe row is visible at a given decision time.

### Supported universe

Symbols: `BTCUSDT`, `ETHUSDT`, `AVAXUSDT`, `AAVEUSDT`, `NEARUSDT`, `LINKUSDT`, `LTCUSDT`, `OPUSDT`, `DOTUSDT`.

Timeframes: `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`.

Unknown symbols or timeframe names are rejected. Names are not case-folded, aliased, or guessed (`1H`, `5min`, and Spot pairs all fail).

### Half-open UTC ranges

All queries use **`[start_utc, end_utc)`**.

- `start_utc` is inclusive.
- `end_utc` is exclusive.
- Membership is by **`open_time`**.
- A candle is in the trading range iff `open_time >= start_utc` and `open_time < end_utc`.

Naive datetimes are rejected. Aware non-UTC inputs are converted to UTC before filtering. Windows local time is never used.

**Stored datasets always remain UTC. Turkey time (`Europe/Istanbul`) is display-only and never participates in bucket membership or alignment.**

### Precision modes

The mode is explicit (`precision_mode=`). Default: `"exact"`.

#### `exact`

- Preserves `decimal128(38, 8)` OHLC and volume fields.
- Preserves UTC `timestamp[ms, tz=UTC]`.
- Returns a PyArrow table.
- Used for validation and exact comparisons.

Canonical `1m` physical names (`quote_volume`, `count`, `taker_buy_volume`, `taker_buy_quote_volume`) are mapped **in memory** onto the analytical interface. `source_candle_count` is `1` for canonical `1m`. Stored files are not rewritten.

#### `analysis`

- Converts OHLC and volume fields to in-memory `float64` after the read.
- Keeps timestamps UTC-aware.
- Keeps `number_of_trades` as int64 and `source_candle_count` as integer.
- Never writes converted values to disk.
- Never mutates an exact-mode table.
- Rejects NaN and infinity.

Analysis values are the PyArrow `float64` cast of the stored decimal. They agree with `float(decimal)` within absolute `1e-10` or relative `1e-10`. **Strategy code must not use exact floating-point equality**; use tick size or a tolerance.

### Warm-up

`warmup_bars=N` prepends exactly N **completed** candles with `close_time < start_utc`.

- Warm-up rows come first.
- `trade_start_index == N` when the full request is satisfied.
- `CandleSlice.warmup_mask()` / `warmup_table()` / `trade_table()` distinguish the regions.
- Warm-up is counted from real prior candles, including month, year, leap-day, and `1d` yearly partitions. It is not estimated by subtracting a calendar duration.
- `require_full_warmup=True` (default) raises `InsufficientWarmupError` if history is short.
- `require_full_warmup=False` returns what exists and reports `warmup_shortage`.

No warm-up marker is written to source Parquet.

### Multi-timeframe loading

`load_multi_timeframe` loads each requested timeframe from **its own verified dataset**. It never derives one timeframe from another and never forward-fills an unfinished candle.

The base timeframe must be in the list. A higher-frequency timeframe cannot be treated as a higher timeframe of a coarser base (for example base `1h` plus `5m` is rejected).

### Decision clock and no-look-ahead

Higher-timeframe visibility rule:

```text
higher_timeframe.close_time <= decision_time
```

`decision_clock="close"` (default): `decision_time = base.close_time`.
`decision_clock="open"`: `decision_time = base.open_time`.

Alignment returns **integer row indices** into the higher-timeframe table (`-1` if none is complete). OHLC values are not duplicated onto every base row.

This layer does **not** decide whether a signal computed at candle close may execute at that close or only at the next tradable price.

### Worked example (base `5m`, `decision_clock="close"`)

Base candle open `2026-08-10T10:30:00Z`, close `2026-08-10T10:34:59.999Z`:

| Higher TF | Latest available open | Not yet available |
| --- | --- | --- |
| `15m` | `10:15` (closed `10:29:59.999`) | `10:30` |
| `30m` | `10:00` | `10:30` |
| `1h` | `09:00` | `10:00` |
| `4h` | `04:00` | `08:00` (closes `11:59:59.999`) |
| `1d` | previous UTC day | current UTC day |

At `10:55` close, `15m 10:45`, `30m 10:30`, and `1h 10:00` are available; `4h 08:00` and the current `1d` are still not. The `4h 08:00` candle becomes available at the `11:55` `5m` close. The current UTC `1d` candle becomes available only when `close_time <= decision_time`, i.e. at `23:59:59.999`.

### Exceptions

Expected failures raise explicit types in `data/exceptions.py`, including `UnsupportedSymbolError`, `UnsupportedTimeframeError`, `NaiveDatetimeError`, `InvalidTimeRangeError`, `EmptyRangeError`, `PartitionNotFoundError`, `SchemaMismatchError`, `DataGapError`, `DuplicateTimestampError`, `OutOfOrderDataError`, `InsufficientWarmupError`, `LookaheadViolationError`, and `SourceMutationError`. Validation failures are not repaired.

### Minimal usage

```python
from datetime import datetime, timezone
from data.loader import load_candles, load_multi_timeframe

utc = timezone.utc
start = datetime(2026, 8, 10, tzinfo=utc)
end = datetime(2026, 8, 11, tzinfo=utc)

day = load_candles("BTCUSDT", "5m", start, end)  # 288 rows

hour = load_candles(
    "ETHUSDT", "1h",
    datetime(2025, 1, 1, tzinfo=utc),
    datetime(2025, 1, 8, tzinfo=utc),
    warmup_bars=100,
)

bundle = load_multi_timeframe(
    "BTCUSDT",
    ["5m", "15m", "30m", "1h", "4h", "1d"],
    base_timeframe="5m",
    start_utc=start,
    end_utc=end,
    decision_clock="close",
)
idx = bundle.available_index("1h", day.index_of_open(datetime(2026, 8, 10, 10, 30, tzinfo=utc)))
```
