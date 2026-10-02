#!/usr/bin/env python3
"""
Download and verify the ORIGINAL Binance public bulk-data archives that contain
1-minute (1m) candlesticks for a fixed set of Binance USD-M, USDT-margined
PERPETUAL futures contracts over a fixed, inclusive UTC range.

Scope -- audit these facts here and in the CONFIGURATION block below:
  * Provider / market : Binance Futures USD-M (data.binance.vision namespace ``futures/um``).
                        NOT Spot, NOT COIN-M (``futures/cm``), NOT dated delivery contracts.
  * Contract type     : PERPETUAL with quoteAsset = marginAsset = USDT, validated before any
                        download against the official USD-M metadata endpoint
                        ``fapi/v1/exchangeInfo`` (public, no API key). Nothing is substituted.
  * Interval          : 1m ONLY. Every URL, filename, directory and check refers to ``1m``.
  * Required range    : 2024-01-01T00:00:00Z .. 2026-09-15T23:59:00Z inclusive (candle OPEN times).
  * Archive selection : one official monthly archive per complete month; official daily archives
                        only for the partial month (2026-09-01 .. 2026-09-15) or when a monthly archive
                        is legitimately unavailable (HTTP 404 for a month at/after the listing date).
  * Incremental runs  : moving RANGE_END_UTC forward extends the dataset in place -- archives already on
                        disk are re-verified and skipped, only the new periods are downloaded, and the
                        report carries the previous run summaries forward (``previous_runs``).
  * Preservation      : downloaded ZIP bytes and the official ``.CHECKSUM`` sidecar are stored
                        unchanged; nothing is extracted, rewritten, normalised or converted.
                        Gaps / anomalies inside an authentic archive are REPORTED, never repaired.
  * Resumability      : transfers go to ``<name>.part`` and are atomically renamed only after
                        SHA-256 + ZIP CRC + CSV validation succeed; verified files are skipped on
                        re-runs; files that fail verification are moved to ``quarantine/``.

Standard library only (Python 3.9+). No CLI: edit the CONFIGURATION block or set the
``DATA_ROOT`` environment variable, then run ``python download_binance_um_perp_1m.py``.
Outputs: ``<DATA_ROOT>/raw/...`` archives, ``<DATA_ROOT>/reports/download_report.json``,
``<DATA_ROOT>/logs/download.log``, ``<DATA_ROOT>/quarantine/`` (only when something fails).
"""
from __future__ import annotations

import csv
import hashlib
import http.client
import io
import json
import logging
import os
import random
import shutil
import ssl
import sys
import time
import urllib.parse
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional

# =============================================================================
# CONFIGURATION -- the only place that defines WHAT is downloaded.
# =============================================================================
# Local destination root (user-supplied). Override with the DATA_ROOT environment variable or edit
# the default. The directory and all sub-directories are created automatically. Never commit the
# data directory to Git.
DATA_ROOT: Path = Path(os.environ.get("DATA_ROOT") or r"C:\MarketData").expanduser()

# Binance USD-M (USDT-margined) PERPETUAL contracts. Exact symbols -- nothing is substituted.
SYMBOLS: tuple[str, ...] = (
    "BTCUSDT", "ETHUSDT", "AVAXUSDT", "AAVEUSDT", "NEARUSDT",
    "LINKUSDT", "LTCUSDT", "OPUSDT", "DOTUSDT",
)
INTERVAL = "1m"  # the ONLY kline interval this script will ever request

# Fixed inclusive UTC range of required candle OPEN times (must cover complete UTC days).
RANGE_START_UTC = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
RANGE_END_UTC = datetime(2026, 9, 15, 23, 59, tzinfo=timezone.utc)   # extended from 2026-09-01 (incremental)

# Official sources (public, no API key). ``futures/um`` is the USD-M namespace.
ARCHIVE_BASE_URL = "https://data.binance.vision/data/futures/um"
EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
USER_AGENT = "binance-um-perpetual-1m-raw-archive-downloader/1.0 (Python stdlib; bounded concurrency)"

# Politeness / reliability.
MAX_WORKERS = 3                 # concurrent transfers
CONNECT_TIMEOUT_S = 15.0
READ_TIMEOUT_S = 90.0
MAX_HTTP_ATTEMPTS = 6           # per request, incl. mid-stream restarts of the same file
BACKOFF_BASE_S = 1.0
BACKOFF_CAP_S = 60.0
RETRY_AFTER_CAP_S = 120.0
CHUNK_BYTES = 1024 * 1024
DOWNLOAD_VERIFY_ATTEMPTS = 2    # a fresh download that fails integrity checks is re-downloaded once

# Conservative disk-space estimate (observed: ~1.8 MiB per monthly 1m ZIP, ~60 KiB per daily ZIP).
EST_MONTHLY_ZIP_BYTES = 8 * 1024 * 1024
EST_DAILY_ZIP_BYTES = 512 * 1024
DISK_SAFETY_MARGIN_BYTES = 1024 * 1024 * 1024
MAX_GAP_RANGES_REPORTED = 25    # per archive, in the JSON report
# =============================================================================

MINUTE_MS = 60_000
KLINE_COLUMNS = 12
EXPECTED_HEADER = (
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore",
)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
SSL_CONTEXT = ssl.create_default_context()
STATUS_DEFINITIONS = {
    "verified": "downloaded in this run; SHA-256 matches the official .CHECKSUM, ZIP CRC ok, expected CSV "
                "member present, all rows structurally valid. Content anomalies (if any) are listed per archive.",
    "skipped-as-already-verified": "a final ZIP already existed locally and passed the full verification "
                                   "again; nothing was downloaded or overwritten.",
    "unavailable": "Binance returned HTTP 404 for this period (e.g. period predates the contract listing, or "
                   "the archive is not published). Nothing was fabricated.",
    "failed": "no verified file exists for this period after this run (network exhaustion, non-retryable HTTP "
              "error, checksum/ZIP/CSV failure of a fresh download, or local I/O error). See 'error'.",
    "quarantined": "a file for this period failed verification and was moved to <DATA_ROOT>/quarantine/<run_id>/ "
                   "as evidence, and no verified replacement exists after this run. See 'quarantined_files'.",
}

log = logging.getLogger("binance_um_1m")


# ----------------------------------------------------------------------------- time helpers
def ms_of(dt: datetime) -> int:
    return (dt - EPOCH) // timedelta(milliseconds=1)


def iso_of_ms(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return (EPOCH + timedelta(milliseconds=ms)).strftime("%Y-%m-%dT%H:%M:%SZ")


def ceil_to_minute(ms: int) -> int:
    return -(-ms // MINUTE_MS) * MINUTE_MS


def month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    nxt = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, nxt


# ----------------------------------------------------------------------------- planning
@dataclass(frozen=True)
class Archive:
    symbol: str
    kind: str                    # "monthly" | "daily"
    period: str                  # "YYYY-MM" | "YYYY-MM-DD"
    start_ms: int                # first minute open time represented by the archive (inclusive)
    end_ms: int                  # last minute open time represented by the archive (inclusive)
    fallback_for: Optional[str] = None   # monthly period this daily archive substitutes for

    @property
    def zip_name(self) -> str:
        return f"{self.symbol}-{INTERVAL}-{self.period}.zip"

    @property
    def csv_name(self) -> str:
        return f"{self.symbol}-{INTERVAL}-{self.period}.csv"

    @property
    def zip_url(self) -> str:
        return f"{ARCHIVE_BASE_URL}/{self.kind}/klines/{self.symbol}/{INTERVAL}/{self.zip_name}"

    @property
    def checksum_url(self) -> str:
        return self.zip_url + ".CHECKSUM"

    def zip_path(self, root: Path) -> Path:
        return (root / "raw" / "binance" / "futures" / "um" / "perpetual" / self.kind / "klines"
                / self.symbol / INTERVAL / self.zip_name)

    def checksum_path(self, root: Path) -> Path:
        return self.zip_path(root).with_name(self.zip_name + ".CHECKSUM")


def daily_archives_for_month(symbol: str, year: int, month: int, range_start_ms: int, range_end_ms: int,
                             fallback_for: Optional[str] = None, not_before_ms: Optional[int] = None) -> list[Archive]:
    m_start, m_next = month_bounds(year, month)
    out: list[Archive] = []
    day = m_start
    while day < m_next:
        d_start, d_end = ms_of(day), ms_of(day) + 1439 * MINUTE_MS
        if d_start >= range_start_ms and d_end <= range_end_ms and (not_before_ms is None or d_end >= not_before_ms):
            out.append(Archive(symbol, "daily", day.strftime("%Y-%m-%d"), d_start, d_end, fallback_for))
        day += timedelta(days=1)
    return out


def plan_symbol(symbol: str, range_start: datetime, range_end: datetime) -> list[Archive]:
    """Monthly archive for every complete month inside the range; daily archives for partial months."""
    rs, re_ = ms_of(range_start), ms_of(range_end)
    out: list[Archive] = []
    y, m = range_start.year, range_start.month
    while (y, m) <= (range_end.year, range_end.month):
        m_start, m_next = month_bounds(y, m)
        m_first, m_last = ms_of(m_start), ms_of(m_next) - MINUTE_MS
        if m_first >= rs and m_last <= re_:
            out.append(Archive(symbol, "monthly", f"{y:04d}-{m:02d}", m_first, m_last))
        else:
            out.extend(daily_archives_for_month(symbol, y, m, rs, re_))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


# ----------------------------------------------------------------------------- HTTP (stdlib)
class TransientHttpError(Exception):
    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


class HttpError(Exception):
    """Non-retryable HTTP problem (unexpected status code, non-https URL, ...)."""


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return None


class Http:
    """Minimal https client with connect/read timeouts, bounded retries (exponential backoff + jitter,
    honouring Retry-After), streaming downloads and explicit 404 classification."""

    def _open(self, method: str, url: str) -> tuple[http.client.HTTPSConnection, http.client.HTTPResponse]:
        u = urllib.parse.urlsplit(url)
        if u.scheme != "https" or not u.hostname:
            raise HttpError(f"refusing non-https URL {url!r}")
        conn = http.client.HTTPSConnection(u.hostname, u.port or 443, timeout=CONNECT_TIMEOUT_S, context=SSL_CONTEXT)
        try:
            conn.connect()                          # connect (and TLS handshake) timeout
            conn.sock.settimeout(READ_TIMEOUT_S)    # read timeout for everything that follows
            path = u.path + (f"?{u.query}" if u.query else "")
            conn.request(method, path, headers={"User-Agent": USER_AGENT, "Accept": "*/*",
                                                "Accept-Encoding": "identity", "Connection": "close"})
            resp = conn.getresponse()
        except (OSError, http.client.HTTPException) as exc:
            conn.close()
            raise TransientHttpError(f"{type(exc).__name__}: {exc}") from exc
        if resp.status in (200, 404):
            return conn, resp
        retry_after = _parse_retry_after(resp.getheader("Retry-After"))
        status, reason = resp.status, resp.reason
        conn.close()
        if status == 429 or 500 <= status <= 599:
            raise TransientHttpError(f"HTTP {status} {reason}", retry_after)
        raise HttpError(f"HTTP {status} {reason} for {method} {url}")

    @staticmethod
    def _with_retries(fn, what: str):
        last: Optional[Exception] = None
        for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
            try:
                return fn()
            except TransientHttpError as exc:
                last = exc
                if attempt == MAX_HTTP_ATTEMPTS:
                    break
                delay = min(BACKOFF_CAP_S, BACKOFF_BASE_S * 2 ** (attempt - 1)) + random.uniform(0.0, 1.0)
                if exc.retry_after is not None:
                    delay = max(delay, min(exc.retry_after, RETRY_AFTER_CAP_S))
                log.warning("transient failure on %s (attempt %d/%d): %s -- retrying in %.1fs",
                            what, attempt, MAX_HTTP_ATTEMPTS, exc, delay)
                time.sleep(delay)
        raise TransientHttpError(f"gave up after {MAX_HTTP_ATTEMPTS} attempts on {what}: {last}")

    def get_bytes(self, url: str, max_bytes: int = 4 * 1024 * 1024) -> tuple[int, bytes]:
        """Small bodies only (checksum files, metadata). Returns (200, body) or (404, b'')."""
        def attempt():
            conn, resp = self._open("GET", url)
            try:
                if resp.status == 404:
                    return 404, b""
                try:
                    body = resp.read(max_bytes + 1)
                except (OSError, http.client.HTTPException) as exc:
                    raise TransientHttpError(f"read failed: {type(exc).__name__}: {exc}") from exc
                if len(body) > max_bytes:
                    raise HttpError(f"response body larger than {max_bytes} bytes for {url}")
                return 200, body
            finally:
                conn.close()
        return self._with_retries(attempt, url)

    def head_status(self, url: str) -> int:
        def attempt():
            conn, resp = self._open("HEAD", url)
            try:
                return resp.status
            finally:
                conn.close()
        return self._with_retries(attempt, url)

    def download(self, url: str, dest_part: Path) -> tuple[int, int]:
        """Stream url into dest_part (restarted from scratch on any transient failure).
        Returns (200, byte_count) or (404, 0)."""
        def attempt():
            if dest_part.exists():
                dest_part.unlink()
            conn, resp = self._open("GET", url)
            try:
                if resp.status == 404:
                    return 404, 0
                declared = resp.getheader("Content-Length")
                received = 0
                with open(dest_part, "wb") as fh:
                    while True:
                        try:
                            chunk = resp.read(CHUNK_BYTES)
                        except (OSError, http.client.HTTPException) as exc:
                            raise TransientHttpError(f"stream interrupted: {type(exc).__name__}: {exc}") from exc
                        if not chunk:
                            break
                        fh.write(chunk)
                        received += len(chunk)
                    fh.flush()
                    os.fsync(fh.fileno())
                if declared is not None and declared.strip().isdigit() and int(declared) != received:
                    raise TransientHttpError(f"short read: received {received} of {declared} bytes")
                return 200, received
            finally:
                conn.close()
        return self._with_retries(attempt, url)


# ----------------------------------------------------------------------------- verification
def parse_checksum_bytes(body: bytes, expected_zip_name: str) -> str:
    """Defensively parse an official ``.CHECKSUM`` file (``<sha256hex>  <zipname>``). Returns lowercase hex."""
    if len(body) > 4096:
        raise ValueError(f".CHECKSUM body is {len(body)} bytes; not a checksum file")
    text = body.decode("utf-8", errors="replace").lstrip("\ufeff")
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        digest = parts[0].lower()
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError(f"malformed .CHECKSUM line: {line[:120]!r}")
        if len(parts) > 1:
            named = parts[1].lstrip("*").replace("\\", "/").rsplit("/", 1)[-1]
            if named != expected_zip_name:
                raise ValueError(f".CHECKSUM names {named!r} but {expected_zip_name!r} was expected")
        return digest
    raise ValueError(".CHECKSUM file contains no digest line")


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(CHUNK_BYTES), b""):
            h.update(block)
    return h.hexdigest()


@dataclass
class Verification:
    ok: bool = False                        # integrity + structure (authentic, well-formed kline archive)
    error: Optional[str] = None
    actual_sha256: Optional[str] = None
    file_size_bytes: Optional[int] = None
    csv_member: Optional[str] = None
    has_header_row: Optional[bool] = None
    timestamp_unit: Optional[str] = None
    row_count: int = 0
    first_open_time_utc: Optional[str] = None
    last_open_time_utc: Optional[str] = None
    open_time_min_utc: Optional[str] = None   # only set when rows are out of order
    open_time_max_utc: Optional[str] = None
    expected_minute_count: int = 0
    present_minute_count: int = 0
    missing_minute_count: int = 0
    missing_ranges: list = field(default_factory=list)
    missing_ranges_truncated: bool = False
    duplicate_minute_count: int = 0
    out_of_order_count: int = 0
    rows_outside_period: int = 0
    misaligned_open_time_count: int = 0
    ohlc_violation_count: int = 0
    negative_volume_count: int = 0
    malformed_row_count: int = 0
    malformed_examples: list = field(default_factory=list)
    content_checks_passed: bool = False
    anomalies: list = field(default_factory=list)


def _timestamp_unit(raw: int) -> Optional[str]:
    if 10**12 <= raw < 10**13:
        return "milliseconds"
    if 10**15 <= raw < 10**16:
        return "microseconds"
    return None


def _scan_kline_csv(text: io.TextIOBase, archive: Archive, exp_start: int, exp_end: int, v: Verification) -> None:
    """Stream the CSV once; collect structural, chronological, coverage and OHLC statistics."""
    reader = csv.reader(text)
    seen: set[int] = set()
    prev: Optional[int] = None
    first = last = lo = hi = None
    unit: Optional[str] = None

    def malformed(lineno: int, row: list, why: str) -> None:
        v.malformed_row_count += 1
        if len(v.malformed_examples) < 5:
            v.malformed_examples.append({"line": lineno, "reason": why, "row": ",".join(row)[:200]})

    for lineno, row in enumerate(reader, 1):
        if not row:
            continue
        if v.row_count == 0 and v.has_header_row is None and not row[0].strip().lstrip("-").isdigit():
            v.has_header_row = True
            if tuple(c.strip().lower() for c in row) != EXPECTED_HEADER:
                v.anomalies.append(f"unexpected header row: {','.join(row)[:200]}")
            continue
        if v.has_header_row is None:
            v.has_header_row = False
        if len(row) != KLINE_COLUMNS:
            malformed(lineno, row, f"{len(row)} columns, expected {KLINE_COLUMNS}")
            continue
        try:
            raw_open = int(row[0])
            o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
            vol, qvol = float(row[5]), float(row[7])
            int(row[6])                                   # close_time must parse
            trades = int(row[8])
            tb_vol, tb_qvol = float(row[9]), float(row[10])
        except ValueError as exc:
            malformed(lineno, row, f"unparseable field: {exc}")
            continue
        row_unit = _timestamp_unit(raw_open)
        if row_unit is None:
            malformed(lineno, row, f"open_time {raw_open} is neither ms nor us epoch")
            continue
        if unit is None:
            unit = row_unit
            v.timestamp_unit = unit
        elif row_unit != unit:
            malformed(lineno, row, "mixed timestamp units in one archive")
            continue
        open_ms, rem = (raw_open, 0) if unit == "milliseconds" else divmod(raw_open, 1000)
        if rem or open_ms % MINUTE_MS:
            v.misaligned_open_time_count += 1

        v.row_count += 1
        if first is None:
            first = lo = hi = open_ms
        last = open_ms
        lo, hi = min(lo, open_ms), max(hi, open_ms)
        if prev is not None and open_ms < prev:
            v.out_of_order_count += 1
        prev = open_ms
        if open_ms in seen:
            v.duplicate_minute_count += 1
        else:
            seen.add(open_ms)
        if not (h >= max(o, c) and l <= min(o, c) and h >= l):
            v.ohlc_violation_count += 1
        if vol < 0 or qvol < 0 or tb_vol < 0 or tb_qvol < 0 or trades < 0:
            v.negative_volume_count += 1
        if open_ms < archive.start_ms or open_ms > archive.end_ms:
            v.rows_outside_period += 1

    v.first_open_time_utc, v.last_open_time_utc = iso_of_ms(first), iso_of_ms(last)
    if v.out_of_order_count:
        v.open_time_min_utc, v.open_time_max_utc = iso_of_ms(lo), iso_of_ms(hi)

    # Coverage inside (archive period ∩ required range ∩ [listing date, ...)) -- only minute-aligned opens count.
    v.expected_minute_count = (exp_end - exp_start) // MINUTE_MS + 1 if exp_end >= exp_start else 0
    present = 0
    gaps: list[tuple[int, int]] = []
    gap_start: Optional[int] = None
    for t in range(exp_start, exp_end + 1, MINUTE_MS) if v.expected_minute_count else ():
        if t in seen:
            present += 1
            if gap_start is not None:
                gaps.append((gap_start, t - MINUTE_MS))
                gap_start = None
        elif gap_start is None:
            gap_start = t
    if gap_start is not None:
        gaps.append((gap_start, exp_end))
    v.present_minute_count = present
    v.missing_minute_count = v.expected_minute_count - present
    v.missing_ranges = [{"from": iso_of_ms(a), "to": iso_of_ms(b), "minutes": (b - a) // MINUTE_MS + 1}
                        for a, b in gaps[:MAX_GAP_RANGES_REPORTED]]
    v.missing_ranges_truncated = len(gaps) > MAX_GAP_RANGES_REPORTED


def verify_archive_file(path: Path, archive: Archive, expected_sha256: str, exp_start: int, exp_end: int) -> Verification:
    """Full verification of a ZIP on disk: SHA-256 vs official checksum, ZIP CRC, expected CSV member,
    streamed structural validation. Content anomalies are recorded but do not make an authentic archive fail."""
    v = Verification()
    v.file_size_bytes = path.stat().st_size
    v.actual_sha256 = sha256_of_file(path)
    if v.actual_sha256 != expected_sha256.lower():
        v.error = f"SHA-256 mismatch: expected {expected_sha256}, got {v.actual_sha256}"
        return v
    try:
        with zipfile.ZipFile(path) as zf:
            bad = zf.testzip()
            if bad is not None:
                v.error = f"ZIP CRC integrity test failed for member {bad!r}"
                return v
            names = zf.namelist()
            if names != [archive.csv_name]:
                v.error = f"unexpected ZIP members {names!r}; expected exactly [{archive.csv_name!r}]"
                return v
            v.csv_member = archive.csv_name
            with zf.open(archive.csv_name) as raw:
                _scan_kline_csv(io.TextIOWrapper(raw, encoding="utf-8", errors="strict", newline=""),
                                archive, exp_start, exp_end, v)
    except zipfile.BadZipFile as exc:
        v.error = f"not a valid ZIP archive: {exc}"
        return v
    except UnicodeDecodeError as exc:
        v.error = f"CSV member is not valid UTF-8: {exc}"
        return v
    if v.malformed_row_count:
        v.error = f"{v.malformed_row_count} structurally invalid kline row(s); examples: {v.malformed_examples}"
        return v

    if v.row_count == 0:
        v.anomalies.append("CSV contains no kline rows")
    if v.timestamp_unit not in (None, "milliseconds"):
        v.anomalies.append(f"open_time values are {v.timestamp_unit}, not millisecond timestamps")
    if v.duplicate_minute_count:
        v.anomalies.append(f"{v.duplicate_minute_count} duplicate open time(s)")
    if v.out_of_order_count:
        v.anomalies.append(f"{v.out_of_order_count} open time(s) not in chronological order")
    if v.rows_outside_period:
        v.anomalies.append(f"{v.rows_outside_period} row(s) with open time outside the archive period")
    if v.misaligned_open_time_count:
        v.anomalies.append(f"{v.misaligned_open_time_count} open time(s) not aligned to a minute boundary")
    if v.ohlc_violation_count:
        v.anomalies.append(f"{v.ohlc_violation_count} row(s) violate OHLC invariants")
    if v.negative_volume_count:
        v.anomalies.append(f"{v.negative_volume_count} row(s) with negative volume/count fields")
    v.content_checks_passed = not v.anomalies
    v.ok = True
    return v


# ----------------------------------------------------------------------------- per-archive processing
@dataclass
class Job:
    root: Path
    run_id: str
    range_start_ms: int
    range_end_ms: int
    http: Http
    onboard_ms: dict          # symbol -> onboardDate in ms (from exchangeInfo), or None


def _atomic_write_bytes(part: Path, final: Path, body: bytes) -> None:
    part.parent.mkdir(parents=True, exist_ok=True)
    with open(part, "wb") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(part, final)


def _quarantine(job: Job, paths: list[Path], reason: str, rec: dict) -> None:
    """Move evidence (never delete) to <DATA_ROOT>/quarantine/<run_id>/<relative path> + a reason file."""
    raw_root = job.root / "raw"
    for p in paths:
        if not p.exists():
            continue
        try:
            rel = p.relative_to(raw_root)
        except ValueError:
            rel = Path(p.name)
        dest = job.root / "quarantine" / job.run_id / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            dest = dest.with_name(f"{dest.name}.{int(time.time() * 1000)}")
        shutil.move(str(p), str(dest))
        dest.with_name(dest.name + ".quarantine_reason.txt").write_text(
            f"{datetime.now(timezone.utc).isoformat()}\n{reason}\n", encoding="utf-8")
        rec["quarantined_files"].append(str(dest))
        log.warning("%s: quarantined %s -> %s", rec["zip_url"].rsplit("/", 1)[-1], p.name, dest)


def _new_record(archive: Archive, zip_path: Path, chk_path: Path) -> dict:
    rec = {
        "symbol": archive.symbol, "interval": INTERVAL, "archive_type": archive.kind, "period": archive.period,
        "period_start_utc": iso_of_ms(archive.start_ms), "period_end_utc": iso_of_ms(archive.end_ms),
        "fallback_for_monthly": archive.fallback_for,
        "zip_url": archive.zip_url, "checksum_url": archive.checksum_url,
        "zip_path": str(zip_path), "checksum_path": str(chk_path),
        "status": None, "expected_sha256": None, "error": None, "download_attempts": 0,
    }
    v = asdict(Verification())
    v.pop("ok"), v.pop("error")
    rec.update(v)
    rec.update({"notes": [], "quarantined_files": []})
    return rec


def _apply_verification(rec: dict, v: Verification) -> None:
    d = asdict(v)
    d.pop("ok"), d.pop("error")
    rec.update(d)


def _unavailable_reason(archive: Archive, onboard_ms: Optional[int]) -> str:
    if onboard_ms is not None and archive.end_ms < onboard_ms:
        return f"HTTP 404 -- period predates the contract onboardDate {iso_of_ms(onboard_ms)}; no data exists"
    return "HTTP 404 -- archive not published on data.binance.vision for this period"


def process_archive(job: Job, archive: Archive) -> tuple[Archive, dict]:
    root, name = job.root, archive.zip_name
    zip_path, chk_path = archive.zip_path(root), archive.checksum_path(root)
    zip_part, chk_part = zip_path.with_name(zip_path.name + ".part"), chk_path.with_name(chk_path.name + ".part")
    onboard = job.onboard_ms.get(archive.symbol)
    exp_start = max(archive.start_ms, job.range_start_ms, ceil_to_minute(onboard) if onboard is not None else archive.start_ms)
    exp_end = min(archive.end_ms, job.range_end_ms)
    rec = _new_record(archive, zip_path, chk_path)

    def finish(status: str, error: Optional[str] = None) -> tuple[Archive, dict]:
        rec["status"], rec["error"] = status, error
        return archive, rec

    def unverified_status() -> str:
        return "quarantined" if rec["quarantined_files"] else "failed"

    try:
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        for p in (zip_part, chk_part):
            if p.exists():
                log.info("%s: discarding stale partial file %s", name, p.name)
                p.unlink()

        # --- A. a final ZIP already exists: re-verify it fully, never overwrite it silently
        if zip_path.exists():
            expected = None
            if chk_path.exists():
                try:
                    expected = parse_checksum_bytes(chk_path.read_bytes(), name)
                except ValueError as exc:
                    rec["notes"].append(f"local .CHECKSUM unreadable ({exc}); re-fetching the official one")
            if expected is None:
                code, body = job.http.get_bytes(archive.checksum_url)
                if code == 404:
                    reason = "official .CHECKSUM returned HTTP 404; the local ZIP cannot be verified"
                    _quarantine(job, [zip_path, chk_path], reason, rec)
                    return finish(unverified_status(), reason)
                expected = parse_checksum_bytes(body, name)
                _atomic_write_bytes(chk_part, chk_path, body)
                rec["notes"].append("official .CHECKSUM (re-)fetched for pre-existing ZIP")
            rec["expected_sha256"] = expected
            v = verify_archive_file(zip_path, archive, expected, exp_start, exp_end)
            _apply_verification(rec, v)
            if v.ok:
                log.info("%s: already present, re-verified (sha256 ok, %d rows, %d missing minutes) -- skipped",
                         name, v.row_count, v.missing_minute_count)
                return finish("skipped-as-already-verified")
            log.warning("%s: pre-existing archive FAILED verification: %s", name, v.error)
            _quarantine(job, [zip_path, chk_path], v.error or "verification failed", rec)
            rec["notes"].append("pre-existing archive failed verification and was quarantined; fresh download attempted")

        # --- B. official checksum first (small); classify 404s
        code, body = job.http.get_bytes(archive.checksum_url)
        if code == 404:
            zip_status = job.http.head_status(archive.zip_url)
            if zip_status == 404:
                reason = _unavailable_reason(archive, onboard)
                log.info("%s: unavailable -- %s", name, reason)
                return finish("unavailable", reason)
            reason = f"ZIP is published (HEAD {zip_status}) but its official .CHECKSUM returned HTTP 404; not downloaded"
            log.error("%s: %s", name, reason)
            return finish(unverified_status(), reason)
        expected = parse_checksum_bytes(body, name)
        rec["expected_sha256"] = expected
        _atomic_write_bytes(chk_part, chk_path, body)

        # --- C. stream the ZIP to .part, verify, then atomically rename
        for attempt in range(1, DOWNLOAD_VERIFY_ATTEMPTS + 1):
            rec["download_attempts"] = attempt
            code, received = job.http.download(archive.zip_url, zip_part)
            if code == 404:
                if chk_path.exists():
                    chk_path.unlink()
                    rec["notes"].append("removed orphan .CHECKSUM sidecar because the ZIP does not exist")
                reason = "official .CHECKSUM exists but the ZIP returned HTTP 404"
                log.warning("%s: unavailable -- %s", name, reason)
                return finish("unavailable", reason)
            v = verify_archive_file(zip_part, archive, expected, exp_start, exp_end)
            _apply_verification(rec, v)
            if v.ok:
                os.replace(zip_part, zip_path)
                log.info("%s: downloaded %d bytes, sha256 ok, %d rows, %d missing minutes%s", name, received,
                         v.row_count, v.missing_minute_count, "" if v.content_checks_passed else f", ANOMALIES: {v.anomalies}")
                return finish("verified")
            log.warning("%s: fresh download FAILED verification (attempt %d/%d): %s", name, attempt,
                        DOWNLOAD_VERIFY_ATTEMPTS, v.error)
            if attempt < DOWNLOAD_VERIFY_ATTEMPTS:
                zip_part.unlink()
                continue
            _quarantine(job, [zip_part], v.error or "verification failed", rec)
            return finish(unverified_status(), v.error)
        return finish(unverified_status(), "unreachable")
    except Exception as exc:  # network exhaustion, malformed checksum text, local I/O errors, ...
        reason = f"{type(exc).__name__}: {exc}"
        log.error("%s: FAILED -- %s", name, reason)
        for p in (zip_part, chk_part):
            try:
                if p.exists():
                    p.unlink()          # an incomplete transfer is not evidence of anything
            except OSError:
                pass
        return finish(unverified_status(), reason)


# ----------------------------------------------------------------------------- symbol validation
def validate_symbols(http_: Http, symbols: tuple[str, ...]) -> dict:
    code, body = http_.get_bytes(EXCHANGE_INFO_URL, max_bytes=64 * 1024 * 1024)
    if code != 200:
        raise RuntimeError(f"{EXCHANGE_INFO_URL} returned HTTP {code}")
    info = json.loads(body.decode("utf-8"))
    by_symbol = {s.get("symbol"): s for s in info.get("symbols", [])}
    out: dict = {}
    for sym in symbols:
        s = by_symbol.get(sym)
        if s is None:
            out[sym] = {"validated": False, "reason": "symbol not present in Binance USD-M exchangeInfo",
                        "onboard_date_ms": None, "onboard_date_utc": None}
            continue
        problems = []
        if s.get("contractType") != "PERPETUAL":
            problems.append(f"contractType={s.get('contractType')!r}, expected 'PERPETUAL'")
        if s.get("quoteAsset") != "USDT":
            problems.append(f"quoteAsset={s.get('quoteAsset')!r}, expected 'USDT'")
        if s.get("marginAsset") != "USDT":
            problems.append(f"marginAsset={s.get('marginAsset')!r}, expected 'USDT'")
        onboard = s.get("onboardDate")
        onboard = int(onboard) if isinstance(onboard, (int, float, str)) and str(onboard).isdigit() else None
        out[sym] = {
            "validated": not problems, "reason": "; ".join(problems) or None,
            "contract_type": s.get("contractType"), "pair": s.get("pair"), "base_asset": s.get("baseAsset"),
            "quote_asset": s.get("quoteAsset"), "margin_asset": s.get("marginAsset"), "status": s.get("status"),
            "underlying_type": s.get("underlyingType"),
            "onboard_date_ms": onboard, "onboard_date_utc": iso_of_ms(onboard),
            "warnings": [] if s.get("status") == "TRADING" else [f"status is {s.get('status')!r}, not 'TRADING'"],
        }
    return out


# ----------------------------------------------------------------------------- reporting
def load_previous_report(path: Path) -> Optional[dict]:
    """The download_report.json left by the previous run (if any). Its summary is carried forward so an
    incremental extension still documents the complete dataset and its history."""
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except (OSError, ValueError) as exc:
        log.warning("previous report %s could not be parsed (%s); run history restarts with this run", path, exc)
        return None


def previous_range_end_ms(previous: Optional[dict]) -> Optional[int]:
    try:
        text = previous["required_range_utc"]["last_open_time"]
        return ms_of(datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc))
    except (TypeError, KeyError, ValueError):
        return None


def on_disk_inventory(root: Path) -> dict:
    """Counts of what physically exists under <DATA_ROOT>/raw (independent of this run's records)."""
    base = root / "raw" / "binance" / "futures" / "um" / "perpetual"
    inv: dict = {}
    for kind in ("monthly", "daily"):
        d = base / kind / "klines"
        inv[f"{kind}_zip_files"] = sum(1 for _ in d.rglob("*.zip")) if d.exists() else 0
        inv[f"{kind}_checksum_files"] = sum(1 for _ in d.rglob("*.zip.CHECKSUM")) if d.exists() else 0
    inv["zip_files_total"] = inv["monthly_zip_files"] + inv["daily_zip_files"]
    inv["checksum_files_total"] = inv["monthly_checksum_files"] + inv["daily_checksum_files"]
    inv["part_files_in_raw"] = sum(1 for _ in (root / "raw").rglob("*.part"))
    inv["interval_directories"] = sorted({p.name for kind in ("monthly", "daily")
                                          for p in (base / kind / "klines").glob("*/*") if p.is_dir()})
    inv["quarantine_files"] = sum(1 for p in (root / "quarantine").rglob("*") if p.is_file())
    return inv


def find_superseded_daily(root: Path, symbol: str, verified_monthly_periods: set) -> list[str]:
    """Daily ZIPs left on disk (from an earlier fallback run) whose month now has a verified monthly archive.
    They are not active coverage and are NOT deleted; they are only listed."""
    daily_dir = root / "raw" / "binance" / "futures" / "um" / "perpetual" / "daily" / "klines" / symbol / INTERVAL
    if not daily_dir.exists():
        return []
    prefix = f"{symbol}-{INTERVAL}-"
    return [str(p) for p in sorted(daily_dir.glob(f"{prefix}????-??-??.zip"))
            if p.stem[len(prefix):][:7] in verified_monthly_periods]


def build_report(job: Job, started: datetime, symbols: tuple[str, ...], validation: dict,
                 planned: list[Archive], results: list[tuple[Archive, dict]], fallback_added: int, interrupted: bool,
                 previous: Optional[dict] = None) -> dict:
    order = {s: i for i, s in enumerate(symbols)}
    results = sorted(results, key=lambda ar: (order.get(ar[0].symbol, 999), ar[0].kind != "monthly", ar[0].period))
    recs = [rec for _, rec in results]
    ok_statuses = ("verified", "skipped-as-already-verified")
    coverage: dict = {}
    for sym in symbols:
        v = validation[sym]
        sym_recs = [r for r in recs if r["symbol"] == sym]
        if not v["validated"]:
            coverage[sym] = {"validated": False, "reason": v["reason"], "archives_planned": 0, "coverage_complete": False}
            continue
        onboard = v.get("onboard_date_ms")
        exp_start = max(job.range_start_ms, ceil_to_minute(onboard) if onboard is not None else job.range_start_ms)
        expected = (job.range_end_ms - exp_start) // MINUTE_MS + 1 if job.range_end_ms >= exp_start else 0
        ok_recs = [r for r in sym_recs if r["status"] in ok_statuses]
        covered = sum(r["present_minute_count"] for r in ok_recs)
        by_status = Counter(r["status"] for r in sym_recs)
        verified_months = {r["period"] for r in ok_recs if r["archive_type"] == "monthly"}
        coverage[sym] = {
            "validated": True, "contract_type": v["contract_type"], "status_in_exchange_info": v["status"],
            "onboard_date_utc": v["onboard_date_utc"],
            "expected_first_open_time_utc": iso_of_ms(exp_start), "expected_last_open_time_utc": iso_of_ms(job.range_end_ms),
            "expected_minute_count": expected, "covered_minute_count": covered,
            "missing_minute_count": expected - covered,
            "coverage_pct": round(100.0 * covered / expected, 4) if expected else None,
            "row_count_total": sum(r["row_count"] for r in ok_recs),
            "first_open_time_utc": min((r["first_open_time_utc"] for r in ok_recs if r["first_open_time_utc"]), default=None),
            "last_open_time_utc": max((r["last_open_time_utc"] for r in ok_recs if r["last_open_time_utc"]), default=None),
            "archives_planned": len(sym_recs), "archives_by_status": dict(sorted(by_status.items())),
            "unavailable_periods": [r["period"] for r in sym_recs if r["status"] == "unavailable"],
            "failed_periods": [r["period"] for r in sym_recs if r["status"] == "failed"],
            "quarantined_periods": [r["period"] for r in sym_recs if r["status"] == "quarantined"],
            "archives_with_missing_minutes": [r["period"] for r in ok_recs if r["missing_minute_count"]],
            "archives_with_content_anomalies": [r["period"] for r in ok_recs if not r["content_checks_passed"]],
            "duplicate_minute_count_total": sum(r["duplicate_minute_count"] for r in ok_recs),
            "out_of_order_count_total": sum(r["out_of_order_count"] for r in ok_recs),
            "ohlc_violation_count_total": sum(r["ohlc_violation_count"] for r in ok_recs),
            "superseded_daily_archives_on_disk": find_superseded_daily(job.root, sym, verified_months),
            "coverage_complete": expected - covered == 0 and not by_status["failed"] and not by_status["quarantined"],
        }
    totals = Counter(r["status"] for r in recs)
    now = datetime.now(timezone.utc)

    # Whole-dataset figures over every archive that is verified after this run (new or pre-existing).
    ok_all = [r for r in recs if r["status"] in ok_statuses]
    valid_cov = [c for c in coverage.values() if c["validated"]]
    dataset_summary = {
        "verified_archives": len(ok_all),
        "total_row_count": sum(r["row_count"] for r in ok_all),
        "total_file_size_bytes": sum(r["file_size_bytes"] or 0 for r in ok_all),
        "first_open_time_utc": min((r["first_open_time_utc"] for r in ok_all if r["first_open_time_utc"]), default=None),
        "last_open_time_utc": max((r["last_open_time_utc"] for r in ok_all if r["last_open_time_utc"]), default=None),
        "total_missing_minute_count": sum(c["missing_minute_count"] for c in valid_cov),
        "total_duplicate_minute_count": sum(c["duplicate_minute_count_total"] for c in valid_cov),
        "total_out_of_order_count": sum(c["out_of_order_count_total"] for c in valid_cov),
        "total_ohlc_violation_count": sum(c["ohlc_violation_count_total"] for c in valid_cov),
        "all_symbols_coverage_complete": bool(valid_cov) and all(c["coverage_complete"] for c in coverage.values()),
    }

    # Incremental extension relative to the previous report: archives lying entirely after its range end.
    extension = None
    previous_runs: list = []
    if previous is not None:
        previous_runs = list(previous.get("previous_runs") or [])
        previous_runs.append({k: previous.get(k) for k in ("run_id", "run_started_utc", "generated_at_utc", "duration_seconds",
                                                           "interrupted", "required_range_utc", "plan_summary", "run_summary")})
        prev_end = previous_range_end_ms(previous)
        if prev_end is not None:
            new_planned = [a for a in planned if a.start_ms > prev_end]
            new_results = [(a, r) for a, r in results if a.start_ms > prev_end]
            extension = {
                "previous_report_run_id": previous.get("run_id"),
                "previous_last_open_time_utc": iso_of_ms(prev_end),
                "new_last_open_time_utc": iso_of_ms(job.range_end_ms),
                "new_periods_utc": sorted({a.period for a in new_planned}),
                "expected_new_archives": len(new_planned),
                "new_archives_by_type": {"monthly": sum(a.kind == "monthly" for a in new_planned),
                                         "daily": sum(a.kind == "daily" for a in new_planned)},
                "new_archives_by_status": dict(sorted(Counter(r["status"] for _, r in new_results).items())),
                "new_row_count": sum(r["row_count"] for _, r in new_results if r["status"] in ok_statuses),
                "new_archives": [{"zip": a.zip_name, "status": r["status"]} for a, r in new_results],
            }

    return {
        "report_version": 1,
        "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_id": job.run_id,
        "run_started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_seconds": round((now - started).total_seconds(), 1),
        "interrupted": interrupted,
        "provider": "Binance",
        "market": "Binance USD-M futures (USDT-margined PERPETUAL contracts)",
        "archive_namespace": "futures/um",
        "explicitly_not_downloaded": ["spot", "futures/cm (COIN-M)", "dated delivery contracts",
                                      "any kline interval other than 1m", "mark/index/premium-index klines"],
        "source_base_url": ARCHIVE_BASE_URL,
        "metadata_source_url": EXCHANGE_INFO_URL,
        "interval": INTERVAL,
        "required_range_utc": {
            "first_open_time": iso_of_ms(job.range_start_ms), "last_open_time": iso_of_ms(job.range_end_ms),
            "inclusive": True, "complete_utc_days": (job.range_end_ms + MINUTE_MS - job.range_start_ms) // (1440 * MINUTE_MS),
        },
        "data_root": str(job.root),
        "requested_symbols": list(symbols),
        "validated_symbols": [s for s in symbols if validation[s]["validated"]],
        "symbol_validation": validation,
        "status_definitions": STATUS_DEFINITIONS,
        "plan_summary": {
            "monthly_archives_planned": sum(a.kind == "monthly" for a in planned),
            "daily_archives_planned": sum(a.kind == "daily" for a in planned),
            "daily_fallback_archives_added": fallback_added,
            "archives_processed": len(recs),
        },
        "run_summary": dict(sorted(totals.items())),
        "incremental_extension": extension,
        "previous_runs": previous_runs,
        "dataset_summary": dataset_summary,
        "on_disk_inventory": on_disk_inventory(job.root),
        "coverage_summary": coverage,
        "archives": recs,
        "data_quality_note": "Gaps, duplicates and OHLC anomalies are reported exactly as observed in the official "
                             "archives; nothing was repaired, interpolated, altered or converted. This report holds "
                             "metadata only (no candle rows).",
    }


def write_report(path: Path, report: dict) -> None:
    part = path.with_name(path.name + ".part")
    with open(part, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
        fh.write("\n")
    os.replace(part, path)


# ----------------------------------------------------------------------------- orchestration
def setup_logging(log_file: Path) -> None:
    fmt = logging.Formatter("%(asctime)sZ %(levelname)-7s %(threadName)-6s %(message)s", "%Y-%m-%dT%H:%M:%S")
    fmt.converter = time.gmtime
    log.setLevel(logging.INFO)
    log.handlers.clear()
    for handler in (logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)):
        handler.setFormatter(fmt)
        log.addHandler(handler)


def run_pool(job: Job, archives: list[Archive], results: list) -> None:
    if not archives:
        return
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="dl") as ex:
        futures = [ex.submit(process_archive, job, a) for a in archives]
        try:
            for fut in as_completed(futures):
                results.append(fut.result())
        except KeyboardInterrupt:
            log.warning("interrupted -- cancelling pending transfers; in-flight .part files are discarded on the next run")
            ex.shutdown(wait=False, cancel_futures=True)
            raise


def run(root: Path, symbols: tuple[str, ...], range_start: datetime, range_end: datetime) -> int:
    started = datetime.now(timezone.utc)
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    root = root.resolve()
    for sub in ("logs", "reports", "quarantine", "raw"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    setup_logging(root / "logs" / "download.log")
    log.info("run %s started -- data root %s", run_id, root)
    report_path = root / "reports" / "download_report.json"
    previous = load_previous_report(report_path)
    prev_end_ms = previous_range_end_ms(previous)

    if not (range_start.tzinfo and range_end.tzinfo and range_start <= range_end
            and (range_start.hour, range_start.minute) == (0, 0) and (range_end.hour, range_end.minute) == (23, 59)):
        log.error("BLOCKER: RANGE_START_UTC/RANGE_END_UTC must be tz-aware UTC and cover complete UTC days (00:00 .. 23:59)")
        return 2
    http_ = Http()

    # 1. validate every configured symbol against official USD-M metadata
    try:
        validation = validate_symbols(http_, symbols)
    except Exception as exc:
        log.error("BLOCKER: cannot validate symbols against %s: %s", EXCHANGE_INFO_URL, exc)
        return 2
    for sym in symbols:
        v = validation[sym]
        if v["validated"]:
            log.info("symbol %-9s OK  contractType=%s quote=%s margin=%s status=%s onboard=%s%s", sym, v["contract_type"],
                     v["quote_asset"], v["margin_asset"], v["status"], v["onboard_date_utc"],
                     f"  WARNING: {v['warnings']}" if v["warnings"] else "")
        else:
            log.error("symbol %-9s REJECTED -- %s (nothing will be downloaded for it)", sym, v["reason"])
    valid_symbols = tuple(s for s in symbols if validation[s]["validated"])
    onboard_ms = {s: validation[s].get("onboard_date_ms") for s in valid_symbols}

    # 2. exact expected archive list per symbol
    planned: list[Archive] = [a for s in valid_symbols for a in plan_symbol(s, range_start, range_end)]
    monthly = [a for a in planned if a.kind == "monthly"]
    daily = [a for a in planned if a.kind == "daily"]
    already_present = [a for a in planned if a.zip_path(root).exists()]
    months = sorted({a.period for a in monthly})
    days = sorted({a.period for a in daily})

    # 3. pre-download summary
    total_days = (ms_of(range_end) + MINUTE_MS - ms_of(range_start)) // (1440 * MINUTE_MS)
    lines = [
        "=" * 100,
        "Binance USD-M (USDT-margined) PERPETUAL futures raw 1m archive download -- NOT Spot, NOT COIN-M, NOT delivery",
        f"Symbols ({len(valid_symbols)}/{len(symbols)} validated as PERPETUAL/USDT via exchangeInfo): {', '.join(valid_symbols)}",
        f"Interval: {INTERVAL} candles only",
        f"UTC range: {iso_of_ms(ms_of(range_start))} through {iso_of_ms(ms_of(range_end))} inclusive ({total_days} complete UTC days)",
        f"Destination: {root}",
        f"Planned: {len(monthly)} monthly archives ({months[0] if months else '-'}..{months[-1] if months else '-'}) + "
        f"{len(daily)} daily archives ({', '.join(days) if days else '-'}) = {len(planned)} ZIP + {len(planned)} .CHECKSUM files",
        f"Already on disk (re-verified, not re-downloaded): {len(already_present)}",
        f"Source: {ARCHIVE_BASE_URL}/{{monthly|daily}}/klines/{{SYMBOL}}/{INTERVAL}/ ; concurrency={MAX_WORKERS}",
    ]
    if prev_end_ms is not None:
        new_planned = [a for a in planned if a.start_ms > prev_end_ms]
        new_periods = sorted({a.period for a in new_planned})
        lines += [
            f"Incremental extension of previous report (run {previous.get('run_id')}): existing end "
            f"{iso_of_ms(prev_end_ms)} -> new end {iso_of_ms(ms_of(range_end))}",
            f"New archives beyond the existing end: {len(new_planned)} ({sum(a.kind == 'monthly' for a in new_planned)} monthly, "
            f"{sum(a.kind == 'daily' for a in new_planned)} daily"
            f"{f'; periods {new_periods[0]}..{new_periods[-1]}' if new_periods else ''}); "
            f"of these already on disk: {sum(a.zip_path(root).exists() for a in new_planned)}",
        ]
    lines.append("=" * 100)
    for line in lines:
        log.info(line)
    if not planned:
        log.error("BLOCKER: nothing to download (no symbol validated)")
        return 2

    # 4. disk-space check (conservative estimate for archives not yet present)
    need = sum(EST_MONTHLY_ZIP_BYTES if a.kind == "monthly" else EST_DAILY_ZIP_BYTES
               for a in planned if not a.zip_path(root).exists()) + DISK_SAFETY_MARGIN_BYTES
    free = shutil.disk_usage(root).free
    log.info("disk: %.2f GiB free, conservative requirement %.2f GiB", free / 2**30, need / 2**30)
    if free < need:
        log.error("BLOCKER: insufficient disk space at %s (free %.2f GiB < required %.2f GiB)", root, free / 2**30, need / 2**30)
        return 2

    # 5. download + verify: monthly first, then daily (planned partial month + fallbacks for missing months)
    job = Job(root, run_id, ms_of(range_start), ms_of(range_end), http_, onboard_ms)
    results: list[tuple[Archive, dict]] = []
    fallback: list[Archive] = []
    interrupted = False
    try:
        run_pool(job, monthly, results)
        for archive, rec in list(results):
            onboard = onboard_ms.get(archive.symbol)
            if rec["status"] == "unavailable" and not (onboard is not None and archive.end_ms < onboard):
                y, m = (int(x) for x in archive.period.split("-"))
                days_ = daily_archives_for_month(archive.symbol, y, m, job.range_start_ms, job.range_end_ms,
                                                 fallback_for=archive.period, not_before_ms=onboard)
                rec["notes"].append(f"monthly archive unavailable; falling back to {len(days_)} official daily archive(s)")
                log.warning("%s: monthly archive unavailable -> falling back to %d daily archives", archive.zip_name, len(days_))
                fallback.extend(days_)
        run_pool(job, daily + fallback, results)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        report = build_report(job, started, symbols, validation, planned, results, len(fallback), interrupted, previous)
        write_report(report_path, report)

    # 6. final summary
    log.info("-" * 100)
    log.info("run summary: %s%s", report["run_summary"], "  (INTERRUPTED)" if interrupted else "")
    for sym in symbols:
        c = report["coverage_summary"][sym]
        if not c["validated"]:
            log.error("%-9s NOT VALIDATED: %s", sym, c["reason"])
            continue
        log.info("%-9s coverage %8.4f%% (%d/%d minutes) statuses=%s missing=%d dup=%d ohlc=%d unavailable=%s failed=%s quarantined=%s",
                 sym, c["coverage_pct"], c["covered_minute_count"], c["expected_minute_count"], c["archives_by_status"],
                 c["missing_minute_count"], c["duplicate_minute_count_total"], c["ohlc_violation_count_total"],
                 c["unavailable_periods"], c["failed_periods"], c["quarantined_periods"])
    ext, ds, inv = report["incremental_extension"], report["dataset_summary"], report["on_disk_inventory"]
    if ext is not None:
        log.info("extension: %s -> %s, %d new archives expected, statuses=%s, new rows=%d", ext["previous_last_open_time_utc"],
                 ext["new_last_open_time_utc"], ext["expected_new_archives"], ext["new_archives_by_status"], ext["new_row_count"])
    log.info("dataset: %d verified archives, %d rows, %s .. %s, missing=%d dup=%d out_of_order=%d ohlc=%d",
             ds["verified_archives"], ds["total_row_count"], ds["first_open_time_utc"], ds["last_open_time_utc"],
             ds["total_missing_minute_count"], ds["total_duplicate_minute_count"], ds["total_out_of_order_count"],
             ds["total_ohlc_violation_count"])
    log.info("on disk: %d ZIP (%d monthly + %d daily), %d .CHECKSUM, %d .part in raw/, interval dirs=%s, quarantine files=%d",
             inv["zip_files_total"], inv["monthly_zip_files"], inv["daily_zip_files"], inv["checksum_files_total"],
             inv["part_files_in_raw"], inv["interval_directories"], inv["quarantine_files"])
    log.info("report: %s", report_path)
    log.info("log:    %s", root / "logs" / "download.log")
    bad = sum(report["run_summary"].get(s, 0) for s in ("failed", "quarantined"))
    exit_code = 1 if (bad or interrupted or len(valid_symbols) != len(symbols)) else 0
    log.info("run %s finished with exit code %d", run_id, exit_code)
    return exit_code


def main() -> int:
    return run(DATA_ROOT, SYMBOLS, RANGE_START_UTC, RANGE_END_UTC)


if __name__ == "__main__":
    sys.exit(main())
