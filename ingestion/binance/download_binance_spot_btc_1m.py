#!/usr/bin/env python3
"""Download and verify official Binance Spot 1-minute archives for ten BTC pairs.

The symbol list, interval, and archive plan are fixed. A missing archive, checksum
mismatch, or invalid ZIP is a hard failure. Nothing under the futures tree is read
or written. Importing this module does not download or publish files.
"""
from __future__ import annotations

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from ingestion.binance.spot_btc_pairs import (
    SPOT_SYMBOLS,
    TASK_NAME,
    Archive,
    TaskLock,
    assert_under,
    file_signature,
    assert_complete_minutes,
    iter_csv_rows,
    parse_checksum_bytes,
    plan_archives,
    require_spot_symbols,
    sha256_file,
    validate_zip_members,
)

DATA_ROOT = Path(r"C:\MarketData")
REPORT_PATH = DATA_ROOT / "reports" / "binance_spot_btc_pairs_1m_download_report.json"
LOG_PATH = DATA_ROOT / "logs" / "binance_spot_btc_pairs_1m_download.log"
EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
USER_AGENT = "binance-spot-btc-1m-archive-downloader/1.0"
MAX_HTTP_ATTEMPTS = 6
BACKOFF_BASE_S = 1.0
BACKOFF_CAP_S = 60.0
CONNECT_TIMEOUT_S = 20.0
READ_TIMEOUT_S = 120.0
EST_MONTHLY_BYTES = 8 * 1024 * 1024
EST_DAILY_BYTES = 512 * 1024
DISK_MARGIN_BYTES = 2 * 1024 * 1024 * 1024
SSL_CONTEXT = ssl.create_default_context()

log = logging.getLogger("binance_spot_btc_1m")


class TransientHttpError(Exception):
    pass


class HttpError(Exception):
    pass


class Http:
    def _open(self, method: str, url: str):
        parts = urllib.parse.urlsplit(url)
        if parts.scheme != "https" or not parts.hostname:
            raise HttpError(f"refusing non-https URL {url!r}")
        conn = http.client.HTTPSConnection(
            parts.hostname, parts.port or 443, timeout=CONNECT_TIMEOUT_S, context=SSL_CONTEXT,
        )
        try:
            conn.connect()
            conn.sock.settimeout(READ_TIMEOUT_S)
            path = parts.path + (f"?{parts.query}" if parts.query else "")
            conn.request(method, path, headers={
                "User-Agent": USER_AGENT,
                "Accept": "*/*",
                "Accept-Encoding": "identity",
                "Connection": "close",
            })
            response = conn.getresponse()
        except (OSError, http.client.HTTPException) as exc:
            conn.close()
            raise TransientHttpError(f"{type(exc).__name__}: {exc}") from exc
        if response.status in (200, 404):
            return conn, response
        status, reason = response.status, response.reason
        conn.close()
        if status == 429 or 500 <= status <= 599:
            raise TransientHttpError(f"HTTP {status} {reason}")
        raise HttpError(f"HTTP {status} {reason} for {method} {url}")

    def _retry(self, fn, what: str):
        last: Optional[Exception] = None
        for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
            try:
                return fn()
            except TransientHttpError as exc:
                last = exc
                if attempt == MAX_HTTP_ATTEMPTS:
                    break
                delay = min(BACKOFF_CAP_S, BACKOFF_BASE_S * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                log.warning("transient failure on %s attempt %d/%d: %s", what, attempt, MAX_HTTP_ATTEMPTS, exc)
                time.sleep(delay)
        raise TransientHttpError(f"gave up on {what}: {last}")

    def head_status(self, url: str) -> int:
        def attempt():
            conn, response = self._open("HEAD", url)
            try:
                return response.status
            finally:
                conn.close()
        return self._retry(attempt, url)

    def get_bytes(self, url: str, limit: int = 4 * 1024 * 1024) -> tuple[int, bytes]:
        def attempt():
            conn, response = self._open("GET", url)
            try:
                if response.status == 404:
                    return 404, b""
                body = response.read(limit + 1)
                if len(body) > limit:
                    raise HttpError(f"response exceeds {limit} bytes for {url}")
                return 200, body
            finally:
                conn.close()
        return self._retry(attempt, url)

    def download(self, url: str, dest: Path) -> tuple[int, int]:
        def attempt():
            if dest.exists():
                dest.unlink()
            conn, response = self._open("GET", url)
            try:
                if response.status == 404:
                    return 404, 0
                declared = response.getheader("Content-Length")
                received = 0
                dest.parent.mkdir(parents=True, exist_ok=True)
                with dest.open("wb") as handle:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        handle.write(chunk)
                        received += len(chunk)
                    handle.flush()
                    os.fsync(handle.fileno())
                if declared and declared.strip().isdigit() and int(declared) != received:
                    raise TransientHttpError(f"short read {received} of {declared}")
                return 200, received
            finally:
                conn.close()
        return self._retry(attempt, url)


def _setup_logging(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.INFO)
    log.handlers.clear()
    log.propagate = False
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    log.addHandler(file_handler)
    log.addHandler(stream)


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _exchange_symbol(http: Http, symbol: str) -> dict:
    code, body = http.get_bytes(f"{EXCHANGE_INFO_URL}?symbol={symbol}")
    if code != 200:
        raise RuntimeError(f"exchangeInfo for {symbol} returned HTTP {code}")
    payload = json.loads(body)
    symbols = payload.get("symbols") or []
    if len(symbols) != 1:
        raise RuntimeError(f"exchangeInfo did not return exactly one record for {symbol}")
    item = symbols[0]
    if item.get("symbol") != symbol or item.get("baseAsset") != symbol[:-3] or item.get("quoteAsset") != "BTC":
        raise RuntimeError(f"exchangeInfo identity mismatch for {symbol}: {item!r}")
    if str(item.get("status", "")).upper() == "PENDING_TRADING":
        raise RuntimeError(f"{symbol} is not a listed Spot symbol")
    return {"symbol": symbol, "baseAsset": item["baseAsset"], "quoteAsset": item["quoteAsset"], "status": item.get("status")}


def _disk_estimate(archives: list[Archive]) -> int:
    monthly = sum(1 for item in archives if item.kind == "monthly")
    daily = sum(1 for item in archives if item.kind == "daily")
    return monthly * EST_MONTHLY_BYTES + daily * EST_DAILY_BYTES + DISK_MARGIN_BYTES


def preflight(http: Http, root: Path, archives: list[Archive]) -> dict:
    symbols = require_spot_symbols(SPOT_SYMBOLS)
    identities = [_exchange_symbol(http, symbol) for symbol in symbols]
    raw_root = (root / "raw" / "binance" / "spot").resolve()
    processed_root = (root / "processed" / "binance" / "spot").resolve()
    for archive in archives:
        assert_under(archive.zip_path(root), raw_root)
        assert_under(archive.checksum_path(root), raw_root)
        if "/futures/" in archive.zip_url or "futures" in archive.zip_url.split("/"):
            raise RuntimeError(f"futures URL in Spot plan: {archive.zip_url}")
    usage = shutil.disk_usage(root if root.exists() else root.anchor)
    needed = _disk_estimate(archives)
    if usage.free < needed:
        raise RuntimeError(f"insufficient disk space: free {usage.free} required {needed}")
    def probe(archive: Archive) -> dict:
        client = Http()
        zip_status = client.head_status(archive.zip_url)
        checksum_status = client.head_status(archive.checksum_url)
        return {
            "symbol": archive.symbol,
            "kind": archive.kind,
            "period": archive.period,
            "zip_url": archive.zip_url,
            "zip_status": zip_status,
            "checksum_status": checksum_status,
        }

    matrix = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        for index, row in enumerate(pool.map(probe, archives), 1):
            matrix.append(row)
            if index % 100 == 0:
                log.info("availability %d/%d", index, len(archives))
    unavailable = [row for row in matrix if row["zip_status"] != 200 or row["checksum_status"] != 200]
    return {
        "symbols": list(symbols),
        "exchange_info": identities,
        "archive_count": len(archives),
        "disk_free_bytes": usage.free,
        "disk_required_bytes": needed,
        "unavailable": unavailable,
        "availability": matrix,
    }


def _validate_downloaded_archive(path: Path, archive: Archive) -> None:
    validate_zip_members(path, archive)
    opens = []
    with zipfile.ZipFile(path) as handle:
        with handle.open(archive.csv_name) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            for row in iter_csv_rows(text, archive.timestamp_unit):
                opens.append(row["open_ms"])
    assert_complete_minutes(opens, archive.start_ms, archive.end_ms, archive.zip_name)


def _publish_verified(temp_path: Path, final_path: Path, quarantine: Path) -> str:
    """Move a verified temp file, or skip when the destination bytes already match."""
    final_path.parent.mkdir(parents=True, exist_ok=True)
    if not final_path.exists():
        os.replace(temp_path, final_path)
        return "downloaded"
    before = file_signature(final_path)
    if before["sha256"] == sha256_file(temp_path):
        temp_path.unlink()
        after = file_signature(final_path)
        if after["size"] != before["size"] or after["mtime_ns"] != before["mtime_ns"] or after["sha256"] != before["sha256"]:
            raise RuntimeError(f"equivalent skip changed {final_path}")
        return "skipped-equivalent"
    quarantine.mkdir(parents=True, exist_ok=True)
    dest = quarantine / f"{final_path.name}.{sha256_file(temp_path)[:12]}"
    os.replace(temp_path, dest)
    raise RuntimeError(
        f"conflict: existing {final_path} sha256 {before['sha256']} differs; candidate quarantined at {dest}"
    )


def _write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(path.suffix + ".part")
    part.write_text(json.dumps(report, indent=2), encoding="utf-8")
    os.replace(part, path)


def _load_report(path: Path) -> dict:
    if not path.exists():
        return {"task": TASK_NAME, "runs": []}
    return json.loads(path.read_text(encoding="utf-8"))


def run(root: Path = DATA_ROOT, report_path: Path = REPORT_PATH, log_path: Path = LOG_PATH) -> int:
    require_spot_symbols(SPOT_SYMBOLS)
    run_id = _run_id()
    _setup_logging(log_path)
    lock = TaskLock(root / "tmp" / "locks" / f"{TASK_NAME}.lock")
    work = root / "tmp" / f"{TASK_NAME}_{run_id}"
    quarantine = root / "tmp" / f"{TASK_NAME}_quarantine_{run_id}"
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    archives = plan_archives()
    http = Http()
    current = {
        "run_id": run_id,
        "mode": "DOWNLOAD",
        "started": started,
        "symbols": list(SPOT_SYMBOLS),
        "excluded_symbols": ["BTCBTC", "AAVEBTC", "OPBTC"],
        "downloaded_zip_archives": 0,
        "downloaded_checksum_files": 0,
        "skipped_equivalent_zip_archives": 0,
        "skipped_equivalent_checksum_files": 0,
        "unavailable": 0,
        "failed": 0,
        "quarantined": 0,
    }
    try:
        lock.acquire()
        log.info("preflight %d archives for %s", len(archives), ",".join(SPOT_SYMBOLS))
        checked = preflight(http, root, archives)
        current["exchange_info"] = checked["exchange_info"]
        current["disk_free_bytes"] = checked["disk_free_bytes"]
        if checked["unavailable"]:
            current["mode"] = "PREFLIGHT_UNAVAILABLE"
            current["unavailable"] = len(checked["unavailable"])
            current["unavailable_files"] = checked["unavailable"]
            log.error("preflight found %d unavailable archives; no production files written", len(checked["unavailable"]))
            _store_run(report_path, current, archives, [])
            return 2
        def fetch_one(archive: Archive) -> None:
            client = Http()
            zip_tmp = work / archive.kind / archive.zip_name
            chk_tmp = work / archive.kind / (archive.zip_name + ".CHECKSUM")
            code, body = client.get_bytes(archive.checksum_url)
            if code != 200:
                raise RuntimeError(f"checksum unavailable after preflight: {archive.checksum_url} HTTP {code}")
            digest = parse_checksum_bytes(body, archive.zip_name)
            chk_tmp.parent.mkdir(parents=True, exist_ok=True)
            chk_part = chk_tmp.with_name(chk_tmp.name + ".part")
            chk_part.write_bytes(body)
            os.replace(chk_part, chk_tmp)
            part = zip_tmp.with_name(zip_tmp.name + ".part")
            code, _size = client.download(archive.zip_url, part)
            if code != 200:
                raise RuntimeError(f"ZIP unavailable after preflight: {archive.zip_url} HTTP {code}")
            os.replace(part, zip_tmp)
            actual = sha256_file(zip_tmp)
            if actual != digest:
                raise RuntimeError(f"checksum mismatch for {archive.zip_name}: official {digest} actual {actual}")
            _validate_downloaded_archive(zip_tmp, archive)
            log.info("verified %s sha256=%s unit=%s", archive.zip_name, actual, archive.timestamp_unit)

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(fetch_one, archive) for archive in archives]
            finished = 0
            try:
                for future in as_completed(futures):
                    future.result()
                    finished += 1
                    if finished % 25 == 0:
                        log.info("verified archives %d/%d", finished, len(archives))
            except Exception:
                for future in futures:
                    future.cancel()
                raise
        conflicts = []
        for archive in archives:
            for temp_path, final_path in (
                (work / archive.kind / archive.zip_name, archive.zip_path(root)),
                (work / archive.kind / (archive.zip_name + ".CHECKSUM"), archive.checksum_path(root)),
            ):
                if final_path.exists() and sha256_file(final_path) != sha256_file(temp_path):
                    conflicts.append(str(final_path))
        if conflicts:
            quarantine.mkdir(parents=True, exist_ok=True)
            for archive in archives:
                for name in (archive.zip_name, archive.zip_name + ".CHECKSUM"):
                    source = work / archive.kind / name
                    if source.exists():
                        os.replace(source, quarantine / name)
            current["quarantined"] = len(conflicts)
            raise RuntimeError("existing Spot files differ from official bytes: " + "; ".join(conflicts[:10]))
        published = []
        for archive in archives:
            zip_tmp = work / archive.kind / archive.zip_name
            chk_tmp = work / archive.kind / (archive.zip_name + ".CHECKSUM")
            zip_status = _publish_verified(zip_tmp, archive.zip_path(root), quarantine)
            chk_status = _publish_verified(chk_tmp, archive.checksum_path(root), quarantine)
            published.append({"symbol": archive.symbol, "period": archive.period, "zip": zip_status, "checksum": chk_status})
            current["downloaded_zip_archives" if zip_status == "downloaded" else "skipped_equivalent_zip_archives"] += 1
            current["downloaded_checksum_files" if chk_status == "downloaded" else "skipped_equivalent_checksum_files"] += 1
        current["finished"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        current["published"] = published
        _store_run(report_path, current, archives, published)
        log.info(
            "download complete downloaded_zips=%d skipped_zips=%d skipped_checksums=%d",
            current["downloaded_zip_archives"],
            current["skipped_equivalent_zip_archives"],
            current["skipped_equivalent_checksum_files"],
        )
        return 0
    except Exception as exc:
        current["mode"] = "FAILED"
        current["failed"] = 1
        current["error"] = str(exc)
        current["finished"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if quarantine.exists():
            current["quarantined"] = sum(1 for _ in quarantine.iterdir())
        log.error("download failed: %s", exc)
        _store_run(report_path, current, archives, [])
        return 1
    finally:
        lock.release()
        if work.exists():
            shutil.rmtree(work, ignore_errors=True)
        if current.get("mode") != "FAILED" and quarantine.exists():
            shutil.rmtree(quarantine, ignore_errors=True)


def _store_run(path: Path, current: dict, archives: list[Archive], published: list[dict]) -> None:
    previous = _load_report(path)
    runs = list(previous.get("runs") or [])
    runs.append({k: v for k, v in current.items() if k != "published"})
    by_symbol: dict[str, int] = {}
    for archive in archives:
        by_symbol[archive.symbol] = by_symbol.get(archive.symbol, 0) + 1
    report = {
        "task": TASK_NAME,
        "market": "spot",
        "interval": "1m",
        "quote_asset": "BTC",
        "symbols": list(SPOT_SYMBOLS),
        "excluded_symbols": ["BTCBTC", "AAVEBTC", "OPBTC"],
        "current_run": {k: v for k, v in current.items() if k != "published"},
        "final_inventory": {
            "planned_zip_archives": len(archives),
            "planned_checksum_files": len(archives),
            "archives_per_symbol": by_symbol,
            "monthly_per_symbol": 33,
            "daily_per_symbol": 27,
            "october_2026_monthly_archives": 0,
        },
        "published_this_run": published,
        "runs": runs,
    }
    _write_report(path, report)


def main() -> int:
    if len(sys.argv) > 1:
        raise SystemExit("this Spot downloader accepts no symbol arguments")
    return run()


if __name__ == "__main__":
    sys.exit(main())
