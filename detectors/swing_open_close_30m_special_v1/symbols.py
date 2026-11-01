"""Immutable USD-M perpetual profiles for the shared 30-minute special swing detector."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from detectors.swing_open_close_30m_special_v1.config import (
    ALGORITHM_VERSION,
    DETECTOR_VERSION,
    PROJECT_ROOT,
    USER_PARAMETERS,
)

SOURCE_ROOT = Path(r"C:\MarketData\derived\binance\futures\um\perpetual\30m")
APPROVED_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "AVAXUSDT",
    "AAVEUSDT",
    "NEARUSDT",
    "LINKUSDT",
    "LTCUSDT",
    "OPUSDT",
    "DOTUSDT",
)


class SymbolSelectionError(ValueError):
    """Raised when a symbol is outside the approved nine-symbol registry."""


@dataclass(frozen=True, slots=True)
class SymbolProfile:
    symbol: str
    tradingview_symbol: str
    provider: str
    market: str
    contract_type: str
    timeframe: str
    source_dir: Path
    workbook_prefix: str
    report_filename: str
    log_filename: str

    @property
    def report_path(self) -> Path:
        return PROJECT_ROOT / "reports" / self.report_filename

    @property
    def log_path(self) -> Path:
        return PROJECT_ROOT / "logs" / self.log_filename


def _profile(symbol: str) -> SymbolProfile:
    return SymbolProfile(
        symbol=symbol,
        tradingview_symbol=f"BINANCE:{symbol}.P",
        provider="Binance",
        market="Binance USD-M futures/um",
        contract_type="USDT-margined perpetual",
        timeframe="30m",
        source_dir=SOURCE_ROOT / f"symbol={symbol}",
        workbook_prefix=f"{symbol}_30M_Special_Swings",
        report_filename=f"special_30m_swing_{symbol}_report.json",
        log_filename=f"special_30m_swing_{symbol}.log",
    )


PROFILES: tuple[SymbolProfile, ...] = tuple(_profile(symbol) for symbol in APPROVED_SYMBOLS)
PROFILE_BY_SYMBOL = {profile.symbol: profile for profile in PROFILES}


def require_profile(symbol: str) -> SymbolProfile:
    """Return one approved profile. Unknown text and path-like symbols are rejected."""
    if type(symbol) is not str or symbol not in PROFILE_BY_SYMBOL:
        raise SymbolSelectionError(f"unsupported symbol {symbol!r}")
    if any(part in symbol for part in ("/", "\\", "..", ":")):
        raise SymbolSelectionError(f"unsupported symbol {symbol!r}")
    return PROFILE_BY_SYMBOL[symbol]


def parameters_for(profile: SymbolProfile, revision: int) -> tuple[tuple[str, str], ...]:
    """Copy the shared parameter contract and record the symbol that was analyzed."""
    detector_version = DETECTOR_VERSION if profile.symbol == "BTCUSDT" else ALGORITHM_VERSION
    overrides = {
        "Symbol": profile.symbol,
        "TradingView Symbol": profile.tradingview_symbol,
        "Detector Version": detector_version,
    }
    rows = [(name, overrides.get(name, value)) for name, value in USER_PARAMETERS]
    rows.extend((
        ("Provider", profile.provider),
        ("Contract Type", profile.contract_type),
        ("Algorithm Identity", ALGORITHM_VERSION),
        ("Legacy BTC Detector Identity", DETECTOR_VERSION),
        ("Source Directory", str(profile.source_dir)),
        ("Workbook Revision", f"rev{revision:02d}"),
    ))
    return tuple(rows)
