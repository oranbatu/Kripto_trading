"""30m mapping parameters. These do not change 4h detection."""
from __future__ import annotations

MAPPING_VERSION = "BTCUSDT_4H_WITH_30M_MAPPING_V1"
MAPPING_CONFIGURATION_VERSION = "BTCUSDT_4H_WITH_30M_CONFIG_V1"
CHILD_TIMEFRAME = "30m"
CHILDREN_PER_PARENT = 8
CHILD_MS = 1_800_000
CHILD_DATASET = r"C:\MarketData\derived\binance\futures\um\perpetual\30m\symbol=BTCUSDT"
EXPECTED_CHILD_ROWS = 12384
EXPECTED_CHILD_MONTHLY = {
    (2026, 1): 1488,
    (2026, 2): 1344,
    (2026, 3): 1488,
    (2026, 4): 1440,
    (2026, 5): 1488,
    (2026, 6): 1440,
    (2026, 7): 1488,
    (2026, 8): 1488,
    (2026, 9): 720,
}
CHILD_END_OPEN_UTC = "2026-09-15T23:30:00Z"
STRUCTURE_PATTERN = r"^BTCUSDT_4H_Swing_Structure_rev(\d{2,})\.xlsx$"
MAPPING_PATTERN = r"^BTCUSDT_4H_Swings_on_30M_rev(\d{2,})\.xlsx$"
NOT_OBSERVED = "DIRECT_30M_SWING_CLOSE_NOT_OBSERVED"
FOUND = "DIRECT_30M_SWING_CLOSE_FOUND"
MAPPED = "MAPPED_30M_COMPLETE"
MAP_SHEETS = (
    "Executive Summary",
    "4H Swing Index",
    "30M Swing Index",
    "30M Annotated Candles",
    "Swing Boundaries",
    "4H to 30M Mapping",
    "Extreme Matches",
    "Parent Aggregation",
    "Overlap Analysis",
    "Parameters",
    "Diagnostics",
    "README",
)

PROTECTED_DETECTOR = r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1"
PROTECTED_FINGERPRINTS = {
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\README.md": ("f6e255f98ca8e3906cbd8e1eade700c03de1e41f5a549af51f76f218192c5a85", 2931, 1790965500297812500),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\__init__.py": ("779def3359868fe2870ad2bb3826078e362366326187ac890e6090eeccdfb142", 50, 1790964306549849200),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\engine.py": ("94c0e3620e000512066037621c80edaca5ccec181fd797f427fde5e4fd367ecf", 75124, 1790965499175151700),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\run_swing_open_close_4h_detector.py": ("872692b6d7816ca33dd121c233ffd17f394ddb687dbd4f60e205867025d7f156", 32968, 1790965519108579500),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\swing_config.py": ("bb5760659c9c3b67fb356b09d4530ea9218490764f942fffee9704a25420c8d3", 34055, 1790965515451132500),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\workbook.py": ("670d866a072e9e3d2e19fd158a57c87f502107c1c8071d389f6bb21931b5f6cb", 39586, 1790965514801446300),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\tests\__init__.py": ("63bd8929c6b6890971304dacd0724bf79fad5a0398d3c1a2ce2b3625fe4838ef", 56, 1790964306510199500),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\tests\test_swing_open_close_4h_detector.py": ("84b169ad3e58599f838c0c344d75dfb7cde956852be9e83cd10253bb15cadc47", 57405, 1790965542447129500),
}


def mapping_parameter_rows() -> list[tuple]:
    hard = "HARD_RULE"
    rows = [
        ("Mapping", "authoritative_timeframe", "4h", "string", "timeframe", hard, "4h", "Swings are detected only on 4h.", "mapping_config", "mapping"),
        ("Mapping", "mapping_timeframe", CHILD_TIMEFRAME, "string", "timeframe", hard, "30m", "30m decomposes the detected 4h swings.", "mapping_config", "mapping"),
        ("Mapping", "independent_30m_swing_detection_enabled", "FALSE", "boolean", "flag", hard, "FALSE", "30m candles do not create swings.", "mapping_config", "mapping"),
        ("Mapping", "30m_candles_per_4h_parent", CHILDREN_PER_PARENT, "integer", "candles", hard, "8", "Each 4h candle contains eight 30m candles.", "mapping_config", "alignment"),
        ("Mapping", "30m_mapping_uses_primary_4h_swings_only", "TRUE", "boolean", "flag", hard, "PRIMARY_SWING", "Derived same-extreme rows are not principal mappings.", "mapping_config", "mapping"),
        ("Mapping", "30m_reference_price_source", "ORIGINAL_4H_SWING_OPEN_OPEN", "enum", "price", hard, "4h open", "Reference stays the 4h Swing Open open.", "mapping_config", "open"),
        ("Mapping", "30m_extreme_price_source", "ORIGINAL_4H_EXTREME_PRICE", "enum", "price", hard, "4h extreme", "Extreme price is not recomputed from 30m.", "mapping_config", "extreme"),
        ("Mapping", "30m_extreme_match_comparison", "EXACT_DECIMAL_EQUALITY", "enum", "price", hard, "Decimal equality", "No nearest-price matching.", "mapping_config", "extreme"),
        ("Mapping", "30m_extreme_representative", "LAST_CHRONOLOGICAL_EXACT_MATCH", "enum", "candle", hard, "last exact match", "Representative child of the 4h extreme event.", "mapping_config", "extreme"),
        ("Mapping", "30m_direct_close_search_starts_after_extreme", "TRUE", "boolean", "flag", hard, "TRUE", "Search starts after the representative extreme close.", "mapping_config", "close"),
        ("Mapping", "30m_direct_close_requires_completed_close", "TRUE", "boolean", "flag", hard, "close", "Wick contact does not pass.", "mapping_config", "close"),
        ("Mapping", "30m_wick_only_return_passes", "FALSE", "boolean", "flag", hard, "FALSE", "A wick without a qualifying close fails.", "mapping_config", "close"),
        ("Mapping", "30m_first_qualifying_close_is_binding", "TRUE", "boolean", "flag", hard, "TRUE", "The first passing 30m close is the direct close.", "mapping_config", "close"),
        ("Mapping", "30m_search_outside_original_4h_window", "FALSE", "boolean", "flag", hard, "FALSE", "Search ends at the official 4h swing close.", "mapping_config", "close"),
        ("Mapping", "30m_can_change_4h_detection", "FALSE", "boolean", "flag", hard, "FALSE", "30m detail cannot change a 4h decision.", "mapping_config", "causality"),
        ("Mapping", "30m_can_backdate_4h_confirmation", "FALSE", "boolean", "flag", hard, "FALSE", "Official confirmation stays the 4h close time.", "mapping_config", "causality"),
        ("Mapping", "official_confirmation_timeframe", "4h", "string", "timeframe", hard, "4h", "Official confirmation remains 4h.", "mapping_config", "causality"),
        ("Mapping", "timezone_conversion", 'ZoneInfo("Europe/Istanbul")', "string", "timezone", hard, "ZoneInfo", "Turkey display conversion.", "mapping_config", "time"),
        ("Mapping", "mapping_version", MAPPING_VERSION, "string", "version", hard, MAPPING_VERSION, "Mapping implementation version.", "mapping_config", "identity"),
        ("Mapping", "mapping_configuration_version", MAPPING_CONFIGURATION_VERSION, "string", "version", hard, MAPPING_CONFIGURATION_VERSION, "Mapping configuration version.", "mapping_config", "identity"),
        ("Mapping", "expected_30m_rows", EXPECTED_CHILD_ROWS, "integer", "rows", hard, "12384", "Required 30m row count.", "mapping_config", "validation"),
        ("Mapping", "30m_analysis_end_utc", CHILD_END_OPEN_UTC, "timestamp", "UTC", hard, "inclusive open", "Last mapped 30m open.", "mapping_config", "validation"),
    ]
    return rows
