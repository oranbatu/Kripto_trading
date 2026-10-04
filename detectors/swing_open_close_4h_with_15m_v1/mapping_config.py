"""15m mapping parameters. These do not change 4h detection."""
from __future__ import annotations

MAPPING_VERSION = "BTCUSDT_4H_WITH_15M_MAPPING_V1"
MAPPING_CONFIGURATION_VERSION = "BTCUSDT_4H_WITH_15M_CONFIG_V1"
CHILD_TIMEFRAME = "15m"
CHILDREN_PER_PARENT = 16
CHILD_MS = 900_000
CHILD_DATASET = r"C:\MarketData\derived\binance\futures\um\perpetual\15m\symbol=BTCUSDT"
EXPECTED_CHILD_ROWS = 24768
EXPECTED_CHILD_MONTHLY = {
    (2026, 1): 2976,
    (2026, 2): 2688,
    (2026, 3): 2976,
    (2026, 4): 2880,
    (2026, 5): 2976,
    (2026, 6): 2880,
    (2026, 7): 2976,
    (2026, 8): 2976,
    (2026, 9): 1440,
}
CHILD_END_OPEN_UTC = "2026-09-15T23:45:00Z"
STRUCTURE_PATTERN = r"^BTCUSDT_4H_Swing_Structure_rev(\d{2,})\.xlsx$"
MAPPING_PATTERN = r"^BTCUSDT_4H_Swings_on_15M_rev(\d{2,})\.xlsx$"
NOT_OBSERVED = "DIRECT_15M_SWING_CLOSE_NOT_OBSERVED"
FOUND = "DIRECT_15M_SWING_CLOSE_FOUND"
MAPPED = "MAPPED_15M_COMPLETE"
MAP_SHEETS = (
    "Executive Summary",
    "4H Swing Index",
    "15M Swing Index",
    "15M Annotated Candles",
    "Swing Boundaries",
    "4H to 15M Mapping",
    "Extreme Matches",
    "Parent Aggregation",
    "Overlap Analysis",
    "Parameters",
    "Diagnostics",
    "README",
)

PROTECTED_DETECTOR = r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1"
PROTECTED_30M_DETECTOR = r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1"
PROTECTED_1H_DETECTOR = r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1"
PROTECTED_FINGERPRINTS = {
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\README.md": ("f6e255f98ca8e3906cbd8e1eade700c03de1e41f5a549af51f76f218192c5a85", 2931, 1790965500297812500),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\__init__.py": ("779def3359868fe2870ad2bb3826078e362366326187ac890e6090eeccdfb142", 50, 1790964306549849200),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\engine.py": ("94c0e3620e000512066037621c80edaca5ccec181fd797f427fde5e4fd367ecf", 75124, 1790965499175151700),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\run_swing_open_close_4h_detector.py": ("872692b6d7816ca33dd121c233ffd17f394ddb687dbd4f60e205867025d7f156", 32968, 1790965519108579500),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\swing_config.py": ("bb5760659c9c3b67fb356b09d4530ea9218490764f942fffee9704a25420c8d3", 34055, 1790965515451132500),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\workbook.py": ("670d866a072e9e3d2e19fd158a57c87f502107c1c8071d389f6bb21931b5f6cb", 39586, 1790965514801446300),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\tests\__init__.py": ("63bd8929c6b6890971304dacd0724bf79fad5a0398d3c1a2ce2b3625fe4838ef", 56, 1790964306510199500),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_v1\tests\test_swing_open_close_4h_detector.py": ("84b169ad3e58599f838c0c344d75dfb7cde956852be9e83cd10253bb15cadc47", 57405, 1790965542447129500),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\README.md": ("2e0716d3599ae1a56bf31a6d3c52a595b8be5cf476b1ff9aea6b12644366aa70", 1273, 1791121208284188100),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\__init__.py": ("f7ad1ea3da9f1179a277bbaa7add952ceb71556cdb36d8febe01b752bae7b6f4", 120, 1791121045426990200),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\engine.py": ("01867c3c7b3e135663aa3f2478a195d12a7e4f1d21e4b57bccae0969b56d6d3b", 75133, 1791120992465338200),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\map_workbook.py": ("1eecefa90b4a47ecc0dc4290763f138948964cf4e396516a06106e349b390406", 27722, 1791121114155900700),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\mapping.py": ("2ea9c58aa10340942bde40d102923728f9ea8942cb201d3feac96f8c3e900f32", 17967, 1791121259408781900),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\mapping_config.py": ("4ee0762a96f2ef2c4ab0d2598d2eeb028e11590cb94931b21858d52c5f147a11", 7000, 1791121053759611400),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\run_swing_open_close_4h_with_30m_detector.py": ("011681007fbe71207f44863f544027333b04bbf16c8064a4dc38ee1e57e8fa86", 43350, 1791121170662905300),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\swing_config.py": ("f4e3adeefcf17cc22f594e0d49c3c1a881c742bce706df39ece138383ee1a3b4", 34082, 1791120992466347100),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\workbook.py": ("eabaa1042c9306a50a9e8ccb6e4d8983ae0223cd60e776bf64b8f1f26a132763", 39613, 1791120992466347100),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\tests\__init__.py": ("63bd8929c6b6890971304dacd0724bf79fad5a0398d3c1a2ce2b3625fe4838ef", 56, 1791120992467580600),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\tests\test_30m_mapping.py": ("3b6555031222da9b03b24d3f222e527925c8fe79fb8b97047bd7b7325cea553d", 11462, 1791121285876189500),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_30m_v1\tests\test_swing_open_close_4h_detector.py": ("52d83c03f9887cc4b83c8df950729d3e4acb8e634c553efdbcc85947b9c9c226", 57477, 1791121018778461900),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\README.md": ("66cfdd8c9bf3108bb2140ada39e97b5ce818f23249ad0ae4a9914fcf356229bf", 1831, 1791124622624289200),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\__init__.py": ("db8d7a19bec32aaf1cfec618ef8498571a0e69bcbee5fd61d3d16db3a428c25d", 68, 1791124622746302400),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\engine.py": ("c7547e9a59b0d3875fac867247e81b3e13676005d7b5805f83d7794911b799cd", 75132, 1791124344362446600),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\map_workbook.py": ("a5c083d6a297be92d725349d3d11cdb968203ea5859f36e3d8b6502e51ec31ba", 30430, 1791124609587887700),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\mapping.py": ("78b1e6c39886e91e49bff22e3e7a7244b356fd0db67444b78198e074d2187505", 18588, 1791124594404405700),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\mapping_config.py": ("aab2a1da65e88e4532fb4308c7b504f712f7fa6c2e0491e2e905b05900097a7c", 9725, 1791124443705358400),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\run_swing_open_close_4h_with_1h_detector.py": ("9c997c9acaf27605709ceec2ca997686c1c994035d83d91c858033e3dcf18f57", 43925, 1791124612468480200),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\swing_config.py": ("f75c353f2a345cf79e5bd9bd66ce000bc33e13cc910de7239f86920e25442a35", 34079, 1791124344367452600),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\workbook.py": ("8c3587b39a65e84e8ee6c76f5900811b98d907416c68f5db74aecb5729ebf4bc", 39610, 1791124344368452300),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\tests\__init__.py": ("63bd8929c6b6890971304dacd0724bf79fad5a0398d3c1a2ce2b3625fe4838ef", 56, 1791124344370451200),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\tests\test_1h_mapping.py": ("fe0226cafebed4d78d89c7e30c09d031d8d46bf7a2ad84238093c7a683f602f5", 13859, 1791124640581034800),
    r"C:\Users\oranb\Desktop\backtest_system\detectors\swing_open_close_4h_with_1h_v1\tests\test_swing_open_close_4h_detector.py": ("5f5fd1888c18f31058c8e8562610b9fd1b90692d51af00b926d7086487ad9ca0", 57469, 1791124623148674200),
}


def mapping_parameter_rows() -> list[tuple]:
    hard = "HARD_RULE"
    rows = [
        ("Mapping", "authoritative_timeframe", "4h", "string", "timeframe", hard, "4h", "Swings are detected only on 4h.", "mapping_config", "mapping"),
        ("Mapping", "mapping_timeframe", CHILD_TIMEFRAME, "string", "timeframe", hard, "15m", "15m decomposes the detected 4h swings.", "mapping_config", "mapping"),
        ("Mapping", "independent_15m_swing_detection_enabled", "FALSE", "boolean", "flag", hard, "FALSE", "15m candles do not create swings.", "mapping_config", "mapping"),
        ("Mapping", "15m_candles_per_4h_parent", CHILDREN_PER_PARENT, "integer", "candles", hard, "16", "Each 4h candle contains sixteen 15m candles.", "mapping_config", "alignment"),
        ("Mapping", "15m_mapping_uses_primary_4h_swings_only", "TRUE", "boolean", "flag", hard, "PRIMARY_SWING", "Derived same-extreme rows are not principal mappings.", "mapping_config", "mapping"),
        ("Mapping", "15m_reference_price_source", "ORIGINAL_4H_SWING_OPEN_OPEN", "enum", "price", hard, "4h open", "Reference stays the 4h Swing Open open.", "mapping_config", "open"),
        ("Mapping", "15m_extreme_price_source", "ORIGINAL_4H_EXTREME_PRICE", "enum", "price", hard, "4h extreme", "Extreme price is not recomputed from 15m.", "mapping_config", "extreme"),
        ("Mapping", "15m_extreme_match_comparison", "EXACT_DECIMAL_EQUALITY", "enum", "price", hard, "Decimal equality", "No nearest-price matching.", "mapping_config", "extreme"),
        ("Mapping", "15m_extreme_representative", "LAST_CHRONOLOGICAL_EXACT_MATCH", "enum", "candle", hard, "last exact match", "Representative child of the 4h extreme event.", "mapping_config", "extreme"),
        ("Mapping", "15m_direct_close_search_starts_after_extreme", "TRUE", "boolean", "flag", hard, "TRUE", "Search starts after the representative extreme close.", "mapping_config", "close"),
        ("Mapping", "15m_direct_close_requires_completed_close", "TRUE", "boolean", "flag", "close", "Wick contact does not pass.", "mapping_config", "close"),
        ("Mapping", "15m_wick_only_return_passes", "FALSE", "boolean", "flag", hard, "FALSE", "A wick without a qualifying close fails.", "mapping_config", "close"),
        ("Mapping", "15m_first_qualifying_close_is_binding", "TRUE", "boolean", "flag", hard, "TRUE", "The first passing 15m close is the direct close.", "mapping_config", "close"),
        ("Mapping", "15m_search_outside_original_4h_window", "FALSE", "boolean", "flag", hard, "FALSE", "Search ends at the official 4h swing close.", "mapping_config", "close"),
        ("Mapping", "15m_can_change_4h_detection", "FALSE", "boolean", "flag", hard, "FALSE", "15m detail cannot change a 4h decision.", "mapping_config", "causality"),
        ("Mapping", "15m_can_change_primary_selection", "FALSE", "boolean", "flag", hard, "FALSE", "15m detail cannot change Primary selection.", "mapping_config", "causality"),
        ("Mapping", "15m_can_backdate_4h_confirmation", "FALSE", "boolean", "flag", hard, "FALSE", "Official confirmation stays the 4h close time.", "mapping_config", "causality"),
        ("Mapping", "official_confirmation_timeframe", "4h", "string", "timeframe", hard, "4h", "Official confirmation remains 4h.", "mapping_config", "causality"),
        ("Mapping", "timezone_conversion", 'ZoneInfo("Europe/Istanbul")', "string", "timezone", hard, "ZoneInfo", "Turkey display conversion.", "mapping_config", "time"),
        ("Mapping", "mapping_version", MAPPING_VERSION, "string", "version", hard, MAPPING_VERSION, "Mapping implementation version.", "mapping_config", "identity"),
        ("Mapping", "mapping_configuration_version", MAPPING_CONFIGURATION_VERSION, "string", "version", hard, MAPPING_CONFIGURATION_VERSION, "Mapping configuration version.", "mapping_config", "identity"),
        ("Mapping", "expected_15m_rows", EXPECTED_CHILD_ROWS, "integer", "rows", hard, "24768", "Required 15m row count.", "mapping_config", "validation"),
        ("Mapping", "15m_analysis_end_utc", CHILD_END_OPEN_UTC, "timestamp", "UTC", hard, "inclusive open", "Last mapped 15m open.", "mapping_config", "validation"),
    ]
    return rows
