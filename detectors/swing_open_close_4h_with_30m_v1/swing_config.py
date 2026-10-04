"""Frozen configuration for the 2026 4h Swing Open/Close detector."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parents[1]
DESKTOP_DIR = Path(r"C:\Users\oranb\Desktop")
TMP_DIR = PROJECT_ROOT / "tmp" / "swing_open_close_4h_with_30m_v1"
SOURCE_DATASET = Path(r"C:\MarketData\derived\binance\futures\um\perpetual\4h\symbol=BTCUSDT")

DETECTOR_VERSION = "BTCUSDT_4H_SWING_OPEN_CLOSE_2026_STD200_V5"
CONFIGURATION_VERSION = "BTCUSDT_4H_SWING_STD200_CONFIG_V5"
TIMEFRAME = "4h"
DISPLAY_TZ = ZoneInfo("Europe/Istanbul")
BAR_HOURS = 4
STEP_MS = 14_400_000
HOURS_PER_DAY = 24
WARMUP_BARS = 0
PRECISION_MODE = "exact"

ANALYSIS_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ANALYSIS_END_OPEN = datetime(2026, 9, 15, 20, tzinfo=timezone.utc)
ANALYSIS_END_CLOSE = datetime(2026, 9, 15, 23, 59, 59, 999000, tzinfo=timezone.utc)
EXCLUSIVE_LOAD_END = datetime(2026, 9, 16, tzinfo=timezone.utc)
EXPECTED_ROWS = 1548
EXPECTED_MONTHLY = {
    (2026, 1): 186,
    (2026, 2): 168,
    (2026, 3): 186,
    (2026, 4): 180,
    (2026, 5): 186,
    (2026, 6): 180,
    (2026, 7): 186,
    (2026, 8): 186,
    (2026, 9): 90,
}

COMPACT_MIN_INTERIOR = 1
COMPACT_MAX_INTERIOR = 6
DURATION_GAP_INTERIOR = 7
STANDARD_MIN_INTERIOR = 8
STANDARD_MAX_INTERIOR = 20
MAX_INTERIOR = 25
SEARCH_ONLY_MIN_INTERIOR = 21
ALTERNATIVE_MIN_INTERIOR = 4
ALTERNATIVE_MAX_INTERIOR = 10
ATR_PERIOD = 14
COMPACT_WIDTH = "3.50"
STANDARD_WIDTH = "2.00"
EQUALITY_TOLERANCE_PERCENT = "0.05"
DIRECTION_PRIORITY = "SWING_HIGH_THEN_SWING_LOW"
QUALITY_COMPACT_WIDTH_SPAN = "6.50"
QUALITY_STANDARD_WIDTH_SPAN = "7.50"
QUALITY_WEIGHT_WIDTH = "0.45"
QUALITY_WEIGHT_PRECISION = "0.25"
QUALITY_WEIGHT_PATH = "0.20"
QUALITY_WEIGHT_BALANCE = "0.10"
QUALITY_RETURN_SCALE = "1"
QUALITY_EXCEPTIONAL_MIN = "85"
QUALITY_HIGH_MIN = "70"
QUALITY_SIGNIFICANT_MIN = "50"
QUALITY_MODERATE_MIN = "30"
PROMINENCE_SENTINEL = "999999999"
EXCEL_WRITER = "openpyxl_3.1.5"
EXCEL_FORMAT = "XLSX_OFFICE_OPEN_XML"
EXCEL_CELL_LIMIT = 32767

REVISION_PATTERN_TEXT = r"^BTCUSDT_4H_Swing_Structure_rev(\d{2,})\.xlsx$"
WORKBOOK_SHEETS = (
    "Executive Summary",
    "Primary Swings",
    "Primary Swing Highs",
    "Primary Swing Lows",
    "Compact Primary",
    "Standard Primary",
    "Alternative Primary",
    "Derived Same Extreme",
    "Formation Candles",
    "Candidate Audit",
    "Overlap Analysis",
    "Forward Evaluation",
    "Parameters",
    "Diagnostics",
    "README",
)
PLAIN_RANGE_SHEETS = frozenset({
    "Executive Summary",
    "Derived Same Extreme",
    "Formation Candles",
    "Candidate Audit",
    "Diagnostics",
    "README",
})
TABLE_NAMES = {
    "Primary Swings": "tblPrimarySwings4H",
    "Primary Swing Highs": "tblPrimaryHighs4H",
    "Primary Swing Lows": "tblPrimaryLows4H",
    "Compact Primary": "tblCompactPrimary4H",
    "Standard Primary": "tblStandardPrimary4H",
    "Alternative Primary": "tblAlternativePrimary4H",
    "Overlap Analysis": "tblOverlapAnalysis4H",
    "Forward Evaluation": "tblForwardEvaluation4H",
    "Parameters": "tblParameters4H",
}


@dataclass(frozen=True, slots=True)
class ParameterRecord:
    category: str
    name: str
    value: str
    data_type: str
    unit: str
    rule: str
    formula: str
    description: str
    source: str
    used_in: str
    config_constant: str = ""


def _parameter(
    category: str,
    name: str,
    value: object,
    data_type: str,
    unit: str,
    rule: str,
    formula: str,
    description: str,
    used_in: str,
    config_constant: str = "",
) -> ParameterRecord:
    return ParameterRecord(
        category, name, str(value), data_type, unit, rule, formula, description,
        "detectors.swing_open_close_4h_with_30m_v1.swing_config", used_in, config_constant,
    )


def parameter_registry() -> tuple[ParameterRecord, ...]:
    """Every parameter the detector uses. The Parameters sheet must match this registry."""
    hard = "HARD_RULE"
    descriptive = "DESCRIPTIVE"
    sheets = " | ".join(WORKBOOK_SHEETS)
    monthly = ", ".join(f"{year}-{month:02d}={count}" for (year, month), count in EXPECTED_MONTHLY.items())
    tables = ", ".join(f"{sheet}={name}" for sheet, name in TABLE_NAMES.items())
    return (
        _parameter("Market", "symbol", "BTCUSDT", "string", "symbol", hard, "BTCUSDT", "Stored perpetual symbol.", "load, validation", ""),
        _parameter("Market", "tradingview_symbol", "BINANCE:BTCUSDT.P", "string", "symbol", descriptive, "BINANCE:BTCUSDT.P", "TradingView equivalent.", "parameters", ""),
        _parameter("Market", "market", "Binance USD-M PERPETUAL", "string", "market", hard, "futures/um PERPETUAL", "USDT-margined perpetual market.", "load, validation"),
        _parameter("Market", "timeframe", TIMEFRAME, "string", "timeframe", hard, "4h", "Analysis timeframe.", "load, validation", "TIMEFRAME"),
        _parameter("Market", "precision_mode", PRECISION_MODE, "string", "mode", hard, "exact", "Use stored price precision.", "load", "PRECISION_MODE"),
        _parameter("Market", "warmup_bars", WARMUP_BARS, "integer", "candles", hard, "0", "No pre-2026 warmup candles.", "load", "WARMUP_BARS"),
        _parameter("Window", "analysis_start_utc", "2026-01-01T00:00:00Z", "timestamp", "UTC", hard, "inclusive open_time", "First authorized candle open.", "load, validation", "ANALYSIS_START"),
        _parameter("Window", "analysis_end_open_utc", "2026-09-15T20:00:00Z", "timestamp", "UTC", hard, "inclusive open_time", "Last authorized candle open.", "load, validation", "ANALYSIS_END_OPEN"),
        _parameter("Window", "analysis_end_close_utc", "2026-09-15T23:59:59.999Z", "timestamp", "UTC", hard, "open_time + 4h - 1ms", "Last authorized candle close.", "validation", "ANALYSIS_END_CLOSE"),
        _parameter("Window", "exclusive_load_end_utc", "2026-09-16T00:00:00Z", "timestamp", "UTC", hard, "half-open loader end", "Loader end exclusive of the next day.", "load", "EXCLUSIVE_LOAD_END"),
        _parameter("Window", "expected_analysis_rows", EXPECTED_ROWS, "integer", "rows", hard, "1548", "Required selected row count.", "validation", "EXPECTED_ROWS"),
        _parameter("Window", "expected_monthly_row_counts", monthly, "map", "rows", hard, "calendar month counts", "Required monthly row counts for 2026.", "validation", "EXPECTED_MONTHLY"),
        _parameter("Window", "pre_2026_context_allowed", "FALSE", "boolean", "flag", hard, "FALSE", "Pre-2026 candles are forbidden for every detector step.", "validation"),
        _parameter("Window", "post_dataset_data_allowed", "FALSE", "boolean", "flag", hard, "FALSE", "No candles after the available 2026 close.", "validation"),
        _parameter("Formation", "swing_reference", "SWING_OPEN_CANDLE_OPEN", "enum", "price", hard, "open", "Reference is the Swing Open candle open.", "scan"),
        _parameter("Formation", "swing_high_extreme", "MAXIMUM_INTERIOR_HIGH", "enum", "price", hard, "max interior high", "Swing High extreme uses High.", "scan"),
        _parameter("Formation", "swing_low_extreme", "MINIMUM_INTERIOR_LOW", "enum", "price", hard, "min interior low", "Swing Low extreme uses Low.", "scan"),
        _parameter("Formation", "extreme_must_be_interior", "TRUE", "boolean", "flag", hard, "open_row < extreme_row < close_row", "Open and close candles cannot be the extreme.", "scan, audit"),
        _parameter("Formation", "extreme_must_be_central", "FALSE", "boolean", "flag", hard, "FALSE", "The extreme may sit at any interior position.", "scan"),
        _parameter("Formation", "fixed_left_bars", "NONE", "enum", "bars", hard, "NONE", "No fixed left pivot span.", "scan"),
        _parameter("Formation", "fixed_right_bars", "NONE", "enum", "bars", hard, "NONE", "No fixed right pivot span.", "scan"),
        _parameter("Duration", "compact_minimum_interior_candles", COMPACT_MIN_INTERIOR, "integer", "candles", hard, "1", "Shortest compact interior count.", "classification", "COMPACT_MIN_INTERIOR"),
        _parameter("Duration", "compact_maximum_interior_candles", COMPACT_MAX_INTERIOR, "integer", "candles", hard, "6", "Longest compact interior count.", "classification", "COMPACT_MAX_INTERIOR"),
        _parameter("Width", "compact_minimum_width_percent", COMPACT_WIDTH, "decimal", "percent", hard, "both widths >= 3.50", "Compact applicable minimum width.", "scan, audit", "COMPACT_WIDTH"),
        _parameter("Duration", "compact_total_formation_candles", "3 through 8", "range", "candles", hard, "interior + 2", "Compact formation length.", "classification"),
        _parameter("Duration", "compact_open_to_close_bars", "2 through 7", "range", "bars", descriptive, "(close_row - open_row)", "Compact open-to-close bar distance.", "diagnostics"),
        _parameter("Duration", "compact_open_to_close_hours", "8 through 28", "range", "hours", descriptive, "bars * 4", "Compact open-to-close elapsed time.", "diagnostics"),
        _parameter("Duration", "compact_coverage_hours", "12 through 32", "range", "hours", descriptive, "total_candles * 4", "Compact candle coverage.", "diagnostics"),
        _parameter("Duration", "duration_gap_interior_candles", DURATION_GAP_INTERIOR, "integer", "candles", hard, "7", "Interior count that belongs to neither class.", "scan", "DURATION_GAP_INTERIOR"),
        _parameter("Duration", "duration_gap_is_valid", "FALSE", "boolean", "flag", hard, "FALSE", "Seven interior candles never confirm a swing.", "scan"),
        _parameter("Duration", "duration_gap_rejection", "REJECTED_DURATION_GAP_7_INTERIOR_BARS", "enum", "status", hard, "REJECTED_DURATION_GAP_7_INTERIOR_BARS", "Status when the first return has seven interiors.", "scan"),
        _parameter("Duration", "standard_minimum_interior_candles", STANDARD_MIN_INTERIOR, "integer", "candles", hard, "8", "Shortest standard interior count.", "classification", "STANDARD_MIN_INTERIOR"),
        _parameter("Duration", "standard_maximum_interior_candles", STANDARD_MAX_INTERIOR, "integer", "candles", hard, "20", "Longest standard interior count.", "classification", "STANDARD_MAX_INTERIOR"),
        _parameter("Width", "standard_minimum_width_percent", STANDARD_WIDTH, "decimal", "percent", hard, "both widths >= 2.00", "Standard applicable minimum width.", "scan, audit", "STANDARD_WIDTH"),
        _parameter("Duration", "standard_total_formation_candles", "10 through 22", "range", "candles", hard, "interior + 2", "Standard formation length.", "classification"),
        _parameter("Duration", "standard_open_to_close_bars", "9 through 21", "range", "bars", descriptive, "(close_row - open_row)", "Standard open-to-close bar distance.", "diagnostics"),
        _parameter("Duration", "standard_open_to_close_hours", "36 through 84", "range", "hours", descriptive, "bars * 4", "Standard open-to-close elapsed time.", "diagnostics"),
        _parameter("Duration", "standard_coverage_hours", "40 through 88", "range", "hours", descriptive, "total_candles * 4", "Standard candle coverage.", "diagnostics"),
        _parameter("Duration", "maximum_search_interior_candles", MAX_INTERIOR, "integer", "candles", hard, "25", "Search-control horizon. Not a valid formation duration.", "scan, primary horizon", "MAX_INTERIOR"),
        _parameter("Duration", "maximum_search_total_formation_candles", "27", "integer", "candles", hard, "1 + 25 + 1", "Largest inspected formation. Not a valid duration.", "scan"),
        _parameter("Duration", "search_horizon_is_formation_validity_limit", "FALSE", "boolean", "flag", hard, "FALSE", "Reaching the search horizon does not make a formation valid.", "scan"),
        _parameter("Duration", "close_candidates_beyond_search_horizon_allowed", "FALSE", "boolean", "flag", hard, "FALSE", "Interior counts above 25 are not inspected.", "scan"),
        _parameter("Duration", "reference_search_only_minimum_interior_candles", SEARCH_ONLY_MIN_INTERIOR, "integer", "candles", hard, "21", "First reference interior that is searched but cannot confirm.", "scan", "SEARCH_ONLY_MIN_INTERIOR"),
        _parameter("Duration", "reference_search_only_maximum_interior_candles", MAX_INTERIOR, "integer", "candles", hard, "25", "Last reference interior that is searched but cannot confirm.", "scan", "MAX_INTERIOR"),
        _parameter("Duration", "reference_search_only_candidates_can_confirm", "FALSE", "boolean", "flag", hard, "FALSE", "A reference return at 21 through 25 is rejected.", "scan"),
        _parameter("Method", "reference_method_enabled", "TRUE", "boolean", "flag", hard, "REFERENCE_RETURN", "Reference-return confirmation remains enabled.", "scan"),
        _parameter("Method", "alternative_method_enabled", "TRUE", "boolean", "flag", hard, "ALTERNATIVE_3_5_WIDTH", "Alternative 3.50 percent confirmation is enabled.", "scan"),
        _parameter("Method", "alternative_minimum_interior_candles", ALTERNATIVE_MIN_INTERIOR, "integer", "candles", hard, "4", "Shortest alternative interior count.", "alternative", "ALTERNATIVE_MIN_INTERIOR"),
        _parameter("Method", "alternative_maximum_interior_candles", ALTERNATIVE_MAX_INTERIOR, "integer", "candles", hard, "10", "Longest alternative interior count.", "alternative", "ALTERNATIVE_MAX_INTERIOR"),
        _parameter("Method", "alternative_minimum_total_formation_candles", "6", "integer", "candles", hard, "interior + 2", "Shortest alternative formation.", "alternative"),
        _parameter("Method", "alternative_maximum_total_formation_candles", "12", "integer", "candles", hard, "interior + 2", "Longest alternative formation.", "alternative"),
        _parameter("Method", "alternative_minimum_width_percent", "3.50", "decimal", "percent", hard, "both widths >= 3.50", "Alternative method keeps 3.50 percent and does not use the Standard 2.00 percent threshold.", "alternative"),
        _parameter("Method", "alternative_reference_return_required", "FALSE", "boolean", "flag", hard, "FALSE", "Alternative close must stay on the non-return side.", "alternative"),
        _parameter("Method", "alternative_swing_high_close_condition", "CLOSE_GREATER_THAN_REFERENCE", "enum", "price", hard, "close > reference", "Alternative Swing High close condition.", "alternative"),
        _parameter("Method", "alternative_swing_low_close_condition", "CLOSE_LESS_THAN_REFERENCE", "enum", "price", hard, "close < reference", "Alternative Swing Low close condition.", "alternative"),
        _parameter("Method", "alternative_first_qualifying_close_is_binding", "TRUE", "boolean", "flag", hard, "TRUE", "The first close that passes every alternative rule is binding.", "alternative"),
        _parameter("Method", "interior_7_can_be_valid_on_alternative_method", "TRUE", "boolean", "flag", hard, "TRUE", "Seven interiors are invalid only for reference return.", "alternative"),
        _parameter("Method", "alternative_confirmation_occurs_at_completed_close", "TRUE", "boolean", "flag", hard, "close_time", "Alternative confirmation waits for the Swing Close candle.", "alternative"),
        _parameter("Method", "alternative_search_after_10_for_confirmation", "FALSE", "boolean", "flag", hard, "FALSE", "No close after 10 interiors can confirm an alternative swing.", "alternative"),
        _parameter("Method", "primary_method_preference", "NONE", "enum", "method", hard, "NONE", "Primary selection does not prefer a detection method.", "families"),
        _parameter("ATR", "atr_period", ATR_PERIOD, "integer", "bars", descriptive, "14", "Wilder ATR period. Descriptive only.", "diagnostics", "ATR_PERIOD"),
        _parameter("ATR", "atr_method", "WILDER", "enum", "method", descriptive, "(previous * 13 + TR) / 14", "ATR continuation formula. Seed is the SMA of the first 14 true ranges.", "diagnostics"),
        _parameter("ATR", "atr_hard_filter", "FALSE", "boolean", "flag", hard, "FALSE", "ATR cannot accept or reject a swing.", "diagnostics"),
        _parameter("ATR", "historical_context_before_2026", "FALSE", "boolean", "flag", hard, "FALSE", "ATR is seeded from 2026 candles only.", "diagnostics"),
        _parameter("Width", "both_width_measurements_must_pass", "TRUE", "boolean", "flag", hard, "open width and close width", "One passing width cannot save the other.", "scan"),
        _parameter("Width", "threshold_comparison", "UNROUNDED_GREATER_THAN_OR_EQUAL", "enum", "comparison", hard, "unrounded >=", "Width tests use exact unrounded percentages.", "scan"),
        _parameter("Width", "compact_boundary_pass", "3.50000000", "decimal", "percent", hard, ">= 3.50", "Exact compact boundary passes.", "scan"),
        _parameter("Width", "compact_boundary_fail", "3.49999999", "decimal", "percent", hard, "< 3.50", "Any value below 3.50 fails compact.", "scan"),
        _parameter("Width", "standard_boundary_pass", "2.00", "decimal", "percent", hard, ">= 2.00", "Exact unrounded Standard boundary passes. Both widths must pass.", "scan"),
        _parameter("Width", "standard_boundary_fail", "1.99999999", "decimal", "percent", hard, "< 2.00", "Any unrounded value below 2.00 fails Standard.", "scan"),
        _parameter("Width", "rejected_width_below_3_50", "REJECTED_WIDTH_BELOW_3_50", "enum", "status", hard, "compact failure", "Compact width failure status.", "scan"),
        _parameter("Width", "rejected_width_below_2_50", "UNUSED_LEGACY_STATUS", "enum", "status", hard, "not assigned", "Legacy name retained only as an unused label. Live Standard failures use the reference width codes at 2.00 percent.", "scan"),
        _parameter("Completion", "swing_high_completion", "SWING_CLOSE_CLOSE_LESS_THAN_OR_EQUAL_TO_REFERENCE", "enum", "price", hard, "close <= reference", "Swing High completion uses Close.", "scan"),
        _parameter("Completion", "swing_low_completion", "SWING_CLOSE_CLOSE_GREATER_THAN_OR_EQUAL_TO_REFERENCE", "enum", "price", hard, "close >= reference", "Swing Low completion uses Close.", "scan"),
        _parameter("Completion", "exact_reference_close_passes", "TRUE", "boolean", "flag", hard, "EXACT_RETURN", "An equal close confirms.", "scan"),
        _parameter("Completion", "crossed_reference_close_passes", "TRUE", "boolean", "flag", hard, "CROSS_RETURN", "A crossed close confirms.", "scan"),
        _parameter("Completion", "first_return_close_is_binding", "TRUE", "boolean", "flag", hard, "TRUE", "The first qualifying return terminates the attempt.", "scan"),
        _parameter("Completion", "later_close_cherry_picking", "FALSE", "boolean", "flag", hard, "FALSE", "A later close cannot replace a failed or gapped first return.", "scan"),
        _parameter("Completion", "confirmation_timestamp", "swing_close_open + 4h - 1ms", "formula", "UTC", hard, "close_time", "Confirmation is the completed Swing Close candle.", "audit"),
        _parameter("Extreme", "plateau_representative", "LAST_EXTREME_OCCURRENCE", "enum", "candle", hard, "last equal interior candle", "Representative extreme is the last occurrence before close.", "scan"),
        _parameter("Extreme", "distant_equal_prices_same_family", "FALSE", "boolean", "flag", hard, "FALSE", "Unrelated equal prices stay in separate events.", "families"),
        _parameter("Family", "same_extreme_family_enabled", "TRUE", "boolean", "flag", hard, "TRUE", "Same-extreme consolidation is mandatory.", "families"),
        _parameter("Family", "same_extreme_family_key", "SWING_TYPE + EXACT_EXTREME_PRICE + EXTREME_EVENT_ID", "formula", "key", hard, "exact Decimal equality", "Family membership key.", "families"),
        _parameter("Family", "extreme_price_comparison", "EXACT_UNROUNDED_DECIMAL_EQUALITY", "enum", "price", hard, "Decimal equality", "Display rounding does not create a family.", "families"),
        _parameter("Primary", "primary_selection_first_rule", "MINIMUM_TOTAL_FORMATION_CANDLES", "enum", "candles", hard, "minimum total candles", "Shortest valid formation is Primary.", "families"),
        _parameter("Primary", "primary_selection_tie_break_1", "MINIMUM_ABSOLUTE_COMPLETION_ERROR_PERCENT", "enum", "percent", hard, "abs(close-reference)/reference*100", "First equal-length tie-break.", "families"),
        _parameter("Primary", "primary_selection_tie_break_2", "MAXIMUM_MINIMUM_OF_TWO_WIDTHS", "enum", "percent", hard, "max of min(open width, close width)", "Second equal-length tie-break.", "families"),
        _parameter("Primary", "primary_selection_tie_break_3", "EARLIEST_CONFIRMATION", "enum", "timestamp", hard, "earliest confirmed_at", "Third equal-length tie-break.", "families"),
        _parameter("Primary", "primary_selection_tie_break_4", "LATEST_SWING_OPEN", "enum", "timestamp", hard, "latest swing open", "Fourth equal-length tie-break.", "families"),
        _parameter("Primary", "primary_selection_tie_break_5", "LOWEST_STABLE_RAW_SWING_ID", "enum", "id", hard, "RAW-SW*4H id", "Final equal-length tie-break.", "families"),
        _parameter("Primary", "derived_same_extreme_excluded_from_main_results", "TRUE", "boolean", "flag", hard, "TRUE", "Derived rows stay out of principal sheets.", "workbook"),
        _parameter("Primary", "primary_count_per_family", "1", "integer", "count", hard, "1", "Each family has one Primary.", "audit"),
        _parameter("Primary", "derived_reason_longer", "LONGER_FORMATION_FOR_SAME_EXTREME", "enum", "reason", hard, "more candles than primary", "Longer same-extreme classification.", "families"),
        _parameter("Primary", "derived_reason_tie", "EQUAL_MINIMUM_LENGTH_TIE_BREAK_LOSS", "enum", "reason", hard, "equal length loser", "Equal-length loser classification.", "families"),
        _parameter("Primary", "raw_confirmation_timing", "CAUSAL_AT_SWING_CLOSE", "enum", "timing", hard, "CAUSAL_AT_SWING_CLOSE", "Raw confirmation is known when Swing Close completes.", "causality"),
        _parameter("Primary", "primary_designation_timing", "POST_DETECTION_SAME_EXTREME_CONSOLIDATION", "enum", "timing", hard, "POST_DETECTION_SAME_EXTREME_CONSOLIDATION", "Primary status is assigned after family comparison.", "causality"),
        _parameter("Primary", "primary_shortest_formation_wins", "TRUE", "boolean", "flag", hard, "TRUE", "The shortest valid formation is Primary.", "families"),
        _parameter("Primary", "primary_designation_horizon_bars", MAX_INTERIOR, "integer", "bars", hard, "run_end + 25, capped at dataset end", "Latest bar needed to know competing family members.", "families", "MAX_INTERIOR"),
        _parameter("Overlap", "overlapping_primary_swings_retained", "TRUE", "boolean", "flag", hard, "TRUE", "Overlapping Primaries are retained.", "overlap"),
        _parameter("Overlap", "derived_swings_in_overlap_analysis", "FALSE", "boolean", "flag", hard, "FALSE", "Derived swings do not enter overlap counts.", "overlap"),
        _parameter("Labels", "strict_alternation_required", "FALSE", "boolean", "flag", hard, "FALSE", "Structure labels do not require High/Low alternation.", "labels"),
        _parameter("Labels", "equality_tolerance_percent", EQUALITY_TOLERANCE_PERCENT, "decimal", "percent", descriptive, "0.05", "HH/LH/EH and HL/LL/EL equality band. Not used for families.", "labels", "EQUALITY_TOLERANCE_PERCENT"),
        _parameter("Conflict", "direction_conflict_priority", DIRECTION_PRIORITY, "enum", "direction", hard, "SWING_HIGH_THEN_SWING_LOW", "Final conflict priority after width and time.", "conflict", "DIRECTION_PRIORITY"),
        _parameter("Conflict", "conflict_resolution_order", "EARLIER_WIDTH_THEN_EARLIER_RETURN_THEN_LARGER_WIDTH_THEN_PRIORITY", "enum", "order", hard, "4-step order", "Same-interval High/Low conflict order.", "conflict"),
        _parameter("Quality", "quality_score_is_hard_filter", "FALSE", "boolean", "flag", hard, "FALSE", "Quality cannot select or reject a Primary.", "quality"),
        _parameter("Quality", "quality_compact_width_span", QUALITY_COMPACT_WIDTH_SPAN, "decimal", "percent", descriptive, "(min width - 3.50) / 6.50", "Compact quality width divisor.", "quality", "QUALITY_COMPACT_WIDTH_SPAN"),
        _parameter("Quality", "quality_standard_width_span", QUALITY_STANDARD_WIDTH_SPAN, "decimal", "percent", descriptive, "(min width - 2.00) / 7.50", "Standard quality width divisor. The base is the 2.00 percent Standard threshold.", "quality", "QUALITY_STANDARD_WIDTH_SPAN"),
        _parameter("Quality", "quality_weight_width", QUALITY_WEIGHT_WIDTH, "decimal", "weight", descriptive, "0.45", "Quality weight for width.", "quality", "QUALITY_WEIGHT_WIDTH"),
        _parameter("Quality", "quality_weight_return_precision", QUALITY_WEIGHT_PRECISION, "decimal", "weight", descriptive, "0.25", "Quality weight for return precision.", "quality", "QUALITY_WEIGHT_PRECISION"),
        _parameter("Quality", "quality_weight_path_efficiency", QUALITY_WEIGHT_PATH, "decimal", "weight", descriptive, "0.20", "Quality weight for path efficiency.", "quality", "QUALITY_WEIGHT_PATH"),
        _parameter("Quality", "quality_weight_formation_balance", QUALITY_WEIGHT_BALANCE, "decimal", "weight", descriptive, "0.10", "Quality weight for left/right balance.", "quality", "QUALITY_WEIGHT_BALANCE"),
        _parameter("Quality", "quality_return_precision_scale", QUALITY_RETURN_SCALE, "decimal", "percent", descriptive, "1 - clamp(abs error / 1, 0, 1)", "Return-precision scale.", "quality", "QUALITY_RETURN_SCALE"),
        _parameter("Quality", "quality_exceptional_minimum", QUALITY_EXCEPTIONAL_MIN, "decimal", "score", descriptive, "85-100", "EXCEPTIONAL label.", "quality", "QUALITY_EXCEPTIONAL_MIN"),
        _parameter("Quality", "quality_high_minimum", QUALITY_HIGH_MIN, "decimal", "score", descriptive, "70 to below 85", "HIGH_QUALITY label.", "quality", "QUALITY_HIGH_MIN"),
        _parameter("Quality", "quality_significant_minimum", QUALITY_SIGNIFICANT_MIN, "decimal", "score", descriptive, "50 to below 70", "SIGNIFICANT label.", "quality", "QUALITY_SIGNIFICANT_MIN"),
        _parameter("Quality", "quality_moderate_minimum", QUALITY_MODERATE_MIN, "decimal", "score", descriptive, "30 to below 50", "MODERATE label. Below 30 is BASIC_CONFIRMED.", "quality", "QUALITY_MODERATE_MIN"),
        _parameter("Path", "extreme_prominence_sentinel", PROMINENCE_SENTINEL, "decimal", "price", descriptive, "suffix floor", "Sentinel used when no second extreme exists.", "forward", "PROMINENCE_SENTINEL"),
        _parameter("Time", "computational_timezone", "UTC", "timezone", "zone", hard, "UTC", "All calculations use UTC.", "timestamps"),
        _parameter("Time", "display_timezone", "Europe/Istanbul", "timezone", "zone", descriptive, "ZoneInfo", "Display timestamps use Turkey time.", "workbook", "DISPLAY_TZ"),
        _parameter("Time", "bar_hours", BAR_HOURS, "integer", "hours", hard, "4", "Duration of one 4h candle.", "duration", "BAR_HOURS"),
        _parameter("Time", "bar_step_milliseconds", STEP_MS, "integer", "milliseconds", hard, "14400000", "Open-time step.", "validation", "STEP_MS"),
        _parameter("Time", "hours_per_day", HOURS_PER_DAY, "integer", "hours", descriptive, "24", "Duration days divisor.", "diagnostics", "HOURS_PER_DAY"),
        _parameter("Time", "close_time_formula", "open_time + 4 hours - 1 millisecond", "formula", "UTC", hard, "close_time", "Required 4h close timestamp.", "validation"),
        _parameter("Paths", "project_root", str(PROJECT_ROOT), "path", "path", hard, "existing project", "Detector stays inside this project.", "runner", "PROJECT_ROOT"),
        _parameter("Paths", "package_dir", str(PACKAGE_DIR), "path", "path", hard, "swing_open_close_4h_with_30m_v1", "Isolated detector package.", "runner", "PACKAGE_DIR"),
        _parameter("Paths", "desktop_dir", str(DESKTOP_DIR), "path", "path", hard, "revision scan root", "Workbook revision directory.", "runner", "DESKTOP_DIR"),
        _parameter("Paths", "temporary_dir", str(TMP_DIR), "path", "path", hard, "tmp workbook", "Temporary workbook directory.", "runner", "TMP_DIR"),
        _parameter("Paths", "source_dataset", str(SOURCE_DATASET), "path", "path", hard, "read-only parquet", "BTCUSDT 4h derived dataset.", "load", "SOURCE_DATASET"),
        _parameter("Output", "revision_pattern", REVISION_PATTERN_TEXT, "regex", "filename", hard, "revNN", "Desktop workbook revision pattern.", "runner", "REVISION_PATTERN_TEXT"),
        _parameter("Output", "workbook_sheets", sheets, "list", "sheet", hard, "15 named sheets", "Required sheet order.", "workbook", "WORKBOOK_SHEETS"),
        _parameter("Output", "plain_range_sheets", ", ".join(sorted(PLAIN_RANGE_SHEETS)), "list", "sheet", hard, "no formal table", "Sheets that stay plain filtered ranges.", "workbook", "PLAIN_RANGE_SHEETS"),
        _parameter("Output", "excel_table_names", tables, "map", "table", descriptive, "unique table names", "Formal tables on the smaller result sheets.", "workbook", "TABLE_NAMES"),
        _parameter("Output", "excel_writer", EXCEL_WRITER, "string", "library", hard, "openpyxl==3.1.5", "Workbook writer.", "workbook", "EXCEL_WRITER"),
        _parameter("Output", "excel_format", EXCEL_FORMAT, "enum", "format", hard, "XLSX_OFFICE_OPEN_XML", "Office Open XML workbook.", "workbook", "EXCEL_FORMAT"),
        _parameter("Output", "excel_macros", "FALSE", "boolean", "flag", hard, "FALSE", "Macros are forbidden.", "workbook"),
        _parameter("Output", "excel_external_links", "FALSE", "boolean", "flag", hard, "FALSE", "External links are forbidden.", "workbook"),
        _parameter("Output", "excel_cell_character_limit", EXCEL_CELL_LIMIT, "integer", "characters", hard, "32767", "Maximum Excel cell string length.", "workbook", "EXCEL_CELL_LIMIT"),
        _parameter("Identity", "detector_version", DETECTOR_VERSION, "string", "version", hard, DETECTOR_VERSION, "Detector version written to the workbook.", "workbook", "DETECTOR_VERSION"),
        _parameter("Identity", "configuration_version", CONFIGURATION_VERSION, "string", "version", hard, CONFIGURATION_VERSION, "Configuration version for this alternative-method revision.", "workbook", "CONFIGURATION_VERSION"),
        _parameter("Identity", "config_object", "SwingConfig", "object", "config", descriptive, "frozen defaults", "Runtime config object.", "runner", "CONFIG"),
        _parameter("Identity", "raw_swing_id_pattern", "RAW-SWH4H-000001 / RAW-SWL4H-000001", "pattern", "id", hard, "confirmation order", "Raw swing identifiers.", "ids"),
        _parameter("Identity", "primary_swing_id_pattern", "PRI-SWH4H-000001 / PRI-SWL4H-000001", "pattern", "id", hard, "primary confirmation order", "Primary swing identifiers.", "ids"),
        _parameter("Identity", "attempt_id_pattern", "ATT4H-H-000001 / ATT4H-L-000001", "pattern", "id", hard, "open-row order", "Attempt identifiers.", "ids"),
        _parameter("Identity", "family_id_pattern", "FAM-H-000001 / FAM-L-000001", "pattern", "id", hard, "event order", "Extreme family identifiers.", "ids"),
        _parameter("Identity", "extreme_event_id_pattern", "EXH4H-000001 / EXL4H-000001", "pattern", "id", hard, "event order", "Extreme event identifiers.", "ids"),
    )


def assert_parameter_registry() -> None:
    records = parameter_registry()
    names = [item.name for item in records]
    if len(names) != len(set(names)):
        raise RuntimeError("parameter registry contains duplicate names")
    linked = {item.config_constant for item in records if item.config_constant}
    public = {name for name in globals() if name.isupper()}
    missing = sorted(public - linked)
    if missing:
        raise RuntimeError("config constants missing from the parameter registry: " + ", ".join(missing))
    mandatory = {
        "symbol", "tradingview_symbol", "market", "timeframe", "analysis_start_utc", "analysis_end_open_utc",
        "analysis_end_close_utc", "expected_analysis_rows", "pre_2026_context_allowed", "post_dataset_data_allowed",
        "swing_reference", "swing_high_extreme", "swing_low_extreme", "extreme_must_be_interior", "extreme_must_be_central",
        "fixed_left_bars", "fixed_right_bars", "compact_minimum_interior_candles", "compact_maximum_interior_candles",
        "compact_minimum_width_percent", "duration_gap_interior_candles", "duration_gap_is_valid", "duration_gap_rejection",
        "standard_minimum_interior_candles", "standard_maximum_interior_candles", "standard_minimum_width_percent",
        "both_width_measurements_must_pass", "swing_high_completion", "swing_low_completion", "exact_reference_close_passes",
        "crossed_reference_close_passes", "first_return_close_is_binding", "later_close_cherry_picking",
        "maximum_search_interior_candles", "plateau_representative", "same_extreme_family_enabled", "same_extreme_family_key",
        "extreme_price_comparison", "distant_equal_prices_same_family", "primary_selection_first_rule",
        "primary_selection_tie_break_1", "primary_selection_tie_break_2", "primary_selection_tie_break_3",
        "primary_selection_tie_break_4", "primary_selection_tie_break_5", "derived_same_extreme_excluded_from_main_results",
        "primary_count_per_family", "overlapping_primary_swings_retained", "derived_swings_in_overlap_analysis",
        "strict_alternation_required", "equality_tolerance_percent", "quality_score_is_hard_filter",
        "computational_timezone", "display_timezone", "excel_writer", "excel_format", "excel_macros",
        "excel_external_links", "detector_version",
    }
    absent = sorted(mandatory - set(names))
    if absent:
        raise RuntimeError("mandatory parameters missing: " + ", ".join(absent))


@dataclass(frozen=True, slots=True)
class SwingConfig:
    project_root: Path = PROJECT_ROOT
    package_dir: Path = PACKAGE_DIR
    desktop_dir: Path = DESKTOP_DIR
    temporary_dir: Path = TMP_DIR
    detector_version: str = DETECTOR_VERSION


CONFIG = SwingConfig()
