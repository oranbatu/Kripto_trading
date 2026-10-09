"""Workbook writer for the BTCUSDT 30-minute special swing detector."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.workbook.properties import CalcProperties
from openpyxl.worksheet.table import Table, TableStyleInfo

from detectors.swing_open_close_30m_special_v1.config import (
    FORMATION_CLASS,
    MIN_BOUNDARY,
    RESULT_COLUMNS,
    SHEETS,
    SWING_HIGH,
    SWING_LOW,
    USER_PARAMETERS,
)
from detectors.swing_open_close_30m_special_v1.engine import Analysis, candle_direction, turkey_text

HEADER_FILL = PatternFill("solid", fgColor="1F4E79")
HEADER_FONT = Font(color="FFFFFF", bold=True, name="Calibri")
BULLISH_FONT = Font(color="006600", bold=True, name="Calibri")
BEARISH_FONT = Font(color="990000", bold=True, name="Calibri")
HIGH_FILL = PatternFill("solid", fgColor="F4CCCC")
HIGH_ALT = PatternFill("solid", fgColor="F8E0E0")
LOW_FILL = PatternFill("solid", fgColor="D9EAD3")
LOW_ALT = PatternFill("solid", fgColor="E7F3E4")
REFERENCE_FILL = PatternFill("solid", fgColor="D6EAF8")
VALIDATION_FILL = PatternFill("solid", fgColor="FDEBD0")
COMPATIBILITY_FILL = PatternFill("solid", fgColor="F5EEF8")
WRAP = Alignment(wrap_text=True, vertical="center")
EIGHT = Decimal("0.00000001")
PRICE_COLUMNS = (9, 10, 11, 12, 16, 17, 18, 19, 23, 30, 31, 32, 33, 34, 37, 38, 39, 40, 44, 47, 55, 56, 57)
PERCENT_COLUMNS = (45, 48, 50, 51, 58, 59)
REFERENCE_COLUMNS = range(24, 35)
VALIDATION_COLUMNS = range(35, 41)
PRE_CLOSE_COLUMNS = range(41, 43)
COMPATIBILITY_COLUMNS = range(43, 46)
DIRECTION_COLUMNS = (8, 15, 29, 36)
TABLE_NAMES = {
    "Special Swings": "SpecialSwings",
    "Swing Highs": "SwingHighs",
    "Swing Lows": "SwingLows",
    "Parameters": "DetectorParameters",
}


def shown(value: Decimal) -> Decimal:
    return value.quantize(EIGHT, rounding=ROUND_HALF_UP)


def _optional(value) -> Decimal | None:
    return None if value is None else shown(value)


def _row(item, bars) -> list:
    opened = bars[item.open_row]
    closed = bars[item.close_row]
    extreme = bars[item.extreme_row]
    reference = None if item.body_reference_row is None else bars[item.body_reference_row]
    validation = None if item.body_reference_validation_row is None else bars[item.body_reference_validation_row]
    return [
        item.swing_id,
        item.direction,
        item.formation_class,
        item.interior,
        item.total,
        turkey_text(opened.open_time),
        turkey_text(opened.close_time),
        candle_direction(opened.open, opened.close),
        shown(opened.open),
        shown(opened.high),
        shown(opened.low),
        shown(opened.close),
        turkey_text(closed.open_time),
        turkey_text(closed.close_time),
        candle_direction(closed.open, closed.close),
        shown(closed.open),
        shown(closed.high),
        shown(closed.low),
        shown(closed.close),
        item.earlier_compatibility_rejections,
        turkey_text(extreme.open_time),
        "HIGH" if item.direction == SWING_HIGH else "LOW",
        shown(item.extreme_price),
        item.body_reference_status,
        item.body_reference_pair_type or None,
        item.body_reference_rule,
        item.body_reference_eligible_count,
        None if reference is None else turkey_text(reference.open_time),
        item.body_reference_direction or None,
        _optional(None if reference is None else reference.open),
        _optional(None if reference is None else reference.high),
        _optional(None if reference is None else reference.low),
        _optional(None if reference is None else reference.close),
        _optional(item.body_reference_price),
        None if validation is None else turkey_text(validation.open_time),
        item.body_reference_validation_direction or None,
        _optional(None if validation is None else validation.open),
        _optional(None if validation is None else validation.high),
        _optional(None if validation is None else validation.low),
        _optional(None if validation is None else validation.close),
        None if reference is None else "No",
        item.body_reference_bars_before_close,
        item.compatibility_status or None,
        _optional(item.reference_to_swing_close_margin),
        _optional(item.reference_to_swing_close_margin_percent),
        item.body_reference_row_difference,
        _optional(item.body_reference_close_change),
        _optional(item.body_reference_close_change_percent),
        item.body_reference_bars_after_open,
        _optional(item.body_reference_position_fraction),
        _optional(item.body_reference_validation_position_fraction),
        item.body_reference_tie_count,
        None if item.body_reference_matches_extremum is None else ("Yes" if item.body_reference_matches_extremum else "No"),
        None if item.body_reference_validation_matches_extremum is None else ("Yes" if item.body_reference_validation_matches_extremum else "No"),
        _optional(item.extremum_to_body_reference_distance),
        shown(item.structure_high),
        shown(item.structure_low),
        shown(item.open_width_percent),
        shown(item.close_width_percent),
    ]


def _paint(sheet, rows: list[list]) -> None:
    for cell in sheet[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = WRAP
    for index, row in enumerate(rows, start=2):
        fill = (HIGH_ALT if index % 2 == 0 else HIGH_FILL) if row[1] == SWING_HIGH else (LOW_ALT if index % 2 == 0 else LOW_FILL)
        for cell in sheet[index]:
            cell.fill = fill
        for column in REFERENCE_COLUMNS:
            sheet.cell(index, column).fill = REFERENCE_FILL
        for column in VALIDATION_COLUMNS:
            sheet.cell(index, column).fill = VALIDATION_FILL
        for column in PRE_CLOSE_COLUMNS:
            sheet.cell(index, column).fill = REFERENCE_FILL
        for column in COMPATIBILITY_COLUMNS:
            sheet.cell(index, column).fill = COMPATIBILITY_FILL
        for column in DIRECTION_COLUMNS:
            label = sheet.cell(index, column).value
            if label == "BULLISH":
                sheet.cell(index, column).font = BULLISH_FONT
            elif label == "BEARISH":
                sheet.cell(index, column).font = BEARISH_FONT
        for column in PRICE_COLUMNS + PERCENT_COLUMNS:
            sheet.cell(index, column).number_format = "0.00000000"
    sheet.freeze_panes = "A2"
    last_row = max(1, sheet.max_row)
    last_column = get_column_letter(len(RESULT_COLUMNS))
    sheet.auto_filter.ref = f"A1:{last_column}{last_row}"
    sheet.row_dimensions[1].height = 32
    for column in range(1, len(RESULT_COLUMNS) + 1):
        sheet.column_dimensions[get_column_letter(column)].width = 24
    table = Table(displayName=TABLE_NAMES[sheet.title], ref=f"A1:{last_column}{last_row}")
    table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
    sheet.add_table(table)


def write_workbook(path: Path, result: Analysis) -> None:
    book = Workbook()
    book.calculation = CalcProperties(calcMode="auto", fullCalcOnLoad=False)
    book.remove(book.active)
    ordered = sorted(result.displayed, key=lambda item: (item.open_row, item.close_row, item.swing_id))
    rows = [_row(item, result.bars) for item in ordered]
    for title, selected in (
        ("Special Swings", rows),
        ("Swing Highs", [row for row in rows if row[1] == SWING_HIGH]),
        ("Swing Lows", [row for row in rows if row[1] == "SWING_LOW"]),
    ):
        sheet = book.create_sheet(title)
        sheet.append(list(RESULT_COLUMNS))
        for row in selected:
            sheet.append(row)
        _paint(sheet, selected)
    parameters = book.create_sheet("Parameters")
    parameters.append(["Parameter", "Value"])
    for name, value in USER_PARAMETERS:
        parameters.append([name, value])
    for cell in parameters[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = WRAP
    parameters.freeze_panes = "A2"
    parameters.auto_filter.ref = f"A1:B{parameters.max_row}"
    parameters.column_dimensions["A"].width = 52
    parameters.column_dimensions["B"].width = 64
    parameter_table = Table(displayName="DetectorParameters", ref=f"A1:B{parameters.max_row}")
    parameter_table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=False)
    parameters.add_table(parameter_table)
    if book.sheetnames != list(SHEETS):
        raise RuntimeError(f"sheet order {book.sheetnames}")
    path.parent.mkdir(parents=True, exist_ok=True)
    book.save(path)
    book.close()


def _validate_reference_row(row, result: Analysis) -> None:
    item = next(candidate for candidate in result.displayed if candidate.swing_id == row[0])
    blank_indexes = (24, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54)
    if row[25] != item.body_reference_rule or not row[25]:
        raise RuntimeError("body reference selection rule is missing")
    if "SINGLE" in str(row[25]).upper() or "AMONG_STRICTLY_INTERIOR_BEARISH_CANDLES" in str(row[25]):
        raise RuntimeError("obsolete single-candle reference rule is present")
    if row[42] == "FAIL":
        raise RuntimeError("accepted row contains failed compatibility")
    if int(row[19]) != item.earlier_compatibility_rejections:
        raise RuntimeError("earlier compatibility rejection count is wrong")
    if item.body_reference_row is None:
        if row[23] not in {
            "NO_BEARISH_TO_BULLISH_INTERIOR_PAIR_FOR_SWING_HIGH",
            "NO_BULLISH_TO_BEARISH_INTERIOR_PAIR_FOR_SWING_LOW",
        }:
            raise RuntimeError("missing body reference status is wrong")
        if row[42] != "NOT_APPLICABLE_NO_REFERENCE_PAIR":
            raise RuntimeError("missing pair compatibility status is wrong")
        if any(row[index] not in (None, "") for index in blank_indexes):
            raise RuntimeError("missing body reference contains a price or timestamp")
        if row[26] != 0 or row[51] != 0:
            raise RuntimeError("missing body reference counts are not zero")
        return
    candle = result.bars[item.body_reference_row]
    follower = result.bars[item.body_reference_validation_row]
    if item.body_reference_validation_row != item.body_reference_row + 1:
        raise RuntimeError("reference pair is not consecutive")
    if not (item.open_row < item.body_reference_row < item.body_reference_validation_row < item.close_row):
        raise RuntimeError("reference pair is not strictly interior")
    if row[27] != turkey_text(candle.open_time) or row[34] != turkey_text(follower.open_time):
        raise RuntimeError("reference pair Turkey time does not match the source candles")
    expected_reference = "BEARISH" if item.direction == SWING_HIGH else "BULLISH"
    expected_validation = "BULLISH" if item.direction == SWING_HIGH else "BEARISH"
    if row[28] != expected_reference or row[35] != expected_validation:
        raise RuntimeError("reference pair directions are wrong")
    if row[40] != "No":
        raise RuntimeError("selected reference is immediately before Swing Close")
    if int(row[41]) != item.close_row - item.body_reference_row or int(row[41]) < 2:
        raise RuntimeError("reference rows before Swing Close are below 2")
    if row[42] != "PASS":
        raise RuntimeError("accepted reference compatibility did not pass")
    if row[45] != 1:
        raise RuntimeError("reference-to-validation row difference is not 1")
    if item.direction == SWING_HIGH and item.body_reference_price < item.close_price:
        raise RuntimeError("swing high reference close is below the swing close")
    if item.direction == SWING_LOW and item.body_reference_price > item.close_price:
        raise RuntimeError("swing low reference close is above the swing close")
    compared = (
        (29, candle.open),
        (30, candle.high),
        (31, candle.low),
        (32, candle.close),
        (33, item.body_reference_price),
        (36, follower.open),
        (37, follower.high),
        (38, follower.low),
        (39, follower.close),
        (43, item.reference_to_swing_close_margin),
        (44, item.reference_to_swing_close_margin_percent),
        (46, item.body_reference_close_change),
        (47, item.body_reference_close_change_percent),
        (49, item.body_reference_position_fraction),
        (50, item.body_reference_validation_position_fraction),
        (54, item.extremum_to_body_reference_distance),
    )
    for index, value in compared:
        if _cell_decimal(row[index]) != shown(value):
            raise RuntimeError(f"body reference column {index + 1} does not match the source candle")
    if int(row[48]) != item.body_reference_bars_after_open:
        raise RuntimeError("body reference bar offsets are wrong")
    if item.body_reference_price == candle.low or item.body_reference_price == candle.high:
        if candle.low == candle.close or candle.high == candle.close:
            pass
        else:
            raise RuntimeError("reference price was taken from a wick")
    if int(row[26]) != item.body_reference_eligible_count or int(row[51]) != item.body_reference_tie_count:
        raise RuntimeError("body reference pair counts are wrong")
    if row[52] != ("Yes" if item.body_reference_matches_extremum else "No"):
        raise RuntimeError("body reference extremum match flag is wrong")
    if row[53] != ("Yes" if item.body_reference_validation_matches_extremum else "No"):
        raise RuntimeError("validation extremum match flag is wrong")
    if item.body_reference_price != candle.close:
        raise RuntimeError("body reference price is not the reference candle Close")
    if item.extremum_to_body_reference_distance < 0:
        raise RuntimeError("extremum-to-body-reference distance is negative")
    if item.reference_to_swing_close_margin < 0:
        raise RuntimeError("accepted compatibility margin is negative")


def _cell_decimal(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(format(value, ".8f"))


def validate_workbook(path: Path, result: Analysis) -> dict:
    normal = load_workbook(path, read_only=False, data_only=False)
    readonly = load_workbook(path, read_only=True, data_only=False)
    data_only = load_workbook(path, read_only=True, data_only=True)
    try:
        if normal.sheetnames != list(SHEETS) or readonly.sheetnames != list(SHEETS) or data_only.sheetnames != list(SHEETS):
            raise RuntimeError(f"sheets {normal.sheetnames}")
        if normal.vba_archive is not None or list(normal._external_links):
            raise RuntimeError("workbook contains macros or external links")
        for name in ("Special Swings", "Swing Highs", "Swing Lows"):
            headers = [cell.value for cell in next(normal[name].iter_rows(max_row=1))]
            if tuple(headers) != RESULT_COLUMNS:
                raise RuntimeError(f"{name} columns differ")
            if any(header and "UTC" in str(header).upper() for header in headers):
                raise RuntimeError("UTC column present")
            if not normal[name].tables:
                raise RuntimeError(f"{name} has no Excel table")
        values = {row[0]: row[1] for row in normal["Parameters"].iter_rows(min_row=2, values_only=True)}
        if values != dict(USER_PARAMETERS):
            raise RuntimeError("parameters sheet does not match the configured list")
        blob = " ".join(str(item) for item in values.values())
        if "0.80%" in blob or values["Maximum Interior Candles"] in {"10", "20"} or values["Minimum Interior Candles"] == "7":
            raise RuntimeError("obsolete boundary or horizon is present")
        if "7–10" in blob or "7_TO_10" in blob or "BELOW_7" in blob or "ABOVE_10" in blob:
            raise RuntimeError("obsolete 7-10 configuration is present")
        if "Lowest Close Among Bearish Interior Candles" in blob or "Highest Close Among Bullish Interior Candles" in blob:
            raise RuntimeError("obsolete single-candle reference rule is present")
        if values["Swing High Reference-To-Close Rule"] != "Reference Close >= Swing Close Close":
            raise RuntimeError("swing high reference-to-close rule is missing")
        if values["Swing Low Reference-To-Close Rule"] != "Reference Close <= Swing Close Close":
            raise RuntimeError("swing low reference-to-close rule is missing")
        if values["Failed Compatibility Binds"] != "No" or values["Search Continues After Compatibility Failure"] != "Yes":
            raise RuntimeError("compatibility continuation rule is wrong")
        if values["Search Extension Beyond Nine Interiors"] != "No" or values["Second-Best Reference May Replace Failed Min/Max"] != "No":
            raise RuntimeError("compatibility search limit is wrong")
        if "NOT_PRE_CLOSE_COMPATIBILITY_V10" not in values["Configuration Version"]:
            raise RuntimeError("configuration version is not V10")
        if values["Pre-Close Interior Candle Eligible As Reference"] != "No" or values["Reference Row Maximum"] != "close_row - 2":
            raise RuntimeError("pre-close reference exclusion is missing")
        if values["Reference OHLC Exported"] != "Open, High, Low, Close":
            raise RuntimeError("reference OHLC export is missing")
        if values["First Eligible Swing Close Offset"] != "open_row + 7" or values["Final Eligible Swing Close Offset"] != "open_row + 10":
            raise RuntimeError("close offsets are not +7 and +10")
        counts = {}
        for sheet_name in ("Special Swings", "Swing Highs", "Swing Lows"):
            rows = [row for row in normal[sheet_name].iter_rows(min_row=2, values_only=True) if row[0]]
            counts[sheet_name] = len(rows)
            for row in rows:
                if row[2] != FORMATION_CLASS:
                    raise RuntimeError("non-standard formation in workbook")
                interior = int(row[3])
                total = int(row[4])
                if interior < 6 or interior > 9 or total < 8 or total > 11 or total != interior + 2:
                    raise RuntimeError("workbook duration is outside 6-9 interiors")
                if interior == 10 or interior < 6:
                    raise RuntimeError("obsolete duration retained")
                if _cell_decimal(row[57]) < MIN_BOUNDARY or _cell_decimal(row[58]) < MIN_BOUNDARY:
                    raise RuntimeError("workbook boundary is below 0.90")
                if "UTC" in str(row[5]) or "UTC" in str(row[12]):
                    raise RuntimeError("UTC timestamp leaked into a result sheet")
                if row[1] == SWING_HIGH and (row[7] != "BULLISH" or row[14] != "BEARISH"):
                    raise RuntimeError("swing high direction labels are wrong")
                if row[1] != SWING_HIGH and (row[7] != "BEARISH" or row[14] != "BULLISH"):
                    raise RuntimeError("swing low direction labels are wrong")
                _validate_reference_row(row, result)
        expected = len(result.displayed)
        highs = sum(1 for item in result.displayed if item.direction == SWING_HIGH)
        lows = expected - highs
        if counts != {"Special Swings": expected, "Swing Highs": highs, "Swing Lows": lows}:
            raise RuntimeError(f"row counts {counts}")
        return {"sheets": list(normal.sheetnames), "rows": counts, "readable": True, "macros": False, "external_links": False}
    finally:
        normal.close()
        readonly.close()
        data_only.close()
