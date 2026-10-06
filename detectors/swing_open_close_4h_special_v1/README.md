# 4H Swing Special

This detector finds BTCUSDT 4h special swings from a Swing Open, a strictly interior extreme, and the first Swing Close that reaches the Swing Open price.

Only the Standard reference-return method is enabled. Compact and Alternative detection are disabled. Two formation classes are valid. A zero-interior structure is exactly two adjacent candles, returns fully to the Swing Open price, and takes its extreme from the higher high or lower low of those endpoints. A normal Standard structure has 1 to 5 interior candles, a strictly interior extreme, and confirms when the Swing Close penetrates one-third of the Swing Open real body. Wicks are not part of that body. Both boundary percentages must be at least 1.30%. There is no maximum boundary percentage. Both boundary percentages must sit inside 1.30% to 3.50%, inclusive. The shortest valid confirmed formation wins. Derived, nested, and longer overlapping copies stay in the technical report and do not appear in the workbook.

The original `swing_open_close_4h_v1` detector is a separate package and is not used at runtime.
