"""Fixed ADAUSDT entry point for the shared price-touch hierarchy detector.

Importing this module does not load market data or publish a workbook.
"""
from __future__ import annotations

from detectors.price_touch_hierarchy_v1.run_price_touch_hierarchy import run_for_symbol_cli

SYMBOL = "ADAUSDT"


def main() -> int:
    code = run_for_symbol_cli(SYMBOL)
    if code == 0:
        print(f"{SYMBOL} STATUS SUCCESS", flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
