"""Run the shared 30-minute special swing detector for BTCUSDT only."""
from detectors.swing_open_close_30m_special_v1.run_swing_open_close_30m_special_detector import run_for_symbol_cli

SYMBOL = "BTCUSDT"


def main() -> int:
    return run_for_symbol_cli(SYMBOL)


if __name__ == "__main__":
    raise SystemExit(main())
