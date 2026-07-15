"""Central configuration for the Nexus trading bot.

Parameters under STRATEGY_PARAMS are the ones self_learning.py is allowed
to rewrite after walk-forward optimization. Everything else is fixed.
"""
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# --- Alpaca ---------------------------------------------------------------
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
# Paper trading only. Do not point this at the live endpoint.
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
ALPACA_DATA_FEED = os.getenv("ALPACA_DATA_FEED", "iex")  # iex = free tier

# --- Telegram --------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# --- Instrument universe ----------------------------------------------------
MEAN_REVERSION_SYMBOLS = ["SPY", "QQQ"]
MOMENTUM_SYMBOLS = ["BTC/USD"]
TREND_SYMBOLS = ["GLD", "USO"]
ALL_SYMBOLS = MEAN_REVERSION_SYMBOLS + MOMENTUM_SYMBOLS + TREND_SYMBOLS

# --- Risk management ---------------------------------------------------------
RISK_PER_TRADE_PCT = 0.01          # 1% of equity risked per trade (1 ATR move)
MAX_PORTFOLIO_DRAWDOWN_PCT = 0.10  # circuit breaker trips at 10% DD from peak equity
ATR_LOOKBACK_DAYS = 14
CORRELATION_BLOCK_ENABLED = True

# --- Self-learning / walk-forward optimization -------------------------------
WFO_LOOKBACK_DAYS = 30
WFO_TRIALS = 60
WFO_MIN_SHARPE_IMPROVEMENT = 0.10   # require >=10% relative Sharpe improvement to adopt
WFO_MIN_DRAWDOWN_IMPROVEMENT = 0.0  # new DD must be <= old DD (0 = no worse)
LEARNING_LOG_PATH = DATA_DIR / "learning_log.csv"
TRADES_LOG_PATH = DATA_DIR / "trades.csv"
DAILY_PNL_LOG_PATH = DATA_DIR / "daily_pnl.csv"

# --- Strategy parameters (mutable — self_learning.py may overwrite these) ----
STRATEGY_PARAMS = {
    "mean_reversion": {
        "SPY": {"sma_period": 20, "entry_sd": 1.5, "timeframe": "15Min"},
        "QQQ": {"sma_period": 20, "entry_sd": 1.8, "timeframe": "15Min"},
        "sd_min": 1.2,
        "sd_max": 2.5,
    },
    "momentum_breakout": {
        "BTC/USD": {
            "lookback_period": 20,
            "volume_multiplier": 1.5,
            "atr_multiplier": 2.0,
            "timeframe": "1Hour",
        },
    },
    "trend_following": {
        "GLD": {"fast_ema": 50, "slow_ema": 200, "atr_multiplier": 3.0, "timeframe": "4Hour"},
        "USO": {"fast_ema": 50, "slow_ema": 200, "atr_multiplier": 3.0, "timeframe": "4Hour"},
    },
}


def save_strategy_params(new_params: dict) -> None:
    """Persist an updated STRATEGY_PARAMS dict back into this file.

    Used by self_learning.py after a walk-forward optimization run that
    finds a better parameter set. Rewrites only the STRATEGY_PARAMS block.
    """
    import re

    text = Path(__file__).read_text()
    block_start = text.index("STRATEGY_PARAMS = {")
    block_end = text.index("\n}\n", block_start) + 3
    new_block = "STRATEGY_PARAMS = " + _pretty_dict(new_params) + "\n"
    updated = text[:block_start] + new_block + text[block_end:]
    Path(__file__).write_text(updated)


def _pretty_dict(d: dict, indent: int = 0) -> str:
    import json
    return json.dumps(d, indent=4)


@dataclass
class RuntimeState:
    """In-memory state shared across the async main loop."""
    peak_equity: float = 0.0
    trading_paused: bool = False
    open_positions: dict = field(default_factory=dict)
