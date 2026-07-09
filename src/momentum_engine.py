"""
NEXUS Momentum Engine — Short-Term Technical & Flow Signal Calculator

Implements the Short-Term Momentum Engine from the Nexus Pro blueprint:
  - Minervini Trend Template (8 concurrent criteria for Stage 2 classification)
  - Volatility Contraction Pattern (VCP) detection via ATR ratio & range tightness
  - Volume Buzz / Relative Volume (RVol) — intraday-proxied from daily EOD data
  - RSI(14) optimal zone + MACD(12,26,9) bullish histogram expansion
  - S_Momentum composite score [0–10]:
      S_Momentum = 3·M + 4·C + 2·V + 1·O + Alt_mod
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import yfinance as yf

log = logging.getLogger("momentum_engine")

# SPY 6-month return — cached once per process for RS rank approximation
_SPY_6M: float | None = None


def _spy_benchmark() -> float:
    global _SPY_6M
    if _SPY_6M is None:
        try:
            spy = yf.Ticker("SPY").history(period="9mo")["Close"]
            _SPY_6M = float(spy.iloc[-1] / spy.iloc[-126] - 1) if len(spy) >= 126 else 0.05
        except Exception:
            _SPY_6M = 0.05
    return _SPY_6M


# ── Indicator math helpers ────────────────────────────────────────────────────

def _sma(s: pd.Series, n: int) -> float:
    return float(s.iloc[-n:].mean()) if len(s) >= n else float("nan")


def _ema_last(s: pd.Series, n: int) -> float:
    return float(s.ewm(span=n, adjust=False).mean().iloc[-1]) if len(s) >= n else float("nan")


def _rsi(s: pd.Series, n: int = 14) -> float:
    if len(s) < n + 1:
        return float("nan")
    d    = s.diff().dropna()
    gain = d.clip(lower=0).rolling(n).mean()
    loss = (-d.clip(upper=0)).rolling(n).mean()
    rs   = gain / loss.replace(0, np.nan)
    return float((100 - 100 / (1 + rs)).iloc[-1])


def _macd(s: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9
          ) -> tuple[float, float, float]:
    if len(s) < slow + sig:
        return float("nan"), float("nan"), float("nan")
    ml = s.ewm(span=fast, adjust=False).mean() - s.ewm(span=slow, adjust=False).mean()
    sl = ml.ewm(span=sig, adjust=False).mean()
    return float(ml.iloc[-1]), float(sl.iloc[-1]), float((ml - sl).iloc[-1])


def _atr(h: pd.DataFrame, n: int) -> float:
    if len(h) < n + 1:
        return float("nan")
    tr = pd.concat([
        h["High"] - h["Low"],
        (h["High"] - h["Close"].shift(1)).abs(),
        (h["Low"]  - h["Close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    return float(tr.rolling(n).mean().iloc[-1])


# ── Raw indicator fetch ───────────────────────────────────────────────────────

def indicators_from_hist(hist: pd.DataFrame, spy_6m: float) -> dict | None:
    """
    Compute every indicator required for Stage 2 classification and VCP
    detection from a daily OHLCV DataFrame.

    IMPORTANT — this uses ONLY the data present in `hist`, treating the last
    row as "now". Feeding it a historical slice that ends at some past date T
    therefore yields a point-in-time, lookahead-free snapshot: exactly the
    indicators the live engine would have computed on date T. This is what
    lets the backtester test precisely what runs in production.

    `spy_6m` is SPY's trailing ~6-month return as of the same point in time,
    used for the relative-strength leadership criterion.

    Returns None if < 220 trading days of history are available.
    """
    if hist is None or len(hist) < 220:
        return None
    c, v = hist["Close"], hist["Volume"]
    price = float(c.iloc[-1])
    if price <= 0:
        return None

    sma200_now  = _sma(c, 200)
    sma200_prev = _sma(c.iloc[:-30], 200) if len(c) >= 230 else sma200_now
    sma200_slope = (sma200_now - sma200_prev) / sma200_prev if sma200_prev > 0 else 0.0

    vol_today = float(v.iloc[-1])
    vol_10d   = float(v.iloc[-11:-1].mean()) if len(v) >= 11 else vol_today
    rvol      = vol_today / vol_10d if vol_10d > 0 else 1.0

    stock_6m  = float(c.iloc[-1] / c.iloc[-126] - 1) if len(c) >= 126 else 0.0

    return {
        "price":        price,
        "ema10":        _ema_last(c, 10),
        "ema20":        _ema_last(c, 20),
        "sma50":        _sma(c, 50),
        "sma150":       _sma(c, 150),
        "sma200":       sma200_now,
        "sma200_slope": sma200_slope,
        "low52":        float(c.iloc[-252:].min()) if len(c) >= 252 else float(c.min()),
        "high52":       float(c.iloc[-252:].max()) if len(c) >= 252 else float(c.max()),
        "atr14":        _atr(hist, 14),
        "atr50":        _atr(hist, 50),
        "range1m":      (float(c.iloc[-21:].max()) - float(c.iloc[-21:].min())) / price,
        "rsi14":        _rsi(c, 14),
        "macd_line":    _macd(c)[0],
        "macd_sig":     _macd(c)[1],
        "macd_hist":    _macd(c)[2],
        "rvol":         rvol,
        "vol_buzz":     (rvol - 1.0) * 100.0,
        "vol_dryup":    (float(v.iloc[-20:].mean()) < float(v.iloc[-50:].mean()))
                        if len(v) >= 50 else False,
        "rs_leading":   stock_6m > spy_6m * 1.25,
    }


def _fetch_indicators(ticker: str) -> dict | None:
    """
    Download 15 months of daily OHLCV and compute every indicator via
    indicators_from_hist(). Live wrapper — uses the current SPY 6-month
    return for the RS criterion.
    Returns None if < 220 trading days of history are available.
    """
    try:
        hist = yf.Ticker(ticker).history(period="15mo")
        return indicators_from_hist(hist, _spy_benchmark())
    except Exception as e:
        log.debug("[%s] indicator fetch error: %s", ticker, e)
        return None


# ── Minervini Stage 2 Template ────────────────────────────────────────────────

def _stage2(ind: dict) -> bool:
    """
    Returns True only if all 8 Minervini Trend Template criteria are satisfied.

    Criteria:
      1. Price > SMA150 and Price > SMA200
      2. SMA150 > SMA200
      3. SMA200 rising for ≥ 30 consecutive trading days
      4. SMA50 > SMA150 and SMA50 > SMA200
      5. Price > SMA50
      6. Price ≥ 30% above its 52-week low
      7. Price within 25% of its 52-week high
      8. RS rank ≥ 75th percentile (approx: stock 6m return > SPY × 1.25)
    """
    p = ind["price"]
    for key in ("ema10", "ema20", "sma50", "sma150", "sma200"):
        if np.isnan(ind[key]):
            return False
    return all([
        p > ind["sma150"],
        p > ind["sma200"],
        ind["sma150"] > ind["sma200"],
        ind["sma200_slope"] > 0,
        ind["sma50"] > ind["sma150"],
        ind["sma50"] > ind["sma200"],
        p > ind["sma50"],
        ind["low52"] > 0 and (p - ind["low52"]) / ind["low52"] >= 0.30,
        ind["high52"] > 0 and (ind["high52"] - p) / ind["high52"] <= 0.25,
        ind["rs_leading"],
    ])


# ── Public API ────────────────────────────────────────────────────────────────

def compute_s_momentum(
    ticker:             str,
    sector_rrg:         str   = "Lagging",
    short_interest_pct: float = 0.0,
    days_to_cover:      float = 0.0,
) -> dict | None:
    """
    Compute Short-Term Momentum Score S_Momentum ∈ [0, 10].

    Formula:
        S_Momentum = 3·M + 4·C + 2·V + 1·O + Alt_mod

    M = Minervini MA alignment (0 or 1)
    C = VCP squeeze quality [0, 1]  — tighter range → higher score
    V = Volume Buzz scaled [0, 1]  — RVol 2.0 = full score
    O = RSI optimal (50–70) + MACD bullish (0 or 1)

    Alt_mod:
      +0.5 if sector in RRG Leading / Improving quadrant
      +1.0 if short squeeze setup (utilisation > 90%, DTC > 5)

    Returns None when < 220 days of price history are available.
    Also returns position-sizing fields:
      stop_loss_atr = entry − 1.5 × ATR14   (Engine 1 stop placement)
    """
    ind = _fetch_indicators(ticker)
    if ind is None:
        return None

    p = ind["price"]

    # M factor: full Minervini Stage 2 alignment
    m = 1.0 if _stage2(ind) else 0.0

    # C factor: VCP squeeze tightness
    atr_r = ind["atr14"] / p if (p > 0 and not np.isnan(ind["atr14"])) else 1.0
    vcp   = (
        atr_r < 0.05
        and not np.isnan(ind["atr50"])
        and ind["atr14"] < ind["atr50"]
        and ind["range1m"] < 0.10
    )
    c = max(0.0, min(1.0, 1.0 - atr_r / 0.05)) if vcp else 0.0

    # V factor: volume buzz (RVol 2.0 → full score, linearly)
    v = min(1.0, max(0.0, ind["vol_buzz"] / 100.0))

    # O factor: RSI in optimal zone AND MACD bullish crossover
    rsi   = ind["rsi14"]
    mbull = (
        not np.isnan(ind["macd_line"])
        and not np.isnan(ind["macd_sig"])
        and ind["macd_line"] > ind["macd_sig"]
    )
    o = 1.0 if (not np.isnan(rsi) and 50.0 <= rsi <= 70.0 and mbull) else 0.0

    # Alternative modifiers
    alt = 0.0
    if sector_rrg in ("Leading", "Improving"):
        alt += 0.5
    if short_interest_pct > 15.0 and days_to_cover > 5.0:
        alt += 1.0

    s = round(max(0.0, min(10.0, 3.0 * m + 4.0 * c + 2.0 * v + 1.0 * o + alt)), 2)

    return {
        "s_momentum":    s,
        "stage2":        _stage2(ind),
        "vcp_active":    vcp,
        "rvol":          round(ind["rvol"], 2),
        "vol_buzz_pct":  round(ind["vol_buzz"], 1),
        "vol_dryup":     bool(ind["vol_dryup"]),
        "rsi14":         round(rsi, 1) if not np.isnan(rsi) else None,
        "macd_bullish":  bool(mbull),
        "atr14":         round(ind["atr14"], 2) if not np.isnan(ind["atr14"]) else None,
        "atr_ratio_pct": round(atr_r * 100, 2),
        "m_factor":      m,
        "c_factor":      round(c, 3),
        "v_factor":      round(v, 3),
        "o_factor":      o,
        "sma50":         round(ind["sma50"], 2)  if not np.isnan(ind["sma50"])  else None,
        "sma200":        round(ind["sma200"], 2) if not np.isnan(ind["sma200"]) else None,
        "stop_loss_atr": round(p - 1.5 * ind["atr14"], 2) if not np.isnan(ind["atr14"]) else None,
    }
