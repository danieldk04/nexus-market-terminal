"""
NEXUS Outcome Updater — realiseer uitkomsten van live signalen.

Loopt alle live-signalen af waarvan de uitkomst nog NULL is, en vult de
forward return (t.o.v. het instapmoment) + de benchmark-return over dezelfde
periode in zodra de horizon verstreken is. Signalen waarvan de horizon nog
niet om is, worden overgeslagen (geen giswerk).

Draai dit periodiek (bv. dagelijks/wekelijks in de workflow). Naarmate live
signalen 'rijpen', groeit het aantal gerealiseerde rijen met fundamentele +
sentiment-features — de basis voor multi-factor kalibratie.

Gebruikt yfinance voor de koersen (net als de rest van het systeem).
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
import yfinance as yf

import signal_store as ss

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("update_outcomes")

BENCHMARK = "SPY"

# Simpele in-run cache zodat we per ticker maar één keer downloaden.
_price_cache: dict[str, pd.Series] = {}


def _close_series(ticker: str) -> pd.Series | None:
    if ticker in _price_cache:
        return _price_cache[ticker]
    try:
        hist = yf.Ticker(ticker).history(period="2y", auto_adjust=True)
        series = hist["Close"].dropna()
        series.index = series.index.tz_localize(None)
        _price_cache[ticker] = series
        return series
    except Exception as e:
        log.debug("Koers-fetch faalde voor %s: %s", ticker, e)
        return None


def _forward_return(close: pd.Series, from_date: str, horizon: int) -> float | None:
    """Return over `horizon` handelsdagen na from_date. None als de horizon
    nog niet verstreken is (te weinig toekomstige koersen)."""
    try:
        t0 = pd.Timestamp(from_date)
    except Exception:
        return None
    idx = close.index.searchsorted(t0)
    if idx >= len(close):
        return None
    end_idx = idx + horizon
    if end_idx >= len(close):
        return None  # horizon nog niet om
    p0, p1 = float(close.iloc[idx]), float(close.iloc[end_idx])
    if p0 <= 0:
        return None
    return p1 / p0 - 1


def _price_lookup(ticker: str, from_date: str, horizon: int):
    """(forward_return, benchmark_return) of None als nog niet realiseerbaar."""
    stock = _close_series(ticker)
    bench = _close_series(BENCHMARK)
    if stock is None or bench is None:
        return None
    fwd = _forward_return(stock, from_date, horizon)
    if fwd is None:
        return None
    bench_ret = _forward_return(bench, from_date, horizon)
    if bench_ret is None:
        return None
    return fwd, bench_ret


def main():
    print("=== NEXUS OUTCOME UPDATER ===")
    conn = ss.init_db()
    before = ss.stats(conn)
    updated = ss.update_pending_outcomes(conn, _price_lookup)
    after = ss.stats(conn)
    log.info("Uitkomsten gerealiseerd deze run: %d", updated)
    log.info("DB: %d signalen, %d met uitkomst (was %d)",
             after["total_signals"], after["realized_outcomes"], before["realized_outcomes"])


if __name__ == "__main__":
    main()
