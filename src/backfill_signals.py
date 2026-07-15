"""
NEXUS Signal Backfill — vul de signaal-database met historie.

Hergebruikt exact de lookahead-vrije machinery van backtest_engine: op elke
handelsdag T in het verleden wordt de koershistorie afgekapt op T, dezelfde
live scoring gedraaid (momentum_engine), en de gerealiseerde forward return
over elke vaste horizon gemeten. Elk (ticker, datum, horizon)-drieluik wordt
één gelabelde rij in nexus_signals.db.

Zo bouw je in één run een dataset van duizenden waarnemingen op waar de
kalibratie (signal_store.calibrated_confidence) op steunt — zonder ooit
toekomstige data te gebruiken om een signaal te bepalen.

Beperking (eerlijk): dit backfilt alleen TECHNISCHE features. Point-in-time
fundamentals en historisch social-sentiment zijn met gratis data niet
betrouwbaar te reconstrueren, dus die kolommen blijven NULL voor backfill-
rijen. Live signalen vullen ze wél. De kalibratie werkt daarom eerst op de
technische edge; naarmate live fundamentele/sentiment-signalen binnenkomen
wordt die laag rijker.

Gebruik:
    python src/backfill_signals.py                 # 6 jaar, wekelijkse samples
    python src/backfill_signals.py --years 8 --step-days 5
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import backtest_engine as bt
import momentum_engine as me
import signal_store as ss

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("backfill")


def _forward_return_at_horizon(close: pd.Series, t0: pd.Timestamp, horizon: int) -> float | None:
    """Return over `horizon` handelsdagen ná t0. None als er te weinig
    toekomstige data is (dan is de uitkomst nog onbekend — die rij slaan we
    over i.p.v. te raden)."""
    idx = close.index.searchsorted(t0)
    if idx >= len(close):
        return None
    end_idx = idx + horizon
    if end_idx >= len(close):
        return None
    p0 = float(close.iloc[idx])
    p1 = float(close.iloc[end_idx])
    if p0 <= 0:
        return None
    return p1 / p0 - 1


def run_backfill(years: int = 6, step_days: int = 5, use_cache: bool = True) -> dict:
    conn = ss.init_db()

    universe = bt.load_universe()
    frames = bt.download_prices(universe, years, use_cache=use_cache)
    if bt.BENCHMARK not in frames:
        raise RuntimeError(f"Benchmark {bt.BENCHMARK} ontbreekt.")

    spy = frames[bt.BENCHMARK]
    spy_close = spy["Close"]
    tradable = [t for t in universe if t in frames]
    calendar = spy_close.index

    warmup = bt.MIN_HISTORY_DAYS + 5
    max_horizon = max(ss.HORIZONS)
    # Alleen datums waarop we (a) genoeg historie hebben om te scoren en
    # (b) genoeg toekomst hebben om de langste horizon te realiseren.
    sample_positions = range(warmup, len(calendar) - max_horizon, step_days)
    sample_dates = [calendar[i] for i in sample_positions]
    log.info("Backfill: %d sampledatums × %d tickers, horizons %s",
             len(sample_dates), len(tradable), ss.HORIZONS)

    recorded = 0
    for n_date, t0 in enumerate(sample_dates, 1):
        spy_6m = bt._spy_trailing_6m(spy_close, t0)
        vix_regime = None  # historische VIX niet in frames; live vult dit
        spy_ma200 = float(spy_close.loc[:t0].iloc[-200:].mean()) if len(spy_close.loc[:t0]) >= 200 else None
        spy_now = float(spy_close.loc[:t0].iloc[-1])
        spy_above = 1 if (spy_ma200 and spy_now >= spy_ma200) else 0

        bench_fwd = {h: _forward_return_at_horizon(spy_close, t0, h) for h in ss.HORIZONS}

        for ticker in tradable:
            df = frames.get(ticker)
            if df is None:
                continue
            hist = df.loc[:t0]
            if len(hist) < bt.MIN_HISTORY_DAYS:
                continue
            ind = me.indicators_from_hist(hist, spy_6m)
            if ind is None:
                continue
            score = me.score_from_indicators(ind)

            features = {
                "s_momentum":    score["s_momentum"],
                "m_factor":      score["m_factor"],
                "c_factor":      score["c_factor"],
                "v_factor":      score["v_factor"],
                "o_factor":      score["o_factor"],
                "stage2":        score["stage2"],
                "vcp_active":    score["vcp_active"],
                "rsi14":         score["rsi14"],
                "macd_bullish":  score["macd_bullish"],
                "atr_ratio_pct": score["atr_ratio_pct"],
                "rvol":          score["rvol"],
                "vol_buzz_pct":  score["vol_buzz_pct"],
                # Geen point-in-time fundamentals in backfill: convergence == momentum
                "convergence_score": score["s_momentum"],
                "spy_above_ma200": spy_above,
            }

            close = df["Close"]
            for h in ss.HORIZONS:
                fwd = _forward_return_at_horizon(close, t0, h)
                if fwd is None or bench_fwd[h] is None:
                    continue
                ss.record_signal(
                    conn, ticker, t0.strftime("%Y-%m-%d"), "backtest", h,
                    features, forward_return=fwd, benchmark_return=bench_fwd[h],
                )
                recorded += 1

        if n_date % 20 == 0:
            conn.commit()
            log.info("... %d/%d datums verwerkt, %d rijen", n_date, len(sample_dates), recorded)

    conn.commit()
    result = ss.stats(conn)
    result["recorded_this_run"] = recorded
    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    log.info("Klaar. %d rijen toegevoegd. Totaal in DB: %d (%d met uitkomst).",
             recorded, result["total_signals"], result["realized_outcomes"])
    return result


def main():
    ap = argparse.ArgumentParser(description="Vul de NEXUS signaal-database met historie")
    ap.add_argument("--years", type=int, default=6)
    ap.add_argument("--step-days", type=int, default=5, help="Sample-interval in handelsdagen")
    ap.add_argument("--no-cache", action="store_true")
    args = ap.parse_args()

    res = run_backfill(years=args.years, step_days=args.step_days, use_cache=not args.no_cache)

    print("\n" + "=" * 64)
    print("           NEXUS SIGNAL BACKFILL — RESULTAAT")
    print("=" * 64)
    print(f"Rijen deze run:       {res['recorded_this_run']}")
    print(f"Totaal in database:   {res['total_signals']}")
    print(f"Met bekende uitkomst: {res['realized_outcomes']}")
    print(f"Beat-rate (alles):    {res['overall_beat_rate']}")
    print("=" * 64)
    print("\nTest de kalibratie nu met: python src/signal_cli.py confidence --score 7.5")


if __name__ == "__main__":
    main()
