"""
NEXUS Backtest Engine — echte, lookahead-vrije walk-forward validatie.

Beantwoordt de enige vraag die telt: "Als ik in het verleden telkens de
hoogst-scorende Convergence/Momentum-aandelen had gekozen, had ik dan de
index (SPY) verslagen — en is dat verschil échte skill of toeval?"

Kernprincipes (waarom dit te vertrouwen is):
  1. GEEN lookahead-bias. Op elke herbalanceer-datum T wordt de koershistorie
     afgekapt op T en exact dezelfde scoring gedraaid als het live systeem
     (via momentum_engine.indicators_from_hist + score_from_indicators).
     Er wordt nooit toekomstige data gebruikt om een keuze te maken.
  2. Test-wat-je-draait. De backtest importeert de live scoring-functies;
     er is geen tweede, afwijkende implementatie die kan wegdriften.
  3. Realistische kosten. Elke herbalancering rekent transactiekosten.
  4. Eerlijk over beperkingen. Zie CAVEATS onderaan de output.

Gratis: gebruikt alleen yfinance. Data wordt lokaal gecachet zodat een
tweede run instant is.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parent))
import momentum_engine as me

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backtest_engine")

ROOT = Path(__file__).resolve().parent.parent
CACHE_PATH = ROOT / "data" / "backtest_prices.pkl"
RESULTS_PATH = ROOT / "backtest_results.json"

BENCHMARK = "SPY"
TRADING_DAYS_PER_YEAR = 252
MIN_HISTORY_DAYS = 220        # momentum_engine requires this many rows to score


# ── Universe ──────────────────────────────────────────────────────────────────

def load_universe() -> list[str]:
    """
    Reuse the live scanner's growth universe + fallbacks so we backtest the
    same kind of names we actually trade. Kept static (not scraped) for
    reproducibility. NB: this introduces mild survivorship bias — see CAVEATS.
    """
    try:
        from tier1_scanner import GROWTH_UNIVERSE, FALLBACK_TICKERS
        universe = list(dict.fromkeys(list(GROWTH_UNIVERSE) + list(FALLBACK_TICKERS)))
    except Exception as e:
        log.warning("Kon universe niet uit tier1_scanner laden (%s), gebruik ingebouwde lijst", e)
        universe = [
            "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "AVGO", "TSLA", "AMD", "CRM",
            "ADBE", "NFLX", "COST", "LLY", "V", "MA", "UNH", "HD", "PG", "JPM",
            "ORCL", "NOW", "CRWD", "PANW", "SNOW", "ANET", "MU", "QCOM", "INTC", "AMAT",
            "ISRG", "REGN", "VRTX", "BKNG", "MELI", "SHOP", "UBER", "ABNB", "PLTR", "DELL",
        ]
    # Never let the benchmark leak into the tradable universe
    return [t for t in universe if t != BENCHMARK]


# ── Data ──────────────────────────────────────────────────────────────────────

def download_prices(tickers: list[str], years: int, use_cache: bool = True) -> dict[str, pd.DataFrame]:
    """
    Download auto-adjusted daily OHLCV per ticker (matches the live engine's
    .history() adjustment). Cached to disk; delete data/backtest_prices.pkl to
    force a refresh.
    """
    if use_cache and CACHE_PATH.exists():
        try:
            cached = pd.read_pickle(CACHE_PATH)
            if cached.get("_years") == years and set(cached.get("_tickers", [])) >= set(tickers + [BENCHMARK]):
                log.info("Prijsdata uit cache geladen (%s)", CACHE_PATH.name)
                return cached["frames"]
        except Exception as e:
            log.warning("Cache onbruikbaar (%s), opnieuw downloaden", e)

    all_tickers = tickers + [BENCHMARK]
    log.info("Downloaden van %d tickers, %d jaar historie...", len(all_tickers), years)
    period = f"{years}y"

    raw = yf.download(
        all_tickers, period=period, interval="1d",
        auto_adjust=True, group_by="ticker", threads=True, progress=False,
    )

    frames: dict[str, pd.DataFrame] = {}
    for t in all_tickers:
        try:
            df = raw[t] if len(all_tickers) > 1 else raw
            df = df.dropna(subset=["Close"])
            if len(df) >= MIN_HISTORY_DAYS:
                frames[t] = df
        except Exception:
            continue

    log.info("Bruikbare tickers met voldoende historie: %d/%d", len(frames), len(all_tickers))

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.to_pickle({"_years": years, "_tickers": all_tickers, "frames": frames}, CACHE_PATH)
    return frames


# ── Rebalance date generation ─────────────────────────────────────────────────

def rebalance_dates(index: pd.DatetimeIndex, freq_months: int, warmup_days: int) -> list[pd.Timestamp]:
    """
    Trading days on which we re-pick the portfolio. Starts only after enough
    warmup history exists to score, then steps ~freq_months apart.
    """
    if len(index) <= warmup_days:
        return []
    start = index[warmup_days]
    candidates = pd.date_range(start=start, end=index[-1], freq=f"{freq_months}MS")
    dates = []
    for cand in candidates:
        pos = index.searchsorted(cand)
        if pos < len(index):
            dates.append(index[pos])
    # De-dupe while preserving order
    return list(dict.fromkeys(dates))


# ── Core backtest ─────────────────────────────────────────────────────────────

def _spy_trailing_6m(spy_close: pd.Series, as_of: pd.Timestamp) -> float:
    window = spy_close.loc[:as_of]
    if len(window) >= 126:
        return float(window.iloc[-1] / window.iloc[-126] - 1)
    return 0.05


def score_universe_at(frames: dict[str, pd.DataFrame], tickers: list[str],
                      as_of: pd.Timestamp, spy_6m: float) -> list[tuple[str, float]]:
    """
    Point-in-time S_Momentum for every ticker, using ONLY data up to `as_of`.
    Returns (ticker, score) pairs, highest first.
    """
    scored = []
    for t in tickers:
        df = frames.get(t)
        if df is None:
            continue
        hist = df.loc[:as_of]
        if len(hist) < MIN_HISTORY_DAYS:
            continue
        ind = me.indicators_from_hist(hist, spy_6m)
        if ind is None:
            continue
        res = me.score_from_indicators(ind)
        scored.append((t, res["s_momentum"]))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


def forward_return(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> float | None:
    """Adjusted-close return from start to end. None if data is missing."""
    try:
        s = df["Close"].loc[:start]
        e = df["Close"].loc[:end]
        if s.empty or e.empty:
            return None
        p0, p1 = float(s.iloc[-1]), float(e.iloc[-1])
        if p0 <= 0:
            return None
        return p1 / p0 - 1
    except Exception:
        return None


def run_backtest(years: int = 6, freq_months: int = 3, top_n: int = 10,
                 min_score: float = 3.0, cost_bps: float = 10.0,
                 use_cache: bool = True) -> dict:
    """
    Walk-forward backtest. cost_bps = round-trip transaction cost per
    rebalance in basis points (10 bps = 0.10%).
    """
    universe = load_universe()
    frames = download_prices(universe, years, use_cache=use_cache)

    if BENCHMARK not in frames:
        raise RuntimeError(f"Benchmark {BENCHMARK} data ontbreekt — kan niet valideren.")

    spy = frames[BENCHMARK]
    spy_close = spy["Close"]
    tradable = [t for t in universe if t in frames]

    # Master trading calendar = SPY's index (the benchmark is always present)
    calendar = spy_close.index
    warmup = MIN_HISTORY_DAYS + 5
    rebals = rebalance_dates(calendar, freq_months, warmup)
    if len(rebals) < 2:
        raise RuntimeError("Te weinig historie voor een zinvolle backtest.")

    log.info("Herbalanceringen: %d (elke %d mnd), universe: %d tickers",
             len(rebals), freq_months, len(tradable))

    cost = cost_bps / 10000.0
    strat_equity = 100.0
    bench_equity = 100.0
    equity_curve = [{"date": rebals[0].strftime("%Y-%m-%d"), "strategy": 100.0, "benchmark": 100.0}]
    rebal_log = []
    excess_returns = []

    for i in range(len(rebals) - 1):
        t0, t1 = rebals[i], rebals[i + 1]
        spy_6m = _spy_trailing_6m(spy_close, t0)

        ranked = score_universe_at(frames, tradable, t0, spy_6m)
        picks = [(t, s) for t, s in ranked if s >= min_score][:top_n]

        # Strategy period return
        if picks:
            rets = [forward_return(frames[t], t0, t1) for t, _ in picks]
            rets = [r for r in rets if r is not None]
            gross = float(np.mean(rets)) if rets else 0.0
            period_ret = gross - cost            # pay cost to rotate in
            held = [{"ticker": t, "score": round(s, 2),
                     "ret": round((forward_return(frames[t], t0, t1) or 0.0) * 100, 2)}
                    for t, s in picks]
        else:
            # Niets kwalificeert → in cash (0% i.p.v. gokken). Dit is een
            # bewuste, verdedigbare keuze en voorkomt geforceerde slechte trades.
            period_ret = 0.0
            held = []

        bench_ret = forward_return(spy, t0, t1) or 0.0

        strat_equity *= (1 + period_ret)
        bench_equity *= (1 + bench_ret)
        excess_returns.append(period_ret - bench_ret)

        equity_curve.append({
            "date": t1.strftime("%Y-%m-%d"),
            "strategy": round(strat_equity, 2),
            "benchmark": round(bench_equity, 2),
        })
        rebal_log.append({
            "date": t0.strftime("%Y-%m-%d"),
            "n_picks": len(held),
            "period_return": round(period_ret * 100, 2),
            "benchmark_return": round(bench_ret * 100, 2),
            "excess": round((period_ret - bench_ret) * 100, 2),
            "picks": held,
        })

    metrics = _compute_metrics(equity_curve, rebal_log, excess_returns, freq_months)

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "years": years,
            "rebalance_months": freq_months,
            "top_n": top_n,
            "min_score": min_score,
            "cost_bps": cost_bps,
            "universe_size": len(tradable),
            "benchmark": BENCHMARK,
            "engine": "S_Momentum (Minervini Stage-2 + VCP + volume + RSI/MACD)",
        },
        "metrics": metrics,
        "equity_curve": equity_curve,
        "rebalances": rebal_log,
        "caveats": [
            "Survivorship bias: de universe-lijst bevat de huidige namen; "
            "in het verleden gedelistte verliezers ontbreken, wat resultaten iets flatteert.",
            "Alleen de MOMENTUM-engine (S_Momentum) wordt getest — die is volledig uit "
            "koershistorie te reproduceren. De fundamentele S_Growth-engine vereist "
            "point-in-time fundamentals die gratis data niet betrouwbaar levert, en zit "
            "dus (nog) niet in deze backtest.",
            "Geen slippage/marktimpact gemodelleerd, alleen een vaste transactiekost.",
            "Verleden rendement is geen garantie voor de toekomst.",
        ],
    }
    return result


def _max_drawdown(values: list[float]) -> float:
    peak = -math.inf
    max_dd = 0.0
    for v in values:
        peak = max(peak, v)
        if peak > 0:
            max_dd = min(max_dd, v / peak - 1)
    return max_dd


def _compute_metrics(equity_curve, rebal_log, excess_returns, freq_months) -> dict:
    strat_vals = [p["strategy"] for p in equity_curve]
    bench_vals = [p["benchmark"] for p in equity_curve]
    n_periods = len(rebal_log)
    periods_per_year = 12.0 / freq_months
    years_elapsed = n_periods / periods_per_year if periods_per_year else 1

    strat_total = strat_vals[-1] / strat_vals[0] - 1
    bench_total = bench_vals[-1] / bench_vals[0] - 1
    strat_cagr = (strat_vals[-1] / strat_vals[0]) ** (1 / years_elapsed) - 1 if years_elapsed > 0 else 0
    bench_cagr = (bench_vals[-1] / bench_vals[0]) ** (1 / years_elapsed) - 1 if years_elapsed > 0 else 0

    period_rets = [r["period_return"] / 100 for r in rebal_log]
    vol = float(np.std(period_rets, ddof=1)) if len(period_rets) > 1 else 0.0
    ann_vol = vol * math.sqrt(periods_per_year)
    mean_ret = float(np.mean(period_rets)) if period_rets else 0.0
    ann_ret = mean_ret * periods_per_year
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0

    beats = sum(1 for r in rebal_log if r["excess"] > 0)
    hit_rate = beats / n_periods if n_periods else 0.0

    # Statistische significantie: is de gemiddelde excess-return echt > 0,
    # of kan het toeval zijn? Eenzijdige t-test (t = mean/(sd/sqrt(n))).
    exc = np.array(excess_returns)
    if len(exc) > 1 and exc.std(ddof=1) > 0:
        t_stat = float(exc.mean() / (exc.std(ddof=1) / math.sqrt(len(exc))))
    else:
        t_stat = 0.0

    return {
        "strategy_total_return": round(strat_total * 100, 2),
        "benchmark_total_return": round(bench_total * 100, 2),
        "strategy_cagr": round(strat_cagr * 100, 2),
        "benchmark_cagr": round(bench_cagr * 100, 2),
        "alpha_cagr": round((strat_cagr - bench_cagr) * 100, 2),
        "hit_rate": round(hit_rate * 100, 1),
        "periods": n_periods,
        "max_drawdown": round(_max_drawdown(strat_vals) * 100, 2),
        "benchmark_max_drawdown": round(_max_drawdown(bench_vals) * 100, 2),
        "annualized_vol": round(ann_vol * 100, 2),
        "sharpe": round(sharpe, 2),
        "t_stat": round(t_stat, 2),
        "significant": bool(abs(t_stat) >= 2.0),
        "verdict": _verdict(strat_cagr - bench_cagr, hit_rate, t_stat, n_periods),
    }


def _verdict(alpha: float, hit_rate: float, t_stat: float, n: int) -> str:
    if n < 8:
        return "ONVOLDOENDE DATA — te weinig herbalanceringen voor een betrouwbaar oordeel."
    if alpha <= 0:
        return "GEEN EDGE — de strategie verslaat de index niet. Koop een indexfonds."
    if t_stat >= 2.0 and hit_rate >= 55:
        return "STATISTISCH SIGNIFICANTE EDGE — verslaat de index consistent, waarschijnlijk skill."
    if alpha > 0:
        return "MOGELIJKE EDGE — verslaat de index, maar niet statistisch overtuigend. Meer data nodig."
    return "ONDUIDELIJK."


def main():
    ap = argparse.ArgumentParser(description="NEXUS walk-forward backtest")
    ap.add_argument("--years", type=int, default=6)
    ap.add_argument("--freq-months", type=int, default=3)
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--min-score", type=float, default=3.0)
    ap.add_argument("--cost-bps", type=float, default=10.0)
    ap.add_argument("--no-cache", action="store_true", help="Forceer verse download")
    args = ap.parse_args()

    result = run_backtest(
        years=args.years, freq_months=args.freq_months, top_n=args.top_n,
        min_score=args.min_score, cost_bps=args.cost_bps, use_cache=not args.no_cache,
    )

    with open(RESULTS_PATH, "w") as f:
        json.dump(result, f, indent=2)

    m = result["metrics"]
    print("\n" + "=" * 72)
    print("            NEXUS BACKTEST — VALIDATIE-RAPPORT")
    print("=" * 72)
    print(f"Periode:        {result['equity_curve'][0]['date']} → {result['equity_curve'][-1]['date']}")
    print(f"Herbalanceringen: {m['periods']} (elke {result['config']['rebalance_months']} mnd)")
    print(f"Universe:       {result['config']['universe_size']} tickers")
    print("-" * 72)
    print(f"Strategie CAGR:  {m['strategy_cagr']:+.2f}%   (totaal {m['strategy_total_return']:+.1f}%)")
    print(f"Benchmark CAGR:  {m['benchmark_cagr']:+.2f}%   (totaal {m['benchmark_total_return']:+.1f}%)")
    print(f"ALPHA (CAGR):    {m['alpha_cagr']:+.2f}%  per jaar")
    print(f"Hit rate:        {m['hit_rate']:.1f}%  van de periodes verslaat de index")
    print(f"Max drawdown:    {m['max_drawdown']:.1f}%  (index: {m['benchmark_max_drawdown']:.1f}%)")
    print(f"Sharpe:          {m['sharpe']:.2f}")
    print(f"t-statistiek:    {m['t_stat']:.2f}  ({'SIGNIFICANT' if m['significant'] else 'niet significant'})")
    print("-" * 72)
    print(f"OORDEEL: {m['verdict']}")
    print("=" * 72)
    print(f"\nVolledig rapport: {RESULTS_PATH}")


if __name__ == "__main__":
    main()
