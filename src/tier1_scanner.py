"""
NEXUS Tier-1 Scanner — Dual-Engine Convergence Zone Orchestrator

Implements the Nexus Pro Dual-Engine architecture:

  Phase 1 — Fundamental Filter (S_Growth, quarterly, all tickers)
    • Revenue & EPS acceleration (3 consecutive YoY quarters)
    • ROIC ≥ 15%, FCF Margin ≥ 10%
    • Operating leverage (DOL vs GICS sector target)
    • Valuation: PEG < 1.5 and sector-relative PE Z-score

  Phase 2 — Momentum Overlay (S_Momentum, daily, top-80 fundamental candidates)
    • Minervini Trend Template (Stage 2 — 8 concurrent criteria)
    • Volatility Contraction Pattern (ATR ratio + range tightness)
    • Relative Volume / Volume Buzz
    • RSI(14) + MACD bullish crossover

  Convergence Zone = tickers where both S_Growth ≥ 7.5 AND S_Momentum ≥ 7.5
  Convergence_Score = (S_Growth + S_Momentum) / 2
"""
import json
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from io import StringIO

from dcf_engine import compute_dcf, compute_roic, compute_roce, check_dividend_sustainability
import fundamental_engine as fe
import momentum_engine as me
from notifier import notify_scan_complete

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tier1_scanner")

BASE_DIR    = Path(__file__).parent.parent
OUTPUT_PATH = BASE_DIR / "data.json"
MEMORY_PATH = BASE_DIR / "memory.json"

SECTOR_MAP = {
    "Technology":              "Tech & AI",
    "Communication Services":  "Tech & AI",
    "Software":                "Tech & AI",
    "Information Technology":  "Tech & AI",
    "Financial Services":      "Financials",
    "Financial Data Services": "Financials",
    "Banks":                   "Financials",
    "Insurance":               "Financials",
    "Healthcare":              "Healthcare",
    "Biotechnology":           "Healthcare",
    "Pharmaceuticals":         "Healthcare",
    "Medical Devices":         "Healthcare",
    "Energy":                  "Energy",
    "Oil & Gas":               "Energy",
    "Basic Materials":         "Materials",
    "Utilities":               "Utilities",
    "Real Estate":             "Real Estate",
    "Consumer Defensive":      "Consumer Defensive",
    "Consumer Cyclical":       "Consumer Cyclical",
    "Industrials":             "Industrials",
}

FALLBACK_TICKERS = [
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","JPM","V","MA","LLY",
    "AVGO","COST","INTU","ISRG","TMO","ACN","NOW","AMAT","AMD","ARM",
    "CRWD","ZS","PANW","NET","DDOG","SNOW","MDB","GTLB","APP","TTD",
    "PLTR","AXON","MELI","ASML","TSM","NVO","SHOP","MRVL","SMCI","UBER",
]

# Priority growth-theme universe — always at the front of the scan queue
# and used as the thematic bonus proxy for insider/13F accumulation signal.
GROWTH_UNIVERSE = [
    # Semiconductors & AI Infrastructure
    "NVDA","AMD","ARM","SMCI","MRVL","AVGO","AMAT","LRCX","KLAC","ONTO","ACMR",
    # Cybersecurity
    "CRWD","ZS","PANW","NET","DDOG","FTNT","S","CYBR","OKTA","QLYS",
    # Cloud & Enterprise Software
    "NOW","HUBS","SNOW","MDB","GTLB","APP","TTD","MNDY","BILL","AXON",
    # Defense & Space
    "PLTR","RKLB","LDOS","LHX","NOC",
    # Fintech & Payments
    "NU","SOFI","AFRM","UPST","ADYEN.AS",
    # Healthcare Innovation
    "MRNA","VRTX","REGN","EXAS",
    # Energy Transition
    "CEG","VST","FSLR","ENPH",
    # Global growth leaders
    "ASML","ASML.AS","TSM","NVO","MELI","SEA","SHOP","UBER","TSLA",
    # Netherlands
    "BESI.AS","TKWY.AS",
]

GROWTH_THEMES = set(GROWTH_UNIVERSE)

MAX_SCAN = 400   # maximum tickers in Phase 1
MOMENTUM_TOP = 80  # top-N fundamentals that receive a Phase 2 momentum scan
TOP_N    = 40    # final candidates saved to data.json


def get_industry_group(sector: str) -> str:
    return SECTOR_MAP.get(sector, "Others")


def load_memory() -> dict:
    if not MEMORY_PATH.exists():
        return {"lessons": []}
    with open(MEMORY_PATH) as f:
        return json.load(f)


def fetch_global_universe() -> list[str]:
    tickers: list[str] = []
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NEXUSBot/3.0)"}

    # S&P 500
    try:
        res   = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=headers, timeout=15,
        )
        sp500 = pd.read_html(StringIO(res.text))[0]["Symbol"].str.replace(".", "-", regex=False).tolist()
        tickers.extend(sp500)
        log.info("S&P 500: %d tickers", len(sp500))
    except Exception as e:
        log.warning("S&P 500 scraping failed: %s", e)

    # Nasdaq-100
    for idx in [4, 3, 5, 2]:
        try:
            res    = requests.get("https://en.wikipedia.org/wiki/Nasdaq-100", headers=headers, timeout=15)
            tables = pd.read_html(StringIO(res.text))
            for col in ("Ticker", "Symbol", "Tick"):
                if col in tables[idx].columns:
                    ndx = tables[idx][col].dropna().tolist()
                    tickers.extend(ndx)
                    log.info("Nasdaq-100: %d tickers (table %d)", len(ndx), idx)
                    break
            break
        except Exception:
            continue

    # Dutch names
    tickers.extend(["ASML.AS","INGA.AS","ADYEN.AS","UNA.AS","HEIA.AS","PHIA.AS","WKL.AS"])

    if len(tickers) < 60:
        log.warning("Scraping insufficient — using fallback list")
        tickers.extend(FALLBACK_TICKERS)

    # Growth universe always at the front so it fits within MAX_SCAN
    growth_set = set(GROWTH_UNIVERSE)
    others     = [t for t in tickers if t not in growth_set]
    unique     = list(dict.fromkeys(GROWTH_UNIVERSE + others))
    log.info("Universe: %d unique tickers (%d growth-priority)", len(unique), len(GROWTH_UNIVERSE))
    return unique


def fetch_macro() -> dict:
    macro = {"vix": None, "treasury_10y": None}
    try:
        macro["vix"] = round(yf.Ticker("^VIX").info.get("regularMarketPrice", 0) or 0, 2)
    except Exception:
        pass
    try:
        macro["treasury_10y"] = round(yf.Ticker("^TNX").info.get("regularMarketPrice", 0) or 0, 2)
    except Exception:
        pass
    log.info("Macro: VIX=%s  10Y=%s%%", macro["vix"], macro["treasury_10y"])
    return macro


def compute_5yr_data(t) -> dict:
    result = {"rev_cagr_5yr": None, "ni_cagr_5yr": None}
    try:
        fin = t.financials
        if fin is None or fin.empty:
            return result

        def _cagr(keys):
            for key in keys:
                matches = [k for k in fin.index if key in str(k)]
                if matches:
                    series = fin.loc[matches[0]].dropna()
                    if len(series) >= 2:
                        n, o, y = float(series.iloc[0]), float(series.iloc[-1]), len(series) - 1
                        if o > 0 and n > 0:
                            return round(((n / o) ** (1 / y) - 1) * 100, 1)
            return None

        result["rev_cagr_5yr"] = _cagr(["Total Revenue", "Revenue"])
        result["ni_cagr_5yr"]  = _cagr(["Net Income", "Net Income Common Stockholders"])
    except Exception:
        pass
    return result


# ── Phase 1: Fundamental analysis ─────────────────────────────────────────────

def analyse_ticker_fundamental(ticker_symbol: str, memory: dict, post_mortem: dict) -> dict | None:
    """
    Run fundamental analysis and compute S_Growth for a single ticker.
    Hard filters eliminate tickers that are structurally unsuitable for
    growth investing before the (expensive) momentum scan runs.
    Returns None when the ticker fails any hard filter.
    """
    try:
        t    = yf.Ticker(ticker_symbol)
        info = t.info

        # Filter 1: minimum market cap $500M (avoid illiquid micro-caps)
        market_cap = info.get("marketCap", 0) or 0
        if market_cap > 0 and market_cap < 500_000_000:
            return None

        sector = info.get("sector", "Unknown")
        group  = get_industry_group(sector)

        # Filter 2: structural debt ceiling
        de_raw   = info.get("debtToEquity", 0) or 0
        de_ratio = (de_raw / 100) if de_raw > 5 else de_raw
        if de_ratio > 3.0:
            return None

        # Filter 3: declining revenue AND unprofitable = no growth path
        rev_growth    = info.get("revenueGrowth", 0) or 0
        profit_margin = info.get("profitMargins", 0) or 0
        if rev_growth < -0.10 and profit_margin < 0:
            return None

        # Trend filter: deep downtrend + revenue declining → skip
        price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
        ma50  = info.get("fiftyDayAverage") or 0
        ma200 = info.get("twoHundredDayAverage") or 0
        if ma50 > 0 and ma200 > 0 and price < ma50 * 0.88 and price < ma200 * 0.88 and rev_growth < 0:
            return None

        # Trend penalty (for score adjustment, not a hard filter)
        trend_penalty = 0.0
        if ma50 > 0 and price < ma50 * 0.97:
            trend_penalty = 0.3
        if ma200 > 0 and price < ma200 * 0.97:
            trend_penalty = max(trend_penalty, 0.6)

        # Standard financial metrics
        roic      = compute_roic(info)
        roce      = compute_roce(info)
        fcf       = info.get("freeCashflow")
        beta      = info.get("beta", 1.0) or 1.0
        pe        = info.get("trailingPE", 0) or 0
        roe       = info.get("returnOnEquity", 0) or 0
        gross_margin = info.get("grossMargins", 0) or 0

        # P/FCF
        pfcf = round(market_cap / fcf, 1) if fcf and fcf > 0 and market_cap > 0 else None

        # Analyst consensus
        analyst_target = info.get("targetMeanPrice")
        analyst_count  = info.get("numberOfAnalystOpinions", 0) or 0
        analyst_upside = None
        if analyst_target and price and price > 0:
            analyst_upside = round(((analyst_target / price) - 1) * 100, 1)

        # 5-year CAGR
        five_yr = compute_5yr_data(t)

        # DCF and dividend sustainability
        dcf       = compute_dcf(info)
        div_check = check_dividend_sustainability(info)

        # S_Growth from fundamental engine
        sg_data = fe.compute_s_growth(
            info=info, ticker=ticker_symbol, group=group,
            t=t, roic=roic, growth_themes=GROWTH_THEMES,
        )
        s_growth = sg_data["s_growth"]

        # Memory / post-mortem penalty applied to fundamental score
        memory_penalty = 0.0
        for lesson in memory.get("lessons", []):
            if lesson.get("sector") == group and lesson.get("type") == "NEGATIVE_LEARNING":
                memory_penalty += 0.4
        pm_adj = post_mortem.get("sector_adjustments", {}).get(group, 0)
        memory_penalty = min(2.0, memory_penalty)

        # Adjusted S_Growth (clamp after penalties/trend)
        adjusted_s_growth = round(
            max(0.0, min(10.0, s_growth - memory_penalty + pm_adj - trend_penalty)), 2
        )

        return {
            # Identity
            "ticker":           ticker_symbol,
            "name":             info.get("shortName", ticker_symbol),
            "sector":           sector,
            "industry_group":   group,
            "price":            round(price, 2) if price else 0,
            # Core fundamentals
            "roe":              round(roe * 100, 2),
            "pe_ratio":         round(pe, 2),
            "debt_to_equity":   round(de_ratio, 2),
            "revenue_growth":   round(rev_growth * 100, 1),
            "gross_margin":     round(gross_margin * 100, 1),
            "profit_margin":    round(profit_margin * 100, 1),
            "fcf_positive":     fcf is None or fcf >= 0,
            "beta":             round(beta, 2),
            "roic":             roic,
            "roce":             roce,
            "pfcf":             pfcf,
            "rev_cagr_5yr":     five_yr["rev_cagr_5yr"],
            "ni_cagr_5yr":      five_yr["ni_cagr_5yr"],
            "analyst_target":   analyst_target,
            "analyst_upside":   analyst_upside,
            "analyst_count":    analyst_count,
            "dcf":              dcf,
            "dividend":         div_check,
            # S_Growth engine output
            "s_growth":         adjusted_s_growth,
            **{k: v for k, v in sg_data.items() if k != "s_growth"},
            # Placeholder momentum fields (populated in Phase 2)
            "s_momentum":       None,
            "convergence_score": adjusted_s_growth,
            "convergence_trigger": False,
            "stage2":           False,
            "vcp_active":       False,
            "rvol":             None,
            "vol_buzz_pct":     None,
            "rsi14":            None,
            "macd_bullish":     None,
            "stop_loss_atr":    None,
            # Legacy field used for dashboard sorting
            "score":            adjusted_s_growth,
            "penalty_applied":  memory_penalty > 0 or pm_adj < 0,
        }

    except Exception:
        return None


# ── Phase 2: Momentum overlay ─────────────────────────────────────────────────

def apply_momentum(candidate: dict) -> dict:
    """
    Run the momentum engine on a single pre-qualified fundamental candidate.
    Updates the candidate dict in-place with S_Momentum fields.
    Computes the Convergence Zone score and trigger flag.
    """
    ticker = candidate["ticker"]
    mom = me.compute_s_momentum(ticker)

    if mom is None:
        # Insufficient price history — keep S_Growth as convergence score
        return candidate

    s_growth   = candidate["s_growth"]
    s_momentum = mom["s_momentum"]
    convergence = round((s_growth + s_momentum) / 2.0, 2)
    trigger     = s_growth >= 7.5 and s_momentum >= 7.5

    candidate.update({
        "s_momentum":         s_momentum,
        "convergence_score":  convergence,
        "convergence_trigger": trigger,
        "stage2":             mom["stage2"],
        "vcp_active":         mom["vcp_active"],
        "rvol":               mom["rvol"],
        "vol_buzz_pct":       mom["vol_buzz_pct"],
        "rsi14":              mom["rsi14"],
        "macd_bullish":       mom["macd_bullish"],
        "atr14":              mom["atr14"],
        "atr_ratio_pct":      mom["atr_ratio_pct"],
        "stop_loss_atr":      mom["stop_loss_atr"],
        "m_factor":           mom["m_factor"],
        "c_factor":           mom["c_factor"],
        "v_factor":           mom["v_factor"],
        "o_factor":           mom["o_factor"],
        "sma50":              mom["sma50"],
        "sma200":             mom["sma200"],
        # Legacy sort key updated to convergence
        "score":              convergence,
    })
    return candidate


# ── Main orchestrator ─────────────────────────────────────────────────────────

def main():
    log.info("=== NEXUS DUAL-ENGINE SCAN STARTING ===")
    memory      = load_memory()
    post_mortem = memory.get("post_mortem", {})
    universe    = fetch_global_universe()
    log.info("Universe: %d tickers, scanning max %d", len(universe), MAX_SCAN)

    # ── Phase 1: Fundamental scan ─────────────────────────────────────────────
    log.info("--- Phase 1: Fundamental (S_Growth) scan ---")
    fundamental_candidates: list[dict] = []
    scanned = 0
    for ticker in universe:
        if scanned >= MAX_SCAN:
            break
        scanned += 1
        data = analyse_ticker_fundamental(ticker, memory, post_mortem)
        if data:
            fundamental_candidates.append(data)
            log.info(
                "PASS [F]: %-8s S_Growth=%.2f  A=%.0f E=%.2f L=%.2f U=%.2f",
                ticker, data["s_growth"],
                data.get("A_factor", 0), data.get("E_factor", 0),
                data.get("L_factor", 0), data.get("U_factor", 0),
            )
        time.sleep(0.05)

    log.info("Phase 1 complete: %d candidates from %d scanned", len(fundamental_candidates), scanned)

    # Sort by S_Growth, keep top MOMENTUM_TOP for Phase 2
    top_fundamental = sorted(fundamental_candidates, key=lambda x: x["s_growth"], reverse=True)[:MOMENTUM_TOP]

    # ── Phase 2: Momentum overlay ─────────────────────────────────────────────
    log.info("--- Phase 2: Momentum (S_Momentum) overlay on top %d ---", len(top_fundamental))
    for candidate in top_fundamental:
        ticker = candidate["ticker"]
        try:
            apply_momentum(candidate)
            mom_str = (
                f"S_Mom={candidate['s_momentum']:.2f}  "
                f"Stage2={candidate['stage2']}  VCP={candidate['vcp_active']}  "
                f"Conv={candidate['convergence_score']:.2f}"
                + (" *** CONVERGENCE ZONE ***" if candidate["convergence_trigger"] else "")
            )
            log.info("PASS [M]: %-8s %s", ticker, mom_str)
        except Exception as e:
            log.debug("Momentum failed for %s: %s", ticker, e)
        time.sleep(0.10)

    # Final sort by convergence score (fallback to S_Growth if no momentum)
    candidates = sorted(top_fundamental, key=lambda x: x["convergence_score"], reverse=True)[:TOP_N]
    convergence_count = sum(1 for c in candidates if c.get("convergence_trigger"))
    log.info(
        "Top %d selected — %d in Convergence Zone (both scores ≥ 7.5)",
        len(candidates), convergence_count,
    )

    # Tier-2 AI sentiment carry-over (from previous run)
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH) as f:
            old_data = json.load(f)
    else:
        old_data = {}

    old_tier2 = {
        c["ticker"]: c["tier2"]
        for c in old_data.get("top_candidates", [])
        if c.get("tier2")
    }
    for c in candidates:
        if c["ticker"] in old_tier2:
            c["tier2"] = old_tier2[c["ticker"]]
            sentiment  = old_tier2[c["ticker"]].get("sentiment_score", "")
            if sentiment == "BULLISH":
                c["score"] = round(min(10.0, c["score"] + 0.3), 1)
            elif sentiment == "BEARISH":
                c["score"] = round(max(1.0, c["score"] - 0.5), 1)

    candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)

    output = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "top_candidates": candidates,
        "active_trades":  old_data.get("active_trades", []),
        "equity_history": old_data.get("equity_history", []),
        "memory":         old_data.get("memory", {}),
        "macro":          fetch_macro(),
        "portfolio":      old_data.get("portfolio", {"cash": 10000.0, "starting_capital": 10000.0}),
        "watchlist":      old_data.get("watchlist", []),
        "filings":        old_data.get("filings", {}),
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=4)

    log.info("Done — %d candidates saved.", len(candidates))
    notify_scan_complete(candidates, scanned)


if __name__ == "__main__":
    main()
