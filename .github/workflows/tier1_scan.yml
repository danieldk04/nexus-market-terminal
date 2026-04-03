"""
NEXUS MARKET TERMINAL - Tier 1 Scanner
Scans S&P 500 + global trending stocks via yfinance.
Filters: Dividend >3%, P/E < sector avg, EPS growth >10-15% over 3yr.
Writes results to data.json for the HTML dashboard.
"""

import json
import os
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf
import pandas as pd
import requests

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tier1_scanner")

# ── Config ────────────────────────────────────────────────────────────────────
MIN_DIVIDEND_YIELD = 0.03          # 3 %
MAX_PE_RATIO_PREMIUM = 1.0         # P/E must be BELOW sector avg (factor ≤ 1.0)
MIN_EPS_GROWTH_3YR = 0.10          # 10 % annualised EPS growth over 3 years
TOP_N = 10                         # candidates passed to Tier 2
RATE_LIMIT_SLEEP = 0.4             # seconds between yfinance calls
OUTPUT_PATH = Path(__file__).parent.parent / "data.json"
MEMORY_PATH = Path(__file__).parent.parent / "memory.json"

# S&P 500 tickers (Wikipedia table — no API key needed)
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Sector median P/E estimates (updated quarterly; fallback if live data absent)
SECTOR_PE_MEDIANS: dict[str, float] = {
    "Technology": 28.0,
    "Health Care": 22.0,
    "Financials": 14.0,
    "Consumer Discretionary": 20.0,
    "Consumer Staples": 18.0,
    "Industrials": 19.0,
    "Communication Services": 22.0,
    "Utilities": 16.0,
    "Real Estate": 30.0,
    "Materials": 17.0,
    "Energy": 12.0,
    "Unknown": 20.0,
}

# Extra global / trending tickers to include beyond S&P 500
GLOBAL_TICKERS: list[str] = [
    # European blue chips
    "ASML", "SAP", "NVO", "UL", "BP",
    # Asia / EM ADRs
    "BABA", "TSM", "SONY", "INFY", "VALE",
    # Dividend ETF proxies (excluded from individual ranking but useful for VIX context)
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_sp500_tickers() -> list[str]:
    """Return all S&P 500 tickers scraped from Wikipedia."""
    try:
        tables = pd.read_html(SP500_URL)
        df = tables[0]
        tickers = df["Symbol"].str.replace(".", "-", regex=False).tolist()
        log.info("Fetched %d S&P 500 tickers", len(tickers))
        return tickers
    except Exception as exc:
        log.warning("Wikipedia scrape failed (%s); using small fallback list", exc)
        return ["AAPL", "MSFT", "JNJ", "KO", "PG", "XOM", "T", "VZ", "MO", "PFE"]


def fetch_fear_and_greed() -> dict:
    """CNN Fear & Greed via the unofficial public API (no key needed)."""
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        data = r.json()
        score = data["fear_and_greed"]["score"]
        rating = data["fear_and_greed"]["rating"]
        return {"score": round(score, 1), "rating": rating}
    except Exception as exc:
        log.warning("Fear & Greed fetch failed: %s", exc)
        return {"score": None, "rating": "unavailable"}


def fetch_macro_indicators() -> dict:
    """VIX, 10Y Treasury yield, and S&P 500 RSI (14-day)."""
    indicators: dict = {}

    # VIX
    try:
        vix = yf.Ticker("^VIX")
        indicators["vix"] = round(vix.fast_info["lastPrice"], 2)
    except Exception:
        indicators["vix"] = None

    # 10Y Treasury yield (^TNX = yield * 10 in Yahoo Finance convention)
    try:
        tnx = yf.Ticker("^TNX")
        indicators["treasury_10y"] = round(tnx.fast_info["lastPrice"] / 10, 3)
    except Exception:
        indicators["treasury_10y"] = None

    # S&P 500 RSI-14
    try:
        spy = yf.download("^GSPC", period="3mo", interval="1d", progress=False, auto_adjust=True)
        close = spy["Close"].squeeze()
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        indicators["sp500_rsi"] = round(float(rsi.iloc[-1]), 1)
    except Exception:
        indicators["sp500_rsi"] = None

    return indicators


def calc_eps_growth(ticker_obj: yf.Ticker) -> float | None:
    """
    Annualised EPS growth over ~3 years using yfinance earnings history.
    Returns None when insufficient data is available.
    """
    try:
        hist = ticker_obj.earnings_history
        if hist is None or hist.empty:
            return None
        # Keep only annual (quarterly=False) rows if available
        if "period" in hist.columns:
            hist = hist[hist["period"].str.len() == 4]  # '2021', '2022' etc.
        hist = hist.sort_index()
        if len(hist) < 2:
            return None
        eps_col = [c for c in hist.columns if "eps" in c.lower() and "estimate" not in c.lower()]
        if not eps_col:
            return None
        col = eps_col[0]
        values = hist[col].dropna().tolist()
        if len(values) < 2:
            return None
        oldest, latest = values[0], values[-1]
        if oldest <= 0:
            return None
        years = min(len(values) - 1, 3)
        cagr = (latest / oldest) ** (1 / years) - 1
        return round(cagr, 4)
    except Exception:
        return None


def score_candidate(info: dict, eps_growth: float | None) -> float:
    """
    Composite attractiveness score (higher = better).
    Components: dividend yield, P/E discount to sector, EPS growth.
    """
    score = 0.0

    div = info.get("dividendYield") or 0
    score += min(div * 10, 4.0)          # up to 4 pts for yield

    sector = info.get("sector", "Unknown")
    pe = info.get("trailingPE")
    sector_pe = SECTOR_PE_MEDIANS.get(sector, SECTOR_PE_MEDIANS["Unknown"])
    if pe and pe > 0:
        discount = (sector_pe - pe) / sector_pe  # positive = cheaper than sector
        score += max(min(discount * 5, 3.0), -3.0)

    if eps_growth is not None:
        score += min(eps_growth * 10, 3.0)       # up to 3 pts for 30 % growth

    return round(score, 3)


def analyse_ticker(ticker: str) -> dict | None:
    """
    Fetch fundamentals for one ticker and apply all three filters.
    Returns a candidate dict or None if the ticker fails any filter.
    """
    try:
        t = yf.Ticker(ticker)
        info = t.fast_info
        full_info = t.info  # slower but needed for dividends / P/E

        div_yield = full_info.get("dividendYield") or 0
        if div_yield < MIN_DIVIDEND_YIELD:
            return None

        pe = full_info.get("trailingPE")
        sector = full_info.get("sector", "Unknown")
        sector_pe = SECTOR_PE_MEDIANS.get(sector, SECTOR_PE_MEDIANS["Unknown"])
        if pe is None or pe <= 0 or pe >= sector_pe * MAX_PE_RATIO_PREMIUM:
            return None

        eps_growth = calc_eps_growth(t)
        if eps_growth is None or eps_growth < MIN_EPS_GROWTH_3YR:
            return None

        return {
            "ticker": ticker,
            "name": full_info.get("shortName", ticker),
            "sector": sector,
            "price": round(full_info.get("currentPrice") or info.get("lastPrice", 0), 2),
            "dividend_yield": round(div_yield * 100, 2),
            "pe_ratio": round(pe, 2),
            "sector_pe_median": sector_pe,
            "eps_growth_3yr": round(eps_growth * 100, 2),
            "market_cap": full_info.get("marketCap"),
            "score": score_candidate(full_info, eps_growth),
            "scanned_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        log.debug("Skip %s: %s", ticker, exc)
        return None


# ── Main scan ─────────────────────────────────────────────────────────────────

def run_scan() -> list[dict]:
    sp500 = fetch_sp500_tickers()
    universe = list(dict.fromkeys(sp500 + GLOBAL_TICKERS))  # deduplicate, preserve order
    log.info("Universe: %d tickers to scan", len(universe))

    candidates: list[dict] = []
    for i, ticker in enumerate(universe):
        result = analyse_ticker(ticker)
        if result:
            candidates.append(result)
            log.info("  PASS %s  score=%.2f  div=%.1f%%  pe=%.1f  eps_growth=%.1f%%",
                     ticker, result["score"], result["dividend_yield"],
                     result["pe_ratio"], result["eps_growth_3yr"])
        if i % 50 == 49:
            log.info("Progress: %d / %d scanned, %d passing", i + 1, len(universe), len(candidates))
        time.sleep(RATE_LIMIT_SLEEP)

    candidates.sort(key=lambda x: x["score"], reverse=True)
    top = candidates[:TOP_N]
    log.info("Scan complete. Top %d candidates selected from %d passing.", len(top), len(candidates))
    return top


# ── Persistence ───────────────────────────────────────────────────────────────

def load_memory() -> dict:
    if MEMORY_PATH.exists():
        with open(MEMORY_PATH) as f:
            return json.load(f)
    return {"predictions": [], "evaluations": [], "prompt_adjustments": []}


def save_memory(memory: dict) -> None:
    with open(MEMORY_PATH, "w") as f:
        json.dump(memory, f, indent=2)


def write_data_json(top_candidates: list[dict], macro: dict, fear_greed: dict) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "macro": macro,
        "fear_and_greed": fear_greed,
        "top_candidates": top_candidates,
        "filters_used": {
            "min_dividend_yield_pct": MIN_DIVIDEND_YIELD * 100,
            "max_pe_vs_sector": MAX_PE_RATIO_PREMIUM,
            "min_eps_growth_3yr_pct": MIN_EPS_GROWTH_3YR * 100,
        },
    }
    with open(OUTPUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)
    log.info("Wrote %s", OUTPUT_PATH)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== NEXUS Tier 1 Scanner starting ===")

    # 1. Macro context (fast, run first)
    log.info("Fetching macro indicators...")
    macro = fetch_macro_indicators()
    fear_greed = fetch_fear_and_greed()
    log.info("VIX=%.2f | 10Y=%.3f%% | RSI=%.1f | F&G=%s (%.1f)",
             macro.get("vix") or 0,
             (macro.get("treasury_10y") or 0) * 100,
             macro.get("sp500_rsi") or 0,
             fear_greed.get("rating"), fear_greed.get("score") or 0)

    # 2. Full universe scan
    top = run_scan()

    # 3. Persist results
    write_data_json(top, macro, fear_greed)

    # 4. Append to memory (Tier 2 / self-eval will enrich these later)
    memory = load_memory()
    memory["predictions"].append({
        "date": datetime.now(timezone.utc).date().isoformat(),
        "candidates": [
            {"ticker": c["ticker"], "price": c["price"], "score": c["score"]}
            for c in top
        ],
        "macro_snapshot": macro,
        "fear_greed_snapshot": fear_greed,
        "confidence": None,     # filled by Tier 2 Claude analysis
        "actuals": None,        # filled by weekly evaluator
    })
    save_memory(memory)
    log.info("Memory updated. %d prediction records total.", len(memory["predictions"]))
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
