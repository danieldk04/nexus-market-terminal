"""
NEXUS MARKET TERMINAL - Tier 1 Scanner (CORRECTED UNITS)
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
MIN_DIVIDEND_YIELD = 0.005          # Verlaagd naar 0.5% om kwaliteitsaandelen als MSFT te vangen
MAX_PE_RATIO_PREMIUM = 1.2          # Iets meer ruimte voor groei (1.2x sector)
MIN_EPS_GROWTH_3YR = 0.08           # 8% groei
TOP_N = 10
RATE_LIMIT_SLEEP = 0.5
OUTPUT_PATH = Path(__file__).parent.parent / "data.json"
MEMORY_PATH = Path(__file__).parent.parent / "memory.json"

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

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

GLOBAL_TICKERS: list[str] = ["ASML", "SAP", "NVO", "UL", "BP", "BABA", "TSM", "SONY", "INFY", "VALE"]

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_sp500_tickers() -> list[str]:
    try:
        # Gebruik headers om Wikipedia te overtuigen dat we een browser zijn
        html = requests.get(SP500_URL, headers={'User-Agent': 'Mozilla/5.0'}).text
        tables = pd.read_html(html)
        df = tables[0]
        return df["Symbol"].str.replace(".", "-", regex=False).tolist()
    except Exception as exc:
        log.warning("Wikipedia scrape failed: %s. Using fallback.", exc)
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "BRK-B", "JPM", "V"]

def fetch_macro_indicators() -> dict:
    indicators: dict = {}
    
    # VIX
    try:
        vix = yf.Ticker("^VIX").fast_info["lastPrice"]
        indicators["vix"] = round(vix, 2)
    except: indicators["vix"] = 0

    # 10Y Treasury yield (FIX: ^TNX is basis points / 10)
    try:
        tnx = yf.Ticker("^TNX").fast_info["lastPrice"]
        # Als tnx 43.10 is, is de echte yield 4.31%
        indicators["treasury_10y"] = round(tnx / 10, 3) 
    except: indicators["treasury_10y"] = 0

    # S&P 500 RSI
    try:
        spy = yf.download("^GSPC", period="1mo", progress=False)
        delta = spy["Close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        indicators["sp500_rsi"] = round(100 - (100 / (1 + rs.iloc[-1])), 1)
    except: indicators["sp500_rsi"] = 50

    return indicators

def analyse_ticker(ticker: str) -> dict | None:
    try:
        t = yf.Ticker(ticker)
        info = t.info
        
        # --- DIVIDEND FIX ---
        raw_div = info.get("dividendYield") or 0
        # Sanity check: als yfinance 0.97 geeft bedoelen ze 97%. 
        # Maar als ze 97 geven, bedoelen ze ook 97%.
        # We schalen alles naar een decimaal (0.00 tot 1.00)
        div_yield = raw_div if raw_div < 1 else raw_div / 100
        
        # --- PE FIX ---
        pe = info.get("trailingPE")
        sector = info.get("sector", "Unknown")
        sector_pe = SECTOR_PE_MEDIANS.get(sector, 20.0)

        # Filters
        if div_yield < MIN_DIVIDEND_YIELD or div_yield > 0.25: # Filter out crazy >25% errors
            return None
        if not pe or pe > sector_pe * MAX_PE_RATIO_PREMIUM:
            return None

        return {
            "ticker": ticker,
            "name": info.get("shortName", ticker),
            "sector": sector,
            "price": round(info.get("currentPrice", 0), 2),
            "dividend_yield": round(div_yield * 100, 2),
            "pe_ratio": round(pe, 2),
            "sector_pe_median": sector_pe,
            "eps_growth_3yr": 15.0, # Fallback value
            "market_cap": info.get("marketCap", 0),
            "score": 5.0, # Base score
            "scanned_at": datetime.now(timezone.utc).isoformat(),
        }
    except:
        return None

def main():
    log.info("=== NEXUS Tier 1 Scanner starting ===")
    macro = fetch_macro_indicators()
    
    universe = fetch_sp500_tickers() + GLOBAL_TICKERS
    candidates = []
    
    # Scan de eerste 50 voor snelheid in deze test, of de hele lijst
    for ticker in universe[:100]: 
        res = analyse_ticker(ticker)
        if res:
            candidates.append(res)
            log.info(f"PASS {ticker}")
        time.sleep(0.2)

    # Opslaan
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "macro": macro,
        "top_candidates": candidates[:10]
    }
    with open(OUTPUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)
    log.info("Done.")

if __name__ == "__main__":
    main()
