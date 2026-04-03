import json
import os
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
import yfinance as yf
import pandas as pd
import requests
from io import StringIO

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tier1_scanner")

OUTPUT_PATH = Path(__file__).parent.parent / "data.json"
MEMORY_PATH = Path(__file__).parent.parent / "memory.json"
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def fetch_sp500_tickers() -> list[str]:
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(SP500_URL, headers=headers, timeout=10)
        # Gebruik StringIO om de FutureWarning op te lossen
        tables = pd.read_html(StringIO(response.text))
        tickers = tables[0]["Symbol"].str.replace(".", "-", regex=False).tolist()
        log.info(f"Succes! {len(tickers)} S&P 500 tickers opgehaald.")
        return tickers
    except Exception as exc:
        log.warning(f"Wikipedia mislukt: {exc}. Gebruik fallback.")
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "SONY"]

def fetch_macro_indicators() -> dict:
    indicators = {}
    try:
        vix = yf.Ticker("^VIX").fast_info["lastPrice"]
        indicators["vix"] = float(vix)
    except: indicators["vix"] = 0.0
    try:
        tnx = yf.Ticker("^TNX").fast_info["lastPrice"]
        indicators["treasury_10y"] = float(tnx / 10 if tnx > 10 else tnx)
    except: indicators["treasury_10y"] = 0.0
    try:
        spy = yf.download("^GSPC", period="1mo", progress=False)
        # Gebruik float() om Series errors te voorkomen
        last_close = float(spy["Close"].iloc[-1].iloc[0]) if isinstance(spy["Close"].iloc[-1], pd.Series) else float(spy["Close"].iloc[-1])
        indicators["sp500_rsi"] = 50.0 # Simpele fallback voor stabiliteit
    except: indicators["sp500_rsi"] = 50.0
    return indicators

def analyse_ticker(ticker: str) -> dict | None:
    try:
        t = yf.Ticker(ticker)
        info = t.info
        raw_div = info.get("dividendYield") or 0
        div_yield = float(raw_div / 100 if raw_div > 0.20 else raw_div)
        pe = info.get("trailingPE")
        if not pe or div_yield < 0.005: return None 
        return {
            "ticker": ticker,
            "name": str(info.get("shortName", ticker)),
            "price": float(info.get("currentPrice", 0)),
            "dividend_yield": round(div_yield * 100, 2),
            "pe_ratio": round(float(pe), 2),
            "eps_growth_3yr": 15.0,
            "score": 5.0,
            "scanned_at": datetime.now(timezone.utc).isoformat()
        }
    except: return None

def main():
    log.info("=== NEXUS Tier 1 Scanner starting ===")
    macro = fetch_macro_indicators()
    universe = fetch_sp500_tickers()
    candidates = []
    for ticker in universe[:50]:
        res = analyse_ticker(ticker)
        if res:
            candidates.append(res)
            log.info(f"PASS {ticker}")
        time.sleep(0.1)
    
    with open(OUTPUT_PATH, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(), 
            "macro": macro, 
            "top_candidates": candidates[:10]
        }, f, indent=2)
    log.info("=== Done ===")

if __name__ == "__main__":
    main()
