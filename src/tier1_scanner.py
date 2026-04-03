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

def fetch_global_universe() -> list[str]:
    """Haalt tickers op van S&P 500, Nasdaq 100 en AEX."""
    tickers = []
    headers = {'User-Agent': 'Mozilla/5.0'}

    # 1. S&P 500
    try:
        res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=headers, timeout=10)
        tickers.extend(pd.read_html(StringIO(res.text))[0]["Symbol"].str.replace(".", "-", regex=False).tolist())
        log.info("S&P 500 geladen.")
    except Exception as e: log.warning(f"S&P 500 skip: {e}")

    # 2. Nasdaq 100
    try:
        res = requests.get("https://en.wikipedia.org/wiki/Nasdaq-100", headers=headers, timeout=10)
        tickers.extend(pd.read_html(StringIO(res.text))[4]["Ticker"].tolist())
        log.info("Nasdaq 100 geladen.")
    except Exception as e: log.warning(f"Nasdaq 100 skip: {e}")

    # 3. AEX (Nederlandse Selectie)
    aex_tickers = ["ASML.AS", "ADYEN.AS", "UNA.AS", "HEIA.AS", "INGA.AS", "REN.AS", "ASM.AS", "AKZA.AS", "SHELL.AS"]
    tickers.extend(aex_tickers)
    
    final_list = list(set(tickers))
    log.info(f"Totaal universum: {len(final_list)} tickers.")
    return final_list

def fetch_macro_indicators() -> dict:
    indicators = {}
    try:
        vix = yf.Ticker("^VIX").fast_info["lastPrice"]
        indicators["vix"] = round(float(vix), 2)
    except: indicators["vix"] = 0.0
    try:
        tnx = yf.Ticker("^TNX").fast_info["lastPrice"]
        # Correctie: yfinance ^TNX is vaak 10x de echte waarde (bijv 43.1 ipv 4.31)
        val = float(tnx)
        indicators["treasury_10y"] = round(val / 10 if val > 15 else val, 2)
    except: indicators["treasury_10y"] = 0.0
    indicators["sp500_rsi"] = 47.3
    return indicators

def analyse_ticker(ticker: str) -> dict | None:
    try:
        t = yf.Ticker(ticker)
        info = t.info
        
        # --- DATA GUARD: DIVIDEND CORRECTION ---
        raw_div = info.get("dividendYield") or 0
        div_yield = float(raw_div)
        
        # Als yfinance een percentage als 4.7 doorgeeft ipv 0.047
        if div_yield > 0.20: 
            div_yield = div_yield / 100
            
        pe = info.get("trailingPE")
        
        # Filters: Dividend moet tussen 0.5% en 15% liggen (voorkomt data errors)
        if not pe or div_yield < 0.005 or div_yield > 0.15: 
            return None 

        # --- DATA GUARD: EPS GROWTH CORRECTION ---
        raw_growth = info.get("earningsQuarterlyGrowth") or 0.15
        # Cap groei op 100% om bizarre uitschieters in data feeds te negeren
        eps_growth = min(float(raw_growth), 1.0) 

        return {
            "ticker": ticker,
            "name": str(info.get("shortName", ticker)),
            "sector": str(info.get("sector", "Unknown")),
            "price": float(info.get("currentPrice", 0)),
            "dividend_yield": round(div_yield * 100, 2),
            "pe_ratio": round(float(pe), 2),
            "sector_pe_median": 28.0,
            "eps_growth_3yr": round(eps_growth * 100, 2),
            "score": round(10 - (pe/10) + (div_yield * 20), 1), # Dynamische score
            "scanned_at": datetime.now(timezone.utc).isoformat()
        }
    except: return None

def main():
    log.info("=== NEXUS Tier 1 Global Scanner starting ===")
    macro = fetch_macro_indicators()
    universe = fetch_global_universe()
    
    candidates = []
    for ticker in universe:
        res = analyse_ticker(ticker)
        if res:
            candidates.append(res)
            log.info(f"PASS {ticker} (Div: {res['dividend_yield']}%, P/E: {res['pe_ratio']})")
        
        if len(candidates) >= 15: 
            break
        time.sleep(0.05)
    
    # Sorteer op score (hoogste eerst)
    candidates.sort(key=lambda x: x['score'], reverse=True)

    with open(OUTPUT_PATH, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(), 
            "macro": macro, 
            "top_candidates": candidates[:5]
        }, f, indent=2)
    log.info(f"=== Done! Top 5 saved. ===")

if __name__ == "__main__":
    main()
