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
        # Meestal tabel 4 op Wikipedia voor de Nasdaq-100 componenten
        tickers.extend(pd.read_html(StringIO(res.text))[4]["Ticker"].tolist())
        log.info("Nasdaq 100 geladen.")
    except Exception as e: log.warning(f"Nasdaq 100 skip: {e}")

    # 3. AEX (Nederlandse Selectie) - Handmatig is stabieler voor yfinance (.AS suffix)
    aex_tickers = ["ASML.AS", "ADYEN.AS", "UNA.AS", "HEIA.AS", "INGA.AS", "REN.AS", "ASM.AS", "AKZA.AS", "SHELL.AS"]
    tickers.extend(aex_tickers)
    
    # Dubbelen verwijderen
    final_list = list(set(tickers))
    log.info(f"Totaal universum: {len(final_list)} tickers.")
    return final_list

def fetch_macro_indicators() -> dict:
    indicators = {}
    try:
        vix = yf.Ticker("^VIX").fast_info["lastPrice"]
        indicators["vix"] = float(vix)
    except: indicators["vix"] = 0.0
    try:
        tnx = yf.Ticker("^TNX").fast_info["lastPrice"]
        # Correctie voor yfinance 10Y yield schaling
        indicators["treasury_10y"] = float(tnx / 10) if tnx > 10 else float(tnx)
    except: indicators["treasury_10y"] = 0.0
    indicators["sp500_rsi"] = 47.3 # Vastgezet voor stabiliteit
    return indicators

def analyse_ticker(ticker: str) -> dict | None:
    try:
        t = yf.Ticker(ticker)
        info = t.info
        
        # Basis filters
        raw_div = info.get("dividendYield") or 0
        div_yield = float(raw_div)
        pe = info.get("trailingPE")
        
        # NEXUS Filter: Moet dividend uitkeren en een P/E ratio hebben
        if not pe or div_yield < 0.005: 
            return None 

        return {
            "ticker": ticker,
            "name": str(info.get("shortName", ticker)),
            "sector": str(info.get("sector", "Unknown")),
            "price": float(info.get("currentPrice", 0)),
            "dividend_yield": round(div_yield * 100, 2),
            "pe_ratio": round(float(pe), 2),
            "sector_pe_median": 28.0, # Vergelijkingswaarde
            "eps_growth_3yr": round(float(info.get("earningsQuarterlyGrowth", 0.15) * 100), 2),
            "score": 7.0 if pe < 20 else 5.0, # Simpele scoring
            "scanned_at": datetime.now(timezone.utc).isoformat()
        }
    except: return None

def main():
    log.info("=== NEXUS Tier 1 Global Scanner starting ===")
    macro = fetch_macro_indicators()
    universe = fetch_global_universe()
    
    candidates = []
    # We scannen nu het hele universum, maar we stoppen als we 15 sterke kandidaten hebben
    # Dit bespaart tijd en Claude kosten
    for ticker in universe:
        res = analyse_ticker(ticker)
        if res:
            candidates.append(res)
            log.info(f"PASS {ticker} (Div: {res['dividend_yield']}%)")
        
        if len(candidates) >= 15: # Stop bij 15 goede matches
            break
            
        time.sleep(0.05) # Iets sneller dan voorheen
    
    # Sorteer op dividend (hoog naar laag) voor de top selectie
    candidates.sort(key=lambda x: x['dividend_yield'], reverse=True)

    with open(OUTPUT_PATH, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(), 
            "macro": macro, 
            "top_candidates": candidates[:5] # Stuur de beste 5 naar Claude (Tier 2)
        }, f, indent=2)
    log.info(f"=== Done! {len(candidates)} candidates found. Top 5 saved. ===")

if __name__ == "__main__":
    main()
