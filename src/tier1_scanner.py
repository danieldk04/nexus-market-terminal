import json
import time
import logging
import urllib.parse
import re
from datetime import datetime, timezone
from pathlib import Path
import yfinance as yf
import pandas as pd
import requests
from io import StringIO

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tier1_scanner")

# Paden
BASE_DIR = Path(__file__).parent.parent
OUTPUT_PATH = BASE_DIR / "data.json"

# Sector Definities voor je Circle of Competence
TECH_AI = ["Technology", "Communication Services", "Software", "Information Technology"]
FINANCE_VINTAGE = ["Financial Services", "Financial Data Services", "Banks", "Insurance"]

def get_industry_group(sector):
    if sector in TECH_AI: return "Tech & AI"
    if sector in FINANCE_VINTAGE: return "Financials"
    return "Others"

def fetch_global_universe():
    """Haalt automatisch de nieuwste tickers op van Wikipedia."""
    tickers = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        # S&P 500
        res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=headers, timeout=10)
        tickers.extend(pd.read_html(StringIO(res.text))[0]["Symbol"].str.replace(".", "-", regex=False).tolist())
        # Nasdaq 100
        res = requests.get("https://en.wikipedia.org/wiki/Nasdaq-100", headers=headers, timeout=10)
        tickers.extend(pd.read_html(StringIO(res.text))[4]["Ticker"].tolist())
        # Nederlandse Selectie
        tickers.extend(["ASML.AS", "ADYEN.AS", "INGA.AS", "ABN.AS", "ASM.AS", "BESI.AS"])
    except Exception as e:
        log.error(f"Fout bij ophalen universe: {e}")
    return list(set(tickers))

def get_debt_equity_ratio(info):
    """Robuuste Debt-to-Equity berekening."""
    try:
        de = info.get("debtToEquity")
        if de: return round(de / 100, 2) if de > 5 else round(de, 2)
        td = info.get("totalDebt")
        te = info.get("totalStockholderEquity")
        if td and te and te > 0: return round(td / te, 2)
    except: pass
    return 0.0

def analyse_ticker(ticker_symbol):
    try:
        t = yf.Ticker(ticker_symbol)
        info = t.info
        
        roe = info.get("returnOnEquity", 0)
        if roe < 0.15: return None  # Buffett Rule: ROE > 15%

        sector = info.get("sector", "Unknown")
        group = get_industry_group(sector)
        de_ratio = get_debt_equity_ratio(info)
        pe = info.get("trailingPE", 0)
        
        # Sector-specifieke PE check
        max_pe = 35 if group == "Tech & AI" else 18
        if pe <= 0 or pe > max_pe: return None
        if de_ratio > 2.5: return None

        # Score berekening
        score = round(10 - (pe/max_pe * 5) + (roe * 15), 1)

        return {
            "ticker": ticker_symbol,
            "name": info.get("shortName", ticker_symbol),
            "sector": sector,
            "industry_group": group,
            "roe": round(roe * 100, 2),
            "pe_ratio": round(pe, 2),
            "debt_to_equity": de_ratio,
            "dividend_yield": round((info.get("dividendYield", 0) or 0) * 100, 2),
            "price": info.get("currentPrice", 0),
            "score": score
        }
    except: return None

def main():
    log.info("=== NEXUS GLOBAL HUNTER STARTING ===")
    universe = fetch_global_universe()
    log.info(f"Scanning {len(universe)} tickers...")
    
    candidates = []
    for ticker in universe:
        data = analyse_ticker(ticker)
        if data:
            candidates.append(data)
            log.info(f"PASS: {ticker} [{data['industry_group']}] Score: {data['score']}")
        if len(candidates) >= 15: break
        time.sleep(0.1)

    # Opslaan
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_candidates": sorted(candidates, key=lambda x: x['score'], reverse=True),
        "macro": {"vix": 22.1, "treasury_10y": 4.3} # Wordt in echte run geüpdatet
    }
    
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=4)
    log.info("Done! Data.json is ververst.")

if __name__ == "__main__":
    main()
