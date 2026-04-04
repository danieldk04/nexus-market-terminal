import json
import time
import logging
import urllib.parse
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
MEMORY_PATH = BASE_DIR / "memory.json"

# Sector Definities
TECH_AI = ["Technology", "Communication Services", "Software", "Information Technology"]
FINANCE_VINTAGE = ["Financial Services", "Financial Data Services", "Banks", "Insurance"]

def get_industry_group(sector):
    if sector in TECH_AI: return "Tech & AI"
    if sector in FINANCE_VINTAGE: return "Financials"
    return "Others"

def load_memory():
    """Laadt het geheugen van de bot om te leren van fouten."""
    if not MEMORY_PATH.exists():
        return {"lessons": []}
    with open(MEMORY_PATH, "r") as f:
        return json.load(f)

def fetch_global_universe():
    tickers = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        # S&P 500
        res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=headers, timeout=10)
        tickers.extend(pd.read_html(StringIO(res.text))[0]["Symbol"].str.replace(".", "-", regex=False).tolist())
        # Nasdaq 100
        res = requests.get("https://en.wikipedia.org/wiki/Nasdaq-100", headers=headers, timeout=10)
        tickers.extend(pd.read_html(StringIO(res.text))[4]["Ticker"].tolist())
        # NL Selectie
        tickers.extend(["ASML.AS", "INGA.AS", "ADYEN.AS", "UNA.AS", "HEIA.AS"])
    except Exception as e:
        log.error(f"Fout bij ophalen universe: {e}")
    return list(set(tickers))

def analyse_ticker(ticker_symbol, memory):
    try:
        t = yf.Ticker(ticker_symbol)
        info = t.info
        
        roe = info.get("returnOnEquity", 0)
        if roe < 0.15: return None 

        sector = info.get("sector", "Unknown")
        group = get_industry_group(sector)
        pe = info.get("trailingPE", 0)
        de_ratio = (info.get("debtToEquity", 0) / 100) if info.get("debtToEquity", 0) > 5 else info.get("debtToEquity", 0)
        
        max_pe = 35 if group == "Tech & AI" else 18
        if pe <= 0 or pe > max_pe: return None
        if de_ratio > 2.5: return None

        # --- INTELLIGENTE SCORE LOGICA MET GEHEUGEN ---
        base = 5.0
        pe_penalty = (pe / max_pe) * 4.0
        roe_bonus = min(4.0, (roe / 0.40) * 3.0)
        debt_penalty = min(1.0, de_ratio / 2.5)

        # DE ZELFREFLECTIE STAP:
        # Check of we in deze sector eerder verlies hebben gemaakt
        memory_penalty = 0.0
        for lesson in memory.get('lessons', []):
            if lesson.get('sector') == group and lesson.get('type') == "NEGATIVE_LEARNING":
                memory_penalty += 0.5 # Trek 0.5 punt af per fout in deze sector
        
        # Maximaal 2.0 punten aftrek door fouten (om te voorkomen dat een sector op 0 komt)
        memory_penalty = min(2.0, memory_penalty)

        raw_score = base - pe_penalty + roe_bonus - debt_penalty - memory_penalty
        score = round(max(1.0, min(10.0, raw_score)), 1)

        return {
            "ticker": ticker_symbol,
            "name": info.get("shortName", ticker_symbol),
            "sector": sector,
            "industry_group": group,
            "roe": round(roe * 100, 2),
            "pe_ratio": round(pe, 2),
            "debt_to_equity": round(de_ratio, 2),
            "price": info.get("currentPrice", 0),
            "score": score,
            "penalty_applied": memory_penalty > 0
        }
    except: return None

MAX_SCAN = 300   # Maximaal te scannen tickers
TOP_N    = 15    # Beste N kandidaten teruggeven

def main():
    log.info("=== NEXUS INTELLIGENT SCAN STARTING ===")
    memory = load_memory()
    universe = fetch_global_universe()
    log.info(f"Universe: {len(universe)} tickers beschikbaar, max {MAX_SCAN} worden gescand.")

    candidates = []
    scanned = 0
    for ticker in universe:
        if scanned >= MAX_SCAN:
            break
        scanned += 1
        data = analyse_ticker(ticker, memory)
        if data:
            candidates.append(data)
            penalty_str = " [PENALTY APPLIED]" if data['penalty_applied'] else ""
            log.info(f"PASS: {ticker} Score: {data['score']}{penalty_str}")
        time.sleep(0.05)

    # Sorteer ALLE gevonden kandidaten op score, neem dan de top N
    candidates = sorted(candidates, key=lambda x: x['score'], reverse=True)[:TOP_N]
    log.info(f"Beste {len(candidates)} kandidaten geselecteerd na full scan van {scanned} tickers.")

    # Inladen van bestaande data om trades niet te overschrijven
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH, "r") as f:
            old_data = json.load(f)
    else:
        old_data = {}

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_candidates": candidates,  # Al gesorteerd en afgekapt in main()
        "active_trades": old_data.get("active_trades", []), # Behou de trades!
        "equity_history": old_data.get("equity_history", []),
        "macro": {"vix": 22.1, "treasury_10y": 4.3}
    }
    
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=4)
    log.info(f"Done! {len(candidates)} candidates saved with memory-logic.")

if __name__ == "__main__":
    main()
