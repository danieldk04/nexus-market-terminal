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
from notifier import notify_scan_complete

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tier1_scanner")

# Paden
BASE_DIR = Path(__file__).parent.parent
OUTPUT_PATH = BASE_DIR / "data.json"
MEMORY_PATH = BASE_DIR / "memory.json"

# Sector Definities — uitgebreid voor betere diversificatiecontrole
SECTOR_MAP = {
    "Technology":             "Tech & AI",
    "Communication Services": "Tech & AI",
    "Software":               "Tech & AI",
    "Information Technology": "Tech & AI",
    "Financial Services":     "Financials",
    "Financial Data Services":"Financials",
    "Banks":                  "Financials",
    "Insurance":              "Financials",
    "Healthcare":             "Healthcare",
    "Biotechnology":          "Healthcare",
    "Pharmaceuticals":        "Healthcare",
    "Medical Devices":        "Healthcare",
    "Energy":                 "Energy",
    "Oil & Gas":              "Energy",
    "Basic Materials":        "Materials",
    "Utilities":              "Utilities",
    "Real Estate":            "Real Estate",
    "Consumer Defensive":     "Consumer Defensive",
    "Consumer Cyclical":      "Consumer Cyclical",
    "Industrials":            "Industrials",
}

def get_industry_group(sector):
    return SECTOR_MAP.get(sector, "Others")

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

def fetch_macro():
    """Haalt live VIX en 10-jaars rente op via yfinance."""
    macro = {"vix": None, "treasury_10y": None}
    try:
        macro["vix"] = round(yf.Ticker("^VIX").info.get("regularMarketPrice", 0), 2)
    except Exception:
        pass
    try:
        macro["treasury_10y"] = round(yf.Ticker("^TNX").info.get("regularMarketPrice", 0), 2)
    except Exception:
        pass
    log.info(f"Macro: VIX={macro['vix']}  10Y={macro['treasury_10y']}%")
    return macro

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

        # Aanvullende fundamentals voor rijkere scoring
        rev_growth  = info.get("revenueGrowth", 0) or 0       # bv. 0.12 = 12% groei
        fcf         = info.get("freeCashflow", None)           # absoluut getal
        profit_margin = info.get("profitMargins", 0) or 0      # bv. 0.18 = 18%
        beta        = info.get("beta", 1.0) or 1.0

        # ── SCORE LOGICA ────────────────────────────────────────────────────
        base         = 5.0
        pe_penalty   = (pe / max_pe) * 4.0
        roe_bonus    = min(4.0, (roe / 0.40) * 3.0)
        debt_penalty = min(1.0, de_ratio / 2.5)

        # Groeibonus: omzetgroei > 10% geeft +0.4
        growth_bonus = 0.4 if rev_growth > 0.10 else (0.2 if rev_growth > 0 else 0.0)

        # FCF-kwaliteit: negatieve vrije kasstroom = rode vlag
        fcf_penalty = 0.5 if (fcf is not None and fcf < 0) else 0.0

        # Winstmarge-bonus: gezonde marge > 15% geeft +0.3
        margin_bonus = 0.3 if profit_margin > 0.15 else 0.0

        # Beta-correctie: extreem volatiele aandelen (beta > 2) krijgen kleine straf
        beta_penalty = 0.3 if beta > 2.0 else 0.0

        # Memory-penalty: lessen uit eerdere verliezen in dezelfde sector
        memory_penalty = 0.0
        for lesson in memory.get("lessons", []):
            if lesson.get("sector") == group and lesson.get("type") == "NEGATIVE_LEARNING":
                memory_penalty += 0.5
        memory_penalty = min(2.0, memory_penalty)

        raw_score = (base
                     - pe_penalty
                     + roe_bonus
                     - debt_penalty
                     + growth_bonus
                     - fcf_penalty
                     + margin_bonus
                     - beta_penalty
                     - memory_penalty)
        score = round(max(1.0, min(10.0, raw_score)), 1)

        return {
            "ticker":          ticker_symbol,
            "name":            info.get("shortName", ticker_symbol),
            "sector":          sector,
            "industry_group":  group,
            "roe":             round(roe * 100, 2),
            "pe_ratio":        round(pe, 2),
            "debt_to_equity":  round(de_ratio, 2),
            "revenue_growth":  round(rev_growth * 100, 1),
            "profit_margin":   round(profit_margin * 100, 1),
            "fcf_positive":    fcf is None or fcf >= 0,
            "beta":            round(beta, 2),
            "price":           info.get("currentPrice", 0),
            "score":           score,
            "penalty_applied": memory_penalty > 0,
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

    # Inladen van bestaande data om trades, equity en tier2-analyses niet te overschrijven
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH, "r") as f:
            old_data = json.load(f)
    else:
        old_data = {}

    # DATA-INTEGRITEIT: bewaar bestaande tier2-analyses voor tickers die nog in de top zitten
    old_tier2_by_ticker = {
        c["ticker"]: c["tier2"]
        for c in old_data.get("top_candidates", [])
        if c.get("tier2")
    }
    for c in candidates:
        if c["ticker"] in old_tier2_by_ticker:
            c["tier2"] = old_tier2_by_ticker[c["ticker"]]
            log.info(f"Tier2 cache bewaard voor {c['ticker']}")

    output = {
        "generated_at":  datetime.now(timezone.utc).isoformat(),
        "top_candidates": candidates,
        "active_trades":  old_data.get("active_trades", []),
        "equity_history": old_data.get("equity_history", []),
        "memory":         old_data.get("memory", {}),
        "macro":          fetch_macro(),
        "portfolio":      old_data.get("portfolio", {"cash": 10000.0, "starting_capital": 10000.0}),
    }
    
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=4)
    log.info(f"Done! {len(candidates)} candidates saved with memory-logic.")

    # Telegram: stuur scan-samenvatting
    notify_scan_complete(candidates, scanned)

if __name__ == "__main__":
    main()
