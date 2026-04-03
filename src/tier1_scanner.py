import json
import os
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tier1_scanner")

OUTPUT_PATH = Path(__file__).parent.parent / "data.json"

def fetch_news(query: str) -> list:
    """Scoort de laatste 3 nieuwskoppen via Google News met robuuste Regex."""
    news_items = []
    try:
        clean_query = query.split(',')[0].split(' N.V.')[0] + " stock news"
        encoded_query = urllib.parse.quote(clean_query)
        url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en&gl=US&ceid=US:en"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        
        titles = re.findall(r'<title>(.*?)</title>', response.text)
        links = re.findall(r'<link>(.*?)</link>', response.text)
        
        for i in range(1, min(len(titles), 4)):
            clean_title = titles[i].replace('<![CDATA[', '').replace(']]>', '')
            news_items.append({
                "title": clean_title,
                "link": links[i],
                "date": "Recent"
            })
    except Exception as e:
        log.warning(f"Nieuws fetch mislukt voor {query}: {e}")
    return news_items

def fetch_global_universe() -> list[str]:
    """Combineert S&P 500, Nasdaq 100 en de volledige Nederlandse markt."""
    tickers = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 1. S&P 500
    try:
        res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=headers, timeout=10)
        tickers.extend(pd.read_html(StringIO(res.text))[0]["Symbol"].str.replace(".", "-", regex=False).tolist())
        log.info("S&P 500 tickers toegevoegd.")
    except Exception as e: log.error(f"S&P 500 fetch error: {e}")

    # 2. Nasdaq 100
    try:
        res = requests.get("https://en.wikipedia.org/wiki/Nasdaq-100", headers=headers, timeout=10)
        # Wikipedia heeft vaak meerdere tabellen, tabel 4 is meestal de Nasdaq 100 lijst
        nasdaq_df = pd.read_html(StringIO(res.text))[4]
        tickers.extend(nasdaq_df["Ticker"].tolist())
        log.info("Nasdaq 100 tickers toegevoegd.")
    except: pass

    # 3. Nederlandse Markt (AEX + AMX + Selectie)
    dutch_market = [
        # AEX
        "ASML.AS", "ADYEN.AS", "UNA.AS", "HEIA.AS", "INGA.AS", "REN.AS", "ASM.AS", "AKZA.AS", "SHELL.AS",
        "AD.AS", "ABN.AS", "ASRNL.AS", "BEP0.AS", "BESI.AS", "DSFIR.AS", "IMCD.AS", "KPN.AS", "MT.AS",
        "NN.AS", "PHIA.AS", "PRX.AS", "RAND.AS", "UMG.AS", "URW.AS", "WKL.AS",
        # AMX & Midcaps
        "AALB.AS", "AIRF.AS", "AMG.AS", "APAM.AS", "ARDS.AS", "BAMN.AS", "BFIT.AS", "CORB.AS", "CTP.AS", 
        "FLOW.AS", "FUGR.AS", "GLPG.AS", "JDEP.AS", "LIGHT.AS", "SBMO.AS", "VOPA.AS"
    ]
    tickers.extend(dutch_market)
    
    return list(set(tickers))

def fetch_macro_indicators() -> dict:
    indicators = {}
    try:
        vix = yf.Ticker("^VIX").fast_info["lastPrice"]
        indicators["vix"] = round(float(vix), 2)
        tnx = yf.Ticker("^TNX").fast_info["lastPrice"]
        val = float(tnx)
        indicators["treasury_10y"] = round(val / 10 if val > 15 else val, 2)
    except: pass
    indicators["sp500_rsi"] = 47.3
    return indicators

def analyse_ticker(ticker: str) -> dict | None:
    try:
        t = yf.Ticker(ticker)
        info = t.info
        raw_div = info.get("dividendYield") or 0
        div_yield = float(raw_div / 100 if raw_div > 0.20 else raw_div)
        pe = info.get("trailingPE")
        
        # Strenge filter: Alleen winstgevende dividend-uitkerende aandelen
        if not pe or div_yield < 0.005 or div_yield > 0.15: return None 

        name = str(info.get("shortName", ticker))
        news = fetch_news(name)

        return {
            "ticker": ticker,
            "name": name,
            "sector": str(info.get("sector", "Unknown")),
            "price": float(info.get("currentPrice", 0)),
            "dividend_yield": round(div_yield * 100, 2),
            "pe_ratio": round(float(pe), 2),
            "eps_growth_3yr": round(min(float(info.get("earningsQuarterlyGrowth", 0.15)), 1.0) * 100, 2),
            "news": news,
            "score": round(10 - (pe/10) + (div_yield * 20), 1),
            "scanned_at": datetime.now(timezone.utc).isoformat()
        }
    except: return None

def main():
    log.info("=== NEXUS Tier 1 GLOBAL HUNTER starting ===")
    macro = fetch_macro_indicators()
    universe = fetch_global_universe()
    log.info(f"Scanning universe of {len(universe)} tickers...")
    
    candidates = []
    
    for ticker in universe:
        res = analyse_ticker(ticker)
        if res:
            candidates.append(res)
            log.info(f"PASS {ticker} (Score: {res['score']})")
        
        # We scannen door tot we 15 sterke kandidaten hebben (iets meer voor Claude om uit te kiezen)
        if len(candidates) >= 15: break
        time.sleep(0.05) # Iets sneller scannen
    
    candidates.sort(key=lambda x: x['score'], reverse=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(), 
            "macro": macro, 
            "top_candidates": candidates[:10] # De beste 10 gaan naar data.json
        }, f, indent=2)
    log.info(f"=== Done! {len(candidates)} candidates found. ===")

if __name__ == "__main__":
    main()
