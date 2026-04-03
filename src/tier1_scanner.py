import json
import os
import time
import logging
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
import yfinance as yf
import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tier1_scanner")

OUTPUT_PATH = Path(__file__).parent.parent / "data.json"

def fetch_news(query: str) -> list:
    """Scoort de laatste 3 nieuwskoppen via Google News (Gratis)."""
    news_items = []
    try:
        encoded_query = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded_query}&hl=nl&gl=NL&ceid=NL:nl"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'xml')
        
        items = soup.find_all('item')
        for item in items[:3]: # Pak de top 3
            news_items.append({
                "title": item.title.text,
                "link": item.link.text,
                "date": item.pubDate.text
            })
    except Exception as e:
        log.warning(f"Nieuws fetch mislukt voor {query}: {e}")
    return news_items

def fetch_global_universe() -> list[str]:
    tickers = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    # 1. S&P 500
    try:
        res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=headers, timeout=10)
        tickers.extend(pd.read_html(StringIO(res.text))[0]["Symbol"].str.replace(".", "-", regex=False).tolist())
    except: pass
    # 2. AEX selectie
    tickers.extend(["ASML.AS", "ADYEN.AS", "UNA.AS", "HEIA.AS", "INGA.AS", "REN.AS", "ASM.AS", "AKZA.AS", "SHELL.AS"])
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
        
        if not pe or div_yield < 0.005 or div_yield > 0.15: return None 

        name = str(info.get("shortName", ticker))
        # Haal nieuws op voor dit specifieke aandeel
        news = fetch_news(name)

        return {
            "ticker": ticker,
            "name": name,
            "sector": str(info.get("sector", "Unknown")),
            "price": float(info.get("currentPrice", 0)),
            "dividend_yield": round(div_yield * 100, 2),
            "pe_ratio": round(float(pe), 2),
            "eps_growth_3yr": round(min(float(info.get("earningsQuarterlyGrowth", 0.15)), 1.0) * 100, 2),
            "news": news, # Hier zit de winst!
            "score": round(10 - (pe/10) + (div_yield * 20), 1),
            "scanned_at": datetime.now(timezone.utc).isoformat()
        }
    except: return None

def main():
    log.info("=== NEXUS Tier 1 News Hunter starting ===")
    macro = fetch_macro_indicators()
    universe = fetch_global_universe()
    candidates = []
    
    for ticker in universe:
        res = analyse_ticker(ticker)
        if res:
            candidates.append(res)
            log.info(f"PASS {ticker} met nieuws-update.")
        if len(candidates) >= 10: break
        time.sleep(0.1)
    
    candidates.sort(key=lambda x: x['score'], reverse=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(), 
            "macro": macro, 
            "top_candidates": candidates[:5]
        }, f, indent=2)
    log.info("=== Done! Dashboard verrijkt met nieuws. ===")

if __name__ == "__main__":
    main()
