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

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tier1_scanner")

# Paden definitie
BASE_DIR = Path(__file__).parent.parent
OUTPUT_PATH = BASE_DIR / "data.json"
TRADES_PATH = BASE_DIR / "trades.json"

def fetch_news(query: str) -> list:
    """Scoort de laatste 3 nieuwskoppen via Google News."""
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
    """Combineert S&P 500, Nasdaq 100 en de Nederlandse markt."""
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
        nasdaq_df = pd.read_html(StringIO(res.text))[4]
        tickers.extend(nasdaq_df["Ticker"].tolist())
        log.info("Nasdaq 100 tickers toegevoegd.")
    except: pass

    # 3. Nederlandse Markt
    dutch_market = [
        "ASML.AS", "ADYEN.AS", "UNA.AS", "HEIA.AS", "INGA.AS", "REN.AS", "ASM.AS", "AKZA.AS", "SHELL.AS",
        "AD.AS", "ABN.AS", "ASRNL.AS", "BEP0.AS", "BESI.AS", "DSFIR.AS", "IMCD.AS", "KPN.AS", "MT.AS",
        "NN.AS", "PHIA.AS", "PRX.AS", "RAND.AS", "UMG.AS", "URW.AS", "WKL.AS",
        "AALB.AS", "AIRF.AS", "AMG.AS", "APAM.AS", "ARDS.AS", "BAMN.AS", "BFIT.AS", "CORB.AS", "CTP.AS", 
        "FLOW.AS", "FUGR.AS", "GLPG.AS", "JDEP.AS", "LIGHT.AS", "SBMO.AS", "VOPA.AS"
    ]
    tickers.extend(dutch_market)
    return list(set(tickers))

def fetch_macro_indicators() -> dict:
    """Haalt VIX en Treasury yields op."""
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

def check_buffett_quality(info: dict) -> tuple[bool, str]:
    """
    Buffett Check: Kijkt of het bedrijf een kwalitatief goede business is.
    ROE > 15% en Debt-to-Equity < 200%.
    """
    roe = info.get("returnOnEquity", 0)
    debt_equity = info.get("debtToEquity", 0)
    
    # Als ROE negatief is of lager dan 15% (0.15), skippen we.
    if roe < 0.15:
        return False, f"ROE te laag ({round(roe*100, 1)}%)"
    
    # Als de schuld meer dan 2x het eigen vermogen is, skippen we (behalve bij financials/utilities, maar we houden het streng)
    if debt_equity > 200:
        return False, f"Schuld te hoog ({round(debt_equity, 1)}%)"
    
    return True, "Quality Business"

def analyse_ticker(ticker: str) -> dict | None:
    """Filtert op basis van kwaliteit, fundamentelen en scores."""
    try:
        t = yf.Ticker(ticker)
        info = t.info
        
        # --- BUFFETT QUALITY FILTER (BUSINESS FIRST) ---
        is_quality, reason = check_buffett_quality(info)
        if not is_quality:
            # We loggen dit niet voor elk aandeel om de console schoon te houden
            return None

        # --- FUNDAMENTELE CIJFERS ---
        raw_div = info.get("dividendYield") or 0
        div_yield = float(raw_div / 100 if raw_div > 0.20 else raw_div)
        pe = info.get("trailingPE")
        
        # Extra prijs-checks
        if not pe or div_yield < 0.005 or div_yield > 0.15: 
            return None 

        name = str(info.get("shortName", ticker))
        news = fetch_news(name)

        # Bereken de score (Buffett-bonus voor hoge ROE)
        roe = info.get("returnOnEquity", 0)
        score = round(10 - (pe/10) + (div_yield * 20) + (roe * 5), 1)

        return {
            "ticker": ticker,
            "name": name,
            "sector": str(info.get("sector", "Unknown")),
            "price": float(info.get("currentPrice", 0)),
            "dividend_yield": round(div_yield * 100, 2),
            "pe_ratio": round(float(pe), 2),
            "roe": round(roe * 100, 2),
            "news": news,
            "score": score,
            "scanned_at": datetime.now(timezone.utc).isoformat()
        }
    except Exception:
        return None

def main():
    log.info("=== NEXUS Tier 1 GLOBAL HUNTER starting ===")
    log.info("BUFFETT MODE: ROE > 15% filter enabled.")
    
    macro = fetch_macro_indicators()
    universe = fetch_global_universe()
    log.info(f"Scanning universe of {len(universe)} tickers...")
    
    candidates = []
    for ticker in universe:
        res = analyse_ticker(ticker)
        if res:
            candidates.append(res)
            log.info(f"PASS {ticker} (Quality Score: {res['score']}, ROE: {res['roe']}%)")
        
        # We stoppen bij 15 sterke kandidaten voor de Tier 2 Analyse
        if len(candidates) >= 15: 
            break
        
        time.sleep(0.05) # Rate limiting voorkomen
    
    candidates.sort(key=lambda x: x['score'], reverse=True)

    # --- BACKTESTER INTEGRATIE ---
    active_trades = []
    if TRADES_PATH.exists():
        try:
            with open(TRADES_PATH, "r") as f:
                all_trades = json.load(f)
                active_trades = all_trades[-5:] # Laatste 5 trades
        except Exception as e:
            log.warning(f"Kon trades.json niet laden: {e}")

    # Opslaan van resultaten
    with open(OUTPUT_PATH, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(), 
            "macro": macro, 
            "top_candidates": candidates[:10],
            "active_trades": active_trades
        }, f, indent=2)
    
    log.info(f"=== Done! {len(candidates)} quality candidates passed the Buffett-filter. ===")

if __name__ == "__main__":
    main()
