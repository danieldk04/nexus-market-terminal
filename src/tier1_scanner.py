import json
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path
import yfinance as yf
import pandas as pd
from io import StringIO
from dcf_engine import compute_dcf, compute_roic, compute_roce, check_dividend_sustainability
from notifier import notify_scan_complete

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tier1_scanner")

BASE_DIR    = Path(__file__).parent.parent
OUTPUT_PATH = BASE_DIR / "data.json"
MEMORY_PATH = BASE_DIR / "memory.json"

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

FALLBACK_TICKERS = [
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","JPM","V","MA","LLY",
    "AVGO","COST","INTU","ISRG","TMO","ACN","NOW","AMAT","AMD","ARM",
    "CRWD","ZS","PANW","NET","DDOG","SNOW","MDB","GTLB","APP","TTD",
    "PLTR","AXON","MELI","ASML","TSM","NVO","SHOP","MRVL","SMCI","UBER",
]

# High-growth universe: always scanned regardless of scraping success
# AI / Semiconductors / Cyber / Defense / Cloud / Fintech / Energy
GROWTH_UNIVERSE = [
    # Semiconductors & AI Infrastructure
    "NVDA","AMD","ARM","SMCI","MRVL","AVGO","AMAT","LRCX","KLAC","ONTO","ACMR",
    # Cybersecurity
    "CRWD","ZS","PANW","NET","DDOG","FTNT","S","CYBR","OKTA","QLYS",
    # Cloud & Enterprise Software
    "NOW","HUBS","SNOW","MDB","GTLB","APP","TTD","MNDY","BILL","AXON",
    # Defense & Space
    "PLTR","RKLB","LDOS","LHX","NOC",
    # Fintech & Payments
    "NU","SOFI","AFRM","UPST","ADYEN.AS",
    # Healthcare Innovation
    "MRNA","VRTX","REGN","EXAS",
    # Energy Transition
    "CEG","VST","FSLR","ENPH",
    # Global growth leaders
    "ASML","ASML.AS","TSM","NVO","MELI","SEA","SHOP","UBER","TSLA",
    # Netherlands
    "BESI.AS","TKWY.AS",
]


def get_industry_group(sector):
    return SECTOR_MAP.get(sector, "Others")


def load_memory():
    if not MEMORY_PATH.exists():
        return {"lessons": []}
    with open(MEMORY_PATH) as f:
        return json.load(f)


def fetch_global_universe():
    tickers = []
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NEXUSBot/3.0)"}

    # S&P 500
    try:
        res = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=headers, timeout=15
        )
        sp500 = pd.read_html(StringIO(res.text))[0]["Symbol"].str.replace(".", "-", regex=False).tolist()
        tickers.extend(sp500)
        log.info(f"S&P 500: {len(sp500)} tickers")
    except Exception as e:
        log.warning(f"S&P 500 scraping mislukt: {e}")

    # Nasdaq-100 — meerdere tabel-indices proberen
    for idx in [4, 3, 5, 2]:
        try:
            res    = requests.get("https://en.wikipedia.org/wiki/Nasdaq-100", headers=headers, timeout=15)
            tables = pd.read_html(StringIO(res.text))
            for col in ["Ticker", "Symbol", "Tick"]:
                if col in tables[idx].columns:
                    ndx = tables[idx][col].dropna().tolist()
                    tickers.extend(ndx)
                    log.info(f"Nasdaq-100: {len(ndx)} tickers (tabel {idx})")
                    break
            break
        except Exception:
            continue

    # NL selectie
    tickers.extend(["ASML.AS","INGA.AS","ADYEN.AS","UNA.AS","HEIA.AS","PHIA.AS","WKL.AS"])

    if len(tickers) < 60:
        log.warning("Scraping laag — fallback actief")
        tickers.extend(FALLBACK_TICKERS)

    # Altijd de high-growth universe toevoegen (prioriteit bij sortering)
    tickers.extend(GROWTH_UNIVERSE)

    # Groei-universe vooraan zetten zodat ze altijd binnen MAX_SCAN vallen
    growth_set = set(GROWTH_UNIVERSE)
    others = [t for t in tickers if t not in growth_set]
    unique = list(dict.fromkeys(GROWTH_UNIVERSE + others))  # dedup, volgorde bewaren
    log.info(f"Universe: {len(unique)} unieke tickers ({len(GROWTH_UNIVERSE)} growth-priority)")
    return unique


def fetch_macro():
    macro = {"vix": None, "treasury_10y": None}
    try:
        macro["vix"] = round(yf.Ticker("^VIX").info.get("regularMarketPrice", 0) or 0, 2)
    except Exception:
        pass
    try:
        macro["treasury_10y"] = round(yf.Ticker("^TNX").info.get("regularMarketPrice", 0) or 0, 2)
    except Exception:
        pass
    log.info(f"Macro: VIX={macro['vix']}  10Y={macro['treasury_10y']}%")
    return macro


def compute_5yr_data(t):
    result = {"rev_cagr_5yr": None, "ni_cagr_5yr": None}
    try:
        fin = t.financials
        if fin is None or fin.empty:
            return result

        def _cagr(keys):
            for key in keys:
                matches = [k for k in fin.index if key in str(k)]
                if matches:
                    series = fin.loc[matches[0]].dropna()
                    if len(series) >= 2:
                        n = float(series.iloc[0])
                        o = float(series.iloc[-1])
                        y = len(series) - 1
                        if o > 0 and n > 0:
                            return round(((n / o) ** (1 / y) - 1) * 100, 1)
            return None

        result["rev_cagr_5yr"] = _cagr(["Total Revenue", "Revenue"])
        result["ni_cagr_5yr"]  = _cagr(["Net Income", "Net Income Common Stockholders"])
    except Exception:
        pass
    return result


def analyse_ticker(ticker_symbol, memory, post_mortem):
    try:
        t    = yf.Ticker(ticker_symbol)
        info = t.info

        # Minimum marktkapitalisatie: $500M — vermijd micro-caps
        market_cap = info.get("marketCap", 0) or 0
        if market_cap > 0 and market_cap < 500_000_000:
            return None

        sector = info.get("sector", "Unknown")
        group  = get_industry_group(sector)
        pe     = info.get("trailingPE", 0) or 0

        # Schuldenfilter — te veel schuld = structureel risico
        de_raw   = info.get("debtToEquity", 0) or 0
        de_ratio = (de_raw / 100) if de_raw > 5 else de_raw
        if de_ratio > 3.0:
            return None

        rev_growth    = info.get("revenueGrowth", 0) or 0
        gross_margin  = info.get("grossMargins", 0) or 0
        fcf           = info.get("freeCashflow")
        profit_margin = info.get("profitMargins", 0) or 0
        beta          = info.get("beta", 1.0) or 1.0
        price         = info.get("currentPrice") or info.get("regularMarketPrice") or 0
        roe           = info.get("returnOnEquity", 0) or 0
        total_revenue = info.get("totalRevenue", 0) or 0

        # Hard filter: dalende omzet + verlieslatend = geen groeikans
        if rev_growth < -0.10 and profit_margin < 0:
            return None

        # Trend-filter (versoepeld: groeiaandelen mogen correctie-fase doorstaan)
        ma50  = info.get("fiftyDayAverage") or 0
        ma200 = info.get("twoHundredDayAverage") or 0
        trend_penalty = 0.0
        if ma50 > 0 and price < ma50 * 0.97:
            trend_penalty = 0.3
        if ma200 > 0 and price < ma200 * 0.97:
            trend_penalty = max(trend_penalty, 0.6)
        # Hard filter alleen bij diepe downtrend + omzetdaling
        if ma50 > 0 and ma200 > 0 and price < ma50 * 0.88 and price < ma200 * 0.88 and rev_growth < 0:
            return None

        # Hulpberekeningen
        roic = compute_roic(info)
        roce = compute_roce(info)
        pfcf = round(market_cap / fcf, 1) if fcf and fcf > 0 and market_cap > 0 else None

        analyst_target = info.get("targetMeanPrice")
        analyst_count  = info.get("numberOfAnalystOpinions", 0) or 0
        analyst_upside = None
        if analyst_target and price and price > 0:
            analyst_upside = round(((analyst_target / price) - 1) * 100, 1)

        five_yr   = compute_5yr_data(t)
        dcf       = compute_dcf(info)
        div_check = check_dividend_sustainability(info)

        # ── HIGH-GROWTH SCORE LOGICA ──────────────────────────────────────────
        base  = 5.0
        rg_pct = rev_growth * 100
        gm_pct = gross_margin * 100

        # 1. Omzetgroei — primair signaal (max +2.5, min -1.0)
        if rg_pct >= 50:     growth_bonus = 2.5
        elif rg_pct >= 30:   growth_bonus = 2.0
        elif rg_pct >= 20:   growth_bonus = 1.5
        elif rg_pct >= 10:   growth_bonus = 0.8
        elif rg_pct >= 0:    growth_bonus = 0.2
        else:                growth_bonus = max(-1.0, rg_pct * 0.04)

        # 2. Bruto-marge — kwaliteit van het businessmodel (max +1.0, min -0.3)
        if gm_pct >= 70:     margin_bonus = 1.0
        elif gm_pct >= 50:   margin_bonus = 0.6
        elif gm_pct >= 30:   margin_bonus = 0.2
        elif gm_pct > 0:     margin_bonus = max(-0.3, (gm_pct - 30) * 0.01)
        else:                margin_bonus = 0.0

        # 3. Omzet-CAGR 5 jaar — aanhoudende compounding (max +0.8)
        rev_cagr = five_yr.get("rev_cagr_5yr")
        cagr_bonus = 0.0
        if rev_cagr is not None:
            if rev_cagr >= 25:   cagr_bonus = 0.8
            elif rev_cagr >= 15: cagr_bonus = 0.5
            elif rev_cagr >= 10: cagr_bonus = 0.2

        # 4. PEG-ratio (P/E ÷ groeipercentage) — slimmer dan kale P/E
        peg_bonus = 0.0
        if pe > 0 and rg_pct > 2:
            peg = pe / rg_pct
            if peg < 1.0:    peg_bonus = 0.8
            elif peg < 2.0:  peg_bonus = 0.4
            elif peg < 3.5:  peg_bonus = 0.1
            elif peg > 7.0:  peg_bonus = -0.5
        elif pe <= 0 and rg_pct < 20:
            # Verlieslatend én niet snel genoeg groeiend
            peg_bonus = -0.3

        # 5. Free Cash Flow — toont of het model al werkt op schaal
        fcf_bonus = 0.0
        if fcf is not None and fcf > 0 and total_revenue > 0:
            fcf_margin = fcf / total_revenue
            if fcf_margin > 0.20:   fcf_bonus = 0.5
            elif fcf_margin > 0.10: fcf_bonus = 0.3
            else:                   fcf_bonus = 0.1
        elif fcf is not None and fcf < 0:
            # Negatieve FCF acceptabel voor snelle groeiers
            fcf_bonus = -0.1 if rg_pct >= 30 else -0.5

        # 6. Momentum — koers bevestigt de groeiverhaal
        momentum_bonus = 0.0
        if ma50 > 0 and ma200 > 0:
            if price > ma50 and ma50 > ma200:
                momentum_bonus = 0.5    # Golden cross, opgaande trend
            elif price > ma200:
                momentum_bonus = 0.2

        # 7. Analisten-upside
        analyst_bonus = 0.0
        if analyst_upside is not None and analyst_count >= 5:
            if analyst_upside > 30:   analyst_bonus = 0.5
            elif analyst_upside > 20: analyst_bonus = 0.3
            elif analyst_upside > 10: analyst_bonus = 0.15

        # 8. DCF-opwaarts potentieel (ondersteunend, niet dominant)
        dcf_bonus = 0.0
        if dcf is not None and dcf.get("dcf_upside") is not None:
            upside = dcf["dcf_upside"]
            if upside > 40:    dcf_bonus = 0.6
            elif upside > 20:  dcf_bonus = 0.3
            elif upside < -20: dcf_bonus = -0.4

        # 9. Thematische bonus: AI / Semiconductors / Cyber / Defense / Space
        GROWTH_THEMES = {
            "NVDA","AMD","ARM","SMCI","MRVL","AVGO","TSM","AMAT","LRCX","KLAC","ACMR","ONTO",
            "CRWD","ZS","PANW","NET","DDOG","SNOW","S","FTNT","CYBR","OKTA","QLYS",
            "PLTR","RKLB","AXON","LHX","NOC","LDOS",
            "NOW","HUBS","GTLB","APP","TTD","MDB","MNDY","BILL",
            "NU","SOFI","AFRM","UPST","ADYEN.AS",
            "CEG","VST","FSLR","ENPH",
            "ASML","ASML.AS","NVO","MELI","SEA","SHOP","TSLA",
        }
        thematic_bonus = 0.3 if ticker_symbol in GROWTH_THEMES else 0.0

        # 10. Schulden-straf (behouden)
        debt_penalty = min(1.2, de_ratio / 3.0)

        # 11. Beta-straf (alleen bij extreme volatiliteit)
        beta_penalty = 0.3 if beta > 2.5 else 0.0

        # 12. Dividend-risico (kleiner gewicht — minder relevant voor groeiaandelen)
        div_penalty = 0.0
        if div_check is not None and not div_check["sustainable"]:
            div_penalty = 0.3

        # 13. Memory-lessen en post-mortem sectoraanpassingen
        memory_penalty = 0.0
        for lesson in memory.get("lessons", []):
            if lesson.get("sector") == group and lesson.get("type") == "NEGATIVE_LEARNING":
                memory_penalty += 0.4
        pm_adjustments = post_mortem.get("sector_adjustments", {})
        pm_adj = pm_adjustments.get(group, 0)
        memory_penalty = min(2.0, memory_penalty)

        raw_score = (
            base + growth_bonus + margin_bonus + cagr_bonus + peg_bonus
            + fcf_bonus + momentum_bonus + analyst_bonus + dcf_bonus
            + thematic_bonus - debt_penalty - beta_penalty
            - div_penalty - memory_penalty + pm_adj - trend_penalty
        )
        score = round(max(1.0, min(10.0, raw_score)), 1)

        return {
            "ticker":          ticker_symbol,
            "name":            info.get("shortName", ticker_symbol),
            "sector":          sector,
            "industry_group":  group,
            "price":           round(price, 2) if price else 0,
            "roe":             round(roe * 100, 2),
            "pe_ratio":        round(pe, 2),
            "debt_to_equity":  round(de_ratio, 2),
            "revenue_growth":  round(rev_growth * 100, 1),
            "gross_margin":    round(gross_margin * 100, 1),
            "profit_margin":   round(profit_margin * 100, 1),
            "fcf_positive":    fcf is None or fcf >= 0,
            "beta":            round(beta, 2),
            "roic":            roic,
            "roce":            roce,
            "pfcf":            pfcf,
            "rev_cagr_5yr":    five_yr["rev_cagr_5yr"],
            "ni_cagr_5yr":     five_yr["ni_cagr_5yr"],
            "analyst_target":  analyst_target,
            "analyst_upside":  analyst_upside,
            "analyst_count":   analyst_count,
            "dcf":             dcf,
            "dividend":        div_check,
            "score":           score,
            "penalty_applied": memory_penalty > 0 or pm_adj < 0,
        }
    except Exception:
        return None


MAX_SCAN = 400
TOP_N    = 40


def main():
    log.info("=== NEXUS DEEP VALUE SCAN STARTING ===")
    memory     = load_memory()
    post_mortem = memory.get("post_mortem", {})
    universe   = fetch_global_universe()
    log.info(f"Universe: {len(universe)} tickers, scanning max {MAX_SCAN}")

    candidates = []
    scanned    = 0
    for ticker in universe:
        if scanned >= MAX_SCAN:
            break
        scanned += 1
        data = analyse_ticker(ticker, memory, post_mortem)
        if data:
            candidates.append(data)
            dcf_str = f" DCF={data['dcf']['dcf_upside']}%" if data.get("dcf") else ""
            log.info(
                f"PASS: {ticker:8s} Score={data['score']} "
                f"ROIC={data['roic']} ROCE={data['roce']}{dcf_str}"
            )
        time.sleep(0.05)

    candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)[:TOP_N]
    log.info(f"Top {len(candidates)} geselecteerd uit {scanned} gescand.")

    # Laad bestaande data
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH) as f:
            old_data = json.load(f)
    else:
        old_data = {}

    # Bewaar bestaande tier2 analyses
    old_tier2 = {
        c["ticker"]: c["tier2"]
        for c in old_data.get("top_candidates", [])
        if c.get("tier2")
    }
    for c in candidates:
        if c["ticker"] in old_tier2:
            c["tier2"] = old_tier2[c["ticker"]]
            # Pas score aan op basis van Tier-2 AI-sentiment (eerder berekend maar nooit gebruikt)
            sentiment = old_tier2[c["ticker"]].get("sentiment_score", "")
            if sentiment == "BULLISH":
                c["score"] = round(min(10.0, c["score"] + 0.3), 1)
            elif sentiment == "BEARISH":
                c["score"] = round(max(1.0, c["score"] - 0.5), 1)

    # Hersorteren na sentiment-aanpassingen
    candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)

    output = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "top_candidates": candidates,
        "active_trades":  old_data.get("active_trades", []),
        "equity_history": old_data.get("equity_history", []),
        "memory":         old_data.get("memory", {}),
        "macro":          fetch_macro(),
        "portfolio":      old_data.get("portfolio", {"cash": 10000.0, "starting_capital": 10000.0}),
        "watchlist":      old_data.get("watchlist", []),
        "filings":        old_data.get("filings", {}),
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=4)

    log.info(f"Klaar — {len(candidates)} kandidaten opgeslagen.")
    notify_scan_complete(candidates, scanned)


if __name__ == "__main__":
    main()
