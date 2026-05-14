"""
NEXUS Stock Lookup — individuele aandelenzoeker
Gebruik: python src/stock_lookup.py AAPL
Of via env:  LOOKUP_TICKER=AAPL python src/stock_lookup.py

Resultaat wordt opgeslagen onder data.json["watchlist"].
"""
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import yfinance as yf
import anthropic

BASE_DIR  = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"

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
    "Basic Materials":        "Materials",
    "Utilities":              "Utilities",
    "Real Estate":            "Real Estate",
    "Consumer Defensive":     "Consumer Defensive",
    "Consumer Cyclical":      "Consumer Cyclical",
    "Industrials":            "Industrials",
}


def get_industry_group(sector):
    return SECTOR_MAP.get(sector, "Others")


def _fmt(val, suffix="", na="n/b"):
    return f"{val}{suffix}" if val is not None else na


def compute_roic(info):
    roic = info.get("returnOnCapital")
    if roic is not None and roic != 0:
        return round(float(roic) * 100, 2)
    try:
        op_income    = info.get("operatingCashflow") or info.get("ebitda") or 0
        total_assets = info.get("totalAssets", 0) or 0
        current_liab = info.get("currentLiabilities", 0) or 0
        cash         = info.get("cash", 0) or 0
        invested_cap = total_assets - current_liab - cash
        if op_income and invested_cap > 0:
            tax_rate = info.get("effectiveTaxRate", 0.21) or 0.21
            nopat    = op_income * (1 - tax_rate)
            return round((nopat / invested_cap) * 100, 2)
    except Exception:
        pass
    return None


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


def fetch_fundamentals(ticker_symbol):
    print(f"Fundamentals ophalen voor {ticker_symbol}...")
    t    = yf.Ticker(ticker_symbol)
    info = t.info

    name   = info.get("shortName") or info.get("longName") or ticker_symbol
    sector = info.get("sector", "Unknown")
    group  = get_industry_group(sector)
    price  = info.get("currentPrice") or info.get("regularMarketPrice") or 0

    roe           = (info.get("returnOnEquity", 0) or 0) * 100
    pe            = info.get("trailingPE", 0) or 0
    forward_pe    = info.get("forwardPE", 0) or 0
    de_raw        = info.get("debtToEquity", 0) or 0
    de_ratio      = (de_raw / 100) if de_raw > 5 else de_raw
    rev_growth    = (info.get("revenueGrowth", 0) or 0) * 100
    profit_margin = (info.get("profitMargins", 0) or 0) * 100
    fcf           = info.get("freeCashflow")
    beta          = info.get("beta", 1.0) or 1.0
    market_cap    = info.get("marketCap", 0) or 0

    roic = compute_roic(info)
    pfcf = round(market_cap / fcf, 1) if fcf and fcf > 0 and market_cap > 0 else None

    analyst_target = info.get("targetMeanPrice")
    analyst_count  = info.get("numberOfAnalystOpinions", 0) or 0
    analyst_upside = None
    if analyst_target and price and price > 0:
        analyst_upside = round(((analyst_target / price) - 1) * 100, 1)

    five_yr = compute_5yr_data(t)

    # Nieuws
    news_items = t.news or []
    headlines  = []
    for item in news_items[:8]:
        title = item.get("title") or item.get("headline", "")
        if title:
            headlines.append(f"• {title}")
    news_text = "\n".join(headlines) if headlines else "Geen recent nieuws."

    # Business description
    description = info.get("longBusinessSummary", "")[:400] if info.get("longBusinessSummary") else ""

    return {
        "ticker":          ticker_symbol.upper(),
        "name":            name,
        "sector":          sector,
        "industry_group":  group,
        "price":           round(price, 2),
        "market_cap":      market_cap,
        "roe":             round(roe, 2),
        "pe_ratio":        round(pe, 2),
        "forward_pe":      round(forward_pe, 2),
        "debt_to_equity":  round(de_ratio, 2),
        "revenue_growth":  round(rev_growth, 1),
        "profit_margin":   round(profit_margin, 1),
        "fcf_positive":    fcf is None or fcf >= 0,
        "beta":            round(beta, 2),
        "roic":            roic,
        "pfcf":            pfcf,
        "rev_cagr_5yr":    five_yr["rev_cagr_5yr"],
        "ni_cagr_5yr":     five_yr["ni_cagr_5yr"],
        "analyst_target":  analyst_target,
        "analyst_upside":  analyst_upside,
        "analyst_count":   analyst_count,
        "description":     description,
        "news":            news_text,
    }


def run_ai_analysis(client, fund):
    ticker = fund["ticker"]
    print(f"AI-analyse starten voor {ticker}...")

    prompt = (
        f"Analyseer {ticker} ({fund['name']}) als een waardebelegger met een horizon van 3-7 jaar.\n\n"
        f"BEDRIJFSBESCHRIJVING:\n{fund['description']}\n\n"
        f"FUNDAMENTALS:\n"
        f"- Sector: {fund['industry_group']} | Prijs: ${fund['price']}\n"
        f"- ROE: {_fmt(fund['roe'], '%')} | ROIC: {_fmt(fund['roic'], '%')} "
        f"| P/E: {_fmt(fund['pe_ratio'])} | Forward P/E: {_fmt(fund['forward_pe'])}\n"
        f"- D/E: {_fmt(fund['debt_to_equity'])} | P/FCF: {_fmt(fund['pfcf'])}\n"
        f"- FCF: {'positief' if fund['fcf_positive'] else 'NEGATIEF'} | Beta: {_fmt(fund['beta'])}\n"
        f"- Winstmarge: {_fmt(fund['profit_margin'], '%')} | Omzetgroei: {_fmt(fund['revenue_growth'], '%')}\n"
        f"- 5-jaar omzet CAGR: {_fmt(fund['rev_cagr_5yr'], '% p.j.')} "
        f"| 5-jaar winst CAGR: {_fmt(fund['ni_cagr_5yr'], '% p.j.')}\n"
        f"- Analistendoel: ${_fmt(fund['analyst_target'])} "
        f"({_fmt(fund['analyst_upside'], '% upside')}, {fund['analyst_count']} analisten)\n\n"
        f"RECENT NIEUWS:\n{fund['news']}\n\n"
        f"Schrijf een grondige analyse (max 450 woorden) in 6 punten:\n"
        f"1. BUSINESS KWALITEIT: Begrijpelijk businessmodel? Duurzame inkomsten? Pricing power?\n"
        f"2. MOAT: Concurrentievoordeel (switching costs, network effects, schaalvoordeel, merken)?\n"
        f"3. MANAGEMENT & ROIC: Bewijst management goed kapitaal te alloceren? ROIC trend?\n"
        f"4. WAARDERING: Goedkoop, fair of duur t.o.v. intrinsieke waarde? P/FCF en groei in context.\n"
        f"5. GROEIPOTENTIEEL: Seculiere tailwinds? TAM? 5-jaars trend duurzaam?\n"
        f"6. RISICO'S: Top 2 concrete risico's voor beleggers.\n\n"
        f"Sluit af met:\n"
        f"EINDOORDEEL: [KOOP / HOUD / MIJDEN]\n"
        f"CONVICTION: [1-10]\n"
        f"KOERSDOEL (12-18mnd): $[bedrag] ([X]% van huidige prijs)"
    )

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1100,
        system=[{
            "type": "text",
            "text": (
                "Je bent een expert waardebelegger die analyseert als Buffett, Munger en Graham. "
                "Je schrijft heldere, kritische en diepgaande analyses in het Nederlands. "
                "Je bent direct en waarschuwt eerlijk bij risico's of dure waarderingen."
            ),
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def load_json(path, default):
    if not path.exists():
        return default
    with open(path) as f:
        try:
            return json.load(f)
        except Exception:
            return default


def main():
    # Ticker ophalen: argv → env var
    ticker_input = None
    if len(sys.argv) > 1:
        ticker_input = sys.argv[1].strip().upper()
    if not ticker_input:
        ticker_input = os.environ.get("LOOKUP_TICKER", "").strip().upper()
    if not ticker_input:
        print("Gebruik: python src/stock_lookup.py TICKER")
        print("Of:      LOOKUP_TICKER=AAPL python src/stock_lookup.py")
        sys.exit(1)

    print(f"=== NEXUS STOCK LOOKUP: {ticker_input} ===")

    # Fundamentals ophalen
    try:
        fund = fetch_fundamentals(ticker_input)
    except Exception as e:
        print(f"Fout bij ophalen data voor {ticker_input}: {e}")
        sys.exit(1)

    print(f"  {fund['name']} | ${fund['price']} | {fund['industry_group']}")
    print(f"  ROE={fund['roe']}% | ROIC={fund['roic']} | P/E={fund['pe_ratio']} | P/FCF={fund['pfcf']}")

    # AI-analyse
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    analysis_text = None
    if api_key:
        client = anthropic.Anthropic(api_key=api_key)
        try:
            analysis_text = run_ai_analysis(client, fund)
            print("\n--- ANALYSE ---")
            print(analysis_text)
        except Exception as e:
            print(f"AI-analyse mislukt: {e}")
    else:
        print("Geen ANTHROPIC_API_KEY — alleen fundamentals opgeslagen.")

    # Opslaan in data.json watchlist
    data = load_json(DATA_PATH, {})
    watchlist = data.get("watchlist", [])

    # Verwijder eventueel bestaande entry voor dit ticker
    watchlist = [w for w in watchlist if w.get("ticker") != ticker_input]

    entry = {
        **fund,
        "lookup_at": datetime.now(timezone.utc).isoformat(),
        "tier2": {
            "analysis": analysis_text,
            "last_run": datetime.now(timezone.utc).isoformat(),
            "model":    "claude-sonnet-4-6",
        } if analysis_text else None,
    }
    watchlist.insert(0, entry)
    data["watchlist"] = watchlist[:20]  # Bewaar max 20 watchlist-items

    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=4)

    print(f"\nResultaat opgeslagen in data.json (watchlist).")
    print("=== LOOKUP KLAAR ===")


if __name__ == "__main__":
    main()
