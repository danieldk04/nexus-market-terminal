"""
NEXUS Stock Lookup — watchlist-analyse voor één aandeel.

Gebruik: python src/stock_lookup.py AAPL
Of via env:  LOOKUP_TICKER=AAPL python src/stock_lookup.py
Verwijderen: LOOKUP_ACTION=remove LOOKUP_TICKER=AAPL python src/stock_lookup.py

Haalt fundamentals, kwartaalcijfers, waardering (DCF), koersmomentum en
sentiment op, laat Claude er een oordeel over vellen en slaat het resultaat
gestructureerd op onder data.json["watchlist"].
"""
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf
import anthropic

BASE_DIR = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"

MODEL = "claude-sonnet-5"
MAX_WATCHLIST = 40

SECTOR_MAP = {
    "Technology":              "Tech & AI",
    "Communication Services":  "Tech & AI",
    "Software":                "Tech & AI",
    "Information Technology":  "Tech & AI",
    "Financial Services":      "Financials",
    "Financial Data Services": "Financials",
    "Banks":                   "Financials",
    "Insurance":               "Financials",
    "Healthcare":              "Healthcare",
    "Biotechnology":           "Healthcare",
    "Pharmaceuticals":         "Healthcare",
    "Medical Devices":         "Healthcare",
    "Energy":                  "Energy",
    "Basic Materials":         "Materials",
    "Utilities":               "Utilities",
    "Real Estate":             "Real Estate",
    "Consumer Defensive":      "Consumer Defensive",
    "Consumer Cyclical":       "Consumer Cyclical",
    "Industrials":             "Industrials",
}


def get_industry_group(sector):
    return SECTOR_MAP.get(sector, "Others")


def _fmt(val, suffix="", na="n/b"):
    return f"{val}{suffix}" if val is not None else na


def _num(v):
    """Maakt van numpy/pandas-types gewone floats, en van NaN None."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


# ── DCF & rendementsmaatstaven ────────────────────────────────────────────────
try:
    from dcf_engine import compute_dcf, compute_roic, compute_roce, check_dividend_sustainability
except ImportError:  # pragma: no cover - fallback als PYTHONPATH niet op src staat
    sys.path.insert(0, str(BASE_DIR / "src"))
    from dcf_engine import compute_dcf, compute_roic, compute_roce, check_dividend_sustainability


def compute_5yr_data(t):
    """5-jaars CAGR voor omzet en nettowinst uit de jaarcijfers."""
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
        result["ni_cagr_5yr"] = _cagr(["Net Income", "Net Income Common Stockholders"])
    except Exception:
        pass
    return result


def compute_ttm_fcf(t):
    """
    Vrije kasstroom over de laatste vier kwartalen, uit het kasstroomoverzicht.
    Yahoo's eigen `freeCashflow` slaat er bij grote bedrijven regelmatig fors
    naast (bij MSFT een factor 4), en die waarde bepaalt de hele waardering.
    Daarom rekenen we hem hier zelf: operationele kasstroom minus investeringen.
    """
    for attr in ("quarterly_cashflow", "quarterly_cash_flow"):
        try:
            cf = getattr(t, attr, None)
            if cf is None or cf.empty:
                continue

            def _row(names):
                for n in names:
                    hit = [k for k in cf.index if str(k) == n]
                    if hit:
                        return cf.loc[hit[0]]
                for n in names:
                    hit = [k for k in cf.index if n in str(k)]
                    if hit:
                        return cf.loc[hit[0]]
                return None

            direct = _row(["Free Cash Flow"])
            if direct is not None:
                vals = [_num(v) for v in direct.tolist()[:4]]
                vals = [v for v in vals if v is not None]
                if len(vals) == 4:
                    return sum(vals)

            ocf = _row(["Operating Cash Flow", "Total Cash From Operating Activities"])
            capex = _row(["Capital Expenditure", "Capital Expenditures"])
            if ocf is not None and capex is not None:
                o = [_num(v) for v in ocf.tolist()[:4]]
                c = [_num(v) for v in capex.tolist()[:4]]
                if all(v is not None for v in o + c) and len(o) == 4:
                    # capex staat negatief in het overzicht
                    return sum(o) + sum(c) if sum(c) < 0 else sum(o) - sum(c)
        except Exception:
            continue
    return None


def fetch_quarters(t):
    """
    Laatste kwartalen omzet + winst, met groei t.o.v. hetzelfde kwartaal een jaar
    eerder én t.o.v. het vorige kwartaal. Dit maakt zichtbaar of het bedrijf
    versnelt of juist afkoelt.
    """
    out = {"quarters": [], "rev_accel": None, "eps_accel": None}
    try:
        qf = t.quarterly_financials
        if qf is None or qf.empty:
            return out

        def _row(keys):
            for key in keys:
                matches = [k for k in qf.index if key == str(k)]
                if matches:
                    return qf.loc[matches[0]]
            for key in keys:
                matches = [k for k in qf.index if key in str(k)]
                if matches:
                    return qf.loc[matches[0]]
            return None

        rev = _row(["Total Revenue", "Revenue"])
        ni = _row(["Net Income", "Net Income Common Stockholders"])
        if rev is None:
            return out

        cols = list(qf.columns)  # nieuw → oud
        quarters = []
        for i, col in enumerate(cols[:6]):
            r = _num(rev.get(col)) if rev is not None else None
            n = _num(ni.get(col)) if ni is not None else None

            def _growth(series, idx):
                if series is None or idx >= len(cols):
                    return None
                cur = _num(series.get(cols[i]))
                prv = _num(series.get(cols[idx]))
                if cur is not None and prv and prv > 0:
                    return round((cur / prv - 1) * 100, 1)
                return None

            quarters.append({
                "period": col.strftime("%Y-%m") if hasattr(col, "strftime") else str(col)[:7],
                "revenue_m": round(r / 1e6, 1) if r is not None else None,
                "net_income_m": round(n / 1e6, 1) if n is not None else None,
                "rev_yoy": _growth(rev, i + 4),
                "eps_yoy": _growth(ni, i + 4),
                "rev_qoq": _growth(rev, i + 1),
                "eps_qoq": _growth(ni, i + 1),
            })

        out["quarters"] = quarters

        def _accel(key_yoy, key_qoq):
            yoy = [q[key_yoy] for q in quarters if q[key_yoy] is not None]
            if len(yoy) >= 2:
                return yoy[0] > yoy[1]
            qoq = [q[key_qoq] for q in quarters if q[key_qoq] is not None]
            if len(qoq) >= 2:
                return qoq[0] > qoq[1]
            return None

        out["rev_accel"] = _accel("rev_yoy", "rev_qoq")
        out["eps_accel"] = _accel("eps_yoy", "eps_qoq")
    except Exception:
        pass
    return out


def fetch_technicals(t, price):
    """Koersmomentum: 52-weeks positie, afstand tot gemiddelden, jaarrendement."""
    out = {
        "return_52w": None, "from_52w_high": None, "range_pos_52w": None,
        "above_ma50": None, "above_ma200": None, "sma50": None, "sma200": None,
        "spark": [],
    }
    try:
        hist = t.history(period="1y", interval="1d")
        if hist is None or hist.empty:
            return out
        closes = [c for c in hist["Close"].tolist() if c == c]
        if len(closes) < 30:
            return out

        px = price or closes[-1]
        hi, lo = max(closes), min(closes)
        out["return_52w"] = round((px / closes[0] - 1) * 100, 1) if closes[0] else None
        out["from_52w_high"] = round((px / hi - 1) * 100, 1) if hi else None
        out["range_pos_52w"] = round((px - lo) / (hi - lo) * 100) if hi > lo else None

        if len(closes) >= 50:
            sma50 = sum(closes[-50:]) / 50
            out["sma50"] = round(sma50, 2)
            out["above_ma50"] = px > sma50
        if len(closes) >= 200:
            sma200 = sum(closes[-200:]) / 200
            out["sma200"] = round(sma200, 2)
            out["above_ma200"] = px > sma200

        # mini-chart: 60 punten over het hele jaar, uitgedund
        step = max(1, len(closes) // 60)
        out["spark"] = [round(c, 2) for c in closes[::step]][-60:]
    except Exception:
        pass
    return out


def fetch_sentiment(ticker, name):
    """Sentiment uit StockTwits / Bluesky / Reddit / nieuws. Faalt zacht."""
    try:
        from social_sentiment import build_sentiment_context
        ctx = build_sentiment_context(ticker, name)
        ratio = ctx.get("combined_bull_ratio")
        label = None
        if ratio is not None:
            label = "BULLISH" if ratio >= 0.62 else "BEARISH" if ratio <= 0.42 else "NEUTRAAL"
        return {
            "text_block": ctx.get("text_block", ""),
            "bull_ratio": ratio,
            "label": label,
            "news_count": ctx.get("news_count", 0),
            "reddit_mentions": ctx.get("reddit_mention_count", 0),
        }
    except Exception as e:
        print(f"  Sentiment overgeslagen: {e}")
        return {"text_block": "Geen sentimentdata beschikbaar.", "bull_ratio": None,
                "label": None, "news_count": 0, "reddit_mentions": 0}


def fetch_fundamentals(ticker_symbol):
    print(f"Fundamentals ophalen voor {ticker_symbol}...")
    t = yf.Ticker(ticker_symbol)
    info = t.info

    if not info or not (info.get("currentPrice") or info.get("regularMarketPrice")):
        raise ValueError(f"Geen koersdata gevonden voor {ticker_symbol}")

    name = info.get("shortName") or info.get("longName") or ticker_symbol
    sector = info.get("sector", "Unknown")
    industry = info.get("industry") or ""
    group = get_industry_group(sector)
    price = info.get("currentPrice") or info.get("regularMarketPrice") or 0

    roe = (info.get("returnOnEquity", 0) or 0) * 100
    pe = info.get("trailingPE", 0) or 0
    forward_pe = info.get("forwardPE", 0) or 0
    de_raw = info.get("debtToEquity", 0) or 0
    de_ratio = (de_raw / 100) if de_raw > 5 else de_raw
    rev_growth = (info.get("revenueGrowth", 0) or 0) * 100
    gross_margin = (info.get("grossMargins", 0) or 0) * 100
    profit_margin = (info.get("profitMargins", 0) or 0) * 100
    beta = info.get("beta", 1.0) or 1.0
    market_cap = info.get("marketCap", 0) or 0
    currency = info.get("currency", "USD")

    # Vrije kasstroom: eigen TTM-berekening gaat vóór Yahoo's onbetrouwbare veld
    fcf_yahoo = info.get("freeCashflow")
    fcf_ttm = compute_ttm_fcf(t)
    fcf = fcf_ttm if fcf_ttm else fcf_yahoo
    fcf_source = "kasstroomoverzicht (TTM)" if fcf_ttm else "Yahoo"
    if fcf_ttm:
        info = dict(info)
        info["freeCashflow"] = fcf_ttm

    roic = compute_roic(info)
    roce = compute_roce(info)
    pfcf = round(market_cap / fcf, 1) if fcf and fcf > 0 and market_cap > 0 else None
    ps_ratio = info.get("priceToSalesTrailing12Months")
    pb_ratio = info.get("priceToBook")
    peg = info.get("trailingPegRatio")

    analyst_target = info.get("targetMeanPrice")
    analyst_count = info.get("numberOfAnalystOpinions", 0) or 0
    analyst_upside = None
    if analyst_target and price and price > 0:
        analyst_upside = round(((analyst_target / price) - 1) * 100, 1)

    # Dividendrendement: Yahoo mengt fracties en procenten door elkaar, wat in de
    # oude versie tot rendementen van 78% en 540% leidde. De trailing-variant is
    # altijd een fractie en dus betrouwbaar; anders normaliseren we zelf.
    div_yield = info.get("trailingAnnualDividendYield")
    if div_yield:
        div_yield = div_yield * 100
    else:
        raw = info.get("dividendYield")
        div_yield = (raw * 100 if raw and raw < 0.25 else raw) if raw else None
    if div_yield and div_yield > 30:
        div_yield = None  # onmogelijk hoog → data is stuk

    five_yr = compute_5yr_data(t)
    dcf = compute_dcf(info)
    dividend = check_dividend_sustainability(info)
    if dividend:
        dividend["yield"] = div_yield
    quarters = fetch_quarters(t)
    tech = fetch_technicals(t, price)

    cap = market_cap or 0
    cap_cat = ("mega" if cap >= 200e9 else "large" if cap >= 10e9
               else "mid" if cap >= 2e9 else "small" if cap > 0 else "?")

    news_items = t.news or []
    headlines = []
    for item in news_items[:8]:
        title = item.get("title") or item.get("content", {}).get("title") or item.get("headline", "")
        if title:
            headlines.append(f"• {title}")
    news_text = "\n".join(headlines) if headlines else "Geen recent nieuws."

    description = (info.get("longBusinessSummary") or "")[:700]

    return {
        "ticker":         ticker_symbol.upper(),
        "name":           name,
        "sector":         sector,
        "industry":       industry,
        "industry_group": group,
        "currency":       currency,
        "price":          round(price, 2),
        "market_cap":     market_cap,
        "market_cap_cat": cap_cat,
        "roe":            round(roe, 2),
        "roic":           roic,
        "roce":           roce,
        "pe_ratio":       round(pe, 2) if pe else None,
        "forward_pe":     round(forward_pe, 2) if forward_pe else None,
        "ps_ratio":       round(ps_ratio, 2) if ps_ratio else None,
        "pb_ratio":       round(pb_ratio, 2) if pb_ratio else None,
        "peg_ratio":      round(peg, 2) if peg else None,
        "pfcf":           pfcf,
        "debt_to_equity": round(de_ratio, 2),
        "revenue_growth": round(rev_growth, 1),
        "gross_margin":   round(gross_margin, 1),
        "profit_margin":  round(profit_margin, 1),
        "fcf_positive":   fcf is None or fcf >= 0,
        "beta":           round(beta, 2),
        "div_yield":      round(div_yield, 2) if div_yield else None,
        "dividend":       dividend,
        "fcf_ttm":        round(fcf / 1e6, 1) if fcf else None,
        "fcf_source":     fcf_source,
        "rev_cagr_5yr":   five_yr["rev_cagr_5yr"],
        "ni_cagr_5yr":    five_yr["ni_cagr_5yr"],
        "analyst_target": round(analyst_target, 2) if analyst_target else None,
        "analyst_upside": analyst_upside,
        "analyst_count":  analyst_count,
        "dcf":            dcf,
        "quarters":       quarters["quarters"],
        "rev_accel":      quarters["rev_accel"],
        "eps_accel":      quarters["eps_accel"],
        "description":    description,
        "news":           news_text,
        **tech,
    }


# ── AI-oordeel ────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = (
    "Je bent een ervaren waardebelegger die denkt als Buffett, Munger en Graham, "
    "maar die ook begrijpt dat momentum en marktsentiment het instapmoment bepalen. "
    "Je schrijft in helder Nederlands voor iemand die geen financieel jargon spreekt: "
    "korte zinnen, concrete gevolgen, geen holle frasen. "
    "Je bent eerlijk: je waarschuwt hard bij dure waarderingen, zwakke balansen of "
    "verhalen die niet door de cijfers worden gedragen. Je verzint nooit cijfers — "
    "wat ontbreekt, noem je ontbrekend."
)

JSON_INSTRUCTIE = """
Sluit je antwoord af met exact één JSON-blok tussen ```json en ```, met deze velden:

{
  "verdict": "KOOP" | "HOUD" | "MIJDEN",
  "score": <getal 1-10, jouw conviction; 1 = blijf weg, 10 = uitzonderlijke kans>,
  "one_liner": "<max 15 woorden: waarom dit cijfer, in gewone taal>",
  "niche": "<max 6 woorden: wat dit bedrijf precies doet, specifiek>",
  "theme": "<max 4 woorden: de lange-termijn trend waar het op meelift>",
  "valuation": "GOEDKOOP" | "FAIR" | "DUUR",
  "valuation_note": "<max 12 woorden waarom>",
  "moat": "BREED" | "SMAL" | "GEEN",
  "quality": <getal 1-10, kwaliteit van het bedrijf los van de prijs>,
  "risk": "LAAG" | "GEMIDDELD" | "HOOG",
  "target_price": <getal, koersdoel 12-18 maanden in dezelfde valuta als de huidige koers>,
  "bull": ["<punt 1>", "<punt 2>", "<punt 3>"],
  "bear": ["<punt 1>", "<punt 2>", "<punt 3>"],
  "watch": "<max 12 woorden: het ene ding dat je in de gaten moet houden>"
}

Elk bull- en bear-punt is één zin van maximaal 15 woorden.
"""


def build_prompt(f, sent):
    q_lines = []
    for q in f["quarters"]:
        q_lines.append(
            f"  {q['period']}: omzet {_fmt(q['revenue_m'], 'M')} "
            f"({_fmt(q['rev_yoy'], '% j-o-j')}), winst {_fmt(q['net_income_m'], 'M')} "
            f"({_fmt(q['eps_yoy'], '% j-o-j')})"
        )
    q_block = "\n".join(q_lines) if q_lines else "  Geen kwartaaldata beschikbaar."

    dcf = f.get("dcf") or {}
    dcf_block = (
        f"- DCF intrinsieke waarde: {_fmt(dcf.get('dcf_per_share'))} "
        f"({_fmt(dcf.get('dcf_upside'), '% t.o.v. koers')}), "
        f"veiligheidsmarge-instap {_fmt(dcf.get('mos_price'))}, WACC {_fmt(dcf.get('wacc'), '%')}"
        if dcf else "- DCF: niet te berekenen (negatieve of ontbrekende vrije kasstroom)"
    )

    return f"""Beoordeel {f['ticker']} ({f['name']}) als belegging met een horizon van 3-7 jaar.

BEDRIJF
{f['description']}
Sector: {f['industry_group']} / {f['industry']} | Marktwaarde: {f['market_cap'] / 1e9:.1f} mrd {f['currency']}

WAARDERING (koers {f['price']} {f['currency']})
- K/W: {_fmt(f['pe_ratio'])} | Verwacht K/W: {_fmt(f['forward_pe'])} | PEG: {_fmt(f['peg_ratio'])}
- K/omzet: {_fmt(f['ps_ratio'])} | K/boekwaarde: {_fmt(f['pb_ratio'])} | K/vrije kasstroom: {_fmt(f['pfcf'])}
{dcf_block}
- Analistendoel: {_fmt(f['analyst_target'])} ({_fmt(f['analyst_upside'], '% upside')}, {f['analyst_count']} analisten)

RENDEMENT & BALANS
- ROE {_fmt(f['roe'], '%')} | ROIC {_fmt(f['roic'], '%')} | ROCE {_fmt(f['roce'], '%')}
- Brutomarge {_fmt(f['gross_margin'], '%')} | Winstmarge {_fmt(f['profit_margin'], '%')}
- Schuld/eigen vermogen {_fmt(f['debt_to_equity'])} | Vrije kasstroom {'positief' if f['fcf_positive'] else 'NEGATIEF'}
- Dividendrendement {_fmt(f['div_yield'], '%')} | Beta {_fmt(f['beta'])}

GROEI
- Omzetgroei laatste jaar {_fmt(f['revenue_growth'], '%')}
- 5-jaars omzet-CAGR {_fmt(f['rev_cagr_5yr'], '% p.j.')} | 5-jaars winst-CAGR {_fmt(f['ni_cagr_5yr'], '% p.j.')}

KWARTAALCIJFERS (nieuwste eerst)
{q_block}
- Omzetgroei versnelt: {f['rev_accel']} | Winstgroei versnelt: {f['eps_accel']}

KOERSMOMENTUM
- Rendement 12 maanden {_fmt(f['return_52w'], '%')} | Afstand tot 52-weeks top {_fmt(f['from_52w_high'], '%')}
- Positie in 52-weeks range {_fmt(f['range_pos_52w'], '%')} | Boven 50-daags gemiddelde: {f['above_ma50']} | Boven 200-daags: {f['above_ma200']}

SENTIMENT & NIEUWS
{sent['text_block'][:1800]}

RECENTE HEADLINES
{f['news']}

Schrijf een analyse van maximaal 500 woorden in zes korte kopjes:
1. WAT HET BEDRIJF DOET — in gewone taal, en waar het geld vandaan komt.
2. KWALITEIT & MOAT — hoe verdedigbaar is de winst?
3. DE CIJFERS NU — wat de kwartaalcijfers en marges echt laten zien.
4. WAARDERING — is de prijs redelijk voor wat je krijgt?
5. SENTIMENT & MOMENTUM — wat de markt er nu van vindt, en of dat klopt.
6. RISICO'S — de twee concrete dingen die dit kunnen breken.
{JSON_INSTRUCTIE}"""


def parse_verdict(text):
    """Haalt het JSON-blok uit het antwoord. Geeft None als het ontbreekt of stuk is."""
    if not text:
        return None
    blocks = re.findall(r"```json\s*(\{.*?\})\s*```", text, re.S)
    if not blocks:
        blocks = re.findall(r"(\{[^{}]*\"verdict\".*?\})", text, re.S)
    for block in reversed(blocks):
        try:
            v = json.loads(block)
        except Exception:
            continue
        if "verdict" in v or "score" in v:
            try:
                v["score"] = round(float(v.get("score", 0)), 1)
            except (TypeError, ValueError):
                v["score"] = None
            for key in ("quality", "target_price"):
                try:
                    v[key] = round(float(v[key]), 2) if v.get(key) is not None else None
                except (TypeError, ValueError):
                    v[key] = None
            for key in ("bull", "bear"):
                if not isinstance(v.get(key), list):
                    v[key] = []
                v[key] = [str(x) for x in v[key]][:4]
            return v
    return None


def strip_json_block(text):
    out = re.sub(r"```json\s*\{.*?\}\s*```", "", text or "", flags=re.S)
    # Ook een blok dat halverwege is afgekapt hoort niet in de leesbare analyse
    out = re.sub(r"```json\s*\{.*\Z", "", out, flags=re.S)
    return out.strip()


def _text_of(message):
    return "".join(b.text for b in message.content if getattr(b, "type", "") == "text")


def run_ai_analysis(client, fund, sent):
    print(f"AI-analyse starten voor {fund['ticker']} ({MODEL})...")
    message = client.messages.create(
        model=MODEL,
        max_tokens=4000,
        system=[{
            "type": "text",
            "text": SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{"role": "user", "content": build_prompt(fund, sent)}],
    )
    raw = _text_of(message)
    print(f"  antwoord: {len(raw)} tekens, stop_reason={message.stop_reason}")
    return raw


def rescue_verdict(client, fund, analysis_text):
    """
    Vangnet: als het gestructureerde oordeel ontbreekt of halverwege is
    afgekapt, vragen we het in een tweede, korte aanroep alsnog op. De analyse
    zelf gaat mee als context, zodat het oordeel er niet los van staat.
    """
    print("  Oordeel ontbreekt — tweede poging voor alleen het JSON-blok...")
    message = client.messages.create(
        model=MODEL,
        max_tokens=1200,
        system=[{"type": "text", "text": SYSTEM_PROMPT}],
        messages=[{"role": "user", "content":
            f"Dit is jouw analyse van {fund['ticker']} ({fund['name']}), "
            f"huidige koers {fund['price']} {fund['currency']}:\n\n"
            f"{analysis_text}\n\n"
            f"Geef nu uitsluitend het gestructureerde oordeel terug. Geen inleiding, "
            f"geen toelichting, alleen het JSON-blok.\n{JSON_INSTRUCTIE}"}],
    )
    return parse_verdict(_text_of(message))


# ── opslag ────────────────────────────────────────────────────────────────────
def load_json(path, default):
    if not path.exists():
        return default
    with open(path) as f:
        try:
            return json.load(f)
        except Exception:
            return default


def save_watchlist(watchlist):
    data = load_json(DATA_PATH, {})
    data["watchlist"] = watchlist[:MAX_WATCHLIST]
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=4, default=str)


def remove_ticker(ticker):
    data = load_json(DATA_PATH, {})
    before = data.get("watchlist", [])
    after = [w for w in before if w.get("ticker") != ticker]
    if len(after) == len(before):
        print(f"{ticker} stond niet in de watchlist.")
        return
    save_watchlist(after)
    print(f"{ticker} verwijderd uit de watchlist ({len(after)} over).")


def main():
    action = os.environ.get("LOOKUP_ACTION", "add").strip().lower()

    ticker_input = None
    if len(sys.argv) > 1:
        ticker_input = sys.argv[1].strip().upper()
    if not ticker_input:
        ticker_input = os.environ.get("LOOKUP_TICKER", "").strip().upper()
    if not ticker_input:
        print("Gebruik: python src/stock_lookup.py TICKER")
        sys.exit(1)

    if action == "remove":
        remove_ticker(ticker_input)
        return

    print(f"=== NEXUS WATCHLIST: {ticker_input} ===")

    try:
        fund = fetch_fundamentals(ticker_input)
    except Exception as e:
        print(f"Fout bij ophalen data voor {ticker_input}: {e}")
        sys.exit(1)

    print(f"  {fund['name']} | {fund['price']} {fund['currency']} | {fund['industry_group']}")
    print(f"  ROE={fund['roe']}% ROIC={fund['roic']} K/W={fund['pe_ratio']} K/FCF={fund['pfcf']}")
    print(f"  {len(fund['quarters'])} kwartalen geladen")

    sent = fetch_sentiment(ticker_input, fund["name"])
    print(f"  Sentiment: {sent['label'] or 'geen data'} ({sent['news_count']} nieuwsitems)")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    analysis_text, verdict = None, None
    if api_key:
        client = anthropic.Anthropic(api_key=api_key)
        try:
            raw = run_ai_analysis(client, fund, sent)
            verdict = parse_verdict(raw)
            analysis_text = strip_json_block(raw)
            if not verdict and analysis_text:
                try:
                    verdict = rescue_verdict(client, fund, analysis_text)
                except Exception as e:
                    print(f"  Tweede poging mislukt: {e}")
            print("\n--- OORDEEL ---")
            print(json.dumps(verdict, indent=2, ensure_ascii=False) if verdict
                  else "Geen gestructureerd oordeel teruggekregen.")
        except Exception as e:
            print(f"AI-analyse mislukt: {e}")
    else:
        print("Geen ANTHROPIC_API_KEY — alleen cijfers opgeslagen.")

    data = load_json(DATA_PATH, {})
    watchlist = [w for w in data.get("watchlist", []) if w.get("ticker") != ticker_input]

    # Bewaar de datum waarop dit aandeel voor het eerst op de watchlist kwam
    prev = next((w for w in data.get("watchlist", []) if w.get("ticker") == ticker_input), None)
    added_at = (prev or {}).get("added_at") or datetime.now(timezone.utc).isoformat()

    entry = {
        **fund,
        "added_at": added_at,
        "lookup_at": datetime.now(timezone.utc).isoformat(),
        "sentiment": {
            "label": sent["label"],
            "bull_ratio": sent["bull_ratio"],
            "news_count": sent["news_count"],
            "reddit_mentions": sent["reddit_mentions"],
        },
        "verdict": verdict,
        "tier2": {
            "analysis": analysis_text,
            "last_run": datetime.now(timezone.utc).isoformat(),
            "model": MODEL,
        } if analysis_text else None,
    }
    watchlist.insert(0, entry)
    save_watchlist(watchlist)

    print(f"\nOpgeslagen in data.json — watchlist telt nu {min(len(watchlist), MAX_WATCHLIST)} aandelen.")
    print("=== KLAAR ===")


if __name__ == "__main__":
    main()
