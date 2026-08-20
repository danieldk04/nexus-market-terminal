"""
DOORKIJK-KWALITEIT — Langeveld-criteria toegepast op de bedrijven ÍN een ETF.

Een ETF heeft zelf geen balans. De kwaliteit van een ETF is de gewogen kwaliteit
van de bedrijven die erin zitten. Deze module scoort elk onderliggend bedrijf op
de negen criteria van Marc Langeveld (probeleggen / De Aandeelhouder):

  1. Moat        — verdedigbare marktpositie      (brutomarge, ROIC)
  2. Groeimarkt  — markt groeit sneller dan BBP   (omzetgroei vs 2x BBP)
  3. Marktaandeel— groeit harder dan de markt     (omzetgroei vs thema-CAGR)
  4. Op. hefboom — 5% omzet → >10% EBIT           (winstgroei / omzetgroei)
  5. Capex-fase  — grote investeringen achter rug (capex/omzet, FCF-marge)
  6. Balans      — solide                          (netto schuld / EBITDA)
  7. Aandeelhouder-vriendelijk                     (dividend + inkoop)
  8. Management  — stabiel, geen verwatering       (insider-belang, ROE)
  9. Waardering  — FCF-rendement / PEG

Elk criterium levert 0–10. De bedrijfsscore is het gewogen gemiddelde.
Onbekende data telt niet mee (geen straf, maar wel lagere dekking — die
dekking wordt apart gerapporteerd zodat je weet hoe hard het oordeel is).
"""
from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import yfinance as yf

log = logging.getLogger("etf_quality")

CACHE_PATH = os.path.join("data", "etf_fundamentals.json")
CACHE_TTL_DAYS = 7   # kwartaalcijfers werken zo binnen een week door
GDP_GROWTH = 0.03  # nominale wereldgroei; "2x BBP" = 6% omzetgroei

# Gewichten per criterium (som = 100). Management en balans wegen zwaar:
# "management voorop, cijfers daarachter".
WEIGHTS = {
    "moat": 16, "groeimarkt": 8, "marktaandeel": 10, "hefboom": 14,
    "capex": 12, "balans": 14, "aandeelhouder": 10, "management": 10,
    "waardering": 6,
}


def _clip(x: float) -> float:
    return max(0.0, min(10.0, x))


def _scale(value, lo, hi):
    """Lineair van 0 (bij lo) naar 10 (bij hi). None → None."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if hi == lo:
        return None
    return _clip((v - lo) / (hi - lo) * 10.0)


# ── Fundamentals ophalen (met schijf-cache) ───────────────────────────────────

def _load_cache() -> dict:
    try:
        with open(CACHE_PATH, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def _save_cache(cache: dict) -> None:
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    tmp = CACHE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cache, fh)
    os.replace(tmp, CACHE_PATH)


FIELDS = (
    "shortName", "sector", "grossMargins", "operatingMargins", "profitMargins",
    "returnOnEquity", "returnOnAssets", "revenueGrowth", "earningsGrowth",
    "earningsQuarterlyGrowth", "freeCashflow", "operatingCashflow",
    "totalRevenue", "totalDebt", "totalCash", "ebitda", "marketCap",
    "dividendYield", "payoutRatio", "heldPercentInsiders", "trailingPegRatio",
    "forwardPE", "debtToEquity", "capitalExpenditures",
)


def _fetch_one(ticker: str) -> dict:
    try:
        info = yf.Ticker(ticker).info or {}
        return {k: info.get(k) for k in FIELDS}
    except Exception as exc:  # netwerkfout / onbekend fonds
        log.debug("fundamentals %s mislukt: %s", ticker, exc)
        return {}


def fetch_fundamentals(tickers: list[str], workers: int = 8) -> dict[str, dict]:
    """Haalt fundamentals voor alle tickers op, met 30-daagse cache."""
    cache = _load_cache()
    now = time.time()
    ttl = CACHE_TTL_DAYS * 86400
    todo = [t for t in tickers
            if now - cache.get(t, {}).get("_ts", 0) > ttl]

    if todo:
        log.info("Fundamentals ophalen voor %d bedrijven (%d uit cache)",
                 len(todo), len(tickers) - len(todo))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for tkr, data in zip(todo, pool.map(_fetch_one, todo)):
                if data:
                    data["_ts"] = now
                    cache[tkr] = data
        _save_cache(cache)

    return {t: cache[t] for t in tickers if t in cache}


# ── Criteria ──────────────────────────────────────────────────────────────────

def score_company(f: dict, theme_cagr: float | None = None) -> dict:
    """Scoort één bedrijf op de negen criteria. Retourneert score + dekking."""
    s: dict[str, float | None] = {}

    # 1. Moat: brutomarge 20%→60% en ROE 5%→30%, gemiddeld
    gm = _scale(f.get("grossMargins"), 0.20, 0.60)
    roe = _scale(f.get("returnOnEquity"), 0.05, 0.30)
    parts = [x for x in (gm, roe) if x is not None]
    s["moat"] = sum(parts) / len(parts) if parts else None

    # 2. Groeimarkt: omzetgroei t.o.v. 2x BBP (6%)
    s["groeimarkt"] = _scale(f.get("revenueGrowth"), 0.0, 2 * GDP_GROWTH * 2)

    # 3. Marktaandeel: omzetgroei boven de themagroei
    rev = f.get("revenueGrowth")
    if rev is None:
        s["marktaandeel"] = None
    else:
        ref = theme_cagr if theme_cagr else 2 * GDP_GROWTH
        s["marktaandeel"] = _scale(float(rev) - ref, -0.10, 0.15)

    # 4. Operationele hefboom: winstgroei / omzetgroei ≥ 2 is de norm
    earn = f.get("earningsGrowth") or f.get("earningsQuarterlyGrowth")
    if earn is None or not rev or float(rev) <= 0.01:
        s["hefboom"] = None
    else:
        s["hefboom"] = _scale(float(earn) / float(rev), 0.5, 2.5)

    # 5. Capex-fase: FCF-marge (grote investeringen achter de rug = hoge FCF)
    fcf, sales = f.get("freeCashflow"), f.get("totalRevenue")
    if fcf and sales and float(sales) > 0:
        s["capex"] = _scale(float(fcf) / float(sales), 0.0, 0.25)
    else:
        s["capex"] = None

    # 6. Balans: netto schuld / EBITDA — 3x is zwak, 0x of netto cash is top
    debt, cash, ebitda = f.get("totalDebt"), f.get("totalCash"), f.get("ebitda")
    if ebitda and float(ebitda) > 0:
        net = (float(debt or 0) - float(cash or 0)) / float(ebitda)
        s["balans"] = _clip((3.0 - net) / 3.5 * 10.0)
    else:
        s["balans"] = _scale(f.get("debtToEquity"), 200.0, 0.0)

    # 7. Aandeelhoudersvriendelijk: dividend + ruimte om in te kopen
    dy = f.get("dividendYield")
    payout = f.get("payoutRatio")
    base = _scale(dy if dy is None or dy < 1 else float(dy) / 100.0, 0.0, 0.04)
    if base is not None and payout is not None and 0 < float(payout) < 0.6:
        base = _clip(base + 2.0)          # bonus: uitkeert én houdt over
    if base is None and fcf and float(fcf) > 0:
        base = 5.0                        # genereert cash, keert (nog) niet uit
    s["aandeelhouder"] = base

    # 8. Management: insider-belang + rendement op eigen vermogen
    ins = _scale(f.get("heldPercentInsiders"), 0.0, 0.10)
    roa = _scale(f.get("returnOnAssets"), 0.0, 0.15)
    parts = [x for x in (ins, roa) if x is not None]
    s["management"] = sum(parts) / len(parts) if parts else None

    # 9. Waardering: PEG onder 1.5 is goed; anders FCF-rendement
    peg = f.get("trailingPegRatio")
    if peg and float(peg) > 0:
        s["waardering"] = _scale(float(peg), 3.0, 0.5)
    elif fcf and f.get("marketCap"):
        s["waardering"] = _scale(float(fcf) / float(f["marketCap"]), 0.0, 0.06)
    else:
        s["waardering"] = None

    used = {k: v for k, v in s.items() if v is not None}
    wsum = sum(WEIGHTS[k] for k in used)
    total = (sum(v * WEIGHTS[k] for k, v in used.items()) / wsum) if wsum else None

    return {
        "score": round(total, 2) if total is not None else None,
        "dekking": round(wsum / sum(WEIGHTS.values()), 2),
        "criteria": {k: (round(v, 1) if v is not None else None) for k, v in s.items()},
        "naam": f.get("shortName"),
        "sector": f.get("sector"),
    }
