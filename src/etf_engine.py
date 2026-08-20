"""
ETF-ENGINE — bouwt etf_data.json voor de ETF-tab van het dashboard.

Methodiek (kwaliteit boven momentum, 60/40):

  KWALITEIT (60%) — doorkijk naar de bedrijven in de ETF, gescoord op de negen
  criteria van Marc Langeveld (zie etf_quality.py), gewogen naar hun gewicht in
  het fonds. Plus een kostencomponent: elke basispunt TER is zeker verlies.

  MOMENTUM (40%) — 12-1 momentum, trendkracht (koers vs 200-daags gemiddelde) en
  risico-gecorrigeerd rendement. Momentum is timing, geen reden om te kopen.

  VERWACHTING — het koersrendement afgezet tegen de verwachte marktgroei van het
  thema. Rendement ver bóven de themagroei = de markt loopt vooruit op de feiten
  (oververhitting); ver eronder bij goede kwaliteit = mogelijk instapmoment.

Uitvoer: etf_data.json in de projectroot.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import yfinance as yf

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from etf_quality import fetch_fundamentals, score_company  # noqa: E402
from etf_universe import THEMES, UNIVERSE  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("etf_engine")

OUT_PATH = "etf_data.json"
STATE_PATH = os.path.join("data", "etf_state.json")
QUALITY_WEIGHT = 0.60
MOMENTUM_WEIGHT = 0.40
TOP_HOLDINGS = 10


# ── Koersdata ─────────────────────────────────────────────────────────────────

def download_prices(tickers: list[str]) -> pd.DataFrame:
    log.info("Koershistorie downloaden voor %d ETF's...", len(tickers))
    raw = yf.download(tickers, period="3y", interval="1d",
                      auto_adjust=True, progress=False, threads=True)
    close = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
    return close.dropna(how="all")


def _ret(series: pd.Series, days: int) -> float | None:
    s = series.dropna()
    if len(s) < days + 5:
        return None
    return float(s.iloc[-1] / s.iloc[-days] - 1)


def price_metrics(series: pd.Series) -> dict:
    s = series.dropna()
    if len(s) < 60:
        return {}
    r1m, r3m, r6m, r12m = (_ret(s, d) for d in (21, 63, 126, 252))
    r3y = float(s.iloc[-1] / s.iloc[0] - 1) if len(s) > 600 else None

    daily = s.pct_change().dropna()
    vol = float(daily.std() * np.sqrt(252)) if len(daily) > 30 else None
    dd = float((s / s.cummax() - 1).min())
    ma200 = float(s.rolling(200).mean().iloc[-1]) if len(s) >= 200 else None
    trend = float(s.iloc[-1] / ma200 - 1) if ma200 else None

    # 12-1 momentum: rendement over 12 maanden minus de laatste maand
    mom = None
    if r12m is not None and r1m is not None:
        mom = (1 + r12m) / (1 + r1m) - 1

    return {
        "prijs": round(float(s.iloc[-1]), 2),
        "r1m": r1m, "r3m": r3m, "r6m": r6m, "r12m": r12m, "r3y": r3y,
        "cagr3j": ((1 + r3y) ** (1 / 3) - 1) if r3y is not None else None,
        "vol": vol, "max_drawdown": dd, "trend_vs_ma200": trend,
        "momentum_12_1": mom,
        "sharpe": (r12m / vol) if (r12m is not None and vol) else None,
        "spark": [round(float(x), 2) for x in s.iloc[-120::3]],
    }


def momentum_score(m: dict) -> float | None:
    """0–10 momentumscore uit 12-1 momentum, trend en risicocorrectie."""
    def sc(v, lo, hi):
        if v is None:
            return None
        return max(0.0, min(10.0, (v - lo) / (hi - lo) * 10.0))

    parts = [(sc(m.get("momentum_12_1"), -0.20, 0.45), 0.45),
             (sc(m.get("trend_vs_ma200"), -0.15, 0.20), 0.30),
             (sc(m.get("sharpe"), -0.5, 2.0), 0.25)]
    used = [(v, w) for v, w in parts if v is not None]
    if not used:
        return None
    return sum(v * w for v, w in used) / sum(w for _, w in used)


# ── Holdings ──────────────────────────────────────────────────────────────────

def get_holdings(ticker: str) -> tuple[list[dict], dict]:
    try:
        fd = yf.Ticker(ticker).funds_data
        top = fd.top_holdings
        holdings = []
        if top is not None and len(top):
            col = "Holding Percent"
            for sym, row in top.head(TOP_HOLDINGS).iterrows():
                holdings.append({
                    "ticker": str(sym),
                    "naam": str(row.get("Name", sym)),
                    "gewicht": float(row.get(col, 0) or 0),
                })
        sectors = dict(fd.sector_weightings or {})
        return holdings, sectors
    except Exception as exc:
        log.debug("holdings %s mislukt: %s", ticker, exc)
        return [], {}


# ── Oordeel ───────────────────────────────────────────────────────────────────

def verdict(pq: float, quality: float | None, mom: float | None,
            gap: float | None) -> tuple[str, str]:
    """Oordeel op basis van de RANG binnen het universum, niet op losse cijfers.

    Een absolute drempel ("kwaliteit boven 6.5") levert tientallen koopsignalen
    op zodra de hele markt goed scoort. Door te kijken waar een fonds staat
    t.o.v. de rest blijft "kopen" schaars: alleen de bovenste 30% op kwaliteit,
    en dan nog alleen als de trend meeloopt en de verwachting niet al betaald is.
    """
    if quality is None:
        return "ONBEKEND", "Te weinig doorkijkdata om over de kwaliteit te oordelen."
    m = mom or 0.0

    if quality < 4.5 or pq < 0.20:
        return "VERMIJDEN", ("De bedrijven in dit fonds horen bij de zwakste van "
                             "het universum. Rendement uit het verleden weegt "
                             "daar niet tegenop.")
    if pq < 0.35 and m >= 7.0:
        return "MOMENTUM-VAL", ("Hard gelopen, maar de onderliggende kwaliteit "
                                "blijft achter. Dit is speculatie, geen belegging.")
    if pq >= 0.70:
        if gap is not None and gap > 0.35:
            return "STERK, MAAR DUUR", ("Sterke bedrijven, maar de koers is veel "
                                        "harder gelopen dan de markt zelf groeit. "
                                        "Gespreid instappen.")
        if m < 4.5:
            return "KANS", ("Bij de beste bedrijven van het universum, maar de "
                            "koers ligt achter. Vaak het moment waarop je het "
                            "meest verdient \u2014 mits je geduld hebt.")
        if m >= 6.0:
            return "KOPEN", "Bij de beste bedrijven \u00e9n de trend staat mee."
        return "HOUDEN", "Sterke bedrijven, maar de trend is nog niet overtuigend."
    if pq >= 0.45:
        return "HOUDEN", "Middenmoot op kwaliteit. Geen reden tot haast."
    return "AFWACHTEN", "Niet sterk genoeg op kwaliteit \u00e9n niet in trend."


def assign_verdicts(etfs: list[dict]) -> None:
    """Kent oordelen toe zodra alle kwaliteitsscores bekend zijn."""
    aandelen = [e for e in etfs
                if e.get("kwaliteit") is not None
                and e.get("kwaliteit_bron") != "obligatie"]
    ranked = sorted(aandelen, key=lambda e: e["kwaliteit"])
    n = len(ranked)
    for i, e in enumerate(ranked):
        e["kwaliteit_percentiel"] = round((i + 1) / n, 3) if n else None

    for e in etfs:
        if e.get("kwaliteit_bron") == "obligatie":
            continue
        pq = e.get("kwaliteit_percentiel")
        if pq is None:
            e["oordeel"] = "ONBEKEND"
            e["uitleg"] = "Te weinig doorkijkdata om over de kwaliteit te oordelen."
            continue
        e["oordeel"], e["uitleg"] = verdict(
            pq, e["kwaliteit"], e["momentum"], e["verwachting_gap"])
        if e.get("kwaliteit_bron") == "thema-schatting":
            e["uitleg"] += (" Let op: het fonds geeft zijn posities niet door, "
                            "de kwaliteit is geschat op basis van vergelijkbare "
                            "fondsen in dit thema.")


def _fill_missing_quality(etfs: list[dict]) -> None:
    """Vult kwaliteit voor ETF's zonder doorkijkdata.

    Obligatiefondsen krijgen een eigen score (kosten + stabiliteit — doorkijk
    naar bedrijfscijfers is daar zinloos). De rest erft het thema-gemiddelde,
    duidelijk gemarkeerd als schatting zodat je weet dat het geleend oordeel is.
    """
    per_theme: dict[str, list[float]] = {}
    for e in etfs:
        if e["kwaliteit_holdings"] is not None:
            per_theme.setdefault(e["thema"], []).append(e["kwaliteit_holdings"])

    for e in etfs:
        if e["kwaliteit"] is not None:
            e["kwaliteit_bron"] = "doorkijk"
            continue

        if THEMES.get(e["thema"], {}).get("kind") == "bond":
            stabiel = 10.0 - min(10.0, (e.get("vol") or 0.10) * 60)
            e["kwaliteit"] = round(e["kosten_score"] * 0.5 + stabiel * 0.5, 2)
            e["kwaliteit_bron"] = "obligatie"
            e["uitleg"] = ("Obligatiefonds: beoordeeld op kosten en stabiliteit, "
                           "niet op bedrijfskwaliteit.")
            e["oordeel"] = "DEFENSIEF"
        else:
            peers = per_theme.get(e["thema"])
            if not peers:
                continue
            avg = sum(peers) / len(peers)
            e["kwaliteit"] = round(avg * 0.85 + e["kosten_score"] * 0.15, 2)
            e["kwaliteit_bron"] = "thema-schatting"

        if e["momentum"] is not None and e["kwaliteit"] is not None:
            e["totaal"] = round(
                e["kwaliteit"] * QUALITY_WEIGHT + e["momentum"] * MOMENTUM_WEIGHT, 2)



# ── Historie: wat is er veranderd sinds de vorige scan? ───────────────────────

def track_changes(etfs: list[dict]) -> list[dict]:
    """Vergelijkt met de vorige scan en geeft de wijzigingen terug.

    Zonder geheugen is elk oordeel een momentopname. Door het vorige oordeel en
    de vorige kwaliteitsscore te bewaren zie je bewéging: een fonds dat na
    kwartaalcijfers omhoog kruipt is interessanter dan een fonds dat al hoog
    stond. De wijzigingen voeden ook het Telegram-signaal (src/etf_watch.py).
    """
    try:
        with open(STATE_PATH, encoding="utf-8") as fh:
            prev = json.load(fh)
    except (OSError, ValueError):
        prev = {}

    wijzigingen = []
    state = {}
    for e in etfs:
        old = prev.get(e["ticker"], {})
        oq, oo = old.get("kwaliteit"), old.get("oordeel")

        e["kwaliteit_vorige"] = oq
        e["kwaliteit_delta"] = (round(e["kwaliteit"] - oq, 2)
                                if (oq is not None and e["kwaliteit"] is not None)
                                else None)
        e["oordeel_vorige"] = oo

        if oo and e["oordeel"] and oo != e["oordeel"]:
            wijzigingen.append({
                "ticker": e["ticker"], "naam": e["naam"], "thema": e["thema"],
                "van": oo, "naar": e["oordeel"],
                "kwaliteit": e["kwaliteit"], "delta": e["kwaliteit_delta"],
                "r12m": e.get("r12m"), "tr": e.get("tr"),
            })

        state[e["ticker"]] = {"kwaliteit": e["kwaliteit"], "oordeel": e["oordeel"],
                              "datum": datetime.now(timezone.utc).date().isoformat()}

    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=1)

    rang = {"KOPEN": 0, "KANS": 1, "STERK, MAAR DUUR": 2, "VERMIJDEN": 3}
    wijzigingen.sort(key=lambda w: rang.get(w["naar"], 9))
    return wijzigingen


# ── Hoofdroutine ──────────────────────────────────────────────────────────────

def build() -> dict:
    tickers = list(UNIVERSE)
    prices = download_prices(tickers)

    log.info("Holdings ophalen...")
    holdings_map, sectors_map = {}, {}
    all_symbols: set[str] = set()
    for t in tickers:
        h, sec = get_holdings(t)
        holdings_map[t], sectors_map[t] = h, sec
        all_symbols.update(x["ticker"] for x in h)

    log.info("%d unieke onderliggende bedrijven", len(all_symbols))
    fundamentals = fetch_fundamentals(sorted(all_symbols))

    etfs = []
    for t, meta in UNIVERSE.items():
        if t not in prices.columns:
            continue
        m = price_metrics(prices[t])
        if not m:
            continue

        theme = meta["theme"]
        cagr = THEMES.get(theme, {}).get("cagr")

        # Doorkijk-kwaliteit: gewogen gemiddelde over de top-holdings
        rows, wsum, qsum, cov = [], 0.0, 0.0, 0.0
        for h in holdings_map.get(t, []):
            f = fundamentals.get(h["ticker"])
            if not f:
                continue
            cs = score_company(f, cagr)
            if cs["score"] is None:
                continue
            w = h["gewicht"] or 0.01
            wsum += w
            qsum += cs["score"] * w
            cov += cs["dekking"] * w
            rows.append({**h, "score": cs["score"], "criteria": cs["criteria"],
                         "sector": cs["sector"]})

        holdings_quality = (qsum / wsum) if wsum else None
        dekking = (cov / wsum) if wsum else 0.0

        # Kostencomponent: 0.05% TER = 10, 0.75% = 0
        kosten = max(0.0, min(10.0, (0.75 - meta["ter"]) / 0.70 * 10.0))
        if holdings_quality is not None:
            quality = holdings_quality * 0.85 + kosten * 0.15
        else:
            quality = None

        mom = momentum_score(m)

        # Verwachtingskloof: 12m-rendement vs verwachte themagroei
        gap = (m["r12m"] - cagr) if (cagr and m.get("r12m") is not None) else None

        if quality is not None and mom is not None:
            totaal = quality * QUALITY_WEIGHT + mom * MOMENTUM_WEIGHT
        else:
            totaal = quality if quality is not None else mom

        etfs.append({
            "ticker": t, "naam": meta["name"], "thema": theme, "ter": meta["ter"],
            **m,
            "kwaliteit": round(quality, 2) if quality is not None else None,
            "kwaliteit_holdings": round(holdings_quality, 2) if holdings_quality else None,
            "kosten_score": round(kosten, 1),
            "momentum": round(mom, 2) if mom is not None else None,
            "totaal": round(totaal, 2) if totaal is not None else None,
            "dekking": round(dekking, 2),
            "verwachte_groei": cagr,
            "verwachting_gap": gap,
            "oordeel": None, "uitleg": None,
            "tr": meta.get("tr"), "groep": meta.get("groep"),
            "holdings": sorted(rows, key=lambda r: -r["gewicht"])[:TOP_HOLDINGS],
            "sectoren": sectors_map.get(t, {}),
        })

    _fill_missing_quality(etfs)
    assign_verdicts(etfs)
    wijzigingen = track_changes(etfs)
    etfs.sort(key=lambda e: -(e["totaal"] or 0))

    # Thema-aggregatie: gemiddeld rendement en kwaliteit per thema
    themes_out = []
    for th, cfg in THEMES.items():
        rows = [e for e in etfs if e["thema"] == th]
        if not rows:
            continue
        def avg(key):
            vals = [r[key] for r in rows if r.get(key) is not None]
            return sum(vals) / len(vals) if vals else None
        themes_out.append({
            "thema": th, "icon": cfg["icon"], "kind": cfg["kind"],
            "aantal": len(rows), "verwachte_groei": cfg["cagr"],
            "r12m": avg("r12m"), "r3m": avg("r3m"), "cagr3j": avg("cagr3j"),
            "kwaliteit": avg("kwaliteit"), "momentum": avg("momentum"),
            "totaal": avg("totaal"),
            "beste": rows[0]["ticker"] if rows else None,
        })
    themes_out.sort(key=lambda t: -(t["r12m"] or -9))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "methodiek": {
            "kwaliteit_gewicht": QUALITY_WEIGHT,
            "momentum_gewicht": MOMENTUM_WEIGHT,
            "bron": "Langeveld-criteria doorkijk op holdings + thema-CAGR",
        },
        "wijzigingen": wijzigingen,
        "etfs": etfs,
        "themas": themes_out,
    }


def main() -> None:
    data = build()
    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, separators=(",", ":"))
    log.info("Klaar: %d ETF's, %d thema's → %s",
             len(data["etfs"]), len(data["themas"]), OUT_PATH)


if __name__ == "__main__":
    main()
