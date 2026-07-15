"""
NEXUS Signal Logger — schrijf live scan-signalen naar de database.

Draait na de scan (tier1) + AI-analyse (tier2), zodat elke kandidaat mét
zijn volledige featurevector — technisch én fundamenteel én, voor de top-N
die tier2 kreeg, sentiment — als 'live' signaal in nexus_signals.db belandt.

De uitkomst (forward return over elke horizon) is op dit moment nog onbekend
en blijft NULL; update_outcomes.py vult die later in zodra de horizon
verstreken is. Zo groeit vanzelf de RIJKE laag (fundamenteel + sentiment) die
de backfill-historie mist en waarop de echte multi-factor kalibratie gaat
steunen.

Geen nieuwe HTTP-calls: alle data komt uit data.json (door tier1/tier2 al
opgehaald).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import signal_store as ss

BASE_DIR = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"


def _candidate_features(c: dict, macro: dict, spy_above_ma200: int | None) -> dict:
    dcf = c.get("dcf") or {}
    tier2 = c.get("tier2") or {}
    # Gecombineerd sentiment (StockTwits + Bluesky) waar tier2 het ophaalde;
    # anders StockTwits-only ratio; anders NULL.
    sentiment_ratio = tier2.get("combined_bull_ratio")
    if sentiment_ratio is None:
        sentiment_ratio = tier2.get("stocktwits_bullish_ratio")

    return {
        # Technisch
        "s_momentum":    c.get("s_momentum"),
        "m_factor":      c.get("m_factor"),
        "c_factor":      c.get("c_factor"),
        "v_factor":      c.get("v_factor"),
        "o_factor":      c.get("o_factor"),
        "stage2":        c.get("stage2"),
        "vcp_active":    c.get("vcp_active"),
        "rsi14":         c.get("rsi14"),
        "macd_bullish":  c.get("macd_bullish"),
        "atr_ratio_pct": c.get("atr_ratio_pct"),
        "rvol":          c.get("rvol"),
        "vol_buzz_pct":  c.get("vol_buzz_pct"),
        # Fundamenteel
        "s_growth":          c.get("s_growth"),
        "convergence_score": c.get("convergence_score"),
        "roic":              c.get("roic"),
        "dcf_upside":        dcf.get("dcf_upside"),
        "pe_ratio":          c.get("pe_ratio"),
        # Sentiment
        "sentiment_bull_ratio": sentiment_ratio,
        "reddit_mentions":      tier2.get("reddit_mentions"),
        "news_count":           tier2.get("news_count"),
        # Regime
        "vix":             macro.get("vix"),
        "spy_above_ma200": spy_above_ma200,
    }


def log_live_signals() -> dict:
    if not DATA_PATH.exists():
        print("data.json niet gevonden — niets te loggen.")
        return {"logged": 0}

    with open(DATA_PATH) as f:
        data = json.load(f)

    candidates = data.get("top_candidates", [])
    macro = data.get("macro", {})
    mem = data.get("memory", {})
    spy_above = mem.get("sp500_above_ma200")
    spy_above = int(spy_above) if isinstance(spy_above, bool) else spy_above

    as_of = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    conn = ss.init_db()
    logged = 0
    for c in candidates:
        ticker = c.get("ticker")
        if not ticker:
            continue
        features = _candidate_features(c, macro, spy_above)
        for h in ss.HORIZONS:
            ss.record_signal(conn, ticker, as_of, "live", h, features)
            logged += 1
    conn.commit()

    s = ss.stats(conn)
    print(f"Live signalen gelogd: {len(candidates)} kandidaten × {len(ss.HORIZONS)} horizons = {logged} rijen")
    print(f"DB totaal: {s['total_signals']} ({s['realized_outcomes']} met uitkomst, "
          f"per bron: {s['by_source']})")
    return {"logged": logged, "stats": s}


if __name__ == "__main__":
    print("=== NEXUS SIGNAL LOGGER ===")
    log_live_signals()
