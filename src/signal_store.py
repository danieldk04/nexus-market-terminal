"""
NEXUS Signal Store — persistente signaal→uitkomst database + kalibratie.

Dit is het geheugen waar de bot naartoe leert. Elke keer dat een ticker
gescoord wordt (live scan óf historische backtest) leggen we de volledige
feature-vector vast, samen met — zodra bekend — de gerealiseerde forward
return over een vaste horizon en of die de benchmark (SPY) versloeg.

Die groeiende, gelabelde dataset maakt één eerlijke vraag beantwoordbaar:

    "Toen een signaal dat lijkt op dít signaal in het verleden voorkwam,
     hoe vaak versloeg het daarna de index — en hoe zeker zijn we daarvan?"

Dat is GEKALIBREERDE confidence, geen belofte. We rapporteren altijd:
  - de empirische hit-rate van het cohort,
  - de steekproefgrootte n,
  - de Wilson-ondergrens (95%): de conservatieve confidence.

Een "90%-signaal" = een waarvan de Wilson-ondergrens ≥ 0.90 ligt. Dat
vereist een groot cohort én een echt sterke edge; precies zoals het hoort.

Opslag: één SQLite-bestand (data/nexus_signals.db). Geen server, geen
dependencies buiten de stdlib. Persistent tussen runs — de bot onthoudt.
"""
from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "nexus_signals.db"

# Vaste horizons (handelsdagen) waarover we uitkomsten meten. Meerdere
# horizons zodat we per horizon apart kunnen kalibreren.
HORIZONS = (21, 63)  # ~1 maand, ~1 kwartaal

# Feature-kolommen die we vastleggen. Technische features zijn point-in-time
# reproduceerbaar (dus backfillbaar uit historie); fundamentele + sentiment
# features bestaan alleen voor live signalen en zijn NULL bij backfill.
FEATURE_COLUMNS = [
    # Technisch / momentum (backfillbaar)
    "s_momentum", "m_factor", "c_factor", "v_factor", "o_factor",
    "stage2", "vcp_active", "rsi14", "macd_bullish", "atr_ratio_pct",
    "rvol", "vol_buzz_pct",
    # Fundamenteel (alleen live)
    "s_growth", "convergence_score", "roic", "dcf_upside", "pe_ratio",
    # Sentiment (alleen live)
    "sentiment_bull_ratio", "reddit_mentions", "news_count",
    # Regime
    "vix", "spy_above_ma200",
]


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection | None = None) -> sqlite3.Connection:
    """Maak het schema aan (idempotent)."""
    own = conn is None
    conn = conn or _connect()
    feature_defs = ",\n        ".join(f"{c} REAL" for c in FEATURE_COLUMNS)
    conn.executescript(f"""
    CREATE TABLE IF NOT EXISTS signals (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker          TEXT NOT NULL,
        as_of_date      TEXT NOT NULL,       -- ISO datum waarop gescoord (point-in-time)
        source          TEXT NOT NULL,       -- 'live' | 'backtest'
        horizon_days    INTEGER NOT NULL,
        {feature_defs},
        -- Uitkomst (NULL tot de horizon verstreken en bekend is)
        forward_return  REAL,               -- fractie, bv. 0.083 = +8.3%
        benchmark_return REAL,
        beat_benchmark  INTEGER,            -- 1/0
        outcome_positive INTEGER,           -- 1/0 (forward_return > 0)
        outcome_date    TEXT,
        created_at      TEXT NOT NULL,
        UNIQUE(ticker, as_of_date, source, horizon_days)
    );
    CREATE INDEX IF NOT EXISTS idx_signals_realized
        ON signals(horizon_days, beat_benchmark);
    CREATE INDEX IF NOT EXISTS idx_signals_conv
        ON signals(horizon_days, convergence_score);
    """)
    conn.commit()
    if own:
        return conn
    return conn


def record_signal(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_date: str,
    source: str,
    horizon_days: int,
    features: dict,
    forward_return: float | None = None,
    benchmark_return: float | None = None,
) -> None:
    """
    Leg een signaal vast (UPSERT). Als forward_return al bekend is (backfill
    uit historie) wordt de uitkomst meteen ingevuld; anders blijft die NULL
    tot een latere update_pending_outcomes()-run.
    """
    cols = ["ticker", "as_of_date", "source", "horizon_days", "created_at"]
    vals: list = [ticker, as_of_date, source, horizon_days,
                  datetime.now(timezone.utc).isoformat()]

    for c in FEATURE_COLUMNS:
        if c in features and features[c] is not None:
            cols.append(c)
            v = features[c]
            vals.append(int(v) if isinstance(v, bool) else v)

    if forward_return is not None:
        beat = None
        if benchmark_return is not None:
            beat = 1 if forward_return > benchmark_return else 0
        cols += ["forward_return", "benchmark_return", "beat_benchmark",
                 "outcome_positive", "outcome_date"]
        vals += [forward_return, benchmark_return, beat,
                 1 if forward_return > 0 else 0, as_of_date]

    placeholders = ",".join("?" for _ in cols)
    col_sql = ",".join(cols)
    update_sql = ",".join(f"{c}=excluded.{c}" for c in cols if c not in
                          ("ticker", "as_of_date", "source", "horizon_days"))
    conn.execute(
        f"INSERT INTO signals ({col_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT(ticker, as_of_date, source, horizon_days) "
        f"DO UPDATE SET {update_sql}",
        vals,
    )


def _wilson_lower_bound(successes: int, n: int, z: float = 1.96) -> float:
    """
    Wilson score-ondergrens (95%) voor een binomiale proportie. Dit is de
    conservatieve confidence: bij kleine n trekt hij hard omlaag, precies
    zoals je wil — 3/3 winst is GEEN 100% confidence.
    """
    if n == 0:
        return 0.0
    p = successes / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return max(0.0, (centre - margin) / denom)


def calibrated_confidence(
    conn: sqlite3.Connection,
    features: dict,
    horizon_days: int = 21,
    band: float = 0.75,
    min_sample: int = 30,
) -> dict:
    """
    Gekalibreerde confidence voor een nieuw signaal op basis van historische
    cohort-uitkomsten.

    Cohort = alle gerealiseerde signalen met dezelfde horizon waarvan de
    convergence_score (of s_momentum als convergence ontbreekt) binnen ±band
    van dit signaal ligt. We verbreden de band automatisch tot er minstens
    min_sample waarnemingen zijn.

    Retourneert de empirische beat-rate, n, en de Wilson-ondergrens. Die
    laatste is het eerlijke "hoe zeker"-getal.
    """
    key = "convergence_score" if features.get("convergence_score") is not None else "s_momentum"
    q = features.get(key)
    if q is None:
        return {"available": False, "reason": f"geen {key} in signaal"}

    for widen in (0, 1, 2, 3):
        b = band * (1.5 ** widen)
        rows = conn.execute(
            f"SELECT beat_benchmark, outcome_positive FROM signals "
            f"WHERE horizon_days = ? AND beat_benchmark IS NOT NULL "
            f"AND {key} IS NOT NULL AND {key} BETWEEN ? AND ?",
            (horizon_days, q - b, q + b),
        ).fetchall()
        if len(rows) >= min_sample:
            break

    n = len(rows)
    if n == 0:
        return {"available": False, "reason": "geen vergelijkbare historie"}

    beats = sum(r["beat_benchmark"] for r in rows)
    wins = sum(r["outcome_positive"] for r in rows)
    beat_rate = beats / n
    win_rate = wins / n
    conf = _wilson_lower_bound(beats, n)

    return {
        "available": True,
        "cohort_key": key,
        "cohort_value": round(q, 2),
        "band": round(b, 2),
        "n": n,
        "beat_benchmark_rate": round(beat_rate, 3),
        "positive_return_rate": round(win_rate, 3),
        "confidence": round(conf, 3),          # Wilson-ondergrens = eerlijke confidence
        "high_conviction": conf >= 0.90 and n >= 100,
    }


def update_pending_outcomes(conn: sqlite3.Connection, price_lookup) -> int:
    """
    Vul uitkomsten in voor live-signalen waarvan de horizon inmiddels verstreken
    is. `price_lookup(ticker, from_date, horizon_days)` moet
    (forward_return, benchmark_return) of None teruggeven. Retourneert het
    aantal bijgewerkte rijen. (De aanroeper levert de prijs-functie zodat dit
    module geen yfinance-dependency heeft.)
    """
    pending = conn.execute(
        "SELECT id, ticker, as_of_date, horizon_days FROM signals "
        "WHERE source = 'live' AND beat_benchmark IS NULL"
    ).fetchall()
    updated = 0
    for row in pending:
        res = price_lookup(row["ticker"], row["as_of_date"], row["horizon_days"])
        if res is None:
            continue
        fwd, bench = res
        beat = 1 if fwd > bench else 0
        conn.execute(
            "UPDATE signals SET forward_return=?, benchmark_return=?, "
            "beat_benchmark=?, outcome_positive=?, outcome_date=? WHERE id=?",
            (fwd, bench, beat, 1 if fwd > 0 else 0,
             datetime.now(timezone.utc).strftime("%Y-%m-%d"), row["id"]),
        )
        updated += 1
    conn.commit()
    return updated


def stats(conn: sqlite3.Connection) -> dict:
    total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    realized = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE beat_benchmark IS NOT NULL"
    ).fetchone()[0]
    by_source = {
        r["source"]: r["n"]
        for r in conn.execute(
            "SELECT source, COUNT(*) n FROM signals GROUP BY source"
        ).fetchall()
    }
    overall_beat = conn.execute(
        "SELECT AVG(beat_benchmark) FROM signals WHERE beat_benchmark IS NOT NULL"
    ).fetchone()[0]
    return {
        "total_signals": total,
        "realized_outcomes": realized,
        "by_source": by_source,
        "overall_beat_rate": round(overall_beat, 3) if overall_beat is not None else None,
    }


if __name__ == "__main__":
    conn = init_db()
    s = stats(conn)
    print("=" * 60)
    print("NEXUS SIGNAL STORE")
    print("=" * 60)
    print(f"DB: {DB_PATH}")
    print(f"Totaal signalen:      {s['total_signals']}")
    print(f"Met bekende uitkomst: {s['realized_outcomes']}")
    print(f"Per bron:             {s['by_source']}")
    print(f"Beat-rate (alles):    {s['overall_beat_rate']}")
    print("=" * 60)
    if s["realized_outcomes"] == 0:
        print("Nog leeg — draai: python src/backfill_signals.py")
