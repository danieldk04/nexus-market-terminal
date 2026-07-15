"""
NEXUS Multi-Factor Calibration — waar de echte edge (als die er is) zichtbaar wordt.

Enkelvoudige kalibratie (signal_store.calibrated_confidence) liet zien dat de
momentum-score ALLEEN een muntworp is tegen de index. Dat wast interactie-
effecten weg: misschien werkt momentum wél, maar alléén in een bull-regime,
of alléén met een actieve volatiliteits-squeeze. Deze module conditioneert
daarom op MEERDERE factoren tegelijk.

Twee functies:

  confidence_for_signal() — voor een concreet (live) signaal: bouwt een cohort
    van historische signalen die op meerdere assen op dit signaal lijken, en
    geeft de eerlijke Wilson-confidence. Verruimt automatisch (laat de minst
    belangrijke conditie vallen) tot er genoeg waarnemingen zijn.

  discover_edges() — verkenning: zoekt combinaties van condities af en
    rangschikt op Wilson-ondergrens. Past een multiple-testing-drempel toe
    (zoals backtest_sweep) zodat we data-mining niet als edge aanzien.

Alles blijft interpreteerbaar: geen black box, maar "wanneer A én B én C gold,
versloeg het historisch X% (n=..., Wilson-ondergrens L%)".
"""
from __future__ import annotations

import math
import sqlite3
from itertools import combinations

import signal_store as ss

# ── Condities: hoe we op een feature 'matchen' ────────────────────────────────
# Elke conditie levert een SQL-fragment + params op. Geordend van belangrijkst
# (regime) naar minst belangrijk — auto-verruiming laat van achteren vallen.

def _momentum_bucket(v: float) -> tuple[float, float]:
    if v < 3:   return (0.0, 3.0)
    if v < 5:   return (3.0, 5.0)
    if v < 7:   return (5.0, 7.0)
    return (7.0, 10.001)


def _build_conditions(features: dict) -> list[tuple[str, str, list]]:
    """
    Bouw een geordende lijst (label, sql_fragment, params) uit een signaal.
    Booleans → exacte match; momentum → bucket; continue features → band.
    Alleen condities waarvoor het signaal een waarde heeft worden opgenomen.
    """
    conds: list[tuple[str, str, list]] = []

    if features.get("spy_above_ma200") is not None:
        conds.append(("regime", "spy_above_ma200 = ?", [int(features["spy_above_ma200"])]))

    mom = features.get("convergence_score")
    if mom is None:
        mom = features.get("s_momentum")
    if mom is not None:
        lo, hi = _momentum_bucket(mom)
        conds.append((f"momentum∈[{lo:.0f},{hi:.0f})", "convergence_score >= ? AND convergence_score < ?", [lo, hi]))

    for col in ("stage2", "vcp_active", "macd_bullish"):
        if features.get(col) is not None:
            conds.append((f"{col}={int(features[col])}", f"{col} = ?", [int(features[col])]))

    rsi = features.get("rsi14")
    if rsi is not None:
        in_zone = 1 if 50.0 <= rsi <= 70.0 else 0
        # match op 'in optimale RSI-zone ja/nee' i.p.v. exacte waarde
        op = "BETWEEN 50 AND 70" if in_zone else "NOT BETWEEN 50 AND 70"
        conds.append((f"rsi_zone={in_zone}", f"rsi14 {op}", []))

    return conds


def _query_cohort(conn: sqlite3.Connection, horizon: int,
                  conds: list[tuple[str, str, list]]) -> list[sqlite3.Row]:
    where = ["horizon_days = ?", "beat_benchmark IS NOT NULL"]
    params: list = [horizon]
    for _, frag, p in conds:
        where.append(frag)
        params += p
    sql = f"SELECT beat_benchmark, outcome_positive, forward_return FROM signals WHERE {' AND '.join(where)}"
    return conn.execute(sql, params).fetchall()


def confidence_for_signal(conn: sqlite3.Connection, features: dict,
                          horizon: int = 21, min_sample: int = 40) -> dict:
    """
    Multi-factor confidence voor één signaal, met automatische verruiming.
    Begint met alle condities; laat telkens de laatste (minst belangrijke)
    vallen tot er ≥ min_sample waarnemingen zijn. Retourneert welke condities
    overbleven, plus beat-rate, n en Wilson-ondergrens (de eerlijke confidence).
    """
    conds = _build_conditions(features)
    dropped: list[str] = []

    while True:
        rows = _query_cohort(conn, horizon, conds)
        if len(rows) >= min_sample or not conds:
            break
        dropped.append(conds.pop()[0])  # laat minst belangrijke conditie vallen

    n = len(rows)
    if n == 0:
        return {"available": False, "reason": "geen vergelijkbare historie"}

    beats = sum(r["beat_benchmark"] for r in rows)
    wins = sum(r["outcome_positive"] for r in rows)
    avg_ret = sum(r["forward_return"] for r in rows) / n
    conf = ss._wilson_lower_bound(beats, n)

    return {
        "available": True,
        "horizon_days": horizon,
        "n": n,
        "conditions_used": [c[0] for c in conds],
        "conditions_dropped": dropped,
        "beat_benchmark_rate": round(beats / n, 3),
        "positive_return_rate": round(wins / n, 3),
        "avg_forward_return": round(avg_ret, 4),
        "confidence": round(conf, 3),
        "high_conviction": conf >= 0.60 and n >= 100,
    }


# ── Edge-discovery ────────────────────────────────────────────────────────────

# Bouwstenen: (label, sql_fragment). Discovery test enkele condities en paren.
_ATOMS = [
    ("bull_regime",   "spy_above_ma200 = 1"),
    ("bear_regime",   "spy_above_ma200 = 0"),
    ("mom_high",      "convergence_score >= 7"),
    ("mom_mid",       "convergence_score >= 5 AND convergence_score < 7"),
    ("mom_low",       "convergence_score < 5"),
    ("stage2",        "stage2 = 1"),
    ("vcp",           "vcp_active = 1"),
    ("macd_bull",     "macd_bullish = 1"),
    ("rsi_zone",      "rsi14 BETWEEN 50 AND 70"),
    ("high_rvol",     "rvol >= 1.5"),
]


def _cohort_stats(conn, horizon: int, frags: list[str]) -> tuple[int, int, float]:
    where = ["horizon_days = ?", "beat_benchmark IS NOT NULL"] + frags
    rows = conn.execute(
        f"SELECT beat_benchmark FROM signals WHERE {' AND '.join(where)}", (horizon,)
    ).fetchall()
    n = len(rows)
    beats = sum(r["beat_benchmark"] for r in rows)
    return n, beats, (beats / n if n else 0.0)


def discover_edges(conn: sqlite3.Connection, horizon: int = 21,
                   min_sample: int = 100, max_combo: int = 2) -> dict:
    """
    Doorzoek combinaties van condities (tot max_combo tegelijk) en rangschik op
    Wilson-ondergrens. Past een multiple-testing-drempel toe: hoe meer combo's
    getest, hoe hoger de lat om iets 'echt' te noemen (Bonferroni-benadering,
    net als backtest_sweep). Combo's die die lat halen worden gemarkeerd als
    kandidaat-edge — nog steeds te bevestigen out-of-sample.
    """
    baseline_n, baseline_beats, baseline_rate = _cohort_stats(conn, horizon, [])

    combos = []
    for k in range(1, max_combo + 1):
        combos.extend(combinations(_ATOMS, k))

    n_tests = len(combos)
    # Bonferroni op een 1-proportie z-test t.o.v. baseline-rate.
    from statistics import NormalDist
    z_threshold = NormalDist().inv_cdf(1 - (0.05 / n_tests) / 2)

    results = []
    for combo in combos:
        labels = [a[0] for a in combo]
        frags = [a[1] for a in combo]
        # Sla logisch tegenstrijdige combo's over (bv. bull_regime+bear_regime)
        if ("bull_regime" in labels and "bear_regime" in labels):
            continue
        if sum(l.startswith("mom_") for l in labels) > 1:
            continue
        n, beats, rate = _cohort_stats(conn, horizon, frags)
        if n < min_sample:
            continue
        conf = ss._wilson_lower_bound(beats, n)
        # z-score t.o.v. baseline-beat-rate (edge boven de gemiddelde muntworp)
        se = math.sqrt(baseline_rate * (1 - baseline_rate) / n) if 0 < baseline_rate < 1 else 0
        z = (rate - baseline_rate) / se if se > 0 else 0.0
        results.append({
            "conditions": labels,
            "n": n,
            "beat_rate": round(rate, 3),
            "vs_baseline": round(rate - baseline_rate, 3),
            "confidence": round(conf, 3),
            "z": round(z, 2),
            "significant": bool(z >= z_threshold),
        })

    results.sort(key=lambda r: r["confidence"], reverse=True)
    sig = [r for r in results if r["significant"]]

    return {
        "horizon_days": horizon,
        "baseline_beat_rate": round(baseline_rate, 3),
        "baseline_n": baseline_n,
        "n_tests": n_tests,
        "z_threshold_bonferroni": round(z_threshold, 2),
        "significant_count": len(sig),
        "results": results,
        "conclusion": _conclude(sig, results, baseline_rate),
    }


def _conclude(sig, allr, baseline) -> str:
    if sig:
        best = sig[0]
        return (f"{len(sig)} conditie-combinatie(s) verslaan de index significant BOVEN de "
                f"baseline ({baseline:.0%}) na multiple-testing-correctie. Sterkste: "
                f"{' + '.join(best['conditions'])} → {best['beat_rate']:.0%} beat-rate "
                f"(n={best['n']}, Wilson {best['confidence']:.0%}, z={best['z']}). "
                f"Bevestig out-of-sample voordat je hierop leunt.")
    if allr:
        best = max(allr, key=lambda r: r["confidence"])
        return (f"GEEN combinatie haalt de strenge multiple-testing-lat. Beste kandidaat: "
                f"{' + '.join(best['conditions'])} → {best['beat_rate']:.0%} (n={best['n']}, "
                f"Wilson {best['confidence']:.0%}). Suggestief maar niet bewijskrachtig — "
                f"meer (live, rijkere) data nodig.")
    return ("Onvoldoende data: geen enkele combinatie haalt de minimale steekproefgrootte. "
            "Draai meer backfill of laat live signalen rijpen.")


if __name__ == "__main__":
    conn = ss.init_db()
    for h in ss.HORIZONS:
        print("\n" + "=" * 72)
        print(f"  EDGE-DISCOVERY — horizon {h} handelsdagen")
        print("=" * 72)
        d = discover_edges(conn, horizon=h)
        print(f"Baseline beat-rate: {d['baseline_beat_rate']:.1%} (n={d['baseline_n']})")
        print(f"Combinaties getest: {d['n_tests']} | Bonferroni z-lat: {d['z_threshold_bonferroni']}")
        print("-" * 72)
        for r in d["results"][:10]:
            flag = "✅" if r["significant"] else "  "
            print(f"{flag} {' + '.join(r['conditions']):<38} "
                  f"beat {r['beat_rate']:.0%} (n={r['n']:>5}) "
                  f"Wilson {r['confidence']:.0%}  z={r['z']:+.2f}")
        print("-" * 72)
        print("CONCLUSIE:", d["conclusion"])
