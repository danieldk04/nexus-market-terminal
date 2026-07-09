"""
NEXUS Backtest Sweep — parameter-grid met anti-overfitting bescherming.

Draait de backtest over veel configuraties en zoekt naar een STATISTISCH
ROBUUSTE edge — niet zomaar de hoogste t-statistiek (dat is data-mining).

Twee valkuilen die dit script expliciet adresseert:
  1. Multiple testing: hoe meer configs je probeert, hoe groter de kans dat
     er eentje puur door toeval goed scoort. Daarom wordt de significantie-lat
     verhoogd naarmate er meer configs getest zijn (Bonferroni-benadering).
  2. Overfitting: een config die alleen op de volledige periode goed oogt maar
     niet in BEIDE helften apart, is waarschijnlijk curve-fit. Elke config
     wordt daarom out-of-sample gesplitst (eerste helft vs tweede helft).

Een config telt pas als "ROBUUST" wanneer:
  - alpha > 0 in de eerste EN de tweede helft (consistentie), en
  - de volledige-periode t-statistiek de multiple-testing-lat haalt.

Gebruikt de prijs-cache van backtest_engine, dus na de eerste run is dit snel.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from itertools import product
from pathlib import Path

import numpy as np

import backtest_engine as bt

ROOT = Path(__file__).resolve().parent.parent
SWEEP_PATH = ROOT / "backtest_sweep_results.json"

# Parameter grid — uitbreidbaar
GRID = {
    "top_n":        [3, 5, 8, 10, 15],
    "freq_months":  [1, 2, 3, 6],
    "min_score":    [0.0, 3.0, 5.0],
}


def _tstat(vals: list[float]) -> float:
    a = np.array(vals, dtype=float)
    if len(a) < 2 or a.std(ddof=1) == 0:
        return 0.0
    return float(a.mean() / (a.std(ddof=1) / math.sqrt(len(a))))


def _half_alpha(excess: list[float]) -> tuple[float, float]:
    """Gemiddelde excess-return (per periode, %) voor eerste en tweede helft."""
    if len(excess) < 4:
        return 0.0, 0.0
    mid = len(excess) // 2
    first = float(np.mean(excess[:mid]))
    second = float(np.mean(excess[mid:]))
    return round(first, 3), round(second, 3)


def run_sweep() -> dict:
    combos = list(product(GRID["top_n"], GRID["freq_months"], GRID["min_score"]))
    n_tests = len(combos)
    # Bonferroni: verdeel alpha=0.05 over het aantal tests; vertaal naar
    # een benaderde t-drempel (tweezijdig, grote-n normaalbenadering).
    from statistics import NormalDist
    adj_p = 0.05 / n_tests
    t_threshold = NormalDist().inv_cdf(1 - adj_p / 2)

    print(f"Sweep: {n_tests} configuraties. Multiple-testing t-drempel = {t_threshold:.2f} "
          f"(vs 1.96 bij één test).\n")

    results = []
    for i, (top_n, freq, min_score) in enumerate(combos, 1):
        try:
            res = bt.run_backtest(
                years=6, freq_months=freq, top_n=top_n,
                min_score=min_score, cost_bps=10.0, use_cache=True,
            )
        except Exception as e:
            print(f"[{i}/{n_tests}] top_n={top_n} freq={freq} min={min_score} -> FOUT: {e}")
            continue

        m = res["metrics"]
        excess = [r["excess"] for r in res["rebalances"]]
        full_t = _tstat(excess)
        h1, h2 = _half_alpha(excess)
        consistent = (h1 > 0 and h2 > 0)
        robust = consistent and (abs(full_t) >= t_threshold)

        row = {
            "top_n": top_n, "freq_months": freq, "min_score": min_score,
            "periods": m["periods"],
            "alpha_cagr": m["alpha_cagr"],
            "strategy_cagr": m["strategy_cagr"],
            "hit_rate": m["hit_rate"],
            "sharpe": m["sharpe"],
            "t_stat": round(full_t, 2),
            "half1_alpha": h1, "half2_alpha": h2,
            "consistent": consistent,
            "robust": robust,
        }
        results.append(row)
        flag = "✅ ROBUUST" if robust else ("~ consistent" if consistent else "")
        print(f"[{i}/{n_tests}] top{top_n:>2} {freq}mnd min{min_score:>3} | "
              f"alpha {m['alpha_cagr']:+6.2f}% | t={full_t:+.2f} | "
              f"hit {m['hit_rate']:>4.1f}% | H1 {h1:+.2f} H2 {h2:+.2f} {flag}")

    # Rank: robuuste eerst, dan op consistentie + alpha
    results.sort(key=lambda r: (r["robust"], r["consistent"], r["alpha_cagr"]), reverse=True)

    robust_configs = [r for r in results if r["robust"]]

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_tests": n_tests,
        "t_threshold_bonferroni": round(t_threshold, 2),
        "results": results,
        "robust_count": len(robust_configs),
        "conclusion": _conclude(robust_configs, results),
    }
    with open(SWEEP_PATH, "w") as f:
        json.dump(out, f, indent=2)

    print("\n" + "=" * 72)
    print("CONCLUSIE:", out["conclusion"])
    print("=" * 72)
    print(f"Volledig rapport: {SWEEP_PATH}")
    return out


def _conclude(robust, allr) -> str:
    if robust:
        best = robust[0]
        return (f"{len(robust)} van {len(allr)} configs zijn ROBUUST (significant + "
                f"consistent in beide helften). Beste: top_n={best['top_n']}, "
                f"{best['freq_months']}-mnd, min_score={best['min_score']} → "
                f"alpha {best['alpha_cagr']:+.2f}%/jr, t={best['t_stat']}. "
                f"Dit overleeft de multiple-testing-correctie en out-of-sample split — "
                f"dit is de sterkste kandidaat voor een echte edge.")
    consistent = [r for r in allr if r["consistent"]]
    if consistent:
        best = max(consistent, key=lambda r: r["alpha_cagr"])
        return (f"GEEN enkele config haalt de strenge multiple-testing-lat, maar "
                f"{len(consistent)} zijn wel consistent (alpha>0 in beide helften). "
                f"Beste consistente: top_n={best['top_n']}, {best['freq_months']}-mnd, "
                f"min_score={best['min_score']} → alpha {best['alpha_cagr']:+.2f}%/jr, "
                f"t={best['t_stat']}. Veelbelovend maar nog niet bewijskrachtig — "
                f"meer historie/tijd nodig, of edge is te zwak om zeker te zijn.")
    return ("GEEN robuuste én GEEN consistente config gevonden. De schijnbare "
            "outperformance is waarschijnlijk toeval/curve-fitting. Op basis hiervan "
            "is een indexfonds de rationele keuze totdat er sterker bewijs is.")


if __name__ == "__main__":
    run_sweep()
