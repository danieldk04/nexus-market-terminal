"""
NEXUS Signal CLI — inspecteer de signaal-database en test de kalibratie.

    python src/signal_cli.py stats
    python src/signal_cli.py confidence --score 7.5 --horizon 21
"""
import argparse

import signal_store as ss
import calibration as cal


def cmd_stats():
    conn = ss.init_db()
    s = ss.stats(conn)
    print(f"Totaal signalen:      {s['total_signals']}")
    print(f"Met bekende uitkomst: {s['realized_outcomes']}")
    print(f"Per bron:             {s['by_source']}")
    print(f"Beat-rate (alles):    {s['overall_beat_rate']}")


def cmd_confidence(score: float, horizon: int):
    conn = ss.init_db()
    res = ss.calibrated_confidence(
        conn, {"convergence_score": score, "s_momentum": score}, horizon_days=horizon
    )
    if not res.get("available"):
        print(f"Geen kalibratie mogelijk: {res.get('reason')}")
        return
    print(f"Signaal-score {score} @ {horizon}d horizon:")
    print(f"  Cohort:              n={res['n']} (band ±{res['band']} op {res['cohort_key']})")
    print(f"  Versloeg de index:   {res['beat_benchmark_rate']:.0%} van de keren")
    print(f"  Positief rendement:  {res['positive_return_rate']:.0%} van de keren")
    print(f"  CONFIDENCE (eerlijk): {res['confidence']:.0%}  ← Wilson-ondergrens 95%")
    print(f"  High-conviction:     {'JA' if res['high_conviction'] else 'nee'} "
          f"(vereist conf≥90% én n≥100)")


def cmd_discover(horizon: int):
    conn = ss.init_db()
    d = cal.discover_edges(conn, horizon=horizon)
    print(f"Baseline beat-rate: {d['baseline_beat_rate']:.1%} (n={d['baseline_n']})")
    print(f"Combinaties getest: {d['n_tests']} | Bonferroni z-lat: {d['z_threshold_bonferroni']}")
    print("-" * 68)
    for r in d["results"][:12]:
        flag = "OK" if r["significant"] else "  "
        print(f"{flag} {' + '.join(r['conditions']):<34} beat {r['beat_rate']:.0%} "
              f"(n={r['n']:>5}) Wilson {r['confidence']:.0%} z={r['z']:+.2f}")
    print("-" * 68)
    print("CONCLUSIE:", d["conclusion"])


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("stats")
    c = sub.add_parser("confidence")
    c.add_argument("--score", type=float, required=True)
    c.add_argument("--horizon", type=int, default=21, choices=list(ss.HORIZONS))
    d = sub.add_parser("discover")
    d.add_argument("--horizon", type=int, default=21, choices=list(ss.HORIZONS))
    args = ap.parse_args()

    if args.cmd == "stats":
        cmd_stats()
    elif args.cmd == "confidence":
        cmd_confidence(args.score, args.horizon)
    elif args.cmd == "discover":
        cmd_discover(args.horizon)


if __name__ == "__main__":
    main()
