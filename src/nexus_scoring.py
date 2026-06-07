"""
NEXUS Vectorized Scoring — Pandas batch processor for backtesting / simulation.

Implements the Nexus Pro scoring functions from the architectural blueprint
as vectorized pandas operations. This module is used for batch processing
a pre-fetched universe dataframe, not for live scanning (use tier1_scanner).

Input dataframe columns required:
  Technical:
    close, ema_10, ema_20, sma_50, sma_150, sma_200, sma_200_slope_30d,
    atr_14, atr_50, one_month_range_pct, volume_buzz_pct, rsi_14,
    macd_line, macd_signal

  Fundamental:
    rev_growth_q1, rev_growth_q2, rev_growth_q3 (YoY fractions, most-recent first)
    eps_growth_q1, eps_growth_q2, eps_growth_q3 (YoY fractions, most-recent first)
    roic (fraction, e.g. 0.18 = 18%)
    fcf_margin (fraction)
    dol (degree of operating leverage, e.g. 3.85)
    peg_ratio
    pe_z_score

  Alternative data:
    insider_buying_cluster (bool)
    short_interest_pct (percentage, e.g. 18.2)
    days_to_cover (float)
    sector_rrg_quadrant (str: "Leading" | "Weakening" | "Lagging" | "Improving")
    inst_flow_classification (str: see 13F table in blueprint)
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def calculate_nexus_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized calculation of S_Momentum, S_Growth, Convergence_Score,
    and Convergence_Trigger for a universe of pre-fetched candidates.

    Parameters
    ----------
    df : pd.DataFrame
        Universe dataframe with columns listed in module docstring.

    Returns
    -------
    pd.DataFrame
        Sorted by Convergence_Score descending. Contains scores, factors,
        and an execution flag for the Convergence Zone trigger.
    """
    res = df.copy()

    # ── Fill defaults for optional alternative-data columns ──────────────────
    res = res.fillna({
        "volume_buzz_pct":          0.0,
        "pe_z_score":               1.0,
        "peg_ratio":                2.0,
        "insider_buying_cluster":   False,
        "short_interest_pct":       0.0,
        "days_to_cover":            0.0,
        "sector_rrg_quadrant":      "Lagging",
        "inst_flow_classification": "Neutral",
    })

    # =========================================================================
    # ENGINE 1 — SHORT-TERM MOMENTUM SCORE (S_Momentum)
    # =========================================================================

    # M factor: Minervini MA alignment (0 or 1)
    ma_align = (
        (res["close"]   > res["ema_10"])
        & (res["ema_10"]  > res["ema_20"])
        & (res["ema_20"]  > res["sma_50"])
        & (res["sma_50"]  > res["sma_150"])
        & (res["sma_150"] > res["sma_200"])
        & (res["sma_200_slope_30d"] > 0)
    )
    res["M_factor"] = np.where(ma_align, 1.0, 0.0)

    # C factor: VCP squeeze quality [0, 1]
    atr_ratio = res["atr_14"] / res["close"]
    vcp_valid = (
        (atr_ratio < 0.05)
        & (res["atr_14"] < res["atr_50"])
        & (res["one_month_range_pct"] < 0.10)
    )
    res["C_factor"] = np.where(
        vcp_valid,
        (1.0 - (atr_ratio / 0.05)).clip(0.0, 1.0),
        0.0,
    )

    # V factor: Volume Buzz scaled to [0, 1]  (RVol 2.0 → 100% buzz → 1.0)
    res["V_factor"] = (res["volume_buzz_pct"] / 100.0).clip(0.0, 1.0)

    # O factor: RSI optimal zone (50–70) AND MACD bullish (0 or 1)
    macd_bull  = res["macd_line"] > res["macd_signal"]
    rsi_zone   = (res["rsi_14"] >= 50.0) & (res["rsi_14"] <= 70.0)
    res["O_factor"] = np.where(macd_bull & rsi_zone, 1.0, 0.0)

    # Alternative data modifiers
    res["Alt_mom"] = 0.0
    res.loc[res["sector_rrg_quadrant"].isin(["Leading", "Improving"]), "Alt_mom"] += 0.5
    squeeze = (res["short_interest_pct"] > 15.0) & (res["days_to_cover"] > 5.0)
    res.loc[squeeze, "Alt_mom"] += 1.0

    # Composite S_Momentum [0, 10]
    res["S_Momentum"] = (
        3.0 * res["M_factor"]
        + 4.0 * res["C_factor"]
        + 2.0 * res["V_factor"]
        + 1.0 * res["O_factor"]
        + res["Alt_mom"]
    ).clip(0.0, 10.0)

    # =========================================================================
    # ENGINE 2 — LONG-TERM GROWTH SCORE (S_Growth)
    # =========================================================================

    # A factor: 3 consecutive quarters of YoY sales + EPS acceleration (0 or 1)
    rev_accel = (
        (res["rev_growth_q1"] > res["rev_growth_q2"])
        & (res["rev_growth_q2"] > res["rev_growth_q3"])
        & (res["rev_growth_q3"] > 0)
        & (res["rev_growth_q1"] >= 0.25)
    )
    eps_accel = (
        (res["eps_growth_q1"] > res["eps_growth_q2"])
        & (res["eps_growth_q2"] > res["eps_growth_q3"])
        & (res["eps_growth_q3"] > 0)
        & (res["eps_growth_q1"] >= 0.30)
    )
    res["A_factor"] = np.where(rev_accel & eps_accel, 1.0, 0.0)

    # E factor: capital efficiency [0, 1]
    roic_score = (res["roic"] / 0.15).clip(0.0, 1.0)
    fcf_score  = (res["fcf_margin"] / 0.10).clip(0.0, 1.0)
    res["E_factor"] = 0.5 * (roic_score + fcf_score)

    # L factor: operating leverage vs baseline DOL of 3.0× [0, 1]
    res["L_factor"] = (res["dol"] / 3.0).clip(0.0, 1.0)

    # U factor: dual valuation filter — PE Z-score + PEG < 1.5 [0, 1]
    z_capped  = res["pe_z_score"].clip(0.0, 2.0)
    pe_score  = (1.0 - z_capped / 2.0).clip(0.0, 1.0)
    res["U_factor"] = np.where(res["peg_ratio"] < 1.5, pe_score, 0.0)

    # Alternative data modifiers
    res["Alt_growth"] = 0.0
    res.loc[res["insider_buying_cluster"] == True, "Alt_growth"] += 1.0
    res.loc[res["inst_flow_classification"] == "Healthy Accumulation",   "Alt_growth"] += 1.0
    res.loc[res["inst_flow_classification"] == "Contrarian Accumulation", "Alt_growth"] += 0.5
    res.loc[res["inst_flow_classification"] == "Distribution",            "Alt_growth"] -= 1.0
    res.loc[res["inst_flow_classification"] == "Capitulation",            "Alt_growth"] -= 2.0

    # Composite S_Growth [0, 10]
    res["S_Growth"] = (
        4.0 * res["A_factor"]
        + 3.0 * res["E_factor"]
        + 2.0 * res["L_factor"]
        + 1.0 * res["U_factor"]
        + res["Alt_growth"]
    ).clip(0.0, 10.0)

    # =========================================================================
    # CONVERGENCE ZONE RESOLUTION
    # =========================================================================

    res["Convergence_Score"]   = ((res["S_Momentum"] + res["S_Growth"]) / 2.0).round(2)
    res["Convergence_Trigger"] = ((res["S_Momentum"] >= 7.5) & (res["S_Growth"] >= 7.5))

    # ── Output columns ────────────────────────────────────────────────────────
    target_cols = [
        "ticker",
        "S_Momentum", "M_factor", "C_factor", "V_factor", "O_factor",
        "S_Growth",   "A_factor", "E_factor", "L_factor", "U_factor",
        "Convergence_Score", "Convergence_Trigger",
    ]
    available = [c for c in target_cols if c in res.columns]
    return res[available].sort_values("Convergence_Score", ascending=False)


# ── Pipeline validation & simulation ─────────────────────────────────────────

if __name__ == "__main__":
    mock = pd.DataFrame({
        "ticker": ["AAPL", "NVDA", "CRWD", "XOM"],
        "close":  [175.50, 480.20, 360.10, 145.00],
        # Technical indicators
        "ema_10":             [178.20, 478.50, 355.00, 142.10],
        "ema_20":             [176.10, 465.10, 352.10, 140.20],
        "sma_50":             [170.10, 440.00, 348.00, 138.50],
        "sma_150":            [165.20, 390.20, 340.00, 132.00],
        "sma_200":            [160.00, 360.50, 335.00, 130.00],
        "sma_200_slope_30d":  [0.015,  0.042,  0.008,  0.021],
        "atr_14":             [3.10,   8.20,   5.10,   4.20],
        "atr_50":             [4.80,   12.50,  5.05,   4.00],
        "one_month_range_pct":[0.04,   0.03,   0.08,   0.12],
        "volume_buzz_pct":    [125.0,  145.0,  85.0,   15.0],
        "rsi_14":             [61.2,   64.5,   54.2,   48.1],
        "macd_line":          [1.85,   12.45,  2.10,   -0.45],
        "macd_signal":        [1.20,   9.10,   1.95,   -0.10],
        # Fundamental acceleration (YoY fractions, most-recent first)
        "rev_growth_q1":      [0.12,   0.45,   0.15,   0.08],
        "rev_growth_q2":      [0.10,   0.38,   0.16,   0.06],
        "rev_growth_q3":      [0.08,   0.32,   0.14,   0.05],
        "eps_growth_q1":      [0.15,   0.55,   0.18,   0.10],
        "eps_growth_q2":      [0.12,   0.42,   0.20,   0.08],
        "eps_growth_q3":      [0.09,   0.35,   0.15,   0.07],
        # Capital efficiency & operating leverage
        "roic":               [0.18,   0.32,   0.22,   0.12],
        "fcf_margin":         [0.14,   0.24,   0.21,   0.07],
        "dol":                [2.10,   3.85,   2.95,   1.45],
        # Valuation
        "peg_ratio":          [1.10,   0.95,   1.65,   2.10],
        "pe_z_score":         [0.85,   0.25,   1.45,   1.95],
        # Alternative data
        "insider_buying_cluster": [False, True, False, False],
        "short_interest_pct":     [1.50,  18.20, 2.10,  4.50],
        "days_to_cover":          [1.20,  6.40,  1.50,  2.20],
        "sector_rrg_quadrant":    ["Leading", "Leading", "Improving", "Lagging"],
        "inst_flow_classification": [
            "Healthy Accumulation", "Healthy Accumulation",
            "Healthy Accumulation", "Capitulation",
        ],
    })

    report = calculate_nexus_scores(mock)

    print("\n" + "=" * 80)
    print("           NEXUS PRO — CONVERGENCE ZONE COHORT REPORT")
    print("=" * 80 + "\n")
    print(report.to_string(index=False))
    print("\n" + "=" * 80)
    convergence_hits = report[report["Convergence_Trigger"] == True]
    print(f"\nConvergence Zone activations: {len(convergence_hits)}")
    if not convergence_hits.empty:
        for _, row in convergence_hits.iterrows():
            print(f"  → {row['ticker']:8s}  S_Momentum={row['S_Momentum']:.2f}  S_Growth={row['S_Growth']:.2f}  Conv={row['Convergence_Score']:.2f}")
    print()
