"""
NEXUS Fundamental Engine — Long-Term Multi-Bagger Signal Computation

Implements the Long-Term Multi-Bagger Engine from the Nexus Pro blueprint:
  - Quarterly revenue & EPS acceleration (3 consecutive YoY quarters)
  - Capital efficiency: ROIC ≥ 15% and FCF Margin ≥ 10%
  - Operating leverage: DOL via accounting proxy (Gross Profit / EBIT)
  - Sector-relative valuation: PE Z-score + PEG < 1.5 dual filter
  - S_Growth composite score [0–10]:
      S_Growth = 4·A + 3·E + 2·L + 1·U + Alt_mod
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger("fundamental_engine")


# ── Sector calibration tables ─────────────────────────────────────────────────

# Approximate sector NTM forward PE mean and std for Z-score normalisation.
# Based on historical market averages — updated quarterly in production.
SECTOR_PE_STATS: dict[str, dict] = {
    "Tech & AI":          {"mean": 38.0, "std": 18.0},
    "Healthcare":         {"mean": 26.0, "std": 11.0},
    "Consumer Cyclical":  {"mean": 23.0, "std":  9.0},
    "Consumer Defensive": {"mean": 21.0, "std":  6.0},
    "Industrials":        {"mean": 23.0, "std":  8.0},
    "Financials":         {"mean": 13.0, "std":  5.0},
    "Energy":             {"mean": 12.0, "std":  5.0},
    "Materials":          {"mean": 16.0, "std":  6.0},
    "Utilities":          {"mean": 18.0, "std":  5.0},
    "Real Estate":        {"mean": 32.0, "std": 14.0},
    "Others":             {"mean": 22.0, "std": 10.0},
}

# GICS sector DOL targets for operating leverage scoring (blueprint Table 3).
DOL_TARGETS: dict[str, float] = {
    "Tech & AI":          3.5,   # High upfront R&D; near-zero marginal replication
    "Healthcare":         2.5,
    "Consumer Cyclical":  1.8,   # High variable costs (inventory, shipping)
    "Consumer Defensive": 1.8,
    "Industrials":        2.2,   # Heavy fixed asset depreciation
    "Financials":         2.0,
    "Energy":             2.0,
    "Materials":          2.0,
    "Utilities":          2.0,
    "Real Estate":        2.0,
    "Others":             2.5,
}


# ── Statement retrieval helpers ───────────────────────────────────────────────

def _quarterly_stmt(t) -> pd.DataFrame | None:
    """Return quarterly income statement, trying new API then legacy."""
    for attr in ("quarterly_income_stmt", "quarterly_financials"):
        try:
            df = getattr(t, attr, None)
            if df is not None and not df.empty and df.shape[1] >= 8:
                return df
        except Exception:
            continue
    return None


def _annual_stmt(t) -> pd.DataFrame | None:
    for attr in ("income_stmt", "financials"):
        try:
            df = getattr(t, attr, None)
            if df is not None and not df.empty:
                return df
        except Exception:
            continue
    return None


def _find_row(df: pd.DataFrame, keys: list[str]) -> pd.Series | None:
    """Case-insensitive substring match; returns first non-empty row found."""
    for key in keys:
        matches = [r for r in df.index if key.lower() in str(r).lower()]
        if matches:
            row = df.loc[matches[0]].dropna()
            if len(row) >= 1:
                return row
    return None


def _yoy(row: pd.Series, i: int) -> float | None:
    """
    Year-over-year growth: (row[i] − row[i+4]) / |row[i+4]|.
    Assumes quarterly columns ordered most-recent first.
    Returns None when data is missing or previous period is zero.
    """
    if len(row) <= i + 4:
        return None
    prev = float(row.iloc[i + 4])
    curr = float(row.iloc[i])
    if prev == 0 or np.isnan(prev) or np.isnan(curr):
        return None
    return (curr - prev) / abs(prev)


# ── Quarterly Acceleration ────────────────────────────────────────────────────

def compute_quarterly_acceleration(t) -> dict:
    """
    Verify 3 consecutive quarters of YoY revenue AND EPS acceleration.

    Acceleration pattern (most-recent quarter first):
        q1_yoy > q2_yoy > q3_yoy > 0

    Minimum thresholds:
        revenue_q1_yoy ≥ 25%   (blueprint §2 growth threshold)
        eps_q1_yoy     ≥ 30%

    Returns a dict with rev_accel (bool), eps_accel (bool), and all
    individual quarter YoY growth percentages for dashboard display.
    """
    out: dict = {
        "rev_accel":   False,  "eps_accel":   False,
        "rev_q1_yoy":  None,   "rev_q2_yoy":  None,  "rev_q3_yoy":  None,
        "eps_q1_yoy":  None,   "eps_q2_yoy":  None,  "eps_q3_yoy":  None,
    }
    try:
        qfin = _quarterly_stmt(t)
        if qfin is None:
            return out

        rev_row = _find_row(qfin, ["Total Revenue", "Revenue", "Operating Revenue"])
        if rev_row is not None and len(rev_row) >= 8:
            q1, q2, q3 = _yoy(rev_row, 0), _yoy(rev_row, 1), _yoy(rev_row, 2)
            out.update({
                "rev_q1_yoy": round(q1 * 100, 1) if q1 is not None else None,
                "rev_q2_yoy": round(q2 * 100, 1) if q2 is not None else None,
                "rev_q3_yoy": round(q3 * 100, 1) if q3 is not None else None,
            })
            if all(x is not None for x in (q1, q2, q3)):
                out["rev_accel"] = q1 >= 0.25 and q1 > q2 > q3 > 0

        eps_row = _find_row(qfin, ["Basic EPS", "Diluted EPS", "EPS Diluted", "EPS"])
        if eps_row is not None and len(eps_row) >= 8:
            q1, q2, q3 = _yoy(eps_row, 0), _yoy(eps_row, 1), _yoy(eps_row, 2)
            out.update({
                "eps_q1_yoy": round(q1 * 100, 1) if q1 is not None else None,
                "eps_q2_yoy": round(q2 * 100, 1) if q2 is not None else None,
                "eps_q3_yoy": round(q3 * 100, 1) if q3 is not None else None,
            })
            if all(x is not None for x in (q1, q2, q3)):
                out["eps_accel"] = q1 >= 0.30 and q1 > q2 > q3 > 0

    except Exception as e:
        log.debug("quarterly_accel error: %s", e)
    return out


# ── Degree of Operating Leverage ──────────────────────────────────────────────

def compute_dol(info: dict, t) -> float | None:
    """
    Degree of Operating Leverage — accounting proxy (blueprint §2):
        DOL_accounting = Gross Profit / EBIT

    Where Gross Profit = Revenue − COGS  and  EBIT = Operating Income.
    Falls back to the annual income statement when info dict is incomplete.
    """
    try:
        gp   = float(info.get("grossProfits") or 0)
        ebit = float(info.get("ebit") or info.get("operatingIncome") or 0)
        if gp and ebit > 0:
            return round(gp / ebit, 2)

        fin = _annual_stmt(t)
        if fin is None:
            return None
        gp_row   = _find_row(fin, ["Gross Profit"])
        ebit_row = _find_row(fin, ["Operating Income", "EBIT", "Ebit"])
        if gp_row is not None and ebit_row is not None:
            gp_v, ebit_v = float(gp_row.iloc[0]), float(ebit_row.iloc[0])
            if ebit_v > 0 and not np.isnan(gp_v):
                return round(gp_v / ebit_v, 2)
    except Exception as e:
        log.debug("DOL error: %s", e)
    return None


# ── Valuation helpers ─────────────────────────────────────────────────────────

def compute_pe_z_score(pe: float, group: str) -> float:
    """
    Sector-relative PE Z-score (blueprint §2 normalisation):
        Z_PE = (Forward_PE_stock − μ_sector_PE) / σ_sector_PE

    Z_PE ≤ 1.0 = within normal valuation range
    Z_PE > 2.0 = statistically overvalued relative to peers
    """
    if pe <= 0:
        return 0.0
    s = SECTOR_PE_STATS.get(group, SECTOR_PE_STATS["Others"])
    return round((pe - s["mean"]) / s["std"], 2) if s["std"] > 0 else 0.0


def _accel_grade(q1: float | None, q2: float | None, q3: float | None, min_q1: float) -> float:
    """
    Graded acceleration score in [0, 1].

    The strict blueprint definition (3 straight accelerating quarters above a
    hypergrowth threshold) only ever fires for a handful of hypergrowth names,
    which left A_factor at 0 for almost the entire scan universe and made the
    S_Growth ceiling unreachable for perfectly good, merely-solid growers.
    This grades partial credit so quality-but-not-hypergrowth companies still
    score proportionally instead of being clipped to zero.
    """
    if q1 is None:
        return 0.0
    grade = 0.0
    if q1 > 0:
        grade += 0.35 * min(1.0, q1 / min_q1)
    if q2 is not None and q1 > q2:
        grade += 0.25
    if q3 is not None and q2 is not None and q2 > q3:
        grade += 0.20
    if q3 is not None and q3 > 0:
        grade += 0.20
    return round(min(1.0, grade), 3)


# ── S_Growth composite score ──────────────────────────────────────────────────

def compute_s_growth(
    info:          dict,
    ticker:        str,
    group:         str,
    t,
    roic:          float | None,
    growth_themes: set[str],
) -> dict:
    """
    Compute Long-Term Growth Score S_Growth ∈ [0, 10]:

        S_Growth = 4·A + 3·E + 2·L + 1·U + Alt_mod

    A — Acceleration factor [0, 1]
        1.0 if 3 consecutive quarters of accelerating YoY rev+EPS growth

    E — Capital efficiency factor [0, 1]
        0.5 × (min(1, ROIC/15%) + min(1, FCF_Margin/10%))

    L — Operating leverage factor [0, 1]
        min(1, DOL / GICS_sector_target)

    U — Valuation attractiveness factor [0, 1]
        max(0, 1 − Z_PE/2) × 𝟙(PEG < 1.5)

    Alt_mod:
        +0.5 if ticker is in the curated growth-theme universe
        (serves as proxy for insider/13F accumulation signal)
    """
    qdata = compute_quarterly_acceleration(t)

    # A: fundamental acceleration (graded — see _accel_grade docstring)
    rev_grade = _accel_grade(qdata["rev_q1_yoy"] and qdata["rev_q1_yoy"] / 100,
                              qdata["rev_q2_yoy"] and qdata["rev_q2_yoy"] / 100,
                              qdata["rev_q3_yoy"] and qdata["rev_q3_yoy"] / 100,
                              min_q1=0.25)
    eps_grade = _accel_grade(qdata["eps_q1_yoy"] and qdata["eps_q1_yoy"] / 100,
                              qdata["eps_q2_yoy"] and qdata["eps_q2_yoy"] / 100,
                              qdata["eps_q3_yoy"] and qdata["eps_q3_yoy"] / 100,
                              min_q1=0.30)
    A = round(0.5 * (rev_grade + eps_grade), 3)

    # E: capital efficiency
    roic_score = min(1.0, float(roic) / 15.0) if roic is not None else 0.0
    fcf        = float(info.get("freeCashflow") or 0)
    rev        = float(info.get("totalRevenue") or 0)
    fcf_margin = fcf / rev if (fcf > 0 and rev > 0) else 0.0
    E = 0.5 * (roic_score + min(1.0, fcf_margin / 0.10))

    # L: operating leverage vs GICS target
    dol        = compute_dol(info, t) or 0.0
    dol_target = DOL_TARGETS.get(group, 2.5)
    L = min(1.0, dol / dol_target) if dol_target > 0 else 0.0

    # U: dual valuation filter — PE Z-score + PEG
    pe      = float(info.get("trailingPE", 0) or 0)
    z_pe    = compute_pe_z_score(pe, group)
    u_base  = max(0.0, 1.0 - max(0.0, min(2.0, z_pe)) / 2.0)
    peg     = info.get("pegRatio")
    if peg is None and pe > 0:
        rg  = float(info.get("revenueGrowth", 0) or 0)
        peg = pe / (rg * 100) if rg > 0.02 else None
    U = u_base if (peg is not None and float(peg) < 1.5) else 0.0

    # Alt modifier: thematic growth bonus (proxy for insider / 13F signal)
    alt = 0.5 if ticker in growth_themes else 0.0

    raw = 4.0 * A + 3.0 * E + 2.0 * L + 1.0 * U + alt
    s   = round(max(0.0, min(10.0, raw)), 2)

    return {
        "s_growth":       s,
        "A_factor":       A,
        "E_factor":       round(E, 3),
        "L_factor":       round(L, 3),
        "U_factor":       round(U, 3),
        "dol":            round(dol, 2),
        "pe_z_score":     z_pe,
        "peg_ratio":      round(float(peg), 2) if peg is not None else None,
        "fcf_margin_pct": round(fcf_margin * 100, 1),
        **qdata,
    }
