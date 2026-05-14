"""
NEXUS DCF Engine — Discounted Cash Flow berekeningen met dynamische WACC
Importeer als module: from dcf_engine import compute_dcf, compute_wacc, compute_roce, compute_roic
"""
from __future__ import annotations

RISK_FREE_RATE  = 0.045   # 10-jaars US Treasury baseline
MARKET_PREMIUM  = 0.055   # Historische equity risk premium
TERMINAL_GROWTH = 0.025   # Conservatieve lange-termijn groei (GDP-niveau)
MOS_DISCOUNT    = 0.25    # Margin of Safety: 25% korting op intrinsieke waarde


def compute_wacc(info: dict) -> float:
    """
    Weighted Average Cost of Capital via CAPM.
    Ke = Rf + Beta * ERP
    WACC = (E/V)*Ke + (D/V)*Kd*(1-T)
    """
    beta = float(info.get("beta", 1.0) or 1.0)
    beta = max(0.5, min(3.0, beta))

    ke = RISK_FREE_RATE + beta * MARKET_PREMIUM

    interest_exp = abs(info.get("interestExpense", 0) or 0)
    total_debt   = info.get("totalDebt", 0) or 0
    kd_pretax    = interest_exp / total_debt if total_debt > 0 else 0.05
    kd_pretax    = max(0.02, min(0.15, kd_pretax))
    tax_rate     = float(info.get("effectiveTaxRate", 0.21) or 0.21)
    kd           = kd_pretax * (1 - tax_rate)

    market_cap  = info.get("marketCap", 0) or 0
    total_value = market_cap + total_debt
    if total_value == 0:
        return round(ke, 4)

    e_ratio = market_cap / total_value
    d_ratio = total_debt / total_value
    wacc    = e_ratio * ke + d_ratio * kd
    return round(max(0.05, min(0.22, wacc)), 4)


def compute_dcf(info: dict) -> dict | None:
    """
    5-jaars DCF met twee-fase groeimodel:
      Fase 1: 5 jaar op geschatte groeivoet (van revenue- en EPS-groei)
      Fase 2: terminal value op TERMINAL_GROWTH voor altijd
    Geeft None als verplichte velden ontbreken of ongeldig zijn.
    """
    fcf    = info.get("freeCashflow", 0) or 0
    shares = (info.get("sharesOutstanding") or info.get("impliedSharesOutstanding") or 0)
    price  = info.get("currentPrice") or info.get("regularMarketPrice") or 0

    if fcf <= 0 or shares <= 0 or price <= 0:
        return None

    wacc = compute_wacc(info)

    # Groeivoet fase 1: gemiddelde omzet- en winstgroei, geclipped 0-25%
    rev_growth = float(info.get("revenueGrowth", 0.05) or 0.05)
    eps_growth = float(info.get("earningsGrowth", 0.05) or 0.05)
    g1 = min(0.25, max(0.0, (rev_growth + eps_growth) / 2))

    # FCF projectie fase 1
    projected = []
    cf = float(fcf)
    for _ in range(5):
        cf *= (1 + g1)
        projected.append(cf)

    pv_fcf = sum(c / (1 + wacc) ** (i + 1) for i, c in enumerate(projected))

    # Terminal value
    if wacc <= TERMINAL_GROWTH:
        return None
    tv    = projected[-1] * (1 + TERMINAL_GROWTH) / (wacc - TERMINAL_GROWTH)
    pv_tv = tv / (1 + wacc) ** 5

    # Enterprise Value → Equity Value
    total_debt = info.get("totalDebt", 0) or 0
    cash       = info.get("cash", 0) or 0
    net_debt   = total_debt - cash
    equity_val = pv_fcf + pv_tv - net_debt

    if equity_val <= 0:
        return None

    intrinsic  = equity_val / shares
    dcf_upside = round(((intrinsic / price) - 1) * 100, 1)

    return {
        "dcf_per_share":   round(intrinsic, 2),
        "dcf_upside":      dcf_upside,
        "mos_price":       round(intrinsic * (1 - MOS_DISCOUNT), 2),
        "wacc":            round(wacc * 100, 2),
        "growth_phase1":   round(g1 * 100, 1),
        "terminal_growth": round(TERMINAL_GROWTH * 100, 1),
        "pv_fcf":          round(pv_fcf / 1e6, 1),
        "pv_terminal_m":   round(pv_tv / 1e6, 1),
    }


def compute_roce(info: dict) -> float | None:
    """
    Return on Capital Employed = EBIT / Capital Employed
    Capital Employed = Total Assets − Current Liabilities
    """
    try:
        ebit         = info.get("ebit") or info.get("operatingIncome") or 0
        total_assets = info.get("totalAssets", 0) or 0
        current_liab = info.get("currentLiabilities", 0) or 0
        cap_employed = total_assets - current_liab
        if ebit and cap_employed > 0:
            return round((ebit / cap_employed) * 100, 2)
    except Exception:
        pass
    return None


def compute_roic(info: dict) -> float | None:
    """
    Return on Invested Capital = NOPAT / Invested Capital
    Invested Capital = Total Assets − Current Liabilities − Cash
    """
    direct = info.get("returnOnCapital")
    if direct is not None and direct != 0:
        return round(float(direct) * 100, 2)
    try:
        ebit         = info.get("operatingIncome") or info.get("ebit") or 0
        tax_rate     = float(info.get("effectiveTaxRate", 0.21) or 0.21)
        nopat        = ebit * (1 - tax_rate)
        total_assets = info.get("totalAssets", 0) or 0
        current_liab = info.get("currentLiabilities", 0) or 0
        cash         = info.get("cash", 0) or 0
        invested_cap = total_assets - current_liab - cash
        if nopat and invested_cap > 0:
            return round((nopat / invested_cap) * 100, 2)
    except Exception:
        pass
    return None


def check_dividend_sustainability(info: dict) -> dict | None:
    """
    Controleert of dividend houdbaar is op basis van FCF Payout Ratio.
    Geeft None als geen dividend.
    """
    div_yield = info.get("dividendYield", 0) or 0
    if div_yield == 0:
        return None

    div_rate  = info.get("dividendRate", 0) or 0
    fcf       = info.get("freeCashflow", 0) or 0
    eps       = info.get("trailingEps", 0) or 0
    shares    = info.get("sharesOutstanding", 0) or 1

    eps_payout = round((div_rate / eps) * 100, 1) if eps > 0 else None
    total_divs = div_rate * shares
    fcf_payout = round((total_divs / fcf) * 100, 1) if fcf > 0 else None

    sustainable = True
    risk_flag   = None
    if fcf_payout is not None and fcf_payout > 80:
        sustainable = False
        risk_flag   = f"FCF payout {fcf_payout}% — risico op dividendverlaging"
    elif eps_payout is not None and eps_payout > 90:
        sustainable = False
        risk_flag   = f"EPS payout {eps_payout}% — hoog t.o.v. winst"

    return {
        "yield":       round(div_yield * 100, 2),
        "eps_payout":  eps_payout,
        "fcf_payout":  fcf_payout,
        "sustainable": sustainable,
        "risk_flag":   risk_flag,
    }


def vix_dynamic_threshold(vix: float) -> float:
    """
    Dynamische instapdrempel op basis van marktangst (VIX).
    Lage VIX = complacency = wees kritischer.
    Hoge VIX = fear = pak de kansen.
    """
    if vix <= 0:
        return 6.5    # Onbekend → conservatief
    if vix < 15:
        return 7.5    # Extreme complacency — only best-of-best
    if vix < 20:
        return 6.5    # Normaal
    if vix < 28:
        return 6.0    # Enige onrust — begin te kopen
    if vix < 36:
        return 5.5    # Angst — agressief instappen
    return 5.0        # Extreme angst — maximale agressiviteit


def kelly_position_size(score: float, dcf_upside: float | None, cash: float,
                        max_pct: float = 0.20, half_kelly: bool = True) -> tuple[float, float]:
    """
    Kelly Criterion positiebepaling.
    f* = (b*p - q) / b   →  half-Kelly voor risicobeheer
    p  = winkans (afgeleid van conviction score)
    b  = verwacht rendement (DCF upside of fallback 20%)
    Geeft (fractie, eurobedrag) terug.
    """
    p = min(0.88, max(0.40, (score / 10) * 0.80 + 0.10))
    q = 1 - p
    b = min(1.0, max(0.10, (dcf_upside or 20.0) / 100))

    kelly_full = (b * p - q) / b
    kelly_f    = kelly_full * 0.5 if half_kelly else kelly_full
    kelly_f    = min(max_pct, max(0.05, kelly_f))

    return round(kelly_f, 4), round(cash * kelly_f, 2)
