"""
NEXUS Trade Republic Portfolio — CSV-vrije aanpak via TR_HOLDINGS secret
─────────────────────────────────────────────────────────────────────────
Workflow:
  1. Gebruiker exporteert maandelijks transacties als CSV (herinnering: 19e)
  2. Plakt bijgewerkte posities in het GitHub-secret TR_HOLDINGS
  3. Dit module haalt actuele prijzen op via yfinance en berekent totaalwaarde

TR_HOLDINGS format (één positie per regel):
  ISIN_of_symbool  aantalAandelen
  BTC              0.0702790
  IE00BK5BQT80     5.1854850
  ...
"""
from __future__ import annotations

import logging
import os
import time

import yfinance as yf

log = logging.getLogger("tr_portfolio")

# ─── ISIN → yfinance tickers (primair + fallbacks) ──────────────────────────
# Lijst per ISIN: probeer in volgorde tot een prijs gevonden wordt
ISIN_TO_TICKERS: dict[str, list[str]] = {
    # Vanguard FTSE All-World UCITS ETF USD Acc
    "IE00BK5BQT80": ["VWCE.DE", "VWCE.MI", "VWRD.AS"],
    # Vanguard FTSE All-World High Dividend Yield UCITS ETF Dist
    "IE00BK5BR626": ["VHYL.AS", "VHYL.DE", "VGWD.DE"],
    # Amundi S&P 500 UCITS ETF EUR Acc
    "LU1681048804": ["PCAR.DE", "CSP1.PA", "CSPX.AS"],
    # VanEck Morningstar Developed Markets Dividend Leaders
    "NL0011683594": ["TDIV.AS"],
    # Bitcoin — in EUR
    "BTC":          ["BTC-EUR"],
}

# Backwards-compat: gebruik eerste ticker als primair
ISIN_TO_TICKER: dict[str, str] = {k: v[0] for k, v in ISIN_TO_TICKERS.items()}

# Mooie weergavenamen voor de briefing
DISPLAY_NAMES: dict[str, str] = {
    "IE00BK5BQT80": "VWCE (All-World Acc)",
    "IE00BK5BR626": "VHYL (High Div Yield)",
    "LU1681048804": "CSP1 (S&P 500 EUR)",
    "NL0011683594": "TDIV (Dev Div Leaders)",
    "BTC":          "Bitcoin",
}


def _parse_tr_holdings() -> list[dict]:
    """
    Lees TR_HOLDINGS omgevingsvariabele.
    Formaat: één positie per regel, ISIN/symbool + aantal gescheiden door spatie/tab.
    Regels die beginnen met # worden overgeslagen.
    """
    raw = os.environ.get("TR_HOLDINGS", "").strip()
    if not raw:
        log.info("TR_HOLDINGS niet ingesteld.")
        return []

    holdings = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        isin    = parts[0].upper()
        try:
            shares  = float(parts[1])
        except ValueError:
            log.warning(f"TR_HOLDINGS: ongeldige hoeveelheid voor {isin}: {parts[1]}")
            continue
        if shares > 0:
            holdings.append({"isin": isin, "shares": shares})

    log.info(f"TR_HOLDINGS: {len(holdings)} posities geladen.")
    return holdings


def _fetch_price_eur(isin: str) -> tuple[str | None, float | None]:
    """
    Probeer tickers in volgorde tot een geldige prijs gevonden wordt.
    Geeft (gebruikte_ticker, prijs) terug, of (None, None) als alles mislukt.
    """
    tickers = ISIN_TO_TICKERS.get(isin, [ISIN_TO_TICKER.get(isin, "")])
    for ticker in tickers:
        if not ticker:
            continue
        try:
            info  = yf.Ticker(ticker).fast_info
            price = getattr(info, "last_price", None)
            if price and float(price) > 0:
                log.info(f"{isin} → {ticker}: €{price:.2f}")
                return ticker, float(price)
        except Exception as e:
            log.debug(f"yfinance {ticker}: {e}")
        time.sleep(0.2)
    log.warning(f"{isin}: geen prijs gevonden (geprobeerd: {tickers})")
    return None, None


def fetch_tr_portfolio() -> dict | None:
    """
    Bouw Trade Republic portfolio op vanuit TR_HOLDINGS secret + yfinance prijzen.
    Geeft None terug als TR_HOLDINGS niet beschikbaar is.
    """
    holdings = _parse_tr_holdings()
    if not holdings:
        return None

    positions = []
    total     = 0.0

    for h in holdings:
        isin   = h["isin"]
        shares = h["shares"]
        ticker, price = _fetch_price_eur(isin)

        if price is None or ticker is None:
            # Positie tonen zonder waarde (prijs onbekend)
            positions.append({
                "name":   DISPLAY_NAMES.get(isin, isin),
                "ticker": isin,
                "isin":   isin,
                "size":   shares,
                "price":  None,
                "value":  0.0,
                "pl_pct": None,
            })
            continue

        value = shares * price
        total += value
        positions.append({
            "name":   DISPLAY_NAMES.get(isin, isin),
            "ticker": ticker,
            "isin":   isin,
            "size":   round(shares, 6),
            "price":  round(price, 4),
            "value":  round(value, 2),
            "pl_pct": None,   # Aankoopprijs n/b — toe te voegen aan TR_HOLDINGS
        })
        time.sleep(0.1)

    if not positions:
        log.warning("TR portfolio: geen posities met prijs.")
        return None

    positions.sort(key=lambda p: p["value"], reverse=True)
    log.info(f"Trade Republic: {len(positions)} posities, totaal €{total:.2f}")
    return {"positions": positions, "total": round(total, 2)}
