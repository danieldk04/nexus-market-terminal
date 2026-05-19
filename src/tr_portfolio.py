"""
NEXUS Trade Republic Portfolio — CSV-vrije aanpak via TR_HOLDINGS secret
─────────────────────────────────────────────────────────────────────────
Workflow:
  1. Gebruiker exporteert transacties als CSV (TR app → Profiel → Documenten)
     Kies datumbereik "Alle tijd" voor volledige kostenhistorie.
  2. Plakt CSV-inhoud in het GitHub-secret TR_TRANSACTIONS_CSV
  3. Plakt actuele posities in het GitHub-secret TR_HOLDINGS
     (één positie per regel: ISIN_of_symbool  aantalAandelen)
  4. Dit module haalt actuele prijzen op via yfinance en berekent totaalwaarde

TR_HOLDINGS format (één positie per regel):
  BTC              0.0702790
  IE00BK5BQT80     5.1854850
  ...

TR_TRANSACTIONS_CSV: volledige inhoud van de TR CSV-export (plak alles incl. header).
"""
from __future__ import annotations

import csv
import io
import logging
import os
import time

import yfinance as yf

log = logging.getLogger("tr_portfolio")

# ─── ISIN → yfinance tickers (primair + fallbacks) ──────────────────────────
ISIN_TO_TICKERS: dict[str, list[str]] = {
    # Vanguard FTSE All-World UCITS ETF USD Acc
    "IE00BK5BQT80": ["VWCE.DE", "VWCE.MI", "VWRD.AS"],
    # Vanguard FTSE All-World High Dividend Yield UCITS ETF Dist
    "IE00BK5BR626": ["VHYL.AS", "VHYL.DE", "VGWD.DE"],
    # Amundi S&P 500 UCITS ETF EUR Acc (LU1681048804, ~€123/stuk)
    # Amundi staat niet op Yahoo Finance. Proxy: SXR8.DE (iShares Core S&P 500, Xetra EUR)
    # → prijs wordt geschaald via _scale_proxy hieronder.
    "LU1681048804": ["SXR8.DE"],
    # VanEck Morningstar Developed Markets Dividend Leaders
    "NL0011683594": ["TDIV.AS"],
    # Bitcoin — in EUR
    "BTC":          ["BTC-EUR"],
}

ISIN_TO_TICKER: dict[str, str] = {k: v[0] for k, v in ISIN_TO_TICKERS.items()}

# Proxy-schaling voor ISINs die geen eigen Yahoo Finance ticker hebben.
# ratio = eigen_NAV / proxy_NAV  →  pas aan als NAV sterk verschuift.
# SXR8.DE (iShares Core S&P 500 EUR, Xetra) ≈ €540
# Amundi S&P 500 (LU1681048804) ≈ €123  →  ratio = 123/540 ≈ 0.228
PROXY_RATIO: dict[str, float] = {
    "LU1681048804": 0.228,
}

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
        isin = parts[0].upper()
        try:
            shares = float(parts[1])
        except ValueError:
            log.warning(f"TR_HOLDINGS: ongeldige hoeveelheid voor {isin}: {parts[1]}")
            continue
        # Optionele derde kolom: handmatige gemiddelde aankoopprijs in EUR (fallback)
        avg_price = None
        if len(parts) >= 3:
            try:
                avg_price = float(parts[2])
            except ValueError:
                pass
        if shares > 0:
            holdings.append({"isin": isin, "shares": shares, "avg_price": avg_price})

    log.info(f"TR_HOLDINGS: {len(holdings)} posities geladen.")
    return holdings


def _parse_tr_transactions_csv() -> dict[str, float]:
    """
    Lees TR_TRANSACTIONS_CSV omgevingsvariabele (volledige inhoud van TR CSV-export).
    Berekent gewogen gemiddelde aankoopprijs per symbool op basis van transactiehistorie.

    Verwerkt:
    - BUY  (category=TRADING):   voegt aandelen + EUR-kosten toe (abs(amount))
    - FREE_RECEIPT (DELIVERY):   transfer van andere broker, kostenbasis = shares * price
    - SELL (category=TRADING):   vermindert aandelen + kosten proportioneel

    Geeft dict terug: symbool → gemiddelde_aankoopprijs_eur

    Let op: exporteer met datumbereik "Alle tijd" voor volledige kostenhistorie.
    """
    raw = os.environ.get("TR_TRANSACTIONS_CSV", "").strip()
    if not raw:
        log.info("TR_TRANSACTIONS_CSV niet ingesteld — geen automatische aankoopprijzen.")
        return {}

    acc: dict[str, dict] = {}  # symbol → {shares, cost_eur}

    try:
        reader = csv.DictReader(io.StringIO(raw))
        rows_processed = 0
        for row in reader:
            category = (row.get("category") or "").strip()
            tx_type  = (row.get("type")     or "").strip()
            symbol   = (row.get("symbol")   or "").strip().upper()

            if not symbol:
                continue

            shares_str = (row.get("shares") or "").strip()
            price_str  = (row.get("price")  or "").strip()
            amount_str = (row.get("amount") or "").strip()

            if not shares_str:
                continue

            try:
                qty = float(shares_str)
            except ValueError:
                continue

            if symbol not in acc:
                acc[symbol] = {"shares": 0.0, "cost_eur": 0.0}

            h = acc[symbol]

            if category == "TRADING" and tx_type == "BUY":
                # amount is negatief in de export (bijv. "-299.96")
                if amount_str:
                    try:
                        cost = abs(float(amount_str))
                    except ValueError:
                        cost = qty * float(price_str) if price_str else 0.0
                elif price_str:
                    cost = qty * float(price_str)
                else:
                    continue
                h["shares"]   += qty
                h["cost_eur"] += cost
                rows_processed += 1

            elif category == "DELIVERY" and tx_type == "FREE_RECEIPT":
                # Ontvangst van andere broker: gebruik marktprijs als kostenbasis
                if price_str:
                    try:
                        price = float(price_str)
                        if price > 0:
                            h["shares"]   += qty
                            h["cost_eur"] += qty * price
                            rows_processed += 1
                    except ValueError:
                        pass

            elif category == "TRADING" and tx_type == "SELL":
                if h["shares"] > 0 and qty > 0:
                    ratio = min(qty / h["shares"], 1.0)
                    h["cost_eur"] *= (1 - ratio)
                    h["shares"]   = max(0.0, h["shares"] - qty)
                    rows_processed += 1

        log.info(f"TR CSV: {rows_processed} transactieregels verwerkt voor {len(acc)} symbolen.")

    except Exception as e:
        log.warning(f"TR CSV parsing fout: {e}")
        return {}

    # Bereken gemiddelde aankoopprijs per symbool
    result: dict[str, float] = {}
    for symbol, h in acc.items():
        if h["shares"] > 0.0001 and h["cost_eur"] > 0:
            avg = round(h["cost_eur"] / h["shares"], 4)
            result[symbol] = avg
            log.info(
                f"TR CSV avg: {symbol} → €{avg:.4f}/stuk "
                f"({h['shares']:.6f} aandelen, €{h['cost_eur']:.2f} totaal)"
            )

    return result


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
                price = float(price)
                # Schaal proxy-prijs naar werkelijke ETF NAV indien nodig
                if isin in PROXY_RATIO:
                    price = round(price * PROXY_RATIO[isin], 4)
                    log.info(f"{isin} → {ticker} (proxy×{PROXY_RATIO[isin]}): €{price:.2f}")
                else:
                    log.info(f"{isin} → {ticker}: €{price:.2f}")
                return ticker, price
        except Exception as e:
            log.debug(f"yfinance {ticker}: {e}")
        time.sleep(0.2)
    log.warning(f"{isin}: geen prijs gevonden (geprobeerd: {tickers})")
    return None, None


def fetch_tr_portfolio() -> dict | None:
    """
    Bouw Trade Republic portfolio op vanuit TR_HOLDINGS + yfinance prijzen.
    Gemiddelde aankoopprijzen komen uit TR_TRANSACTIONS_CSV (automatisch berekend)
    of als fallback uit de handmatige 3e kolom in TR_HOLDINGS.
    Geeft None terug als TR_HOLDINGS niet beschikbaar is.
    """
    holdings = _parse_tr_holdings()
    if not holdings:
        return None

    # Laad gemiddelde aankoopprijzen uit CSV-transactiehistorie
    csv_avg = _parse_tr_transactions_csv()

    positions = []
    total     = 0.0

    for h in holdings:
        isin   = h["isin"]
        shares = h["shares"]
        ticker, price = _fetch_price_eur(isin)

        if price is None or ticker is None:
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

        value  = shares * price
        total += value

        # Prioriteit: CSV-berekend gemiddelde > handmatig in TR_HOLDINGS
        avg_price = csv_avg.get(isin) or h.get("avg_price")

        if avg_price and avg_price > 0:
            cost_eur = shares * avg_price
            pl_pct   = round((price / avg_price - 1) * 100, 2)
            pl_eur   = round(value - cost_eur, 2)
        else:
            cost_eur = None
            pl_pct   = None
            pl_eur   = None

        positions.append({
            "name":      DISPLAY_NAMES.get(isin, isin),
            "ticker":    ticker,
            "isin":      isin,
            "size":      round(shares, 6),
            "price":     round(price, 4),
            "value":     round(value, 2),
            "avg_price": avg_price,
            "cost_eur":  round(cost_eur, 2) if cost_eur else None,
            "pl_pct":    pl_pct,
            "pl_eur":    pl_eur,
            "avg_source": "csv" if isin in csv_avg else ("manual" if h.get("avg_price") else None),
        })
        time.sleep(0.1)

    if not positions:
        log.warning("TR portfolio: geen posities met prijs.")
        return None

    positions.sort(key=lambda p: p["value"], reverse=True)

    total_invested = sum(p["cost_eur"] for p in positions if p.get("cost_eur"))
    total_pl_pct   = round((total / total_invested - 1) * 100, 2) if total_invested > 0 else None

    log.info(f"Trade Republic: {len(positions)} posities, totaal €{total:.2f}, P&L {total_pl_pct}%")
    return {
        "positions":      positions,
        "total":          round(total, 2),
        "total_invested": round(total_invested, 2) if total_invested else None,
        "total_pl_pct":   total_pl_pct,
    }
