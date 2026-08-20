"""
ETF-BACKTEST — heeft de selectiemethode in het verleden gewerkt?

Belangrijke beperking, eerlijk vooraf:
  De kwaliteitsscore is NIET terug in de tijd te reconstrueren. Yahoo geeft de
  cijfers van vandaag, niet die van drie jaar geleden. Een backtest die de
  kwaliteitsscore van nu op het verleden loslaat kijkt vooruit en levert een
  veel te mooi resultaat op ("lookahead bias").

  Wat wél zuiver te toetsen is, is de MOMENTUM-kant: die volgt uitsluitend uit
  koersen tot dat moment. Deze backtest doet dus twee dingen:

  1. Momentum-strategie, zuiver: koop elke maand de top-N ETF's op 12-1
     momentum, houd een maand vast, vergelijk met MSCI World.
  2. Kwaliteitsfilter, mét waarschuwing: dezelfde test maar alleen binnen de
     ETF's die vandaag hoog scoren op kwaliteit. Dit ís besmet met lookahead —
     het laat zien of het kwaliteitsfilter richting geeft, niet wat je had
     verdiend.

Uitvoer: het blok "backtest" in etf_data.json.
"""
from __future__ import annotations

import json
import logging
import os
import sys

import numpy as np
import pandas as pd
import yfinance as yf

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from etf_universe import UNIVERSE  # noqa: E402

log = logging.getLogger("etf_backtest")

BENCHMARK = "EUNL.DE"       # iShares Core MSCI World
TOP_N = 8
LOOKBACK_YEARS = 5


def _monthly_closes(tickers: list[str]) -> pd.DataFrame:
    raw = yf.download(tickers, period=f"{LOOKBACK_YEARS}y", interval="1d",
                      auto_adjust=True, progress=False, threads=True)
    close = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
    return close.resample("ME").last().dropna(how="all")


def _momentum(m: pd.DataFrame, i: int) -> pd.Series:
    """12-1 momentum op maand i: rendement over 12 maanden minus de laatste."""
    if i < 13:
        return pd.Series(dtype=float)
    r12 = m.iloc[i - 1] / m.iloc[i - 13] - 1
    return r12.dropna()


def run(quality: dict[str, float] | None = None) -> dict:
    tickers = list(UNIVERSE)
    m = _monthly_closes(tickers)
    m = m.dropna(axis=1, thresh=int(len(m) * 0.7))
    if BENCHMARK not in m.columns or len(m) < 20:
        return {"fout": "Te weinig historie voor een backtest."}

    # Kwaliteitsfilter: de bovenste helft van vandaag (bevat lookahead-bias)
    kwal = set()
    if quality:
        beschikbaar = [t for t in m.columns if quality.get(t) is not None]
        mediaan = np.median([quality[t] for t in beschikbaar]) if beschikbaar else 0
        kwal = {t for t in beschikbaar if quality[t] >= mediaan}

    curves = {"Momentum (zuiver)": [1.0], "Momentum + kwaliteit": [1.0],
              "MSCI World": [1.0]}
    keuzes = []

    for i in range(13, len(m) - 1):
        mom = _momentum(m, i)
        nxt = (m.iloc[i + 1] / m.iloc[i] - 1).dropna()

        def maandrendement(pool):
            picks = [t for t in mom.sort_values(ascending=False).index if t in pool][:TOP_N]
            rets = [nxt[t] for t in picks if t in nxt.index and np.isfinite(nxt[t])]
            return (float(np.mean(rets)) if rets else 0.0), picks

        r_pure, picks = maandrendement(set(m.columns))
        r_qual, qpicks = maandrendement(kwal or set(m.columns))
        r_bench = float(nxt.get(BENCHMARK, 0.0) or 0.0)

        curves["Momentum (zuiver)"].append(curves["Momentum (zuiver)"][-1] * (1 + r_pure))
        curves["Momentum + kwaliteit"].append(curves["Momentum + kwaliteit"][-1] * (1 + r_qual))
        curves["MSCI World"].append(curves["MSCI World"][-1] * (1 + r_bench))
        keuzes.append({"maand": str(m.index[i].date()), "picks": picks[:5],
                       "rendement": round(r_pure, 4)})

    def stats(curve: list[float]) -> dict:
        arr = np.array(curve)
        jaren = len(arr) / 12
        totaal = arr[-1] - 1
        maand = np.diff(arr) / arr[:-1]
        return {
            "totaal": round(float(totaal), 4),
            "per_jaar": round(float((arr[-1]) ** (1 / jaren) - 1), 4) if jaren else None,
            "max_drawdown": round(float((arr / np.maximum.accumulate(arr) - 1).min()), 4),
            "beste_maand": round(float(maand.max()), 4),
            "slechtste_maand": round(float(maand.min()), 4),
            "curve": [round(float(x), 3) for x in arr],
        }

    maanden = [str(d.date()) for d in m.index[13:len(m)]]
    return {
        "periode": {"van": maanden[0], "tot": maanden[-1], "maanden": len(maanden)},
        "top_n": TOP_N,
        "strategieen": {k: stats(v) for k, v in curves.items()},
        "labels": maanden,
        "laatste_keuzes": keuzes[-6:],
        "waarschuwing": ("De zuivere momentumtest gebruikt alleen koersen tot dat "
                         "moment en is eerlijk. De variant met kwaliteitsfilter "
                         "gebruikt de kwaliteitsscores van vandaag en kijkt dus "
                         "vooruit — die uitkomst is optimistischer dan de "
                         "werkelijkheid."),
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    with open("etf_data.json", encoding="utf-8") as fh:
        data = json.load(fh)
    quality = {e["ticker"]: e.get("kwaliteit") for e in data["etfs"]}

    data["backtest"] = run(quality)
    with open("etf_data.json", "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, separators=(",", ":"))

    st = data["backtest"].get("strategieen", {})
    for naam, s in st.items():
        log.info("%-22s %6.1f%% totaal · %5.1f%% p.j. · dip %5.1f%%",
                 naam, s["totaal"] * 100, s["per_jaar"] * 100, s["max_drawdown"] * 100)


if __name__ == "__main__":
    main()
