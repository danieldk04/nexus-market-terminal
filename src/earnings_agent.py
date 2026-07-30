"""
NEXUS Earnings Agent — Kwartaalcijfers kalender + resultaten

Bouwt twee dingen op voor het dashboard:

  1. KALENDER  — wanneer komen de belangrijkste bedrijven met cijfers?
                 Datum, bevestigd/geschat, EPS- en omzetverwachting,
                 historische beat-rate en of jij het bedrijf bezit.

  2. RESULTATEN — wat kwam er uit de laatste rapportage?
                 EPS actueel vs. verwacht (surprise %), omzet + YoY-groei,
                 EPS YoY, koersreactie op de dag na publicatie en een
                 rule-based oordeel (BEAT / INLINE / MISS).

Optioneel schrijft Claude er een korte seizoenssamenvatting bij
(alleen als ANTHROPIC_API_KEY beschikbaar is — zonder key draait de rest
gewoon door).

Resultaat: data.json["earnings"] = {generated_at, calendar, reported, stats, ai_summary}
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("earnings_agent")

BASE_DIR    = Path(__file__).parent.parent
DATA_PATH   = BASE_DIR / "data.json"
MEMORY_PATH = BASE_DIR / "memory.json"

CALENDAR_HORIZON_DAYS = 75    # Hoe ver vooruit de kalender kijkt
REPORTED_LOOKBACK_DAYS = 45   # Hoe ver terug "recent gerapporteerd" loopt
MAX_TICKERS = 70              # Rem op de yfinance-calls per run
TOP_CANDIDATES = 15           # Aantal scanner-kandidaten dat meegaat

# ─── Kern-universum: de indexzwaargewichten die de markt sturen ──────────────
CORE_TICKERS = [
    # US mega-cap tech
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO",
    "ORCL", "AMD", "CRM", "ADBE", "NFLX", "PLTR",
    # US financials / healthcare / consumer / industrials
    "JPM", "BAC", "V", "MA", "BRK-B", "GS",
    "UNH", "LLY", "JNJ", "ABBV", "MRK",
    "WMT", "COST", "PG", "KO", "PEP", "MCD",
    "XOM", "CVX", "CAT", "GE", "BA", "HON",
    # EU zwaargewichten
    "ASML.AS", "ADYEN.AS", "INGA.AS", "UNA.AS", "PHIA.AS", "HEIA.AS",
    "SAP.DE", "SIE.DE", "ALV.DE", "MC.PA", "OR.PA", "AIR.PA",
    "AZN.L", "SHEL.L", "HSBA.L", "NOVO-B.CO",
]

# Namen/tickers die geen kwartaalcijfers hebben (ETF's, crypto, cash)
SKIP_TOKENS = (
    "BITCOIN", "ETHEREUM", "CRYPTO", "CASH", "GOUD", "GOLD", "SILVER",
    "ETF", "INDEX", "TRACKER", "OBLIGATIE", "BOND",
    "VANGUARD", "ISHARES", "AMUNDI", "XTRACKERS", "SPDR", "INVESCO",
)
_DATES_WARNED = False   # zie _from_earnings_dates: waarschuw hooguit één keer

SKIP_TICKERS = {
    "VWRL.AS", "VUSA.AS", "VHYL.AS", "VWCE.DE", "VWCE", "VHYL", "IWDA.AS",
    "TDIV.AS", "TDIV", "CSP1", "CSPX.AS", "SPY", "QQQ", "VOO",
    "IEMA.AS", "EUNL.DE",
}


# ── helpers ──────────────────────────────────────────────────────────────────

def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def _num(v):
    """Maak een yfinance/pandas-waarde JSON-veilig (NaN → None)."""
    if v is None:
        return None
    try:
        if isinstance(v, str):
            return None
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):   # NaN / inf
        return None
    return f


def _round(v, dec=2):
    n = _num(v)
    return None if n is None else round(n, dec)


def _to_date(v):
    """Normaliseer datetime / date / Timestamp / str naar een date."""
    if v is None:
        return None
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v[:10]).date()
        except ValueError:
            return None
    if isinstance(v, pd.Timestamp):
        return v.date()
    if isinstance(v, datetime):
        return v.date()
    if hasattr(v, "year") and hasattr(v, "month"):
        return v
    return None


def looks_like_fund(ticker: str, name: str = "") -> bool:
    """ETF's, crypto en cash hebben geen kwartaalcijfers — die slaan we over."""
    if ticker.upper() in SKIP_TICKERS:
        return True
    haystack = f"{ticker} {name}".upper()
    return any(tok in haystack for tok in SKIP_TOKENS)


# ── universum: welke bedrijven zijn "belangrijk" voor deze gebruiker? ─────────

def build_universe(data: dict, memory: dict) -> list[dict]:
    """
    Stelt de lijst bedrijven samen die op de kalender horen, in volgorde
    van relevantie: eigen posities → bot-posities → scanner-kandidaten →
    watchlist → index-zwaargewichten.
    Retourneert [{ticker, name, tags}] met tags als ['portfolio', 'kandidaat'].
    """
    order: list[str] = []
    meta: dict[str, dict] = {}

    def add(ticker, name=None, tag=None):
        if not ticker:
            return
        tk = str(ticker).strip().upper()
        if not tk or looks_like_fund(tk, name or ""):
            return
        if tk not in meta:
            meta[tk] = {"ticker": tk, "name": name or tk, "tags": []}
            order.append(tk)
        if name and meta[tk]["name"] == tk:
            meta[tk]["name"] = name
        if tag and tag not in meta[tk]["tags"]:
            meta[tk]["tags"].append(tag)

    # 1. Eigen brokerposities (DEGIRO / Trade Republic / BUX)
    for key in ("degiro_summary", "tr_summary", "bux_summary"):
        for p in (memory.get(key) or {}).get("positions", []) or []:
            name   = p.get("name") or ""
            ticker = p.get("ticker") or (name if " " not in name else None)
            add(ticker, name, "portfolio")

    # 2. Open posities van de bot
    for t in data.get("active_trades", []) or []:
        add(t.get("ticker"), t.get("name"), "bot")

    # 3. Scanner-kandidaten (hoogste convergence eerst — data.json is al gesorteerd)
    for c in (data.get("top_candidates") or [])[:TOP_CANDIDATES]:
        add(c.get("ticker"), c.get("name"), "kandidaat")

    # 4. Watchlist
    for w in data.get("watchlist", []) or []:
        add(w.get("ticker"), w.get("name"), "watchlist")
    for tk in memory.get("watch_list", []) or []:
        add(tk, None, "watchlist")

    # 5. Index-zwaargewichten — de cijfers die de hele markt bewegen
    for tk in CORE_TICKERS:
        add(tk, None, "mega-cap")

    return [meta[tk] for tk in order][:MAX_TICKERS]


# ── yfinance ophalen ─────────────────────────────────────────────────────────

def _next_earnings_date(t, info: dict) -> tuple:
    """(date, is_estimate, eps_estimate, revenue_estimate) voor de eerstvolgende rapportage."""
    eps_est = rev_est = None
    is_estimate = bool(info.get("isEarningsDateEstimate", False))
    nxt = None

    try:
        cal = t.calendar
    except Exception:
        cal = None

    if isinstance(cal, dict):
        raw = cal.get("Earnings Date")
        if isinstance(raw, (list, tuple)):
            dates = [d for d in (_to_date(x) for x in raw) if d]
            nxt = min(dates) if dates else None
        else:
            nxt = _to_date(raw)
        eps_est = _round(cal.get("Earnings Average"), 3)
        rev_est = _num(cal.get("Revenue Average"))
    elif isinstance(cal, pd.DataFrame) and not cal.empty:
        # Oudere yfinance-versies leveren een DataFrame met kolom per datum
        try:
            row = cal.loc["Earnings Date"] if "Earnings Date" in cal.index else None
            if row is not None:
                dates = [d for d in (_to_date(x) for x in list(row)) if d]
                nxt = min(dates) if dates else None
        except Exception:
            pass

    if nxt is None:
        ts = info.get("earningsTimestampStart") or info.get("earningsTimestamp")
        if ts:
            try:
                nxt = datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
            except Exception:
                nxt = None

    if eps_est is None:
        eps_est = _round(info.get("epsForward"), 3)

    return nxt, is_estimate, eps_est, rev_est


def _mk_row(d, actual, estimate, surprise, is_report_date: bool) -> dict:
    if surprise is not None and abs(surprise) <= 1.5 and actual and estimate:
        # yfinance levert soms een fractie (0.05) i.p.v. procenten (5.0)
        surprise = surprise * 100
    if surprise is None and actual is not None and estimate:
        surprise = (actual - estimate) / abs(estimate) * 100
    return {
        "date":           d.isoformat() if d else None,
        "eps_actual":     _round(actual, 3),
        "eps_estimate":   _round(estimate, 3),
        "surprise_pct":   _round(surprise, 1),
        "is_report_date": is_report_date,
    }


def _from_earnings_dates(t) -> list[dict]:
    """
    Historie op basis van de échte rapportagedatum.
    Ticker.earnings_dates is geïndexeerd op het moment van publicatie — dat is
    wat we nodig hebben om de koersreactie te kunnen meten.
    """
    df = None
    try:
        getter = getattr(t, "get_earnings_dates", None)
        df = getter(limit=24) if getter else t.earnings_dates
    except Exception as e:
        # Eén keer luidruchtig: als deze bron structureel wegvalt (bijv. lxml
        # ontbreekt, want yfinance scrapet dit via pandas.read_html) verliezen
        # we alle rapportagedatums en dus de koersreacties — dat mag niet
        # stilletjes gebeuren.
        global _DATES_WARNED
        if not _DATES_WARNED:
            _DATES_WARNED = True
            log.warning("earnings_dates niet beschikbaar (%s: %s) — terugval op "
                        "kwartaaleinde, zónder koersreactie.", type(e).__name__, e)
        return []
    if df is None or not hasattr(df, "empty") or df.empty:
        return []

    def pick(r, *names):
        for n in names:
            if n in r.index:
                v = _num(r.get(n))
                if v is not None:
                    return v
        return None

    rows = []
    for idx, r in df.iterrows():
        d = _to_date(idx)
        actual = pick(r, "Reported EPS", "reportedEPS", "epsActual")
        if d is None or actual is None:
            continue   # toekomstige of lege rijen
        rows.append(_mk_row(
            d, actual,
            pick(r, "EPS Estimate", "epsEstimate"),
            pick(r, "Surprise(%)", "surprisePercent"),
            True,
        ))
    rows.sort(key=lambda r: r["date"])
    return rows


def _from_earnings_history(t) -> list[dict]:
    """
    Terugval: Ticker.earnings_history is geïndexeerd op kwartaaleinde, niet op
    de publicatiedatum. Bruikbaar voor EPS en surprise, maar niet om er een
    koersreactie aan te hangen — vandaar is_report_date=False.
    """
    try:
        hist = t.earnings_history
    except Exception:
        return []
    if hist is None or not hasattr(hist, "empty") or hist.empty:
        return []

    rows = []
    for idx, r in hist.iterrows():
        d = _to_date(idx) or _to_date(r.get("quarter") if hasattr(r, "get") else None)
        if d is None:
            continue
        rows.append(_mk_row(
            d, _num(r.get("epsActual")), _num(r.get("epsEstimate")),
            _num(r.get("surprisePercent")), False,
        ))
    rows.sort(key=lambda r: r["date"])
    return rows


def _earnings_history(t) -> list[dict]:
    """Laatste kwartaalrapportages, oud→nieuw. Bij voorkeur op rapportagedatum."""
    return _from_earnings_dates(t) or _from_earnings_history(t)


def _beat_stats(history: list[dict]) -> tuple:
    """(beat_rate %, gemiddelde surprise %) over de laatste 8 kwartalen."""
    recent = [r for r in history if r["surprise_pct"] is not None][-8:]
    if not recent:
        return None, None
    beats = sum(1 for r in recent if r["surprise_pct"] > 0)
    avg   = sum(r["surprise_pct"] for r in recent) / len(recent)
    return int(round(beats / len(recent) * 100)), round(avg, 1)


def _revenue_snapshot(t) -> tuple:
    """(omzet laatste kwartaal, YoY-groei %) uit de kwartaalwinst-en-verliesrekening."""
    try:
        stmt = t.quarterly_income_stmt
    except Exception:
        return None, None
    if stmt is None or not hasattr(stmt, "empty") or stmt.empty:
        return None, None

    row = None
    for label in ("Total Revenue", "Operating Revenue", "TotalRevenue"):
        if label in stmt.index:
            row = stmt.loc[label]
            break
    if row is None:
        return None, None

    try:
        # Kolommen zijn kwartaaleinddatums, nieuwste eerst
        cols = list(row.index)
        latest = _num(row.iloc[0])
        yoy = None
        if len(cols) >= 5:
            year_ago = _num(row.iloc[4])
            if latest is not None and year_ago:
                yoy = (latest / year_ago - 1) * 100
        return latest, _round(yoy, 1)
    except Exception:
        return None, None


def _price_reaction(t, report_date) -> float | None:
    """Koersreactie (%) op de eerste handelsdag ná publicatie van de cijfers."""
    if not report_date:
        return None
    try:
        start = report_date - timedelta(days=10)
        end   = report_date + timedelta(days=10)
        hist  = t.history(start=start.isoformat(), end=end.isoformat())
    except Exception:
        return None
    if hist is None or hist.empty or "Close" not in hist.columns:
        return None

    closes = hist["Close"].dropna()
    if len(closes) < 2:
        return None

    for i, idx in enumerate(closes.index):
        d = _to_date(idx)
        if d and d > report_date and i > 0:
            prev, cur = _num(closes.iloc[i - 1]), _num(closes.iloc[i])
            if prev and cur:
                return round((cur / prev - 1) * 100, 1)
            return None
    return None


def _verdict(surprise_pct, revenue_yoy, reaction_pct) -> tuple:
    """Rule-based oordeel + Nederlandse samenvatting van één kwartaal."""
    if surprise_pct is None:
        label = "GEEN DATA"
    elif surprise_pct >= 2:
        label = "BEAT"
    elif surprise_pct <= -2:
        label = "MISS"
    else:
        label = "INLINE"

    parts = []
    if surprise_pct is not None:
        if label == "BEAT":
            parts.append(f"EPS {surprise_pct:+.1f}% boven verwachting")
        elif label == "MISS":
            parts.append(f"EPS {surprise_pct:+.1f}% onder verwachting")
        else:
            parts.append(f"EPS in lijn ({surprise_pct:+.1f}%)")
    if revenue_yoy is not None:
        trend = "omzetgroei" if revenue_yoy >= 0 else "omzetkrimp"
        parts.append(f"{trend} {revenue_yoy:+.1f}% j-o-j")
    if reaction_pct is not None:
        mood = "beloond" if reaction_pct > 0 else "afgestraft"
        parts.append(f"markt {mood} ({reaction_pct:+.1f}% dag erna)")

    return label, ", ".join(parts).capitalize() if parts else "Geen cijferdetails beschikbaar."


def analyse_ticker(entry: dict, today) -> dict | None:
    """Haal kalender + laatste kwartaalresultaat op voor één bedrijf."""
    ticker = entry["ticker"]
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}
    except Exception as e:
        log.warning("%s: info ophalen mislukt (%s)", ticker, e)
        return None

    if not info.get("shortName") and not info.get("longName"):
        return None
    # Fondsen/ETF's die niet op naam herkend werden alsnog eruit filteren
    if str(info.get("quoteType", "")).upper() in ("ETF", "MUTUALFUND", "CRYPTOCURRENCY", "INDEX"):
        return None

    name = entry["name"] if entry["name"] != ticker else (
        info.get("shortName") or info.get("longName") or ticker
    )

    nxt, is_estimate, eps_est, rev_est = _next_earnings_date(t, info)
    history = _earnings_history(t)
    beat_rate, avg_surprise = _beat_stats(history)

    record = {
        "ticker":        ticker,
        "name":          name,
        "sector":        info.get("sector"),
        "tags":          entry["tags"],
        "price":         _round(info.get("currentPrice") or info.get("regularMarketPrice")),
        "currency":      info.get("currency"),
        "market_cap":    _num(info.get("marketCap")),
        "beat_rate":     beat_rate,
        "avg_surprise":  avg_surprise,
        "next_date":     nxt.isoformat() if nxt else None,
        "days_until":    (nxt - today).days if nxt else None,
        "date_estimate": is_estimate,
        "eps_estimate":  eps_est,
        "rev_estimate":  _num(rev_est),
        "last":          None,
    }

    # Laatste gerapporteerde kwartaal — alleen ophalen als het recent genoeg is
    last_idx = next(
        (i for i in range(len(history) - 1, -1, -1) if history[i]["eps_actual"] is not None),
        None,
    )
    if last_idx is not None:
        last = history[last_idx]
        last_date = _to_date(last["date"])
        age = (today - last_date).days if last_date else 9999
        eps_yoy = None
        if last_idx >= 4:
            prev = history[last_idx - 4].get("eps_actual")
            if prev and prev > 0:
                eps_yoy = round((last["eps_actual"] / prev - 1) * 100, 1)

        revenue = revenue_yoy = reaction = None
        if age <= REPORTED_LOOKBACK_DAYS:
            revenue, revenue_yoy = _revenue_snapshot(t)
            # Alleen zinvol als we de échte publicatiedatum hebben; bij een
            # kwartaaleinde-datum zouden we een willekeurige dag meten.
            if last.get("is_report_date"):
                reaction = _price_reaction(t, last_date)

        verdict, summary = _verdict(last["surprise_pct"], revenue_yoy, reaction)
        record["last"] = {
            **last,
            "days_ago":     age if age < 9999 else None,
            "eps_yoy":      eps_yoy,
            "revenue":      _num(revenue),
            "revenue_yoy":  revenue_yoy,
            "reaction_pct": reaction,
            "verdict":      verdict,
            "summary":      summary,
        }

    record["history"] = history[-8:]
    return record


# ── AI-seizoenssamenvatting (optioneel) ──────────────────────────────────────

SYSTEM_PROMPT = (
    "Je bent een senior aandelenanalist die kwartaalcijfers duidt voor een "
    "particuliere belegger. Je schrijft kort, concreet en in het Nederlands. "
    "Geen disclaimers, geen algemeenheden — alleen wat er echt uit de cijfers volgt."
)


def build_ai_summary(reported: list[dict], upcoming: list[dict]) -> dict | None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.info("Geen ANTHROPIC_API_KEY — AI-samenvatting overgeslagen.")
        return None
    if not reported and not upcoming:
        return None

    try:
        import anthropic
    except ImportError:
        log.info("anthropic-package niet beschikbaar — AI-samenvatting overgeslagen.")
        return None

    rep_lines = []
    for r in reported[:12]:
        last = r["last"]
        rep_lines.append(
            f"• {r['ticker']} ({r['name']}) — {last['date']}: {last['verdict']}, "
            f"EPS {last.get('eps_actual')} vs verwacht {last.get('eps_estimate')} "
            f"(surprise {last.get('surprise_pct')}%), omzet YoY {last.get('revenue_yoy')}%, "
            f"koersreactie {last.get('reaction_pct')}%, beat-rate {r.get('beat_rate')}%"
        )
    up_lines = [
        f"• {u['ticker']} ({u['name']}) — {u['next_date']} "
        f"(over {u['days_until']} dagen), EPS-verwachting {u.get('eps_estimate')}, "
        f"beat-rate {u.get('beat_rate')}%"
        for u in upcoming[:12]
    ]

    prompt = (
        "Hieronder de kwartaalcijfers van de bedrijven die er voor deze belegger toe doen "
        "(eigen posities, botposities, scannerkandidaten en indexzwaargewichten).\n\n"
        f"NET GERAPPORTEERD:\n{chr(10).join(rep_lines) or 'Niets recent gerapporteerd.'}\n\n"
        f"KOMENDE RAPPORTAGES:\n{chr(10).join(up_lines) or 'Niets ingepland.'}\n\n"
        "Schrijf maximaal 180 woorden in exact deze structuur:\n\n"
        "## Wat opviel\n"
        "[2-3 bullets over het patroon in de gerapporteerde cijfers — wie verraste positief/negatief "
        "en of de koersreactie daarbij paste]\n\n"
        "## Om op te letten\n"
        "[2-3 bullets over de komende rapportages die er het meest toe doen en waarom]"
    )

    try:
        client  = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return {
            "text":         message.content[0].text,
            "model":        "claude-sonnet-4-6",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        log.warning("AI-samenvatting mislukt: %s", e)
        return None


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== NEXUS EARNINGS AGENT STARTING ===")

    data   = load_json(DATA_PATH, {})
    memory = load_json(MEMORY_PATH, {})
    if not data:
        log.error("data.json niet gevonden of leeg — stoppen.")
        return

    today    = datetime.now(timezone.utc).date()
    universe = build_universe(data, memory)
    log.info("Universum: %d bedrijven", len(universe))

    records = []
    for i, entry in enumerate(universe, 1):
        try:
            rec = analyse_ticker(entry, today)
        except Exception as e:
            log.warning("%s: overgeslagen (%s)", entry["ticker"], e)
            rec = None
        if rec:
            records.append(rec)
            log.info(
                "  [%2d/%d] %-10s cijfers: %s | laatste: %s",
                i, len(universe), rec["ticker"],
                rec["next_date"] or "onbekend",
                (rec["last"] or {}).get("verdict", "—"),
            )
        time.sleep(0.15)   # Respecteer Yahoo rate limits

    # Kalender: alles wat nog moet komen binnen de horizon. Een datum in het
    # verleden betekent dat Yahoo de volgende rapportage nog niet ingepland
    # heeft — dat bedrijf staat al bij de resultaten en hoort hier niet.
    calendar = sorted(
        [
            r for r in records
            if r["days_until"] is not None and 0 <= r["days_until"] <= CALENDAR_HORIZON_DAYS
        ],
        key=lambda r: (r["days_until"], -(r["market_cap"] or 0)),
    )

    # Resultaten: recent gerapporteerd, nieuwste eerst
    reported = sorted(
        [
            r for r in records
            if r["last"] and (r["last"].get("days_ago") or 9999) <= REPORTED_LOOKBACK_DAYS
        ],
        key=lambda r: r["last"]["date"],
        reverse=True,
    )

    surprises = [r["last"]["surprise_pct"] for r in reported if r["last"]["surprise_pct"] is not None]
    stats = {
        "tracked":       len(records),
        "upcoming_7d":   sum(1 for r in calendar if 0 <= (r["days_until"] or 99) <= 7),
        "upcoming_30d":  sum(1 for r in calendar if 0 <= (r["days_until"] or 99) <= 30),
        "reported":      len(reported),
        "beats":         sum(1 for r in reported if r["last"]["verdict"] == "BEAT"),
        "misses":        sum(1 for r in reported if r["last"]["verdict"] == "MISS"),
        "inline":        sum(1 for r in reported if r["last"]["verdict"] == "INLINE"),
        "avg_surprise":  round(sum(surprises) / len(surprises), 1) if surprises else None,
    }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "horizon_days": CALENDAR_HORIZON_DAYS,
        "stats":        stats,
        "calendar":     calendar,
        "reported":     reported,
    }

    ai = build_ai_summary(reported, calendar)
    if ai:
        payload["ai_summary"] = ai
    else:
        # Bewaar een eerdere samenvatting als deze run er geen kon maken
        prev = (data.get("earnings") or {}).get("ai_summary")
        if prev:
            payload["ai_summary"] = prev

    data["earnings"]    = payload
    data["earnings_at"] = payload["generated_at"]

    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=4)

    log.info(
        "Klaar — %d bedrijven gevolgd, %d op de kalender, %d recent gerapporteerd "
        "(%d beats / %d misses).",
        stats["tracked"], len(calendar), len(reported), stats["beats"], stats["misses"],
    )
    log.info("=== EARNINGS AGENT COMPLETE ===")


if __name__ == "__main__":
    main()
