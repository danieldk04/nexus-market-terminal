"""
NEXUS Morning Briefing — Dagelijkse marktupdate (07:00 UTC)
─────────────────────────────────────────────────────────────
Marktdata   : yfinance — AEX, S&P 500, NASDAQ, BTC, goud
Nieuws      : Google News RSS — actueel, Nederlandstalig, gratis
Portfolio   : DEGIRO REST + Trade Republic via TR_HOLDINGS secret
Snapshots   : dagelijkse opslag in memory.json → dag/week/maand/YTD
AI          : Claude Haiku marktbrief
Output      : Telegram
"""
import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

import anthropic
import requests
import yfinance as yf

from notifier import send, DASHBOARD_URL
from tr_portfolio import fetch_tr_portfolio

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("morning_briefing")

BASE_DIR     = Path(__file__).parent.parent
DATA_PATH    = BASE_DIR / "data.json"
MEMORY_PATH  = BASE_DIR / "memory.json"
HISTORY_KEY  = "portfolio_history"
MAX_HISTORY  = 400   # dagen bewaren (~13 maanden)

INDICES = [
    ("S&P 500", "^GSPC"), ("NASDAQ",  "^IXIC"),
    ("AEX",     "^AEX"),  ("DAX",     "^GDAXI"),
    ("BTC/USD", "BTC-USD"),("Goud",   "GC=F"),
]

DEGIRO_LOGIN_URL   = "https://trader.degiro.nl/login/secure/login"
DEGIRO_CONFIG_URL  = "https://trader.degiro.nl/pa/secure/client"
DEGIRO_PORT_URL    = "https://trader.degiro.nl/trading/secure/v5/update/{int_account}"
DEGIRO_PROD_URL    = "https://trader.degiro.nl/product_search/secure/v5/products/info"
DEGIRO_TRANS_URL   = "https://trader.degiro.nl/reporting/secure/v4/transactions"


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _arrow(pct: float) -> str:
    if pct >= 1.5:  return "🚀"
    if pct >= 0.5:  return "📈"
    if pct >= 0.0:  return "🔼"
    if pct >= -0.5: return "🔽"
    if pct >= -1.5: return "📉"
    return "💥"


def _pct_str(pct: float | None) -> str:
    if pct is None: return "n/b"
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def _load_json(path: Path, default):
    if not path.exists(): return default
    try:
        with open(path) as f: return json.load(f)
    except Exception: return default


def _save_json(path: Path, data):
    with open(path, "w") as f: json.dump(data, f, indent=2)


# ─── PORTFOLIO HISTORY (dag/week/maand/YTD) ───────────────────────────────────

def load_history() -> list[dict]:
    mem = _load_json(MEMORY_PATH, {})
    return mem.get(HISTORY_KEY, [])


def save_dashboard_data(news: list[str], degiro: dict | None, tr: dict | None,
                        news_summary: list[dict] | None = None,
                        bux: dict | None = None,
                        investment_timeline: list[dict] | None = None,
                        first_investment_date: str | None = None,
                        portfolio_value_history: list[dict] | None = None):
    """Sla nieuws + portfolio-samenvatting op in memory.json voor het dashboard."""
    mem = _load_json(MEMORY_PATH, {})
    mem["last_news"] = news[:8]
    if news_summary:
        mem["news_summary"] = news_summary
    if bux:
        mem["bux_summary"] = {
            "total":          bux.get("total"),
            "total_pl_pct":   bux.get("total_pl_pct"),
            "total_invested": bux.get("total_invested"),
            "positions": [
                {
                    "name":   p.get("name", "?"),
                    "ticker": p.get("pid", p.get("name", "?")),
                    "value":  p.get("value", 0),
                    "price":  p.get("price"),
                    "pl_pct": p.get("pl_pct"),
                    "pl_eur": p.get("pl_eur"),
                    "weight": round(p["value"] / bux["total"] * 100, 1) if bux.get("total") else None,
                }
                for p in bux.get("positions", []) if p.get("value", 0) > 0
            ],
        }
    if degiro:
        mem["degiro_summary"] = {
            "total":          degiro.get("total"),
            "total_pl_pct":   degiro.get("total_pl_pct"),
            "total_invested": degiro.get("total_invested"),
            "positions": [
                {
                    "name":    p.get("name", "?"),
                    "value":   p.get("value", 0),
                    "pl_pct":  p.get("pl_pct"),
                    "pl_eur":  p.get("pl_eur"),
                    "weight":  round(p["value"] / degiro["total"] * 100, 1) if degiro.get("total") else None,
                }
                for p in degiro.get("positions", []) if p.get("value", 0) > 0
            ],
        }
    if tr:
        mem["tr_summary"] = {
            "total":         tr.get("total"),
            "total_pl_pct":  tr.get("total_pl_pct"),
            "positions": [
                {
                    "name":   p.get("name", "?"),
                    "value":  p.get("value", 0),
                    "pl_pct": p.get("pl_pct"),
                    "weight": round(p["value"] / tr["total"] * 100, 1) if tr.get("total") else None,
                }
                for p in tr.get("positions", []) if p.get("value", 0) > 0
            ],
        }
    if investment_timeline:
        mem["investment_timeline"] = investment_timeline
        log.info(f"Investment timeline opgeslagen: {len(investment_timeline)} maanden")
    if first_investment_date:
        mem["first_investment_date"] = first_investment_date
    if portfolio_value_history:
        mem["portfolio_value_history"] = portfolio_value_history
        log.info(f"Portfolio waarde geschiedenis opgeslagen: {len(portfolio_value_history)} maanden")
    mem["dashboard_updated"] = datetime.now(timezone.utc).isoformat()
    _save_json(MEMORY_PATH, mem)
    log.info("Dashboard data opgeslagen in memory.json")


def _compute_investment_timeline(transactions: list[dict]) -> tuple[list[dict], str | None]:
    """
    Bereken cumulatief geïnvesteerd kapitaal per maand vanuit DEGIRO API-transacties.
    Geeft (timeline, eerste_datum) terug.
    timeline = [{date: "YYYY-MM", invested: float}, ...]
    """
    monthly_net: dict[str, float] = {}
    first_date: str | None = None

    for txn in transactions:
        date_str = (txn.get("date") or "")[:10]
        if not date_str:
            continue
        year_month = date_str[:7]
        action = txn.get("buysell", "").upper()
        total  = abs(float(txn.get("totalInBaseCurrency", 0) or 0))

        if first_date is None or date_str < first_date:
            first_date = date_str

        monthly_net.setdefault(year_month, 0.0)
        if action == "B":
            monthly_net[year_month] += total
        elif action == "S":
            monthly_net[year_month] -= total

    timeline: list[dict] = []
    cumulative = 0.0
    for ym in sorted(monthly_net.keys()):
        cumulative = max(0.0, cumulative + monthly_net[ym])
        timeline.append({"date": ym, "invested": round(cumulative, 2)})

    return timeline, first_date


def _parse_degiro_transactions_csv() -> tuple[list[dict], str | None]:
    """
    Verwerk DEGIRO_TRANSACTIONS_CSV (CSV-export van DEGIRO website).

    Ondersteunde kolomnamen:
      Datum / Date          — datum van de transactie
      Mutatie / Mutation    — bedrag (standaard DEGIRO-export, negatief = koop)
      Totaal / Total        — alternatief bedrag (oudere exports)
      Beschrijving / Description — voor koop/verkoop-detectie

    Filtert automatisch dividenden, kosten en stortingen eruit.
    Geeft (timeline, eerste_datum) terug.
    """
    import csv as _csv, io as _io
    raw = os.environ.get("DEGIRO_TRANSACTIONS_CSV", "").strip()
    if not raw:
        return [], None

    # Strip BOM (komt voor bij DEGIRO-exports op Windows)
    if raw.startswith("﻿"):
        raw = raw[1:]

    def _parse_num(s: str) -> float:
        s = s.strip().strip('"').replace(" ", "").replace("\xa0", "")
        if not s or s in ("-", "+"):
            return 0.0
        if "," in s and "." in s:
            # Nederlands: 1.234,56 → punt = duizendtal, komma = decimaal
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return 0.0

    monthly_net: dict[str, float] = {}
    first_date: str | None = None

    try:
        first_line = raw.split("\n")[0]
        delim = ";" if first_line.count(";") > first_line.count(",") else ","
        reader = _csv.DictReader(_io.StringIO(raw), delimiter=delim)
        fieldnames = reader.fieldnames or []
        log.info(f"DEGIRO CSV kolommen: {[f for f in fieldnames if f][:12]}")

        rows_ok = 0
        for row in reader:
            # Datum
            date_str = (row.get("Datum") or row.get("Date") or "").strip().strip('"')

            # Bedrag — vier mogelijke kolomnamen afhankelijk van exporttype:
            #   Rekeningoverzicht (NL): Mutatie
            #   Rekeningoverzicht (EN): Mutation
            #   Transactie-export (NL): Waarde EUR
            #   Transactie-export (EN): Value EUR  / Total
            #   Oud formaat:            Totaal
            total_str = (
                row.get("Totaal") or row.get("Total") or
                row.get("Mutatie") or row.get("Mutation") or
                row.get("Waarde EUR") or row.get("Value EUR") or ""
            ).strip()

            # Beschrijving aanwezig? (rekeningoverzicht-export)
            desc = (row.get("Beschrijving") or row.get("Description") or "").strip().lower()

            # ISIN aanwezig? (transactie-export — alle rijen zijn koop/verkoop)
            isin = (row.get("ISIN") or "").strip()

            if not date_str or not total_str:
                continue

            try:
                if len(date_str.split("-")[0]) == 2:
                    # DD-MM-YYYY (Nederlands)
                    d, m, y = date_str.split("-")
                    date_iso   = f"{y}-{m}-{d}"
                    year_month = f"{y}-{m}"
                else:
                    date_iso   = date_str[:10]
                    year_month = date_str[:7]
                total = _parse_num(total_str)
            except Exception:
                continue

            if total == 0:
                continue

            if desc:
                # Rekeningoverzicht: filter op koop/verkoop via beschrijving
                is_buy  = "koop" in desc or desc.startswith("buy ")
                is_sell = "verkoop" in desc or desc.startswith("sell ")
                if not (is_buy or is_sell):
                    continue
            elif isin:
                # Transactie-export: elke rij met ISIN is een effectentransactie
                # Waarde EUR negatief = koop (geld betaald), positief = verkoop
                is_buy  = total < 0
                is_sell = total > 0
            else:
                # Geen beschrijving en geen ISIN → geen effectentransactie
                continue

            if first_date is None or date_iso < first_date:
                first_date = date_iso

            monthly_net.setdefault(year_month, 0.0)
            if is_buy:
                monthly_net[year_month] += abs(total)
            else:
                monthly_net[year_month] -= abs(total)
            rows_ok += 1

        log.info(f"DEGIRO CSV: {rows_ok} koop/verkoop-regels verwerkt, eerste datum {first_date}")
    except Exception as e:
        log.warning(f"DEGIRO CSV parse fout: {e}")
        return [], None

    timeline: list[dict] = []
    cumulative = 0.0
    for ym in sorted(monthly_net.keys()):
        cumulative = max(0.0, cumulative + monthly_net[ym])
        timeline.append({"date": ym, "invested": round(cumulative, 2)})

    return timeline, first_date


def save_snapshot(degiro_total: float | None, tr_total: float | None, bux_total: float | None):
    """Sla dagelijkse portfoliowaarden op in memory.json."""
    mem      = _load_json(MEMORY_PATH, {})
    history  = mem.get(HISTORY_KEY, [])
    today    = date.today().isoformat()

    # Vervang eventuele bestaande entry voor vandaag
    history  = [h for h in history if h.get("date") != today]
    history.append({
        "date":   today,
        "degiro": degiro_total,
        "tr":     tr_total,
        "bux":    bux_total,
    })
    # Bewaar max MAX_HISTORY dagen, nieuwste eerst
    history  = sorted(history, key=lambda h: h["date"], reverse=True)[:MAX_HISTORY]
    mem[HISTORY_KEY] = history
    _save_json(MEMORY_PATH, mem)
    log.info(f"Snapshot opgeslagen: DEGIRO={degiro_total} TR={tr_total} BUX={bux_total}")


def _find_snapshot(history: list[dict], days_ago: int) -> dict | None:
    """Zoek de dichtst beschikbare snapshot rond `days_ago` dagen terug."""
    target = (date.today() - timedelta(days=days_ago)).isoformat()
    best   = None
    for h in history:
        if h["date"] <= target:
            best = h
            break   # history is nieuwste-eerst gesorteerd
    return best


def compute_perf(history: list[dict], current_total: float | None, key: str) -> dict:
    """Bereken dag/week/maand/YTD performance voor één portfolio."""
    if current_total is None:
        return {}
    ytd_date = f"{date.today().year}-01-01"
    snapshots = {
        "dag":   _find_snapshot(history, 1),
        "week":  _find_snapshot(history, 7),
        "maand": _find_snapshot(history, 30),
        "ytd":   next((h for h in reversed(history) if h["date"] >= ytd_date), None),
    }
    result = {}
    for label, snap in snapshots.items():
        if snap and snap.get(key) and snap[key] > 0:
            result[label] = round((current_total / snap[key] - 1) * 100, 1)
    return result


# ─── MARKTDATA ────────────────────────────────────────────────────────────────

def fetch_market_data() -> list[dict]:
    results = []
    for label, sym in INDICES:
        try:
            info  = yf.Ticker(sym).fast_info
            price = getattr(info, "last_price", None)
            prev  = getattr(info, "previous_close", None)
            if price and prev and prev > 0:
                pct = (price - prev) / prev * 100
                results.append({"label": label, "price": price, "pct": round(pct, 2)})
            else:
                results.append({"label": label, "price": None, "pct": 0})
        except Exception as e:
            log.warning(f"{label}: {e}")
            results.append({"label": label, "price": None, "pct": 0})
        time.sleep(0.2)
    return results


# ─── GOOGLE NEWS RSS ──────────────────────────────────────────────────────────

NEWS_FEEDS = [
    "https://news.google.com/rss/search?q=beurs+aandelen+koers&hl=nl&gl=NL&ceid=NL:nl",
    "https://news.google.com/rss/search?q=AEX+beurs&hl=nl&gl=NL&ceid=NL:nl",
    "https://news.google.com/rss/search?q=stock+market+S%26P500&hl=en&gl=US&ceid=US:en",
]


def fetch_news(max_items: int = 16) -> list[str]:
    """Haal actuele headlines op via Google News RSS (meer items voor betere AI-samenvatting)."""
    headlines = []
    seen      = set()
    for url in NEWS_FEEDS:
        try:
            r    = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            root = ET.fromstring(r.text)
            for item in root.iter("item"):
                title = item.findtext("title", "").strip()
                # Strip bron-suffix "- NOS" / "- Bloomberg" etc.
                title = title.rsplit(" - ", 1)[0].strip()
                if title and title not in seen and len(title) > 15:
                    seen.add(title)
                    headlines.append(title)
                if len(headlines) >= max_items:
                    return headlines
        except Exception as e:
            log.warning(f"Nieuws RSS mislukt ({url[:50]}): {e}")
    return headlines[:max_items]


def generate_news_summary(client: anthropic.Anthropic, headlines: list[str]) -> list[dict]:
    """
    Gebruik Claude om ruwe headlines om te zetten in 4 bruikbare nieuwssegmenten.
    Geeft lijst van dicts: [{"theme": str, "summary": str}, ...]
    """
    if not headlines or not client:
        return []

    headlines_text = "\n".join(f"{i+1}. {h}" for i, h in enumerate(headlines))

    prompt = (
        f"Hieronder staan {len(headlines)} beursnieuws-headlines van vandaag.\n\n"
        f"{headlines_text}\n\n"
        "Groepeer dit in precies 4 thematische nieuwssegmenten voor een belegger. "
        "Per segment:\n"
        "- theme: korte titel (max 5 woorden, Nederlands)\n"
        "- summary: 2-3 zinnen die uitleggen WAT er precies speelt en WAAROM het relevant is "
        "voor iemand met posities in ETFs (AEX, S&P500, World) en aandelen\n\n"
        "Antwoord ALLEEN in dit JSON-formaat, geen uitleg eromheen:\n"
        '[{"theme":"...","summary":"..."},{"theme":"...","summary":"..."},'
        '{"theme":"...","summary":"..."},{"theme":"...","summary":"..."}]'
    )

    try:
        import json as _json
        msg = client.messages.create(
            model="claude-haiku-4-5", max_tokens=700,
            system=(
                "Je bent een beursanalist die headlines omzet in bruikbare marktinzichten. "
                "Antwoord altijd als geldig JSON-array, niets anders."
            ),
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        # Verwijder eventuele markdown code-block wrapper
        if "```" in text:
            text = text.split("```")[1]
            if text.lower().startswith("json"):
                text = text[4:]
            text = text.strip()
        result = _json.loads(text)
        log.info(f"Nieuws-samenvatting: {len(result)} segmenten gegenereerd")
        return result if isinstance(result, list) else []
    except Exception as e:
        log.warning(f"Nieuws-samenvatting mislukt: {e}")
        return []


# ─── NEXUS PORTFOLIO ──────────────────────────────────────────────────────────

def fetch_nexus_portfolio() -> dict:
    data   = _load_json(DATA_PATH, {})
    trades = data.get("active_trades", [])
    port   = data.get("portfolio", {})
    cash   = port.get("cash", 0)
    total  = port.get("total_value", 0)

    positions, total_pl = [], 0.0
    for t in trades:
        pl = t.get("pl_percent", 0)
        total_pl += pl
        positions.append({
            "ticker": t["ticker"],
            "sector": t.get("sector", "?"),
            "pl":     round(pl, 2),
            "value":  round(t.get("current_value", t.get("position_value", 0)), 2),
            "tp":     t.get("tp_target", 30),
        })

    positions.sort(key=lambda p: p["pl"], reverse=True)
    return {
        "positions":  positions,
        "cash":       round(cash, 2),
        "total":      round(total, 2),
        "avg_pl":     round(total_pl / len(trades), 2) if trades else 0,
        "n":          len(trades),
        "top_cands":  [
            {"ticker": c["ticker"], "score": c.get("score", 0),
             "dcf": (c.get("dcf") or {}).get("dcf_upside")}
            for c in data.get("top_candidates", [])[:3]
        ],
    }


# ─── DEGIRO ───────────────────────────────────────────────────────────────────

def _parse_degiro_secret() -> tuple[str | None, str | None]:
    """
    Haal DEGIRO-credentials op uit het DEGIRO-secret.
    Ondersteunt meerdere formaten:
      DEGIRO_USERNAME email@example.com   ← spatie-gescheiden met prefix
      DEGIRO_PASSWORD wachtwoord
      username=email@example.com          ← = gescheiden zonder prefix
      password=wachtwoord
      username email@example.com          ← spatie-gescheiden zonder prefix
      password wachtwoord
    """
    raw = os.environ.get("DEGIRO", "")
    parsed: dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Normaliseer: vervang '=' door spatie voor uniforme verwerking
        if "=" in line and " " not in line.split("=")[0]:
            line = line.replace("=", " ", 1)
        parts = line.split(None, 1)
        if len(parts) == 2:
            parsed[parts[0].lower().replace("degiro_", "")] = parts[1].strip()

    username = (
        parsed.get("username")
        or os.environ.get("DEGIRO_USERNAME")
        or parsed.get("degiro_username")
    )
    password = (
        parsed.get("password")
        or os.environ.get("DEGIRO_PASSWORD")
        or parsed.get("degiro_password")
    )

    if username:
        log.info(f"DEGIRO credentials geladen: {username[:3]}***@***")
    else:
        log.warning("DEGIRO: geen username gevonden in secret. "
                    "Controleer het DEGIRO secret formaat:\n"
                    "  username jouw@email.nl\n"
                    "  password jouwwachtwoord")
    return username, password


def _fetch_degiro_transactions(session: requests.Session, session_id: str,
                               int_account) -> list[dict]:
    """
    Haal volledige transactiehistorie op via DEGIRO reporting API.
    Gebruikt totalInBaseCurrency (altijd EUR) voor valuta-onafhankelijke P&L.
    """
    try:
        today = date.today().strftime("%d/%m/%Y")
        r = session.get(
            DEGIRO_TRANS_URL,
            params={
                "fromDate":                "01/01/2010",
                "toDate":                  today,
                "groupTransactionsByOrder": 0,
                "intAccount":              int_account,
                "sessionId":               session_id,
            },
            timeout=25,
        )
        txns = r.json().get("data", [])
        log.info(f"DEGIRO: {len(txns)} transacties opgehaald.")
        return txns
    except Exception as e:
        log.warning(f"DEGIRO transacties mislukt: {e}")
        return []


def _compute_avg_costs(transactions: list[dict]) -> dict[str, dict]:
    """
    Bereken gewogen gemiddelde aankoopkosten in EUR per productId.
    Bij bijkopen (DCA): gewogen gemiddelde.
    Bij verkopen: proportioneel de kostenbasis verminderen.
    Geeft {pid: {cost_eur, shares}} terug voor posities met resterende aandelen.
    """
    holdings: dict[str, dict] = {}

    for txn in sorted(transactions, key=lambda t: t.get("date", "")):
        pid     = str(txn.get("productId", ""))
        action  = txn.get("buysell", "").upper()
        qty     = abs(float(txn.get("quantity", 0) or 0))
        # totalInBaseCurrency is negatief bij koop (geld uit), positief bij verkoop
        cost    = abs(float(txn.get("totalInBaseCurrency", 0) or 0))

        if not pid or qty == 0:
            continue

        if pid not in holdings:
            holdings[pid] = {"shares": 0.0, "cost_eur": 0.0}

        if action == "B":
            holdings[pid]["shares"]   += qty
            holdings[pid]["cost_eur"] += cost
        elif action == "S" and holdings[pid]["shares"] > 0:
            ratio = min(qty / holdings[pid]["shares"], 1.0)
            holdings[pid]["cost_eur"] *= (1 - ratio)
            holdings[pid]["shares"]    = max(0.0, holdings[pid]["shares"] - qty)

    return {
        pid: {"shares": round(h["shares"], 6), "cost_eur": round(h["cost_eur"], 2)}
        for pid, h in holdings.items()
        if h["shares"] > 0.001 and h["cost_eur"] > 0
    }


def _degiro_resolve_names(session: requests.Session, session_id: str,
                           int_account, product_ids: list[str]) -> dict[str, str]:
    """Vertaal DEGIRO product-ID's naar aandelennamen."""
    if not product_ids:
        return {}
    try:
        params = {"sessionId": session_id, "intAccount": int_account}
        for pid in product_ids[:20]:
            params.setdefault("ids", [])
            if isinstance(params["ids"], list):
                params["ids"].append(pid)
        # requests stuurt lijsten als herhaalde params
        r = session.get(DEGIRO_PROD_URL,
                        params=[("sessionId", session_id), ("intAccount", int_account)]
                                + [("ids", pid) for pid in product_ids[:20]],
                        timeout=10)
        data = r.json().get("data", {})
        return {pid: data[pid].get("symbol") or data[pid].get("name", pid)
                for pid in product_ids if pid in data}
    except Exception as e:
        log.warning(f"DEGIRO naameresolutie mislukt: {e}")
        return {}


def fetch_degiro_portfolio() -> dict | None:
    username, password = _parse_degiro_secret()
    if not username or not password:
        log.info("DEGIRO credentials niet beschikbaar.")
        return None

    session = requests.Session()
    # Browser-achtige headers om Cloudflare/bot-detectie te omzeilen
    session.headers.update({
        "User-Agent":      ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         "https://trader.degiro.nl/",
        "Origin":          "https://trader.degiro.nl",
        "Content-Type":    "application/json",
        "sec-ch-ua":       '"Chromium";v="124", "Google Chrome";v="124"',
        "sec-ch-ua-mobile":"?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest":  "empty",
        "Sec-Fetch-Mode":  "cors",
        "Sec-Fetch-Site":  "same-origin",
    })

    # Login
    try:
        log.info(f"DEGIRO: inloggen op {DEGIRO_LOGIN_URL} ...")
        r = session.post(DEGIRO_LOGIN_URL,
                         json={"username": username, "password": password,
                               "isRedirectToMobile": False}, timeout=15)
        log.info(f"DEGIRO login response: HTTP {r.status_code}")
        if not r.ok:
            log.warning(f"DEGIRO login mislukt: {r.status_code} — {r.text[:200]}")
            return None
        resp_json = r.json()
        session_id = resp_json.get("sessionId")
        if not session_id:
            log.warning(f"DEGIRO: geen sessionId in response. Keys: {list(resp_json.keys())}")
            return None
        session.headers.update({"Cookie": f"JSESSIONID={session_id}"})
        log.info("DEGIRO: succesvol ingelogd.")
    except Exception as e:
        log.warning(f"DEGIRO login fout: {e}")
        return None

    # Account-ID
    int_account = os.environ.get("DEGIRO_INT_ACCOUNT")
    if not int_account:
        try:
            r = session.get(DEGIRO_CONFIG_URL, params={"sessionId": session_id}, timeout=10)
            int_account = r.json().get("data", {}).get("intAccount")
        except Exception:
            log.warning("DEGIRO: int_account ophalen mislukt.")
            return None

    # Portfolio
    try:
        r = session.get(DEGIRO_PORT_URL.format(int_account=int_account),
                        params={"sessionId": session_id, "portfolio": 0}, timeout=15)
        raw    = r.json().get("portfolio", {}).get("value", [])
        items  = []
        pids   = []
        for entry in raw:
            vals  = {v["name"]: v.get("value") for v in entry.get("value", [])}
            pid   = str(vals.get("productId", ""))
            size  = vals.get("size", 0) or 0
            price = vals.get("price", 0) or 0
            value = vals.get("value", size * price) or 0
            if size and size > 0 and pid:
                items.append({"pid": pid, "size": size, "price": round(price, 2),
                               "value": round(value, 2), "name": pid})
                pids.append(pid)

        # Vertaal product-ID's naar aandelennamen
        names = _degiro_resolve_names(session, session_id, int_account, pids)
        for item in items:
            item["name"] = names.get(item["pid"], item["pid"])

        total = sum(i["value"] for i in items)

        # Transactiehistorie → gemiddelde aankoopkosten → P&L per positie
        transactions = _fetch_degiro_transactions(session, session_id, int_account)
        avg_costs    = _compute_avg_costs(transactions)
        total_invested = 0.0
        for item in items:
            h = avg_costs.get(item["pid"])
            if h and h["cost_eur"] > 0 and item["value"] > 0:
                item["cost_eur"]   = h["cost_eur"]
                item["pl_pct"]     = round((item["value"] / h["cost_eur"] - 1) * 100, 2)
                item["pl_eur"]     = round(item["value"] - h["cost_eur"], 2)
                total_invested    += h["cost_eur"]
            else:
                item["cost_eur"]   = None
                item["pl_pct"]     = None
                item["pl_eur"]     = None

        total_pl_pct = round((total / total_invested - 1) * 100, 2) if total_invested > 0 else None
        items.sort(key=lambda i: i["value"], reverse=True)
        log.info(f"DEGIRO: {len(items)} posities, totaal €{total:.2f}, totaal P&L {total_pl_pct}%")
        return {
            "positions":       items,
            "total":           round(total, 2),
            "total_invested":  round(total_invested, 2),
            "total_pl_pct":    total_pl_pct,
        }
    except Exception as e:
        log.warning(f"DEGIRO portfolio fout: {e}")
        return None


# ─── TRADE REPUBLIC ───────────────────────────────────────────────────────────
# fetch_tr_portfolio() is geïmporteerd uit tr_portfolio.py
# Gebruikt TR_HOLDINGS secret (ISIN + aantal per regel) + yfinance voor prijzen.
# Geen pytr / SMS 2FA nodig — werkt in GitHub Actions.


# ─── CLAUDE MARKTBRIEF ────────────────────────────────────────────────────────

def generate_ai_briefing(client: anthropic.Anthropic, market: list[dict],
                          news: list[str], nexus: dict) -> str:
    market_lines = "\n".join(
        f"  {m['label']}: {m['price']:.2f} ({m['pct']:+.2f}%)" if m["price"]
        else f"  {m['label']}: n/b"
        for m in market
    )
    news_lines = "\n".join(f"  • {h}" for h in news) if news else "  Geen nieuws."
    top3 = nexus.get("positions", [])[:3]
    pos_lines = "\n".join(f"  {p['ticker']} {p['pl']:+.1f}%" for p in top3)

    prompt = (
        "Schrijf een scherpe, professionele Nederlandse morning note (max 180 woorden) "
        "voor een waardebelegger. Direct, analytisch, geen wollig taalgebruik.\n\n"
        f"MARKTEN:\n{market_lines}\n\n"
        f"ACTUEEL NIEUWS:\n{news_lines}\n\n"
        f"TOP NEXUS POSITIES:\n{pos_lines}\n\n"
        "Structuur (gewone alinea's, geen headers):\n"
        "1. Marktsfeer vandaag in één zin\n"
        "2. Wat drijft de beweging? (macro, sector, event)\n"
        "3. Één concrete observatie of kans voor de belegger\n"
        "4. Één risico om vandaag op te letten"
    )
    try:
        msg = client.messages.create(
            model="claude-haiku-4-5", max_tokens=350,
            system=("Je bent een senior marktstrateeg bij een Europees hedgefund. "
                    "Dagelijkse morning note voor partners. Scherp, bondig."),
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        log.warning(f"Claude mislukt: {e}")
        return "AI-samenvatting niet beschikbaar."


# ─── TELEGRAM OPMAAK ──────────────────────────────────────────────────────────

SEP = "─" * 30


def _perf_line(perf: dict) -> str:
    parts = []
    for key, label in [("dag", "Dag"), ("week", "Week"), ("maand", "Maand"), ("ytd", "YTD")]:
        v = perf.get(key)
        parts.append(f"{label}: {_pct_str(v)}")
    return "  " + " · ".join(parts)


def _portfolio_block(icon: str, label: str, data: dict | None, perf: dict) -> str:
    """Gestructureerd portfolio-blok met P&L per positie."""
    if not data:
        return ""
    total    = data["total"]
    pl_pct   = data.get("total_pl_pct")
    invested = data.get("total_invested")

    # Header
    pl_s = f"  _{'+' if (pl_pct or 0) >= 0 else ''}{pl_pct:.1f}% totaal_" if pl_pct is not None else ""
    inv_s = f"  _(inleg €{invested:,.0f})_" if invested else ""
    lines = [f"{icon} *{label}*  `€{total:,.0f}`{pl_s}{inv_s}"]

    if perf:
        lines.append(_perf_line(perf))

    lines.append("")  # lege regel voor leesbaarheid

    # Posities — alleen met waarde > 0
    visible = [p for p in data["positions"] if p.get("value", 0) > 0][:10]
    for p in visible:
        name   = p.get("name", "?")[:13]
        val    = p.get("value", 0)
        pl     = p.get("pl_pct")
        weight = round(val / total * 100, 1) if total > 0 else 0

        if pl is not None:
            dot = "🟢" if pl >= 0 else "🔴"
            pl_s = f"`{pl:+.1f}%`"
        else:
            dot  = "⬜"
            pl_s = "`  n/b `"

        lines.append(f"  {dot} `{name:<13}` {weight:>4.1f}%  {pl_s}  `€{val:>7,.0f}`")

    return "\n".join(lines)


def build_telegram_message(market, news, nexus, degiro, tr,
                            degiro_perf, tr_perf, ai_text,
                            news_summary=None, bux=None, bux_perf=None) -> str:
    now    = datetime.now(timezone.utc)
    dag_nl = ["ma","di","wo","do","vr","za","zo"][now.weekday()]
    mnd_nl = ["jan","feb","mrt","apr","mei","jun",
               "jul","aug","sep","okt","nov","dec"][now.month - 1]
    date_s = f"{dag_nl} {now.day} {mnd_nl} {now.year}"

    # ── 1. HEADER ────────────────────────────────────────────────────────────
    total_portfolio = (
        (degiro.get("total") or 0 if degiro else 0) +
        (tr.get("total") or 0 if tr else 0) +
        (bux.get("total") or 0 if bux else 0)
    )
    portfolio_line = f"\n💰 Portfolio totaal: `€{total_portfolio:,.0f}`" if total_portfolio else ""
    header = (
        f"🌅 *NEXUS MORNING BRIEFING*\n"
        f"_{date_s} · {now.strftime('%H:%M')} UTC_"
        f"{portfolio_line}"
    )

    # ── 2. MARKTEN ───────────────────────────────────────────────────────────
    mkt_lines = []
    for m in market:
        if m["price"] is None:
            mkt_lines.append(f"  ❓ *{m['label']}*: n/b")
            continue
        p       = m["price"]
        price_s = f"{p:,.0f}" if p >= 1000 else f"{p:,.2f}"
        mkt_lines.append(
            f"  {_arrow(m['pct'])} *{m['label']}* `{price_s}` ({m['pct']:+.2f}%)"
        )
    markten = "📊 *MARKTEN*\n" + "\n".join(mkt_lines)

    # ── 3. EIGEN PORTFOLIO (DEGIRO + TR) ─────────────────────────────────────
    eigen_parts = []
    db = _portfolio_block("🏦", "DEGIRO", degiro, degiro_perf)
    tb = _portfolio_block("📱", "Trade Republic", tr, tr_perf)
    if db: eigen_parts.append(db)
    if tb: eigen_parts.append(tb)

    eigen = ""
    if eigen_parts:
        eigen = "💼 *EIGEN PORTFOLIO*\n\n" + f"\n{SEP}\n".join(eigen_parts)

    # ── 4. BUX ───────────────────────────────────────────────────────────────
    bux_block = ""
    if bux and bux.get("positions"):
        bux_total = bux.get("total", 0)
        bux_pl    = bux.get("total_pl_pct")
        pl_s      = f"  _{'+' if (bux_pl or 0) >= 0 else ''}{bux_pl:.1f}%_" if bux_pl is not None else ""
        lines = [f"📲 *BUX*  `€{bux_total:,.0f}`{pl_s}"]
        if bux_perf:
            lines.append(_perf_line(bux_perf))
        lines.append("")
        for p in bux.get("positions", []):
            if not p.get("value", 0):
                continue
            dot  = "🟢" if (p.get("pl_pct") or 0) >= 0 else "🔴"
            pl_s2 = f"`{p['pl_pct']:+.1f}%`" if p.get("pl_pct") is not None else "`n/b`"
            lines.append(f"  {dot} `{p['name']:<8}` {pl_s2}  `€{p['value']:>7,.0f}`")
        bux_block = "\n".join(lines)

    # ── 5. NEXUS BOT (papier) ────────────────────────────────────────────────
    nexus_block = ""
    if nexus.get("positions"):
        pos   = nexus["positions"]
        lines = [
            f"🤖 *NEXUS BOT* _(papier trading)_\n"
            f"  💼 `€{nexus['total']:,.0f}` · {nexus['n']} posities · gem. `{nexus['avg_pl']:+.1f}%`\n"
            f"  💵 Cash: `€{nexus['cash']:,.0f}`"
        ]
        for p in pos[:6]:
            dot = "🟢" if p["pl"] >= 0 else "🔴"
            lines.append(f"  {dot} `{p['ticker']:<6}` `{p['pl']:+5.1f}%`  `€{p['value']:>6,.0f}`")
        nexus_block = "\n".join(lines)

    # ── 6. NIEUWS ────────────────────────────────────────────────────────────
    nieuws = ""
    if news_summary:
        parts = []
        for seg in news_summary[:4]:
            parts.append(f"  📌 *{seg['theme']}*\n  {seg['summary']}")
        nieuws = "📰 *NIEUWS*\n\n" + "\n\n".join(parts)
    elif news:
        nieuws = "📰 *NIEUWS*\n" + "\n".join(f"  • {h}" for h in news[:6])

    # ── 7. AI ANALYSE ────────────────────────────────────────────────────────
    ai_block = f"🧠 *AI ANALYSE*\n{ai_text}"

    # ── SAMENSTELLEN ─────────────────────────────────────────────────────────
    sections = [header, markten]
    if eigen:       sections.append(eigen)
    if bux_block:   sections.append(bux_block)
    if nexus_block: sections.append(nexus_block)
    if nieuws:      sections.append(nieuws)
    sections.append(ai_block)
    sections.append(f"🌐 [Open Dashboard]({DASHBOARD_URL})")

    return f"\n{SEP}\n".join(sections)


# ─── DEGIRO HANDMATIG (fallback als API geblokkeerd is) ───────────────────────

def fetch_degiro_manual() -> dict | None:
    """Lees DEGIRO_HOLDINGS als fallback (Cloudflare blokkeert API vanuit GitHub Actions)."""
    holdings = _parse_holdings_secret("DEGIRO_HOLDINGS", "DEGIRO")
    if not holdings:
        log.info("DEGIRO_HOLDINGS niet ingesteld — DEGIRO wordt overgeslagen.")
        return None
    log.info(f"DEGIRO_HOLDINGS: {len(holdings)} posities geladen — prijzen ophalen via yfinance...")
    return _holdings_to_portfolio(holdings, "DEGIRO manual")


# ─── BUX HANDMATIG (via BUX_HOLDINGS secret of BUX CSV-export) ───────────────

def _parse_holdings_secret(env_key: str, label: str) -> list[dict]:
    """
    Generieke parser voor holdings-secrets.
    Formaat: yfinance_ticker  aandelen  [gemiddelde_aankoopprijs_eur]
    """
    raw = os.environ.get(env_key, "").strip()
    if not raw:
        return []
    holdings = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        ticker = parts[0]
        try:
            shares = float(parts[1])
        except ValueError:
            log.warning(f"{label}: ongeldige hoeveelheid voor {ticker}: {parts[1]}")
            continue
        avg_price = None
        if len(parts) >= 3:
            try:
                avg_price = float(parts[2])
            except ValueError:
                pass
        if shares > 0:
            holdings.append({"ticker": ticker, "shares": shares, "avg_price": avg_price})
    return holdings


def _holdings_to_portfolio(holdings: list[dict], label: str) -> dict | None:
    """Haal yfinance-prijzen op voor een lijst holdings en bereken totaalwaarde + P&L."""
    if not holdings:
        return None
    positions = []
    total     = 0.0
    for h in holdings:
        ticker = h["ticker"]
        shares = h["shares"]
        price = None
        # Stap 1: fast_info (snel, werkt goed voor US-tickers en Europese ETFs)
        try:
            p = getattr(yf.Ticker(ticker).fast_info, "last_price", None)
            if p and float(p) > 0:
                price = float(p)
        except Exception:
            pass
        # Stap 2: volledige info (meer velden, maar trager)
        if not price:
            try:
                full = yf.Ticker(ticker).info
                p    = (full.get("currentPrice") or
                        full.get("regularMarketPrice") or
                        full.get("previousClose"))
                if p and float(p) > 0:
                    price = float(p)
            except Exception:
                pass
        # Stap 3: .DE-ticker niet beschikbaar → probeer US-ticker + USD→EUR conversie
        if not price and ticker.endswith(".DE"):
            try:
                us_ticker = ticker[:-3]
                usd_price = getattr(yf.Ticker(us_ticker).fast_info, "last_price", None)
                if usd_price and float(usd_price) > 0:
                    eur_usd = getattr(yf.Ticker("EURUSD=X").fast_info, "last_price", None) or 1.08
                    price   = round(float(usd_price) / float(eur_usd), 4)
                    log.info(f"{ticker} via {us_ticker} (USD→EUR /{eur_usd:.4f}): €{price:.2f}")
            except Exception:
                pass
        if not price:
            log.warning(f"{label}: geen prijs voor {ticker} — overgeslagen")
            continue
        value     = shares * price
        total    += value
        avg_price = h.get("avg_price")
        if avg_price and avg_price > 0:
            cost_eur = shares * avg_price
            pl_pct   = round((price / avg_price - 1) * 100, 2)
            pl_eur   = round(value - cost_eur, 2)
        else:
            cost_eur = pl_pct = pl_eur = None
        positions.append({
            "name":      ticker,
            "pid":       ticker,
            "size":      round(shares, 4),
            "price":     round(price, 2),
            "value":     round(value, 2),
            "avg_price": avg_price,
            "cost_eur":  round(cost_eur, 2) if cost_eur else None,
            "pl_pct":    pl_pct,
            "pl_eur":    pl_eur,
        })
        time.sleep(0.15)
    if not positions:
        log.warning(f"{label}: geen posities met prijs.")
        return None
    positions.sort(key=lambda p: p["value"], reverse=True)
    total_invested = sum(p["cost_eur"] for p in positions if p.get("cost_eur"))
    total_pl_pct   = round((total / total_invested - 1) * 100, 2) if total_invested > 0 else None
    log.info(f"{label}: {len(positions)} posities, totaal €{total:.2f}, P&L {total_pl_pct}%")
    return {
        "positions":      positions,
        "total":          round(total, 2),
        "total_invested": round(total_invested, 2) if total_invested else None,
        "total_pl_pct":   total_pl_pct,
    }


# BUX ISIN → yfinance-ticker mapping
# Voeg toe als je nieuwe posities hebt die niet herkend worden
BUX_ISIN_TO_TICKER: dict[str, str | None] = {
    "NL0000009082": "KPN.AS",       # KPN — Amsterdam (EUR)
    "US88160R1014": "TSLA.DE",      # Tesla — Frankfurt (EUR)
    "US7134481081": "PEP",          # PepsiCo — NYSE (USD)
    "US29355A1079": "ENPH",         # Enphase Energy — NASDAQ (USD)
    "US30303M1027": "META",         # Meta Platforms — NASDAQ (USD)
    "US02079K3059": "GOOGL",        # Alphabet A — NASDAQ (USD)
    "US98986T1088": None,           # Zynga — overgenomen door Take-Two
    "US5949181045": "MSFT",         # Microsoft — NASDAQ (USD)
    "US0231351067": "AMZN",         # Amazon — NASDAQ (USD)
    "US0378331005": "AAPL",         # Apple — NASDAQ (USD)
    "US67066G1040": "NVDA",         # NVIDIA — NASDAQ (USD)
    "US46090E1038": "IVV",          # iShares S&P 500 ETF (USD)
    "US4592001014": "IBM",          # IBM — NYSE (USD)
    "NL0015436031": "ASML.AS",      # ASML — Amsterdam (EUR)
    "US46625H1005": "JPM",          # JPMorgan — NYSE (USD)
}


def _parse_bux_transactions_csv() -> dict | None:
    """
    Verwerk BUX_TRANSACTIONS_CSV omgevingsvariabele (volledige BUX export).
    Berekent automatisch aandelen en gewogen gemiddelde aankoopprijs per ISIN.

    BUX CSV-formaat:
      Transaction Time, Transaction Category, Transaction Type, Transfer Type,
      Transaction Amount, Transaction Currency, ..., Asset Id, Asset Name,
      Asset Quantity, Asset Price, Asset Currency, ...

    Geeft een portfolio-dict terug of None als niet beschikbaar.
    """
    import csv as _csv, io as _io
    raw = os.environ.get("BUX_TRANSACTIONS_CSV", "").strip()
    if not raw:
        return None

    acc: dict[str, dict] = {}   # ISIN → {name, shares, cost_eur}

    try:
        reader = _csv.DictReader(_io.StringIO(raw))
        rows_ok = 0
        for row in reader:
            tx_type   = (row.get("Transaction Type")    or "").strip()
            xfer_type = (row.get("Transfer Type")       or "").strip()
            isin      = (row.get("Asset Id")             or "").strip()
            name      = (row.get("Asset Name")           or isin).strip()
            currency  = (row.get("Transaction Currency") or "").strip()
            qty_str   = (row.get("Asset Quantity")       or "").strip()
            amt_str   = (row.get("Transaction Amount")   or "").strip()

            if not isin or not qty_str or currency != "EUR":
                continue

            try:
                qty = abs(float(qty_str))
            except ValueError:
                continue

            if isin not in acc:
                acc[isin] = {"name": name, "shares": 0.0, "cost_eur": 0.0}
            h = acc[isin]

            if tx_type == "Buy Trade" and xfer_type == "CASH_DEBIT":
                try:
                    h["shares"]   += qty
                    h["cost_eur"] += abs(float(amt_str))
                    rows_ok += 1
                except ValueError:
                    pass

            elif tx_type == "Sell Trade" and xfer_type == "CASH_CREDIT":
                if h["shares"] > 0 and qty > 0:
                    ratio = min(qty / h["shares"], 1.0)
                    h["cost_eur"] *= (1 - ratio)
                    h["shares"]    = max(0.0, h["shares"] - qty)
                    rows_ok += 1

        log.info(f"BUX CSV: {rows_ok} regels verwerkt, {len(acc)} ISINs gevonden")

    except Exception as e:
        log.warning(f"BUX CSV parsing fout: {e}")
        return None

    # Bouw holdings op vanuit open posities
    holdings = []
    for isin, h in acc.items():
        if h["shares"] < 0.0001:
            continue  # Gesloten positie

        ticker = BUX_ISIN_TO_TICKER.get(isin)
        if ticker is None:
            if isin in BUX_ISIN_TO_TICKER:
                log.info(f"BUX CSV: {isin} ({h['name']}) overgeslagen (acquired/delisted)")
            else:
                log.warning(f"BUX CSV: geen ticker voor ISIN {isin} ({h['name']}) — voeg toe aan BUX_ISIN_TO_TICKER")
            continue

        avg_eur = round(h["cost_eur"] / h["shares"], 4)
        holdings.append({"ticker": ticker, "shares": round(h["shares"], 6), "avg_price": avg_eur})
        log.info(f"BUX CSV: {ticker} ({isin}) — {h['shares']:.4f} aandelen, avg €{avg_eur:.2f}")

    if not holdings:
        log.warning("BUX CSV: geen open posities gevonden na verwerking.")
        return None

    return _holdings_to_portfolio(holdings, "BUX CSV")


def fetch_bux_manual() -> dict | None:
    """
    BUX portfolio ophalen:
    1. Probeert eerst BUX_TRANSACTIONS_CSV (volledige CSV-export, auto-berekening)
    2. Valt terug op BUX_HOLDINGS (handmatig: ticker  aandelen  [avg_prijs])
    """
    # 1. Auto via CSV-export
    csv_result = _parse_bux_transactions_csv()
    if csv_result:
        return csv_result

    # 2. Handmatige fallback
    holdings = _parse_holdings_secret("BUX_HOLDINGS", "BUX")
    if not holdings:
        log.info("BUX_HOLDINGS niet ingesteld — BUX wordt overgeslagen.")
        return None
    log.info(f"BUX_HOLDINGS: {len(holdings)} posities geladen — prijzen ophalen via yfinance...")
    return _holdings_to_portfolio(holdings, "BUX manual")


# ─── PORTFOLIO WAARDE GESCHIEDENIS ──────────────────────────────────────────────

# ISIN → yfinance ticker voor historische prijzen
_ISIN_TO_YF: dict[str, str] = {
    # Vanguard ETFs — Euronext Amsterdam
    "IE00B3RBWM25": "VWRL.AS",   # FTSE All-World Distributing
    "IE00B3XXRP09": "VUSA.AS",   # S&P 500 UCITS
    "IE00B8GKDB10": "VHYL.AS",   # High Dividend Yield
    # Vanguard ETFs — Xetra (ACC varianten)
    "IE00BK5BQT80": "VWCE.DE",   # FTSE All-World Accumulating
    "IE00BK5BR626": "VHYL.DE",   # High Dividend Yield ACC
    # iShares / Amundi S&P 500
    "IE00B5BMR087": "SXR8.DE",   # iShares Core S&P 500
    "LU1681048804": "SXR8.DE",   # Amundi S&P 500 (proxy via iShares)
    # VanEck
    "NL0011683594": "TDIV.AS",   # Morningstar Developed Markets Dividend Leaders
    # US aandelen (prijs in USD → EUR-conversie via EURUSD=X)
    "US92826C8394": "V",          # Visa
    "NL0009538784": "KPN.AS",    # KPN (EUR)
    "US7134481081": "PEP",        # PepsiCo
    "US29355A1079": "ENPH",       # Enphase
    "US88160R1014": "TSLA",       # Tesla
    "US02079K3059": "GOOGL",      # Alphabet
    "US0378331005": "AAPL",       # Apple
    "US5949181045": "MSFT",       # Microsoft
    "US67066G1040": "NVDA",       # Nvidia
}

# ISINs waarvan de prijs in USD is (conversie naar EUR nodig)
_USD_ISINS = {
    "US92826C8394",  # Visa
    "US7134481081",  # PepsiCo
    "US29355A1079",  # Enphase
    "US88160R1014",  # Tesla
    "US02079K3059",  # Alphabet
    "US0378331005",  # Apple
    "US5949181045",  # Microsoft
    "US67066G1040",  # Nvidia
}


def _build_portfolio_value_history() -> list[dict]:
    """
    Reconstrueer maandelijkse DEGIRO-portfoliowaarde vanuit transactie-CSV + yfinance.

    Algoritme:
      1. DEGIRO CSV → shares per ISIN per maand (cumulatief)
      2. yfinance monthly close prices per ISIN/ticker
      3. Maandelijkse waarde = sum(shares × prijs_EUR)
      4. Blend met portfolio_history (dagelijkse snapshots, incl. TR+BUX) voor recente maanden

    Opgeslagen in memory.json als 'portfolio_value_history'.
    """
    import csv as _csv, io as _io

    raw = os.environ.get("DEGIRO_TRANSACTIONS_CSV", "").strip()
    if not raw:
        log.info("_build_portfolio_value_history: DEGIRO_TRANSACTIONS_CSV niet beschikbaar.")
        return []

    if raw.startswith("﻿"):
        raw = raw[1:]

    # ── Hulpfuncties ────────────────────────────────────────────────────────────
    def _pn(s: str) -> float:
        s = s.strip().strip('"').replace(" ", "").replace("\xa0", "")
        if not s or s in ("-", "+"):
            return 0.0
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return 0.0

    def _parse_date(d: str):
        d = d.strip().strip('"')
        try:
            if len(d.split("-")[0]) == 2:
                dd, mm, yyyy = d.split("-")
                return f"{yyyy}-{mm}-{dd}"
            return d[:10]
        except Exception:
            return None

    # ── Stap 1: lees transacties uit CSV ────────────────────────────────────────
    first_line = raw.split("\n")[0]
    delim = ";" if first_line.count(";") > first_line.count(",") else ","
    txns = []

    try:
        reader = _csv.DictReader(_io.StringIO(raw), delimiter=delim)
        for row in reader:
            date_iso = _parse_date(row.get("Datum") or row.get("Date") or "")
            isin     = (row.get("ISIN") or "").strip()
            if not date_iso or not isin or isin not in _ISIN_TO_YF:
                continue

            shares_str = (row.get("Aantal") or row.get("Shares") or row.get("Quantity") or "").strip()
            amount_str = (
                row.get("Waarde EUR") or row.get("Value EUR") or
                row.get("Mutatie")    or row.get("Mutation") or ""
            ).strip()
            desc = (row.get("Beschrijving") or row.get("Description") or "").strip().lower()

            shares = abs(_pn(shares_str))
            if shares < 0.00001:
                continue

            if desc:
                is_buy  = "koop" in desc or desc.startswith("buy ")
                is_sell = "verkoop" in desc or desc.startswith("sell ")
            else:
                amt = _pn(amount_str)
                is_buy  = amt < 0
                is_sell = amt > 0

            if is_buy:
                txns.append({"date": date_iso, "isin": isin, "delta": +shares})
            elif is_sell:
                txns.append({"date": date_iso, "isin": isin, "delta": -shares})

    except Exception as e:
        log.warning(f"portfolio_value_history: CSV parse fout: {e}")
        return []

    if not txns:
        log.info("portfolio_value_history: geen transacties gevonden (Aantal kolom ontbreekt?).")
        return []

    txns.sort(key=lambda t: t["date"])
    log.info(f"portfolio_value_history: {len(txns)} transacties geladen.")

    # ── Stap 2: bouw cumulatieve maandelijkse holdings ───────────────────────────
    start_ym = txns[0]["date"][:7]
    now      = date.today()
    end_ym   = now.strftime("%Y-%m")

    months: list[str] = []
    y, m = int(start_ym[:4]), int(start_ym[5:7])
    while f"{y:04d}-{m:02d}" <= end_ym:
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1

    txn_by_month: dict[str, list] = {}
    for t in txns:
        txn_by_month.setdefault(t["date"][:7], []).append(t)

    cumulative: dict[str, float] = {}
    holdings_by_month: dict[str, dict[str, float]] = {}
    for mo in months:
        for t in txn_by_month.get(mo, []):
            cumulative[t["isin"]] = cumulative.get(t["isin"], 0.0) + t["delta"]
            if cumulative[t["isin"]] < 0.00001:
                cumulative.pop(t["isin"], None)
        holdings_by_month[mo] = {k: v for k, v in cumulative.items() if v > 0}

    # ── Stap 3: yfinance maandelijkse closing prices ──────────────────────────────
    all_isins = {isin for h in holdings_by_month.values() for isin in h}
    price_hist: dict[str, dict[str, float]] = {}   # isin → {month → prijs_eur}

    # EUR/USD wisselkoers voor USD-aandelen
    eurusd_hist: dict[str, float] = {}
    try:
        eu = yf.Ticker("EURUSD=X").history(period="max", interval="1mo")
        for idx, row in eu.iterrows():
            eurusd_hist[idx.strftime("%Y-%m")] = float(row["Close"])
        log.info(f"EUR/USD history: {len(eurusd_hist)} maanden geladen.")
    except Exception as e:
        log.warning(f"EUR/USD history mislukt: {e}")

    for isin in sorted(all_isins):
        ticker = _ISIN_TO_YF.get(isin)
        if not ticker:
            continue
        try:
            hist = yf.Ticker(ticker).history(period="max", interval="1mo")
            if hist.empty:
                log.warning(f"Geen history voor {ticker} ({isin})")
                continue
            monthly: dict[str, float] = {}
            is_usd = isin in _USD_ISINS
            for idx, row in hist.iterrows():
                ym    = idx.strftime("%Y-%m")
                price = float(row["Close"])
                if is_usd:
                    rate  = eurusd_hist.get(ym, 1.08)
                    price = round(price / rate, 4)
                monthly[ym] = price
            price_hist[isin] = monthly
            log.info(f"Prijs history: {isin} → {ticker} ({len(monthly)} maanden)")
            time.sleep(0.25)
        except Exception as e:
            log.warning(f"yfinance {ticker}: {e}")

    # ── Stap 4: bereken maandelijkse portfoliowaarde ─────────────────────────────
    degiro_monthly: list[dict] = []
    for mo in months:
        holdings = holdings_by_month.get(mo, {})
        value    = 0.0
        for isin, shares in holdings.items():
            ph = price_hist.get(isin, {})
            # Gebruik exacte maand, of meest recente beschikbare prijs daarvoor
            price = ph.get(mo)
            if price is None:
                prior = sorted(k for k in ph if k <= mo)
                if prior:
                    price = ph[prior[-1]]
            if price:
                value += shares * price
        if value > 0:
            degiro_monthly.append({"date": mo, "value": round(value, 2)})

    if not degiro_monthly:
        log.warning("portfolio_value_history: geen waarden berekend (prijzen ontbreken?).")
        return []

    # ── Stap 5: blend met portfolio_history (incl. TR+BUX) voor recente maanden ──
    mem     = _load_json(MEMORY_PATH, {})
    ph_list = mem.get(HISTORY_KEY, [])  # dagelijkse snapshots (nieuwste eerst)

    # Groepeer portfolio_history per maand: gebruik de laatste snapshot van die maand
    ph_by_month: dict[str, float] = {}
    for snap in ph_list:
        ym  = snap["date"][:7]
        tot = (snap.get("degiro") or 0) + (snap.get("tr") or 0) + (snap.get("bux") or 0)
        if tot > 0 and ym not in ph_by_month:
            ph_by_month[ym] = tot   # eerste match = meest recente dag in die maand

    result: list[dict] = []
    for item in degiro_monthly:
        mo    = item["date"]
        value = ph_by_month.get(mo, item["value"])   # portfolio_history wint als beschikbaar
        result.append({"date": mo, "value": value})

    log.info(f"portfolio_value_history: {len(result)} maanden berekend "
             f"({result[0]['date']} → {result[-1]['date']})")
    return result


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run_morning_briefing():
    log.info("=== NEXUS MORNING BRIEFING STARTING ===")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    client  = anthropic.Anthropic(api_key=api_key) if api_key else None

    log.info("Marktdata ophalen...")
    market = fetch_market_data()

    log.info("Nieuws ophalen (Google News RSS)...")
    news = fetch_news()

    log.info("NEXUS portfolio ophalen...")
    nexus = fetch_nexus_portfolio()

    log.info("DEGIRO portfolio ophalen...")
    degiro = fetch_degiro_portfolio()
    if degiro is None:
        log.info("DEGIRO API niet bereikbaar — probeer DEGIRO_HOLDINGS secret...")
        degiro = fetch_degiro_manual()

    log.info("Trade Republic portfolio ophalen...")
    tr = fetch_tr_portfolio()

    log.info("BUX portfolio ophalen...")
    bux = fetch_bux_manual()

    # Performance berekenen vanuit history
    history     = load_history()
    degiro_perf = compute_perf(history, degiro.get("total") if degiro else None, "degiro")
    tr_perf     = compute_perf(history, tr.get("total") if tr else None, "tr")
    bux_perf    = compute_perf(history, bux.get("total") if bux else None, "bux")

    # Snapshot + dashboard data opslaan
    save_snapshot(
        degiro_total=degiro.get("total") if degiro else None,
        tr_total=tr.get("total") if tr else None,
        bux_total=bux.get("total") if bux else None,
    )

    log.info("AI nieuws-samenvatting genereren...")
    news_summary = generate_news_summary(client, news) if client else []

    # Investment timeline: probeer API (degiro), anders CSV-export secret
    inv_timeline = degiro.get("investment_timeline") if degiro else []
    inv_first    = degiro.get("first_investment_date") if degiro else None
    if not inv_timeline:
        inv_timeline, inv_first = _parse_degiro_transactions_csv()

    log.info("Portfolio waarde geschiedenis berekenen...")
    pv_history = _build_portfolio_value_history()

    save_dashboard_data(news, degiro, tr, news_summary, bux,
                        investment_timeline=inv_timeline,
                        first_investment_date=inv_first,
                        portfolio_value_history=pv_history if pv_history else None)

    log.info("AI-briefing genereren...")
    ai_text = generate_ai_briefing(client, market, news, nexus) if client else "API key niet beschikbaar."

    msg = build_telegram_message(market, news, nexus, degiro, tr, degiro_perf, tr_perf, ai_text,
                                  news_summary, bux, bux_perf)

    log.info("Telegram verzenden...")
    ok = send(msg)
    log.info(f"Telegram: {'✓ verzonden' if ok else '✗ mislukt'}")
    log.info("=== MORNING BRIEFING COMPLETE ===")


if __name__ == "__main__":
    run_morning_briefing()
