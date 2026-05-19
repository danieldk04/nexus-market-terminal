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


def save_snapshot(degiro_total: float | None, tr_total: float | None, nexus_total: float | None):
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
        "nexus":  nexus_total,
    })
    # Bewaar max MAX_HISTORY dagen, nieuwste eerst
    history  = sorted(history, key=lambda h: h["date"], reverse=True)[:MAX_HISTORY]
    mem[HISTORY_KEY] = history
    _save_json(MEMORY_PATH, mem)
    log.info(f"Snapshot opgeslagen: DEGIRO={degiro_total} TR={tr_total} NEXUS={nexus_total}")


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


def fetch_news(max_items: int = 8) -> list[str]:
    """Haal actuele headlines op via Google News RSS."""
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


# ─── NEXUS PORTFOLIO ──────────────────────────────────────────────────────────

def fetch_nexus_portfolio() -> dict:
    data = _load_json(DATA_PATH, {})
    trades = data.get("active_trades", [])
    port   = data.get("portfolio", {})
    cash   = port.get("cash", 0)

    positions, total_pl = [], 0.0
    for t in trades:
        pl = t.get("pl_percent", 0)
        total_pl += pl
        positions.append({"ticker": t["ticker"], "sector": t.get("sector", "?"), "pl": round(pl, 2)})

    positions.sort(key=lambda p: p["pl"], reverse=True)
    return {
        "positions": positions,
        "cash":      round(cash, 2),
        "avg_pl":    round(total_pl / len(trades), 2) if trades else 0,
        "n":         len(trades),
    }


# ─── DEGIRO ───────────────────────────────────────────────────────────────────

def _parse_degiro_secret() -> tuple[str | None, str | None]:
    raw = os.environ.get("DEGIRO", "")
    parsed: dict[str, str] = {}
    for line in raw.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2:
            parsed[parts[0]] = parts[1]
    username = parsed.get("DEGIRO_USERNAME") or os.environ.get("DEGIRO_USERNAME")
    password = parsed.get("DEGIRO_PASSWORD") or os.environ.get("DEGIRO_PASSWORD")
    return username, password


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
    session.headers.update({"Content-Type": "application/json"})

    # Login
    try:
        r = session.post(DEGIRO_LOGIN_URL,
                         json={"username": username, "password": password,
                               "isRedirectToMobile": False}, timeout=15)
        if not r.ok:
            log.warning(f"DEGIRO login mislukt: {r.status_code}")
            return None
        session_id = r.json().get("sessionId")
        if not session_id:
            log.warning("DEGIRO: geen sessionId.")
            return None
        session.headers.update({"Cookie": f"JSESSIONID={session_id}"})
        log.info("DEGIRO: ingelogd.")
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
        items.sort(key=lambda i: i["value"], reverse=True)
        log.info(f"DEGIRO: {len(items)} posities, totaal €{total:.2f}")
        return {"positions": items, "total": round(total, 2)}
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

def _perf_line(perf: dict) -> str:
    """Dag | Week | Maand | YTD als één regel."""
    parts = []
    for key, label in [("dag", "Dag"), ("week", "Week"), ("maand", "Maand"), ("ytd", "YTD")]:
        v = perf.get(key)
        parts.append(f"{label}: {_pct_str(v)}")
    return "  " + " · ".join(parts)


def _portfolio_block(label: str, data: dict | None, perf: dict) -> str:
    if not data:
        return ""
    lines = [f"💼 *{label}*  (€{data['total']:,.0f})"]
    if perf:
        lines.append(_perf_line(perf))
    for p in data["positions"][:6]:
        name  = p.get("name", "?")[:12]
        val   = p.get("value", 0)
        size  = p.get("size", "")
        size_s = f"{size}×" if size else ""
        lines.append(f"  `{name:<12}` {size_s:>5}  €{val:>8,.0f}")
    return "\n".join(lines)


def build_telegram_message(market, news, nexus, degiro, tr,
                            degiro_perf, tr_perf, ai_text) -> str:
    now     = datetime.now(timezone.utc)
    dag_nl  = ["ma","di","wo","do","vr","za","zo"][now.weekday()]
    mnd_nl  = ["jan","feb","mrt","apr","mei","jun","jul","aug","sep","okt","nov","dec"][now.month-1]
    date_s  = f"{dag_nl} {now.day} {mnd_nl} {now.year}"

    # Markten
    mkt_lines = []
    for m in market:
        if m["price"] is None:
            mkt_lines.append(f"  ❓ *{m['label']}*: n/b")
            continue
        p = m["price"]
        price_s = f"{p:,.0f}" if p >= 1000 else f"{p:,.2f}"
        mkt_lines.append(f"  {_arrow(m['pct'])} *{m['label']}* `{price_s}` ({m['pct']:+.2f}%)")

    # NEXUS posities
    nexus_lines = []
    if nexus.get("positions"):
        nexus_lines.append(f"\n🤖 *NEXUS TRACKER* ({nexus['n']} posities · gem. {nexus['avg_pl']:+.1f}%)")
        for p in nexus["positions"][:5]:
            nexus_lines.append(f"  {_arrow(p['pl'])} `{p['ticker']}` {p['pl']:+.1f}%")

    # Portfolios
    port_blocks = []
    db = _portfolio_block("DEGIRO", degiro, degiro_perf)
    tb = _portfolio_block("Trade Republic", tr, tr_perf)
    if db: port_blocks.append(db)
    if tb: port_blocks.append(tb)

    # Nieuws
    news_lines = ["📰 *NIEUWS*"] + [f"  • {h}" for h in news[:6]] if news else []

    sep = "─" * 28
    msg = (
        f"🌅 *NEXUS MORNING BRIEFING*\n"
        f"_{date_s} · {now.strftime('%H:%M')} UTC_\n"
        f"{sep}\n\n"
        f"📊 *MARKTEN*\n" + "\n".join(mkt_lines)
        + "".join(f"\n{b}" for b in ["\n".join(nexus_lines)] if b)
        + ("\n\n" + "\n\n".join(port_blocks) if port_blocks else "")
        + ("\n\n" + "\n".join(news_lines) if news_lines else "")
        + f"\n\n{sep}\n"
        f"🧠 *NEXUS AI ANALYSE*\n{ai_text}\n\n"
        f"🌐 [Open Dashboard]({DASHBOARD_URL})"
    )
    return msg


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

    log.info("Trade Republic portfolio ophalen...")
    tr = fetch_tr_portfolio()

    # Performance berekenen vanuit history
    history     = load_history()
    degiro_perf = compute_perf(history, degiro.get("total") if degiro else None, "degiro")
    tr_perf     = compute_perf(history, tr.get("total") if tr else None, "tr")

    # Snapshot opslaan VOOR de briefing zodat morgen al data beschikbaar is
    save_snapshot(
        degiro_total=degiro.get("total") if degiro else None,
        tr_total=tr.get("total") if tr else None,
        nexus_total=nexus.get("cash"),
    )

    log.info("AI-briefing genereren...")
    ai_text = generate_ai_briefing(client, market, news, nexus) if client else "API key niet beschikbaar."

    msg = build_telegram_message(market, news, nexus, degiro, tr, degiro_perf, tr_perf, ai_text)

    log.info("Telegram verzenden...")
    ok = send(msg)
    log.info(f"Telegram: {'✓ verzonden' if ok else '✗ mislukt'}")
    log.info("=== MORNING BRIEFING COMPLETE ===")


if __name__ == "__main__":
    run_morning_briefing()
