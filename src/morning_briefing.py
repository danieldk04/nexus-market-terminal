"""
NEXUS Morning Briefing — Dagelijkse marktupdate (08:00 UTC)
─────────────────────────────────────────────────────────────
Marktdata : yfinance — AEX, S&P 500, NASDAQ, BTC, goud
NEXUS P&L : leest data.json (eigen portefeuille-tracker)
DEGIRO    : directe REST-login (geen externe library vereist)
Trade Rep : pytr library (optioneel; vereist eenmalige setup)
AI        : Claude Haiku — snelle, goedkope marktbrief
Output    : Telegram (hergebruikt bestaande notifier-token)
"""
import json
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import anthropic
import requests
import yfinance as yf

from notifier import send, DASHBOARD_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("morning_briefing")

BASE_DIR  = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"

# ─── MARKTINDICES ─────────────────────────────────────────────────────────────
INDICES = [
    ("S&P 500",  "^GSPC"),
    ("NASDAQ",   "^IXIC"),
    ("AEX",      "^AEX"),
    ("DAX",      "^GDAXI"),
    ("BTC/USD",  "BTC-USD"),
    ("Goud",     "GC=F"),
]


def _pct_arrow(pct: float) -> str:
    if pct >= 1.5:  return "🚀"
    if pct >= 0.5:  return "📈"
    if pct >= 0.0:  return "🔼"
    if pct >= -0.5: return "🔽"
    if pct >= -1.5: return "📉"
    return "💥"


def fetch_market_data() -> list[dict]:
    """Haal actuele koersen en dagwijziging op voor alle indices."""
    results = []
    for label, sym in INDICES:
        try:
            t    = yf.Ticker(sym)
            info = t.fast_info
            price = getattr(info, "last_price", None)
            prev  = getattr(info, "previous_close", None)
            if price and prev and prev > 0:
                pct = (price - prev) / prev * 100
                results.append({
                    "label": label,
                    "symbol": sym,
                    "price": price,
                    "pct": round(pct, 2),
                    "arrow": _pct_arrow(pct),
                })
                log.info(f"{label}: {price:.2f} ({pct:+.2f}%)")
            else:
                results.append({"label": label, "symbol": sym, "price": None, "pct": 0, "arrow": "❓"})
        except Exception as e:
            log.warning(f"Marktdata mislukt voor {sym}: {e}")
            results.append({"label": label, "symbol": sym, "price": None, "pct": 0, "arrow": "❓"})
        time.sleep(0.2)
    return results


# ─── NEXUS PORTEFEUILLE (data.json) ──────────────────────────────────────────

def fetch_nexus_portfolio() -> dict:
    """Laad de eigen NEXUS gesimuleerde portefeuille uit data.json."""
    if not DATA_PATH.exists():
        return {}
    try:
        with open(DATA_PATH) as f:
            data = json.load(f)

        trades  = data.get("active_trades", [])
        port    = data.get("portfolio", {})
        equity  = port.get("cash", 0)
        start   = port.get("starting_capital", 10_000)

        positions = []
        total_pl  = 0.0
        for t in trades:
            pl = t.get("pl_percent", 0)
            total_pl += pl
            positions.append({
                "ticker": t["ticker"],
                "sector": t.get("sector", "?"),
                "pl":     round(pl, 2),
                "value":  t.get("current_value", 0),
            })

        avg_pl = round(total_pl / len(trades), 2) if trades else 0
        return {
            "positions": positions,
            "cash":      round(equity, 2),
            "start":     round(start, 2),
            "avg_pl":    avg_pl,
            "n":         len(trades),
        }
    except Exception as e:
        log.warning(f"NEXUS portfolio laden mislukt: {e}")
        return {}


# ─── DEGIRO REST API ──────────────────────────────────────────────────────────

DEGIRO_LOGIN_URL = "https://trader.degiro.nl/login/secure/login"
DEGIRO_CONFIG_URL = "https://trader.degiro.nl/pa/secure/client"
DEGIRO_PORT_URL   = "https://trader.degiro.nl/trading/secure/v5/update/{int_account}"


def _parse_degiro_secret() -> tuple[str | None, str | None]:
    """
    Leest gebruikersnaam en wachtwoord uit één GitHub Secret genaamd DEGIRO.
    Verwacht formaat (twee regels):
        DEGIRO_USERNAME danieldk04
        DEGIRO_PASSWORD mijnwachtwoord
    Valt terug op losse env vars DEGIRO_USERNAME / DEGIRO_PASSWORD.
    """
    raw = os.environ.get("DEGIRO", "")
    parsed: dict[str, str] = {}
    for line in raw.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2:
            parsed[parts[0]] = parts[1]
    username = parsed.get("DEGIRO_USERNAME") or os.environ.get("DEGIRO_USERNAME")
    password = parsed.get("DEGIRO_PASSWORD") or os.environ.get("DEGIRO_PASSWORD")
    return username, password


def fetch_degiro_portfolio() -> dict | None:
    """
    Login bij DEGIRO via REST en haal portefeuille op.
    Leest uit één secret DEGIRO (twee regels: DEGIRO_USERNAME / DEGIRO_PASSWORD).
    Optioneel: DEGIRO_INT_ACCOUNT als losse secret voor snellere login.
    """
    username, password = _parse_degiro_secret()
    if not username or not password:
        log.info("DEGIRO credentials niet beschikbaar — sectie overgeslagen.")
        return None

    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})

    # 1. Login
    try:
        r = session.post(
            DEGIRO_LOGIN_URL,
            json={"username": username, "password": password, "isRedirectToMobile": False},
            timeout=15,
        )
        if not r.ok:
            log.warning(f"DEGIRO login mislukt: {r.status_code}")
            return None
        session_id = r.json().get("sessionId")
        if not session_id:
            log.warning("DEGIRO: geen sessionId ontvangen.")
            return None
        session.headers.update({"Cookie": f"JSESSIONID={session_id}"})
        log.info("DEGIRO: ingelogd.")
    except Exception as e:
        log.warning(f"DEGIRO login fout: {e}")
        return None

    # 2. Account-ID ophalen
    int_account = os.environ.get("DEGIRO_INT_ACCOUNT")
    if not int_account:
        try:
            r = session.get(DEGIRO_CONFIG_URL, params={"sessionId": session_id}, timeout=10)
            int_account = r.json().get("data", {}).get("intAccount")
        except Exception:
            log.warning("DEGIRO: int_account ophalen mislukt.")
            return None

    # 3. Portfolio
    try:
        r = session.get(
            DEGIRO_PORT_URL.format(int_account=int_account),
            params={"sessionId": session_id, "portfolio": 0},
            timeout=15,
        )
        if not r.ok:
            log.warning(f"DEGIRO portfolio mislukt: {r.status_code}")
            return None
        raw   = r.json().get("portfolio", {}).get("value", [])
        total = 0.0
        items = []
        for entry in raw:
            vals = {v["name"]: v.get("value") for v in entry.get("value", [])}
            name  = vals.get("productId", "?")
            size  = vals.get("size", 0) or 0
            price = vals.get("price", 0) or 0
            value = vals.get("value", size * price) or 0
            pl    = vals.get("breakEvenPrice")  # not always present
            if size and size > 0:
                items.append({"name": str(name), "size": size, "value": round(value, 2)})
                total += value
        log.info(f"DEGIRO: {len(items)} posities, totaal €{total:.2f}")
        return {"positions": items, "total": round(total, 2)}
    except Exception as e:
        log.warning(f"DEGIRO portfolio fout: {e}")
        return None


# ─── TRADE REPUBLIC (pytr) ────────────────────────────────────────────────────

def fetch_tr_portfolio() -> dict | None:
    """
    Trade Republic portfolio via pytr library.
    Vereist: TR_PHONE (+31612345678), TR_PIN (4 cijfers)
    ⚠️  Eerste login vereist SMS-verificatie — doe dit eenmalig lokaal,
         commit de sessie NIET naar git (staat in .gitignore).
    In GitHub Actions werkt dit alleen als de sessie gecached is.
    """
    phone = os.environ.get("TR_PHONE")
    pin   = os.environ.get("TR_PIN")
    if not phone or not pin:
        log.info("TR credentials niet beschikbaar — sectie overgeslagen.")
        return None

    try:
        from pytr.api import TradeRepublicApi  # type: ignore
        api = TradeRepublicApi(phone=phone, pin=pin, locale="nl")
        api.login()
        portfolio = api.get_portfolio()
        items  = []
        total  = 0.0
        for pos in portfolio.get("positions", []):
            name  = pos.get("name", pos.get("instrumentId", "?"))
            qty   = pos.get("quantity", 0)
            price = pos.get("currentPrice", 0)
            value = qty * price
            items.append({"name": name, "size": qty, "value": round(value, 2)})
            total += value
        log.info(f"Trade Republic: {len(items)} posities, totaal €{total:.2f}")
        return {"positions": items, "total": round(total, 2)}
    except ImportError:
        log.info("pytr niet geïnstalleerd — Trade Republic overgeslagen.")
        return None
    except Exception as e:
        log.warning(f"Trade Republic fout: {e}")
        return None


# ─── NIEUWS HEADLINES VIA YFINANCE ───────────────────────────────────────────

def fetch_market_news(n: int = 8) -> list[str]:
    """Haal headlines op via yfinance (gecombineerde tickers)."""
    headlines = []
    seen      = set()
    for sym in ["SPY", "QQQ", "BTC-USD", "^AEX"]:
        try:
            news = yf.Ticker(sym).news or []
            for item in news[:4]:
                title = item.get("title") or item.get("headline", "")
                if title and title not in seen:
                    seen.add(title)
                    headlines.append(title)
        except Exception:
            pass
        if len(headlines) >= n:
            break
    return headlines[:n]


# ─── CLAUDE MARKTBRIEF ────────────────────────────────────────────────────────

def generate_ai_briefing(client: anthropic.Anthropic, market: list[dict],
                         news: list[str], nexus: dict) -> str:
    """Vraag Claude Haiku om een beknopte, scherpe marktbrief."""
    market_lines = "\n".join(
        f"  {m['label']}: {m['price']:.2f} ({m['pct']:+.2f}%)" if m["price"] else f"  {m['label']}: n/b"
        for m in market
    )
    news_lines = "\n".join(f"  • {h}" for h in news) if news else "  Geen nieuws beschikbaar."
    nexus_lines = ""
    if nexus.get("positions"):
        nexus_lines = (
            f"NEXUS eigen portefeuille ({nexus['n']} posities, gem. P&L {nexus['avg_pl']:+.1f}%):\n"
            + "\n".join(f"  {p['ticker']} ({p['sector']}) {p['pl']:+.1f}%" for p in nexus["positions"][:6])
        )

    prompt = (
        f"Schrijf een scherpe, professionele Nederlandse marktbrief (max 200 woorden) "
        f"voor een waardebelegger. Stijl: direct, informatief, analytisch — geen wollig taalgebruik.\n\n"
        f"MARKTEN VANDAAG:\n{market_lines}\n\n"
        f"RECENTE HEADLINES:\n{news_lines}\n\n"
        f"{nexus_lines}\n\n"
        f"Structuur (geen markdown-headers, gewone alinea's):\n"
        f"1. Marktsfeer in één zin\n"
        f"2. Wat drijft de markt vandaag? (macro, sector, event)\n"
        f"3. Één concrete watchlist-observatie voor de belegger\n"
        f"4. Korte risicowaarschuwing of kans"
    )

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=400,
            system=(
                "Je bent een senior marktstrateeg bij een Europees hedgefund. "
                "Je schrijft dagelijks een morning note voor partners. "
                "Scherp, bondig, zonder open deuren."
            ),
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        log.warning(f"Claude briefing mislukt: {e}")
        return "AI-samenvatting niet beschikbaar."


# ─── TELEGRAM OPMAAK ──────────────────────────────────────────────────────────

def _fmt_portfolio_block(label: str, data: dict | None) -> str:
    if not data or not data.get("positions"):
        return ""
    items = data["positions"][:8]
    lines = [f"💼 *{label}*"]
    for p in items:
        name  = p.get("name") or p.get("ticker", "?")
        val   = p.get("value", 0)
        size  = p.get("size", "")
        size_s = f" · {size}×" if size else ""
        lines.append(f"  `{name}`{size_s} €{val:,.0f}")
    if data.get("total"):
        lines.append(f"  ─ Totaal: *€{data['total']:,.0f}*")
    return "\n".join(lines)


def build_telegram_message(market: list[dict], news: list[str],
                            nexus: dict, degiro: dict | None,
                            tr: dict | None, ai_text: str) -> str:
    now = datetime.now(timezone.utc)
    day_nl = ["maandag","dinsdag","woensdag","donderdag","vrijdag","zaterdag","zondag"][now.weekday()]
    date_str = f"{day_nl} {now.day} {['jan','feb','mrt','apr','mei','jun','jul','aug','sep','okt','nov','dec'][now.month-1]} {now.year}"

    # Markten
    market_lines = []
    for m in market:
        if m["price"] is None:
            market_lines.append(f"  {m['arrow']} {m['label']}: _n/b_")
            continue
        sym = m["symbol"]
        # Prijsformat: crypto en goud met 2 decimalen, indices zonder decimalen
        if m["price"] < 1000:
            price_str = f"{m['price']:,.2f}"
        else:
            price_str = f"{m['price']:,.0f}"
        market_lines.append(
            f"  {m['arrow']} *{m['label']}* `{price_str}` ({m['pct']:+.2f}%)"
        )

    # NEXUS posities
    nexus_block = ""
    if nexus.get("positions"):
        pos_lines = []
        for p in nexus["positions"][:6]:
            sign = "+" if p["pl"] >= 0 else ""
            pos_lines.append(f"  `{p['ticker']}` {sign}{p['pl']:.1f}%")
        nexus_block = (
            f"\n🤖 *NEXUS TRACKER* ({nexus['n']} posities)\n"
            + "\n".join(pos_lines)
            + f"\n  ─ Gem. P&L: *{nexus['avg_pl']:+.1f}%*"
        )

    # Externe portefeuilles
    ext_blocks = []
    db = _fmt_portfolio_block("DEGIRO", degiro)
    tb = _fmt_portfolio_block("Trade Republic", tr)
    if db: ext_blocks.append(db)
    if tb: ext_blocks.append(tb)
    ext_section = ("\n\n" + "\n\n".join(ext_blocks)) if ext_blocks else ""

    msg = (
        f"🌅 *NEXUS MORNING BRIEFING*\n"
        f"_{date_str} · {now.strftime('%H:%M')} UTC_\n"
        f"{'─' * 30}\n\n"
        f"📊 *MARKTEN*\n"
        + "\n".join(market_lines)
        + nexus_block
        + ext_section
        + f"\n\n{'─' * 30}\n"
        f"🧠 *NEXUS AI ANALYSE*\n"
        f"{ai_text}\n\n"
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

    log.info("NEXUS portfolio ophalen...")
    nexus  = fetch_nexus_portfolio()

    log.info("Nieuws ophalen...")
    news   = fetch_market_news()

    log.info("DEGIRO portfolio ophalen...")
    degiro = fetch_degiro_portfolio()

    log.info("Trade Republic portfolio ophalen...")
    tr     = fetch_tr_portfolio()

    log.info("AI-briefing genereren...")
    if client:
        ai_text = generate_ai_briefing(client, market, news, nexus)
    else:
        ai_text = "ANTHROPIC_API_KEY niet beschikbaar."

    msg = build_telegram_message(market, news, nexus, degiro, tr, ai_text)

    log.info("Telegram verzenden...")
    ok  = send(msg)
    log.info(f"Telegram: {'✓ verzonden' if ok else '✗ mislukt'}")
    log.info("=== MORNING BRIEFING COMPLETE ===")


if __name__ == "__main__":
    run_morning_briefing()
