"""
NEXUS Quant Bot — Telegram Notifier
Chat ID : 7995706133  (Nexus_Quant_Bot)
Token   : sla op als GitHub Secret -> TELEGRAM_BOT_TOKEN
"""

import json
import os
import requests
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("notifier")

BASE_DIR = Path(__file__).parent.parent

TELEGRAM_CHAT_ID = "7995706133"
TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"
DASHBOARD_URL    = "https://danieldk04.github.io/nexus-market-terminal/pro.html"


def _token():
    return os.environ.get("TELEGRAM_BOT_TOKEN")


def _now():
    """Geeft huidige UTC-tijd als string — buiten f-string om Python 3.10 compatibel te blijven."""
    return datetime.now(timezone.utc).strftime("%d-%m %H:%M UTC")


def _now_long():
    return datetime.now(timezone.utc).strftime("%d-%m-%Y %H:%M UTC")


def _send_single(message: str, parse_mode: str) -> bool:
    """Verstuur één Telegram-bericht (max 4096 tekens)."""
    token = _token()
    if not token:
        log.warning("TELEGRAM_BOT_TOKEN niet gevonden — melding overgeslagen.")
        return False
    try:
        resp = requests.post(
            TELEGRAM_API_URL.format(token=token),
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            },
            timeout=8,
        )
        if not resp.ok:
            log.error("Telegram fout %s: %s", resp.status_code, resp.text[:120])
            return False
        return True
    except Exception as e:
        log.error("Telegram verbindingsfout: %s", e)
        return False


def send(message, parse_mode="Markdown"):
    """Stuur een bericht naar Nexus_Quant_Bot. Splitst automatisch bij > 4000 tekens."""
    if len(message) <= 4000:
        return _send_single(message, parse_mode)
    # Splits op laatste newline vóór de grens zodat opmaakblokken heel blijven
    split = message.rfind("\n", 0, 4000)
    if split == -1:
        split = 4000
    ok1 = _send_single(message[:split], parse_mode)
    ok2 = send(message[split:].lstrip("\n"), parse_mode)
    log.info("Lang bericht gesplitst in 2 delen (totaal %d tekens)", len(message))
    return ok1 and ok2


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _fetch_brief_news(n: int = 3) -> list[str]:
    """Haal n actuele headlines op via Google News RSS."""
    feeds = [
        "https://news.google.com/rss/search?q=beurs+aandelen&hl=nl&gl=NL&ceid=NL:nl",
        "https://news.google.com/rss/search?q=stock+market&hl=en&gl=US&ceid=US:en",
    ]
    headlines, seen = [], set()
    for url in feeds:
        try:
            r    = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            root = ET.fromstring(r.text)
            for item in root.iter("item"):
                title = item.findtext("title", "").strip().rsplit(" - ", 1)[0].strip()
                if title and title not in seen and len(title) > 12:
                    seen.add(title)
                    headlines.append(title)
                if len(headlines) >= n:
                    return headlines
        except Exception:
            pass
    return headlines[:n]


def _load_portfolio_snapshot() -> dict | None:
    """Lees de meest recente portfolio-snapshot uit memory.json (geschreven door morning briefing)."""
    try:
        mem_path = BASE_DIR / "memory.json"
        if not mem_path.exists():
            return None
        with open(mem_path) as f:
            mem = json.load(f)
        history = mem.get("portfolio_history", [])
        return history[0] if history else None   # Gesorteerd nieuwste-eerst
    except Exception:
        return None


# ─── Kant-en-klare berichten ─────────────────────────────────────────────────

def notify_scan_complete(candidates, scanned):
    if not candidates:
        return
    top = candidates[:5]
    lines = []
    for i, c in enumerate(top):
        group  = c.get("industry_group", "?")
        score  = c.get("score", "?")
        dcf    = c.get("dcf") or {}
        upside = dcf.get("dcf_upside")
        upside_s = f" · DCF `{upside:+.0f}%`" if upside is not None else ""
        lines.append("  {}. *{}* — score `{}`{} | {}".format(
            i + 1, c["ticker"], score, upside_s, group))
    candidate_list = "\n".join(lines)

    # Sector-verdeling top 10
    from collections import Counter
    sector_dist = Counter(c.get("industry_group", "?") for c in candidates[:10])
    sector_line = " · ".join(f"{s}: {n}" for s, n in sector_dist.most_common(4))

    msg = (
        "🔍 *NEXUS SCAN KLAAR* — {ts}\n"
        "📊 {scanned} tickers gescand · {n} kandidaten\n\n"
        "🏆 *Top 5:*\n"
        "{candidates}\n\n"
        "📂 Sectoren top 10: _{sector_line}_\n\n"
        "🌐 [Open Dashboard]({url})"
    ).format(
        ts=_now(), scanned=scanned, n=len(candidates),
        candidates=candidate_list, sector_line=sector_line,
        url=DASHBOARD_URL,
    )
    send(msg)


def notify_trade_opened(ticker, price, score, sector):
    ts = _now()
    msg = (
        "🟢 *TRADE GEOPEND*\n"
        "📈 `{ticker}` · {sector}\n"
        "💰 Instapprijs: `${price:.2f}`\n"
        "⭐ Score bij instap: `{score}/10`\n"
        "🕒 {ts}\n\n"
        "🌐 [Open Dashboard]({url})"
    ).format(ticker=ticker, sector=sector, price=price, score=score, ts=ts, url=DASHBOARD_URL)
    send(msg)


def notify_stop_loss(ticker, pl_pct, sector):
    ts = _now()
    msg = (
        "🔴 *STOP-LOSS GERAAKT*\n"
        "📉 `{ticker}` gesloten\n"
        "💸 Verlies: `{pl:.1f}%`\n"
        "🧠 Les opgeslagen voor sector: _{sector}_\n"
        "🕒 {ts}\n\n"
        "🌐 [Open Dashboard]({url})"
    ).format(ticker=ticker, pl=pl_pct, sector=sector, ts=ts, url=DASHBOARD_URL)
    send(msg)


def notify_take_profit(ticker, pl_pct, sector):
    ts = _now()
    msg = (
        "💰 *TAKE-PROFIT!*\n"
        "📈 `{ticker}` gesloten\n"
        "✅ Winst: `+{pl:.1f}%`\n"
        "🧠 Positive les opgeslagen voor sector: _{sector}_\n"
        "🕒 {ts}\n\n"
        "🌐 [Open Dashboard]({url})"
    ).format(ticker=ticker, pl=pl_pct, sector=sector, ts=ts, url=DASHBOARD_URL)
    send(msg)


def notify_warning(ticker, pl_pct, sector):
    msg = (
        "⚠️ *WAARSCHUWING*\n"
        "`{ticker}` nadert stop-loss grens\n"
        "📉 Huidig verlies: `{pl:.1f}%`\n"
        "Sector: _{sector}_\n\n"
        "🌐 [Open Dashboard]({url})"
    ).format(ticker=ticker, pl=pl_pct, sector=sector, url=DASHBOARD_URL)
    send(msg)


def notify_evolution_summary(active_trades, closed_count, new_count, equity_value):
    n = len(active_trades)
    if n > 0:
        avg_pl = sum(t.get("pl_percent", 0) for t in active_trades) / n
    else:
        avg_pl = 0.0

    pl_emoji = "📈" if avg_pl >= 0 else "📉"
    pl_sign  = "+" if avg_pl >= 0 else ""
    pl_str   = "{}{:.2f}%".format(pl_sign, avg_pl)

    # Posities gesorteerd op P&L
    pos_lines = ""
    if active_trades:
        sorted_trades = sorted(active_trades, key=lambda t: t.get("pl_percent", 0), reverse=True)
        rows = []
        for t in sorted_trades:
            pl   = t.get("pl_percent", 0)
            sign = "+" if pl >= 0 else ""
            emoji = "🟢" if pl >= 0 else "🔴"
            rows.append("  {} `{}` {}{:.1f}%".format(emoji, t["ticker"], sign, pl))
        pos_lines = "\n*Posities:*\n" + "\n".join(rows)

    ts = _now_long()
    msg = (
        "📊 *NEXUS DAGRAPPORT*\n"
        "🕒 {ts}\n\n"
        "📌 Actieve posities: `{n}`\n"
        "{pl_emoji} Gem. P&L: `{pl_str}`\n"
        "💼 NEXUS waarde: `€{equity:,.2f}`\n"
        "🔒 Gesloten vandaag: `{closed}`\n"
        "🆕 Nieuw geopend: `{new}`"
        "{pos_lines}\n\n"
        "🌐 [Open Dashboard]({url})"
    ).format(
        ts=ts, n=n, pl_emoji=pl_emoji, pl_str=pl_str,
        equity=equity_value, closed=closed_count, new=new_count,
        pos_lines=pos_lines, url=DASHBOARD_URL,
    )
    send(msg)
