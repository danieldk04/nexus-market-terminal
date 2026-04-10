"""
NEXUS Quant Bot — Telegram Notifier
Chat ID : 7995706133  (Nexus_Quant_Bot)
Token   : sla op als GitHub Secret -> TELEGRAM_BOT_TOKEN
"""

import os
import requests
import logging
from datetime import datetime, timezone

log = logging.getLogger("notifier")

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


def send(message, parse_mode="Markdown"):
    """Stuur een bericht naar Nexus_Quant_Bot. Geeft True terug bij succes."""
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


# ─── Kant-en-klare berichten ─────────────────────────────────────────────────

def notify_scan_complete(candidates, scanned):
    if not candidates:
        return
    top = candidates[:5]
    lines = []
    for i, c in enumerate(top):
        group = c.get("industry_group", "?")
        score = c.get("score", "?")
        lines.append("  {}. *{}* — score `{}` | {}".format(i + 1, c["ticker"], score, group))
    candidate_list = "\n".join(lines)
    msg = (
        "🔍 *NEXUS SCAN KLAAR* — {ts}\n"
        "📊 {scanned} tickers gescand · {n} kandidaten\n\n"
        "🏆 *Top kandidaten:*\n"
        "{candidates}\n\n"
        "🌐 [Open Dashboard]({url})"
    ).format(ts=_now(), scanned=scanned, n=len(candidates), candidates=candidate_list, url=DASHBOARD_URL)
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
    pl_sign = "+" if avg_pl >= 0 else ""
    pl_str = "{}{:.2f}%".format(pl_sign, avg_pl)

    pos_lines = ""
    if active_trades:
        rows = []
        for t in active_trades:
            pl = t.get("pl_percent", 0)
            sign = "+" if pl >= 0 else ""
            rows.append("  • `{}` {}{:.1f}%".format(t["ticker"], sign, pl))
        pos_lines = "\n*Posities:*\n" + "\n".join(rows)

    ts = _now_long()
    msg = (
        "📊 *NEXUS DAGRAPPORT*\n"
        "🕒 {ts}\n\n"
        "📌 Actieve posities: `{n}`\n"
        "{pl_emoji} Gem. P&L: `{pl_str}`\n"
        "💼 Portfolio waarde: `€{equity:,.2f}`\n"
        "🔒 Gesloten vandaag: `{closed}`\n"
        "🆕 Nieuw geopend: `{new}`"
        "{pos_lines}\n\n"
        "🌐 [Open Dashboard]({url})"
    ).format(
        ts=ts, n=n, pl_emoji=pl_emoji, pl_str=pl_str,
        equity=equity_value, closed=closed_count,
        new=new_count, pos_lines=pos_lines, url=DASHBOARD_URL,
    )
    send(msg)
