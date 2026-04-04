"""
NEXUS Quant Bot — Telegram Notifier
Chat ID : 7995706133  (Nexus_Quant_Bot)
Token   : sla op als GitHub Secret → TELEGRAM_BOT_TOKEN
"""

import os
import requests
import logging
from datetime import datetime, timezone

log = logging.getLogger("notifier")

TELEGRAM_CHAT_ID = "7995706133"
TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

def _token() -> str | None:
    return os.environ.get("TELEGRAM_BOT_TOKEN")

def send(message: str, parse_mode: str = "Markdown") -> bool:
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
            log.error(f"Telegram fout {resp.status_code}: {resp.text[:120]}")
            return False
        return True
    except Exception as e:
        log.error(f"Telegram verbindingsfout: {e}")
        return False


# ─── Kant-en-klare berichten ─────────────────────────────────────────────────

def notify_scan_complete(candidates: list, scanned: int) -> None:
    if not candidates:
        return
    top = candidates[:5]
    lines = "\n".join(
        f"  {i+1}\\. *{c['ticker']}* — score `{c['score']}` | {c.get('industry_group','?')}"
        for i, c in enumerate(top)
    )
    timestamp = datetime.now(timezone.utc).strftime("%d\\-%m %H:%M UTC")
    msg = (
        f"🔍 *NEXUS SCAN KLAAR* — {timestamp}\n"
        f"📊 {scanned} tickers gescand · {len(candidates)} kandidaten\n\n"
        f"🏆 *Top kandidaten:*\n{lines}"
    )
    send(msg)


def notify_trade_opened(ticker: str, price: float, score: float, sector: str) -> None:
    msg = (
        f"🟢 *TRADE GEOPEND*\n"
        f"📈 `{ticker}` · {sector}\n"
        f"💰 Instapprijs: `${price:.2f}`\n"
        f"⭐ Score bij instap: `{score}/10`\n"
        f"🕒 {datetime.now(timezone.utc).strftime('%d-%m %H:%M UTC')}"
    )
    send(msg)


def notify_stop_loss(ticker: str, pl_pct: float, sector: str) -> None:
    msg = (
        f"🔴 *STOP\\-LOSS GERAAKT*\n"
        f"📉 `{ticker}` gesloten\n"
        f"💸 Verlies: `{pl_pct:.1f}%`\n"
        f"🧠 Les opgeslagen voor sector: _{sector}_\n"
        f"🕒 {datetime.now(timezone.utc).strftime('%d-%m %H:%M UTC')}"
    )
    send(msg)


def notify_take_profit(ticker: str, pl_pct: float, sector: str) -> None:
    msg = (
        f"💰 *TAKE\\-PROFIT\\!*\n"
        f"📈 `{ticker}` gesloten\n"
        f"✅ Winst: `+{pl_pct:.1f}%`\n"
        f"🧠 Positive les opgeslagen voor sector: _{sector}_\n"
        f"🕒 {datetime.now(timezone.utc).strftime('%d-%m %H:%M UTC')}"
    )
    send(msg)


def notify_warning(ticker: str, pl_pct: float, sector: str) -> None:
    msg = (
        f"⚠️ *WAARSCHUWING*\n"
        f"`{ticker}` nadert stop\\-loss grens\n"
        f"📉 Huidig verlies: `{pl_pct:.1f}%`\n"
        f"Sector: _{sector}_"
    )
    send(msg)


def notify_evolution_summary(
    active_trades: list,
    closed_count: int,
    new_count: int,
    equity_value: float,
) -> None:
    n = len(active_trades)
    avg_pl = (
        sum(t.get("pl_percent", 0) for t in active_trades) / n
        if n else 0.0
    )
    pl_emoji = "📈" if avg_pl >= 0 else "📉"
    pl_str = f"+{avg_pl:.2f}%" if avg_pl >= 0 else f"{avg_pl:.2f}%"

    pos_lines = ""
    if active_trades:
        pos_lines = "\n".join(
            f"  • `{t['ticker']}` {'+' if t.get('pl_percent',0)>=0 else ''}{t.get('pl_percent',0):.1f}%"
            for t in active_trades
        )
        pos_lines = f"\n*Posities:*\n{pos_lines}"

    msg = (
        f"📊 *NEXUS DAGRAPPORT*\n"
        f"🕒 {datetime.now(timezone.utc).strftime('%d\\-%m\\-%Y %H:%M UTC')}\n\n"
        f"📌 Actieve posities: `{n}`\n"
        f"{pl_emoji} Gem\\. P&L: `{pl_str}`\n"
        f"💼 Portfolio waarde: `€{equity_value:,.2f}`\n"
        f"🔒 Gesloten vandaag: `{closed_count}`\n"
        f"🆕 Nieuw geopend: `{new_count}`"
        f"{pos_lines}"
    )
    send(msg)
