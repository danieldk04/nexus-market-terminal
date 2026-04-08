import json
import yfinance as yf
from datetime import datetime, timezone
from pathlib import Path
from notifier import (
    notify_stop_loss, notify_take_profit,
    notify_warning, notify_trade_opened,
    notify_evolution_summary,
)

# Paden instellen
BASE_DIR = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"
MEMORY_PATH = BASE_DIR / "memory.json"

# ─── PORTFOLIO CONFIGURATIE ──────────────────────────────────────────────────
STARTING_CAPITAL   = 10_000.0  # Virtueel startkapitaal in euro
ENTRY_THRESHOLD    = 6.5       # Minimale score om een positie te openen
ALLOC_LOW          = 0.10      # 10% van cash bij score 6.5–7.5
ALLOC_HIGH         = 0.20      # 20% van cash bij score > 7.5
STOP_LOSS_PCT      = -5.0      # Sluit positie bij >= 5% verlies
TAKE_PROFIT_PCT    = 15.0      # Sluit positie bij >= 15% winst
MAX_TRADES         = 5         # Maximaal 5 gelijktijdige posities
MAX_PER_SECTOR     = 2         # Max 2 posities per sector
VIX_BLOCK          = 35.0      # Geen nieuwe trades bij paniek (VIX > 35)
VIX_CAUTION        = 30.0      # Alleen high-conviction (score > 7.5) bij VIX 30–35


def load_json(path, default):
    if not path.exists():
        return default
    with open(path, "r") as f:
        try:
            return json.load(f)
        except Exception:
            return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=4)


def run_evolution():
    print("--- NEXUS EVOLUTION ENGINE STARTING ---")

    # ── 1. Data laden ────────────────────────────────────────────────────────
    data   = load_json(DATA_PATH,   {"top_candidates": [], "active_trades": [], "equity_history": []})
    memory = load_json(MEMORY_PATH, {"lessons": [], "version": "nexus-v2"})
    if "lessons" not in memory:
        memory["lessons"] = []

    candidates    = data.get("top_candidates", [])
    active_trades = data.get("active_trades", [])
    macro         = data.get("macro", {})
    vix           = macro.get("vix") or 0.0

    # ── Portfolio state ──────────────────────────────────────────────────────
    portfolio  = data.get("portfolio", {"cash": STARTING_CAPITAL, "starting_capital": STARTING_CAPITAL})
    cash       = portfolio.get("cash", STARTING_CAPITAL)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def add_lesson(ticker, sector, insight, lesson_type):
        lesson = {"date": today, "ticker": ticker, "sector": sector,
                  "insight": insight, "type": lesson_type}
        if not any(l["ticker"] == ticker and l["date"] == today and l["type"] == lesson_type
                   for l in memory["lessons"]):
            memory["lessons"].append(lesson)

    # ── 2. Actieve trades updaten + exit-logica ───────────────────────────────
    updated_trades = []
    closed_count   = 0

    for trade in active_trades:
        try:
            ticker      = trade["ticker"]
            t_info      = yf.Ticker(ticker).info
            cur_price   = t_info.get("currentPrice", trade["buy_price"])
            pl_pct      = round(((cur_price - trade["buy_price"]) / trade["buy_price"]) * 100, 2)
            sector      = trade.get("industry_group", "Unknown")
            shares      = trade.get("shares", 0)
            cur_value   = round(shares * cur_price, 2) if shares else trade.get("position_value", 0)

            # Exit: stop-loss
            if pl_pct <= STOP_LOSS_PCT:
                cash += cur_value
                add_lesson(ticker, sector,
                           "Stop-loss geraakt op {} ({:.1f}%). Vermijd {} bij zwak sentiment.".format(
                               ticker, pl_pct, sector),
                           "NEGATIVE_LEARNING")
                notify_stop_loss(ticker, pl_pct, sector)
                print("STOP-LOSS: {} gesloten op {:.1f}% | teruggestort: €{:.2f}".format(
                    ticker, pl_pct, cur_value))
                closed_count += 1
                continue

            # Exit: take-profit
            if pl_pct >= TAKE_PROFIT_PCT:
                cash += cur_value
                add_lesson(ticker, sector,
                           "Take-profit op {} (+{:.1f}%). {}-signalen werken goed.".format(
                               ticker, pl_pct, sector),
                           "POSITIVE_LEARNING")
                notify_take_profit(ticker, pl_pct, sector)
                print("TAKE-PROFIT: {} gesloten op +{:.1f}% | teruggestort: €{:.2f}".format(
                    ticker, pl_pct, cur_value))
                closed_count += 1
                continue

            # Vroegtijdige waarschuwing
            if pl_pct < -3.0:
                add_lesson(ticker, sector,
                           "Verlies op {} ({:.1f}%). Monitor {} nauwlettend.".format(
                               ticker, pl_pct, sector),
                           "NEGATIVE_LEARNING")
                notify_warning(ticker, pl_pct, sector)

            trade["current_price"]   = cur_price
            trade["current_value"]   = cur_value
            trade["pl_percent"]      = pl_pct
            updated_trades.append(trade)
        except Exception as e:
            print("Update fout voor {}: {}".format(trade.get("ticker"), e))
            updated_trades.append(trade)

    if closed_count:
        print("{} positie(s) gesloten.".format(closed_count))

    # ── 3. VIX-filter bepalen ────────────────────────────────────────────────
    vix_str = "VIX={:.1f}".format(vix) if vix else "VIX=onbekend"
    if vix >= VIX_BLOCK:
        print("MARKTPAUZE: {} > {} — geen nieuwe trades.".format(vix_str, VIX_BLOCK))
        vix_min_score = 999  # Geen enkele trade
    elif vix >= VIX_CAUTION:
        print("VOORZICHT: {} — alleen high-conviction trades (score > 7.5).".format(vix_str))
        vix_min_score = 7.5
    else:
        print("MARKT OK: {} — normaal handelen.".format(vix_str))
        vix_min_score = ENTRY_THRESHOLD

    # ── 4. Nieuwe trades openen — position sizing + sectordiversificatie ─────
    positive_sectors = {l["sector"] for l in memory["lessons"] if l.get("type") == "POSITIVE_LEARNING"}

    current_tickers = {t["ticker"] for t in updated_trades}
    sector_counts   = {}
    for t in updated_trades:
        sg = t.get("industry_group", "Others")
        sector_counts[sg] = sector_counts.get(sg, 0) + 1

    new_count = 0
    for c in candidates:
        if len(updated_trades) >= MAX_TRADES or cash < 100:
            break

        sector = c.get("industry_group", "Others")
        score  = c.get("score", 0)
        ticker = c["ticker"]
        price  = c.get("price", 0)

        if ticker in current_tickers or price <= 0:
            continue
        if sector_counts.get(sector, 0) >= MAX_PER_SECTOR:
            print("Overgeslagen: {} — max {}/{} in {}.".format(ticker, MAX_PER_SECTOR, MAX_PER_SECTOR, sector))
            continue

        # Score-drempel rekening houdend met VIX en sectorbonus
        effective_threshold = vix_min_score - (0.3 if sector in positive_sectors else 0)
        if score < effective_threshold:
            continue

        # Position sizing: 10% of 20% van beschikbaar cash
        alloc_pct      = ALLOC_HIGH if score > 7.5 else ALLOC_LOW
        position_value = round(min(cash * alloc_pct, cash), 2)
        shares         = round(position_value / price, 4)

        new_trade = {
            "ticker":         ticker,
            "buy_price":      price,
            "current_price":  price,
            "current_value":  position_value,
            "buy_date":       today,
            "industry_group": sector,
            "sector":         c.get("sector", sector),
            "score_at_entry": score,
            "allocation_pct": alloc_pct,
            "position_value": position_value,
            "shares":         shares,
            "pl_percent":     0.0,
        }
        cash -= position_value
        updated_trades.append(new_trade)
        current_tickers.add(ticker)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        new_count += 1
        notify_trade_opened(ticker, price, score, sector)
        print("Nieuwe trade: {} | score: {} | alloc: {}% | €{:.2f} ({} aandelen)".format(
            ticker, score, int(alloc_pct * 100), position_value, shares))

    # ── 5. Portfolio waarde berekenen ────────────────────────────────────────
    open_value     = sum(t.get("current_value", t.get("position_value", 0)) for t in updated_trades)
    portfolio_value = round(cash + open_value, 2)

    # ── 6. Equity curve (cumulatief) ─────────────────────────────────────────
    if "equity_history" not in data:
        data["equity_history"] = []
    equity_history = data["equity_history"]
    equity_history.append({
        "date":  datetime.now(timezone.utc).strftime("%m-%d %H:%M"),
        "value": portfolio_value,
    })
    data["equity_history"] = equity_history[-30:]

    # ── 7. Opslaan ───────────────────────────────────────────────────────────
    sorted_lessons = sorted(memory["lessons"][-20:], key=lambda l: l.get("type", ""), reverse=True)
    data["active_trades"] = updated_trades
    data["portfolio"]     = {"cash": round(cash, 2), "starting_capital": STARTING_CAPITAL}
    data["memory"]        = {
        "lessons":          sorted_lessons[-8:],
        "positive_sectors": list(positive_sectors),
        "last_update":      datetime.now(timezone.utc).isoformat(),
    }

    notify_evolution_summary(
        active_trades=updated_trades,
        closed_count=closed_count,
        new_count=new_count,
        equity_value=portfolio_value,
    )

    save_json(DATA_PATH, data)
    save_json(MEMORY_PATH, memory)

    print("Portfolio: €{:.2f} | Cash: €{:.2f} | Posities: {}".format(
        portfolio_value, cash, len(updated_trades)))
    print("--- EVOLUTION COMPLETE ---")


if __name__ == "__main__":
    run_evolution()
