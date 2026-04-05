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

def load_json(path, default):
    """Laadt JSON veilig; als het bestand corrupt is of mist, gebruik default."""
    if not path.exists():
        return default
    with open(path, "r") as f:
        try:
            return json.load(f)
        except:
            return default

def save_json(path, data):
    """Slaat data netjes op in JSON formaat."""
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

def run_evolution():
    print("--- NEXUS EVOLUTION ENGINE STARTING ---")
    
    # 1. Data laden (met extra veiligheidscheck voor 'lessons')
    data = load_json(DATA_PATH, {"top_candidates": [], "active_trades": [], "equity_history": []})
    memory = load_json(MEMORY_PATH, {"lessons": [], "total_profit_pct": 0})
    
    # Cruciale fix: zorg dat de 'lessons' lijst altijd bestaat in het geheugen
    if "lessons" not in memory:
        memory["lessons"] = []
    
    candidates = data.get("top_candidates", [])
    active_trades = data.get("active_trades", [])
    
    ENTRY_THRESHOLD    = 7.5   # Top kandidaten in een dure markt scoren 6-7; 7.5 is realistisch haalbaar
    STOP_LOSS_PCT      = -5.0  # Sluit positie bij >= 5% verlies
    TAKE_PROFIT_PCT    = 15.0  # Sluit positie bij >= 15% winst
    MAX_TRADES         = 5     # Maximaal 5 gelijktijdige posities
    MAX_PER_SECTOR     = 2     # Max 2 posities per sector (diversificatie)

    # 2. Check huidige performance van actieve trades + exit-logica
    updated_trades = []
    total_pl_pct = 0.0
    closed_count = 0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def add_lesson(ticker, sector, insight, lesson_type):
        lesson = {
            "date": today,
            "ticker": ticker,
            "sector": sector,
            "insight": insight,
            "type": lesson_type
        }
        if not any(l['ticker'] == ticker and l['date'] == today and l['type'] == lesson_type
                   for l in memory['lessons']):
            memory['lessons'].append(lesson)

    for trade in active_trades:
        try:
            ticker = trade['ticker']
            t_info = yf.Ticker(ticker).info
            current_price = t_info.get("currentPrice", trade['buy_price'])
            pl_pct = round(((current_price - trade['buy_price']) / trade['buy_price']) * 100, 2)
            sector = trade.get("industry_group", "Unknown")

            # Exit: stop-loss
            if pl_pct <= STOP_LOSS_PCT:
                add_lesson(ticker, sector,
                           f"Stop-loss geraakt op {ticker} ({pl_pct:.1f}%). Vermijd {sector} bij zwak sentiment.",
                           "NEGATIVE_LEARNING")
                notify_stop_loss(ticker, pl_pct, sector)
                print(f"STOP-LOSS: {ticker} gesloten op {pl_pct:.1f}%")
                closed_count += 1
                continue

            # Exit: take-profit — sla ook positief les op
            if pl_pct >= TAKE_PROFIT_PCT:
                add_lesson(ticker, sector,
                           f"Take-profit op {ticker} (+{pl_pct:.1f}%). {sector}-signalen werken goed.",
                           "POSITIVE_LEARNING")
                notify_take_profit(ticker, pl_pct, sector)
                print(f"TAKE-PROFIT: {ticker} gesloten op +{pl_pct:.1f}%")
                closed_count += 1
                continue

            # Vroegtijdige waarschuwing bij aanhoudend verlies
            if pl_pct < -3.0:
                add_lesson(ticker, sector,
                           f"Verlies op {ticker} ({pl_pct:.1f}%). Monitor {sector} nauwlettend.",
                           "NEGATIVE_LEARNING")
                notify_warning(ticker, pl_pct, sector)

            trade['current_price'] = current_price
            trade['pl_percent'] = pl_pct
            updated_trades.append(trade)
            total_pl_pct += pl_pct
        except Exception as e:
            print(f"Update fout voor {trade.get('ticker')}: {e}")
            updated_trades.append(trade)

    if closed_count:
        print(f"{closed_count} positie(s) gesloten via exit-logica.")

    # Gemiddelde P&L over actieve trades (correct gewogen)
    avg_pl_pct = (total_pl_pct / len(updated_trades)) if updated_trades else 0.0

    # 3. Nieuwe trades aangaan — met sectordiversificatie
    current_tickers = {t['ticker'] for t in updated_trades}
    sector_counts = {}
    for t in updated_trades:
        sg = t.get("industry_group", "Others")
        sector_counts[sg] = sector_counts.get(sg, 0) + 1

    # Bereken sectorbonus op basis van positieve lessen
    positive_sectors = set(
        l['sector'] for l in memory['lessons'] if l.get('type') == "POSITIVE_LEARNING"
    )

    for c in candidates:
        if len(updated_trades) >= MAX_TRADES:
            break
        sector = c.get("industry_group", "Others")
        score = c.get('score', 0)
        ticker = c['ticker']

        if ticker in current_tickers:
            continue
        if sector_counts.get(sector, 0) >= MAX_PER_SECTOR:
            print(f"Overgeslagen: {ticker} — max {MAX_PER_SECTOR} posities in {sector} bereikt.")
            continue
        # Geef sectoren met bewezen positieve track-record een bonus
        effective_threshold = ENTRY_THRESHOLD - (0.3 if sector in positive_sectors else 0)
        if score < effective_threshold:
            continue

        new_trade = {
            "ticker": ticker,
            "buy_price": c['price'],
            "current_price": c['price'],
            "buy_date": today,
            "industry_group": sector,
            "sector": c.get("sector", sector),
            "score_at_entry": score,
            "pl_percent": 0.0
        }
        updated_trades.append(new_trade)
        current_tickers.add(ticker)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        notify_trade_opened(ticker, c['price'], score, sector)
        print(f"Nieuwe trade: {ticker} | sector: {sector} | score: {score}")

    new_count = len(updated_trades) - (len(active_trades) - closed_count)

    # 4. Resultaten voorbereiden voor Dashboard
    data['active_trades'] = updated_trades
    # Toon laatste 8 lessen in data.json voor dashboard (gesorteerd: negatief eerst voor zichtbaarheid)
    sorted_lessons = sorted(memory['lessons'][-20:], key=lambda l: l.get('type', ''), reverse=True)
    data['memory'] = {
        "lessons": sorted_lessons[-8:],
        "positive_sectors": list(positive_sectors),
        "last_update": datetime.now(timezone.utc).isoformat()
    }
    
    # Equity Curve punt toevoegen — cumulatief op basis van vorig datapunt
    if "equity_history" not in data:
        data["equity_history"] = []
    equity_history = data["equity_history"]
    last_value = equity_history[-1]["value"] if equity_history else 10000.0
    # Pas de gemiddelde P&L toe op het vorige equity-niveau (compound effect)
    new_value = round(last_value * (1 + avg_pl_pct / 100), 2)
    history_point = {
        "date": datetime.now(timezone.utc).strftime("%m-%d %H:%M"),
        "value": new_value
    }
    equity_history.append(history_point)
    data["equity_history"] = equity_history[-20:]

    # Telegram dagrapport sturen
    notify_evolution_summary(
        active_trades=updated_trades,
        closed_count=closed_count,
        new_count=new_count,
        equity_value=new_value,
    )

    # Alles opslaan
    save_json(DATA_PATH, data)
    save_json(MEMORY_PATH, memory)
    print("--- EVOLUTION COMPLETE ---")

if __name__ == "__main__":
    run_evolution()
