import json
import yfinance as yf
from datetime import datetime, timezone
from pathlib import Path

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
    
    ENTRY_THRESHOLD  = 8.0   # Lagere drempel: mathematisch haalbaar met huidige scoringformule
    STOP_LOSS_PCT    = -5.0  # Sluit positie bij >= 5% verlies
    TAKE_PROFIT_PCT  = 15.0  # Sluit positie bij >= 15% winst

    # 2. Check huidige performance van actieve trades + exit-logica
    updated_trades = []
    total_pl_pct = 0.0
    closed_count = 0

    for trade in active_trades:
        try:
            ticker = trade['ticker']
            t_info = yf.Ticker(ticker).info
            current_price = t_info.get("currentPrice", trade['buy_price'])

            pl_pct = round(((current_price - trade['buy_price']) / trade['buy_price']) * 100, 2)

            # Exit-logica: stop-loss of take-profit
            if pl_pct <= STOP_LOSS_PCT:
                lesson = {
                    "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "ticker": ticker,
                    "sector": trade.get("industry_group", "Unknown"),
                    "insight": f"Stop-loss geraakt op {ticker} ({pl_pct:.1f}%). Sector sentiment mogelijk zwak.",
                    "type": "NEGATIVE_LEARNING"
                }
                if not any(l['ticker'] == ticker and l['date'] == lesson['date'] for l in memory['lessons']):
                    memory['lessons'].append(lesson)
                print(f"STOP-LOSS: {ticker} gesloten op {pl_pct:.1f}%")
                closed_count += 1
                continue  # Positie niet toevoegen aan updated_trades = sluiten

            if pl_pct >= TAKE_PROFIT_PCT:
                print(f"TAKE-PROFIT: {ticker} gesloten op {pl_pct:.1f}%")
                closed_count += 1
                continue

            # Zelfreflectie bij aanhoudend verlies (maar nog geen stop-loss)
            if pl_pct < -3.0:
                lesson = {
                    "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "ticker": ticker,
                    "sector": trade.get("industry_group", "Unknown"),
                    "insight": f"Verlies op {ticker} ({pl_pct:.1f}%). Sector sentiment mogelijk zwak.",
                    "type": "NEGATIVE_LEARNING"
                }
                if not any(l['ticker'] == ticker and l['date'] == lesson['date'] for l in memory['lessons']):
                    memory['lessons'].append(lesson)

            trade['current_price'] = current_price
            trade['pl_percent'] = pl_pct
            updated_trades.append(trade)
            total_pl_pct += pl_pct
        except Exception as e:
            print(f"Update fout voor {trade.get('ticker')}: {e}")
            updated_trades.append(trade)

    if closed_count:
        print(f"{closed_count} positie(s) gesloten via exit-logica.")

    # Gemiddelde P&L over actieve trades (voorkomt inflatie bij veel posities)
    avg_pl_pct = (total_pl_pct / len(updated_trades)) if updated_trades else 0.0

    # 3. Nieuwe trades aangaan (drempel verlaagd naar 8.0 — mathematisch haalbaar)
    current_tickers = [t['ticker'] for t in updated_trades]
    for c in candidates:
        if c.get('score', 0) >= ENTRY_THRESHOLD and c['ticker'] not in current_tickers and len(updated_trades) < 5:
            new_trade = {
                "ticker": c['ticker'],
                "buy_price": c['price'],
                "current_price": c['price'],
                "buy_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "industry_group": c.get("industry_group", "Others"),
                "pl_percent": 0.0
            }
            updated_trades.append(new_trade)
            print(f"Nieuwe trade geopend: {c['ticker']} (score {c['score']})")

    # 4. Resultaten voorbereiden voor Dashboard
    data['active_trades'] = updated_trades
    data['memory'] = {
        "lessons": memory['lessons'][-5:], # Laatste 5 lessen tonen
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

    # Alles opslaan
    save_json(DATA_PATH, data)
    save_json(MEMORY_PATH, memory)
    print("--- EVOLUTION COMPLETE ---")

if __name__ == "__main__":
    run_evolution()
