import json
import yfinance as yf
from datetime import datetime, timezone
from pathlib import Path

# Paden instellen
BASE_DIR = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"
MEMORY_PATH = BASE_DIR / "memory.json"

def load_json(path, default):
    if not path.exists():
        return default
    with open(path, "r") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

def run_evolution():
    print("--- NEXUS EVOLUTION ENGINE STARTING ---")
    
    # 1. Data laden
    data = load_json(DATA_PATH, {"top_candidates": [], "active_trades": [], "equity_history": []})
    memory = load_json(MEMORY_PATH, {"lessons": [], "total_profit_pct": 0})
    
    candidates = data.get("top_candidates", [])
    active_trades = data.get("active_trades", [])
    
    # 2. Check huidige performance van actieve trades
    updated_trades = []
    daily_performance = 0
    
    for trade in active_trades:
        try:
            ticker = trade['ticker']
            current_price = yf.Ticker(ticker).info.get("currentPrice", trade['buy_price'])
            pl_pct = round(((current_price - trade['buy_price']) / trade['buy_price']) * 100, 2)
            
            # Zelfreflectie: Als we meer dan 3% verliezen, leer een les
            if pl_pct < -3.0:
                lesson = {
                    "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "ticker": ticker,
                    "sector": trade.get("industry_group", "Unknown"),
                    "insight": f"Verlies op {ticker}. Reden: Mogelijk te hoge PE voor huidige markt-volatiliteit.",
                    "type": "NEGATIVE_LEARNING"
                }
                if lesson not in memory['lessons']:
                    memory['lessons'].append(lesson)
                print(f"❌ LEERMOMENT: {ticker} staat op {pl_pct}%. Les opgeslagen.")
            
            trade['current_price'] = current_price
            trade['pl_percent'] = pl_pct
            updated_trades.append(trade)
            daily_performance += pl_pct
        except Exception as e:
            print(f"Error bij updaten trade {trade['ticker']}: {e}")
            updated_trades.append(trade)

    # 3. Nieuwe trades aangaan (als score > 9.0 en we hebben plek)
    current_tickers = [t['ticker'] for t in updated_trades]
    for c in candidates:
        if c['score'] >= 9.0 and c['ticker'] not in current_tickers and len(updated_trades) < 5:
            new_trade = {
                "ticker": c['ticker'],
                "buy_price": c['price'],
                "current_price": c['price'],
                "buy_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "industry_group": c.get("industry_group", "Others"),
                "pl_percent": 0.0
            }
            updated_trades.append(new_trade)
            print(f"✅ NIEUWE TRADE: {c['ticker']} gekocht op basis van score {c['score']}")

    # 4. Resultaten opslaan
    data['active_trades'] = updated_trades
    data['memory'] = {
        "lessons": memory['lessons'][-5:], # Alleen de laatste 5 lessen voor dashboard
        "last_update": datetime.now(timezone.utc).isoformat()
    }
    
    # Equity history updaten voor de grafiek
    history_point = {
        "date": datetime.now(timezone.utc).strftime("%m-%d %H:%M"),
        "value": 10000 + (daily_performance * 10) # Fictieve startwaarde 10k
    }
    if "equity_history" not in data: data["equity_history"] = []
    data["equity_history"].append(history_point)
    data["equity_history"] = data["equity_history"][-20:] # Laatste 20 punten

    save_json(DATA_PATH, data)
    save_json(MEMORY_PATH, memory)
    print("--- EVOLUTION COMPLETE: MEMORY UPDATED ---")

if __name__ == "__main__":
    run_evolution()