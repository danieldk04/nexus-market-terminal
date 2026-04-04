import json
import yfinance as yf
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
TRADES_PATH = ROOT / "trades.json"
DATA_PATH = ROOT / "data.json"

def update_backtester():
    if not TRADES_PATH.exists():
        return

    with open(TRADES_PATH, "r") as f:
        trades = json.load(f)

    active_trades = []
    total_portfolio_value = 0
    # We gaan uit van een startkapitaal van 10.000 euro
    cash = 10000 

    for t in trades:
        if t["status"] == "OPEN":
            ticker = t["ticker"]
            try:
                data = yf.Ticker(ticker).history(period="1d")
                if not data.empty:
                    current_price = data['Close'].iloc[-1]
                    t["current_price"] = round(float(current_price), 2)
                    
                    # Bereken Profit/Loss percentage
                    entry = t["entry_price"]
                    t["pl_percent"] = round(((current_price - entry) / entry) * 100, 2)
                    
                    # Voor de equity curve: we doen alsof we 1000 euro per trade inleggen
                    trade_investment = 1000
                    current_value = trade_investment * (1 + (t["pl_percent"] / 100))
                    total_portfolio_value += current_value
                    cash -= trade_investment
                    
                    active_trades.append(t)
            except Exception as e:
                print(f"Error updating {ticker}: {e}")

    # Totale waarde = Cash + Waarde van actieve trades
    final_value = round(cash + total_portfolio_value, 2)

    # Update data.json voor het dashboard
    if DATA_PATH.exists():
        with open(DATA_PATH, "r") as f:
            dashboard_data = json.load(f)
        
        dashboard_data["active_trades"] = active_trades
        
        # Sla de historie op voor de grafiek
        if "equity_history" not in dashboard_data:
            dashboard_data["equity_history"] = []
        
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Voorkom dubbele datapunten voor dezelfde dag
        dashboard_data["equity_history"] = [h for h in dashboard_data["equity_history"] if h["date"] != today]
        dashboard_data["equity_history"].append({"date": today, "value": final_value})
        
        # Hou het compact (max laatste 30 dagen)
        dashboard_data["equity_history"] = dashboard_data["equity_history"][-30:]

        with open(DATA_PATH, "w") as f:
            json.dump(dashboard_data, f, indent=2)

    # Sla de bijgewerkte trades ook weer op in trades.json
    with open(TRADES_PATH, "w") as f:
        json.dump(trades, f, indent=2)

if __name__ == "__main__":
    update_backtester()
