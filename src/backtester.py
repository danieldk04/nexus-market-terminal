import json
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path

TRADE_FILE = Path(__file__).parent.parent / "trades.json"

def update_paper_trades():
    if not TRADE_FILE.exists():
        with open(TRADE_FILE, "w") as f: json.dump([], f)
        return

    with open(TRADE_FILE, "r") as f:
        trades = json.load(f)

    for trade in trades:
        if trade["status"] == "OPEN":
            current_price = yf.Ticker(trade["ticker"]).fast_info["lastPrice"]
            trade["current_price"] = round(current_price, 2)
            trade["pl_percent"] = round(((current_price - trade["entry_price"]) / trade["entry_price"]) * 100, 2)
            
            # Check of 30 dagen voorbij zijn
            entry_date = datetime.fromisoformat(trade["entry_date"])
            if datetime.now() > entry_date + timedelta(days=30):
                trade["status"] = "CLOSED"

    with open(TRADE_FILE, "w") as f:
        json.dump(trades, f, indent=2)

if __name__ == "__main__":
    update_paper_trades()
