"""
NEXUS Ticker Updater
Haalt alleen actuele koersen op voor actieve posities en schrijft ze terug
naar data.json. Geen AI, geen Telegram, geen wijzigingen aan portfolio-state.
"""
import json
import time
import yfinance as yf
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR  = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"


def load_json(path, default):
    if not path.exists():
        return default
    with open(path) as f:
        try:
            return json.load(f)
        except Exception:
            return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=4)


def run_ticker_update():
    print("--- NEXUS TICKER UPDATE ---")
    data          = load_json(DATA_PATH, {})
    active_trades = data.get("active_trades", [])

    if not active_trades:
        print("Geen actieve posities. Niets te updaten.")
        return

    updated = []
    for trade in active_trades:
        ticker    = trade["ticker"]
        buy_price = trade["buy_price"]
        shares    = trade.get("shares", 0)

        try:
            info      = yf.Ticker(ticker).info
            cur_price = (
                info.get("currentPrice")
                or info.get("regularMarketPrice")
                or buy_price
            )
            pl_pct    = round(((cur_price - buy_price) / buy_price) * 100, 2)
            cur_value = round(shares * cur_price, 2) if shares else trade.get("current_value", 0)

            trade["current_price"] = round(cur_price, 4)
            trade["current_value"] = cur_value
            trade["pl_percent"]    = pl_pct

            sign = "+" if pl_pct >= 0 else ""
            print(f"  {ticker:6s}  €{cur_price:.2f}  ({sign}{pl_pct:.2f}%)")
            time.sleep(0.1)   # vriendelijk voor de API
        except Exception as e:
            print(f"  {ticker:6s}  fout: {e}")

        updated.append(trade)

    data["active_trades"]      = updated
    data["ticker_updated_at"]  = datetime.now(timezone.utc).isoformat()

    # Portefeuillewaarde meebijwerken. Zonder dit toont het dashboard posities van
    # een paar minuten oud naast een totaalwaarde van vanochtend — een verschil dat
    # oploopt met de dag en nergens te verklaren is.
    pf = data.get("portfolio")
    if pf:
        open_value = round(sum(t.get("current_value") or 0 for t in updated), 2)
        cash       = pf.get("cash", 0)
        start      = pf.get("starting_capital") or 0
        pf["open_value"]  = open_value
        pf["total_value"] = round(cash + open_value, 2)
        if start:
            pf["return_pct"] = round((pf["total_value"] / start - 1) * 100, 2)
        data["portfolio"] = pf
        print(f"  portefeuille: €{pf['total_value']:.2f} ({pf.get('return_pct')}%)")

    save_json(DATA_PATH, data)
    print(f"Updated {len(updated)} posities.")
    print("--- TICKER UPDATE COMPLETE ---")


if __name__ == "__main__":
    run_ticker_update()
