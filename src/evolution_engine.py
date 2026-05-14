"""
NEXUS Evolution Engine — Agressieve Investor Mode met Kelly Criterion
DCF-gebaseerde exits + dynamische VIX-drempel + post-mortem sector-aanpassingen
"""
import json
import yfinance as yf
from datetime import datetime, timezone
from pathlib import Path
from dcf_engine import vix_dynamic_threshold, kelly_position_size
from notifier import (
    notify_stop_loss, notify_take_profit,
    notify_warning, notify_trade_opened,
    notify_evolution_summary,
)

BASE_DIR    = Path(__file__).parent.parent
DATA_PATH   = BASE_DIR / "data.json"
MEMORY_PATH = BASE_DIR / "memory.json"

# ─── INVESTOR CONFIGURATIE ───────────────────────────────────────────────────
STARTING_CAPITAL = 10_000.0
STOP_LOSS_PCT    = -8.0    # Ruimer voor langetermijn (investor)
TAKE_PROFIT_PCT  = 30.0    # Hoog doel — laat winnaars lopen
MAX_TRADES       = 10      # Gespreide portefeuille
MAX_PER_SECTOR   = 3       # Diversificatie
VIX_BLOCK        = 42.0    # Blokkeer alleen bij extreme paniek
VIX_CAUTION      = 36.0    # Alleen high-conviction bij hoge VIX
MAX_KELLY_PCT    = 0.20    # Maximaal 20% per positie (Kelly-cap)


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


def get_dcf_take_profit(trade: dict, c_data: dict | None) -> float:
    """
    Bereken take-profit op basis van DCF intrinsieke waarde.
    Als DCF beschikbaar is: gebruik upside vs. huidige prijs.
    Anders: vaste 30% take-profit.
    """
    if c_data and c_data.get("dcf"):
        dcf_upside = c_data["dcf"].get("dcf_upside")
        if dcf_upside is not None and dcf_upside > 10:
            # Take-profit bij 80% van de DCF upside (veiligheidsmarge)
            return round(dcf_upside * 0.80, 1)
    return TAKE_PROFIT_PCT


def run_evolution():
    print("=== NEXUS EVOLUTION ENGINE (KELLY + DCF MODE) STARTING ===")

    data         = load_json(DATA_PATH,   {"top_candidates": [], "active_trades": [], "equity_history": []})
    memory       = load_json(MEMORY_PATH, {"lessons": [], "version": "nexus-v3"})
    if "lessons" not in memory:
        memory["lessons"] = []

    candidates    = data.get("top_candidates", [])
    active_trades = data.get("active_trades", [])
    macro         = data.get("macro", {})
    vix           = macro.get("vix") or 0.0
    post_mortem   = memory.get("post_mortem", {})

    portfolio = data.get("portfolio", {"cash": STARTING_CAPITAL, "starting_capital": STARTING_CAPITAL})
    cash      = float(portfolio.get("cash", STARTING_CAPITAL))
    today     = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Kandidaten-index voor DCF-lookup
    cand_by_ticker = {c["ticker"]: c for c in candidates}

    def add_lesson(ticker, sector, insight, lesson_type):
        lesson = {"date": today, "ticker": ticker, "sector": sector,
                  "insight": insight, "type": lesson_type}
        if not any(
            l["ticker"] == ticker and l["date"] == today and l["type"] == lesson_type
            for l in memory["lessons"]
        ):
            memory["lessons"].append(lesson)

    # ── 2. Actieve trades updaten + exit-logica ───────────────────────────────
    updated_trades = []
    closed_count   = 0

    for trade in active_trades:
        try:
            ticker    = trade["ticker"]
            t_info    = yf.Ticker(ticker).info
            cur_price = (
                t_info.get("currentPrice")
                or t_info.get("regularMarketPrice")
                or trade["buy_price"]
            )
            pl_pct    = round(((cur_price - trade["buy_price"]) / trade["buy_price"]) * 100, 2)
            sector    = trade.get("industry_group", "Unknown")
            shares    = trade.get("shares", 0)
            cur_value = round(shares * cur_price, 2) if shares else trade.get("position_value", 0)

            # DCF-gebaseerde take-profit
            c_data   = cand_by_ticker.get(ticker)
            tp_target = get_dcf_take_profit(trade, c_data)

            # Stop-loss
            if pl_pct <= STOP_LOSS_PCT:
                cash += cur_value
                add_lesson(ticker, sector,
                           f"Stop-loss {ticker} ({pl_pct:.1f}%). Sector {sector} voorzichtig.",
                           "NEGATIVE_LEARNING")
                notify_stop_loss(ticker, pl_pct, sector)
                print(f"STOP-LOSS: {ticker} gesloten op {pl_pct:.1f}% | €{cur_value:.2f}")
                closed_count += 1
                continue

            # Take-profit (dynamisch o.b.v. DCF)
            if pl_pct >= tp_target:
                cash += cur_value
                add_lesson(ticker, sector,
                           f"Take-profit {ticker} (+{pl_pct:.1f}%, doel was {tp_target}%). {sector} werkt.",
                           "POSITIVE_LEARNING")
                notify_take_profit(ticker, pl_pct, sector)
                print(f"TAKE-PROFIT: {ticker} gesloten op +{pl_pct:.1f}% (DCF-doel: +{tp_target}%) | €{cur_value:.2f}")
                closed_count += 1
                continue

            # Vroege waarschuwing
            if pl_pct < -5.0:
                add_lesson(ticker, sector,
                           f"Verlies {ticker} ({pl_pct:.1f}%). Bewaken.",
                           "NEGATIVE_LEARNING")
                notify_warning(ticker, pl_pct, sector)

            trade["current_price"] = cur_price
            trade["current_value"] = cur_value
            trade["pl_percent"]    = pl_pct
            trade["tp_target"]     = tp_target
            updated_trades.append(trade)

        except Exception as e:
            print(f"Update fout {trade.get('ticker')}: {e}")
            updated_trades.append(trade)

    if closed_count:
        print(f"{closed_count} positie(s) gesloten.")

    # ── 3. VIX-filter: dynamische instapdrempel ───────────────────────────────
    vix_str       = f"VIX={vix:.1f}" if vix else "VIX=onbekend"
    vix_threshold = vix_dynamic_threshold(vix)

    if vix >= VIX_BLOCK:
        print(f"MARKTPAUZE: {vix_str} ≥ {VIX_BLOCK} — geen nieuwe trades.")
        vix_threshold = 999
    elif vix >= VIX_CAUTION:
        print(f"VOORZICHT: {vix_str} — alleen score > 7.5.")
        vix_threshold = max(vix_threshold, 7.5)
    else:
        print(f"MARKT: {vix_str} — dynamische drempel = {vix_threshold}")

    # ── 4. Nieuwe posities openen — Kelly Criterion sizing ───────────────────
    positive_sectors = {l["sector"] for l in memory["lessons"] if l.get("type") == "POSITIVE_LEARNING"}
    pm_sector_adj    = post_mortem.get("sector_adjustments", {})

    current_tickers = {t["ticker"] for t in updated_trades}
    sector_counts   = {}
    for t in updated_trades:
        sg = t.get("industry_group", "Others")
        sector_counts[sg] = sector_counts.get(sg, 0) + 1

    new_count = 0
    for c in candidates:
        if len(updated_trades) >= MAX_TRADES or cash < 300:
            break

        sector = c.get("industry_group", "Others")
        score  = c.get("score", 0)
        ticker = c["ticker"]
        price  = c.get("price", 0)

        if ticker in current_tickers or price <= 0:
            continue
        if sector_counts.get(sector, 0) >= MAX_PER_SECTOR:
            print(f"Overgeslagen: {ticker} — max {MAX_PER_SECTOR} in {sector}.")
            continue

        # Sectorbonus uit post-mortem
        sector_adj = pm_sector_adj.get(sector, 0)
        effective_threshold = vix_threshold - (0.3 if sector in positive_sectors else 0) - sector_adj

        if score < effective_threshold:
            continue

        # Kelly Criterion positie-sizing
        dcf = c.get("dcf") or {}
        dcf_upside = dcf.get("dcf_upside")
        kelly_frac, position_value = kelly_position_size(
            score=score,
            dcf_upside=dcf_upside,
            cash=cash,
            max_pct=MAX_KELLY_PCT,
        )
        position_value = min(position_value, cash)
        shares = round(position_value / price, 4)

        # DCF-gebaseerde take-profit target voor nieuwe trade
        tp_target = get_dcf_take_profit({}, c)

        new_trade = {
            "ticker":          ticker,
            "name":            c.get("name", ticker),
            "buy_price":       price,
            "current_price":   price,
            "current_value":   position_value,
            "buy_date":        today,
            "industry_group":  sector,
            "sector":          c.get("sector", sector),
            "score_at_entry":  score,
            "roic_at_entry":   c.get("roic"),
            "roce_at_entry":   c.get("roce"),
            "dcf_upside_entry": dcf_upside,
            "tp_target":       tp_target,
            "kelly_fraction":  kelly_frac,
            "position_value":  position_value,
            "shares":          shares,
            "pl_percent":      0.0,
        }
        cash -= position_value
        updated_trades.append(new_trade)
        current_tickers.add(ticker)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        new_count += 1
        notify_trade_opened(ticker, price, score, sector)
        print(
            f"Nieuwe positie: {ticker} | score={score} | kelly={kelly_frac:.1%} "
            f"| €{position_value:.2f} | TP target: +{tp_target:.1f}%"
        )

    # ── 5. Portfolio-waarde ───────────────────────────────────────────────────
    open_value      = sum(t.get("current_value", t.get("position_value", 0)) for t in updated_trades)
    portfolio_value = round(cash + open_value, 2)

    # ── 6. Equity curve ───────────────────────────────────────────────────────
    equity_history = data.get("equity_history", [])
    equity_history.append({
        "date":  datetime.now(timezone.utc).strftime("%m-%d %H:%M"),
        "value": portfolio_value,
    })
    data["equity_history"] = equity_history[-60:]

    # ── 7. Self-learning memory updaten ──────────────────────────────────────
    sorted_lessons = sorted(memory["lessons"][-30:], key=lambda l: l.get("type", ""), reverse=True)

    data["active_trades"] = updated_trades
    data["portfolio"]     = {
        "cash":             round(cash, 2),
        "starting_capital": STARTING_CAPITAL,
        "open_value":       round(open_value, 2),
        "total_value":      portfolio_value,
        "return_pct":       round(((portfolio_value / STARTING_CAPITAL) - 1) * 100, 2),
    }
    data["memory"] = {
        "lessons":          sorted_lessons[-15:],
        "positive_sectors": list(positive_sectors),
        "last_update":      datetime.now(timezone.utc).isoformat(),
        "version":          "nexus-v3-kelly",
        "vix_threshold":    vix_threshold,
    }

    notify_evolution_summary(
        active_trades=updated_trades,
        closed_count=closed_count,
        new_count=new_count,
        equity_value=portfolio_value,
    )

    save_json(DATA_PATH,   data)
    save_json(MEMORY_PATH, memory)

    rp   = data["portfolio"]["return_pct"]
    sign = "+" if rp >= 0 else ""
    print(f"Portfolio: €{portfolio_value:.2f} ({sign}{rp}%) | Cash: €{cash:.2f} | Posities: {len(updated_trades)}")
    print("=== EVOLUTION COMPLETE ===")


if __name__ == "__main__":
    run_evolution()
