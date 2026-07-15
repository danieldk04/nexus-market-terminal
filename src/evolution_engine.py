"""
NEXUS Evolution Engine — Agressieve Investor Mode met Kelly Criterion
DCF-gebaseerde exits + dynamische VIX-drempel + post-mortem sector-aanpassingen
+ Cooldown per ticker (48u na sluiten)
+ Sector-cap op portfoliowaarde (max 40%)
"""
import json
import yfinance as yf
from datetime import datetime, date, timezone, timedelta
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
STARTING_CAPITAL  = 10_000.0
STOP_LOSS_PCT     = -8.0    # Ruimer voor langetermijn (investor)
TAKE_PROFIT_PCT   = 30.0    # Hoog doel — laat winnaars lopen
MAX_TRADES        = 12      # Gespreide portefeuille
MAX_PER_SECTOR    = 3       # Max aantal posities per sector
MAX_SECTOR_PCT    = 0.40    # Max 40% van portfoliowaarde in één sector
VIX_BLOCK         = 42.0    # Blokkeer alleen bij extreme paniek
VIX_CAUTION       = 36.0    # Alleen high-conviction bij hoge VIX
MAX_KELLY_PCT     = 0.20    # Maximaal 20% per positie (Kelly-cap)
COOLDOWN_DAYS     = 2       # Geen herinstap binnen 2 dagen na sluiten
ROTATION_MIN_DAYS   = 14    # Positie moet minstens 14 dagen oud zijn voor rotatie
ROTATION_MAX_PL_PCT = 5.0   # Roteer alleen stagnerende posities (< 5% winst)
ROTATION_SCORE_GAP  = 1.0   # Nieuwe kandidaat moet minstens 1.0 punt beter scoren
DCF_TP_MAX          = 35.0  # Cap op DCF-gebaseerde take-profit (voorkomt 130%+ doelen)

# Relative ranking window: the buy threshold is capped at the score of the
# RANK_WINDOW-th best candidate in today's scan, so the bot always has real
# opportunities to evaluate instead of silently sitting on 100% cash whenever
# the absolute scoring scale drifts (which is what was happening before —
# realistic S_Growth/S_Momentum values rarely reached the old fixed 6.5-7.5
# floor). VIX/bear-market logic still adjusts within that ceiling.
RANK_WINDOW       = 10
SCORE_FLOOR       = 2.0     # absolute sanity floor — never buy below this regardless of rank
ATR_STOP_MULT     = 1.5     # hard technical stop = price - 1.5x ATR14 at entry
ATR_VOL_TARGET    = 0.04    # position-size scalar target: 4% ATR/price is "normal" volatility


def _is_in_cooldown(ticker: str, cooldowns: dict, today: str) -> bool:
    """Geeft True als ticker recent gesloten is en cooldown nog actief is."""
    closed_date_str = cooldowns.get(ticker)
    if not closed_date_str:
        return False
    try:
        closed = date.fromisoformat(closed_date_str)
        current = date.fromisoformat(today)
        return (current - closed).days < COOLDOWN_DAYS
    except Exception:
        return False


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
            # Take-profit bij 80% van de DCF upside, maar max DCF_TP_MAX
            return min(round(dcf_upside * 0.80, 1), DCF_TP_MAX)
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

    # ── S&P 500 MA200 — macro filter voor berenmarkt ──────────────────────────
    sp500_above_ma200 = True
    try:
        sp_info  = yf.Ticker("^GSPC").info
        sp_price = sp_info.get("regularMarketPrice") or 0
        sp_ma200 = sp_info.get("twoHundredDayAverage") or 0
        if sp_price > 0 and sp_ma200 > 0:
            sp500_above_ma200 = sp_price >= sp_ma200
            status = "boven" if sp500_above_ma200 else "ONDER"
            print(f"S&P 500: {sp_price:.0f} {status} MA200 ({sp_ma200:.0f})")
    except Exception as e:
        print(f"S&P 500 macro check fout: {e}")

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

            # ── Trailing stop: bij +15% vergrendel 60% van winst ────────────
            trailing_stop = trade.get("trailing_stop_price") or 0.0
            if pl_pct >= 15.0:
                new_ts = round(trade["buy_price"] * (1 + 0.6 * pl_pct / 100), 4)
                if new_ts > trailing_stop:
                    trailing_stop = new_ts
                    trade["trailing_stop_price"] = trailing_stop
                    print(f"TRAIL UP: {ticker} bodem → ${trailing_stop:.2f} ({0.6*pl_pct:.1f}%+ geborgd)")

            # DCF-gebaseerde take-profit
            c_data   = cand_by_ticker.get(ticker)
            tp_target = get_dcf_take_profit(trade, c_data)

            # Stop-loss: percentage floor OR the ATR-based technical stop set
            # at entry, whichever triggers first (tighter of the two wins —
            # this is the "harde stop-loss" risk control, never moved down).
            atr_stop = trade.get("atr_stop_price")
            atr_hit  = atr_stop and cur_price <= atr_stop
            if pl_pct <= STOP_LOSS_PCT or atr_hit:
                cash += cur_value
                reason = f"ATR-stop (${cur_price:.2f} ≤ ${atr_stop:.2f})" if atr_hit else f"{pl_pct:.1f}%"
                add_lesson(ticker, sector,
                           f"Stop-loss {ticker} ({reason}). Sector {sector} voorzichtig.",
                           "NEGATIVE_LEARNING")
                notify_stop_loss(ticker, pl_pct, sector)
                # Cooldown: blokkeer herinstap voor COOLDOWN_DAYS
                if "cooldowns" not in memory:
                    memory["cooldowns"] = {}
                memory["cooldowns"][ticker] = today
                print(f"STOP-LOSS: {ticker} gesloten op {pl_pct:.1f}% ({reason}) | €{cur_value:.2f} | cooldown {COOLDOWN_DAYS}d")
                closed_count += 1
                continue

            # Trailing stop check
            if trailing_stop > 0 and cur_price <= trailing_stop:
                cash += cur_value
                add_lesson(ticker, sector,
                           f"Trailing-stop {ticker}: prijs ${cur_price:.2f} ≤ bodem ${trailing_stop:.2f} (+{pl_pct:.1f}% geboekt).",
                           "POSITIVE_LEARNING")
                notify_take_profit(ticker, pl_pct, sector)
                if "cooldowns" not in memory:
                    memory["cooldowns"] = {}
                memory["cooldowns"][ticker] = today
                print(f"TRAIL-EXIT: {ticker} gesloten op +{pl_pct:.1f}% | €{cur_value:.2f}")
                closed_count += 1
                continue

            # Take-profit (dynamisch o.b.v. DCF)
            if pl_pct >= tp_target:
                cash += cur_value
                add_lesson(ticker, sector,
                           f"Take-profit {ticker} (+{pl_pct:.1f}%, doel was {tp_target}%). {sector} werkt.",
                           "POSITIVE_LEARNING")
                notify_take_profit(ticker, pl_pct, sector)
                # Korte cooldown na take-profit (herinstap mag, maar niet meteen)
                if "cooldowns" not in memory:
                    memory["cooldowns"] = {}
                memory["cooldowns"][ticker] = today
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
        print(f"VOORZICHT: {vix_str} — verhoogde drempel.")
        vix_threshold = vix_threshold + 1.5
    else:
        print(f"MARKT: {vix_str} — dynamische drempel = {vix_threshold}")

    # ── 3a. Relative rank ceiling ─────────────────────────────────────────────
    # Cap the threshold at the score of the RANK_WINDOW-th best candidate in
    # today's scan. Without this, an absolute threshold calibrated for one
    # scoring regime can end up permanently unreachable if the underlying
    # score distribution shifts (e.g. after a scoring recalibration or simply
    # a quiet market with no hypergrowth names) — silently parking the bot in
    # 100% cash forever instead of trading its best available ideas.
    if vix < VIX_BLOCK and candidates:
        ranked   = sorted(candidates, key=lambda c: c.get("score", 0), reverse=True)
        rank_idx = min(RANK_WINDOW, len(ranked)) - 1
        rank_cap = ranked[rank_idx].get("score", 0)
        vix_threshold = max(SCORE_FLOOR, min(vix_threshold, rank_cap))
        print(f"Rank-cap: top-{RANK_WINDOW} kandidaat scoort {rank_cap} → basisdrempel {vix_threshold}")

    # ── 3b. Score-rotatie: vervang stagnerende positie door betere kandidaat ──
    cooldowns = memory.get("cooldowns", {})
    if len(updated_trades) >= MAX_TRADES and candidates:
        today_dt  = date.fromisoformat(today)
        occupied  = {t["ticker"] for t in updated_trades}

        best_new_cand = next(
            (c for c in candidates
             if c["ticker"] not in occupied
             and not _is_in_cooldown(c["ticker"], cooldowns, today)),
            None
        )
        if best_new_cand:
            stagnant = []
            for t in updated_trades:
                try:
                    held = (today_dt - date.fromisoformat(t.get("buy_date", today))).days
                except Exception:
                    held = 0
                if held >= ROTATION_MIN_DAYS and t.get("pl_percent", 0) < ROTATION_MAX_PL_PCT:
                    stagnant.append(t)

            if stagnant:
                worst = min(stagnant, key=lambda t: t.get("score_at_entry", 10))
                gap   = best_new_cand["score"] - worst.get("score_at_entry", 10)
                if gap >= ROTATION_SCORE_GAP:
                    ticker  = worst["ticker"]
                    cur_val = worst.get("current_value", worst.get("position_value", 0))
                    sector  = worst.get("industry_group", "Unknown")
                    pl      = worst.get("pl_percent", 0)
                    held    = (today_dt - date.fromisoformat(worst.get("buy_date", today))).days

                    cash += cur_val
                    lesson_type = "POSITIVE_LEARNING" if pl >= 0 else "NEGATIVE_LEARNING"
                    add_lesson(ticker, sector,
                               f"Rotatie ({lesson_type}): {ticker} (score={worst.get('score_at_entry')}, {pl:+.1f}%, {held}d) "
                               f"→ {best_new_cand['ticker']} (score={best_new_cand['score']}).",
                               lesson_type)
                    memory.setdefault("cooldowns", {})[ticker] = today
                    updated_trades = [t for t in updated_trades if t["ticker"] != ticker]
                    print(
                        f"ROTATIE: {ticker} (score={worst.get('score_at_entry')}, {pl:+.1f}%, {held}d) "
                        f"→ {best_new_cand['ticker']} (score={best_new_cand['score']}, gap={gap:+.1f})"
                    )

    # ── 4. Nieuwe posities openen — Kelly Criterion sizing ───────────────────
    positive_sectors = {l["sector"] for l in memory["lessons"] if l.get("type") == "POSITIVE_LEARNING"}
    pm_sector_adj    = post_mortem.get("sector_adjustments", {})
    # cooldowns already set in 3b (or here if rotation block was skipped)
    cooldowns        = memory.get("cooldowns", {})

    current_tickers = {t["ticker"] for t in updated_trades}
    sector_counts   = {}
    sector_values   = {}
    for t in updated_trades:
        sg = t.get("industry_group", "Others")
        sector_counts[sg] = sector_counts.get(sg, 0) + 1
        sector_values[sg] = sector_values.get(sg, 0) + t.get("current_value", t.get("position_value", 0))

    # Huidige totale portfoliowaarde voor sector-% berekening
    current_open_value = sum(t.get("current_value", t.get("position_value", 0)) for t in updated_trades)
    current_portfolio  = cash + current_open_value

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

        # Cooldown-check: geen herinstap binnen COOLDOWN_DAYS na sluiten
        if _is_in_cooldown(ticker, cooldowns, today):
            print(f"Overgeslagen: {ticker} — cooldown actief ({COOLDOWN_DAYS}d).")
            continue

        # Sector-cap: max MAX_PER_SECTOR posities EN max MAX_SECTOR_PCT van portfolio
        if sector_counts.get(sector, 0) >= MAX_PER_SECTOR:
            print(f"Overgeslagen: {ticker} — max {MAX_PER_SECTOR} posities in {sector}.")
            continue

        # Sectorbonus uit post-mortem + sectorrotatie
        sector_adj   = pm_sector_adj.get(sector, 0)
        rotation_adj = memory.get("sector_rotation_adj", {}).get(sector, 0)
        watch_adj    = 0.4 if ticker in memory.get("watch_list", []) else 0.0
        effective_threshold = (vix_threshold
                               - (0.3 if sector in positive_sectors else 0)
                               - sector_adj
                               - rotation_adj
                               - watch_adj)
        # Bearmarkt (S&P onder MA200): striktere instapdrempel — een vast punt
        # op de oude 0-10 schaal (7.5) is nu onbereikbaar op de gekalibreerde
        # schaal, dus verhoog relatief (+1.0) in plaats van een hard plafond.
        if not sp500_above_ma200:
            effective_threshold = effective_threshold + 1.0

        if score < effective_threshold:
            continue

        # Kelly Criterion positie-sizing (voorlopige schatting voor sector-% check)
        dcf = c.get("dcf") or {}
        dcf_upside = dcf.get("dcf_upside")
        kelly_frac, position_value = kelly_position_size(
            score=score,
            dcf_upside=dcf_upside,
            cash=cash,
            max_pct=MAX_KELLY_PCT,
        )

        # ATR-based volatility scaling: shrink size for high-ATR/volatile
        # names so a 1-ATR adverse move stays close to a fixed risk budget,
        # instead of Kelly sizing alone (which ignores volatility).
        atr14 = c.get("atr14")
        if atr14 and price > 0:
            atr_pct    = atr14 / price
            vol_scalar = max(0.5, min(1.0, ATR_VOL_TARGET / atr_pct)) if atr_pct > 0 else 1.0
            position_value = round(position_value * vol_scalar, 2)

        position_value = min(position_value, cash)

        # Sector-% cap: voorkom dat één sector > MAX_SECTOR_PCT van totaal portfolio wordt
        projected_sector_val = sector_values.get(sector, 0) + position_value
        projected_port_val   = max(current_portfolio, 1)
        if projected_sector_val / projected_port_val > MAX_SECTOR_PCT:
            print(
                f"Overgeslagen: {ticker} — {sector} zou "
                f"{projected_sector_val / projected_port_val:.0%} van portfolio worden "
                f"(max {MAX_SECTOR_PCT:.0%})."
            )
            continue

        shares = round(position_value / price, 4)

        # DCF-gebaseerde take-profit target voor nieuwe trade
        tp_target = get_dcf_take_profit({}, c)

        # Hard technical stop set at entry — never moved down afterwards.
        atr_stop_price = round(price - ATR_STOP_MULT * atr14, 4) if atr14 else None

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
            "atr_stop_price":  atr_stop_price,
            "shares":          shares,
            "pl_percent":      0.0,
        }
        cash -= position_value
        updated_trades.append(new_trade)
        current_tickers.add(ticker)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        sector_values[sector] = sector_values.get(sector, 0) + position_value
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
        "lessons":           sorted_lessons[-15:],
        "positive_sectors":  list(positive_sectors),
        "last_update":       datetime.now(timezone.utc).isoformat(),
        "version":           "nexus-v3-kelly",
        "vix_threshold":     vix_threshold,
        "sp500_above_ma200": sp500_above_ma200,
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
