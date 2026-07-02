"""
NEXUS Post-Mortem Analyser — zelfleerende verliesanalyse
Detecteert patronen in mislukte trades en genereert aanbevelingen.
Slaat inzichten op in memory.json["post_mortem"].
"""
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import yfinance as yf

BASE_DIR    = Path(__file__).parent.parent
DATA_PATH   = BASE_DIR / "data.json"
MEMORY_PATH = BASE_DIR / "memory.json"

# Sector ETFs voor rotatie-tracking (1-maand performance)
SECTOR_ETFS = {
    "Tech & AI":         "XLK",
    "Financials":        "XLF",
    "Healthcare":        "XLV",
    "Energy":            "XLE",
    "Industrials":       "XLI",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive":"XLP",
    "Utilities":         "XLU",
    "Real Estate":       "XLRE",
    "Materials":         "XLB",
}


def load_json(path, default):
    if not path.exists():
        return default
    with open(path) as f:
        try:
            return json.load(f)
        except Exception:
            return default


def compute_pattern_stats(lessons: list) -> dict | None:
    """Analyseer lessons en return statistisch profiel."""
    neg = [l for l in lessons if l.get("type") == "NEGATIVE_LEARNING"]
    pos = [l for l in lessons if l.get("type") == "POSITIVE_LEARNING"]

    if len(neg) + len(pos) < 2:
        return None

    total    = len(neg) + len(pos)
    win_rate = round(len(pos) / total * 100, 1)

    # Sector frequentie verliezen
    sector_loss = defaultdict(int)
    for l in neg:
        sector_loss[l.get("sector", "Unknown")] += 1
    worst_sectors = sorted(sector_loss.items(), key=lambda x: -x[1])[:4]

    # Sector frequentie wins
    sector_win = defaultdict(int)
    for l in pos:
        sector_win[l.get("sector", "Unknown")] += 1
    best_sectors = sorted(sector_win.items(), key=lambda x: -x[1])[:3]

    # Recurring tickers in losses
    ticker_loss = defaultdict(int)
    for l in neg:
        ticker_loss[l.get("ticker", "?")] += 1
    repeat_losers = [t for t, n in ticker_loss.items() if n > 1]

    return {
        "total_trades":   total,
        "total_losses":   len(neg),
        "total_wins":     len(pos),
        "win_rate":       win_rate,
        "worst_sectors":  worst_sectors,
        "best_sectors":   best_sectors,
        "repeat_losers":  repeat_losers,
        "recent_losses":  neg[-6:],
        "recent_wins":    pos[-3:],
    }


def fetch_sector_rotation() -> dict:
    """Haal 1-maand ETF performance op voor sectorrotatie-signalen."""
    rotation = {}
    for sector, etf in SECTOR_ETFS.items():
        try:
            hist = yf.Ticker(etf).history(period="1mo", auto_adjust=True)
            if hist is not None and len(hist) >= 5:
                ret = round((float(hist["Close"].iloc[-1]) / float(hist["Close"].iloc[0]) - 1) * 100, 1)
                rotation[sector] = ret
        except Exception:
            pass
    return rotation


def run_post_mortem():
    print("=== NEXUS POST-MORTEM STARTING ===")

    memory  = load_json(MEMORY_PATH, {"lessons": []})
    lessons = memory.get("lessons", [])

    stats = compute_pattern_stats(lessons)
    if stats is None:
        print("Onvoldoende data voor post-mortem.")
        return

    print(f"Win rate: {stats['win_rate']}% | Verliezen: {stats['total_losses']} | Wins: {stats['total_wins']}")

    # Sector-aanpassingen altijd berekenen (ongeacht API-key)
    sector_adjustments = {}
    for sector, count in stats["worst_sectors"]:
        sector_adjustments[sector] = -0.5 * min(count, 3)
    for sector, count in stats["best_sectors"]:
        sector_adjustments[sector] = sector_adjustments.get(sector, 0) + 0.3 * min(count, 3)

    # Sectorrotatie: 1-maand ETF performance → aanvullende score-aanpassingen
    print("Sector rotatie ophalen...")
    sector_rotation = fetch_sector_rotation()
    rotation_adj = {}
    for sector, perf in sector_rotation.items():
        if perf >= 5:
            rotation_adj[sector] = 0.3   # Sterke sector: beloon
        elif perf <= -5:
            rotation_adj[sector] = -0.3  # Zwakke sector: straf
    memory["sector_rotation"]     = sector_rotation
    memory["sector_rotation_adj"] = rotation_adj
    memory["sector_rotation_date"] = datetime.now(timezone.utc).isoformat()
    if sector_rotation:
        best_s  = max(sector_rotation, key=sector_rotation.get)
        worst_s = min(sector_rotation, key=sector_rotation.get)
        print(f"Rotatie: beste={best_s}({sector_rotation[best_s]}%), slechtste={worst_s}({sector_rotation[worst_s]}%)")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Geen ANTHROPIC_API_KEY — statistische summary opgeslagen zonder AI.")
        memory["post_mortem"] = {
            "win_rate":           stats["win_rate"],
            "worst_sectors":      stats["worst_sectors"],
            "best_sectors":       stats["best_sectors"],
            "sector_adjustments": sector_adjustments,
            "run_at":             datetime.now(timezone.utc).isoformat(),
        }
        with open(MEMORY_PATH, "w") as f:
            json.dump(memory, f, indent=4)
        print("Sector rotatie en post-mortem opgeslagen (geen AI).")
        return

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    # Bouw context voor Claude
    loss_lines = "\n".join([
        f"  • {l.get('date','?')} {l.get('ticker','?')} ({l.get('sector','?')}): {l.get('insight','')}"
        for l in stats["recent_losses"]
    ])
    win_lines = "\n".join([
        f"  • {l.get('date','?')} {l.get('ticker','?')} ({l.get('sector','?')}): {l.get('insight','')}"
        for l in stats["recent_wins"]
    ])
    worst_s = ", ".join([f"{s} ({n}x)" for s, n in stats["worst_sectors"]])
    best_s  = ", ".join([f"{s} ({n}x)" for s, n in stats["best_sectors"]])
    repeat  = ", ".join(stats["repeat_losers"]) or "geen"

    prompt = (
        f"Je bent een kwantitatieve risicoanalist bij een hedge fund. "
        f"Analyseer onderstaande verliesdatabase van onze autonome beleggingsbot.\n\n"
        f"STATISTIEKEN:\n"
        f"- Win rate: {stats['win_rate']}% ({stats['total_wins']} wins, {stats['total_losses']} verliezen)\n"
        f"- Slechtste sectoren: {worst_s}\n"
        f"- Beste sectoren: {best_s}\n"
        f"- Herhaalde verliezers: {repeat}\n\n"
        f"RECENTE VERLIEZEN:\n{loss_lines}\n\n"
        f"RECENTE WINS:\n{win_lines}\n\n"
        f"Schrijf een beknopt post-mortem rapport (max 200 woorden) met:\n"
        f"1. PATROON: Wat is het dominante patroon in de verliezen?\n"
        f"2. ROOT CAUSE: Systematisch falen in de scorelogica of timing?\n"
        f"3. AANPASSING: Twee concrete aanbevelingen om de win rate te verhogen.\n"
        f"4. SECTOR BIAS: Welke sector te mijden / zwaarder wegen?\n"
        f"Wees direct en kwantitatief."
    )

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        analysis = msg.content[0].text

        memory["post_mortem"] = {
            "analysis":           analysis,
            "win_rate":           stats["win_rate"],
            "worst_sectors":      stats["worst_sectors"],
            "best_sectors":       stats["best_sectors"],
            "repeat_losers":      stats["repeat_losers"],
            "sector_adjustments": sector_adjustments,  # computed above, before API call
            "run_at":             datetime.now(timezone.utc).isoformat(),
        }

        with open(MEMORY_PATH, "w") as f:
            json.dump(memory, f, indent=4)

        print("Post-mortem opgeslagen.")
        print(analysis[:300] + "...")

    except Exception as e:
        print(f"Post-mortem AI-analyse mislukt: {e}")

    print("=== POST-MORTEM COMPLETE ===")


if __name__ == "__main__":
    run_post_mortem()
