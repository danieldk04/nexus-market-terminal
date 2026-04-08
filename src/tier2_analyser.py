import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import anthropic

# Paden
BASE_DIR = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"

def run_smart_analysis():
    print("--- NEXUS SMART ANALYSER STARTING (COST-SAVING MODE) ---")
    if not DATA_PATH.exists(): return
    
    with open(DATA_PATH, "r") as f:
        data = json.load(f)
    
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Geen API Key gevonden.")
        return

    client = anthropic.Anthropic(api_key=api_key)
    candidates = data.get("top_candidates", [])
    
    updated = False
    # Analyseer de TOP 5 — meer coverage zonder hoge kosten dankzij 3-daags cache
    for i, c in enumerate(candidates[:5]):
        ticker = c['ticker']
        
        # Check of we al een recent rapport hebben (niet ouder dan 3 dagen)
        tier2_data = c.get("tier2", {})
        existing_report = tier2_data.get("analysis")
        last_run_str = tier2_data.get("last_run")
        
        should_analyze = True
        if existing_report and last_run_str:
            try:
                last_run = datetime.fromisoformat(last_run_str)
                if datetime.now(timezone.utc) - last_run < timedelta(days=3):
                    should_analyze = False 
                    print(f"Hergebruik rapport voor {ticker} (credits bespaard).")
            except:
                pass
        
        if should_analyze:
            print(f"--- AI Analyse voor {ticker} wordt gestart ---")
            fcf_str     = "positief" if c.get("fcf_positive", True) else "NEGATIEF (risico)"
            growth_str  = "{}%".format(c.get("revenue_growth", "?"))
            margin_str  = "{}%".format(c.get("profit_margin", "?"))
            beta_str    = str(c.get("beta", "?"))
            prompt = (
                "Je bent een topanalist die denkt als Warren Buffett, Charlie Munger en Benjamin Graham gecombineerd. "
                "Analyseer {} ({}) voor een waardebelegger.\n\n"
                "Fundamentals:\n"
                "- Sector: {} | Score: {}/10\n"
                "- ROE: {}% | P/E: {} | D/E: {}\n"
                "- Omzetgroei: {} | Winstmarge: {} | FCF: {} | Beta: {}\n\n"
                "Beantwoord kort (max 200 woorden) in 4 punten:\n"
                "1. MOAT: Heeft het bedrijf een duurzaam concurrentievoordeel? (pricing power, switching costs, network effects, cost advantage)\n"
                "2. KWALITEIT: Is het management-beslissingen goed kapitaalallloceerders? FCF-trend?\n"
                "3. WAARDERING: Is de prijs fair of goedkoop t.o.v. intrinsieke waarde? Value trap risico?\n"
                "4. RISICO: Noem het één grootste concrete risico voor de komende 12 maanden.\n"
                "Geef een eindoordeel: KOOP / HOUD / MIJDEN."
            ).format(
                ticker, c.get("name", ticker),
                c.get("industry_group", "?"), c.get("score", "?"),
                c["roe"], c["pe_ratio"], c.get("debt_to_equity", "?"),
                growth_str, margin_str, fcf_str, beta_str,
            )

            try:
                message = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=600,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                c["tier2"] = {
                    "analysis": message.content[0].text,
                    "last_run": datetime.now(timezone.utc).isoformat()
                }
                updated = True
            except Exception as e:
                print(f"Fout bij Claude voor {ticker}: {e}")

    if updated:
        with open(DATA_PATH, "w") as f:
            json.dump(data, f, indent=4)
        print("Nieuwe analyses toegevoegd aan data.json")
    else:
        print("Geen nieuwe AI-analyses nodig.")

if __name__ == "__main__":
    run_smart_analysis()
