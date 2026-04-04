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
    # We analyseren alleen de TOP 3 om kosten te besparen
    for i, c in enumerate(candidates[:3]):
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
            prompt = (
                f"Analyseer {ticker} ({c.get('name', ticker)}) als waardebelegger. "
                f"Sector: {c.get('industry_group', 'Onbekend')}. "
                f"Score: {c.get('score', '?')}/10. "
                f"ROE: {c['roe']}%, P/E: {c['pe_ratio']}, D/E: {c.get('debt_to_equity', '?')}. "
                f"Focus op: (1) economische Moat, (2) Value Trap risico, (3) één concreet risicofactor. "
                f"Max 150 woorden."
            )

            try:
                message = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=400,
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
