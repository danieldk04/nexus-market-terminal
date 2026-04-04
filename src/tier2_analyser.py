import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import anthropic

# Paden instellen
BASE_DIR = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"

def run_smart_analysis():
    print("--- NEXUS SMART ANALYSER STARTING (COST-SAVING MODE) ---")
    
    if not DATA_PATH.exists():
        print("Geen data.json gevonden.")
        return
        
    with open(DATA_PATH, "r") as f:
        data = json.load(f)
    
    # Initialiseer Claude
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Geen API key gevonden in omgeving.")
        return
        
    client = anthropic.Anthropic(api_key=api_key)
    candidates = data.get("top_candidates", [])
    
    updated = False
    # Focus alleen op de TOP 3 om credits te besparen
    for i, c in enumerate(candidates[:3]):
        ticker = c['ticker']
        
        # Check of we al een recent rapport hebben (niet ouder dan 3 dagen)
        existing_report = c.get("tier2", {}).get("analysis")
        last_run_str = c.get("tier2", {}).get("last_run")
        
        should_analyze = True
        if existing_report and last_run_str:
            try:
                last_run = datetime.fromisoformat(last_run_str)
                # Als het rapport jonger is dan 3 dagen, hergebruiken we het
                if datetime.now(timezone.utc) - last_run < timedelta(days=3):
                    should_analyze = False
                    print(f"Hergebruik bestaand rapport voor {ticker} (besparing).")
            except:
                pass
        
        if should_analyze:
            print(f"Nieuwe AI Analyse aanvragen voor {ticker}...")
            prompt = (f"Analyseer {ticker} als waardebelegger. "
                     f"ROE: {c['roe']}%, PE: {c['pe_ratio']}. "
                     f"Focus op Moat (concurrentievoordeel) en Value Trap risico. "
                     f"Houd het kort en krachtig (max 120 woorden).")
            
            try:
                message = client.messages.create(
                    model="claude-3-5-haiku-latest", # Goedkoopste en snelste model
                    max_tokens=300,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                # Sla het resultaat op in de kandidaat-data
                c["tier2"] = {
                    "analysis": message.content[0].text,
                    "last_run": datetime.now(timezone.utc).isoformat()
                }
                updated = True
                print(f"Succesvolle analyse voor {ticker}.")
            except Exception as e:
                print(f"Fout bij aanroepen Claude voor {ticker}: {e}")

    # Alleen opslaan als er daadwerkelijk iets nieuws is toegevoegd
    if updated:
        with open(DATA_PATH, "w") as f:
            json.dump(data, f, indent=4)
        print("Data.json bijgewerkt met nieuwe AI rapporten.")
    else:
        print("Geen nieuwe analyses nodig. Credits bespaard.")

if __name__ == "__main__":
    run_smart_analysis()
