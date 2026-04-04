import os
import json
import logging
from anthropic import Anthropic
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(INFO)s] %(message)s', datefmt='%H:%M:%S')

def get_moat_analysis(ticker, name, sector, price, pe, div, news_summary):
    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    
    # De "Buffett-Style" Prompt
    prompt = f"""
    You are a Senior Value Investor following the 'Business First' principle of Warren Buffett.
    Analyze the following asset: {name} ({ticker}) in the {sector} sector.
    Current Price: €{price}, P/E: {pe}, Dividend: {div}%.
    
    Recent News Summary:
    {news_summary}
    
    Your task is to determine if this is a 'Great Business' or just a 'Cheap Stock'.
    Address the following points:
    1. THE MOAT: Does this company have a durable competitive advantage (Brand, Network Effect, Cost, Switching Costs)?
    2. BUSINESS QUALITY: Is the business model resilient against macro headwinds and high VIX?
    3. VALUE TRAP CHECK: Is the low valuation a gift or a warning of structural decline?
    
    Write a concise analysis (max 150 words). 
    End with:
    CONVICTION: [Score 1-10]
    SENTIMENT: [Score 1-10]
    RECOMMENDED ACTION: [BUY, HOLD, or AVOID]
    TARGET_PRICE: [Estimate for 30 days]
    """

    response = client.messages.create(
        model="claude-3-5-sonnet-20240620", # Claude 4.6 Sonnet identifier
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text

def run_tier2():
    logging.info("Analysing candidates with 'Business First' Moat-Logic...")
    
    with open('data.json', 'r') as f:
        data = json.load(f)
        
    candidates = data.get('top_candidates', [])
    if not candidates:
        logging.info("No candidates to analyse.")
        return

    for c in candidates:
        logging.info(f"  → Deep-dive into {c['ticker']} (Moat Check)")
        
        # Verzamel nieuws voor Claude
        news_text = ""
        for n in c.get('news', []):
            news_text += f"- {n.get('title')}: {n.get('description')}\n"
            
        analysis_raw = get_moat_analysis(
            c['ticker'], c.get('name'), c.get('sector'), 
            c.get('price'), c.get('pe_ratio'), c.get('dividend_yield'),
            news_text
        )
        
        # Parsen van de AI output (simpele versie voor demo)
        lines = analysis_raw.split('\n')
        c['tier2'] = {
            "analysis": analysis_raw.split('CONVICTION:')[0].strip(),
            "conviction_score": int([l for l in lines if 'CONVICTION:' in l][0].split(':')[1].strip().split('/')[0]),
            "recommended_action": [l for l in lines if 'RECOMMENDED ACTION:' in l][0].split(':')[1].strip(),
            "target_price": [l for l in lines if 'TARGET_PRICE:' in l][0].split(':')[1].strip()
        }

    with open('data.json', 'w') as f:
        json.dump(data, f, indent=4)
    logging.info("Business-First analysis complete.")

if __name__ == "__main__":
    run_tier2()
