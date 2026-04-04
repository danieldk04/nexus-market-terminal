"""
NEXUS MARKET TERMINAL - Tier 2 Analyser (V3.0 - Learning Loop & Virtual Trading)
"""

import json
import logging
import os
import time
import re
import requests
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import anthropic

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tier2_analyser")

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA_PATH = ROOT / "data.json"
TRADES_PATH = ROOT / "trades.json"

# ── Claude config ─────────────────────────────────────────────────────────────
MODEL = "claude-sonnet-4-6" # Gebruik de actuele stabiele identifier
SLEEP_BETWEEN_CALLS = 3.0

# ── Telegram Config ───────────────────────────────────────────────────────────
TELEGRAM_TOKEN = "8561838956:AAE6Xw_nl9acbtY7bmea--ovgNaLnh9Hvzk"
TELEGRAM_CHAT_ID = "7995706133"

# ── Helpers ───────────────────────────────────────────────────────────────────

def send_telegram_msg(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        log.info("Telegram notification sent successfully.")
    except Exception as e:
        log.error(f"Telegram error: {e}")

def log_virtual_trade(candidate, analysis):
    """Slaat een nieuwe BUY trade op in trades.json voor de backtester."""
    trades = []
    if TRADES_PATH.exists():
        with open(TRADES_PATH, "r") as f:
            try: trades = json.load(f)
            except: trades = []
    
    ticker = candidate["ticker"]
    # Voorkom dubbele actieve trades
    if any(t["ticker"] == ticker and t["status"] == "OPEN" for t in trades):
        return

    new_trade = {
        "ticker": ticker,
        "entry_date": datetime.now(timezone.utc).isoformat(),
        "entry_price": candidate.get("price", 0),
        "predicted_target": analysis.get("target_price", "N/A"),
        "conviction": analysis.get("conviction_score", 0),
        "status": "OPEN",
        "current_price": candidate.get("price", 0),
        "pl_percent": 0.0
    }
    trades.append(new_trade)
    with open(TRADES_PATH, "w") as f:
        json.dump(trades, f, indent=2)
    log.info(f"VIRTUAL TRADE LOGGED: {ticker}")

def build_system_prompt(history_context: str) -> str:
    return dedent(f"""
        You are the NEXUS Risk Engine, a senior equity analyst.
        
        LEARNING LOOP CONTEXT:
        {history_context}
        Use the history above to avoid repeating past mistakes (e.g., being too bullish in high VIX).

        STRICT RULES:
        1. Respond ONLY with a valid JSON object.
        2. Format: {{
            "analysis": "concise note", 
            "conviction_score": 1-10, 
            "sentiment_score": 1-10,
            "recommended_action": "buy/hold/sell",
            "target_price": "estimated price in 30 days",
            "upside_percentage": "expected return in %"
        }}
        3. Do NOT use markdown code blocks.
    """).strip()

def build_user_prompt(candidate: dict, macro: dict) -> str:
    news_items = candidate.get('news', [])
    news_block = "\n".join([f"- {n.get('title')}" for n in news_items]) if news_items else "No news."

    return dedent(f"""
        Ticker: {candidate.get('ticker')}
        Price: €{candidate.get('price')}
        PE: {candidate.get('pe_ratio')} | Div: {candidate.get('dividend_yield')}%
        Macro: VIX={macro.get('vix')}, RSI={macro.get('sp500_rsi', 'N/A')}
        
        Recent News:
        {news_block}
        
        Analyze and provide JSON.
    """).strip()

def extract_json(text: str) -> dict | None:
    try:
        match = re.search(r'(\{.*\})', text, re.DOTALL)
        return json.loads(match.group(1)) if match else json.loads(text)
    except:
        return None

def run_analysis() -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key: return

    client = anthropic.Anthropic(api_key=api_key)
    if not DATA_PATH.exists(): return
        
    data = json.load(open(DATA_PATH))
    candidates = data.get("top_candidates", [])
    macro = data.get("macro", {})

    # Haal historie op voor de Learning Loop
    history_context = "No history available."
    if TRADES_PATH.exists():
        with open(TRADES_PATH, "r") as f:
            try:
                hist = json.load(f)[-5:] # Laatste 5 trades
                history_context = f"Recent performance history: {json.dumps(hist)}"
            except: pass

    log.info("Analysing %d candidates with Learning Loop...", len(candidates))

    for candidate in candidates:
        ticker = candidate["ticker"]
        log.info("  → Processing %s", ticker)
        
        try:
            message = client.messages.create(
                model=MODEL,
                max_tokens=1000,
                system=build_system_prompt(history_context),
                messages=[{"role": "user", "content": build_user_prompt(candidate, macro)}],
            )
            
            analysis = extract_json(message.content[0].text)

            if analysis:
                candidate["tier2"] = {**analysis, "analysed_at": datetime.now(timezone.utc).isoformat()}
                
                action = analysis.get("recommended_action", "").lower()
                conviction = analysis.get("conviction_score", 0)

                # Log trade als het een sterke BUY is
                if action == "buy":
                    log_virtual_trade(candidate, analysis)
                
                # Telegram alert voor High Conviction BUY
                if action == "buy" and conviction >= 7:
                    msg = f"🚀 <b>NEXUS BUY: {ticker}</b>\nTarget: €{analysis.get('target_price')}\nConviction: {conviction}/10\n{analysis.get('analysis')}"
                    send_telegram_msg(msg)
                
            time.sleep(SLEEP_BETWEEN_CALLS)
        except Exception as e:
            log.error("Error for %s: %s", ticker, e)

    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)
    log.info("Analysis complete. Memory updated.")

if __name__ == "__main__":
    run_analysis()
