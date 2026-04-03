"""
NEXUS MARKET TERMINAL - Tier 2 Analyser (V2.1 - Sentiment & Telegram Fix)
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

# ── Claude config ─────────────────────────────────────────────────────────────
MODEL = "claude-sonnet-4-6" 
SLEEP_BETWEEN_CALLS = 3.0

# ── Telegram Config ───────────────────────────────────────────────────────────
TELEGRAM_TOKEN = "8561838956:AAE6Xw_nl9acbtY7bmea--ovgNaLnh9Hvzk"
TELEGRAM_CHAT_ID = "7995706133"

# ── Helpers ───────────────────────────────────────────────────────────────────

def send_telegram_msg(message):
    """Verstuurt een notificatie naar Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        log.info("Telegram notification sent successfully.")
    except Exception as e:
        log.error(f"Telegram error: {e}")

def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)

def save_json(path: Path, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def build_system_prompt() -> str:
    return dedent("""
        You are a senior equity analyst for the NEXUS Market Terminal.
        Assess the stock based on fundamentals and news provided.
        
        STRICT RULES:
        1. Respond ONLY with a valid JSON object.
        2. Format: {
            "analysis": "text", 
            "conviction_score": 1-10, 
            "sentiment_score": 1-10,
            "recommended_action": "buy/hold/sell"
        }
        3. 'sentiment_score' must reflect the tone of the news items provided (1=panic, 10=hype).
        4. 'analysis' should be a concise analyst note.
        5. Do NOT use markdown code blocks.
        6. Do NOT include any introductory or concluding text.
    """).strip()

def build_user_prompt(candidate: dict, macro: dict) -> str:
    news_items = candidate.get('news', [])
    news_block = ""
    if not news_items:
        news_block = "No recent news found in feed."
    else:
        for i, n in enumerate(news_items, 1):
            news_block += f"{i}. {n.get('title')} ({n.get('source')})\n"

    return dedent(f"""
        Ticker: {candidate.get('ticker')}
        Scan Score: {candidate.get('score')}
        Dividend Yield: {candidate.get('dividend_yield')}%
        PE Ratio: {candidate.get('pe_ratio')}
        
        Macro Context: VIX={macro.get('vix')}, RSI={macro.get('rsi')}
        
        Recent News:
        {news_block}
        
        Analyze the news sentiment vs the fundamentals and provide your JSON response.
    """).strip()

def extract_json(text: str) -> dict | None:
    try:
        # Zoek naar alles tussen { }
        match = re.search(r'(\{.*\})', text, re.DOTALL)
        if match:
            return json.loads(match.group(1))
        return json.loads(text)
    except Exception as e:
        log.error(f"JSON Parsing failed: {e}")
        return None

def run_analysis() -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY missing!")
        return

    client = anthropic.Anthropic(api_key=api_key)
    if not DATA_PATH.exists():
        log.error("data.json not found")
        return
        
    data = load_json(DATA_PATH)
    candidates = data.get("top_candidates", [])
    macro = data.get("macro", {})

    if not candidates:
        log.info("No candidates to analyse.")
        return

    system_prompt = build_system_prompt()
    log.info("Analysing %d candidates...", len(candidates))

    for candidate in candidates:
        ticker = candidate["ticker"]
        log.info("  → Processing %s", ticker)
        
        user_prompt = build_user_prompt(candidate, macro)
        
        try:
            message = client.messages.create(
                model=MODEL,
                max_tokens=1000,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            
            raw_text = message.content[0].text
            analysis = extract_json(raw_text)

            if analysis:
                # Opslaan in data.json
                candidate["tier2"] = {**analysis, "analysed_at": datetime.now(timezone.utc).isoformat()}
                
                # ── Telegram Notificatie Logica ────────────────────────────────
                action = analysis.get("recommended_action", "").lower()
                conviction = analysis.get("conviction_score", 0)
                sentiment = analysis.get("sentiment_score", 0)
                
                # Alleen berichten sturen bij sterke signalen
                if action == "buy" and conviction >= 7:
                    msg = dedent(f"""
                        🚀 <b>NEXUS BUY SIGNAL: {ticker}</b>
                        
                        <b>Conviction:</b> {conviction}/10
                        <b>Sentiment:</b> {sentiment}/10
                        
                        <b>Analysis:</b> {analysis.get('analysis')}
                        
                        <i>Checked at: {datetime.now().strftime('%H:%M:%S')}</i>
                    """).strip()
                    send_telegram_msg(msg)
                # ──────────────────────────────────────────────────────────────
                
            else:
                candidate["tier2"] = {"error": "json_parsing_failed"}
                
        except Exception as exc:
            log.error("Claude call failed for %s: %s", ticker, exc)
            candidate["tier2"] = {"error": "api_call_failed"}
        
        # Voorkom Rate Limits
        time.sleep(SLEEP_BETWEEN_CALLS)

    save_json(DATA_PATH, data)
    log.info("Analysis complete and data.json updated.")

if __name__ == "__main__":
    run_analysis()
