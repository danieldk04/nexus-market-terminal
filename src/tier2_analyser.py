"""
NEXUS MARKET TERMINAL - Tier 2 Analyser (V2 Robust Version with Telegram)
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
MEMORY_PATH = ROOT / "memory.json"

# ── Claude config ─────────────────────────────────────────────────────────────
MODEL = "claude-3-5-sonnet-latest" 
SLEEP_BETWEEN_CALLS = 3.0

# ── Telegram Config ───────────────────────────────────────────────────────────
# Tip: Gebruik GitHub Secrets voor deze waarden in productie!
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
        2. Do NOT use markdown code blocks (no ```json).
        3. Do NOT include any introductory or concluding text.
        4. Ensure all strings are properly escaped.
        5. Conviction scale: 1-10.
    """).strip()

def build_user_prompt(candidate: dict, macro: dict) -> str:
    news_items = candidate.get('news', [])
    news_block = ""
    if not news_items:
        news_block = "No recent news found in feed."
    else:
        for i, n in enumerate(news_items[:5]):
            news_block += f"[{i+1}] {n.get('date', 'Recent')} | {n.get('title', 'No Title')}\n"

    return dedent(f"""
        Analalyse candidate: {candidate.get('ticker')} ({candidate.get('name')})
        Sector: {candidate.get('sector')}
        Div Yield: {candidate.get('dividend_yield')}% | P/E: {candidate.get('pe_ratio')} | EPS Growth: {candidate.get('eps_growth_3yr')}%
        
        Macro Context: VIX {macro.get('vix')}, 10Y Yield {macro.get('treasury_10y')}%
        
        Recent News:
        {news_block}

        Return this JSON structure:
        {{
          "sentiment": "bullish" | "neutral" | "bearish",
          "conviction": 1-10,
          "key_positives": ["...", "..."],
          "key_risks": ["...", "..."],
          "macro_alignment": "tailwind" | "neutral" | "headwind",
          "analyst_note": "Short summary",
          "recommended_action": "buy" | "watch" | "avoid"
        }}
        
        Final reminder: Respond ONLY with JSON.
    """).strip()

def extract_json(text: str) -> dict | None:
    try:
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
                candidate["tier2"] = {**analysis, "analysed_at": datetime.now(timezone.utc).isoformat()}
                
                # ── Telegram Notificatie Logica ────────────────────────────────
                action = analysis.get("recommended_action", "").lower()
                conviction = analysis.get("conviction", 0)
                
                if action == "buy" and conviction >= 7:
                    msg = dedent(f"""
                        🚀 <b>NEXUS BUY SIGNAL</b>
                        
                        <b>Ticker:</b> ${ticker} ({candidate.get('name')})
                        <b>Conviction:</b> {conviction}/10
                        <b>Sentiment:</b> {analysis.get('sentiment').upper()}
                        
                        <b>Note:</b> {analysis.get('analyst_note')}
                        
                        <pre>Check terminal voor volledige details.</pre>
                    """).strip()
                    send_telegram_msg(msg)
                # ──────────────────────────────────────────────────────────────
                
            else:
                candidate["tier2"] = {"error": "json_parsing_failed"}
                
        except Exception as exc:
            log.error("Claude call failed for %s: %s", ticker, exc)
            candidate["tier2"] = {"error": "api_call_failed"}
        
        time.sleep(SLEEP_BETWEEN_CALLS)

    save_json(DATA_PATH, data)
    log.info("Analysis complete and data.json updated.")

if __name__ == "__main__":
    run_analysis()
