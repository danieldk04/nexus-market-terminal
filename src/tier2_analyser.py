"""
NEXUS MARKET TERMINAL - Tier 2 Analyser
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import anthropic
import yfinance as yf

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
# GEBRUIK SONNET 3.5 VOOR DE BESTE BALANS TUSSEN SNELHEID EN INTELLIGENTIE
MODEL = "claude-4-6-sonnet-latest" 
MAX_NEWS_ITEMS = 8
SLEEP_BETWEEN_CALLS = 1.0

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)

def save_json(path: Path, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def fetch_news(ticker: str) -> list[dict]:
    try:
        t = yf.Ticker(ticker)
        raw = t.news or []
        items = []
        for article in raw[:MAX_NEWS_ITEMS]:
            items.append({
                "title": article.get("title", ""),
                "publisher": article.get("publisher", ""),
                "published_at": datetime.fromtimestamp(
                    article.get("providerPublishTime", 0), tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M UTC"),
                "summary": article.get("summary", ""),
            })
        return items
    except Exception as exc:
        log.warning("News fetch failed for %s: %s", ticker, exc)
        return []

def load_prompt_adjustments(memory: dict) -> str:
    adjustments = memory.get("prompt_adjustments", [])
    if not adjustments:
        return ""
    recent = adjustments[-5:]
    lines = "\n".join(f"- {a['rule']}" for a in recent)
    return f"\n\n## Self-Learned Adjustments (applied automatically)\n{lines}"

def build_system_prompt(adjustments: str) -> str:
    base = dedent("""
        You are a senior equity analyst for the NEXUS Market Terminal.
        Your task is to evaluate a stock candidate that has already passed
        fundamental screening (dividend, P/E, EPS growth).

        Your job: assess the QUALITATIVE picture — news sentiment, narrative
        risk, macro tailwinds/headwinds — and produce a structured verdict.

        Rules:
        - Be concise and data-driven. No padding.
        - Separate facts from opinions clearly.
        - If news is sparse or old (>14 days), say so and reduce confidence.
        - Always flag if the macro context (VIX, yield, RSI) is a headwind.
        - Conviction scale: 1 (avoid) to 10 (high conviction buy).
    """).strip()
    return base + adjustments

def build_user_prompt(candidate: dict, news: list[dict], macro: dict, fear_greed: dict) -> str:
    # VEILIGE EXTRACTIE
    ticker = candidate.get('ticker', 'Unknown')
    name = candidate.get('name', 'Unknown')
    sector = candidate.get('sector', 'N/A')
    price = candidate.get('price', 'N/A')
    div = candidate.get('dividend_yield', 'N/A')
    pe = candidate.get('pe_ratio', 'N/A')
    pe_median = candidate.get('sector_pe_median', 'N/A')
    growth = candidate.get('eps_growth_3yr', 'N/A')
    t1_score = candidate.get('score', 'N/A')

    news_block = "\n".join(
        f"  [{i+1}] {n['published_at']} | {n['publisher']}\n"
        f"      {n['title']}\n"
        f"      {n['summary'][:200] if n['summary'] else '(no summary)'}"
        for i, n in enumerate(news)
    ) or "  No recent news found."

    return dedent(f"""
        ## Candidate: {ticker} — {name}

        ### Fundamentals (Tier 1 output)
        - Sector: {sector}
        - Price: ${price}
        - Dividend yield: {div}%
        - Trailing P/E: {pe} (sector median: {pe_median})
        - EPS CAGR 3yr: {growth}%
        - Tier 1 score: {t1_score}

        ### Macro Context
        - VIX: {macro.get('vix', 'N/A')}
        - 10Y Treasury: {macro.get('treasury_10y', 'N/A')}%
        - S&P 500 RSI-14: {macro.get('sp500_rsi', 'N/A')}
        - Fear & Greed: {fear_greed.get('rating', 'N/A')} ({fear_greed.get('score', 'N/A')}/100)

        ### Recent News
        {news_block}

        ---
        Respond in this exact JSON format (no markdown fences, just JSON):
        {{
          "sentiment": "bullish" | "neutral" | "bearish",
          "conviction": <1-10>,
          "key_positives": ["...", "..."],
          "key_risks": ["...", "..."],
          "macro_alignment": "tailwind" | "neutral" | "headwind",
          "analyst_note": "<2-3 sentence synthesis>",
          "recommended_action": "buy" | "watch" | "avoid"
        }}
    """).strip()

def call_claude(client: anthropic.Anthropic, system: str, user: str) -> dict | None:
    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=512,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as exc:
        log.error("Claude call failed: %s", exc)
        return None

def run_analysis() -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY missing!")
        return

    client = anthropic.Anthropic(api_key=api_key)
    data = load_json(DATA_PATH)
    memory = load_json(MEMORY_PATH) if MEMORY_PATH.exists() else {"predictions": []}

    candidates = data.get("top_candidates", [])
    macro = data.get("macro", {})
    fear_greed = data.get("fear_and_greed", {})

    if not candidates:
        log.error("No candidates in data.json")
        return

    system_prompt = build_system_prompt(load_prompt_adjustments(memory))
    log.info("Analysing %d candidates...", len(candidates))

    enriched = []
    for candidate in candidates:
        ticker = candidate["ticker"]
        log.info("  → %s", ticker)
        news = fetch_news(ticker)
        user_prompt = build_user_prompt(candidate, news, macro, fear_greed)
        analysis = call_claude(client, system_prompt, user_prompt)

        if analysis:
            candidate["tier2"] = {**analysis, "analysed_at": datetime.now(timezone.utc).isoformat()}
        else:
            candidate["tier2"] = {"error": "analysis_failed"}
        
        enriched.append(candidate)
        time.sleep(SLEEP_BETWEEN_CALLS)

    data["top_candidates"] = enriched
    save_json(DATA_PATH, data)
    log.info("Analysis complete and data.json updated.")

if __name__ == "__main__":
    run_analysis()
