"""
NEXUS MARKET TERMINAL - Tier 2 Analyser
Reads top 10 candidates from data.json, enriches each with recent news
(via yfinance), then asks Claude to score sentiment + produce a
conviction rating. Updates data.json and memory.json in place.

Requires: ANTHROPIC_API_KEY environment variable (GitHub Actions Secret).
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
MODEL = "claude-opus-4-6"          # change to claude-haiku-4-5-20251001 to cut cost
MAX_NEWS_ITEMS = 8                  # per ticker, to keep context compact
SLEEP_BETWEEN_CALLS = 1.0          # seconds, to respect rate limits


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def save_json(path: Path, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def fetch_news(ticker: str) -> list[dict]:
    """
    Returns up to MAX_NEWS_ITEMS recent news items for a ticker.
    Each item: { title, publisher, published_at, summary (if available) }
    """
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
    """
    Returns a string with self-learned adjustments to inject into the system
    prompt. Built up over time by the weekly evaluator.
    """
    adjustments = memory.get("prompt_adjustments", [])
    if not adjustments:
        return ""
    # Only use the 5 most recent to keep context short
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
    news_block = "\n".join(
        f"  [{i+1}] {n['published_at']} | {n['publisher']}\n"
        f"      {n['title']}\n"
        f"      {n['summary'][:200] if n['summary'] else '(no summary)'}"
        for i, n in enumerate(news)
    ) or "  No recent news found."

    return dedent(f"""
        ## Candidate: {candidate['ticker']} — {candidate['name']}

        ### Fundamentals (Tier 1 output)
        - Sector: {candidate['sector']}
        - Price: ${candidate['price']}
        - Dividend yield: {candidate['dividend_yield']}%
        - Trailing P/E: {candidate['pe_ratio']} (sector median: {candidate['sector_pe_median']})
        - EPS CAGR 3yr: {candidate['eps_growth_3yr']}%
        - Tier 1 score: {candidate['score']}

        ### Macro Context
        - VIX: {macro.get('vix', 'N/A')}
        - 10Y Treasury: {(macro.get('treasury_10y') or 0) * 100:.2f}%
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


def call_claude(client: anthropic.Anthropic,
                system: str,
                user: str) -> dict | None:
    """Calls Claude and parses the JSON response."""
    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=512,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        raw = message.content[0].text.strip()
        # Strip accidental markdown code fences
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("Claude response not valid JSON: %s", exc)
        return None
    except anthropic.APIError as exc:
        log.error("Claude API error: %s", exc)
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def run_analysis() -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY not set. "
            "Add it as a GitHub Actions Secret or export it locally."
        )

    client = anthropic.Anthropic(api_key=api_key)

    data = load_json(DATA_PATH)
    memory = load_json(MEMORY_PATH) if MEMORY_PATH.exists() else {}

    candidates = data.get("top_candidates", [])
    macro = data.get("macro", {})
    fear_greed = data.get("fear_and_greed", {})

    if not candidates:
        log.error("No candidates found in data.json — run tier1_scanner.py first.")
        return

    adjustments = load_prompt_adjustments(memory)
    system_prompt = build_system_prompt(adjustments)

    log.info("Analysing %d candidates with Claude (%s)...", len(candidates), MODEL)

    enriched: list[dict] = []
    for candidate in candidates:
        ticker = candidate["ticker"]
        log.info("  → %s", ticker)

        news = fetch_news(ticker)
        log.info("     %d news items fetched", len(news))

        user_prompt = build_user_prompt(candidate, news, macro, fear_greed)
        analysis = call_claude(client, system_prompt, user_prompt)

        if analysis:
            candidate["tier2"] = {
                **analysis,
                "analysed_at": datetime.now(timezone.utc).isoformat(),
                "news_count": len(news),
            }
            log.info("     conviction=%s  action=%s  sentiment=%s",
                     analysis.get("conviction"),
                     analysis.get("recommended_action"),
                     analysis.get("sentiment"))
        else:
            candidate["tier2"] = {"error": "analysis_failed"}
            log.warning("     Analysis failed for %s", ticker)

        enriched.append(candidate)
        time.sleep(SLEEP_BETWEEN_CALLS)

    # Sort by combined score: Tier1 score × conviction (nulls last)
    def combined_sort_key(c: dict) -> float:
        t2 = c.get("tier2", {})
        conviction = t2.get("conviction") or 0
        return c.get("score", 0) * conviction

    enriched.sort(key=combined_sort_key, reverse=True)

    # Write back enriched candidates
    data["top_candidates"] = enriched
    data["tier2_completed_at"] = datetime.now(timezone.utc).isoformat()
    save_json(DATA_PATH, data)
    log.info("data.json updated with Tier 2 analysis.")

    # Update memory: fill in confidence for today's prediction entry
    if memory and memory.get("predictions"):
        today = datetime.now(timezone.utc).date().isoformat()
        for pred in reversed(memory["predictions"]):
            if pred.get("date") == today:
                for c in enriched:
                    t2 = c.get("tier2", {})
                    for pred_c in pred.get("candidates", []):
                        if pred_c["ticker"] == c["ticker"]:
                            pred_c["conviction"] = t2.get("conviction")
                            pred_c["recommended_action"] = t2.get("recommended_action")
                pred["confidence"] = _avg_conviction(enriched)
                break
        save_json(MEMORY_PATH, memory)
        log.info("memory.json confidence scores updated.")

    # Print final ranking to stdout (visible in Actions logs)
    log.info("=== Final Ranking ===")
    for i, c in enumerate(enriched, 1):
        t2 = c.get("tier2", {})
        log.info(
            "  #%d %s | conviction=%s | action=%s | %s",
            i, c["ticker"],
            t2.get("conviction", "?"),
            t2.get("recommended_action", "?"),
            t2.get("analyst_note", "")[:80],
        )


def _avg_conviction(candidates: list[dict]) -> float | None:
    scores = [
        c["tier2"]["conviction"]
        for c in candidates
        if isinstance(c.get("tier2", {}).get("conviction"), (int, float))
    ]
    return round(sum(scores) / len(scores), 1) if scores else None


if __name__ == "__main__":
    log.info("=== NEXUS Tier 2 Analyser starting ===")
    run_analysis()
    log.info("=== Done ===")
