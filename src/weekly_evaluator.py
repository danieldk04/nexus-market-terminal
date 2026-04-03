"""
NEXUS MARKET TERMINAL - Weekly Evaluator
Runs every Friday. For each prediction batch from ~7 days ago:
  1. Fetches current prices via yfinance.
  2. Calculates actual % return per ticker.
  3. Classifies each call as HIT, MISS, or NEUTRAL.
  4. Asks Claude to diagnose why calls succeeded or failed,
     cross-referencing the macro snapshot from prediction day.
  5. Distils findings into concrete prompt-adjustment rules and
     writes them back to memory.json so Tier 2 learns from them.

Requires: ANTHROPIC_API_KEY environment variable.
"""

import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from textwrap import dedent
from typing import Literal

import anthropic
import yfinance as yf

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("weekly_evaluator")

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
MEMORY_PATH = ROOT / "memory.json"

# ── Config ────────────────────────────────────────────────────────────────────
MODEL = "claude-opus-4-6"
LOOKBACK_DAYS = 7           # evaluate predictions this many days old
LOOKBACK_WINDOW = 2         # ± days tolerance when searching the right batch
MAX_ADJUSTMENTS_KEPT = 20   # cap on stored rules to prevent prompt bloat
HIT_THRESHOLD = 0.02        # +2 % = confirmed hit for a "buy" call
MISS_THRESHOLD = -0.02      # -2 % = confirmed miss for a "buy" call


# ── Types ─────────────────────────────────────────────────────────────────────

Verdict = Literal["HIT", "MISS", "NEUTRAL"]

class EvaluatedCall:
    def __init__(self, ticker: str, action: str, price_then: float,
                 price_now: float, conviction: int | None):
        self.ticker = ticker
        self.action = action            # "buy" | "watch" | "avoid"
        self.price_then = price_then
        self.price_now = price_now
        self.conviction = conviction
        self.pct_change = round((price_now - price_then) / price_then * 100, 2) if price_then else None
        self.verdict: Verdict = self._classify()

    def _classify(self) -> Verdict:
        if self.pct_change is None:
            return "NEUTRAL"
        if self.action == "buy":
            if self.pct_change >= HIT_THRESHOLD * 100:
                return "HIT"
            if self.pct_change <= MISS_THRESHOLD * 100:
                return "MISS"
        elif self.action == "avoid":
            if self.pct_change <= MISS_THRESHOLD * 100:   # stock fell → good call
                return "HIT"
            if self.pct_change >= HIT_THRESHOLD * 100:    # stock rose → bad call
                return "MISS"
        return "NEUTRAL"

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "action": self.action,
            "conviction": self.conviction,
            "price_then": self.price_then,
            "price_now": self.price_now,
            "pct_change": self.pct_change,
            "verdict": self.verdict,
        }


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_memory() -> dict:
    if MEMORY_PATH.exists():
        with open(MEMORY_PATH) as f:
            return json.load(f)
    return {"predictions": [], "evaluations": [], "prompt_adjustments": []}


def save_memory(memory: dict) -> None:
    with open(MEMORY_PATH, "w") as f:
        json.dump(memory, f, indent=2)


def find_target_prediction(memory: dict) -> dict | None:
    """
    Returns the prediction batch closest to LOOKBACK_DAYS ago,
    within ± LOOKBACK_WINDOW days. Skips already-evaluated batches.
    """
    target_date = date.today() - timedelta(days=LOOKBACK_DAYS)
    best: dict | None = None
    best_delta = timedelta(days=999)

    for pred in memory.get("predictions", []):
        # Skip if already evaluated
        if pred.get("actuals") is not None:
            continue
        try:
            pred_date = date.fromisoformat(pred["date"])
        except (KeyError, ValueError):
            continue
        delta = abs(pred_date - target_date)
        if delta <= timedelta(days=LOOKBACK_WINDOW) and delta < best_delta:
            best = pred
            best_delta = delta

    return best


def fetch_current_price(ticker: str) -> float | None:
    try:
        t = yf.Ticker(ticker)
        price = t.fast_info.get("lastPrice") or t.fast_info.get("regularMarketPrice")
        return round(float(price), 4) if price else None
    except Exception as exc:
        log.warning("Price fetch failed for %s: %s", ticker, exc)
        return None


def evaluate_batch(prediction: dict) -> list[EvaluatedCall]:
    """Builds EvaluatedCall objects for every ticker in a prediction batch."""
    results: list[EvaluatedCall] = []
    for c in prediction.get("candidates", []):
        ticker = c["ticker"]
        price_then = c.get("price")
        price_now = fetch_current_price(ticker)
        action = c.get("recommended_action", "watch")
        conviction = c.get("conviction")

        if price_then is None or price_now is None:
            log.warning("Skipping %s — missing price data", ticker)
            continue

        call = EvaluatedCall(ticker, action, price_then, price_now, conviction)
        results.append(call)
        log.info(
            "  %s | %s → %s | %+.2f%% | conviction=%s | %s",
            ticker, price_then, price_now,
            call.pct_change or 0, conviction, call.verdict,
        )
        time.sleep(0.3)

    return results


def build_diagnosis_prompt(prediction: dict, evaluated: list[EvaluatedCall]) -> str:
    macro = prediction.get("macro_snapshot", {})
    fg = prediction.get("fear_greed_snapshot", {})
    pred_date = prediction.get("date", "unknown")

    hits = [e for e in evaluated if e.verdict == "HIT"]
    misses = [e for e in evaluated if e.verdict == "MISS"]
    neutrals = [e for e in evaluated if e.verdict == "NEUTRAL"]

    def fmt_calls(calls: list[EvaluatedCall]) -> str:
        if not calls:
            return "  (none)"
        return "\n".join(
            f"  {e.ticker}: action={e.action}, conviction={e.conviction}, "
            f"return={e.pct_change:+.2f}%"
            for e in calls
        )

    hit_rate = round(len(hits) / len(evaluated) * 100) if evaluated else 0

    return dedent(f"""
        You are reviewing the NEXUS trading system's 7-day performance.
        Prediction date: {pred_date}
        Evaluation date: {date.today().isoformat()}
        Hit rate: {hit_rate}% ({len(hits)} hits / {len(misses)} misses / {len(neutrals)} neutral)

        ## Macro context ON prediction day
        - VIX: {macro.get('vix', 'N/A')}
        - 10Y Treasury: {(macro.get('treasury_10y') or 0) * 100:.2f}%
        - S&P 500 RSI-14: {macro.get('sp500_rsi', 'N/A')}
        - Fear & Greed: {fg.get('rating', 'N/A')} (score: {fg.get('score', 'N/A')}/100)

        ## HITS (correct calls)
        {fmt_calls(hits)}

        ## MISSES (wrong calls)
        {fmt_calls(misses)}

        ## NEUTRAL (inconclusive)
        {fmt_calls(neutrals)}

        ---
        Analyse this week's performance. For each MISS, identify the most likely
        cause. For HITS, identify what signals were reliable.

        Then generate up to 5 concrete, actionable rules that should be added to
        the analyst prompt to improve future performance. Each rule must be
        specific and testable (e.g. reference VIX levels, F&G scores, RSI ranges).

        Respond ONLY in this exact JSON format (no markdown fences):
        {{
          "hit_rate_pct": {hit_rate},
          "summary": "<2-3 sentence overall assessment>",
          "miss_diagnoses": [
            {{"ticker": "XYZ", "reason": "..."}}
          ],
          "hit_insights": [
            {{"ticker": "ABC", "insight": "..."}}
          ],
          "new_rules": [
            {{
              "rule": "<actionable instruction for the analyst prompt>",
              "confidence": <0.0-1.0>,
              "based_on": "<brief evidence>"
            }}
          ]
        }}
    """).strip()


def call_claude(client: anthropic.Anthropic, prompt: str) -> dict | None:
    system = (
        "You are a quantitative analyst performing post-mortem analysis "
        "on algorithmic trading predictions. Be rigorous, concise, and specific. "
        "Never speculate beyond what the data supports."
    )
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
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


def update_prompt_adjustments(memory: dict, new_rules: list[dict]) -> None:
    """
    Merges newly learned rules into memory["prompt_adjustments"].
    Keeps only the most recent MAX_ADJUSTMENTS_KEPT rules so the
    injected prompt block stays concise.
    """
    existing = memory.setdefault("prompt_adjustments", [])
    today = date.today().isoformat()

    for r in new_rules:
        # Avoid exact duplicates
        if any(e["rule"] == r["rule"] for e in existing):
            continue
        existing.append({
            "rule": r["rule"],
            "confidence": r.get("confidence", 0.5),
            "based_on": r.get("based_on", ""),
            "added_on": today,
        })

    # Keep only the most recent / highest-confidence rules
    existing.sort(key=lambda x: (x.get("added_on", ""), x.get("confidence", 0)), reverse=True)
    memory["prompt_adjustments"] = existing[:MAX_ADJUSTMENTS_KEPT]


# ── Main ──────────────────────────────────────────────────────────────────────

def run_evaluation() -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY not set.")
    client = anthropic.Anthropic(api_key=api_key)

    memory = load_memory()

    # ── 1. Find the batch to evaluate ─────────────────────────────────────────
    prediction = find_target_prediction(memory)
    if prediction is None:
        log.info("No unevaluated predictions from ~%d days ago. Nothing to do.", LOOKBACK_DAYS)
        return

    log.info("Evaluating prediction batch from %s", prediction["date"])

    # ── 2. Fetch current prices & score each call ─────────────────────────────
    evaluated = evaluate_batch(prediction)
    if not evaluated:
        log.error("No valid price data retrieved. Aborting.")
        return

    hits   = sum(1 for e in evaluated if e.verdict == "HIT")
    misses = sum(1 for e in evaluated if e.verdict == "MISS")
    log.info("Results: %d hits, %d misses, %d neutral out of %d",
             hits, misses,
             sum(1 for e in evaluated if e.verdict == "NEUTRAL"),
             len(evaluated))

    # ── 3. Ask Claude for diagnosis ───────────────────────────────────────────
    log.info("Sending batch to Claude for post-mortem analysis...")
    prompt = build_diagnosis_prompt(prediction, evaluated)
    diagnosis = call_claude(client, prompt)

    if diagnosis is None:
        log.error("Claude diagnosis failed — saving raw actuals only.")
        diagnosis = {}

    # ── 4. Write actuals back to the prediction record ────────────────────────
    prediction["actuals"] = {
        "evaluated_on": date.today().isoformat(),
        "calls": [e.to_dict() for e in evaluated],
        "hit_rate_pct": round(hits / len(evaluated) * 100) if evaluated else 0,
    }
    prediction["diagnosis"] = diagnosis

    # ── 5. Store learned rules ────────────────────────────────────────────────
    new_rules = diagnosis.get("new_rules", [])
    if new_rules:
        log.info("Storing %d new prompt-adjustment rule(s):", len(new_rules))
        for r in new_rules:
            log.info("  [%.2f] %s", r.get("confidence", 0), r["rule"])
        update_prompt_adjustments(memory, new_rules)
    else:
        log.info("No new rules generated this week.")

    # ── 6. Append to evaluations log ─────────────────────────────────────────
    memory.setdefault("evaluations", []).append({
        "week_of": prediction["date"],
        "evaluated_on": date.today().isoformat(),
        "hit_rate_pct": prediction["actuals"]["hit_rate_pct"],
        "summary": diagnosis.get("summary", ""),
        "rules_added": len(new_rules),
        "total_rules_in_memory": len(memory.get("prompt_adjustments", [])),
    })

    save_memory(memory)
    log.info("memory.json updated.")

    # ── 7. Human-readable summary ─────────────────────────────────────────────
    log.info("=== Weekly Evaluation Summary ===")
    log.info("Hit rate this week: %d%%", prediction["actuals"]["hit_rate_pct"])
    log.info("Claude says: %s", diagnosis.get("summary", "—"))
    log.info("Prompt adjustments in memory: %d", len(memory.get("prompt_adjustments", [])))
    log.info("=================================")


if __name__ == "__main__":
    log.info("=== NEXUS Weekly Evaluator starting ===")
    run_evaluation()
    log.info("=== Done ===")
