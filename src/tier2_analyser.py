"""
NEXUS Tier-2 Analyser — Agentic Deep Research met Bull vs Bear structuur
Analyseert top 10 kandidaten met: fundamentals, DCF, nieuws, SEC filings, sentiment
"""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import yfinance as yf
import anthropic

BASE_DIR  = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"

REFRESH_DAYS  = 7
TOP_N_ANALYSE = 10

SYSTEM_PROMPT = (
    "Je bent een senior quant-analist bij een hedge fund die denkt als "
    "Warren Buffett, Charlie Munger en Benjamin Graham gecombineerd. "
    "Je schrijft scherpe, eerlijke analyses in het Nederlands. "
    "Je bent kritisch op dure waarderingen, waarschuwt voor value traps "
    "en beloont echte compounding-machines met hoge ROIC."
)


def _fmt(val, suffix="", na="n/b"):
    return f"{val}{suffix}" if val is not None else na


def get_news(ticker_symbol: str) -> str:
    """Haal recente nieuwskoppen op via yfinance."""
    try:
        news  = yf.Ticker(ticker_symbol).news or []
        lines = [f"• {n.get('title','')}" for n in news[:7] if n.get("title")]
        return "\n".join(lines) if lines else "Geen recent nieuws."
    except Exception:
        return "Nieuws niet beschikbaar."


def get_filing_context(ticker: str, filings_data: dict) -> str:
    """Haal relevante SEC-filing context op als beschikbaar."""
    entry = filings_data.get(ticker, {})
    filings = entry.get("filings", [])
    if not filings:
        return ""
    lines = []
    for f in filings[:3]:
        form = f.get("form", "")
        date = f.get("date", "")
        desc = f.get("description", "")
        lines.append(f"• {form} ({date}): {desc[:80]}")
        if f.get("excerpt"):
            lines.append(f"  Excerpt: {f['excerpt'][:200]}...")
    return "\n".join(lines)


def build_bull_bear_prompt(c: dict, news: str, filing_ctx: str) -> str:
    ticker  = c["ticker"]
    name    = c.get("name", ticker)
    group   = c.get("industry_group", "?")
    score   = c.get("score", "?")
    price   = _fmt(c.get("price"), "")

    roe          = _fmt(c.get("roe"), "%")
    roic         = _fmt(c.get("roic"), "%")
    roce         = _fmt(c.get("roce"), "%")
    pe           = _fmt(c.get("pe_ratio"))
    de           = _fmt(c.get("debt_to_equity"))
    pfcf         = _fmt(c.get("pfcf"))
    margin       = _fmt(c.get("profit_margin"), "%")
    rev_growth   = _fmt(c.get("revenue_growth"), "%")
    fcf_quality  = "positief" if c.get("fcf_positive", True) else "NEGATIEF"
    beta         = _fmt(c.get("beta"))
    rev5         = _fmt(c.get("rev_cagr_5yr"), "% p.j.")
    ni5          = _fmt(c.get("ni_cagr_5yr"), "% p.j.")
    a_target     = _fmt(c.get("analyst_target"), "")
    a_upside     = _fmt(c.get("analyst_upside"), "%")
    a_count      = c.get("analyst_count", 0)

    # DCF sectie
    dcf = c.get("dcf") or {}
    dcf_block = ""
    if dcf:
        dcf_block = (
            f"\nDCF WAARDERING:\n"
            f"- Intrinsieke waarde: ${_fmt(dcf.get('dcf_per_share'))} "
            f"(upside {_fmt(dcf.get('dcf_upside'), '%')})\n"
            f"- WACC: {_fmt(dcf.get('wacc'), '%')} | Groei fase-1: {_fmt(dcf.get('growth_phase1'), '%')}\n"
            f"- Margin of Safety prijs (25% korting): ${_fmt(dcf.get('mos_price'))}"
        )

    # Dividend sectie
    div = c.get("dividend") or {}
    div_block = ""
    if div:
        sust_str = "JA" if div.get("sustainable") else f"RISICO — {div.get('risk_flag','')}"
        div_block = (
            f"\nDIVIDEND:\n"
            f"- Yield: {_fmt(div.get('yield'), '%')} | FCF payout: {_fmt(div.get('fcf_payout'), '%')}\n"
            f"- Houdbaar: {sust_str}"
        )

    # Filing context
    filing_block = f"\nRECENTE SEC FILINGS:\n{filing_ctx}" if filing_ctx else ""

    return (
        f"Schrijf een diepgaande Bull vs Bear analyse (max 400 woorden) voor {ticker} ({name}).\n\n"
        f"FUNDAMENTALS (sector: {group} | NEXUS score: {score}/10 | prijs: ${price}):\n"
        f"- ROE: {roe} | ROIC: {roic} | ROCE: {roce}\n"
        f"- P/E: {pe} | D/E: {de} | P/FCF: {pfcf}\n"
        f"- FCF: {fcf_quality} | Winstmarge: {margin} | Beta: {beta}\n"
        f"- Omzetgroei: {rev_growth} | 5-jaar omzet CAGR: {rev5} | 5-jaar winst CAGR: {ni5}\n"
        f"- Analistendoel: ${a_target} ({a_upside} upside, {a_count} analisten)"
        f"{dcf_block}{div_block}\n\n"
        f"RECENT NIEUWS:\n{news}"
        f"{filing_block}\n\n"
        f"Structureer je analyse als:\n\n"
        f"## MOAT & BUSINESS KWALITEIT\n"
        f"[Pricing power, switching costs, network effects, ROIC-trend — bewijs van echte compounding?]\n\n"
        f"## DCF & WAARDERING\n"
        f"[Is de marktprijs gerechtvaardigd? DCF upside vs. risico. Value trap check.]\n\n"
        f"## 🟢 BULL CASE (3 sterkste argumenten om te kopen)\n"
        f"1. ...\n2. ...\n3. ...\n\n"
        f"## 🔴 BEAR CASE (3 grootste risico's)\n"
        f"1. ...\n2. ...\n3. ...\n\n"
        f"## VERDICT\n"
        f"**ACTIE:** [KOOP / HOUD / MIJDEN]\n"
        f"**CONVICTION:** [1-10]\n"
        f"**KOERSDOEL 18mnd:** $[bedrag] ([X]% van huidig)\n"
        f"**SENTIMENT SCORE:** [BULLISH / NEUTRAAL / BEARISH]\n"
        f"**INSTAPMOMENT:** [Nu / Wacht op dip tot $X / Niet]"
    )


def extract_sentiment_score(analysis_text: str) -> str:
    """Haal sentiment label uit de analyse-tekst."""
    upper = analysis_text.upper()
    if "BULLISH" in upper:
        return "BULLISH"
    if "BEARISH" in upper:
        return "BEARISH"
    return "NEUTRAAL"


def run_smart_analysis():
    print("=== NEXUS DEEP RESEARCH ANALYSER STARTING ===")

    if not DATA_PATH.exists():
        print("data.json niet gevonden.")
        return

    with open(DATA_PATH) as f:
        data = json.load(f)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Geen ANTHROPIC_API_KEY — analyse overgeslagen.")
        return

    client      = anthropic.Anthropic(api_key=api_key)
    candidates  = data.get("top_candidates", [])
    filings     = data.get("filings", {})
    updated     = False

    for c in candidates[:TOP_N_ANALYSE]:
        ticker    = c["ticker"]
        tier2     = c.get("tier2", {})
        existing  = tier2.get("analysis")
        last_run  = tier2.get("last_run")

        should_analyse = True
        if existing and last_run:
            try:
                age = datetime.now(timezone.utc) - datetime.fromisoformat(last_run)
                if age < timedelta(days=REFRESH_DAYS):
                    print(f"Cache OK: {ticker} ({age.days}d oud).")
                    should_analyse = False
            except Exception:
                pass

        if not should_analyse:
            continue

        print(f"--- Analyseren: {ticker} ---")
        news        = get_news(ticker)
        filing_ctx  = get_filing_context(ticker, filings)
        prompt      = build_bull_bear_prompt(c, news, filing_ctx)

        try:
            message = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1100,
                system=[{
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": prompt}],
            )
            analysis_text = message.content[0].text
            sentiment     = extract_sentiment_score(analysis_text)

            c["tier2"] = {
                "analysis":       analysis_text,
                "sentiment_score": sentiment,
                "last_run":       datetime.now(timezone.utc).isoformat(),
                "model":          "claude-sonnet-4-6",
                "news_used":      True,
                "filing_used":    bool(filing_ctx),
                "dcf_used":       bool(c.get("dcf")),
            }
            updated = True
            print(f"  → {ticker} klaar | Sentiment: {sentiment}")

        except Exception as e:
            print(f"Fout bij Claude voor {ticker}: {e}")

    if updated:
        with open(DATA_PATH, "w") as f:
            json.dump(data, f, indent=4)
        print("Analyses bijgewerkt in data.json.")
    else:
        print("Geen nieuwe analyses nodig.")

    print("=== ANALYSER COMPLETE ===")


if __name__ == "__main__":
    run_smart_analysis()
