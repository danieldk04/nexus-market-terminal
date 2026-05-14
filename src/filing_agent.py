"""
NEXUS Filing Agent — SEC EDGAR 8-K/10-K Scanner
Haalt recente regulatory filings op voor top candidates.
Resultaat: data.json["filings"] = {TICKER: {filings, cik, retrieved_at}}
"""
import json
import time
import re
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE_DIR  = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / "data.json"

EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
EDGAR_SUBS_URL    = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_VIEWER_URL  = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{fname}"

HEADERS = {
    "User-Agent": "NEXUS-MarketBot/1.0 danieldekoning66@gmail.com",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/html",
}

MAX_FILINGS = 4       # Max filings per ticker
LOOKBACK_DAYS = 45   # Kijk max 45 dagen terug
TEXT_LIMIT = 2500    # Tekens per filing document


def load_json(path, default):
    if not path.exists():
        return default
    with open(path) as f:
        try:
            return json.load(f)
        except Exception:
            return default


def get_cik_map() -> dict:
    """Ticker → CIK mapping ophalen van EDGAR (bijv. 'AAPL' → '0000320193')."""
    try:
        resp = requests.get(EDGAR_TICKERS_URL, headers=HEADERS, timeout=20)
        raw  = resp.json()
        return {
            str(v["ticker"]).upper(): str(v["cik_str"]).zfill(10)
            for v in raw.values()
        }
    except Exception as e:
        print(f"CIK map laden mislukt: {e}")
        return {}


def get_recent_filings(cik: str, form_types=("8-K", "10-K", "10-Q")) -> list:
    """Haal recente filings op voor een bedrijf via EDGAR submissions API."""
    url = EDGAR_SUBS_URL.format(cik=cik)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        data = resp.json()
    except Exception as e:
        print(f"Submissions mislukt voor CIK {cik}: {e}")
        return []

    recent  = data.get("filings", {}).get("recent", {})
    forms   = recent.get("form", [])
    dates   = recent.get("filingDate", [])
    accnums = recent.get("accessionNumber", [])
    docs    = recent.get("primaryDocument", [])
    descs   = recent.get("primaryDocDescription", [])

    cutoff  = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
    results = []

    for form, date, acc, doc, desc in zip(forms, dates, accnums, docs, descs):
        if date < cutoff:
            break
        if form not in form_types:
            continue
        results.append({
            "form":       form,
            "date":       date,
            "accession":  acc,
            "document":   doc,
            "description": desc or form,
        })
        if len(results) >= MAX_FILINGS:
            break

    return results


def fetch_filing_excerpt(cik: str, accession: str, filename: str) -> str:
    """Haal beknopte tekst op uit een EDGAR filing (HTML/TXT)."""
    acc_clean = accession.replace("-", "")
    url = EDGAR_VIEWER_URL.format(cik=int(cik), acc_clean=acc_clean, fname=filename)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        text = resp.text
        # Strip HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        # Return most informative section (skip XBRL headers — find first paragraph)
        start = text.find("ITEM") if "ITEM" in text.upper() else 0
        excerpt = text[start:start + TEXT_LIMIT]
        return excerpt if len(excerpt) > 100 else text[:TEXT_LIMIT]
    except Exception as e:
        return f"Tekst niet beschikbaar: {e}"


def normalize_ticker(ticker: str) -> str:
    """Verwijder beurssuffix voor EDGAR lookup."""
    return ticker.split(".")[0].replace("-", "").upper()


def scan_filings(candidates: list, cik_map: dict) -> dict:
    """Scan filings voor lijst van kandidaten."""
    results = {}

    for c in candidates:
        ticker     = c["ticker"]
        edgar_tick = normalize_ticker(ticker)
        cik        = cik_map.get(edgar_tick)

        if not cik:
            print(f"  Geen EDGAR CIK voor {ticker} ({edgar_tick})")
            continue

        print(f"  {ticker} → CIK {cik}: filings ophalen...")
        filings = get_recent_filings(cik)

        if not filings:
            print(f"    Geen recente filings gevonden")
            continue

        enriched = []
        for f in filings:
            entry = {
                "form":        f["form"],
                "date":        f["date"],
                "accession":   f["accession"],
                "description": f["description"],
            }
            # Haal alleen 8-K tekst op (meest actueel & klein)
            if f["form"] == "8-K" and f["document"]:
                excerpt = fetch_filing_excerpt(cik, f["accession"], f["document"])
                entry["excerpt"] = excerpt[:1500]
                time.sleep(0.3)
            enriched.append(entry)
            print(f"    {f['form']} — {f['date']}: {f['description'][:60]}")

        results[ticker] = {
            "filings":      enriched,
            "cik":          cik,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }
        time.sleep(0.5)   # Respecteer EDGAR rate limits

    return results


def main():
    print("=== NEXUS FILING AGENT STARTING ===")

    data       = load_json(DATA_PATH, {})
    candidates = data.get("top_candidates", [])[:12]

    if not candidates:
        print("Geen kandidaten in data.json")
        return

    print(f"CIK mapping ophalen van EDGAR...")
    cik_map = get_cik_map()
    if not cik_map:
        print("CIK mapping leeg — filing agent stopt.")
        return
    print(f"{len(cik_map):,} bedrijven in EDGAR database")

    print(f"Filings scannen voor {len(candidates)} kandidaten...")
    filing_data = scan_filings(candidates, cik_map)

    # Sla op in data.json
    data["filings"]    = filing_data
    data["filings_at"] = datetime.now(timezone.utc).isoformat()

    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Filing data opgeslagen voor {len(filing_data)} bedrijven.")
    print("=== FILING AGENT COMPLETE ===")


if __name__ == "__main__":
    main()
