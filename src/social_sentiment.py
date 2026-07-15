"""
NEXUS Social Sentiment — gratis, geen API-key nodig
Haalt crowd-sentiment op uit drie bronnen die geen betaalde key vereisen:
  - StockTwits: expliciete, door gebruikers zelf getagde Bullish/Bearish labels
  - Reddit: publieke JSON search endpoint (geen OAuth nodig voor read-only)
  - Google News RSS: bredere nieuwsdekking dan yfinance's 7 koppen

Resultaat wordt zowel als tekstblok (voor de Tier-2 LLM-prompt) als
gestructureerde data (bull/bear ratio) teruggegeven, zodat het ook
buiten de LLM-analyse om bruikbaar is.
"""
import os
import re
import time
import xml.etree.ElementTree as ET
from urllib.parse import quote

import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; NEXUSBot/3.0; +danieldekoning66@gmail.com)"}
TIMEOUT = 8

# Reddit locked down its anonymous JSON endpoints (they now 403 regardless of
# User-Agent). A free "script" app at https://www.reddit.com/prefs/apps still
# gets full read access at zero cost — just set these two env vars to enable
# it. Without them, Reddit mentions are silently skipped (StockTwits + Google
# News still work with no setup at all).
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = "python:nexus-sentiment:v1.0 (by /u/nexus-bot)"

_reddit_token = {"value": None, "expires_at": 0}


def _get_reddit_token() -> str | None:
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        return None
    if _reddit_token["value"] and time.time() < _reddit_token["expires_at"]:
        return _reddit_token["value"]
    try:
        res = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": REDDIT_USER_AGENT},
            timeout=TIMEOUT,
        )
        if res.status_code != 200:
            return None
        payload = res.json()
        _reddit_token["value"] = payload["access_token"]
        _reddit_token["expires_at"] = time.time() + payload.get("expires_in", 3600) - 60
        return _reddit_token["value"]
    except Exception:
        return None


def get_stocktwits_sentiment(ticker: str) -> dict:
    """
    Publieke StockTwits stream voor een symbool. Elke message kan door de
    auteur zelf getagd zijn als Bullish/Bearish — een direct crowd-signaal,
    geen sentiment-inferentie nodig.
    """
    try:
        url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
        res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if res.status_code != 200:
            return {"available": False}

        data = res.json()
        messages = data.get("messages", [])

        bullish = bearish = untagged = 0
        sample_titles = []
        for m in messages:
            sentiment = (m.get("entities", {}) or {}).get("sentiment")
            label = (sentiment or {}).get("basic") if sentiment else None
            if label == "Bullish":
                bullish += 1
            elif label == "Bearish":
                bearish += 1
            else:
                untagged += 1
            if len(sample_titles) < 5 and m.get("body"):
                sample_titles.append(m["body"][:140])

        tagged = bullish + bearish
        ratio = bullish / tagged if tagged else None

        return {
            "available": True,
            "message_count": len(messages),
            "bullish": bullish,
            "bearish": bearish,
            "untagged": untagged,
            "bullish_ratio": round(ratio, 2) if ratio is not None else None,
            "sample_messages": sample_titles,
        }
    except Exception:
        return {"available": False}


def get_reddit_mentions(ticker: str, subreddits=("wallstreetbets", "stocks", "investing")) -> dict:
    """
    Reddit search via OAuth (free "script" app credentials, see
    REDDIT_CLIENT_ID/SECRET above). Reddit's anonymous JSON endpoint now
    403s unconditionally, so this is skipped entirely if no token can be
    obtained — the rest of the sentiment pipeline still works fine.
    """
    token = _get_reddit_token()
    if not token:
        return {"available": False, "mention_count": 0, "top_posts": []}

    auth_headers = {"Authorization": f"bearer {token}", "User-Agent": REDDIT_USER_AGENT}

    all_posts = []
    for sub in subreddits:
        try:
            url = f"https://oauth.reddit.com/r/{sub}/search"
            params = {"q": ticker, "restrict_sr": "on", "sort": "new", "t": "week", "limit": 10}
            res = requests.get(url, headers=auth_headers, params=params, timeout=TIMEOUT)
            if res.status_code != 200:
                continue
            children = res.json().get("data", {}).get("children", [])
            for child in children:
                post = child.get("data", {})
                title = post.get("title", "")
                # Filter false positives (ticker as substring of a normal word)
                if re.search(rf"\b{re.escape(ticker)}\b", title, re.IGNORECASE) or \
                   re.search(rf"\${re.escape(ticker)}\b", title, re.IGNORECASE):
                    all_posts.append({
                        "subreddit": sub,
                        "title": title,
                        "score": post.get("score", 0),
                        "num_comments": post.get("num_comments", 0),
                    })
        except Exception:
            continue

    all_posts.sort(key=lambda p: p["score"], reverse=True)

    return {
        "available": True,
        "mention_count": len(all_posts),
        "top_posts": all_posts[:5],
    }


# Lichte keyword-lexicons voor bronnen zonder expliciete bull/bear-tags
# (Bluesky). Bewust simpel gehouden — dit is een 'tell', geen NLP-model.
_BULL_WORDS = ("buy", "bullish", "long", "calls", "moon", "breakout", "rally",
               "undervalued", "strong buy", "accumulate", "up", "🚀", "📈")
_BEAR_WORDS = ("sell", "bearish", "short", "puts", "crash", "dump", "overvalued",
               "avoid", "weak", "down", "📉", "bagholder")


def get_bluesky_sentiment(ticker: str, company_name: str = "") -> dict:
    """
    Bluesky post-search via het publieke AppView-endpoint (AT Protocol).
    Geen key/OAuth nodig voor read-only search. Bluesky kent geen expliciete
    Bullish/Bearish tags zoals StockTwits, dus we leiden een licht sentiment
    af met een keyword-lexicon — een indicatie, geen bewijs.
    """
    try:
        url = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
        # Cashtag-zoekopdracht; valt terug op bedrijfsnaam voor bredere dekking
        params = {"q": f"${ticker}", "limit": 25, "sort": "latest"}
        res = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        if res.status_code != 200:
            return {"available": False}

        posts = res.json().get("posts", [])
        bullish = bearish = 0
        samples = []
        for p in posts:
            text = ((p.get("record") or {}).get("text") or "").lower()
            if not text:
                continue
            b = sum(w in text for w in _BULL_WORDS)
            s = sum(w in text for w in _BEAR_WORDS)
            if b > s:
                bullish += 1
            elif s > b:
                bearish += 1
            if len(samples) < 5:
                samples.append(text[:140])

        tagged = bullish + bearish
        ratio = bullish / tagged if tagged else None
        return {
            "available": True,
            "post_count": len(posts),
            "bullish": bullish,
            "bearish": bearish,
            "bullish_ratio": round(ratio, 2) if ratio is not None else None,
            "sample_posts": samples,
        }
    except Exception:
        return {"available": False}


def get_broad_news(ticker: str, company_name: str = "") -> list[str]:
    """
    Google News RSS — gratis, geen key, bredere dekking dan yfinance .news.
    """
    try:
        query = f"{company_name or ticker} stock"
        url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"
        res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if res.status_code != 200:
            return []

        root = ET.fromstring(res.content)
        titles = [item.findtext("title") for item in root.findall(".//item")[:10]]
        return [t for t in titles if t]
    except Exception:
        return []


def build_sentiment_context(ticker: str, company_name: str = "") -> dict:
    """
    Verzamelt alle gratis sentiment-bronnen en bouwt zowel een tekstblok
    (voor injectie in de Tier-2 prompt) als gestructureerde data.
    """
    stocktwits = get_stocktwits_sentiment(ticker)
    reddit = get_reddit_mentions(ticker)
    bluesky = get_bluesky_sentiment(ticker, company_name)
    news = get_broad_news(ticker, company_name)

    lines = []

    if stocktwits.get("available") and stocktwits.get("message_count"):
        ratio = stocktwits.get("bullish_ratio")
        ratio_str = f"{ratio:.0%} bullish" if ratio is not None else "geen expliciete tags"
        lines.append(
            f"StockTwits: {stocktwits['message_count']} recente berichten, "
            f"{stocktwits['bullish']} bullish / {stocktwits['bearish']} bearish getagd ({ratio_str})"
        )
        for msg in stocktwits.get("sample_messages", [])[:3]:
            lines.append(f"  • {msg}")
    else:
        lines.append("StockTwits: geen data beschikbaar")

    if not reddit.get("available"):
        lines.append("\nReddit: overgeslagen (geen REDDIT_CLIENT_ID/SECRET geconfigureerd)")
    elif reddit.get("mention_count"):
        lines.append(f"\nReddit (r/wallstreetbets, r/stocks, r/investing, laatste week): {reddit['mention_count']} mentions")
        for post in reddit.get("top_posts", [])[:3]:
            lines.append(f"  • [{post['subreddit']}] {post['title']} ({post['score']} upvotes, {post['num_comments']} comments)")
    else:
        lines.append("\nReddit: geen recente mentions gevonden")

    if bluesky.get("available") and bluesky.get("post_count"):
        ratio = bluesky.get("bullish_ratio")
        ratio_str = f"{ratio:.0%} bullish (keyword-afgeleid)" if ratio is not None else "neutraal"
        lines.append(
            f"\nBluesky: {bluesky['post_count']} recente posts, "
            f"{bluesky['bullish']} bullish / {bluesky['bearish']} bearish ({ratio_str})"
        )
        for msg in bluesky.get("sample_posts", [])[:3]:
            lines.append(f"  • {msg}")
    else:
        lines.append("\nBluesky: geen data beschikbaar")

    if news:
        lines.append(f"\nBreder nieuws (Google News, top {len(news)}):")
        for headline in news[:7]:
            lines.append(f"  • {headline}")

    # Gecombineerde bull-ratio over bronnen met een expliciete/afgeleide ratio,
    # gewogen naar het aantal getagde berichten per bron. Dit is één samengevat
    # sentiment-getal voor de signaal-database.
    weighted_bull, weight = 0.0, 0
    st_tag = stocktwits.get("bullish", 0) + stocktwits.get("bearish", 0)
    if st_tag and stocktwits.get("bullish_ratio") is not None:
        weighted_bull += stocktwits["bullish_ratio"] * st_tag
        weight += st_tag
    bs_tag = bluesky.get("bullish", 0) + bluesky.get("bearish", 0)
    if bs_tag and bluesky.get("bullish_ratio") is not None:
        weighted_bull += bluesky["bullish_ratio"] * bs_tag
        weight += bs_tag
    combined_ratio = round(weighted_bull / weight, 3) if weight else None

    return {
        "text_block": "\n".join(lines),
        "stocktwits": stocktwits,
        "bluesky": bluesky,
        "reddit_mention_count": reddit.get("mention_count", 0),
        "news_count": len(news),
        "combined_bull_ratio": combined_ratio,
    }


if __name__ == "__main__":
    import sys
    ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    ctx = build_sentiment_context(ticker)
    print(ctx["text_block"])
    print("\n--- structured ---")
    print("StockTwits bullish_ratio:", ctx["stocktwits"].get("bullish_ratio"))
    print("Reddit mentions:", ctx["reddit_mention_count"])
    print("News headlines:", ctx["news_count"])
