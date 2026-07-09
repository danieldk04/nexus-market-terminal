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
import re
import xml.etree.ElementTree as ET
from urllib.parse import quote

import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; NEXUSBot/3.0; +danieldekoning66@gmail.com)"}
TIMEOUT = 8


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
    Publieke Reddit search JSON — geen auth nodig voor read-only queries.
    Telt recente mentions en pakt een paar titels als voorbeeld voor de LLM.
    """
    all_posts = []
    for sub in subreddits:
        try:
            url = f"https://www.reddit.com/r/{sub}/search.json"
            params = {"q": ticker, "restrict_sr": "on", "sort": "new", "t": "week", "limit": 10}
            res = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
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

    if reddit.get("mention_count"):
        lines.append(f"\nReddit (r/wallstreetbets, r/stocks, r/investing, laatste week): {reddit['mention_count']} mentions")
        for post in reddit.get("top_posts", [])[:3]:
            lines.append(f"  • [{post['subreddit']}] {post['title']} ({post['score']} upvotes, {post['num_comments']} comments)")
    else:
        lines.append("\nReddit: geen recente mentions gevonden")

    if news:
        lines.append(f"\nBreder nieuws (Google News, top {len(news)}):")
        for headline in news[:7]:
            lines.append(f"  • {headline}")

    return {
        "text_block": "\n".join(lines),
        "stocktwits": stocktwits,
        "reddit_mention_count": reddit.get("mention_count", 0),
        "news_count": len(news),
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
