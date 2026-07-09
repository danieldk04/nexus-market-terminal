#!/usr/bin/env python3
"""
Shared sentiment engine used by all scrapers.
Uses VADER (free, local, no API key/cost) instead of naive keyword counting.
VADER handles negation, intensifiers, punctuation emphasis and emoji far better
than a fixed positive/negative word list, at zero marginal cost per call.
"""

import logging
from typing import Tuple

logger = logging.getLogger(__name__)

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _analyzer = SentimentIntensityAnalyzer()
    _VADER_AVAILABLE = True
except ImportError:
    _analyzer = None
    _VADER_AVAILABLE = False
    logger.warning(
        "vaderSentiment not installed (pip install vaderSentiment) — "
        "falling back to naive word-count sentiment. Install it for much better accuracy at no cost."
    )

# Domain-specific terms VADER's general lexicon doesn't know about.
# These are added on top of VADER rather than replacing it.
_FINANCE_LEXICON = {
    'bullish': 2.5, 'moon': 2.0, 'mooning': 2.5, 'pump': 1.5, 'pumping': 1.5,
    'rally': 2.0, 'breakout': 2.0, 'ath': 1.5, 'rug': -2.5, 'rugpull': -3.0,
    'dump': -2.0, 'dumping': -2.0, 'bearish': -2.5, 'crash': -2.5, 'crashing': -2.5,
    'plunge': -2.0, 'plunging': -2.0, 'rekt': -2.5, 'bagholder': -1.5,
    '🚀': 2.0, '📈': 1.5, '📉': -1.5, '💎': 1.0, '🔥': 1.0, '💩': -1.5,
}

if _VADER_AVAILABLE:
    _analyzer.lexicon.update(_FINANCE_LEXICON)


def score_text(text: str) -> float:
    """
    Score a piece of text on a -1..+1 sentiment scale.
    Uses VADER when available (context/negation-aware), otherwise a naive
    word-count fallback so the bot still runs without the dependency.
    """
    if not text:
        return 0.0

    if _VADER_AVAILABLE:
        return _analyzer.polarity_scores(text)['compound']

    return _naive_score(text)


_POSITIVE_WORDS = set([
    'bullish', 'moon', 'buy', 'pump', 'gain', 'profit', 'win', 'surge',
    'rally', 'breakout', 'strong', 'confident', 'optimistic', 'green',
    'up', 'rise', 'growth', 'boom', 'success', 'ath', 'rocket', '🚀',
    '📈', '💎', '🔥', 'fire', 'amazing', 'great', 'excellent'
])

_NEGATIVE_WORDS = set([
    'bearish', 'dump', 'sell', 'crash', 'loss', 'fail', 'drop', 'fall',
    'decline', 'weak', 'pessimistic', 'red', 'down', 'plunge',
    'bubble', 'scam', 'rug', 'dead', '📉', '💩', 'terrible',
    'awful', 'disaster', 'warning', 'danger'
])


def _naive_score(text: str) -> float:
    text_lower = text.lower()
    positive_count = sum(1 for w in _POSITIVE_WORDS if w in text_lower)
    negative_count = sum(1 for w in _NEGATIVE_WORDS if w in text_lower)
    total = positive_count + negative_count
    if total == 0:
        return 0.0
    return (positive_count - negative_count) / total


def engine_status() -> str:
    return "vader" if _VADER_AVAILABLE else "naive-fallback"
