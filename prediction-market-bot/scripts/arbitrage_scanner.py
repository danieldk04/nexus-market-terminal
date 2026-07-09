#!/usr/bin/env python3
"""
Arbitrage Scanner Agent
Finds the same real-world question priced differently across Polymarket,
Manifold and Kalshi. This is a free, zero-API-cost edge: no sentiment or
research needed, just a probability mismatch between platforms for an
(approximately) identical event.
"""

import logging
import re
from difflib import SequenceMatcher
from itertools import combinations
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_STOPWORDS = {
    'will', 'the', 'a', 'an', 'be', 'in', 'on', 'by', 'to', 'of', 'for',
    'is', 'are', 'and', 'or', 'at', 'than', 'more', 'less', 'this', 'that'
}


def _normalize(title: str) -> str:
    title = title.lower()
    title = re.sub(r'[^a-z0-9\s]', ' ', title)
    words = [w for w in title.split() if w not in _STOPWORDS]
    return ' '.join(words)


def _title_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()


class ArbitrageScanner:
    """
    Groups markets from different platforms that likely track the same
    event, and flags meaningful probability gaps between them.
    """

    def __init__(self, config: Dict):
        self.config = config
        arb_cfg = config.get('arbitrage', {})
        # How similar two titles must be to be considered "the same event"
        self.similarity_threshold = arb_cfg.get('similarity_threshold', 0.6)
        # Minimum probability gap (in percentage points) worth flagging
        self.min_gap = arb_cfg.get('min_probability_gap', 0.05)
        # Minimum liquidity on BOTH sides for the arb to be executable
        self.min_liquidity = arb_cfg.get('min_liquidity', config.get('trading', {}).get('min_liquidity', 100000))

    def scan(self, markets: List[Dict]) -> List[Dict]:
        """
        markets: combined list of market dicts from all platforms, each with
                 'id', 'title', 'platform', 'probability', 'liquidity'
        Returns a list of arbitrage opportunities, most profitable first.
        """
        by_platform: Dict[str, List[Dict]] = {}
        for m in markets:
            by_platform.setdefault(m.get('platform', 'unknown'), []).append(m)

        platforms = list(by_platform.keys())
        opportunities = []

        for p1, p2 in combinations(platforms, 2):
            for m1 in by_platform[p1]:
                if m1.get('liquidity', 0) < self.min_liquidity:
                    continue
                best_match: Optional[Dict] = None
                best_score = 0.0

                for m2 in by_platform[p2]:
                    if m2.get('liquidity', 0) < self.min_liquidity:
                        continue
                    score = _title_similarity(m1['title'], m2['title'])
                    if score > best_score:
                        best_score = score
                        best_match = m2

                if best_match and best_score >= self.similarity_threshold:
                    gap = abs(m1.get('probability', 0.5) - best_match.get('probability', 0.5))
                    if gap >= self.min_gap:
                        opportunities.append(self._build_opportunity(m1, best_match, best_score, gap))

        opportunities.sort(key=lambda o: o['gap'], reverse=True)

        if opportunities:
            logger.info(f"💰 Found {len(opportunities)} cross-platform arbitrage opportunities")
        else:
            logger.info("🔍 No arbitrage opportunities found this scan")

        return opportunities

    def _build_opportunity(self, m1: Dict, m2: Dict, similarity: float, gap: float) -> Dict:
        cheap, expensive = (m1, m2) if m1['probability'] < m2['probability'] else (m2, m1)

        return {
            'type': 'ARBITRAGE',
            'buy_platform': cheap['platform'],
            'buy_market': cheap,
            'sell_platform': expensive['platform'],
            'sell_market': expensive,
            'gap': gap,
            'title_similarity': similarity,
            # Confidence scales with both how sure we are it's the same
            # event and how big the mispricing is — this needs no
            # sentiment/research input, so it's treated as high-trust.
            'confidence': min(0.5 + gap + (similarity - self.similarity_threshold), 0.98),
            'reasoning': (
                f"'{cheap['title']}' ({cheap['platform']}) priced at {cheap['probability']:.1%} vs "
                f"'{expensive['title']}' ({expensive['platform']}) at {expensive['probability']:.1%} "
                f"— {gap:.1%} gap, {similarity:.0%} title match"
            ),
        }


# Standalone testing
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    config = {'arbitrage': {'similarity_threshold': 0.5, 'min_probability_gap': 0.03}}
    scanner = ArbitrageScanner(config)

    mock_markets = [
        {'id': 'p1', 'title': 'Will Bitcoin exceed $100,000 by June 2026?',
         'platform': 'polymarket', 'probability': 0.62, 'liquidity': 500000},
        {'id': 'k1', 'title': 'Bitcoin above $100K by June 2026',
         'platform': 'kalshi', 'probability': 0.71, 'liquidity': 300000},
        {'id': 'm1', 'title': 'Will the Fed cut rates in Q3 2026?',
         'platform': 'manifold', 'probability': 0.40, 'liquidity': 150000},
    ]

    for opp in scanner.scan(mock_markets):
        print(opp['reasoning'], '| confidence:', f"{opp['confidence']:.0%}")
