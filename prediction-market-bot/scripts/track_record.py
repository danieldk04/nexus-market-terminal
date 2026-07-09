#!/usr/bin/env python3
"""
Local, free track-record store.
Persists every signal and its eventual outcome (win/loss) to a JSON file,
so the bot can compute a REAL historical win rate per topic/category
instead of the hardcoded stub that used to live in SignalAnalyzer.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from threading import Lock

logger = logging.getLogger(__name__)

_DEFAULT_PATH = Path(__file__).resolve().parent.parent / 'data' / 'track_record.json'
_lock = Lock()


class TrackRecord:
    def __init__(self, path: Path = _DEFAULT_PATH):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._records: List[Dict] = self._load()

    def _load(self) -> List[Dict]:
        if not self.path.exists():
            return []
        try:
            with open(self.path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not load track record ({e}), starting fresh")
            return []

    def _save(self):
        with _lock:
            with open(self.path, 'w') as f:
                json.dump(self._records, f, indent=2)

    def record_signal(self, market_id: str, topic: str, category: str, signal_type: str, entry: float):
        """Call when a signal is generated/traded."""
        self._records.append({
            'market_id': market_id,
            'topic': topic,
            'category': category,
            'signal_type': signal_type,
            'entry': entry,
            'outcome': None,  # filled in later via record_outcome()
            'pnl': None,
            'created_at': datetime.now().isoformat(),
            'resolved_at': None,
        })
        self._save()

    def record_outcome(self, market_id: str, won: bool, pnl: float):
        """Call when a position is closed / a market resolves."""
        for r in reversed(self._records):
            if r['market_id'] == market_id and r['outcome'] is None:
                r['outcome'] = 'win' if won else 'loss'
                r['pnl'] = pnl
                r['resolved_at'] = datetime.now().isoformat()
                self._save()
                return
        logger.warning(f"No open record found for market_id={market_id} to resolve")

    def stats_for(self, topic: Optional[str] = None, category: Optional[str] = None) -> Dict:
        """
        Real historical win rate, filtered by topic and/or category.
        Falls back to broader scope when there isn't enough data yet,
        so early on the bot doesn't overfit to a handful of trades.
        """
        resolved = [r for r in self._records if r['outcome'] is not None]

        def _filter(records, t, c):
            out = records
            if t:
                out = [r for r in out if r['topic'] == t]
            if c:
                out = [r for r in out if r['category'] == c]
            return out

        scoped = _filter(resolved, topic, category)

        # Not enough topic-level data -> fall back to category -> fall back to global
        if len(scoped) < 5 and category:
            scoped = _filter(resolved, None, category)
        if len(scoped) < 5:
            scoped = resolved

        if not scoped:
            return {'sample_size': 0, 'win_rate': None, 'score': 0.0, 'confidence': 0.0}

        wins = sum(1 for r in scoped if r['outcome'] == 'win')
        win_rate = wins / len(scoped)

        # score: -1..+1, centered on 50% (a coin-flip topic gets score 0)
        score = (win_rate - 0.5) * 2

        # confidence grows with sample size, capped once we have solid evidence
        confidence = min(len(scoped) / 30, 1.0)

        return {
            'sample_size': len(scoped),
            'win_rate': win_rate,
            'score': score,
            'confidence': confidence,
        }
