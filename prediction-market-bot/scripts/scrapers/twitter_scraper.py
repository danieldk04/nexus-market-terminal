#!/usr/bin/env python3
"""
Twitter Scraper Agent
Scrapes Twitter for market sentiment and trending topics
Analyzes tweet volume, sentiment, and influencer activity
"""

import asyncio
import aiohttp
import logging
from typing import Dict, List
from datetime import datetime, timedelta
from collections import Counter
import re

logger = logging.getLogger(__name__)


class TwitterScraper:
    """
    Scrapes and analyzes Twitter for market sentiment
    Uses Twitter API v2
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.api_base = 'https://api.twitter.com/2'
        self.bearer_token = config['api_keys'].get('twitter', '')
        
        # Keywords to track
        self.keywords = config['scraping']['twitter_keywords']
        self.max_tweets = config['scraping'].get('twitter_max_tweets', 500)
        
        # Sentiment tracking
        self.positive_words = set([
            'bullish', 'moon', 'buy', 'pump', 'gain', 'profit', 'win', 'surge',
            'rally', 'breakout', 'strong', 'confident', 'optimistic', 'green',
            'up', 'rise', 'growth', 'boom', 'success', 'ath', 'rocket', '🚀',
            '📈', '💎', '🔥', 'fire', 'amazing', 'great', 'excellent'
        ])
        
        self.negative_words = set([
            'bearish', 'dump', 'sell', 'crash', 'loss', 'fail', 'drop', 'fall',
            'decline', 'weak', 'bearish', 'pessimistic', 'red', 'down', 'plunge',
            'crash', 'bubble', 'scam', 'rug', 'dead', '📉', '💩', 'terrible',
            'awful', 'disaster', 'warning', 'danger'
        ])
    
    async def scrape(self) -> List[Dict]:
        """
        Scrape Twitter for relevant tweets
        Returns list of tweet data with sentiment analysis
        """
        logger.info("🐦 Starting Twitter scrape...")
        
        all_tweets = []
        
        # Scrape for each keyword
        for keyword in self.keywords:
            try:
                tweets = await self.search_tweets(keyword)
                all_tweets.extend(tweets)
            except Exception as e:
                logger.error(f"Error scraping keyword '{keyword}': {e}")
        
        # Remove duplicates
        unique_tweets = self.deduplicate_tweets(all_tweets)
        
        # Analyze sentiment
        analyzed = [self.analyze_sentiment(tweet) for tweet in unique_tweets]
        
        # Calculate aggregate metrics
        metrics = self.calculate_metrics(analyzed)
        
        logger.info(f"✅ Twitter scrape complete: {len(analyzed)} tweets analyzed")
        logger.info(f"   Sentiment: {metrics['sentiment_score']:.2f} "
                   f"({metrics['positive_pct']:.1%} positive)")
        
        return analyzed
    
    async def search_tweets(self, query: str, max_results: int = 100) -> List[Dict]:
        """
        Search Twitter API for tweets matching query
        """
        if not self.bearer_token or self.bearer_token == 'YOUR_TWITTER_BEARER_TOKEN':
            logger.warning("Twitter API key not configured, using mock data")
            return self.generate_mock_tweets(query, max_results)
        
        try:
            headers = {
                'Authorization': f'Bearer {self.bearer_token}'
            }
            
            # Recent search endpoint
            url = f"{self.api_base}/tweets/search/recent"
            
            # Query params
            params = {
                'query': f'{query} -is:retweet lang:en',
                'max_results': min(max_results, 100),  # API limit
                'tweet.fields': 'created_at,public_metrics,author_id',
                'user.fields': 'verified,public_metrics',
                'expansions': 'author_id'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status != 200:
                        logger.error(f"Twitter API error: {response.status}")
                        return self.generate_mock_tweets(query, max_results)
                    
                    data = await response.json()
                    
                    # Parse response
                    tweets = []
                    users = {u['id']: u for u in data.get('includes', {}).get('users', [])}
                    
                    for tweet_data in data.get('data', []):
                        author = users.get(tweet_data['author_id'], {})
                        
                        tweets.append({
                            'id': tweet_data['id'],
                            'text': tweet_data['text'],
                            'created_at': tweet_data['created_at'],
                            'likes': tweet_data['public_metrics']['like_count'],
                            'retweets': tweet_data['public_metrics']['retweet_count'],
                            'replies': tweet_data['public_metrics']['reply_count'],
                            'author_verified': author.get('verified', False),
                            'author_followers': author.get('public_metrics', {}).get('followers_count', 0),
                            'query': query
                        })
                    
                    return tweets
        
        except Exception as e:
            logger.error(f"Error searching Twitter: {e}", exc_info=True)
            return self.generate_mock_tweets(query, max_results)
    
    def generate_mock_tweets(self, query: str, count: int = 50) -> List[Dict]:
        """Generate mock tweets for testing without API"""
        import random
        
        templates = [
            f"{query} is looking bullish! 🚀",
            f"Just bought more {query}, this is going to moon",
            f"{query} breaking out, get in now!",
            f"Bearish on {query}, time to sell",
            f"{query} is dead, everyone exiting",
            f"Amazing news for {query} community! 📈",
            f"{query} holding strong despite market conditions",
            f"Massive volume on {query} today",
            f"{query} to the moon! 💎🙌",
            f"Selling all my {query}, rug pull incoming"
        ]
        
        tweets = []
        base_time = datetime.now()
        
        for i in range(count):
            text = random.choice(templates)
            
            tweets.append({
                'id': f'mock-{query}-{i}',
                'text': text,
                'created_at': (base_time - timedelta(minutes=random.randint(1, 1440))).isoformat(),
                'likes': random.randint(0, 1000),
                'retweets': random.randint(0, 500),
                'replies': random.randint(0, 100),
                'author_verified': random.random() > 0.8,
                'author_followers': random.randint(100, 100000),
                'query': query
            })
        
        return tweets
    
    def deduplicate_tweets(self, tweets: List[Dict]) -> List[Dict]:
        """Remove duplicate tweets"""
        seen = set()
        unique = []
        
        for tweet in tweets:
            if tweet['id'] not in seen:
                seen.add(tweet['id'])
                unique.append(tweet)
        
        return unique
    
    def analyze_sentiment(self, tweet: Dict) -> Dict:
        """
        Analyze sentiment of a tweet
        Returns tweet with sentiment score added
        """
        text = tweet['text'].lower()
        
        # Count positive and negative words
        positive_count = sum(1 for word in self.positive_words if word in text)
        negative_count = sum(1 for word in self.negative_words if word in text)
        
        # Calculate sentiment score (-1 to +1)
        total = positive_count + negative_count
        if total == 0:
            sentiment_score = 0
        else:
            sentiment_score = (positive_count - negative_count) / total
        
        # Weight by engagement (likes + retweets)
        engagement = tweet['likes'] + tweet['retweets'] * 2
        
        # Weight by author influence
        influence_weight = 1.0
        if tweet['author_verified']:
            influence_weight = 2.0
        if tweet['author_followers'] > 10000:
            influence_weight *= 1.5
        
        tweet['sentiment_score'] = sentiment_score
        tweet['engagement'] = engagement
        tweet['influence_weight'] = influence_weight
        tweet['weighted_sentiment'] = sentiment_score * influence_weight
        
        return tweet
    
    def calculate_metrics(self, tweets: List[Dict]) -> Dict:
        """
        Calculate aggregate metrics from tweets
        """
        if not tweets:
            return {
                'total_tweets': 0,
                'sentiment_score': 0,
                'positive_pct': 0,
                'negative_pct': 0,
                'neutral_pct': 0,
                'avg_engagement': 0,
                'trending_topics': []
            }
        
        # Sentiment distribution
        positive = sum(1 for t in tweets if t['sentiment_score'] > 0.2)
        negative = sum(1 for t in tweets if t['sentiment_score'] < -0.2)
        neutral = len(tweets) - positive - negative
        
        # Weighted average sentiment
        total_weight = sum(t['influence_weight'] for t in tweets)
        weighted_sentiment = sum(t['weighted_sentiment'] for t in tweets) / total_weight if total_weight > 0 else 0
        
        # Average engagement
        avg_engagement = sum(t['engagement'] for t in tweets) / len(tweets)
        
        # Extract trending topics (hashtags and keywords)
        all_text = ' '.join(t['text'] for t in tweets)
        hashtags = re.findall(r'#(\w+)', all_text)
        hashtag_counts = Counter(hashtags).most_common(10)
        
        return {
            'total_tweets': len(tweets),
            'sentiment_score': weighted_sentiment,
            'positive_pct': positive / len(tweets),
            'negative_pct': negative / len(tweets),
            'neutral_pct': neutral / len(tweets),
            'avg_engagement': avg_engagement,
            'trending_topics': hashtag_counts
        }
    
    def get_sentiment_for_topic(self, tweets: List[Dict], topic: str) -> Dict:
        """
        Get sentiment analysis for a specific topic
        """
        # Filter tweets mentioning topic
        relevant = [
            t for t in tweets
            if topic.lower() in t['text'].lower() or topic.lower() in t['query'].lower()
        ]
        
        if not relevant:
            return {
                'topic': topic,
                'tweet_count': 0,
                'sentiment': 0,
                'confidence': 0
            }
        
        # Calculate metrics
        metrics = self.calculate_metrics(relevant)
        
        # Confidence based on tweet count and recency
        recent_tweets = sum(
            1 for t in relevant
            if datetime.fromisoformat(t['created_at'].replace('Z', '+00:00')) > datetime.now() - timedelta(hours=24)
        )
        
        confidence = min(recent_tweets / 100, 1.0)  # Cap at 100 tweets
        
        return {
            'topic': topic,
            'tweet_count': len(relevant),
            'recent_tweet_count': recent_tweets,
            'sentiment': metrics['sentiment_score'],
            'positive_pct': metrics['positive_pct'],
            'negative_pct': metrics['negative_pct'],
            'confidence': confidence,
            'trending_topics': metrics['trending_topics']
        }


# Standalone testing
async def main():
    """Test the Twitter scraper"""
    config = {
        'api_keys': {
            'twitter': 'YOUR_TWITTER_BEARER_TOKEN'
        },
        'scraping': {
            'twitter_keywords': ['bitcoin', 'crypto', 'ethereum'],
            'twitter_max_tweets': 100
        }
    }
    
    scraper = TwitterScraper(config)
    tweets = await scraper.scrape()
    
    print(f"\n{'='*80}")
    print(f"TWITTER SENTIMENT ANALYSIS")
    print(f"{'='*80}\n")
    
    # Overall metrics
    metrics = scraper.calculate_metrics(tweets)
    print(f"Total Tweets: {metrics['total_tweets']}")
    print(f"Sentiment Score: {metrics['sentiment_score']:.2f}")
    print(f"Positive: {metrics['positive_pct']:.1%}")
    print(f"Negative: {metrics['negative_pct']:.1%}")
    print(f"Neutral: {metrics['neutral_pct']:.1%}")
    print(f"\nTrending Topics:")
    for topic, count in metrics['trending_topics'][:5]:
        print(f"  #{topic}: {count} mentions")
    
    # Per-keyword sentiment
    print(f"\n{'='*80}")
    print(f"PER-KEYWORD SENTIMENT")
    print(f"{'='*80}\n")
    
    for keyword in config['scraping']['twitter_keywords']:
        sentiment = scraper.get_sentiment_for_topic(tweets, keyword)
        print(f"{keyword}:")
        print(f"  Tweets: {sentiment['tweet_count']}")
        print(f"  Sentiment: {sentiment['sentiment']:.2f}")
        print(f"  Confidence: {sentiment['confidence']:.1%}")
        print()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
