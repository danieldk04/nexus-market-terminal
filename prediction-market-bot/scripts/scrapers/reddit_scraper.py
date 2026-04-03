#!/usr/bin/env python3
"""
Reddit Scraper Agent
Monitors Reddit for market sentiment and discussion trends
Analyzes subreddits like r/wallstreetbets, r/cryptocurrency, etc.
"""

import asyncio
import aiohttp
import logging
from typing import Dict, List
from datetime import datetime, timedelta
from collections import Counter
import re

logger = logging.getLogger(__name__)


class RedditScraper:
    """
    Scrapes and analyzes Reddit for market sentiment
    Uses Reddit API (PRAW wrapper or direct REST)
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.api_base = 'https://oauth.reddit.com'
        self.auth_url = 'https://www.reddit.com/api/v1/access_token'
        
        self.client_id = config['api_keys'].get('reddit_client_id', '')
        self.client_secret = config['api_keys'].get('reddit_client_secret', '')
        
        self.subreddits = config['scraping']['reddit_subreddits']
        self.max_posts = config['scraping'].get('reddit_max_posts', 100)
        
        self.access_token = None
        
        # Sentiment keywords
        self.bullish_terms = set([
            'bullish', 'buy', 'moon', 'rocket', 'calls', 'long', 'pump',
            'gains', 'yolo', 'hold', 'hodl', 'diamond hands', 'to the moon',
            '🚀', '📈', '💎', '🙌', 'ath', 'breakout', 'surge'
        ])
        
        self.bearish_terms = set([
            'bearish', 'sell', 'puts', 'short', 'dump', 'crash', 'rug',
            'scam', 'bubble', 'overvalued', 'dead', 'paper hands',
            '📉', '💩', 'rip', 'bag holder'
        ])
    
    async def scrape(self) -> List[Dict]:
        """
        Scrape Reddit for relevant posts and comments
        Returns list of post data with sentiment analysis
        """
        logger.info("📡 Starting Reddit scrape...")
        
        # Authenticate
        await self.authenticate()
        
        all_posts = []
        
        # Scrape each subreddit
        for subreddit in self.subreddits:
            try:
                posts = await self.get_subreddit_posts(subreddit)
                all_posts.extend(posts)
            except Exception as e:
                logger.error(f"Error scraping r/{subreddit}: {e}")
        
        # Analyze sentiment
        analyzed = [self.analyze_sentiment(post) for post in all_posts]
        
        # Calculate metrics
        metrics = self.calculate_metrics(analyzed)
        
        logger.info(f"✅ Reddit scrape complete: {len(analyzed)} posts analyzed")
        logger.info(f"   Sentiment: {metrics['sentiment_score']:.2f} "
                   f"({metrics['bullish_pct']:.1%} bullish)")
        
        return analyzed
    
    async def authenticate(self):
        """Authenticate with Reddit API"""
        if not self.client_id or self.client_id == 'YOUR_REDDIT_CLIENT_ID':
            logger.warning("Reddit API credentials not configured, using mock data")
            return
        
        try:
            auth = aiohttp.BasicAuth(self.client_id, self.client_secret)
            data = {
                'grant_type': 'client_credentials'
            }
            headers = {
                'User-Agent': 'PredictionMarketBot/1.0'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(self.auth_url, auth=auth, data=data, headers=headers) as response:
                    if response.status == 200:
                        token_data = await response.json()
                        self.access_token = token_data['access_token']
                        logger.info("✅ Reddit authentication successful")
                    else:
                        logger.error(f"Reddit auth failed: {response.status}")
        
        except Exception as e:
            logger.error(f"Error authenticating with Reddit: {e}")
    
    async def get_subreddit_posts(self, subreddit: str, limit: int = 100) -> List[Dict]:
        """
        Get posts from a subreddit
        """
        if not self.access_token:
            logger.warning(f"No Reddit auth, using mock data for r/{subreddit}")
            return self.generate_mock_posts(subreddit, limit)
        
        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'User-Agent': 'PredictionMarketBot/1.0'
            }
            
            # Get hot posts
            url = f"{self.api_base}/r/{subreddit}/hot"
            params = {
                'limit': min(limit, 100)
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status != 200:
                        logger.error(f"Reddit API error for r/{subreddit}: {response.status}")
                        return self.generate_mock_posts(subreddit, limit)
                    
                    data = await response.json()
                    
                    posts = []
                    for post_data in data['data']['children']:
                        post = post_data['data']
                        
                        posts.append({
                            'id': post['id'],
                            'subreddit': subreddit,
                            'title': post['title'],
                            'text': post.get('selftext', ''),
                            'author': post['author'],
                            'created_utc': post['created_utc'],
                            'score': post['score'],
                            'upvote_ratio': post['upvote_ratio'],
                            'num_comments': post['num_comments'],
                            'url': f"https://reddit.com{post['permalink']}"
                        })
                    
                    return posts
        
        except Exception as e:
            logger.error(f"Error getting posts from r/{subreddit}: {e}")
            return self.generate_mock_posts(subreddit, limit)
    
    def generate_mock_posts(self, subreddit: str, count: int = 50) -> List[Dict]:
        """Generate mock Reddit posts for testing"""
        import random
        
        if subreddit == 'wallstreetbets':
            templates = [
                "YOLO $100k on {keyword} calls, to the moon! 🚀🚀🚀",
                "{keyword} is undervalued, time to load up",
                "Just sold all my {keyword}, this is going to crash",
                "{keyword} DD: Why this is the next 10x",
                "Paper handed my {keyword} positions, AMA",
                "{keyword} megathread - discuss here",
                "Loss porn: Down 50% on {keyword} puts",
                "{keyword} gained 200% today, who's holding?",
                "Hedge funds are manipulating {keyword} again",
                "{keyword} short squeeze incoming!"
            ]
        elif subreddit == 'cryptocurrency':
            templates = [
                "{keyword} just broke ATH, bullish!",
                "Why {keyword} is the future of finance",
                "{keyword} bearish divergence on daily chart",
                "Just accumulated more {keyword} at this dip",
                "{keyword} fundamentals are stronger than ever",
                "Sold my {keyword}, market looking weak",
                "{keyword} whale movement detected",
                "Technical analysis: {keyword} forming ascending triangle",
                "{keyword} news: Major partnership announced",
                "Be careful with {keyword}, possible rug pull"
            ]
        else:
            templates = [
                "{keyword} looking strong today",
                "Thoughts on {keyword}?",
                "{keyword} price prediction thread",
                "Should I buy {keyword} now?",
                "{keyword} discussion megathread"
            ]
        
        keywords = ['Bitcoin', 'Ethereum', 'Stock market', 'Elections', 'AI']
        
        posts = []
        base_time = datetime.now().timestamp()
        
        for i in range(count):
            keyword = random.choice(keywords)
            title = random.choice(templates).format(keyword=keyword)
            
            posts.append({
                'id': f'mock-{subreddit}-{i}',
                'subreddit': subreddit,
                'title': title,
                'text': f"Mock post content about {keyword}...",
                'author': f'user{random.randint(1, 1000)}',
                'created_utc': base_time - random.randint(60, 86400),
                'score': random.randint(-10, 5000),
                'upvote_ratio': random.uniform(0.5, 0.99),
                'num_comments': random.randint(0, 500),
                'url': f'https://reddit.com/r/{subreddit}/comments/mock{i}'
            })
        
        return posts
    
    def analyze_sentiment(self, post: Dict) -> Dict:
        """
        Analyze sentiment of a Reddit post
        """
        text = (post['title'] + ' ' + post['text']).lower()
        
        # Count bullish and bearish terms
        bullish_count = sum(1 for term in self.bullish_terms if term in text)
        bearish_count = sum(1 for term in self.bearish_terms if term in text)
        
        # Calculate sentiment score
        total = bullish_count + bearish_count
        if total == 0:
            sentiment_score = 0
        else:
            sentiment_score = (bullish_count - bearish_count) / total
        
        # Adjust by upvote ratio and score
        engagement_weight = 1 + (post['score'] / 1000)  # Cap influence of high scores
        sentiment_weight = post['upvote_ratio']  # Higher ratio = more agreement
        
        post['sentiment_score'] = sentiment_score
        post['engagement'] = post['score'] + post['num_comments']
        post['engagement_weight'] = engagement_weight
        post['weighted_sentiment'] = sentiment_score * engagement_weight * sentiment_weight
        
        return post
    
    def calculate_metrics(self, posts: List[Dict]) -> Dict:
        """
        Calculate aggregate metrics from posts
        """
        if not posts:
            return {
                'total_posts': 0,
                'sentiment_score': 0,
                'bullish_pct': 0,
                'bearish_pct': 0,
                'neutral_pct': 0,
                'avg_engagement': 0,
                'top_tickers': []
            }
        
        # Sentiment distribution
        bullish = sum(1 for p in posts if p['sentiment_score'] > 0.2)
        bearish = sum(1 for p in posts if p['sentiment_score'] < -0.2)
        neutral = len(posts) - bullish - bearish
        
        # Weighted sentiment
        total_weight = sum(p['engagement_weight'] for p in posts)
        weighted_sentiment = sum(p['weighted_sentiment'] for p in posts) / total_weight if total_weight > 0 else 0
        
        # Average engagement
        avg_engagement = sum(p['engagement'] for p in posts) / len(posts)
        
        # Extract mentioned tickers/coins
        all_text = ' '.join(p['title'] + ' ' + p['text'] for p in posts)
        
        # Common crypto/stock tickers
        tickers = re.findall(r'\b([A-Z]{2,5})\b', all_text)
        ticker_counts = Counter(tickers).most_common(10)
        
        return {
            'total_posts': len(posts),
            'sentiment_score': weighted_sentiment,
            'bullish_pct': bullish / len(posts),
            'bearish_pct': bearish / len(posts),
            'neutral_pct': neutral / len(posts),
            'avg_engagement': avg_engagement,
            'top_tickers': ticker_counts
        }
    
    def get_sentiment_for_topic(self, posts: List[Dict], topic: str) -> Dict:
        """
        Get sentiment for a specific topic
        """
        # Filter relevant posts
        relevant = [
            p for p in posts
            if topic.lower() in p['title'].lower() or topic.lower() in p['text'].lower()
        ]
        
        if not relevant:
            return {
                'topic': topic,
                'post_count': 0,
                'sentiment': 0,
                'confidence': 0
            }
        
        metrics = self.calculate_metrics(relevant)
        
        # Confidence based on post count and recency
        now = datetime.now().timestamp()
        recent_posts = sum(
            1 for p in relevant
            if now - p['created_utc'] < 86400  # Last 24 hours
        )
        
        confidence = min(recent_posts / 50, 1.0)
        
        return {
            'topic': topic,
            'post_count': len(relevant),
            'recent_post_count': recent_posts,
            'sentiment': metrics['sentiment_score'],
            'bullish_pct': metrics['bullish_pct'],
            'bearish_pct': metrics['bearish_pct'],
            'confidence': confidence,
            'top_tickers': metrics['top_tickers']
        }
    
    def get_subreddit_analysis(self, posts: List[Dict]) -> Dict:
        """
        Get analysis broken down by subreddit
        """
        by_subreddit = {}
        
        for post in posts:
            sub = post['subreddit']
            if sub not in by_subreddit:
                by_subreddit[sub] = []
            by_subreddit[sub].append(post)
        
        analysis = {}
        for sub, sub_posts in by_subreddit.items():
            analysis[sub] = self.calculate_metrics(sub_posts)
        
        return analysis


# Standalone testing
async def main():
    """Test the Reddit scraper"""
    config = {
        'api_keys': {
            'reddit_client_id': 'YOUR_CLIENT_ID',
            'reddit_client_secret': 'YOUR_SECRET'
        },
        'scraping': {
            'reddit_subreddits': ['wallstreetbets', 'cryptocurrency', 'stocks'],
            'reddit_max_posts': 100
        }
    }
    
    scraper = RedditScraper(config)
    posts = await scraper.scrape()
    
    print(f"\n{'='*80}")
    print(f"REDDIT SENTIMENT ANALYSIS")
    print(f"{'='*80}\n")
    
    # Overall metrics
    metrics = scraper.calculate_metrics(posts)
    print(f"Total Posts: {metrics['total_posts']}")
    print(f"Sentiment Score: {metrics['sentiment_score']:.2f}")
    print(f"Bullish: {metrics['bullish_pct']:.1%}")
    print(f"Bearish: {metrics['bearish_pct']:.1%}")
    print(f"Neutral: {metrics['neutral_pct']:.1%}")
    print(f"\nTop Tickers:")
    for ticker, count in metrics['top_tickers'][:5]:
        print(f"  ${ticker}: {count} mentions")
    
    # Per-subreddit analysis
    print(f"\n{'='*80}")
    print(f"PER-SUBREDDIT ANALYSIS")
    print(f"{'='*80}\n")
    
    subreddit_analysis = scraper.get_subreddit_analysis(posts)
    for sub, metrics in subreddit_analysis.items():
        print(f"r/{sub}:")
        print(f"  Posts: {metrics['total_posts']}")
        print(f"  Sentiment: {metrics['sentiment_score']:.2f}")
        print(f"  Bullish: {metrics['bullish_pct']:.1%}")
        print()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
