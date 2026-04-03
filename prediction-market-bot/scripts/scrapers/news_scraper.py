#!/usr/bin/env python3
"""
News Scraper Agent
Scrapes news articles for market-relevant information
Analyzes sentiment from major news sources
"""

import asyncio
import aiohttp
import logging
from typing import Dict, List
from datetime import datetime, timedelta
import re

logger = logging.getLogger(__name__)


class NewsScraper:
    """
    Scrapes news articles and analyzes sentiment
    Uses NewsAPI and other news aggregators
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.api_key = config['api_keys'].get('newsapi', '')
        self.api_base = 'https://newsapi.org/v2'
        
        self.sources = config['scraping'].get('news_sources', [
            'bloomberg', 'reuters', 'coindesk', 'cnbc', 'financial-times'
        ])
        
        # Sentiment keywords
        self.positive_terms = set([
            'surge', 'gains', 'rally', 'boom', 'growth', 'profit', 'success',
            'breakthrough', 'record', 'high', 'optimistic', 'bullish', 'rise',
            'increase', 'strong', 'positive', 'advance', 'expand', 'recover'
        ])
        
        self.negative_terms = set([
            'crash', 'fall', 'plunge', 'decline', 'drop', 'loss', 'crisis',
            'collapse', 'failure', 'recession', 'bearish', 'pessimistic', 'weak',
            'negative', 'retreat', 'shrink', 'downturn', 'risk', 'concern'
        ])
    
    async def scrape(self) -> List[Dict]:
        """
        Scrape news articles
        Returns list of articles with sentiment analysis
        """
        logger.info("📰 Starting news scrape...")
        
        all_articles = []
        
        # Get articles for each keyword
        keywords = self.config['scraping'].get('twitter_keywords', ['markets', 'economy'])
        
        for keyword in keywords:
            try:
                articles = await self.search_news(keyword)
                all_articles.extend(articles)
            except Exception as e:
                logger.error(f"Error scraping news for '{keyword}': {e}")
        
        # Remove duplicates
        unique_articles = self.deduplicate_articles(all_articles)
        
        # Analyze sentiment
        analyzed = [self.analyze_sentiment(article) for article in unique_articles]
        
        # Calculate metrics
        metrics = self.calculate_metrics(analyzed)
        
        logger.info(f"✅ News scrape complete: {len(analyzed)} articles analyzed")
        logger.info(f"   Sentiment: {metrics['sentiment_score']:.2f} "
                   f"({metrics['positive_pct']:.1%} positive)")
        
        return analyzed
    
    async def search_news(self, query: str, days: int = 1) -> List[Dict]:
        """
        Search for news articles
        """
        if not self.api_key or self.api_key == 'YOUR_NEWS_API_KEY':
            logger.warning("NewsAPI key not configured, using mock data")
            return self.generate_mock_articles(query, 20)
        
        try:
            url = f"{self.api_base}/everything"
            
            params = {
                'q': query,
                'apiKey': self.api_key,
                'language': 'en',
                'sortBy': 'publishedAt',
                'from': (datetime.now() - timedelta(days=days)).isoformat(),
                'pageSize': 100
            }
            
            # Add source filter if configured
            if self.sources:
                params['sources'] = ','.join(self.sources)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.error(f"NewsAPI error: {response.status}")
                        return self.generate_mock_articles(query, 20)
                    
                    data = await response.json()
                    
                    articles = []
                    for article_data in data.get('articles', []):
                        articles.append({
                            'title': article_data['title'],
                            'description': article_data.get('description', ''),
                            'content': article_data.get('content', ''),
                            'source': article_data['source']['name'],
                            'author': article_data.get('author', 'Unknown'),
                            'url': article_data['url'],
                            'published_at': article_data['publishedAt'],
                            'query': query
                        })
                    
                    return articles
        
        except Exception as e:
            logger.error(f"Error searching news: {e}", exc_info=True)
            return self.generate_mock_articles(query, 20)
    
    def generate_mock_articles(self, query: str, count: int = 20) -> List[Dict]:
        """Generate mock news articles for testing"""
        import random
        
        sources = ['Bloomberg', 'Reuters', 'CNBC', 'Financial Times', 'CoinDesk']
        
        positive_headlines = [
            f"{query} Surges as Market Sentiment Improves",
            f"{query} Rally Continues Amid Positive Indicators",
            f"Investors Optimistic About {query} Future",
            f"{query} Breaks Record High on Strong Fundamentals",
            f"Analysts Bullish on {query} Growth Prospects"
        ]
        
        negative_headlines = [
            f"{query} Plunges on Economic Concerns",
            f"{query} Falls as Market Sentiment Deteriorates",
            f"Analysts Warn of {query} Risks Ahead",
            f"{query} Decline Continues Amid Uncertainty",
            f"Investors Bearish on {query} Outlook"
        ]
        
        neutral_headlines = [
            f"{query} Holds Steady Amid Mixed Signals",
            f"Market Update: {query} Trading Range-Bound",
            f"{query} Analysis: What to Expect Next",
            f"Weekly {query} Report: Key Developments",
            f"{query} Market Overview and Forecast"
        ]
        
        all_headlines = positive_headlines + negative_headlines + neutral_headlines
        
        articles = []
        base_time = datetime.now()
        
        for i in range(count):
            title = random.choice(all_headlines)
            
            articles.append({
                'title': title,
                'description': f"Mock article about {query} market conditions...",
                'content': f"Detailed analysis of {query} trends and market implications...",
                'source': random.choice(sources),
                'author': f"Author {random.randint(1, 100)}",
                'url': f"https://example.com/article/{i}",
                'published_at': (base_time - timedelta(hours=random.randint(1, 24))).isoformat(),
                'query': query
            })
        
        return articles
    
    def deduplicate_articles(self, articles: List[Dict]) -> List[Dict]:
        """Remove duplicate articles based on title"""
        seen = set()
        unique = []
        
        for article in articles:
            title = article['title'].lower().strip()
            if title not in seen:
                seen.add(title)
                unique.append(article)
        
        return unique
    
    def analyze_sentiment(self, article: Dict) -> Dict:
        """
        Analyze sentiment of a news article
        """
        # Combine title and description for analysis
        text = (article['title'] + ' ' + article['description']).lower()
        
        # Count positive and negative terms
        positive_count = sum(1 for term in self.positive_terms if term in text)
        negative_count = sum(1 for term in self.negative_terms if term in text)
        
        # Calculate sentiment score
        total = positive_count + negative_count
        if total == 0:
            sentiment_score = 0
        else:
            sentiment_score = (positive_count - negative_count) / total
        
        # Weight by source credibility
        credible_sources = ['bloomberg', 'reuters', 'financial times', 'wall street journal']
        source_weight = 2.0 if any(s in article['source'].lower() for s in credible_sources) else 1.0
        
        # Recency weight (newer articles weighted more)
        try:
            pub_time = datetime.fromisoformat(article['published_at'].replace('Z', '+00:00'))
            hours_old = (datetime.now(pub_time.tzinfo) - pub_time).total_seconds() / 3600
            recency_weight = max(1.0, 2.0 - (hours_old / 24))  # Decay over 24 hours
        except:
            recency_weight = 1.0
        
        article['sentiment_score'] = sentiment_score
        article['source_weight'] = source_weight
        article['recency_weight'] = recency_weight
        article['weighted_sentiment'] = sentiment_score * source_weight * recency_weight
        
        return article
    
    def calculate_metrics(self, articles: List[Dict]) -> Dict:
        """
        Calculate aggregate metrics from articles
        """
        if not articles:
            return {
                'total_articles': 0,
                'sentiment_score': 0,
                'positive_pct': 0,
                'negative_pct': 0,
                'neutral_pct': 0,
                'top_sources': []
            }
        
        # Sentiment distribution
        positive = sum(1 for a in articles if a['sentiment_score'] > 0.2)
        negative = sum(1 for a in articles if a['sentiment_score'] < -0.2)
        neutral = len(articles) - positive - negative
        
        # Weighted sentiment
        total_weight = sum(a['source_weight'] * a['recency_weight'] for a in articles)
        weighted_sentiment = sum(a['weighted_sentiment'] for a in articles) / total_weight if total_weight > 0 else 0
        
        # Top sources
        from collections import Counter
        source_counts = Counter(a['source'] for a in articles).most_common(5)
        
        return {
            'total_articles': len(articles),
            'sentiment_score': weighted_sentiment,
            'positive_pct': positive / len(articles),
            'negative_pct': negative / len(articles),
            'neutral_pct': neutral / len(articles),
            'top_sources': source_counts
        }
    
    def get_sentiment_for_topic(self, articles: List[Dict], topic: str) -> Dict:
        """
        Get sentiment for a specific topic
        """
        relevant = [
            a for a in articles
            if topic.lower() in a['title'].lower() or 
               topic.lower() in a['description'].lower() or
               topic.lower() in a['query'].lower()
        ]
        
        if not relevant:
            return {
                'topic': topic,
                'article_count': 0,
                'sentiment': 0,
                'confidence': 0
            }
        
        metrics = self.calculate_metrics(relevant)
        
        # Confidence based on article count and source diversity
        unique_sources = len(set(a['source'] for a in relevant))
        confidence = min((len(relevant) / 10) * (unique_sources / 3), 1.0)
        
        return {
            'topic': topic,
            'article_count': len(relevant),
            'sentiment': metrics['sentiment_score'],
            'positive_pct': metrics['positive_pct'],
            'negative_pct': metrics['negative_pct'],
            'confidence': confidence,
            'top_sources': metrics['top_sources']
        }


# Standalone testing
async def main():
    """Test the news scraper"""
    config = {
        'api_keys': {
            'newsapi': 'YOUR_NEWS_API_KEY'
        },
        'scraping': {
            'twitter_keywords': ['bitcoin', 'cryptocurrency', 'markets'],
            'news_sources': ['bloomberg', 'reuters', 'cnbc']
        }
    }
    
    scraper = NewsScraper(config)
    articles = await scraper.scrape()
    
    print(f"\n{'='*80}")
    print(f"NEWS SENTIMENT ANALYSIS")
    print(f"{'='*80}\n")
    
    # Overall metrics
    metrics = scraper.calculate_metrics(articles)
    print(f"Total Articles: {metrics['total_articles']}")
    print(f"Sentiment Score: {metrics['sentiment_score']:.2f}")
    print(f"Positive: {metrics['positive_pct']:.1%}")
    print(f"Negative: {metrics['negative_pct']:.1%}")
    print(f"Neutral: {metrics['neutral_pct']:.1%}")
    print(f"\nTop Sources:")
    for source, count in metrics['top_sources']:
        print(f"  {source}: {count} articles")
    
    # Per-topic sentiment
    print(f"\n{'='*80}")
    print(f"PER-TOPIC SENTIMENT")
    print(f"{'='*80}\n")
    
    for topic in config['scraping']['twitter_keywords']:
        sentiment = scraper.get_sentiment_for_topic(articles, topic)
        print(f"{topic}:")
        print(f"  Articles: {sentiment['article_count']}")
        print(f"  Sentiment: {sentiment['sentiment']:.2f}")
        print(f"  Confidence: {sentiment['confidence']:.1%}")
        print()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
