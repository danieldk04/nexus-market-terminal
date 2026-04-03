#!/usr/bin/env python3
"""
Market Monitor Agent
Scans prediction markets for trading opportunities
Filters by liquidity, volume, and activity
"""

import asyncio
import aiohttp
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class MarketMonitor:
    """
    Monitors prediction markets across multiple platforms
    Filters for high-liquidity, active markets
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.markets_cache = {}
        self.last_scan = None
        
        # Platform APIs
        self.platforms = {
            'polymarket': 'https://gamma-api.polymarket.com',
            'manifold': 'https://api.manifold.markets/v0',
            'kalshi': 'https://trading-api.kalshi.com/trade-api/v2'
        }
    
    async def scan_markets(self) -> List[Dict]:
        """
        Scan all platforms for active markets
        Returns list of markets meeting criteria
        """
        logger.info("🔍 Scanning prediction markets...")
        
        all_markets = []
        
        # Scan each platform concurrently
        tasks = [
            self.scan_polymarket(),
            self.scan_manifold(),
            self.scan_kalshi()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for platform_markets in results:
            if isinstance(platform_markets, Exception):
                logger.error(f"Error scanning platform: {platform_markets}")
                continue
            all_markets.extend(platform_markets)
        
        # Filter markets
        filtered = self.filter_markets(all_markets)
        
        self.last_scan = datetime.now()
        self.markets_cache = {m['id']: m for m in filtered}
        
        logger.info(f"✅ Scan complete: {len(all_markets)} total, {len(filtered)} filtered")
        
        return filtered
    
    async def scan_polymarket(self) -> List[Dict]:
        """Scan Polymarket for active markets"""
        try:
            async with aiohttp.ClientSession() as session:
                # Get active markets
                url = f"{self.platforms['polymarket']}/markets"
                params = {
                    'closed': 'false',
                    'archived': 'false',
                    'limit': 100,
                    'order': 'liquidity',
                    'ascending': 'false'
                }
                
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.warning(f"Polymarket API error: {response.status}")
                        return []
                    
                    data = await response.json()
                    
                    markets = []
                    for market_data in data:
                        market = self.parse_polymarket_data(market_data)
                        if market:
                            markets.append(market)
                    
                    logger.info(f"📊 Polymarket: {len(markets)} markets found")
                    return markets
        
        except Exception as e:
            logger.error(f"Error scanning Polymarket: {e}", exc_info=True)
            return []
    
    async def scan_manifold(self) -> List[Dict]:
        """Scan Manifold Markets"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.platforms['manifold']}/markets"
                params = {
                    'limit': 100,
                    'sort': 'liquidity',
                    'filter': 'open'
                }
                
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.warning(f"Manifold API error: {response.status}")
                        return []
                    
                    data = await response.json()
                    
                    markets = []
                    for market_data in data:
                        market = self.parse_manifold_data(market_data)
                        if market:
                            markets.append(market)
                    
                    logger.info(f"📊 Manifold: {len(markets)} markets found")
                    return markets
        
        except Exception as e:
            logger.error(f"Error scanning Manifold: {e}", exc_info=True)
            return []
    
    async def scan_kalshi(self) -> List[Dict]:
        """Scan Kalshi markets"""
        try:
            # Note: Kalshi requires authentication
            # This is a placeholder - implement actual API calls
            
            logger.info("📊 Kalshi: Scanning (placeholder)")
            
            # Mock data for demonstration
            markets = [
                {
                    'id': 'kalshi-example-1',
                    'platform': 'kalshi',
                    'title': 'Fed Rate Cut by June 2026',
                    'description': 'Will the Federal Reserve cut interest rates by June 2026?',
                    'probability': 0.68,
                    'liquidity': 500000,
                    'volume_24h': 125000,
                    'category': 'economics',
                    'close_time': (datetime.now() + timedelta(days=120)).isoformat(),
                    'url': 'https://kalshi.com/events/FED-RATE-CUT'
                }
            ]
            
            return markets
        
        except Exception as e:
            logger.error(f"Error scanning Kalshi: {e}", exc_info=True)
            return []
    
    def parse_polymarket_data(self, data: Dict) -> Optional[Dict]:
        """Parse Polymarket API response into standard format"""
        try:
            return {
                'id': f"polymarket-{data.get('id', '')}",
                'platform': 'polymarket',
                'title': data.get('question', ''),
                'description': data.get('description', ''),
                'probability': float(data.get('outcomePrices', [0.5])[0]),
                'liquidity': float(data.get('liquidity', 0)),
                'volume_24h': float(data.get('volume24hr', 0)),
                'category': data.get('category', 'other'),
                'close_time': data.get('endDate', ''),
                'url': f"https://polymarket.com/market/{data.get('slug', '')}"
            }
        except Exception as e:
            logger.warning(f"Error parsing Polymarket data: {e}")
            return None
    
    def parse_manifold_data(self, data: Dict) -> Optional[Dict]:
        """Parse Manifold API response into standard format"""
        try:
            return {
                'id': f"manifold-{data.get('id', '')}",
                'platform': 'manifold',
                'title': data.get('question', ''),
                'description': data.get('description', ''),
                'probability': float(data.get('probability', 0.5)),
                'liquidity': float(data.get('totalLiquidity', 0)),
                'volume_24h': float(data.get('volume24Hours', 0)),
                'category': data.get('category', 'other'),
                'close_time': data.get('closeTime', ''),
                'url': data.get('url', '')
            }
        except Exception as e:
            logger.warning(f"Error parsing Manifold data: {e}")
            return None
    
    def filter_markets(self, markets: List[Dict]) -> List[Dict]:
        """
        Filter markets by criteria:
        - Minimum liquidity
        - Not closing soon
        - Active volume
        """
        min_liquidity = self.config['trading']['min_liquidity']
        
        filtered = []
        
        for market in markets:
            # Check liquidity
            if market['liquidity'] < min_liquidity:
                continue
            
            # Check close time (must be at least 7 days away)
            try:
                close_time = datetime.fromisoformat(market['close_time'].replace('Z', '+00:00'))
                if close_time < datetime.now() + timedelta(days=7):
                    continue
            except:
                pass  # Skip close time check if parsing fails
            
            # Check for minimum volume (indicates active market)
            if market['volume_24h'] < 1000:
                continue
            
            # Check probability isn't too extreme (90%+ or 10%-)
            if market['probability'] > 0.90 or market['probability'] < 0.10:
                continue
            
            filtered.append(market)
        
        # Sort by liquidity (highest first)
        filtered.sort(key=lambda x: x['liquidity'], reverse=True)
        
        return filtered
    
    async def get_market_details(self, market_id: str) -> Optional[Dict]:
        """Get detailed information for a specific market"""
        # Check cache first
        if market_id in self.markets_cache:
            return self.markets_cache[market_id]
        
        # Determine platform from market_id
        if market_id.startswith('polymarket-'):
            return await self.get_polymarket_details(market_id)
        elif market_id.startswith('manifold-'):
            return await self.get_manifold_details(market_id)
        elif market_id.startswith('kalshi-'):
            return await self.get_kalshi_details(market_id)
        
        return None
    
    async def get_polymarket_details(self, market_id: str) -> Optional[Dict]:
        """Get Polymarket market details"""
        try:
            # Extract actual ID
            poly_id = market_id.replace('polymarket-', '')
            
            async with aiohttp.ClientSession() as session:
                url = f"{self.platforms['polymarket']}/markets/{poly_id}"
                
                async with session.get(url) as response:
                    if response.status != 200:
                        return None
                    
                    data = await response.json()
                    return self.parse_polymarket_data(data)
        
        except Exception as e:
            logger.error(f"Error getting Polymarket details: {e}")
            return None
    
    async def get_manifold_details(self, market_id: str) -> Optional[Dict]:
        """Get Manifold market details"""
        try:
            manifold_id = market_id.replace('manifold-', '')
            
            async with aiohttp.ClientSession() as session:
                url = f"{self.platforms['manifold']}/market/{manifold_id}"
                
                async with session.get(url) as response:
                    if response.status != 200:
                        return None
                    
                    data = await response.json()
                    return self.parse_manifold_data(data)
        
        except Exception as e:
            logger.error(f"Error getting Manifold details: {e}")
            return None
    
    async def get_kalshi_details(self, market_id: str) -> Optional[Dict]:
        """Get Kalshi market details"""
        # Placeholder - implement actual Kalshi API
        logger.warning("Kalshi details not implemented")
        return None
    
    def get_trending_markets(self, limit: int = 10) -> List[Dict]:
        """Get markets with highest volume spikes"""
        markets = list(self.markets_cache.values())
        
        # Sort by 24h volume
        markets.sort(key=lambda x: x['volume_24h'], reverse=True)
        
        return markets[:limit]
    
    def get_markets_by_category(self, category: str) -> List[Dict]:
        """Get markets filtered by category"""
        return [
            m for m in self.markets_cache.values()
            if m['category'].lower() == category.lower()
        ]


# Standalone testing
async def main():
    """Test the market monitor"""
    config = {
        'trading': {
            'min_liquidity': 100000
        }
    }
    
    monitor = MarketMonitor(config)
    markets = await monitor.scan_markets()
    
    print(f"\n{'='*80}")
    print(f"MARKET SCAN RESULTS")
    print(f"{'='*80}\n")
    
    for i, market in enumerate(markets[:10], 1):
        print(f"{i}. {market['title']}")
        print(f"   Platform: {market['platform']}")
        print(f"   Probability: {market['probability']:.1%}")
        print(f"   Liquidity: ${market['liquidity']:,.0f}")
        print(f"   24h Volume: ${market['volume_24h']:,.0f}")
        print(f"   Category: {market['category']}")
        print(f"   URL: {market['url']}")
        print()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
