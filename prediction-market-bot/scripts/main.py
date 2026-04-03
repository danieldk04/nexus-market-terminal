#!/usr/bin/env python3
"""
Prediction Market Trading Bot - Main Orchestrator
Coordinates all agents for autonomous trading with >60% win rate
"""

import asyncio
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import signal
import sys

# Import all agents
from market_monitor import MarketMonitor
from signal_analyzer import SignalAnalyzer
from trade_executor import TradeExecutor
from risk_manager import RiskManager
from scrapers.twitter_scraper import TwitterScraper
from scrapers.reddit_scraper import RedditScraper
from scrapers.news_scraper import NewsScraper

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../logs/bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PredictionMarketBot:
    """
    Main bot orchestrator that coordinates all agents
    """
    
    def __init__(self, config_path: str = '../config/config.json', monitor_only: bool = False):
        """Initialize the bot with configuration"""
        self.config = self.load_config(config_path)
        self.monitor_only = monitor_only
        self.running = False
        
        # Initialize all agents
        logger.info("🚀 Initializing Prediction Market Bot...")
        
        self.market_monitor = MarketMonitor(self.config)
        self.twitter_scraper = TwitterScraper(self.config)
        self.reddit_scraper = RedditScraper(self.config)
        self.news_scraper = NewsScraper(self.config)
        self.signal_analyzer = SignalAnalyzer(self.config)
        self.trade_executor = TradeExecutor(self.config) if not monitor_only else None
        self.risk_manager = RiskManager(self.config)
        
        # State tracking
        self.active_markets: List[Dict] = []
        self.active_signals: List[Dict] = []
        self.active_positions: List[Dict] = []
        
        # Performance metrics
        self.metrics = {
            'total_trades': 0,
            'winning_trades': 0,
            'total_pnl': 0,
            'win_rate': 0,
            'start_time': datetime.now()
        }
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        
        logger.info("✅ Bot initialized successfully")
    
    def load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"📋 Configuration loaded from {config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"❌ Config file not found: {config_path}")
            logger.info("💡 Creating default config from template...")
            return self.create_default_config(config_path)
    
    def create_default_config(self, config_path: str) -> Dict:
        """Create default configuration"""
        default_config = {
            "api_keys": {
                "polymarket": "YOUR_POLYMARKET_KEY",
                "manifold": "YOUR_MANIFOLD_KEY",
                "kalshi": "YOUR_KALSHI_KEY",
                "twitter": "YOUR_TWITTER_BEARER_TOKEN",
                "reddit_client_id": "YOUR_REDDIT_CLIENT_ID",
                "reddit_client_secret": "YOUR_REDDIT_SECRET",
                "newsapi": "YOUR_NEWS_API_KEY"
            },
            "trading": {
                "min_liquidity": 100000,
                "max_position_size": 5000,
                "min_confidence": 0.70,
                "stop_loss": -0.15,
                "max_exposure": 0.50,
                "max_positions": 10
            },
            "scraping": {
                "twitter_keywords": ["bitcoin", "crypto", "elections", "AI", "markets"],
                "twitter_max_tweets": 500,
                "reddit_subreddits": ["wallstreetbets", "cryptocurrency", "politicalbetting", "stocks"],
                "reddit_max_posts": 100,
                "news_sources": ["bloomberg", "reuters", "coindesk", "cnbc"],
                "scrape_interval": 300
            },
            "risk": {
                "max_daily_loss": 0.20,
                "max_correlated_positions": 3,
                "position_sizing": "kelly_criterion",
                "rebalance_interval": 3600
            },
            "intervals": {
                "market_scan": 60,
                "signal_check": 30,
                "risk_check": 10,
                "scrape_cycle": 300
            }
        }
        
        # Save default config
        Path(config_path).parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        logger.info(f"✅ Default config created at {config_path}")
        logger.warning("⚠️  Please edit config.json with your API keys before running!")
        
        return default_config
    
    async def run(self):
        """Main bot loop"""
        self.running = True
        logger.info("🎯 Starting bot main loop...")
        
        if self.monitor_only:
            logger.info("👁️  Running in MONITOR ONLY mode (no trades will be executed)")
        
        try:
            # Start all async tasks
            await asyncio.gather(
                self.market_monitoring_loop(),
                self.scraping_loop(),
                self.signal_generation_loop(),
                self.trading_loop(),
                self.risk_monitoring_loop(),
                self.metrics_loop()
            )
        except Exception as e:
            logger.error(f"❌ Critical error in main loop: {e}", exc_info=True)
            self.running = False
    
    async def market_monitoring_loop(self):
        """Continuously monitor markets for opportunities"""
        logger.info("👁️  Market monitoring agent started")
        
        while self.running:
            try:
                # Scan for active markets
                markets = await self.market_monitor.scan_markets()
                
                # Filter by liquidity
                filtered_markets = [
                    m for m in markets 
                    if m['liquidity'] >= self.config['trading']['min_liquidity']
                ]
                
                self.active_markets = filtered_markets
                
                logger.info(f"📊 Found {len(filtered_markets)} markets above liquidity threshold")
                
                # Wait before next scan
                await asyncio.sleep(self.config['intervals']['market_scan'])
                
            except Exception as e:
                logger.error(f"❌ Error in market monitoring: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def scraping_loop(self):
        """Continuously scrape social media and news"""
        logger.info("🔍 Scraping agents started")
        
        while self.running:
            try:
                # Scrape Twitter
                logger.info("🐦 Scraping Twitter...")
                twitter_data = await self.twitter_scraper.scrape()
                
                # Scrape Reddit
                logger.info("📡 Scraping Reddit...")
                reddit_data = await self.reddit_scraper.scrape()
                
                # Scrape News
                logger.info("📰 Scraping News...")
                news_data = await self.news_scraper.scrape()
                
                # Store aggregated sentiment data
                self.sentiment_data = {
                    'twitter': twitter_data,
                    'reddit': reddit_data,
                    'news': news_data,
                    'timestamp': datetime.now()
                }
                
                logger.info(f"✅ Scraping complete - Twitter: {len(twitter_data)} tweets, "
                          f"Reddit: {len(reddit_data)} posts, News: {len(news_data)} articles")
                
                # Wait before next scrape cycle
                await asyncio.sleep(self.config['intervals']['scrape_cycle'])
                
            except Exception as e:
                logger.error(f"❌ Error in scraping loop: {e}", exc_info=True)
                await asyncio.sleep(300)
    
    async def signal_generation_loop(self):
        """Generate trading signals from all data sources"""
        logger.info("📈 Signal analyzer agent started")
        
        while self.running:
            try:
                signals = []
                
                # Generate signals for each active market
                for market in self.active_markets:
                    # Analyze market with sentiment data
                    signal = await self.signal_analyzer.analyze(
                        market=market,
                        sentiment_data=getattr(self, 'sentiment_data', None)
                    )
                    
                    if signal and signal['confidence'] >= self.config['trading']['min_confidence']:
                        signals.append(signal)
                        logger.info(f"⚡ Signal generated: {signal['type']} {market['title']} "
                                  f"(Confidence: {signal['confidence']:.1%})")
                
                self.active_signals = signals
                
                if signals:
                    logger.info(f"🎯 {len(signals)} high-confidence signals active")
                
                # Wait before next signal check
                await asyncio.sleep(self.config['intervals']['signal_check'])
                
            except Exception as e:
                logger.error(f"❌ Error in signal generation: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def trading_loop(self):
        """Execute trades based on signals"""
        if self.monitor_only or not self.trade_executor:
            logger.info("⏸️  Trading loop disabled (monitor-only mode)")
            return
        
        logger.info("⚡ Trade executor agent started")
        
        while self.running:
            try:
                # Check risk limits before trading
                can_trade = await self.risk_manager.check_can_trade(self.active_positions)
                
                if not can_trade:
                    logger.warning("🛡️ Risk limits reached - skipping trades")
                    await asyncio.sleep(60)
                    continue
                
                # Execute high-confidence signals
                for signal in self.active_signals:
                    try:
                        # Calculate position size
                        position_size = await self.risk_manager.calculate_position_size(
                            signal=signal,
                            portfolio_value=self.get_portfolio_value()
                        )
                        
                        # Execute trade
                        result = await self.trade_executor.execute(
                            signal=signal,
                            position_size=position_size
                        )
                        
                        if result['success']:
                            self.active_positions.append(result['position'])
                            logger.info(f"✅ Trade executed: {signal['type']} {signal['market']['title']} "
                                      f"Size: ${position_size:,.0f}")
                            
                            # Update metrics
                            self.metrics['total_trades'] += 1
                        else:
                            logger.error(f"❌ Trade failed: {result['error']}")
                    
                    except Exception as e:
                        logger.error(f"❌ Error executing trade: {e}", exc_info=True)
                
                # Wait before next trading cycle
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"❌ Error in trading loop: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def risk_monitoring_loop(self):
        """Monitor and manage risk"""
        logger.info("🛡️ Risk manager agent started")
        
        while self.running:
            try:
                # Check all active positions
                for position in self.active_positions[:]:  # Copy list
                    # Check stop loss
                    if await self.risk_manager.check_stop_loss(position):
                        logger.warning(f"🛑 Stop loss triggered for {position['market']}")
                        
                        if not self.monitor_only:
                            await self.trade_executor.close_position(position)
                            self.active_positions.remove(position)
                            
                            # Update metrics
                            if position['pnl'] > 0:
                                self.metrics['winning_trades'] += 1
                            self.metrics['total_pnl'] += position['pnl']
                
                # Check portfolio exposure
                exposure = await self.risk_manager.calculate_exposure(self.active_positions)
                if exposure > self.config['trading']['max_exposure']:
                    logger.warning(f"⚠️ Portfolio exposure high: {exposure:.1%}")
                
                # Update win rate
                if self.metrics['total_trades'] > 0:
                    self.metrics['win_rate'] = self.metrics['winning_trades'] / self.metrics['total_trades']
                
                # Wait before next risk check
                await asyncio.sleep(self.config['intervals']['risk_check'])
                
            except Exception as e:
                logger.error(f"❌ Error in risk monitoring: {e}", exc_info=True)
                await asyncio.sleep(30)
    
    async def metrics_loop(self):
        """Log performance metrics periodically"""
        logger.info("📊 Metrics logger started")
        
        while self.running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                logger.info("=" * 60)
                logger.info("📊 PERFORMANCE METRICS")
                logger.info(f"Win Rate: {self.metrics['win_rate']:.1%}")
                logger.info(f"Total Trades: {self.metrics['total_trades']}")
                logger.info(f"Winning Trades: {self.metrics['winning_trades']}")
                logger.info(f"Total P&L: ${self.metrics['total_pnl']:,.2f}")
                logger.info(f"Active Positions: {len(self.active_positions)}")
                logger.info(f"Active Signals: {len(self.active_signals)}")
                logger.info(f"Markets Monitored: {len(self.active_markets)}")
                logger.info("=" * 60)
                
            except Exception as e:
                logger.error(f"❌ Error in metrics loop: {e}", exc_info=True)
    
    def get_portfolio_value(self) -> float:
        """Calculate current portfolio value"""
        # In production, this would query actual account balance
        # For now, return a placeholder
        return 50000.0
    
    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown"""
        logger.info("🛑 Shutdown signal received, stopping bot...")
        self.running = False
        
        # Close all positions if in live mode
        if not self.monitor_only and self.trade_executor:
            logger.info("📤 Closing all active positions...")
            # In production, implement actual position closing
        
        # Save final metrics
        self.save_metrics()
        
        logger.info("👋 Bot stopped successfully")
        sys.exit(0)
    
    def save_metrics(self):
        """Save performance metrics to file"""
        metrics_file = Path('../logs/metrics.json')
        
        try:
            # Load existing metrics if available
            if metrics_file.exists():
                with open(metrics_file, 'r') as f:
                    all_metrics = json.load(f)
            else:
                all_metrics = []
            
            # Add current session metrics
            session_metrics = {
                **self.metrics,
                'start_time': self.metrics['start_time'].isoformat(),
                'end_time': datetime.now().isoformat()
            }
            all_metrics.append(session_metrics)
            
            # Save updated metrics
            with open(metrics_file, 'w') as f:
                json.dump(all_metrics, f, indent=2)
            
            logger.info(f"💾 Metrics saved to {metrics_file}")
            
        except Exception as e:
            logger.error(f"❌ Error saving metrics: {e}", exc_info=True)


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Prediction Market Trading Bot')
    parser.add_argument('--monitor-only', action='store_true', 
                       help='Run in monitoring mode without executing trades')
    parser.add_argument('--config', default='../config/config.json',
                       help='Path to configuration file')
    
    args = parser.parse_args()
    
    # Create and run bot
    bot = PredictionMarketBot(
        config_path=args.config,
        monitor_only=args.monitor_only
    )
    
    await bot.run()


if __name__ == '__main__':
    asyncio.run(main())
