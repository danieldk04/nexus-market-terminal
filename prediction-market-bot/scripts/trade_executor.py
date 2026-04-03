#!/usr/bin/env python3
"""
Trade Executor Agent
Executes trades on prediction markets based on signals
Handles order placement, position tracking, and exit management
"""

import asyncio
import aiohttp
import logging
from typing import Dict, List,  Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class TradeExecutor:
    """
    Executes trades on prediction market platforms
    Supports Polymarket, Manifold, Kalshi
    """
    
    def __init__(self, config: Dict):
        self.config = config
        
        # API credentials
        self.api_keys = config['api_keys']
        
        # Trading parameters
        self.max_position_size = config['trading']['max_position_size']
        self.stop_loss_pct = abs(config['trading']['stop_loss'])
        
        # Position tracking
        self.open_positions = {}
        
        # Platform APIs
        self.platforms = {
            'polymarket': 'https://gamma-api.polymarket.com',
            'manifold': 'https://api.manifold.markets/v0',
            'kalshi': 'https://trading-api.kalshi.com/trade-api/v2'
        }
    
    async def execute(self, signal: Dict, position_size: float) -> Dict:
        """
        Execute a trade based on signal
        
        Args:
            signal: Trading signal from SignalAnalyzer
            position_size: Dollar amount to trade
        
        Returns:
            Result dict with success status and position details
        """
        market = signal['market']
        platform = market['platform']
        
        logger.info(f"⚡ Executing {signal['type']} trade for {market['title']}")
        logger.info(f"   Platform: {platform}")
        logger.info(f"   Size: ${position_size:,.0f}")
        logger.info(f"   Entry: {signal['entry']:.1%}")
        
        try:
            # Route to appropriate platform
            if platform == 'polymarket':
                result = await self.execute_polymarket(signal, position_size)
            elif platform == 'manifold':
                result = await self.execute_manifold(signal, position_size)
            elif platform == 'kalshi':
                result = await self.execute_kalshi(signal, position_size)
            else:
                raise ValueError(f"Unsupported platform: {platform}")
            
            if result['success']:
                # Track position
                position_id = result['position']['id']
                self.open_positions[position_id] = result['position']
                
                logger.info(f"✅ Trade executed successfully: Position {position_id}")
                
                # Save to trade log
                self.log_trade(signal, result)
            else:
                logger.error(f"❌ Trade execution failed: {result['error']}")
            
            return result
        
        except Exception as e:
            logger.error(f"❌ Error executing trade: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    async def execute_polymarket(self, signal: Dict, position_size: float) -> Dict:
        """
        Execute trade on Polymarket
        """
        # In production, implement actual Polymarket API calls
        # For now, simulate execution
        
        logger.info("📊 Simulating Polymarket trade (API not implemented)")
        
        market = signal['market']
        
        # Simulate successful execution
        await asyncio.sleep(0.5)  # Simulate network delay
        
        position = {
            'id': f"pos-{market['id']}-{datetime.now().timestamp()}",
            'market_id': market['id'],
            'market_title': market['title'],
            'platform': 'polymarket',
            'type': signal['type'],
            'entry_price': signal['entry'],
            'target_price': signal['target'],
            'stop_price': signal['stop'],
            'position_size': position_size,
            'shares': position_size / signal['entry'],  # Simplified
            'opened_at': datetime.now().isoformat(),
            'status': 'open',
            'pnl': 0,
            'signal': signal
        }
        
        return {
            'success': True,
            'position': position,
            'order_id': f"order-{datetime.now().timestamp()}"
        }
    
    async def execute_manifold(self, signal: Dict, position_size: float) -> Dict:
        """
        Execute trade on Manifold Markets
        """
        logger.info("📊 Simulating Manifold trade (API not implemented)")
        
        market = signal['market']
        
        await asyncio.sleep(0.5)
        
        position = {
            'id': f"pos-{market['id']}-{datetime.now().timestamp()}",
            'market_id': market['id'],
            'market_title': market['title'],
            'platform': 'manifold',
            'type': signal['type'],
            'entry_price': signal['entry'],
            'target_price': signal['target'],
            'stop_price': signal['stop'],
            'position_size': position_size,
            'shares': position_size / signal['entry'],
            'opened_at': datetime.now().isoformat(),
            'status': 'open',
            'pnl': 0,
            'signal': signal
        }
        
        return {
            'success': True,
            'position': position,
            'order_id': f"order-{datetime.now().timestamp()}"
        }
    
    async def execute_kalshi(self, signal: Dict, position_size: float) -> Dict:
        """
        Execute trade on Kalshi
        """
        logger.info("📊 Simulating Kalshi trade (API not implemented)")
        
        market = signal['market']
        
        await asyncio.sleep(0.5)
        
        position = {
            'id': f"pos-{market['id']}-{datetime.now().timestamp()}",
            'market_id': market['id'],
            'market_title': market['title'],
            'platform': 'kalshi',
            'type': signal['type'],
            'entry_price': signal['entry'],
            'target_price': signal['target'],
            'stop_price': signal['stop'],
            'position_size': position_size,
            'shares': position_size / signal['entry'],
            'opened_at': datetime.now().isoformat(),
            'status': 'open',
            'pnl': 0,
            'signal': signal
        }
        
        return {
            'success': True,
            'position': position,
            'order_id': f"order-{datetime.now().timestamp()}"
        }
    
    async def close_position(self, position: Dict, reason: str = 'manual') -> Dict:
        """
        Close an open position
        
        Args:
            position: Position dict
            reason: Reason for closing (stop_loss, target, manual, etc.)
        
        Returns:
            Result dict with final P&L
        """
        logger.info(f"📤 Closing position {position['id']}")
        logger.info(f"   Reason: {reason}")
        
        try:
            platform = position['platform']
            
            # In production, call actual API to close position
            # For now, simulate
            
            await asyncio.sleep(0.3)
            
            # Calculate P&L (simplified)
            # In production, use actual exit price from market
            current_price = position['entry_price']  # Placeholder
            
            if position['type'] == 'BUY':
                pnl = (current_price - position['entry_price']) * position['shares']
            else:
                pnl = (position['entry_price'] - current_price) * position['shares']
            
            # Update position
            position['status'] = 'closed'
            position['closed_at'] = datetime.now().isoformat()
            position['exit_price'] = current_price
            position['pnl'] = pnl
            position['close_reason'] = reason
            
            # Remove from open positions
            if position['id'] in self.open_positions:
                del self.open_positions[position['id']]
            
            logger.info(f"✅ Position closed: P&L = ${pnl:,.2f}")
            
            # Log to trade history
            self.log_closed_position(position)
            
            return {
                'success': True,
                'position': position,
                'pnl': pnl
            }
        
        except Exception as e:
            logger.error(f"❌ Error closing position: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    async def update_position(self, position: Dict, current_price: float) -> Dict:
        """
        Update position with current market price
        Calculate unrealized P&L
        """
        if position['type'] == 'BUY':
            unrealized_pnl = (current_price - position['entry_price']) * position['shares']
        else:
            unrealized_pnl = (position['entry_price'] - current_price) * position['shares']
        
        position['current_price'] = current_price
        position['pnl'] = unrealized_pnl
        position['updated_at'] = datetime.now().isoformat()
        
        return position
    
    def get_open_positions(self) -> List[Dict]:
        """Get all open positions"""
        return list(self.open_positions.values())
    
    def get_position(self, position_id: str) -> Optional[Dict]:
        """Get specific position by ID"""
        return self.open_positions.get(position_id)
    
    def log_trade(self, signal: Dict, result: Dict):
        """Log executed trade to file"""
        try:
            log_file = '../logs/trades.jsonl'
            
            trade_log = {
                'timestamp': datetime.now().isoformat(),
                'market': signal['market']['title'],
                'signal_type': signal['type'],
                'confidence': signal['confidence'],
                'position_size': result['position']['position_size'],
                'entry': signal['entry'],
                'target': signal['target'],
                'stop': signal['stop'],
                'position_id': result['position']['id']
            }
            
            with open(log_file, 'a') as f:
                f.write(json.dumps(trade_log) + '\n')
        
        except Exception as e:
            logger.error(f"Error logging trade: {e}")
    
    def log_closed_position(self, position: Dict):
        """Log closed position to file"""
        try:
            log_file = '../logs/closed_positions.jsonl'
            
            closed_log = {
                'timestamp': datetime.now().isoformat(),
                'position_id': position['id'],
                'market': position['market_title'],
                'type': position['type'],
                'entry': position['entry_price'],
                'exit': position['exit_price'],
                'pnl': position['pnl'],
                'close_reason': position['close_reason'],
                'duration': self.calculate_duration(position)
            }
            
            with open(log_file, 'a') as f:
                f.write(json.dumps(closed_log) + '\n')
        
        except Exception as e:
            logger.error(f"Error logging closed position: {e}")
    
    def calculate_duration(self, position: Dict) -> str:
        """Calculate position holding duration"""
        try:
            opened = datetime.fromisoformat(position['opened_at'])
            closed = datetime.fromisoformat(position['closed_at'])
            duration = closed - opened
            
            hours = duration.total_seconds() / 3600
            return f"{hours:.1f} hours"
        except:
            return "unknown"


# Standalone testing
async def main():
    """Test the trade executor"""
    config = {
        'api_keys': {
            'polymarket': 'test',
            'manifold': 'test',
            'kalshi': 'test'
        },
        'trading': {
            'max_position_size': 5000,
            'stop_loss': -0.15
        }
    }
    
    executor = TradeExecutor(config)
    
    # Mock signal
    signal = {
        'type': 'BUY',
        'confidence': 0.85,
        'market': {
            'id': 'test-market-1',
            'title': 'Bitcoin > $100K by Q2 2026',
            'platform': 'polymarket'
        },
        'entry': 0.75,
        'target': 0.85,
        'stop': 0.65
    }
    
    print(f"\n{'='*80}")
    print(f"TRADE EXECUTION TEST")
    print(f"{'='*80}\n")
    
    # Execute trade
    result = await executor.execute(signal, 1000)
    
    if result['success']:
        position = result['position']
        print(f"✅ Trade executed successfully")
        print(f"\nPosition Details:")
        print(f"  ID: {position['id']}")
        print(f"  Market: {position['market_title']}")
        print(f"  Type: {position['type']}")
        print(f"  Size: ${position['position_size']:,.0f}")
        print(f"  Entry: {position['entry_price']:.1%}")
        print(f"  Target: {position['target_price']:.1%}")
        print(f"  Stop: {position['stop_price']:.1%}")
        
        # Simulate closing
        await asyncio.sleep(1)
        close_result = await executor.close_position(position, 'manual')
        
        if close_result['success']:
            print(f"\n✅ Position closed")
            print(f"  P&L: ${close_result['pnl']:,.2f}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
