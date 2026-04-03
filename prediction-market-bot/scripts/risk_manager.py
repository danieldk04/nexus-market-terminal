#!/usr/bin/env python3
"""
Risk Manager Agent
Manages portfolio risk and protects against losses
Enforces position limits, stop losses, and exposure controls
"""

import asyncio
import logging
from typing import Dict, List
from datetime import datetime
import math

logger = logging.getLogger(__name__)


class RiskManager:
    """
    Manages trading risk across all positions
    Enforces risk limits and prevents over-exposure
    """
    
    def __init__(self, config: Dict):
        self.config = config
        
        # Risk parameters
        self.max_position_size = config['trading']['max_position_size']
        self.stop_loss_pct = abs(config['trading']['stop_loss'])
        self.max_exposure = config['trading']['max_exposure']
        self.max_daily_loss = config['risk']['max_daily_loss']
        self.max_positions = config['trading'].get('max_positions', 10)
        
        # Position sizing method
        self.sizing_method = config['risk'].get('position_sizing', 'kelly_criterion')
        
        # Track daily performance
        self.daily_pnl = 0
        self.daily_start_value = None
        self.last_reset = datetime.now().date()
    
    async def check_can_trade(self, positions: List[Dict]) -> bool:
        """
        Check if bot can open new positions
        
        Args:
            positions: List of current open positions
        
        Returns:
            True if can trade, False otherwise
        """
        # Check position count limit
        if len(positions) >= self.max_positions:
            logger.warning(f"⚠️ Max positions reached ({self.max_positions})")
            return False
        
        # Check daily loss limit
        if self.check_daily_loss_limit():
            logger.warning(f"⚠️ Daily loss limit reached")
            return False
        
        # Check portfolio exposure
        exposure = await self.calculate_exposure(positions)
        if exposure >= self.max_exposure:
            logger.warning(f"⚠️ Max exposure reached ({exposure:.1%})")
            return False
        
        return True
    
    async def calculate_position_size(self, signal: Dict, portfolio_value: float) -> float:
        """
        Calculate optimal position size based on signal and risk parameters
        
        Args:
            signal: Trading signal
            portfolio_value: Current portfolio value
        
        Returns:
            Position size in dollars
        """
        if self.sizing_method == 'kelly_criterion':
            size = self.kelly_criterion_size(signal, portfolio_value)
        elif self.sizing_method == 'fixed_fraction':
            size = self.fixed_fraction_size(portfolio_value)
        elif self.sizing_method == 'volatility_adjusted':
            size = self.volatility_adjusted_size(signal, portfolio_value)
        else:
            size = self.fixed_size()
        
        # Apply maximum position size limit
        size = min(size, self.max_position_size)
        
        logger.info(f"💰 Position size calculated: ${size:,.0f} "
                   f"({size/portfolio_value:.1%} of portfolio)")
        
        return size
    
    def kelly_criterion_size(self, signal: Dict, portfolio_value: float) -> float:
        """
        Calculate position size using Kelly Criterion
        
        Kelly % = (Win% * Avg Win) - (Loss% * Avg Loss) / Avg Win
        """
        confidence = signal['confidence']
        
        # Estimate win probability and payoff
        win_prob = confidence
        loss_prob = 1 - confidence
        
        # Estimate payoff ratio (target vs stop distance)
        upside = abs(signal['target'] - signal['entry'])
        downside = abs(signal['entry'] - signal['stop'])
        
        if downside == 0:
            payoff_ratio = 2.0  # Default
        else:
            payoff_ratio = upside / downside
        
        # Kelly formula
        kelly_pct = (win_prob * payoff_ratio - loss_prob) / payoff_ratio
        
        # Use fractional Kelly (1/4 or 1/2) for safety
        fractional_kelly = max(kelly_pct * 0.25, 0)  # Quarter Kelly
        
        # Cap at 10% of portfolio
        fractional_kelly = min(fractional_kelly, 0.10)
        
        size = portfolio_value * fractional_kelly
        
        logger.debug(f"Kelly sizing: Win%={win_prob:.1%}, Payoff={payoff_ratio:.2f}, "
                    f"Kelly%={kelly_pct:.1%}, Size%={fractional_kelly:.1%}")
        
        return size
    
    def fixed_fraction_size(self, portfolio_value: float, fraction: float = 0.05) -> float:
        """
        Fixed fraction of portfolio (e.g., 5%)
        """
        return portfolio_value * fraction
    
    def volatility_adjusted_size(self, signal: Dict, portfolio_value: float) -> float:
        """
        Adjust size based on signal volatility/uncertainty
        Higher confidence = larger size
        """
        base_size = portfolio_value * 0.05  # 5% base
        confidence_multiplier = signal['confidence']
        
        return base_size * confidence_multiplier
    
    def fixed_size(self) -> float:
        """
        Fixed dollar amount per trade
        """
        return min(self.max_position_size, 1000)
    
    async def check_stop_loss(self, position: Dict) -> bool:
        """
        Check if position hit stop loss
        
        Args:
            position: Position dict with current price
        
        Returns:
            True if stop loss triggered
        """
        if 'current_price' not in position:
            return False
        
        current = position['current_price']
        entry = position['entry_price']
        stop = position['stop_price']
        position_type = position['type']
        
        if position_type == 'BUY':
            # For longs, stop is below entry
            if current <= stop:
                logger.warning(f"🛑 Stop loss triggered: {position['id']}")
                logger.warning(f"   Entry: {entry:.1%}, Current: {current:.1%}, Stop: {stop:.1%}")
                return True
        else:
            # For shorts, stop is above entry
            if current >= stop:
                logger.warning(f"🛑 Stop loss triggered: {position['id']}")
                logger.warning(f"   Entry: {entry:.1%}, Current: {current:.1%}, Stop: {stop:.1%}")
                return True
        
        return False
    
    async def calculate_exposure(self, positions: List[Dict]) -> float:
        """
        Calculate total portfolio exposure
        
        Returns:
            Exposure as decimal (0.5 = 50%)
        """
        if not positions:
            return 0.0
        
        # Sum of all position sizes
        total_exposure = sum(p.get('position_size', 0) for p in positions)
        
        # Assume $50,000 portfolio for calculation
        # In production, get actual portfolio value
        portfolio_value = 50000
        
        return total_exposure / portfolio_value
    
    def check_daily_loss_limit(self) -> bool:
        """
        Check if daily loss limit has been hit
        
        Returns:
            True if limit hit (stop trading)
        """
        # Reset daily tracking if new day
        today = datetime.now().date()
        if today != self.last_reset:
            self.daily_pnl = 0
            self.daily_start_value = None
            self.last_reset = today
        
        # Check if loss exceeds limit
        if self.daily_start_value is None:
            return False  # No reference point yet
        
        loss_pct = abs(self.daily_pnl / self.daily_start_value)
        
        if self.daily_pnl < 0 and loss_pct >= self.max_daily_loss:
            logger.warning(f"⚠️ Daily loss limit reached: {loss_pct:.1%}")
            return True
        
        return False
    
    def update_daily_pnl(self, pnl: float):
        """Update daily P&L tracking"""
        self.daily_pnl += pnl
        
        if self.daily_start_value is None:
            self.daily_start_value = 50000  # Placeholder
    
    def check_correlation(self, positions: List[Dict], new_market: Dict) -> float:
        """
        Check correlation with existing positions
        High correlation = higher risk
        
        Returns:
            Correlation score (0-1)
        """
        if not positions:
            return 0.0
        
        # Check if same category
        new_category = new_market.get('category', '').lower()
        
        same_category_count = sum(
            1 for p in positions
            if p.get('signal', {}).get('market', {}).get('category', '').lower() == new_category
        )
        
        correlation = same_category_count / len(positions)
        
        if correlation > 0.5:
            logger.warning(f"⚠️ High correlation detected: {correlation:.1%} in {new_category}")
        
        return correlation
    
    def calculate_sharpe_ratio(self, returns: List[float], risk_free_rate: float = 0.02) -> float:
        """
        Calculate Sharpe ratio from returns
        
        Args:
            returns: List of daily returns
            risk_free_rate: Annual risk-free rate
        
        Returns:
            Sharpe ratio
        """
        if not returns or len(returns) < 2:
            return 0.0
        
        # Calculate average return and std dev
        avg_return = sum(returns) / len(returns)
        
        variance = sum((r - avg_return) ** 2 for r in returns) / len(returns)
        std_dev = math.sqrt(variance)
        
        if std_dev == 0:
            return 0.0
        
        # Annualize (assuming daily returns)
        daily_rf = risk_free_rate / 252
        sharpe = (avg_return - daily_rf) / std_dev * math.sqrt(252)
        
        return sharpe
    
    def get_risk_metrics(self, positions: List[Dict]) -> Dict:
        """
        Calculate comprehensive risk metrics
        
        Returns:
            Dict with various risk metrics
        """
        exposure = asyncio.run(self.calculate_exposure(positions))
        
        # Calculate position concentration
        if positions:
            position_sizes = [p.get('position_size', 0) for p in positions]
            max_position = max(position_sizes) if position_sizes else 0
            total_capital = 50000  # Placeholder
            concentration = max_position / total_capital if total_capital > 0 else 0
        else:
            concentration = 0
        
        return {
            'total_exposure': exposure,
            'position_count': len(positions),
            'max_positions': self.max_positions,
            'positions_available': self.max_positions - len(positions),
            'daily_pnl': self.daily_pnl,
            'max_daily_loss': self.max_daily_loss,
            'concentration': concentration,
            'can_trade': asyncio.run(self.check_can_trade(positions))
        }


# Standalone testing
async def main():
    """Test the risk manager"""
    config = {
        'trading': {
            'max_position_size': 5000,
            'stop_loss': -0.15,
            'max_exposure': 0.50,
            'max_positions': 10
        },
        'risk': {
            'max_daily_loss': 0.20,
            'position_sizing': 'kelly_criterion'
        }
    }
    
    risk_manager = RiskManager(config)
    
    print(f"\n{'='*80}")
    print(f"RISK MANAGER TEST")
    print(f"{'='*80}\n")
    
    # Test position sizing
    signal = {
        'confidence': 0.75,
        'entry': 0.65,
        'target': 0.80,
        'stop': 0.55
    }
    
    portfolio_value = 50000
    size = await risk_manager.calculate_position_size(signal, portfolio_value)
    
    print(f"Position Sizing:")
    print(f"  Portfolio: ${portfolio_value:,.0f}")
    print(f"  Signal Confidence: {signal['confidence']:.1%}")
    print(f"  Calculated Size: ${size:,.0f}")
    print(f"  Size as % of Portfolio: {size/portfolio_value:.1%}")
    
    # Test with some positions
    positions = [
        {
            'id': 'pos-1',
            'position_size': 2000,
            'entry_price': 0.70,
            'current_price': 0.65,
            'stop_price': 0.60,
            'type': 'BUY'
        },
        {
            'id': 'pos-2',
            'position_size': 3000,
            'entry_price': 0.55,
            'current_price': 0.60,
            'stop_price': 0.45,
            'type': 'BUY'
        }
    ]
    
    print(f"\n{'='*80}")
    print(f"Risk Metrics:")
    print(f"{'='*80}\n")
    
    metrics = risk_manager.get_risk_metrics(positions)
    print(f"Total Exposure: {metrics['total_exposure']:.1%}")
    print(f"Position Count: {metrics['position_count']} / {metrics['max_positions']}")
    print(f"Positions Available: {metrics['positions_available']}")
    print(f"Can Trade: {'Yes' if metrics['can_trade'] else 'No'}")
    
    # Test stop loss
    print(f"\n{'='*80}")
    print(f"Stop Loss Check:")
    print(f"{'='*80}\n")
    
    for pos in positions:
        triggered = await risk_manager.check_stop_loss(pos)
        print(f"Position {pos['id']}: {'STOP TRIGGERED' if triggered else 'OK'}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
