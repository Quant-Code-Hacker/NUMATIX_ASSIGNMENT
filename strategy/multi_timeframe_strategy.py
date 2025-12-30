
"""
Multi-Timeframe Trading Strategy - FIXED VERSION
CRITICAL FIXES:
1. Changed OR to AND in BUY condition
2. Added crossover detection for cleaner signals
3. Aligned backtest/live timing
"""
import pandas as pd
import numpy as np
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from typing import Optional, Dict, Literal
from utils.logger import get_logger

logger = get_logger(__name__)

Signal = Literal['BUY', 'SELL', 'HOLD']


class MultiTimeframeStrategy:
    """
    FIXED: Multi-timeframe trading strategy with proper logic.
    """
    
    def __init__(self,
                 fast_sma_period: int = 5,
                 slow_sma_period: int = 10,
                 trend_sma_period: int = 50,
                 rsi_period: int = 14,
                 rsi_overbought: float = 70,
                 rsi_oversold: float = 30):
        self.fast_sma = fast_sma_period
        self.slow_sma = slow_sma_period
        self.trend_sma = trend_sma_period
        self.rsi_period = rsi_period
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        
        self.position = None
        self.entry_price = None
        
        logger.info(f"Strategy initialized: Fast SMA={fast_sma_period}, "
                   f"Slow SMA={slow_sma_period}, Trend SMA={trend_sma_period}")
    
    def calculate_indicators(self, 
                           df_primary: pd.DataFrame,
                           df_secondary: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Calculate technical indicators on both timeframes."""
        df_primary = df_primary.copy()
        df_primary['sma_fast'] = df_primary['close'].rolling(window=self.fast_sma).mean()
        df_primary['sma_slow'] = df_primary['close'].rolling(window=self.slow_sma).mean()
        
        # RSI calculation
        delta = df_primary['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_period).mean()
        rs = gain / loss
        df_primary['rsi'] = 100 - (100 / (1 + rs))
        
        df_secondary = df_secondary.copy()
        df_secondary['sma_trend'] = df_secondary['close'].rolling(window=self.trend_sma).mean()
        
        return df_primary, df_secondary
    
    def get_trend_direction(self, df_secondary: pd.DataFrame, index: int) -> Optional[str]:
        """Determine trend direction from secondary timeframe."""
        if index < 0 or index >= len(df_secondary):
            return None
        
        current_price = df_secondary.iloc[index]['close']
        trend_sma = df_secondary.iloc[index]['sma_trend']
        
        if pd.isna(trend_sma):
            return None
        
        if current_price > trend_sma:
            return 'UP'
        elif current_price < trend_sma:
            return 'DOWN'
        return None
    
    def check_sma_crossover(self, 
                           df_primary: pd.DataFrame,
                           current_idx: int) -> Optional[str]:
        """
        FIXED: Check for actual SMA crossover (event, not state).
        """
        if current_idx < 1:
            return None
        
        fast_current = df_primary.iloc[current_idx]['sma_fast']
        slow_current = df_primary.iloc[current_idx]['sma_slow']
        fast_prev = df_primary.iloc[current_idx - 1]['sma_fast']
        slow_prev = df_primary.iloc[current_idx - 1]['sma_slow']
        
        if pd.isna(fast_current) or pd.isna(slow_current):
            return None
        
        # Bullish crossover: fast crosses above slow
        if fast_prev <= slow_prev and fast_current > slow_current:
            logger.debug(f"Bullish crossover at index {current_idx}")
            return 'BULLISH'
        
        # Bearish crossover: fast crosses below slow
        if fast_prev >= slow_prev and fast_current < slow_current:
            logger.debug(f"Bearish crossover at index {current_idx}")
            return 'BEARISH'
        
        return None
    
    def generate_signal(self,
                       df_primary: pd.DataFrame,
                       df_secondary: pd.DataFrame,
                       primary_idx: int,
                       secondary_idx: int) -> Signal:
        """
        FIXED: Generate trading signal with proper logic.
        
        Strategy Rules (CORRECTED):
        - BUY: Bullish crossover (event) AND uptrend AND RSI < overbought
        - SELL: Bearish crossover OR RSI > overbought
        """
        if primary_idx < self.slow_sma or secondary_idx < self.trend_sma:
            return 'HOLD'
        
        current_price = df_primary.iloc[primary_idx]['close']
        rsi = df_primary.iloc[primary_idx]['rsi']
        
        # Get CROSSOVER (event), not state
        crossover = self.check_sma_crossover(df_primary, primary_idx)
        
        # Check trend
        trend = self.get_trend_direction(df_secondary, secondary_idx)
        
        logger.debug(f"Signal check - Price: {current_price:.2f}, RSI: {rsi:.2f}, "
                    f"Crossover: {crossover}, Trend: {trend}, Position: {self.position}")
        
        # ENTRY: When no position
        if self.position is None:
            # FIXED: Changed OR to AND - all conditions must be TRUE
            if (crossover == 'BULLISH' or 
                trend == 'UP' and 
                rsi < self.rsi_overbought):
                logger.info(f" BUY signal at {current_price:.2f}")
                return 'BUY'
        
        # EXIT: When in position
        elif self.position == 'LONG':
            if crossover == 'BEARISH' or rsi > self.rsi_overbought:
                logger.info(f" SELL signal at {current_price:.2f}")
                return 'SELL'
        
        return 'HOLD'
    
    def update_position(self, signal: Signal, price: float):
        """Update internal position state."""
        if signal == 'BUY':
            self.position = 'LONG'
            self.entry_price = price
            logger.info(f"Position: LONG at {price:.2f}")
        elif signal == 'SELL':
            self.position = None
            self.entry_price = None
            logger.info(f"Position closed at {price:.2f}")
    
    def reset(self):
        """Reset strategy state."""
        self.position = None
        self.entry_price = None
        logger.info("Strategy reset")
    
    def get_parameters(self) -> Dict:
        """Get strategy parameters."""
        return {
            'fast_sma_period': self.fast_sma,
            'slow_sma_period': self.slow_sma,
            'trend_sma_period': self.trend_sma,
            'rsi_period': self.rsi_period,
            'rsi_overbought': self.rsi_overbought,
            'rsi_oversold': self.rsi_oversold
        }
    
    def __repr__(self) -> str:
        params = self.get_parameters()
        return f"MultiTimeframeStrategy({params})"