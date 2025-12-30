"""
Backtesting engine using backtesting.py library.
Uses the MultiTimeframeStrategy class for signal generation.
"""
import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
from typing import Optional
from strategy.multi_timeframe_strategy import MultiTimeframeStrategy
from data.binance_client import BinanceClient
from utils.logger import get_logger

logger = get_logger(__name__)


class MultiTimeframeBacktestStrategy(Strategy):
    """
    Wrapper strategy for backtesting.py that uses MultiTimeframeStrategy.
    This bridges our strategy class with the backtesting.py framework.
    
    The SAME MultiTimeframeStrategy class is used for both backtesting and live trading
    to ensure identical signal generation logic.
    """
    
    # Strategy parameters (will be set during init)
    fast_sma_period = 10
    slow_sma_period = 20
    trend_sma_period = 50
    rsi_period = 14
    rsi_overbought = 70
    rsi_oversold = 30
    
    # Store secondary data as class variable (workaround for backtesting.py limitations)
    _df_secondary = None
    
    def init(self):
        """Initialize strategy and indicators."""
        # Initialize the unified strategy class (used by both backtest and live)
        self.strategy = MultiTimeframeStrategy(
            fast_sma_period=self.fast_sma_period,
            slow_sma_period=self.slow_sma_period,
            trend_sma_period=self.trend_sma_period,
            rsi_period=self.rsi_period,
            rsi_overbought=self.rsi_overbought,
            rsi_oversold=self.rsi_oversold
        )
        
        # Access secondary timeframe data from class variable
        self.df_secondary = self._df_secondary
        
        # Calculate indicators
        # Convert backtesting.py capitalized columns to lowercase for strategy
        self.df_primary = self.data.df.copy()
        self.df_primary.columns = [col.lower() for col in self.df_primary.columns]
        self.df_primary, self.df_secondary = self.strategy.calculate_indicators(
            self.df_primary, self.df_secondary
        )
        
        logger.info("Backtest strategy initialized")
    
    def next(self):
        """Execute on each bar (called by backtesting.py)."""
        # Get current index in primary timeframe
        primary_idx = len(self.data) - 1
        
        # Find corresponding index in secondary timeframe
        current_time = self.data.index[-1]
        secondary_idx = self._find_secondary_index(current_time)
        
        if secondary_idx is None:
            return
        
        # Generate signal using our strategy
        signal = self.strategy.generate_signal(
            self.df_primary,
            self.df_secondary,
            primary_idx,
            secondary_idx
        )
        
        # Execute trades based on signal
        if signal == 'BUY' and not self.position:
            self.buy()
            self.strategy.update_position('BUY', self.data.Close[-1])
            logger.info(f"BUY executed at {self.data.Close[-1]:.2f} "
                       f"on {self.data.index[-1]}")
        
        elif signal == 'SELL' and self.position:
            self.position.close()
            self.strategy.update_position('SELL', self.data.Close[-1])
            logger.info(f"SELL executed at {self.data.Close[-1]:.2f} "
                       f"on {self.data.index[-1]}")
    
    def _find_secondary_index(self, primary_time) -> Optional[int]:
        """
        Find the corresponding index in secondary timeframe.
        
        Args:
            primary_time: Timestamp from primary timeframe
        
        Returns:
            Index in secondary timeframe or None
        """
        # Find the last secondary candle that closed before or at primary_time
        secondary_times = self.df_secondary.index
        valid_times = secondary_times[secondary_times <= primary_time]
        
        if len(valid_times) == 0:
            return None
        
        return len(valid_times) - 1


class BacktestEngine:
    """
    Backtesting engine that runs historical simulations.
    Uses the same MultiTimeframeStrategy class as live trading.
    """
    
    def __init__(self, 
                 strategy: MultiTimeframeStrategy,
                 initial_capital: float = 10000.0,
                 commission: float = 0.001,
                 client: Optional[BinanceClient] = None):
        """
        Initialize backtest engine.
        
        Args:
            strategy: MultiTimeframeStrategy instance
            initial_capital: Starting capital (or fetch from Binance)
            commission: Commission per trade (0.001 = 0.1%)
            client: Optional BinanceClient to fetch actual balance
        """
        self.strategy = strategy
        self.commission = commission
        self.client = client
        
        # Try to get balance from Binance if client provided
        if client and hasattr(client, 'api_key') and client.api_key:
            try:
                account = client.get_account_info()
                usdt_balance = next((float(b['free']) for b in account['balances'] 
                                   if b['asset'] == 'USDT'), 0)
                if usdt_balance > 10:  # Only use if reasonable balance
                    self.initial_capital = usdt_balance
                    logger.info(f"Using Binance USDT balance: ${usdt_balance:.2f}")
                else:
                    self.initial_capital = initial_capital
                    logger.info(f"Using default capital: ${initial_capital:.2f}")
            except Exception as e:
                logger.warning(f"Could not fetch balance: {e}, using default")
                self.initial_capital = initial_capital
        else:
            self.initial_capital = initial_capital
            logger.info(f"Using default capital: ${initial_capital:.2f}")
        
        self.results = None
        self.trades = []
        
        logger.info(f"BacktestEngine initialized with capital=${self.initial_capital:.2f}")
    
    def run(self,
            df_primary: pd.DataFrame,
            df_secondary: pd.DataFrame,
            symbol: str = 'BTCUSDT') -> pd.DataFrame:
        """
        Run backtest on historical data.
        
        Args:
            df_primary: Primary timeframe OHLCV data
            df_secondary: Secondary timeframe OHLCV data
            symbol: Trading symbol
        
        Returns:
            DataFrame with trade results
        """
        logger.info("Starting backtest...")
        
        # Reset strategy state
        self.strategy.reset()
        
        # Calculate indicators
        df_primary, df_secondary = self.strategy.calculate_indicators(
            df_primary, df_secondary
        )
        
        # Ensure data is properly formatted
        df_primary = df_primary[['open', 'high', 'low', 'close', 'volume']].copy()
        df_primary.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        # Store secondary data in class variable (workaround for backtesting.py)
        MultiTimeframeBacktestStrategy._df_secondary = df_secondary
        
        # Calculate appropriate cash for fractional trading
        # Use small fraction of capital to avoid fractional trading warning
        avg_price = df_primary['Close'].mean()
        cash_to_use = max(self.initial_capital, avg_price * 2)
        
        logger.info(f"Using cash: ${cash_to_use:.2f} (avg price: ${avg_price:.2f})")
        
        # Create backtest instance
        bt = Backtest(
            df_primary,
            MultiTimeframeBacktestStrategy,
            cash=cash_to_use,
            commission=self.commission,
            exclusive_orders=True,
            trade_on_close=True  # Execute at candle close
        )
        
        # Set strategy parameters
        self.results = bt.run(
            fast_sma_period=self.strategy.fast_sma,
            slow_sma_period=self.strategy.slow_sma,
            trend_sma_period=self.strategy.trend_sma,
            rsi_period=self.strategy.rsi_period,
            rsi_overbought=self.strategy.rsi_overbought,
            rsi_oversold=self.strategy.rsi_oversold
        )
        
        logger.info("Backtest completed")
        self._process_trades(symbol)
        
        return self.get_trades_dataframe()
    
    def _process_trades(self, symbol: str):
        """Process trade results from backtest."""
        if self.results is None:
            return
        
        trades_data = self.results._trades
        
        if trades_data.empty:
            logger.warning("No trades executed during backtest")
            return
        
        self.trades = []
        
        for idx, trade in trades_data.iterrows():
            # Record BUY entry
            buy_record = {
                'timestamp': trade['EntryTime'],
                'symbol': symbol,
                'side': 'BUY',
                'entry_price': trade['EntryPrice'],
                'exit_time': None,
                'exit_price': None,
                'size': trade['Size'],
                'pnl': None,
                'return_pct': None
            }
            self.trades.append(buy_record)
            
            # Record SELL exit
            sell_record = {
                'timestamp': trade['ExitTime'],
                'symbol': symbol,
                'side': 'SELL',
                'entry_price': trade['EntryPrice'],
                'exit_time': trade['ExitTime'],
                'exit_price': trade['ExitPrice'],
                'size': trade['Size'],
                'pnl': trade['PnL'],
                'return_pct': trade['ReturnPct'] * 100
            }
            self.trades.append(sell_record)
        
        logger.info(f"Processed {len(trades_data)} trade pairs ({len(self.trades)} total records: {len(trades_data)} BUY + {len(trades_data)} SELL)")
    
    def get_trades_dataframe(self) -> pd.DataFrame:
        """Get trades as DataFrame."""
        if not self.trades:
            return pd.DataFrame()
        
        return pd.DataFrame(self.trades)
    
    def save_trades(self, filepath: str):
        """
        Save trades to CSV file.
        
        Args:
            filepath: Path to save CSV
        """
        df = self.get_trades_dataframe()
        
        if df.empty:
            logger.warning("No trades to save")
            return
        
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(df)} trades to {filepath}")
    
    def get_summary(self) -> dict:
        """Get backtest performance summary."""
        if self.results is None:
            return {}
        
        # Scale returns back to original capital if we adjusted cash
        actual_return_pct = self.results['Return [%]']
        
        # Count complete trades (round trips) - each trade has BUY + SELL, so divide by 2
        # Or count only BUY rows
        complete_trades = len([t for t in self.trades if t.get('side') == 'BUY'])
        
        summary = {
            'total_trades': complete_trades,  # Number of complete round trips
            'win_rate': self.results['Win Rate [%]'],
            'total_return': actual_return_pct,
            'sharpe_ratio': self.results.get('Sharpe Ratio', 0),
            'max_drawdown': self.results['Max. Drawdown [%]'],
            'final_equity': self.results['Equity Final [$]'],
            'initial_capital': self.initial_capital
        }
        
        return summary
    
    def print_summary(self):
        """Print backtest summary."""
        if self.results is None:
            logger.error("No backtest results available")
            return
        
        summary = self.get_summary()
        
        print("\n" + "="*50)
        print("BACKTEST SUMMARY")
        print("="*50)
        print(f"Initial Capital: ${summary['initial_capital']:,.2f}")
        print(f"Final Equity: ${summary['final_equity']:,.2f}")
        print(f"Total Trades: {summary['total_trades']}")
        print(f"Win Rate: {summary['win_rate']:.2f}%")
        print(f"Total Return: {summary['total_return']:.2f}%")
        print(f"Sharpe Ratio: {summary['sharpe_ratio']:.2f}")
        print(f"Max Drawdown: {summary['max_drawdown']:.2f}%")
        print("="*50 + "\n")