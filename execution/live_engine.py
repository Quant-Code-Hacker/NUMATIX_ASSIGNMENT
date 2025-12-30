# Live trading engine with candle-close timing alignment. Waits for candle close to match backtest behavior.

import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from data.binance_client import BinanceClient
from data.data_handler import DataHandler
from strategy.multi_timeframe_strategy import MultiTimeframeStrategy, Signal
from utils.logger import get_logger

logger = get_logger(__name__)


class LiveTradingEngine:
    """
    FIXED: Live trading engine that checks signals at candle closes.
    This matches backtest behavior (trade_on_close=True).
    """
    
    def __init__(self,
                 client: BinanceClient,
                 strategy: MultiTimeframeStrategy,
                 symbol: str,
                 primary_timeframe: str,
                 secondary_timeframe: str,
                 trade_quantity: float,
                 check_interval: int = 60):
        """
        Initialize live trading engine with timing alignment.
        
        Args:
            check_interval: IGNORED - We now calculate from primary_timeframe
        """
        self.client = client
        self.strategy = strategy
        self.symbol = symbol
        self.primary_tf = primary_timeframe
        self.secondary_tf = secondary_timeframe
        self.quantity = trade_quantity
        
        # Calculate check interval from primary timeframe
        self.check_interval = self._parse_timeframe_to_seconds(primary_timeframe)
        
        self.data_handler = DataHandler(client, symbol)
        self.trades = []
        self.is_running = False
        
        logger.info(f"LiveTradingEngine initialized for {symbol}")
        logger.info(f"Primary TF: {primary_timeframe}, Secondary TF: {secondary_timeframe}")
        logger.info(f"Check interval: {self.check_interval}s (aligned with {primary_timeframe})")
    
    def _parse_timeframe_to_seconds(self, timeframe: str) -> int:
        """
        Parse timeframe string to seconds.
        
        Args:
            timeframe: e.g., '15m', '1h', '4h'
        
        Returns:
            Seconds in the timeframe
        """
        if timeframe.endswith('m'):
            minutes = int(timeframe[:-1])
            return minutes * 60
        elif timeframe.endswith('h'):
            hours = int(timeframe[:-1])
            return hours * 60 * 60
        elif timeframe.endswith('d'):
            days = int(timeframe[:-1])
            return days * 24 * 60 * 60
        else:
            logger.warning(f"Unknown timeframe format: {timeframe}, using 60s")
            return 60
    
    def _wait_for_candle_close(self):
        """
        CRITICAL FIX: Wait until the next candle close.
        This ensures we check at the same time as backtest.
        """
        now = datetime.utcnow()
        
        # Parse timeframe (e.g., "15m" -> 15 minutes)
        if self.primary_tf.endswith('m'):
            interval_minutes = int(self.primary_tf[:-1])
        elif self.primary_tf.endswith('h'):
            interval_minutes = int(self.primary_tf[:-1]) * 60
        else:
            logger.warning(f"Unknown timeframe: {self.primary_tf}")
            time.sleep(self.check_interval)
            return
        
        # Calculate when current candle closes
        current_minute = now.minute
        current_second = now.second
        
        # Find next candle close
        minutes_into_candle = current_minute % interval_minutes
        minutes_until_close = interval_minutes - minutes_into_candle
        seconds_until_close = (minutes_until_close * 60) - current_second
        
        # If we're very close to close (within 5 seconds), wait for next candle
        if seconds_until_close < 5:
            seconds_until_close += interval_minutes * 60
        
        next_close = now + timedelta(seconds=seconds_until_close)
        
        logger.info(f"Current time: {now.strftime('%H:%M:%S')} UTC")
        logger.info(f"Next candle close: {next_close.strftime('%H:%M:%S')} UTC")
        logger.info(f"Waiting {seconds_until_close} seconds...")
        
        time.sleep(seconds_until_close)
    
    def start(self, max_trades: Optional[int] = None):
        """
        FIXED: Start live trading loop with candle-close alignment.
        
        Args:
            max_trades: Maximum number of trades (BUY orders) to execute
        """
        print(f"\n{'='*60}")
        print(f"STARTING TIMING-ALIGNED LIVE TRADING")
        print(f"{'='*60}")
        if max_trades:
            print(f"Max trades limit: {max_trades}")
        print(f"Checking at candle closes ({self.primary_tf}) to match backtest")
        print(f"{'='*60}\n")
        
        logger.info("Starting timing-aligned live trading...")
        self.is_running = True
        self.strategy.reset()
        
        trade_count = 0
        iteration = 0
        
        try:
            while self.is_running:
                iteration += 1
                
                # CRITICAL FIX: Wait for candle close before checking
                if iteration > 1:  # Skip on first iteration to check immediately
                    self._wait_for_candle_close()
                
                print(f"\n{'-'*60}")
                print(f" Iteration #{iteration} | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
                print(f"{'-'*60}")
                
                logger.info(f"\n--- Iteration {iteration} at candle close ---")
                
                if max_trades and trade_count >= max_trades:
                    print(f"\n Reached max trades limit: {max_trades}")
                    logger.info(f"Reached max trades limit: {max_trades}")
                    break
                
                try:
                    # Fetch latest data
                    print(" Fetching market data at candle close...")
                    logger.debug("Fetching market data...")
                    df_primary, df_secondary = self._fetch_latest_data()
                    
                    if df_primary.empty or df_secondary.empty:
                        print(" Empty data received, skipping...")
                        logger.warning("Empty data received, skipping this iteration")
                        continue
                    
                    print(f" Data fetched: {len(df_primary)} primary, {len(df_secondary)} secondary candles")
                    
                    # Calculate indicators
                    print(" Calculating indicators...")
                    logger.debug("Calculating indicators...")
                    df_primary, df_secondary = self.strategy.calculate_indicators(
                        df_primary, df_secondary
                    )
                    
                    # Generate signal
                    print(" Generating signal...")
                    logger.debug("Generating signal...")
                    signal = self._generate_signal(df_primary, df_secondary)
                    
                    # Show signal result
                    if signal == 'BUY':
                        print(f"BUY SIGNAL DETECTED!")
                    elif signal == 'SELL':
                        print(f"SELL SIGNAL DETECTED!")
                    else:
                        print(f"HOLD (No signal)")
                    
                    logger.info(f"Signal: {signal}")
                    
                    # Execute trade if signal
                    if signal != 'HOLD':
                        print(f"\n{'='*60}")
                        print(f" EXECUTING {signal} ORDER...")
                        print(f"{'='*60}")
                        logger.info(f"{signal} SIGNAL DETECTED - Executing trade...")
                        success = self._execute_trade(signal, df_primary)
                        if success:
                            if signal == 'BUY':
                                trade_count += 1
                                print(f"BUY order executed! Complete trades so far: {trade_count}")
                                logger.info(f" BUY order executed! Complete trades so far: {trade_count}")
                            else:
                                print(f" SELL order executed! Closed trade #{trade_count}")
                                logger.info(f"SELL order executed! Closed trade #{trade_count}")
                            print(f"Total complete trades: {trade_count}/{max_trades if max_trades else 'unlimited'}")
                            logger.info(f"Total complete trades: {trade_count}/{max_trades if max_trades else 'unlimited'}")
                        else:
                            print(f"Trade execution failed for {signal} signal")
                            logger.warning(f" Trade execution failed for {signal} signal")
                    else:
                        if self.strategy.position is None:
                            print("Waiting for BUY signal conditions...")
                            logger.debug("HOLDING - Waiting for BUY signal conditions...")
                        else:
                            print("In position, waiting for SELL signal...")
                            logger.debug("HOLDING - In position, waiting for SELL signal...")
                    
                except Exception as e:
                    print(f"[ERROR] Error in iteration {iteration}: {e}")
                    logger.error(f"Error in trading iteration {iteration}: {e}", exc_info=True)
                    print("Continuing to next iteration...")
                    logger.info("Continuing to next iteration...")
        
        except KeyboardInterrupt:
            print(f"\n\n{'='*60}")
            print("Live trading interrupted by user (Ctrl+C)")
            print(f"{'='*60}")
            logger.info("Live trading interrupted by user")
        except Exception as e:
            print(f"\n[ERROR] FATAL ERROR: {e}")
            logger.error(f"Fatal error in live trading loop: {e}", exc_info=True)
        finally:
            self.stop()
            print(f"\n{'='*60}")
            print(f"Live trading stopped. Total trades executed: {trade_count}")
            print(f"{'='*60}")
            logger.info(f"Live trading stopped. Total trades executed: {trade_count}")
            
            # Log current trades state
            logger.info(f"Current trades in memory: {len(self.trades)} records")
            if self.trades:
                buy_count = sum(1 for t in self.trades if t.get('side') == 'BUY')
                sell_count = sum(1 for t in self.trades if t.get('side') == 'SELL')
                logger.info(f"  - {buy_count} BUY orders, {sell_count} SELL orders")
                print(f"Trade records in memory: {len(self.trades)} ({buy_count} BUY, {sell_count} SELL)")
    
    def stop(self):
        """Stop live trading."""
        logger.info("Stopping live trading engine...")
        self.is_running = False
    
    def _fetch_latest_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Fetch latest market data for both timeframes."""
        logger.debug("Fetching latest market data...")
        
        try:
            df_primary, df_secondary = self.data_handler.get_latest_multi_timeframe_data(
                self.primary_tf,
                self.secondary_tf,
                primary_limit=100,
                secondary_limit=100
            )
            
            if df_primary.empty:
                logger.warning("Primary timeframe data is empty!")
            if df_secondary.empty:
                logger.warning("Secondary timeframe data is empty!")
            
            logger.debug(f"Fetched {len(df_primary)} primary and "
                        f"{len(df_secondary)} secondary candles")
            
            return df_primary, df_secondary
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}", exc_info=True)
            raise
    
    def _generate_signal(self, 
                        df_primary: pd.DataFrame,
                        df_secondary: pd.DataFrame) -> Signal:
        """Generate trading signal using strategy (SAME as backtest)."""
        primary_idx = len(df_primary) - 1
        secondary_idx = len(df_secondary) - 1
        
        current_price = df_primary.iloc[primary_idx]['close']
        
        # Get indicators for diagnostic logging
        fast_sma = df_primary.iloc[primary_idx].get('sma_fast', float('nan'))
        slow_sma = df_primary.iloc[primary_idx].get('sma_slow', float('nan'))
        rsi = df_primary.iloc[primary_idx].get('rsi', float('nan'))
        trend_sma = df_secondary.iloc[secondary_idx].get('sma_trend', float('nan'))
        
        # Check crossover and trend
        crossover = None
        trend = None
        if hasattr(self.strategy, 'check_sma_crossover'):
            crossover = self.strategy.check_sma_crossover(df_primary, primary_idx)
        if hasattr(self.strategy, 'get_trend_direction'):
            trend = self.strategy.get_trend_direction(df_secondary, secondary_idx)
        
        # Log market state
        fast_str = f"${fast_sma:.2f}" if not pd.isna(fast_sma) else "N/A"
        slow_str = f"${slow_sma:.2f}" if not pd.isna(slow_sma) else "N/A"
        rsi_str = f"{rsi:.1f}" if not pd.isna(rsi) else "N/A"
        trend_str = trend if trend else "N/A"
        crossover_str = crossover if crossover else "None"
        
        print(f"[MARKET] Market State:")
        print(f"   Price: ${current_price:.2f}")
        print(f"   Fast SMA: {fast_str} | Slow SMA: {slow_str}")
        print(f"   RSI: {rsi_str} | Trend: {trend_str}")
        print(f"   Crossover: {crossover_str} | Position: {self.strategy.position or 'None'}")
        
        logger.info(f"Market Check - Price: ${current_price:.2f} | "
                   f"Fast SMA: {fast_str} | Slow SMA: {slow_str} | "
                   f"RSI: {rsi_str} | Trend: {trend_str} | Crossover: {crossover_str}")
        
        # Generate signal using the SAME strategy method as backtesting
        signal = self.strategy.generate_signal(
            df_primary,
            df_secondary,
            primary_idx,
            secondary_idx
        )
        
        if signal != 'HOLD':
            logger.info(f"[SIGNAL] Signal generated: {signal}")
        
        return signal
    
    def _execute_trade(self, signal: Signal, df_primary: pd.DataFrame) -> bool:
        """Execute trade on Binance Testnet."""
        current_price = df_primary.iloc[-1]['close']
        timestamp = df_primary.index[-1]
        
        try:
            if signal == 'BUY':
                print(f"[ORDER] Placing BUY order: {self.quantity} {self.symbol} @ market price ~${current_price:.2f}")
                logger.info(f"Placing BUY order: {self.quantity} {self.symbol} "
                          f"@ market price ~{current_price:.2f}")
                
                order = self.client.create_order(
                    symbol=self.symbol,
                    side='BUY',
                    order_type='MARKET',
                    quantity=self.quantity
                )
                
                trade_record = {
                    'timestamp': timestamp,
                    'symbol': self.symbol,
                    'side': 'BUY',
                    'entry_price': current_price,
                    'quantity': self.quantity,
                    'order_id': order['orderId'],
                    'status': order['status']
                }
                
                self.trades.append(trade_record)
                self.strategy.update_position('BUY', current_price)
                
                print(f"[SUCCESS] BUY order executed successfully!")
                print(f"   Order ID: {order['orderId']}")
                print(f"   Status: {order['status']}")
                logger.info(f"BUY order executed: Order ID {order['orderId']}")
                return True
            
            elif signal == 'SELL':
                print(f"[ORDER] Placing SELL order: {self.quantity} {self.symbol} @ market price ~${current_price:.2f}")
                logger.info(f"Placing SELL order: {self.quantity} {self.symbol} "
                          f"@ market price ~{current_price:.2f}")
                
                order = self.client.create_order(
                    symbol=self.symbol,
                    side='SELL',
                    order_type='MARKET',
                    quantity=self.quantity
                )
                
                # Calculate PnL from last BUY trade
                entry_price = None
                pnl = None
                pnl_pct = None
                if self.trades:
                    for trade in reversed(self.trades):
                        if trade.get('side') == 'BUY' and trade.get('exit_time') is None:
                            entry_price = trade['entry_price']
                            pnl = (current_price - entry_price) * self.quantity
                            pnl_pct = ((current_price - entry_price) / entry_price) * 100
                            
                            trade['exit_time'] = timestamp
                            trade['exit_price'] = current_price
                            trade['pnl'] = pnl
                            trade['return_pct'] = pnl_pct
                            break
                
                sell_record = {
                    'timestamp': timestamp,
                    'symbol': self.symbol,
                    'side': 'SELL',
                    'entry_price': entry_price if entry_price else current_price,
                    'quantity': self.quantity,
                    'order_id': order['orderId'],
                    'status': order['status'],
                    'exit_time': timestamp,
                    'exit_price': current_price,
                    'pnl': pnl,
                    'return_pct': pnl_pct
                }
                
                self.trades.append(sell_record)
                self.strategy.update_position('SELL', current_price)
                
                print(f"[SUCCESS] SELL order executed successfully!")
                print(f"   Order ID: {order['orderId']}")
                print(f"   Status: {order['status']}")
                if pnl is not None:
                    print(f"   PnL: ${pnl:.2f} ({pnl_pct:+.2f}%)")
                logger.info(f"SELL order executed: Order ID {order['orderId']}")
                return True
        
        except Exception as e:
            print(f"[ERROR] Failed to execute {signal} order: {e}")
            logger.error(f"Failed to execute {signal} order: {e}", exc_info=True)
            return False
        
        return False
    
    def get_trades_dataframe(self) -> pd.DataFrame:
        """Get executed trades as DataFrame."""
        if not self.trades:
            logger.debug("No trades in memory")
            return pd.DataFrame()
        
        logger.debug(f"Converting {len(self.trades)} trade records to DataFrame")
        try:
            df = pd.DataFrame(self.trades)
            
            required_columns = ['timestamp', 'symbol', 'side', 'entry_price', 'quantity', 
                               'order_id', 'status', 'exit_time', 'exit_price', 'pnl', 'return_pct']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            
            return df
        except Exception as e:
            logger.error(f"Error converting trades to DataFrame: {e}", exc_info=True)
            return pd.DataFrame()
    
    def save_trades(self, filepath: str):
        """Save trades to CSV file."""
        df = self.get_trades_dataframe()
        
        from pathlib import Path
        file_path = Path(filepath)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if df.empty:
            print("\n[WARNING] No trades executed - creating empty trades file")
            logger.warning("No trades executed - creating empty trades file")

            empty_columns = [
                'timestamp', 'symbol', 'side', 'entry_price', 'quantity',
                'order_id', 'status', 'exit_time', 'exit_price', 'pnl', 'return_pct'
            ]
            pd.DataFrame(columns=empty_columns).to_csv(filepath, index=False)

            print(f"[SUCCESS] Empty trade file created at {filepath}")
            logger.info(f"Empty trade file created at {filepath}")
            return
        
        df.to_csv(filepath, index=False)
        
        print(f"\n{'='*60}")
        print(f"TRADES SAVED SUCCESSFULLY")
        print(f"{'='*60}")
        print(f"File: {filepath}")
        print(f"Total records: {len(df)}")
        
        buy_count = len(df[df['side'] == 'BUY'])
        sell_count = len(df[df['side'] == 'SELL'])
        print(f"   - {buy_count} BUY orders")
        print(f"   - {sell_count} SELL orders")
        print(f"{'='*60}")
        
        logger.info(f"Saved {len(df)} trade records to {filepath}")
    
    def get_summary(self) -> dict:
        """Get trading session summary."""
        df = self.get_trades_dataframe()
        
        if df.empty:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'total_pnl': 0.0,
                'win_rate': 0.0
            }
        
        buy_trades = df[df['side'] == 'BUY']
        completed_trades = buy_trades[buy_trades['exit_price'].notna()]
        
        total_trades = len(completed_trades)
        winning_trades = len(completed_trades[completed_trades['pnl'] > 0])
        losing_trades = len(completed_trades[completed_trades['pnl'] < 0])
        total_pnl = completed_trades['pnl'].sum() if not completed_trades.empty else 0
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        return {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'total_pnl': total_pnl,
            'win_rate': win_rate
        }
    
    def print_summary(self):
        """Print trading session summary."""
        summary = self.get_summary()
        
        print("\n" + "="*60)
        print("LIVE TRADING SUMMARY")
        print("="*60)
        print(f"Total Trades: {summary['total_trades']}")
        print(f"Winning Trades: {summary['winning_trades']}")
        print(f"Losing Trades: {summary['losing_trades']}")
        print(f"Win Rate: {summary['win_rate']:.2f}%")
        print(f"Total PnL: ${summary['total_pnl']:.2f}")
        print("="*60 + "\n")