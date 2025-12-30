
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))

from data.binance_client import BinanceClient
from data.data_handler import DataHandler
from strategy.multi_timeframe_strategy import MultiTimeframeStrategy
from execution.backtest_engine import BacktestEngine
from config import settings
from utils.logger import setup_logger_for_module

logger = setup_logger_for_module('backtest_validation', settings.LOGS_DIR, prefix='')


def main():
    print("\n" + "="*60)
    print("BACKTEST")
    print("="*60)
    
    print(f"\nReading live trades from: {settings.LIVE_TRADES_FILE}")
    try:
        live_df = pd.read_csv(settings.LIVE_TRADES_FILE)
        
        # Get time range
        live_df['timestamp'] = pd.to_datetime(live_df['timestamp'])
        first_trade_time = live_df['timestamp'].min()
        last_trade_time = live_df['timestamp'].max()
        
        print(f"Found {len(live_df)} live trade records")
        print(f"\n Live Trading Period:")
        print(f"   First trade: {first_trade_time}")
        print(f"   Last trade:  {last_trade_time}")
        print(f"   Duration: {last_trade_time - first_trade_time}")
        
    except Exception as e:
        print(f"\n ERROR reading live trades: {e}")
        sys.exit(1)
    
    # Get COMPLETE date range
    # Get the date range, not just trade times
    first_date = first_trade_time.date()
    last_date = last_trade_time.date()
    
    # Start from SAME day as first trade and end later than the live trade period

    backtest_start = datetime.combine(first_date, datetime.min.time())
    backtest_end = datetime.combine(last_date, datetime.max.time()) + timedelta(days = 1)
    
    backtest_end = backtest_end + timedelta(hours=1)
    
    start_str = backtest_start.strftime('%Y-%m-%d')
    end_str = backtest_end.strftime('%Y-%m-%d')
    
    print(f"\nBacktest Period:")
    print(f"   Start: {start_str} 00:00:00")
    print(f"   End:   {end_str} 23:59:59")
    print(f"   Total days: {(backtest_end - backtest_start).days}")
    
    # Step 4: Connect and fetch data
    print(f"\n Connecting to Binance...")
    
    if settings.BINANCE_API_KEY and settings.BINANCE_API_KEY != 'your_api_key_here':
        client = BinanceClient(
            api_key=settings.BINANCE_API_KEY,
            api_secret=settings.BINANCE_API_SECRET,
            testnet=True
        )
        print("Using Testnet with API keys")
    else:
        client = BinanceClient(api_key='', api_secret='', testnet=True)
        print(" Using public testnet endpoint")
    
    data_handler = DataHandler(client, settings.SYMBOL)
    
    print(f"\nFetching historical data...")
    print(f"   Symbol: {settings.SYMBOL}")
    print(f"   Primary TF: {settings.PRIMARY_TIMEFRAME}")
    print(f"   Secondary TF: {settings.SECONDARY_TIMEFRAME}")
    print(f"   Date range: {start_str} to {end_str}")
    
    try:
        df_primary, df_secondary = data_handler.get_multi_timeframe_data(
            primary_tf=settings.PRIMARY_TIMEFRAME,
            secondary_tf=settings.SECONDARY_TIMEFRAME,
            start_date=start_str,
            end_date=end_str
        )

        print(f"   Primary candles: {len(df_primary)}")
        print(f"   Secondary candles: {len(df_secondary)}")
    
        print(f"   Primary data range:")
        print(f"      First: {df_primary.index.min()}")
        print(f"      Last:  {df_primary.index.max()}")
        print(f"   Secondary data range:")
        print(f"      First: {df_secondary.index.min()}")
        print(f"      Last:  {df_secondary.index.max()}")
        
        # Check if we have data covering the last trade
        if df_primary.index.max() < last_trade_time:
            print(f"\n  WARNING: Primary data ends BEFORE last trade!")
            print(f"   Last primary candle: {df_primary.index.max()}")
            print(f"   Last trade time:     {last_trade_time}")
            print(f"   Gap: {last_trade_time - df_primary.index.max()}")
        else:
            print(f" Primary data covers all live trades")
        
        if df_secondary.index.max() < last_trade_time:
            print(f"\n  WARNING: Secondary data ends BEFORE last trade!")
            print(f"   Last secondary candle: {df_secondary.index.max()}")
            print(f"   Last trade time:       {last_trade_time}")
        else:
            print(f"Secondary data covers all live trades")
            
    except Exception as e:
        print(f"\n Failed to fetch data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Step 5: Check data quality
    print(f"\n Data Quality Check:")
    primary_days = (df_primary.index.max() - df_primary.index.min()).days
    secondary_days = (df_secondary.index.max() - df_secondary.index.min()).days
    print(f"   Primary spans: {primary_days} days")
    print(f"   Secondary spans: {secondary_days} days")
    
    # Calculate expected candles based on timeframe
    def parse_timeframe(tf):
        """Parse timeframe string to minutes"""
        if tf.endswith('m'):
            return int(tf[:-1])
        elif tf.endswith('h'):
            return int(tf[:-1]) * 60
        return 0
    
    primary_minutes = parse_timeframe(settings.PRIMARY_TIMEFRAME)
    secondary_minutes = parse_timeframe(settings.SECONDARY_TIMEFRAME)
    
    if primary_minutes > 0:
        expected_primary = (primary_days * 24 * 60) / primary_minutes
        print(f"   Expected primary candles: ~{expected_primary:.0f}")
        print(f"   Actual primary candles: {len(df_primary)}")
        coverage_pct = (len(df_primary) / expected_primary) * 100
        print(f"   Coverage: {coverage_pct:.1f}%")
    
    # Step 6: Initialize strategy (SAME as live)
    print(f"\n  Initializing strategy...")
    strategy = MultiTimeframeStrategy(
        fast_sma_period=settings.STRATEGY_PARAMS['fast_sma_period'],
        slow_sma_period=settings.STRATEGY_PARAMS['slow_sma_period'],
        trend_sma_period=settings.STRATEGY_PARAMS['trend_sma_period'],
        rsi_period=settings.STRATEGY_PARAMS['rsi_period'],
        rsi_overbought=settings.STRATEGY_PARAMS['rsi_overbought'],
        rsi_oversold=settings.STRATEGY_PARAMS['rsi_oversold']
    )
    
    # Step 7: Run backtest
    print(f"\n Running backtest...")
    backtest_engine = BacktestEngine(
        strategy=strategy,
        initial_capital=settings.INITIAL_CAPITAL,
        commission=settings.COMMISSION,
        client=client
    )
    
    trades_df = backtest_engine.run(df_primary, df_secondary, settings.SYMBOL)
    
    print(f" Backtest complete!")
    print(f"   Total trades: {len(trades_df)}")
    
    # Step 8: Save trades
    print(f"\n Saving backtest trades...")
    backtest_engine.save_trades(str(settings.BACKTEST_TRADES_FILE))
    print(f" Saved to: {settings.BACKTEST_TRADES_FILE}")
    
    # Step 9: Print summary
    print("\n" + "="*60)
    backtest_engine.print_summary()
    print("="*60)

    if not trades_df.empty:
        # Create a fixed version where BUY rows have exit info
        trades_fixed = []
        
        buy_trades = {}  # Store BUY trades by their index
        
        for idx, row in trades_df.iterrows():
            if row['side'] == 'BUY':
                # Store BUY trade
                buy_trades[idx] = row.to_dict()
                trades_fixed.append(row.to_dict())
            elif row['side'] == 'SELL':
                # Find matching BUY trade
                sell_time = row['timestamp']
                matching_buy_idx = None
                
                # Find the most recent BUY before this SELL
                for buy_idx, buy_trade in reversed(list(buy_trades.items())):
                    if buy_trade['timestamp'] < sell_time:
                        matching_buy_idx = buy_idx
                        break
                
                if matching_buy_idx is not None:
                    # Update the BUY trade with exit info from SELL
                    buy_trade_dict = trades_fixed[matching_buy_idx]
                    buy_trade_dict['exit_time'] = row.get('exit_time', row['timestamp'])
                    buy_trade_dict['exit_price'] = row.get('exit_price', row.get('entry_price'))
                    buy_trade_dict['pnl'] = row.get('pnl')
                    buy_trade_dict['return_pct'] = row.get('return_pct')
                    
                    # Remove from pending buys
                    del buy_trades[matching_buy_idx]
                
                # Add SELL row
                trades_fixed.append(row.to_dict())
        
        # Save fixed version
        trades_df_fixed = pd.DataFrame(trades_fixed)
        backtest_engine.save_trades(str(settings.BACKTEST_TRADES_FILE))
 
    print("\n" + "="*60)
    print(" BACKTEST VALIDATION COMPLETE")
    print("="*60)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n Backtest failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)