"""
Script to run live trading on Binance Testnet.
Uses the SAME strategy as backtesting to ensure parity.
"""
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from data.binance_client import BinanceClient
from strategy.multi_timeframe_strategy import MultiTimeframeStrategy
from execution.live_engine import LiveTradingEngine
from config import settings
from utils.logger import setup_logger_for_module

# Setup logging
logger = setup_logger_for_module('live_trading', settings.LOGS_DIR, prefix='')


def main():
    """Run live trading on Binance Testnet."""
    logger.info("="*60)
    logger.info("STARTING LIVE TRADING")
    logger.info("="*60)
    
    # Check API keys
    if not settings.BINANCE_API_KEY or settings.BINANCE_API_KEY == 'your_api_key_here':
        logger.error("Binance Testnet API key not configured!")
        logger.error("Please set BINANCE_TESTNET_API_KEY in config/settings.py or environment")
        logger.error("Get keys from: https://testnet.binance.vision/")
        sys.exit(1)
    
    if not settings.BINANCE_API_SECRET or settings.BINANCE_API_SECRET == 'your_api_secret_here':
        logger.error("Binance Testnet API secret not configured!")
        logger.error("Please set BINANCE_TESTNET_API_SECRET in config/settings.py or environment")
        sys.exit(1)
    
    # Initialize Binance Testnet client
    logger.info("Connecting to Binance Testnet...")
    client = BinanceClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        testnet=True
    )
    
    # Verify connection
    try:
        account = client.get_account_info()
        logger.info("Successfully connected to Binance Testnet")
        
        # Show balances
        balances = {b['asset']: float(b['free']) 
                   for b in account['balances'] 
                   if float(b['free']) > 0}
        if balances:
            logger.info(f"Account balances: {balances}")
        else:
            logger.warning("No balances found - you may need to fund your testnet account")
    except Exception as e:
        logger.error(f"Failed to connect to Binance Testnet: {e}")
        logger.error("Please check your API keys and internet connection")
        sys.exit(1)
    
    # Initialize strategy (SAME as backtest)
    strategy = MultiTimeframeStrategy(
        fast_sma_period=settings.STRATEGY_PARAMS['fast_sma_period'],
        slow_sma_period=settings.STRATEGY_PARAMS['slow_sma_period'],
        trend_sma_period=settings.STRATEGY_PARAMS['trend_sma_period'],
        rsi_period=settings.STRATEGY_PARAMS['rsi_period'],
        rsi_overbought=settings.STRATEGY_PARAMS['rsi_overbought'],
        rsi_oversold=settings.STRATEGY_PARAMS['rsi_oversold']
    )
    
    logger.info(f"Strategy initialized: {strategy}")
    logger.info(f"Symbol: {settings.SYMBOL}")
    logger.info(f"Primary TF: {settings.PRIMARY_TIMEFRAME}, Secondary TF: {settings.SECONDARY_TIMEFRAME}")
    logger.info(f"Trade Quantity: {settings.TRADE_QUANTITY} {settings.SYMBOL}")
    logger.info(f"Check Interval: {settings.LIVE_CHECK_INTERVAL} seconds")
    
    # Initialize live trading engine
    engine = LiveTradingEngine(
        client=client,
        strategy=strategy,
        symbol=settings.SYMBOL,
        primary_timeframe=settings.PRIMARY_TIMEFRAME,
        secondary_timeframe=settings.SECONDARY_TIMEFRAME,
        trade_quantity=settings.TRADE_QUANTITY,
        check_interval=settings.LIVE_CHECK_INTERVAL
    )
    
    logger.info("\n" + "="*60)
    logger.info("LIVE TRADING STARTED")
    logger.info("="*60 + "\n")
    
    try:
        # Start trading
        # TO LIMIT NUMBER OF TRADES: Change engine.start() to engine.start(max_trades=3)
        # Example: engine.start(max_trades=3) will stop after 3 complete trades (3 BUY + 3 SELL)
        # Currently: No limit (runs until Ctrl+C)
        engine.start()  # <-- Add max_trades=3 here if you want to limit trades
    except KeyboardInterrupt:
        logger.info("\nStopping live trading...")
    except Exception as e:
        logger.error(f"Live trading error: {e}", exc_info=True)
    finally:
        engine.stop()
        
        # Save trades
        print(f"\nSaving trades to {settings.LIVE_TRADES_FILE}...")
        logger.info(f"Saving trades to {settings.LIVE_TRADES_FILE}...")
        engine.save_trades(str(settings.LIVE_TRADES_FILE))
        
        # Print summary
        print("\n" + "="*60)
        engine.print_summary()
        print("="*60)
        
        logger.info("="*60)
        logger.info("LIVE TRADING COMPLETE")
        logger.info("="*60)
        logger.info(f"Trades saved to: {settings.LIVE_TRADES_FILE}")
        
        # Verify file was created
        if settings.LIVE_TRADES_FILE.exists():
            print(f"File verified: {settings.LIVE_TRADES_FILE}")
        else:
            print(f"WARNING: File not found at {settings.LIVE_TRADES_FILE}")
            logger.warning(f"File not found at {settings.LIVE_TRADES_FILE}")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"Live trading failed: {e}", exc_info=True)
        sys.exit(1)