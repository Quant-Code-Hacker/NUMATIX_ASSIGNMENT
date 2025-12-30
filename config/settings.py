
import os
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent

# Binance Testnet API Configuration
BINANCE_API_KEY = os.getenv('BINANCE_TESTNET_API_KEY', 'ADD HERE')
BINANCE_API_SECRET = os.getenv('BINANCE_TESTNET_API_SECRET', 'ADD HERE')
BINANCE_TESTNET_URL = 'https://testnet.binance.vision/api'


SYMBOL = 'BTCUSDT'
PRIMARY_TIMEFRAME = '1m'    
SECONDARY_TIMEFRAME = '3m'  
TRADE_QUANTITY = 0.001       

STRATEGY_PARAMS = {
    'fast_sma_period': 5,
    'slow_sma_period': 10,
    'trend_sma_period': 50,
    'rsi_period': 14,
    'rsi_overbought': 80,
    'rsi_oversold': 30,
}

BACKTEST_START_DATE = '2024-11-01' 
BACKTEST_END_DATE = '2024-12-27'
INITIAL_CAPITAL = 10000.0
COMMISSION = 0.0 
USE_MAINNET_FOR_BACKTEST = True 
CANDLE_SECONDS = 180

LIVE_CHECK_INTERVAL = 1  
MAX_POSITION_SIZE = 0.01  


OUTPUT_DIR = BASE_DIR / 'output'
BACKTEST_TRADES_FILE = OUTPUT_DIR / 'backtest_trades.csv'
LIVE_TRADES_FILE = OUTPUT_DIR / 'live_trades.csv'
LOGS_DIR = OUTPUT_DIR / 'logs'


OUTPUT_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)


LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


PRICE_TOLERANCE = 0.02  
TIME_TOLERANCE = 300   