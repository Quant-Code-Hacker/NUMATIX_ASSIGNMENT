"""
Binance Testnet API client wrapper.
Handles all interactions with Binance Testnet REST API.
"""
import time
import hmac
import hashlib
import requests
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)


class BinanceClient:
    """Wrapper for Binance Testnet REST API."""
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        """
        Initialize Binance client.
        
        Args:
            api_key: Binance API key
            api_secret: Binance API secret
            testnet: Use testnet (default: True)
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = 'https://testnet.binance.vision/api' if testnet else 'https://api.binance.com/api'
        self.session = requests.Session()
        self.session.headers.update({
            'X-MBX-APIKEY': api_key
        })
        logger.info(f"Initialized Binance client (testnet={testnet})")
    
    def _generate_signature(self, params: Dict) -> str:
        """Generate HMAC SHA256 signature for authenticated requests."""
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def get_server_time(self) -> int:
        """Get Binance server time."""
        response = self.session.get(f"{self.base_url}/v3/time")
        response.raise_for_status()
        return response.json()['serverTime']
    
    def get_klines(self, symbol: str, interval: str, 
                   start_time: Optional[int] = None,
                   end_time: Optional[int] = None,
                   limit: int = 1000) -> pd.DataFrame:
        """
        Fetch historical kline/candlestick data.
        
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            interval: Timeframe (1m, 5m, 15m, 1h, 4h, 1d, etc.)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            limit: Number of candles to fetch (max 1000)
        
        Returns:
            DataFrame with OHLCV data
        """
        endpoint = f"{self.base_url}/v3/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        logger.debug(f"Fetching klines: {symbol} {interval} limit={limit}")
        response = self.session.get(endpoint, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])
        
        # Convert types
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        
        df.set_index('timestamp', inplace=True)
        
        logger.info(f"Fetched {len(df)} candles for {symbol} {interval}")
        return df[['open', 'high', 'low', 'close', 'volume']]
    
    def get_account_info(self) -> Dict:
        """Get account information."""
        endpoint = f"{self.base_url}/v3/account"
        params = {
            'timestamp': int(time.time() * 1000)
        }
        params['signature'] = self._generate_signature(params)
        
        response = self.session.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def create_order(self, symbol: str, side: str, order_type: str,
                    quantity: float, price: Optional[float] = None,
                    time_in_force: str = 'GTC') -> Dict:
        """
        Place a new order.
        
        Args:
            symbol: Trading pair
            side: 'BUY' or 'SELL'
            order_type: 'LIMIT', 'MARKET', etc.
            quantity: Order quantity
            price: Order price (required for LIMIT orders)
            time_in_force: Time in force (GTC, IOC, FOK)
        
        Returns:
            Order response
        """
        endpoint = f"{self.base_url}/v3/order"
        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'quantity': quantity,
            'timestamp': int(time.time() * 1000)
        }
        
        if order_type == 'LIMIT':
            if price is None:
                raise ValueError("Price required for LIMIT orders")
            params['price'] = price
            params['timeInForce'] = time_in_force
        
        params['signature'] = self._generate_signature(params)
        
        logger.info(f"Creating order: {side} {quantity} {symbol} @ {price}")
        response = self.session.post(endpoint, params=params)
        response.raise_for_status()
        
        order_data = response.json()
        logger.info(f"Order created: {order_data['orderId']}")
        return order_data
    
    def get_order_status(self, symbol: str, order_id: int) -> Dict:
        """Get order status."""
        endpoint = f"{self.base_url}/v3/order"
        params = {
            'symbol': symbol,
            'orderId': order_id,
            'timestamp': int(time.time() * 1000)
        }
        params['signature'] = self._generate_signature(params)
        
        response = self.session.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        """Get all open orders."""
        endpoint = f"{self.base_url}/v3/openOrders"
        params = {
            'timestamp': int(time.time() * 1000)
        }
        if symbol:
            params['symbol'] = symbol
        
        params['signature'] = self._generate_signature(params)
        
        response = self.session.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def cancel_order(self, symbol: str, order_id: int) -> Dict:
        """Cancel an order."""
        endpoint = f"{self.base_url}/v3/order"
        params = {
            'symbol': symbol,
            'orderId': order_id,
            'timestamp': int(time.time() * 1000)
        }
        params['signature'] = self._generate_signature(params)
        
        logger.info(f"Cancelling order: {order_id}")
        response = self.session.delete(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_symbol_info(self, symbol: str) -> Dict:
        """Get symbol trading rules and info."""
        endpoint = f"{self.base_url}/v3/exchangeInfo"
        response = self.session.get(endpoint)
        response.raise_for_status()
        
        data = response.json()
        for s in data['symbols']:
            if s['symbol'] == symbol:
                return s
        
        raise ValueError(f"Symbol {symbol} not found")
