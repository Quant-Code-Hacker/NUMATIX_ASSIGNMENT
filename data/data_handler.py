
# Data handler for fetching and processing market data 
# Manages multi-timeframe data synchronization.
# Data fetched for  BTCUSDT
import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple, Optional
from data.binance_client import BinanceClient
from utils.logger import get_logger

logger = get_logger(__name__)


class DataHandler:
    
    def __init__(self, client: BinanceClient, symbol: str):
        self.client = client
        self.symbol = symbol
        logger.info(f"Initialized DataHandler for {symbol}")
    
    def fetch_historical_data(self, 
                             timeframe: str,
                             start_date: str,
                             end_date: str) -> pd.DataFrame:
        logger.info(f"Fetching historical data: {self.symbol} {timeframe} "
                   f"from {start_date} to {end_date}")
        
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            start_ms = int(start_dt.timestamp() * 1000)
            end_ms = int(end_dt.timestamp() * 1000)
        except Exception as e:
            logger.error(f"Invalid date format: {e}")
            return pd.DataFrame()
        
        all_data = []
        current_start = start_ms
        
        # Binance limits to 1000 candles per request
        while current_start < end_ms:
            try:
                df = self.client.get_klines(
                    symbol=self.symbol,
                    interval=timeframe,
                    start_time=current_start,
                    end_time=end_ms,
                    limit=1000
                )
                
                if df.empty:
                    logger.warning(f"No more data available from {datetime.fromtimestamp(current_start/1000)}")
                    break
                
                all_data.append(df)
                
                # Update start time for next batch
                last_timestamp = df.index[-1]
                current_start = int(last_timestamp.timestamp() * 1000) + 1
                
                logger.debug(f"Fetched batch: {len(df)} candles")
                
            except Exception as e:
                logger.error(f"Error fetching data batch: {e}")
                break
        
        if not all_data:
            logger.warning("No data fetched")
            return pd.DataFrame()
        
        # Combine all batches
        result = pd.concat(all_data)
        result = result[~result.index.duplicated(keep='first')]
        result.sort_index(inplace=True)
        
        logger.info(f"Total candles fetched: {len(result)}")
        return result
    
    def fetch_latest_data(self, timeframe: str, limit: int = 100) -> pd.DataFrame:
        """
        Fetch latest data for live trading.
        
        Args:
            timeframe: Candle interval
            limit: Number of recent candles
        
        Returns:
            DataFrame with recent OHLCV data
        """
        logger.debug(f"Fetching latest {limit} candles for {timeframe}")
        
        df = self.client.get_klines(
            symbol=self.symbol,
            interval=timeframe,
            limit=limit
        )
        
        return df
    
    def get_multi_timeframe_data(self,
                                primary_tf: str,
                                secondary_tf: str,
                                start_date: str,
                                end_date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Fetch data for multiple timeframes with proper alignment.
        
        Args:
            primary_tf: Primary timeframe (e.g., '15m')
            secondary_tf: Secondary timeframe (e.g., '1h')
            start_date: Start date
            end_date: End date
        
        Returns:
            Tuple of (primary_df, secondary_df)
        """
        logger.info(f"Fetching multi-timeframe data: {primary_tf} and {secondary_tf}")
        
        # Fetch both timeframes
        df_primary = self.fetch_historical_data(primary_tf, start_date, end_date)
        df_secondary = self.fetch_historical_data(secondary_tf, start_date, end_date)
        
        if df_primary.empty or df_secondary.empty:
            raise ValueError("Failed to fetch data for one or both timeframes")
        
        logger.info(f"Primary TF: {len(df_primary)} candles, "
                   f"Secondary TF: {len(df_secondary)} candles")
        
        return df_primary, df_secondary
    
    def get_latest_multi_timeframe_data(self,
                                       primary_tf: str,
                                       secondary_tf: str,
                                       primary_limit: int = 100,
                                       secondary_limit: int = 100) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Fetch latest data for multiple timeframes (live trading).
        
        Args:
            primary_tf: Primary timeframe
            secondary_tf: Secondary timeframe
            primary_limit: Number of primary candles
            secondary_limit: Number of secondary candles
        
        Returns:
            Tuple of (primary_df, secondary_df)
        """
        df_primary = self.fetch_latest_data(primary_tf, primary_limit)
        df_secondary = self.fetch_latest_data(secondary_tf, secondary_limit)
        
        return df_primary, df_secondary
    
    def resample_timeframe(self, df: pd.DataFrame, target_timeframe: str) -> pd.DataFrame:
        """
        Resample data to a different timeframe.
        
        Args:
            df: Source DataFrame
            target_timeframe: Target timeframe
        
        Returns:
            Resampled DataFrame
        """
        # Map timeframe strings to pandas offset aliases
        tf_map = {
            '1m': '1T', '5m': '5T', '15m': '15T', '30m': '30T',
            '1h': '1H', '4h': '4H', '1d': '1D'
        }
        
        if target_timeframe not in tf_map:
            raise ValueError(f"Unsupported timeframe: {target_timeframe}")
        
        rule = tf_map[target_timeframe]
        
        resampled = df.resample(rule).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        logger.debug(f"Resampled to {target_timeframe}: {len(resampled)} candles")
        return resampled
    
    def add_indicators(self, df: pd.DataFrame,
                      sma_periods: list = None,
                      rsi_period: int = None) -> pd.DataFrame:
        """
        Add technical indicators to DataFrame.
        
        Args:
            df: Source DataFrame
            sma_periods: List of SMA periods to calculate
            rsi_period: RSI period
        
        Returns:
            DataFrame with indicators added
        """
        df = df.copy()
        
        # Add SMAs
        if sma_periods:
            for period in sma_periods:
                df[f'sma_{period}'] = df['close'].rolling(window=period).mean()
                logger.debug(f"Added SMA {period}")
        
        # Add RSI
        if rsi_period:
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=rsi_period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_period).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            logger.debug(f"Added RSI {rsi_period}")
        
        return df