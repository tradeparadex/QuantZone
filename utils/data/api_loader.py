from typing import Dict, List, Tuple, Callable, Any
import duckdb
import pandas as pd
from paradex_py.api.api_client import ParadexApiClient
from paradex_py.environment import PROD
from pathlib import Path
import backoff
import httpx
from datetime import datetime
from dataclasses import dataclass
import logging

# Create module logger with NullHandler
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

@dataclass
class DataConfig:
    """Configuration for Paradex data operations"""
    client: ParadexApiClient = ParadexApiClient(env=PROD)
    base_dir: Path = Path("data/paradex")

    def mkdirs(self):
        self.base_dir.mkdir(exist_ok=True, parents=True)

def unpack(results: Dict) -> List:
    """Unpack results"""
    return results['results']

def unpack_pagination(results: Dict) -> Tuple[List, str, str]:
    """Unpack pagination results""" 
    data = results['results']
    next = results['next']
    prev = results['prev']
    return data, next, prev


def retry_fetch(func: Callable) -> Callable:
    """Wrapper to add exponential backoff retries for Paradex fetch API calls"""
    @backoff.on_exception(
        backoff.expo,
        (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.HTTPError),
        max_time=300,
        on_backoff=lambda details: logger.warning(f"Retrying {func.__name__}... (attempt {details['tries']})")
    )
    def wrapper(self, *args, **kwargs):
        return func(self, *args, **kwargs)
    return wrapper

# Patch ParadexApiClient fetch methods with retries
ParadexApiClient.fetch_trades = retry_fetch(ParadexApiClient.fetch_trades)
ParadexApiClient.fetch_markets = retry_fetch(ParadexApiClient.fetch_markets)
ParadexApiClient.fetch_markets_summary = retry_fetch(ParadexApiClient.fetch_markets_summary)

def load_trades_market(market: str, start_at: str = None, end_at: str = None, data_config: DataConfig = DataConfig()):
    """Load trades for a specific market from Paradex API and save to parquet files"""
    params = {'market': market, 'page_size': 1000}
    if start_at:
        params['start_at'] = start_at
    if end_at:
        params['end_at'] = end_at
    trades, next, prev = unpack_pagination(data_config.client.fetch_trades(params))
    logger.info(f"Loaded {len(trades)} trades")
    df = pd.DataFrame(trades)
    counter = 1
    chunk_size = 100_000 # roughly 2MB
    
    while next:
        logger.info(f"Loading page {counter} for {market}")
        params['cursor'] = next
        trades, next, prev = unpack_pagination(data_config.client.fetch_trades(params))
        logger.info(f"Loaded {len(trades)} trades")
        df = pd.concat([df, pd.DataFrame(trades)])
        counter += 1
        
        if len(df) >= chunk_size:
            min_date = df['created_at'].min()
            max_date = df['created_at'].max()
            logger.debug(f"Date range: {min_date} to {max_date}")
            
            path = data_config.base_dir / "trades" / market / f"{min_date}-{max_date}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(str(path))
            logger.info(f"Saved {len(df)} trades to {path}")
            df = pd.DataFrame()
    
    # Save any remaining rows
    if not df.empty:
        min_date = df['created_at'].min()
        max_date = df['created_at'].max()
        logger.debug(f"Date range: {min_date} to {max_date}")
        
        path = data_config.base_dir / "trades" / market / f"{min_date}-{max_date}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(str(path))
    logger.info(f"Saved {len(df)} trades to {path}")

def load_trades(data_config: DataConfig = DataConfig()):
    """Load all trades from Paradex API for each market"""
    data_config.mkdirs()
    markets = duckdb.query(f"select symbol from '{data_config.base_dir}/markets.parquet' order by symbol").df()
    for market in markets['symbol']:
        logger.info(f"Loading trades for {market}")
        # Get latest trade date for this market
        try:
            latest = duckdb.query(
                f"select max(created_at) as max_date from '{data_config.base_dir}/trades/{market}/*.parquet'"
            ).df()
            start_at = None if latest['max_date'].iloc[0] is None else latest['max_date'].iloc[0]
            logger.info(f"Latest trade date: {start_at} / {datetime.fromtimestamp(start_at/1000) if start_at else 'None'}")
        except duckdb.IOException:
            logger.warning(f"No existing trades found for {market}")
            start_at = None
            
        load_trades_market(market, start_at, data_config=data_config)

def load_markets_summary_single(market: str, start_at: int | None = None, end_at: int | None = None, data_config: DataConfig = DataConfig()):
    """Load markets summary data for a specific market and save to parquet files"""
    # Set default start_at to 24h ago if None
    if start_at is None:
        start_at = int(datetime.now().timestamp() * 1000) - (24 * 60 * 60 * 1000)  # 24h ago
    
    range_end = end_at if end_at else start_at + (24 * 60 * 60 * 1000)  # 24h from start
    
    params = {
        'market': market, 
        'start': start_at,
        'end': start_at + (60 * 60 * 1000)  # 1h chunks
    }
    
    logger.info(f"Loading markets summary {params}")
    markets = unpack(data_config.client.fetch_markets_summary(params))
    logger.info(f"Loaded {len(markets)} markets summary")
    df = pd.DataFrame(markets)
    counter = 1
    chunk_size = 10_000
    max_timestamp = df['created_at'].max()

    while max_timestamp < range_end:
        logger.info(f"Loading page {counter} for {market}")
        params['start'] = max_timestamp
        params['end'] = max_timestamp + (60 * 60 * 1000)
        markets = unpack(data_config.client.fetch_markets_summary(params))
        logger.info(f"Loaded {len(markets)} markets summary")
        df = pd.concat([df, pd.DataFrame(markets)])
        counter += 1
        
        if len(df) >= chunk_size:
            min_timestamp = df['created_at'].min()
            max_timestamp = df['created_at'].max()
            logger.debug(f"Date range: {min_timestamp} to {max_timestamp}")
            
            path = data_config.base_dir / "markets_summary" / market / f"{min_timestamp}-{max_timestamp}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(str(path))
            logger.info(f"Saved {len(df)} markets summary to {path}")
            df = pd.DataFrame()

    # Save any remaining rows
    if not df.empty:
        min_timestamp = df['created_at'].min()
        max_timestamp = df['created_at'].max()
        logger.debug(f"Date range: {min_timestamp} to {max_timestamp}")
        
        path = data_config.base_dir / "markets_summary" / market / f"{min_timestamp}-{max_timestamp}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(str(path))
    logger.info(f"Saved {len(df)} markets summary to {path}")


def load_markets_summary(data_config: DataConfig = DataConfig()):
    """Load market summary data for all available markets from Paradex API"""
    data_config.mkdirs()
    markets = duckdb.query(f"select symbol from '{data_config.base_dir}/markets.parquet' order by symbol").df()
    for market in markets['symbol']:
        logger.info(f"Loading markets summary for {market}")
        try:
            latest = duckdb.query(
                f"select max(created_at) as max_date from '{data_config.base_dir}/markets_summary/{market}/*.parquet'"
            ).df()
            start_at = None if latest['max_date'].iloc[0] is None else latest['max_date'].iloc[0]
            logger.info(f"Latest markets summary date: {start_at} / {datetime.fromtimestamp(start_at/1000) if start_at else 'None'}")
        except duckdb.IOException:
            logger.warning(f"No existing markets summary found for {market}")
            start_at = None
            
        load_markets_summary_single(market, start_at, data_config=data_config)


def load_markets(data_config: DataConfig = DataConfig()):
    """Load markets data from Paradex API and save to parquet file"""
    markets = data_config.client.fetch_markets()
    df = pd.DataFrame(unpack(markets))
    
    # Create data directory if it doesn't exist
    data_config.mkdirs()
    
    df.to_parquet(str(data_config.base_dir / "markets.parquet"))
    logger.info(f"Saved {len(df)} markets to {data_config.base_dir / 'markets.parquet'}")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    config = DataConfig()
    load_markets(config)
    load_markets_summary(config)
    # load_trades(config)