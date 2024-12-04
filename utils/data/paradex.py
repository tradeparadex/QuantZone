from typing import Dict, List, Tuple, Callable, Any
import duckdb
import pandas as pd
from paradex_py.api.api_client import ParadexApiClient
from paradex_py.environment import PROD
from pathlib import Path
import backoff
import httpx
from datetime import datetime

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
        on_backoff=lambda details: print(f"Retrying {func.__name__}... (attempt {details['tries']})")
    )
    def wrapper(self, *args, **kwargs):
        return func(self, *args, **kwargs)
    return wrapper

# Patch ParadexApiClient fetch methods with retries
ParadexApiClient.fetch_trades = retry_fetch(ParadexApiClient.fetch_trades)
ParadexApiClient.fetch_markets = retry_fetch(ParadexApiClient.fetch_markets)
ParadexApiClient.fetch_markets_summary = retry_fetch(ParadexApiClient.fetch_markets_summary)

def load_trades_market(paradex: ParadexApiClient, market: str, start_at: str = None, end_at: str = None):
    """Load trades for a specific market from Paradex API and save to parquet files
    
    Fetches trades in batches using pagination and saves to parquet files when the batch size
    reaches chunk_size. Files are saved under data/trades/{market}/ with filenames containing 
    the date range of trades in that file.

    Args:
        paradex: Initialized ParadexApiClient instance
        market: Market symbol to fetch trades for (e.g. "ETH-USD-PERP")
        start_at: Optional timestamp to fetch trades from (milliseconds since epoch)
        end_at: Optional timestamp to fetch trades until (milliseconds since epoch)
    
    Returns:
        None. Saves trades to parquet files under data/trades/{market}/
        
    Example:
        >>> paradex = ParadexApiClient(PROD)
        >>> load_trades_market(paradex, "ETH-USD-PERP", start_at="1672531200000")
    """
    params = {'market': market, 'page_size': 1000}
    if start_at:
        params['start_at'] = start_at
    if end_at:
        params['end_at'] = end_at
    trades, next, prev = unpack_pagination(paradex.fetch_trades(params))
    print(f"Loaded {len(trades)} trades")
    df = pd.DataFrame(trades)
    counter = 1
    chunk_size = 100_000 # roughly 2MB
    
    while next:
        print(f"Loading {counter} page for {market}")
        params['cursor'] = next
        trades, next, prev = unpack_pagination(paradex.fetch_trades(params))
        print(f"Loaded {len(trades)} trades")
        df = pd.concat([df, pd.DataFrame(trades)])
        counter += 1
        
        if len(df) >= chunk_size:
            min_date = df['created_at'].min()
            max_date = df['created_at'].max()
            print(f"Date range: {min_date} to {max_date}")
            
            path = Path(f'data/trades/{market}/{min_date}-{max_date}.parquet')
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(str(path))
            print(f"Saved {len(df)} trades to {path}")
            
            # Reset dataframe
            df = pd.DataFrame()
    
    # Save any remaining rows
    if not df.empty:
        min_date = df['created_at'].min()
        max_date = df['created_at'].max()
        print(f"Date range: {min_date} to {max_date}")
        
        path = Path(f'data/trades/{market}/{min_date}-{max_date}.parquet')
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(str(path))
    print(f"Saved {len(df)} trades to {path}")

def load_trades(paradex:ParadexApiClient):
    """Load all trades from Paradex API for each market
    
    Loads trades incrementally by:
    1. Getting list of markets from markets.parquet
    2. For each market:
        - Finds latest trade timestamp from existing parquet files
        - Loads all trades since that timestamp using load_trades_market()
        - If no existing trades, loads complete history
    
    Args:
        paradex: Initialized ParadexApiClient instance
        
    Returns:
        None. Saves trades to parquet files under data/trades/{market}/
    """
    markets = duckdb.query("select symbol from 'data/markets.parquet' order by symbol").df()
    for market in markets['symbol']:
        print(f"Loading trades for {market}")
        # Get latest trade date for this market
        try:
            latest = duckdb.query(f"select max(created_at) as max_date from 'data/trades/{market}/*.parquet'").df()
            start_at = None if latest['max_date'].iloc[0] is None else latest['max_date'].iloc[0]
            print(f"Latest trade date: {start_at} / {datetime.fromtimestamp(start_at/1000) if start_at else 'None'}")
        except duckdb.IOException:
            print(f"No existing trades found for {market}")
            start_at = None
            
        load_trades_market(paradex, market, start_at)

def load_markets_summary_single(paradex:ParadexApiClient, market:str, start_at: int | None = None, end_at: int | None = None):
    """Load markets summary data for a specific market and save to parquet files
    
    Loads market summary data incrementally in 1-hour chunks and saves to parquet files
    when chunk_size is reached. Data is saved under data/markets_summary/{market}/.
    
    Args:
        paradex: Initialized ParadexApiClient instance
        market: Market symbol to load data for (e.g. "ETH-USD-PERP")
        start_at: Optional start timestamp in milliseconds. If None, defaults to 24h ago
        end_at: Optional end timestamp in milliseconds. If None, defaults to 24h after start_at
        
    Returns:
        None. Saves market summary data to parquet files.
        
    File format:
        data/markets_summary/{market}/{min_timestamp}-{max_timestamp}.parquet
        Where timestamps are in milliseconds
    """
    # Set default start_at to 24h ago if None
    if start_at is None:
        start_at = int(datetime.now().timestamp() * 1000) - (24 * 60 * 60 * 1000)  # 24h ago
    
    range_end = end_at if end_at else start_at + (24 * 60 * 60 * 1000)  # 24h from start
    
    params = {
        'market': market, 
        'start': start_at,
        'end': start_at + (60 * 60 * 1000)  # 1h chunks
    }
    
    print(f"Loading markets summary {params}")
    markets = unpack(paradex.fetch_markets_summary(params))
    print(f"Loaded {len(markets)} markets summary")
    df = pd.DataFrame(markets)
    counter = 1
    chunk_size = 10_000
    max_timestamp = df['created_at'].max()

    while max_timestamp < range_end:
        print(f"Loading {counter} page for {market}")
        params['start'] = max_timestamp
        params['end'] = max_timestamp + (60 * 60 * 1000)
        markets = unpack(paradex.fetch_markets_summary(params))
        print(f"Loaded {len(markets)} markets summary")
        df = pd.concat([df, pd.DataFrame(markets)])
        counter += 1
        
        if len(df) >= chunk_size:
            min_timestamp = df['created_at'].min()
            max_timestamp = df['created_at'].max()
            print(f"Date range: {min_timestamp} to {max_timestamp}")
            
            path = Path(f'data/markets_summary/{market}/{min_timestamp}-{max_timestamp}.parquet')
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(str(path))
            print(f"Saved {len(df)} markets summary to {path}")
            
            # Reset dataframe
            df = pd.DataFrame()

    # Save any remaining rows
    if not df.empty:
        min_timestamp = df['created_at'].min()
        max_timestamp = df['created_at'].max()
        print(f"Date range: {min_timestamp} to {max_timestamp}")
        
        path = Path(f'data/markets_summary/{market}/{min_timestamp}-{max_timestamp}.parquet')
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(str(path))
    print(f"Saved {len(df)} markets summary to {path}")


def load_markets_summary(paradex: ParadexApiClient):
    """Load market summary data for all available markets from Paradex API
    
    Fetches market summary data for each market listed in data/markets.parquet.
    For each market, it checks the latest timestamp in existing data files and
    fetches new data from that point onwards using load_markets_summary_single().
    
    Args:
        paradex: Initialized ParadexApiClient instance
    
    Returns:
        None. Saves market summary data to parquet files under data/markets_summary/{market}/
        
    Example:
        >>> paradex = ParadexApiClient(PROD)
        >>> load_markets_summary(paradex)
    """
    markets = duckdb.query("select symbol from 'data/markets.parquet' order by symbol").df()
    for market in markets['symbol']:
        print(f"Loading markets summary for {market}")
        # Get latest trade date for this market
        try:
            latest = duckdb.query(f"select max(created_at) as max_date from 'data/markets_summary/{market}.parquet'").df()
            start_at = None if latest['max_date'].iloc[0] is None else latest['max_date'].iloc[0]
            print(f"Latest markets summary date: {start_at} / {datetime.fromtimestamp(start_at/1000) if start_at else 'None'}")
        except duckdb.IOException:
            print(f"No existing markets summary found for {market}")
            start_at = None
            
        load_markets_summary_single(paradex, market, start_at)


def load_markets(paradex: ParadexApiClient):
    """Load markets data from Paradex API and save to parquet file
    
    Fetches the list of available markets from the Paradex API and saves it to a
    parquet file. This provides the base market data used by other functions to
    load market-specific data.

    Args:
        paradex: Initialized ParadexApiClient instance
    
    Returns:
        None. Saves markets data to data/markets.parquet
        
    Example:
        >>> paradex = ParadexApiClient(PROD)
        >>> load_markets(paradex)
    """
    markets = paradex.fetch_markets()
    paradex.fetch_funding_data
    df = pd.DataFrame(unpack(markets))
    
    # Create data directory if it doesn't exist
    Path('data').mkdir(exist_ok=True)
    
    df.to_parquet('data/markets.parquet')
    print(f"Saved {len(df)} markets to data/markets.parquet")

if __name__ == "__main__":
    paradex = ParadexApiClient(env=PROD)
    load_markets(paradex)
    load_markets_summary(paradex)
    # load_trades(paradex)