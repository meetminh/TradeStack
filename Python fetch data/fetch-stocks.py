import yfinance as yf
import pandas as pd
from typing import Set, Dict, List, Tuple
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import psycopg2
from functools import partial
import logging
from psycopg2.extras import execute_batch

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Simple dataclass-like configuration


class DBConfig:
    def __init__(self, host='host.docker.internal', port=8812):
        self.host = host
        self.port = port
        self.dbname = 'qdb'
        self.user = 'admin'
        self.password = 'quest'


def get_sp500_tickers() -> Set[str]:
    """Fetch S&P 500 tickers with basic retry logic"""
    try:
        table = pd.read_html(
            'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        tickers = set(table[0]['Symbol'].str.replace('.', '-'))
        extra_tickers = {'SPY', 'QQQ', 'VOOG', 'SHY', 'TLT', 'GLD', '^VIX'}
        return tickers.union(extra_tickers)
    except Exception as e:
        logger.error(f"Failed to fetch tickers: {e}")
        raise


def clean_and_validate_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Clean and validate stock data in one pass"""
    if df.empty:
        logger.warning(f"No data for {ticker}")
        return pd.DataFrame()

    # Remove NaN values and log
    initial_len = len(df)
    df = df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'])
    if len(df) < initial_len:
        logger.warning(f"Removed {initial_len - len(df)
                                  } rows with NaN values from {ticker}")

    # Basic validity checks
    invalid_mask = (
        (df['Low'] > df['High']) |
        (df['Close'] < 0) |
        (df['Volume'] < 0)
    )
    df = df[~invalid_mask]

    if df.empty:
        logger.warning(f"No valid data remaining for {ticker}")

    return df


def fetch_ticker_data(ticker: str) -> Tuple[str, pd.DataFrame]:
    """Fetch and validate data for a single ticker"""
    try:
        time.sleep(0.1)  # Basic rate limiting
        df = yf.Ticker(ticker).history(period="max")
        df = clean_and_validate_data(df, ticker)
        if not df.empty:
            df.reset_index(inplace=True)
            logger.info(f"Fetched {len(df)} rows for {ticker}")
        return ticker, df
    except Exception as e:
        logger.error(f"Error fetching {ticker}: {e}")
        return ticker, pd.DataFrame()


def insert_batch_data(batch_data: Dict[str, pd.DataFrame], db_config: DBConfig) -> None:
    """Insert data with basic error handling"""
    try:
        with psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            dbname=db_config.dbname,
            user=db_config.user,
            password=db_config.password
        ) as conn, conn.cursor() as cursor:

            for ticker, df in batch_data.items():
                if df.empty:
                    continue

                symbol = ticker.replace(
                    '^', '') if ticker.startswith('^') else ticker

                data = [
                    (
                        row['Date'].astimezone(timezone.utc).strftime(
                            '%Y-%m-%d %H:%M:%S'),
                        symbol,
                        float(row['Open']),
                        float(row['High']),
                        float(row['Low']),
                        float(row['Close']),
                        int(row['Volume'])
                    )
                    for _, row in df.iterrows()
                ]

                try:
                    execute_batch(cursor, """
                        INSERT INTO stock_data2 (time, symbol, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, data)
                    conn.commit()
                    logger.info(f"Inserted {len(data)} rows for {symbol}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Failed to insert {symbol}: {e}")

    except Exception as e:
        logger.error(f"Database connection failed: {e}")


def process_batch(tickers_batch: List[str], max_workers: int = 5) -> Dict[str, pd.DataFrame]:
    """Process a batch of tickers concurrently"""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(
            fetch_ticker_data, ticker): ticker for ticker in tickers_batch}
        for future in as_completed(futures):
            ticker, df = future.result()
            if not df.empty:
                results[ticker] = df
    return results


def main():
    try:
        # Get tickers
        tickers = get_sp500_tickers()
        logger.info(f"Processing {len(tickers)} tickers")

        # Process in batches of 100
        tickers_list = list(tickers)
        batch_size = 100
        batches = [tickers_list[i:i + batch_size]
                   for i in range(0, len(tickers_list), batch_size)]

        # Process each batch
        for i, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {i}/{len(batches)}")
            batch_data = process_batch(batch)
            if batch_data:
                insert_batch_data(batch_data, DBConfig())
            time.sleep(1)  # Rate limiting between batches

    except Exception as e:
        logger.error(f"Program failed: {e}")
        raise


if __name__ == "__main__":
    main()
