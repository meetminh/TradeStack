import yfinance as yf
import pandas as pd
from typing import Set, Dict, List, Tuple
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import psycopg2
import logging
from psycopg2.extras import execute_batch

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
UNIX_EPOCH = pd.Timestamp('1970-01-01').tz_localize(timezone.utc)
DB_CONFIG = {
    'host': 'host.docker.internal',
    'port': 8812,
    'dbname': 'qdb',
    'user': 'admin',
    'password': 'quest'
}


def get_existing_tickers() -> Set[str]:
    """
    Fetch all unique tickers currently stored in QuestDB
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT symbol FROM stock_data2")
                existing_tickers = {row[0] for row in cursor.fetchall()}
                logger.info(f"Found {len(existing_tickers)
                                     } existing tickers in database")
                return existing_tickers
    except Exception as e:
        logger.error(f"Failed to fetch existing tickers: {e}")
        raise


def get_sp500_tickers() -> Set[str]:
    """
    Fetch current S&P 500 constituents plus additional tickers
    """
    try:
        table = pd.read_html(
            'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        tickers = set(table[0]['Symbol'].str.replace('.', '-'))
        extra_tickers = {'SPY', 'QQQ', 'VOOG', 'SHY', 'TLT', 'GLD', '^VIX'}
        all_tickers = tickers.union(extra_tickers)
        logger.info(f"Found {len(all_tickers)} total tickers to process")
        return all_tickers
    except Exception as e:
        logger.error(f"Failed to fetch S&P 500 tickers: {e}")
        raise


def find_missing_tickers(sp500_tickers: Set[str], existing_tickers: Set[str]) -> Set[str]:
    """
    Compare sets to find missing tickers
    """
    missing_tickers = sp500_tickers - existing_tickers
    logger.info(f"Found {len(missing_tickers)} missing tickers to fetch")
    if missing_tickers:
        logger.info(f"Missing tickers: {sorted(missing_tickers)}")
    return missing_tickers


def clean_and_validate_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Clean and validate stock data with timestamp filtering
    """
    if df.empty:
        logger.warning(f"No data for {ticker}")
        return pd.DataFrame()

    # Reset index if Date is in index
    if 'Date' in df.index.names:
        df = df.reset_index()

    # Remove NaN values
    initial_len = len(df)
    df = df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'])
    if len(df) < initial_len:
        logger.warning(f"Removed {initial_len - len(df)
                                  } rows with NaN values from {ticker}")

    # Filter out pre-1970 dates
    pre_1970_count = len(df[df['Date'] < UNIX_EPOCH])
    if pre_1970_count > 0:
        logger.warning(
            f"Removing {pre_1970_count} pre-1970 records from {ticker}")
        df = df[df['Date'] >= UNIX_EPOCH]

    # Validate price and volume data
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
    """
    Fetch and validate data for a single ticker
    """
    try:
        time.sleep(0.1)  # Rate limiting
        df = yf.Ticker(ticker).history(period="max")
        df = clean_and_validate_data(df, ticker)
        if not df.empty:
            logger.info(f"Fetched {len(df)} valid rows for {ticker}")
        return ticker, df
    except Exception as e:
        logger.error(f"Error fetching {ticker}: {e}")
        return ticker, pd.DataFrame()


def insert_ticker_data(ticker: str, df: pd.DataFrame) -> None:
    """
    Insert single ticker data into database
    """
    if df.empty:
        return

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
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

                execute_batch(cursor, """
                    INSERT INTO stock_data2 (time, symbol, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, data)

                conn.commit()
                logger.info(f"Successfully inserted {
                            len(data)} rows for {symbol}")

    except Exception as e:
        logger.error(f"Failed to insert data for {ticker}: {e}")


def process_missing_tickers(missing_tickers: Set[str], max_workers: int = 5) -> None:
    """
    Process missing tickers with parallel execution
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_ticker_data, ticker): ticker
                   for ticker in missing_tickers}

        for future in as_completed(futures):
            ticker, df = future.result()
            if not df.empty:
                insert_ticker_data(ticker, df)
            time.sleep(1)  # Rate limiting between insertions


def main():
    try:
        # 1. Get existing tickers from database
        existing_tickers = get_existing_tickers()

        # 2. Get current S&P 500 tickers
        sp500_tickers = get_sp500_tickers()

        # 3. Find missing tickers
        missing_tickers = find_missing_tickers(sp500_tickers, existing_tickers)

        if not missing_tickers:
            logger.info("No missing tickers to process")
            return

        # 4. Process missing tickers
        logger.info("Starting to process missing tickers")
        process_missing_tickers(missing_tickers)
        logger.info("Completed processing missing tickers")

    except Exception as e:
        logger.error(f"Program failed: {e}")
        raise


if __name__ == "__main__":
    main()
