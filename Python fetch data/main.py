import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Set, Dict, Tuple
import time
import concurrent.futures
from tqdm import tqdm
import numpy as np
from functools import partial
import psycopg2
from datetime import timezone


def get_sp500_tickers() -> Set[str]:
    """Fetch current S&P 500 constituents"""
    table = pd.read_html(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    tickers = set(table[0]['Symbol'].str.replace('.', '-'))
    extra_tickers = {'SPY', 'QQQ', 'VOOG', 'SHY', 'TLT', 'GLD', '^VIX'}
    return tickers.union(extra_tickers)


def get_last_timestamps(host: str, port: int) -> Dict[str, datetime]:
    """Get the most recent timestamp for each ticker from the database"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname='qdb',
            user='admin',
            password='quest'
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT ticker, MAX(time) FROM stock_data GROUP BY ticker")
        last_times = dict(cursor.fetchall())
        conn.close()
        return {ticker: pd.to_datetime(timestamp) for ticker, timestamp in last_times.items()}
    except Exception as e:
        print(f"Error getting last timestamps: {e}")
        return {}


def get_latest_available_date() -> datetime:
    """Get the latest available date from Yahoo Finance"""
    ticker = yf.Ticker("AAPL")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    df = ticker.history(start=start_date, end=end_date)
    if not df.empty:
        return df.index[-1].to_pydatetime().replace(tzinfo=None)
    else:
        return end_date.replace(tzinfo=None)


def fetch_ticker_data(ticker: str, start_date: datetime, end_date: datetime) -> Tuple[str, pd.DataFrame]:
    """Fetch data for a single ticker"""
    try:
        time.sleep(np.random.uniform(0.1, 0.3))
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_date, end=end_date, interval='1d')
        if df.empty:
            print(f"No data available for {ticker}")
            return ticker, pd.DataFrame()
        df.reset_index(inplace=True)
        return ticker, df
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return ticker, pd.DataFrame()


def process_batch(tickers_batch: List[str], start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
    """Process a batch of tickers in parallel"""
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        fetch_func = partial(
            fetch_ticker_data, start_date=start_date, end_date=end_date)
        futures = {executor.submit(fetch_func, ticker): ticker for ticker in tickers_batch}
        for future in concurrent.futures.as_completed(futures):
            ticker, df = future.result()
            if not df.empty:
                results[ticker] = df
    return results


def ingest_batch_data(batch_data: Dict[str, pd.DataFrame], host: str, port: int) -> None:
    """Ingest a batch of ticker data into QuestDB using PostgreSQL wire protocol"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname='qdb',
            user='admin',
            password='quest'
        )
        cursor = conn.cursor()

        for ticker, df in batch_data.items():
            symbol = ticker.replace(
                '^', '') if ticker.startswith('^') else ticker

            for _, row in df.iterrows():
                try:
                    # Convert timestamp to UTC and format it correctly
                    timestamp = row['Date'].astimezone(
                        timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

                    cursor.execute("""
                        INSERT INTO stock_data (time, ticker, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        timestamp,
                        symbol,
                        float(row['Open']),
                        float(row['High']),
                        float(row['Low']),
                        float(row['Close']),
                        int(row['Volume'])
                    ))
                except Exception as e:
                    print(f"Error ingesting row for {ticker}: {e}")
                    continue

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error connecting to QuestDB for ingestion: {e}")


def fetch_and_ingest_data(
    tickers: Set[str],
    last_timestamps: Dict[str, datetime],
    end_date: datetime,
    host: str = 'host.docker.internal',
    port: int = 8812,
    batch_size: int = 20
) -> None:
    """Fetch and ingest missing data for each ticker"""
    tickers_list = list(tickers)
    batches = [tickers_list[i:i + batch_size]
               for i in range(0, len(tickers_list), batch_size)]

    print(f"Processing {len(tickers)} tickers in {len(batches)} batches")

    for batch_num, batch in enumerate(batches, 1):
        print(f"\nProcessing batch {batch_num}/{len(batches)}")
        batch_data = {}
        for ticker in batch:
            start_date = last_timestamps.get(ticker, datetime(2000, 1, 1))
            if start_date < end_date:
                _, df = fetch_ticker_data(
                    ticker, start_date + timedelta(days=1), end_date)
                if not df.empty:
                    batch_data[ticker] = df

        if batch_data:
            print(f"Ingesting data for batch {batch_num}")
            ingest_batch_data(batch_data=batch_data, host=host, port=port)
        else:
            print(f"No new data to ingest for batch {batch_num}")
        time.sleep(1)


def main():
    HOST = 'host.docker.internal'
    PORT = 8812  # PostgreSQL wire protocol port
    BATCH_SIZE = 20

    print("Fetching S&P 500 constituents...")
    tickers = get_sp500_tickers()

    print("Getting last timestamps from database...")
    last_timestamps = get_last_timestamps(HOST, PORT)

    print("Getting latest available date from Yahoo Finance...")
    END_DATE = get_latest_available_date()
    print(f"Latest available date: {END_DATE}")

    print(f"Fetching and ingesting missing data up to {END_DATE}")

    fetch_and_ingest_data(
        tickers=tickers,
        last_timestamps=last_timestamps,
        end_date=END_DATE,
        host=HOST,
        port=PORT,
        batch_size=BATCH_SIZE
    )


if __name__ == "__main__":
    main()
