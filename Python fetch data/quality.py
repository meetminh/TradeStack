import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Set, Dict, Tuple
import time
import concurrent.futures
from tqdm import tqdm
import numpy as np
import psycopg2
from datetime import timezone


def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host='host.docker.internal',
        port=8812,
        dbname='qdb',
        user='admin',
        password='quest'
    )


def get_all_db_data() -> Dict[str, Dict[str, datetime]]:
    """Get all tickers and their data dates from database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all distinct dates for each ticker
        cursor.execute("""
            SELECT ticker, time::date 
            FROM stock_data 
            ORDER BY ticker, time
        """)

        results = {}
        for ticker, date in cursor.fetchall():
            if ticker not in results:
                results[ticker] = {'dates': set()}
            results[ticker]['dates'].add(date)

        conn.close()
        return results
    except Exception as e:
        print(f"Error getting database data: {e}")
        return {}


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


def ingest_batch_data(batch_data: Dict[str, pd.DataFrame], host: str = 'host.docker.internal', port: int = 8812) -> None:
    """Ingest a batch of ticker data into QuestDB"""
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


def analyze_and_fill_gaps():
    """Main function to analyze data quality and fill gaps"""
    print("Fetching all data from database...")
    db_data = get_all_db_data()
    print(f"Found {len(db_data)} tickers")

    # Get reference trading days using SPY
    print("Getting trading calendar from SPY...")
    spy = yf.Ticker("SPY")
    full_history = spy.history(
        start="2000-01-01", end=datetime.now(), interval='1d')
    trading_days = set(d.date() for d in full_history.index)

    for ticker, data in tqdm(db_data.items(), desc="Analyzing tickers"):
        print(f"\nProcessing {ticker}")

        # Find missing dates
        ticker_dates = data['dates']
        missing_dates = sorted(list(trading_days - ticker_dates))

        if missing_dates:
            print(f"Found {len(missing_dates)} missing dates for {ticker}")
            print(f"First missing date: {missing_dates[0]}")
            print(f"Last missing date: {missing_dates[-1]}")

            # Split missing dates into chunks to avoid too large requests
            chunk_size = 1000
            date_chunks = [missing_dates[i:i + chunk_size]
                           for i in range(0, len(missing_dates), chunk_size)]

            for chunk in date_chunks:
                start_date = chunk[0]
                end_date = chunk[-1]

                ticker_obj = yf.Ticker(ticker)
                df = ticker_obj.history(
                    start=start_date, end=end_date + timedelta(days=1))

                if not df.empty:
                    df.reset_index(inplace=True)
                    ingest_batch_data({ticker: df})
                    print(f"Inserted chunk of {len(df)} records for {ticker}")

                time.sleep(1)  # Avoid rate limiting
        else:
            print(f"No missing dates found for {ticker}")


if __name__ == "__main__":
    analyze_and_fill_gaps()
