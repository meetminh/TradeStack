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
            SELECT ticker, 
                   time::date as trading_date
            FROM stock_data 
            WHERE time IS NOT NULL
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


def validate_ticker_data(df: pd.DataFrame, ticker: str) -> bool:
    """Validate data before ingestion"""
    if df.empty:
        return False
    
    required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    if not all(col in df.columns for col in required_columns):
        print(f"Missing required columns for {ticker}")
        return False
        
    # Check for invalid values
    if df.isnull().any().any():
        print(f"Found null values in {ticker} data")
        return False
        
    return True


def ingest_batch_data(batch_data: Dict[str, pd.DataFrame]) -> None:
    """Ingest a batch of ticker data into QuestDB"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for ticker, df in batch_data.items():
            if df.empty:
                continue
                
            symbol = ticker.replace('^', '') if ticker.startswith('^') else ticker
            
            # Prepare batch insert
            values = []
            for _, row in df.iterrows():
                try:
                    # Ensure timestamp is in UTC
                    timestamp = pd.to_datetime(row['Date'])
                    if timestamp.tz is None:
                        timestamp = timestamp.tz_localize('UTC')
                    else:
                        timestamp = timestamp.tz_convert('UTC')
                        
                    values.append((
                        timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        symbol,
                        float(row['Open']),
                        float(row['High']),
                        float(row['Low']),
                        float(row['Close']),
                        int(row['Volume'])
                    ))
                except Exception as e:
                    print(f"Error processing row for {ticker}: {e}")
                    continue
            
            # Batch insert
            if values:
                cursor.executemany("""
                    INSERT INTO stock_data (time, ticker, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, values)
                
        conn.commit()
    except Exception as e:
        print(f"Error in batch ingestion: {e}")
    finally:
        cursor.close()
        conn.close()


def analyze_and_fill_gaps():
    """Main function to analyze data quality and fill gaps"""
    print("Fetching all data from database...")
    db_data = get_all_db_data()
    
    for ticker, data in tqdm(db_data.items(), desc="Analyzing tickers"):
        if not data['dates']:
            continue
            
        # Get ticker's actual trading range
        ticker_dates = sorted(list(data['dates']))
        first_date = ticker_dates[0]
        last_date = ticker_dates[-1]
        
        # Get SPY data only for this range
        spy = yf.Ticker("SPY")
        reference_history = spy.history(start=first_date, end=last_date, interval='1d')
        trading_days = set(d.date() for d in reference_history.index)
        
        # Now compare only within the stock's actual trading period
        missing_dates = sorted(list(trading_days - set(ticker_dates)))

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
