import yfinance as yf
import pandas as pd
from typing import Set, Dict
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import os
from datetime import datetime, timezone
import psycopg2


def get_sp500_tickers() -> Set[str]:
    """Fetch current S&P 500 constituents"""
    table = pd.read_html(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    tickers = set(table[0]['Symbol'].str.replace('.', '-'))
    extra_tickers = {'SPY', 'QQQ', 'VOOG', 'SHY', 'TLT', 'GLD', '^VIX'}
    return tickers.union(extra_tickers)


def fetch_ticker_data(ticker: str) -> tuple[str, pd.DataFrame]:
    """Fetch data for a single ticker"""
    try:
        time.sleep(np.random.uniform(0.1, 0.3))  # Rate limiting
        df = yf.download(ticker, period='max',
                         progress=False, auto_adjust=True)
        if not df.empty:
            df = df.reset_index()
            symbol = ticker.replace(
                '^', '') if ticker.startswith('^') else ticker
            return symbol, df
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
    return ticker, pd.DataFrame()


def fetch_all_data(tickers: Set[str]) -> Dict[str, pd.DataFrame]:
    """Fetch data for all tickers in parallel"""
    all_data = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_ticker = {executor.submit(
            fetch_ticker_data, ticker): ticker for ticker in tickers}

        for future in tqdm(as_completed(future_to_ticker), total=len(tickers), desc="Fetching data"):
            ticker = future_to_ticker[future]
            try:
                symbol, df = future.result()
                if not df.empty:
                    all_data[symbol] = df
            except Exception as e:
                print(f"Error processing {ticker}: {e}")

    return all_data


def save_to_csv(data: Dict[str, pd.DataFrame], base_dir: str = 'stock_data') -> None:
    """Save all fetched data to CSV files"""
    os.makedirs(base_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    data_dir = os.path.join(base_dir, f'fetch_{timestamp}')
    os.makedirs(data_dir, exist_ok=True)

    for symbol, df in tqdm(data.items(), desc="Saving CSV files"):
        clean_symbol = symbol.replace('^', '').replace('/', '_')
        filename = os.path.join(data_dir, f'{clean_symbol}.csv')
        df.to_csv(filename, index=False)

    print("Creating combined CSV file...")
    combined_df = pd.concat(
        [df.assign(Symbol=symbol) for symbol, df in data.items()],
        ignore_index=True
    )
    combined_df.to_csv(os.path.join(data_dir, 'all_stocks.csv'), index=False)
    print(f"Data saved to {data_dir}")


def insert_all_data(data: Dict[str, pd.DataFrame], host: str = 'host.docker.internal', port: int = 8812) -> None:
    """Insert all data into QuestDB"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname='qdb',
            user='admin',
            password='quest'
        )
        cursor = conn.cursor()

        total_rows = sum(len(df) for df in data.values())
        progress_bar = tqdm(total=total_rows, desc="Inserting data")

        for symbol, df in data.items():
            for _, row in df.iterrows():
                try:
                    # Convert timestamp to UTC
                    timestamp = pd.to_datetime(row['Date']).tz_localize(
                        'UTC' if pd.isnull(row['Date'].tzinfo) else None
                    ).strftime('%Y-%m-%d %H:%M:%S')

                    cursor.execute("""
                        INSERT INTO stock_data2 (time, symbol, open, high, low, close, volume)
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
                    progress_bar.update(1)
                except Exception as e:
                    print(f"Error inserting row for {symbol}: {e}")
                    continue

            # Commit after each symbol to avoid memory issues
            conn.commit()

        progress_bar.close()
        cursor.close()
        conn.close()
        print("Data insertion completed successfully")

    except Exception as e:
        print(f"Database connection error: {e}")
        import traceback
        traceback.print_exc()


def main():
    # Get tickers
    print("Fetching S&P 500 constituents...")
    tickers = get_sp500_tickers()

    # Fetch all data first
    print("Fetching historical data for all tickers...")
    all_data = fetch_all_data(tickers)

    # Save to CSV files
    # print("Saving data to CSV files...")
    # save_to_csv(all_data)

    # Insert all data into QuestDB
    print("Inserting data into QuestDB...")
    insert_all_data(all_data)


if __name__ == "__main__":
    main()
