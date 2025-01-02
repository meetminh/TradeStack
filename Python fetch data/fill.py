import concurrent.futures
import pandas as pd
from yahooquery import Ticker
from datetime import datetime, timedelta
import numpy as np
import warnings
import psycopg
import os
from tenacity import retry, wait_exponential, stop_after_attempt
import logging
from questdb.ingress import Sender, TimestampNanos

try:
    import pandas as pd
    import numpy as np
    import pyarrow
except ImportError as e:
    logging.error(f"Missing dependencies: {
                  e}. Please install `pandas`, `numpy`, and `pyarrow`.")
    raise

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[
                        logging.FileHandler('app.log'),
                        logging.StreamHandler()
                    ])

# Suppress FutureWarnings
warnings.filterwarnings('ignore', category=FutureWarning)

# Database connection string - QuestDB specific
CONN_STR = 'user=admin password=quest host=questdb.orb.local port=8812 dbname=qdb'
QDB_CONF = "http::addr=questdb.orb.local:9000;username=admin;password=quest;"


@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
def fetch_tickers_from_db():
    """Fetch all unique tickers and their last updated date from QuestDB."""
    query = """
    SELECT 
        ticker,
        max(time) as last_update
    FROM stock_data_daily
    GROUP BY ticker
    ORDER BY ticker;
    """
    try:
        with psycopg.connect(CONN_STR, autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                return [(row[0], row[1].strftime('%Y-%m-%d')) for row in rows]
    except Exception as e:
        logging.error(f"Failed to fetch tickers from QuestDB: {e}")
        raise


def normalize_market_time(df):
    """Normalize all timestamps to 16:00 ET and handle timezone conversion"""
    try:
        df['time'] = pd.to_datetime(df['time'])
        if df['time'].dt.tz is not None:
            df['time'] = df['time'].dt.tz_convert('UTC').dt.tz_localize(None)
        df['time'] = df['time'].dt.floor('D') + timedelta(hours=16)
        return df
    except Exception as e:
        logging.error(f"Error in normalize_market_time: {e}")
        raise


def handle_nan_values(df):
    """Handle NaN values in the DataFrame"""
    if 'volume' in df.columns:
        df['volume'] = df['volume'].fillna(0).astype(np.int64)
    return df


def validate_types(df):
    """Validate data types of the DataFrame"""
    type_issues = []
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        type_issues.append(
            "time should be datetime, got {}".format(df['time'].dtype))
    if not pd.api.types.is_string_dtype(df['ticker']):
        type_issues.append(
            "ticker should be string, got {}".format(df['ticker'].dtype))
    for col in ['open', 'high', 'low', 'close']:
        if not pd.api.types.is_float_dtype(df[col]):
            type_issues.append(
                "{} should be float64, got {}".format(col, df[col].dtype))
    if 'volume' in df.columns and not pd.api.types.is_integer_dtype(df['volume']):
        type_issues.append(
            "volume should be int64, got {}".format(df['volume'].dtype))
    return type_issues


def ensure_required_columns(df):
    """Ensure the DataFrame contains the required columns with correct data types."""
    required_columns = {
        'time': 'datetime64[ns]',
        'ticker': 'string',
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'close': 'float64',
        'volume': 'int64'
    }

    columns_order = ['time', 'ticker', 'open',
                     'high', 'low', 'close', 'volume']

    for col, dtype in required_columns.items():
        if col not in df.columns:
            df[col] = pd.Series(dtype=dtype)
        else:
            df[col] = df[col].astype(dtype)

    return df[columns_order]


def fetch_historical_data_parallel(tickers_with_dates, max_workers=20):
    """Fetch historical data for multiple tickers in parallel with increased workers"""
    all_data = []

    chunk_size = 50
    ticker_chunks = []
    current_chunk = []
    current_start_date = None

    for ticker, start_date in tickers_with_dates:
        if current_start_date is None:
            current_start_date = start_date

        if start_date == current_start_date and len(current_chunk) < chunk_size:
            current_chunk.append(ticker)
        else:
            if current_chunk:
                ticker_chunks.append(
                    (current_chunk.copy(), current_start_date))
            current_chunk = [ticker]
            current_start_date = start_date

    if current_chunk:
        ticker_chunks.append((current_chunk, current_start_date))

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        def fetch_chunk(chunk_data):
            tickers, start_date = chunk_data
            try:
                logging.info(f"Fetching chunk of {
                             len(tickers)} tickers from {start_date}")
                ticker_obj = Ticker(tickers, asynchronous=True)
                history = ticker_obj.history(
                    start=start_date, interval='1d', adj_ohlc=True)

                if not history.empty:
                    processed_data = []
                    for ticker in tickers:
                        try:
                            ticker_data = history.xs(
                                ticker, level='symbol', drop_level=False)
                            if not ticker_data.empty:
                                df = ticker_data.reset_index()
                                df = df.drop(
                                    columns=['dividends', 'splits'], errors='ignore')
                                df['ticker'] = ticker
                                df = df.rename(columns={'date': 'time'})
                                df = handle_nan_values(df)
                                df = normalize_market_time(df)
                                df = ensure_required_columns(df)

                                type_issues = validate_types(df)
                                if not type_issues:
                                    processed_data.append(df)
                                else:
                                    logging.warning(f"Data type issues for {
                                                    ticker}: {type_issues}")
                        except Exception as e:
                            logging.error(
                                f"Error processing ticker {ticker}: {e}")
                            continue

                    if processed_data:
                        return pd.concat(processed_data, ignore_index=True)
                return pd.DataFrame()
            except Exception as e:
                logging.error(f"Failed to fetch chunk {tickers}: {e}")
                return pd.DataFrame()

        future_to_chunk = {executor.submit(
            fetch_chunk, chunk): chunk for chunk in ticker_chunks}

        for future in concurrent.futures.as_completed(future_to_chunk):
            chunk = future_to_chunk[future]
            try:
                data = future.result()
                if not data.empty:
                    all_data.append(data)
                    logging.info(f"Successfully processed chunk of {
                                 len(chunk[0])} tickers")
                else:
                    logging.info(f"No data for chunk {chunk[0]}")
            except Exception as e:
                logging.error(f"Error processing chunk {chunk[0]}: {e}")

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()


def process_in_batches(tickers_with_dates, batch_size=100):
    """Process tickers in larger batches due to optimized parallel processing"""
    all_data = []
    batches_processed = 0

    for i in range(0, len(tickers_with_dates), batch_size):
        batch = tickers_with_dates[i:i + batch_size]
        logging.info(f"\nProcessing batch {i//batch_size + 1}...")
        try:
            data = fetch_historical_data_parallel(batch)
            if not data.empty:
                all_data.append(data)
                batches_processed += 1
                # Insert data every 10 batches
                if batches_processed % 10 == 0:
                    combined_batch_data = pd.concat(
                        all_data, ignore_index=True)
                    insert_into_questdb(combined_batch_data)
                    all_data = []
                    logging.info("Inserted 10 batches into QuestDB.")
        except Exception as e:
            logging.error(f"Failed to process batch: {e}")
            continue

    # Insert any remaining data
    if all_data:
        combined_batch_data = pd.concat(all_data, ignore_index=True)
        insert_into_questdb(combined_batch_data)
        logging.info("Inserted remaining data into QuestDB.")

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()


def insert_into_questdb(df):
    """Insert data into QuestDB"""
    try:
        with Sender.from_conf(QDB_CONF) as sender:
            sender.dataframe(
                df, table_name='stock_data_daily', at=TimestampNanos.now())
        logging.info("Data inserted into QuestDB successfully.")
    except Exception as e:
        logging.error(f"Failed to insert data into QuestDB: {e}")


def main():
    try:
        tickers_with_last_update = fetch_tickers_from_db()

        if not tickers_with_last_update:
            logging.warning("No tickers found in the database.")
            return

        tickers_with_dates = [
            (ticker, (datetime.strptime(last_update, '%Y-%m-%d') -
             timedelta(days=1)).strftime('%Y-%m-%d'))
            for ticker, last_update in tickers_with_last_update
        ]

        logging.info(f"Fetching data for {len(tickers_with_dates)} tickers...")

        combined_data = process_in_batches(
            tickers_with_dates,
            batch_size=100  # Increased batch size due to optimized processing
        )

        if not combined_data.empty:
            logging.info("\nFinal transformed data summary:")
            logging.info(f"Total rows: {len(combined_data)}")
            logging.info("\nSample of data (first row of each ticker):")
            for ticker in combined_data['ticker'].unique():
                ticker_data = combined_data[combined_data['ticker'] == ticker]
                if not ticker_data.empty:
                    logging.info(ticker_data.iloc[0])
        else:
            logging.warning("No data fetched for any ticker.")

    except Exception as e:
        logging.error(f"Application failed: {e}")
        raise


if __name__ == "__main__":
    main()
