import pandas as pd
import psycopg
import numpy as np

# Database connection parameters
conn_str = 'user=admin password=quest host=questdb.orb.local port=8812 dbname=qdb'

# Connect to QuestDB and fetch data


def fetch_db_data():
    print("Connecting to QuestDB...")
    try:
        with psycopg.connect(conn_str, autocommit=True) as connection:
            print("Connected to QuestDB successfully.")
            with connection.cursor() as cur:
                print("Executing query to fetch data...")
                query = "SELECT time, open, high, low, close, volume FROM stock_data_daily WHERE ticker = 'NVDA' ORDER BY time ASC"
                cur.execute(query)
                print("Query executed successfully. Fetching records...")
                records = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                df_db = pd.DataFrame(records, columns=columns)
                print(f"Fetched {len(df_db)} rows from QuestDB.")
        return df_db
    except Exception as e:
        print(f"Error fetching data from QuestDB: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

# Read CSS file and clean duplicate columns


def read_css_file(file_path):
    print(f"Reading CSS file from {file_path}...")
    try:
        df_csv = pd.read_csv(file_path)
        print(f"Successfully read {len(df_csv)} rows from CSS file.")

        # Remove duplicate columns (keep the first occurrence of each column)
        df_csv = df_csv.loc[:, ~df_csv.columns.duplicated()]
        print("Removed duplicate columns from CSV data.")

        return df_csv
    except Exception as e:
        print(f"Error reading CSS file: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

# Transform time column in QuestDB data


def transform_time_column(df_db):
    print("Transforming time column to match CSS file format...")
    df_db['time'] = pd.to_datetime(df_db['time']).dt.strftime('%Y-%m-%d')
    print("Time column transformation complete.")
    return df_db

# Normalize column names to lowercase


def normalize_column_names(df):
    df.columns = df.columns.str.lower()  # Convert all column names to lowercase
    return df

# Remove duplicate rows in the CSV data


def remove_duplicates(df_csv):
    print("Removing duplicate rows in CSV data...")
    initial_rows = len(df_csv)
    # Keep the first occurrence of each date
    df_csv = df_csv.drop_duplicates(subset=['time'], keep='first')
    final_rows = len(df_csv)
    print(f"Removed {initial_rows - final_rows} duplicate rows.")
    return df_csv

# Compare data


def compare_data(df_db, df_csv):
    print("Starting data comparison...")
    inaccuracies = []
    incorrect_rows = 0

    # Normalize column names to lowercase
    df_db = normalize_column_names(df_db)
    df_csv = normalize_column_names(df_csv)

    # Remove duplicate rows in the CSV data
    df_csv = remove_duplicates(df_csv)

    # Check if 'volume' column exists in both datasets
    volume_in_db = 'volume' in df_db.columns
    volume_in_csv = 'volume' in df_csv.columns

    for index, row in df_db.iterrows():
        date = row['time']
        csv_row = df_csv[df_csv['time'] == date]

        if not csv_row.empty:
            for field in ['open', 'high', 'low', 'close']:
                db_value = row[field]
                # Convert to Python float
                csv_value = float(csv_row[field].values[0])

                # Calculate percentage inaccuracy
                if csv_value != 0:  # Avoid division by zero
                    percentage_inaccuracy = abs(
                        (db_value - csv_value) / csv_value) * 100
                else:
                    percentage_inaccuracy = 0  # If CSV value is 0, assume no inaccuracy

                # Determine if the discrepancy is tolerable
                tolerable = percentage_inaccuracy <= 0.1  # 0.1% tolerance for price data

                # Flag discrepancies
                # Small tolerance for floating-point precision
                if abs(db_value - csv_value) > 1e-5:
                    inaccuracies.append({
                        'date': date,
                        'field': field,
                        'db_value': db_value,
                        'csv_value': csv_value,
                        'percentage_inaccuracy': round(percentage_inaccuracy, 2),
                        'tolerable': tolerable
                    })
                    incorrect_rows += 1  # Increment for every discrepancy

            # Compare volume if the column exists in both datasets
            if volume_in_db and volume_in_csv:
                db_volume = row['volume']
                csv_volume = float(csv_row['volume'].values[0])

                # Calculate percentage inaccuracy for volume
                if csv_volume != 0:  # Avoid division by zero
                    percentage_inaccuracy = abs(
                        (db_volume - csv_volume) / csv_volume) * 100
                else:
                    percentage_inaccuracy = 0  # If CSV value is 0, assume no inaccuracy

                # Determine if the discrepancy is tolerable
                tolerable = percentage_inaccuracy <= 1  # 1% tolerance for volume

                # Flag discrepancies
                # Small tolerance for floating-point precision
                if abs(db_volume - csv_volume) > 1e-5:
                    inaccuracies.append({
                        'date': date,
                        'field': 'volume',
                        'db_value': db_volume,
                        'csv_value': csv_volume,
                        'percentage_inaccuracy': round(percentage_inaccuracy, 2),
                        'tolerable': tolerable
                    })
                    incorrect_rows += 1  # Increment for every discrepancy

    print("Data comparison complete.")
    return inaccuracies, incorrect_rows

# Main function


def main():
    print("Starting script...")
    df_db = fetch_db_data()
    if df_db.empty:
        print("No data fetched from QuestDB. Exiting.")
        return

    df_db = transform_time_column(df_db)

    css_file_path = 'NASDAQ_NVDA, 1D_180c4.csv'
    df_csv = read_css_file(css_file_path)
    if df_csv.empty:
        print("No data read from CSS file. Exiting.")
        return

    inaccuracies, incorrect_rows = compare_data(df_db, df_csv)

    print(f"Total Rows with Inaccuracies: {incorrect_rows}")
    print("List of Inaccuracies:")
    for inaccuracy in inaccuracies:
        print(inaccuracy)

    print("Script execution complete.")


if __name__ == "__main__":
    main()
