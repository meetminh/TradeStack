import csv
from datetime import datetime
from questdb.ingress import Sender, TimestampNanos, IngressError
import sys
import os

# Define the folder containing the CSV files
data_folder = 'testdata'

# Function to set the timestamp to 16:00:00 UTC


def set_to_16pm_utc(date_str):
    # Parse the date
    date = datetime.strptime(date_str, '%Y-%m-%d')
    # Set the time to 16:00:00 UTC
    utc_time = date.replace(hour=16, minute=0, second=0, microsecond=0)
    return utc_time

# Insert data into QuestDB


def insert_into_questdb(csv_file, ticker):
    try:
        # Connect to QuestDB
        conf = 'http::addr=questdb.orb.local:9000;'
        with Sender.from_conf(conf) as sender:
            with open(csv_file, mode='r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Set the timestamp to 16:00:00 UTC
                    utc_time = set_to_16pm_utc(row['date'])
                    # Convert the timestamp to nanoseconds as an integer
                    timestamp_nanos = int(utc_time.timestamp() * 1e9)
                    # Insert the data
                    sender.row(
                        'stock_data_daily',
                        symbols={'ticker': ticker},
                        columns={
                            'close': float(row['close']),
                            'high': float(row['high']),
                            'low': float(row['low']),
                            'open': float(row['open']),
                            'volume': int(row['volume']),
                            'adjClose': float(row['adjClose']),
                            'adjHigh': float(row['adjHigh']),
                            'adjLow': float(row['adjLow']),
                            'adjOpen': float(row['adjOpen']),
                            'adjVolume': int(row['adjVolume']),
                            'divCash': float(row['divCash']),
                            'splitFactor': float(row['splitFactor'])
                        },
                        at=TimestampNanos(timestamp_nanos)
                    )
            # Flush any remaining rows
            sender.flush()
        print(f"Data from {csv_file} inserted into QuestDB")
    except IngressError as e:
        sys.stderr.write(f'Got error: {e}\n')


# Iterate over all files in the testdata folder
for filename in os.listdir(data_folder):
    if filename.endswith('.csv'):
        # Extract the ticker symbol from the filename
        ticker = filename.replace('prices.csv', '')
        # Construct the full file path
        input_file = os.path.join(data_folder, filename)
        # Insert the data into QuestDB
        insert_into_questdb(input_file, ticker)
