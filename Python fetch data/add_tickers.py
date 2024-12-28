# import pandas as pd
# from questdb.ingress import Sender, IngressError, TimestampNanos
# from datetime import datetime
# import sys


# def print_dataframe_info(df, stage):
#     print(f"\n=== {stage} ===")
#     print(f"Shape: {df.shape}")
#     print("First 3 rows:")
#     print(df.head(3))
#     print("\nColumns:", df.columns.tolist())
#     print("Data types:")
#     print(df.dtypes)
#     print("=" * 50)


# try:
#     # Step 1: Read the CSV file
#     print("\nAttempting to read CSV from URL...")
#     df = pd.read_csv(
#         "https://r2.datahub.io/clt98kj4f0005ia08b4a7ergo/main/raw/data/nasdaq-listed-symbols.csv")
#     print_dataframe_info(df, "After reading CSV")

#     # Step 2: Select the first two columns and rename them
#     print("\nSelecting and renaming columns...")
#     df = df.iloc[:, :2]
#     df.columns = ['ticker_symbol', 'company_name']
#     print_dataframe_info(df, "After column selection and renaming")

#     # Step 3: Add the 'exchange' column
#     print("\nAdding exchange column...")
#     df['exchange'] = 'NASDAQ'
#     print_dataframe_info(df, "After adding exchange")

#     # Step 4: Add the 'last_updated' column as simple string date
#     print("\nAdding timestamp...")
#     current_date = datetime.now().strftime('%Y-%m-%d')
#     print(f"Current date being used: {current_date}")
#     df['last_updated'] = pd.to_datetime(
#         current_date)  # Convert to datetime64[ns]
#     print_dataframe_info(df, "Final DataFrame before insertion")

#     # Step 5: Connect to QuestDB and insert data
#     print("\nAttempting to connect to QuestDB...")
#     conf = 'http::addr=questdb.orb.local:9000;auto_flush_rows=1000;'

#     try:
#         with Sender.from_conf(conf) as sender:
#             total_rows = len(df)
#             print(f"\nStarting data insertion of {total_rows} rows...")
#             errors = []

#             try:
#                 sender.dataframe(
#                     df,
#                     table_name='stock_tickers',
#                     symbols=['ticker_symbol', 'exchange'],
#                     at='last_updated'  # Use the last_updated column instead of TimestampNanos.now()
#                 )
#                 print("\nDataFrame sent to QuestDB")

#             except Exception as e:
#                 error_msg = f"Error during dataframe insertion: {str(e)}"
#                 print(error_msg)
#                 errors.append(error_msg)

#             print("\nFlushing data...")
#             sender.flush()

#     except IngressError as e:
#         print(f"\nQuestDB Ingress Error: {e}")
#         sys.exit(1)

#     # Final status report
#     print("\n=== INSERTION SUMMARY ===")
#     print(f"Total rows in DataFrame: {total_rows}")
#     if not errors:
#         print("Data insertion completed successfully!")
#     else:
#         print("\nErrors encountered:")
#         for error in errors:
#             print(f"- {error}")

# except Exception as e:
#     print(f"\n!!! CRITICAL ERROR !!!")
#     print(f"Type: {type(e).__name__}")
#     print(f"Error: {str(e)}")
#     print(f"Location: {sys.exc_info()[2].tb_lineno}")
#     sys.exit(1)


##FOR NYSE stock data

import pandas as pd
from questdb.ingress import Sender, IngressError
from datetime import datetime
import sys


def print_dataframe_info(df, stage):
    print(f"\n=== {stage} ===")
    print(f"Shape: {df.shape}")
    print("First 3 rows:")
    print(df.head(3))
    print("\nColumns:", df.columns.tolist())
    print("Data types:")
    print(df.dtypes)
    print("=" * 50)


try:
    # Step 1: Read the CSV file
    print("\nAttempting to read CSV from URL...")
    df = pd.read_csv(
        "https://r2.datahub.io/clt98mjxo000pl708niy4jpmy/main/raw/data/other-listed.csv")
    print_dataframe_info(df, "After reading CSV")

    # Step 2: Select the first two columns and rename them
    print("\nSelecting and renaming columns...")
    df = df.iloc[:, :2]
    df.columns = ['ticker_symbol', 'company_name']
    print_dataframe_info(df, "After column selection and renaming")

    # Step 3: Add the 'exchange' column
    print("\nAdding exchange column...")
    df['exchange'] = 'NYSE'
    print_dataframe_info(df, "After adding exchange")

    # Step 4: Add the 'last_updated' column
    print("\nAdding timestamp...")
    current_date = datetime.now().strftime('%Y-%m-%d')
    print(f"Current date being used: {current_date}")
    df['last_updated'] = pd.to_datetime(current_date)
    print_dataframe_info(df, "Final DataFrame before insertion")

    # Step 5: Connect to QuestDB and insert data
    print("\nAttempting to connect to QuestDB...")
    conf = 'http::addr=questdb.orb.local:9000;auto_flush_rows=1000;'

    try:
        with Sender.from_conf(conf) as sender:
            total_rows = len(df)
            print(f"\nStarting data insertion of {total_rows} rows...")
            errors = []

            try:
                sender.dataframe(
                    df,
                    table_name='stock_tickers',
                    symbols=['ticker_symbol', 'exchange'],
                    at='last_updated'
                )
                print("\nDataFrame sent to QuestDB")

            except Exception as e:
                error_msg = f"Error during dataframe insertion: {str(e)}"
                print(error_msg)
                errors.append(error_msg)

            print("\nFlushing data...")
            sender.flush()

    except IngressError as e:
        print(f"\nQuestDB Ingress Error: {e}")
        sys.exit(1)

    # Final status report
    print("\n=== INSERTION SUMMARY ===")
    print(f"Total rows in DataFrame: {total_rows}")
    if not errors:
        print("Data insertion completed successfully!")
    else:
        print("\nErrors encountered:")
        for error in errors:
            print(f"- {error}")

except Exception as e:
    print(f"\n!!! CRITICAL ERROR !!!")
    print(f"Type: {type(e).__name__}")
    print(f"Error: {str(e)}")
    print(f"Location: {sys.exc_info()[2].tb_lineno}")
    sys.exit(1)
