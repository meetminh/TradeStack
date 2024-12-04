import yfinance as yf
from datetime import datetime, timedelta


def get_latest_stock_date():
    # Use a popular stock that's likely to have up-to-date data
    ticker = yf.Ticker("AAPL")

    # Start from today and go back a few days to ensure we catch the latest trading day
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    # Fetch the data
    df = ticker.history(start=start_date, end=end_date)

    if not df.empty:
        latest_date = df.index[-1]
        print(f"Latest available date for AAPL: {latest_date}")
        return latest_date
    else:
        print("No data available in the specified range.")
        return None


if __name__ == "__main__":
    get_latest_stock_date()
