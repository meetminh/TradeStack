from questdb.ingress import Sender
import pandas as pd
from datetime import datetime, timedelta, date
import pandas_market_calendars as mcal
import holidays  # Import the holidays API

# Function to get all weekends between two dates


def get_weekends(start_date, end_date):
    weekends = []
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() >= 5:  # Saturday (5) or Sunday (6)
            weekends.append(current_date)
        current_date += timedelta(days=1)
    return weekends

# Function to get all holidays between two dates


def get_holidays(start_date, end_date):
    # Get historical holidays (1980-2021) using pandas_market_calendars
    nyse = mcal.get_calendar('NASDAQ')
    holidays_data = nyse.holidays().holidays
    historical_holidays = {pd.Timestamp(date).to_pydatetime().date(): "Holiday" for date in holidays_data if start_date.date(
    ) <= pd.Timestamp(date).to_pydatetime().date() <= end_date.date()}

    # Add future holidays (2022-2050) from the provided data
    future_holidays = {
        "2022-01-17": "Martin Luther King, Jr. Day",
        "2022-02-21": "Presidents' Day",
        "2022-04-15": "Good Friday",
        "2022-05-30": "Memorial Day",
        "2022-06-20": "Juneteenth",
        "2022-07-04": "Independence Day",
        "2022-09-05": "Labor Day",
        "2022-11-24": "Thanksgiving Day",
        "2022-12-26": "Christmas",
        "2023-01-02": "New Year's Day",
        "2023-01-16": "Martin Luther King, Jr. Day",
        "2023-02-20": "Washington's Birthday",
        "2023-04-07": "Good Friday",
        "2023-05-29": "Memorial Day",
        "2023-06-19": "Juneteenth",
        "2023-07-04": "Independence Day",
        "2023-09-04": "Labor Day",
        "2023-11-23": "Thanksgiving Day",
        "2023-12-25": "Christmas",
        "2024-01-01": "New Year's Day",
        "2024-01-15": "Martin Luther King, Jr. Day",
        "2024-02-19": "Washington's Birthday",
        "2024-03-29": "Good Friday",
        "2024-05-27": "Memorial Day",
        "2024-06-19": "Juneteenth",
        "2024-07-04": "Independence Day",
        "2024-09-02": "Labor Day",
        "2024-11-28": "Thanksgiving Day",
        "2024-12-25": "Christmas",
        "2025-01-01": "New Year's Day",
        "2025-01-20": "Martin Luther King, Jr. Day",
        "2025-02-17": "Washington's Birthday",
        "2025-04-18": "Good Friday",
        "2025-05-26": "Memorial Day",
        "2025-06-19": "Juneteenth",
        "2025-07-04": "Independence Day",
        "2025-09-01": "Labor Day",
        "2025-11-27": "Thanksgiving Day",
        "2025-12-25": "Christmas",
        "2026-01-01": "New Year's Day",
        "2026-01-19": "Martin Luther King, Jr. Day",
        "2026-02-16": "Washington's Birthday",
        "2026-04-03": "Good Friday",
        "2026-05-25": "Memorial Day",
        "2026-06-19": "Juneteenth",
        "2026-07-03": "Independence Day",
        "2026-09-07": "Labor Day",
        "2026-11-26": "Thanksgiving Day",
        "2026-12-25": "Christmas",
        "2027-01-01": "New Year's Day",
        "2027-01-18": "Martin Luther King, Jr. Day",
        "2027-02-15": "Presidents' Day",
        "2027-03-26": "Good Friday",
        "2027-05-31": "Memorial Day",
        "2027-06-18": "Juneteenth",
        "2027-07-05": "Independence Day",
        "2027-09-06": "Labor Day",
        "2027-11-25": "Thanksgiving Day",
        "2027-12-31": "Christmas"
    }
    future_holidays = {datetime.strptime(
        date, "%Y-%m-%d").date(): name for date, name in future_holidays.items()}
    future_holidays = {date: name for date, name in future_holidays.items(
    ) if start_date.date() <= date <= end_date.date()}

    # Combine historical and future holidays
    all_holidays = {**historical_holidays, **future_holidays}

    # Use the holidays API to fill in missing holiday names
    us_holidays = holidays.US(years=range(start_date.year, end_date.year + 1))
    for date in all_holidays.copy():
        if all_holidays[date] == "Holiday":  # If the holiday name is generic
            # Replace with specific name if available
            all_holidays[date] = us_holidays.get(date, "Holiday")

    return all_holidays

# Function to get all closed days (weekends + holidays)


def get_closed_days(start_date, end_date):
    weekends = get_weekends(start_date, end_date)
    holidays = get_holidays(start_date, end_date)
    closed_days = set(weekends + [datetime.combine(date, datetime.min.time())
                      for date in holidays.keys()])  # Convert to datetime
    return sorted(closed_days), holidays

# Function to get the next trading day


def get_next_trading_day(date, closed_days):
    next_day = date + timedelta(days=1)
    while next_day in closed_days:
        next_day += timedelta(days=1)
    return next_day


# Define the date range
start_date = datetime(1980, 1, 1)
end_date = datetime(2050, 12, 31)

# Get all closed days and holidays
closed_days, holidays = get_closed_days(start_date, end_date)

# Create a list of dictionaries for the table
table_data = []
for day in closed_days:
    weekday = day.strftime('%A')  # Get the weekday name
    # Get the holiday name or "Weekend"
    holiday_name = holidays.get(day.date(), "Weekend")
    next_trading_day = get_next_trading_day(day, closed_days)
    days_until_next_trading_day = (next_trading_day - day).days
    is_start_of_month = day.day == 1
    is_last_day_of_month = (day + timedelta(days=1)).month != day.month

    table_data.append({
        "Date": day.strftime('%Y-%m-%d'),
        "Year": day.year,
        "Month": day.strftime('%B'),
        "Day of Month": day.day,
        "Weekday": weekday,
        "Is Weekend": day.weekday() >= 5,
        "Is Holiday": day.date() in holidays,
        "Holiday Name": holiday_name,
        "Season": "Winter" if day.month in [12, 1, 2] else
                  "Spring" if day.month in [3, 4, 5] else
                  "Summer" if day.month in [6, 7, 8] else
                  "Fall",
        "Days Until Next Trading Day": days_until_next_trading_day,
        "Date of Next Trading Day": next_trading_day.strftime('%Y-%m-%d'),
        "Is Start of Month": is_start_of_month,
        "Is Last Day of Month": is_last_day_of_month
    })

# Convert the list to a DataFrame
df = pd.DataFrame(table_data)

# Save the DataFrame to a CSV file (optional)
df.to_csv("nasdaq_closed_days.csv", index=False)

# Print the DataFrame
print(df)

# Connect to QuestDB and insert data
conf = "http::addr=questdb.orb.local:9000;username=admin;password=quest;"
with Sender.from_conf(conf) as sender:
    for row in table_data:
        sender.row(
            table_name='nasdaq_closed_days',
            symbols={
                'Month': row['Month'],
                'Weekday': row['Weekday'],
                'Holiday_Name': row['Holiday Name'],
                'Season': row['Season']
            },
            columns={
                'Date': pd.Timestamp(row['Date']),
                'Year': row['Year'],
                'Day_of_Month': row['Day of Month'],
                'Is_Weekend': row['Is Weekend'],
                'Is_Holiday': row['Is Holiday'],
                'Days_Until_Next_Trading_Day': row['Days Until Next Trading Day'],
                'Date_of_Next_Trading_Day': pd.Timestamp(row['Date of Next Trading Day']),
                'Is_Start_of_Month': row['Is Start of Month'],
                'Is_Last_Day_of_Month': row['Is Last Day of Month']
            },
            at=pd.Timestamp(row['Date'])  # Designated timestamp
        )
    sender.flush()  # Send all rows to QuestDB
