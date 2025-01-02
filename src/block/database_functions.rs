use chrono::{DateTime, NaiveDateTime, Utc};
use deadpool_postgres::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_postgres::Error as PgError;

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("Database error: {0}")]
    PostgresError(#[from] PgError),
    #[error("Pool error: {0}")]
    PoolError(#[from] deadpool_postgres::PoolError),
    #[error("Invalid date range")]
    InvalidDateRange,
    #[error("Invalid ticker symbol")]
    InvalidTicker,
    #[error("Invalid period: {0}")]
    InvalidPeriod(String),
    #[error("Insufficient data: {0}")]
    InsufficientData(String),
    #[error("Insufficient data for MA: {0}")]
    InsufficientDataForMA(String),
    #[error("Invalid calculation: {0}")]
    InvalidCalculation(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StockDataPoint {
    pub time: DateTime<Utc>,
    pub ticker: String,
    pub close: f64,
    pub sma: f64,
}

fn validate_price(price: f64, context: &str) -> Result<(), DatabaseError> {
    if !price.is_finite() || price <= 0.0 {
        return Err(DatabaseError::InvalidCalculation(format!(
            "Invalid price in {}: {}",
            context, price
        )));
    }
    Ok(())
}
#[derive(Debug)]
struct StartDateResult {
    time: NaiveDateTime,
}
use chrono::SecondsFormat;
// pub async fn get_start_date(
//     client: &Client,
//     ticker: &str,
//     execution_date: &str,
//     trading_days: i64,
// ) -> Result<String, DatabaseError> {
//     validate_ticker(ticker)?;
//     validate_period(trading_days, "Trading days")?;
//     tracing::debug!("Starting get_start_date");

//     let query = format!(
//         "SELECT min(time) as start_date
//         FROM (
//             SELECT time
//             FROM stock_data_daily
//             WHERE ticker = $1
//             AND time <= '{}'
//             ORDER BY time DESC
//             LIMIT $2
//         ) AS subquery",
//         execution_date
//     );

//     let row = client.query_one(&query, &[&ticker, &trading_days]).await?;

//     let time: NaiveDateTime = row.get("start_date");
//     let start_date = DateTime::<Utc>::from_naive_utc_and_offset(time, Utc)
//         .to_rfc3339_opts(SecondsFormat::Micros, true);

//     print!("Found start date: {}", start_date);

//     tracing::debug!(
//         execution_date = %execution_date,
//         start_date = %start_date,
//         "Retrieved start date for historical data"
//     );

//     Ok(start_date)
// }

pub async fn get_start_date(
    client: &Client,
    ticker: &str,
    execution_date: &str,
    trading_days: i64,
) -> Result<String, DatabaseError> {
    // Input validation
    validate_ticker(ticker)?;
    validate_period(trading_days, "Trading days")?;

    // Validate execution_date format
    if !execution_date.contains('T') || !execution_date.contains('Z') {
        return Err(DatabaseError::InvalidInput(
            "Execution date must be in format YYYY-MM-DDT16:00:00.000000Z".to_string(),
        ));
    }

    tracing::debug!(
        "Starting get_start_date for ticker: {}, execution_date: {}",
        ticker,
        execution_date
    );

    let query = format!(
        "SELECT min(time) as start_date
        FROM (
            SELECT time
            FROM stock_data_daily
            WHERE ticker = $1
            AND time <= '{}'
            ORDER BY time DESC
            LIMIT {}
        ) AS subquery",
        &execution_date,
        trading_days // Interpolate the limit directly
    );

    // Now only passing one parameter (ticker)
    let row = client
        .query_one(&query, &[&ticker])
        .await
        .map_err(|e| match e {
            e if e.as_db_error().map_or(false, |dbe| {
                dbe.code() == &tokio_postgres::error::SqlState::NO_DATA
            }) =>
            {
                DatabaseError::InsufficientData(format!(
                    "No data found for ticker {} before {}",
                    ticker, execution_date
                ))
            }
            other => DatabaseError::PostgresError(other),
        })?;

    // Handle null result
    let time: Option<NaiveDateTime> = row.get("start_date");
    let time = time.ok_or_else(|| {
        DatabaseError::InsufficientData(format!(
            "No valid start date found for {} data points for {}",
            trading_days, ticker
        ))
    })?;

    // Convert to UTC and format
    let start_date = DateTime::<Utc>::from_naive_utc_and_offset(time, Utc)
        .to_rfc3339_opts(SecondsFormat::Micros, true);

    print!(
        "Found start date: {} for ticker: {}, execution_date: {}",
        start_date, ticker, execution_date
    );

    tracing::debug!(
        execution_date = %execution_date,
        start_date = %start_date,
        ticker = %ticker,
        trading_days = %trading_days,
        "Retrieved start date for historical data"
    );

    Ok(start_date)
}

// Validation functions remain unchanged
fn validate_ticker(ticker: &str) -> Result<(), DatabaseError> {
    if ticker.trim().is_empty() || ticker.len() > 10 {
        return Err(DatabaseError::InvalidTicker);
    }

    if !ticker
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '.' || c == '-')
    {
        print!("\nError to validate ticker {}", ticker);
        return Err(DatabaseError::InvalidTicker);
    }

    Ok(())
}

fn validate_date_range(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<(), DatabaseError> {
    if start_date >= end_date {
        Err(DatabaseError::InvalidDateRange)
    } else {
        Ok(())
    }
}

fn validate_period(period: i64, context: &str) -> Result<(), DatabaseError> {
    if period <= 0 {
        return Err(DatabaseError::InvalidPeriod(format!(
            "{} must be positive",
            context
        )));
    }
    // Different limits for different periods based on context
    let max_period = match context {
        "EMA period" => 500,   // Allow up to 500 days for EMA
        "Trading days" => 500, // Also allow up to 500 days for trading days
        _ => 260,              // Keep default 100 days for other indicators
    };

    if period > max_period {
        return Err(DatabaseError::InvalidPeriod(format!(
            "{} too large, maximum is {}",
            context, max_period
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct SMAResult {
    sma: f64,
}

pub async fn get_sma(
    client: &Client,
    ticker: &str,         // Changed from &String to &str
    execution_date: &str, // Changed from &String to &str
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(ticker)?;
    validate_period(period, "SMA period")?;

    tracing::debug!("Getting start date for SMA calculation"); // Better logging
    print!("\nTry to get start date for SMA calculation\n");
    let start_date = get_start_date(client, ticker, execution_date, period).await?;
    print!("\nReceived start date for SMA calculation\n");
    tracing::debug!("Retrieved start date for SMA calculation");

    let query = format!(
        r#"
        SELECT avg(close) OVER (
            PARTITION BY ticker
            ORDER BY time
            ROWS BETWEEN {} PRECEDING AND CURRENT ROW
        ) AS sma
        FROM stock_data_daily
        WHERE ticker = $1
        AND time BETWEEN '{}'
        AND '{}'
        ORDER BY time DESC
        LIMIT 1
        "#,
        period - 1,
        start_date,
        execution_date
    );

    let row = client
        .query_one(&query, &[&ticker]) // Removed &start_date and &execution_date since they're interpolated
        .await
        .map_err(|e| match e {
            e if e.as_db_error().map_or(false, |dbe| {
                dbe.code() == &tokio_postgres::error::SqlState::NO_DATA
            }) =>
            {
                DatabaseError::InsufficientData(format!(
                    "No data found for {} between {} and {}",
                    ticker, start_date, execution_date
                ))
            }
            other => DatabaseError::PostgresError(other),
        })?;

    let sma: f64 = row.get("sma");
    if !sma.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "SMA calculation resulted in invalid value".to_string(),
        ));
    }

    tracing::debug!(ticker, %start_date, %execution_date, %sma, "SMA calculation completed");
    Ok(sma)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentPrice {
    pub time: NaiveDateTime,
    pub ticker: String,
    pub close: f64,
}

pub async fn get_current_price(
    client: &Client,
    ticker: &str,
    execution_date: &str,
) -> Result<CurrentPrice, DatabaseError> {
    validate_ticker(&ticker)?;

    // Log the query parameters
    print!(
        "Querying current price for ticker: {} on date: {} ",
        ticker, execution_date
    );

    // Interpolate execution_date into the query string
    let query = format!(
        "SELECT time, ticker, close
             FROM stock_data_daily
             WHERE ticker = $1
             AND time = '{}'",
        execution_date
    );

    let row = client
        .query_one(&query, &[&ticker])
        .await
        .map_err(|e| match e {
            e if e.as_db_error().map_or(false, |dbe| {
                dbe.code() == &tokio_postgres::error::SqlState::NO_DATA
            }) =>
            {
                DatabaseError::InsufficientData(format!(
                    "No price data found for {} at {}",
                    ticker, execution_date
                ))
            }
            other => DatabaseError::PostgresError(other),
        })?;

    // Log the result
    let time: chrono::NaiveDateTime = row.get("time");
    let ticker: String = row.get("ticker");
    let close: f64 = row.get("close");
    print!(
        "\nFound price data: Time: {}, Ticker: {}, Close: {}\n",
        time, ticker, close
    );

    Ok(CurrentPrice {
        time,
        ticker,
        close,
    })
}
#[derive(Debug)]
struct CumulativeReturnResult {
    return_percentage: f64,
}

pub async fn get_cumulative_return(
    client: &Client,
    ticker: &str, // Changed from &String to &str for more flexibility
    execution_date: &str,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(ticker)?;
    validate_period(period, "Return period")?;

    let start_date = get_start_date(client, ticker, execution_date, period).await?;

    // Modified query to use string interpolation for dates with single quotes for QuestDB
    let query = format!(
        r#"
        WITH period_prices AS (
            SELECT
                first(close) as start_price,
                last(close) as end_price
            FROM stock_data_daily
            WHERE ticker = $1
            AND time BETWEEN '{}'
            AND '{}'
        )
        SELECT
            ((end_price - start_price) / start_price * 100) as return_percentage
        FROM period_prices
        "#,
        start_date, execution_date
    );

    // Removed date parameters since they're now interpolated in the query
    let row = client
        .query_one(&query, &[&ticker])
        .await
        .map_err(|e| match e {
            e if e.as_db_error().map_or(false, |dbe| {
                dbe.code() == &tokio_postgres::error::SqlState::NO_DATA
            }) =>
            {
                DatabaseError::InsufficientData(format!(
                    "No price data found for {} between {} and {}",
                    ticker, start_date, execution_date
                ))
            }
            other => DatabaseError::PostgresError(other),
        })?;

    let return_percentage: f64 = row.get("return_percentage");

    // Added more descriptive error message
    if !return_percentage.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Cumulative return calculation resulted in invalid value".to_string(),
        ));
    }

    // Added debug logging for better observability
    tracing::debug!(
        %ticker,
        %start_date,
        %execution_date,
        %return_percentage,
        "Cumulative return calculation completed"
    );

    Ok(return_percentage)
}
#[derive(Debug)]
struct EMAResult {
    close: f64,
}

pub async fn get_ema(
    client: &Client,
    ticker: &str, // Changed from &String to &str
    execution_date: &str,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(ticker)?;
    validate_period(period, "EMA period")?;

    let start_date = get_start_date(client, ticker, execution_date, period).await?;

    let query = format!(
        r#"
        SELECT
            time,
            close
        FROM stock_data_daily
        WHERE ticker = $1
        AND time BETWEEN '{}'
        AND '{}'
        AND close > 0
        ORDER BY time ASC
        "#,
        start_date, execution_date
    );

    let rows = client
        .query(&query, &[&ticker]) // Removed date parameters since they're interpolated
        .await?;

    if rows.len() < period as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least {} data points for {} between {} and {}",
            period, ticker, start_date, execution_date
        )));
    }

    let prices: Vec<f64> = rows.iter().map(|row| row.get("close")).collect();

    let initial_sma = prices[..period as usize].iter().sum::<f64>() / period as f64;
    let smoothing = 2.0;
    let multiplier = smoothing / (period as f64 + 1.0);

    let mut ema = initial_sma;
    for price in prices[period as usize..].iter() {
        ema = price * multiplier + ema * (1.0 - multiplier);
    }

    if !ema.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "EMA calculation resulted in invalid value".to_string(),
        ));
    }

    tracing::debug!(
        %ticker,
        %start_date,
        %execution_date,
        %period,
        %ema,
        "EMA calculation completed"
    );

    Ok(ema)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrawdownResult {
    pub max_drawdown_percentage: f64,
    pub max_drawdown_value: f64,
    pub peak_price: f64,
    pub trough_price: f64,
    pub peak_time: NaiveDateTime,
    pub trough_time: NaiveDateTime,
}

#[derive(Debug)]
struct PriceResult {
    time: NaiveDateTime,
    close: f64,
}

pub async fn get_max_drawdown(
    client: &Client,
    ticker: &str, // Changed from &String to &str
    execution_date: &str,
    period: i64,
) -> Result<DrawdownResult, DatabaseError> {
    validate_ticker(ticker)?;
    validate_period(period, "Drawdown period")?;

    let start_date = get_start_date(client, ticker, execution_date, period).await?;

    tracing::debug!("Starting max drawdown calculation"); // Replaced println

    // Modified query to use string interpolation for dates with single quotes for QuestDB
    let query = format!(
        r#"
        SELECT
            time,
            close
        FROM stock_data_daily
        WHERE ticker = $1
        AND time BETWEEN '{}'
        AND '{}'
        ORDER BY time ASC
        "#,
        start_date, execution_date
    );

    // Removed date parameters since they're interpolated in the query
    let rows = client.query(&query, &[&ticker]).await?;

    tracing::debug!("Retrieved price data for drawdown calculation"); // Replaced println

    if rows.len() < 2 {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least 2 data points for drawdown calculation for {}",
            ticker
        )));
    }

    let mut max_drawdown = 0.0;
    let mut max_drawdown_value = 0.0;
    let mut peak_price = f64::NEG_INFINITY;
    let mut peak_time = rows[0].get::<_, NaiveDateTime>("time");
    let mut max_drawdown_peak_time = peak_time;
    let mut max_drawdown_trough_time = peak_time;
    let mut max_drawdown_peak_price = 0.0;
    let mut max_drawdown_trough_price = 0.0;

    // Added logging for initialization values
    tracing::debug!(
        initial_time = %peak_time,
        "Initialized drawdown calculation"
    );

    for row in rows.iter() {
        let current_price: f64 = row.get("close");
        let current_time: NaiveDateTime = row.get("time");

        if current_price > peak_price {
            peak_price = current_price;
            peak_time = current_time;
        }

        let drawdown = (peak_price - current_price) / peak_price * 100.0;
        let drawdown_value = peak_price - current_price;

        if drawdown > max_drawdown {
            max_drawdown = drawdown;
            max_drawdown_value = drawdown_value;
            max_drawdown_peak_time = peak_time;
            max_drawdown_trough_time = current_time;
            max_drawdown_peak_price = peak_price;
            max_drawdown_trough_price = current_price;

            // Added logging for new max drawdown
            tracing::debug!(
                %max_drawdown,
                %max_drawdown_value,
                peak_time = %max_drawdown_peak_time,
                trough_time = %max_drawdown_trough_time,
                "New maximum drawdown found"
            );
        }
    }

    let result = DrawdownResult {
        max_drawdown_percentage: max_drawdown,
        max_drawdown_value,
        peak_price: max_drawdown_peak_price,
        trough_price: max_drawdown_trough_price,
        peak_time: max_drawdown_peak_time,
        trough_time: max_drawdown_trough_time,
    };

    // Added final debug log with calculation results
    tracing::debug!(
        %ticker,
        %start_date,
        %execution_date,
        max_drawdown = %result.max_drawdown_percentage,
        peak_time = %result.peak_time,
        trough_time = %result.trough_time,
        "Max drawdown calculation completed"
    );

    Ok(result)
}
// #[derive(Debug)]
// struct MAResult {
//     moving_average: f64,
// }

// pub async fn get_ma_of_price(
//     client: &Client,
//     ticker: &str, // Changed from &String to &str
//     execution_date: &str,
//     period: i64,
// ) -> Result<f64, DatabaseError> {
//     validate_ticker(ticker)?;
//     validate_period(period, "Moving average period")?;

//     let preceding_rows = period - 1;

//     // Modified query to use string interpolation for execution_date with single quotes for QuestDB
//     let query = format!(
//         r#"
//         WITH latest_ma AS (
//             SELECT
//                 time,
//                 ticker,
//                 close,
//                 avg(close) OVER (
//                     PARTITION BY ticker
//                     ORDER BY time
//                     ROWS {} PRECEDING
//                 ) as moving_average
//             FROM stock_data_daily
//             WHERE ticker = $1
//             AND time <= '{}'
//             ORDER BY time DESC
//             LIMIT 1
//         )
//         SELECT moving_average
//         FROM latest_ma
//         "#,
//         preceding_rows, execution_date
//     );

//     // Removed execution_date parameter since it's now interpolated
//     let row = client
//         .query_one(&query, &[&ticker])
//         .await
//         .map_err(|e| match e {
//             e if e.as_db_error().map_or(false, |dbe| {
//                 dbe.code() == &tokio_postgres::error::SqlState::NO_DATA
//             }) =>
//             {
//                 DatabaseError::InsufficientData(format!(
//                     "Insufficient data for {}-day moving average calculation for {}",
//                     period,
//                     ticker // Added ticker to error message for better context
//                 ))
//             }
//             other => DatabaseError::PostgresError(other),
//         })?;

//     let ma: f64 = row.get("moving_average");

//     // Added more descriptive error message
//     if !ma.is_finite() {
//         return Err(DatabaseError::InvalidCalculation(format!(
//             "Moving average calculation for {} resulted in invalid value",
//             ticker
//         )));
//     }

//     // Added debug logging for better observability
//     tracing::debug!(
//         %ticker,
//         %execution_date,
//         %period,
//         %ma,
//         "Moving average calculation completed"
//     );

//     Ok(ma)
// }

#[derive(Debug)]
struct ReturnPriceResult {
    close: f64,
}

pub async fn get_ma_of_returns(
    client: &Client,
    ticker: &str,
    execution_date: &str,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(ticker)?;
    validate_period(period, "Moving average period")?;

    let start_date = get_start_date(client, ticker, execution_date, period + 1).await?;

    tracing::debug!("Starting MA of returns calculation");

    // Ensure dates are properly sanitized
    let query = format!(
        r#"
        SELECT time, close
        FROM stock_data_daily
        WHERE ticker = $1
        AND time BETWEEN '{}'
        AND '{}'
        ORDER BY time ASC
        "#,
        start_date, execution_date
    );

    let rows = client.query(&query, &[&ticker]).await?;

    tracing::debug!("Retrieved price data successfully");

    if rows.len() < 2 {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least 2 price points to calculate returns for {}",
            ticker
        )));
    }

    let prices: Vec<f64> = rows.iter().map(|row| row.get("close")).collect();

    let daily_returns: Vec<f64> = prices
        .windows(2)
        .map(|window| {
            let previous_close = window[0];
            let current_close = window[1];

            if previous_close == 0.0 {
                return Err(DatabaseError::InsufficientData(format!(
                    "Invalid price data for {}: zero price encountered",
                    ticker
                )));
            }

            let daily_return = (current_close - previous_close) / previous_close * 100.0;

            if daily_return.abs() > 100.0 {
                return Err(DatabaseError::InsufficientData(format!(
                    "Suspicious return value detected for {}: {}%",
                    ticker, daily_return
                )));
            }

            Ok(daily_return)
        })
        .collect::<Result<Vec<f64>, DatabaseError>>()?;

    if daily_returns.len() < period as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least {} data points for {}-day MA of returns for {}",
            period, period, ticker
        )));
    }

    let ma_return = daily_returns
        .windows(period as usize)
        .last()
        .map(|window| window.iter().sum::<f64>() / window.len() as f64)
        .ok_or_else(|| {
            DatabaseError::InsufficientData(format!(
                "Failed to calculate moving average of returns for {}",
                ticker
            ))
        })?;

    if !ma_return.is_finite() {
        return Err(DatabaseError::InvalidCalculation(format!(
            "MA of returns calculation for {} resulted in invalid value",
            ticker
        )));
    }

    tracing::debug!(
        %ticker,
        %start_date,
        %execution_date,
        %period,
        %ma_return,
        "MA of returns calculation completed"
    );

    Ok(ma_return)
}
#[derive(Debug)]
struct RSIResult {
    close: f64,
}
pub async fn get_rsi(
    client: &Client,
    ticker: &str, // Changed from &String to &str
    execution_date: &str,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(ticker)?;
    validate_period(period, "RSI period")?;

    let start_date = get_start_date(client, ticker, execution_date, period + 1).await?;

    // Modified query to use string interpolation for dates with single quotes for QuestDB
    let query = format!(
        r#"
        SELECT
            time,
            close
        FROM stock_data_daily
        WHERE ticker = $1
        AND time BETWEEN '{}'
        AND '{}'
        ORDER BY time ASC
        "#,
        start_date, execution_date
    );

    // Removed date parameters since they're interpolated in the query
    let rows = client.query(&query, &[&ticker]).await?;

    if rows.len() < (period + 1) as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Found {} data points but need {} for {}-period RSI calculation for {}",
            rows.len(),
            period + 1,
            period,
            ticker
        )));
    }

    let prices: Vec<f64> = rows.iter().map(|row| row.get("close")).collect();

    // Calculate gains and losses
    let (gains, losses): (Vec<f64>, Vec<f64>) = prices
        .windows(2)
        .map(|window| {
            let change = window[1] - window[0];
            if change > 0.0 {
                (change, 0.0)
            } else {
                (0.0, change.abs())
            }
        })
        .unzip();

    let period_idx = period as usize;
    let avg_gain = gains[..period_idx].iter().sum::<f64>() / period as f64;
    let avg_loss = losses[..period_idx].iter().sum::<f64>() / period as f64;

    // Added logging before RSI calculation
    tracing::debug!(
        %ticker,
        %avg_gain,
        %avg_loss,
        "Calculated average gains and losses for RSI"
    );

    let rsi = match (avg_gain, avg_loss) {
        (g, l) if l == 0.0 && g == 0.0 => Ok(50.0),
        (_, l) if l == 0.0 => Ok(100.0),
        (g, _) if g == 0.0 => Ok(0.0),
        (g, l) => {
            let rs = g / l;
            let rsi = 100.0 - (100.0 / (1.0 + rs));
            if !rsi.is_finite() || rsi < 0.0 || rsi > 100.0 {
                Err(DatabaseError::InvalidCalculation(format!(
                    "RSI calculation for {} resulted in invalid value: {}",
                    ticker, rsi
                )))
            } else {
                Ok(rsi)
            }
        }
    }?;

    // Added final debug log with calculation results
    tracing::debug!(
        %ticker,
        %start_date,
        %execution_date,
        %period,
        %rsi,
        "RSI calculation completed"
    );

    Ok(rsi)
}

/// Calculates the standard deviation of prices for a given stock between two dates.
///
/// # Arguments
/// * `pool` - Database connection pool
/// * `ticker` - Stock ticker symbol
/// * `start_date` - Start of the calculation period
/// * `end_date` - End of the calculation period
///
/// # Returns
/// * `Result<f64, DatabaseError>` - Standard deviation in dollars
///
/// # Examples
/// ```
/// let std_dev = calculate_price_std_dev(
///     &pool,
///     "AAPL".to_string(),
///     start_date,
///     end_date
/// ).await?;
/// println!("Price standard deviation: ${:.2}", std_dev);
/// ```
///
/// #[derive(Debug)]
struct PriceStdDevResult {
    time: DateTime<Utc>,
    close: f64,
}
pub async fn get_price_std_dev(
    client: &Client,
    ticker: &str,
    execution_date: &str,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Moving average period")?;

    let start_date = get_start_date(client, &ticker, &execution_date, period).await?;

    let query = format!(
        r#"
        SELECT
            time,
            close
        FROM stock_data_daily
        WHERE ticker = $1
        AND time BETWEEN '{}'
        AND '{}'
        ORDER BY time ASC
        "#,
        start_date, execution_date
    );

    let rows = client.query(&query, &[&ticker]).await?;

    if rows.len() < 2 {
        return Err(DatabaseError::InsufficientData(
            "Need at least 2 price points to calculate standard deviation".to_string(),
        ));
    }

    let prices: Vec<f64> = rows.iter().map(|row| row.get("close")).collect();
    let mean = prices.iter().sum::<f64>() / prices.len() as f64;

    let variance = prices
        .iter()
        .map(|price| {
            let diff = price - mean;
            diff * diff
        })
        .sum::<f64>()
        / (prices.len() - 1) as f64;

    let std_dev = variance.sqrt();

    if !std_dev.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Standard deviation calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(std_dev)
}

/// Calculates the standard deviation of returns over a specified period.
///
/// # Arguments
/// * `pool` - Database connection pool
/// * `ticker` - Stock ticker symbol
/// * `execution_date` - The end date for the calculation
/// * `period` - Number of days to calculate the return standard deviation
///
/// # Returns
/// * `Result<f64, DatabaseError>` - Standard deviation of returns in percentage
#[derive(Debug)]
struct StdDevPriceResult {
    close: f64,
}

pub async fn get_returns_std_dev(
    client: &Client,
    ticker: &str, // Changed from &String to &str
    execution_date: &str,
    period: i64,
) -> Result<f64, DatabaseError> {
    // Input validation
    validate_ticker(ticker)?;
    validate_period(period, "Return std dev period")?;

    // Get the start date
    let start_date = get_start_date(client, ticker, execution_date, period).await?;

    tracing::debug!("Starting returns standard deviation calculation");

    // Modified query to use string interpolation for dates with single quotes for QuestDB
    let query = format!(
        r#"
        SELECT
            time,
            close
        FROM stock_data_daily
        WHERE ticker = $1
        AND time BETWEEN '{}'
        AND '{}'
        ORDER BY time ASC
        "#,
        start_date, execution_date
    );

    // Fetch all prices for the period
    // Removed date parameters since they're interpolated in the query
    let rows = client.query(&query, &[&ticker]).await?;

    // Check if we have enough data points
    if rows.len() < 2 {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least 2 price points to calculate return standard deviation for {}",
            ticker
        )));
    }

    // Extract close prices
    let prices: Vec<f64> = rows.iter().map(|row| row.get("close")).collect();

    // Calculate daily returns with validation
    let mut daily_returns = Vec::with_capacity(prices.len() - 1);
    for i in 1..prices.len() {
        let previous_close = prices[i - 1];
        let current_close = prices[i];

        if previous_close == 0.0 {
            return Err(DatabaseError::InvalidCalculation(format!(
                "Invalid price data for {}: zero price encountered",
                ticker
            )));
        }

        let daily_return = (current_close - previous_close) / previous_close * 100.0;

        if !daily_return.is_finite() {
            return Err(DatabaseError::InvalidCalculation(format!(
                "Return calculation for {} resulted in invalid value",
                ticker
            )));
        }

        daily_returns.push(daily_return);
    }

    // Calculate mean return
    let mean_return = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;

    tracing::debug!(
        %ticker,
        %mean_return,
        returns_count = daily_returns.len(),
        "Calculated mean return"
    );

    // Calculate variance (sum of squared deviations from mean)
    let variance = daily_returns
        .iter()
        .map(|return_value| {
            let diff = return_value - mean_return;
            diff * diff
        })
        .sum::<f64>()
        / (daily_returns.len() - 1) as f64;

    // Calculate standard deviation
    let std_dev = variance.sqrt();

    // Validate final result
    if !std_dev.is_finite() {
        return Err(DatabaseError::InvalidCalculation(format!(
            "Standard deviation calculation for {} resulted in invalid value",
            ticker
        )));
    }

    // Added final debug log with calculation results
    tracing::debug!(
        %ticker,
        %start_date,
        %execution_date,
        %period,
        %std_dev,
        %mean_return,
        %variance,
        "Returns standard deviation calculation completed"
    );

    Ok(std_dev)
}


use deadpool_postgres::PoolError;
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use deadpool_postgres::{Config, Runtime};
    use tokio_postgres::NoTls;

    #[test]
    fn test_validate_ticker() {
        assert!(validate_ticker("AAPL").is_ok());
        assert!(validate_ticker("").is_err());
        assert!(validate_ticker("TOOLONG").is_err());
        assert!(validate_ticker("aapl").is_err()); // lowercase should fail
    }

    #[test]
    fn test_validate_period() {
        assert!(validate_period(14, "Test").is_ok());
        assert!(validate_period(0, "Test").is_err());
        assert!(validate_period(101, "Test").is_err());
    }

    #[test]
    fn test_validate_date_range() {
        let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 12, 31, 0, 0, 0).unwrap();
        assert!(validate_date_range(start, end).is_ok());
        assert!(validate_date_range(end, start).is_err());
    }

    #[tokio::test]
    async fn test_get_current_price() -> Result<(), DatabaseError> {
        let client = setup_test_client().await?;
        let execution_date = "2015-01-01".to_string();

        let current_price =
            get_current_price(&client, &"AAPL".to_string(), &execution_date).await?;
        assert!(current_price.close > 0.0);
        assert_eq!(current_price.ticker, "AAPL");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_sma() -> Result<(), DatabaseError> {
        let client = setup_test_client().await?;
        let execution_date = "2020-01-01".to_string();

        let sma = get_sma(&client, &"AAPL".to_string(), &execution_date, 20).await?;
        assert!(sma.is_finite());
        assert!(sma > 0.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_ema() -> Result<(), DatabaseError> {
        let client = setup_test_client().await?;
        let execution_date = "2020-01-01".to_string();

        let ema = get_ema(&client, &"AAPL".to_string(), &execution_date, 20).await?;
        assert!(ema.is_finite());
        assert!(ema > 0.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_rsi() -> Result<(), DatabaseError> {
        let client = setup_test_client().await?;
        let execution_date = "2020-01-01".to_string();

        let rsi = get_rsi(&client, &"AAPL".to_string(), &execution_date, 14).await?;
        assert!(rsi >= 0.0 && rsi <= 100.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_ticker() -> Result<(), DatabaseError> {
        let client = setup_test_client().await?;
        let execution_date = "2020-01-01".to_string();

        let result = get_current_price(&client, &"INVALID".to_string(), &execution_date).await;
        assert!(matches!(result, Err(DatabaseError::InsufficientData(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_future_date() -> Result<(), DatabaseError> {
        let client = setup_test_client().await?;
        let future_date = (Utc::now() + chrono::Duration::days(365))
            .format("%Y-%m-%d")
            .to_string();

        let result = get_current_price(&client, &"AAPL".to_string(), &future_date).await;
        assert!(matches!(result, Err(DatabaseError::InsufficientData(_))));

        Ok(())
    }
}
