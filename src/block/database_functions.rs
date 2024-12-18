use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres, Row};
use thiserror::Error; // Add this at the top with other imports

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("Database error: {0}")]
    SqlxError(#[from] sqlx::Error),
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
pub async fn get_start_date(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    trading_days: i64,
) -> Result<String, DatabaseError> {
    // Still return String as planned
    println!("Starting get_start_date with date: {}", execution_date);
    validate_ticker(&ticker)?;
    validate_period(trading_days, "Trading days")?;

    let start_date = sqlx::query(
        r#"
        SELECT min(time) as start_date
        FROM (
            SELECT time 
            FROM stock_data 
            WHERE ticker = $1 
            AND time <= $2
            ORDER BY time DESC 
            LIMIT $3
        ) AS subquery"#,
    )
    .bind(&ticker)
    .bind(execution_date)
    .bind(trading_days)
    .map(|row: sqlx::postgres::PgRow| StartDateResult {
        time: row.get("start_date"),
    })
    .fetch_one(pool)
    .await
    .map(|result| DateTime::<Utc>::from_naive_utc_and_offset(result.time, Utc).to_rfc3339()) // Convert to String
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::InsufficientData(format!(
            "Not enough historical data available. Requested {} trading days but found fewer.",
            trading_days
        )),
        other => DatabaseError::SqlxError(other),
    })?;

    println!("Found start_date: {}", start_date);
    Ok(start_date)
}

// Validation functions
fn validate_ticker(ticker: &str) -> Result<(), DatabaseError> {
    if ticker.trim().is_empty() || ticker.len() > 10 {
        return Err(DatabaseError::InvalidTicker);
    }

    if !ticker
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '.' || c == '-')
    {
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
    if period > 100 {
        // Consistent upper bound
        return Err(DatabaseError::InvalidPeriod(format!(
            "{} too large, maximum is 100",
            context
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct SMAResult {
    sma: f64,
}

pub async fn get_sma(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "SMA period")?;
    println!("About to get start date...");
    let start_date = get_start_date(pool, &ticker, &execution_date, period).await?;
    println!("Start date: {}", start_date);
    println!("Executing SMA query...");

    // Build query with concrete number for ROWS BETWEEN
    let query = format!(
        r#"
        SELECT avg(close) OVER (
            PARTITION BY ticker
            ORDER BY time
            ROWS BETWEEN {} PRECEDING AND CURRENT ROW
        ) AS sma
        FROM stock_data
        WHERE ticker = $1
        AND time >= $2
        AND time <= $3
        ORDER BY time DESC
        LIMIT 1
        "#,
        period - 1
    );

    let record = sqlx::query(&query)
        .bind(&ticker)
        .bind(&start_date)
        .bind(&execution_date)
        .map(|row: sqlx::postgres::PgRow| SMAResult {
            sma: row.get("sma"),
        })
        .fetch_one(pool)
        .await
        .map(|result| result.sma)
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => DatabaseError::InsufficientData(format!(
                "No data found for {} between {} and {}",
                ticker, start_date, execution_date
            )),
            other => DatabaseError::SqlxError(other),
        })?;

    if !record.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "SMA calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(record)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CurrentPrice {
    pub time: NaiveDateTime, // For timestamp type
    pub ticker: String,      // For symbol type
    pub close: f64,
}

pub async fn get_current_price(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
) -> Result<CurrentPrice, DatabaseError> {
    validate_ticker(&ticker)?;

    sqlx::query(
        "SELECT time, ticker, close
         FROM stock_data
         WHERE ticker = $1
         AND time = $2",
    )
    .bind(&ticker)
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| CurrentPrice {
        time: row.get("time"),
        ticker: row.get("ticker"),
        close: row.get("close"),
    })
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::InsufficientData(format!(
            "No price data found for {} at {}",
            ticker, execution_date
        )),
        other => DatabaseError::SqlxError(other),
    })
}

#[derive(Debug)]
struct CumulativeReturnResult {
    return_percentage: f64,
}

pub async fn get_cumulative_return(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Return period")?;

    let start_date = get_start_date(&pool, &ticker, &execution_date, period).await?;

    let record = sqlx::query(
        r#"
        WITH period_prices AS (
            SELECT
                first(close) as start_price,
                last(close) as end_price
            FROM stock_data
            WHERE ticker = $1
            AND time >= $2
            AND time <= $3
        )
        SELECT
            ((end_price - start_price) / start_price * 100) as return_percentage
        FROM period_prices
        "#,
    )
    .bind(&ticker)
    .bind(&start_date)
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| CumulativeReturnResult {
        return_percentage: row.get("return_percentage"),
    })
    .fetch_one(pool)
    .await
    .map(|result| result.return_percentage)
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::InsufficientData(format!(
            "No price data found for {} between {} and {}",
            ticker, start_date, execution_date
        )),
        other => DatabaseError::SqlxError(other),
    })?;

    Ok(record)
}

#[derive(Debug)]
struct EMAResult {
    close: f64,
}

pub async fn get_ema(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "EMA period")?;

    let start_date = get_start_date(pool, &ticker, &execution_date, period).await?;

    let prices = sqlx::query(
        r#"
        SELECT 
            time,
            close
        FROM stock_data
        WHERE ticker = $1
        AND time >= $2
        AND time <= $3
        AND close > 0
        ORDER BY time ASC
        "#,
    )
    .bind(&ticker)
    .bind(&start_date)
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| EMAResult {
        close: row.get("close"),
    })
    .fetch_all(pool)
    .await?;

    if prices.len() < period as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least {} data points",
            period
        )));
    }

    let prices: Vec<f64> = prices.into_iter().map(|record| record.close).collect();
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

    Ok(ema)
}

#[derive(Debug, Serialize, Deserialize)]
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
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<DrawdownResult, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Drawdown period")?;

    let start_date = get_start_date(pool, &ticker, &execution_date, period).await?;

    println!("Start fetch");
    let prices = sqlx::query(
        r#"
        SELECT
            time,
            close
        FROM stock_data
        WHERE ticker = $1
        AND time >= $2
        AND time <= $3
        ORDER BY time ASC
        "#,
    )
    .bind(&ticker)
    .bind(&start_date)
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| PriceResult {
        time: row.get("time"),
        close: row.get("close"),
    })
    .fetch_all(pool)
    .await?;

    println!("End fetch");

    if prices.len() < 2 {
        return Err(DatabaseError::InsufficientData(
            "Need at least 2 data points".to_string(),
        ));
    }

    let mut max_drawdown = 0.0;
    let mut max_drawdown_value = 0.0;
    let mut peak_price = f64::NEG_INFINITY;
    let mut peak_time = prices[0].time;
    let mut max_drawdown_peak_time = prices[0].time;
    let mut max_drawdown_trough_time = prices[0].time;
    let mut max_drawdown_peak_price = 0.0;
    let mut max_drawdown_trough_price = 0.0;

    for price_record in prices.iter() {
        if price_record.close > peak_price {
            peak_price = price_record.close;
            peak_time = price_record.time;
        }

        let drawdown = (peak_price - price_record.close) / peak_price * 100.0;
        let drawdown_value = peak_price - price_record.close;

        if drawdown > max_drawdown {
            max_drawdown = drawdown;
            max_drawdown_value = drawdown_value;
            max_drawdown_peak_time = peak_time;
            max_drawdown_trough_time = price_record.time;
            max_drawdown_peak_price = peak_price;
            max_drawdown_trough_price = price_record.close;
        }
    }

    Ok(DrawdownResult {
        max_drawdown_percentage: max_drawdown,
        max_drawdown_value,
        peak_price: max_drawdown_peak_price,
        trough_price: max_drawdown_trough_price,
        peak_time: max_drawdown_peak_time,
        trough_time: max_drawdown_trough_time,
    })
}

#[derive(Debug)]
struct MAResult {
    moving_average: f64,
}

pub async fn get_ma_of_price(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Moving average period")?;

    let preceding_rows = period - 1;

    // Build query with concrete number for ROWS clause
    let query = format!(
        r#"
        WITH latest_ma AS (
            SELECT 
                time,
                ticker,
                close,
                avg(close) OVER (
                    PARTITION BY ticker
                    ORDER BY time
                    ROWS {} PRECEDING
                ) as moving_average
            FROM stock_data
            WHERE ticker = $1
            AND time <= $2
            ORDER BY time DESC
            LIMIT 1
        )
        SELECT moving_average
        FROM latest_ma
        "#,
        preceding_rows
    );

    let record = sqlx::query(&query)
        .bind(ticker)
        .bind(execution_date)
        .map(|row: sqlx::postgres::PgRow| MAResult {
            moving_average: row.get("moving_average"),
        })
        .fetch_one(pool)
        .await
        .map(|result| result.moving_average)
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => DatabaseError::InsufficientData(format!(
                "Insufficient data for {}-day moving average calculation",
                period
            )),
            other => DatabaseError::SqlxError(other),
        })?;

    if !record.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(record)
}

#[derive(Debug)]
struct ReturnPriceResult {
    close: f64,
}

pub async fn get_ma_of_returns(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Moving average period")?;
    // Need an extra day for return calculation
    let start_date = get_start_date(&pool, &ticker, &execution_date, period + 1).await?;
    println!("Start get MA Query");

    let execution_timestamp = DateTime::parse_from_rfc3339(execution_date)
        .map_err(|e| DatabaseError::InvalidInput(format!("Invalid date format: {}", e)))?
        .timestamp_micros();

    // Simple SQL query to get just the closing prices
    let prices = sqlx::query(
        r#"
        SELECT close
        FROM stock_data
        WHERE ticker = $1
        AND time >= $2
        AND time <= $3
        ORDER BY time ASC
        "#,
    )
    .bind(&ticker)
    .bind(&start_date)
    .bind(execution_timestamp)
    .map(|row: sqlx::postgres::PgRow| ReturnPriceResult {
        close: row.get("close"),
    })
    .fetch_all(pool)
    .await?;

    println!("Queires prices! succesffully");

    // Basic validation
    if prices.len() < 2 {
        return Err(DatabaseError::InsufficientData(
            "Need at least 2 price points to calculate returns".to_string(),
        ));
    }

    // Calculate daily returns with validation
    let daily_returns: Vec<f64> = prices
        .windows(2)
        .map(|window| {
            let previous_close = window[0].close;
            let current_close = window[1].close;

            if previous_close == 0.0 {
                return Err(DatabaseError::InsufficientData(
                    "Invalid price data: zero price encountered".to_string(),
                ));
            }

            let daily_return = (current_close - previous_close) / previous_close * 100.0;

            if daily_return.abs() > 100.0 {
                return Err(DatabaseError::InsufficientData(format!(
                    "Suspicious return value detected: {}%",
                    daily_return
                )));
            }

            Ok(daily_return)
        })
        .collect::<Result<Vec<f64>, DatabaseError>>()?;

    // Validate we have enough return data
    if daily_returns.len() < period as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least {} data points for {}-day MA",
            period, period
        )));
    }

    // Calculate moving average of returns
    let ma_return = daily_returns
        .windows(period as usize)
        .last()
        .map(|window| window.iter().sum::<f64>() / window.len() as f64)
        .ok_or_else(|| {
            DatabaseError::InsufficientData("Failed to calculate moving average".to_string())
        })?;

    // Final validation
    if !ma_return.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(ma_return)
}

#[derive(Debug)]
struct RSIResult {
    close: f64,
}
pub async fn get_rsi(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "RSI period")?;

    // Wir brauchen period + 1 Tage für die Berechnung der Preisänderungen
    let start_date = get_start_date(pool, &ticker, &execution_date, period + 1).await?;

    let prices = sqlx::query(
        r#"
        SELECT
            time,
            close
        FROM stock_data
        WHERE ticker = $1
        AND time >= $2
        AND time <= $3
        ORDER BY time ASC
        "#,
    )
    .bind(&ticker)
    .bind(&start_date)
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| RSIResult {
        close: row.get("close"),
    })
    .fetch_all(pool)
    .await?;

    if prices.len() < (period + 1) as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Found {} data points but need {} for {}-period RSI calculation",
            prices.len(),
            period + 1,
            period
        )));
    }

    let prices: Vec<f64> = prices.into_iter().map(|p| p.close).collect();

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

    match (avg_gain, avg_loss) {
        (g, l) if l == 0.0 && g == 0.0 => Ok(50.0),
        (_, l) if l == 0.0 => Ok(100.0),
        (g, _) if g == 0.0 => Ok(0.0),
        (g, l) => {
            let rs = g / l;
            let rsi = 100.0 - (100.0 / (1.0 + rs));

            if !rsi.is_finite() || rsi < 0.0 || rsi > 100.0 {
                Err(DatabaseError::InvalidCalculation(format!(
                    "RSI calculation resulted in invalid value: {}",
                    rsi
                )))
            } else {
                Ok(rsi)
            }
        }
    }
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
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<f64, DatabaseError> {
    // Validate inputs
    validate_ticker(&ticker)?;
    validate_period(period, "Moving average period")?;

    let start_date = get_start_date(pool, &ticker, &execution_date, period).await?;

    // Fetch prices
    let prices = sqlx::query(
        r#"
        SELECT
            time,
            close
        FROM stock_data
        WHERE ticker = $1
        AND time >= $2
        AND time <= $3
        ORDER BY time ASC
        "#,
    )
    .bind(&ticker)
    .bind(&start_date)
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| PriceStdDevResult {
        time: row.get("time"),
        close: row.get("close"),
    })
    .fetch_all(pool)
    .await?;

    // Check if we have enough data
    if prices.len() < 2 {
        return Err(DatabaseError::InsufficientData(
            "Need at least 2 price points to calculate standard deviation".to_string(),
        ));
    }

    // Calculate mean
    let prices: Vec<f64> = prices.into_iter().map(|p| p.close).collect();
    let mean = prices.iter().sum::<f64>() / prices.len() as f64;

    // Calculate sum of squared differences
    let variance = prices
        .iter()
        .map(|price| {
            let diff = price - mean;
            diff * diff
        })
        .sum::<f64>()
        / (prices.len() - 1) as f64;

    // Calculate standard deviation
    let std_dev = variance.sqrt();

    // Validate result
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
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<f64, DatabaseError> {
    // Validate inputs
    validate_ticker(&ticker)?;
    validate_period(period, "Return std dev period")?;

    // Calculate start date using the helper function
    let start_date = get_start_date(pool, &ticker, &execution_date, period).await?;

    // Fetch prices
    let prices = sqlx::query(
        r#"
        SELECT
            time,
            close
        FROM stock_data
        WHERE ticker = $1
        AND time >= $2
        AND time <= $3
        ORDER BY time ASC
        "#,
    )
    .bind(&ticker)
    .bind(&start_date)
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| StdDevPriceResult {
        close: row.get("close"),
    })
    .fetch_all(pool)
    .await?;

    // Need at least 2 prices to calculate returns
    if prices.len() < 2 {
        return Err(DatabaseError::InsufficientData(
            "Need at least 2 price points to calculate return standard deviation".to_string(),
        ));
    }

    // Calculate daily returns
    let mut daily_returns = Vec::new();
    for i in 1..prices.len() {
        let previous_close = prices[i - 1].close;
        let current_close = prices[i].close;

        // Avoid division by zero
        if previous_close == 0.0 {
            return Err(DatabaseError::InvalidCalculation(
                "Invalid price data: zero price encountered".to_string(),
            ));
        }

        let daily_return = (current_close - previous_close) / previous_close * 100.0;

        // Validate return value
        if !daily_return.is_finite() {
            return Err(DatabaseError::InvalidCalculation(
                "Return calculation resulted in invalid value".to_string(),
            ));
        }

        daily_returns.push(daily_return);
    }

    // Calculate mean of returns
    let mean_return = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;

    // Calculate sum of squared differences from mean
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
        return Err(DatabaseError::InvalidCalculation(
            "Standard deviation calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(std_dev)
}

pub async fn get_market_cap(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
) -> Result<f64, DatabaseError> {
    // TODO: Implement actual market cap calculation
    // For now, return a dummy value
    Ok(1000000.0)
}

// pub async fn get_sorted_universe(
//     pool: &Pool<Postgres>,
//     universe: &[String],
//     execution_date: &String,
//     sort_config: &SortConfig,
// ) -> Result<Vec<String>, DatabaseError> {
//     // Validate inputs
//     for ticker in universe {
//         validate_ticker(ticker)?;
//     }

//     let mut ticker_values: Vec<(String, f64)> = Vec::new();

//     // Get values for each ticker based on sort function
//     for ticker in universe {
//         let value = match sort_config.function.as_str() {
//             "cumulative_return" => {
//                 get_cumulative_return(
//                     pool,
//                     ticker,
//                     execution_date,
//                     sort_config.params[0].parse().unwrap(),
//                 )
//                 .await?
//             }
//             "current_price" => get_current_price(pool, ticker, execution_date).await?.close,
//             "rsi" => {
//                 get_rsi(
//                     pool,
//                     ticker,
//                     execution_date,
//                     sort_config.params[0].parse().unwrap(),
//                 )
//                 .await?
//             }
//             "sma" => {
//                 get_sma(
//                     pool,
//                     ticker,
//                     execution_date,
//                     sort_config.params[0].parse().unwrap(),
//                 )
//                 .await?
//             }
//             "ema" => {
//                 get_ema(
//                     pool,
//                     ticker,
//                     execution_date,
//                     sort_config.params[0].parse().unwrap(),
//                 )
//                 .await?
//             }
//             "ma_of_price" => {
//                 get_ma_of_price(
//                     pool,
//                     ticker,
//                     execution_date,
//                     sort_config.params[0].parse().unwrap(),
//                 )
//                 .await?
//             }
//             "ma_of_returns" => {
//                 get_ma_of_returns(
//                     pool,
//                     ticker,
//                     execution_date,
//                     sort_config.params[0].parse().unwrap(),
//                 )
//                 .await?
//             }
//             "max_drawdown" => {
//                 get_max_drawdown(
//                     pool,
//                     ticker,
//                     execution_date,
//                     sort_config.params[0].parse().unwrap(),
//                 )
//                 .await?
//                 .max_drawdown_percentage // Use the percentage field
//             }
//             "price_std_dev" => {
//                 get_price_std_dev(
//                     pool,
//                     ticker,
//                     execution_date,
//                     sort_config.params[0].parse().unwrap(),
//                 )
//                 .await?
//             }
//             "returns_std_dev" => {
//                 get_returns_std_dev(
//                     pool,
//                     ticker,
//                     execution_date,
//                     sort_config.params[0].parse().unwrap(),
//                 )
//                 .await?
//             }
//             _ => {
//                 return Err(DatabaseError::InvalidInput(format!(
//                     "Unsupported sort function: {}",
//                     sort_config.function
//                 )))
//             }
//         };
//         ticker_values.push((ticker.clone(), value));
//     }

//     // Sort based on order
//     ticker_values.sort_by(|a, b| {
//         if sort_config.order == "DESC" {
//             b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
//         } else {
//             a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
//         }
//     });

//     Ok(ticker_values
//         .into_iter()
//         .map(|(ticker, _)| ticker)
//         .collect())
// }

// pub async fn apply_weighting(
//     sorted_tickers: Vec<String>,
//     select_config: &SelectConfig,
// ) -> Result<Vec<(String, f64)>, DatabaseError> {
//     if select_config.count as usize > sorted_tickers.len() {
//         return Err(DatabaseError::InvalidInput(format!(
//             "Requested {} tickers but only {} available",
//             select_config.count,
//             sorted_tickers.len()
//         )));
//     }

//     let selected_tickers = match select_config.direction.as_str() {
//         "TOP" => &sorted_tickers[..select_config.count as usize],
//         "BOTTOM" => &sorted_tickers[sorted_tickers.len() - select_config.count as usize..],
//         _ => {
//             return Err(DatabaseError::InvalidInput(format!(
//                 "Invalid direction: {}",
//                 select_config.direction
//             )))
//         }
//     };

//     Ok(selected_tickers
//         .iter()
//         .zip(select_config.weights.iter())
//         .map(|(ticker, weight)| (ticker.clone(), *weight))
//         .collect())
// }

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    async fn setup_test_pool() -> Result<Pool<Postgres>, DatabaseError> {
        PgPoolOptions::new()
            .max_connections(5)
            .connect("postgresql://admin:quest@localhost:9000/qdb")
            .await
            .map_err(DatabaseError::SqlxError)
    }

    // Helper function tests
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

    // Database function tests
    #[tokio::test]
    async fn test_get_current_price() -> Result<(), DatabaseError> {
        let pool = setup_test_pool().await?;
        let execution_date = Utc.with_ymd_and_hms(2015, 1, 1, 0, 0, 0).unwrap();

        let current_price = get_current_price(&pool, "AAPL".to_string(), execution_date).await?;
        assert!(current_price.close > 0.0);
        assert_eq!(current_price.ticker, "AAPL");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_sma() -> Result<(), DatabaseError> {
        let pool = setup_test_pool().await?;
        let execution_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        let sma = get_sma(&pool, "AAPL".to_string(), execution_date, 20).await?;
        assert!(sma.is_finite());
        assert!(sma > 0.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_ema() -> Result<(), DatabaseError> {
        let pool = setup_test_pool().await?;
        let execution_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        let ema = get_ema(&pool, "AAPL".to_string(), execution_date, 20).await?;
        assert!(ema.is_finite());
        assert!(ema > 0.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_cumulative_return() -> Result<(), DatabaseError> {
        let pool = setup_test_pool().await?;
        let execution_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        let return_value =
            get_cumulative_return(&pool, "AAPL".to_string(), execution_date, 20).await?;

        assert!(return_value.is_finite());
        println!("20-day return: {:.2}%", return_value);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_ma_of_price() -> Result<(), DatabaseError> {
        let pool = setup_test_pool().await?;
        let execution_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        let ma = get_ma_of_price(&pool, "AAPL".to_string(), execution_date, 20).await?;
        assert!(ma.is_finite());
        assert!(ma > 0.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_ma_of_returns() -> Result<(), DatabaseError> {
        let pool = setup_test_pool().await?;
        let execution_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        let ma = get_ma_of_returns(&pool, "AAPL".to_string(), execution_date, 20).await?;
        assert!(ma.is_finite());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_rsi() -> Result<(), DatabaseError> {
        let pool = setup_test_pool().await?;
        let execution_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        let rsi = get_rsi(&pool, "AAPL".to_string(), execution_date, 14).await?;
        assert!(rsi >= 0.0 && rsi <= 100.0);

        Ok(())
    }

    // Error case tests
    #[tokio::test]
    async fn test_invalid_ticker() -> Result<(), DatabaseError> {
        let pool = setup_test_pool().await?;
        let execution_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        let result = get_current_price(&pool, "INVALID".to_string(), execution_date).await;
        assert!(matches!(result, Err(DatabaseError::InsufficientData(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_period() -> Result<(), DatabaseError> {
        let pool = setup_test_pool().await?;
        let execution_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        let result = get_sma(&pool, "AAPL".to_string(), execution_date, 0).await;
        assert!(matches!(result, Err(DatabaseError::InvalidPeriod(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_future_date() -> Result<(), DatabaseError> {
        let pool = setup_test_pool().await?;
        let future_date = Utc::now() + chrono::Duration::days(365);

        let result = get_current_price(&pool, "AAPL".to_string(), future_date).await;
        assert!(matches!(result, Err(DatabaseError::InsufficientData(_))));

        Ok(())
    }
}
