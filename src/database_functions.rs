use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};
use thiserror::Error;

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

pub async fn get_start_date(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    trading_days: i32,
) -> Result<DateTime<Utc>, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(trading_days, "Trading days")?;

    let start_date = sqlx::query!(
        r#"
        WITH first_available AS (
            SELECT MIN(time) as min_date
            FROM stock_data
            WHERE ticker = $1
        )
        SELECT time
        FROM stock_data, first_available
        WHERE ticker = $1 
        AND time <= $2
        AND time >= min_date
        ORDER BY time DESC
        OFFSET $3
        LIMIT 1
        "#,
        ticker,
        execution_date,
        trading_days as i64
    )
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::InsufficientData(format!(
            "Not enough historical data available. Requested {} trading days but found fewer.",
            trading_days
        )),
        other => DatabaseError::SqlxError(other),
    })?;

    Ok(start_date.time)
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

fn validate_period(period: i32, context: &str) -> Result<(), DatabaseError> {
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

// Main functions
pub async fn get_sma(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    period: i32,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "SMA period")?;

    let start_date = get_start_date(pool, ticker.clone(), execution_date, period).await?;

    let record = sqlx::query!(
        r#"
        WITH sma_calculation AS (
            SELECT 
                time,
                ticker,
                close,
                avg(close) OVER (
                    PARTITION BY ticker
                    ORDER BY time
                    ROWS BETWEEN $4 - 1 PRECEDING AND CURRENT ROW
                ) AS "sma!"
            FROM stock_data
            WHERE ticker = $1 
                AND time >= $2 
                AND time <= $3
        )
        SELECT sma as "sma!"
        FROM sma_calculation
        ORDER BY time DESC
        LIMIT 1
        "#,
        ticker,
        start_date,
        execution_date,
        period
    )
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::InsufficientData(format!(
            "No data found for {} between {} and {}",
            ticker, start_date, execution_date
        )),
        other => DatabaseError::SqlxError(other),
    })?;

    // Validiere das Ergebnis
    if !record.sma.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "SMA calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(record.sma)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CurrentPrice {
    pub time: DateTime<Utc>,
    pub ticker: String,
    pub close: f64,
}

pub async fn get_current_price(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
) -> Result<CurrentPrice, DatabaseError> {
    validate_ticker(&ticker)?;

    sqlx::query_as!(
        CurrentPrice,
        r#"
        SELECT 
            time,
            ticker,
            close
        FROM stock_data
        WHERE ticker = $1
        AND time = $2
        "#,
        ticker,
        execution_date
    )
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::InsufficientData(format!(
            "No price data found for {} at or before {}",
            ticker, execution_date
        )),
        other => DatabaseError::SqlxError(other),
    })
}

pub async fn get_cumulative_return(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    period: i32,
) -> Result<f64, DatabaseError> {
    // Validate inputs
    validate_ticker(&ticker)?;
    validate_period(period, "Return period")?;

    // Calculate start date using the helper function
    let start_date = get_start_date(pool, ticker.clone(), execution_date, period).await?;

    let record = sqlx::query!(
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
            ((end_price - start_price) / start_price * 100) as "return_percentage!"
        FROM period_prices
        "#,
        ticker,
        start_date,
        execution_date,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::InsufficientData(format!(
            "No price data found for {} between {} and {}",
            ticker, start_date, execution_date
        )),
        other => DatabaseError::SqlxError(other),
    })?;

    // Add validation check for the result
    if !record.return_percentage.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(record.return_percentage)
}

pub async fn get_ema(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    period: i32,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "EMA period")?;

    let start_date = get_start_date(pool, ticker.clone(), execution_date, period).await?;

    let prices = sqlx::query!(
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
        ticker,
        start_date,
        execution_date
    )
    .fetch_all(pool)
    .await?;

    if prices.len() < period as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least {} data points",
            period
        )));
    }

    for price in &prices {
        validate_price(price.close, "EMA calculation")?;
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
    pub peak_time: DateTime<Utc>,
    pub trough_time: DateTime<Utc>,
}

pub async fn get_max_drawdown(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    period: i32,
) -> Result<DrawdownResult, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Drawdown period")?;

    let start_date = get_start_date(pool, ticker.clone(), execution_date, period).await?;

    let prices = sqlx::query!(
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
        ticker,
        start_date,
        execution_date
    )
    .fetch_all(pool)
    .await?;

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
pub async fn get_ma_of_price(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    period: i32,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Moving average period")?;

    let start_date = get_start_date(pool, ticker.clone(), execution_date, period).await?;

    let record = sqlx::query!(
        r#"
        WITH latest_ma AS (
            SELECT 
                avg(close) OVER (
                    PARTITION BY ticker
                    ORDER BY time 
                    ROWS $4 PRECEDING
                ) as moving_average
            FROM stock_data
            WHERE ticker = $1
                AND time >= $2
                AND time <= $3
            ORDER BY time DESC
            LIMIT 1
        )
        SELECT moving_average as "moving_average!"
        FROM latest_ma
        "#,
        ticker,
        start_date,
        execution_date,
        period - 1
    )
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::InsufficientData(format!(
            "Insufficient data for {}-day moving average calculation",
            period
        )),
        other => DatabaseError::SqlxError(other),
    })?;

    // Add validation check for the result
    if !record.moving_average.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(record.moving_average)
}

pub async fn get_ma_of_returns(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    period: i32,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Moving average period")?;

    // Wir brauchen einen extra Tag für die Returnberechnung
    let start_date = get_start_date(pool, ticker.clone(), execution_date, period + 1).await?;

    let prices = sqlx::query!(
        r#"
        SELECT time, close
        FROM stock_data
        WHERE ticker = $1
        AND time >= $2
        AND time <= $3
        ORDER BY time ASC
        "#,
        ticker,
        start_date,
        execution_date
    )
    .fetch_all(pool)
    .await?;

    if prices.len() < 2 {
        return Err(DatabaseError::InsufficientData(
            "Need at least 2 price points to calculate returns".to_string(),
        ));
    }

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

    if daily_returns.len() < period as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least {} data points for {}-day MA",
            period, period
        )));
    }

    let ma_return = daily_returns
        .windows(period as usize)
        .last()
        .map(|window| window.iter().sum::<f64>() / window.len() as f64)
        .ok_or_else(|| {
            DatabaseError::InsufficientData("Failed to calculate moving average".to_string())
        })?;

    if !ma_return.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(ma_return)
}

pub async fn get_rsi(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    period: i32,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "RSI period")?;

    // Wir brauchen period + 1 Tage für die Berechnung der Preisänderungen
    let start_date = get_start_date(pool, ticker.clone(), execution_date, period + 1).await?;

    let prices = sqlx::query!(
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
        ticker,
        start_date,
        execution_date
    )
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
pub async fn get_price_std_dev(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    period: i32,
) -> Result<f64, DatabaseError> {
    // Validate inputs
    validate_ticker(&ticker)?;
    validate_period(period, "Moving average period")?;

    let start_date = get_start_date(pool, ticker.clone(), execution_date, period).await?;

    // Fetch prices
    let prices = sqlx::query!(
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
        ticker,
        start_date,
        execution_date
    )
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
pub async fn get_returns_std_dev(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    period: i32,
) -> Result<f64, DatabaseError> {
    // Validate inputs
    validate_ticker(&ticker)?;
    validate_period(period, "Return std dev period")?;

    // Calculate start date using the helper function
    let start_date = get_start_date(pool, ticker.clone(), execution_date, period).await?;

    // Fetch prices
    let prices = sqlx::query!(
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
        ticker,
        start_date,
        execution_date
    )
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
