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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StockDataPoint {
    pub time: DateTime<Utc>,
    pub ticker: String,
    pub close: f64,
    pub sma: f64,
}

pub async fn calculate_start_date(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    trading_days: i32,
) -> Result<DateTime<Utc>, DatabaseError> {
    // Validate inputs
    validate_ticker(&ticker)?;
    validate_period(trading_days, "Trading days")?;

    let start_date = sqlx::query!(
        r#"
        SELECT time
        FROM stock_data
        WHERE ticker = $1 
        AND time <= $2
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
            "Not enough data points. Requested {} trading days but found fewer.",
            trading_days
        )),
        other => DatabaseError::SqlxError(other),
    })?;

    Ok(start_date.time)
}

// Validation functions
fn validate_ticker(ticker: &str) -> Result<(), DatabaseError> {
    if ticker.trim().is_empty() || ticker.len() > 10 {
        Err(DatabaseError::InvalidTicker)
    } else {
        Ok(())
    }
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
    if period < 1 {
        Err(DatabaseError::InvalidPeriod(format!(
            "{} must be positive",
            context
        )))
    } else {
        Ok(())
    }
}

// Main functions
pub async fn calculate_sma(
    pool: &Pool<Postgres>,
    ticker: String,
    execution_date: DateTime<Utc>,
    period: i32,
) -> Result<Vec<StockDataPoint>, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "SMA period")?;

    let start_date = calculate_start_date(pool, ticker.clone(), execution_date, period).await?;

    let records = sqlx::query_as!(
        StockDataPoint,
        r#"
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
        ORDER BY time
        "#,
        ticker,
        start_date,
        execution_date,
        period
    )
    .fetch_all(pool)
    .await?;

    Ok(records)
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
        AND time <= $2
        ORDER BY time DESC
        LIMIT 1
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

#[derive(Debug, Serialize, Deserialize)]
pub struct ReturnCalculation {
    pub return_percentage: f64,
}

pub async fn calculate_return(
    pool: &Pool<Postgres>,
    ticker: String,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<ReturnCalculation, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_date_range(start_date, end_date)?;

    let record = sqlx::query!(
        r#"
        WITH period_prices AS (
            SELECT 
                first(close) as start_price,
                last(close) as end_price
            FROM stock_data
            WHERE ticker = $1
            AND time >= $2
            AND time < $3
        )
        SELECT 
            ((end_price - start_price) / start_price * 100) as "return_percentage!"
        FROM period_prices
        "#,
        ticker,
        start_date,
        end_date,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::SqlxError(sqlx::Error::RowNotFound),
        other => DatabaseError::SqlxError(other),
    })?;

    Ok(ReturnCalculation {
        return_percentage: record.return_percentage,
    })
}

pub async fn calculate_ema(
    pool: &Pool<Postgres>,
    ticker: String,
    period: i32,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "EMA period")?;

    let prices: Vec<(DateTime<Utc>, f64)> = sqlx::query!(
        r#"
        SELECT 
            time,
            close
        FROM stock_data
        WHERE ticker = $1
        ORDER BY time DESC
        LIMIT $2
        "#,
        ticker,
        period as i64 * 2
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|record| (record.time, record.close))
    .collect();

    if prices.len() < period as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least {} data points",
            period
        )));
    }

    let prices: Vec<f64> = prices.into_iter().map(|(_, price)| price).rev().collect();
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

pub async fn calculate_max_drawdown(
    pool: &Pool<Postgres>,
    ticker: String,
    period: i32,
) -> Result<DrawdownResult, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Drawdown period")?;

    let prices = sqlx::query!(
        r#"
        SELECT
            time,
            close
        FROM stock_data
        WHERE ticker = $1
        ORDER BY time ASC
        LIMIT $2
        "#,
        ticker,
        period as i64
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

pub async fn calculate_moving_average_of_price(
    pool: &Pool<Postgres>,
    ticker: String,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    ma_period: i32,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_date_range(start_date, end_date)?;
    validate_period(ma_period, "Moving average period")?;

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
        end_date,
        ma_period - 1
    )
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::SqlxError(sqlx::Error::RowNotFound),
        other => DatabaseError::SqlxError(other),
    })?;

    Ok(record.moving_average)
}

pub async fn calculate_moving_average_of_returns(
    pool: &Pool<Postgres>,
    ticker: String,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    ma_period: i32,
) -> Result<f64, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_date_range(start_date, end_date)?;
    validate_period(ma_period, "Moving average period")?;

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
        end_date
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

    if daily_returns.len() < ma_period as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least {} data points for {}-day MA",
            ma_period, ma_period
        )));
    }

    let ma_return = daily_returns
        .windows(ma_period as usize)
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

pub async fn calculate_rsi(
    pool: &Pool<Postgres>,
    ticker: String,
    period: i32,
) -> Result<f64, DatabaseError> {
    // Validate inputs
    validate_ticker(&ticker)?;
    validate_period(period, "RSI period")?;

    // Fetch prices with better ordering and limit explanation
    let prices = sqlx::query!(
        r#"
        SELECT
            time,
            close
        FROM stock_data
        WHERE ticker = $1
            AND time >= NOW() - INTERVAL '1 year'  -- Add reasonable time boundary
        ORDER BY time DESC
        LIMIT $2
        "#,
        ticker,
        (period + 1) as i64
    )
    .fetch_all(pool)
    .await?;

    // Early validation with more specific error message
    if prices.len() < (period + 1) as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Found {} data points but need {} for {}-period RSI calculation",
            prices.len(),
            period + 1,
            period
        )));
    }

    // Use iterator chain for better performance
    let prices: Vec<f64> = prices.into_iter().map(|p| p.close).rev().collect();

    // Improved price changes calculation with better memory usage
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

    // Calculate averages with bounds checking
    let period_idx = period as usize;
    let avg_gain = gains
        .get(..period_idx)
        .ok_or_else(|| DatabaseError::InvalidCalculation("Invalid gain slice".to_string()))?
        .iter()
        .sum::<f64>()
        / period as f64;

    let avg_loss = losses
        .get(..period_idx)
        .ok_or_else(|| DatabaseError::InvalidCalculation("Invalid loss slice".to_string()))?
        .iter()
        .sum::<f64>()
        / period as f64;

    // Enhanced edge case handling
    match (avg_gain, avg_loss) {
        (g, l) if l == 0.0 && g == 0.0 => Ok(50.0), // No price change
        (_, l) if l == 0.0 => Ok(100.0),            // Only gains
        (g, _) if g == 0.0 => Ok(0.0),              // Only losses
        (g, l) => {
            let rs = g / l;
            let rsi = 100.0 - (100.0 / (1.0 + rs));

            // Validate final result
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

// Helper function to validate period
fn validate_period(period: i32, name: &str) -> Result<(), DatabaseError> {
    if period <= 0 {
        return Err(DatabaseError::InvalidInput(format!(
            "{} must be positive, got {}",
            name, period
        )));
    }
    if period > 100 {
        // Add reasonable upper bound
        return Err(DatabaseError::InvalidInput(format!(
            "{} too large, maximum is 100, got {}",
            name, period
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[tokio::test]
    async fn test_get_current_price() -> Result<(), DatabaseError> {
        let pool = create_test_db_pool().await?;

        let current_price = get_current_price(&pool, "AAPL".to_string()).await?;
        assert!(current_price.close > 0.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_return() -> Result<(), DatabaseError> {
        let pool = create_test_db_pool().await?;

        let start_date = Utc.with_ymd_and_hms(2021, 1, 1, 0, 0, 0).unwrap();
        let end_date = Utc.with_ymd_and_hms(2021, 2, 1, 0, 0, 0).unwrap();

        let return_calc = calculate_return(&pool, "AAPL".to_string(), start_date, end_date).await?;
        assert!(return_calc.return_percentage.is_finite());

        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_rsi() -> Result<(), DatabaseError> {
        let pool = create_test_db_pool().await?;

        let rsi = calculate_rsi(&pool, "AAPL".to_string(), 14).await?;

        println!("14-day RSI for AAPL: {:.2}%", rsi);
        assert!(rsi >= 0.0 && rsi <= 100.0);

        Ok(())
    }

    // Add more tests as needed...
}