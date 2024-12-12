use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Row};
use thiserror::Error;

// New type definitions for standardized returns
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Price {
    pub value: f64,
    pub formatted: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Percentage {
    pub value: f64,
    pub formatted: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RSI {
    pub value: i32,
    pub formatted: String,
}

// Helper functions for formatting
fn format_price(value: f64) -> String {
    format!("${:.2}", value)
}

fn format_percentage(value: f64) -> String {
    format!("{:.2}%", value)
}

fn format_rsi(value: f64) -> String {
    format!("{}", value.round() as i32)
}

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
    pub close: Price,
    pub sma: Price,
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

// get_start_date remains unchanged as it returns a String
pub async fn get_start_date(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    trading_days: i64,
) -> Result<String, DatabaseError> {
    // Your existing implementation remains unchanged
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
    .map(|result| DateTime::<Utc>::from_naive_utc_and_offset(result.time, Utc).to_rfc3339())
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

// Validation functions remain unchanged
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
        return Err(DatabaseError::InvalidPeriod(format!(
            "{} too large, maximum is 100",
            context
        )));
    }
    Ok(())
}

// Modified return types for main functions
pub async fn get_sma(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<Price, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "SMA period")?;
    let start_date = get_start_date(pool, &ticker, &execution_date, period).await?;

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

    let value = sqlx::query(&query)
        .bind(&ticker)
        .bind(&start_date)
        .bind(&execution_date)
        .map(|row: sqlx::postgres::PgRow| row.get::<f64, _>("sma"))
        .fetch_one(pool)
        .await?;

    if !value.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "SMA calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(Price {
        value,
        formatted: format_price(value),
    })
}

pub async fn get_current_price(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
) -> Result<Price, DatabaseError> {
    validate_ticker(&ticker)?;

    let value = sqlx::query(
        "SELECT close
         FROM stock_data
         WHERE ticker = $1
         AND time = $2",
    )
    .bind(&ticker)
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| row.get::<f64, _>("close"))
    .fetch_one(pool)
    .await?;

    Ok(Price {
        value,
        formatted: format_price(value),
    })
}

pub async fn get_cumulative_return(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<Percentage, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Return period")?;

    let start_date = get_start_date(&pool, &ticker, &execution_date, period).await?;

    let value = sqlx::query(
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
    .map(|row: sqlx::postgres::PgRow| row.get::<f64, _>("return_percentage"))
    .fetch_one(pool)
    .await?;

    if !value.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(Percentage {
        value,
        formatted: format_percentage(value),
    })
}

pub async fn get_ema(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<Price, DatabaseError> {
    // Your existing EMA calculation logic
    validate_ticker(&ticker)?;
    validate_period(period, "EMA period")?;

    let start_date = get_start_date(pool, &ticker, &execution_date, period).await?;

    let prices = sqlx::query(
        r#"
        SELECT close
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
    .map(|row: sqlx::postgres::PgRow| row.get::<f64, _>("close"))
    .fetch_all(pool)
    .await?;

    if prices.len() < period as usize {
        return Err(DatabaseError::InsufficientData(format!(
            "Need at least {} data points",
            period
        )));
    }

    let initial_sma = prices[..period as usize].iter().sum::<f64>() / period as f64;
    let smoothing = 2.0;
    let multiplier = smoothing / (period as f64 + 1.0);
    let mut value = initial_sma;

    for price in prices[period as usize..].iter() {
        value = price * multiplier + value * (1.0 - multiplier);
    }

    if !value.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "EMA calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(Price {
        value,
        formatted: format_price(value),
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DrawdownResult {
    pub max_drawdown_percentage: Percentage,
    pub max_drawdown_value: Price,
    pub peak_price: Price,
    pub trough_price: Price,
    pub peak_time: NaiveDateTime,
    pub trough_time: NaiveDateTime,
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
        max_drawdown_percentage: Percentage {
            value: max_drawdown,
            formatted: format_percentage(max_drawdown),
        },
        max_drawdown_value: Price {
            value: max_drawdown_value,
            formatted: format_price(max_drawdown_value),
        },
        peak_price: Price {
            value: max_drawdown_peak_price,
            formatted: format_price(max_drawdown_peak_price),
        },
        trough_price: Price {
            value: max_drawdown_trough_price,
            formatted: format_price(max_drawdown_trough_price),
        },
        peak_time: max_drawdown_peak_time,
        trough_time: max_drawdown_trough_time,
    })
}

pub async fn get_ma_of_price(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<Price, DatabaseError> {
    // Your existing ma_of_price calculation logic
    validate_ticker(&ticker)?;
    validate_period(period, "Moving average period")?;

    let preceding_rows = period - 1;
    let query = format!(
        r#"
        WITH latest_ma AS (
            SELECT 
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

    let value = sqlx::query(&query)
        .bind(ticker)
        .bind(execution_date)
        .map(|row: sqlx::postgres::PgRow| row.get::<f64, _>("moving_average"))
        .fetch_one(pool)
        .await?;

    if !value.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(Price {
        value,
        formatted: format_price(value),
    })
}

pub async fn get_ma_of_returns(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<Percentage, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Moving average period")?;
    let start_date = get_start_date(&pool, &ticker, &execution_date, period + 1).await?;

    let execution_timestamp = DateTime::parse_from_rfc3339(execution_date)
        .map_err(|e| DatabaseError::InvalidInput(format!("Invalid date format: {}", e)))?
        .timestamp_micros();

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
    .map(|row: sqlx::postgres::PgRow| row.get::<f64, _>("close"))
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
            let previous_close = window[0];
            let current_close = window[1];

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

    let value = daily_returns
        .windows(period as usize)
        .last()
        .map(|window| window.iter().sum::<f64>() / window.len() as f64)
        .ok_or_else(|| {
            DatabaseError::InsufficientData("Failed to calculate moving average".to_string())
        })?;

    if !value.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(Percentage {
        value,
        formatted: format_percentage(value),
    })
}

pub async fn get_rsi(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<RSI, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "RSI period")?;

    let start_date = get_start_date(pool, &ticker, &execution_date, period + 1).await?;

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
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| row.get::<f64, _>("close"))
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

    let rsi_value = match (avg_gain, avg_loss) {
        (g, l) if l == 0.0 && g == 0.0 => 50.0,
        (_, l) if l == 0.0 => 100.0,
        (g, _) if g == 0.0 => 0.0,
        (g, l) => {
            let rs = g / l;
            let rsi = 100.0 - (100.0 / (1.0 + rs));
            if !rsi.is_finite() || rsi < 0.0 || rsi > 100.0 {
                return Err(DatabaseError::InvalidCalculation(format!(
                    "RSI calculation resulted in invalid value: {}",
                    rsi
                )));
            }
            rsi
        }
    };

    Ok(RSI {
        value: rsi_value.round() as i32,
        formatted: format_rsi(rsi_value),
    })
}

pub async fn get_price_std_dev(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<Price, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Moving average period")?;

    let start_date = get_start_date(pool, &ticker, &execution_date, period).await?;

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
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| row.get::<f64, _>("close"))
    .fetch_all(pool)
    .await?;

    if prices.len() < 2 {
        return Err(DatabaseError::InsufficientData(
            "Need at least 2 price points to calculate standard deviation".to_string(),
        ));
    }

    let mean = prices.iter().sum::<f64>() / prices.len() as f64;
    let variance = prices
        .iter()
        .map(|price| {
            let diff = price - mean;
            diff * diff
        })
        .sum::<f64>()
        / (prices.len() - 1) as f64;

    let value = variance.sqrt();

    if !value.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Standard deviation calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(Price {
        value,
        formatted: format_price(value),
    })
}

pub async fn get_returns_std_dev(
    pool: &Pool<Postgres>,
    ticker: &String,
    execution_date: &String,
    period: i64,
) -> Result<Percentage, DatabaseError> {
    validate_ticker(&ticker)?;
    validate_period(period, "Return std dev period")?;

    let start_date = get_start_date(pool, &ticker, &execution_date, period).await?;

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
    .bind(&execution_date)
    .map(|row: sqlx::postgres::PgRow| row.get::<f64, _>("close"))
    .fetch_all(pool)
    .await?;

    if prices.len() < 2 {
        return Err(DatabaseError::InsufficientData(
            "Need at least 2 price points to calculate return standard deviation".to_string(),
        ));
    }

    let mut daily_returns = Vec::new();
    for i in 1..prices.len() {
        let previous_close = prices[i - 1];
        let current_close = prices[i];

        if previous_close == 0.0 {
            return Err(DatabaseError::InvalidCalculation(
                "Invalid price data: zero price encountered".to_string(),
            ));
        }

        let daily_return = (current_close - previous_close) / previous_close * 100.0;

        if !daily_return.is_finite() {
            return Err(DatabaseError::InvalidCalculation(
                "Return calculation resulted in invalid value".to_string(),
            ));
        }

        daily_returns.push(daily_return);
    }

    let mean_return = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
    let variance = daily_returns
        .iter()
        .map(|return_value| {
            let diff = return_value - mean_return;
            diff * diff
        })
        .sum::<f64>()
        / (daily_returns.len() - 1) as f64;

    let value = variance.sqrt();

    if !value.is_finite() {
        return Err(DatabaseError::InvalidCalculation(
            "Standard deviation calculation resulted in invalid value".to_string(),
        ));
    }

    Ok(Percentage {
        value,
        formatted: format_percentage(value),
    })
}

// Helper function for metric comparison
pub fn can_compare_metrics(metric1: &str, metric2: &str) -> bool {
    let percentage_metrics = [
        "rsi",
        "cumulative_return",
        "ma_of_returns",
        "returns_std_dev",
    ];
    let price_metrics = [
        "current_price",
        "sma",
        "ema",
        "ma_of_price",
        "price_std_dev",
    ];

    (percentage_metrics.contains(&metric1) && percentage_metrics.contains(&metric2))
        || (price_metrics.contains(&metric1) && price_metrics.contains(&metric2))
}

// Your existing tests module remains unchanged
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    // ... rest of your test cases ...
}
