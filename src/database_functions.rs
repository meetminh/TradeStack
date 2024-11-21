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
    #[error("Invalid SMA period: {0}")]
    InvalidSmaPeriod(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StockDataPoint {
    pub time: DateTime<Utc>,
    pub ticker: String,
    pub close: f64,
    pub sma: f64,
}

pub struct StockQuery {
    pub ticker: String,
    pub start_date: DateTime<Utc>,
    pub end_date: DateTime<Utc>,
    pub sma_period: i32,
}

impl StockQuery {
    pub fn new(
        ticker: String,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
        sma_period: i32,
    ) -> Result<Self, DatabaseError> {
        // Validate ticker (basic validation)
        if ticker.trim().is_empty() || ticker.len() > 10 {
            return Err(DatabaseError::InvalidTicker);
        }

        // Validate date range
        if start_date >= end_date {
            return Err(DatabaseError::InvalidDateRange);
        }

        // Validate SMA period
        if sma_period < 1 {
            return Err(DatabaseError::InvalidSmaPeriod(
                "SMA period must be positive".to_string(),
            ));
        }

        Ok(Self {
            ticker,
            start_date,
            end_date,
            sma_period,
        })
    }
}

pub async fn calculate_sma(
    pool: &Pool<Postgres>,
    query: StockQuery,
) -> Result<Vec<StockDataPoint>, DatabaseError> {
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
            AND time < $3
        ORDER BY time
        "#,
        query.ticker,
        query.start_date,
        query.end_date,
        query.sma_period
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
) -> Result<CurrentPrice, DatabaseError> {
    // Validate ticker
    if ticker.trim().is_empty() || ticker.len() > 10 {
        return Err(DatabaseError::InvalidTicker);
    }

    let record = sqlx::query_as!(
        CurrentPrice,
        r#"
        SELECT 
            time,
            ticker,
            close
        FROM stock_data
        WHERE ticker = $1
        ORDER BY time DESC
        LIMIT 1
        "#,
        ticker
    )
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DatabaseError::SqlxError(sqlx::Error::RowNotFound),
        other => DatabaseError::SqlxError(other),
    })?;

    Ok(record)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReturnCalculation {
    pub return_percentage: f64,
}

pub struct ReturnQuery {
    pub ticker: String,
    pub start_date: DateTime<Utc>,
    pub end_date: DateTime<Utc>,
}

impl ReturnQuery {
    pub fn new(
        ticker: String,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<Self, DatabaseError> {
        // Validate ticker
        if ticker.trim().is_empty() || ticker.len() > 10 {
            return Err(DatabaseError::InvalidTicker);
        }

        // Validate date range
        if start_date >= end_date {
            return Err(DatabaseError::InvalidDateRange);
        }

        Ok(Self {
            ticker,
            start_date,
            end_date,
        })
    }
}

pub async fn cummulative_return(
    pool: &Pool<Postgres>,
    query: ReturnQuery,
) -> Result<ReturnCalculation, DatabaseError> {
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
        query.ticker,
        query.start_date,
        query.end_date,
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

#[derive(Debug, Serialize, Deserialize)]
struct PricePoint {
    time: DateTime<Utc>,
    ticker: String,
    close: f64,
}

pub async fn calculate_ema(
    pool: &Pool<Postgres>,
    ticker: String,
    period: i32,
) -> Result<f64, DatabaseError> {
    // Validate inputs
    if ticker.trim().is_empty() || ticker.len() > 10 {
        return Err(DatabaseError::InvalidTicker);
    }
    if period < 1 {
        return Err(DatabaseError::InvalidSmaPeriod(
            "Period must be positive".to_string(),
        ));
    }

    // Fetch the last n prices
    let prices = sqlx::query_as!(
        PricePoint,
        r#"
        SELECT 
            time,
            ticker,
            close
        FROM stock_data
        WHERE ticker = $1
        ORDER BY time DESC
        LIMIT $2
        "#,
        ticker,
        period as i64
    )
    .fetch_all(pool)
    .await?;

    // Check if we have enough data
    if prices.is_empty() {
        return Err(DatabaseError::SqlxError(sqlx::Error::RowNotFound));
    }

    // Reverse the prices to go from oldest to newest
    let prices: Vec<f64> = prices.into_iter().map(|p| p.close).rev().collect();

    // Calculate EMA
    let smoothing = 2.0;
    let multiplier = smoothing / (period as f64 + 1.0);

    // Start with first price as initial EMA
    let mut ema = prices[0];

    // Calculate EMA for each subsequent price
    for price in prices.iter().skip(1) {
        ema = price * multiplier + ema * (1.0 - multiplier);
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
    // Validate inputs
    if ticker.trim().is_empty() || ticker.len() > 10 {
        return Err(DatabaseError::InvalidTicker);
    }
    if period < 2 {
        return Err(DatabaseError::InvalidSmaPeriod(
            "Period must be at least 2 for drawdown calculation".to_string(),
        ));
    }

    // Fetch prices in chronological order
    let prices = sqlx::query_as!(
        PricePoint,
        r#"
        SELECT
            time,
            ticker,
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
        return Err(DatabaseError::SqlxError(sqlx::Error::RowNotFound));
    }

    let mut max_drawdown = 0.0;
    let mut max_drawdown_value = 0.0;
    let mut peak_price = f64::NEG_INFINITY;
    let mut peak_time = prices[0].time;
    let mut max_drawdown_peak_time = prices[0].time;
    let mut max_drawdown_trough_time = prices[0].time;
    let mut max_drawdown_peak_price = 0.0;
    let mut max_drawdown_trough_price = 0.0;

    for price_point in prices.iter() {
        if price_point.close > peak_price {
            peak_price = price_point.close;
            peak_time = price_point.time;
        }
        let drawdown = (peak_price - price_point.close) / peak_price * 100.0;
        let drawdown_value = peak_price - price_point.close;

        if drawdown > max_drawdown {
            max_drawdown = drawdown;
            max_drawdown_value = drawdown_value;
            max_drawdown_peak_time = peak_time;
            max_drawdown_trough_time = price_point.time;
            max_drawdown_peak_price = peak_price;
            max_drawdown_trough_price = price_point.close;
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_stock_query_validation() {
        let start_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let end_date = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Test invalid ticker
        let result = StockQuery::new("".to_string(), start_date, end_date, 10);
        assert!(matches!(result, Err(DatabaseError::InvalidTicker)));

        // Test invalid date range
        let result = StockQuery::new("AAPL".to_string(), end_date, start_date, 10);
        assert!(matches!(result, Err(DatabaseError::InvalidDateRange)));

        // Test invalid SMA period
        let result = StockQuery::new("AAPL".to_string(), start_date, end_date, 0);
        assert!(matches!(result, Err(DatabaseError::InvalidSmaPeriod(_))));

        // Test valid query
        let result = StockQuery::new("AAPL".to_string(), start_date, end_date, 10);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_current_price() -> Result<(), DatabaseError> {
        let pool = create_database_pool().await?;

        let current_price = get_current_price(&pool, "AAPL".to_string()).await?;

        println!(
            "Latest price for {}: ${:.2} at {}",
            current_price.ticker, current_price.close, current_price.time
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_return() -> Result<(), DatabaseError> {
        let pool = create_database_pool().await?;

        let query = ReturnQuery::new(
            "AAPL".to_string(),
            Utc.with_ymd_and_hms(2021, 1, 27, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2021, 2, 1, 0, 0, 0).unwrap(),
        )?;

        let return_calc = cummulative_return(&pool, query).await?;

        println!("Return percentage: {:.2}%", return_calc.return_percentage);

        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_ema() -> Result<(), DatabaseError> {
        let pool = create_database_pool().await?;

        let ema = calculate_ema(&pool, "AAPL".to_string(), 9).await?;

        println!("9-period EMA for AAPL: ${:.2}", ema);

        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_max_drawdown() -> Result<(), DatabaseError> {
        let pool = create_database_pool().await?;

        let drawdown = calculate_max_drawdown(&pool, "AAPL".to_string(), 30).await?;

        println!(
            "Maximum drawdown: {:.2}% (${:.2})\nPeak: ${:.2} at {}\nTrough: ${:.2} at {}",
            drawdown.max_drawdown_percentage,
            drawdown.max_drawdown_value,
            drawdown.peak_price,
            drawdown.peak_time,
            drawdown.trough_price,
            drawdown.trough_time
        );

        Ok(())
    }
}
