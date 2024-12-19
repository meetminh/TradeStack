mod block {
    pub mod database_functions;
    pub mod filter;
}
mod models;
mod strategy_executor;
mod validate_json;

use chrono::TimeZone;
use deadpool_postgres::{Config, Pool};
use std::error::Error;
use std::fs;

use tokio::time::{sleep, Duration};
use tracing::{error, info};

pub fn create_pool() -> Pool {
    print!("Try creating DB POOL");
    let mut cfg = Config::new();
    cfg.host = Some("questdb.go-server-devcontainer.orb.local".to_string());
    cfg.port = Some(8812);
    cfg.user = Some("admin".to_string());
    cfg.password = Some("quest".to_string());
    cfg.dbname = Some("qdb".to_string());

    cfg.create_pool(
        Some(deadpool_postgres::Runtime::Tokio1),
        tokio_postgres::NoTls,
    )
    .expect("Failed to create pool")
}

// Add a struct to hold our query results
use crate::block::database_functions::DatabaseError;
use deadpool_postgres::Client;

async fn query_stock_data(client: &Client) -> Result<(), DatabaseError> {
    info!("Querying stock_data table...");

    let execution_date = "2005-12-12".to_owned();
    let execution_date = format!("{}T16", execution_date);
    print!("New time {}", execution_date);

    let query = format!(
        "SELECT * FROM stock_data_daily WHERE time = '{}' LIMIT 10",
        execution_date
    );

    // Perform the query to select all rows from stock_data, limited to 50 rows
    let rows = client.query(&query, &[]).await.map_err(|e| {
        error!("Failed to query stock_data table: {}", e);
        DatabaseError::PostgresError(e)
    })?;

    // Log the results
    info!(
        "Query successful! Retrieved {} rows from stock_data.",
        rows.len()
    );
    for row in rows {
        let time: chrono::NaiveDateTime = row.get("time");
        let ticker: String = row.get("ticker");
        let close: f64 = row.get("close");
        info!("Time: {}, Ticker: {}, Close: {}", time, ticker, close);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt().init();

    // Create pool and get initial client
    let pool = create_pool();
    let client = pool.get().await?;

    // Test database connection
    let version: String = client.query_one("SELECT version()", &[]).await?.get(0);
    info!("Connected to QuestDB version: {}", version);

    // Initialize test parameters
    let ticker = "AAPL".to_owned();
    let execution_date = "2005-12-12".to_owned();
    let execution_date =
        chrono::DateTime::parse_from_rfc3339(&format!("{}T16:00:00.000000Z", execution_date))
            .expect("Failed to parse datetime");
    let execution_date = execution_date.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    print!("Execution date is: -{}-", &execution_date);
    let period: i64 = 20;
    const PAUSE_DURATION_MS: u64 = 0;

    println!("\n=== Testing Database Functions ===");

    // Query stock_data table
    match query_stock_data(&client).await {
        Ok(_) => info!("Stock data query completed successfully."),
        Err(e) => error!("Stock data query failed: {}", e),
    }

    // Current Price
    match block::database_functions::get_current_price(&client, &ticker, &execution_date).await {
        Ok(price) => info!(
            "Current price for {} on {}: ${:.2}",
            price.ticker, price.time, price.close
        ),
        Err(e) => error!("Failed to get current price: {}", e),
    };

    // SMA
    match block::database_functions::get_sma(&client, &ticker, &execution_date, period).await {
        Ok(sma) => info!("SMA ({}): ${:.2}", period, sma),
        Err(e) => error!("Failed to get SMA: {}", e),
    };
    sleep(Duration::from_millis(PAUSE_DURATION_MS)).await;

    // EMA
    match block::database_functions::get_ema(&client, &ticker, &execution_date, period).await {
        Ok(ema) => info!("EMA ({}): ${:.2}", period, ema),
        Err(e) => error!("Failed to get EMA: {}", e),
    };
    sleep(Duration::from_millis(PAUSE_DURATION_MS)).await;

    // Cumulative Return
    match block::database_functions::get_cumulative_return(
        &client,
        &ticker,
        &execution_date,
        period,
    )
    .await
    {
        Ok(cumulative_return) => info!("Cumulative Return ({}): {:.2}%", period, cumulative_return),
        Err(e) => error!("Failed to get Cumulative Return: {}", e),
    };
    sleep(Duration::from_millis(PAUSE_DURATION_MS)).await;

    // MA of Price
    match block::database_functions::get_ma_of_price(&client, &ticker, &execution_date, period)
        .await
    {
        Ok(ma_price) => info!("MA of Price ({}): ${:.2}", period, ma_price),
        Err(e) => error!("Failed to get MA of Price: {}", e),
    };
    sleep(Duration::from_millis(PAUSE_DURATION_MS)).await;

    // MA of Returns
    match block::database_functions::get_ma_of_returns(&client, &ticker, &execution_date, period)
        .await
    {
        Ok(ma_returns) => info!("MA of Returns ({}): {:.2}%", period, ma_returns),
        Err(e) => error!("Failed to get MA of Returns: {}", e),
    };
    sleep(Duration::from_millis(PAUSE_DURATION_MS)).await;

    // RSI
    match block::database_functions::get_rsi(&client, &ticker, &execution_date, 14).await {
        Ok(rsi) => info!("RSI (14): {:.2}", rsi),
        Err(e) => error!("Failed to get RSI: {}", e),
    };
    sleep(Duration::from_millis(PAUSE_DURATION_MS)).await;

    // Max Drawdown
    match block::database_functions::get_max_drawdown(&client, &ticker, &execution_date, period)
        .await
    {
        Ok(drawdown) => info!("Max Drawdown: {:.2}%", drawdown.max_drawdown_percentage),
        Err(e) => error!("Failed to get Max Drawdown: {}", e),
    };
    sleep(Duration::from_millis(PAUSE_DURATION_MS)).await;

    // Returns Standard Deviation
    match block::database_functions::get_returns_std_dev(&client, &ticker, &execution_date, period)
        .await
    {
        Ok(std_dev) => info!("Returns StdDev: {:.2}%", std_dev),
        Err(e) => error!("Failed to get Returns Standard Deviation: {}", e),
    };

    //  Process strategy from JSON file
    let json_str = fs::read_to_string("test.json")?;
    if json_str.is_empty() {
        return Err("Empty input file".into());
    }

    let strategy = validate_json::deserialize_json(&json_str)?;
    let strategy_execution_date = chrono::Utc
        .with_ymd_and_hms(2020, 11, 06, 16, 0, 0)
        .unwrap()
        .format("%Y-%m-%dT%H:%M:%S.000000Z")
        .to_string();

    info!("Executing strategy...");
    let allocations =
        strategy_executor::execute_strategy(&strategy, &pool, &strategy_execution_date).await?;

    // Print results
    println!("\nFinal Portfolio Allocations:");
    for allocation in allocations {
        println!("{}: {:.2}%", allocation.ticker, allocation.weight * 100.0);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_with_delay() -> Result<(), Box<dyn Error>> {
        let pool = create_pool();
        let client = pool.get().await?;

        let ticker = "AAPL".to_string();
        let execution_date = chrono::Utc
            .with_ymd_and_hms(2005, 12, 12, 5, 0, 0)
            .unwrap()
            .to_rfc3339();

        // Test sequence with delays
        let current_price =
            block::database_functions::get_current_price(&client, &ticker, &execution_date).await?;
        sleep(Duration::from_millis(100)).await;

        assert!(current_price.close > 0.0);

        Ok(())
    }
}
