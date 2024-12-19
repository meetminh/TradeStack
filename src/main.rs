// mod json_operations;
// mod models;

// use std::error::Error;
// use std::fs;

// fn main() -> Result<(), Box<dyn Error>> {
//     // Read JSON from file
//     let json_str = fs::read_to_string("input.json")?;

//     println!("Original JSON:");
//     println!("{}", json_str);

//     // Deserialize JSON to Node
//     match json_operations::deserialize_json(&json_str) {
//         Ok(deserialized_tree) => {
//             println!("\nDeserialized structure:");
//             println!("{:#?}", deserialized_tree);

//             // Validate the deserialized tree
//             match json_operations::validate_node(&deserialized_tree) {
//                 Ok(()) => {
//                     println!("Validation successful!");
//                 }
//                 Err(e) => {
//                     println!("Validation Error:");
//                     println!("{}", e);
//                 }
//             }
//         }
//         Err(e) => {
//             println!("\nDeserialization Error:");
//             println!("{}", e);
//         }
//     }

//     Ok(())
// }

mod block {
    pub mod database_functions;
    pub mod filter;
}
mod models;
mod strategy_executor;
mod validate_json;

use chrono::{NaiveDateTime, TimeZone, Utc};
use deadpool_postgres::{Config, Pool, PoolConfig};
use std::error::Error;
use std::fs;
use std::time::Instant;
use sysinfo::{ProcessExt, System, SystemExt};
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
#[derive(Debug)]
struct StockData {
    time: NaiveDateTime,
    open: f64,
    volume: i64,
}

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn Error>> {
//     // Initialize logging
//     tracing_subscriber::fmt::init();

//     // Measure start time
//     let start_time = Instant::now();

//     // Create and test database pool
//     let pool = create_pool().await?;
//     let version = sqlx::query_scalar::<_, String>("SELECT version()")
//         .fetch_one(&pool)
//         .await?;
//     info!("Connected to QuestDB version: {}", version);

//     // Measure memory usage before execution
//     let mut sys = System::new_all();
//     sys.refresh_all();
//     let process_id = sysinfo::get_current_pid().unwrap();
//     let memory_before = sys.process(process_id).unwrap().memory();

//     // // After pool creation, add this:
//     // info!("Testing JSON deserialization...");

//     let ticker = "AAPL".to_string();
//     let execution_date = Utc
//         .with_ymd_and_hms(2005, 12, 12, 5, 0, 0)
//         .unwrap()
//         .to_rfc3339();
//     let period: i64 = 20;

//     // Test all database functions
//     println!("\n=== Testing Database Functions ===");

//     let current_price =
//         block::database_functions::get_current_price(&pool, &ticker, &execution_date).await?;
//     println!("Current Price: ${:.2}", current_price.close);

//     let sma = block::database_functions::get_sma(&pool, &ticker, &execution_date, period).await?;
//     println!("SMA ({}): ${:.2}", period, sma);

//     let ema = block::database_functions::get_ema(&pool, &ticker, &execution_date, period).await?;
//     println!("EMA ({}): ${:.2}", period, ema);

//     let cumulative_return =
//         block::database_functions::get_cumulative_return(&pool, &ticker, &execution_date, period)
//             .await?;
//     println!("Cumulative Return ({}): {:.2}%", period, cumulative_return);

//     let ma_price =
//         block::database_functions::get_ma_of_price(&pool, &ticker, &execution_date, period).await?;
//     println!("MA of Price ({}): ${:.2}", period, ma_price);

//     let ma_returns =
//         block::database_functions::get_ma_of_returns(&pool, &ticker, &execution_date, period)
//             .await?;
//     println!("MA of Returns ({}): {:.2}%", period, ma_returns);

//     let rsi = block::database_functions::get_rsi(&pool, &ticker, &execution_date, 14).await?;
//     println!("RSI (14): {:.2}", rsi);

//     let drawdown =
//         block::database_functions::get_max_drawdown(&pool, &ticker, &execution_date, period)
//             .await?;
//     println!("Max Drawdown: {:.2}%", drawdown.max_drawdown_percentage);

//     let std_dev =
//         block::database_functions::get_returns_std_dev(&pool, &ticker, &execution_date, period)
//             .await?;
//     println!("Returns StdDev: {:.2}%", std_dev);

//     // // Read strategy from JSON file
//     // let execution_date = Utc
//     //     .with_ymd_and_hms(2005, 12, 12, 5, 0, 0)
//     //     .unwrap()
//     //     .to_rfc3339();

//     // // Read and execute strategy
//     // let json_str = fs::read_to_string("input.json")?;
//     // let strategy = match validate_json::deserialize_json(&json_str) {
//     //     Ok(strategy) => strategy,
//     //     Err(e) => {
//     //         eprintln!("Error parsing strategy: {}", e);
//     //         return Err(e.into());
//     //     }
//     // };

//     info!("Strategy validation successful");

//     // Measure memory usage after execution
//     sys.refresh_all();
//     let memory_after = sys.process(process_id).unwrap().memory();

//     // Print results
//     println!("\nFinal Portfolio Allocations:");

//     // Measure end time
//     let duration = start_time.elapsed();
//     println!("\nExecution Time: {:?}", duration);
//     println!("Memory Usage: {} KB", memory_after - memory_before);

//     Ok(())
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_database_connection() -> Result<(), Box<dyn Error>> {
//         let pool = create_pool().await?;

//         // Test simple query
//         // Test simple query
//         sqlx::query_as::<_, (i32,)>("SELECT 1 FROM trades_pg LIMIT 1")
//             .fetch_one(&pool)
//             .await?;

//         Ok(())
//     }

//     #[test]
//     fn test_json_reading() -> Result<(), Box<dyn Error>> {
//         let json_str = fs::read_to_string("input.json")?;
//         assert!(!json_str.is_empty());
//         Ok(())
//     }
// }
use crate::block::database_functions::DatabaseError;
use deadpool_postgres::Client;
use tokio_postgres::Error as PgError;
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
    let json_str = fs::read_to_string("input.json")?;
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
