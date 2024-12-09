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

mod database_functions;
mod json_operations;
mod models;
mod strategy_executor;

//use chrono::Utc;
use chrono::NaiveDateTime;
use sqlx::{postgres::PgPoolOptions, Row}; // Add Row trait here // Add this import

// ... rest of the code remains the same
use chrono::{TimeZone, Utc};
use std::error::Error;
use std::fs;
use std::time::Instant;
use sysinfo::{ProcessExt, System, SystemExt};
use tracing::info; // Add this import

// pub async fn create_pool() -> Result<sqlx::Pool<sqlx::Postgres>, sqlx::Error> {
//     dotenv::dotenv().ok();

//     let database_url =
//         std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");

//     PgPoolOptions::new()
//         .max_connections(5)
//         .acquire_timeout(std::time::Duration::from_secs(3))
//         .connect(&database_url)
//         .await
// }

pub async fn create_pool() -> Result<sqlx::Pool<sqlx::Postgres>, sqlx::Error> {
    dotenv::dotenv().ok();

    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");

    PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(std::time::Duration::from_secs(3))
        .idle_timeout(std::time::Duration::from_secs(30))
        .max_lifetime(std::time::Duration::from_secs(30 * 60)) // 30 minutes
        .connect(&database_url)
        .await
}
// Add a struct to hold our query results
#[derive(Debug)]
struct StockData {
    time: NaiveDateTime,
    open: f64,
    volume: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Measure start time
    let start_time = Instant::now();

    // Create database pool
    let pool = create_pool().await?;
    info!("Connected to QuestDB successfully!");

    // Measure memory usage before execution
    let mut sys = System::new_all();
    sys.refresh_all();
    let process_id = sysinfo::get_current_pid().unwrap();
    let memory_before = sys.process(process_id).unwrap().memory();

    // Test the connection
    // Test the connection
    // Query AAPL data for 2000-2001
    // Test the connection
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&pool)
        .await?;
    info!("Database connection test successful");

    // let ticker = "AAPL".to_string();
    // let execution_date = Utc
    //     .with_ymd_and_hms(2005, 12, 12, 5, 0, 0)
    //     .unwrap()
    //     .to_rfc3339();
    // let period: i64 = 20;

    // // Test all database functions
    // println!("\n=== Testing Database Functions ===");

    // let current_price =
    //     database_functions::get_current_price(&pool, &ticker, &execution_date).await?;
    // println!("Current Price: ${:.2}", current_price.close);

    // let sma = database_functions::get_sma(&pool, &ticker, &execution_date, period).await?;
    // println!("SMA ({}): ${:.2}", period, sma);

    // let ema = database_functions::get_ema(&pool, &ticker, &execution_date, period).await?;
    // println!("EMA ({}): ${:.2}", period, ema);

    // let cumulative_return =
    //     database_functions::get_cumulative_return(&pool, &ticker, &execution_date, period).await?;
    // println!("Cumulative Return ({}): {:.2}%", period, cumulative_return);

    // let ma_price =
    //     database_functions::get_ma_of_price(&pool, &ticker, &execution_date, period).await?;
    // println!("MA of Price ({}): ${:.2}", period, ma_price);

    // let ma_returns =
    //     database_functions::get_ma_of_returns(&pool, &ticker, &execution_date, period).await?;
    // println!("MA of Returns ({}): {:.2}%", period, ma_returns);

    // let rsi = database_functions::get_rsi(&pool, &ticker, &execution_date, 14).await?;
    // println!("RSI (14): {:.2}", rsi);

    // let drawdown =
    //     database_functions::get_max_drawdown(&pool, &ticker, &execution_date, period).await?;
    // println!("Max Drawdown: {:.2}%", drawdown.max_drawdown_percentage);

    // let std_dev =
    //     database_functions::get_returns_std_dev(&pool, &ticker, &execution_date, period).await?;
    // println!("Returns StdDev: {:.2}%", std_dev);

    info!("Database connection test successful");
    // Read strategy from JSON file
    let execution_date = Utc
        .with_ymd_and_hms(2005, 12, 12, 5, 0, 0)
        .unwrap()
        .to_rfc3339();

    // Read and execute strategy
    let json_str = fs::read_to_string("input.json")?;
    let strategy: models::Node = serde_json::from_str(&json_str)?;

    let allocations =
        strategy_executor::execute_strategy(&strategy, &pool, &execution_date).await?;

    // Measure memory usage after execution
    sys.refresh_all();
    let memory_after = sys.process(process_id).unwrap().memory();
    // Print results
    println!("\nFinal Portfolio Allocations:");
    println!("----------------------------");
    for allocation in allocations {
        println!(
            "Ticker: {:5} | Weight: {:6.2}% | Date: {}",
            allocation.ticker,
            allocation.weight * 100.0,
            allocation.date
        );
    }

    // Measure end time
    let duration = start_time.elapsed();
    println!("\nExecution Time: {:?}", duration);
    println!("Memory Usage: {} KB", memory_after - memory_before);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_database_connection() -> Result<(), Box<dyn Error>> {
        let pool = create_pool().await?;

        // Test simple query
        // Test simple query
        sqlx::query_as::<_, (i32,)>("SELECT 1 FROM trades_pg LIMIT 1")
            .fetch_one(&pool)
            .await?;

        Ok(())
    }

    #[test]
    fn test_json_reading() -> Result<(), Box<dyn Error>> {
        let json_str = fs::read_to_string("input.json")?;
        assert!(!json_str.is_empty());
        Ok(())
    }
}
