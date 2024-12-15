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

//use chrono::Utc;
use chrono::NaiveDateTime;
use sqlx::postgres::PgPoolOptions; // Add Row trait here // Add this import

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

    // QuestDB specific settings:
    // - Higher timeout because QuestDB might take longer to respond
    // - Fewer max connections as QuestDB has connection limits
    // - No persistent connections (QuestDB may drop them)
    PgPoolOptions::new()
        .max_connections(5) // Lower max connections
        .min_connections(0) // Don't maintain persistent connections
        .acquire_timeout(std::time::Duration::from_secs(30)) // Longer timeout
        .idle_timeout(Some(std::time::Duration::from_secs(30))) // Shorter idle timeout
        .max_lifetime(Some(std::time::Duration::from_secs(3600))) // 1 hour max lifetime
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

    // Create and test database pool
    let pool = create_pool().await?;
    let version = sqlx::query_scalar::<_, String>("SELECT version()")
        .fetch_one(&pool)
        .await?;
    info!("Connected to QuestDB version: {}", version);

    // Measure memory usage before execution
    let mut sys = System::new_all();
    sys.refresh_all();
    let process_id = sysinfo::get_current_pid().unwrap();
    let memory_before = sys.process(process_id).unwrap().memory();

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
    let json_str = fs::read_to_string("test_all.json")?;
    let strategy = match validate_json::deserialize_json(&json_str) {
        Ok(strategy) => strategy,
        Err(e) => {
            eprintln!("Error parsing strategy: {}", e);
            return Err(e.into());
        }
    };

    info!("Strategy validation successful");

    // let allocations =
    //     match strategy_executor::execute_strategy(&strategy, &pool, &execution_date).await {
    //         Ok(allocs) => allocs,
    //         Err(e) => {
    //             eprintln!("Error executing strategy: {}", e);
    //             return Err(e.into());
    //         }
    //     };

    // Measure memory usage after execution
    sys.refresh_all();
    let memory_after = sys.process(process_id).unwrap().memory();
    // Print results
    // println!("\nFinal Portfolio Allocations:");
    // println!("----------------------------");
    // for allocation in allocations {
    //     println!(
    //         "Ticker: {:5} | Weight: {:6.2}% | Date: {}",
    //         allocation.ticker,
    //         allocation.weight * 100.0,
    //         allocation.date
    //     );
    // }

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
