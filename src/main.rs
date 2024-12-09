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
// mod json_operations;
// mod models;
// mod strategy_executor;

//use chrono::Utc;
use chrono::NaiveDateTime;
use sqlx::{postgres::PgPoolOptions, Row}; // Add Row trait here // Add this import

// ... rest of the code remains the same
use chrono::{TimeZone, Utc};
use std::error::Error;
use std::fs;
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

    // Create database pool
    let pool = create_pool().await?;
    info!("Connected to QuestDB successfully!");

    // Test the connection
    // Test the connection
    // Query AAPL data for 2000-2001
    // Test the connection
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&pool)
        .await?;
    info!("Database connection test successful");

    let start_date = Utc.with_ymd_and_hms(2005, 1, 1, 0, 0, 0).unwrap();
    let end_date = Utc.with_ymd_and_hms(2005, 12, 31, 0, 0, 0).unwrap();

    println!("\nQuerying data between dates:");
    println!("Start date: {}", start_date.to_rfc3339());
    println!("End date: {}", end_date.to_rfc3339());

    let date_test_records = sqlx::query(
        "SELECT time, open, volume 
         FROM stock_data 
         WHERE ticker = 'AAPL' 
         AND time BETWEEN $1::text AND $2::text
         ORDER BY time ASC",
    )
    .bind(start_date.to_rfc3339())
    .bind(end_date.to_rfc3339())
    .map(|row: sqlx::postgres::PgRow| StockData {
        time: row.get("time"),
        open: row.get("open"),
        volume: row.get("volume"),
    })
    .fetch_all(&pool)
    .await?;

    println!("\nFound {} records", date_test_records.len());
    println!("\nFirst 5 records:");
    for record in date_test_records.iter().take(5) {
        println!(
            "Date: {}, Open: ${:.2}, Volume: {}",
            record.time, record.open, record.volume
        );
    }

    // Test SMA calculation
    let execution_date = Utc
        .with_ymd_and_hms(2005, 12, 31, 0, 0, 0)
        .unwrap()
        .to_rfc3339();
    let sma = database_functions::get_sma(&pool, "AAPL".to_string(), &execution_date, 100).await?;

    println!("SMA for AAPL: {:.2}", sma);

    // Query AAPL data for 2000-2001
    let records = sqlx::query(
        "SELECT time, open, volume 
         FROM stock_data 
         WHERE ticker = 'AAPL' 
         AND time BETWEEN '2000-01-01' AND '2001-12-31'
         ORDER BY time ASC",
    )
    .map(|row: sqlx::postgres::PgRow| StockData {
        time: row.get("time"),
        open: row.get("open"),
        volume: row.get("volume"),
    })
    .fetch_all(&pool)
    .await?;

    // Print results
    for record in records {
        println!(
            "Date: {}, Open: ${:.2}, Volume: {}",
            record.time, record.open, record.volume
        );
    }
    info!("Database connection test successful");
    // Read strategy from JSON file
    let _json_str = fs::read_to_string("input.json")?;
    info!("Strategy file read successfully");

    // Deserialize and validate strategy
    //let strategy = json_operations::deserialize_json(&json_str)?;
    info!("Strategy deserialized successfully");

    // Execute strategy
    //let execution_date = Utc::now();
    //let allocations = strategy_executor::execute_strategy(&strategy, &pool, execution_date).await?;

    // Print results
    println!("\nFinal Portfolio Allocations:");
    println!("----------------------------");
    // for allocation in allocations {
    //     println!(
    //         "Ticker: {:5} | Weight: {:6.2}% | Date: {}",
    //         allocation.ticker,
    //         allocation.weight * 100.0,
    //         allocation.date.format("%Y-%m-%d %H:%M:%S UTC")
    //     );
    // }

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
