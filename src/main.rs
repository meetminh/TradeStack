// mod market;
// mod portfolio;

// use crate::market::database_functions::{self, DatabaseError};
// use crate::market::database_functions::{
//     get_cumulative_return, get_current_price, get_ema, get_ma_of_returns, get_max_drawdown,
//     get_returns_std_dev, get_rsi, get_sma,
// };
// use crate::portfolio::execution::strategy_executor;
// use portfolio::construction::validate_json;

// use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
// use deadpool_postgres::{Client, Config, Pool};
// use psutil::process::Process;
// use std::error::Error;
// use std::fs;
// use std::time::Instant;
// use tokio::time::Duration;
// use tracing::{error, info, warn};

// use crate::portfolio::execution::time_based_execution::execute_strategy_over_time_span;

// // Performance monitoring struct
// #[derive(Debug)]
// struct PerformanceMetrics {
//     elapsed_time: Duration,
//     memory_usage: u64, // in bytes
//     peak_memory: u64,  // in bytes
// }

// impl PerformanceMetrics {
//     fn new(elapsed: Duration, current_mem: u64, peak_mem: u64) -> Self {
//         Self {
//             elapsed_time: elapsed,
//             memory_usage: current_mem,
//             peak_memory: peak_mem,
//         }
//     }

//     fn log(&self) {
//         info!(
//             "Performance Metrics:\n\tTime: {:.2?}\n\tCurrent Memory: {:.2} MB\n\tPeak Memory: {:.2} MB",
//             self.elapsed_time,
//             self.memory_usage as f64 / 1_048_576.0,  // Convert to MB
//             self.peak_memory as f64 / 1_048_576.0    // Convert to MB
//         );
//     }
// }

// // Performance monitoring wrapper
// struct PerformanceMonitor {
//     start_time: Instant,
//     process: Process,
//     initial_memory: u64,
//     peak_memory: u64,
// }

// impl PerformanceMonitor {
//     fn new() -> Result<Self, Box<dyn Error>> {
//         let process = Process::new(std::process::id() as u32)?;
//         let initial_memory = process.memory_info()?.rss();
//         Ok(Self {
//             start_time: Instant::now(),
//             process,
//             initial_memory,
//             peak_memory: initial_memory,
//         })
//     }

//     fn measure(&mut self) -> Result<PerformanceMetrics, Box<dyn Error>> {
//         let current_memory = self.process.memory_info()?.rss();
//         self.peak_memory = self.peak_memory.max(current_memory);

//         Ok(PerformanceMetrics::new(
//             self.start_time.elapsed(),
//             current_memory - self.initial_memory, // Show only the additional memory used
//             self.peak_memory - self.initial_memory,
//         ))
//     }
// }

// // Constants
// const PAUSE_DURATION_MS: u64 = 0;
// const DEFAULT_RSI_PERIOD: i64 = 14;

// // Configuration structs
// #[derive(Debug)]
// struct DbConfig {
//     host: String,
//     port: u16,
//     user: String,
//     password: String,
//     dbname: String,
// }

// impl Default for DbConfig {
//     fn default() -> Self {
//         Self {
//             host: "questdb.orb.local".to_string(),
//             port: 8812,
//             user: "admin".to_string(),
//             password: "quest".to_string(),
//             dbname: "qdb".to_string(),
//         }
//     }
// }

// // Database connection management
// fn create_pool() -> Pool {
//     info!("Creating database connection pool");
//     let config = DbConfig::default();

//     let mut cfg = Config::new();
//     cfg.host = Some(config.host);
//     cfg.port = Some(config.port);
//     cfg.user = Some(config.user);
//     cfg.password = Some(config.password);
//     cfg.dbname = Some(config.dbname);

//     cfg.create_pool(
//         Some(deadpool_postgres::Runtime::Tokio1),
//         tokio_postgres::NoTls,
//     )
//     .expect("Failed to create connection pool")
// }

// // Helper function to parse a date string into a NaiveDate
// fn parse_date(date_str: &str) -> Result<NaiveDate, chrono::format::ParseError> {
//     NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
// }

// // Helper function to format a NaiveDate into an ISO 8601 string
// fn format_date_iso(date: NaiveDate) -> String {
//     date.format("%Y-%m-%d").to_string()
// }

// // Helper function to create a DateTime<Utc> from a NaiveDate and time
// fn create_datetime(date: NaiveDate, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
//     Utc.from_local_datetime(&date.and_hms_opt(hour, minute, second).unwrap())
//         .unwrap()
// }

// // Query handling
// async fn query_stock_data(client: &Client, execution_date: &str) -> Result<(), DatabaseError> {
//     info!("Querying stock_data table for date: {}", execution_date);

//     // Parse the execution date into a NaiveDate
//     let execution_date = parse_date(execution_date)?;

//     // Format the execution time as an ISO 8601 timestamp
//     let execution_time = format!("{}T16:00:00.000000Z", execution_date.format("%Y-%m-%d"));

//     let query = format!(
//         "SELECT * FROM stock_data_daily WHERE time = '{}' LIMIT 10",
//         execution_time
//     );

//     let rows = client.query(&query, &[]).await.map_err(|e| {
//         error!("Failed to query stock_data table: {}", e);
//         DatabaseError::PostgresError(e)
//     })?;

//     log_query_results(&rows);
//     Ok(())
// }

// fn log_query_results(rows: &[tokio_postgres::Row]) {
//     info!(
//         "Query successful! Retrieved {} rows from stock_data.",
//         rows.len()
//     );

//     for row in rows {
//         let time: chrono::NaiveDateTime = row.get("time");
//         let ticker: String = row.get("ticker");
//         let close: f64 = row.get("close");
//         info!("Time: {}, Ticker: {}, Close: {}", time, ticker, close);
//     }
// }

// // Test execution helper
// async fn run_market_analysis(
//     client: &Client,
//     ticker: &str,
//     execution_date: &str,
//     period: i64,
// ) -> Result<(), Box<dyn Error>> {
//     // Parse the execution date into a NaiveDate
//     let execution_date = parse_date(execution_date)?;

//     // Format the execution date as an ISO 8601 timestamp
//     let execution_time = create_datetime(execution_date, 16, 0, 0)
//         .format("%Y-%m-%dT%H:%M:%S.000000Z")
//         .to_string();

//     // Get the current price
//     let price_result = get_current_price(client, ticker, &execution_time).await;
//     match price_result {
//         Ok(price) => info!(
//             "Current price for {} on {}: ${:.2}",
//             price.ticker, price.time, price.close
//         ),
//         Err(e) => error!("Failed to get current price: {}", e),
//     }

//     // Technical Indicators
//     let indicators = [
//         (
//             "SMA",
//             get_sma(client, ticker, &execution_time, period).await,
//         ),
//         (
//             "EMA",
//             get_ema(client, ticker, &execution_time, period).await,
//         ),
//         (
//             "Cumulative Return",
//             get_cumulative_return(client, ticker, &execution_time, period).await,
//         ),
//         (
//             "MA of Returns",
//             get_ma_of_returns(client, ticker, &execution_time, period).await,
//         ),
//         (
//             "RSI",
//             get_rsi(client, ticker, &execution_time, DEFAULT_RSI_PERIOD).await,
//         ),
//         (
//             "Max Drawdown",
//             get_max_drawdown(client, ticker, &execution_time, period)
//                 .await
//                 .map(|d| d.max_drawdown_percentage),
//         ),
//     ];

//     for (indicator_name, result) in indicators {
//         match result {
//             Ok(value) => info!("{}: {:.2}", indicator_name, value),
//             Err(e) => error!("Failed to get {}: {}", indicator_name, e),
//         }
//     }

//     // Risk Metrics
//     if let Ok(drawdown) = get_max_drawdown(client, ticker, &execution_time, period).await {
//         info!("Max Drawdown: {:.2}%", drawdown.max_drawdown_percentage);
//     }

//     if let Ok(std_dev) = get_returns_std_dev(client, ticker, &execution_time, period).await {
//         info!("Returns StdDev: {:.2}%", std_dev);
//     }

//     Ok(())
// }

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn Error>> {
//     // Initialize logging
//     tracing_subscriber::fmt().init();

//     // Initialize performance monitoring for the entire program
//     let mut program_monitor = match PerformanceMonitor::new() {
//         Ok(monitor) => monitor,
//         Err(e) => {
//             warn!("Failed to initialize performance monitoring: {}", e);
//             return Err(e);
//         }
//     };

//     // Setup database connection
//     let pool = create_pool();
//     let client = pool.get().await?;

//     // Verify database connection
//     let version: String = client.query_one("SELECT version()", &[]).await?.get(0);
//     info!("Connected to QuestDB version: {}", version);

//     // Test parameters
//     let test_params = TestParameters {
//         ticker: "AAPL".to_string(),
//         execution_date: "2024-12-02".to_string(),
//         period: 20,
//     };

//     // Run analysis
//     println!("\n=== Testing Database Functions ===");
//     query_stock_data(&client, &test_params.execution_date).await?;
//     run_market_analysis(
//         &client,
//         &test_params.ticker,
//         &test_params.execution_date,
//         test_params.period,
//     )
//     .await?;

//     // Execute strategy from JSON (commented out)
//     // execute_strategy_from_file(&pool).await?;

//     // Execute strategy over a time span
//     let start_date = parse_date("2023-01-01")?; // Parse into NaiveDate
//     let end_date = Some(parse_date("2025-01-01")?); // Parse into Option<NaiveDate>

//     // Format dates for display or queries
//     let start_date_iso = format_date_iso(start_date);
//     let end_date_iso = end_date.map(|d| format_date_iso(d));

//     // Create a DateTime<Utc> for strategy execution
//     let strategy_execution_date = create_datetime(parse_date("2024-12-31")?, 16, 0, 0)
//         .format("%Y-%m-%dT%H:%M:%S.000000Z")
//         .to_string();

//     let json_str = fs::read_to_string("printing.json")?;
//     if json_str.is_empty() {
//         return Err("Empty input file".into());
//     }

//     let strategy = validate_json::deserialize_json(&json_str)?;

//     let results = execute_strategy_over_time_span(
//         &pool,
//         &strategy,
//         &start_date_iso,
//         end_date_iso.as_deref(),
//         "monthly",
//     )
//     .await?;

//     // Print results
//     println!("\nPortfolio Allocations Over Time:");
//     for result in results {
//         println!("Display Date: {}", result.display_date);
//         println!("Execution Date: {}", result.execution_date);
//         for allocation in result.allocations {
//             println!("  {}: {:.2}%", allocation.ticker, allocation.weight * 100.0);
//         }
//     }
//     // Log final program performance metrics
//     if let Ok(metrics) = program_monitor.measure() {
//         info!("Overall Program Performance:");
//         metrics.log();
//     }

//     Ok(())
// }

// // Helper structs and functions
// struct TestParameters {
//     ticker: String,
//     execution_date: String,
//     period: i64,
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_with_delay() -> Result<(), Box<dyn Error>> {
//         let pool = create_pool();
//         let client = pool.get().await?;

//         let test_params = TestParameters {
//             ticker: "AAPL".to_string(),
//             execution_date: "2005-12-12".to_string(),
//             period: 20,
//         };

//         let current_price = market::database_functions::get_current_price(
//             &client,
//             &test_params.ticker,
//             &test_params.execution_date,
//         )
//         .await?;

//         assert!(current_price.close > 0.0);

//         Ok(())
//     }
// }

use trade_stack::market::database_functions::{self, DatabaseError};
use trade_stack::portfolio::construction::validate_json;
use trade_stack::portfolio::execution::strategy_executor;

use chrono::{NaiveDate, Utc};
use deadpool_postgres::{Client, Config, Pool};
use psutil::process::Process;
use std::error::Error;
use std::fs;
use std::time::Instant;
use tokio::time::Duration;
use tracing::{error, info, warn};

use trade_stack::portfolio::execution::sequential_execution::execute_strategy_over_time_span_sequential;
use trade_stack::portfolio::execution::time_based_execution::{
    execute_strategy_over_time_span, ExecutionResult,
};

// Define `create_pool`
fn create_pool() -> Pool {
    let config = Config {
        host: Some("questdb.orb.local".to_string()),
        port: Some(8812),
        user: Some("admin".to_string()),
        password: Some("quest".to_string()),
        dbname: Some("qdb".to_string()),
        ..Default::default()
    };

    config
        .create_pool(
            Some(deadpool_postgres::Runtime::Tokio1),
            tokio_postgres::NoTls,
        )
        .expect("Failed to create connection pool")
}

// Define `PerformanceMonitor`
struct PerformanceMonitor {
    start_time: Instant,
    process: Process,
    initial_memory: u64,
    peak_memory: u64,
}

impl PerformanceMonitor {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let process = Process::new(std::process::id() as u32)?;
        let initial_memory = process.memory_info()?.rss();
        Ok(Self {
            start_time: Instant::now(),
            process,
            initial_memory,
            peak_memory: initial_memory,
        })
    }

    fn measure(&mut self) -> Result<PerformanceMetrics, Box<dyn std::error::Error>> {
        let current_memory = self.process.memory_info()?.rss();
        self.peak_memory = self.peak_memory.max(current_memory);

        Ok(PerformanceMetrics::new(
            self.start_time.elapsed(),
            current_memory - self.initial_memory,
            self.peak_memory - self.initial_memory,
        ))
    }
}

#[derive(Debug)]
struct PerformanceMetrics {
    elapsed_time: std::time::Duration,
    memory_usage: u64,
    peak_memory: u64,
}

impl PerformanceMetrics {
    fn new(elapsed: std::time::Duration, current_mem: u64, peak_mem: u64) -> Self {
        Self {
            elapsed_time: elapsed,
            memory_usage: current_mem,
            peak_memory: peak_mem,
        }
    }

    fn log(&self, label: &str) {
        println!(
            "{} Performance Metrics:\n\tTime: {:.2?}\n\tCurrent Memory: {:.2} MB\n\tPeak Memory: {:.2} MB",
            label,
            self.elapsed_time,
            self.memory_usage as f64 / 1_048_576.0,  // Convert to MB
            self.peak_memory as f64 / 1_048_576.0    // Convert to MB
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt().init();

    // Setup database connection
    let pool = create_pool();

    // Load strategy from JSON
    let json_str = fs::read_to_string("printing.json")?;
    let strategy = validate_json::deserialize_json(&json_str)?;

    let start_date = "2015-01-01";
    let end_date = Some("2025-01-01");

    //Execute sequential version
    // let mut sequential_monitor = PerformanceMonitor::new()?;
    // let sequential_results = execute_strategy_over_time_span_sequential(
    //     &pool, &strategy, start_date, end_date, "monthly",
    // )
    // .await?;
    // let sequential_metrics = sequential_monitor.measure()?;
    // sequential_metrics.log("Sequential");

    // // Print sequential results
    // println!("\nSequential Results:");
    //print_results(&sequential_results);

    // Execute parallel version
    let mut parallel_monitor = PerformanceMonitor::new()?;
    let parallel_results =
        execute_strategy_over_time_span(&pool, &strategy, start_date, end_date, "monthly").await?;
    let parallel_metrics = parallel_monitor.measure()?;
    parallel_metrics.log("Parallel");

    // Convert parallel_results to the expected format
    let converted_parallel_results = convert_execution_results(parallel_results);

    // Print parallel results
    println!("\nParallel Results:");
    print_results(&converted_parallel_results);

    Ok(())
}

/// Helper function to print results in a readable format
fn print_results(results: &[(String, String, Vec<strategy_executor::Allocation>)]) {
    for (display_date, execution_date, allocations) in results {
        println!("Display Date: {}", display_date);
        println!("Execution Date: {}", execution_date);
        for allocation in allocations {
            println!("  {}: {:.2}%", allocation.ticker, allocation.weight * 100.0);
        }
        println!(); // Add a blank line between entries
    }
}

/// Convert `Vec<ExecutionResult>` to `Vec<(String, String, Vec<Allocation>)>`
fn convert_execution_results(
    results: Vec<ExecutionResult>,
) -> Vec<(String, String, Vec<strategy_executor::Allocation>)> {
    results
        .into_iter()
        .map(|result| {
            (
                result.display_date,
                result.execution_date,
                result.allocations,
            )
        })
        .collect()
}
