mod block {
    pub mod database_functions;
    pub mod filter;
}
mod models;
mod strategy_executor;
mod validate_json;

use chrono::{DateTime, TimeZone, Utc};
use deadpool_postgres::{Client, Config, Pool};
use psutil::process::Process;
use std::error::Error;
use std::fs;
use std::time::Instant;
use tokio::time::Duration;
use tracing::{error, info, warn};

// Performance monitoring struct
#[derive(Debug)]
struct PerformanceMetrics {
    elapsed_time: Duration,
    memory_usage: u64, // in bytes
    peak_memory: u64,  // in bytes
}

impl PerformanceMetrics {
    fn new(elapsed: Duration, current_mem: u64, peak_mem: u64) -> Self {
        Self {
            elapsed_time: elapsed,
            memory_usage: current_mem,
            peak_memory: peak_mem,
        }
    }

    fn log(&self) {
        info!(
            "Performance Metrics:\n\tTime: {:.2?}\n\tCurrent Memory: {:.2} MB\n\tPeak Memory: {:.2} MB",
            self.elapsed_time,
            self.memory_usage as f64 / 1_048_576.0,  // Convert to MB
            self.peak_memory as f64 / 1_048_576.0    // Convert to MB
        );
    }
}

// Performance monitoring wrapper
struct PerformanceMonitor {
    start_time: Instant,
    process: Process,
    initial_memory: u64,
    peak_memory: u64,
}

impl PerformanceMonitor {
    fn new() -> Result<Self, Box<dyn Error>> {
        let process = Process::new(std::process::id() as u32)?;
        let initial_memory = process.memory_info()?.rss();
        Ok(Self {
            start_time: Instant::now(),
            process,
            initial_memory,
            peak_memory: initial_memory,
        })
    }

    fn measure(&mut self) -> Result<PerformanceMetrics, Box<dyn Error>> {
        let current_memory = self.process.memory_info()?.rss();
        self.peak_memory = self.peak_memory.max(current_memory);

        Ok(PerformanceMetrics::new(
            self.start_time.elapsed(),
            current_memory - self.initial_memory, // Show only the additional memory used
            self.peak_memory - self.initial_memory,
        ))
    }
}

use crate::block::database_functions::DatabaseError;

// Constants
const PAUSE_DURATION_MS: u64 = 0;
const DEFAULT_RSI_PERIOD: i64 = 14;

// Configuration structs
#[derive(Debug)]
struct DbConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            host: "questdb.go-server-devcontainer.orb.local".to_string(),
            port: 8812,
            user: "admin".to_string(),
            password: "quest".to_string(),
            dbname: "qdb".to_string(),
        }
    }
}

// Database connection management
fn create_pool() -> Pool {
    info!("Creating database connection pool");
    let config = DbConfig::default();

    let mut cfg = Config::new();
    cfg.host = Some(config.host);
    cfg.port = Some(config.port);
    cfg.user = Some(config.user);
    cfg.password = Some(config.password);
    cfg.dbname = Some(config.dbname);

    cfg.create_pool(
        Some(deadpool_postgres::Runtime::Tokio1),
        tokio_postgres::NoTls,
    )
    .expect("Failed to create connection pool")
}

// Query handling
async fn query_stock_data(client: &Client, execution_date: &str) -> Result<(), DatabaseError> {
    info!("Querying stock_data table for date: {}", execution_date);

    let execution_time = format!("{}T16", execution_date);
    let query = format!(
        "SELECT * FROM stock_data_daily WHERE time = '{}' LIMIT 10",
        execution_time
    );

    let rows = client.query(&query, &[]).await.map_err(|e| {
        error!("Failed to query stock_data table: {}", e);
        DatabaseError::PostgresError(e)
    })?;

    log_query_results(&rows);
    Ok(())
}

fn log_query_results(rows: &[tokio_postgres::Row]) {
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
}

// Test execution helper
async fn run_market_analysis(
    client: &Client,
    ticker: &str,
    execution_date: &str,
    period: i64,
) -> Result<(), Box<dyn Error>> {
    use block::database_functions::*;

    // Current Price
    if let Ok(price) = get_current_price(client, ticker, execution_date).await {
        info!(
            "Current price for {} on {}: ${:.2}",
            price.ticker, price.time, price.close
        );
    }

    // Technical Indicators
    // Handle each indicator separately since they might return different types
    let indicators = [
        ("SMA", get_sma(client, ticker, execution_date, period).await),
        ("EMA", get_ema(client, ticker, execution_date, period).await),
        (
            "Cumulative Return",
            get_cumulative_return(client, ticker, execution_date, period).await,
        ),
        (
            "MA of Returns",
            get_ma_of_returns(client, ticker, execution_date, period).await,
        ),
        (
            "RSI",
            get_rsi(client, ticker, execution_date, DEFAULT_RSI_PERIOD).await,
        ),
        (
            "Max Drawdown",
            get_max_drawdown(client, ticker, execution_date, period)
                .await
                .map(|d| d.max_drawdown_percentage),
        ),
    ];

    for (indicator_name, result) in indicators {
        match result {
            Ok(value) => info!("{}: {:.2}", indicator_name, value),
            Err(e) => error!("Failed to get {}: {}", indicator_name, e),
        }
    }

    // Risk Metrics
    if let Ok(drawdown) = get_max_drawdown(client, ticker, execution_date, period).await {
        info!("Max Drawdown: {:.2}%", drawdown.max_drawdown_percentage);
    }

    if let Ok(std_dev) = get_returns_std_dev(client, ticker, execution_date, period).await {
        info!("Returns StdDev: {:.2}%", std_dev);
    }

    // Initialize performance monitoring for the entire program
    let mut perf_monitor = match PerformanceMonitor::new() {
        Ok(monitor) => monitor,
        Err(e) => {
            warn!("Failed to initialize performance monitoring: {}", e);
            return Err(e);
        }
    };

    // Log performance metrics for market analysis
    if let Ok(metrics) = perf_monitor.measure() {
        info!("Market Analysis Performance:");
        metrics.log();
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt().init();

    // Initialize performance monitoring for the entire program
    let mut program_monitor = match PerformanceMonitor::new() {
        Ok(monitor) => monitor,
        Err(e) => {
            warn!("Failed to initialize performance monitoring: {}", e);
            return Err(e);
        }
    };

    // Setup database connection
    let pool = create_pool();
    let client = pool.get().await?;

    // Verify database connection
    let version: String = client.query_one("SELECT version()", &[]).await?.get(0);
    info!("Connected to QuestDB version: {}", version);

    // Test parameters
    // let test_params = TestParameters {
    //     ticker: "AAPL".to_string(),
    //     execution_date: "2024-12-02".to_string(),
    //     period: 20,
    // };

    // Run analysis
    // println!("\n=== Testing Database Functions ===");
    // query_stock_data(&client, &test_params.execution_date).await?;
    // run_market_analysis(
    //     &client,
    //     &test_params.ticker,
    //     &format_execution_date(&test_params.execution_date),
    //     test_params.period,
    // )
    // .await?;

    // Execute strategy from JSON
    execute_strategy_from_file(&pool).await?;

    // Log final program performance metrics
    if let Ok(metrics) = program_monitor.measure() {
        info!("Overall Program Performance:");
        metrics.log();
    }

    Ok(())
}

// Helper structs and functions
struct TestParameters {
    ticker: String,
    execution_date: String,
    period: i64,
}

fn format_execution_date(date: &str) -> String {
    let datetime = chrono::DateTime::parse_from_rfc3339(&format!("{}T16:00:00.000000Z", date))
        .expect("Failed to parse datetime");
    datetime.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

async fn execute_strategy_from_file(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let json_str = fs::read_to_string("test_all.json")?;
    if json_str.is_empty() {
        return Err("Empty input file".into());
    }

    let strategy = validate_json::deserialize_json(&json_str)?;
    let strategy_execution_date = Utc
        .with_ymd_and_hms(2024, 11, 29, 16, 0, 0)
        .unwrap()
        .format("%Y-%m-%dT%H:%M:%S.000000Z")
        .to_string();

    info!("Executing strategy...");
    let allocations =
        strategy_executor::execute_strategy(&strategy, pool, &strategy_execution_date).await?;

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

        let test_params = TestParameters {
            ticker: "AAPL".to_string(),
            execution_date: "2005-12-12".to_string(),
            period: 20,
        };

        let current_price = block::database_functions::get_current_price(
            &client,
            &test_params.ticker,
            &format_execution_date(&test_params.execution_date),
        )
        .await?;

        assert!(current_price.close > 0.0);

        Ok(())
    }
}
