// use crate::market::database_functions::DatabaseError;
// use crate::portfolio::blocks::models::Block;
// use crate::portfolio::execution::strategy_executor::{execute_strategy, Allocation};
// use chrono::{Datelike, Months, NaiveDate, NaiveDateTime, TimeZone, Utc};
// use deadpool_postgres::{Client, Pool};

// impl From<chrono::format::ParseError> for DatabaseError {
//     fn from(err: chrono::format::ParseError) -> Self {
//         DatabaseError::InvalidInput(err.to_string())
//     }
// }

// async fn get_last_market_open_day_of_previous_month(
//     client: &Client,
//     date: &str,
// ) -> Result<String, DatabaseError> {
//     // Parse the input date
//     let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")?;

//     // Get the last day of the previous month
//     let last_day_of_previous_month = date
//         .with_day(1)
//         .unwrap() // Start of the current month
//         .pred_opt()
//         .unwrap() // Last day of the previous month
//         .format("%Y-%m-%d")
//         .to_string();

//     //println!("Last day of previous month: {}", last_day_of_previous_month);

//     // Check if the last day of the previous month is a market open day
//     let query = format!(
//         r#"
//       SELECT Date, Date_of_Previous_Trading_Day, Is_Holiday, Is_Weekend
//       FROM nasdaq_closed_days
//       WHERE Date = '{}'
//       "#,
//         last_day_of_previous_month
//     );

//     // println!("Executing query: {}", query);

//     // Execute the query and handle the case where no rows are returned
//     let row_result = client.query_opt(&query, &[]).await?;

//     let last_market_open_day = match row_result {
//         Some(row) => {
//             // The date is a closing day (holiday or weekend)
//             let is_holiday: bool = row.get::<_, bool>("Is_Holiday");
//             let is_weekend: bool = row.get::<_, bool>("Is_Weekend");
//             let is_market_open = !is_holiday && !is_weekend;

//             // println!(
//             //     "Is holiday: {}, Is weekend: {}, Is market open: {}",
//             //     is_holiday, is_weekend, is_market_open
//             // );

//             if is_market_open {
//                 // This should not happen since the table only contains closing days
//                 last_day_of_previous_month
//             } else {
//                 // Use the last trading day before the closing day
//                 let previous_trading_day: NaiveDateTime =
//                     row.get::<_, NaiveDateTime>("Date_of_Previous_Trading_Day");
//                 previous_trading_day.format("%Y-%m-%d").to_string()
//             }
//         }
//         None => {
//             // The date is a market open day
//             println!("Date is a market open day: {}", last_day_of_previous_month);
//             last_day_of_previous_month
//         }
//     };

//     // println!("Last market open day: {}", last_market_open_day);

//     // Format the date with the fixed time component
//     let last_market_open_day = Utc
//         .with_ymd_and_hms(
//             last_market_open_day[0..4].parse().unwrap(),
//             last_market_open_day[5..7].parse().unwrap(),
//             last_market_open_day[8..10].parse().unwrap(),
//             16,
//             0,
//             0,
//         )
//         .unwrap()
//         .format("%Y-%m-%dT%H:%M:%S.000000Z")
//         .to_string();

//     // println!("Formatted last market open day: {}", last_market_open_day);

//     Ok(last_market_open_day)
// }

// /// Main function to execute the strategy over a time span
// pub async fn execute_strategy_over_time_span(
//     pool: &Pool,
//     strategy: &Block,
//     start_date: &str,
//     end_date: Option<&str>,
//     frequency: &str, // "monthly", "quarterly", "yearly"
// ) -> Result<Vec<(String, String, Vec<Allocation>)>, DatabaseError> {
//     let client = pool.get().await?;
//     let end_date = end_date
//         .map(|s| s.to_string())
//         .unwrap_or_else(|| Utc::now().format("%Y-%m-%dT%H:%M:%S.000000Z").to_string());

//     let mut current_date = start_date.to_string();
//     let mut results = Vec::new();

//     while &current_date <= &end_date {
//         // Get the last market open trading day of the previous month
//         let last_market_open_day =
//             get_last_market_open_day_of_previous_month(&client, &current_date).await?;

//         // Execute the strategy on the last market open trading day
//         let allocations = execute_strategy(strategy, pool, &last_market_open_day).await?;

//         // Store the results with both the execution date and the display date
//         results.push((current_date.clone(), last_market_open_day, allocations));

//         // Move to the next month (or quarter/year)
//         let next_date = match frequency {
//             "monthly" => {
//                 let date = NaiveDate::parse_from_str(&current_date, "%Y-%m-%d")?;
//                 date.checked_add_months(Months::new(1))
//                     .ok_or(DatabaseError::InvalidInput("Invalid month".to_string()))?
//             }
//             "quarterly" => {
//                 let date = NaiveDate::parse_from_str(&current_date, "%Y-%m-%d")?;
//                 date.checked_add_months(Months::new(3))
//                     .ok_or(DatabaseError::InvalidInput("Invalid month".to_string()))?
//             }
//             "yearly" => {
//                 let date = NaiveDate::parse_from_str(&current_date, "%Y-%m-%d")?;
//                 date.checked_add_months(Months::new(12))
//                     .ok_or(DatabaseError::InvalidInput("Invalid year".to_string()))?
//             }
//             _ => return Err(DatabaseError::InvalidInput("Invalid frequency".to_string())),
//         };
//         current_date = next_date.format("%Y-%m-%d").to_string();
//     }

//     Ok(results)
// }

//START OF PARALLIZED VERSION

use crate::market::database_functions_old::DatabaseError;
use crate::portfolio::blocks::models::Block;
use crate::portfolio::execution::strategy_executorOld::{execute_strategy, Allocation};
use chrono::{Months, NaiveDate, NaiveDateTime, Utc};
use deadpool_postgres::{Client, Pool};
use std::sync::Arc;
use sysinfo::System;
use tokio::sync::{broadcast, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, warn};

// Constants for execution control
const MAX_CONCURRENT_EXECUTIONS: usize = 10;

#[derive(Debug, Clone, Copy)]
pub enum ExecutionFrequency {
    Monthly,
    Quarterly,
    Yearly,
}

impl ExecutionFrequency {
    fn months(&self) -> u32 {
        match self {
            Self::Monthly => 1,
            Self::Quarterly => 3,
            Self::Yearly => 12,
        }
    }
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub display_date: String,
    pub execution_date: String,
    pub allocations: Vec<Allocation>,
}

#[derive(Debug)]
struct ExecutionTask {
    date: NaiveDate,
    strategy: Arc<Block>,
    pool: Arc<Pool>,
}

impl From<chrono::format::ParseError> for DatabaseError {
    fn from(err: chrono::format::ParseError) -> Self {
        DatabaseError::InvalidInput(err.to_string())
    }
}

async fn get_last_market_day(client: &Client, date: NaiveDate) -> Result<String, DatabaseError> {
    let date_str = date.format("%Y-%m-%d").to_string();
    debug!("Checking market day for date: {}", date_str);

    let query = format!(
        r#"
        SELECT Date, Date_of_Previous_Trading_Day, Is_Holiday, Is_Weekend
        FROM nasdaq_closed_days
        WHERE Date = '{}'
        "#,
        date_str
    );

    let row = client.query_opt(&query, &[]).await?;

    let market_day = match row {
        Some(row) => {
            let is_holiday: bool = row.get("Is_Holiday");
            let is_weekend: bool = row.get("Is_Weekend");

            if !is_holiday && !is_weekend {
                date
            } else {
                let prev_day: Option<NaiveDateTime> = row.get("Date_of_Previous_Trading_Day");
                prev_day
                    .unwrap_or_else(|| date.and_hms_opt(16, 0, 0).unwrap())
                    .date()
            }
        }
        None => date,
    };

    Ok(format!(
        "{}T16:00:00.000000Z",
        market_day.format("%Y-%m-%d")
    ))
}

async fn process_execution_task(
    task: ExecutionTask,
    semaphore: Arc<Semaphore>,
) -> Result<ExecutionResult, DatabaseError> {
    let permit = semaphore.acquire().await.unwrap();
    debug!("Acquired execution permit for date: {}", task.date);

    // Add a timeout for the task
    match timeout(Duration::from_secs(30), async {
        let client = task.pool.get().await?;
        let execution_date = get_last_market_day(&client, task.date).await?;
        let allocations = execute_strategy(&task.strategy, &task.pool, &execution_date).await?;
        Ok((execution_date, allocations))
    })
    .await
    {
        Ok(Ok((execution_date, allocations))) => Ok(ExecutionResult {
            display_date: task.date.format("%Y-%m-%d").to_string(),
            execution_date,
            allocations,
        }),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(DatabaseError::InvalidInput("Task timeout".into())),
    }
}

fn generate_execution_dates(
    frequency: ExecutionFrequency,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Vec<NaiveDate> {
    let mut dates = Vec::new();
    let mut current = start_date;

    while current <= end_date {
        dates.push(current);
        if let Some(next) = current.checked_add_months(Months::new(frequency.months() as u32)) {
            current = next;
        } else {
            break;
        }
    }

    dates
}

fn get_recommended_batch_size() -> usize {
    let mut system = System::new_all();
    system.refresh_all();

    let available_memory = system.available_memory(); // in KB
    let cpu_count = system.cpus().len();

    // Adjust batch size based on available resources
    if available_memory < 4_000_000 {
        // Less than 4 GB
        10
    } else if available_memory < 8_000_000 {
        // Less than 8 GB
        20
    } else if cpu_count < 4 {
        20
    } else if cpu_count < 8 {
        50
    } else {
        100
    }
}

pub async fn execute_strategy_over_time_span(
    pool: &Pool,
    strategy: &Block,
    start_date: &str,
    end_date: Option<&str>,
    frequency: &str,
) -> Result<Vec<ExecutionResult>, DatabaseError> {
    // Validate and parse frequency
    let frequency = match frequency.to_lowercase().as_str() {
        "monthly" => ExecutionFrequency::Monthly,
        "quarterly" => ExecutionFrequency::Quarterly,
        "yearly" => ExecutionFrequency::Yearly,
        _ => return Err(DatabaseError::InvalidInput("Invalid frequency".into())),
    };

    // Parse dates
    let start = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")?;
    let end = end_date
        .map(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d"))
        .transpose()?
        .unwrap_or_else(|| Utc::now().naive_utc().date());

    info!("Executing strategy from {} to {}", start, end);

    // Convert pool and strategy to Arc for sharing
    let pool = Arc::new(pool.clone());
    let strategy = Arc::new(strategy.clone());

    // Generate execution dates
    let dates = generate_execution_dates(frequency, start, end);

    // Create semaphore for concurrency control
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_EXECUTIONS));

    // Create a shutdown channel
    let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);

    // Spawn a task to listen for shutdown signals
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            info!("Received shutdown signal");
            let _ = shutdown_tx.send(());
        }
    });

    // Determine batch size dynamically
    let batch_size = get_recommended_batch_size();
    info!("Using batch size: {}", batch_size);

    // Process dates in chunks
    let mut results = Vec::new();

    for chunk in dates.chunks(batch_size) {
        let mut join_set = JoinSet::new();

        for date in chunk {
            let task = ExecutionTask {
                date: *date,
                strategy: strategy.clone(),
                pool: pool.clone(),
            };
            let semaphore = semaphore.clone();

            join_set.spawn(async move { process_execution_task(task, semaphore).await });
        }

        // Collect results for this batch
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Shutting down gracefully...");
                break;
            }
            result = async {
                while let Some(result) = join_set.join_next().await {
                    match result {
                        Ok(Ok(execution_result)) => {
                            results.push(execution_result);
                        }
                        Ok(Err(e)) => {
                            warn!("Task execution failed: {}", e);
                        }
                        Err(e) => {
                            error!("Task join failed: {}", e);
                        }
                    }
                }
                Ok::<(), DatabaseError>(())
            } => result?,
        }
    }

    // Sort results by display date
    results.sort_by(|a, b| a.display_date.cmp(&b.display_date));

    Ok(results)
}
