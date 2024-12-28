use super::Allocation;
use crate::block::database_functions::DatabaseError;
use crate::models::{Block, BlockAttributes};
use deadpool_postgres::Pool;
use futures::stream::{self, StreamExt};
use tracing::{debug, info}; // Use the Allocation from the parent module

const MAX_CONCURRENT_TASKS: usize = 4;

/// Execute children blocks in parallel when the nesting is deep
pub async fn execute_children_parallel(
    children: &[Block],
    pool: &Pool,
    execution_date: &String,
    weight: f64,
) -> Result<Vec<Allocation>, DatabaseError> {
    debug!("Parallel execution of {} children blocks", children.len());

    let results: Vec<Result<Vec<Allocation>, DatabaseError>> = stream::iter(children)
        .map(|child| {
            let pool = pool.clone();
            let exec_date = execution_date.clone();
            async move { super::execute_block(child, &pool, &exec_date, weight).await }
        })
        .buffer_unordered(MAX_CONCURRENT_TASKS)
        .collect()
        .await;

    let mut all_allocations = Vec::new();
    for result in results {
        all_allocations.extend(result?);
    }

    Ok(all_allocations)
}

/// Execute weight calculations in parallel for inverse volatility
pub async fn execute_inverse_volatility_parallel(
    tickers: Vec<String>,
    pool: &Pool,
    execution_date: &String,
    period: u32,
    parent_weight: f64,
) -> Result<Vec<Allocation>, DatabaseError> {
    debug!(
        "Parallel volatility calculation for {} tickers",
        tickers.len()
    );

    // Calculate volatilities in parallel
    let volatility_futures: Vec<_> = stream::iter(tickers)
        .map(|ticker| {
            let pool = pool.clone();
            let exec_date = execution_date.clone();
            async move {
                let client = pool.get().await.map_err(|e| {
                    DatabaseError::InvalidCalculation(format!(
                        "Failed to get database client: {}",
                        e
                    ))
                })?;

                let vol = database_functions::get_returns_std_dev(
                    &client,
                    &ticker,
                    &exec_date,
                    period as i64,
                )
                .await?;

                Ok((ticker, vol))
            }
        })
        .buffer_unordered(MAX_CONCURRENT_TASKS)
        .collect()
        .await;

    // Process results
    let mut inverse_vols = Vec::with_capacity(volatility_futures.len());
    let mut total_inverse_vol = 0.0;

    for result in volatility_futures {
        let (ticker, vol) = result?;
        let inverse_vol = 1.0 / vol;

        if !inverse_vol.is_finite() || inverse_vol <= 0.0 {
            return Err(DatabaseError::InvalidCalculation(format!(
                "Invalid volatility value for {}: {}",
                ticker, vol
            )));
        }

        inverse_vols.push((ticker, inverse_vol));
        total_inverse_vol += inverse_vol;
    }

    // Create final allocations
    let allocations = inverse_vols
        .into_iter()
        .map(|(ticker, inverse_vol)| {
            let weight = parent_weight * (inverse_vol / total_inverse_vol);
            Allocation::new(ticker, weight, execution_date.clone())
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(allocations)
}

/// Check if a block should use parallel execution based on depth
pub fn should_use_parallel(depth: usize) -> bool {
    depth > 5
}
