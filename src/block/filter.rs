use crate::block::database_functions::{self, DatabaseError};
use crate::models::{
    Block, BlockAttributes, FunctionDefinition, FunctionName, SelectConfig, SelectOption,
    SortFunction,
};
use crate::strategy_executor::Allocation;
use deadpool_postgres::{Client, Pool};
use tracing::{debug, info, warn};

const VALID_FUNCTIONS: [FunctionName; 9] = [
    FunctionName::CurrentPrice,
    FunctionName::SimpleMovingAverage,
    FunctionName::ExponentialMovingAverage,
    FunctionName::CumulativeReturn,
    // FunctionName::MovingAverageOfPrice,
    FunctionName::MovingAverageOfReturns,
    FunctionName::RelativeStrengthIndex,
    FunctionName::PriceStandardDeviation,
    FunctionName::ReturnsStandardDeviation,
    FunctionName::MaxDrawdown,
];

/// Applies filtering logic to a set of assets based on a sorting function and selection criteria
pub async fn apply_filter(
    pool: &Pool,
    sort_function: &SortFunction,
    select: &SelectConfig,
    assets: &[Block],
    execution_date: &String,
    parent_weight: f64,
) -> Result<Vec<Allocation>, DatabaseError> {
    debug!(
        "Starting filter application: function={:?}, window={}, select={:?}",
        sort_function.function_name, sort_function.window_of_days, select
    );

    // Input validation
    if assets.is_empty() {
        return Err(DatabaseError::InvalidInput(
            "Assets list cannot be empty".to_string(),
        ));
    }

    if !VALID_FUNCTIONS.contains(&sort_function.function_name) {
        return Err(DatabaseError::InvalidInput(format!(
            "Invalid function: {:?}",
            sort_function.function_name
        )));
    }

    // Step 1: Calculate values for each asset with error handling
    let mut ticker_values = Vec::with_capacity(assets.len());
    for asset in assets {
        if let BlockAttributes::Asset { ticker, .. } = &asset.attributes {
            debug!("Processing asset: {}", ticker);
            match calculate_asset_value(
                pool,
                ticker,
                &FunctionDefinition {
                    function_name: sort_function.function_name.clone(),
                    window_of_days: Some(sort_function.window_of_days),
                    asset: ticker.clone(),
                },
                execution_date,
            )
            .await
            {
                Ok(value) => {
                    debug!("Asset {} value calculated: {}", ticker, value);
                    ticker_values.push((ticker.clone(), value));
                }
                Err(e) => {
                    warn!("Failed to calculate value for {}: {:?}", ticker, e);
                    continue; // Skip this asset but continue processing others
                }
            }
        }
    }

    if ticker_values.is_empty() {
        warn!("No valid assets found to filter");
        return Ok(Vec::new());
    }

    debug!("Calculated values for {} assets", ticker_values.len());

    // Step 2: Sort values (descending order) with NaN handling
    ticker_values.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    // Step 3: Select top/bottom N assets with bounds checking
    let n = select.amount as usize;
    if n > ticker_values.len() {
        return Err(DatabaseError::InvalidInput(format!(
            "Requested {} assets but only {} available",
            n,
            ticker_values.len()
        )));
    }

    // Step 4: Create allocations with proper weights
    let weight_per_ticker = parent_weight / (select.amount as f64);
    let selected_allocations = match select.option {
        SelectOption::Top => ticker_values
            .into_iter()
            .take(n)
            .map(|(ticker, _)| Allocation {
                ticker,
                weight: weight_per_ticker,
                date: execution_date.clone(),
            })
            .collect(),
        SelectOption::Bottom => ticker_values
            .into_iter()
            .rev()
            .take(n)
            .map(|(ticker, _)| Allocation {
                ticker,
                weight: weight_per_ticker,
                date: execution_date.clone(),
            })
            .collect(),
    };

    debug!("Created allocations for selected assets");
    Ok(selected_allocations)
}

async fn calculate_asset_value(
    pool: &Pool,
    ticker: &String,
    function: &FunctionDefinition,
    execution_date: &String,
) -> Result<f64, DatabaseError> {
    let client = pool.get().await?;
    match function.function_name {
        FunctionName::CurrentPrice => {
            let price =
                database_functions::get_current_price(&client, ticker, execution_date).await?;

            Ok(price.close)
        }
        FunctionName::SimpleMovingAverage => {
            let sma = database_functions::get_sma(
                &client,
                ticker,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(sma)
        }
        FunctionName::ExponentialMovingAverage => {
            let ema = database_functions::get_ema(
                &client,
                ticker,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(ema)
        }
        FunctionName::CumulativeReturn => {
            let cum_return = database_functions::get_cumulative_return(
                &client,
                ticker,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(cum_return)
        }
        // FunctionName::MovingAverageOfPrice => {
        //     let ma_price = database_functions::get_ma_of_price(
        //         &client,
        //         ticker,
        //         execution_date,
        //         function.window_of_days.unwrap_or(20) as i64,
        //     )
        //     .await?;
        //     sleep(Duration::from_millis(100)).await;
        //     Ok(ma_price)
        // }
        FunctionName::MaxDrawdown => {
            let result = database_functions::get_max_drawdown(
                &client,
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(result.max_drawdown_percentage) // Note we use the percentage field
        }
        FunctionName::MovingAverageOfReturns => {
            let ma_returns = database_functions::get_ma_of_returns(
                &client,
                ticker,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(ma_returns)
        }
        FunctionName::RelativeStrengthIndex => {
            let rsi = database_functions::get_rsi(
                &client,
                ticker,
                execution_date,
                function.window_of_days.unwrap_or(14) as i64,
            )
            .await?;

            Ok(rsi)
        }
        FunctionName::PriceStandardDeviation => {
            let price_std = database_functions::get_price_std_dev(
                &client,
                ticker,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(price_std)
        }
        FunctionName::ReturnsStandardDeviation => {
            let returns_std = database_functions::get_returns_std_dev(
                &client,
                ticker,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(returns_std)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        Block, BlockAttributes, BlockType, FunctionDefinition, FunctionName, SelectOption,
    };
    use chrono::Utc;

    async fn setup_test_pool() -> Pool {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect("postgresql://admin:quest@localhost:9000/qdb")
            .await
            .expect("Failed to create pool")
    }

    fn create_test_asset(ticker: &str) -> Block {
        Block {
            blocktype: BlockType::Asset, // Changed from "Asset".into()
            attributes: BlockAttributes::Asset {
                ticker: ticker.to_string(),
                company_name: format!("{} Inc.", ticker),
                exchange: "NASDAQ".to_string(),
            },
            children: None,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use chrono::Utc;

        async fn setup_test_pool() -> Pool<Postgres> {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect("postgresql://admin:quest@localhost:9000/qdb")
                .await
                .expect("Failed to create pool")
        }

        fn create_test_asset(ticker: &str) -> Block {
            Block {
                blocktype: BlockType::Asset,
                attributes: BlockAttributes::Asset {
                    ticker: ticker.to_string(),
                    company_name: format!("{} Inc.", ticker),
                    exchange: "NASDAQ".to_string(),
                },
                children: None,
            }
        }

        #[tokio::test]
        async fn test_filter_functionality() {
            let pool = setup_test_pool().await;
            let assets = vec![
                create_test_asset("AAPL"),
                create_test_asset("MSFT"),
                create_test_asset("GOOGL"),
            ];

            let result = apply_filter(
                &pool,
                &SortFunction {
                    function_name: FunctionName::CumulativeReturn,
                    window_of_days: 10,
                },
                &SelectConfig {
                    option: SelectOption::Top,
                    amount: 2,
                },
                &assets,
                &Utc::now().to_rfc3339(),
                1.0,
            )
            .await;

            assert!(result.is_ok());
            let allocations = result.unwrap();
            assert_eq!(allocations.len(), 2);
            assert!(allocations.iter().all(|a| a.weight == 0.5));
        }
    }
}
