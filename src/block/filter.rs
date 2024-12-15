// use crate::block::database_functions::{self, DatabaseError};
// use crate::models::{Attributes, Block, SelectConfig, SortFunction};
// use sqlx::Pool;
// use sqlx::Postgres;
// use tracing::{debug, info, warn};

// const VALID_FUNCTIONS: [&str; 9] = [
//     "current_price",
//     "sma",
//     "ema",
//     "cumulative_return",
//     "ma_of_price",
//     "ma_of_returns",
//     "rsi",
//     "max_drawdown",
//     "price_std_dev",
// ];

// /// Applies filtering logic to a set of assets based on a sorting function and selection criteria
// pub async fn apply_filter(
//     pool: &Pool<Postgres>,
//     sort_function: &SortFunction,
//     select: &SelectConfig,
//     assets: &[Block],
//     execution_date: &String,
// ) -> Result<Vec<String>, DatabaseError> {
//     debug!(
//         "Starting filter application: function={}, window={}, select={:?}",
//         sort_function.function_name, sort_function.window_of_days, select
//     );

//     // Input validation
//     if assets.is_empty() {
//         return Err(DatabaseError::InvalidInput(
//             "Assets list cannot be empty".to_string(),
//         ));
//     }

//     if !VALID_FUNCTIONS.contains(&sort_function.function_name.as_str()) {
//         return Err(DatabaseError::InvalidInput(format!(
//             "Invalid function: {}",
//             sort_function.function_name
//         )));
//     }

//     // Step 1: Calculate values for each asset
//     let mut ticker_values = Vec::with_capacity(assets.len());
//     for asset in assets {
//         if let Attributes::Asset { ticker, .. } = &asset.attributes {
//             let value = calculate_asset_value(pool, ticker, sort_function, execution_date).await?;
//             ticker_values.push((ticker.clone(), value));
//             debug!("Asset {} has value: {}", ticker, value);
//         }
//     }

//     if ticker_values.is_empty() {
//         warn!("No valid assets found to filter");
//         return Ok(Vec::new());
//     }

//     debug!("Calculated values for {} assets", ticker_values.len());

//     // Step 2: Sort values (descending order)
//     ticker_values.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

//     // Step 3: Select top/bottom N assets
//     let n = select.amount as usize;
//     if n > ticker_values.len() {
//         return Err(DatabaseError::InvalidInput(format!(
//             "Requested {} assets but only {} available",
//             n,
//             ticker_values.len()
//         )));
//     }

//     let selected_tickers = match select.option.as_str() {
//         "Top" => ticker_values
//             .into_iter()
//             .take(n)
//             .map(|(ticker, _)| ticker)
//             .collect(),
//         "Bottom" => ticker_values
//             .into_iter()
//             .rev()
//             .take(n)
//             .map(|(ticker, _)| ticker)
//             .collect(),
//         _ => {
//             return Err(DatabaseError::InvalidInput(
//                 "Invalid selection option. Must be 'Top' or 'Bottom'".to_string(),
//             ))
//         }
//     };

//     debug!("Selected tickers: {:?}", selected_tickers);
//     Ok(selected_tickers)
// }

// async fn calculate_asset_value(
//     pool: &Pool<Postgres>,
//     ticker: &String,
//     sort_function: &SortFunction,
//     execution_date: &String,
// ) -> Result<f64, DatabaseError> {
//     match sort_function.function_name.as_str() {
//         "current_price" => database_functions::get_current_price(pool, ticker, execution_date)
//             .await
//             .map(|price| price.close),
//         "sma" => {
//             database_functions::get_sma(
//                 pool,
//                 ticker,
//                 execution_date,
//                 sort_function.window_of_days as i64,
//             )
//             .await
//         }
//         "ema" => {
//             database_functions::get_ema(
//                 pool,
//                 ticker,
//                 execution_date,
//                 sort_function.window_of_days as i64,
//             )
//             .await
//         }
//         "cumulative_return" => {
//             database_functions::get_cumulative_return(
//                 pool,
//                 ticker,
//                 execution_date,
//                 sort_function.window_of_days as i64,
//             )
//             .await
//         }
//         "ma_of_price" => {
//             database_functions::get_ma_of_price(
//                 pool,
//                 ticker,
//                 execution_date,
//                 sort_function.window_of_days as i64,
//             )
//             .await
//         }
//         "ma_of_returns" => {
//             database_functions::get_ma_of_returns(
//                 pool,
//                 ticker,
//                 execution_date,
//                 sort_function.window_of_days as i64,
//             )
//             .await
//         }
//         "rsi" => {
//             database_functions::get_rsi(
//                 pool,
//                 ticker,
//                 execution_date,
//                 sort_function.window_of_days as i64,
//             )
//             .await
//         }
//         "max_drawdown" => database_functions::get_max_drawdown(
//             pool,
//             ticker,
//             execution_date,
//             sort_function.window_of_days as i64,
//         )
//         .await
//         .map(|result| result.max_drawdown_percentage),
//         "price_std_dev" => {
//             database_functions::get_price_std_dev(
//                 pool,
//                 ticker,
//                 execution_date,
//                 sort_function.window_of_days as i64,
//             )
//             .await
//         }
//         _ => unreachable!(), // We validated the function name above
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use chrono::Utc;

//     async fn setup_test_pool() -> Pool<Postgres> {
//         sqlx::postgres::PgPoolOptions::new()
//             .max_connections(5)
//             .connect("postgresql://admin:quest@localhost:9000/qdb")
//             .await
//             .expect("Failed to create pool")
//     }

//     fn create_test_asset(ticker: &str) -> Block {
//         Block {
//             blocktype: "Asset".to_string(),
//             attributes: Attributes::Asset {
//                 ticker: ticker.to_string(),
//                 company_name: format!("{} Inc.", ticker),
//                 exchange: "NASDAQ".to_string(),
//             },
//             children: None,
//         }
//     }

//     #[tokio::test]
//     async fn test_empty_assets() {
//         let pool = setup_test_pool().await;
//         let sort_function = SortFunction {
//             function_name: "cumulative_return".to_string(),
//             window_of_days: 10,
//         };
//         let select = SelectConfig {
//             option: "Top".to_string(),
//             amount: 3,
//         };

//         let result = apply_filter(
//             &pool,
//             &sort_function,
//             &select,
//             &[],
//             &Utc::now().to_rfc3339(),
//         )
//         .await;

//         assert!(result.is_err());
//     }

//     #[tokio::test]
//     async fn test_invalid_function() {
//         let pool = setup_test_pool().await;
//         let sort_function = SortFunction {
//             function_name: "invalid_function".to_string(),
//             window_of_days: 10,
//         };
//         let select = SelectConfig {
//             option: "Top".to_string(),
//             amount: 3,
//         };

//         let assets = vec![create_test_asset("AAPL")];

//         let result = apply_filter(
//             &pool,
//             &sort_function,
//             &select,
//             &assets,
//             &Utc::now().to_rfc3339(),
//         )
//         .await;

//         assert!(result.is_err());
//     }

//     #[tokio::test]
//     async fn test_invalid_select_option() {
//         let pool = setup_test_pool().await;
//         let sort_function = SortFunction {
//             function_name: "cumulative_return".to_string(),
//             window_of_days: 10,
//         };
//         let select = SelectConfig {
//             option: "Invalid".to_string(),
//             amount: 3,
//         };

//         let assets = vec![create_test_asset("AAPL")];

//         let result = apply_filter(
//             &pool,
//             &sort_function,
//             &select,
//             &assets,
//             &Utc::now().to_rfc3339(),
//         )
//         .await;

//         assert!(result.is_err());
//     }

//     #[tokio::test]
//     async fn test_select_amount_too_large() {
//         let pool = setup_test_pool().await;
//         let sort_function = SortFunction {
//             function_name: "cumulative_return".to_string(),
//             window_of_days: 10,
//         };
//         let select = SelectConfig {
//             option: "Top".to_string(),
//             amount: 5,
//         };

//         let assets = vec![create_test_asset("AAPL"), create_test_asset("MSFT")];

//         let result = apply_filter(
//             &pool,
//             &sort_function,
//             &select,
//             &assets,
//             &Utc::now().to_rfc3339(),
//         )
//         .await;

//         assert!(result.is_err());
//     }
// }
