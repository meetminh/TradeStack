// use crate::block::database_functions::{self, DatabaseError};
// use crate::block::filter::apply_filter;
// use crate::models::{
//     Allocation, Attributes, Block, CompareToValue, ConditionConfig, FunctionDefinition,
// };
// use sqlx::Pool;
// use sqlx::Postgres;
// use std::future::Future;
// use std::pin::Pin;
// use tracing::{debug, info, warn};

// pub async fn execute_strategy(
//     block: &Block,
//     pool: &Pool<Postgres>,
//     execution_date: &String,
// ) -> Result<Vec<Allocation>, DatabaseError> {
//     info!("Starting strategy execution for date: {}", execution_date);
//     let allocations = execute_block(block, pool, execution_date, 1.0).await?;
//     normalize_weights(&allocations, execution_date)
// }
// // Helper type alias for the recursive async function
// type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// fn execute_block<'a>(
//     block: &'a Block,
//     pool: &'a Pool<Postgres>,
//     execution_date: &'a String,
//     parent_weight: f64,
// ) -> BoxFuture<'a, Result<Vec<Allocation>, DatabaseError>> {
//     Box::pin(async move {
//         match &block.attributes {
//             Attributes::Group { name } => {
//                 debug!("Executing group: {}", name);
//                 if let Some(children) = &block.children {
//                     execute_children(children, pool, execution_date, parent_weight).await
//                 } else {
//                     Ok(Vec::new())
//                 }
//             }
//             Attributes::Weight {
//                 type_,
//                 values,
//                 window_of_trading_days,
//                 ..
//             } => {
//                 if let Some(children) = &block.children {
//                     match type_.as_str() {
//                         "equal" => {
//                             let weight = 1.0 / children.len() as f64;
//                             execute_children(children, pool, execution_date, parent_weight * weight)
//                                 .await
//                         }
//                         "specified" => {
//                             execute_weighted_children(
//                                 children,
//                                 values,
//                                 pool,
//                                 execution_date,
//                                 parent_weight,
//                             )
//                             .await
//                         }
//                         "inverse_volatility" => {
//                             execute_inverse_volatility_children(
//                                 children,
//                                 window_of_trading_days.unwrap_or(20),
//                                 pool,
//                                 execution_date,
//                                 parent_weight,
//                             )
//                             .await
//                         }
//                         "market_cap" => {
//                             execute_market_cap_children(
//                                 children,
//                                 pool,
//                                 execution_date,
//                                 parent_weight,
//                             )
//                             .await
//                         }
//                         _ => Err(DatabaseError::InvalidInput(format!(
//                             "Unknown weight type: {}",
//                             type_
//                         ))),
//                     }
//                 } else {
//                     Ok(Vec::new())
//                 }
//             }
//             Attributes::Condition { condition } => {
//                 if let Some(children) = &block.children {
//                     let condition_met = evaluate_condition(condition, pool, execution_date).await?;

//                     if condition_met {
//                         execute_block(&children[0], pool, execution_date, parent_weight).await
//                     } else if children.len() > 1 {
//                         execute_block(&children[1], pool, execution_date, parent_weight).await
//                     } else {
//                         Ok(Vec::new())
//                     }
//                 } else {
//                     Ok(Vec::new())
//                 }
//             }
//             Attributes::Asset { ticker, .. } => Ok(vec![Allocation {
//                 ticker: ticker.clone(),
//                 weight: parent_weight,
//                 date: execution_date.to_string(),
//             }]),
//             Attributes::Filter {
//                 sort_function,
//                 select,
//             } => {
//                 if let Some(children) = &block.children {
//                     // Get filtered tickers
//                     let selected_tickers =
//                         apply_filter(pool, sort_function, select, children, execution_date).await?;

//                     // Convert selected tickers to Allocations
//                     // Each selected ticker gets equal weight for now
//                     // (actual weights will be applied by parent Weight block)
//                     Ok(selected_tickers
//                         .into_iter()
//                         .map(|ticker| Allocation {
//                             ticker,
//                             weight: parent_weight / selected_tickers.len() as f64,
//                             date: execution_date.to_string(),
//                         })
//                         .collect())
//                 } else {
//                     Ok(Vec::new())
//                 }
//             }
//         }
//     })
// }

// async fn execute_inverse_volatility_children(
//     children: &[Block],
//     window: u32,
//     pool: &Pool<Postgres>,
//     execution_date: &String,
//     parent_weight: f64,
// ) -> Result<Vec<Allocation>, DatabaseError> {
//     let mut volatilities = Vec::new();
//     let mut allocations = Vec::new();

//     // Calculate volatilities
//     for child in children {
//         if let Attributes::Asset { ticker, .. } = &child.attributes {
//             let volatility = database_functions::get_returns_std_dev(
//                 pool,
//                 ticker,
//                 execution_date,
//                 window as i64,
//             )
//             .await?;
//             volatilities.push((ticker.clone(), 1.0 / volatility));
//         }
//     }

//     // Calculate weights
//     let total_inverse: f64 = volatilities.iter().map(|(_, inv)| inv).sum();
//     for (ticker, inverse) in volatilities {
//         allocations.push(Allocation {
//             ticker,
//             weight: parent_weight * (inverse / total_inverse),
//             date: execution_date.to_string(),
//         });
//     }

//     Ok(allocations)
// }

// async fn execute_market_cap_children(
//     children: &[Block],
//     pool: &Pool<Postgres>,
//     execution_date: &String,
//     parent_weight: f64,
// ) -> Result<Vec<Allocation>, DatabaseError> {
//     let mut market_caps = Vec::new();
//     let mut allocations = Vec::new();

//     // Get market caps
//     for child in children {
//         if let Attributes::Asset { ticker, .. } = &child.attributes {
//             let market_cap =
//                 database_functions::get_market_cap(pool, ticker, execution_date).await?;
//             market_caps.push((ticker.clone(), market_cap));
//         }
//     }

//     // Calculate weights
//     let total_market_cap: f64 = market_caps.iter().map(|(_, cap)| cap).sum();
//     for (ticker, cap) in market_caps {
//         allocations.push(Allocation {
//             ticker,
//             weight: parent_weight * (cap / total_market_cap),
//             date: execution_date.to_string(),
//         });
//     }

//     Ok(allocations)
// }

// async fn execute_children(
//     children: &[Block],
//     pool: &Pool<Postgres>,
//     execution_date: &String,
//     parent_weight: f64,
// ) -> Result<Vec<Allocation>, DatabaseError> {
//     let mut allocations = Vec::new();
//     for child in children {
//         let mut child_allocations =
//             execute_block(child, pool, execution_date, parent_weight).await?;
//         allocations.append(&mut child_allocations);
//     }
//     Ok(allocations)
// }
// async fn execute_weighted_children(
//     children: &[Block],
//     weights: &[f64],
//     pool: &Pool<Postgres>,
//     execution_date: &String,
//     parent_weight: f64,
// ) -> Result<Vec<Allocation>, DatabaseError> {
//     let mut allocations = Vec::new();
//     for (child, &weight) in children.iter().zip(weights.iter()) {
//         let mut child_allocations =
//             execute_block(child, pool, execution_date, parent_weight * weight / 100.0).await?;
//         allocations.append(&mut child_allocations);
//     }
//     Ok(allocations)
// }

// async fn evaluate_condition(
//     condition: &ConditionConfig,
//     pool: &Pool<Postgres>,
//     execution_date: &String,
// ) -> Result<bool, DatabaseError> {
//     let left_value = evaluate_function(&condition.function, pool, execution_date).await?;

//     let right_value = match &condition.compare_to {
//         CompareToValue::Function { function } => {
//             evaluate_function(function, pool, execution_date).await?
//         }
//         CompareToValue::Fixed { value, unit } => {
//             if unit.as_ref().map_or(false, |u| u == "%") {
//                 value / 100.0
//             } else {
//                 *value
//             }
//         }
//     };

//     Ok(match condition.operator.as_str() {
//         ">" => left_value > right_value,
//         "<" => left_value < right_value,
//         ">=" => left_value >= right_value,
//         "<=" => left_value <= right_value,
//         "==" => (left_value - right_value).abs() < f64::EPSILON,
//         _ => {
//             return Err(DatabaseError::InvalidInput(format!(
//                 "Unknown operator: {}",
//                 condition.operator
//             )))
//         }
//     })
// }

// async fn evaluate_function(
//     function: &FunctionDefinition,
//     pool: &Pool<Postgres>,
//     execution_date: &String,
// ) -> Result<f64, DatabaseError> {
//     let window = function.window_of_days.unwrap_or(0);

//     match function.function_name.as_str() {
//         "cumulative_return" => {
//             database_functions::get_cumulative_return(
//                 pool,
//                 &function.asset,
//                 execution_date,
//                 window as i64,
//             )
//             .await
//         }
//         "rsi" => {
//             database_functions::get_rsi(pool, &function.asset, execution_date, window as i64).await
//         }
//         "sma" => {
//             database_functions::get_sma(pool, &function.asset, execution_date, window as i64).await
//         }
//         "ema" => {
//             database_functions::get_ema(pool, &function.asset, execution_date, window as i64).await
//         }
//         "price_std_dev" => {
//             database_functions::get_price_std_dev(
//                 pool,
//                 &function.asset,
//                 execution_date,
//                 window as i64,
//             )
//             .await
//         }
//         "returns_std_dev" => {
//             database_functions::get_returns_std_dev(
//                 pool,
//                 &function.asset,
//                 execution_date,
//                 window as i64,
//             )
//             .await
//         }
//         "ma_of_returns" => {
//             database_functions::get_ma_of_returns(
//                 pool,
//                 &function.asset,
//                 execution_date,
//                 window as i64,
//             )
//             .await
//         }
//         "ma_of_price" => {
//             database_functions::get_ma_of_price(
//                 pool,
//                 &function.asset,
//                 execution_date,
//                 window as i64,
//             )
//             .await
//         }
//         "current_price" => {
//             database_functions::get_current_price(pool, &function.asset, execution_date)
//                 .await
//                 .map(|price| price.close)
//         }
//         "max_drawdown" => database_functions::get_max_drawdown(
//             pool,
//             &function.asset,
//             execution_date,
//             window as i64,
//         )
//         .await
//         .map(|result| result.max_drawdown_percentage),
//         _ => Err(DatabaseError::InvalidInput(format!(
//             "Unknown function: {}",
//             function.function_name
//         ))),
//     }
// }

// fn normalize_weights(
//     allocations: &[Allocation],
//     execution_date: &str,
// ) -> Result<Vec<Allocation>, DatabaseError> {
//     let total_weight: f64 = allocations.iter().map(|a| a.weight).sum();

//     if total_weight == 0.0 {
//         warn!(
//             "Total allocation weight is zero for date: {}",
//             execution_date
//         );
//         return Err(DatabaseError::InvalidCalculation(
//             "Total allocation weight is zero".to_string(),
//         ));
//     }

//     Ok(allocations
//         .iter()
//         .map(|a| Allocation {
//             ticker: a.ticker.clone(),
//             weight: a.weight / total_weight,
//             date: execution_date.to_string(),
//         })
//         .collect())
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

//     // Add tests here...
// }
