use crate::block::database_functions::{self, DatabaseError};
use crate::block::filter::apply_filter;
use crate::models::{
    Block, BlockAttributes, CompareToValue, ComparisonOperator, FunctionDefinition, FunctionName,
    SelectOption, WeightType,
};
use deadpool_postgres::{Client, Pool}; // Import Pool and Client from deadpool-postgres
use std::future::Future;
use std::pin::Pin;
use tokio_postgres::Error as PgError;
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct Allocation {
    pub ticker: String,
    pub weight: f64,
    pub date: String,
}

impl Allocation {
    pub fn new(ticker: String, weight: f64, date: String) -> Result<Self, DatabaseError> {
        if weight.is_finite() && weight >= 0.0 {
            Ok(Self {
                ticker,
                weight,
                date,
            })
        } else {
            Err(DatabaseError::InvalidCalculation(format!(
                "Invalid weight value: {}",
                weight
            )))
        }
    }
}

pub async fn execute_strategy(
    block: &Block,
    pool: &Pool,
    execution_date: &String,
) -> Result<Vec<Allocation>, DatabaseError> {
    info!("Starting strategy execution for date: {}", execution_date);
    let allocations = execute_block(block, pool, execution_date, 1.0).await?;
    normalize_weights(&allocations)
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

fn execute_block<'a>(
    block: &'a Block,
    pool: &'a Pool,
    execution_date: &'a String,
    parent_weight: f64,
) -> BoxFuture<'a, Result<Vec<Allocation>, DatabaseError>> {
    Box::pin(async move {
        match &block.attributes {
            BlockAttributes::Group { name } => {
                debug!("Executing group: {}", name);
                if let Some(children) = &block.children {
                    execute_children(children, pool, execution_date, parent_weight).await
                } else {
                    Ok(Vec::new())
                }
            }
            BlockAttributes::Condition {
                function,
                operator,
                compare_to,
            } => {
                if let Some(children) = &block.children {
                    let condition_met =
                        evaluate_condition(function, operator, compare_to, pool, execution_date)
                            .await?;

                    if condition_met {
                        debug!("Condition met - executing first branch");
                        execute_block(&children[0], pool, execution_date, parent_weight).await
                    } else if children.len() > 1 {
                        debug!("Condition not met - executing second branch");
                        execute_block(&children[1], pool, execution_date, parent_weight).await
                    } else {
                        Ok(Vec::new())
                    }
                } else {
                    Ok(Vec::new())
                }
            }
            BlockAttributes::Weight {
                weight_type,
                values,
                ..
            } => {
                if let Some(children) = &block.children {
                    match weight_type {
                        WeightType::Equal => {
                            let weight = parent_weight / children.len() as f64;
                            execute_children(children, pool, execution_date, weight).await
                        }
                        WeightType::Specified => {
                            let mut weighted_allocations = Vec::new();
                            for (child, &weight) in children.iter().zip(values.iter()) {
                                let child_weight = parent_weight * (weight / 100.0);
                                let mut child_allocations =
                                    execute_block(child, pool, execution_date, child_weight)
                                        .await?;
                                weighted_allocations.extend(child_allocations);
                            }
                            Ok(weighted_allocations)
                        }
                        _ => Err(DatabaseError::InvalidInput(format!(
                            "Unsupported weight type: {:?}",
                            weight_type
                        ))),
                    }
                } else {
                    Ok(Vec::new())
                }
            }
            BlockAttributes::Filter {
                sort_function,
                select,
            } => {
                if let Some(children) = &block.children {
                    apply_filter(
                        pool,
                        sort_function,
                        select,
                        children,
                        execution_date,
                        parent_weight,
                    )
                    .await
                } else {
                    Ok(Vec::new())
                }
            }
            BlockAttributes::Asset { ticker, .. } => {
                // Special handling for BIL (cash equivalent)
                if ticker == "BIL" {
                    debug!("Converting BIL allocation to CASH");
                    Ok(vec![Allocation {
                        ticker: String::from("CASH"),
                        weight: parent_weight,
                        date: execution_date.clone(),
                    }])
                } else {
                    Ok(vec![Allocation {
                        ticker: ticker.clone(),
                        weight: parent_weight,
                        date: execution_date.clone(),
                    }])
                }
            }
        }
    })
}

async fn execute_children<'a>(
    children: &'a [Block],
    pool: &'a Pool,
    execution_date: &'a String,
    weight: f64,
) -> Result<Vec<Allocation>, DatabaseError> {
    let mut all_allocations = Vec::new();
    for child in children {
        let mut child_allocations = execute_block(child, pool, execution_date, weight).await?;
        all_allocations.append(&mut child_allocations);
    }
    Ok(all_allocations)
}

async fn evaluate_condition(
    function: &FunctionDefinition,
    operator: &ComparisonOperator,
    compare_to: &CompareToValue,
    pool: &Pool,
    execution_date: &String,
) -> Result<bool, DatabaseError> {
    debug!(
        "Starting condition evaluation: {:?} {:?}",
        function.function_name, operator
    );

    // First function evaluation
    debug!("Evaluating first function: {:?}", function);
    let function_value = evaluate_function(function, pool, execution_date).await?;
    debug!("First function value: {}", function_value);

    // Second function/value evaluation
    let compare_value = match compare_to {
        CompareToValue::Function {
            function: compare_function,
        } => {
            debug!("Evaluating comparison function: {:?}", compare_function);
            evaluate_function(compare_function, pool, execution_date).await?
        }
        CompareToValue::Fixed { value, .. } => {
            debug!("Using fixed comparison value: {}", value);
            *value
        }
    };
    debug!("Comparison value: {}", compare_value);

    // Final comparison
    let result = match operator {
        ComparisonOperator::GreaterThan => function_value > compare_value,
        ComparisonOperator::LessThan => function_value < compare_value,
        ComparisonOperator::Equal => (function_value - compare_value).abs() < f64::EPSILON,
        ComparisonOperator::GreaterThanOrEqual => function_value >= compare_value,
        ComparisonOperator::LessThanOrEqual => function_value <= compare_value,
    };

    debug!(
        "Condition result: {} {:?} {} = {}",
        function_value, operator, compare_value, result
    );

    Ok(result)
}

async fn evaluate_function(
    function: &FunctionDefinition,
    pool: &Pool,
    execution_date: &String,
) -> Result<f64, DatabaseError> {
    debug!("Evaluating function with date: {}", execution_date);
    info!("Start eval");

    // Get a client from the pool
    let client = pool.get().await?;

    match function.function_name {
        FunctionName::CumulativeReturn => {
            let result = database_functions::get_cumulative_return(
                &client, // Pass the client instead of the pool
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(result)
        }
        FunctionName::CurrentPrice => {
            let price = database_functions::get_current_price(
                &client, // Pass the client instead of the pool
                &function.asset,
                execution_date,
            )
            .await?;

            Ok(price.close)
        }
        FunctionName::RelativeStrengthIndex => {
            let rsi = database_functions::get_rsi(
                &client, // Pass the client instead of the pool
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(14) as i64,
            )
            .await?;

            Ok(rsi)
        }
        FunctionName::SimpleMovingAverage => {
            let sma = database_functions::get_sma(
                &client, // Pass the client instead of the pool
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(sma)
        }
        FunctionName::ExponentialMovingAverage => {
            let ema = database_functions::get_ema(
                &client, // Pass the client instead of the pool
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(ema)
        }
        // FunctionName::MovingAverageOfPrice => {
        //     let ma_price = database_functions::get_ma_of_price(
        //         &client, // Pass the client instead of the pool
        //         &function.asset,
        //         execution_date,
        //         function.window_of_days.unwrap_or(20) as i64,
        //     )
        //     .await?;
        //
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
                &client, // Pass the client instead of the pool
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(ma_returns)
        }
        FunctionName::PriceStandardDeviation => {
            let price_std = database_functions::get_price_std_dev(
                &client, // Pass the client instead of the pool
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(price_std)
        }
        FunctionName::ReturnsStandardDeviation => {
            let returns_std = database_functions::get_returns_std_dev(
                &client, // Pass the client instead of the pool
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;

            Ok(returns_std)
        }
        FunctionName::MarketCap => {
            let market_cap = database_functions::get_market_cap(
                &client, // Pass the client instead of the pool
                &function.asset,
                execution_date,
            )
            .await?;

            Ok(market_cap)
        }
    }
}
fn normalize_weights(allocations: &[Allocation]) -> Result<Vec<Allocation>, DatabaseError> {
    if allocations.is_empty() {
        return Err(DatabaseError::InvalidCalculation(
            "No allocations provided".into(),
        ));
    }

    let total_weight: f64 = allocations.iter().map(|a| a.weight).sum();

    if !total_weight.is_finite() || total_weight <= 0.0 {
        return Err(DatabaseError::InvalidCalculation(format!(
            "Invalid total weight: {}",
            total_weight
        )));
    }

    Ok(allocations
        .iter()
        .map(|a| Allocation {
            ticker: a.ticker.clone(),
            weight: a.weight / total_weight,
            date: a.date.clone(),
        })
        .collect())
}
