use crate::block::database_functions::{self, DatabaseError};
use crate::models::{
    Block, BlockAttributes, CompareToValue, ComparisonOperator, FunctionDefinition, FunctionName,
    SelectOption, WeightType,
};
use sqlx::Pool;
use sqlx::Postgres;
use std::future::Future;
use std::pin::Pin;
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
    pool: &Pool<Postgres>,
    execution_date: &String,
) -> Result<Vec<Allocation>, DatabaseError> {
    info!("Starting strategy execution for date: {}", execution_date);
    let allocations = execute_block(block, pool, execution_date, 1.0).await?;
    normalize_weights(&allocations)
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

fn execute_block<'a>(
    block: &'a Block,
    pool: &'a Pool<Postgres>,
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
                    let mut ticker_values = Vec::with_capacity(children.len());

                    // Collect all tickers and their sort values
                    for child in children {
                        if let BlockAttributes::Asset { ticker, .. } = &child.attributes {
                            let value = evaluate_function(
                                &FunctionDefinition {
                                    function_name: sort_function.function_name.clone(),
                                    window_of_days: Some(sort_function.window_of_days),
                                    asset: ticker.clone(),
                                },
                                pool,
                                execution_date,
                            )
                            .await?;
                            ticker_values.push((ticker.clone(), value));
                        }
                    }

                    // Sort based on values
                    ticker_values
                        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

                    // Select top/bottom N
                    let selected_tickers: Vec<String> = match select.option {
                        SelectOption::Top => ticker_values
                            .iter()
                            .take(select.amount as usize)
                            .map(|(ticker, _)| ticker.clone())
                            .collect(),
                        SelectOption::Bottom => ticker_values
                            .iter()
                            .rev()
                            .take(select.amount as usize)
                            .map(|(ticker, _)| ticker.clone())
                            .collect(),
                    };

                    // Create allocations with equal weights for selected tickers
                    let weight_per_ticker = parent_weight / selected_tickers.len() as f64;
                    Ok(selected_tickers
                        .into_iter()
                        .map(|ticker| Allocation {
                            ticker,
                            weight: weight_per_ticker,
                            date: execution_date.clone(),
                        })
                        .collect())
                } else {
                    Ok(Vec::new())
                }
            }
            BlockAttributes::Asset { ticker, .. } => Ok(vec![Allocation {
                ticker: ticker.clone(),
                weight: parent_weight,
                date: execution_date.clone(),
            }]),
        }
    })
}

async fn execute_children<'a>(
    children: &'a [Block],
    pool: &'a Pool<Postgres>,
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
    pool: &Pool<Postgres>,
    execution_date: &String,
) -> Result<bool, DatabaseError> {
    let function_value = evaluate_function(function, pool, execution_date).await?;

    let compare_value = match compare_to {
        CompareToValue::Function {
            function: compare_function,
        } => evaluate_function(compare_function, pool, execution_date).await?,
        CompareToValue::Fixed { value, .. } => *value,
    };

    Ok(match operator {
        ComparisonOperator::GreaterThan => function_value > compare_value,
        ComparisonOperator::LessThan => function_value < compare_value,
        ComparisonOperator::Equal => (function_value - compare_value).abs() < f64::EPSILON,
        ComparisonOperator::GreaterThanOrEqual => function_value >= compare_value,
        ComparisonOperator::LessThanOrEqual => function_value <= compare_value,
    })
}
use tokio::time::sleep;
use tokio::time::Duration;

async fn evaluate_function(
    function: &FunctionDefinition,
    pool: &Pool<Postgres>,
    execution_date: &String,
) -> Result<f64, DatabaseError> {
    let sleeptime: u64 = 20;
    match function.function_name {
        FunctionName::CumulativeReturn => {
            let result = database_functions::get_cumulative_return(
                pool,
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;
            sleep(Duration::from_millis(sleeptime)).await;
            Ok(result)
        }
        FunctionName::CurrentPrice => {
            let price =
                database_functions::get_current_price(pool, &function.asset, execution_date)
                    .await?;
            sleep(Duration::from_millis(sleeptime)).await;
            Ok(price.close)
        }
        FunctionName::RelativeStrengthIndex => {
            let rsi = database_functions::get_rsi(
                pool,
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(14) as i64,
            )
            .await?;
            sleep(Duration::from_millis(sleeptime)).await;
            Ok(rsi)
        }
        FunctionName::SimpleMovingAverage => {
            let sma = database_functions::get_sma(
                pool,
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;
            sleep(Duration::from_millis(sleeptime)).await;
            Ok(sma)
        }
        FunctionName::ExponentialMovingAverage => {
            let ema = database_functions::get_ema(
                pool,
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;
            sleep(Duration::from_millis(sleeptime)).await;
            Ok(ema)
        }
        FunctionName::MovingAverageOfPrice => {
            let ma_price = database_functions::get_ma_of_price(
                pool,
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;
            sleep(Duration::from_millis(sleeptime)).await;
            Ok(ma_price)
        }
        FunctionName::MovingAverageOfReturns => {
            let ma_returns = database_functions::get_ma_of_returns(
                pool,
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;
            sleep(Duration::from_millis(sleeptime)).await;
            Ok(ma_returns)
        }
        FunctionName::PriceStandardDeviation => {
            let price_std = database_functions::get_price_std_dev(
                pool,
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;
            sleep(Duration::from_millis(sleeptime)).await;
            Ok(price_std)
        }
        FunctionName::ReturnsStandardDeviation => {
            let returns_std = database_functions::get_returns_std_dev(
                pool,
                &function.asset,
                execution_date,
                function.window_of_days.unwrap_or(20) as i64,
            )
            .await?;
            sleep(Duration::from_millis(sleeptime)).await;
            Ok(returns_std)
        }
        FunctionName::MarketCap => {
            let market_cap =
                database_functions::get_market_cap(pool, &function.asset, execution_date).await?;
            sleep(Duration::from_millis(sleeptime)).await;
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
