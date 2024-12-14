use crate::block::database_functions::{self, DatabaseError};
use crate::block::filter::{apply_filter, FilterConfig};
use crate::models::{Allocation, Condition, ConditionValue, Node};
use sqlx::Pool;
use sqlx::Postgres;
use std::future::Future;
use std::pin::Pin;

pub async fn execute_strategy(
    node: &Node,
    pool: &Pool<Postgres>,
    execution_date: &String,
) -> Result<Vec<Allocation>, DatabaseError> {
    let allocations = execute_node(node, pool, execution_date, 1.0).await?;
    normalize_weights(&allocations, execution_date)
}

fn execute_node<'a>(
    node: &'a Node,
    pool: &'a Pool<Postgres>,
    execution_date: &'a String,
    parent_weight: f64,
) -> Pin<Box<dyn Future<Output = Result<Vec<Allocation>, DatabaseError>> + Send + 'a>> {
    Box::pin(async move {
        match node {
            Node::Root { weight, children } => {
                execute_children(children, pool, execution_date, weight * parent_weight).await
            }
            Node::Condition {
                weight,
                condition,
                if_true,
                if_false,
            } => {
                let condition_met = evaluate_condition(condition, pool, execution_date).await?;
                let selected_node = if condition_met { if_true } else { if_false };
                execute_node(selected_node, pool, execution_date, weight * parent_weight).await
            }
            Node::Group { weight, children } => {
                execute_children(children, pool, execution_date, weight * parent_weight).await
            }
            Node::Weighting { weight, children } => {
                execute_children(children, pool, execution_date, weight * parent_weight).await
            }
            Node::Filter {
                weight,
                children,
                universe,
                sort,
                select,
            } => {
                let filtered_allocations = apply_filter(
                    pool,
                    &FilterConfig {
                        universe: universe.clone(),
                        sort: sort.clone(),
                        select: select.clone(),
                    },
                    execution_date,
                )
                .await?;
                Ok(filtered_allocations
                    .into_iter()
                    .map(|(ticker, weight)| Allocation {
                        ticker,
                        weight: weight * parent_weight,
                        date: execution_date.clone(),
                    })
                    .collect())
            }
            Node::Asset { ticker, weight } => Ok(vec![Allocation {
                ticker: ticker.clone(),
                weight: weight * parent_weight,
                date: execution_date.clone(),
            }]),
        }
    })
}

async fn execute_children(
    children: &[Node],
    pool: &Pool<Postgres>,
    execution_date: &String,
    parent_weight: f64,
) -> Result<Vec<Allocation>, DatabaseError> {
    let mut allocations = Vec::new();
    for child in children {
        let mut child_allocations =
            execute_node(child, pool, execution_date, parent_weight).await?;
        allocations.append(&mut child_allocations);
    }
    Ok(allocations)
}

async fn evaluate_condition(
    condition: &Condition,
    pool: &Pool<Postgres>,
    execution_date: &String,
) -> Result<bool, DatabaseError> {
    let ticker = &condition.params[0];
    let period: i64 = condition.params[1].parse().map_err(|_| {
        DatabaseError::InvalidInput("Invalid period parameter in condition".to_string())
    })?;

    // Get the left side value
    let left_value = match condition.function.as_str() {
        "cumulative_return" => {
            database_functions::get_cumulative_return(pool, ticker, execution_date, period).await?
        }
        "rsi" => database_functions::get_rsi(pool, ticker, execution_date, period).await?,
        "sma" => database_functions::get_sma(pool, ticker, execution_date, period).await?,
        "ema" => database_functions::get_ema(pool, ticker, execution_date, period).await?,
        _ => {
            return Err(DatabaseError::InvalidInput(format!(
                "Unknown function: {}",
                condition.function
            )))
        }
    };

    // Get the right side value
    let right_value = match &condition.value {
        ConditionValue::Static(value) => *value,
        ConditionValue::Dynamic { function, params } => {
            match function.as_str() {
                "current_price" => {
                    database_functions::get_current_price(pool, &params[0], execution_date)
                        .await?
                        .close
                }
                // Add other functions as needed
                _ => {
                    return Err(DatabaseError::InvalidInput(format!(
                        "Unknown function in value: {}",
                        function
                    )))
                }
            }
        }
    };

    Ok(match condition.operator.as_str() {
        ">" => left_value > right_value,
        "<" => left_value < right_value,
        ">=" => left_value >= right_value,
        "<=" => left_value <= right_value,
        "==" => (left_value - right_value).abs() < f64::EPSILON,
        _ => {
            return Err(DatabaseError::InvalidInput(format!(
                "Unknown operator: {}",
                condition.operator
            )))
        }
    })
}

fn normalize_weights(
    allocations: &[Allocation],
    execution_date: &String,
) -> Result<Vec<Allocation>, DatabaseError> {
    let total_weight: f64 = allocations.iter().map(|a| a.weight).sum();
    if total_weight == 0.0 {
        return Err(DatabaseError::InvalidCalculation(
            "Total allocation weight is zero".to_string(),
        ));
    }

    Ok(allocations
        .iter()
        .map(|a| Allocation {
            ticker: a.ticker.clone(),
            weight: a.weight / total_weight,
            date: execution_date.clone(),
        })
        .collect())
}
