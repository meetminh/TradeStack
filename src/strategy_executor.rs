use crate::database_functions::{self, DatabaseError};
use crate::models::{Allocation, Condition, Node};
use sqlx::Pool;
use sqlx::Postgres;

pub async fn execute_strategy(
    node: &Node,
    pool: &Pool<Postgres>,
    execution_date: &String,
) -> Result<Vec<Allocation>, DatabaseError> {
    let allocations = execute_node(node, pool, execution_date, 1.0).await?;
    normalize_weights(&allocations)
}

async fn execute_node(
    node: &Node,
    pool: &Pool<Postgres>,
    execution_date: &String,
    parent_weight: f64,
) -> Result<Vec<Allocation>, DatabaseError> {
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
        Node::Group { weight, children } | Node::Weighting { weight, children } => {
            execute_children(children, pool, execution_date, weight * parent_weight).await
        }
        Node::Asset { ticker, weight } => Ok(vec![Allocation {
            ticker: ticker.clone(),
            weight: weight * parent_weight,
            date: execution_date.clone(),
        }]),
    }
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

    let value = match condition.function.as_str() {
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

    Ok(match condition.operator.as_str() {
        ">" => value > condition.value as f64,
        "<" => value < condition.value as f64,
        ">=" => value >= condition.value as f64,
        "<=" => value <= condition.value as f64,
        "==" => (value - condition.value as f64).abs() < f64::EPSILON,
        _ => {
            return Err(DatabaseError::InvalidInput(format!(
                "Unknown operator: {}",
                condition.operator
            )))
        }
    })
}

fn normalize_weights(allocations: &[Allocation]) -> Result<Vec<Allocation>, DatabaseError> {
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
            date: a.date.clone(),
        })
        .collect())
}
