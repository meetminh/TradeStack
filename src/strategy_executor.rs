use crate::database_functions::{self, DatabaseError};
use crate::models::{Condition, Node};
use chrono::{DateTime, Utc};
use sqlx::Pool;
use sqlx::Postgres;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] DatabaseError),
    #[error("Invalid condition: {0}")]
    InvalidCondition(String),
    #[error("Strategy execution error: {0}")]
    ExecutionError(String),
}

#[derive(Debug)]
pub struct Allocation {
    pub ticker: String,
    pub weight: f32,
    pub date: DateTime<Utc>,
}

async fn evaluate_condition(
    condition: &Condition,
    pool: &Pool<Postgres>,
    date: DateTime<Utc>,
) -> Result<bool, ExecutionError> {
    match condition.function.as_str() {
        "cumulative_return" => {
            let period = condition.params[1]
                .parse::<i32>()
                .map_err(|_| ExecutionError::InvalidCondition("Invalid period".to_string()))?;

            let value = database_functions::get_cumulative_return(
                pool,
                condition.params[0].clone(),
                date,
                period,
            )
            .await?;

            Ok(match condition.operator.as_str() {
                ">" => value > condition.value as f64,
                "<" => value < condition.value as f64,
                ">=" => value >= condition.value as f64,
                "<=" => value <= condition.value as f64,
                _ => false,
            })
        }
        "rsi" => {
            let period = condition.params[1]
                .parse::<i32>()
                .map_err(|_| ExecutionError::InvalidCondition("Invalid period".to_string()))?;

            let value =
                database_functions::get_rsi(pool, condition.params[0].clone(), date, period)
                    .await?;

            Ok(match condition.operator.as_str() {
                ">" => value > condition.value as f64,
                "<" => value < condition.value as f64,
                ">=" => value >= condition.value as f64,
                "<=" => value <= condition.value as f64,
                _ => false,
            })
        }
        _ => Err(ExecutionError::InvalidCondition(format!(
            "Unknown function: {}",
            condition.function
        ))),
    }
}

pub async fn execute_strategy(
    node: &Node,
    pool: &Pool<Postgres>,
    date: DateTime<Utc>,
) -> Result<Vec<Allocation>, ExecutionError> {
    match node {
        Node::Root { children, .. } => {
            let mut allocations = Vec::new();
            for child in children {
                let mut child_allocations = Box::pin(execute_strategy(child, pool, date)).await?;
                allocations.append(&mut child_allocations);
            }
            Ok(allocations)
        }
        Node::Condition {
            condition,
            if_true,
            if_false,
            weight,
            ..
        } => {
            let condition_result = evaluate_condition(condition, pool, date).await?;
            let selected_branch = if condition_result { if_true } else { if_false };
            let mut allocations = execute_strategy(selected_branch, pool, date).await?;

            // Apply condition weight to all allocations
            for allocation in &mut allocations {
                allocation.weight *= weight;
            }
            Ok(allocations)
        }
        Node::Group {
            children, weight, ..
        }
        | Node::Weighting {
            children, weight, ..
        } => {
            let mut allocations = Vec::new();
            for child in children {
                let mut child_allocations = execute_strategy(child, pool, date).await?;
                for allocation in &mut child_allocations {
                    allocation.weight *= weight;
                }
                allocations.append(&mut child_allocations);
            }
            Ok(allocations)
        }
        Node::Asset { ticker, weight } => Ok(vec![Allocation {
            ticker: ticker.clone(),
            weight: *weight,
            date,
        }]),
    }
}
