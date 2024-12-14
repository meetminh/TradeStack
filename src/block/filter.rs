use crate::block::database_functions::{self, DatabaseError};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterConfig {
    pub universe: Vec<String>,
    pub sort: SortConfig,
    pub select: SelectConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SortConfig {
    pub function: String,
    pub params: Vec<String>,
    pub order: String, // "ASC" or "DESC"
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectConfig {
    pub direction: String, // "TOP" or "BOTTOM"
    pub count: i32,
    pub weights: Vec<f64>,
}

pub async fn apply_filter(
    pool: &Pool<Postgres>,
    config: &FilterConfig,
    execution_date: &String,
) -> Result<Vec<(String, f64)>, DatabaseError> {
    // Input validation
    if config.universe.is_empty() {
        return Err(DatabaseError::InvalidInput(
            "Universe cannot be empty".to_string(),
        ));
    }

    let weight_sum: f64 = config.select.weights.iter().sum();
    if (weight_sum - 1.0).abs() > 0.0001 {
        return Err(DatabaseError::InvalidInput(format!(
            "Weights must sum to 1.0, got {}",
            weight_sum
        )));
    }

    if config.select.weights.len() != config.select.count as usize {
        return Err(DatabaseError::InvalidInput(
            "Number of weights must match count".to_string(),
        ));
    }

    // Validate function name and parameters
    let valid_functions = [
        "current_price",
        "sma",
        "ema",
        "cumulative_return",
        "ma_of_price",
        "ma_of_returns",
        "rsi",
        "max_drawdown",
        "price_std_dev",
    ];
    if !valid_functions.contains(&config.sort.function.as_str()) {
        return Err(DatabaseError::InvalidInput(format!(
            "Invalid function: {}",
            config.sort.function
        )));
    }

    // Step 1: Calculate values for each ticker
    let mut ticker_values = Vec::with_capacity(config.universe.len());

    for ticker in &config.universe {
        let value = match config.sort.function.as_str() {
            "current_price" => {
                database_functions::get_current_price(pool, ticker, execution_date)
                    .await?
                    .close
            }
            "sma" | "ema" | "rsi" | "ma_of_price" | "ma_of_returns" | "price_std_dev" => {
                let period: i64 = config.sort.params[0]
                    .parse()
                    .map_err(|_| DatabaseError::InvalidInput("Invalid period".to_string()))?;

                match config.sort.function.as_str() {
                    "sma" => {
                        database_functions::get_sma(pool, ticker, execution_date, period).await?
                    }
                    "ema" => {
                        database_functions::get_ema(pool, ticker, execution_date, period).await?
                    }
                    "rsi" => {
                        database_functions::get_rsi(pool, ticker, execution_date, period).await?
                    }
                    "ma_of_price" => {
                        database_functions::get_ma_of_price(pool, ticker, execution_date, period)
                            .await?
                    }
                    "ma_of_returns" => {
                        database_functions::get_ma_of_returns(pool, ticker, execution_date, period)
                            .await?
                    }
                    "price_std_dev" => {
                        database_functions::get_price_std_dev(pool, ticker, execution_date, period)
                            .await?
                    }
                    _ => unreachable!(),
                }
            }
            "cumulative_return" => {
                let period: i64 = config.sort.params[0]
                    .parse()
                    .map_err(|_| DatabaseError::InvalidInput("Invalid period".to_string()))?;
                database_functions::get_cumulative_return(pool, ticker, execution_date, period)
                    .await?
            }
            "max_drawdown" => {
                let period: i64 = config.sort.params[0]
                    .parse()
                    .map_err(|_| DatabaseError::InvalidInput("Invalid period".to_string()))?;
                database_functions::get_max_drawdown(pool, ticker, execution_date, period)
                    .await?
                    .max_drawdown_percentage
            }
            _ => unreachable!(),
        };

        ticker_values.push((ticker.clone(), value));
    }

    // Step 2: Sort based on values (using stable sort for consistent results)
    ticker_values.sort_by(|a, b| {
        if config.sort.order == "DESC" {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        }
    });

    // Step 3: Select tickers based on direction and count
    let selected_indices = match config.select.direction.as_str() {
        "TOP" => 0..config.select.count as usize,
        "BOTTOM" => {
            let start = ticker_values.len() - config.select.count as usize;
            start..ticker_values.len()
        }
        _ => return Err(DatabaseError::InvalidInput("Invalid direction".to_string())),
    };

    // Step 4: Apply weights
    let selected: Vec<(String, f64)> = selected_indices
        .zip(&config.select.weights)
        .map(|(i, &weight)| (ticker_values[i].0.clone(), weight))
        .collect();

    Ok(selected)
}
