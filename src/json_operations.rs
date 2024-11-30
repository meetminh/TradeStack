use crate::models::{Condition, Node};
use std::error::Error as StdError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Unknown function: {0}")]
    UnknownFunction(String),
    #[error("Invalid parameters for function {function}: {message}")]
    InvalidParameters { function: String, message: String },
    #[error("Invalid operator: {0}")]
    InvalidOperator(String),
    #[error("Invalid weight: {0}")]
    InvalidWeight(f32),
    #[error("Invalid Group: {0}")]
    InvalidGroup(String),
}

pub fn deserialize_json(json_str: &str) -> Result<Node, Box<dyn StdError>> {
    // Parse the JSON string into our Node structure
    let deserialized_tree: Node = serde_json::from_str(json_str)?;
    // Check if the tree structure is valid according to our custom rules
    validate_node(&deserialized_tree)?;
    // If everything is okay, return the tree structure
    Ok(deserialized_tree)
}

pub fn validate_node(node: &Node) -> Result<(), ValidationError> {
    match node {
        Node::Root { weight, children } => {
            validate_weight(*weight)?;
            children.iter().try_for_each(validate_node)?;
        }
        Node::Condition {
            weight,
            condition,
            if_true,
            if_false,
            ..
        } => {
            validate_weight(*weight)?;
            validate_condition(condition)?;
            validate_node(if_true)?;
            validate_node(if_false)?;
        }
        Node::Group { weight, children } | Node::Weighting { weight, children } => {
            validate_weight(*weight)?;

            // Check if group is empty
            if children.is_empty() {
                return Err(ValidationError::InvalidGroup(
                    "Group cannot be empty".to_string(),
                ));
            }

            // Validate sum of weights equals 1.0
            let weight_sum: f32 = children
                .iter()
                .map(|child| match child {
                    Node::Asset { weight, .. } => *weight,
                    _ => 0.0,
                })
                .sum();

            if (weight_sum - 1.0).abs() > 0.0001 {
                // Allow small floating-point differences
                return Err(ValidationError::InvalidGroup(format!(
                    "Group weights must sum to 1.0, got {}",
                    weight_sum
                )));
            }

            children.iter().try_for_each(validate_node)?;
        }
        Node::Asset { weight, .. } => {
            validate_weight(*weight)?;
        }
    }
    Ok(())
}

fn validate_weight(weight: f32) -> Result<(), ValidationError> {
    if !(0.0..=1.0).contains(&weight) {
        return Err(ValidationError::InvalidWeight(weight));
    }
    Ok(())
}

fn validate_condition(condition: &Condition) -> Result<(), ValidationError> {
    // Validate function name
    let valid_functions = [
        "cumulative_return", // get_cumulative_return
        "rsi",               // get_rsi
        "sma",               // get_sma
        "ema",               // get_ema
        "price_std_dev",     // get_price_std_dev
        "returns_std_dev",   // get_returns_std_dev
        "ma_of_returns",     // get_ma_of_returns
        "ma_of_price",       // get_ma_of_price
        "current_price",     // get_current_price
        "max_drawdown",      // get_max_drawdown
    ];

    if !valid_functions.contains(&condition.function.as_str()) {
        return Err(ValidationError::UnknownFunction(condition.function.clone()));
    }

    // Validate operator
    let valid_operators = [">", "<", ">=", "<=", "=="];
    if !valid_operators.contains(&condition.operator.as_str()) {
        return Err(ValidationError::InvalidOperator(condition.operator.clone()));
    }

    // Validate parameters based on function
    match condition.function.as_str() {
        // Functions requiring ticker and period
        "cumulative_return" | "sma" | "ema" | "price_std_dev" | "returns_std_dev"
        | "ma_of_returns" | "ma_of_price" | "max_drawdown" => {
            if condition.params.len() != 2 {
                return Err(ValidationError::InvalidParameters {
                    function: condition.function.clone(),
                    message: format!(
                        "Expected 2 parameters (ticker, period), got {}",
                        condition.params.len()
                    ),
                });
            }
            // Validate period is a number
            if let Err(_) = condition.params[1].parse::<i32>() {
                return Err(ValidationError::InvalidParameters {
                    function: condition.function.clone(),
                    message: format!("Period must be a number, got {}", condition.params[1]),
                });
            }
        }
        "rsi" => {
            if condition.params.len() != 2 {
                return Err(ValidationError::InvalidParameters {
                    function: condition.function.clone(),
                    message: "RSI requires ticker and period".to_string(),
                });
            }
            // Validate RSI period specifically
            match condition.params[1].parse::<i32>() {
                Ok(period) if period >= 14 && period <= 100 => Ok(()),
                Ok(period) => Err(ValidationError::InvalidParameters {
                    function: condition.function.clone(),
                    message: format!("RSI period must be between 14 and 100, got {}", period),
                }),
                Err(_) => Err(ValidationError::InvalidParameters {
                    function: condition.function.clone(),
                    message: "RSI period must be a number".to_string(),
                }),
            }?;
        }
        // Functions requiring only ticker
        "current_price" => {
            if condition.params.len() != 1 {
                return Err(ValidationError::InvalidParameters {
                    function: condition.function.clone(),
                    message: format!(
                        "Expected 1 parameter (ticker), got {}",
                        condition.params.len()
                    ),
                });
            }
        }
        _ => unreachable!(), // We've already validated function names
    }

    Ok(())
}

// pub fn serialize_to_json(node: &Node) -> Result<String, Box<dyn StdError>> {
//     let serialized_json = serde_json::to_string_pretty(node)?;
//     Ok(serialized_json)
// }
