use crate::models::{Condition, Node};
use std::error::Error as StdError;
use thiserror::Error;

const MAX_TREE_DEPTH: usize = 100;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Unknown function: {0}")]
    UnknownFunction(String),
    #[error("Invalid parameters for function {function}: {message}")]
    InvalidParameters { function: String, message: String },
    #[error("Invalid operator: {0}")]
    InvalidOperator(String),
    #[error("Invalid weight: {0}")]
    InvalidWeight(f64),
    #[error("Invalid Group: {0}")]
    InvalidGroup(String),
    #[error("Invalid ticker: {0}")]
    InvalidTicker(String),
    #[error("Invalid value range for {function}: {message}")]
    InvalidValueRange { function: String, message: String },
    #[error("Invalid period range for {function}: {message}")]
    InvalidPeriodRange { function: String, message: String },
    #[error("Maximum tree depth of {0} exceeded")]
    MaxDepthExceeded(usize),
    #[error("Floating-point equality not allowed for {0}")]
    FloatingPointEqualityNotAllowed(String),
    #[error("Insufficient data points for {function}: minimum required is {required}, period specified is {specified}")]
    InsufficientDataPoints {
        function: String,
        required: u16,
        specified: u16,
    },

    #[error("{function} must be a positive integer, got {value}")]
    NonIntegerValue { function: String, value: u32 },
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
    validate_node_with_depth(node, 0)?;
    Ok(())
}

fn validate_node_with_depth(node: &Node, depth: usize) -> Result<(), ValidationError> {
    if depth > MAX_TREE_DEPTH {
        return Err(ValidationError::MaxDepthExceeded(MAX_TREE_DEPTH));
    }

    match node {
        Node::Root {
            weight: _,
            children,
        } => {
            // Add root children weight validation
            let total_weight: f64 = children
                .iter()
                .map(|child| match child {
                    Node::Condition { weight, .. }
                    | Node::Asset { weight, .. }
                    | Node::Group { weight, .. }
                    | Node::Weighting { weight, .. } => *weight,
                    _ => 0.0,
                })
                .sum();

            if (total_weight - 1.0).abs() > 0.0001 {
                return Err(ValidationError::InvalidWeight(total_weight));
            }

            children
                .iter()
                .try_for_each(|child| validate_node_with_depth(child, depth + 1))?;
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
            validate_node_with_depth(if_true, depth + 1)?;
            validate_node_with_depth(if_false, depth + 1)?;
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
            let weight_sum: f64 = children
                .iter()
                .map(|child| match child {
                    Node::Asset { weight, .. } => *weight,
                    _ => 0.0,
                })
                .sum();

            if (weight_sum - 1.0).abs() > 0.0001 {
                return Err(ValidationError::InvalidGroup(format!(
                    "Group weights must sum to 1.0, got {}",
                    weight_sum
                )));
            }

            children
                .iter()
                .try_for_each(|child| validate_node_with_depth(child, depth + 1))?;
        }
        Node::Asset { weight, ticker } => {
            validate_weight(*weight)?;
            validate_ticker(ticker)?;
        }
    }
    Ok(())
}

fn validate_weight(weight: f64) -> Result<(), ValidationError> {
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
        "cumulative_return" | "price_std_dev" | "returns_std_dev" | "ma_of_returns"
        | "ma_of_price" | "max_drawdown" => {
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
        "rsi" | "sma" | "ema" => {
            if condition.params.len() != 2 {
                return Err(ValidationError::InvalidParameters {
                    function: condition.function.clone(),
                    message: format!("{} requires ticker and period", condition.function),
                });
            }
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

    // Add value range validations for floating-point functions
    match condition.function.as_str() {
        "cumulative_return" => {
            validate_value_range("cumulative_return", condition.value as f32, -100.0, 1000.0)?;
        }
        "price_std_dev" | "returns_std_dev" => {
            validate_value_range(
                &condition.function,
                condition.value as f32,
                0.0,
                f32::INFINITY,
            )?;
        }
        _ => {}
    }

    // Add integer validation for specific functions
    match condition.function.as_str() {
        "rsi" | "sma" | "ema" => {
            // Convert value to unsigned integer
            let value = condition.value as u32;
            if condition.value.fract() != 0.0 || condition.value < 0.0 {
                return Err(ValidationError::NonIntegerValue {
                    function: condition.function.clone(),
                    value,
                });
            }

            // Validate value ranges with unsigned integers (1 to 1000)
            validate_value_range_uint(&condition.function, value, 1, 1000)?;

            // Validate period is also a positive integer (1 to 1000)
            if let Some(period_str) = condition.params.get(1) {
                let period =
                    period_str
                        .parse::<u32>()
                        .map_err(|_| ValidationError::InvalidParameters {
                            function: condition.function.clone(),
                            message: "Period must be a positive integer".to_string(),
                        })?;

                validate_period_range(&condition.function, period as i32, 1, 1000)?;
            }
        }
        // For floating point metrics, disallow equality comparisons
        "cumulative_return" | "price_std_dev" | "returns_std_dev" | "current_price"
        | "ma_of_returns" | "ma_of_price" | "max_drawdown" => {
            if condition.operator == "==" {
                return Err(ValidationError::FloatingPointEqualityNotAllowed(
                    condition.function.clone(),
                ));
            }
        }
        _ => unreachable!(),
    }

    // Add minimum data points validation
    match condition.function.as_str() {
        "rsi" => {
            let period: u16 = condition.params[1].parse().unwrap();
            if period < 2 {
                // RSI needs at least 2 data points to calculate
                return Err(ValidationError::InsufficientDataPoints {
                    function: condition.function.clone(),
                    required: 2,
                    specified: period,
                });
            }
        }
        "sma" | "ema" => {
            let period: u16 = condition.params[1].parse().unwrap();
            if period < 2 {
                return Err(ValidationError::InsufficientDataPoints {
                    function: condition.function.clone(),
                    required: 2,
                    specified: period,
                });
            }
        }
        "max_drawdown" => {
            let period: u16 = condition.params[1].parse().unwrap();
            if period < 2 {
                return Err(ValidationError::InsufficientDataPoints {
                    function: condition.function.clone(),
                    required: 2,
                    specified: period,
                });
            }
        }
        _ => {}
    }

    Ok(())
}
fn validate_ticker(ticker: &str) -> Result<(), ValidationError> {
    if ticker.is_empty() || ticker.len() > 5 || !ticker.chars().all(|c| c.is_ascii_uppercase()) {
        return Err(ValidationError::InvalidTicker(ticker.to_string()));
    }
    Ok(())
}

fn validate_value_range(
    function: &str,
    value: f32,
    min: f32,
    max: f32,
) -> Result<(), ValidationError> {
    if !(min..=max).contains(&value) {
        return Err(ValidationError::InvalidValueRange {
            function: function.to_string(),
            message: format!("Value must be between {} and {}, got {}", min, max, value),
        });
    }
    Ok(())
}

fn validate_period_range(
    function: &str,
    period: i32,
    min: i32,
    max: i32,
) -> Result<(), ValidationError> {
    if !(min..=max).contains(&period) {
        return Err(ValidationError::InvalidPeriodRange {
            function: function.to_string(),
            message: format!("Period must be between {} and {}, got {}", min, max, period),
        });
    }
    Ok(())
}

// Update the validate_value_range_uint function
fn validate_value_range_uint(
    function: &str,
    value: u32,
    min: u32,
    max: u32,
) -> Result<(), ValidationError> {
    if value < min || value > max {
        return Err(ValidationError::InvalidValueRange {
            function: function.to_string(),
            message: format!("Value must be between {} and {}, got {}", min, max, value),
        });
    }
    Ok(())
}

// pub fn serialize_to_json(node: &Node) -> Result<String, Box<dyn StdError>> {
//     let serialized_json = serde_json::to_string_pretty(node)?;
//     Ok(serialized_json)
// }

//TEST FILES

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_weight_validation() {
        let json = r#"{
            "type": "root",
            "weight": 1.0,
            "children": [
                { "type": "asset", "ticker": "SPY", "weight": 0.3 },
                { "type": "asset", "ticker": "QQQ", "weight": 0.3 }
            ]
        }"#;

        let result = deserialize_json(json);
        assert!(
            result.is_err(),
            "Should fail when children weights don't sum to 1.0"
        );
    }

    #[test]
    fn test_group_weight_validation() {
        let json = r#"{
            "type": "root",
            "weight": 1.0,
            "children": [{
                "type": "group",
                "weight": 1.0,
                "children": [
                    { "type": "asset", "ticker": "SPY", "weight": 0.3 },
                    { "type": "asset", "ticker": "QQQ", "weight": 0.3 }
                ]
            }]
        }"#;

        let result = deserialize_json(json);
        assert!(
            result.is_err(),
            "Should fail when group weights don't sum to 1.0"
        );
    }

    #[test]
    fn test_ticker_validation() {
        let json = r#"{
            "type": "root",
            "weight": 1.0,
            "children": [
                { "type": "asset", "ticker": "spy", "weight": 1.0 }
            ]
        }"#;

        let result = deserialize_json(json);
        assert!(result.is_err(), "Should fail with lowercase ticker");
    }

    #[test]
    fn test_condition_validation() {
        let json = r#"{
            "type": "root",
            "weight": 1.0,
            "children": [{
                "type": "condition",
                "weight": 1.0,
                "condition": {
                    "function": "unknown_function",
                    "params": ["SPY", "14"],
                    "operator": ">",
                    "value": 30
                },
                "if_true": { "type": "asset", "ticker": "SPY", "weight": 1.0 },
                "if_false": { "type": "asset", "ticker": "QQQ", "weight": 1.0 }
            }]
        }"#;

        let result = deserialize_json(json);
        assert!(result.is_err(), "Should fail with unknown function");
    }

    #[test]
    fn test_valid_strategy() {
        let json = r#"{
            "type": "root",
            "weight": 1.0,
            "children": [{
                "type": "condition",
                "weight": 1.0,
                "condition": {
                    "function": "rsi",
                    "params": ["SPY", "14"],
                    "operator": "<",
                    "value": 30
                },
                "if_true": { "type": "asset", "ticker": "SPY", "weight": 1.0 },
                "if_false": { "type": "asset", "ticker": "QQQ", "weight": 1.0 }
            }]
        }"#;

        let result = deserialize_json(json);
        assert!(result.is_ok(), "Should accept valid strategy");
    }

    #[test]
    fn test_valid_weighting_node() {
        let json = r#"{
            "type": "root",
            "weight": 1.0,
            "children": [{
                "type": "weighting",
                "weight": 1.0,
                "children": [
                    { "type": "asset", "ticker": "SPY", "weight": 0.6 },
                    { "type": "asset", "ticker": "QQQ", "weight": 0.4 }
                ]
            }]
        }"#;

        let result = deserialize_json(json);
        assert!(result.is_ok(), "Should accept valid weighting node");
    }
}
