// use crate::models::*;
// use serde::{Deserialize, Serialize};
// use std::error::Error as StdError;
// use thiserror::Error; // Add this import

// const MAX_TREE_DEPTH: usize = 100;
// const VALID_FUNCTIONS: [&str; 10] = [
//     "cumulative_return",
//     "rsi",
//     "sma",
//     "ema",
//     "price_std_dev",
//     "returns_std_dev",
//     "ma_of_returns",
//     "ma_of_price",
//     "current_price",
//     "max_drawdown",
// ];

// #[derive(Debug, Error)]
// pub enum ValidationError {
//     #[error("Invalid blocktype: {0}")]
//     InvalidBlockType(String),

//     #[error("Invalid weight configuration: {0}")]
//     InvalidWeight(String),

//     #[error("Invalid function: {0}")]
//     InvalidFunction(String),

//     #[error("Invalid condition: {0}")]
//     InvalidCondition(String),

//     #[error("Invalid filter: {0}")]
//     InvalidFilter(String),

//     #[error("Invalid asset: {0}")]
//     InvalidAsset(String),

//     #[error("Maximum tree depth exceeded: {0}")]
//     MaxDepthExceeded(usize),

//     #[error("Invalid group: {0}")]
//     InvalidGroup(String),

//     #[error("JSON parsing error: {0}")]
//     JsonParse(#[from] serde_json::Error),
// }

// pub fn deserialize_json(json_str: &str) -> Result<Block, Box<dyn StdError>> {
//     println!("Attempting to parse JSON:\n{}", json_str);
//     let block: Result<Block, serde_json::Error> = serde_json::from_str(json_str);

//     match &block {
//         Ok(b) => println!("Successfully parsed block type: {}", b.blocktype),
//         Err(e) => println!(
//             "Error at line {}, column {}: {}",
//             e.line(),
//             e.column(),
//             e.to_string()
//         ),
//     }

//     let block = block?;
//     validate_block(&block, 0)?;
//     Ok(block)
// }

// pub fn validate_block(block: &Block, depth: usize) -> Result<(), ValidationError> {
//     // Check max depth
//     if depth > MAX_TREE_DEPTH {
//         return Err(ValidationError::MaxDepthExceeded(depth));
//     }

//     // Validate blocktype and attributes
//     match block.blocktype.as_str() {
//         "Group" => validate_group(block)?,
//         "Weight" => validate_weight(block)?,
//         "Condition" => validate_condition(block)?,
//         "Filter" => validate_filter(block)?,
//         "Asset" => validate_asset(block)?,
//         _ => return Err(ValidationError::InvalidBlockType(block.blocktype.clone())),
//     }

//     // Recursively validate children
//     if let Some(children) = &block.children {
//         for child in children {
//             validate_block(child, depth + 1)?;
//         }
//     }

//     Ok(())
// }

// fn validate_group(block: &Block) -> Result<(), ValidationError> {
//     match &block.attributes {
//         Attributes::Group { name } => {
//             if name.is_empty() {
//                 return Err(ValidationError::InvalidGroup(
//                     "Group name cannot be empty".to_string(),
//                 ));
//             }
//             Ok(())
//         }
//         _ => Err(ValidationError::InvalidGroup(
//             "Invalid group attributes".to_string(),
//         )),
//     }
// }

// // In json_operations.rs

// fn validate_weight(block: &Block) -> Result<(), ValidationError> {
//     match &block.attributes {
//         Attributes::Weight {
//             type_,
//             allocation_type,
//             values,
//             window_of_trading_days,
//         } => match type_.as_str() {
//             "equal" => Ok(()),
//             "specified" => {
//                 if allocation_type != "percentage" {
//                     return Err(ValidationError::InvalidWeight(
//                         "allocation_type must be 'percentage'".to_string(),
//                     ));
//                 }

//                 let sum: f64 = values.iter().sum();
//                 if (sum - 100.0).abs() > 0.001 {
//                     return Err(ValidationError::InvalidWeight(format!(
//                         "Weights must sum to 100%, got {}",
//                         sum
//                     )));
//                 }
//                 Ok(())
//             }
//             "inverse_volatility" => {
//                 if window_of_trading_days.unwrap_or(0) < 1 {
//                     return Err(ValidationError::InvalidWeight(
//                         "window_of_trading_days must be positive".to_string(),
//                     ));
//                 }
//                 Ok(())
//             }
//             "market_cap" => Ok(()),
//             _ => Err(ValidationError::InvalidWeight(format!(
//                 "Invalid weight type: {}",
//                 type_
//             ))),
//         },
//         _ => Err(ValidationError::InvalidWeight(
//             "Invalid weight attributes".to_string(),
//         )),
//     }
// }

// fn validate_condition(block: &Block) -> Result<(), ValidationError> {
//     match &block.attributes {
//         Attributes::Condition { condition } => {
//             // Validate function
//             validate_function(&condition.function)?;

//             // Validate operator
//             match condition.operator.as_str() {
//                 ">" | "<" | ">=" | "<=" | "==" => Ok(()),
//                 _ => Err(ValidationError::InvalidCondition(format!(
//                     "Invalid operator: {}",
//                     condition.operator
//                 ))),
//             }?;

//             // Validate compare_to
//             validate_compare_to(&condition.compare_to)?;

//             Ok(())
//         }
//         _ => Err(ValidationError::InvalidCondition(
//             "Invalid condition attributes".to_string(),
//         )),
//     }
// }

// fn validate_function(func: &FunctionDefinition) -> Result<(), ValidationError> {
//     // Validate function name
//     if !VALID_FUNCTIONS.contains(&func.function_name.as_str()) {
//         return Err(ValidationError::InvalidFunction(format!(
//             "Unknown function: {}",
//             func.function_name
//         )));
//     }

//     // Validate window_of_days if present
//     if let Some(days) = func.window_of_days {
//         if days < 1 || days > 1000 {
//             return Err(ValidationError::InvalidFunction(format!(
//                 "Invalid window_of_days: {}",
//                 days
//             )));
//         }
//     }

//     // Validate asset ticker
//     validate_ticker(&func.asset)?;

//     Ok(())
// }

// fn validate_compare_to(compare_to: &CompareToValue) -> Result<(), ValidationError> {
//     match compare_to {
//         CompareToValue::Function { function } => validate_function(function),
//         CompareToValue::Fixed { value, .. } => {
//             if !value.is_finite() {
//                 return Err(ValidationError::InvalidCondition(
//                     "Compare value must be finite".to_string(),
//                 ));
//             }
//             Ok(())
//         }
//     }
// }

// fn validate_filter(block: &Block) -> Result<(), ValidationError> {
//     match &block.attributes {
//         Attributes::Filter {
//             sort_function,
//             select,
//         } => {
//             // Add validation for select.option
//             if !["Top", "Bottom"].contains(&select.option.as_str()) {
//                 return Err(ValidationError::InvalidFilter(
//                     "Select option must be 'Top' or 'Bottom'".to_string(),
//                 ));
//             }

//             // Rest of the validation remains the same
//             if !VALID_FUNCTIONS.contains(&sort_function.function_name.as_str()) {
//                 return Err(ValidationError::InvalidFilter(format!(
//                     "Invalid sort function: {}",
//                     sort_function.function_name
//                 )));
//             }

//             if select.amount < 1 {
//                 return Err(ValidationError::InvalidFilter(
//                     "Select amount must be positive".to_string(),
//                 ));
//             }

//             Ok(())
//         }
//         _ => Err(ValidationError::InvalidFilter(
//             "Invalid filter attributes".to_string(),
//         )),
//     }
// }

// fn validate_asset(block: &Block) -> Result<(), ValidationError> {
//     match &block.attributes {
//         Attributes::Asset {
//             ticker, exchange, ..
//         } => {
//             validate_ticker(ticker)?;
//             if exchange.is_empty() {
//                 return Err(ValidationError::InvalidAsset(
//                     "Exchange cannot be empty".to_string(),
//                 ));
//             }
//             Ok(())
//         }
//         _ => Err(ValidationError::InvalidAsset(
//             "Invalid asset attributes".to_string(),
//         )),
//     }
// }

// fn validate_ticker(ticker: &str) -> Result<(), ValidationError> {
//     if ticker.is_empty() || ticker.len() > 5 || !ticker.chars().all(|c| c.is_ascii_uppercase()) {
//         return Err(ValidationError::InvalidAsset(format!(
//             "Invalid ticker: {}",
//             ticker
//         )));
//     }
//     Ok(())
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_input_json() {
//         let json = include_str!("../input.json");
//         let result = deserialize_json(json);
//         assert!(
//             result.is_ok(),
//             "Failed to parse input.json: {:?}",
//             result.err()
//         );
//     }

//     #[test]
//     fn test_problematic_json() {
//         let json = r#"{
//         "blocktype": "Condition",
//         "attributes": {
//             "condition": {
//                 "function": {
//                     "function_name": "cumulative_return",
//                     "window_of_days": 50,
//                     "asset": "QQQ"
//                 },
//                 "operator": "<",
//                 "compare_to": {
//                     "type": "function",
//                     "function": {
//                         "function_name": "current_price",
//                         "window_of_days": null,
//                         "asset": "QQQ"
//                     }
//                 }
//             }
//         },
//         "children": []
//     }"#;

//         let result = deserialize_json(json);
//         assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
//     }

//     #[test]
//     fn test_filtertest_json() {
//         let json = include_str!("../filtertest.json");
//         let result = deserialize_json(json);
//         assert!(
//             result.is_ok(),
//             "Failed to parse filtertest.json: {:?}",
//             result.err()
//         );
//     }
// }

// #[test]
// fn test_valid_group() {
//     let json = r#"{
//             "blocktype": "Group",
//             "attributes": {
//                 "name": "Test Group"
//             },
//             "children": []
//         }"#;

//     let result = deserialize_json(json);
//     assert!(result.is_ok());
// }

// // Weitere Tests hier...
