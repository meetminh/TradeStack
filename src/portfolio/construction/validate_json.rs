//! Investment portfolio block system validator.
//! Implements validation rules for all block types and their configurations.

use crate::portfolio::blocks::models::{
    Block, BlockAttributes, BlockType, WeightType, AllocationType, CompareToValue, FunctionDefinition, FunctionName,
};
use thiserror::Error;

/// Custom error types for validation failures
#[derive(Error, Debug, PartialEq)]
pub enum ValidationError {
    #[error("Group block validation failed: {0}")]
    GroupError(GroupError),

    #[error("Weight block validation failed: {0}")]
    WeightError(WeightError),

    #[error("Condition block validation failed: {0}")]
    ConditionError(ConditionError),

    #[error("Filter block validation failed: {0}")]
    FilterError(FilterError),

    #[error("Asset block validation failed: {0}")]
    AssetError(AssetError),

    #[error("Block type mismatch: expected {expected}, got {found}")]
    BlockTypeMismatch { expected: String, found: String },
}

/// Group block specific errors
#[derive(Debug, Error, PartialEq)]
pub enum GroupError {
    #[error("Group block must have at least one child")]
    NoChildren,

    #[error("First child must be a Weight block")]
    FirstChildNotWeight,

    #[error("Missing name attribute")]
    MissingName,
}

/// Weight block specific errors
#[derive(Debug, Error, PartialEq)]
pub enum WeightError {
    #[error("Values array length ({found}) does not match number of children ({expected})")]
    ValueChildrenMismatch { expected: usize, found: usize },

    #[error("Percentage values must sum to 100 (current sum: {sum:.2})")]
    InvalidPercentageSum { sum: f64 },

    #[error("Missing window_of_trading_days for inverse_volatility weight type")]
    MissingVolatilityWindow,

    #[error("Missing allocation type for specified weights")]
    MissingAllocationType,

    #[error("Missing values array for specified weights")]
    MissingValues,

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
}

/// Condition block specific errors
#[derive(Debug, Error, PartialEq)]
pub enum ConditionError {
    #[error("Must have exactly 2 children (found {0})")]
    InvalidChildCount(usize),

    #[error("Function validation failed: {0}")]
    FunctionError(String),

    #[error("Window of days not allowed for current_price function")]
    InvalidWindowDays,

    #[error("Missing window of days for {0} function")]
    MissingWindowDays(String),
}

/// Filter block specific errors
#[derive(Debug, Error, PartialEq)]
pub enum FilterError {
    #[error("Invalid sort function: {0}")]
    InvalidSortFunction(String),

    #[error("Child at position {0} must be an Asset block")]
    NonAssetChild(usize),

    #[error("Missing sort function configuration")]
    MissingSortFunction,

    #[error("Missing select configuration")]
    MissingSelectConfig,
}

/// Asset block specific errors
#[derive(Debug, Error, PartialEq)]
pub enum AssetError {
    #[error("Asset blocks cannot have children")]
    HasChildren,

    #[error("Missing ticker symbol")]
    MissingTicker,

    #[error("Missing company name")]
    MissingCompanyName,

    #[error("Missing exchange")]
    MissingExchange,
}

/// Validation trait for block structures
pub trait Validate {
    fn validate(&self) -> Result<(), ValidationError>;
}

pub fn deserialize_json(json_str: &str) -> Result<Block, Box<dyn std::error::Error>> {
    // println!("Attempting to deserialize JSON:");
    // println!("{}", json_str);

    match serde_json::from_str::<Block>(json_str) {
        Ok(block) => {
            // println!("Successfully deserialized to:");
            // println!("{:#?}", block);
            Ok(block)
        }
        Err(e) => {
            println!("Deserialization error details:");
            println!("Error: {}", e);
            println!("Location: line {}, column {}", e.line(), e.column());

            // Try to deserialize into Value first to see the raw structure
            if let Ok(raw_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                println!("\nRaw JSON structure:");
                println!("{:#?}", raw_value);
            }

            Err(e.into())
        }
    }
}

impl Validate for Block {
    fn validate(&self) -> Result<(), ValidationError> {
        // Validate block type matches attributes
        match (&self.blocktype, &self.attributes) {
            (BlockType::Group, BlockAttributes::Group { .. }) => validate_group_block(self)?,
            (BlockType::Weight, BlockAttributes::Weight { .. }) => validate_weight_block(self)?,
            (BlockType::Condition, BlockAttributes::Condition { .. }) => {
                validate_condition_block(self)?
            }
            (BlockType::Filter, BlockAttributes::Filter { .. }) => validate_filter_block(self)?,
            (BlockType::Asset, BlockAttributes::Asset { .. }) => validate_asset_block(self)?,
            _ => {
                return Err(ValidationError::BlockTypeMismatch {
                    expected: self.blocktype.to_string(),
                    found: format!("{:?}", self.attributes),
                })
            }
        }

        Ok(())
    }
}

fn validate_group_block(block: &Block) -> Result<(), ValidationError> {
    if let BlockAttributes::Group { name } = &block.attributes {
        // Validate name is not empty
        if name.trim().is_empty() {
            return Err(ValidationError::GroupError(GroupError::MissingName));
        }

        // Validate children
        let children = block
            .children
            .as_ref()
            .ok_or_else(|| ValidationError::GroupError(GroupError::NoChildren))?;

        if children.is_empty() {
            return Err(ValidationError::GroupError(GroupError::NoChildren));
        }

        // Validate first child is Weight block
        if let Some(first_child) = children.first() {
            if first_child.blocktype != BlockType::Weight {
                return Err(ValidationError::GroupError(GroupError::FirstChildNotWeight));
            }
        }

        // Recursively validate all children
        for child in children {
            child.validate()?;
        }

        Ok(())
    } else {
        unreachable!("Block type mismatch should have been caught earlier")
    }
}

fn validate_weight_block(block: &Block) -> Result<(), ValidationError> {
    if let BlockAttributes::Weight {
        weight_type,
        allocation_type,
        values,
        window_of_trading_days,
    } = &block.attributes
    {
        match weight_type {
            WeightType::Specified => {
                // Validate allocation type is present
                if values.is_empty() {
                    return Err(ValidationError::WeightError(WeightError::MissingValues));
                }
                if allocation_type.is_none() {
                    return Err(ValidationError::WeightError(
                        WeightError::MissingAllocationType,
                    ));
                }

                if let Some(children) = &block.children {
                    if values.len() != children.len() {
                        return Err(ValidationError::WeightError(
                            WeightError::ValueChildrenMismatch {
                                expected: children.len(),
                                found: values.len(),
                            },
                        ));
                    }

                    // For percentage allocation, validate sum is 100
                    if let Some(AllocationType::Percentage) = allocation_type {
                        let sum: f64 = values.iter().sum();
                        if (sum - 100.0).abs() > 0.01 {
                            return Err(ValidationError::WeightError(
                                WeightError::InvalidPercentageSum { sum },
                            ));
                        }
                    }
                }
            }
            WeightType::Equal => {
                if !values.is_empty() {
                    return Err(ValidationError::WeightError(
                        WeightError::InvalidConfiguration(
                            "Equal weights should not have values specified".into(),
                        ),
                    ));
                }
            }
            WeightType::InverseVolatility => {
                if window_of_trading_days.is_none() {
                    return Err(ValidationError::WeightError(
                        WeightError::MissingVolatilityWindow,
                    ));
                }
            }
            _ => {}
        }

        // Recursively validate children
        if let Some(children) = &block.children {
            for child in children {
                child.validate()?;
            }
        }

        Ok(())
    } else {
        unreachable!("Block type mismatch should have been caught earlier")
    }
}

fn validate_condition_block(block: &Block) -> Result<(), ValidationError> {
    if let BlockAttributes::Condition {
        function,
        compare_to,
        ..
    } = &block.attributes
    {
        // Validate function configuration
        validate_function_definition(function).map_err(|e| ValidationError::ConditionError(e))?;

        // Validate compare_to function if present
        if let CompareToValue::Function { function } = compare_to {
            validate_function_definition(function)
                .map_err(|e| ValidationError::ConditionError(e))?;
        }

        // Validate child count
        let children = block
            .children
            .as_ref()
            .ok_or_else(|| ValidationError::ConditionError(ConditionError::InvalidChildCount(0)))?;

        if children.len() != 2 {
            return Err(ValidationError::ConditionError(
                ConditionError::InvalidChildCount(children.len()),
            ));
        }

        // Recursively validate children
        for child in children {
            child.validate()?;
        }

        Ok(())
    } else {
        unreachable!("Block type mismatch should have been caught earlier")
    }
}

fn validate_filter_block(block: &Block) -> Result<(), ValidationError> {
    if let BlockAttributes::Filter {
        sort_function,
        select,
    } = &block.attributes
    {
        // Validiere sort_function
        if !sort_function.function_name.requires_window_of_days() {
            return Err(ValidationError::FilterError(
                FilterError::InvalidSortFunction(
                    "Sort function must require window_of_days".to_string(),
                ),
            ));
        }
        // Validate sort_function configuration
        validate_function_definition(&FunctionDefinition {
            function_name: sort_function.function_name.clone(),
            window_of_days: Some(sort_function.window_of_days),
            asset: "".to_string(), // Sort function doesn't require asset
        })
        .map_err(|e| ValidationError::ConditionError(e))?;

        // Validate children are all Asset blocks
        if let Some(children) = &block.children {
            for (index, child) in children.iter().enumerate() {
                if child.blocktype != BlockType::Asset {
                    return Err(ValidationError::FilterError(FilterError::NonAssetChild(
                        index,
                    )));
                }
                child.validate()?;
            }
        }

        Ok(())
    } else {
        unreachable!("Block type mismatch should have been caught earlier")
    }
}

fn validate_asset_block(block: &Block) -> Result<(), ValidationError> {
    if let BlockAttributes::Asset {
        ticker,
        company_name,
        exchange,
    } = &block.attributes
    {
        // Validate no children
        if block.children.is_some() {
            return Err(ValidationError::AssetError(AssetError::HasChildren));
        }

        // Validate required fields
        if ticker.trim().is_empty() {
            return Err(ValidationError::AssetError(AssetError::MissingTicker));
        }
        if company_name.trim().is_empty() {
            return Err(ValidationError::AssetError(AssetError::MissingCompanyName));
        }
        if exchange.trim().is_empty() {
            return Err(ValidationError::AssetError(AssetError::MissingExchange));
        }

        Ok(())
    } else {
        unreachable!("Block type mismatch should have been caught earlier")
    }
}

fn validate_function_definition(function: &FunctionDefinition) -> Result<(), ConditionError> {
    // Validate asset is not empty
    if function.asset.trim().is_empty() {
        return Err(ConditionError::FunctionError(
            "Asset cannot be empty".to_string(),
        ));
    }

    // Validate window_of_days based on function type
    if function.function_name.requires_window_of_days() {
        match function.window_of_days {
            None => {
                return Err(ConditionError::MissingWindowDays(
                    function.function_name.to_string(),
                ))
            }
            Some(days) if days == 0 => {
                return Err(ConditionError::FunctionError(
                    "Window of days must be greater than 0".to_string(),
                ))
            }
            Some(days) => {
                // Different limits for different functions
                let max_days = match function.function_name {
                    FunctionName::ExponentialMovingAverage => 500, // Increased limit for EMA
                    _ => 252, // Default limit for other functions
                };

                if days > max_days {
                    return Err(ConditionError::FunctionError(format!(
                        "Window of days cannot exceed {} for {}",
                        max_days, function.function_name
                    )));
                }
            } //  _ => {}
        }
    } else if function.window_of_days.is_some() {
        return Err(ConditionError::InvalidWindowDays);
    }

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_validate_group_block() {
        // Valid group block
        let valid_group = json!({
            "blocktype": "Group",
            "name": "Test Group",
            "children": [{
                "blocktype": "Weight",
                "type": "equal",
                "children": []
            }]
        });

        let block: Block = serde_json::from_value(valid_group).unwrap();
        assert!(block.validate().is_ok());

        // Invalid group block (no children)
        let invalid_group = json!({
            "blocktype": "Group",
            "name": "Test Group",
            "children": []
        });

        let block: Block = serde_json::from_value(invalid_group).unwrap();
        assert!(matches!(
            block.validate(),
            Err(ValidationError::GroupError(GroupError::NoChildren))
        ));
    }

    #[test]
    fn test_validate_weight_block_percentage() {
        // Valid weight block with percentages
        let valid_weight = json!({
            "blocktype": "Weight",
            "type": "specified",
            "allocation_type": "percentage",
            "values": [60.0, 40.0],
            "children": [
                {
                    "blocktype": "Asset",
                    "ticker": "AAPL",
                    "company_name": "Apple Inc.",
                    "exchange": "NASDAQ"
                },
                {
                    "blocktype": "Asset",
                    "ticker": "MSFT",
                    "company_name": "Microsoft Corporation",
                    "exchange": "NASDAQ"
                }
            ]
        });

        let block: Block = serde_json::from_value(valid_weight).unwrap();
        assert!(block.validate().is_ok());

        // Invalid percentages (don't sum to 100)
        let invalid_weight = json!({
            "blocktype": "Weight",
            "type": "specified",
            "allocation_type": "percentage",
            "values": [60.0, 20.0],
            "children": [
                {
                    "blocktype": "Asset",
                    "ticker": "AAPL",
                    "company_name": "Apple Inc.",
                    "exchange": "NASDAQ"
                },
                {
                    "blocktype": "Asset",
                    "ticker": "MSFT",
                    "company_name": "Microsoft Corporation",
                    "exchange": "NASDAQ"
                }
            ]
        });

        let block: Block = serde_json::from_value(invalid_weight).unwrap();
        assert!(matches!(
            block.validate(),
            Err(ValidationError::WeightError(
                WeightError::InvalidPercentageSum { .. }
            ))
        ));
    }

    #[test]
    fn test_validate_condition_block() {
        // Valid condition block
        let valid_condition = json!({
            "blocktype": "Condition",
            "function": {
                "function_name": "current_price",
                "asset": "AAPL"
            },
            "operator": ">",
            "compare_to": {
                "type": "fixed",
                "value": 150.0
            },
            "children": [
                {
                    "blocktype": "Asset",
                    "ticker": "AAPL",
                    "company_name": "Apple Inc.",
                    "exchange": "NASDAQ"
                },
                {
                    "blocktype": "Asset",
                    "ticker": "MSFT",
                    "company_name": "Microsoft Corporation",
                    "exchange": "NASDAQ"
                }
            ]
        });

        let block: Block = serde_json::from_value(valid_condition).unwrap();
        assert!(block.validate().is_ok());

        // Invalid condition (wrong number of children)
        let invalid_condition = json!({
            "blocktype": "Condition",
            "function": {
                "function_name": "current_price",
                "asset": "AAPL"
            },
            "operator": ">",
            "compare_to": {
                "type": "fixed",
                "value": 150.0
            },
            "children": [
                {
                    "blocktype": "Asset",
                    "ticker": "AAPL",
                    "company_name": "Apple Inc.",
                    "exchange": "NASDAQ"
                }
            ]
        });

        let block: Block = serde_json::from_value(invalid_condition).unwrap();
        assert!(matches!(
            block.validate(),
            Err(ValidationError::ConditionError(
                ConditionError::InvalidChildCount(1)
            ))
        ));
    }

    #[test]
    fn test_validate_filter_block() {
        // Valid filter block
        let valid_filter = json!({
            "blocktype": "Filter",
            "sort_function": {
                "function_name": "cumulative_return",
                "window_of_days": 10
            },
            "select": {
                "option": "Top",
                "amount": 3
            },
            "children": [
                {
                    "blocktype": "Asset",
                    "ticker": "AAPL",
                    "company_name": "Apple Inc.",
                    "exchange": "NASDAQ"
                },
                {
                    "blocktype": "Asset",
                    "ticker": "MSFT",
                    "company_name": "Microsoft Corporation",
                    "exchange": "NASDAQ"
                }
            ]
        });

        let block: Block = serde_json::from_value(valid_filter).unwrap();
        assert!(block.validate().is_ok());

        // Invalid filter (non-asset child)
        let invalid_filter = json!({
            "blocktype": "Filter",
            "sort_function": {
                "function_name": "cumulative_return",
                "window_of_days": 10
            },
            "select": {
                "option": "Top",
                "amount": 3
            },
            "children": [
                {
                    "blocktype": "Group",
                    "name": "Invalid Child",
                    "children": []
                }
            ]
        });

        let block: Block = serde_json::from_value(invalid_filter).unwrap();
        assert!(matches!(
            block.validate(),
            Err(ValidationError::FilterError(FilterError::NonAssetChild(0)))
        ));
    }

    #[test]
    fn test_validate_asset_block() {
        // Valid asset block
        let valid_asset = json!({
            "blocktype": "Asset",
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "exchange": "NASDAQ"
        });

        let block: Block = serde_json::from_value(valid_asset).unwrap();
        assert!(block.validate().is_ok());

        // Invalid asset (with children)
        let invalid_asset = json!({
            "blocktype": "Asset",
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "exchange": "NASDAQ",
            "children": []
        });

        let block: Block = serde_json::from_value(invalid_asset).unwrap();
        assert!(matches!(
            block.validate(),
            Err(ValidationError::AssetError(AssetError::HasChildren))
        ));

        // Invalid asset (missing fields)
        let invalid_asset = json!({
            "blocktype": "Asset",
            "ticker": "",
            "company_name": "Apple Inc.",
            "exchange": "NASDAQ"
        });

        let block: Block = serde_json::from_value(invalid_asset).unwrap();
        assert!(matches!(
            block.validate(),
            Err(ValidationError::AssetError(AssetError::MissingTicker))
        ));
    }

    #[test]
    fn test_validate_function_definition() {
        // Test current_price with window_of_days (should fail)
        let invalid_current_price = FunctionDefinition {
            function_name: FunctionName::CurrentPrice,
            window_of_days: Some(10),
            asset: "AAPL".to_string(),
        };
        assert!(matches!(
            validate_function_definition(&invalid_current_price),
            Err(ConditionError::InvalidWindowDays)
        ));

        // Test cumulative_return without window_of_days (should fail)
        let invalid_cumulative_return = FunctionDefinition {
            function_name: FunctionName::CumulativeReturn,
            window_of_days: None,
            asset: "AAPL".to_string(),
        };
        assert!(matches!(
            validate_function_definition(&invalid_cumulative_return),
            Err(ConditionError::MissingWindowDays(_))
        ));

        // Test valid current_price
        let valid_current_price = FunctionDefinition {
            function_name: FunctionName::CurrentPrice,
            window_of_days: None,
            asset: "AAPL".to_string(),
        };
        assert!(validate_function_definition(&valid_current_price).is_ok());

        // Test valid cumulative_return
        let valid_cumulative_return = FunctionDefinition {
            function_name: FunctionName::CumulativeReturn,
            window_of_days: Some(10),
            asset: "AAPL".to_string(),
        };
        assert!(validate_function_definition(&valid_cumulative_return).is_ok());
    }
}
