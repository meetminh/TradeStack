//! Models for the investment portfolio block system.
//! This module contains all data structures and their serialization logic.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Main block structure representing any type of investment block
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Block {
    pub blocktype: BlockType,
    #[serde(flatten)]
    pub attributes: BlockAttributes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<Block>>,
}
/// Available block types in the system
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum BlockType {
    Group,
    Weight,
    Condition,
    Filter,
    Asset,
}

impl fmt::Display for BlockType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockType::Group => write!(f, "Group"),
            BlockType::Weight => write!(f, "Weight"),
            BlockType::Condition => write!(f, "Condition"),
            BlockType::Filter => write!(f, "Filter"),
            BlockType::Asset => write!(f, "Asset"),
        }
    }
}

/// Attributes specific to each block type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BlockAttributes {
    Group {
        name: String,
    },
    Weight {
        #[serde(rename = "type")]
        weight_type: WeightType,
        #[serde(skip_serializing_if = "Option::is_none")]
        allocation_type: Option<AllocationType>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        values: Vec<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        window_of_trading_days: Option<u32>,
    },
    Condition {
        function: FunctionDefinition,
        operator: ComparisonOperator,
        compare_to: CompareToValue,
    },
    Filter {
        sort_function: SortFunction,
        select: SelectConfig,
    },
    Asset {
        ticker: String,
        company_name: String,
        exchange: String,
    },
}

/// Types of weight calculations available
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WeightType {
    Equal,
    Specified,
    InverseVolatility,
    MarketCap,
}

/// Types of allocation methods
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AllocationType {
    Percentage,
    Fraction,
}

/// Available comparison operators for conditions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ComparisonOperator {
    #[serde(rename = ">")]
    GreaterThan,
    #[serde(rename = "<")]
    LessThan,
    #[serde(rename = "=")]
    Equal,
    #[serde(rename = ">=")]
    GreaterThanOrEqual,
    #[serde(rename = "<=")]
    LessThanOrEqual,
}

/// Function definition for conditions and filters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDefinition {
    pub function_name: FunctionName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_of_days: Option<u32>,
    pub asset: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FunctionName {
    CurrentPrice,
    CumulativeReturn,
    SimpleMovingAverage,
    ExponentialMovingAverage,
    MovingAverageOfPrice,
    MovingAverageOfReturns,
    RelativeStrengthIndex,
    PriceStandardDeviation,
    ReturnsStandardDeviation,
    MarketCap,
}

// Hilfsmethode hinzufügen
impl FunctionName {
    pub fn requires_window_of_days(&self) -> bool {
        match self {
            FunctionName::CurrentPrice | FunctionName::MarketCap => false,
            _ => true,
        }
    }
}

impl fmt::Display for FunctionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self) // Uses debug formatting as the base representation
    }
}

/// Comparison value types for conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CompareToValue {
    Function {
        function: FunctionDefinition,
    },
    #[serde(rename = "fixed_value")]
    Fixed {
        value: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        unit: Option<String>,
    },
}

/// Sort function configuration for filters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortFunction {
    pub function_name: FunctionName,
    pub window_of_days: u32,
}

/// Selection configuration for filters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectConfig {
    pub option: SelectOption,
    pub amount: u32,
}

/// Available selection options
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum SelectOption {
    Top,
    Bottom,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_serialize_deserialize_group() {
        let json = json!({
            "blocktype": "Group",
            "name": "Test Group",
            "children": []
        });

        let block: Block = serde_json::from_value(json).unwrap();
        assert!(matches!(block.blocktype, BlockType::Group));

        if let BlockAttributes::Group { name } = block.attributes {
            assert_eq!(name, "Test Group");
        } else {
            panic!("Expected Group attributes");
        }
    }

    #[test]
    fn test_serialize_deserialize_weight() {
        let json = json!({
            "blocktype": "Weight",
            "type": "specified",
            "allocation_type": "percentage",
            "values": [50.0, 50.0],
            "children": []
        });

        let block: Block = serde_json::from_value(json).unwrap();
        assert!(matches!(block.blocktype, BlockType::Weight));

        if let BlockAttributes::Weight {
            weight_type,
            values,
            ..
        } = &block.attributes
        {
            assert_eq!(*weight_type, WeightType::Specified);
            assert_eq!(values, &vec![50.0, 50.0]);
        } else {
            panic!("Expected Weight attributes");
        }
    }

    #[test]
    fn test_serialize_deserialize_condition() {
        let json = json!({
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
            "children": []
        });

        let block: Block = serde_json::from_value(json).unwrap();
        assert!(matches!(block.blocktype, BlockType::Condition));
    }

    #[test]
    fn test_serialize_deserialize_asset() {
        let json = json!({
            "blocktype": "Asset",
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "exchange": "NASDAQ"
        });

        let block: Block = serde_json::from_value(json).unwrap();
        assert!(matches!(block.blocktype, BlockType::Asset));

        if let BlockAttributes::Asset {
            ticker,
            company_name,
            exchange,
        } = &block.attributes
        {
            assert_eq!(ticker, "AAPL");
            assert_eq!(company_name, "Apple Inc.");
            assert_eq!(exchange, "NASDAQ");
        } else {
            panic!("Expected Asset attributes");
        }
    }
}
