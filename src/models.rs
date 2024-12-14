use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Block {
    pub blocktype: String,
    pub attributes: Attributes,
    pub children: Option<Vec<Block>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Attributes {
    Group {
        name: String,
    },
    Weight {
        #[serde(rename = "type")]
        type_: String,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        allocation_type: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        values: Vec<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        window_of_trading_days: Option<u32>,
    },
    Condition {
        condition: ConditionConfig,
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

// Rest of the structs remain the same
#[derive(Debug, Serialize, Deserialize)]
pub struct ConditionConfig {
    pub function: FunctionDefinition,
    pub operator: String,
    pub compare_to: CompareToValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FunctionDefinition {
    pub function_name: String,
    pub window_of_days: Option<u32>,
    pub asset: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CompareToValue {
    #[serde(rename = "function")]
    Function { function: FunctionDefinition },
    #[serde(rename = "fixed")]
    Fixed { value: f64, unit: Option<String> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SortFunction {
    pub function_name: String,
    pub window_of_days: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectConfig {
    pub option: String,
    pub amount: u32,
}

#[derive(Debug, Serialize)]
pub struct Allocation {
    pub ticker: String,
    pub weight: f64,
    pub date: String,
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_block() {
        let json = r#"{
            "blocktype": "Group",
            "attributes": {
                "name": "Test Group"
            },
            "children": []
        }"#;

        let block: Block = serde_json::from_str(json).unwrap();
        match block.attributes {
            Attributes::Group { name } => assert_eq!(name, "Test Group"),
            _ => panic!("Expected Group attributes"),
        }
    }

    #[test]
    fn test_weight_block() {
        let json = r#"{
            "blocktype": "Weight",
            "attributes": {
                "type": "specified",
                "allocation_type": "percentage",
                "values": [50, 50]
            },
            "children": []
        }"#;

        let block: Block = serde_json::from_str(json).unwrap();
        match block.attributes {
            Attributes::Weight { type_, values, .. } => {
                assert_eq!(type_, "specified");
                assert_eq!(values, vec![50.0, 50.0]);
            }
            _ => panic!("Expected Weight attributes"),
        }
    }

    #[test]
    fn test_condition_block() {
        let json = r#"{
            "blocktype": "Condition",
            "attributes": {
                "condition": {
                    "function": {
                        "function_name": "cumulative_return",
                        "window_of_days": 30,
                        "asset": "VIX"
                    },
                    "operator": ">",
                    "compare_to": {
                        "type": "fixed_value",
                        "value": 20,
                        "unit": "%"
                    }
                }
            },
            "children": []
        }"#;

        let block: Block = serde_json::from_str(json).unwrap();
        assert!(matches!(block.attributes, Attributes::Condition { .. }));
    }

    #[test]
    fn test_filter_block() {
        let json = r#"{
            "blocktype": "Filter",
            "attributes": {
                "sort_function": {
                    "function_name": "cumulative_return",
                    "window_of_days": 10
                },
                "select": {
                    "option": "Top",
                    "amount": 3
                }
            },
            "children": []
        }"#;

        let block: Block = serde_json::from_str(json).unwrap();
        assert!(matches!(block.attributes, Attributes::Filter { .. }));
    }

    #[test]
    fn test_asset_block() {
        let json = r#"{
            "blocktype": "Asset",
            "attributes": {
                "ticker": "AAPL",
                "company_name": "Apple Inc.",
                "exchange": "NASDAQ"
            }
        }"#;

        let block: Block = serde_json::from_str(json).unwrap();
        match block.attributes {
            Attributes::Asset { ticker, .. } => assert_eq!(ticker, "AAPL"),
            _ => panic!("Expected Asset attributes"),
        }
    }
}
