use crate::block::filter::{FilterConfig, SelectConfig, SortConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct Condition {
    pub function: String,
    pub params: Vec<String>,
    pub operator: String,
    pub value: ConditionValue,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ConditionValue {
    Static(f64),
    Dynamic {
        function: String,
        params: Vec<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Node {
    #[serde(rename = "root")]
    Root { weight: f64, children: Vec<Node> },
    #[serde(rename = "condition")]
    Condition {
        weight: f64,
        condition: Condition,
        if_true: Box<Node>,
        if_false: Box<Node>,
    },
    #[serde(rename = "group")]
    Group { weight: f64, children: Vec<Node> },
    #[serde(rename = "weighting")]
    Weighting { weight: f64, children: Vec<Node> },
    #[serde(rename = "asset")]
    Asset { ticker: String, weight: f64 },
    #[serde(rename = "filter")]
    Filter {
        weight: f64,
        children: Vec<Node>,
        universe: Vec<String>,
        sort: SortConfig,
        select: SelectConfig,
    },
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
    fn test_condition_value_deserialization() {
        // Test static value
        let static_json =
            r#"{"function": "rsi", "params": ["SPY", "14"], "operator": "<", "value": 30}"#;
        let _: Condition = serde_json::from_str(static_json).unwrap();

        // Test dynamic value
        let dynamic_json = r#"{"function": "rsi", "params": ["SPY", "14"], "operator": "<", "value": {"function": "current_price", "params": ["QQQ"]}}"#;
        let _: Condition = serde_json::from_str(dynamic_json).unwrap();
    }
}
