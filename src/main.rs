use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
enum Node {
    Root {
        weight: f32,
        children: Vec<Node>,
    },
    Condition {
        weight: f32,
        condition: Condition,
        if_true: Box<Node>,
        if_false: Box<Node>,
    },
    Group {
        weight: f32,
        children: Vec<Node>,
    },
    Weighting {
        weight: f32,
        children: Vec<Node>,
    },
    Asset {
        ticker: String,
        weight: f32,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct Condition {
    function: String,
    params: Vec<String>,
    operator: String,
    value: f32,
}

// Example usage
fn main() -> Result<(), Box<dyn Error>> {
    let json_str = r#"{
        "type": "root",
        "weight": 1.0,
        "children": [
            {
                "type": "condition",
                "weight": 0.6,
                "condition": {
                    "function": "volatility",
                    "params": ["VIX", "30"],
                    "operator": ">",
                    "value": 20
                },
                "if_true": {
                    "type": "group",
                    "weight": 1.0,
                    "children": [
                        {"type": "asset", "ticker": "TLT", "weight": 0.7},
                        {"type": "asset", "ticker": "GLD", "weight": 0.3}
                    ]
                },
                "if_false": {
                    "type": "condition",
                    "weight": 1.0,
                    "condition": {
                        "function": "rsi",
                        "params": ["SPY", "14"],
                        "operator": "<",
                        "value": 30
                    },
                    "if_true": {
                        "type": "weighting",
                        "weight": 1.0,
                        "children": [
                            {"type": "asset", "ticker": "SPY", "weight": 0.8},
                            {"type": "asset", "ticker": "QQQ", "weight": 0.2}
                        ]
                    },
                    "if_false": {"type": "asset", "ticker": "VOOG", "weight": 1.0}
                }
            },
            {"type": "asset", "ticker": "SHY", "weight": 0.1}
        ]
    }"#;

    println!("Original JSON:");
    println!("{}", json_str);

    // Deserialize JSON to Node
    let deserialized_tree: Node = serde_json::from_str(json_str)?;
    println!("\nDeserialized structure:");
    println!("{:#?}", deserialized_tree);

    // Serialize Node back to JSON
    let serialized_json = serde_json::to_string_pretty(&deserialized_tree)?;
    println!("\nRe-serialized JSON:");
    println!("{}", serialized_json);

    Ok(())
}
