use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum Node {
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
pub struct Condition {
    pub function: String,
    pub params: Vec<String>,
    pub operator: String,
    pub value: f32,
}
