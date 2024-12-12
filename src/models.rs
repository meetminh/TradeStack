use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct Condition {
    pub function: String,
    pub params: Vec<String>,
    pub operator: String,
    pub value: f64,
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
        tickers: Vec<String>,
        sort: FilterSort,
        select: FilterSelect,
        children: Vec<Node>,
    },
}

#[derive(Debug, Deserialize)]
pub struct FilterSort {
    pub function: String,
    pub params: Vec<String>,
    pub order: String,
}

#[derive(Debug, Deserialize)]
pub struct FilterSelect {
    pub direction: String, // "TOP" or "BOTTOM"
    pub count: usize,      // Number of tickers to select
}

#[derive(Debug, Serialize)]
pub struct Allocation {
    pub ticker: String,
    pub weight: f64,
    pub date: String,
}
