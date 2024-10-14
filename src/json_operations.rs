use crate::models::Node;
use std::error::Error;

// Senior perspective: This function is the entry point for JSON deserialization and validation.
// It combines parsing and custom validation in a single, public-facing function.
//
// High school perspective: This function takes a JSON string, turns it into a tree-like structure,
// and checks if everything in that structure is correct according to our rules.
pub fn deserialize_json(json_str: &str) -> Result<Node, Box<dyn Error>> {
    // Parse the JSON string into our Node structure
    let deserialized_tree: Node = serde_json::from_str(json_str)?;
    // Check if the tree structure is valid according to our custom rules
    validate_node(&deserialized_tree)?;
    // If everything is okay, return the tree structure
    Ok(deserialized_tree)
}

// Senior perspective: This recursive function traverses the Node tree, applying specific
// validations based on the node type. It's a depth-first traversal ensuring all nodes are valid.
//
// High school perspective: This function checks each part of our tree structure. It looks at
// each "node" (a part of the tree) and makes sure it follows our rules. It does this for every
// single part, no matter how deep in the tree it is.
fn validate_node(node: &Node) -> Result<(), Box<dyn Error>> {
    match node {
        // For nodes with children, we validate the weights and then recursively validate each child
        Node::Root { children, .. }
        | Node::Group { children, .. }
        | Node::Weighting { children, .. } => {
            validate_children_weights(children)?;
            for child in children {
                validate_node(child)?;
            }
        }
        // For condition nodes, we validate both the true and false branches
        Node::Condition {
            if_true, if_false, ..
        } => {
            validate_node(if_true)?;
            validate_node(if_false)?;
        }
        // For asset nodes, we validate the ticker symbol
        Node::Asset { ticker, .. } => {
            validate_ticker(ticker)?;
        }
    }
    Ok(())
}

// Senior perspective: This function ensures that the weights of child nodes sum to 1,
// which is crucial for maintaining the integrity of the weighting system in the tree.
//
// High school perspective: This function checks if the "weights" of all the parts add up to 1.
// It's like making sure all the slices of a pie add up to a whole pie.
fn validate_children_weights(children: &[Node]) -> Result<(), Box<dyn Error>> {
    // Calculate the sum of all child weights
    let total_weight: f32 = children
        .iter()
        .map(|child| match child {
            Node::Root { weight, .. }
            | Node::Group { weight, .. }
            | Node::Weighting { weight, .. }
            | Node::Condition { weight, .. }
            | Node::Asset { weight, .. } => *weight,
        })
        .sum();

    // Check if the total weight is close enough to 1 (allowing for small floating-point errors)
    if (total_weight - 1.0).abs() > 1e-6 {
        return Err(format!("Sum of child weights ({}) is not equal to 1", total_weight).into());
    }
    Ok(())
}

// Senior perspective: This is a placeholder for more complex ticker validation.
// In a production environment, this would likely involve API calls to financial data providers.
//
// High school perspective: This function checks if a stock symbol (ticker) is valid.
// Right now, it just checks if the ticker isn't empty and isn't too long, but in a real system,
// it would check against a list of real stock symbols.
fn validate_ticker(ticker: &str) -> Result<(), Box<dyn Error>> {
    if ticker.is_empty() || ticker.len() > 5 {
        return Err(format!("Invalid ticker: {}", ticker).into());
    }
    Ok(())
}

// Senior perspective: This function handles the serialization of our Node structure back into JSON.
// It's the counterpart to deserialize_json, completing the round-trip of data transformation.
//
// High school perspective: This function takes our tree structure and turns it back into a JSON string.
// It's like translating our special tree language back into something that can be easily shared or stored.
pub fn serialize_to_json(node: &Node) -> Result<String, Box<dyn Error>> {
    let serialized_json = serde_json::to_string_pretty(node)?;
    Ok(serialized_json)
}
