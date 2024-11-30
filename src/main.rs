mod json_operations;
mod models;

use std::error::Error;
use std::fs;

fn main() -> Result<(), Box<dyn Error>> {
    // Read JSON from file
    let json_str = fs::read_to_string("input.json")?;

    println!("Original JSON:");
    println!("{}", json_str);

    // Deserialize JSON to Node
    match json_operations::deserialize_json(&json_str) {
        Ok(deserialized_tree) => {
            println!("\nDeserialized structure:");
            println!("{:#?}", deserialized_tree);

            // Validate the deserialized tree
            match json_operations::validate_node(&deserialized_tree) {
                Ok(()) => {
                    println!("Validation successful!");
                }
                Err(e) => {
                    println!("Validation Error:");
                    println!("{}", e);
                }
            }
        }
        Err(e) => {
            println!("\nDeserialization Error:");
            println!("{}", e);
        }
    }

    Ok(())
}
