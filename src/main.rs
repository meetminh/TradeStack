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

            // Serialize Node back to JSON
            let serialized_json = json_operations::serialize_to_json(&deserialized_tree)?;
            println!("\nRe-serialized JSON:");
            println!("{}", serialized_json);
        }
        Err(e) => {
            println!("\nValidation Error:");
            println!("{}", e);
        }
    }

    Ok(())
}
