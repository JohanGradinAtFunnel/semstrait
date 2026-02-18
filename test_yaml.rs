use semstrait::parser;

fn main() {
    match parser::parse_file("test_data/steelwheels.yaml") {
        Ok(schema) => println!("Parsed successfully"),
        Err(e) => println!("Parse error: {:?}", e),
    }
}
