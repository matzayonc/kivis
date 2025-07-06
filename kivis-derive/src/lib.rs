use proc_macro::TokenStream;
use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};
use syn::{DeriveInput, parse_macro_input};

mod generator;
mod schema;

use crate::generator::generate_record_impl;
use crate::schema::Schema;

// Global registry to track table IDs across compilation
static TABLE_REGISTRY: LazyLock<Mutex<HashSet<u8>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

#[proc_macro_derive(Record, attributes(table, key, index))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    let visibility = input.vis.clone();

    // Create schema from the parsed input
    let schema = match Schema::from_derive_input(input) {
        Ok(schema) => schema,
        Err(error_tokens) => return error_tokens,
    };

    // Check for duplicate table IDs
    {
        let mut registry = TABLE_REGISTRY.lock().unwrap();
        if registry.contains(&schema.table_value) {
            return syn::Error::new_spanned(
                &schema.name,
                format!(
                    "Duplicate table ID {}. Each table must have a unique integer identifier.",
                    schema.table_value
                ),
            )
            .to_compile_error()
            .into();
        }
        registry.insert(schema.table_value);
    }

    // Generate the implementation
    generate_record_impl(&schema, visibility)
}
