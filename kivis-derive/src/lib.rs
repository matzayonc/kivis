use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod generator;
mod schema;

use crate::generator::generate_record_impl;
use crate::schema::Schema;

#[proc_macro_derive(Record, attributes(table, key))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Create schema from the parsed input
    let schema = match Schema::from_derive_input(input) {
        Ok(schema) => schema,
        Err(error_tokens) => return error_tokens,
    };

    // Generate the implementation
    generate_record_impl(&schema)
}
