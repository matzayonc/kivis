//! # Kivis Derive Macros
//!
//! Procedural macros for the Kivis database schema library.
//! This crate provides the `Record` derive macro for automatically generating database schema types.

use proc_macro::TokenStream;
use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};
use syn::{parse_macro_input, DeriveInput};

mod generator;
mod schema;

use crate::generator::generate_record_impl;
use crate::schema::Schema;

// Global registry to track table IDs across compilation
static TABLE_REGISTRY: LazyLock<Mutex<HashSet<u8>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Derive macro for generating database record implementations.
///
/// This macro generates the necessary traits and types for a struct to be used as a database record in Kivis.
/// It creates key types, index types, and implements the required traits for database operations.
///
/// # Attributes
///
/// - `#[table(N)]`: Required. Specifies the table ID (must be unique across all records)
/// - `#[key]`: Marks fields as part of the primary key
/// - `#[index]`: Marks fields for secondary indexing
///
/// # Examples
///
/// ```ignore
/// use kivis::Record;
///
/// #[derive(Record, serde::Serialize, serde::Deserialize)]
/// #[table(1)]
/// struct User {
///     #[index]
///     name: String,
///     email: String,
/// }
/// ```
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
