//! # Kivis Derive Macros
//!
//! Procedural macros for the Kivis database schema library.
//! This crate provides the `Record` derive macro for automatically generating database schema types.
#![warn(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod generator;
mod schema;

use crate::generator::Generator;
use crate::schema::Schema;

/// Derive macro for generating database record implementations.
///
/// This macro generates the necessary traits and types for a struct to be used as a database record in Kivis.
/// It creates key types, index types, and implements the required traits for database operations.
///
/// # Attributes
///
/// - `#[key]`: Marks fields as part of the primary key
/// - `#[index]`: Marks fields for secondary indexing  
/// - `#[derived_key(Type1, Type2, ...)]`: Specifies types for a derived key (mutually exclusive with `#[key]`)
///
/// # Key Strategies
///
/// The Record derive macro supports three key strategies:
///
/// 1. **Autoincrement keys**: No `#[key]` fields or `#[derived_key]` attribute (default u64 autoincrement)
/// 2. **Field keys**: Fields marked with `#[key]` attribute (derived from struct fields)
/// 3. **Derived keys**: Struct with `#[derived_key(...)]` attribute (requires manual `DeriveKey` implementation)
///
/// # Examples
///
/// ## Autoincrement Key
/// ```
/// use serde::{Serialize, Deserialize};
///
/// // This shows the basic structure - the actual Record derive would be used in practice
/// #[derive(Serialize, Deserialize, Debug, Clone)]
/// struct AutoIncrement {
///     name: String,
///     email: String,
/// }
/// ```
///
/// ## Field Key  
/// ```
/// use serde::{Serialize, Deserialize};
///
/// // This shows the basic structure with key field - the actual Record derive would be used in practice
/// #[derive(Serialize, Deserialize, Debug)]
/// struct User {
///     id: u64,        // Would be marked with #[key] in actual usage
///     name: String,   // Would be marked with #[index] in actual usage  
///     email: String,
/// }
/// ```
///
/// For complete working examples, see the tests in the `tests/` directory.
#[proc_macro_derive(Record, attributes(key, index, derived_key))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    let visibility = input.vis.clone();

    // Create schema from the parsed input
    let schema = match Schema::from_derive_input(input) {
        Ok(schema) => schema,
        Err(error_tokens) => return error_tokens,
    };

    // Generate the implementation
    Generator::new(schema).generate_record_impl(&visibility)
}
