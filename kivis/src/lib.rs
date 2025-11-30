//! # Kivis Database
//!
//! A lightweight, type-safe database schema library for Rust with support for custom storage backends,
//! automatic indexing, and foreign key relationships.
//!
//! ## Features
//!
//! - **Type-safe schema generation**: Automatically derive database schemas from Rust structs
//! - **Generic storage backend support**: Work with any ordered key-value store
//! - **Automatic key generation and indexing**: Support for auto-increment keys and secondary indexes
//! - **Foreign key relationships**: Type-safe references between records
//! - **Layered cache architectures**: Compose multiple storage implementations
//!
//! ## Quick Start
//!
//! ```rust
//! use kivis::{Database, MemoryStorage, Record, manifest};
//!
//! #[derive(Record, serde::Serialize, serde::Deserialize, Debug)]
//! struct User {
//!     name: String,
//!     email: String,
//! }
//!
//! // Define the manifest for the database
//! manifest![MyDatabase: User];
//!
//! # fn main() -> Result<(), kivis::DatabaseError<kivis::MemoryStorage>> {
//! let mut db = Database::<MemoryStorage, MyDatabase>::new(MemoryStorage::new())?;
//! let user = User {
//!     name: "Alice".to_string(),
//!     email: "alice@example.com".to_string(),
//! };
//! let user_key = db.put(&user)?;
//! # Ok(())
//! # }
//! ```

#![cfg_attr(not(feature = "std"), no_std)]
#![warn(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

// Always link to the `alloc` crate and re-export it so generated code inside macros
// can reference `::kivis::alloc::vec::Vec` and not depend on the consumer to import alloc.
pub extern crate alloc;

#[cfg(feature = "memory-storage")]
mod btreemap;
mod database;
mod errors;
mod lexicographic;
mod traits;
mod transaction;
mod wrap;

#[cfg(feature = "memory-storage")]
pub use btreemap::{MemoryStorage, MemoryStorageError};
pub use database::Database;
pub use kivis_derive::Record;
pub use lexicographic::*;
pub use paste::paste;
pub use traits::*;

pub use crate::errors::{DatabaseError, InternalDatabaseError};

#[cfg(feature = "atomic")]
// Database transaction is only usefull if atomic storage is enabled.
pub use transaction::DatabaseTransaction;
