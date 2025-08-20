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
//! use kivis::{Database, MemoryStorage, Record};
//!
//! #[derive(Record, serde::Serialize, serde::Deserialize, Debug)]
//! #[table(1)]
//! struct User {
//!     name: String,
//!     email: String,
//! }
//!
//! # fn main() -> Result<(), kivis::DatabaseError<kivis::MemoryStorageError>> {
//! let mut db = Database::new(MemoryStorage::new());
//! let user = User {
//!     name: "Alice".to_string(),
//!     email: "alice@example.com".to_string(),
//! };
//! let user_key = db.put(user)?;
//! # Ok(())
//! # }
//! ```

mod btreemap;
mod database;
mod errors;
mod manifest;
mod traits;
mod wrap;

pub use btreemap::{MemoryStorage, MemoryStorageError};
pub use database::Database;
pub use kivis_derive::Record;
pub use traits::*;

pub use crate::errors::{DatabaseError, InternalDatabaseError};
