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
//! ## Feature Flags
//!
//! - `std` (default): Enable standard library support (implies `alloc`)
//! - `alloc`: Enable `Vec` and `String` support without requiring the full standard library
//! - `atomic` (default): Enable atomic transaction support (requires `alloc`)
//! - `memory-storage` (default): Include in-memory storage implementation
//! - `heapless`: Enable `UnifierData` implementation for `heapless::Vec<u8, N>`, allowing fixed-capacity
//!   stack-allocated vectors for embedded environments
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
//! let user_key = db.put(user)?;
//! # Ok(())
//! # }
//! ```

#![cfg_attr(not(feature = "std"), no_std)]
#![warn(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod database;
mod errors;
mod impls;
mod integrations;
mod traits;
mod transaction;
mod utils;
mod wrap;

pub use database::Database;
pub use kivis_derive::Record;
pub use paste::paste;
pub use traits::*;
pub use utils::*;

pub use crate::errors::{
    BufferOverflowError, BufferOverflowOr, DatabaseError, InternalDatabaseError,
};

#[cfg(feature = "atomic")]
// Database transaction is only useful if atomic storage is enabled.
pub use transaction::{
    BufferOp, BufferOpsContainer, DatabaseTransaction, OpsIter, TransactionError,
};

#[cfg(feature = "sled")]
pub use integrations::{PostcardUnifier, SledStorageError};
