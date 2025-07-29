//! # Kivis Database
//!
//! A lightweight, database schema library for Rust with support for custom storage backends,
//! automatic indexing, and type-safe record operations.
//!
//! ## Features
//!
//! - Generic storage backend support
//! - Automatic key generation and indexing
//! - Type-safe record operations
//! - Layered cache support

mod btreemap;
mod database;
mod errors;
mod traits;
mod wrap;

pub use btreemap::{MemoryStorage, MemoryStorageError};
pub use database::Database;
pub use kivis_derive::Record;
pub use traits::*;

pub use crate::errors::{DatabaseError, InternalDatabaseError};
