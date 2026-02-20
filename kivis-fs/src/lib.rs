//! File system storage backend for [kivis](https://crates.io/crates/kivis) databases.
//!
//! This crate provides a simple, human-readable file-based storage implementation for kivis,
//! where each record is stored as an individual file using CSV serialization. It's designed
//! for use cases where data inspection, manual editing, or simple persistence is more
//! important than raw performance.
//!
//! # Features
//!
//! - **Human-Readable Storage**: Records are stored as CSV-formatted files that can be
//!   easily inspected and modified with standard text editors
//! - **One Record Per File**: Each key-value pair becomes a separate `.dat` file, making
//!   it trivial to understand the storage structure
//! - **URL-Encoded Filenames**: Keys are safely encoded as filenames while remaining
//!   largely human-readable
//! - **Full kivis Integration**: Implements the `Storage` trait for seamless integration
//!   with kivis databases, supporting all features including indexes and foreign keys
//!
//! # When to Use
//!
//! `kivis-fs` is ideal for:
//!
//! - **Development and Testing**: Quick prototyping where you want to inspect data easily
//! - **Configuration Storage**: Persisting application settings or small datasets
//! - **Data Interchange**: Scenarios requiring manual inspection or editing of stored records
//! - **Audit Trails**: When having individual files per record aids in version control
//! - **Educational Purposes**: Learning kivis concepts with transparent storage
//!
//! For production systems requiring high performance, consider using more optimized
//! backends like Sled or RocksDB.
//!
//! # Example
//!
//! ```rust
//! use kivis::{Database, Record, manifest};
//! use kivis_fs::FileStore;
//!
//! #[derive(Record, Debug, Clone, serde::Serialize, serde::Deserialize)]
//! struct User {
//!     name: String,
//!     email: String,
//! }
//!
//! manifest![MyApp: User];
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let temp_dir = tempfile::tempdir()?;
//! #
//! // Create a file-based database in the "./data" directory
//! let storage = FileStore::new(temp_dir.path())?;
//! let mut db: Database<_, MyApp> = Database::new(storage)?;
//!
//! // Insert a user - creates a new .dat file
//! let user = User {
//!     name: "Alice".to_string(),
//!     email: "alice@example.com".to_string(),
//! };
//! let key = db.put(user)?;
//!
//! // Retrieve the user - reads from the corresponding .dat file
//! let retrieved = db.get(&key)?;
//! assert!(retrieved.is_some());
//! # Ok(())
//! # }
//! ```
//!
//! # Storage Format
//!
//! Records are stored in the filesystem as follows:
//!
//! - Each record is a separate file with a `.dat` extension
//! - Filenames are derived from URL-encoded keys
//! - File contents use CSV format for the serialized data
//! - The storage directory is created automatically if it doesn't exist
//!
//! This makes the storage directory easy to navigate, backup, and inspect manually.

mod error;
mod repository;
mod serializer;

use kivis::{BufferOp, Storage};

use crate::serializer::CsvSerializer;

pub use crate::repository::FileStore;

impl Storage for FileStore {
    type Repo = Self;
    type KeyUnifier = CsvSerializer;
    type ValueUnifier = CsvSerializer;
    type Container = Vec<BufferOp>;

    fn repository(&self) -> &Self::Repo {
        self
    }

    fn repository_mut(&mut self) -> &mut Self::Repo {
        self
    }
}
