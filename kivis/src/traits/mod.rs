mod incrementable_types;
mod schema;
mod serialization;
mod storage;

use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

pub use schema::*;
pub use serialization::*;
pub use storage::*;

/// Error type for serialization operations, re-exported from the BCS crate.
pub type SerializationError = bcs::Error;

/// The main trait of the crate, defines a database entry that can be stored with its indexes.
pub trait DatabaseEntry: Scope + Serialize + DeserializeOwned + Debug {
    /// The primary key type for this database entry.
    type Key: RecordKey;

    /// Returns the index keys for this entry.
    /// Each tuple contains the index discriminator and the key bytes.
    fn index_keys(&self) -> Vec<(u8, &dyn KeyBytes)> {
        vec![]
    }
}

pub trait Scope {
    /// Unique table identifier for this database entry type.
    /// Must be unique across all tables in a database instance.
    const SCOPE: u8;
}

/// Declarative macro to implement the Scope trait for multiple structs.
/// Each struct gets its position in the array as its SCOPE value.
///
/// # Example
///
/// ```rust
/// use kivis::scope_impl;
///
/// scope_impl![User, Post, Comment, Tag];
/// // This generates:
/// // impl Scope for User { const SCOPE: u8 = 0; }
/// // impl Scope for Post { const SCOPE: u8 = 1; }
/// // impl Scope for Comment { const SCOPE: u8 = 2; }
/// // impl Scope for Tag { const SCOPE: u8 = 3; }
/// ```
#[macro_export]
macro_rules! manifest {
    // Base case: empty list
    () => {};

    // Single item case (first item, index 0)
    ($first:ty) => {
        impl $crate::Scope for $first {
            const SCOPE: u8 = 0;
        }
    };

    // Multiple items case - generate implementations with incrementing indices
    ($($ty:ty),+ $(,)?) => {
        $crate::scope_impl_with_index!(0; $($ty),+);
    };
}

/// Helper macro for scope_impl that tracks the current index
#[macro_export]
macro_rules! scope_impl_with_index {
    // Base case: no more types
    ($index:expr;) => {};

    // Single type remaining
    ($index:expr; $ty:ty) => {
        impl $crate::Scope for $ty {
            const SCOPE: u8 = $index;
        }
    };

    // Multiple types remaining - implement for first and recurse
    ($index:expr; $ty:ty, $($rest:ty),+) => {
        impl $crate::Scope for $ty {
            const SCOPE: u8 = $index;
        }
        $crate::scope_impl_with_index!($index + 1; $($rest),+);
    };
}
