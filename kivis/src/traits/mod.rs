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
    type Manifest;
}

/// Declarative macro to implement the Scope trait for multiple structs with a named manifest.
/// Each struct gets its position in the array as its SCOPE value.
/// Also generates an empty manifest struct and assigns it as the Manifest type.
///
/// # Example
///
/// ```rust
/// use kivis::{manifest, Scope};
///
/// struct User;
/// struct Post;
/// struct Comment;
/// struct Tag;
///
/// manifest![MyDatabase: User, Post, Comment, Tag];
///
/// // Verify the generated implementations
/// assert_eq!(User::SCOPE, 0);
/// assert_eq!(Post::SCOPE, 1);
/// assert_eq!(Comment::SCOPE, 2);
/// assert_eq!(Tag::SCOPE, 3);
/// ```
///
/// # Compilation Errors
///
/// The macro requires a manifest name followed by a colon. Using the old syntax will fail:
///
/// ```compile_fail
/// use kivis::manifest;
///
/// struct User;
/// struct Post;
///
/// // This will fail to compile with the error:
/// // "manifest! macro requires a manifest name followed by a colon. Use: manifest![ManifestName: Type1, Type2, ...]"
/// manifest![User, Post];
/// ```
///
/// Single type without colon also fails:
///
/// ```compile_fail
/// use kivis::manifest;
///
/// struct User;
///
/// // This will fail to compile with the same error:
/// // "manifest! macro requires a manifest name followed by a colon. Use: manifest![ManifestName: Type1, Type2, ...]"
/// manifest![User];
/// ```
#[macro_export]
macro_rules! manifest {
    // Base case: empty list with manifest name
    ($manifest_name:ident:) => {
        pub struct $manifest_name;
    };

    // Single item case (first item, index 0) with manifest name
    ($manifest_name:ident: $first:ty) => {
        pub struct $manifest_name;

        impl $crate::Scope for $first {
            const SCOPE: u8 = 0;
            type Manifest = $manifest_name;
        }
    };

    // Multiple items case with manifest name - generate implementations with incrementing indices
    ($manifest_name:ident: $($ty:ty),+ $(,)?) => {
        pub struct $manifest_name;

        $crate::scope_impl_with_index!($manifest_name, 0; $($ty),+);
    };

    // Error case: catch patterns without the required colon delimiter
    ($($ty:ty),+ $(,)?) => {
        compile_error!("manifest! macro requires a manifest name followed by a colon. Use: manifest![ManifestName: Type1, Type2, ...]");
    };

    // Error case: single type without colon
    ($ty:ty) => {
        compile_error!("manifest! macro requires a manifest name followed by a colon. Use: manifest![ManifestName: Type1, Type2, ...]");
    };
}

/// Helper macro for scope_impl that tracks the current index and manifest name
#[macro_export]
macro_rules! scope_impl_with_index {
    // Base case: no more types
    ($manifest_name:ident, $index:expr;) => {};

    // Single type remaining
    ($manifest_name:ident, $index:expr; $ty:ty) => {
        impl $crate::Scope for $ty {
            const SCOPE: u8 = $index;
            type Manifest = $manifest_name;
        }
    };

    // Multiple types remaining - implement for first and recurse
    ($manifest_name:ident, $index:expr; $ty:ty, $($rest:ty),+) => {
        impl $crate::Scope for $ty {
            const SCOPE: u8 = $index;
            type Manifest = $manifest_name;
        }
        $crate::scope_impl_with_index!($manifest_name, $index + 1; $($rest),+);
    };
}
