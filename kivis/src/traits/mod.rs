mod incrementable_types;
mod schema;
mod storage;

#[cfg(feature = "atomic")]
mod atomic;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use bincode::config::Configuration;
use core::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

pub use schema::*;
pub use storage::*;

/// Error type for serialization operations, re-exported from the [`bincode`] crate.
// pub type SerializationError = bincode::error::EncodeError;
// pub type DeserializationError = bincode::error::DecodeError;

#[cfg(feature = "atomic")]
pub use atomic::*;

use crate::{Database, DatabaseError};

/// The main trait of the crate, defines a database entry that can be stored with its indexes.
#[allow(unused_variables)] // Defalt implementation may not use all variables.
pub trait DatabaseEntry: Scope + Serialize + DeserializeOwned + Debug {
    /// The primary key type for this database entry.
    type Key: RecordKey;
    const INDEX_COUNT_HINT: usize = 0;

    /// Returns the index keys for this entry.
    /// Each tuple contains the index discriminator and the key bytes.
    /// # Errors
    /// Returns an error if serializing any of the index keys fails.
    fn index_keys<I: Indexer>(&self, indexer: &mut I) -> Result<(), I::Error> {
        Ok(())
    }
}

pub trait Manifests<T: Scope + DatabaseEntry> {
    fn last(&mut self) -> &mut Option<T::Key>;
}

pub trait Manifest: Default {
    fn members() -> Vec<u8>;
    /// # Errors
    ///
    /// Returns a [`DatabaseError`] if loading manifests requires access to the
    /// underlying storage and that operation fails.
    fn load<S: Storage>(&mut self, db: &mut Database<S, Self>) -> Result<(), DatabaseError<S>>
    where
        Self: Sized;
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
/// use kivis::{manifest, Scope, Record};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Record, Debug, Serialize, Deserialize)]
/// struct User {
///     id: u64,
/// }
///
/// #[derive(Record, Debug, Serialize, Deserialize)]
/// struct Post {
///     id: u64,
/// }
///
/// #[derive(Record, Debug, Serialize, Deserialize)]
/// struct Comment {
///     id: u64,
/// }
///
/// #[derive(Record, Debug, Serialize, Deserialize)]
/// struct Tag {
///     id: u64,
/// }
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
/// manifest![M: User];
/// ```
#[macro_export]
macro_rules! manifest {
    // Base case: empty list with manifest name
    ($manifest_name:ident:) => {
        #[derive(Default)]
        pub struct $manifest_name;
    };

    // Multiple items case with manifest name - generate implementations with incrementing indices
    ($manifest_name:ident: $($ty:ty),+ $(,)?) => {
                $crate::paste! {
                    #[derive(Default)]
                    pub struct $manifest_name {
                        $(
                            [<last_ $ty:snake>]: ::core::option::Option<<$ty as $crate::DatabaseEntry>::Key>,
                        )*
                    }
                }

        $crate::scope_impl_with_index!($manifest_name, 0; $($ty),+);

        impl $crate::Manifest for $manifest_name {
            fn members() -> ::kivis::alloc::vec::Vec<u8> {
                $crate::generate_member_scopes!(0; $($ty),+)
            }

            fn load<S: $crate::Storage>(&mut self, db: &mut $crate::Database<S, Self>) -> ::core::result::Result<(), $crate::DatabaseError<S::StoreError>> {
                $crate::paste! {
                    $(
                        self.[<last_ $ty:snake>] = ::core::option::Option::Some(db.last_id()?);
                    )*
                }
                ::core::result::Result::Ok(())
            }
        }
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

/// Helper macro to generate member scope list
#[macro_export]
macro_rules! generate_member_scopes {
    // Base case: no more types
    ($index:expr;) => {
        {
            ::kivis::alloc::vec::Vec::new()
        }
    };

    // Single type remaining
    ($index:expr; $ty:ty) => {
        {
            ::kivis::alloc::vec::Vec::from([$index])
        }
    };

    // Multiple types remaining - add current index and recurse
    ($index:expr; $ty:ty, $($rest:ty),+) => {
        {
            let mut scopes = {
                ::kivis::alloc::vec::Vec::from([$index])
            };
            scopes.extend($crate::generate_member_scopes!($index + 1; $($rest),+));
            scopes
        }
    };
}

/// Helper macro for `scope_impl` that tracks the current index and manifest name
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
        impl $crate::Manifests<$ty> for $manifest_name {
            fn last(&mut self) -> &mut ::core::option::Option<<$ty as $crate::DatabaseEntry>::Key> {
                $crate::paste! {
                    &mut self.[<last_ $ty:snake>]
                }
            }
        }
    };

    // Multiple types remaining - implement for first and recurse
    ($manifest_name:ident, $index:expr; $ty:ty, $($rest:ty),+) => {
        impl $crate::Scope for $ty {
            const SCOPE: u8 = $index;
            type Manifest = $manifest_name;
        }
        impl $crate::Manifests<$ty> for $manifest_name {
            fn last(&mut self) -> &mut ::core::option::Option<<$ty as $crate::DatabaseEntry>::Key> {
                $crate::paste! {
                    &mut self.[<last_ $ty:snake>]
                }
            }
        }
        $crate::scope_impl_with_index!($manifest_name, $index + 1; $($rest),+);
    };
}
