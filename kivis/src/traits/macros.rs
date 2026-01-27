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
            fn members() -> &'static [u8] {
                &$crate::generate_member_scopes!(0; $($ty),+)
            }

            fn load<S: $crate::Storage>(&mut self, db: &mut $crate::Database<S, Self>) -> ::core::result::Result<(), $crate::DatabaseError<S>> {
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
    ($index:expr; $($ty:ty),+) => {
        {
            [
                $(<$ty as $crate::Scope>::SCOPE),+
            ]
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
