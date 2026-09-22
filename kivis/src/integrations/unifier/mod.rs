//! Implementations of the `Unifier` trait for various serialization formats.

#[cfg(any(feature = "std", feature = "alloc"))]
mod bincode;

#[cfg(any(feature = "std", feature = "alloc"))]
pub use bincode::{OrderedKeyConfig, ordered_key_config};
