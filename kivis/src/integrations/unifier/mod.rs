//! Implementations of the `Unifier` trait for various serialization formats.

#[cfg(any(feature = "std", feature = "alloc"))]
mod bincode;
