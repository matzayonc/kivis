#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::{Repository, Unifier, UnifierData};

pub type Deleted<S> = Vec<Option<<<S as Repository>::V as crate::UnifierData>::Owned>>;

/// Represents a batch operation: either insert or delete.
pub enum BatchOp<'a, K: UnifierData + ?Sized, V: UnifierData + ?Sized> {
    /// Insert operation with key and value references
    Insert {
        key: K::View<'a>,
        value: V::View<'a>,
    },
    /// Delete operation with key reference
    Delete { key: K::View<'a> },
}

/// A trait defining a storage backend for the database.
///
/// The storage backend is responsible for storing and retrieving records and their associated indexes.
/// It defines methods for inserting, getting, removing, and iterating over keys in the storage.
/// All storage operations are defined over serialized byte data.
pub trait Storage:
    Repository<
        K = <<Self as Storage>::KeyUnifier as Unifier>::D,
        V = <<Self as Storage>::ValueUnifier as Unifier>::D,
    >
{
    /// Unifier type used to serialize/deserialize keys.
    type KeyUnifier: Unifier + Default + Copy;

    /// Unifier type used to serialize/deserialize values.
    type ValueUnifier: Unifier + Default + Copy;
}
