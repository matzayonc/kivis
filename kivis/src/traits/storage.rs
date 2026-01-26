#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::{fmt::Display, ops::Range};

use crate::{OpsIter, Unifier};

use super::Debug;

type KeysIteratorItem<S> = Result<
    <<<S as Storage>::Serializer as Unifier>::K as crate::UnifierData>::Owned,
    <S as Storage>::StoreError,
>;

type Value<S> = <<<S as Storage>::Serializer as Unifier>::V as crate::UnifierData>::Owned;
type KeyRef<S> = <<S as Storage>::Serializer as Unifier>::K;
type ValueRef<S> = <<S as Storage>::Serializer as Unifier>::V;
pub type Deleted<S> = Vec<Option<Value<S>>>;

/// Represents a batch operation: either insert or delete.
pub enum BatchOp<'a, K: ?Sized, V: ?Sized> {
    /// Insert operation with key and value references
    Insert { key: &'a K, value: &'a V },
    /// Delete operation with key reference
    Delete { key: &'a K },
}

/// A trait defining a storage backend for the database.
///
/// The storage backend is responsible for storing and retrieving records and their associated indexes.
/// It defines methods for inserting, getting, removing, and iterating over keys in the storage.
/// All storage operations are defined over serialized byte data.
pub trait Storage {
    /// Serializer type used to convert data to/from bytes.
    type Serializer: Unifier + Default + Copy;

    /// Error type returned by storage operations.
    /// Must be able to represent serialization and deserialization errors.
    type StoreError: Debug
        + Display
        + Eq
        + PartialEq
        + From<<<Self as Storage>::Serializer as Unifier>::SerError>
        + From<<<Self as Storage>::Serializer as Unifier>::DeError>;

    /// Should insert the given key-value pair into the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails to insert the key-value pair.
    fn insert(
        &mut self,
        key: &KeyRef<Self>,
        value: &ValueRef<Self>,
    ) -> Result<(), Self::StoreError>;

    /// Should retrieve the value associated with the given key from the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while retrieving the value.
    fn get(&self, key: &KeyRef<Self>) -> Result<Option<Value<Self>>, Self::StoreError>;

    /// Should remove the value associated with the given key from the storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails while removing the value.
    fn remove(&mut self, key: &KeyRef<Self>) -> Result<Option<Value<Self>>, Self::StoreError>;

    /// Should iterate over the keys in the storage that are in range.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storages fails during iteration.
    fn iter_keys(
        &self,
        range: Range<<<Self::Serializer as Unifier>::K as crate::UnifierData>::Owned>,
    ) -> Result<impl Iterator<Item = KeysIteratorItem<Self>>, Self::StoreError>
    where
        Self: Sized;

    /// Execute mixed insert and delete operations.
    ///
    /// Default implementation applies operations one by one (not atomic).
    /// Storage backends can override this method to provide atomic behavior.
    ///
    /// # Arguments
    /// * `operations` - A vector of batch operations (inserts and deletes)
    ///
    /// # Returns
    ///
    /// Returns `Ok(Vec<Option<V>>)` with the previous values (if any) for deleted keys only,
    /// in the order delete operations appear in the operations vector.
    /// Insert operations do not contribute to the result vector.
    ///
    /// # Errors
    /// Returns an error if any of the insert or remove operations fail.
    fn batch_mixed(
        &mut self,
        operations: OpsIter<'_, Self::Serializer>,
    ) -> Result<Vec<Option<Value<Self>>>, Self::StoreError> {
        // Default implementation: apply operations one by one (not atomic)
        let mut deleted = Vec::new();

        for op in operations {
            match op {
                BatchOp::Insert { key, value } => {
                    self.insert(key, value)?;
                }
                BatchOp::Delete { key } => {
                    deleted.push(self.remove(key)?);
                }
            }
        }

        Ok(deleted)
    }
}
