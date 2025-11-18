use super::Storage;

/// A trait defining atomic operations for storage backends.
///
/// This trait extends the basic [`Storage`] trait with atomic batch operations.
/// Storage implementations that support atomic transactions can implement this trait
/// to provide guarantees that multiple operations either all succeed or all fail.
///
/// This trait is only available when the "atomic" feature is enabled.
#[cfg(feature = "atomic")]
pub trait AtomicStorage: Storage {
    /// Execute mixed insert and delete operations atomically.
    ///
    /// Either all operations succeed, or none of them are persisted.
    /// This method is used internally by the database to ensure consistency
    /// when performing operations that involve multiple writes.
    ///
    /// # Arguments
    /// * `inserts` - A vector of key-value pairs to insert
    /// * `removes` - A vector of keys to remove
    ///
    /// # Returns
    ///
    /// Returns `Ok(Vec<Option<Vec<u8>>>)` with the previous values (if any) for removed keys,
    /// or an error if any operation fails. In case of error, no changes should be persisted.
    ///
    /// # Errors
    /// Returns an error if any of the insert or remove operations fail.
    fn batch_mixed(
        &mut self,
        inserts: Vec<(Vec<u8>, Vec<u8>)>,
        removes: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::StoreError>;
}
