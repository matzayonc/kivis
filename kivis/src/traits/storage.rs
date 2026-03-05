use crate::{Repository, Unifier, UnifierPair};

/// Error returned by [`Repository::try_apply`]: either the iterator produced an error or
/// the underlying storage did.
pub enum TryApplyError<IterErr, StorageErr> {
    /// The fallible iterator yielded an error before all operations were applied.
    Iterator(IterErr),
    /// The underlying storage returned an error while applying operations.
    Storage(StorageErr),
}

/// Represents a batch operation: either insert or delete.
pub enum BatchOp<U: UnifierPair> {
    /// Insert operation with owned key and value
    Insert {
        key: <U::KeyUnifier as Unifier>::D,
        value: <U::ValueUnifier as Unifier>::D,
    },
    /// Delete operation with owned key
    Delete { key: <U::KeyUnifier as Unifier>::D },
}

/// A trait defining a storage backend for the database.
///
/// The storage backend is responsible for storing and retrieving records and their associated indexes.
/// It defines methods for inserting, getting, removing, and iterating over keys in the storage.
/// All storage operations are defined over serialized byte data.
pub trait Storage {
    /// Combined key+value unifier pair for this storage.
    type Unifiers: UnifierPair;

    /// The repository type that this storage uses for low-level key-value operations.
    type Repo: Repository<
            K = <<<Self as Storage>::Unifiers as UnifierPair>::KeyUnifier as Unifier>::D,
            V = <<<Self as Storage>::Unifiers as UnifierPair>::ValueUnifier as Unifier>::D,
        >;

    /// Returns a reference to the underlying repository.
    fn repository(&self) -> &Self::Repo;

    /// Returns a mutable reference to the underlying repository.
    fn repository_mut(&mut self) -> &mut Self::Repo;
}
