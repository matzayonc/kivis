use crate::{Repository, Unifier, UnifierData, UnifierPair};

/// Represents a batch operation: either insert or delete.
pub enum BatchOp<'a, K: UnifierData, V: UnifierData> {
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
