#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::fmt::Debug;
use super::*;

/// A trait defining an index in the database.
///
/// An index is a way to efficiently look up records in the database by a specific key.
/// It defines a table, primary key type, and an unique prefix for the index.
pub trait Index: Unifiable + Debug {
    /// The key type used by this index.
    type Key: Unifiable + Clone + Eq + Debug;
    /// The record type that this index applies to.
    type Record: DatabaseEntry;
    /// Unique identifier for this index within the record type.
    const INDEX: u8;
}

pub struct IndexBuilder<U: Unifier> {
    /// List of end positions for each index
    indices: Vec<usize>,
    /// Shared buffer for all index keys
    key_data: <U::K as UnifierData>::Owned,
    serializer: U,
}

pub struct IndexIter<U: Unifier> {
    indices: alloc::vec::IntoIter<usize>,
    key_data: <U::K as UnifierData>::Owned,
    current_start: usize,
    current_discriminator: u8,
}

impl<U: Unifier> Iterator for IndexIter<U> {
    type Item = (u8, <U::K as UnifierData>::Owned);

    fn next(&mut self) -> Option<Self::Item> {
        let end = self.indices.next()?;
        let key_slice =
            <U::K as UnifierData>::extract_range(&self.key_data, self.current_start, end);
        let key_owned = <U::K as UnifierData>::to_owned(key_slice);
        let discriminator = self.current_discriminator;
        self.current_start = end;
        self.current_discriminator += 1;
        Some((discriminator, key_owned))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.indices.size_hint()
    }
}

impl<U: Unifier> ExactSizeIterator for IndexIter<U> {
    fn len(&self) -> usize {
        self.indices.len()
    }
}

impl<U: Unifier> IndexBuilder<U> {
    pub fn new(serializer: U) -> Self {
        Self {
            indices: Vec::new(),
            key_data: <U::K as UnifierData>::Owned::default(),
            serializer,
        }
    }

    pub fn iter(self) -> IndexIter<U> {
        IndexIter {
            indices: self.indices.into_iter(),
            key_data: self.key_data,
            current_start: 0,
            current_discriminator: 0,
        }
    }

    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn add(&mut self, index: &impl UnifiableRef) -> Result<(), U::SerError> {
        self.serializer
            .serialize_key_ref(&mut self.key_data, index)?;
        let end = <U::K as UnifierData>::len(&self.key_data);
        self.indices.push(end);
        Ok(())
    }
}
