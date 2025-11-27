pub use super::*;

/// A trait defining that the implementing type is a key of some record.
/// Each type can be a key of only one record type, which is defined by the [`DatabaseEntry`] trait.
pub trait RecordKey: Serialize + DeserializeOwned + Clone + Eq {
    /// The record type that this key identifies.
    type Record: DatabaseEntry;
}

/// A trait defining how a key can be extracted from a record.
/// This might be one of the fields, a composite key, a hash, random uuid or any other type of derivation.
/// It shouldn't be implemented for auto-incrementing keys.
pub trait DeriveKey {
    /// The key type that can be derived from this record.
    type Key: RecordKey;
    /// Derives the key from the record.
    fn key(c: &<Self::Key as RecordKey>::Record) -> Self::Key;
}

/// A trait describing how a key can be auto-incremented, defined for numeric types.
pub trait Incrementable: Default + Sized {
    /// The first and last valid values of the type.
    // const BOUNDS: (Self, Self);
    /// Returns the next value of the type, if applicable.
    fn next_id(&self) -> Option<Self>;
}

/// A trait defining an index in the database.
///
/// An index is a way to efficiently look up records in the database by a specific key.
/// It defines a table, primary key type, and an unique prefix for the index.
pub trait Index: Serialize + Debug {
    /// The key type used by this index.
    type Key: Serialize + DeserializeOwned + Clone + Eq + Debug;
    /// The record type that this index applies to.
    type Record: DatabaseEntry;
    /// Unique identifier for this index within the record type.
    const INDEX: u8;
}

pub trait Indexer {
    fn add(&mut self, discriminator: u8, value: &impl Serialize);
}

pub struct SimpleIndexer(Vec<(u8, Vec<u8>)>, Configuration);
impl SimpleIndexer {
    pub fn new(config: Configuration) -> Self {
        Self(Vec::new(), config)
    }

    pub fn into_index_keys(self) -> Vec<(u8, Vec<u8>)> {
        self.0
    }
}
impl Indexer for SimpleIndexer {
    fn add(&mut self, discriminator: u8, index: &impl Serialize) {
        let bytes = bincode::serde::encode_to_vec(index, self.1)
            .expect("Serialization failed in SimpleIndexer");
        self.0.push((discriminator, bytes));
    }
}
