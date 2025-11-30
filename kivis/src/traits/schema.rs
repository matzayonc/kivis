use bincode::{
    error::{DecodeError, EncodeError},
    serde::{decode_from_slice, encode_to_vec},
};

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
    type Error;
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn add(&mut self, discriminator: u8, value: &impl Serialize) -> Result<(), Self::Error>;
}

pub trait UnifierData {
    fn combine(&mut self, other: Self);
    fn next(&mut self);
}

impl UnifierData for Vec<u8> {
    fn combine(&mut self, other: Self) {
        self.extend(other);
    }
    fn next(&mut self) {
        for i in (0..self.len()).rev() {
            // Add one if possible
            if self[i] < 255 {
                self[i] += 1;
                return;
            }
            // Otherwise, set to zero and carry over
            self[i] = 0;
        }

        // If all bytes were 255, we need to add a new byte
        self.push(0);
    }
}

pub trait Unifier {
    type D: UnifierData + Clone + PartialEq + Eq;
    type SerError: Debug;
    type DeError: Debug;

    /// Serializes a key.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize_key(&self, data: impl Serialize) -> Result<Self::D, Self::SerError>;

    /// Serializes a value. By default, uses the same serialization as keys.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize_value(&self, data: impl Serialize) -> Result<Self::D, Self::SerError> {
        self.serialize_key(data)
    }

    /// Deserializes a key from the given data.
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    fn deserialize_key<T: DeserializeOwned>(&self, data: &Self::D) -> Result<T, Self::DeError>;

    /// Deserializes a value from the given data. By default, uses the same deserialization as keys.
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    fn deserialize_value<T: DeserializeOwned>(&self, data: &Self::D) -> Result<T, Self::DeError> {
        self.deserialize_key(data)
    }
}

impl Unifier for Configuration {
    type D = Vec<u8>;
    type SerError = EncodeError;
    type DeError = DecodeError;

    fn serialize_key(&self, data: impl Serialize) -> Result<Self::D, Self::SerError> {
        encode_to_vec(data, Self::default())
    }

    fn deserialize_key<T: DeserializeOwned>(&self, data: &Self::D) -> Result<T, Self::DeError> {
        Ok(decode_from_slice(data, Self::default())?.0)
    }
}

pub struct SimpleIndexer<U: Unifier>(Vec<(u8, U::D)>, U);
impl<U: Unifier> SimpleIndexer<U> {
    pub fn new(serializer: U) -> Self {
        Self(Vec::new(), serializer)
    }

    pub fn into_index_keys(self) -> Vec<(u8, U::D)> {
        self.0
    }
}
impl<U: Unifier> Indexer for SimpleIndexer<U> {
    type Error = U::SerError;
    fn add(&mut self, discriminator: u8, index: &impl Serialize) -> Result<(), Self::Error> {
        let data = self.1.serialize_key(index)?;
        self.0.push((discriminator, data));
        Ok(())
    }
}
