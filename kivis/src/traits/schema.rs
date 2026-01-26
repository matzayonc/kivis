use bincode::{
    error::{DecodeError, EncodeError},
    serde::{decode_from_slice, encode_to_vec},
};

pub use super::*;

/// A trait defining that the implementing type is a key of some record.
/// Each type can be a key of only one record type, which is defined by the [`DatabaseEntry`] trait.
pub trait RecordKey: Serialize + DeserializeOwned + Clone + Eq + UnifiableRef {
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
pub trait Index: Unifiable + Debug {
    /// The key type used by this index.
    type Key: Unifiable + Clone + Eq + Debug;
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
    fn add(&mut self, discriminator: u8, value: &impl UnifiableRef) -> Result<(), Self::Error>;
}

pub trait UnifierData {
    /// The owned type for this data (e.g., Vec<u8> for [u8], String for str)
    type Owned: Default + Clone + AsRef<Self>;

    /// Increments the buffer to the next value.
    fn next(buffer: &mut Self::Owned);

    /// Appends a single part to the buffer.
    fn extend(buffer: &mut Self::Owned, part: &Self);

    /// Returns the current length of the buffer.
    fn len(buffer: &Self::Owned) -> usize;

    /// Extracts a range from the owned buffer as a reference.
    /// This is used to extract individual values from buffered data.
    #[must_use]
    fn extract_range(buffer: &Self::Owned, start: usize, end: usize) -> &Self;

    /// Converts a reference to an owned value.
    #[must_use]
    fn to_owned(data: &Self) -> Self::Owned;

    /// Duplicates data by converting to owned.
    #[must_use]
    fn duplicate(data: &Self) -> Self::Owned {
        Self::to_owned(data)
    }
}

impl UnifierData for [u8] {
    type Owned = Vec<u8>;

    fn next(buffer: &mut Self::Owned) {
        for i in (0..buffer.len()).rev() {
            // Add one if possible
            if buffer[i] < 255 {
                buffer[i] += 1;
                return;
            }
            // Otherwise, set to zero and carry over
            buffer[i] = 0;
        }

        // If all bytes were 255, we need to add a new byte
        buffer.push(0);
    }

    fn extend(buffer: &mut Self::Owned, part: &Self) {
        buffer.extend_from_slice(part);
    }

    fn len(buffer: &Self::Owned) -> usize {
        buffer.len()
    }

    fn extract_range(buffer: &Self::Owned, start: usize, end: usize) -> &Self {
        &buffer[start..end]
    }

    fn to_owned(data: &Self) -> Self::Owned {
        data.to_vec()
    }
}

#[cfg(feature = "std")]
impl UnifierData for str {
    type Owned = alloc::string::String;

    fn next(buffer: &mut Self::Owned) {
        let mut bytes = buffer.as_bytes().to_vec();

        let next_valid_string = loop {
            <[u8]>::next(&mut bytes);

            if let Ok(parsed_back) = alloc::string::String::from_utf8(bytes.clone()) {
                // If the bytes are not valid UTF-8, increment and try again.
                break parsed_back;
            }
        };

        *buffer = next_valid_string;
    }

    fn extend(buffer: &mut Self::Owned, part: &Self) {
        buffer.push_str(part);
    }

    fn len(buffer: &Self::Owned) -> usize {
        buffer.len()
    }

    fn extract_range(buffer: &Self::Owned, start: usize, end: usize) -> &Self {
        &buffer[start..end]
    }

    fn to_owned(data: &Self) -> Self::Owned {
        data.to_string()
    }
}

pub trait Unifiable: Serialize + DeserializeOwned {}
pub trait UnifiableRef: Unifiable + Clone {}

impl<T: Serialize + DeserializeOwned> Unifiable for T {}
impl<T: Serialize + DeserializeOwned + Clone> UnifiableRef for T {}

pub trait Unifier {
    type K: UnifierData + ?Sized;
    type V: UnifierData + ?Sized;
    type SerError: Debug;
    type DeError: Debug;

    /// Serializes a key directly into an existing buffer and returns the start and end positions.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize_key(
        &self,
        buffer: &mut <Self::K as UnifierData>::Owned,
        data: impl Unifiable,
    ) -> Result<(usize, usize), Self::SerError>;

    /// Serializes a borrowed key directly into an existing buffer and returns the start and end positions.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize_key_ref<R: UnifiableRef>(
        &self,
        buffer: &mut <Self::K as UnifierData>::Owned,
        data: &R,
    ) -> Result<(usize, usize), Self::SerError> {
        self.serialize_key(buffer, data.clone())
    }

    /// Serializes a value directly into an existing buffer and returns the start and end positions.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize_value(
        &self,
        buffer: &mut <Self::V as UnifierData>::Owned,
        data: impl Unifiable,
    ) -> Result<(usize, usize), Self::SerError>;

    /// Serializes a borrowed value directly into an existing buffer and returns the start and end positions.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize_value_ref<R: UnifiableRef>(
        &self,
        buffer: &mut <Self::V as UnifierData>::Owned,
        data: &R,
    ) -> Result<(usize, usize), Self::SerError> {
        self.serialize_value(buffer, data.clone())
    }

    /// Deserializes a key from the given data.
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    fn deserialize_key<T: Unifiable>(
        &self,
        data: &<Self::K as UnifierData>::Owned,
    ) -> Result<T, Self::DeError>;

    /// Deserializes a value from the given data.
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    fn deserialize_value<T: Unifiable>(
        &self,
        data: &<Self::V as UnifierData>::Owned,
    ) -> Result<T, Self::DeError>;
}

impl Unifier for Configuration {
    type K = [u8];
    type V = [u8];
    type SerError = EncodeError;
    type DeError = DecodeError;

    fn serialize_key(
        &self,
        buffer: &mut Vec<u8>,
        data: impl Serialize,
    ) -> Result<(usize, usize), Self::SerError> {
        let start = <[u8]>::len(buffer);
        let serialized = encode_to_vec(data, Self::default())?;
        <[u8]>::extend(buffer, &serialized);
        Ok((start, <[u8]>::len(buffer)))
    }

    fn serialize_value(
        &self,
        buffer: &mut Vec<u8>,
        data: impl Serialize,
    ) -> Result<(usize, usize), Self::SerError> {
        let start = <[u8]>::len(buffer);
        let serialized = encode_to_vec(data, Self::default())?;
        <[u8]>::extend(buffer, &serialized);
        Ok((start, <[u8]>::len(buffer)))
    }

    fn deserialize_key<T: DeserializeOwned>(&self, data: &Vec<u8>) -> Result<T, Self::DeError> {
        Ok(decode_from_slice(data, Self::default())?.0)
    }

    fn deserialize_value<T: DeserializeOwned>(&self, data: &Vec<u8>) -> Result<T, Self::DeError> {
        Ok(decode_from_slice(data, Self::default())?.0)
    }
}

pub struct IndexBuilder<U: Unifier>(Vec<(u8, <U::K as UnifierData>::Owned)>, U);
impl<U: Unifier> IndexBuilder<U> {
    pub fn new(serializer: U) -> Self {
        Self(Vec::new(), serializer)
    }

    pub fn into_index_keys(self) -> Vec<(u8, <U::K as UnifierData>::Owned)> {
        self.0
    }
}
impl<U: Unifier> Indexer for IndexBuilder<U> {
    type Error = U::SerError;
    fn add(&mut self, discriminator: u8, index: &impl UnifiableRef) -> Result<(), Self::Error> {
        let mut buffer = <U::K as UnifierData>::Owned::default();
        self.1.serialize_key_ref(&mut buffer, index)?;
        self.0.push((discriminator, buffer));
        Ok(())
    }
}
