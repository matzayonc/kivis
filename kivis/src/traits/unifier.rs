use bincode::{
    error::{DecodeError, EncodeError},
    serde::{decode_from_slice, encode_to_vec},
};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use bincode::config::Configuration;
use core::fmt::Debug;
use serde::{Serialize, de::DeserializeOwned};

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

    /// Duplicates data by cloning the owned buffer.
    #[must_use]
    fn duplicate(data: &Self) -> Self::Owned
    where
        Self::Owned: Clone + AsRef<Self>,
    {
        let mut result = Self::Owned::default();
        Self::extend(&mut result, data);
        result
    }

    /// Duplicates a range from the buffer and appends it to the same buffer.
    /// Equivalent to extract_range + to_owned + extend combined.
    fn duplicate_within(buffer: &mut Self::Owned, start: usize, end: usize);
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

    fn duplicate_within(buffer: &mut Self::Owned, start: usize, end: usize) {
        let len = buffer.len();
        let copy_len = end - start;
        buffer.resize(len + copy_len, 0);
        buffer.copy_within(start..end, len);
    }
}

#[cfg(feature = "std")]
impl UnifierData for str {
    type Owned = String;

    fn next(buffer: &mut Self::Owned) {
        let mut bytes = buffer.as_bytes().to_vec();

        let next_valid_string = loop {
            <[u8]>::next(&mut bytes);

            if let Ok(parsed_back) = String::from_utf8(bytes.clone()) {
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

    fn duplicate_within(buffer: &mut Self::Owned, start: usize, end: usize) {
        buffer.extend_from_within(start..end);
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
