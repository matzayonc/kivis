use bincode::{
    error::{DecodeError, EncodeError},
    serde::{decode_from_slice, encode_to_vec},
};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use bincode::config::Configuration;
use core::fmt::Debug;
use serde::{Serialize, de::DeserializeOwned};
use std::error::Error;
use std::fmt::Display;

use crate::{BufferOverflowError, BufferOverflowOr};

pub trait UnifierData: Default + Clone {
    /// The borrowed view type for this buffer (e.g., &[u8] for Vec<u8>, &str for String)
    type View<'a>;

    /// Converts the buffer to a view with explicit lifetime
    fn as_view(&self) -> Self::View<'_> {
        self.extract_range(0, self.len())
    }

    /// Creates a buffer from a view
    fn from_view(data: Self::View<'_>) -> Self;

    /// Increments the buffer to the next value.
    fn next(&mut self);

    /// Appends a single part to the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn extend(&mut self, part: Self::View<'_>) -> Result<(), BufferOverflowError>;

    /// Returns the current length of the buffer.
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Extracts a range from the buffer as a reference.
    /// This is used to extract individual values from buffered data.
    #[must_use]
    fn extract_range(&self, start: usize, end: usize) -> Self::View<'_>;

    /// Duplicates data by cloning the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn duplicate(data: Self::View<'_>) -> Result<Self, BufferOverflowError> {
        let mut result = Self::default();
        result.extend(data)?;
        Ok(result)
    }

    /// Duplicates a range from the buffer and appends it to the same buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn duplicate_within(&mut self, start: usize, end: usize) -> Result<(), BufferOverflowError>;
}

impl UnifierData for Vec<u8> {
    type View<'a> = &'a [u8];

    fn from_view(data: Self::View<'_>) -> Self {
        data.to_vec()
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

    fn extend(&mut self, part: Self::View<'_>) -> Result<(), BufferOverflowError> {
        self.try_reserve(part.len())
            .map_err(|_| BufferOverflowError)?;
        self.extend_from_slice(part);
        Ok(())
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn extract_range(&self, start: usize, end: usize) -> Self::View<'_> {
        &self[start..end]
    }

    fn duplicate_within(&mut self, start: usize, end: usize) -> Result<(), BufferOverflowError> {
        let len = self.len();
        let part_len = end - start;
        self.try_reserve(part_len)
            .map_err(|_| BufferOverflowError)?;
        self.resize(len + part_len, 0);
        self.copy_within(start..end, len);
        Ok(())
    }
}

#[cfg(feature = "std")]
impl UnifierData for String {
    type View<'a> = &'a str;

    fn from_view(data: Self::View<'_>) -> Self {
        data.to_string()
    }

    fn next(&mut self) {
        let mut bytes = self.as_bytes().to_vec();

        let next_valid_string = loop {
            bytes.next();

            if let Ok(parsed_back) = String::from_utf8(bytes.clone()) {
                // If the bytes are not valid UTF-8, increment and try again.
                break parsed_back;
            }
        };

        *self = next_valid_string;
    }

    fn extend(&mut self, part: Self::View<'_>) -> Result<(), BufferOverflowError> {
        self.try_reserve(part.len())
            .map_err(|_| BufferOverflowError)?;
        self.push_str(part);
        Ok(())
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn extract_range(&self, start: usize, end: usize) -> Self::View<'_> {
        &self[start..end]
    }

    fn duplicate_within(&mut self, start: usize, end: usize) -> Result<(), BufferOverflowError> {
        self.try_reserve(end - start)
            .map_err(|_| BufferOverflowError)?;
        self.extend_from_within(start..end);
        Ok(())
    }
}

pub trait Unifiable: Serialize + DeserializeOwned {}
pub trait UnifiableRef: Unifiable + Clone {}

impl<T: Serialize + DeserializeOwned> Unifiable for T {}
impl<T: Serialize + DeserializeOwned + Clone> UnifiableRef for T {}

pub trait Unifier {
    type D: UnifierData;
    type SerError: Debug + Display + Error;
    type DeError: Debug + Display + Error;

    /// Serializes data directly into an existing buffer and returns the start and end positions.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize(
        &self,
        buffer: &mut Self::D,
        data: impl Unifiable,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>>;

    /// Serializes borrowed data directly into an existing buffer and returns the start and end positions.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize_ref<R: UnifiableRef>(
        &self,
        buffer: &mut Self::D,
        data: &R,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        self.serialize(buffer, data.clone())
    }

    /// Deserializes data from the given buffer.
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    fn deserialize<T: Unifiable>(&self, data: &Self::D) -> Result<T, Self::DeError>;
}

impl Unifier for Configuration {
    type D = Vec<u8>;
    type SerError = EncodeError;
    type DeError = DecodeError;

    fn serialize(
        &self,
        buffer: &mut Vec<u8>,
        data: impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        let start = buffer.len();
        let serialized = encode_to_vec(data, Self::default())?;
        UnifierData::extend(buffer, &serialized).map_err(BufferOverflowOr::overflow)?;
        Ok((start, buffer.len()))
    }

    fn deserialize<T: DeserializeOwned>(&self, data: &Vec<u8>) -> Result<T, Self::DeError> {
        Ok(decode_from_slice(data, Self::default())?.0)
    }
}
