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

pub trait UnifierData {
    /// The owned type for this data (e.g., Vec<u8> for [u8], String for str)
    type Owned: Default + Clone + AsRef<Self> + for<'a> From<&'a Self>;
    type Buffer: Default + Clone + AsRef<Self> + From<Self::Owned>;
    type View<'a>;

    /// Increments the buffer to the next value.
    fn next(buffer: &mut Self::Buffer);

    /// Appends a single part to the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn extend(buffer: &mut Self::Buffer, part: &Self) -> Result<(), BufferOverflowError>;

    /// Returns the current length of the buffer.
    fn len(buffer: &Self::Buffer) -> usize;

    /// Extracts a range from the owned buffer as a reference.
    /// This is used to extract individual values from buffered data.
    #[must_use]
    fn extract_range(buffer: &Self::Buffer, start: usize, end: usize) -> Self::View<'_>;
    // TODO: consider removing
    fn extract_full(buffer: &Self::Buffer) -> Self::View<'_> {
        Self::extract_range(buffer, 0, Self::len(buffer))
    }

    /// Duplicates data by cloning the owned buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn duplicate(data: &Self) -> Result<Self::Buffer, BufferOverflowError>
    where
        Self::Buffer: Clone + AsRef<Self>,
    {
        let mut result = Self::Buffer::default();
        Self::extend(&mut result, data)?;
        Ok(result)
    }

    /// Duplicates a range from the buffer and appends it to the same buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn duplicate_within(
        buffer: &mut Self::Buffer,
        start: usize,
        end: usize,
    ) -> Result<(), BufferOverflowError>;
}

impl UnifierData for [u8] {
    type Owned = Vec<u8>;
    type Buffer = Vec<u8>;
    type View<'a> = &'a [u8];

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

    fn extend(buffer: &mut Self::Buffer, part: &Self) -> Result<(), BufferOverflowError> {
        buffer
            .try_reserve(part.len())
            .map_err(|_| BufferOverflowError)?;
        buffer.extend_from_slice(part);
        Ok(())
    }

    fn len(buffer: &Self::Buffer) -> usize {
        buffer.len()
    }

    fn extract_range(buffer: &Self::Buffer, start: usize, end: usize) -> Self::View<'_> {
        &buffer[start..end]
    }

    fn duplicate_within(
        buffer: &mut Self::Buffer,
        start: usize,
        end: usize,
    ) -> Result<(), BufferOverflowError> {
        let len = buffer.len();
        let part_len = end - start;
        buffer
            .try_reserve(part_len)
            .map_err(|_| BufferOverflowError)?;
        buffer.resize(len + part_len, 0);
        buffer.copy_within(start..end, len);
        Ok(())
    }
}

#[cfg(feature = "std")]
impl UnifierData for str {
    type Owned = String;
    type Buffer = String;
    type View<'a> = &'a str;

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

    fn extend(buffer: &mut Self::Owned, part: &Self) -> Result<(), BufferOverflowError> {
        buffer
            .try_reserve(part.len())
            .map_err(|_| BufferOverflowError)?;
        buffer.push_str(part);
        Ok(())
    }

    fn len(buffer: &Self::Owned) -> usize {
        buffer.len()
    }

    fn extract_range(buffer: &Self::Owned, start: usize, end: usize) -> Self::View<'_> {
        &buffer[start..end]
    }

    fn duplicate_within(
        buffer: &mut Self::Owned,
        start: usize,
        end: usize,
    ) -> Result<(), BufferOverflowError> {
        buffer
            .try_reserve(end - start)
            .map_err(|_| BufferOverflowError)?;
        buffer.extend_from_within(start..end);
        Ok(())
    }
}

pub trait Unifiable: Serialize + DeserializeOwned {}
pub trait UnifiableRef: Unifiable + Clone {}

impl<T: Serialize + DeserializeOwned> Unifiable for T {}
impl<T: Serialize + DeserializeOwned + Clone> UnifiableRef for T {}

pub trait Unifier {
    type D: UnifierData + ?Sized;
    type SerError: Debug + Display + Error;
    type DeError: Debug + Display + Error;

    /// Serializes data directly into an existing buffer and returns the start and end positions.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize(
        &self,
        buffer: &mut <Self::D as UnifierData>::Buffer,
        data: impl Unifiable,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>>;

    /// Serializes borrowed data directly into an existing buffer and returns the start and end positions.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize_ref<R: UnifiableRef>(
        &self,
        buffer: &mut <Self::D as UnifierData>::Buffer,
        data: &R,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        self.serialize(buffer, data.clone())
    }

    /// Deserializes data from the given buffer.
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    fn deserialize<T: Unifiable>(
        &self,
        data: &<Self::D as UnifierData>::Owned,
    ) -> Result<T, Self::DeError>;
}

impl Unifier for Configuration {
    type D = [u8];
    type SerError = EncodeError;
    type DeError = DecodeError;

    fn serialize(
        &self,
        buffer: &mut Vec<u8>,
        data: impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        let start = <[u8]>::len(buffer);
        let serialized = encode_to_vec(data, Self::default())?;
        <[u8]>::extend(buffer, &serialized).map_err(BufferOverflowOr::overflow)?;
        Ok((start, <[u8]>::len(buffer)))
    }

    fn deserialize<T: DeserializeOwned>(&self, data: &Vec<u8>) -> Result<T, Self::DeError> {
        Ok(decode_from_slice(data, Self::default())?.0)
    }
}
