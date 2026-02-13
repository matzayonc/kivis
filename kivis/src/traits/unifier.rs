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
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn next(&mut self) -> Result<(), BufferOverflowError>;

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

    fn next(&mut self) -> Result<(), BufferOverflowError> {
        for i in (0..self.len()).rev() {
            // Add one if possible
            if self[i] < 255 {
                self[i] += 1;
                return Ok(());
            }
            // Otherwise, set to zero and carry over
            self[i] = 0;
        }

        // If all bytes were 255, we need to add a new byte
        self.push(0);
        Ok(())
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

/// Implementation of `UnifierData` for `heapless::Vec<u8, N>`.
///
/// This enables the use of fixed-capacity, stack-allocated vectors as key/value buffers
/// in embedded environments where heap allocation is undesirable or unavailable.
///
/// This implementation is only available when the `heapless` feature is enabled.
///
/// # Example
///
/// ```ignore
/// use heapless::Vec;
/// use kivis::UnifierData;
///
/// let mut buffer = Vec::<u8, 256>::new();
/// UnifierData::extend(&mut buffer, &[1, 2, 3]).unwrap();
/// assert_eq!(buffer.as_slice(), &[1, 2, 3]);
/// ```
#[cfg(feature = "heapless")]
impl<const N: usize> UnifierData for heapless::Vec<u8, N> {
    type View<'a> = &'a [u8];

    fn from_view(data: Self::View<'_>) -> Self {
        let mut vec = heapless::Vec::new();
        vec.extend_from_slice(data).ok();
        vec
    }

    fn next(&mut self) -> Result<(), BufferOverflowError> {
        for i in (0..self.len()).rev() {
            // Add one if possible
            if self[i] < 255 {
                self[i] += 1;
                return Ok(());
            }
            // Otherwise, set to zero and carry over
            self[i] = 0;
        }

        // If all bytes were 255, try to add a new byte (may fail if at capacity)
        self.push(0).map_err(|_| BufferOverflowError)
    }

    fn extend(&mut self, part: Self::View<'_>) -> Result<(), BufferOverflowError> {
        self.extend_from_slice(part)
            .map_err(|()| BufferOverflowError)
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn extract_range(&self, start: usize, end: usize) -> Self::View<'_> {
        &self[start..end]
    }

    fn duplicate_within(&mut self, start: usize, end: usize) -> Result<(), BufferOverflowError> {
        let part_len = end - start;

        // Check if we have enough capacity
        if self.len() + part_len > N {
            return Err(BufferOverflowError);
        }

        // Copy the range to a temporary buffer
        let mut temp = heapless::Vec::<u8, N>::new();
        temp.extend_from_slice(&self[start..end])
            .map_err(|()| BufferOverflowError)?;

        // Extend self with the temporary buffer
        self.extend_from_slice(&temp)
            .map_err(|()| BufferOverflowError)
    }
}

#[cfg(feature = "std")]
impl UnifierData for String {
    type View<'a> = &'a str;

    fn from_view(data: Self::View<'_>) -> Self {
        data.to_string()
    }

    fn next(&mut self) -> Result<(), BufferOverflowError> {
        let mut bytes = self.as_bytes().to_vec();

        let next_valid_string = loop {
            bytes.next()?;

            if let Ok(parsed_back) = String::from_utf8(bytes.clone()) {
                // If the bytes are not valid UTF-8, increment and try again.
                break parsed_back;
            }
        };

        *self = next_valid_string;
        Ok(())
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

#[cfg(test)]
#[cfg(feature = "heapless")]
mod tests {
    use super::*;

    #[test]
    fn test_heapless_unifier_data() -> anyhow::Result<()> {
        let mut vec = heapless::Vec::<u8, 256>::new();

        // Test extend
        assert_eq!(UnifierData::extend(&mut vec, &[1, 2, 3]), Ok(()));
        assert_eq!(vec.as_slice(), &[1, 2, 3]);

        // Test len
        assert_eq!(UnifierData::len(&vec), 3);

        // Test extract_range
        assert_eq!(UnifierData::extract_range(&vec, 1, 3), &[2, 3]);

        // Test next
        UnifierData::next(&mut vec)?;
        assert_eq!(vec.as_slice(), &[1, 2, 4]);

        // Test from_view
        let vec2 = <heapless::Vec<u8, 256> as UnifierData>::from_view(&[5, 6, 7]);
        assert_eq!(vec2.as_slice(), &[5, 6, 7]);

        // Test duplicate
        let vec3 = <heapless::Vec<u8, 256> as UnifierData>::duplicate(&[8, 9])?;
        assert_eq!(vec3.as_slice(), &[8, 9]);

        // Test duplicate_within
        let mut vec4 = heapless::Vec::<u8, 256>::new();
        vec4.extend_from_slice(&[1, 2, 3, 4])
            .map_err(|()| BufferOverflowError)?;
        UnifierData::duplicate_within(&mut vec4, 1, 3)?;
        assert_eq!(vec4.as_slice(), &[1, 2, 3, 4, 2, 3]);

        // Test overflow on extend
        let mut vec5 = heapless::Vec::<u8, 4>::new();
        vec5.extend_from_slice(&[1, 2, 3, 4])
            .map_err(|()| BufferOverflowError)?;
        assert!(UnifierData::extend(&mut vec5, &[5]).is_err());

        // Test overflow on next
        let mut vec6 = heapless::Vec::<u8, 2>::new();
        vec6.extend_from_slice(&[255, 255])
            .map_err(|()| BufferOverflowError)?;
        assert!(UnifierData::next(&mut vec6).is_err());

        Ok(())
    }
}
