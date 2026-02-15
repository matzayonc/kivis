//! Implementations of the `UnifierData` trait for various buffer types.

#[cfg(all(feature = "alloc", not(feature = "std")))]
use alloc::{string::String, vec::Vec};

use crate::{BufferOverflowError, UnifierData};

/// Implementation of `UnifierData` for `Vec<u8>`.
///
/// This is the default buffer type for standard environments where heap allocation is available.
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

    fn extend_from(&mut self, part: Self::View<'_>) -> Result<(), BufferOverflowError> {
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

/// Implementation of `UnifierData` for `String`.
///
/// This buffer type is used when working with string keys or values.
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

    fn extend_from(&mut self, part: Self::View<'_>) -> Result<(), BufferOverflowError> {
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
