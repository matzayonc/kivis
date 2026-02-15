use heapless::CapacityError;

use crate::{BufferOverflowError, UnifierData};

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
/// buffer.extend(&[1, 2, 3]).unwrap();
/// assert_eq!(buffer.as_slice(), &[1, 2, 3]);
/// ```
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

    fn extend_from(&mut self, part: Self::View<'_>) -> Result<(), BufferOverflowError> {
        self.extend_from_slice(part)
            .map_err(|_: CapacityError| BufferOverflowError)
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
            .map_err(|_: CapacityError| BufferOverflowError)?;

        // Extend self with the temporary buffer
        self.extend_from_slice(&temp)
            .map_err(|_: CapacityError| BufferOverflowError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heapless_unifier_data() -> anyhow::Result<()> {
        let mut vec = heapless::Vec::<u8, 256>::new();

        // Test extend
        assert_eq!(vec.extend_from(&[1, 2, 3]), Ok(()));
        assert_eq!(vec.as_slice(), &[1, 2, 3]);

        // Test len
        assert_eq!(vec.len(), 3);

        // Test extract_range
        assert_eq!(vec.extract_range(1, 3), &[2, 3]);

        // Test next
        vec.next()?;
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
            .map_err(|_: CapacityError| BufferOverflowError)?;
        UnifierData::duplicate_within(&mut vec4, 1, 3)?;
        assert_eq!(vec4.as_slice(), &[1, 2, 3, 4, 2, 3]);

        // Test overflow on extend
        let mut vec5 = heapless::Vec::<u8, 4>::new();
        vec5.extend_from_slice(&[1, 2, 3, 4])
            .map_err(|_: CapacityError| BufferOverflowError)?;
        assert!(vec5.extend_from(&[5]).is_err());

        // Test overflow on next
        let mut vec6 = heapless::Vec::<u8, 2>::new();
        vec6.extend_from_slice(&[255, 255])
            .map_err(|_: CapacityError| BufferOverflowError)?;
        assert!(vec6.next().is_err());

        Ok(())
    }
}
