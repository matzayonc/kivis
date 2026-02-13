#![allow(dead_code)]

use kivis::{BufferOverflowError, UnifierData};

const BUFFER_SIZE: usize = 16;

#[derive(Clone, Default)]
struct Bytes([u8; BUFFER_SIZE], usize);

impl AsRef<Bytes> for Bytes {
    fn as_ref(&self) -> &Bytes {
        self
    }
}

impl From<&Bytes> for Bytes {
    fn from(slice: &Bytes) -> Self {
        Bytes(slice.0, slice.1)
    }
}

impl UnifierData for Bytes {
    type View<'a> = &'a [u8];

    fn from_view(data: &[u8]) -> Self {
        let mut bytes = Bytes::default();
        bytes.extend(data).unwrap();
        bytes
    }

    fn next(&mut self) -> Result<(), BufferOverflowError> {
        let mut owned = self.0[..self.1].to_vec();
        Vec::next(&mut owned)?;
        if owned.len() <= BUFFER_SIZE {
            self.0[..owned.len()].copy_from_slice(owned.as_slice());
            self.1 = owned.len();
            Ok(())
        } else {
            Err(BufferOverflowError)
        }
    }

    fn extend(&mut self, part: &[u8]) -> Result<(), BufferOverflowError> {
        let current_len = self.1;
        let part_len = part.len();
        if current_len + part_len <= BUFFER_SIZE {
            self.0[current_len..current_len + part_len].copy_from_slice(part);
            self.1 += part_len;
        } else {
            return Err(BufferOverflowError);
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.1
    }

    fn extract_range(&self, start: usize, end: usize) -> &[u8] {
        &self.0[start..end]
    }

    fn duplicate_within(&mut self, start: usize, end: usize) -> Result<(), BufferOverflowError> {
        let part_len = end - start;
        if self.1 + part_len > BUFFER_SIZE {
            return Err(BufferOverflowError);
        }
        self.0.copy_within(start..end, self.1);
        self.1 += part_len;
        Ok(())
    }
}
