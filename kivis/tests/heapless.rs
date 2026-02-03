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
    type Owned = Bytes;
    type Buffer = Bytes;
    type View<'a> = &'a [u8];

    fn next(buffer: &mut Self) {
        let mut owned = buffer.0.to_vec();
        <[u8] as UnifierData>::next(&mut owned);
        if owned.len() <= BUFFER_SIZE {
            buffer.0[..owned.len()].copy_from_slice(owned.as_slice());
        } else {
            panic!("Buffer overflow in Bytes UnifierData implementation");
        }
    }

    fn extend(buffer: &mut Self, part: &Self) -> Result<(), BufferOverflowError> {
        let current_len = buffer.1;
        let part_len = part.1;
        if current_len + part_len <= BUFFER_SIZE {
            buffer.0[current_len..current_len + part_len].copy_from_slice(&part.0[..part_len]);
            buffer.1 += part_len;
        } else {
            return Err(BufferOverflowError);
        }
        Ok(())
    }

    fn len(buffer: &Self) -> usize {
        buffer.1
    }

    fn extract_range(buffer: &Self, _start: usize, _end: usize) -> &[u8] {
        &buffer.0[..buffer.1]
    }

    fn duplicate_within(
        buffer: &mut Self,
        start: usize,
        end: usize,
    ) -> Result<(), BufferOverflowError> {
        let part_len = end - start;
        if buffer.1 + part_len > BUFFER_SIZE {
            return Err(BufferOverflowError);
        }
        buffer.0.copy_within(start..end, buffer.1);
        buffer.1 += part_len;
        Ok(())
    }
}
