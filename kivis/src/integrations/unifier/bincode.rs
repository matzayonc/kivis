//! Implementations of the `Unifier` trait for various serialization formats.

#[cfg(all(feature = "alloc", not(feature = "std")))]
use alloc::vec::Vec;

use bincode::{
    config::{BigEndian, Configuration, Fixint, LittleEndian, NoLimit, Varint},
    error::{DecodeError, EncodeError},
    serde::{decode_from_slice, encode_to_vec},
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{BufferOverflowOr, Unified, Unifier};

/// Order-preserving bincode configuration for **keys**.
///
/// Big-endian, fixed-width integers: the byte order of the encoded key matches the numeric order
/// of unsigned integer components, so range scans and autoincrement recovery behave correctly.
///
/// Caveats:
/// - Signed integers are two's complement, so negative values sort *after* positive ones.
/// - `String` / `Vec` are length-prefixed and therefore sort by length first;
///   use [`Lexicographic`](crate::Lexicographic) for string keys that need lexicographic order.
pub type OrderedKeyConfig = Configuration<BigEndian, Fixint, NoLimit>;

/// Returns the [`OrderedKeyConfig`] value.
#[must_use]
pub const fn ordered_key_config() -> OrderedKeyConfig {
    bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding()
}

macro_rules! impl_bincode_unifier {
    ($($e:ty, $i:ty, $l:ty);* $(;)?) => {
        $(
            #[cfg(any(feature = "std", feature = "alloc"))]
            impl Unifier for Configuration<$e, $i, $l> {
                type D = Vec<u8>;
                type SerError = EncodeError;
                type DeError = DecodeError;

                fn serialize(
                    &self,
                    buffer: &mut Vec<u8>,
                    data: &impl Serialize,
                ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
                    let start = buffer.len();
                    let serialized = encode_to_vec(data, *self)?;
                    buffer
                        .extend_from(&serialized)
                        .map_err(BufferOverflowOr::overflow)?;
                    Ok((start, buffer.len()))
                }

                fn deserialize<T: DeserializeOwned>(&self, data: &Vec<u8>) -> Result<T, Self::DeError> {
                    Ok(decode_from_slice(data, *self)?.0)
                }
            }
        )*
    };
}

// `Configuration` (the default: little-endian varint) is kept as the value unifier.
// `OrderedKeyConfig` (big-endian fixint) is the key unifier.
impl_bincode_unifier! {
    LittleEndian, Varint, NoLimit;
    BigEndian, Fixint, NoLimit;
    LittleEndian, Fixint, NoLimit;
    BigEndian, Varint, NoLimit;
}
