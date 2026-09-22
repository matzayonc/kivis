//! Order-preserving, prefix-free, filesystem-safe key encoding.
//!
//! `kivis` builds storage keys by *concatenating* several independently serialized
//! components (table prelude, then the record key; for index entries the index value and
//! then the primary key) and later reads a main key back as a single `Wrap { prelude, key }`
//! struct. That only works if the encoding of a struct is exactly the concatenation of the
//! encodings of its fields and every scalar is self-delimiting. It also has to be
//! order-preserving when compared as plain strings, because range scans and autoincrement
//! recovery compare keys that way.
//!
//! The encoding produced here satisfies all of that while staying readable:
//!
//! | Rust type              | Encoding                                                     |
//! |------------------------|--------------------------------------------------------------|
//! | `u8`/`u16`/`u32`/`u64`/`u128` | zero-padded decimal of fixed width (3/5/10/20/39) + `.` |
//! | `i8`..`i128`           | sign bit flipped, then as the unsigned type of the same width |
//! | `bool`                 | `0.` / `1.`                                                  |
//! | `str`, `char`, bytes   | ASCII alphanumerics verbatim, every other byte as `_XX` (hex), then `.` |
//! | `Option<T>`            | `0.` for `None`, `1.` followed by `T` for `Some`              |
//! | structs, tuples, newtypes | fields concatenated, no framing                           |
//! | sequences              | length as `u64`, then elements                               |
//! | enums                  | variant index as `u32`, then the payload                     |
//! | `()`                   | nothing                                                      |
//!
//! Every scalar ends with the terminator `.`, which sorts below every character that can
//! appear inside a component (`0-9`, `A-Z`, `_`, `a-z`), so `bob.` never prefixes `bobby.`
//! and `[prefix, next(prefix))` scans match exactly one value. The output alphabet is
//! `[A-Za-z0-9._]`, so keys are used directly as file names without further escaping.
//!
//! Floats and maps are not supported as key components.

use core::fmt::{self, Display, Write};

use kivis::{BufferOverflowOr, Unifier};
use serde::{
    Serialize,
    de::{self, DeserializeSeed, EnumAccess, IntoDeserializer, SeqAccess, VariantAccess, Visitor},
    ser::{self, Impossible},
};

const TERMINATOR: char = '.';
const ESCAPE: char = '_';

/// Error produced while encoding or decoding a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyError(String);

impl Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "key encoding error: {}", self.0)
    }
}

impl std::error::Error for KeyError {}

impl ser::Error for KeyError {
    fn custom<T: Display>(msg: T) -> Self {
        KeyError(msg.to_string())
    }
}

impl de::Error for KeyError {
    fn custom<T: Display>(msg: T) -> Self {
        KeyError(msg.to_string())
    }
}

/// [`Unifier`] for keys: encodes with the order-preserving format described in the module docs.
#[derive(Debug, Clone, Copy, Default)]
pub struct KeyCodec;

impl Unifier for KeyCodec {
    type D = String;
    type SerError = KeyError;
    type DeError = KeyError;

    fn serialize(
        &self,
        buffer: &mut String,
        data: &impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<KeyError>> {
        let start = buffer.len();
        data.serialize(&mut KeySerializer { out: buffer })?;
        Ok((start, buffer.len()))
    }

    fn deserialize<T: de::DeserializeOwned>(&self, data: &String) -> Result<T, KeyError> {
        let mut de = KeyDeserializer { input: data };
        T::deserialize(&mut de)
    }
}

// ---------------------------------------------------------------------------
// Serializer
// ---------------------------------------------------------------------------

struct KeySerializer<'a> {
    out: &'a mut String,
}

impl KeySerializer<'_> {
    fn unsigned(&mut self, value: u128, width: usize) -> Result<(), KeyError> {
        write!(self.out, "{value:0width$}{TERMINATOR}").map_err(ser::Error::custom)
    }

    fn bytes(&mut self, bytes: &[u8]) -> Result<(), KeyError> {
        for &b in bytes {
            if b.is_ascii_alphanumeric() {
                self.out.push(b as char);
            } else {
                write!(self.out, "{ESCAPE}{b:02X}").map_err(ser::Error::custom)?;
            }
        }
        self.out.push(TERMINATOR);
        Ok(())
    }
}

macro_rules! serialize_unsigned {
    ($($fn:ident: $ty:ty => $width:expr),* $(,)?) => {
        $(
            fn $fn(self, v: $ty) -> Result<(), KeyError> {
                self.unsigned(u128::from(v), $width)
            }
        )*
    };
}

macro_rules! serialize_signed {
    ($($fn:ident: $ty:ty => $unsigned:ty, $width:expr),* $(,)?) => {
        $(
            #[allow(clippy::cast_sign_loss)]
            fn $fn(self, v: $ty) -> Result<(), KeyError> {
                // Flipping the sign bit maps the signed range onto the unsigned range
                // monotonically (offset binary), so ordering is preserved.
                let flipped = (v as $unsigned) ^ (1 << (<$unsigned>::BITS - 1));
                self.unsigned(u128::from(flipped), $width)
            }
        )*
    };
}

impl ser::Serializer for &mut KeySerializer<'_> {
    type Ok = ();
    type Error = KeyError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Impossible<(), KeyError>;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    serialize_unsigned! {
        serialize_u8: u8 => 3,
        serialize_u16: u16 => 5,
        serialize_u32: u32 => 10,
        serialize_u64: u64 => 20,
        serialize_u128: u128 => 39,
    }

    serialize_signed! {
        serialize_i8: i8 => u8, 3,
        serialize_i16: i16 => u16, 5,
        serialize_i32: i32 => u32, 10,
        serialize_i64: i64 => u64, 20,
        serialize_i128: i128 => u128, 39,
    }

    fn serialize_bool(self, v: bool) -> Result<(), KeyError> {
        self.out.push(if v { '1' } else { '0' });
        self.out.push(TERMINATOR);
        Ok(())
    }

    fn serialize_f32(self, _: f32) -> Result<(), KeyError> {
        Err(ser::Error::custom("floats are not supported in keys"))
    }

    fn serialize_f64(self, _: f64) -> Result<(), KeyError> {
        Err(ser::Error::custom("floats are not supported in keys"))
    }

    fn serialize_char(self, v: char) -> Result<(), KeyError> {
        let mut buf = [0u8; 4];
        self.bytes(v.encode_utf8(&mut buf).as_bytes())
    }

    fn serialize_str(self, v: &str) -> Result<(), KeyError> {
        self.bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), KeyError> {
        self.bytes(v)
    }

    fn serialize_none(self) -> Result<(), KeyError> {
        self.out.push('0');
        self.out.push(TERMINATOR);
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<(), KeyError> {
        self.out.push('1');
        self.out.push(TERMINATOR);
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), KeyError> {
        Ok(())
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<(), KeyError> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
    ) -> Result<(), KeyError> {
        self.serialize_u32(variant_index)
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<(), KeyError> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
        value: &T,
    ) -> Result<(), KeyError> {
        self.unsigned(u128::from(variant_index), 10)?;
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self, KeyError> {
        let len = len.ok_or_else(|| ser::Error::custom("sequences in keys need a known length"))?;
        let len = u64::try_from(len).map_err(ser::Error::custom)?;
        self.unsigned(u128::from(len), 20)?;
        Ok(self)
    }

    fn serialize_tuple(self, _: usize) -> Result<Self, KeyError> {
        Ok(self)
    }

    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, KeyError> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self, KeyError> {
        self.unsigned(u128::from(variant_index), 10)?;
        Ok(self)
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, KeyError> {
        Err(ser::Error::custom("maps are not supported in keys"))
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, KeyError> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self, KeyError> {
        self.unsigned(u128::from(variant_index), 10)?;
        Ok(self)
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl ser::SerializeSeq for &mut KeySerializer<'_> {
    type Ok = ();
    type Error = KeyError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), KeyError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), KeyError> {
        Ok(())
    }
}

impl ser::SerializeTuple for &mut KeySerializer<'_> {
    type Ok = ();
    type Error = KeyError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), KeyError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), KeyError> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for &mut KeySerializer<'_> {
    type Ok = ();
    type Error = KeyError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), KeyError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), KeyError> {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for &mut KeySerializer<'_> {
    type Ok = ();
    type Error = KeyError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), KeyError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), KeyError> {
        Ok(())
    }
}

impl ser::SerializeStruct for &mut KeySerializer<'_> {
    type Ok = ();
    type Error = KeyError;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _: &'static str,
        value: &T,
    ) -> Result<(), KeyError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), KeyError> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for &mut KeySerializer<'_> {
    type Ok = ();
    type Error = KeyError;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _: &'static str,
        value: &T,
    ) -> Result<(), KeyError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), KeyError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Deserializer
// ---------------------------------------------------------------------------

struct KeyDeserializer<'de> {
    input: &'de str,
}

impl<'de> KeyDeserializer<'de> {
    fn take(&mut self, n: usize) -> Result<&'de str, KeyError> {
        if self.input.len() < n || !self.input.is_char_boundary(n) {
            return Err(de::Error::custom(format!(
                "unexpected end of key, wanted {n} more bytes in {:?}",
                self.input
            )));
        }
        let (head, tail) = self.input.split_at(n);
        self.input = tail;
        Ok(head)
    }

    fn terminator(&mut self) -> Result<(), KeyError> {
        match self.input.strip_prefix(TERMINATOR) {
            Some(rest) => {
                self.input = rest;
                Ok(())
            }
            None => Err(de::Error::custom(format!(
                "expected terminator {TERMINATOR:?} in {:?}",
                self.input
            ))),
        }
    }

    fn unsigned(&mut self, width: usize) -> Result<u128, KeyError> {
        let digits = self.take(width)?;
        let value = digits
            .parse::<u128>()
            .map_err(|e| de::Error::custom(format!("invalid integer {digits:?} in key: {e}")))?;
        self.terminator()?;
        Ok(value)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, KeyError> {
        let mut out = Vec::new();
        loop {
            let Some(c) = self.input.chars().next() else {
                return Err(de::Error::custom("unterminated string in key"));
            };
            self.input = &self.input[c.len_utf8()..];
            match c {
                TERMINATOR => return Ok(out),
                ESCAPE => {
                    let hex = self.take(2)?;
                    let byte = u8::from_str_radix(hex, 16).map_err(|e| {
                        de::Error::custom(format!("invalid escape {ESCAPE}{hex} in key: {e}"))
                    })?;
                    out.push(byte);
                }
                c if c.is_ascii_alphanumeric() => out.push(c as u8),
                c => {
                    return Err(de::Error::custom(format!(
                        "unexpected character {c:?} in key string"
                    )));
                }
            }
        }
    }

    fn string(&mut self) -> Result<String, KeyError> {
        String::from_utf8(self.bytes()?)
            .map_err(|e| de::Error::custom(format!("key string is not valid UTF-8: {e}")))
    }
}

macro_rules! deserialize_unsigned {
    ($($fn:ident: $ty:ty => $width:expr, $visit:ident),* $(,)?) => {
        $(
            fn $fn<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
                let value = <$ty>::try_from(self.unsigned($width)?)
                    .map_err(|e| de::Error::custom(format!("integer out of range: {e}")))?;
                visitor.$visit(value)
            }
        )*
    };
}

macro_rules! deserialize_signed {
    ($($fn:ident: $ty:ty => $unsigned:ty, $width:expr, $visit:ident),* $(,)?) => {
        $(
            #[allow(clippy::cast_possible_wrap)]
            fn $fn<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
                let raw = <$unsigned>::try_from(self.unsigned($width)?)
                    .map_err(|e| de::Error::custom(format!("integer out of range: {e}")))?;
                let value = (raw ^ (1 << (<$unsigned>::BITS - 1))) as $ty;
                visitor.$visit(value)
            }
        )*
    };
}

impl<'de> de::Deserializer<'de> for &mut KeyDeserializer<'de> {
    type Error = KeyError;

    deserialize_unsigned! {
        deserialize_u8: u8 => 3, visit_u8,
        deserialize_u16: u16 => 5, visit_u16,
        deserialize_u32: u32 => 10, visit_u32,
        deserialize_u64: u64 => 20, visit_u64,
        deserialize_u128: u128 => 39, visit_u128,
    }

    deserialize_signed! {
        deserialize_i8: i8 => u8, 3, visit_i8,
        deserialize_i16: i16 => u16, 5, visit_i16,
        deserialize_i32: i32 => u32, 10, visit_i32,
        deserialize_i64: i64 => u64, 20, visit_i64,
        deserialize_i128: i128 => u128, 39, visit_i128,
    }

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, KeyError> {
        Err(de::Error::custom("key format is not self-describing"))
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
        let flag = self.take(1)?;
        self.terminator()?;
        match flag {
            "0" => visitor.visit_bool(false),
            "1" => visitor.visit_bool(true),
            other => Err(de::Error::custom(format!("invalid bool {other:?} in key"))),
        }
    }

    fn deserialize_f32<V: Visitor<'de>>(self, _: V) -> Result<V::Value, KeyError> {
        Err(de::Error::custom("floats are not supported in keys"))
    }

    fn deserialize_f64<V: Visitor<'de>>(self, _: V) -> Result<V::Value, KeyError> {
        Err(de::Error::custom("floats are not supported in keys"))
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
        let s = self.string()?;
        let mut chars = s.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) => visitor.visit_char(c),
            _ => Err(de::Error::custom(format!(
                "expected a single char, got {s:?}"
            ))),
        }
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
        visitor.visit_string(self.string()?)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
        visitor.visit_string(self.string()?)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
        visitor.visit_byte_buf(self.bytes()?)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
        visitor.visit_byte_buf(self.bytes()?)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
        let flag = self.take(1)?;
        self.terminator()?;
        match flag {
            "0" => visitor.visit_none(),
            "1" => visitor.visit_some(self),
            other => Err(de::Error::custom(format!(
                "invalid option tag {other:?} in key"
            ))),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, KeyError> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, KeyError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, KeyError> {
        let len = usize::try_from(self.unsigned(20)?)
            .map_err(|e| de::Error::custom(format!("sequence length out of range: {e}")))?;
        visitor.visit_seq(Counted {
            de: self,
            remaining: len,
        })
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, KeyError> {
        visitor.visit_seq(Counted {
            de: self,
            remaining: len,
        })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, KeyError> {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, _: V) -> Result<V::Value, KeyError> {
        Err(de::Error::custom("maps are not supported in keys"))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, KeyError> {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, KeyError> {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _: V) -> Result<V::Value, KeyError> {
        Err(de::Error::custom("identifiers are not supported in keys"))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, KeyError> {
        Err(de::Error::custom("key format is not self-describing"))
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

/// Sequence access that yields at most `remaining` elements and stops early at end of input.
///
/// Stopping at end of input matters for [`kivis::Lexicographic`], which deserializes as a
/// tuple of `usize::MAX` elements and relies on `None` to end the sequence.
struct Counted<'a, 'de> {
    de: &'a mut KeyDeserializer<'de>,
    remaining: usize,
}

impl<'de> SeqAccess<'de> for Counted<'_, 'de> {
    type Error = KeyError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, KeyError> {
        if self.remaining == 0 || self.de.input.is_empty() {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }
}

impl<'de> EnumAccess<'de> for &mut KeyDeserializer<'de> {
    type Error = KeyError;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self), KeyError> {
        let index = u32::try_from(self.unsigned(10)?)
            .map_err(|e| de::Error::custom(format!("variant index out of range: {e}")))?;
        let value = seed.deserialize(index.into_deserializer())?;
        Ok((value, self))
    }
}

impl<'de> VariantAccess<'de> for &mut KeyDeserializer<'de> {
    type Error = KeyError;

    fn unit_variant(self) -> Result<(), KeyError> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, KeyError> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, KeyError> {
        de::Deserializer::deserialize_tuple(self, len, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, KeyError> {
        de::Deserializer::deserialize_tuple(self, fields.len(), visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde::Deserialize;

    fn encode<T: Serialize>(value: &T) -> String {
        let mut buf = String::new();
        KeyCodec.serialize(&mut buf, value).expect("encode");
        buf
    }

    fn decode<T: de::DeserializeOwned>(s: &str) -> T {
        KeyCodec.deserialize(&s.to_string()).expect("decode")
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Prelude {
        scope: u8,
        subtable: u8,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Wrapped<K> {
        prelude: Prelude,
        key: K,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum Kind {
        A,
        B(u16),
        C { x: i8, y: String },
    }

    #[test]
    fn fixed_width_ints() {
        assert_eq!(encode(&7u8), "007.");
        assert_eq!(encode(&7u64), "00000000000000000007.");
        assert_eq!(encode(&-1i8), "127.");
        assert_eq!(encode(&0i8), "128.");
        assert_eq!(encode(&true), "1.");
    }

    #[test]
    fn strings_are_escaped_and_terminated() {
        assert_eq!(encode(&"bob"), "bob.");
        assert_eq!(encode(&"a b/c"), "a_20b_2Fc.");
        assert_eq!(encode(&"héllo"), "h_C3_A9llo.");
        assert_eq!(decode::<String>("h_C3_A9llo."), "héllo");
    }

    #[test]
    fn concatenation_equals_struct_encoding() {
        // This is what kivis does: serialize prelude, then key, into one buffer,
        // and later read it back as one struct.
        let mut buf = String::new();
        KeyCodec
            .serialize(
                &mut buf,
                &Prelude {
                    scope: 1,
                    subtable: 0,
                },
            )
            .expect("prelude");
        KeyCodec.serialize(&mut buf, &42u64).expect("key");
        let wrapped: Wrapped<u64> = decode(&buf);
        assert_eq!(
            wrapped,
            Wrapped {
                prelude: Prelude {
                    scope: 1,
                    subtable: 0
                },
                key: 42
            }
        );
    }

    #[test]
    fn enums_and_options_roundtrip() {
        for v in [
            Kind::A,
            Kind::B(9),
            Kind::C {
                x: -5,
                y: "z.z".into(),
            },
        ] {
            let decoded: Kind = decode(&encode(&v));
            assert_eq!(decoded, v);
        }
        assert_eq!(decode::<Option<u8>>(&encode(&None::<u8>)), None);
        assert_eq!(decode::<Option<u8>>(&encode(&Some(3u8))), Some(3));
        assert_eq!(decode::<Vec<u8>>(&encode(&vec![1u8, 2, 3])), vec![1, 2, 3]);
    }

    #[test]
    fn lexicographic_roundtrip() {
        let v = kivis::Lexicographic::<String>::from("Alice");
        let decoded: kivis::Lexicographic<String> = decode(&encode(&v));
        assert_eq!(decoded, v);
    }

    #[test]
    fn output_is_filename_safe() {
        let s = encode(&("a/b\\c:d*e?f\"g<h>i|j\n", 1u8));
        assert!(
            s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_'),
            "{s}"
        );
    }

    proptest! {
        #[test]
        fn string_roundtrip(s in "\\PC*") {
            let decoded: String = decode(&encode(&s));
            prop_assert_eq!(decoded, s);
        }

        #[test]
        fn string_prefix_free(a in "\\PC*", b in "\\PC*") {
            let (ea, eb) = (encode(&a), encode(&b));
            prop_assert!(a == b || !(ea.starts_with(&eb) || eb.starts_with(&ea)));
        }

        #[test]
        fn u64_order_preserved(a in any::<u64>(), b in any::<u64>()) {
            prop_assert_eq!(a.cmp(&b), encode(&a).cmp(&encode(&b)));
        }

        #[test]
        fn i64_order_preserved(a in any::<i64>(), b in any::<i64>()) {
            prop_assert_eq!(a.cmp(&b), encode(&a).cmp(&encode(&b)));
            let decoded: i64 = decode(&encode(&a));
            prop_assert_eq!(decoded, a);
        }

        #[test]
        fn alnum_string_order_preserved(a in "[a-zA-Z0-9]{0,8}", b in "[a-zA-Z0-9]{0,8}") {
            prop_assert_eq!(a.cmp(&b), encode(&a).cmp(&encode(&b)));
        }
    }
}
