use core::fmt;
use core::ops::{Deref, DerefMut};

#[cfg(not(feature = "std"))]
use alloc::{string::String, string::ToString, vec::Vec};

use serde::{de::Visitor, ser::SerializeTuple, Serialize};

#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LexicographicString(String);

impl LexicographicString {
    /// Creates a new `LexicographicString` from a `String`.
    #[must_use]
    pub fn new(s: String) -> Self {
        LexicographicString(s)
    }
}

impl PartialEq<str> for LexicographicString {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for LexicographicString {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl AsRef<str> for LexicographicString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for LexicographicString {
    fn from(s: &str) -> Self {
        LexicographicString(s.to_string())
    }
}

impl From<String> for LexicographicString {
    fn from(s: String) -> Self {
        LexicographicString(s)
    }
}

impl Deref for LexicographicString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for LexicographicString {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Serialize for LexicographicString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_tuple(self.0.len())?;
        for byte in self.0.as_bytes() {
            s.serialize_element(byte)?;
        }
        s.serialize_element(&b'\0')?;
        s.end()
    }
}

impl<'de> serde::Deserialize<'de> for LexicographicString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_tuple(usize::MAX, LexicographicStringVisitor)
    }
}

struct LexicographicStringVisitor;

impl<'de> Visitor<'de> for LexicographicStringVisitor {
    type Value = LexicographicString;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a lexicographically ordered string")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut bytes = Vec::new();

        // Collect all bytes from the tuple sequence
        while let Some(byte) = seq.next_element::<u8>()? {
            if byte == 0 {
                // Found null terminator, stop collecting
                break;
            }
            bytes.push(byte);
        }

        // Convert bytes to string
        let s =
            core::str::from_utf8(&bytes).map_err(|_| serde::de::Error::custom("invalid UTF-8"))?;
        Ok(LexicographicString::from(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: bincode::config::Configuration = bincode::config::standard();

    fn is_less(
        a: &LexicographicString,
        b: &LexicographicString,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let a = bincode::serde::encode_to_vec(a, CONFIG)?;
        let b = bincode::serde::encode_to_vec(b, CONFIG)?;
        Ok(a < b)
    }

    #[test]
    fn test_lexicographic_string_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let original = LexicographicString::from("Hello, World!");
        let serialized = bincode::serde::encode_to_vec(&original, CONFIG)?;
        let (deserialized, _): (LexicographicString, _) =
            bincode::serde::decode_from_slice(&serialized, CONFIG)?;
        assert_eq!(original, deserialized);
        Ok(())
    }

    #[test]
    fn test_serialization_as_expected() -> Result<(), Box<dyn std::error::Error>> {
        let lex_str = LexicographicString::from("A");
        let serialized = bincode::serde::encode_to_vec(&lex_str, CONFIG)?;
        assert_eq!(serialized, [65u8, 0u8].to_vec()); // ASCII 'A' + null terminator
        Ok(())
    }

    #[test]
    fn test_order_of_same_length() -> Result<(), Box<dyn std::error::Error>> {
        let smaller = LexicographicString::from("Apples");
        let larger = LexicographicString::from("Banana");
        assert!(is_less(&smaller, &larger)?);
        Ok(())
    }

    #[test]
    fn test_order_of_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let smaller = LexicographicString::from("Cat");
        let larger = LexicographicString::from("Caterpillar");
        assert!(is_less(&smaller, &larger)?);
        Ok(())
    }

    #[test]
    fn test_order_of_different_length() -> Result<(), Box<dyn std::error::Error>> {
        let first = LexicographicString::from("Aa");
        let second = LexicographicString::from("B");
        assert!(is_less(&first, &second)?);
        Ok(())
    }

    #[test]
    fn test_serialization_in_structs() -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Serialize, serde::Deserialize, PartialEq, Debug)]
        struct TestStruct {
            before: u16,
            name: LexicographicString,
            value: u32,
        }

        let original = TestStruct {
            before: 42,
            name: LexicographicString::from("TestName"),
            value: 100,
        };

        let serialized = bincode::serde::encode_to_vec(&original, CONFIG)?;
        let (deserialized, _): (TestStruct, _) =
            bincode::serde::decode_from_slice(&serialized, CONFIG)?;
        assert_eq!(original, deserialized);
        Ok(())
    }
}
