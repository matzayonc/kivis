use core::fmt;
use core::ops::{Deref, DerefMut};
use std::marker::PhantomData;

#[cfg(all(feature = "alloc", not(feature = "std")))]
use alloc::{string::String, string::ToString, vec::Vec};

use serde::{Serialize, de::Visitor, ser::SerializeTuple};

#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lexicographic<S>(S);

impl<S> Lexicographic<S> {
    /// Creates a new `LexicographicString` from a `String`.
    #[must_use]
    pub fn new(s: S) -> Self {
        Lexicographic(s)
    }
}

impl<S> PartialEq<str> for Lexicographic<S>
where
    S: AsRef<str>,
{
    fn eq(&self, other: &str) -> bool {
        self.0.as_ref() == other
    }
}

impl<S> PartialEq<&str> for Lexicographic<S>
where
    S: AsRef<str>,
{
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == *other
    }
}

impl<S> AsRef<str> for Lexicographic<S>
where
    S: AsRef<str>,
{
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'a, S: From<&'a str>> From<&'a str> for Lexicographic<S> {
    fn from(s: &'a str) -> Self {
        Lexicographic(s.into())
    }
}

impl<S: From<String>> From<String> for Lexicographic<S> {
    fn from(s: String) -> Self {
        Lexicographic(s.into())
    }
}

impl<S> Deref for Lexicographic<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> DerefMut for Lexicographic<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S> Serialize for Lexicographic<S>
where
    S: AsRef<str>,
{
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: serde::Serializer,
    {
        let mut s = serializer.serialize_tuple(self.0.as_ref().len())?;
        for byte in self.0.as_ref().as_bytes() {
            s.serialize_element(byte)?;
        }
        s.serialize_element(&b'\0')?;
        s.end()
    }
}

impl<'de, S> serde::Deserialize<'de> for Lexicographic<S>
where
    S: for<'a> From<&'a str>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_tuple(usize::MAX, LexicographicStringVisitor::<S>(PhantomData))
    }
}

struct LexicographicStringVisitor<S>(PhantomData<S>);

impl<'de, S> Visitor<'de> for LexicographicStringVisitor<S>
where
    S: for<'a> From<&'a str>,
{
    type Value = Lexicographic<S>;

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
        Ok(Lexicographic::<S>(S::from(s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: bincode::config::Configuration = bincode::config::standard();

    fn is_less<S>(
        a: &Lexicographic<S>,
        b: &Lexicographic<S>,
    ) -> Result<bool, Box<dyn std::error::Error>>
    where
        S: AsRef<str>,
    {
        let a = bincode::serde::encode_to_vec(a, CONFIG)?;
        let b = bincode::serde::encode_to_vec(b, CONFIG)?;
        Ok(a < b)
    }

    #[test]
    fn test_lexicographic_string_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let original = Lexicographic::<String>::from("Hello, World!");
        let serialized = bincode::serde::encode_to_vec(&original, CONFIG)?;
        let (deserialized, _): (Lexicographic<String>, _) =
            bincode::serde::decode_from_slice(&serialized, CONFIG)?;
        assert_eq!(original, deserialized);
        Ok(())
    }

    #[test]
    fn test_serialization_as_expected() -> Result<(), Box<dyn std::error::Error>> {
        let lex_str = Lexicographic::<String>::from("A");
        let serialized = bincode::serde::encode_to_vec(&lex_str, CONFIG)?;
        assert_eq!(serialized, [65u8, 0u8].to_vec()); // ASCII 'A' + null terminator
        Ok(())
    }

    #[test]
    fn test_order_of_same_length() -> Result<(), Box<dyn std::error::Error>> {
        let smaller = Lexicographic::<String>::from("Apples");
        let larger = Lexicographic::<String>::from("Banana");
        assert!(is_less(&smaller, &larger)?);
        Ok(())
    }

    #[test]
    fn test_order_of_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let smaller = Lexicographic::<String>::from("Cat");
        let larger = Lexicographic::<String>::from("Caterpillar");
        assert!(is_less(&smaller, &larger)?);
        Ok(())
    }

    #[test]
    fn test_order_of_different_length() -> Result<(), Box<dyn std::error::Error>> {
        let first = Lexicographic::<String>::from("Aa");
        let second = Lexicographic::<String>::from("B");
        assert!(is_less(&first, &second)?);
        Ok(())
    }

    #[test]
    fn test_serialization_in_structs() -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Serialize, serde::Deserialize, PartialEq, Debug)]
        struct TestStruct {
            before: u16,
            name: Lexicographic<String>,
            value: u32,
        }

        let original = TestStruct {
            before: 42,
            name: Lexicographic::<String>::from("TestName"),
            value: 100,
        };

        let serialized = bincode::serde::encode_to_vec(&original, CONFIG)?;
        let (deserialized, _): (TestStruct, _) =
            bincode::serde::decode_from_slice(&serialized, CONFIG)?;
        assert_eq!(original, deserialized);
        Ok(())
    }
}
