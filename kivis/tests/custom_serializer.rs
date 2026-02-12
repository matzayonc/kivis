// Test demonstrating custom key vs value serialization in Unifier trait

use bincode::error::{DecodeError, EncodeError};
use kivis::{
    BufferOverflowError, BufferOverflowOr, Database, Record, Repository, Storage, Unifier, manifest,
};
use serde::{Deserialize, Serialize};
use std::{cmp::Reverse, collections::BTreeMap, ops::Range};
use thiserror::Error;

/// Trait for providing a constant prefix
pub trait Prefix {
    fn prefix() -> &'static [u8];
}

/// A generic unifier that adds a constant prefix to serialized data
#[derive(Debug, Clone, Copy, Default)]
pub struct PrefixUnifier<P: Prefix>(std::marker::PhantomData<P>);

impl<P: Prefix> Unifier for PrefixUnifier<P> {
    type D = Vec<u8>;
    type SerError = EncodeError;
    type DeError = DecodeError;

    fn serialize(
        &self,
        buffer: &mut Self::D,
        data: impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        let start = buffer.len();
        buffer.extend_from_slice(P::prefix());
        let encoded = bincode::serde::encode_to_vec(data, bincode::config::standard())?;
        buffer.extend(encoded);
        let end = buffer.len();
        Ok((start, end))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(
        &self,
        data: &Self::D,
    ) -> Result<T, Self::DeError> {
        // Strip the prefix and deserialize
        let prefix = P::prefix();
        if !data.starts_with(prefix) {
            return Err(DecodeError::UnexpectedEnd { additional: 0 });
        }
        let data_without_prefix = &data[prefix.len()..];
        Ok(bincode::serde::decode_from_slice(data_without_prefix, bincode::config::standard())?.0)
    }
}

/// Prefix for keys
#[derive(Debug, Clone, Copy, Default)]
pub struct KeyPrefix;
impl Prefix for KeyPrefix {
    fn prefix() -> &'static [u8] {
        b"KEY:"
    }
}

/// Prefix for values
#[derive(Debug, Clone, Copy, Default)]
pub struct ValuePrefix;
impl Prefix for ValuePrefix {
    fn prefix() -> &'static [u8] {
        b"VAL:"
    }
}

/// Type alias for key unifier with "KEY:" prefix
pub type CustomKeyUnifier = PrefixUnifier<KeyPrefix>;

/// Type alias for value unifier with "VAL:" prefix
pub type CustomValueUnifier = PrefixUnifier<ValuePrefix>;

#[derive(Debug, Error)]
pub enum CustomError {
    #[error("Serialization error")]
    Serialization(#[from] EncodeError),
    #[error("Deserialization error")]
    Deserialization(#[from] DecodeError),
    #[error("Buffer overflow error")]
    BufferOverflow(#[from] BufferOverflowError),
}

#[derive(Debug, Default)]
pub struct CustomStorage {
    data: BTreeMap<Reverse<Vec<u8>>, Vec<u8>>,
}

impl CustomStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn raw_data(&self) -> &BTreeMap<Reverse<Vec<u8>>, Vec<u8>> {
        &self.data
    }
}

impl Storage for CustomStorage {
    type Repo = Self;
    type KeyUnifier = CustomKeyUnifier;
    type ValueUnifier = CustomValueUnifier;

    fn repository(&self) -> &Self::Repo {
        self
    }

    fn repository_mut(&mut self) -> &mut Self::Repo {
        self
    }
}

impl Repository for CustomStorage {
    type K = Vec<u8>;
    type V = Vec<u8>;
    type Error = CustomError;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.data.insert(Reverse(key.to_vec()), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Self::V>, Self::Error> {
        Ok(self.data.get(&Reverse(key.to_vec())).cloned())
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<Self::V>, Self::Error> {
        Ok(self.data.remove(&Reverse(key.to_vec())))
    }

    fn iter_keys(
        &self,
        range: Range<Self::K>,
    ) -> Result<impl Iterator<Item = Result<Self::K, Self::Error>>, Self::Error> {
        let reverse_range = Reverse(range.end)..Reverse(range.start);
        let iter = self.data.range(reverse_range);
        Ok(iter.map(|(k, _v)| Ok(k.0.clone())))
    }
}

#[derive(Record, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestRecord {
    name: String,
    value: u32,
}

manifest![TestManifest: TestRecord];

#[test]
fn test_custom_key_value_serialization() -> anyhow::Result<()> {
    let storage = CustomStorage::new();
    let mut db = Database::<CustomStorage, TestManifest>::new(storage)?;

    let record = TestRecord {
        name: "test".to_string(),
        value: 42,
    };

    let key = db.put(record.clone())?;

    // Verify we can retrieve the record correctly
    let retrieved = db.get(&key)?;
    assert_eq!(retrieved, Some(record.clone()));

    // Now verify that keys and values use different prefixes
    let key_unifier = CustomKeyUnifier::default();
    let value_unifier = CustomValueUnifier::default();

    let mut key_buffer = Vec::new();
    let (start, end) = key_unifier
        .serialize(&mut key_buffer, "test_key".to_string())
        .unwrap();
    let key_data = &key_buffer[start..end];

    let mut value_buffer = Vec::new();
    let (start, end) = value_unifier
        .serialize(&mut value_buffer, "test_value".to_string())
        .unwrap();
    let value_data = &value_buffer[start..end];

    assert!(
        key_data.starts_with(b"KEY:"),
        "Keys should have KEY: prefix"
    );
    assert!(
        value_data.starts_with(b"VAL:"),
        "Values should have VAL: prefix"
    );

    // Verify they're different
    assert_ne!(
        key_data, value_data,
        "Key and value serialization should differ"
    );

    Ok(())
}

#[test]
fn test_unifier_consistency() {
    // Create a test unifier
    #[derive(Debug, Clone, Copy, Default)]
    struct TestUnifier;

    impl Unifier for TestUnifier {
        type D = Vec<u8>;
        type SerError = EncodeError;
        type DeError = DecodeError;

        fn serialize(
            &self,
            buffer: &mut Self::D,
            data: impl Serialize,
        ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
            let start = buffer.len();
            let encoded = bincode::serde::encode_to_vec(data, bincode::config::standard())?;
            buffer.extend(encoded);
            let end = buffer.len();
            Ok((start, end))
        }

        fn deserialize<T: serde::de::DeserializeOwned>(
            &self,
            data: &Self::D,
        ) -> Result<T, Self::DeError> {
            Ok(bincode::serde::decode_from_slice(data, bincode::config::standard())?.0)
        }
    }

    let unifier = TestUnifier;

    // Test serialization
    let mut buffer = Vec::new();
    let (start, end) = unifier.serialize(&mut buffer, 42u32).unwrap();
    let result = &buffer[start..end];

    // Test deserialization
    let test_data = bincode::serde::encode_to_vec(42u32, bincode::config::standard()).unwrap();
    let deser: u32 = unifier.deserialize(&test_data).unwrap();

    assert_eq!(deser, 42);
    assert_eq!(result, test_data.as_slice());
}
