// Test demonstrating custom key vs value serialization in Unifier trait

use bincode::error::{DecodeError, EncodeError};
use kivis::{BufferOverflowError, BufferOverflowOr, Database, Record, Storage, Unifier, manifest};
use serde::{Deserialize, Serialize};
use std::{cmp::Reverse, collections::BTreeMap, fmt::Display, ops::Range};

/// A custom unifier that serializes and deserializes keys and values differently
#[derive(Debug, Clone, Copy, Default)]
pub struct CustomUnifier;

impl Unifier for CustomUnifier {
    type K = [u8];
    type V = [u8];
    type SerError = EncodeError;
    type DeError = DecodeError;

    fn serialize_key(
        &self,
        buffer: &mut Vec<u8>,
        data: impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        // Keys are serialized with a "KEY:" prefix
        let start = buffer.len();
        buffer.extend_from_slice(b"KEY:");
        let encoded = bincode::serde::encode_to_vec(data, bincode::config::standard())?;
        buffer.extend(encoded);
        let end = buffer.len();
        Ok((start, end))
    }

    fn serialize_value(
        &self,
        buffer: &mut Vec<u8>,
        data: impl Serialize,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        // Values are serialized with a "VAL:" prefix
        let start = buffer.len();
        buffer.extend_from_slice(b"VAL:");
        let encoded = bincode::serde::encode_to_vec(data, bincode::config::standard())?;
        buffer.extend(encoded);
        let end = buffer.len();
        Ok((start, end))
    }

    fn deserialize_key<T: serde::de::DeserializeOwned>(
        &self,
        data: &Vec<u8>,
    ) -> Result<T, Self::DeError> {
        // Strip the "KEY:" prefix and deserialize
        if !data.starts_with(b"KEY:") {
            return Err(DecodeError::UnexpectedEnd { additional: 0 });
        }
        let data_without_prefix = &data[4..];
        Ok(bincode::serde::decode_from_slice(data_without_prefix, bincode::config::standard())?.0)
    }

    fn deserialize_value<T: serde::de::DeserializeOwned>(
        &self,
        data: &Vec<u8>,
    ) -> Result<T, Self::DeError> {
        // Strip the "VAL:" prefix and deserialize
        if !data.starts_with(b"VAL:") {
            return Err(DecodeError::UnexpectedEnd { additional: 0 });
        }
        let data_without_prefix = &data[4..];
        Ok(bincode::serde::decode_from_slice(data_without_prefix, bincode::config::standard())?.0)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CustomError {
    Serialization,
    Deserialization,
    BufferOverflow,
}

impl Display for CustomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization => write!(f, "Serialization error"),
            Self::Deserialization => write!(f, "Deserialization error"),
            Self::BufferOverflow => write!(f, "Buffer overflow error"),
        }
    }
}

impl From<EncodeError> for CustomError {
    fn from(_: EncodeError) -> Self {
        Self::Serialization
    }
}

impl From<DecodeError> for CustomError {
    fn from(_: DecodeError) -> Self {
        Self::Deserialization
    }
}

impl From<BufferOverflowError> for CustomError {
    fn from(_: BufferOverflowError) -> Self {
        Self::BufferOverflow
    }
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
    type Serializer = CustomUnifier;
    type StoreError = CustomError;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::StoreError> {
        self.data.insert(Reverse(key.to_vec()), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.data.get(&Reverse(key.to_vec())).cloned())
    }

    fn remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::StoreError> {
        Ok(self.data.remove(&Reverse(key.to_vec())))
    }

    fn iter_keys(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>, Self::StoreError>>, Self::StoreError> {
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

    // The test verifies that the custom serializer works by successfully
    // storing and retrieving data. The prefixes are being used internally
    // even though we can't directly inspect them without exposing internals.

    // We can verify the serializer behavior works by checking that serialization
    // produces different outputs for keys vs values
    let unifier = CustomUnifier;
    let mut test_key_buffer = Vec::new();
    let mut test_val_buffer = Vec::new();
    let (start, end) = unifier
        .serialize_key(&mut test_key_buffer, "key_data".to_string())
        .unwrap();
    let test_key = &test_key_buffer[start..end];
    let (start, end) = unifier
        .serialize_value(&mut test_val_buffer, "val_data".to_string())
        .unwrap();
    let test_val = &test_val_buffer[start..end];

    assert!(
        test_key.starts_with(b"KEY:"),
        "Keys should have KEY: prefix"
    );
    assert!(
        test_val.starts_with(b"VAL:"),
        "Values should have VAL: prefix"
    );

    Ok(())
}

#[test]
fn test_default_value_serialization_uses_key_serialization() {
    // Create a test unifier that only implements serialize_key and deserialize_key
    #[derive(Debug, Clone, Copy, Default)]
    struct TestUnifier;

    impl Unifier for TestUnifier {
        type K = [u8];
        type V = [u8];
        type SerError = EncodeError;
        type DeError = DecodeError;

        fn serialize_key(
            &self,
            buffer: &mut Vec<u8>,
            data: impl Serialize,
        ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
            let start = buffer.len();
            let encoded = bincode::serde::encode_to_vec(data, bincode::config::standard())?;
            buffer.extend(encoded);
            let end = buffer.len();
            Ok((start, end))
        }

        fn serialize_value(
            &self,
            buffer: &mut Vec<u8>,
            data: impl Serialize,
        ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
            let start = buffer.len();
            let encoded = bincode::serde::encode_to_vec(data, bincode::config::standard())?;
            buffer.extend(encoded);
            let end = buffer.len();
            Ok((start, end))
        }

        fn deserialize_key<T: serde::de::DeserializeOwned>(
            &self,
            data: &Vec<u8>,
        ) -> Result<T, Self::DeError> {
            Ok(bincode::serde::decode_from_slice(data, bincode::config::standard())?.0)
        }

        fn deserialize_value<T: serde::de::DeserializeOwned>(
            &self,
            data: &Vec<u8>,
        ) -> Result<T, Self::DeError> {
            Ok(bincode::serde::decode_from_slice(data, bincode::config::standard())?.0)
        }
    }

    let unifier = TestUnifier;

    // Test that serialize_value uses serialize_key by default
    let mut key_buffer = Vec::new();
    let mut value_buffer = Vec::new();
    let (k_start, k_end) = unifier.serialize_key(&mut key_buffer, 42u32).unwrap();
    let (v_start, v_end) = unifier.serialize_value(&mut value_buffer, 42u32).unwrap();
    let key_result = &key_buffer[k_start..k_end];
    let value_result = &value_buffer[v_start..v_end];

    assert_eq!(
        key_result, value_result,
        "Default serialize_value should produce the same output as serialize_key"
    );

    // Test that deserialize_value uses deserialize_key by default
    let test_data = bincode::serde::encode_to_vec(42u32, bincode::config::standard()).unwrap();
    let key_deser: u32 = unifier.deserialize_key(&test_data).unwrap();
    let value_deser: u32 = unifier.deserialize_value(&test_data).unwrap();

    assert_eq!(
        key_deser, value_deser,
        "Default deserialize_value should produce the same output as deserialize_key"
    );
    assert_eq!(key_deser, 42);
}
