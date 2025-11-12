use kivis::{manifest, Record};
use serde::{Deserialize, Serialize};

// Test autoincrement tuple struct
#[derive(Record, Debug, Clone, Serialize, Deserialize)]
struct AutoIncrementTuple(String, u32);

// Test tuple struct with key field
#[derive(Record, Debug, Clone, Serialize, Deserialize)]
struct TupleWithKey(#[key] u64, String, u32);

// Test tuple struct with index field
#[derive(Record, Debug, Clone, Serialize, Deserialize)]
struct TupleWithIndex(String, #[index] u32);

// Test tuple struct with key and index
#[derive(Record, Debug, Clone, Serialize, Deserialize)]
struct TupleWithKeyAndIndex(#[key] u64, #[index] String);

manifest![TestManifest: AutoIncrementTuple, TupleWithKey, TupleWithIndex, TupleWithKeyAndIndex];

#[test]
fn test_autoincrement_tuple() {
    let auto = AutoIncrementTuple("test".to_string(), 42);
    assert_eq!(auto.0, "test");
    assert_eq!(auto.1, 42);
}

#[test]
fn test_tuple_with_key() {
    let with_key = TupleWithKey(1, "test".to_string(), 42);
    assert_eq!(with_key.0, 1);
    assert_eq!(with_key.1, "test");
    assert_eq!(with_key.2, 42);
}

#[test]
fn test_tuple_with_index() {
    let with_index = TupleWithIndex("test".to_string(), 42);
    assert_eq!(with_index.0, "test");
    assert_eq!(with_index.1, 42);
}

#[test]
fn test_tuple_with_key_and_index() {
    let with_both = TupleWithKeyAndIndex(1, "test".to_string());
    assert_eq!(with_both.0, 1);
    assert_eq!(with_both.1, "test");
}
