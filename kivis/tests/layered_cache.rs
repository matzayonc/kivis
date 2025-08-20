use kivis::{manifest, Database, MemoryStorage, Record};
use serde::{Deserialize, Serialize};

#[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct CacheTestRecord {
    name: String,
    data: Vec<u8>,
}

manifest![Manifest: CacheTestRecord];

#[test]
fn test_layered_cache_architecture() {
    let fallback_storage = MemoryStorage::new();

    // Test data
    let record = CacheTestRecord {
        name: "test_record".to_string(),
        data: vec![1, 2, 3, 4, 5],
    };

    let mut fallback_database = Database::new(fallback_storage.clone());
    let key = fallback_database.put(record.clone()).unwrap();
    let fallback_storage = fallback_database.dissolve();

    let mut database = Database::new(MemoryStorage::new());
    database.set_fallback(Box::new(fallback_storage));

    // Verify record can be retrieved
    let retrieved = database.get(&key).unwrap();
    assert_eq!(retrieved, Some(record.clone()));
}
