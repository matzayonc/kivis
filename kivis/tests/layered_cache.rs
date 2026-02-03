use kivis::{Record, manifest};
use serde::{Deserialize, Serialize};

#[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct CacheTestRecord {
    name: String,
    data: Vec<u8>,
}

manifest![Manifest: CacheTestRecord];

#[test]
#[ignore] // Cache is deprecated at this stage.
fn test_layered_cache_architecture() -> anyhow::Result<()> {
    // let fallback_storage = MemoryStorage::new();

    // // Test data
    // let record = CacheTestRecord {
    //     name: "test_record".to_string(),
    //     data: vec![1, 2, 3, 4, 5],
    // };

    // let mut fallback_database = Database::<_, Manifest>::new(fallback_storage.clone())?;
    // let key = fallback_database.put(record.clone())?;
    // let fallback_storage = fallback_database.dissolve();

    // let mut database = Database::<_, Manifest>::new(MemoryStorage::new())?;
    // database.set_fallback(Box::new(fallback_storage));

    // // Verify record can be retrieved
    // let retrieved = database.get(&key)?;
    // assert_eq!(retrieved, Some(record.clone()));
    Ok(())
}
