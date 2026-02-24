use kivis::{CacheContainer, Record, manifest};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct CacheTestRecord {
    name: String,
    data: Vec<u8>,
}

struct TestCache<K, V>(HashMap<K, V>);

impl<K, V> Default for TestCache<K, V> {
    fn default() -> Self {
        TestCache(HashMap::new())
    }
}

impl<K: Eq + std::hash::Hash + Clone, V: Clone> CacheContainer<K, V> for TestCache<K, V> {
    fn set(&mut self, key: &K, value: &V) {
        self.0.insert(key.clone(), value.clone());
    }

    fn get(&mut self, key: &K) -> Option<V> {
        self.0.get(key).cloned()
    }

    fn expire(&mut self, key: &K) {
        self.0.remove(key);
    }
}

manifest![Manifest + TestCache: CacheTestRecord];

#[test]
#[ignore] // Cache is deprecated at this stage.
fn test_layered_cache_architecture() -> anyhow::Result<()> {
    let _cache = ManifestCache::default();

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
