use kivis::{CacheContainer, Database, DeriveKey, MemoryStorage, Record, manifest};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct CacheTestRecord {
    name: String,
    data: Vec<u8>,
}

struct TestCache<K, V> {
    store: HashMap<K, V>,
    hits: usize,
    misses: usize,
}

impl<K, V> Default for TestCache<K, V> {
    fn default() -> Self {
        TestCache {
            store: HashMap::new(),
            hits: 0,
            misses: 0,
        }
    }
}

impl<K: Eq + std::hash::Hash + Clone, V: Clone> CacheContainer<K, V> for TestCache<K, V> {
    fn set(&mut self, key: &K, value: &V) {
        self.store.insert(key.clone(), value.clone());
    }

    fn get(&mut self, key: &K) -> Option<V> {
        let result = self.store.get(key).cloned();
        if result.is_some() {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
        result
    }

    fn expire(&mut self, key: &K) {
        self.store.remove(key);
    }
}

manifest![Manifest + TestCache: CacheTestRecord];

// A record whose key is derived from its own content (name field),
// so we can insert different data under the same key.
#[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[derived_key(String)]
struct NamedRecord {
    name: String,
    value: u32,
}

impl DeriveKey for NamedRecord {
    type Key = NamedRecordKey;
    fn key(c: &NamedRecord) -> Self::Key {
        NamedRecordKey(c.name.clone())
    }
}

manifest![NamedManifest + TestCache: NamedRecord];

#[test]
fn test_layered_cache_architecture() -> anyhow::Result<()> {
    let mut db = Database::<MemoryStorage, Manifest, ManifestCache>::new(MemoryStorage::new())?;

    let record = CacheTestRecord {
        name: "test_record".to_string(),
        data: vec![1, 2, 3, 4, 5],
    };

    // Insert the record
    let key = db.put(record.clone())?;

    // First get: cache miss → populated
    let retrieved = db.get(&key)?;
    assert_eq!(retrieved, Some(record.clone()));
    assert_eq!(db.cache().cache_test_record.misses, 1);
    assert_eq!(db.cache().cache_test_record.hits, 0);

    // Second get: cache hit
    let retrieved = db.get(&key)?;
    assert_eq!(retrieved, Some(record.clone()));
    assert_eq!(db.cache().cache_test_record.hits, 1);
    assert_eq!(db.cache().cache_test_record.misses, 1);

    // Remove: cache expired (remove calls get internally, so hits go to 2)
    db.remove::<CacheTestRecordKey, CacheTestRecord>(&key)?;

    // Third get: cache miss again (record gone)
    let retrieved = db.get(&key)?;
    assert_eq!(retrieved, None);
    assert_eq!(db.cache().cache_test_record.misses, 2);
    assert_eq!(db.cache().cache_test_record.hits, 2);

    Ok(())
}

#[test]
fn test_insert_expires_cache() -> anyhow::Result<()> {
    let mut db =
        Database::<MemoryStorage, NamedManifest, NamedManifestCache>::new(MemoryStorage::new())?;

    let v1 = NamedRecord {
        name: "alice".to_string(),
        value: 1,
    };
    let v2 = NamedRecord {
        name: "alice".to_string(),
        value: 2,
    };

    // Insert v1, get it → cache miss then hit
    let key = db.insert::<NamedRecordKey, NamedRecord>(v1.clone())?;
    let retrieved = db.get(&key)?;
    assert_eq!(retrieved, Some(v1));
    assert_eq!(db.cache().named_record.misses, 1);
    assert_eq!(db.cache().named_record.hits, 0);

    let retrieved = db.get(&key)?;
    assert_eq!(db.cache().named_record.hits, 1);
    assert_eq!(db.cache().named_record.misses, 1);
    drop(retrieved);

    // Re-insert under the same key with v2 → must expire the cached entry
    db.insert::<NamedRecordKey, NamedRecord>(v2.clone())?;

    // Next get must be a miss and return the fresh value
    let retrieved = db.get(&key)?;
    assert_eq!(db.cache().named_record.misses, 2);
    assert_eq!(db.cache().named_record.hits, 1);
    assert_eq!(retrieved, Some(v2));

    Ok(())
}

// Two record types in one manifest, so the generated `CacheExpiry` dispatch has more
// than one arm to choose between.
#[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[derived_key(String)]
struct Alpha {
    name: String,
    value: u32,
}

impl DeriveKey for Alpha {
    type Key = AlphaKey;
    fn key(c: &Alpha) -> Self::Key {
        AlphaKey(c.name.clone())
    }
}

#[derive(Record, Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[derived_key(String)]
struct Beta {
    name: String,
    value: u32,
}

impl DeriveKey for Beta {
    type Key = BetaKey;
    fn key(c: &Beta) -> Self::Key {
        BetaKey(c.name.clone())
    }
}

manifest![MultiManifest + TestCache: Alpha, Beta];

fn named(name: &str, value: u32) -> NamedRecord {
    NamedRecord {
        name: name.to_string(),
        value,
    }
}

#[test]
fn test_commit_expires_cache() -> anyhow::Result<()> {
    let mut db =
        Database::<MemoryStorage, NamedManifest, NamedManifestCache>::new(MemoryStorage::new())?;

    let key = db.insert::<NamedRecordKey, NamedRecord>(named("alice", 1))?;

    // Populate the cache.
    assert_eq!(db.get(&key)?, Some(named("alice", 1)));
    assert_eq!(db.cache().named_record.misses, 1);
    assert_eq!(db.cache().named_record.hits, 0);

    // Overwrite the same key through a transaction.
    let mut tx = db.create_transaction();
    tx.insert::<NamedRecordKey, NamedRecord>(named("alice", 2))?;
    db.commit(tx)?;

    // The committed value must win, and the read must go to storage.
    assert_eq!(db.get(&key)?, Some(named("alice", 2)));
    assert_eq!(db.cache().named_record.misses, 2);
    assert_eq!(db.cache().named_record.hits, 0);

    Ok(())
}

#[test]
fn test_commit_expires_cache_on_remove() -> anyhow::Result<()> {
    let mut db =
        Database::<MemoryStorage, NamedManifest, NamedManifestCache>::new(MemoryStorage::new())?;

    let key = db.insert::<NamedRecordKey, NamedRecord>(named("bob", 7))?;

    // Populate the cache.
    let record = db.get(&key)?.expect("bob exists");
    assert_eq!(db.cache().named_record.misses, 1);

    let mut tx = db.create_transaction();
    tx.remove::<NamedRecord>(&key, &record)?;
    db.commit(tx)?;

    assert_eq!(db.get(&key)?, None);
    assert_eq!(db.cache().named_record.misses, 2);
    assert_eq!(db.cache().named_record.hits, 0);

    Ok(())
}

#[test]
fn test_rollback_does_not_expire_cache() -> anyhow::Result<()> {
    let mut db =
        Database::<MemoryStorage, NamedManifest, NamedManifestCache>::new(MemoryStorage::new())?;

    let key = db.insert::<NamedRecordKey, NamedRecord>(named("carol", 1))?;

    // Populate the cache.
    assert_eq!(db.get(&key)?, Some(named("carol", 1)));
    assert_eq!(db.cache().named_record.misses, 1);

    // A transaction that is thrown away must leave the cache warm.
    let mut tx = db.create_transaction();
    tx.insert::<NamedRecordKey, NamedRecord>(named("carol", 2))?;
    tx.rollback();

    assert_eq!(db.get(&key)?, Some(named("carol", 1)));
    assert_eq!(db.cache().named_record.hits, 1);
    assert_eq!(db.cache().named_record.misses, 1);

    Ok(())
}

#[test]
fn test_commit_expires_only_the_written_record_type() -> anyhow::Result<()> {
    let mut db =
        Database::<MemoryStorage, MultiManifest, MultiManifestCache>::new(MemoryStorage::new())?;

    let alpha_key = db.insert::<AlphaKey, Alpha>(Alpha {
        name: "x".to_string(),
        value: 1,
    })?;
    let beta = Beta {
        name: "x".to_string(),
        value: 1,
    };
    let beta_key = db.insert::<BetaKey, Beta>(beta.clone())?;

    // Populate both containers.
    db.get(&alpha_key)?;
    db.get(&beta_key)?;
    assert_eq!(db.cache().alpha.misses, 1);
    assert_eq!(db.cache().beta.misses, 1);

    // A transaction touching only Alpha.
    let mut tx = db.create_transaction();
    tx.insert::<AlphaKey, Alpha>(Alpha {
        name: "x".to_string(),
        value: 2,
    })?;
    db.commit(tx)?;

    // Beta's entry is untouched — still a hit.
    assert_eq!(db.get(&beta_key)?, Some(beta));
    assert_eq!(db.cache().beta.hits, 1);
    assert_eq!(db.cache().beta.misses, 1);

    // Alpha's entry was expired — a miss returning the committed value.
    assert_eq!(
        db.get(&alpha_key)?,
        Some(Alpha {
            name: "x".to_string(),
            value: 2,
        })
    );
    assert_eq!(db.cache().alpha.misses, 2);
    assert_eq!(db.cache().alpha.hits, 0);

    Ok(())
}
