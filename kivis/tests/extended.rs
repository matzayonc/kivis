#![allow(clippy::unwrap_used)]
use kivis::{manifest, Database, DeriveKey, MemoryStorage, Record};
use serde::{Deserialize, Serialize};

// Test 1: Default behavior (auto-incrementing key, not part of the struct) - existing test
#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct UserRecord {
    data: Vec<u8>,
}

// Test 2: Specified field as key
#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct ProductRecord {
    name: String,
    #[key]
    sku: String,
    price: u32,
}

// Test 3: Composite key (multiple fields)
#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct OrderRecord {
    #[key]
    user_id: u64,
    #[key]
    order_date: String,
    items: Vec<String>,
    total: u64,
}

manifest![Manifest: UserRecord, OrderRecord, ProductRecord];

#[test]
fn test_default_key() {
    let user = UserRecord {
        data: vec![1, 2, 3, 4],
    };

    let mut store: Database<_, Manifest> = Database::new(MemoryStorage::new()).unwrap();

    let user_key = store.put(&user).unwrap();
    assert_eq!(user_key, UserRecordKey(1));

    assert_eq!(store.get(&user_key).unwrap(), Some(user.clone()));
    store.remove(&user_key).unwrap();
    assert_eq!(store.get(&user_key).unwrap(), None);
}

#[test]
fn test_specified_key() {
    let product1 = ProductRecord {
        name: "Widget".to_string(),
        sku: "WID-001".to_string(),
        price: 999,
    };

    let product2 = ProductRecord {
        name: "Updated Widget".to_string(),
        sku: "WID-001".to_string(), // Same SKU
        price: 1099,
    };

    let key1 = ProductRecord::key(&product1);
    let key2 = ProductRecord::key(&product2);

    // Keys should be equal because SKU is the same
    assert_eq!(key1, key2);
    assert_eq!(key1, ProductRecordKey("WID-001".to_string()));

    let mut store: Database<_, Manifest> = Database::new(MemoryStorage::new()).unwrap();

    store.insert(&product1).unwrap();
    assert_eq!(store.get(&key1).unwrap(), Some(product1.clone()));

    // Insert product2 with same key should overwrite
    store.insert(&product2).unwrap();
    assert_eq!(store.get(&key1).unwrap(), Some(product2));
}

#[test]
fn test_composite_key() {
    let order1 = OrderRecord {
        user_id: 123,
        order_date: "2024-01-01".to_string(),
        items: vec!["item1".to_string(), "item2".to_string()],
        total: 5000,
    };

    let order2 = OrderRecord {
        user_id: 123,
        order_date: "2024-01-02".to_string(), // Different date
        items: vec!["item3".to_string()],
        total: 2500,
    };

    let order3 = OrderRecord {
        user_id: 456, // Different user
        order_date: "2024-01-01".to_string(),
        items: vec!["item4".to_string()],
        total: 3000,
    };

    let key1 = OrderRecord::key(&order1);
    let key2 = OrderRecord::key(&order2);
    let key3 = OrderRecord::key(&order3);

    assert_eq!(key1, OrderRecordKey(123, "2024-01-01".to_string()));

    // All keys should be different
    assert_ne!(key1, key2);
    assert_ne!(key1, key3);
    assert_ne!(key2, key3);

    let mut store: Database<_, Manifest> = Database::new(MemoryStorage::new()).unwrap();

    // Insert all orders
    store.insert(&order1).unwrap();
    store.insert(&order2).unwrap();
    store.insert(&order3).unwrap();

    // Verify all can be retrieved with their respective keys
    assert_eq!(store.get(&key1).unwrap(), Some(order1));
    assert_eq!(store.get(&key2).unwrap(), Some(order2));
    assert_eq!(store.get(&key3).unwrap(), Some(order3));

    // Remove one and verify others remain
    store.remove(&key1).unwrap();
    assert_eq!(store.get(&key1).unwrap(), None);
    assert!(store.get(&key2).unwrap().is_some());
    assert!(store.get(&key3).unwrap().is_some());
}
