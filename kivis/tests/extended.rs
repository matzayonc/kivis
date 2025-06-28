use kivis::{Database, MemoryStorage, Record, Recordable};
use serde::{Deserialize, Serialize};

// Test 1: Default behavior (first field as key) - existing test
#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[table(1)]
struct UserRecord {
    id: u64,
    data: Vec<u8>,
}

// Test 2: Specified field as key
#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[table(2)]
struct ProductRecord {
    name: String,
    #[key]
    sku: String,
    price: u32,
}

// Test 3: Composite key (multiple fields)
#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[table(3)]
struct OrderRecord {
    #[key]
    user_id: u64,
    #[key]
    order_date: String,
    items: Vec<String>,
    total: u64,
}

#[test]
fn test_default_key() {
    let user = UserRecord {
        id: 1,
        data: vec![1, 2, 3, 4],
    };

    let mut store = Database::new(MemoryStorage::new());

    let user_key = store.insert(user.clone()).unwrap();
    assert_eq!(user_key, UserRecordKey(1));

    assert_eq!(store.get(&user_key).unwrap(), Some(user.clone()));
    store.remove::<UserRecord>(&user_key).unwrap();
    assert_eq!(store.get::<UserRecord>(&user_key).unwrap(), None);
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

    let key1 = product1.key().unwrap();
    let key2 = product2.key().unwrap();

    // Keys should be equal because SKU is the same
    assert_eq!(key1, key2);
    assert_eq!(key1, ProductRecordKey("WID-001".to_string()));

    let mut store = Database::new(MemoryStorage::new());

    store.insert(product1.clone()).unwrap();
    assert_eq!(store.get(&key1).unwrap(), Some(product1.clone()));

    // Insert product2 with same key should overwrite
    store.insert(product2.clone()).unwrap();
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

    let key1 = order1.key().unwrap();
    let key2 = order2.key().unwrap();
    let key3 = order3.key().unwrap();

    assert_eq!(key1, OrderRecordKey(123, "2024-01-01".to_string()));

    // All keys should be different
    assert_ne!(key1, key2);
    assert_ne!(key1, key3);
    assert_ne!(key2, key3);

    let mut store = Database::new(MemoryStorage::new());

    // Insert all orders
    store.insert(order1.clone()).unwrap();
    store.insert(order2.clone()).unwrap();
    store.insert(order3.clone()).unwrap();

    // Verify all can be retrieved with their respective keys
    assert_eq!(store.get(&key1).unwrap(), Some(order1));
    assert_eq!(store.get(&key2).unwrap(), Some(order2));
    assert_eq!(store.get(&key3).unwrap(), Some(order3));

    // Remove one and verify others remain
    store.remove::<OrderRecord>(&key1).unwrap();
    assert_eq!(store.get::<OrderRecord>(&key1).unwrap(), None);
    assert_eq!(store.get::<OrderRecord>(&key2).unwrap().is_some(), true);
    assert_eq!(store.get::<OrderRecord>(&key3).unwrap().is_some(), true);
}
