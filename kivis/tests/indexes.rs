use std::collections::BTreeMap;

use kivis::{Record, Recordable, Store};
use serde::{Deserialize, Serialize};

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct UserRecord {
    #[key]
    id: u64,
    #[index]
    email: String,
    #[index] 
    age: u32,
    name: String,
}

#[test]
fn test_index_generation() {
    let user = UserRecord {
        id: 1,
        email: "user@example.com".to_string(),
        age: 25,
        name: "John Doe".to_string(),
    };
    
    // Basic functionality should still work
    let key = user.key();
    assert_eq!(key, UserRecordKey(1));

    let mut store = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    Store::insert(&mut store, user.clone()).unwrap();
    assert_eq!(Store::get(&store, &key).unwrap(), Some(user));
}

#[test]
fn test_index_structures() {
    // Test that the index structures are generated and can be used
    let mut email_index = UserRecordEmailIndex::new();
    let mut age_index = UserRecordAgeIndex::new();
    
    let user1 = UserRecord {
        id: 1,
        email: "user1@example.com".to_string(),
        age: 25,
        name: "John".to_string(),
    };
    
    let user2 = UserRecord {
        id: 2,
        email: "user2@example.com".to_string(),
        age: 25, // Same age as user1
        name: "Jane".to_string(),
    };
    
    let key1 = user1.key();
    let key2 = user2.key();
    
    // Insert into indexes
    email_index.insert(&user1.email, key1.clone());
    email_index.insert(&user2.email, key2.clone());
    
    age_index.insert(&user1.age, key1.clone());
    age_index.insert(&user2.age, key2.clone());
    
    // Test email index (unique values)
    assert_eq!(email_index.get_keys(&"user1@example.com".to_string()), vec![key1.clone()]);
    assert_eq!(email_index.get_keys(&"user2@example.com".to_string()), vec![key2.clone()]);
    
    // Test age index (non-unique values)
    let age_25_keys = age_index.get_keys(&25);
    assert_eq!(age_25_keys.len(), 2);
    assert!(age_25_keys.contains(&key1));
    assert!(age_25_keys.contains(&key2));
    
    // Test removal
    age_index.remove(&25, &key1);
    let age_25_keys_after_removal = age_index.get_keys(&25);
    assert_eq!(age_25_keys_after_removal, vec![key2]);
}
