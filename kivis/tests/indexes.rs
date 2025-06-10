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
fn test_user_record() {
    let user = UserRecord {
        id: 1,
        email: "user@example.com".to_string(),
        age: 25,
        name: "John Doe".to_string(),
    };
    let key = user.key();

    let mut store = BTreeMap::<Vec<u8>, Vec<u8>>::new();

    Store::insert(&mut store, user.clone()).unwrap();
    assert_eq!(Store::get(&store, &key).unwrap(), Some(user.clone()));
    Store::<UserRecord>::remove(&mut store, &key).unwrap();
    assert_eq!(Store::<UserRecord>::get(&store, &key).unwrap(), None);

    let index_key = UserRecordEmailIndexKey("user@example.com".to_string());
    Store::get(&store, &index_key);
}
