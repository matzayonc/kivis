use std::collections::BTreeMap;

use kivis::{Record, Recordable, Store};
use serde::{Deserialize, Serialize};

#[derive(Record, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct UserRecord {
    id: u64,
    data: Vec<u8>,
}

#[test]
fn test_record() {
    let user = UserRecord {
        id: 1,
        data: vec![1, 2, 3, 4],
    };
    let key = user.key();

    let mut store = BTreeMap::<Vec<u8>, Vec<u8>>::new();

    Store::insert(&mut store, user.clone()).unwrap();
    assert_eq!(Store::get(&store, &key).unwrap(), Some(user.clone()));
    Store::<UserRecord>::remove(&mut store, &key).unwrap();
    assert_eq!(Store::<UserRecord>::get(&store, &key).unwrap(), None);
}
