use kivis::{Database, MemoryStorage, Record, manifest};

// A record named `Item` used to break manifest! with "ambiguous associated item".
#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct Item {
    name: String,
}
#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct Key {
    #[key]
    id: u64,
    v: u8,
}
manifest![M: Item, Key];

#[test]
fn record_named_item_works() {
    let mut db = Database::<MemoryStorage, M>::new(MemoryStorage::new()).unwrap();
    let k = db.put(Item { name: "x".into() }).unwrap();
    assert_eq!(db.get(&k).unwrap().unwrap().name, "x");
}

/// T15: container attributes on the record must not be copied onto the generated key struct.
/// `deny_unknown_fields` is meaningless for a newtype key and used to be forwarded verbatim.
#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Tagged {
    #[key]
    id: u64,
    long_name: String,
}

manifest![Tags: Tagged];

#[test]
fn record_container_attrs_are_not_forwarded_to_key() {
    let mut db = Database::<MemoryStorage, Tags>::new(MemoryStorage::new()).unwrap();
    let key = db
        .insert(Tagged {
            id: 7,
            long_name: "x".into(),
        })
        .unwrap();
    assert_eq!(db.get(&key).unwrap().unwrap().long_name, "x");
}
