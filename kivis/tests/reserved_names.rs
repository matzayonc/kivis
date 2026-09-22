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
