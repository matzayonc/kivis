#![allow(clippy::unwrap_used)]
use kivis::{manifest, Database, LexicographicString, MemoryStorage, Record};

// Define a record type for a Pet.
#[derive(Record, Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Pet {
    #[index]
    name: LexicographicString,
    color: Color,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Color {
    Brown,
    Black,
}

manifest![Manifest: Pet];

#[test]
fn test_index_after_remove() {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default()).unwrap();

    // Prepare 2 records with the same index value, name "Al".
    let names = [
        "Al", "Al", // The target name
        "Ak", "Am", // Previous and next names
        "Ala", "A",     // Shorter and longer names
        "Alice", // Alice for tradition
    ];
    for name in names {
        store
            .put(&Pet {
                name: LexicographicString::from(name),
                color: Color::Brown,
            })
            .expect("Put should succeed.");
    }

    let index_query = &PetNameIndex("Al".into());
    let als = store
        .iter_by_index_exact(index_query)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Verify created records.
    assert_eq!(als.len(), 2);
    let [al_1_key, al_2_key] = als.as_slice() else {
        panic!("Expected two results.");
    };
    let al_1 = store.get(al_1_key).unwrap().unwrap();
    let al_2 = store.get(al_2_key).unwrap().unwrap();
    assert_eq!(al_1.name, "Al");
    assert_eq!(al_2.name, "Al");

    // Remove one of the records.
    store.remove(al_1_key).unwrap();

    // Verify that index value was removed, and only one record remains.
    let als_after_removal = store
        .iter_by_index_exact(index_query)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(als_after_removal.len(), 1);
    assert_eq!(als_after_removal[0], *al_2_key);
}
