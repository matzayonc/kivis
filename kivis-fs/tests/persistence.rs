//! Regression tests for T02: reopening a directory, prefix-free index keys and
//! numeric ordering of keys stored as file names.

use kivis::{Database, Lexicographic, Record, manifest};
use kivis_fs::FileStore;
use tempfile::tempdir;

#[derive(Record, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Thing {
    name: String,
}

#[derive(Record, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Person {
    #[index]
    name: String,
    age: u32,
}

#[derive(Record, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Keyed {
    #[key]
    id: u64,
    #[index]
    group: u32,
}

#[derive(Record, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Tagged {
    #[index]
    tag: Lexicographic<String>,
    weight: i32,
}

manifest![Store: Thing, Person, Keyed, Tagged];

fn open(dir: &std::path::Path) -> anyhow::Result<Database<FileStore, Store>> {
    Ok(Database::new(FileStore::new(dir)?)?)
}

fn reopen_after(n: u64) -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut db = open(dir.path())?;
    let mut keys = Vec::new();
    for i in 0..n {
        keys.push(db.put(Thing {
            name: format!("thing{i}"),
        })?);
    }
    drop(db);

    // Reopening must succeed and must continue the autoincrement sequence.
    let mut db = open(dir.path())?;
    assert_eq!(db.last_id::<ThingKey>()?, ThingKey(n));
    let next = db.put(Thing {
        name: "next".into(),
    })?;
    assert_eq!(next, ThingKey(n + 1));
    assert!(!keys.contains(&next));

    // Everything written before is still readable.
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            db.get(key)?,
            Some(Thing {
                name: format!("thing{i}")
            })
        );
    }
    Ok(())
}

#[test]
fn reopen_after_two_records() -> anyhow::Result<()> {
    reopen_after(2)
}

#[test]
fn reopen_after_twelve_records() -> anyhow::Result<()> {
    reopen_after(12)
}

#[test]
fn index_exact_is_prefix_free() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut db = open(dir.path())?;
    let bob = db.put(Person {
        name: "bob".into(),
        age: 1,
    })?;
    db.put(Person {
        name: "bobby".into(),
        age: 2,
    })?;
    db.put(Person {
        name: "bob1".into(),
        age: 3,
    })?;

    let found = db
        .iter_by_index_exact(&PersonNameIndex("bob".into()))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(found, vec![bob]);

    // Three distinct index files must exist: "bob1"+"3" must not collide with "bob"+"13".
    let index_files = std::fs::read_dir(dir.path())?
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().starts_with("001.002."))
        .count();
    assert_eq!(index_files, 3);
    Ok(())
}

#[test]
fn numeric_keys_order_numerically() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut db = open(dir.path())?;
    for id in [100u64, 11, 10, 9, 2, 1] {
        db.insert(Keyed { id, group: 1 })?;
    }

    let keys = db
        .iter_keys(KeyedKey(2)..KeyedKey(11))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(keys, vec![KeyedKey(2), KeyedKey(9), KeyedKey(10)]);

    let all = db
        .iter_all_keys::<KeyedKey>()?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        all,
        [1u64, 2, 9, 10, 11, 100].map(KeyedKey).to_vec(),
        "iter_all_keys must be ascending"
    );

    let by_group = db
        .iter_by_index(KeyedGroupIndex(0)..KeyedGroupIndex(2))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(by_group.len(), 6);
    Ok(())
}

#[test]
fn lexicographic_index_still_works() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut db = open(dir.path())?;
    let apple = db.put(Tagged {
        tag: "apple".into(),
        weight: -3,
    })?;
    let banana = db.put(Tagged {
        tag: "banana".into(),
        weight: 7,
    })?;

    let found = db
        .iter_by_index_exact(&TaggedTagIndex("apple".into()))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(found, vec![apple.clone()]);

    let ranged = db
        .iter_by_index(TaggedTagIndex("a".into())..TaggedTagIndex("c".into()))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(ranged, vec![apple.clone(), banana.clone()]);

    // Reopen and read back a record with a negative integer field.
    drop(db);
    let mut db = open(dir.path())?;
    assert_eq!(db.get(&apple)?.map(|t| t.weight), Some(-3));
    assert_eq!(db.get(&banana)?.map(|t| t.weight), Some(7));
    Ok(())
}

#[test]
fn file_names_are_readable_and_safe() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut db = open(dir.path())?;
    db.put(Person {
        name: "a b/c".into(),
        age: 1,
    })?;

    let names: Vec<String> = std::fs::read_dir(dir.path())?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.contains(&"001.000.00000000000000000001.dat".to_string()),
        "{names:?}"
    );
    assert!(
        names.contains(&"001.002.a_20b_2Fc.00000000000000000001.dat".to_string()),
        "{names:?}"
    );
    for name in &names {
        assert!(
            name.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_'),
            "unsafe filename {name}"
        );
    }
    Ok(())
}
