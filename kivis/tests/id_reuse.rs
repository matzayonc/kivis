//! Autoincrement ids must never be handed out twice, even across reopen.
use kivis::{Database, MemoryStorage, Record, manifest};

#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct Doc {
    title: String,
}

manifest![Docs: Doc];

/// Deleting the highest record used to lower the recovered counter, so the next
/// `put` reissued that id and any stored `DocKey` pointing at the deleted record
/// silently resolved to the new one.
#[test]
fn deleted_trailing_id_is_not_reused() -> anyhow::Result<()> {
    let mut db = Database::<MemoryStorage, Docs>::new(MemoryStorage::new())?;
    let first = db.put(Doc { title: "a".into() })?;
    let last = db.put(Doc { title: "b".into() })?;

    db.remove(&last)?;

    let storage = db.dissolve();
    let mut db = Database::<MemoryStorage, Docs>::new(storage)?;
    let next = db.put(Doc { title: "c".into() })?;

    assert_ne!(next, last, "id of the deleted record was handed out again");
    assert_ne!(next, first);
    assert_eq!(db.get(&last)?, None, "deleted record came back");
    Ok(())
}

/// The counter survives a reopen even when every record is gone.
#[test]
fn counter_survives_emptying_the_table() -> anyhow::Result<()> {
    let mut db = Database::<MemoryStorage, Docs>::new(MemoryStorage::new())?;
    let mut issued = Vec::new();
    for i in 0..5 {
        issued.push(db.put(Doc {
            title: format!("d{i}"),
        })?);
    }
    for key in &issued {
        db.remove(key)?;
    }

    let storage = db.dissolve();
    let mut db = Database::<MemoryStorage, Docs>::new(storage)?;
    let next = db.put(Doc {
        title: "fresh".into(),
    })?;

    assert!(
        !issued.contains(&next),
        "reused an id from the emptied table: {next:?}"
    );
    Ok(())
}

/// Reopening without deletions still continues from the highest stored key.
#[test]
fn counter_continues_after_reopen() -> anyhow::Result<()> {
    let mut db = Database::<MemoryStorage, Docs>::new(MemoryStorage::new())?;
    let mut issued = Vec::new();
    for i in 0..300 {
        issued.push(db.put(Doc {
            title: format!("d{i}"),
        })?);
    }

    let storage = db.dissolve();
    let mut db = Database::<MemoryStorage, Docs>::new(storage)?;
    let next = db.put(Doc {
        title: "next".into(),
    })?;

    assert!(!issued.contains(&next));
    assert_eq!(db.get(&issued[0])?.map(|d| d.title), Some("d0".to_string()));
    Ok(())
}
