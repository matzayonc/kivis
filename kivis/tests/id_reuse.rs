//! Autoincrement id recovery across reopen.
use kivis::{Database, MemoryStorage, Record, manifest};

#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct Doc {
    title: String,
}

manifest![Docs: Doc];

/// Documents a known limitation (see `LIMITATIONS.md`): the counter is recovered from the
/// highest stored key, so the id of a deleted trailing record is issued again after a reopen.
#[test]
fn deleted_trailing_id_is_reused_after_reopen() -> anyhow::Result<()> {
    let mut db = Database::<MemoryStorage, Docs>::new(MemoryStorage::new())?;
    db.put(Doc { title: "a".into() })?;
    let last = db.put(Doc { title: "b".into() })?;

    db.remove(&last)?;

    let storage = db.dissolve();
    let mut db = Database::<MemoryStorage, Docs>::new(storage)?;
    let next = db.put(Doc { title: "c".into() })?;

    assert_eq!(next, last);
    assert_eq!(db.get(&last)?.map(|d| d.title), Some("c".to_string()));
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
