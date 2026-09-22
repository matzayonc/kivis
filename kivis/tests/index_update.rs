//! Overwriting a record must not leave the previous version's secondary-index entries behind.

use kivis::{Database, MemoryStorage, Record, manifest};

#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
struct Acct {
    #[key]
    id: u32,
    #[index]
    status: u8,
}

#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
struct Person {
    #[key]
    id: u32,
    #[index]
    city: u8,
    #[index]
    age: u8,
}

#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
struct Event {
    #[index]
    kind: u8,
}

manifest![M: Acct, Person, Event];

type Db = Database<MemoryStorage, M>;

fn by_status(db: &Db, status: u8) -> anyhow::Result<Vec<AcctKey>> {
    Ok(db
        .iter_by_index_exact(&AcctStatusIndex(status))?
        .collect::<Result<_, _>>()?)
}

#[test]
fn update_via_insert_removes_stale_index_entry() -> anyhow::Result<()> {
    let mut db = Db::new(MemoryStorage::new())?;
    db.insert(Acct { id: 1, status: 0 })?;
    db.insert(Acct { id: 1, status: 1 })?;

    assert_eq!(by_status(&db, 0)?, vec![]);
    assert_eq!(by_status(&db, 1)?, vec![AcctKey(1)]);
    assert_eq!(db.get(&AcctKey(1))?, Some(Acct { id: 1, status: 1 }));
    Ok(())
}

#[test]
fn update_via_transaction_removes_stale_index_entry() -> anyhow::Result<()> {
    let mut db = Db::new(MemoryStorage::new())?;
    db.insert(Acct { id: 1, status: 0 })?;
    db.insert(Acct { id: 2, status: 0 })?;

    let mut tx = db.create_transaction();
    tx.insert(Acct { id: 1, status: 1 })?;
    tx.insert(Acct { id: 2, status: 2 })?;
    db.commit(tx)?;

    assert_eq!(by_status(&db, 0)?, vec![]);
    assert_eq!(by_status(&db, 1)?, vec![AcctKey(1)]);
    assert_eq!(by_status(&db, 2)?, vec![AcctKey(2)]);
    Ok(())
}

#[test]
fn same_key_written_twice_in_one_transaction() -> anyhow::Result<()> {
    let mut db = Db::new(MemoryStorage::new())?;
    db.insert(Acct { id: 1, status: 0 })?;

    let mut tx = db.create_transaction();
    tx.insert(Acct { id: 1, status: 1 })?;
    tx.insert(Acct { id: 1, status: 2 })?;
    db.commit(tx)?;

    assert_eq!(by_status(&db, 0)?, vec![]);
    assert_eq!(by_status(&db, 1)?, vec![]);
    assert_eq!(by_status(&db, 2)?, vec![AcctKey(1)]);
    assert_eq!(db.get(&AcctKey(1))?, Some(Acct { id: 1, status: 2 }));
    Ok(())
}

#[test]
fn remove_then_reinsert_in_one_transaction() -> anyhow::Result<()> {
    let mut db = Db::new(MemoryStorage::new())?;
    let old = Acct { id: 1, status: 0 };
    db.insert(old.clone())?;

    let mut tx = db.create_transaction();
    tx.remove(&AcctKey(1), &old)?;
    tx.insert(Acct { id: 1, status: 3 })?;
    db.commit(tx)?;

    assert_eq!(by_status(&db, 0)?, vec![]);
    assert_eq!(by_status(&db, 3)?, vec![AcctKey(1)]);
    assert_eq!(db.get(&AcctKey(1))?, Some(Acct { id: 1, status: 3 }));
    Ok(())
}

#[test]
fn update_keeping_indexed_value_still_indexed() -> anyhow::Result<()> {
    let mut db = Db::new(MemoryStorage::new())?;
    db.insert(Acct { id: 1, status: 5 })?;
    db.insert(Acct { id: 1, status: 5 })?;

    assert_eq!(by_status(&db, 5)?, vec![AcctKey(1)]);

    let mut tx = db.create_transaction();
    tx.insert(Acct { id: 1, status: 5 })?;
    db.commit(tx)?;

    assert_eq!(by_status(&db, 5)?, vec![AcctKey(1)]);
    Ok(())
}

#[test]
fn update_changing_one_of_two_indexes() -> anyhow::Result<()> {
    let mut db = Db::new(MemoryStorage::new())?;
    db.insert(Person {
        id: 7,
        city: 1,
        age: 30,
    })?;
    db.insert(Person {
        id: 7,
        city: 2,
        age: 30,
    })?;

    let by_city = |db: &Db, city: u8| -> anyhow::Result<Vec<PersonKey>> {
        Ok(db
            .iter_by_index_exact(&PersonCityIndex(city))?
            .collect::<Result<_, _>>()?)
    };
    let by_age = |db: &Db, age: u8| -> anyhow::Result<Vec<PersonKey>> {
        Ok(db
            .iter_by_index_exact(&PersonAgeIndex(age))?
            .collect::<Result<_, _>>()?)
    };

    assert_eq!(by_city(&db, 1)?, vec![]);
    assert_eq!(by_city(&db, 2)?, vec![PersonKey(7)]);
    assert_eq!(by_age(&db, 30)?, vec![PersonKey(7)]);
    Ok(())
}

#[test]
fn autoincrement_put_is_unaffected() -> anyhow::Result<()> {
    let mut db = Db::new(MemoryStorage::new())?;
    let a = db.put(Event { kind: 1 })?;
    let b = db.put(Event { kind: 1 })?;
    let c = db.put(Event { kind: 2 })?;
    assert_ne!(a, b);

    let mut kind1: Vec<EventKey> = db
        .iter_by_index_exact(&EventKindIndex(1))?
        .collect::<Result<_, _>>()?;
    kind1.sort();
    let mut expected = vec![a, b];
    expected.sort();
    assert_eq!(kind1, expected);

    let kind2: Vec<EventKey> = db
        .iter_by_index_exact(&EventKindIndex(2))?
        .collect::<Result<_, _>>()?;
    assert_eq!(kind2, vec![c]);
    Ok(())
}

#[test]
fn indexed_record_without_previous_version_reads_nothing_stale() -> anyhow::Result<()> {
    // Fresh keys must not produce any deletes that could clobber other records' entries.
    let mut db = Db::new(MemoryStorage::new())?;
    db.insert(Acct { id: 1, status: 9 })?;
    db.insert(Acct { id: 2, status: 9 })?;
    let mut keys = by_status(&db, 9)?;
    keys.sort();
    assert_eq!(keys, vec![AcctKey(1), AcctKey(2)]);
    Ok(())
}
