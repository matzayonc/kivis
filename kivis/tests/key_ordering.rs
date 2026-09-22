//! Regression tests for order-preserving key encoding and ascending range iteration.
//!
//! Before `OrderedKeyConfig`, keys were encoded as little-endian varints, so byte order
//! diverged from numeric order above 250. That broke range scans and made autoincrement
//! recovery on reopen hand out keys that already existed (silently overwriting records).

use kivis::{Database, MemoryStorage, Record, manifest};

#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct Auto {
    name: String,
}

#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct Keyed {
    #[key]
    id: u64,
    #[index]
    group: u32,
    name: String,
}

manifest![Ordering: Auto, Keyed];

const IDS: [u64; 11] = [1, 2, 3, 4, 5, 250, 251, 300, 511, 512, 1000];

fn keyed_db() -> anyhow::Result<Database<MemoryStorage, Ordering>> {
    let mut db = Database::<MemoryStorage, Ordering>::new(MemoryStorage::new())?;
    // Insert out of order so storage order is what's being tested, not insertion order.
    for id in IDS.iter().rev() {
        db.insert(Keyed {
            id: *id,
            group: u32::try_from(*id)? / 256,
            name: format!("k{id}"),
        })?;
    }
    Ok(db)
}

#[test]
fn autoincrement_survives_reopen_past_varint_boundaries() -> anyhow::Result<()> {
    let mut db = Database::<MemoryStorage, Ordering>::new(MemoryStorage::new())?;
    let mut keys = Vec::new();
    for i in 0..600u64 {
        keys.push(db.put(Auto {
            name: format!("item{i}"),
        })?);
    }
    assert_eq!(keys.last(), Some(&AutoKey(600)));
    assert_eq!(db.last_id::<AutoKey>()?, AutoKey(600));

    let storage = db.dissolve();
    let mut db = Database::<MemoryStorage, Ordering>::new(storage)?;
    assert_eq!(db.last_id::<AutoKey>()?, AutoKey(600));

    let fresh = db.put(Auto { name: "new".into() })?;
    assert_eq!(fresh, AutoKey(601));
    assert!(!keys.contains(&fresh));
    assert_eq!(
        db.get(&AutoKey(512))?,
        Some(Auto {
            name: "item511".into()
        })
    );
    Ok(())
}

#[test]
fn iter_keys_is_ascending_start_inclusive_end_exclusive() -> anyhow::Result<()> {
    let db = keyed_db()?;

    let ids = |r: std::ops::Range<u64>| -> anyhow::Result<Vec<u64>> {
        Ok(db
            .iter_keys(KeyedKey(r.start)..KeyedKey(r.end))?
            .map(|k| k.map(|k| k.0))
            .collect::<Result<_, _>>()?)
    };

    assert_eq!(ids(1..4)?, vec![1, 2, 3]);
    assert_eq!(ids(250..600)?, vec![250, 251, 300, 511, 512]);
    assert_eq!(ids(0..2000)?, IDS.to_vec());
    assert_eq!(ids(6..250)?, Vec::<u64>::new());
    Ok(())
}

#[test]
fn iter_all_keys_is_ascending_and_double_ended() -> anyhow::Result<()> {
    let db = keyed_db()?;

    let all: Vec<u64> = db
        .iter_all_keys::<KeyedKey>()?
        .map(|k| k.map(|k| k.0))
        .collect::<Result<_, _>>()?;
    assert_eq!(all, IDS.to_vec());

    let rev: Vec<u64> = db
        .iter_all_keys::<KeyedKey>()?
        .rev()
        .map(|k| k.map(|k| k.0))
        .collect::<Result<_, _>>()?;
    assert_eq!(rev, IDS.iter().rev().copied().collect::<Vec<_>>());

    assert_eq!(db.last_id::<KeyedKey>()?, KeyedKey(1000));
    Ok(())
}

#[test]
fn iter_by_index_ranges_are_ascending() -> anyhow::Result<()> {
    let db = keyed_db()?;
    // group = id / 256: ids 1..=251 -> 0, 300 & 511 -> 1, 512 -> 2, 1000 -> 3

    let by_group = |r: std::ops::Range<u32>| -> anyhow::Result<Vec<u64>> {
        Ok(db
            .iter_by_index(KeyedGroupIndex(r.start)..KeyedGroupIndex(r.end))?
            .map(|k| k.map(|k| k.0))
            .collect::<Result<_, _>>()?)
    };

    assert_eq!(by_group(0..1)?, vec![1, 2, 3, 4, 5, 250, 251]);
    assert_eq!(by_group(1..3)?, vec![300, 511, 512]);
    assert_eq!(by_group(0..4)?, IDS.to_vec());
    assert_eq!(by_group(2..2)?, Vec::<u64>::new());

    let exact: Vec<u64> = db
        .iter_by_index_exact(&KeyedGroupIndex(1))?
        .map(|k| k.map(|k| k.0))
        .collect::<Result<_, _>>()?;
    assert_eq!(exact, vec![300, 511]);
    Ok(())
}

#[test]
fn key_encoding_byte_order_matches_numeric_order() -> anyhow::Result<()> {
    use kivis::{Unifier, ordered_key_config};

    let cfg = ordered_key_config();
    let mut prev: Option<Vec<u8>> = None;
    for id in [
        0u64,
        1,
        127,
        128,
        250,
        251,
        255,
        256,
        511,
        512,
        65535,
        65536,
        u64::MAX,
    ] {
        let mut buf = Vec::new();
        cfg.serialize(&mut buf, &id)?;
        assert_eq!(buf.len(), 8, "fixed-width encoding expected");
        if let Some(p) = &prev {
            assert!(p < &buf, "{id} must sort after its predecessor");
        }
        prev = Some(buf);
    }
    Ok(())
}
