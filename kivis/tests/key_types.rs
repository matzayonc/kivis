#![allow(clippy::duplicated_attributes)]

use kivis::{manifest, Database, DeriveKey, MemoryStorage, Record};
use serde::{Deserialize, Serialize};

type Payload = u16;

#[derive(Debug, Serialize, Deserialize, Record)]
struct Autoincremented {
    p: Payload,
}

#[derive(Debug, Serialize, Deserialize, Record)]
struct Field {
    #[key]
    id: u64,
    p: Payload,
}

#[derive(Debug, Serialize, Deserialize, Record)]
struct Composite {
    #[key]
    directory: u32,
    #[key]
    unit: u32,
    p: Payload,
}

#[derive(Debug, Serialize, Deserialize, Record)]
#[derived_key(u32, u32)]
struct WithDerived {
    p: Payload,
}

impl DeriveKey for WithDerived {
    type Key = WithDerivedKey;

    fn key(c: &<Self::Key as kivis::RecordKey>::Record) -> Self::Key {
        WithDerivedKey(c.p as u32 + 100, 0)
    }
}

manifest![Manifest: Autoincremented, Field, Composite, WithDerived];

#[test]
fn test_key_types() -> anyhow::Result<()> {
    let mut database = Database::<kivis::MemoryStorage, Manifest>::new(MemoryStorage::default())?;

    // Autoincremented key
    let autoincremented = Autoincremented { p: 5 };
    let autoincremented_key = database.put(&autoincremented)?;
    assert_eq!(autoincremented_key, AutoincrementedKey(1));

    // Field key
    let field_key = Field { id: 1, p: 20 };
    let field_key = database.insert(&field_key)?;
    assert_eq!(field_key, FieldKey(1));

    // Composite key
    let composite_key = Composite {
        directory: 2,
        unit: 3,
        p: 30,
    };
    let composite_key = database.insert(&composite_key)?;
    assert_eq!(composite_key, CompositeKey(2, 3));

    // Derived key
    let with_derived = WithDerived { p: 50 };
    let with_derived_key = database.insert(&with_derived)?;
    assert_eq!(with_derived_key, WithDerivedKey(150, 0));
    Ok(())
}
