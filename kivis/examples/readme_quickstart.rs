//! The quick start example from the project readme, kept compiling so the readme cannot
//! drift from the API. Update both together.

use kivis::{Database, MemoryStorage, Record, manifest};

#[derive(Record, Debug, Clone, serde::Serialize, serde::Deserialize)]
struct User {
    #[index]
    name: String,
    email: String,
}

// Every record type in a database is listed in one manifest.
manifest![App: User];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Database::<MemoryStorage, App>::new(MemoryStorage::new())?;

    // `put` assigns an autoincremented key and returns it.
    let key = db.put(User {
        name: "Alice".into(),
        email: "alice@example.com".into(),
    })?;

    let user = db.get(&key)?.expect("just inserted");
    assert_eq!(user.name, "Alice");

    // Secondary indexes are derived from `#[index]` fields.
    let found: Vec<_> = db
        .iter_by_index_exact(&UserNameIndex("Alice".into()))?
        .collect::<Result<_, _>>()?;
    assert_eq!(found, vec![key]);

    Ok(())
}
