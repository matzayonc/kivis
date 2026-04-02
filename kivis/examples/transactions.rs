use kivis::{Database, DatabaseError, DeriveKey, MemoryStorage, Record, manifest};

/// A simple account record. The `id` field is the explicit key.
#[derive(Record, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Account {
    #[key]
    id: u32,
    name: String,
    balance: i64,
}

manifest![Bank: Account];

fn main() -> Result<(), DatabaseError<MemoryStorage>> {
    let mut db: Database<_, Bank> = Database::new(MemoryStorage::new()).unwrap();

    // Seed two accounts directly
    let alice = Account {
        id: 1,
        name: "Alice".to_string(),
        balance: 1000,
    };
    let bob = Account {
        id: 2,
        name: "Bob".to_string(),
        balance: 500,
    };
    let alice_key = Account::key(&alice);
    let bob_key = Account::key(&bob);
    db.insert(alice.clone())?;
    db.insert(bob.clone())?;

    // Transfer 200 from Alice to Bob inside a transaction.
    // Either both writes land or neither does.
    let transfer_amount = 200i64;

    let mut tx = db.create_transaction();
    tx.insert(Account {
        balance: alice.balance - transfer_amount,
        ..alice.clone()
    })?;
    tx.insert(Account {
        balance: bob.balance + transfer_amount,
        ..bob.clone()
    })?;
    db.commit(tx)?;

    // Verify the balances after the transfer
    let alice_after = db.get(&alice_key)?.expect("alice exists");
    let bob_after = db.get(&bob_key)?.expect("bob exists");

    println!("{}: {}", alice_after.name, alice_after.balance); // Alice: 800
    println!("{}: {}", bob_after.name, bob_after.balance); // Bob:   700

    assert_eq!(alice_after.balance, 800);
    assert_eq!(bob_after.balance, 700);

    // Rolling back a transaction leaves the database unchanged
    let mut tx = db.create_transaction();
    tx.insert(Account {
        id: 1,
        name: "Alice".to_string(),
        balance: 0,
    })?;
    tx.rollback();

    let alice_unchanged = db.get(&alice_key)?.expect("alice exists");
    assert_eq!(alice_unchanged.balance, 800);
    println!(
        "After rollback, Alice still has: {}",
        alice_unchanged.balance
    );

    Ok(())
}
