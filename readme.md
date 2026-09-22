# Kivis

[![CI](https://github.com/matzayonc/kivis/actions/workflows/ci.yml/badge.svg)](https://github.com/matzayonc/kivis/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/kivis.svg)](https://crates.io/crates/kivis)
[![docs.rs](https://docs.rs/kivis/badge.svg)](https://docs.rs/kivis)
[![license](https://img.shields.io/crates/l/kivis.svg)](LICENSE)

Type-safe database schemas for Rust, generated from your structs, over any ordered
key-value store — `BTreeMap`, sled, the filesystem, or a few hundred kilobytes of flash
on a microcontroller.

```toml
[dependencies]
kivis = "0.6"
serde = { version = "1", features = ["derive"] }
```

## Quick start

```rust
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
```

## What the derive generates

`#[derive(Record)]` on `User` produces `UserKey`, one `User<Field>Index` type per
`#[index]` field, and the trait impls tying them together.

## Keys

Three strategies, one per record:

| Strategy | How | Key type |
|---|---|---|
| Autoincrement | no `#[key]` field | `UserKey(u64)`, assigned by `put` |
| Field key | one or more `#[key]` fields | `UserKey(field types…)`, assigned by `insert` |
| Derived | `#[derived_key(T)]` + your `DeriveKey` impl | whatever you compute — a hash, a UUID |

Autoincrement ids start at 1. The counter is recovered from the highest stored key when
the database is opened, so deleting the most recent record lets its id be issued again —
see [LIMITATIONS.md](LIMITATIONS.md).

## Foreign keys

Storing a `UserKey` in a field is how you reference another record, and the type says
which table it points at:

```rust,ignore
#[derive(Record, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Pet {
    name: String,
    owner: UserKey,   // can only ever hold a key of a User
}
```

Passing a `PetKey` where a `UserKey` belongs does not compile. This is checked at
compile time only: nothing verifies that the referenced record still exists, so
deleting a `User` leaves any `Pet` pointing at it dangling.

## Storage backends

Any ordered key-value store, via the `Storage` and `Repository` traits.

| Backend | Crate / feature | Notes |
|---|---|---|
| `BTreeMap` | `memory-storage` (default) | in-memory, no persistence |
| sled | `sled` feature | embedded database |
| Filesystem | [`kivis-fs`](kivis-fs) | one readable file per record |
| Your own | implement `Repository` | see `examples/remote_storage` |

Because the trait surface is small, backends compose: a layered cache is just a
`Repository` that consults a faster tier before delegating to a slower one.

### Implementing a backend

`Repository` needs get, insert, remove and `scan_range`. `scan_range` yields keys in
**ascending** byte order, start inclusive, end exclusive, as a `DoubleEndedIterator`.
`apply` has a default implementation, but override it to map a batch onto your store's
native atomic write — kivis relies on it so a failed commit leaves no partial state.

## Key ordering

Range scans compare encoded keys byte by byte, so the encoding has to preserve order.
The built-in key encoding is big-endian and fixed-width, which matches numeric order for
unsigned integers. Two cases do not sort the way you might expect:

- **Signed integers** are two's complement, so negatives sort after positives.
- **`String` and `Vec`** are length-prefixed and therefore sort by length first. For
  lexicographic string keys use `Lexicographic<String>`, which encodes so that `"Cat"`
  precedes `"Caterpillar"`.

## Transactions

`atomic` (default) gives you multi-record commits. Every write of a record — the record
itself plus all its index entries — goes into a single batch, so indexes cannot drift
out of sync with the data, even if a commit fails.

```rust,ignore
let mut tx = db.create_transaction();
tx.insert(Account { id: 1, balance: 800 })?;
tx.insert(Account { id: 2, balance: 700 })?;
db.commit(tx)?;   // both, or neither
```

A full example is in `examples/transactions.rs`.

## no_std

Works without the standard library, and without an allocator:

```toml
kivis = { version = "0.6", default-features = false, features = ["alloc", "atomic"] }
```

`examples/embedded.rs` runs against flash through `ekv`, using fixed-capacity
`heapless::Vec` buffers.

### Features

| Feature | Default | Effect |
|---|---|---|
| `std` | ✅ | standard library (implies `alloc`) |
| `alloc` | ✅ via `std` | `Vec`/`String` buffers without std |
| `atomic` | ✅ | transactions |
| `memory-storage` | ✅ | the `BTreeMap` backend |
| `heapless` | | fixed-capacity buffers, for no-allocator targets |
| `sled` | | the sled backend |

## Status

Pre-1.0, and the storage format is not yet stable: **0.6.0 changed it incompatibly and
databases written by 0.5.x cannot be opened.** See the [changelog](CHANGELOG.md).

Known gaps — generic records, key ordering for signed and string keys, cache
coherence after a transaction commit — are written down in
[LIMITATIONS.md](LIMITATIONS.md).

## Crates in this repository

| Crate | What it is |
|---|---|
| [`kivis`](kivis) | the database: derive, manifest, transactions, indexes |
| [`kivis-derive`](kivis-derive) | the `Record` derive macro |
| [`kivis-fs`](kivis-fs) | filesystem backend, one readable file per record |
| [`lexkey`](lexkey) | the key encoding, usable on its own |

`lexkey` is order-preserving, prefix-free and filename-safe, and does not depend on kivis
unless you ask it to. It was extracted from `kivis-fs`, which needs all three properties to
turn a composite key into a file name that still sorts correctly.

## Related work

- **Order-preserving key encoding** (`bytekey`, `storekey`) — both binary; [`lexkey`](lexkey)
  in this repository is the readable counterpart, and kivis ships `Lexicographic` for
  ordering plain string fields under the default binary key encoding.
- **Schema-from-struct modeling** (`native_model`, `struct_db`) — kivis is
  backend-agnostic and keeps its type-safe key wrappers as the referencing mechanism.

## License

MIT — see [LICENSE](LICENSE).
