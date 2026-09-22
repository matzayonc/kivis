# Known limitations

Behaviour that is intentional, or known and not yet fixed. Each entry says what
happens and what to do instead. If something here bites you, it is a known gap
rather than a surprise — but please still open an issue, since that is how these
get prioritised.

See also the [changelog](CHANGELOG.md) for what has already changed.

## Schema and derive

### Generic record types are not supported

`#[derive(Record)]` on a type with type parameters is rejected at compile time:

```rust,ignore
#[derive(Record, serde::Serialize, serde::Deserialize, Debug)]
struct Wrapper<T> {   // error: kivis::Record cannot be derived for generic types
    value: T,
}
```

The derive generates a key type (`WrapperKey`) alongside the record, and that key
carries none of the record's parameters while the generated trait impls refer to
them, so the result cannot compile. Rather than emit code that fails with errors
pointing inside the macro expansion, the derive rejects the input and says why.

**Instead:** derive on a concrete type — `struct WrapperU64 { value: u64 }` — or
keep the generic type as an ordinary field of a concrete record.

### At most 254 indexes per record

A record may carry 254 `#[index]` fields. Index discriminators share one byte with
the table markers (main table and the reserved slot), so the highest usable
discriminator is 253. Exceeding it is a compile error.

### Only `doc` and `cfg` attributes reach the key type

Container attributes on a record are not copied to the generated key struct.
`#[serde(rename_all = ...)]` applies to how the *record* is stored, not its key.

## Key ordering

Range scans compare encoded keys byte by byte, so ordering is a property of the
encoding. The default key encoding is big-endian and fixed-width, which matches
numeric order for unsigned integers. Two cases do not:

- **Signed integers** are stored in two's complement, so negative values sort
  *after* positive ones. A range over a signed key will not behave as expected
  around zero.
- **`String` and `Vec<T>`** are length-prefixed and therefore sort by length
  before content: `"z"` sorts before `"aa"`. For lexicographic ordering wrap the
  field in `Lexicographic<String>`.

Floating-point keys are not ordered correctly by any of the built-in encodings and
are rejected outright by the `kivis-fs` key encoding, which also does not support
maps as key components.

## Referential integrity is a compile-time property only

Storing a `UserKey` inside a record guarantees, at compile time, that the key
belongs to the `User` table — passing a `PetKey` will not compile. Nothing checks
at runtime that the referenced record exists:

- Removing a record does **not** remove, or refuse to remove, records pointing at
  it. Their keys are left dangling and `get` on them returns `None`.
- There are no cascade deletes and no foreign-key constraints.

Autoincrement ids are never reissued, so a dangling key stays dangling rather than
silently resolving to an unrelated record that later took the same id.

## Caching

**A transaction commit does not invalidate the cache.** `Database::put`, `insert`
and `remove` expire the affected entry, but `Database::commit` does not, so a
record written through a user transaction leaves any previously cached value in
place and a later `get` returns the stale record:

```rust,ignore
db.insert(Acct { id: 1, balance: 100 })?;
db.get(&AcctKey(1))?;                     // caches balance 100

let mut tx = db.create_transaction();
tx.insert(Acct { id: 1, balance: 999 })?;
db.commit(tx)?;

db.get(&AcctKey(1))?;                     // still reports 100
```

This only affects databases configured with a cache via `manifest![Name + Cache: ...]`;
the default `NoCache` is unaffected. **Until it is fixed, expire the keys you wrote
yourself after committing**, or route writes that must stay cache-coherent through
`put`/`insert`/`remove`.

## Transactions and autoincrement ids

- **Rolled back transactions burn ids.** The counter advances when a key is issued
  inside `DatabaseTransaction::put`, not at commit, so ids from a transaction that
  is rolled back or fails to commit are never handed out. This keeps ids unique at
  the cost of gaps.
- **Ids issued inside a transaction are not persisted automatically.**
  `Database::put` records the counter for you; after committing a transaction that
  issued keys via `DatabaseTransaction::put`, call `Database::persist_counter` so
  the id cannot be reused if that record is later deleted.

## Storage format stability

The on-disk format is **not stable before 1.0**. 0.6.0 changed it incompatibly for
both `kivis` and `kivis-fs`, and there is no migration tooling: a database written
by an earlier version has to be exported and re-imported. Treat a version bump as
requiring a data migration until this section says otherwise.

## Backend-specific

### kivis-fs

- One file per record, and every range scan lists the whole data directory. This is
  fine for hundreds of records and not intended for large data sets.
- Keys become file names, so the practical key-length limit is the filesystem's.

### sled

- Values are encoded with postcard while keys use the order-preserving encoding;
  the two are not interchangeable.
