# Known limitations

The limitations below are consequences of the design and are not going to change
soon. Each entry says what happens and what to do instead. If something here bites
you it is a known gap rather than a surprise — but please still open an issue, since
that is how these get prioritised.

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

Floating-point keys are not ordered correctly by any of the built-in encodings, and are
rejected outright by [`lexkey`](lexkey), the encoding `kivis-fs` uses, which also does not
support maps as key components.

## Referential integrity is a compile-time property only

Storing a `UserKey` inside a record guarantees, at compile time, that the key
belongs to the `User` table — passing a `PetKey` will not compile. Nothing checks
at runtime that the referenced record exists:

- Removing a record does **not** remove, or refuse to remove, records pointing at
  it. Their keys are left dangling and `get` on them returns `None`.
- There are no cascade deletes and no foreign-key constraints.

Autoincrement ids of deleted records can be issued again (see below), so a dangling
key can later resolve to an unrelated record that took the same id.

## Transactions and autoincrement ids

- **Ids of deleted trailing records are reused after a reopen.** The counter is
  not persisted; on open it is recovered as the highest key still stored. Delete
  the record with the highest id (or empty the table), reopen, and the next `put`
  issues that id again. Any stored key that pointed at the deleted record now
  resolves to the new one. If ids must never repeat, use a `#[derived_key]` such as
  a UUID, or never delete the most recent record.
- **Rolled back transactions burn ids until the next reopen.** The counter
  advances when a key is issued inside `DatabaseTransaction::put`, not at commit,
  so ids from a transaction that is rolled back or fails to commit are skipped for
  the rest of the session. After a reopen, ids above the highest stored key are
  issued again.

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
