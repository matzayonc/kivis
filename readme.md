## Kivis: Type-Safe Database Schema Generation for Rust

Kivis is a Rust crate that provides a powerful procedural macro to automatically generate database schemas directly from your Rust struct definitions. Designed to operate seamlessly over any ordered key-value store, such as `BTreeMap` or `Sled`, Kivis simplifies data persistence by offering robust support for complex data structures, keys, indexes, and foreign key relationships, all while maintaining type safety.

## Schemas

The entire database schema is declaratively defined through intuitive derive macro attributes. By annotating your Rust structs, Kivis handles the underlying schema generation, reducing boilerplate and ensuring consistency between your application's data models and the stored schema.

## Flexible Key Management

Kivis offers two primary mechanisms for defining record keys:

1. Auto-incremented IDs: Records can be assigned unique, automatically incremented identifiers upon insertion, ideal for simple primary keys.
2. Composite and Simple Keys: For more explicit keying, one or more fields within a struct can be designated as key components using the `#[key]` attribute. This allows for the creation of simple or composite keys that uniquely identify records.
3. Custom behavior: For advanced use cases like content addressability and UUIDs.

Both key types are exposed through zero-cost abstraction wrappers, such as `StructNameKey`, which encapsulate the key's type and table correlation, providing compile-time safety and clarity.

## Efficient Data Retrieval with Indexes

To facilitate efficient data retrieval, Kivis supports the definition of arbitrary secondary indexes. Any field can be marked with the `#[index]` attribute, leading to the automatic generation of a corresponding index structure (e.g., `StructNameFieldNameIndex`). These index structures enable fast lookups and range queries based on the indexed fields, similar to traditional database indexes.

## Robust Foreign Key Relationships

A distinguishing feature of Kivis is its sophisticated handling of foreign key relationships. By storing key wrappers (e.g., `UserKey`, `ToyKey`) directly within a struct's fields, Kivis leverages these zero-cost abstractions to embed static table correlation directly into your data model. This approach ensures type-safe references between records in different tables, providing compile-time validation of relationships and enhancing data integrity without runtime overhead.

## Compatibility

Kivis is designed to be backend-agnostic, operating over any ordered key-value store. This flexibility allows developers to choose the underlying storage mechanism that best suits their application's needs, whether it's an in-memory `BTreeMap` for transient data or a persistent solution like `Sled`.

### Layered Cache Architecture

The `Storage` trait's simplicity enables sophisticated layered cache architectures where multiple storage implementations can be composed together. This design pattern allows for complex data hierarchies that optimize both performance and data locality. A typical layered setup might include:

1. **Remote Repository**: The authoritative source containing the complete dataset, potentially hosted on cloud storage or a remote database server
2. **Local Archive**: A comprehensive local copy that mirrors most of the remote data for offline access and reduced network dependency
3. **Local Persistent Cache**: A fast local storage layer (such as SQLite or RocksDB) that maintains frequently accessed records across application restarts
4. **In-Memory Cache**: The fastest access tier using structures like `BTreeMap` for immediate retrieval of hot data

Each layer can implement the `Storage` trait and delegate to the next tier when data is not found locally, creating a transparent cache hierarchy that automatically optimizes data access patterns while maintaining the same simple API surface.

By leveraging Rust's powerful type system and procedural macros, Kivis provides a highly efficient, type-safe, and developer-friendly approach to defining and managing database schemas. It streamlines the process of working with structured data in key-value stores, making it an ideal choice for applications requiring robust data modeling with minimal overhead.


## Key insights

Type-Safe Key-Table Association: Kivis enforces compile-time referential integrity by using zero-cost key wrapper types (e.g., UserKey) to statically embed the target table correlation, preventing runtime errors associated with using the wrong key type for a record.

Backend-Agnostic Storage: The separation of schema definition from the storage mechanism is achieved via a simple Storage trait, enabling Kivis to operate on any ordered key-value store and naturally support complex, layered cache architectures.

Schema-as-Struct: Database schemas are declaratively defined directly from Rust structs using procedural macro attributes (#[key], #[index], etc.), which automatically generates all necessary data structures for persistence and querying, drastically reducing boilerplate and ensuring data model consistency.


## Related work

1. 🔑 Key Serialization preserving order (rel. `bytekey`, `storekey`)

    Kivis uses its custom `LexicographicString` for order preservation, a specialized, self-contained solution that contrasts with the general-purpose library approach of bytekey/storekey for all data types.

2. 🧱 High-Level Modeling (rel. `netabase_store`, `native_model`)

    Kivis is philosophically aligned with netabase_store (backend-agnostic, attribute-driven keys) but uses a unique API centered on zero-cost Key Wrappers and a `Storage` trait for its layered architecture.

3. 🛡️ Data Safety and Integrity (rel. `rkv`, `struct_db`)

    Kivis provides compile-time referential integrity via its type-safe Foreign Key Wrappers, a powerful feature for relationship validation in the non-relational K/V ecosystem that goes beyond the runtime checks of rkv and struct_db.
