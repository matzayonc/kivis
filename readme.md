## `Kivis`: Type-Safe Database Schema Generation for Rust

`Kivis` is a Rust crate that provides a powerful procedural macro to automatically generate database schemas directly from your Rust struct definitions. Designed to operate seamlessly over any ordered key-value store, such as `BTreeMap` or `Sled`, `Kivis` simplifies data persistence by offering robust support for complex data structures, keys, indexes, and foreign key relationships, all while maintaining type safety.

## Schemas

The entire database schema is declaratively defined through intuitive derive macro attributes. By annotating your Rust structs, `Kivis` handles the underlying schema generation, reducing boilerplate and ensuring consistency between your application's data models and the stored schema.

## Flexible Key Management

`Kivis` offers two primary mechanisms for defining record keys:

1. Auto-incremented IDs: Records can be assigned unique, automatically incremented identifiers upon insertion, ideal for simple primary keys.
2. Composite and Simple Keys: For more explicit keying, one or more fields within a struct can be designated as key components using the #[key] attribute. This allows for the creation of simple or composite keys that uniquely identify records.

Both key types are exposed through zero-cost abstraction wrappers, such as StructNameKey, which encapsulate the key's type and table correlation, providing compile-time safety and clarity.

## Efficient Data Retrieval with Indexes

To facilitate efficient data retrieval, `Kivis` supports the definition of arbitrary secondary indexes. Any field can be marked with the #[index] attribute, leading to the automatic generation of a corresponding index structure (e.g., StructNameFieldNameIndex). These index structures enable fast lookups and range queries based on the indexed fields, similar to traditional database indexes.

## Robust Foreign Key Relationships

A distinguishing feature of `Kivis` is its sophisticated handling of foreign key relationships. By storing key wrappers (e.g., UserKey, ToyKey) directly within a struct's fields, `Kivis` leverages these zero-cost abstractions to embed static table correlation directly into your data model. This approach ensures type-safe references between records in different tables, providing compile-time validation of relationships and enhancing data integrity without runtime overhead.

## Compatibility

`Kivis` is designed to be backend-agnostic, operating over any ordered key-value store. This flexibility allows developers to choose the underlying storage mechanism that best suits their application's needs, whether it's an in-memory `BTreeMap` for transient data or a persistent solution like `Sled`.

By leveraging Rust's powerful type system and procedural macros, `Kivis` provides a highly efficient, type-safe, and developer-friendly approach to defining and managing database schemas. It streamlines the process of working with structured data in key-value stores, making it an ideal choice for applications requiring robust data modeling with minimal overhead.
