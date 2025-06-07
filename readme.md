# Kivis

This crate provides a macro that let's users generate a database schema from a struct.

## Key

Each Records has a value and a key. If no #[key] is provided the first field is becomes the key. If multiple #[keys] are provided a composite key is used, where keys are used in the order of definition.

## Progress

* [x] First field is key
* [x] Specified field is key
* [x] Composite keys