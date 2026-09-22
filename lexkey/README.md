# lexkey

[![crates.io](https://img.shields.io/crates/v/lexkey.svg)](https://crates.io/crates/lexkey)
[![docs.rs](https://docs.rs/lexkey/badge.svg)](https://docs.rs/lexkey)
[![license](https://img.shields.io/crates/l/lexkey.svg)](../LICENSE)

Order-preserving, prefix-free, filename-safe key encoding for `serde`.

Sorting a store by its serialized keys only works if the encoding preserves order, and
scanning a key prefix only works if no encoded value can be a prefix of another. Most
formats give you neither: bincode's varints are little-endian, JSON and CSV write integers
as bare decimals where `"10"` sorts before `"2"`, and none of them delimit a string in a way
that stops `"bob"` from prefixing `"bobby"`.

```toml
[dependencies]
lexkey = "0.6"
```

```rust
// Order is preserved: comparing encodings compares the values.
assert!(lexkey::encode(&9u32)? < lexkey::encode(&10u32)?);

// Components are self-delimiting, so no key can be a prefix of another.
assert!(!lexkey::encode(&"bobby")?.starts_with(&lexkey::encode(&"bob")?));

// Concatenation equals struct encoding, so composite keys compose.
let mut composed = String::new();
lexkey::encode_into(&mut composed, &1u8)?;
lexkey::encode_into(&mut composed, &"x")?;
assert_eq!(composed, lexkey::encode(&(1u8, "x"))?);
# Ok::<(), lexkey::KeyError>(())
```

## Guarantees

1. **Order preserving.** Comparing two encodings as strings gives the same answer as
   comparing the values — including negative integers, stored in offset binary rather than
   two's complement.
2. **Prefix-free.** Every scalar ends with the terminator `.`, which sorts below every
   character that can appear inside a component, so `"bob"` never prefixes `"bobby"` and
   `"bob" + 13` never collides with `"bob1" + 3`.
3. **Concatenation is composition.** Encoding two values into one buffer produces exactly
   what encoding a struct or tuple of them produces, so a composite key can be built a piece
   at a time and parsed back as a whole.
4. **Filename-safe.** The output alphabet is `[A-Za-z0-9._]`, so an encoded key is usable
   directly as a file name.

## Format

| Rust type | Encoding |
|---|---|
| `u8`/`u16`/`u32`/`u64`/`u128` | zero-padded decimal of fixed width (3/5/10/20/39) + `.` |
| `i8`..`i128` | sign bit flipped, then as the unsigned type of the same width |
| `bool` | `0.` / `1.` |
| `str`, `char`, bytes | ASCII alphanumerics verbatim, other bytes as `_XX` (hex), then `.` |
| `Option<T>` | `0.` for `None`, `1.` then `T` for `Some` |
| structs, tuples, newtypes | fields concatenated, no framing |
| sequences | length as `u64`, then elements |
| enums | variant index as `u32`, then the payload |
| `()` | nothing |

Fixed-width integers are what make guarantee 1 hold, and they are why the encoding is
verbose: a `u64` always takes 20 characters. Strings sort lexicographically rather than by
length, since they are terminated rather than length-prefixed.

Floats and maps are rejected: no fixed-width decimal form of a float preserves order across
the whole range including negative zero and NaN, and a map has no canonical field order.

## Comparison

[`bytekey`](https://crates.io/crates/bytekey) and
[`storekey`](https://crates.io/crates/storekey) solve the same ordering problem with a
*binary* encoding. Reach for one of those if you want compactness; reach for `lexkey` if the
key has to stay readable — a file name, a URL segment, something you will read in a log.

## Use with kivis

With the `kivis` feature, `KeyCodec` implements `kivis::Unifier` and can be the key codec of
a [kivis](https://crates.io/crates/kivis) storage backend:

```toml
lexkey = { version = "0.6", features = ["kivis"] }
```

This is how [`kivis-fs`](https://crates.io/crates/kivis-fs) names its files.

## License

MIT — see [LICENSE](../LICENSE).
