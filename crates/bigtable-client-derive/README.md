# `bigtable-client-derive`

Derive macros for typed row mapping with
[`bigtable-client`](https://github.com/theutopialabs/bigtable-client-rs).

This package is normally used through the `bigtable-client::FromRow` re-export:

```rust
use bigtable_client::FromRow;

#[derive(FromRow)]
#[bigtable(family = "profile")]
struct User {
    #[bigtable(row_key)]
    key: String,
    name: String,
}
```

See the `bigtable-client` documentation for the supported attributes and row
mapping behavior.
