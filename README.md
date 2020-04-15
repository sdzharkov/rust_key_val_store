# KvStore

KvStore is a log-based, persistant, key value store that is inspired by the
Bitcask store architecture. The project was mainly built as a tool to learn Rust
and log-based key value stores. This is my first personal project that has been
written in Rust so apologies for any informalities / uncleanliness.

## Installing

```bash
cargo build
```

## Running from the CLI

```bash
cargo run -- get key       # Fetching a key from the store
cargo run -- set key value # Setting a value
cargo run -- rm key        # Remove a key
```

## Importing the KvStore directly

```rust
let mut store = KvStore::open(env::current_dir()?)?;

store.set(key.to_string(), value.to_string())?;
store.get(key.to_string())?;
store.remove(key.to_string())
```
