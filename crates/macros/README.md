# krabka-macros

Derive and function macros that let one declaration in a krabka crate stand for code that the crate would otherwise write two or three times.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

The crate is a `proc-macro` crate built on [`moxy`](https://crates.io/crates/moxy). It holds the macros that more than one krabka crate invokes, `krabka-log` among them, which is why it is published. A macro whose expansion names modules of `krabka-broker` lives in the unpublished [`krabka-broker-macros`](../broker-macros/README.md) instead. The crate does not implement a Kafka standard of its own.

## Features

- `#[derive(FieldDefaults)]` — on a struct with named fields, derives a `Default` that fills each field from its `#[default(EXPR)]` attribute, or with `Default::default()`.
- `#[derive(RefinedNewtype)]`, `#[derive(PrimitiveCmp)]`, `#[derive(EnumStr)]` and `#[derive(KafkaUuid)]` — the constructors, parsing, display, comparisons and serde of newtypes and string-valued enums.
- `#[krabka_env]` and `cli_main!` — the `KRABKA_*` environment fallbacks of a clap argument struct and the entry point of an operator CLI.
- `#[derive(SparseIndex)]`, `#[remote_operation]`, `#[model_type]` and `worm_object_set!` — the storage, tracing and proof-model shapes that `krabka-log`, `krabka-remote-storage` and `krabka-verified` share.

## Usage

```rust,ignore
use std::time::Duration;

#[derive(krabka_macros::FieldDefaults)]
pub struct FreezeConfig {
    #[default(Duration::from_secs(30))]
    pub poll_interval: Duration,
    #[default(true)]
    pub enabled: bool,
    pub owners: Vec<String>,
}

let config = FreezeConfig::default();
```

## Documentation

The crate-level rustdoc describes each derive's attributes and each function macro's input.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
