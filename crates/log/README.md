# krabka-log

[![Crates.io](https://img.shields.io/crates/v/krabka-log.svg)](https://crates.io/crates/krabka-log)
[![Docs.rs](https://docs.rs/krabka-log/badge.svg)](https://docs.rs/krabka-log)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Byte-compatible reader and writer for Apache Kafka's on-disk log format.

Part of [Krabka](../../README.md), a Rust implementation of Apache
Kafka-compatible infrastructure and clients.

## Overview

`krabka-log` is the storage layer that the Krabka broker uses. It opens,
recovers, appends, reads, truncates, compacts, and exports Kafka-format log
directories. It keeps Kafka's segment naming and index formats.

The crate works at the single-partition log directory level. Higher layers apply
broker-level topic configuration, leader/follower ownership, remote-tier
scheduling, transaction visibility policy, and write serialization.

The [design document](docs/design.md) records the file layout, the recovery,
retention, and compaction rules, and the decisions behind them.

## Capabilities

- Open and recover Kafka-format log directories.
- Append `RecordBatch` values or verbatim record-batch bytes.
- Read decoded batches or raw bytes from an absolute offset.
- Manage sparse `.index`, `.timeindex`, and `.txnindex` files.
- Roll segments and apply size/time retention.
- Truncate, trim, and reset logs during replication or leader changes.
- Maintain leader-epoch checkpoints for truncation decisions.
- Compact eligible segments and expose tierable segment descriptors.

## Kafka Storage Scope

The crate targets Kafka 4.x log directories. These directories use 20-digit
zero-padded segment file names and append-only `.log` files that contain v2
`RecordBatch` streams. They also hold sparse offset/time indexes, transaction
indexes, and leader-epoch checkpoints.

`read` returns decoded `RecordBatch` values. Use `read_raw` when a caller needs
verbatim bytes for network or tiered-storage transfer.

## Install

```sh
cargo add krabka-log
cargo add krabka-protocol
cargo add krabka-units
```

For workspace development, use the path dependency from this repository.

## Usage

Open a Kafka-compatible log directory, append a batch, and read it back:

```rust,no_run
use krabka_log::{Log, LogConfig, Offset};
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::mebibytes;

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let mut log = Log::open("./target/orders-0", LogConfig::default())?;
let mut batch = RecordBatch::default();

let base_offset = log.append(&mut batch)?;
let output = log.read(Offset(0), mebibytes(1))?;

println!("wrote at {base_offset:?}; read {} batches", output.batches.len());
# Ok(())
# }
```

## Cargo Features

- `test-helpers` - exposes test-only helpers for downstream crate tests.

## Documentation

- [API documentation](https://docs.rs/krabka-log)
- [Repository README](../../README.md)
- [KIP compatibility matrix](../../docs/KIP_MATRIX.md), whose on-disk log
  contract rows this crate's `integration` suite establishes

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
