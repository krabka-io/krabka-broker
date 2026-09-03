# krabka-parse-benches

Parses Criterion benchmark output in bencher format into structured JSON summaries.

Part of [Krabka](../../README.md), a Rust implementation of Apache Kafka.

## Overview

`krabka-parse-benches` scans a directory containing Criterion benchmark text outputs (`--output-format bencher`), parses metrics and variance values, validates metric uniqueness, and emits a structured JSON summary document.

> [!NOTE]
> This crate currently lives in `krabka-broker` to service `krabka-log` microbenchmarks. If a shared `krabka-tools` repository is established across `krabka-io`, this utility can be migrated there as a common benchmark utility for `krabka-broker`, `krabka-protocol`, and `krabka-client-rs`.

## Usage

Run against a directory containing benchmark output files (`*.txt`):

```sh
cargo run -p krabka-parse-benches -- \
  --results-dir bench-results \
  --output bench-results/broker-benchmarks.json \
  --suite krabka-broker
```

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success: benchmark metrics successfully parsed and written to output JSON. |
| `1` | Failure: missing directory, no `*.txt` files, zero metrics parsed, duplicate benchmark name collision, or I/O error. |

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
