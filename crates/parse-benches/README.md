# krabka-parse-benches

Parses Criterion benchmark output in bencher format into structured JSON summaries.

Part of [Krabka](../../README.md), a Rust implementation of Apache Kafka.

## Overview

`krabka-parse-benches` turns Criterion bencher output into JSON. Given three or
more reference and candidate summaries, it also emits a regression verdict
whose tolerance is derived from their median absolute deviation.

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

Compare repeated same-host summaries (a regression exits 1 after writing the
verdict):

```sh
cargo run -p krabka-parse-benches -- \
  --reference-dir bench-results/reference \
  --candidate-dir bench-results/candidate \
  --output bench-results/verdict.json
```

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success: benchmark metrics successfully parsed and written to output JSON. |
| `1` | Failure: invalid or missing input, duplicate metrics, I/O error, or a measured regression. |

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
