# krabka-parse-benches

Parses Criterion benchmark output in bencher format into structured JSON summaries.

Part of [Krabka](../../README.md), a Rust implementation of Apache Kafka.

## Overview

`krabka-parse-benches` turns Criterion bencher output into JSON. Given three or
more reference and candidate summaries, it also emits a regression verdict
whose tolerance is derived from their median absolute deviation.

The same statistics judge the broker performance lane. `check-contract`
validates a regression contract, `assemble` reads a `qualify.sh budget`
artifact directory into a results record, and `evaluate` judges the record
against its contract and renders the published Markdown.
[Performance regression budgets](../../docs/operations/performance-budgets.md)
is the design.

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

To record which bench executables a pass ran, build the revision first and
keep Cargo's JSON messages. `executables` writes a manifest of each bench
executable's path and SHA-256. Parsing a pass's output against that manifest
reads the `Running` lines in Cargo's standard error. It fails unless the pass
ran only the manifest's executables, unchanged, and the summary names them:

```sh
cargo bench --no-run --message-format=json > cargo-build.json
cargo run -p krabka-parse-benches -- executables cargo-build.json \
  --commit "$(git rev-parse HEAD)" --output executables.json
cargo bench -- --output-format bencher 2> run-1/cargo-stderr.log | tee run-1/raw.txt
cargo run -p krabka-parse-benches -- \
  --results-dir run-1 --output summaries/run-1.json \
  --commit "$(git rev-parse HEAD)" \
  --executables executables.json --cargo-log run-1/cargo-stderr.log
```

Compare repeated same-host summaries (a regression exits 1 after writing the
verdict). Every summary must name its executables. A comparison is refused
when one side's passes ran different executables, or when both sides ran an
executable at the same path, which means the two revisions shared a target
directory and may have run one binary:

```sh
cargo run -p krabka-parse-benches -- \
  --reference-dir bench-results/reference \
  --candidate-dir bench-results/candidate \
  --output bench-results/verdict.json
```

Judge a broker performance record (a rejection exits 1 after writing the
verdict and the Markdown):

```sh
cargo run -p krabka-parse-benches -- check-contract \
  packaging/performance/regression-contract.json
cargo run -p krabka-parse-benches -- assemble performance-artifacts \
  --output performance-artifacts/performance-record.json
cargo run -p krabka-parse-benches -- evaluate \
  performance-artifacts/performance-record.json \
  --contract packaging/performance/regression-contract.json \
  --output performance-artifacts/verdict.json \
  --markdown performance-artifacts/performance-budget.md
```

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success: benchmark metrics successfully parsed and written to output JSON. |
| `1` | Failure: invalid or missing input, duplicate metrics, I/O error, a measured regression, or a rejected performance budget. |

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
