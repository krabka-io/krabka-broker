# krabka-broker-macros

Derive and function macros whose expansions name modules of `krabka-broker`, so they compile only inside the broker.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

The crate is a `proc-macro` crate built on [`moxy`](https://crates.io/crates/moxy). Its templates emit paths such as `crate::handlers::...` and `crate::coordinator::...`, which resolve only in `krabka-broker`, its one user. It shares its syntax helpers with [`krabka-macros`](../macros/README.md), which holds the macros that other crates use as well. It is not published.

## Features

- `dispatch_table!` and `throttle_probes!` — the broker's request dispatch adapters and the throttle-echo audit, from one table of api names.
- `#[derive(RegisterMetrics)]` — the constructor and `Registry` registration of every metric handle in `BrokerMetrics`.
- `#[derive(FlexibleEpochValue)]`, `#[flexible_value]`, `#[derive(ShareStateValue)]` and `#[derive(FlexibleArrayValue)]` — the framed codecs of the group-coordinator records.
- `broker_config!`, `set_runtime!`, `response_errors!`, `tagged_wire!` and the handler, coordinator and share-state macros, with the test fixtures that only the broker's tests call.

## Usage

```rust,ignore
use prometheus_client::metrics::{counter::Counter, family::Family, histogram::Histogram};

#[derive(krabka_broker_macros::RegisterMetrics)]
struct Metrics {
    #[metric(help = "Records received")]
    records_total: Counter,
    #[metric(help = "Request latency in seconds", buckets = [0.001, 0.01, 0.1])]
    latency_seconds: Family<Vec<(String, String)>, Histogram>,
}

let metrics = Metrics::unregistered();
let mut registry = prometheus_client::registry::Registry::default();
metrics.register(&mut registry);
```

## Documentation

The crate-level rustdoc describes each derive's attributes and each function macro's input.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
