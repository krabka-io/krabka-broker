# krabka-macros

Derive macros that let one declaration in the krabka broker stand for code that the broker would otherwise write two or three times.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

The crate is a `proc-macro` crate built on [`moxy`](https://crates.io/crates/moxy). The broker is its only user. It does not implement a Kafka standard of its own.

## Features

- `#[derive(RegisterMetrics)]` — on a struct of `prometheus-client` metric handles, derives the constructor of every handle and the registration of every handle with a `Registry`. A `#[metric(help = "...")]` attribute on each field gives the help text, and optional `name`, `buckets`, `new` and `skip` arguments change the registered name, the histogram buckets, the constructor, or leave the field unregistered. `BrokerMetrics` in `krabka-broker` uses it.

## Usage

```rust,ignore
use krabka_macros::RegisterMetrics;
use prometheus_client::metrics::{counter::Counter, family::Family, histogram::Histogram};

#[derive(RegisterMetrics)]
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

The crate-level rustdoc lists every `#[metric(...)]` argument.
