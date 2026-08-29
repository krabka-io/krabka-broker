# krabka-telemetry

[![Crates.io](https://img.shields.io/crates/v/krabka-telemetry.svg)](https://crates.io/crates/krabka-telemetry)
[![Docs.rs](https://docs.rs/krabka-telemetry/badge.svg)](https://docs.rs/krabka-telemetry)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Generic OTLP distributed-tracing pipeline for Krabka services.

This crate is part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add krabka-telemetry
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Install stdout tracing plus optional OTLP export for a Krabka service process:

```rust,no_run
use krabka_telemetry::{init, OtlpConfig};

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let otlp = OtlpConfig::from_env(
    |key| std::env::var(key).ok(),
    "broker-1",
    env!("CARGO_PKG_VERSION"),
    "krabka-broker",
);
let guard = init(otlp, "info", "info", "krabka-broker")?;
tracing::info!("tracing is configured");
guard.shutdown();
# Ok(())
# }
```

## Documentation

Read the API documentation at [docs.rs/krabka-telemetry](https://docs.rs/krabka-telemetry). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
