# krabka-remote-storage-topic

[![Crates.io](https://img.shields.io/crates/v/krabka-remote-storage-topic.svg)](https://crates.io/crates/krabka-remote-storage-topic)
[![Docs.rs](https://docs.rs/krabka-remote-storage-topic/badge.svg)](https://docs.rs/krabka-remote-storage-topic)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Topic-backed RemoteLogMetadataManager for Krabka tiered storage.

This crate is part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add krabka-remote-storage-topic
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Start an in-process topic-backed metadata manager for tests and local tools:

```rust,no_run
use std::{path::PathBuf, time::Duration};
use krabka_remote_storage_topic::{InProcessMetadataEventLog, TopicBasedRemoteLogMetadataManager};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let event_log = InProcessMetadataEventLog::new(16);
let manager = TopicBasedRemoteLogMetadataManager::start(
    event_log,
    tokio::runtime::Handle::current(),
    PathBuf::from("./target/rlmm-cache"),
    Duration::from_secs(30),
).await?;

manager.reconcile_assignment(&[0, 1]).await;
manager.shutdown_and_flush().await;
# Ok(())
# }
```

## Documentation

Read the API documentation at [docs.rs/krabka-remote-storage-topic](https://docs.rs/krabka-remote-storage-topic). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
