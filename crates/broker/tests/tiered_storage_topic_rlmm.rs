//! Topic-backed remote-log-metadata-manager broker integration: the topic-backed
//! [`RemoteLogMetadataManager`](krabka_remote_storage::RemoteLogMetadataManager)
//! wired against a single broker's own loopback listener. The manager is
//! configured with `[remote_storage.kafka_metadata]`.
//!
//! `Broker::start` boots on the fail-closed `NotReadyRlmm` behind a
//! `SwappableRlmm`, then a retry-until-success task dials the broker's own
//! advertised listener, provisions `__remote_log_metadata`, starts the
//! `TopicBasedRemoteLogMetadataManager`, and swaps it in. These tests
//! exercise that path end-to-end with the `Local` tiered-storage backend:
//!
//! * [`topic_rlmm_activates_against_loopback`][]: the bootstrap completes. The
//!   `tiered_storage_rlmm_topic_backed` gauge flips to 1 and the
//!   `__remote_log_metadata` topic exists on the broker.
//! * [`topic_rlmm_copy_then_fetch_round_trip`][]: the test tiers a sealed
//!   segment and reads the records back at offset 0. This proves that the RLM
//!   copy task's `CopySegment*` events round-trip through
//!   `__remote_log_metadata` over the loopback.
//! * [`delete_records_puts_the_tiered_prefix_out_of_range_and_frees_it`][]:
//!   KIP-405's two floors. A tiered offset reads back, then `DeleteRecords`
//!   moves the global floor past it and the same read becomes
//!   `OFFSET_OUT_OF_RANGE` while the expiration pass frees the remote bytes
//!   below the new floor.
//! * [`an_idle_tiered_partition_leaves_local_disk_through_its_last_record`][]:
//!   the active segment of a partition that stops taking writes rolls once
//!   it breaches `local.retention.ms`, so `ListOffsets(EARLIEST_LOCAL)`
//!   reaches the log end, as it does on Kafka.
//!
//! [`topic_rlmm_activates_against_loopback`]: rlmm_loopback::topic_rlmm_activates_against_loopback
//! [`topic_rlmm_copy_then_fetch_round_trip`]: rlmm_loopback::topic_rlmm_copy_then_fetch_round_trip
//! [`delete_records_puts_the_tiered_prefix_out_of_range_and_frees_it`]: rlmm_delete_records::delete_records_puts_the_tiered_prefix_out_of_range_and_frees_it
//! [`an_idle_tiered_partition_leaves_local_disk_through_its_last_record`]: rlmm_idle_partition::an_idle_tiered_partition_leaves_local_disk_through_its_last_record
#![cfg(test)]

mod support;
use {
    crate::support::tiered as topic_fixture,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::fetch_response::PartitionData,
    tempfile::TempDir,
    tokio::runtime::{Builder, Runtime},
};

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `tiered_storage_topic_rlmm/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
#[path = "tiered_storage_topic_rlmm/rlmm_cluster.rs"]
mod rlmm_cluster;
#[path = "tiered_storage_topic_rlmm/rlmm_delete_records.rs"]
mod rlmm_delete_records;
#[path = "tiered_storage_topic_rlmm/rlmm_idle_partition.rs"]
mod rlmm_idle_partition;
#[path = "tiered_storage_topic_rlmm/rlmm_loopback.rs"]
mod rlmm_loopback;
#[path = "tiered_storage_topic_rlmm/rlmm_not_ready.rs"]
mod rlmm_not_ready;
#[path = "tiered_storage_topic_rlmm/rlmm_round_trip.rs"]
mod rlmm_round_trip;
#[path = "tiered_storage_topic_rlmm/rlmm_sasl.rs"]
mod rlmm_sasl;

use std::{
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard, OnceLock},
    time::Duration,
};

/// One bounded broker runtime, serialized through the same gate as legacy cases.
struct TopicRlmmScenario {
    runtime: Runtime,
    guard: MutexGuard<'static, ()>,
    cluster: Option<(krabka_broker::BrokerHandle, TempDir, TempDir)>,
    client: Option<Client>,
    first_value: rstest_bdd::Slot<Option<bytes::Bytes>>,
    topic: String,
    tiered_before: Vec<PathBuf>,
    delete_outcomes: Vec<(i16, i64)>,
    refused: Option<PartitionData>,
    above: rstest_bdd::Slot<Option<Vec<u8>>>,
}

#[rstest::fixture]
fn topic_rlmm() -> TopicRlmmScenario {
    let guard = broker_test_lock()
        .lock()
        .expect("broker test lock should not be poisoned");
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("broker test runtime");
    TopicRlmmScenario {
        runtime,
        guard,
        cluster: None,
        client: None,
        first_value: rstest_bdd::Slot::default(),
        topic: String::new(),
        tiered_before: Vec::new(),
        delete_outcomes: Vec::new(),
        refused: None,
        above: rstest_bdd::Slot::default(),
    }
}

impl TopicRlmmScenario {
    fn start_active_plaintext(&mut self) {
        self.cluster = Some(
            self.runtime
                .block_on(rlmm_cluster::start_broker_with_topic_rlmm()),
        );
        self.runtime
            .block_on(rlmm_cluster::await_activation(self.broker()));
        self.client = Some(
            self.runtime
                .block_on(rlmm_cluster::build_client(self.broker())),
        );
    }

    fn broker(&self) -> &BrokerHandle { &self.cluster.as_ref().expect("topic RLMM broker").0 }

    fn remote_dir(&self) -> &Path { self.cluster.as_ref().expect("topic RLMM broker").2.path() }

    fn client(&self) -> &Client { self.client.as_ref().expect("topic RLMM client") }

    fn shutdown(self) {
        let Self {
            runtime,
            guard,
            cluster,
            client,
            ..
        } = self;
        drop(client);
        let (broker, log_dir, remote_dir) = cluster.expect("topic RLMM broker");
        runtime.block_on(broker.shutdown());
        runtime.shutdown_timeout(Duration::from_secs(5));
        drop((log_dir, remote_dir));
        drop(guard);
    }
}

fn broker_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
