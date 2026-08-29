//! Slice-48f broker integration: the topic-backed
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
//!
//! [`topic_rlmm_activates_against_loopback`]: rlmm_loopback::topic_rlmm_activates_against_loopback
//! [`topic_rlmm_copy_then_fetch_round_trip`]: rlmm_loopback::topic_rlmm_copy_then_fetch_round_trip

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `tiered_storage_topic_rlmm/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
#[path = "tiered_storage_topic_rlmm/rlmm_cluster.rs"]
mod rlmm_cluster;
#[path = "tiered_storage_topic_rlmm/rlmm_loopback.rs"]
mod rlmm_loopback;
#[path = "tiered_storage_topic_rlmm/rlmm_not_ready.rs"]
mod rlmm_not_ready;
#[path = "tiered_storage_topic_rlmm/rlmm_round_trip.rs"]
mod rlmm_round_trip;
#[path = "tiered_storage_topic_rlmm/rlmm_sasl.rs"]
mod rlmm_sasl;

use std::{
    future::Future,
    sync::{Mutex, OnceLock},
    time::Duration,
};

fn run_broker_test(test: impl Future<Output = ()>) {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("broker test lock should not be poisoned");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("broker test runtime");
    runtime.block_on(test);
    // Broker shutdown closes the listeners and async tasks, but some blocking
    // helpers can outlive the test body. A plain Runtime drop waits for those
    // helpers forever and prevents the serialized test cases from advancing.
    // Keep the lock held through a bounded shutdown so teardown cannot overlap
    // the next broker instance while still guaranteeing forward progress.
    runtime.shutdown_timeout(Duration::from_secs(5));
}
