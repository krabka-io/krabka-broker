//! Cluster, topic, and membership plumbing that every KIP-932 consume test in
//! this binary shares. It starts an in-process broker whose share-coordinator
//! state topic has a single partition, creates a data topic and resolves its
//! id, produces records into it, joins a share group, and waits until the
//! group lifecycle has durably initialized the share state a consume needs.

// These single-broker tests only need one state partition. Keeping the test
// geometry small also prevents the parallel test runner from exhausting its
// process-wide file-descriptor limit while eleven brokers run concurrently.
/// Wait until the group-coordinator lifecycle hook has durably initialized the
/// share state for `(group, topic, partition)`. The persister summary then
/// becomes present. Until that happens the share coordinator is not yet
/// write-ready, and a consume's SPSO advance would not persist.
///
/// The lifecycle hook fires on each heartbeat, so this helper drives
/// steady-state heartbeats inside the wait loop rather than sleeping. It mirrors
/// the `lifecycle_initializes_share_state` pattern in `share_groups.rs`.
pub use crate::support::share::wait_for_share_init;
pub use crate::support::share::{
    bootstrap_share_state, broker_config, broker_test_permit, join, produce_n, produce_values,
    topic_id, wire,
};

crate::share_consumption_fixture!(
    /// Initialize the common g1/t fixture before its first consume.
    initialize_consumption,
    join,
    |broker, client, member, epoch, tid| wait_for_share_init(broker, client, &member, epoch, tid)
);

/// Owners and initialized membership for a fresh g1/t consume session.
pub struct ConsumptionFixture {
    pub _permit: tokio::sync::OwnedSemaphorePermit,
    pub broker: krabka_broker::BrokerHandle,
    pub client: std::sync::Arc<krabka_client_core::Client>,
    pub _dir: tempfile::TempDir,
    pub tid: uuid::Uuid,
    pub member: String,
}

impl ConsumptionFixture {
    pub fn session(&self) -> crate::support::share::ShareSessionSetup<'_> {
        crate::support::share::ShareSessionSetup::joined(&self.member, self.tid)
    }
}

pub async fn consumption_fixture(records: i64) -> ConsumptionFixture {
    let (permit, broker, client, dir, tid) =
        crate::support::share::permitted_topic_fixture("t", 1, |_| {}).await;
    let (member, _) = initialize_consumption(&broker, &client, tid, records).await;
    ConsumptionFixture {
        _permit: permit,
        broker,
        client,
        _dir: dir,
        tid,
        member,
    }
}
