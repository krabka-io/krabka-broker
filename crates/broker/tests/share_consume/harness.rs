//! Cluster, topic, and membership plumbing that every KIP-932 consume test in
//! this binary shares. It starts an in-process broker whose share-coordinator
//! state topic has a single partition, creates a data topic and resolves its
//! id, produces records into it, joins a share group, and waits until the
//! group lifecycle has durably initialized the share state a consume needs.

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;

pub use crate::support::share::{
    bootstrap_share_state, broker_config, broker_test_permit, connect, create_topic, join,
    produce_n, produce_values, topic_id, wire,
};

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
pub async fn wait_for_share_init(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    member_id: &str,
    member_epoch: i32,
    tid: uuid::Uuid,
) {
    let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            // Send a steady-state heartbeat to trigger the lifecycle hook.
            let _ = client
                .send(ShareGroupHeartbeatRequest {
                    group_id: "g1".into(),
                    member_id: member_id.into(),
                    member_epoch,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    ..Default::default()
                })
                .await;
            if broker
                .share_state_summary_for_test("g1", tid, 0)
                .await
                .is_some()
            {
                return;
            }
        }
    })
    .await;
    assert!(
        res.is_ok(),
        "share state for g1:{tid}:0 never initialized within 30s"
    );
}

/// Initialize the common g1/t fixture before its first consume.
pub async fn initialize_consumption(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    tid: uuid::Uuid,
    records: i64,
) -> (String, i32) {
    bootstrap_share_state(broker, client, "g1").await;
    produce_n(client, "t", tid, 0, records).await;
    let (member, epoch) = join(client, "g1", "t").await;
    wait_for_share_init(broker, client, &member, epoch, tid).await;
    (member, epoch)
}
