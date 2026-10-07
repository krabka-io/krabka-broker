//! Shared cluster and share-consume helpers for the KIP-932 admin offset suite.
//!
//! The helpers start a single-broker test cluster, create a topic, produce
//! records, drive `ShareGroupHeartbeat` membership, and reuse the shared
//! `ShareFetch` and `ShareAcknowledge` fixtures. The error-code and
//! acknowledgement-type constants live here as well, since every admin surface
//! in this suite asserts on them.

use std::time::Duration;

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;

pub use crate::support::share::{
    ShareAck, acquired_count, bootstrap_share_state, broker_config, broker_test_permit, connect,
    create_topic, produce_n, share_ack,
};

pub const NONE: i16 = 0;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const NON_EMPTY_GROUP: i16 = 68;

pub const ACCEPT: i8 = 1;

pub async fn wait_for_share_init(
    broker: &krabka_broker::BrokerHandle,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) {
    // Delegates to the broker-handle awaiter (30s timeout, 25ms poll interval).
    // `join()` drives the steady-state heartbeats that trigger the lifecycle hook
    // before this is called, so no repeated heartbeats are needed here.
    broker
        .wait_for_share_state_summary(group, tid, partition)
        .await;
}

/// Joins `group` with a subscription to `topic`.
///
/// The function drives steady-state heartbeats, so the lifecycle hook
/// initializes the share state of the subscribed partitions. It returns
/// `(member_id, member_epoch)`, so the caller can leave with the live epoch.
pub async fn join(client: &Client, group: &str, topic: &str) -> (String, i32) {
    let (member_id, mut epoch) = crate::support::share::join(client, group, topic).await;

    for _ in 0..3 {
        let hb = client
            .send(ShareGroupHeartbeatRequest {
                group_id: group.into(),
                member_id: member_id.clone(),
                member_epoch: epoch,
                subscribed_topic_names: Some(vec![topic.into()]),
                ..Default::default()
            })
            .await
            .expect("ShareGroupHeartbeat steady-state");
        epoch = hb.member_epoch;
        // intentional: paces steady-state heartbeats to drive the membership
        // reconciliation / lifecycle hook forward; this drives the protocol rather
        // than waiting on a single observable state (share init is awaited separately).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    (member_id, epoch)
}

/// Leaves the group with `member_epoch == -1`.
///
/// The broker keeps the group but reports zero members, in state "Empty".
pub async fn leave(client: &Client, group: &str, member_id: &str) {
    let resp = client
        .send(ShareGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: member_id.into(),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .expect("ShareGroupHeartbeat leave");
    assert!(resp.error_code == 0, "leave failed: {:?}", resp.error_code);
}

crate::share_first_fetch_fixture!(fetch_until_acquired, false);

crate::share_consumption_fixture!(
    initialize_consumption,
    join,
    |broker, client, member, epoch, tid| wait_for_share_init(broker, "g1", tid, 0)
);

/// Initialize the persisted topic/group fixture before a restart scenario.
pub async fn initialized_topic(
    log_dir: std::path::PathBuf,
    records: i64,
) -> (
    krabka_broker::BrokerHandle,
    std::sync::Arc<Client>,
    uuid::Uuid,
    String,
) {
    let (broker, client, tid) =
        crate::support::share::start_topic(broker_config(log_dir), "t", 1).await;
    let (member, _) = initialize_consumption(&broker, &client, tid, records).await;
    (broker, client, tid, member)
}
