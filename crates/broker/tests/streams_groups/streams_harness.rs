//! Broker boot, feature-finalization and heartbeat helpers shared by the
//! KIP-1071 streams-group scenarios in this suite.
//!
//! Every scenario boots a single-node broker, finalizes `streams.version` to
//! level 1 so the streams handlers stop returning `UNSUPPORTED_VERSION`, and
//! then drives `StreamsGroupHeartbeat` until the coordinator hands out active
//! tasks. Those steps, and the small accessors that read an assignment out of a
//! heartbeat response, live here so each scenario module holds only its own
//! assertions.

pub use crate::support::streams::{
    active_partition_count, finalize_streams_version, first_join, follow_up, topology,
};
use {
    crate::support::streams::{ElectionReadiness, boot as boot_streams, streams_join_and_converge},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        streams_group_describe_request::StreamsGroupDescribeRequest,
        streams_group_describe_response::StreamsGroupDescribeResponse,
        streams_group_heartbeat_request::Topology,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
};

pub async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    boot_streams(ElectionReadiness::CoordinatorReady).await
}

pub use crate::support::client::{connect_c1 as connect, create_topic};

/// Active-task partitions for a given subtopology id, sorted.
pub fn active_partitions_for(resp: &StreamsGroupHeartbeatResponse, sub: &str) -> Vec<i32> {
    let mut parts: Vec<i32> = resp.active_tasks.as_ref().map_or_default(|v| {
        v.iter()
            .filter(|t| t.subtopology_id == sub)
            .flat_map(|t| t.partitions.clone())
            .collect()
    });
    parts.sort_unstable();
    parts
}

/// The response status codes. In the KIP-1071 status enum, 3 is
/// `MISSING_INTERNAL_TOPICS`.
pub fn status_codes(resp: &StreamsGroupHeartbeatResponse) -> Vec<i8> {
    resp.status
        .as_ref()
        .map_or_default(|v| v.iter().map(|s| s.status_code).collect())
}

pub async fn describe(client: &Client, group: &str) -> StreamsGroupDescribeResponse {
    client
        .send(StreamsGroupDescribeRequest {
            group_ids: vec![group.into()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("StreamsGroupDescribe")
}

/// Drive a single member to its first join, then re-heartbeat until convergence
/// returns. The returned tuple is `(member_id, last_response)`.
pub async fn join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    streams_join_and_converge(client, group, topo, want_active, tries, true).await
}

use {
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::sync::Arc,
    tempfile::TempDir,
};

/// Live streams membership and its independently captured admin observations.
#[derive(Default)]
pub struct StreamsGroupScenario {
    pub broker: Option<BrokerHandle>,
    pub directory: Option<TempDir>,
    pub client: Option<Arc<Client>>,
    pub group: String,
    pub topic: String,
    pub member: String,
    pub heartbeat: Option<StreamsGroupHeartbeatResponse>,
    pub topology: Option<Topology>,
    pub described: Option<StreamsGroupDescribeResponse>,
}

#[fixture]
pub fn streams_group() -> StreamsGroupScenario {
    // Every scenario owns a fresh broker and the member identity returned by its coordinator.
    StreamsGroupScenario::default()
}

impl StreamsGroupScenario {
    pub fn client(&self) -> &Client { self.client.as_ref().expect("streams client") }

    pub async fn shutdown(&mut self) {
        self.broker.take().expect("streams broker").shutdown().await;
        self.client.take();
        self.directory.take();
    }
}

#[given(
    "a streams-enabled broker holds topic {topic} with {partitions} partitions for group {group}"
)]
async fn streams_group_topic(
    streams_group: &mut StreamsGroupScenario,
    topic: String,
    partitions: i32,
    group: String,
) {
    let (broker, bootstrap, directory) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, &topic, partitions).await;
    streams_group.broker = Some(broker);
    streams_group.directory = Some(directory);
    streams_group.client = Some(client);
    streams_group.group = group;
    streams_group.topic = topic;
}

#[when("a stateless streams member joins and converges on two active tasks")]
async fn join_stateless_streams_group(streams_group: &mut StreamsGroupScenario) {
    let (member, response) = join_and_converge(
        streams_group.client(),
        &streams_group.group,
        topology(&streams_group.topic, vec![]),
        2,
        10,
    )
    .await;
    streams_group.member = member;
    streams_group.heartbeat = Some(response);
}

#[then("the streams join succeeds with a nonempty member identity and positive epoch")]
fn joined_streams_group(streams_group: &StreamsGroupScenario) {
    let response = streams_group
        .heartbeat
        .as_ref()
        .expect("streams join answered");
    assert!(response.error_code == 0, "{response:?}");
    assert!(!streams_group.member.is_empty());
    assert!(response.member_epoch >= 1);
}

#[when("an administrator describes the streams group")]
async fn describe_streams_group(streams_group: &mut StreamsGroupScenario) {
    streams_group.described = Some(describe(streams_group.client(), &streams_group.group).await);
}
