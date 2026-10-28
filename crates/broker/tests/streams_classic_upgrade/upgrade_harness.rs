//! Error codes, broker boot and wire helpers shared by the classic-to-streams
//! upgrade scenarios in this suite.
//!
//! Both scenarios boot a single-node broker, finalize `streams.version`, create
//! the source topic and then read back topic ids or committed offsets, so those
//! steps live here rather than being repeated per scenario module.

use {
    crate::support::offsets::{offset_fetch_group, offset_fetch_request, offset_fetch_topic},
    assert2::assert,
    krabka_client_core::Client,
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
};

// ── error codes ──────────────────────────────────────────────────────────────
pub const ERR_NONE: i16 = 0;
pub const ERR_MEMBER_ID_REQUIRED: i16 = 79;
pub const ERR_GROUP_ID_NOT_FOUND: i16 = 69;

// ── boot / connect helpers ────────────────────────────────────────────────────

pub use crate::support::{
    client::create_topic,
    streams::{boot_electable as boot, connect_member_client as connect, finalize_streams_version},
    topic_id_for,
};

pub async fn assert_committed_offset(client: &Client, topic_id: WireUuid, expected: i64) {
    let response = client
        .send(offset_fetch_request(offset_fetch_group(
            "g",
            Some(vec![offset_fetch_topic("in", topic_id, vec![0])]),
        )))
        .await
        .expect("OffsetFetch");
    let group = response
        .groups
        .iter()
        .find(|g| g.group_id == "g")
        .expect("group g");
    let topic = group
        .topics
        .iter()
        .find(|t| t.topic_id == topic_id)
        .expect("topic in");
    let partition = topic.partitions.first().expect("partition 0");
    assert!(
        partition.error_code == ERR_NONE,
        "OffsetFetch failed: {partition:?}"
    );
    assert!(
        partition.committed_offset == expected,
        "committed offset was not preserved"
    );
}

use {
    krabka_broker::{BrokerHandle, coordinator::unified::GroupType},
    krabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    rstest::fixture,
    rstest_bdd_macros::{given, then},
    std::sync::Arc,
    tempfile::TempDir,
};

/// Independent classic and streams connections for an actual protocol conversion.
#[derive(Default)]
pub struct UpgradeScenario {
    pub broker: Option<BrokerHandle>,
    pub directory: Option<TempDir>,
    pub bootstrap: String,
    pub classic: Option<Arc<Client>>,
    pub streams: Option<Arc<Client>>,
    pub topic: String,
    pub group: String,
    pub topic_id: Option<WireUuid>,
    pub member: String,
    pub generation: i32,
    pub response: Option<StreamsGroupHeartbeatResponse>,
    pub committed: Option<i16>,
    pub left: Option<i16>,
}

#[fixture]
pub fn upgrade_case() -> UpgradeScenario {
    // Each conversion retains the original classic identity and committed topic offset.
    UpgradeScenario::default()
}

impl UpgradeScenario {
    pub fn classic(&self) -> &Client { self.classic.as_ref().expect("classic connection") }

    pub fn streams(&self) -> &Client { self.streams.as_ref().expect("streams connection") }

    pub fn broker(&self) -> &BrokerHandle { self.broker.as_ref().expect("upgrade broker") }

    pub async fn shutdown(&mut self) {
        self.broker.take().expect("upgrade broker").shutdown().await;
        self.classic.take();
        self.streams.take();
        self.directory.take();
    }
}

#[given("an upgrade broker holds one-partition topic {topic} for group {group}")]
async fn upgrade_broker(upgrade_case: &mut UpgradeScenario, topic: String, group: String) {
    let (broker, bootstrap, directory) = boot().await;
    let classic = connect(&bootstrap).await;
    let streams = connect(&bootstrap).await;
    finalize_streams_version(&classic).await;
    create_topic(&classic, &topic, 1).await;
    upgrade_case.topic_id = Some(topic_id_for(&classic, &topic).await);
    upgrade_case.broker = Some(broker);
    upgrade_case.bootstrap = bootstrap;
    upgrade_case.directory = Some(directory);
    upgrade_case.classic = Some(classic);
    upgrade_case.streams = Some(streams);
    upgrade_case.topic = topic;
    upgrade_case.group = group;
}

#[then("the upgrade group is still Classic-typed")]
async fn classic_upgrade_type(upgrade_case: &UpgradeScenario) {
    upgrade_case
        .broker()
        .wait_until_group_type(&upgrade_case.group, GroupType::Classic)
        .await;
    assert!(
        upgrade_case
            .broker()
            .group_type_for_test(&upgrade_case.group)
            == Some(GroupType::Classic)
    );
}
