//! Cluster boot, topic setup, and offset helpers shared by every test in this
//! suite.
//!
//! The module holds the one-broker in-process cluster and its client, the
//! `CreateTopics` and `UpdateFeatures` calls that a streams group needs before
//! it can form, the `Metadata` lookup that resolves a topic id, the
//! simple-consumer `OffsetCommit` that seeds the offset a downgrade must
//! preserve, and the `BrokerConfig` that restarts a broker on an existing log
//! directory.

pub use crate::support::{
    client::create_topic,
    streams::{boot_electable as boot, connect_member_client as connect, finalize_streams_version},
    topic_id_for,
};
use {
    crate::{
        ERR_NONE,
        support::{
            heartbeat_config,
            offsets::{offset_commit_partition, offset_commit_topic},
        },
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, BrokerConfig},
    krabka_client_core::Client,
    krabka_protocol::{
        owned::offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition},
        primitives::uuid::Uuid as WireUuid,
    },
    std::path::PathBuf,
};

/// Commits an offset as the streams member `member_id` at `member_epoch`, as
/// a Streams client does. Kafka's `StreamsGroup.validateOffsetCommit` refuses
/// a commit with no member on a group that has members.
pub async fn commit_offset_as_member(
    client: &Client,
    group_id: &str,
    (member_id, member_epoch): (&str, i32),
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    offset: i64,
) {
    let cr = client
        .send(OffsetCommitRequest {
            group_id: group_id.into(),
            generation_id_or_member_epoch: member_epoch,
            member_id: member_id.into(),
            topics: vec![offset_commit_topic(topic, topic_id, vec![
                OffsetCommitRequestPartition {
                    committed_leader_epoch: 0,
                    ..offset_commit_partition(partition, offset, Some(String::new()))
                },
            ])],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(
        cr.topics[0].partitions[0].error_code == ERR_NONE,
        "OffsetCommit (streams member) failed: {cr:?}"
    );
}

pub fn rejoin_config(log_dir: PathBuf) -> BrokerConfig {
    let mut cfg = heartbeat_config(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}

use {
    crate::{
        CONVERGE_TRIES,
        downgrade_streams_join::{streams_join_and_converge, topology},
    },
    krabka_broker::{Broker, BrokerHandle, coordinator::unified::GroupType},
    krabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::sync::Arc,
    tempfile::TempDir,
};

/// Independent protocol clients and original streams identity for a downgrade.
#[derive(Default)]
pub struct DowngradeScenario {
    pub broker: Option<BrokerHandle>,
    pub directory: Option<TempDir>,
    pub classic: Option<Arc<Client>>,
    pub streams: Option<Arc<Client>>,
    pub topic: String,
    pub group: String,
    pub topic_id: Option<WireUuid>,
    pub member: String,
    pub epoch: i32,
    pub heartbeat: Option<StreamsGroupHeartbeatResponse>,
}

#[fixture]
pub fn downgrade_case() -> DowngradeScenario {
    // Keep the live streams identity until the classic protocol replaces its group.
    DowngradeScenario::default()
}

impl DowngradeScenario {
    pub fn broker(&self) -> &BrokerHandle { self.broker.as_ref().expect("downgrade broker") }

    pub fn classic(&self) -> &Client { self.classic.as_ref().expect("classic client") }

    pub fn streams(&self) -> &Client { self.streams.as_ref().expect("streams client") }

    pub async fn shutdown(&mut self) {
        self.broker
            .take()
            .expect("downgrade broker")
            .shutdown()
            .await;
        self.classic.take();
        self.streams.take();
        self.directory.take();
    }
}

#[given("a downgrade broker holds one-partition topic {topic} for group {group}")]
async fn downgrade_broker(downgrade_case: &mut DowngradeScenario, topic: String, group: String) {
    let (broker, bootstrap, directory) = boot().await;
    let streams = connect(&bootstrap).await;
    let classic = connect(&bootstrap).await;
    create_topic(&streams, &topic, 1).await;
    finalize_streams_version(&streams).await;
    downgrade_case.topic_id = Some(topic_id_for(&streams, &topic).await);
    downgrade_case.broker = Some(broker);
    downgrade_case.directory = Some(directory);
    downgrade_case.streams = Some(streams);
    downgrade_case.classic = Some(classic);
    downgrade_case.topic = topic;
    downgrade_case.group = group;
}

#[given("a durable downgrade broker holds topic in4 for group g4")]
async fn durable_downgrade_broker(downgrade_case: &mut DowngradeScenario) {
    let directory = TempDir::new().unwrap();
    let broker = Broker::start(heartbeat_config(directory.path().to_path_buf()))
        .await
        .unwrap();
    broker.wait_until_broker_alive(broker.node_id()).await;
    broker.wait_until_group_coordinator_ready().await;
    let bootstrap = broker.listen_addr().to_string();
    let streams = connect(&bootstrap).await;
    let classic = connect(&bootstrap).await;
    finalize_streams_version(&streams).await;
    create_topic(&streams, "in4", 1).await;
    downgrade_case.topic_id = Some(topic_id_for(&streams, "in4").await);
    downgrade_case.broker = Some(broker);
    downgrade_case.directory = Some(directory);
    downgrade_case.streams = Some(streams);
    downgrade_case.classic = Some(classic);
    downgrade_case.topic = "in4".into();
    downgrade_case.group = "g4".into();
}

#[when("a streams member joins the downgrade group and converges on its single task")]
async fn live_downgrade_streams_member(downgrade_case: &mut DowngradeScenario) {
    let (member, response) = streams_join_and_converge(
        downgrade_case.streams(),
        &downgrade_case.group,
        topology(&downgrade_case.topic),
        1,
        CONVERGE_TRIES,
    )
    .await;
    downgrade_case.member = member;
    downgrade_case.epoch = response.member_epoch;
    downgrade_case.heartbeat = Some(response);
}

#[then("the downgrade group has a healthy live Streams member")]
async fn healthy_downgrade_streams_member(downgrade_case: &DowngradeScenario) {
    let response = downgrade_case
        .heartbeat
        .as_ref()
        .expect("streams join answered");
    assert!(response.error_code == ERR_NONE, "{response:?}");
    downgrade_case
        .broker()
        .wait_until_group_type(&downgrade_case.group, GroupType::Streams)
        .await;
    assert!(
        downgrade_case
            .broker()
            .group_type_for_test(&downgrade_case.group)
            == Some(GroupType::Streams)
    );
}
