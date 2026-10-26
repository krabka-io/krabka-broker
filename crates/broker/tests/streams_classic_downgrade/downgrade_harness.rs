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
