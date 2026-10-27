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
