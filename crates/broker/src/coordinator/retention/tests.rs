//! Behavioural tests for the KIP-211 sweep, driven against a running broker.
//!
//! Each test commits through the real `OffsetCommit` handler, so the offsets
//! land in a real `__consumer_offsets` partition, and reads back through the
//! real `OffsetFetch` handler. The sweep's clock is a parameter, so a test
//! moves time forward by passing a later `now_ms` rather than sleeping.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_protocol::owned::{
    leave_group_request::LeaveGroupRequest,
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    offset_commit_response::OffsetCommitResponse,
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic},
    offset_fetch_response::OffsetFetchResponse,
};
use krabka_units::mebibytes;
use tokio::sync::oneshot;

use super::sweep;
use crate::{
    broker::Broker,
    codes,
    coordinator::{
        bootstrap::OFFSETS_TOPIC,
        partitioner::partition_for_group,
        persistence::{Key, OffsetCommitValue, parse_key},
        unified::{
            actor::{GroupActorMessage, GroupKindTag},
            classic_state::{ClassicGroup, GroupState, Member},
            group::{CoordinatorGroup, GroupKind},
        },
    },
    test_support::{
        decode_response, encode_request, peer, principal, request_context,
        start_broker_with_authorizer_no_audit,
    },
};

const GROUP: &str = "retention-group";
const TOPIC: &str = "orders";
const MEMBER: &str = "m1";
const GENERATION: i32 = 1;
/// `retention_time_ms` reaches the broker at v2-v4 only, so the commits here
/// run at v4.
const COMMIT_VERSION: i16 = 4;
/// The legacy single-group `OffsetFetch` path.
const FETCH_VERSION: i16 = 7;
/// One minute of retention keeps the arithmetic readable.
const RETENTION_MS: i64 = 60_000;

async fn start() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    start_broker_with_authorizer_no_audit(Arc::new(crate::authorizer::AllowAllAuthorizer)).await
}

/// Install a `Stable` classic group holding one member, so a commit fences
/// against a real membership and a leave really empties the group.
fn seed_group_with_member(broker: &Broker) {
    let mut state = ClassicGroup::new(GROUP);
    state.protocol_type = Some("consumer".into());
    state.add_member(Member::new(
        MEMBER,
        "client",
        "127.0.0.1",
        Duration::from_secs(30),
        Duration::from_mins(1),
        vec![("range".into(), bytes::Bytes::new())],
    ));
    state.state = GroupState::Stable;
    state.generation_id = GENERATION;
    broker.group_coordinator.seed_classic(
        GROUP,
        Box::new(CoordinatorGroup {
            group_id: GROUP.into(),
            kind: GroupKind::Classic(state),
            committed_offsets: std::collections::HashMap::new(),
            empty_since_ms: None,
        }),
    );
}

/// Commit one offset through the `OffsetCommit` handler.
async fn commit_offset(broker: &Broker, offset: i64, retention_time_ms: i64) {
    let request = OffsetCommitRequest {
        group_id: GROUP.into(),
        generation_id_or_member_epoch: GENERATION,
        member_id: MEMBER.into(),
        retention_time_ms,
        topics: vec![OffsetCommitRequestTopic {
            name: TOPIC.into(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: offset,
                committed_leader_epoch: -1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let principal = principal("admin");
    let peer = peer();
    let ctx = request_context(&principal, &peer, "consumer");
    let bytes = crate::handlers::offset_commit::handle(
        broker,
        COMMIT_VERSION,
        1,
        &encode_request(&request, COMMIT_VERSION),
        &ctx,
    )
    .await
    .expect("OffsetCommit");
    let response: OffsetCommitResponse = decode_response(&bytes, COMMIT_VERSION);
    let code = response.topics[0].partitions[0].error_code;
    assert!(code == codes::NONE, "commit failed with error_code {code}");
}

/// The committed offset the `OffsetFetch` handler reports, `-1` for none.
async fn fetched_offset(broker: &Broker) -> i64 {
    let request = OffsetFetchRequest {
        group_id: GROUP.into(),
        topics: Some(vec![OffsetFetchRequestTopic {
            name: TOPIC.into(),
            partition_indexes: vec![0],
            ..Default::default()
        }]),
        ..Default::default()
    };
    let principal = principal("admin");
    let peer = peer();
    let ctx = request_context(&principal, &peer, "consumer");
    let bytes = crate::handlers::offset_fetch::handle(
        broker,
        FETCH_VERSION,
        2,
        &encode_request(&request, FETCH_VERSION),
        &ctx,
    )
    .await
    .expect("OffsetFetch");
    let response: OffsetFetchResponse = decode_response(&bytes, FETCH_VERSION);
    response.topics[0].partitions[0].committed_offset
}

/// Remove the group's last member through the classic `LeaveGroup` path.
async fn remove_last_member(broker: &Broker) {
    let handle = broker
        .group_coordinator
        .find(GROUP)
        .expect("seeded group actor");
    let (reply, result) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicLeave {
            req: LeaveGroupRequest {
                group_id: GROUP.into(),
                member_id: MEMBER.into(),
                ..Default::default()
            },
            version: 0,
            reply,
        })
        .await
        .expect("send ClassicLeave");
    result.await.expect("LeaveGroup reply");
}

/// Every record the group's `__consumer_offsets` partition holds, as
/// `(key, value)` with a `None` value for a tombstone.
fn offsets_log_records(broker: &Broker) -> Vec<(Key, Option<bytes::Bytes>)> {
    let partition_id = partition_for_group(&broker.controller.current_image(), GROUP);
    let partition = broker
        .partitions
        .get(OFFSETS_TOPIC, PartitionIndex(partition_id))
        .expect("offsets partition is materialized");
    let log = partition.log.lock().expect("log lock");
    let mut next = log.log_start_offset();
    let end = log.log_end_offset();
    let mut out = Vec::new();
    while next < end {
        let read = log.read(next, mebibytes(1)).expect("read offsets log");
        if read.batches.is_empty() {
            break;
        }
        for batch in &read.batches {
            for record in &batch.records {
                if let Some(key) = &record.key
                    && let Ok(key) = parse_key(key)
                {
                    out.push((key, record.value.clone()));
                }
            }
            next = krabka_log::Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
    }
    out
}

fn committed_offset_key() -> Key {
    Key::OffsetCommit {
        group_id: GROUP.into(),
        topic: TOPIC.into(),
        partition: 0,
    }
}

/// `true` when the log holds a null-valued record for `key`.
fn has_tombstone(records: &[(Key, Option<bytes::Bytes>)], key: &Key) -> bool {
    records
        .iter()
        .any(|(logged, value)| logged == key && value.is_none())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_group_loses_its_offsets_after_the_retention() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    seed_group_with_member(&broker);
    commit_offset(&broker, 42, -1).await;
    assert!(fetched_offset(&broker).await == 42);

    remove_last_member(&broker).await;

    let now_ms = crate::time_util::now_ms() + RETENTION_MS + 1;
    let swept = sweep(&broker.group_coordinator, |_| true, now_ms, RETENTION_MS).await;

    assert!(
        swept
            == vec![(
                GROUP.to_string(),
                super::ReapOutcome {
                    reaped: vec![(TOPIC.to_string(), 0)],
                    group_deleted: true,
                },
            )]
    );
    // The group left the directory, rather than merely being emptied. Check it
    // before the fetch, which re-creates an actor for an unknown id.
    check!(broker.group_coordinator.find(GROUP).is_none());
    check!(fetched_offset(&broker).await == -1);
    let records = offsets_log_records(&broker);
    check!(has_tombstone(&records, &committed_offset_key()));
    // The group held nothing else, so its own record went in the same pass.
    check!(has_tombstone(
        &records,
        &Key::GroupMetadata {
            group_id: GROUP.into()
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_group_keeps_its_offsets_across_the_same_interval() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    seed_group_with_member(&broker);
    commit_offset(&broker, 42, -1).await;

    // The same clock reading that reaped the empty group above, and then some.
    let now_ms = crate::time_util::now_ms() + RETENTION_MS * 10;
    let swept = sweep(&broker.group_coordinator, |_| true, now_ms, RETENTION_MS).await;

    assert!(swept.is_empty());
    check!(fetched_offset(&broker).await == 42);
    check!(!has_tombstone(
        &offsets_log_records(&broker),
        &committed_offset_key()
    ));
    check!(broker.group_coordinator.find(GROUP).is_some());
}

/// KIP-211: `retention_time_ms` on a v2-v4 commit overrides the broker's
/// `offsets.retention.minutes` for that one commit. The sweep runs with a
/// broker retention far longer than the per-commit value, so only a broker
/// that honours the request expires the offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_commit_retention_time_expires_before_the_broker_default() {
    const PER_COMMIT_MS: i64 = 1_000;
    const BROKER_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    seed_group_with_member(&broker);
    commit_offset(&broker, 42, PER_COMMIT_MS).await;
    remove_last_member(&broker).await;

    // The record carries the absolute expiry the request asked for.
    let records = offsets_log_records(&broker);
    let value = records
        .iter()
        .rev()
        .find_map(|(key, value)| (*key == committed_offset_key()).then_some(value.as_ref()?))
        .expect("committed offset record");
    let decoded = OffsetCommitValue::decode_value(value).expect("decode value");
    let expire_timestamp_ms = decoded
        .expire_timestamp_ms
        .expect("per-commit retention is persisted");
    check!(expire_timestamp_ms == decoded.commit_timestamp_ms + PER_COMMIT_MS);

    // Just before the per-commit expiry nothing goes, even though it is far
    // past nothing at all.
    let before = sweep(
        &broker.group_coordinator,
        |_| true,
        expire_timestamp_ms - 1,
        BROKER_RETENTION_MS,
    )
    .await;
    check!(before.is_empty());

    let after = sweep(
        &broker.group_coordinator,
        |_| true,
        expire_timestamp_ms,
        BROKER_RETENTION_MS,
    )
    .await;
    check!(after.len() == 1);
    check!(fetched_offset(&broker).await == -1);
}

/// A group whose `__consumer_offsets` partition this broker does not lead is
/// left alone: the leader sweeps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_this_broker_does_not_own_is_not_swept() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    seed_group_with_member(&broker);
    commit_offset(&broker, 42, -1).await;
    remove_last_member(&broker).await;

    let now_ms = crate::time_util::now_ms() + RETENTION_MS + 1;
    let swept = sweep(&broker.group_coordinator, |_| false, now_ms, RETENTION_MS).await;

    assert!(swept.is_empty());
    check!(fetched_offset(&broker).await == 42);
}

/// A streams group parks its committed offsets on a classic `groups` entry
/// while its members live in a streams actor, so the sweep must not read that
/// entry's emptiness as "nobody is using these offsets".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_streams_group_offset_home_is_not_swept() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    seed_group_with_member(&broker);
    commit_offset(&broker, 42, -1).await;
    remove_last_member(&broker).await;
    // Lock the id to the streams namespace, as a KIP-1071 group would.
    let _ = broker.group_coordinator.get_or_create_streams(GROUP);

    let now_ms = crate::time_util::now_ms() + RETENTION_MS + 1;
    let swept = sweep(&broker.group_coordinator, |_| true, now_ms, RETENTION_MS).await;

    assert!(swept.is_empty());
    check!(fetched_offset(&broker).await == 42);
}

/// The sweep only touches groups that hold an actor, and an unknown id is not
/// one, so a broker with no groups writes nothing at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_with_no_groups_sweeps_nothing() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    let _ = broker
        .group_coordinator
        .get_or_create_group(GROUP, GroupKindTag::Classic);

    let now_ms = crate::time_util::now_ms() + RETENTION_MS + 1;
    let swept = sweep(&broker.group_coordinator, |_| true, now_ms, RETENTION_MS).await;

    assert!(swept.is_empty());
}
