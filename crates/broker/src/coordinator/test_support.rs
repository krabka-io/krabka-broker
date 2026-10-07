//! Protocol fixtures shared by coordinator retention and deletion tests.

use krabka_protocol::owned::{
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic},
};

use crate::{
    broker::Broker,
    test_support::{peer, principal, request_context},
};

pub(crate) fn commit_request(group: &str, topic: &str, offset: i64) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id: group.into(),
        generation_id_or_member_epoch: -1,
        topics: vec![OffsetCommitRequestTopic {
            name: topic.into(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: offset,
                committed_leader_epoch: -1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(crate) async fn fetch_offset(broker: &Broker, group: &str, topic: &str, version: i16) -> i64 {
    let request = OffsetFetchRequest {
        group_id: group.into(),
        topics: Some(vec![OffsetFetchRequestTopic {
            name: topic.into(),
            partition_indexes: vec![0],
            ..Default::default()
        }]),
        ..Default::default()
    };
    let principal = principal("admin");
    let peer = peer();
    let context = request_context(&principal, &peer, "consumer");
    let response = crate::handlers::offset_fetch::handle(broker, request, version, &context)
        .await
        .expect("OffsetFetch");
    response.topics[0].partitions[0].committed_offset
}

pub(crate) struct CommitWaitCase {
    pub what: &'static str,
    pub hw_now: i64,
    pub moves_to: Option<(u64, i32)>,
    pub hw_later: Option<i64>,
    pub timeout: std::time::Duration,
    pub expected: CommitWaitOutcome,
}

#[derive(Clone, Copy)]
pub(crate) enum CommitWaitOutcome {
    Committed,
    NotLeader,
    TimedOut,
}

pub(crate) fn commit_wait_cases() -> [CommitWaitCase; 6] {
    use std::time::Duration;

    use CommitWaitOutcome::{Committed, NotLeader, TimedOut};
    let long = Duration::from_secs(30);
    [
        CommitWaitCase {
            what: "already committed",
            hw_now: 2,
            moves_to: None,
            hw_later: None,
            timeout: long,
            expected: Committed,
        },
        CommitWaitCase {
            what: "committed when the followers catch up",
            hw_now: 0,
            moves_to: None,
            hw_later: Some(2),
            timeout: long,
            expected: Committed,
        },
        CommitWaitCase {
            what: "another broker takes the partition first",
            hw_now: 0,
            moves_to: Some((2, 1)),
            hw_later: None,
            timeout: long,
            expected: NotLeader,
        },
        CommitWaitCase {
            what: "this broker leads again, at a newer epoch",
            hw_now: 0,
            moves_to: Some((1, 1)),
            hw_later: None,
            timeout: long,
            expected: NotLeader,
        },
        CommitWaitCase {
            what: "the high watermark passes the records after the partition moved",
            hw_now: 0,
            moves_to: Some((2, 1)),
            hw_later: Some(2),
            timeout: long,
            expected: NotLeader,
        },
        CommitWaitCase {
            what: "the followers never catch up",
            hw_now: 0,
            moves_to: None,
            hw_later: None,
            timeout: Duration::from_millis(100),
            expected: TimedOut,
        },
    ]
}
