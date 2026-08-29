//! The one request fixture shared by the `TxnOffsetCommit` unit tests: a
//! two-partition commit for a single topic, which both the response builders
//! and the `__consumer_offsets` append are checked against.

use krabka_protocol::owned::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};

pub(super) fn request() -> TxnOffsetCommitRequest {
    TxnOffsetCommitRequest {
        transactional_id: "tid".into(),
        group_id: "group-a".into(),
        producer_id: 47,
        producer_epoch: 5,
        topics: vec![TxnOffsetCommitRequestTopic {
            name: "orders".into(),
            partitions: vec![
                TxnOffsetCommitRequestPartition {
                    partition_index: 2,
                    committed_offset: 103,
                    committed_leader_epoch: 7,
                    committed_metadata: Some("first".into()),
                    ..Default::default()
                },
                TxnOffsetCommitRequestPartition {
                    partition_index: 3,
                    committed_offset: 107,
                    committed_leader_epoch: 8,
                    committed_metadata: Some("second".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}
