//! `DescribeQuorum` (`api_key` 55, KIP-595): the dispatch glue, the ACL allow
//! path, and the response encoding, driven against the `__cluster_metadata`
//! topic of a one-broker cluster.

use assert2::{assert, check};
use krabka_protocol::owned::describe_quorum_request::{
    DescribeQuorumRequest, PartitionData as DescribeQuorumReqPartition,
    TopicData as DescribeQuorumReqTopic,
};

use crate::{admin_harness::build_client, support::start_n_node};

/// `DescribeQuorum` against the cluster-metadata topic on a 1-broker
/// cluster returns one partition row carrying the broker's voter id with
/// `leader_id` == 1. This test verifies the dispatch glue, the ACL allow
/// path, and the response encoding. The pure `build_topic_responses` helper
/// has its own unit tests in
/// `crates/broker/src/handlers/describe_quorum.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_quorum_reports_cluster_metadata_voter_set() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    let req = DescribeQuorumRequest {
        topics: vec![DescribeQuorumReqTopic {
            topic_name: "__cluster_metadata".into(),
            partitions: vec![DescribeQuorumReqPartition {
                partition_index: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = client.send(req).await.expect("describe_quorum");
    check!(resp.error_code == 0, "top-level error_code");
    assert!(resp.topics.len() == 1, "exactly one topic row");
    check!(resp.topics[0].topic_name == "__cluster_metadata");
    let pd = &resp.topics[0].partitions[0];
    check!(pd.partition_index == 0);
    check!(pd.error_code == 0, "metadata partition 0 succeeds");
    check!(
        pd.leader_id == 1,
        "1-broker cluster: bootstrap voter id=1 is leader"
    );
    check!(
        pd.leader_epoch >= 1,
        "openraft term must be >= 1 once a leader is elected; got {}",
        pd.leader_epoch,
    );
    check!(
        pd.high_watermark >= 0,
        "last_applied_index is non-negative once any record applies; got {}",
        pd.high_watermark,
    );
    assert!(
        pd.current_voters.len() == 1,
        "single voter for 1-broker cluster"
    );
    check!(pd.current_voters[0].replica_id == 1);
    check!(
        pd.current_voters[0].log_end_offset >= 0,
        "leader knows its own matched index; got {}",
        pd.current_voters[0].log_end_offset,
    );
    check!(
        pd.observers.is_empty(),
        "Krabka has no observer-role concept"
    );
}
