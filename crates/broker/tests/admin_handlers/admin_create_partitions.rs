//! `CreatePartitions` (`api_key` 37): extending a topic's partition count, both
//! with broker-chosen placement and with an explicit `assignments` list, and
//! the `INVALID_REPLICA_ASSIGNMENT` path that must add no partition at all.

use assert2::assert;
use krabka_metadata::{BrokerConfigRecord, MetadataRecord};
use krabka_protocol::owned::{
    create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
};

use crate::{
    admin_harness::{build_client, create_topic_helper},
    support::{start_n_node, start_n_node_with_retry, wait_for_all_brokers_registered},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_placement_excludes_a_fenced_broker_for_create_and_expand() {
    let cluster = start_n_node_with_retry(3).await;
    wait_for_all_brokers_registered(&cluster, 3).await;
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    broker
        .submit_metadata_record_for_test(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_broker::NodeId(3),
            config_name: "broker.fenced".into(),
            config_value: Some("true".into()),
        }))
        .await
        .unwrap();
    broker
        .wait_for_image(|_| broker.fenced_broker_ids_for_test().contains(&3))
        .await;

    let created = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "t-usable-brokers".into(),
                num_partitions: 1,
                replication_factor: 2,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(created.topics[0].error_code == 0);
    broker
        .wait_until_partition_present("t-usable-brokers", 0)
        .await;

    let expanded = client
        .send(CreatePartitionsRequest {
            topics: vec![CreatePartitionsTopic {
                name: "t-usable-brokers".into(),
                count: 3,
                assignments: None,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(expanded.results[0].error_code == 0);

    for partition in 0..3 {
        broker
            .wait_until_partition_present("t-usable-brokers", partition)
            .await;
        let record = broker
            .partition_record_for_test("t-usable-brokers", partition)
            .unwrap();
        assert!(!record.replicas.contains(&krabka_broker::NodeId(3)));
        assert!(!record.isr.contains(&krabka_broker::NodeId(3)));
    }

    let rejected = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "t-too-many-replicas".into(),
                num_partitions: 1,
                replication_factor: 3,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(rejected.topics[0].error_code == 38);
}

/// `CreatePartitions`: a request that extends a 1-partition topic to 3
/// returns `error_code == 0`. All three partitions then materialise in the
/// broker's local registry within a few seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_partitions_extends_topic() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-cp", 1).await;

    let req = CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: "t-cp".into(),
            count: 3,
            assignments: None,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("create_partitions");
    assert!(
        resp.results[0].error_code == 0,
        "create_partitions result: {:?}",
        resp.results[0].error_message
    );

    // Wait for the supervisor reconcile to materialise all three partitions.
    for p in 0..3 {
        broker.wait_until_partition_present("t-cp", p).await;
    }
}

/// `CreatePartitions`: explicit `assignments` list. The topic's rf is 1 on a
/// single-broker cluster, and the operator pins the new partition to
/// broker 0. The handler must accept it (`error_code` == 0) and materialise
/// the partition. A second call with a wrong-length assignment list must
/// return `INVALID_REPLICA_ASSIGNMENT` (39).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_partitions_honors_explicit_assignments() {
    use krabka_protocol::owned::create_partitions_request::CreatePartitionsAssignment;

    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-cpa", 1).await;

    // Happy path: 1 existing partition → 2 partitions, explicit assignment
    // pins broker 0 (the only one available).
    let req = CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: "t-cpa".into(),
            count: 2,
            assignments: Some(vec![CreatePartitionsAssignment {
                broker_ids: vec![1],
                ..Default::default()
            }]),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let resp = client
        .send(req)
        .await
        .expect("create_partitions (explicit)");
    assert!(
        resp.results[0].error_code == 0,
        "explicit assignment must succeed: {:?}",
        resp.results[0].error_message
    );

    // Wait for the new partition to materialise.
    broker.wait_until_partition_present("t-cpa", 1).await;

    // Invalid path: ask for 1 more partition (total 3) but supply 2
    // assignments. Must surface INVALID_REPLICA_ASSIGNMENT and NOT add a
    // partition.
    let bad = CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: "t-cpa".into(),
            count: 3,
            assignments: Some(vec![
                CreatePartitionsAssignment {
                    broker_ids: vec![1],
                    ..Default::default()
                },
                CreatePartitionsAssignment {
                    broker_ids: vec![1],
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let bad_resp = client
        .send(bad)
        .await
        .expect("create_partitions (length-mismatch)");
    assert!(
        bad_resp.results[0].error_code == 39,
        "length-mismatch must return INVALID_REPLICA_ASSIGNMENT (39): {:?}",
        bad_resp.results[0].error_message
    );
    assert!(
        !broker.partition_exists_for_test("t-cpa", 2),
        "partition 2 must NOT have been created on an INVALID_REPLICA_ASSIGNMENT path",
    );
}
