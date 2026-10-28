//! `CreatePartitions` (`api_key` 37): extending a topic's partition count, both
//! with broker-chosen placement and with an explicit `assignments` list, and
//! the `INVALID_REPLICA_ASSIGNMENT` path that must add no partition at all.

use {
    crate::{
        admin_harness::{AdminCluster, admin, create_topic_helper},
        support::topics::{creatable_topic, create_topic_request},
    },
    assert2::assert,
    krabka_broker::NodeId,
    krabka_protocol::owned::{
        create_partitions_request::{
            CreatePartitionsAssignment, CreatePartitionsRequest, CreatePartitionsTopic,
        },
        create_partitions_response::CreatePartitionsResponse,
        create_topics_response::CreateTopicsResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

fn create_partitions_request(
    name: &str,
    count: i32,
    assignments: Option<Vec<CreatePartitionsAssignment>>,
) -> CreatePartitionsRequest {
    CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: name.into(),
            count,
            assignments,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    }
}

struct PartitionGrowth {
    name: String,
    fenced: Option<NodeId>,
    created: Option<CreateTopicsResponse>,
    expanded: Option<CreatePartitionsResponse>,
}

#[fixture]
fn growth() -> PartitionGrowth {
    // Start this scenario with fresh inputs and observations.

    PartitionGrowth {
        name: String::new(),
        fenced: None,
        created: None,
        expanded: None,
    }
}

#[given("three brokers are registered and one non-controller peer is fenced")]
async fn fenced_peer(admin: &mut AdminCluster, growth: &mut PartitionGrowth) {
    growth.fenced = Some(admin.fenced_three_brokers().await);
}

#[given("topic {name} starts with one partition")]
async fn original_partition(admin: &AdminCluster, growth: &mut PartitionGrowth, name: String) {
    create_topic_helper(admin.client(), &name, 1).await;
    growth.name = name;
}

#[when("the admin creates topic {name} with replication factor {replicas}")]
async fn placement_topic(
    admin: &AdminCluster,
    growth: &mut PartitionGrowth,
    name: String,
    replicas: i16,
) {
    growth.created = Some(
        admin
            .client()
            .send(create_topic_request(
                creatable_topic()
                    .name(name.clone())
                    .replicas(replicas)
                    .call(),
            ))
            .await
            .expect("CreateTopics for placement"),
    );
    growth.name = name;
}

#[then("the placement topic creation succeeds and partition 0 exists")]
async fn placement_created(admin: &AdminCluster, growth: &PartitionGrowth) {
    let response = growth
        .created
        .as_ref()
        .expect("a placement topic was created");
    assert!(response.topics[0].error_code == 0);
    admin
        .broker()
        .wait_until_partition_present(&growth.name, 0)
        .await;
}

#[when("the admin grows the topic to {count} partitions with automatic placement")]
async fn automatic_growth(admin: &AdminCluster, growth: &mut PartitionGrowth, count: i32) {
    growth.expanded = Some(
        admin
            .client()
            .send(create_partitions_request(&growth.name, count, None))
            .await
            .expect("create_partitions"),
    );
}

#[when(
    "the admin grows the topic to {count} partitions with {assignments} assignments to broker 1"
)]
async fn explicit_growth(
    admin: &AdminCluster,
    growth: &mut PartitionGrowth,
    count: i32,
    assignments: usize,
) {
    let request = create_partitions_request(
        &growth.name,
        count,
        Some(vec![
            CreatePartitionsAssignment {
                broker_ids: vec![1],
                ..Default::default()
            };
            assignments
        ]),
    );
    growth.expanded = Some(
        admin
            .client()
            .send(request)
            .await
            .expect("create_partitions (explicit)"),
    );
}

#[then("CreatePartitions succeeds")]
fn expansion_accepted(growth: &PartitionGrowth) {
    let response = growth
        .expanded
        .as_ref()
        .expect("partition growth was requested");
    assert!(
        response.results[0].error_code == 0,
        "create_partitions result: {:?}",
        response.results[0].error_message
    );
}

#[then("all {count} partitions materialize locally")]
async fn materialized_partitions(admin: &AdminCluster, growth: &PartitionGrowth, count: i32) {
    for partition in 0..count {
        admin
            .broker()
            .wait_until_partition_present(&growth.name, partition)
            .await;
    }
}

#[then("none of the 3 partitions assigns the fenced broker as a replica or ISR member")]
async fn fenced_peer_avoided(admin: &AdminCluster, growth: &PartitionGrowth) {
    let broker = admin.broker();
    let fenced = growth.fenced.expect("a fenced non-controller peer");
    for partition in 0..3 {
        broker
            .wait_until_partition_present(&growth.name, partition)
            .await;
        let record = broker
            .partition_record_for_test(&growth.name, partition)
            .unwrap();
        assert!(!record.replicas.contains(&fenced));
        assert!(!record.isr.contains(&fenced));
    }
}

#[then("the fenced replica is last and remains outside the ISR and leadership")]
fn fenced_peer_last(admin: &AdminCluster, growth: &PartitionGrowth) {
    let fenced = growth.fenced.expect("a fenced non-controller peer");
    let record = admin
        .broker()
        .partition_record_for_test(&growth.name, 0)
        .unwrap();
    assert!(record.replicas.len() == 3);
    assert!(record.replicas[2] == fenced);
    assert!(record.isr == record.replicas[..2]);
    assert!(record.leader == record.replicas[0]);
}

#[then("CreateTopics rejects the replication factor with INVALID_REPLICATION_FACTOR")]
fn impossible_replication(growth: &PartitionGrowth) {
    assert!(
        growth
            .created
            .as_ref()
            .expect("a topic creation was attempted")
            .topics[0]
            .error_code
            == 38
    );
}

#[then("CreatePartitions rejects the assignment count with INVALID_REPLICA_ASSIGNMENT")]
fn mismatched_assignments(growth: &PartitionGrowth) {
    let response = growth
        .expanded
        .as_ref()
        .expect("partition growth was requested");
    assert!(
        response.results[0].error_code == 39,
        "length-mismatch must return INVALID_REPLICA_ASSIGNMENT (39): {:?}",
        response.results[0].error_message
    );
}

#[then("partition 2 has not been created")]
fn no_extra_partition(admin: &AdminCluster, growth: &PartitionGrowth) {
    assert!(
        !admin.broker().partition_exists_for_test(&growth.name, 2),
        "partition 2 must NOT have been created on an INVALID_REPLICA_ASSIGNMENT path"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_create_partitions.feature",
    name = "automatic placement takes a fenced broker only as a last resort"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_placement_takes_a_fenced_broker_only_as_a_last_resort(
    admin: AdminCluster,
    growth: PartitionGrowth,
) {
    admin.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_create_partitions.feature",
    name = "create partitions extends topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_partitions_extends_topic(admin: AdminCluster, growth: PartitionGrowth) {
    admin.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_create_partitions.feature",
    name = "create partitions honors explicit assignments"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_partitions_honors_explicit_assignments(
    admin: AdminCluster,
    growth: PartitionGrowth,
) {
    admin.shutdown().await;
}
