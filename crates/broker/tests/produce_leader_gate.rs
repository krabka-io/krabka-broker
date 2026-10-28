//! Multi-broker coverage for the Produce leadership gate.
//!
//! Kafka semantics: only the partition LEADER accepts a Produce. The broker
//! must reject a Produce misrouted to a non-leader with
//! `NOT_LEADER_OR_FOLLOWER` (6). It must NOT append that Produce to a local
//! follower replica. The client then refreshes metadata and re-routes to the
//! real leader.
//!
//! Two discriminating cases:
//!
//!  * **rf=3 (follower replica present):** every broker hosts a replica, so a
//!    Produce to a non-leader lands on a *follower's* local log if the handler
//!    skips the leadership check. Before the fix, the broker silently appended
//!    to the follower. It returned `error_code=0`, the follower log grew, and
//!    the leader never saw the record. That is silent data loss. The fix
//!    returns 6 and leaves the follower log untouched.
//!
//!  * **rf=1 (no replica on the target):** the non-leader broker holds no
//!    replica at all. Before the fix this returned
//!    `UNKNOWN_TOPIC_OR_PARTITION` (3) because the local registry lookup
//!    missed. Kafka returns 6, because the partition exists cluster-wide and
//!    this broker just isn't its leader. The fix now returns 6 as well. It
//!    consults the metadata image rather than the local registry.
//!
//! The same Produce sent to the actual leader must succeed with code 0 and a
//! durably stored record. This proves the leader path is unaffected.
//!
//! These tests are Windows-gated like the other multi-broker tests. openraft
//! `debug_assert!` races on the hosted Windows scheduler.
#![cfg(test)]

use {
    crate::support::{
        client::connect_client,
        produce::{SinglePartitionProduceSetup, batch_request},
        records::{batch_from_records, value_record},
        registered_cluster, shutdown_cluster,
        topics::creatable_topic,
    },
    assert2::assert,
    krabka_broker::{BrokerConfig, BrokerHandle, NodeId},
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{create_topics_request::CreateTopicsRequest, produce_response::NodeEndpoint},
        primitives::uuid::Uuid as WireUuid,
        records::RecordBatch,
    },
    std::mem,
    support::cluster_lock,
    tempfile::TempDir,
    tokio::sync::MutexGuard,
};

mod support;

/// Serialize the multi-broker tests in this binary. Each one boots a 3-node
/// loopback cluster, and running them concurrently exhausts ephemeral ports
/// and starves openraft election timing. The reason is the same as for the
/// `cluster_lock` in `producer_leader_routing.rs`.
/// The listener the test clients connect on. A KIP-951 `NodeEndpoints` entry
/// advertises the address of the listener the request arrived on, so this is
/// the endpoint name the refusal must answer with.
const PLAINTEXT_LISTENER: &str = "PLAINTEXT";

fn one_record_batch(v: &str) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        ..batch_from_records(vec![value_record(
            0,
            Some(bytes::Bytes::from(v.to_owned())),
        )])
    }
}

/// What a produce answered with for its one partition.
struct ProduceAnswer {
    /// The partition's error code.
    error_code: i16,
    /// The leader the broker names in its `current_leader` hint.
    leader_hint: i32,
    /// The KIP-951 endpoints of the nodes the hint refers to.
    node_endpoints: Vec<NodeEndpoint>,
}

/// Send a single one-record Produce (`acks=1`) for `(topic, partition)` over
/// `client`'s bootstrap connection. That is, send it directly at whichever
/// broker the client was built against, and bypass any leader routing. Returns
/// the error code, the `current_leader` id and the node endpoints. The last is the KIP-951
/// companion to the hint: without it a Java producer cannot resolve the node id
/// the hint names and falls back to the full `Metadata` round-trip.
async fn produce_one(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    value: &str,
) -> ProduceAnswer {
    let resp = client
        .send(batch_request(
            one_record_batch(value),
            SinglePartitionProduceSetup {
                topic: (topic).into(),
                topic_id,
                partition: krabka_ids::PartitionIndex(partition),
                ..Default::default()
            },
        ))
        .await
        .expect("produce round-trip");
    let pr = &resp.responses[0].partition_responses[0];
    ProduceAnswer {
        error_code: pr.error_code,
        leader_hint: pr.current_leader.leader_id,
        node_endpoints: resp.node_endpoints.clone(),
    }
}

use crate::support::partitions::wait_for_local_replica;

#[derive(Default)]
struct ProduceLeadership {
    guard: Option<MutexGuard<'static, ()>>,
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    admin: Option<Client>,
    rf3_id: Option<WireUuid>,
    rf1_id: Option<WireUuid>,
    rf3_leader: Option<u64>,
    rf1_leader: Option<u64>,
    off_node_partition: Option<i32>,
    follower_index: Option<usize>,
    before: Option<i64>,
    after: Option<i64>,
    answer: Option<ProduceAnswer>,
}

#[rstest::fixture]
fn produce_leadership() -> ProduceLeadership {
    // The refusal and successful retry use the same topic identities and logs.
    ProduceLeadership::default()
}

impl ProduceLeadership {
    fn admin(&self) -> &Client { self.admin.as_ref().expect("node 1 client") }

    fn answer(&self) -> &ProduceAnswer { self.answer.as_ref().expect("Produce answered") }

    async fn shutdown(&mut self) {
        let Self {
            guard,
            cluster,
            admin,
            ..
        } = mem::take(self);
        admin.expect("node 1 client").close();
        shutdown_cluster(cluster).await;
        drop(guard);
    }
}

#[rstest_bdd_macros::given(
    "three registered brokers host gate-rf3 with one RF3 partition and gate-rf1 with six RF1 partitions, with every leader elected"
)]
async fn produce_gate_topics(produce_leadership: &mut ProduceLeadership) {
    produce_leadership.guard = Some(cluster_lock().lock().await);
    produce_leadership.cluster = registered_cluster(3).await;
    produce_leadership.admin = Some(
        connect_client(
            produce_leadership.cluster[0].1.listen_addr.to_string(),
            None,
        )
        .await,
    );
    let created = produce_leadership
        .admin()
        .send(CreateTopicsRequest {
            topics: vec![
                creatable_topic().name("gate-rf3".into()).replicas(3).call(),
                creatable_topic()
                    .name("gate-rf1".into())
                    .partitions(6)
                    .call(),
            ],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        created.topics.iter().all(|topic| topic.error_code == 0),
        "create: {created:?}"
    );
    produce_leadership.rf3_id = Some(
        created
            .topics
            .iter()
            .find(|topic| topic.name == "gate-rf3")
            .unwrap()
            .topic_id,
    );
    produce_leadership.rf1_id = Some(
        created
            .topics
            .iter()
            .find(|topic| topic.name == "gate-rf1")
            .unwrap()
            .topic_id,
    );
    produce_leadership.cluster[0]
        .0
        .wait_for_image(|image| {
            image
                .partition("gate-rf3", 0)
                .is_some_and(|partition| partition.leader != 0)
                && (0..6).all(|index| {
                    image
                        .partition("gate-rf1", index)
                        .is_some_and(|partition| partition.leader != 0)
                })
        })
        .await;
}

#[rstest_bdd_macros::when(
    "a one-record rf3-to-follower Produce is sent directly to a materialized RF3 follower and its log end is measured before and after"
)]
async fn produce_gate_follower_write(produce_leadership: &mut ProduceLeadership) {
    let leader = produce_leadership.cluster[0]
        .0
        .partition_leader_for_test("gate-rf3", 0)
        .unwrap();
    produce_leadership.rf3_leader = Some(leader);
    let follower = produce_leadership
        .cluster
        .iter()
        .position(|(broker, _, _)| broker.node_id() != leader)
        .expect("RF3 follower");
    produce_leadership.follower_index = Some(follower);
    wait_for_local_replica(&produce_leadership.cluster[follower].0, "gate-rf3", 0).await;
    produce_leadership.before = Some(
        produce_leadership.cluster[follower]
            .0
            .local_log_end_offset("gate-rf3", 0)
            .expect("follower hosts gate-rf3"),
    );
    let client = connect_client(
        produce_leadership.cluster[follower]
            .1
            .listen_addr
            .to_string(),
        None,
    )
    .await;
    produce_leadership.answer = Some(
        produce_one(
            &client,
            "gate-rf3",
            produce_leadership.rf3_id.expect("RF3 topic"),
            0,
            "rf3-to-follower",
        )
        .await,
    );
    client.close();
    produce_leadership.after = Some(
        produce_leadership.cluster[follower]
            .0
            .local_log_end_offset("gate-rf3", 0)
            .expect("follower hosts gate-rf3"),
    );
}

#[rstest_bdd_macros::then(
    "the RF3 follower returns NOT_LEADER_OR_FOLLOWER 6 with the real leader id and its complete PLAINTEXT endpoint, and its local log does not grow"
)]
fn refused_produce_gate_follower(produce_leadership: &ProduceLeadership) {
    let answer = produce_leadership.answer();
    let leader = produce_leadership.rf3_leader.expect("RF3 leader");
    assert!(
        answer.error_code == 6,
        "RF3 follower must refuse Produce, got {}",
        answer.error_code
    );
    assert!(answer.leader_hint == i32::try_from(leader).unwrap());
    let image = produce_leadership.cluster
        [produce_leadership.follower_index.expect("RF3 follower")]
    .0
    .controller_image_for_test();
    let registration = image
        .broker(krabka_metadata::NodeId(leader))
        .expect("leader registered in follower image");
    let advertised = registration
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name == PLAINTEXT_LISTENER)
        .expect("leader PLAINTEXT endpoint");
    assert!(
        answer.node_endpoints
            == vec![NodeEndpoint {
                node_id: answer.leader_hint,
                host: advertised.host.clone(),
                port: i32::from(advertised.port),
                rack: registration.rack.clone(),
                ..Default::default()
            }],
        "the refused Produce must carry the leader's endpoint; got {:?}",
        answer.node_endpoints
    );
    assert!(
        produce_leadership.after == produce_leadership.before,
        "rejected Produce must NOT append to the follower's local log: before={:?} after={:?}",
        produce_leadership.before,
        produce_leadership.after
    );
}

#[rstest_bdd_macros::when(
    "node 1 receives rf1-to-nonreplica for an RF1 partition led elsewhere and hosted on no local replica"
)]
async fn produce_gate_nonreplica_write(produce_leadership: &mut ProduceLeadership) {
    let node = produce_leadership.cluster[0].0.node_id();
    let partition = (0..6)
        .find(|&partition| {
            produce_leadership.cluster[0]
                .0
                .partition_leader_for_test("gate-rf1", partition)
                .is_some_and(|leader| leader != node)
        })
        .expect("RF1 partition led off node 1");
    produce_leadership.off_node_partition = Some(partition);
    produce_leadership.rf1_leader = produce_leadership.cluster[0]
        .0
        .partition_leader_for_test("gate-rf1", partition);
    assert!(
        produce_leadership.cluster[0]
            .0
            .local_log_end_offset("gate-rf1", partition)
            .is_none(),
        "test premise: node1 must NOT host gate-rf1/{partition} (rf=1)"
    );
    produce_leadership.answer = Some(
        produce_one(
            produce_leadership.admin(),
            "gate-rf1",
            produce_leadership.rf1_id.expect("RF1 topic"),
            partition,
            "rf1-to-nonreplica",
        )
        .await,
    );
}

#[rstest_bdd_macros::then(
    "the non-replica returns NOT_LEADER_OR_FOLLOWER 6 and hints the actual RF1 leader"
)]
fn refused_produce_gate_nonreplica(produce_leadership: &ProduceLeadership) {
    assert!(produce_leadership.answer().error_code == 6);
    assert!(
        produce_leadership.answer().leader_hint
            == i32::try_from(produce_leadership.rf1_leader.expect("RF1 leader")).unwrap()
    );
}

#[rstest_bdd_macros::when(
    "the RF3 leader installs its leadership and receives rf3-to-leader with its log end measured before and after"
)]
async fn produce_gate_rf3_leader_write(produce_leadership: &mut ProduceLeadership) {
    let leader = produce_leadership.rf3_leader.expect("RF3 leader");
    let index = produce_leadership
        .cluster
        .iter()
        .position(|(broker, _, _)| broker.node_id() == leader)
        .unwrap();
    produce_leadership.cluster[index]
        .0
        .wait_until_local_partition_leader("gate-rf3", 0, NodeId(leader))
        .await;
    produce_leadership.before = Some(
        produce_leadership.cluster[index]
            .0
            .local_log_end_offset("gate-rf3", 0)
            .expect("leader hosts gate-rf3"),
    );
    let client = connect_client(
        produce_leadership.cluster[index].1.listen_addr.to_string(),
        None,
    )
    .await;
    produce_leadership.answer = Some(
        produce_one(
            &client,
            "gate-rf3",
            produce_leadership.rf3_id.expect("RF3 topic"),
            0,
            "rf3-to-leader",
        )
        .await,
    );
    client.close();
    produce_leadership.after = Some(
        produce_leadership.cluster[index]
            .0
            .local_log_end_offset("gate-rf3", 0)
            .expect("leader hosts gate-rf3"),
    );
}

#[rstest_bdd_macros::then("the RF3 leader returns NONE 0 and appends exactly one record")]
fn accepted_produce_gate_rf3_leader(produce_leadership: &ProduceLeadership) {
    assert!(produce_leadership.answer().error_code == 0);
    assert!(produce_leadership.after == produce_leadership.before.map(|offset| offset + 1));
}

#[rstest_bdd_macros::when("the RF1 leader installs its leadership and receives rf1-to-leader")]
async fn produce_gate_rf1_leader_write(produce_leadership: &mut ProduceLeadership) {
    let leader = produce_leadership.rf1_leader.expect("RF1 leader");
    let partition = produce_leadership
        .off_node_partition
        .expect("RF1 partition");
    let index = produce_leadership
        .cluster
        .iter()
        .position(|(broker, _, _)| broker.node_id() == leader)
        .unwrap();
    produce_leadership.cluster[index]
        .0
        .wait_until_local_partition_leader("gate-rf1", partition, NodeId(leader))
        .await;
    let client = connect_client(
        produce_leadership.cluster[index].1.listen_addr.to_string(),
        None,
    )
    .await;
    produce_leadership.answer = Some(
        produce_one(
            &client,
            "gate-rf1",
            produce_leadership.rf1_id.expect("RF1 topic"),
            partition,
            "rf1-to-leader",
        )
        .await,
    );
    client.close();
    produce_leadership.after = Some(
        produce_leadership.cluster[index]
            .0
            .local_log_end_offset("gate-rf1", partition)
            .expect("leader hosts gate-rf1"),
    );
}

#[rstest_bdd_macros::then("the RF1 leader returns NONE 0 and durably holds at least one record")]
fn accepted_produce_gate_rf1_leader(produce_leadership: &ProduceLeadership) {
    assert!(produce_leadership.answer().error_code == 0);
    assert!(produce_leadership.after.expect("RF1 log end") >= 1);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/produce_leader_gate.feature",
    name = "produce to non leader is rejected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn produce_to_non_leader_is_rejected(produce_leadership: ProduceLeadership) {
    produce_leadership.shutdown().await;
    drop(produce_leadership);
}
