//! Multi-broker integration coverage for the native `Producer`'s per-leader
//! Produce routing.
//!
//! The native producer used to send every `ProduceRequest` over the bootstrap
//! connection. On a multi-broker cluster that behavior misroutes writes. At
//! rf=1, a partition whose leader is *not* the bootstrap broker holds **no
//! replica at all**, so a bootstrap-routed Produce gets
//! `UNKNOWN_TOPIC_OR_PARTITION` (3) and the broker never stores the record. The
//! producer now caches each partition's leader from `Metadata`, groups its
//! drained batches by leader, and sends one `ProduceRequest` per leader with
//! `Client::broker(id)`. Writes then land on the correct broker whatever broker
//! the producer bootstrapped at.
//!
//! **Why rf=1 matters for test validity:** with rf=1 each partition lives on
//! exactly ONE broker. A producer that only talks to the bootstrap broker
//! cannot store records for partitions led by the other two nodes, because the
//! bootstrap broker has no replica to accept them. With rf=3 the bootstrap
//! broker would hold a replica of every partition and could mask the misroute,
//! and the test would be hollow. rf=1 is what makes this test discriminating.
//!
//! Windows-gated like the other multi-broker tests, because openraft's
//! `debug_assert!` races on the hosted Windows scheduler.
#![cfg(test)]

use {
    crate::support::{
        consumer_groups::{collect_routing_values, routing_consumer},
        partitions::{
            require_non_bootstrap_partitions, start_leader_routing_cluster, wait_for_local_replica,
        },
        producer::producer_record,
        shutdown_cluster,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::{BrokerConfig, BrokerHandle},
    krabka_client_consumer::AutoOffsetReset,
    krabka_client_producer::{Acks, Producer},
    std::{
        collections::{HashMap, HashSet},
        mem,
        time::Duration,
    },
    support::cluster_lock,
    tempfile::TempDir,
    tokio::sync::MutexGuard,
};

mod support;

type RoutingCluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;
struct Routing {
    guard: Option<MutexGuard<'static, ()>>,
    cluster: RoutingCluster,
    bootstrap: String,
    topic: String,
    producer: Option<Producer>,
    non_bootstrap: Vec<i32>,
    acknowledgements: Vec<(i32, i32)>,
    seen: HashSet<String>,
    seen_partitions: HashMap<i32, String>,
}
#[rstest::fixture]
fn routing() -> Routing {
    // Start this scenario with fresh inputs and observations.

    Routing {
        guard: None,
        cluster: Vec::new(),
        bootstrap: String::new(),
        topic: String::new(),
        producer: None,
        non_bootstrap: Vec::new(),
        acknowledgements: Vec::new(),
        seen: HashSet::new(),
        seen_partitions: HashMap::new(),
    }
}
#[rstest_bdd_macros::given(
    "topic {topic} has six single-replica partitions across three brokers including non-bootstrap leaders"
)]
async fn distributed_partitions(routing: &mut Routing, topic: String) {
    routing.guard = Some(cluster_lock().lock().await);
    let (cluster, bootstrap, _, partitions) = start_leader_routing_cluster(&topic).await;
    for partition in 0..partitions {
        let leader = cluster[0]
            .0
            .partition_leader_for_test(&topic, partition)
            .expect("leader");
        wait_for_local_replica(
            &cluster
                .iter()
                .find(|(h, _, _)| h.node_id() == leader)
                .expect("leader broker")
                .0,
            &topic,
            partition,
        )
        .await;
    }
    routing.non_bootstrap = require_non_bootstrap_partitions(&cluster[0].0, &topic, partitions);
    routing.cluster = cluster;
    routing.bootstrap = bootstrap;
    routing.topic = topic;
}
#[rstest_bdd_macros::given(
    "one idempotent producer bootstraps only at broker 1 with all acknowledgements"
)]
async fn bootstrap_producer(routing: &mut Routing) {
    routing.producer = Some(
        Producer::builder()
            .bootstrap(routing.bootstrap.clone())
            .client_id("routing-producer-rf1")
            .enable_idempotence(true)
            .acks(Acks::All)
            .linger(Duration::from_millis(5))
            .retries(8)
            .retry_backoff(Duration::from_millis(150))
            .build()
            .await
            .expect("producer"),
    );
}
#[rstest_bdd_macros::when("that producer enqueues and flushes these partition-pinned records")]
async fn write_records(routing: &mut Routing, datatable: Vec<Vec<String>>) {
    let mut receivers = Vec::new();
    for row in datatable.into_iter().skip(1) {
        let [partition, value]: [String; 2] = row.try_into().expect("partition and value");
        let partition: i32 = partition.parse().expect("partition index");
        let receiver = routing
            .producer
            .as_ref()
            .expect("producer")
            .enqueue(
                producer_record()
                    .topic(routing.topic.clone())
                    .maybe_partition(Some(partition))
                    .maybe_value(Some(Some(Bytes::from(value))))
                    .call(),
            )
            .await
            .expect("enqueue");
        receivers.push((partition, receiver));
    }
    routing
        .producer
        .as_ref()
        .expect("producer")
        .flush()
        .await
        .expect("flush");
    for (partition, receiver) in receivers {
        let metadata = receiver
            .await
            .unwrap_or_else(|error| panic!("partition {partition} failed: {error:?}"));
        routing
            .acknowledgements
            .push((partition, metadata.partition));
    }
}
#[rstest_bdd_macros::then("all {count} records are acknowledged for their requested partitions")]
fn acknowledgements(routing: &Routing, count: usize) {
    assert!(routing.acknowledgements.len() == count);
    assert!(
        routing
            .acknowledgements
            .iter()
            .all(|(requested, actual)| requested == actual)
    );
}
#[rstest_bdd_macros::when(
    "an earliest native consumer reads all six records through partition leaders"
)]
async fn read_records(routing: &mut Routing) {
    let mut consumer = routing_consumer(
        &routing.bootstrap,
        "routing-consumer-rf1",
        "routing-producer-grp-rf1",
        &routing.topic,
        AutoOffsetReset::Earliest,
    )
    .await;
    routing.seen = collect_routing_values(&mut consumer, 6, |partition, value| {
        routing.seen_partitions.insert(partition, value);
    })
    .await;
    consumer.close().await.expect("consumer close");
}
#[rstest_bdd_macros::then(
    "every partition including non-bootstrap partitions contains these exact values"
)]
fn durable_values(routing: &Routing, datatable: Vec<Vec<String>>) {
    let expected: HashMap<i32, String> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [partition, value]: [String; 2] = row.try_into().expect("partition value");
            (partition.parse().expect("partition"), value)
        })
        .collect();
    assert!(routing.seen == expected.values().cloned().collect::<HashSet<_>>());
    assert!(routing.seen_partitions == expected);
    for partition in &routing.non_bootstrap {
        assert!(routing.seen_partitions.get(partition) == expected.get(partition));
    }
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/producer_leader_routing.feature",
    name = "A bootstrap producer stores writes on every partition leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_routes_to_non_bootstrap_leaders(routing: Routing) {
    routing
        .producer
        .take()
        .expect("producer")
        .close()
        .await
        .expect("producer close");
    shutdown_cluster(mem::take(&mut routing.cluster)).await;
    drop(routing);
}
