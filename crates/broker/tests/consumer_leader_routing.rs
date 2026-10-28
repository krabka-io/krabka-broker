//! Multi-broker integration coverage for the native `Consumer`'s per-leader
//! RPC routing.
//!
//! The native consumer used to send every data-plane RPC over the bootstrap
//! connection, both `Fetch` and `OffsetForLeaderEpoch`. On a multi-broker
//! cluster that behavior misroutes `Fetch`. At rf=1, a partition whose leader
//! is *not* the bootstrap broker holds **no replica at all**, so a bootstrap-routed
//! `Fetch` gets `NOT_LEADER_OR_FOLLOWER` and delivers nothing. The consumer
//! now groups fetchable partitions by leader with `Client::broker(id)`, so
//! records flow from every leader whatever broker the consumer bootstrapped at.
//!
//! **Why rf=1 matters for test validity:** with rf=3, the bootstrap broker holds
//! a follower replica of every partition, and Krabka serves consumer fetches
//! from any local replica up to the high-watermark. There is no leadership gate.
//! A bootstrap-only consumer would still succeed in that setup, and the test
//! would be hollow. With rf=1 each partition lives on exactly ONE broker. If the
//! consumer does not route to that broker it gets nothing.
//!
//! Windows-gated like the other multi-broker tests, because openraft's
//! `debug_assert!` races on the hosted Windows scheduler.
#![cfg(test)]

use {
    crate::support::{
        client::connect_with_context,
        consumer_groups::{collect_routing_values, routing_consumer},
        partitions::{require_non_bootstrap_partitions, start_leader_routing_cluster},
        produce::{SinglePartitionProduceSetup, batch_request},
        records::value_record,
        shutdown_cluster,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_client_consumer::AutoOffsetReset,
    krabka_client_core::Client,
    krabka_protocol::{primitives::uuid::Uuid, records::RecordBatch},
    std::{
        collections::{HashMap, HashSet, hash_map::Entry},
        time::Duration,
    },
    support::cluster_lock,
    tokio::time,
};

mod support;

/// Produce one single-record batch to a partition on the broker that owns it.
///
/// The function retries the metadata-apply race, that is,
/// `UNKNOWN_TOPIC_OR_PARTITION` = 3 and `NOT_LEADER_OR_FOLLOWER` = 6.
async fn produce_one(client: &Client, topic: &str, topic_id: Uuid, partition: i32, value: &str) {
    let mut batch = RecordBatch::default();
    batch
        .records
        .push(value_record(0, Some(Bytes::from(value.to_owned()))));
    batch.last_offset_delta = 0;
    for attempt in 1..=10 {
        let resp = client
            .send(batch_request(batch.clone(), SinglePartitionProduceSetup {
                topic: (topic).into(),
                topic_id,
                partition: krabka_ids::PartitionIndex(partition),
                ..SinglePartitionProduceSetup::replicated()
            }))
            .await
            .expect("produce");
        let err = resp.responses[0].partition_responses[0].error_code;
        if err == 0 {
            return;
        }
        // 3 = UNKNOWN_TOPIC_OR_PARTITION, 6 = NOT_LEADER_OR_FOLLOWER,
        // 100 = UNKNOWN_TOPIC_ID: all appear transiently while metadata — the
        // topic's existence, its leadership, and its topic-id mapping — fans
        // out across brokers right after CreateTopics. The producer sends by
        // topic_id, so a target leader that hasn't yet applied the topic record
        // answers UNKNOWN_TOPIC_ID until the metadata image catches up.
        if (err == 3 || err == 6 || err == 100) && attempt < 10 {
            // intentional: retry backoff between produce attempts while the
            // topic's existence / leadership / topic-id mapping fans out across
            // brokers — there is no single broker signal to await here (the
            // target leader varies per partition), so back off and re-send.
            time::sleep(Duration::from_millis(150)).await;
            continue;
        }
        panic!("produce to {topic}-{partition} failed after {attempt} attempt(s): code {err}");
    }
}

use {
    krabka_broker::{BrokerConfig, BrokerHandle},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
    tokio::sync::MutexGuard,
};

#[derive(Default)]
struct LeaderRouting {
    guard: Option<MutexGuard<'static, ()>>,
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    bootstrap: String,
    topic: String,
    non_bootstrap_partitions: Vec<i32>,
    seen: Option<HashSet<String>>,
}

#[fixture]
fn routing() -> LeaderRouting {
    // Start this scenario with fresh inputs and observations.

    LeaderRouting {
        guard: None,
        cluster: Vec::new(),
        bootstrap: String::new(),
        topic: String::new(),
        non_bootstrap_partitions: Vec::new(),
        seen: None,
    }
}

#[given(
    "a three-broker topic {topic} with six sole-replica partitions containing their pPARTITION values"
)]
async fn distributed_records(routing: &mut LeaderRouting, topic: String) {
    routing.guard = Some(cluster_lock().lock().await);
    let (cluster, bootstrap, topic_id, n_partitions) = start_leader_routing_cluster(&topic).await;
    let non_bootstrap_partitions =
        require_non_bootstrap_partitions(&cluster[0].0, &topic, n_partitions);
    let mut producers: HashMap<u64, Client> = HashMap::new();
    for partition in 0..n_partitions {
        let leader = cluster[0]
            .0
            .partition_leader_for_test(&topic, partition)
            .expect("partition has a leader");
        if let Entry::Vacant(entry) = producers.entry(leader) {
            let addr = cluster
                .iter()
                .find(|(handle, _, _)| handle.node_id() == leader)
                .map(|(_, config, _)| config.listen_addr.to_string())
                .expect("leader node is in cluster");
            entry.insert(connect_with_context(addr, None, "producer client").await);
        }
        produce_one(
            &producers[&leader],
            &topic,
            topic_id,
            partition,
            &format!("p{partition}"),
        )
        .await;
    }
    routing.cluster = cluster;
    routing.bootstrap = bootstrap;
    routing.topic = topic;
    routing.non_bootstrap_partitions = non_bootstrap_partitions;
}

#[when("an earliest-offset native consumer subscribes through broker one with group {group}")]
async fn consume(routing: &mut LeaderRouting, group: String) {
    let mut consumer = routing_consumer(
        &routing.bootstrap,
        "routing-consumer-rf1",
        &group,
        &routing.topic,
        AutoOffsetReset::Earliest,
    )
    .await;
    routing.seen = Some(collect_routing_values(&mut consumer, 6, |_, _| {}).await);
    consumer.close().await.unwrap();
}

#[then("records {values} arrive, including a partition with no replica on the bootstrap broker")]
fn every_leader(routing: &LeaderRouting, values: String) {
    assert!(!routing.non_bootstrap_partitions.is_empty());
    let expected: HashSet<String> = values.split(',').map(str::to_owned).collect();
    let seen = routing.seen.as_ref().unwrap();
    assert!(
        *seen == expected,
        "missing: {:?}; non-bootstrap partitions: {:?}",
        expected.difference(seen).collect::<Vec<_>>(),
        routing.non_bootstrap_partitions
    );
}

impl LeaderRouting {
    async fn shutdown(self) {
        let Self { cluster, guard, .. } = self;
        shutdown_cluster(cluster).await;
        drop(guard);
    }
}

#[scenario(
    path = "tests/features/migrated/tests/consumer_leader_routing.feature",
    name = "consumer fetches from non bootstrap leaders"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_fetches_from_non_bootstrap_leaders(routing: LeaderRouting) {
    routing.shutdown().await;
}
