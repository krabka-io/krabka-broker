//! End-to-end coverage for the data-bearing witness role (KFC-2).
//!
//! A witness replicates partition data and votes in `KRaft`, so it is an ISR
//! member and counts toward `min.insync.replicas`. It serves no client traffic
//! and it never leads a partition. Those two halves are what this suite pins
//! down on a live three-site cluster:
//!
//! * **Visible.** The witness registers like any other broker, rack and all.
//!   `kafka-topics` and `kafka-reassign-partitions` render replica ids through
//!   `Metadata.brokers[]`, so a witness that vanished from that list would turn
//!   every admin tool's replica column into an unresolved id.
//! * **In the replica set and in the ISR.** That membership is the whole point
//!   of the role: it is what keeps `acks=all` writable after a site loss.
//! * **Closed to clients.** A `Produce` or a consumer `Fetch` that reaches a
//!   witness gets `NOT_LEADER_OR_FOLLOWER`, the code that makes a Kafka client
//!   refresh its metadata and go elsewhere. A *follower* fetch still works,
//!   which is why the witness holds the data at all.
//! * **Never a read replica.** A KIP-392 consumer whose `client.rack` names the
//!   witness site is not redirected there, even though the witness is an
//!   in-ISR same-rack replica and therefore the most attractive candidate the
//!   rack-aware selector sees.
//! * **Read-only in the config surface.** `broker.witness` is controller-
//!   managed: `DescribeConfigs` reports it read-only and
//!   `IncrementalAlterConfigs` rejects it with `INVALID_CONFIG`.
//!
//! Every wait here is bounded. The shared `support` awaiters carry their own
//! 30s bound; the loops in this file wrap theirs in `tokio::time::timeout`, so
//! a stuck cluster reports in seconds instead of hitting CI's 600s kill, which
//! is reported as TIMEOUT with no cause.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::OnceLock,
    time::Duration,
};

use assert2::{assert, check};
use krabka_broker::{
    BrokerConfig, BrokerHandle, NodeId, codes,
    config::{NodeRole, StretchProfile},
    replica_selector::ReplicaSelectorKind,
};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_configs_response::DescribeConfigsResourceResult,
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tempfile::TempDir;
use tokio::sync::Mutex;

mod support;

/// The preferred leader site: a data site that serves clients.
const SITE_A: &str = "site-a";
/// The second data site.
const SITE_B: &str = "site-b";
/// The witness site. Node 3 lives here and carries [`NodeRole::Witness`].
const SITE_C: &str = "site-c";
/// `SITES[i]` is the rack of broker `i + 1`.
const SITES: [&str; 3] = [SITE_A, SITE_B, SITE_C];

/// The witness's broker id, which is also its `KRaft` node id.
const WITNESS_ID: i32 = 3;
/// The leader's broker id. Placement puts `replicas[0]` in the preferred site.
const LEADER_ID: i32 = 1;

/// Kafka's `BROKER` config-resource type.
const RESOURCE_TYPE_BROKER: i8 = 4;
/// `IncrementalAlterConfigs` SET.
const CONFIG_OP_SET: i8 = 0;
/// `config_source` `DYNAMIC_BROKER_CONFIG`.
const CONFIG_SOURCE_DYNAMIC_BROKER: i8 = 2;
/// `config_source` `DYNAMIC_DEFAULT_BROKER_CONFIG`.
const CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER: i8 = 3;

/// The per-broker config key that marks a data-bearing witness.
const BROKER_WITNESS: &str = "broker.witness";
/// The cluster-default config key that names the preferred leader site.
const STRETCH_PREFERRED_LEADER_SITE: &str = "stretch.preferred.leader.site";

const TOPIC: &str = "witness-topic";
const N_RECORDS: i32 = 5;

/// Generous enough that a loaded runner does not fail a healthy cluster, short
/// enough that a broken one reports in seconds rather than at CI's kill.
const STEP_TIMEOUT: Duration = Duration::from_secs(45);

/// Serialize the whole test binary. Each test boots a three-node loopback
/// cluster with short raft timings; two at once starve the election. The
/// rationale is `replication.rs::cluster_lock`'s.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn within<F: Future>(what: &str, future: F) -> F::Output {
    tokio::time::timeout(STEP_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {STEP_TIMEOUT:?}"))
}

fn stretch_profile() -> StretchProfile {
    StretchProfile {
        sites: SITES.iter().map(|site| (*site).to_string()).collect(),
        witness_site: SITE_C.to_string(),
        preferred_leader_site: SITE_A.to_string(),
    }
}

/// Boot the three-site cluster: two data sites and one witness site, with
/// `min.insync.replicas=2` (the only value a stretch profile accepts) and the
/// rack-aware replica selector, which is what makes the KIP-392 redirect check
/// meaningful.
///
/// Retries like `support::start_n_node_with_retry`, which cannot be reused here
/// because it takes no per-broker customizer.
async fn start_stretch_cluster() -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let mut last_err = None;
    for attempt in 1..=3 {
        let started = support::start_n_node_with(3, |i, cfg| {
            cfg.rack = Some(SITES[i].to_string());
            cfg.stretch = Some(stretch_profile());
            cfg.default_min_insync_replicas = 2;
            cfg.replica_selector = ReplicaSelectorKind::RackAware;
            if SITES[i] == SITE_C {
                cfg.roles.push(NodeRole::Witness);
            }
        })
        .await;
        match started {
            Ok(cluster) => {
                support::wait_for_all_brokers_registered(&cluster, 3).await;
                // Placement and the produce / fetch gates read the role and the
                // preferred site out of the metadata image, so wait until both
                // records have reached every node before a topic is created.
                for (handle, _, _) in &cluster {
                    within(
                        "witness role and preferred site in the image",
                        handle.wait_for_image(|img| {
                            img.broker_config(NodeId(3))
                                .and_then(|configs| configs.get(BROKER_WITNESS))
                                .map(String::as_str)
                                == Some("true")
                                && img
                                    .default_broker_config()
                                    .and_then(|configs| configs.get(STRETCH_PREFERRED_LEADER_SITE))
                                    .map(String::as_str)
                                    == Some(SITE_A)
                        }),
                    )
                    .await;
                }
                return cluster;
            }
            Err(error) => {
                tracing::warn!(attempt, %error, "stretch cluster start failed; retrying");
                last_err = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("stretch cluster start failed after 3 attempts; last error: {last_err:?}");
}

async fn client_at(addr: &str) -> Client {
    Client::builder()
        .bootstrap(addr.to_string())
        .client_id("witness-role-test")
        .build()
        .await
        .expect("client build")
}

/// Create `TOPIC` with one partition and rf=3, and return its id.
async fn create_topic(client: &Client) -> WireUuid {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 3,
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == codes::NONE,
        "CreateTopics {TOPIC}: error_code={}",
        resp.topics[0].error_code
    );
    resp.topics[0].topic_id
}

fn record_batch(n: i32) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: (n - 1).max(0),
        records: (0..n)
            .map(|i| Record {
                offset_delta: i,
                value: Some(bytes::Bytes::from(format!("v{i}"))),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn produce_request(topic_id: WireUuid, n: i32) -> ProduceRequest {
    ProduceRequest {
        acks: -1,
        timeout_ms: 10_000,
        topic_data: vec![TopicProduceData {
            name: TOPIC.into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(record_batch(n).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The partition-level error code of an `acks=all` produce of `n` records.
async fn produce_error(client: &Client, topic_id: WireUuid, n: i32) -> i16 {
    let resp = client
        .send(produce_request(topic_id, n))
        .await
        .expect("Produce round-trip");
    resp.responses[0].partition_responses[0].error_code
}

/// A consumer `Fetch` (`replica_id` = -1) carrying `rack`.
fn consumer_fetch(topic_id: WireUuid, rack: &str) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms: 800,
        min_bytes: 0,
        max_bytes: 10_485_760,
        session_id: 0,
        session_epoch: -1, // sessionless full fetch
        rack_id: rack.to_string(),
        topics: vec![FetchTopic {
            topic: TOPIC.into(),
            topic_id,
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                current_leader_epoch: -1,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The metadata a Kafka admin tool renders for one partition, as one value.
#[derive(Debug, PartialEq, Eq)]
struct PartitionView {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    replica_nodes: Vec<i32>,
    isr_nodes: BTreeSet<i32>,
    offline_replicas: Vec<i32>,
}

async fn partition_view(client: &Client) -> PartitionView {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(TOPIC.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for the topic");
    let partition = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(TOPIC))
        .and_then(|t| t.partitions.first())
        .expect("the topic has partition 0");
    PartitionView {
        error_code: partition.error_code,
        partition_index: partition.partition_index,
        leader_id: partition.leader_id,
        replica_nodes: partition.replica_nodes.clone(),
        isr_nodes: partition.isr_nodes.iter().copied().collect(),
        offline_replicas: partition.offline_replicas.clone(),
    }
}

async fn shutdown(cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>) {
    for (handle, _, _) in cluster {
        handle.shutdown().await;
    }
}

/// The witness registers like any other broker — rack included — and it takes
/// a replica and an ISR seat of an rf=3 partition. A consumer that names the
/// witness site is not redirected to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_is_a_visible_isr_member_that_serves_no_reads() {
    let _guard = cluster_lock().lock().await;
    let cluster = start_stretch_cluster().await;

    let client = client_at(&cluster[0].1.listen_addr.to_string()).await;
    let topic_id = create_topic(&client).await;
    for (handle, _, _) in &cluster {
        within(
            "partition present on every node",
            handle.wait_until_partition_present(TOPIC, 0),
        )
        .await;
    }
    within(
        "the witness joins the ISR",
        cluster[0].0.wait_until_isr_len(TOPIC, 0, 3),
    )
    .await;

    // The witness must stay resolvable: `kafka-topics` and
    // `kafka-reassign-partitions` render replica ids through this list, and its
    // rack is what `kafka-reassign-partitions --generate` reads to keep a
    // reassignment inside a site.
    let resp = client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata for the broker list");
    let racks: BTreeMap<i32, Option<String>> = resp
        .brokers
        .iter()
        .map(|broker| (broker.node_id, broker.rack.clone()))
        .collect();
    check!(
        racks
            == BTreeMap::from([
                (1, Some(SITE_A.to_string())),
                (2, Some(SITE_B.to_string())),
                (WITNESS_ID, Some(SITE_C.to_string())),
            ]),
        "every broker, the witness included, is in Metadata.brokers[] with its rack"
    );

    check!(
        partition_view(&client).await
            == PartitionView {
                error_code: codes::NONE,
                partition_index: 0,
                leader_id: LEADER_ID,
                replica_nodes: vec![1, 2, WITNESS_ID],
                isr_nodes: BTreeSet::from([1, 2, WITNESS_ID]),
                offline_replicas: vec![],
            },
        "the witness is a replica and an ISR member; the leader is in the preferred site"
    );

    // KIP-392: the witness is an in-ISR same-rack replica for a `site-c`
    // consumer, which is exactly the case in which the rack-aware selector
    // would pick it. It must not: a witness serves no client reads.
    let redirected = client
        .send(consumer_fetch(topic_id, SITE_C))
        .await
        .expect("consumer Fetch to the leader with client.rack=site-c");
    check!(
        redirected.responses[0].partitions[0].preferred_read_replica == -1,
        "a consumer in the witness site must not be redirected to the witness"
    );

    shutdown(cluster).await;
}

/// A client `Produce` and a consumer `Fetch` that reach the witness are
/// refused, while replication to that same witness keeps advancing: its local
/// log reaches the produced offset and it stays in the ISR.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_refuses_client_traffic_while_replication_advances() {
    let _guard = cluster_lock().lock().await;
    let cluster = start_stretch_cluster().await;

    let leader = client_at(&cluster[0].1.listen_addr.to_string()).await;
    let topic_id = create_topic(&leader).await;
    for (handle, _, _) in &cluster {
        within(
            "partition present on every node",
            handle.wait_until_partition_present(TOPIC, 0),
        )
        .await;
    }
    within(
        "the witness joins the ISR",
        cluster[0].0.wait_until_isr_len(TOPIC, 0, 3),
    )
    .await;

    check!(
        produce_error(&leader, topic_id, N_RECORDS).await == codes::NONE,
        "acks=all commits on the leader with all three sites up"
    );

    // Replication to the witness advances: it holds the records. This is a
    // FOLLOWER fetch the witness itself issued, so it is the half of the fetch
    // path that must keep working.
    let witness_handle = &cluster[2].0;
    within(
        "the witness replicates every record",
        witness_handle.wait_until_local_log_end_offset(TOPIC, 0, i64::from(N_RECORDS)),
    )
    .await;

    let witness = client_at(&cluster[2].1.listen_addr.to_string()).await;
    check!(
        produce_error(&witness, topic_id, N_RECORDS).await == codes::NOT_LEADER_OR_FOLLOWER,
        "a client Produce to the witness is refused"
    );
    let fetched = witness
        .send(consumer_fetch(topic_id, SITE_C))
        .await
        .expect("consumer Fetch to the witness");
    check!(
        fetched.responses[0].partitions[0].error_code == codes::NOT_LEADER_OR_FOLLOWER,
        "a client Fetch to the witness is refused"
    );

    // The refusals are client-facing only. The witness is still a full ISR
    // member, which is what keeps `min.insync.replicas=2` satisfiable when a
    // data site is lost.
    let isr: BTreeSet<u64> = cluster[0]
        .0
        .partition_isr_for_test(TOPIC, 0)
        .expect("the leader knows the partition")
        .into_iter()
        .collect();
    check!(
        isr == BTreeSet::from([1, 2, 3]),
        "the witness stays in the ISR after refusing client traffic"
    );

    shutdown(cluster).await;
}

/// `broker.witness` is controller-managed: it is published for the witness
/// node, `DescribeConfigs` reports it read-only next to the cluster-default
/// `stretch.preferred.leader.site`, and `IncrementalAlterConfigs` refuses to
/// change it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_role_is_a_read_only_broker_config() {
    let _guard = cluster_lock().lock().await;
    let cluster = start_stretch_cluster().await;

    // Ask the witness itself, the way `kafka-configs --entity-type brokers
    // --entity-name 3 --describe` does.
    let witness = client_at(&cluster[2].1.listen_addr.to_string()).await;
    let described = witness
        .send(DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: WITNESS_ID.to_string(),
                configuration_keys: None,
                ..Default::default()
            }],
            include_synonyms: false,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs for the witness broker");
    let result = &described.results[0];
    check!(result.error_code == codes::NONE, "DescribeConfigs succeeds");

    let witness_entry = result
        .configs
        .iter()
        .find(|entry| entry.name == BROKER_WITNESS)
        .cloned();
    check!(
        witness_entry
            == Some(DescribeConfigsResourceResult {
                name: BROKER_WITNESS.into(),
                value: Some("true".into()),
                read_only: true,
                config_source: CONFIG_SOURCE_DYNAMIC_BROKER,
                ..Default::default()
            }),
        "broker.witness is reported, and reported read-only"
    );
    let site_entry = result
        .configs
        .iter()
        .find(|entry| entry.name == STRETCH_PREFERRED_LEADER_SITE)
        .cloned();
    check!(
        site_entry
            == Some(DescribeConfigsResourceResult {
                name: STRETCH_PREFERRED_LEADER_SITE.into(),
                value: Some(SITE_A.into()),
                read_only: true,
                config_source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                ..Default::default()
            }),
        "the preferred leader site is a read-only cluster default"
    );

    let altered = witness
        .send(IncrementalAlterConfigsRequest {
            resources: vec![AlterConfigsResource {
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: WITNESS_ID.to_string(),
                configs: vec![AlterableConfig {
                    name: BROKER_WITNESS.into(),
                    config_operation: CONFIG_OP_SET,
                    value: Some("false".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            validate_only: false,
            ..Default::default()
        })
        .await
        .expect("IncrementalAlterConfigs round-trip");
    check!(
        altered.responses[0].error_code == codes::INVALID_CONFIG,
        "an operator cannot turn the witness role off through the config API: {:?}",
        altered.responses[0].error_message
    );

    shutdown(cluster).await;
}
