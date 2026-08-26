//! Three-site stretch-cluster durability (KFC-2).
//!
//! The deployment under test is the one the witness role exists for: two data
//! sites that serve clients, one witness site that only replicates and votes,
//! one replica of every partition in each site, and `min.insync.replicas=2`.
//! The claim is that such a cluster survives the loss of **any single site**
//! with `acks=all` intact — including the loss of a data site, where the two
//! survivors are one data replica and the witness.
//!
//! # What each test stops
//!
//! A site is lost by stopping its broker. That covers the site-loss half of the
//! claim: a site that is gone cannot serve, cannot vote, and cannot replicate.
//!
//! # Leadership
//!
//! `replicas[0]` is the preferred leader in Kafka, so site-aware placement puts
//! a broker of the preferred site first. When that site is lost, leadership
//! must move to the **other data site** — never to the witness, which serves no
//! client. Every failover in this file is watched for that, not merely checked
//! at the end: the poll asserts that the witness is not the leader on *every*
//! observation it makes while it waits.
//!
//! # Bounded waits
//!
//! Every wait is bounded with `tokio::time::timeout`. CI kills a test at 600s
//! and reports TIMEOUT with no cause; a bounded wait names the step that hung.

use std::{
    collections::BTreeSet,
    future::Future,
    sync::OnceLock,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use crabka_broker::{
    BrokerConfig, BrokerHandle, NodeId, codes,
    config::{NodeRole, StretchProfile},
};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tempfile::TempDir;
use tokio::sync::Mutex;

mod support;

/// The preferred leader site. Placement puts `replicas[0]` here.
const SITE_A: &str = "site-a";
/// The second data site: the failover target when `site-a` is lost.
const SITE_B: &str = "site-b";
/// The witness site. It replicates and votes; it never leads and serves no
/// client.
const SITE_C: &str = "site-c";
/// `SITES[i]` is the rack of broker `i + 1`.
const SITES: [&str; 3] = [SITE_A, SITE_B, SITE_C];

/// Cluster index of each site's broker. Node ids are the index plus one.
const NODE_A: usize = 0;
const NODE_B: usize = 1;
const NODE_C: usize = 2;

/// The witness's node id.
const WITNESS: u64 = 3;

const TOPIC: &str = "stretch-topic";
const N_RECORDS: i32 = 4;

/// Generous enough that a loaded runner does not fail a healthy cluster, short
/// enough that a broken one reports in seconds rather than at CI's kill.
const STEP_TIMEOUT: Duration = Duration::from_secs(60);

/// Serialize the whole binary: each test boots a three-node loopback cluster
/// with short raft timings, and two at once starve the election. Same rationale
/// as `replication.rs::cluster_lock`.
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

/// A running three-site cluster. Handles are taken out as sites are stopped,
/// so `shutdown` can still drain whatever is left.
struct Cluster {
    handles: Vec<Option<BrokerHandle>>,
    configs: Vec<BrokerConfig>,
    _dirs: Vec<TempDir>,
}

impl Cluster {
    /// Boot the three-site cluster: two data sites, one witness site,
    /// `min.insync.replicas=2` (the only value a stretch profile accepts at
    /// rf=3 over three sites), and the witness role on the `site-c` node.
    ///
    /// Retries like `support::start_n_node_with_retry`, which cannot be reused
    /// here because it takes no per-broker customizer.
    async fn start() -> Self {
        let mut last_err = None;
        for attempt in 1..=3 {
            let started = support::start_n_node_with(3, |i, cfg| {
                cfg.rack = Some(SITES[i].to_string());
                cfg.stretch = Some(stretch_profile());
                cfg.default_min_insync_replicas = 2;
                if SITES[i] == SITE_C {
                    cfg.roles.push(NodeRole::Witness);
                }
            })
            .await;
            match started {
                Ok(cluster) => {
                    support::wait_for_all_brokers_registered(&cluster, 3).await;
                    for (handle, _, _) in &cluster {
                        within(
                            "the witness role and the preferred site reach the image",
                            handle.wait_for_image(|img| {
                                img.broker_config(NodeId(WITNESS))
                                    .and_then(|configs| configs.get("broker.witness"))
                                    .map(String::as_str)
                                    == Some("true")
                                    && img
                                        .default_broker_config()
                                        .and_then(|configs| {
                                            configs.get("stretch.preferred.leader.site")
                                        })
                                        .map(String::as_str)
                                        == Some(SITE_A)
                            }),
                        )
                        .await;
                    }
                    let (handles, configs, dirs) = cluster.into_iter().fold(
                        (Vec::new(), Vec::new(), Vec::new()),
                        |(mut handles, mut configs, mut dirs), (handle, config, dir)| {
                            handles.push(Some(handle));
                            configs.push(config);
                            dirs.push(dir);
                            (handles, configs, dirs)
                        },
                    );
                    return Self {
                        handles,
                        configs,
                        _dirs: dirs,
                    };
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

    fn handle(&self, index: usize) -> &BrokerHandle {
        self.handles[index]
            .as_ref()
            .unwrap_or_else(|| panic!("broker {index} is still running"))
    }

    fn addr(&self, index: usize) -> String {
        self.configs[index].listen_addr.to_string()
    }

    /// Lose a site: stop its broker and let it leave the cluster.
    async fn stop(&mut self, index: usize) {
        let handle = self.handles[index]
            .take()
            .unwrap_or_else(|| panic!("broker {index} was already stopped"));
        within("stopping a site", handle.shutdown()).await;
    }

    async fn shutdown(mut self) {
        for handle in self.handles.drain(..).flatten() {
            within("cluster shutdown", handle.shutdown()).await;
        }
    }
}

async fn client_at(addr: &str) -> Client {
    Client::builder()
        .bootstrap(addr.to_string())
        .client_id("stretch-cluster-test")
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

/// The partition-level error code of one `acks=all` produce.
async fn produce_once(client: &Client, topic_id: WireUuid, timeout_ms: i32) -> i16 {
    let resp = client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms,
            topic_data: vec![TopicProduceData {
                name: TOPIC.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record_batch(N_RECORDS).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce round-trip");
    resp.responses[0].partition_responses[0].error_code
}

/// `acks=all` against `addr`, retried until it commits or the bound expires.
///
/// The retry covers only the window in which the surviving replicas have not
/// yet been dropped from the ISR — the leader answers `REQUEST_TIMED_OUT` or
/// `NOT_ENOUGH_REPLICAS` until the controller commits the shrink. It never
/// turns a persistent refusal into a pass: the last code is what the caller
/// asserts on.
async fn produce_until_committed(addr: &str, topic_id: WireUuid) -> i16 {
    let client = client_at(addr).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut code = produce_once(&client, topic_id, 5_000).await;
    while code != codes::NONE && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        code = produce_once(&client, topic_id, 5_000).await;
    }
    code
}

/// The partition as an operator sees it, as one value.
#[derive(Debug, PartialEq, Eq)]
struct PartitionView {
    leader: u64,
    replicas: Vec<u64>,
    isr: BTreeSet<u64>,
    adding_replicas: Vec<u64>,
    removing_replicas: Vec<u64>,
}

fn partition_view(handle: &BrokerHandle) -> Option<PartitionView> {
    handle
        .partition_record_for_test(TOPIC, 0)
        .map(|record| PartitionView {
            leader: record.leader.0,
            replicas: record.replicas.iter().map(|n| n.0).collect(),
            isr: record.isr.iter().map(|n| n.0).collect(),
            adding_replicas: record.adding_replicas.iter().map(|n| n.0).collect(),
            removing_replicas: record.removing_replicas.iter().map(|n| n.0).collect(),
        })
}

/// Poll `handle`'s image until the partition has `leader` and exactly `isr`,
/// failing the moment the witness is seen as the leader.
///
/// The witness check is inside the loop on purpose. A witness that led for a
/// moment and then handed leadership on would satisfy an after-the-fact check
/// while having served as a leader it must never be.
async fn wait_for_leader_and_isr(handle: &BrokerHandle, what: &str, leader: u64, isr: &[u64]) {
    let want: BTreeSet<u64> = isr.iter().copied().collect();
    let poll = async {
        loop {
            if let Some(view) = partition_view(handle) {
                assert!(
                    view.leader != WITNESS,
                    "the witness led the partition while waiting for {what}: {view:?}"
                );
                if view.leader == leader && view.isr == want {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    within(what, poll).await;
}

/// Watch the partition for `window`, failing if the witness ever leads it.
async fn witness_never_leads(handle: &BrokerHandle, window: Duration) {
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        if let Some(view) = partition_view(handle) {
            assert!(
                view.leader != WITNESS,
                "the witness must never lead the partition: {view:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Bring the cluster up with a topic and every replica in the ISR.
async fn cluster_with_topic() -> (Cluster, WireUuid) {
    let cluster = Cluster::start().await;
    let client = client_at(&cluster.addr(NODE_A)).await;
    let topic_id = create_topic(&client).await;
    for index in [NODE_A, NODE_B, NODE_C] {
        within(
            "the partition reaches every node",
            cluster.handle(index).wait_until_partition_present(TOPIC, 0),
        )
        .await;
    }
    wait_for_leader_and_isr(
        cluster.handle(NODE_A),
        "the initial three-replica ISR",
        1,
        &[1, 2, WITNESS],
    )
    .await;
    (cluster, topic_id)
}

/// Placement pins the partition to the preferred site: `replicas[0]` is the
/// `site-a` broker and it is the leader, with the other data site and the
/// witness in the ISR behind it. `acks=all` commits with all three sites up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_site_holds_replicas_zero_and_the_leader() {
    let _guard = cluster_lock().lock().await;
    let (cluster, topic_id) = cluster_with_topic().await;

    check!(
        partition_view(cluster.handle(NODE_A))
            == Some(PartitionView {
                leader: 1,
                replicas: vec![1, 2, WITNESS],
                isr: BTreeSet::from([1, 2, WITNESS]),
                adding_replicas: vec![],
                removing_replicas: vec![],
            }),
        "replicas[0] and the leader are both the preferred site's broker"
    );

    check!(
        produce_until_committed(&cluster.addr(NODE_A), topic_id).await == codes::NONE,
        "acks=all commits with all three sites up"
    );

    cluster.shutdown().await;
}

/// **The headline claim.** Losing a whole *data* site leaves one data replica
/// and the witness. The witness is a full ISR member, so the in-sync set is
/// still two — `min.insync.replicas` — and `acks=all` keeps committing.
///
/// This is the property the witness role exists for. Without a data-bearing
/// witness the ISR would drop to one member here and every `acks=all` write
/// would be refused for as long as the site was down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_survives_the_loss_of_the_non_preferred_data_site() {
    let _guard = cluster_lock().lock().await;
    let (mut cluster, topic_id) = cluster_with_topic().await;

    cluster.stop(NODE_B).await;

    wait_for_leader_and_isr(
        cluster.handle(NODE_A),
        "the ISR shrinks to the preferred site plus the witness",
        1,
        &[1, WITNESS],
    )
    .await;
    check!(
        partition_view(cluster.handle(NODE_A))
            == Some(PartitionView {
                leader: 1,
                replicas: vec![1, 2, WITNESS],
                isr: BTreeSet::from([1, WITNESS]),
                adding_replicas: vec![],
                removing_replicas: vec![],
            }),
        "the witness keeps the ISR at two members after a data site is lost"
    );

    let code = produce_until_committed(&cluster.addr(NODE_A), topic_id).await;
    assert!(
        code == codes::NONE,
        "THE STRETCH-CLUSTER CLAIM FAILED: with data site {SITE_B} down, the surviving \
         data replica (node 1) and the witness (node {WITNESS}) are two in-sync replicas, \
         so an acks=all write MUST still commit under min.insync.replicas=2. \
         The leader refused it with error_code={code}. Either the witness left the ISR or \
         it stopped counting toward min.insync.replicas — the whole point of the role."
    );

    cluster.shutdown().await;
}

/// One case of the site-loss table: which site is lost, and what the survivors
/// must do afterwards.
struct SiteLossCase {
    /// What the case shows, used in the failure messages.
    claim: &'static str,
    /// The cluster index of the site that goes down.
    stop: usize,
    /// The node that must lead once the loss has settled.
    leader: u64,
    /// The ISR once the loss has settled.
    isr: &'static [u64],
    /// The cluster index that must accept `acks=all` afterwards.
    write_to: usize,
}

/// The remaining single-site losses, which differ only by which site goes down.
///
/// * The witness site is lost: the two data replicas are still in sync, so the
///   ISR is two and writes continue.
/// * The preferred data site is lost: leadership moves to the **other data
///   site**, never to the witness, and writes continue there against an ISR of
///   the surviving data replica plus the witness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_site_loss_keeps_acks_all_committing() {
    let _guard = cluster_lock().lock().await;

    for case in [
        SiteLossCase {
            claim: "the witness site is lost: the two data replicas carry the write",
            stop: NODE_C,
            leader: 1,
            isr: &[1, 2],
            write_to: NODE_A,
        },
        SiteLossCase {
            claim: "the preferred data site is lost: the other data site leads, \
                    with the witness in the ISR behind it",
            stop: NODE_A,
            leader: 2,
            isr: &[2, WITNESS],
            write_to: NODE_B,
        },
    ] {
        let (mut cluster, topic_id) = cluster_with_topic().await;
        cluster.stop(case.stop).await;

        let observer = cluster.handle(case.write_to);
        wait_for_leader_and_isr(observer, case.claim, case.leader, case.isr).await;
        check!(
            partition_view(observer)
                == Some(PartitionView {
                    leader: case.leader,
                    replicas: vec![1, 2, WITNESS],
                    isr: case.isr.iter().copied().collect(),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                }),
            "{}",
            case.claim
        );
        check!(
            produce_until_committed(&cluster.addr(case.write_to), topic_id).await == codes::NONE,
            "acks=all must still commit — {}",
            case.claim
        );

        cluster.shutdown().await;
    }
}

/// Losing **both** data sites is beyond what the topology promises. The
/// witness holds every committed record, but it serves no client and it never
/// leads: the partition has no leader a client can write to, and the witness
/// refuses the write outright rather than electing itself.
///
/// Refusing is the safe answer. A witness that took leadership here would be
/// serving clients from a node the deployment sized for neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_data_sites_down_leaves_no_leader_and_the_witness_refuses_writes() {
    let _guard = cluster_lock().lock().await;
    let (mut cluster, topic_id) = cluster_with_topic().await;

    cluster.stop(NODE_A).await;
    cluster.stop(NODE_B).await;

    // The witness is the only broker left. Watch it for long enough to cover
    // the failover path it must not take.
    witness_never_leads(cluster.handle(NODE_C), Duration::from_secs(5)).await;

    let witness = client_at(&cluster.addr(NODE_C)).await;
    let code = produce_once(&witness, topic_id, 3_000).await;
    check!(
        code == codes::NOT_LEADER_OR_FOLLOWER,
        "the witness must refuse an acks=all write when both data sites are down, \
         with the code that sends a client looking for another leader; got {code}"
    );

    let view = partition_view(cluster.handle(NODE_C));
    check!(
        view.as_ref().is_some_and(|view| view.leader != WITNESS),
        "no live broker leads the partition, and the witness is not it: {view:?}"
    );

    cluster.shutdown().await;
}
