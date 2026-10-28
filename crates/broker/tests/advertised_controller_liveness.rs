//! A dead broker must stop being advertised as `controller_id`, on every node.
//!
//! `Metadata` and `DescribeCluster` name a registered, unfenced broker rather
//! than the quorum leader (see `handlers::controller_id`), so the id has to be
//! one a client can actually reach. Kafka draws it from
//! `getRandomAliveBrokerId`, and a `KRaft` broker knows who is alive because
//! `BrokerRegistration.fenced` is replicated to it.
//!
//! Only the controller leader holds the heartbeat registry that decides
//! fencing, so it writes the decision into the broker's registration, as
//! Kafka's `BrokerRegistrationChangeRecord` does.
//! This suite asks a node that is *not* the controller: if that node ignored
//! the replicated state it would treat every registration as available, and it
//! would hand a client the dead broker's id — an endpoint nobody answers on,
//! which is the controller-routed dead end the whole feature exists to avoid.
//!
//! `handlers::controller_id`'s own unit test pins the choice given a fenced
//! set. What is unproven there, and proven here, is that a follower's fenced
//! set tracks a death it only learns about through the metadata log.
#![cfg(test)]

use {
    crate::support::{
        client::connect_client, discovery::topic_metadata_request, registered_cluster,
        shutdown_cluster,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::DescribeClusterResponse, metadata_response::MetadataResponse,
    },
    krabka_units::convert::TimeExt as _,
    std::{collections::BTreeSet, net::TcpListener, time::Duration},
    tokio::{time, time::Instant},
};

mod support;

/// Requests per measurement. The advertised id rotates one step per request,
/// so a run of this length visits every candidate of a three-broker cluster at
/// least twice.
const REQUESTS: usize = 6;

/// A bound on a `heartbeat_timeout` (2s under the test config) for the
/// session to expire, a `liveness_tick_interval` (1s) for the tick that
/// publishes the decision, and the metadata propagation behind it, with room
/// for a loaded runner. Only a stuck cluster reaches it.
const FENCING_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Default)]
struct AdvertisedBrokerLiveness {
    cluster: Vec<(BrokerHandle, krabka_broker::BrokerConfig, tempfile::TempDir)>,
    client: Option<Client>,
    observer: u64,
    victim: u64,
    retired: Option<(krabka_broker::BrokerConfig, tempfile::TempDir)>,
    survivors: BTreeSet<i32>,
    responses: Vec<(MetadataResponse, DescribeClusterResponse)>,
}
#[rstest::fixture]
fn advertised_liveness() -> AdvertisedBrokerLiveness {
    let client = None;
    AdvertisedBrokerLiveness {
        client,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "three registered live brokers rotate all advertised controller identities through a follower observer"
)]
async fn live_advertised_controller_set(advertised_liveness: &mut AdvertisedBrokerLiveness) {
    support::init_tracing();
    let cluster = registered_cluster(3).await;
    let (leader, observer_index, victim_index) = support::two_controller_followers(&cluster).await;
    let everyone = cluster
        .iter()
        .map(|(handle, _, _)| node_id_of(handle))
        .collect::<BTreeSet<_>>();
    let client = connect_client(cluster[observer_index].0.listen_addr().to_string(), None).await;
    let leader_index = cluster
        .iter()
        .position(|(handle, _, _)| handle.node_id() == leader.0)
        .expect("controller member");
    settle_unfenced(
        &cluster[leader_index].0,
        &cluster[observer_index].0,
        &everyone,
    )
    .await;
    assert!(
        advertised_over_a_run(&client).await == everyone,
        "live rotation must include every broker"
    );
    advertised_liveness.observer = cluster[observer_index].0.node_id();
    advertised_liveness.victim = cluster[victim_index].0.node_id();
    advertised_liveness.survivors = everyone
        .into_iter()
        .filter(|id| {
            i64::from(*id) != i64::try_from(advertised_liveness.victim).expect("broker ID")
        })
        .collect();
    advertised_liveness.cluster = cluster;
    advertised_liveness.client = Some(client);
}
#[rstest_bdd_macros::when(
    "one other follower crashes, its fence is replicated and the observer serves six Metadata and DescribeCluster request pairs"
)]
async fn observe_dead_advertised_controller(advertised_liveness: &mut AdvertisedBrokerLiveness) {
    let victim_index = advertised_liveness
        .cluster
        .iter()
        .position(|(handle, _, _)| handle.node_id() == advertised_liveness.victim)
        .expect("victim follower");
    let (victim, config, directory) = advertised_liveness.cluster.remove(victim_index);
    advertised_liveness.retired = Some((config, directory));
    victim.crash_for_test().await;
    let observer = &advertised_liveness
        .cluster
        .iter()
        .find(|(handle, _, _)| handle.node_id() == advertised_liveness.observer)
        .expect("observer follower")
        .0;
    wait_until_fenced_set(observer, &BTreeSet::from([advertised_liveness.victim])).await;
    let client = advertised_liveness
        .client
        .as_ref()
        .expect("observer client");
    for _ in 0..REQUESTS {
        let metadata = client
            .send(topic_metadata_request(Some(vec![])))
            .await
            .expect("Metadata");
        let cluster = client
            .send(DescribeClusterRequest::default())
            .await
            .expect("DescribeCluster");
        advertised_liveness.responses.push((metadata, cluster));
    }
}
#[rstest_bdd_macros::then(
    "every response advertises a surviving controller, Metadata excludes the dead broker and the observer remains a follower"
)]
async fn advertised_controller_survivors(advertised_liveness: &mut AdvertisedBrokerLiveness) {
    assert!(advertised_liveness.responses.len() == REQUESTS);
    let dead_id = i32::try_from(advertised_liveness.victim).expect("wire broker ID");
    for (metadata, cluster) in &advertised_liveness.responses {
        assert!(
            metadata
                .brokers
                .iter()
                .all(|broker| broker.node_id != dead_id),
            "dead broker listed: {:?}",
            metadata.brokers
        );
        assert!(
            advertised_liveness
                .survivors
                .contains(&metadata.controller_id),
            "Metadata controller {}",
            metadata.controller_id
        );
        assert!(
            advertised_liveness
                .survivors
                .contains(&cluster.controller_id),
            "DescribeCluster controller {}",
            cluster.controller_id
        );
    }
    let observer = &advertised_liveness
        .cluster
        .iter()
        .find(|(handle, _, _)| handle.node_id() == advertised_liveness.observer)
        .expect("observer follower")
        .0;
    assert!(
        observer.controller_leader_id() != Some(krabka_raft::NodeId(observer.node_id())),
        "observer must remain a follower"
    );
    shutdown_cluster(mem::take(&mut advertised_liveness.cluster)).await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/advertised_liveness.feature",
    name = "Follower discovery excludes a crashed broker after replicated fencing"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_controller_node_stops_advertising_a_broker_that_died(
    advertised_liveness: AdvertisedBrokerLiveness,
) {
}

#[derive(Default)]
struct RestartedHeartbeatSession {
    directory: Option<tempfile::TempDir>,
    config: Option<krabka_broker::BrokerConfig>,
    broker: Option<BrokerHandle>,
    initial: Option<SessionState>,
    restarted: Option<SessionState>,
}
#[rstest::fixture]
fn restarted_heartbeat() -> RestartedHeartbeatSession {
    let directory = None;
    RestartedHeartbeatSession {
        directory,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "a for-tests broker survives three heartbeat timeouts and a fencing publication on its ephemeral controller port"
)]
async fn initial_ephemeral_heartbeat_session(restarted_heartbeat: &mut RestartedHeartbeatSession) {
    support::init_tracing();
    let directory = tempfile::tempdir().expect("log directory");
    let config = krabka_broker::BrokerConfig::for_tests(directory.path().to_path_buf());
    let broker = krabka_broker::Broker::start(config.clone())
        .await
        .expect("start broker");
    restarted_heartbeat.initial = Some(session_state(&broker, &config).await);
    restarted_heartbeat.broker = Some(broker);
    restarted_heartbeat.config = Some(config);
    restarted_heartbeat.directory = Some(directory);
}
#[rstest_bdd_macros::when(
    "the broker rejoins from the same log directory while its first controller port is held unavailable"
)]
async fn rejoin_ephemeral_heartbeat_session(restarted_heartbeat: &mut RestartedHeartbeatSession) {
    let broker = restarted_heartbeat.broker.take().expect("initial broker");
    let port = broker.controller_addr();
    broker.shutdown().await;
    let _held = TcpListener::bind(port).expect("hold first controller port");
    let mut config = restarted_heartbeat
        .config
        .as_ref()
        .expect("test configuration")
        .clone();
    config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let restarted = krabka_broker::Broker::start(config)
        .await
        .expect("rejoin broker");
    restarted_heartbeat.restarted = Some(
        session_state(
            &restarted,
            restarted_heartbeat
                .config
                .as_ref()
                .expect("test configuration"),
        )
        .await,
    );
    restarted.shutdown().await;
}
#[rstest_bdd_macros::then(
    "both initial and rejoined brokers have a live heartbeat session and no fenced brokers"
)]
fn retained_heartbeat_sessions(restarted_heartbeat: &RestartedHeartbeatSession) {
    let healthy = SessionState {
        alive: true,
        fenced: BTreeSet::new(),
    };
    assert!(
        (
            restarted_heartbeat
                .initial
                .as_ref()
                .expect("initial session"),
            restarted_heartbeat
                .restarted
                .as_ref()
                .expect("rejoined session")
        ) == (&healthy, &healthy)
    );
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/advertised_liveness.feature",
    name = "A restarted ephemeral controller port keeps the broker heartbeat alive"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_for_tests_broker_keeps_its_heartbeat_session(
    restarted_heartbeat: RestartedHeartbeatSession,
) {
}

fn node_id_of(handle: &BrokerHandle) -> i32 {
    i32::try_from(handle.node_id()).expect("node id fits an i32")
}

/// Block until the observer's image is known to be past the cluster's start-up
/// unfencing, with an empty fenced set.
///
/// A broker registers fenced and is unfenced on its first caught-up heartbeat,
/// so a freshly booted cluster writes an unfencing edge shortly after start. An
/// observer image that has simply not seen that edge reads exactly like one
/// past it: both report nobody fenced. Waiting on the empty set alone can
/// therefore return before the seed lands, and the death this test measures
/// would then race the unfencing that precedes it.
///
/// Watching for the fenced set to go non-empty and then empty would not settle
/// it either. The liveness tick's fence publication is level-triggered: the
/// controller leader compares its registry against the image and writes only
/// the difference, so it may fence a broker that a heartbeat then unfences,
/// or never fence one at all.
///
/// What is observable is the decision, the pass that published it, and the
/// offset it was written at.
///
/// The leader's liveness registry reporting every broker alive means no later
/// pass computes a fence. It does not mean no earlier one is still committing:
/// a pass that read the registry before the first heartbeats decides
/// a fence and then awaits its own `submit_change`, and while
/// that await is outstanding the registry already reads all-alive and the
/// leader's image does not carry the record yet. Reading the image there would
/// find nobody fenced and be wrong about it. So wait for one publication pass
/// to *complete* after the all-alive point: the pass awaits its commit, so its
/// counter moving means any fence decided before that point is applied and
/// visible, not in flight.
///
/// From there the image is honest, and the leader agreeing that nobody is
/// fenced means the tombstone for anything it did write has committed. An
/// offset read after that is past every fencing record the start-up
/// produced, which is what makes the last wait mean something: an observer
/// that has applied up to that offset has applied the fencing too, so an empty
/// fenced set there can no longer be one that has not seen it yet.
async fn settle_unfenced(leader: &BrokerHandle, observer: &BrokerHandle, everyone: &BTreeSet<i32>) {
    for &id in everyone {
        leader
            .wait_until_broker_alive(u64::try_from(id).expect("node id is positive"))
            .await;
    }
    let published = leader.metrics().controller_fencing_publications_total.get();
    leader
        .wait_for_metrics(
            "a fencing publication completing after every broker is alive",
            |m| m.controller_fencing_publications_total.get() > published,
        )
        .await;
    wait_until_fenced_set(leader, &BTreeSet::new()).await;
    let unfenced_at = leader.metadata_offset_for_test();

    observer
        .wait_until_metadata_offset_at_least(unfenced_at)
        .await;
    wait_until_fenced_set(observer, &BTreeSet::new()).await;
}

/// The ids one run of requests advertises as `controller_id`.
async fn advertised_over_a_run(client: &Client) -> BTreeSet<i32> {
    let mut advertised = BTreeSet::new();
    for _ in 0..REQUESTS {
        let resp: MetadataResponse = client
            .send(topic_metadata_request(Some(vec![])))
            .await
            .unwrap();
        advertised.insert(resp.controller_id);
    }
    advertised
}

/// Block until `handle`'s own replicated image marks exactly `expected` fenced.
///
/// This reads the image rather than a response, on purpose: the wait must not
/// run through the handlers under test, or a handler that ignores the
/// replicated state would simply hang here instead of failing on the id it
/// advertises.
async fn wait_until_fenced_set(handle: &BrokerHandle, expected: &BTreeSet<u64>) {
    let deadline = Instant::now() + FENCING_DEADLINE;
    loop {
        let fenced = handle.fenced_broker_ids_for_test();
        if fenced == *expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the observer's image never settled on fenced brokers {expected:?}; it has {fenced:?}"
        );
        time::sleep(Duration::from_millis(100)).await;
    }
}

/// Whether the controller holds a broker alive, and the brokers its image
/// marks fenced.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SessionState {
    alive: bool,
    fenced: BTreeSet<u64>,
}

/// The [`SessionState`] of `broker`, once a session that never heartbeated
/// would have expired.
///
/// It waits for three session timeouts and a liveness tick, and then for one
/// more fencing publication, so an expired session has been decided and
/// published before it reads the state.
async fn session_state(
    broker: &BrokerHandle,
    config: &krabka_broker::BrokerConfig,
) -> SessionState {
    time::sleep(config.heartbeat_timeout.to_std() * 3 + config.liveness_tick_interval.to_std())
        .await;
    let published = broker.metrics().controller_fencing_publications_total.get();
    broker
        .wait_for_metrics("a fencing publication after the session window", |m| {
            m.controller_fencing_publications_total.get() > published
        })
        .await;
    SessionState {
        alive: broker.broker_alive_for_test(broker.node_id()).await,
        fenced: broker.fenced_broker_ids_for_test(),
    }
}

// BDD scenario entry points; acceptance steps are defined above.

use std::mem;
