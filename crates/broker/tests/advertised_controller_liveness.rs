//! A dead broker must stop being advertised as `controller_id`, on every node.
//!
//! `Metadata` and `DescribeCluster` name a registered, unfenced broker rather
//! than the quorum leader (see `handlers::controller_id`), so the id has to be
//! one a client can actually reach. Kafka draws it from
//! `getRandomAliveBrokerId`, and a `KRaft` broker knows who is alive because
//! `BrokerRegistration.fenced` is replicated to it.
//!
//! Only the controller leader holds the heartbeat registry that decides
//! fencing, so it publishes the decision as the `broker.fenced` broker config.
//! This suite asks a node that is *not* the controller: if that node ignored
//! the replicated state it would treat every registration as available, and it
//! would hand a client the dead broker's id — an endpoint nobody answers on,
//! which is the controller-routed dead end the whole feature exists to avoid.
//!
//! `handlers::controller_id`'s own unit test pins the choice given a fenced
//! set. What is unproven there, and proven here, is that a follower's fenced
//! set tracks a death it only learns about through the metadata log.

use std::{collections::BTreeSet, time::Duration};

use assert2::assert;
use krabka_broker::BrokerHandle;
use krabka_client_core::Client;
use krabka_protocol::owned::{
    describe_cluster_request::DescribeClusterRequest,
    describe_cluster_response::DescribeClusterResponse, metadata_request::MetadataRequest,
    metadata_response::MetadataResponse,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_controller_node_stops_advertising_a_broker_that_died() {
    support::init_tracing();

    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    // The node under test must not be the one holding the heartbeat registry,
    // or the answer could come from local state rather than from the log.
    let leader = cluster[0].0.wait_until_controller_leader().await;
    let followers: Vec<usize> = (0..cluster.len())
        .filter(|&i| cluster[i].0.node_id() != leader.0)
        .collect();
    assert!(
        followers.len() == 2,
        "a three-node cluster has two non-controller nodes"
    );
    // `followers` ascends, so removing the victim leaves the observer's index
    // where it was.
    let (observer_index, victim_index) = (followers[0], followers[1]);
    let observer_addr = cluster[observer_index].0.listen_addr();
    let victim_id = cluster[victim_index].0.node_id();
    let everyone: BTreeSet<i32> = cluster.iter().map(|(h, _, _)| node_id_of(h)).collect();
    let survivors: BTreeSet<i32> = everyone
        .iter()
        .copied()
        .filter(|&id| id != node_id_of(&cluster[victim_index].0))
        .collect();

    let client = Client::builder()
        .bootstrap(observer_addr.to_string())
        .build()
        .await
        .unwrap();

    let leader_index = (0..cluster.len())
        .find(|&i| cluster[i].0.node_id() == leader.0)
        .expect("the elected leader is one of the nodes under test");
    settle_unfenced(
        &cluster[leader_index].0,
        &cluster[observer_index].0,
        &everyone,
    )
    .await;

    // Every broker is advertised while every broker is alive, so what the
    // assertions after the death measure is the death, and not a rotation that
    // was never going to name the victim.
    assert!(
        advertised_over_a_run(&client).await == everyone,
        "a live cluster should advertise every one of its brokers"
    );

    // The config and the log dir stay alive for the rest of the test: a
    // crashed broker's teardown must not race a deleted directory.
    let (victim, _victim_config, _victim_dir) = cluster.remove(victim_index);
    victim.crash_for_test().await;

    wait_until_fenced_set(&cluster[observer_index].0, &BTreeSet::from([victim_id])).await;

    // From here the observer must name only survivors, on both APIs, for every
    // turn of the rotation — while the dead broker keeps the `Metadata`
    // endpoint row a client still needs to route to it.
    let dead_id = i32::try_from(victim_id).expect("node id fits an i32");
    for _ in 0..REQUESTS {
        let resp: MetadataResponse = client
            .send(MetadataRequest {
                topics: Some(vec![]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            resp.brokers.iter().any(|row| row.node_id == dead_id),
            "the dead broker keeps its Metadata endpoint row: {:?}",
            resp.brokers
        );
        assert!(
            survivors.contains(&resp.controller_id),
            "Metadata advertised {} as controller_id; only {survivors:?} are alive",
            resp.controller_id
        );

        let resp: DescribeClusterResponse = client
            .send(DescribeClusterRequest::default())
            .await
            .unwrap();
        assert!(
            survivors.contains(&resp.controller_id),
            "DescribeCluster advertised {} as controller_id; only {survivors:?} are alive",
            resp.controller_id
        );
    }

    // The answers above are only worth anything if they came from a node that
    // does not hold the heartbeat registry.
    let observer = &cluster[observer_index].0;
    assert!(
        observer.controller_leader_id() != Some(krabka_raft::NodeId(observer.node_id())),
        "the observer must still be a follower for this test to mean anything"
    );

    for (handle, _, _) in cluster {
        handle.shutdown().await;
    }
}

fn node_id_of(handle: &BrokerHandle) -> i32 {
    i32::try_from(handle.node_id()).expect("node id fits an i32")
}

/// Block until the observer's image is known to be past the cluster's start-up
/// unfencing, with an empty fenced set.
///
/// A broker registers fenced and is unfenced on its first heartbeat, so a
/// freshly booted cluster publishes an unfencing edge shortly after start. An
/// observer image that has simply not seen that edge reads exactly like one
/// past it: both report nobody fenced. Waiting on the empty set alone can
/// therefore return before the seed lands, and the death this test measures
/// would then race the unfencing that precedes it.
///
/// Watching for the fenced set to go non-empty and then empty would not settle
/// it either. Publication is level-triggered: the controller leader compares
/// its registry against the image and writes only the difference, so a broker
/// whose first heartbeat arrives before the leader's first tick is never
/// published as fenced at all.
///
/// What is observable is the decision and the offset it was written at. The
/// leader's liveness registry reporting every broker alive means its next
/// publication has no fencing left to write, and its own image agreeing means
/// the tombstone for anything it did write has committed. An offset read after
/// that is therefore past any `broker.fenced` record the start-up ever
/// produced, which is what makes the last wait mean something: an observer
/// that has applied up to that offset has applied the fencing too, so an empty
/// fenced set there can no longer be one that has not seen it yet.
async fn settle_unfenced(leader: &BrokerHandle, observer: &BrokerHandle, everyone: &BTreeSet<i32>) {
    for &id in everyone {
        leader
            .wait_until_broker_alive(u64::try_from(id).expect("node id is positive"))
            .await;
    }
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
            .send(MetadataRequest {
                topics: Some(vec![]),
                ..Default::default()
            })
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
    let deadline = tokio::time::Instant::now() + FENCING_DEADLINE;
    loop {
        let fenced = handle.fenced_broker_ids_for_test();
        if fenced == *expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the observer's image never settled on fenced brokers {expected:?}; it has {fenced:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
