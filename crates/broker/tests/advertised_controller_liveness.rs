//! A dead broker must stop being advertised as `controller_id`, on every node.
//!
//! `Metadata` and `DescribeCluster` name a broker rather than the raft leader
//! (see `handlers::advertised_controller`), so the id has to be one a client
//! can actually reach. Kafka draws it from `getRandomAliveBrokerId`, and a
//! `KRaft` broker knows who is alive because `BrokerRegistration.fenced` is
//! replicated to it.
//!
//! Only the controller leader holds the heartbeat registry that decides
//! fencing, so it publishes the decision as the `broker.fenced` broker config.
//! This suite asks a node that is *not* the controller: if that node ignored
//! the replicated state it would treat every registration as available, and
//! its rotation would hand a client the dead broker's id — an endpoint nobody
//! answers on, which is the controller-routed dead end the whole feature
//! exists to avoid.

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

/// Requests per measurement. The rotation walks the candidate list one step
/// per request, so a run of this length visits every candidate of a
/// three-broker cluster at least twice.
const REQUESTS: usize = 6;

/// `heartbeat_timeout` (2s) plus a `liveness_tick_interval` (1s) publication
/// and one metadata propagation, with slack for a loaded runner.
const FENCING_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_controller_node_stops_advertising_a_broker_that_died() {
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
    let victim_id = node_id_of(&cluster[victim_index].0);
    let everyone: BTreeSet<i32> = cluster.iter().map(|(h, _, _)| node_id_of(h)).collect();
    let survivors: BTreeSet<i32> = everyone
        .iter()
        .copied()
        .filter(|&id| id != victim_id)
        .collect();

    let client = Client::builder()
        .bootstrap(observer_addr.to_string())
        .build()
        .await
        .unwrap();

    // The controller opens a *fenced* session for every broker it finds
    // registered but has not yet heard from, and unfences it on that broker's
    // first heartbeat. Both edges are published, so settle on the steady state
    // before measuring: a run started inside the seed window would be reading
    // the seed rather than the death.
    //
    // The registry settles first. An observer image that has simply not seen
    // the seed publication yet reads exactly like one past it, so waiting on
    // the image alone can return before the seed even lands.
    let leader_index = (0..cluster.len())
        .find(|&i| cluster[i].0.node_id() == leader.0)
        .expect("the elected leader is one of these nodes");
    wait_until_the_controller_has_unfenced_every_broker(&cluster[leader_index].0).await;
    wait_until_fenced_set(&cluster[observer_index].0, &BTreeSet::new()).await;

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
    for _ in 0..REQUESTS {
        let resp: MetadataResponse = client
            .send(MetadataRequest {
                topics: Some(vec![]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            resp.brokers.iter().any(|row| row.node_id == victim_id),
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

/// The controller-managed broker config that carries a fencing decision into
/// the metadata log, and the value it takes on a fenced node. A client reads
/// the same pair back through `kafka-configs --describe --entity-type brokers`.
const BROKER_FENCED: &str = "broker.fenced";
const FENCED_TRUE: &str = "true";

/// Block until the controller's own liveness registry holds no fenced or dead
/// broker, so no further `broker.fenced=true` publication is coming.
async fn wait_until_the_controller_has_unfenced_every_broker(handle: &BrokerHandle) {
    let deadline = tokio::time::Instant::now() + FENCING_DEADLINE;
    loop {
        let unavailable = handle.unavailable_brokers_for_test().await;
        if unavailable.is_empty() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the controller still treats brokers {unavailable:?} as unavailable"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Block until `handle`'s own replicated image marks exactly `expected` fenced.
///
/// This reads the image rather than a response, on purpose: the wait must not
/// run through the handlers under test, or a handler that ignores the
/// replicated state would simply hang here instead of failing on the id it
/// advertises.
async fn wait_until_fenced_set(handle: &BrokerHandle, expected: &BTreeSet<i32>) {
    let deadline = tokio::time::Instant::now() + FENCING_DEADLINE;
    loop {
        let image = handle.controller_image_for_test();
        let fenced: BTreeSet<i32> = image
            .brokers()
            .filter(|broker| {
                image
                    .broker_config(broker.node_id)
                    .and_then(|configs| configs.get(BROKER_FENCED))
                    .map(String::as_str)
                    == Some(FENCED_TRUE)
            })
            .map(|broker| i32::try_from(broker.node_id.0).expect("node id fits an i32"))
            .collect();
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
