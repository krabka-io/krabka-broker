//! Tests 3 to 6: the leader-side throttle caps the bytes a replica Fetch
//! returns, an unthrottled partition is left alone, a list that names only the
//! follower throttles nothing, and a follower in the ISR is never throttled.
//!
//! All run on the PLAINTEXT cluster: the throttle decision is made on the
//! fetch path and needs no principal, so the compat shim that allows every
//! operation while there are no ACLs keeps the setup to the minimum. They are
//! the tests that give the suite its name — the config tests only prove the key
//! round-trips, these prove it is enforced.

use {
    crate::{
        CLIENT_ID,
        cluster::{
            add_follower, add_follower_in_isr, create_topic_plaintext, leader_rate_matches,
            start_single_broker_plaintext, wait_partition_exists,
        },
        configs::drive_incremental_alter_configs_plaintext,
        records::{fetch_plaintext_replica, produce_plaintext},
        support::topics::ConfiguredTopicSetup,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::net::SocketAddr,
    tempfile::TempDir,
};

#[derive(Default)]
struct ReplicaThrottle {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    address: Option<SocketAddr>,
    responses: Vec<usize>,
}

#[fixture]
fn replica_throttle() -> ReplicaThrottle {
    // Each replica-fetch scenario owns its broker and recorded wire responses.
    ReplicaThrottle::default()
}

impl ReplicaThrottle {
    fn broker(&self) -> &BrokerHandle { self.broker.as_ref().expect("running leader") }

    fn address(&self) -> SocketAddr { self.address.expect("leader listener") }

    async fn shutdown(&mut self) {
        self.broker.take().expect("running leader").shutdown().await;
        self.directory.take();
    }
}

#[given("a replication leader with follower 2 whose ISR membership is {in_isr}")]
async fn replica_leader(replica_throttle: &mut ReplicaThrottle, in_isr: bool) {
    let (broker, directory, address) = start_single_broker_plaintext().await;
    create_topic_plaintext()
        .addr(address)
        .client_id(CLIENT_ID)
        .topic(ConfiguredTopicSetup {
            name: "bar".into(),
            ..Default::default()
        })
        .call()
        .await;
    wait_partition_exists(&broker, "bar", 0).await;
    if in_isr {
        add_follower_in_isr(&broker, "bar", 2).await;
    } else {
        add_follower(&broker, "bar", 2).await;
    }
    replica_throttle.broker = Some(broker);
    replica_throttle.directory = Some(directory);
    replica_throttle.address = Some(address);
}

#[given("the leader rate is 512 bytes per second and the throttled replica list names {selection}")]
async fn throttle_selection(replica_throttle: &mut ReplicaThrottle, selection: String) {
    let node_id = replica_throttle.broker().node_id();
    let list = match selection.as_str() {
        "the leader" => format!("0:{node_id}"),
        "only follower 2" => "0:2".into(),
        "every replica" => "*".into(),
        _ => panic!("unknown throttle selection {selection}"),
    };
    let error = drive_incremental_alter_configs_plaintext(replica_throttle.address(), vec![(
        4,
        node_id.to_string(),
        vec![(
            "leader.replication.throttled.rate".into(),
            Some("512".into()),
            0,
        )],
    )])
    .await;
    assert!(
        error == 0,
        "broker throttle alter failed: error_code={error}"
    );
    let error = drive_incremental_alter_configs_plaintext(replica_throttle.address(), vec![(
        2,
        "bar".into(),
        vec![(
            "leader.replication.throttled.replicas".into(),
            Some(list.clone()),
            0,
        )],
    )])
    .await;
    assert!(
        error == 0,
        "topic throttle alter failed: error_code={error}"
    );
    replica_throttle
        .broker()
        .wait_for_image(|image| {
            leader_rate_matches(image, node_id, 512)
                && image
                    .topic_config("bar")
                    .and_then(|config| config.get("leader.replication.throttled.replicas"))
                    == Some(&list)
        })
        .await;
}

#[given("the replicated partition contains eight 1024-byte records")]
async fn replication_payload(replica_throttle: &mut ReplicaThrottle) {
    produce_plaintext(replica_throttle.address(), "bar", 1024, 8).await;
}

#[when("follower 2 fetches the replicated partition")]
async fn replica_fetch(replica_throttle: &mut ReplicaThrottle) {
    replica_throttle
        .responses
        .push(fetch_plaintext_replica(replica_throttle.address(), "bar", 2).await);
}

#[when("another eight 1024-byte records are appended before follower 2 fetches again")]
async fn replica_fetch_after_append(replica_throttle: &mut ReplicaThrottle) {
    produce_plaintext(replica_throttle.address(), "bar", 1024, 8).await;
    replica_fetch(replica_throttle).await;
}

#[then("the first replica response contains at least 4096 bytes")]
fn full_replica_payload(replica_throttle: &ReplicaThrottle) {
    let response = *replica_throttle
        .responses
        .first()
        .expect("replica fetch completed");
    assert!(
        response >= 4096,
        "expected full or over-burst payload, got {response} bytes"
    );
}

#[then("the second replica response contains at most 2048 bytes of framing and records")]
fn indebted_replica_payload(replica_throttle: &ReplicaThrottle) {
    assert!(replica_throttle.responses.len() == 2);
    let response = replica_throttle.responses[1];
    assert!(
        response <= 2048,
        "expected debt-throttled response <= 2048 bytes, got {response}"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/throttle/fetch_size.feature",
    name = "throttle rate caps fetch response size"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throttle_rate_caps_fetch_response_size(replica_throttle: ReplicaThrottle) {
    replica_throttle.shutdown().await;
    drop(replica_throttle);
}

#[scenario(
    path = "tests/features/migrated/tests/throttle/fetch_size.feature",
    name = "unthrottled partition unaffected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unthrottled_partition_unaffected(replica_throttle: ReplicaThrottle) {
    replica_throttle.shutdown().await;
    drop(replica_throttle);
}

#[scenario(
    path = "tests/features/migrated/tests/throttle/fetch_size.feature",
    name = "a list naming only the follower throttles nothing on the leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_list_naming_only_the_follower_throttles_nothing_on_the_leader(
    replica_throttle: ReplicaThrottle,
) {
    replica_throttle.shutdown().await;
    drop(replica_throttle);
}

#[scenario(
    path = "tests/features/migrated/tests/throttle/fetch_size.feature",
    name = "a follower in the isr is not throttled"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_in_the_isr_is_not_throttled(replica_throttle: ReplicaThrottle) {
    replica_throttle.shutdown().await;
    drop(replica_throttle);
}
