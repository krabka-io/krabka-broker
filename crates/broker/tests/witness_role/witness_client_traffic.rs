//! The closed half of the role: a client `Produce` or consumer `Fetch` that
//! reaches the witness is refused with `NOT_LEADER_OR_FOLLOWER`, while the
//! witness's own follower fetch keeps replicating.
//!
//! The two halves have to be asserted together. A witness that refused
//! everything would be no ISR member at all, and it is the ISR seat that keeps
//! `acks=all` writable after a site loss, so the test checks the local log and
//! the ISR right after the refusals.
//!
//! Both refusals also carry the KIP-951 redirect, and this is the one place
//! where the whole of it is observable on a live cluster: the refusing node is
//! never the leader, so the `NodeEndpoints` entry has to come out of the
//! metadata image rather than out of the answering broker's own config.

use {
    crate::{
        N_RECORDS, SITE_C, TOPIC, WitnessTopic, within,
        witness_cluster::client_at,
        witness_topic,
        witness_wire::{consumer_fetch, produce_error, produce_response},
    },
    assert2::check,
    krabka_broker::codes,
    krabka_protocol::owned::{
        fetch_response, produce_response::NodeEndpoint as ProduceNodeEndpoint,
    },
    std::collections::BTreeSet,
};

/// The listener every client in this suite connects on. A KIP-951
/// `NodeEndpoints` entry advertises the address of the listener the request
/// arrived on, so this is the endpoint name the refusals must answer with.
const PLAINTEXT_LISTENER: &str = "PLAINTEXT";

#[rstest_bdd_macros::when("leader 1 receives five records with acks all")]
async fn witness_replicated_produce(witness_topic: &mut WitnessTopic) {
    witness_topic.committed =
        Some(produce_error(witness_topic.leader(), witness_topic.topic_id(), N_RECORDS).await);
}

#[rstest_bdd_macros::then("the leader succeeds and witness 3 replicates through log end offset 5")]
async fn witness_replication_progress(witness_topic: &WitnessTopic) {
    check!(
        witness_topic.committed == Some(codes::NONE),
        "acks=all commits on the leader with all three sites up"
    );
    within(
        "the witness replicates every record",
        witness_topic.cluster[2]
            .0
            .wait_until_local_log_end_offset(TOPIC, 0, i64::from(N_RECORDS)),
    )
    .await;
}

#[rstest_bdd_macros::when(
    "a client sends Produce and a site-C consumer Fetch directly to witness 3"
)]
async fn witness_client_requests(witness_topic: &mut WitnessTopic) {
    witness_topic.witness =
        Some(client_at(&witness_topic.cluster[2].1.listen_addr.to_string()).await);
    witness_topic.produced =
        Some(produce_response(witness_topic.witness(), witness_topic.topic_id(), N_RECORDS).await);
    witness_topic.fetched = Some(
        witness_topic
            .witness()
            .send(consumer_fetch(witness_topic.topic_id(), SITE_C))
            .await
            .expect("consumer Fetch to the witness"),
    );
}

#[rstest_bdd_macros::then(
    "both requests return NOT_LEADER_OR_FOLLOWER and the complete leader endpoint, Fetch names the current leader, and ISR remains 1,2,3"
)]
fn refused_witness_clients(witness_topic: &WitnessTopic) {
    // The address both refusals must advertise for the leader, read out of the
    // image of the broker that answers them. That broker is the witness, which
    // never leads, so nothing here can come from its own listener config.
    let image = witness_topic.cluster[2].0.controller_image_for_test();
    let leader = image
        .partition(TOPIC, 0)
        .expect("the witness image knows the partition")
        .leader;
    let registration = image.broker(leader).expect("the leader is registered");
    let advertised = registration
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name == PLAINTEXT_LISTENER)
        .expect("the leader advertises a PLAINTEXT endpoint");
    let leader_id = i32::try_from(leader.0).expect("broker ids fit an i32");

    let produced = witness_topic
        .produced
        .as_ref()
        .expect("witness Produce response");
    check!(
        produced.responses[0].partition_responses[0].error_code == codes::NOT_LEADER_OR_FOLLOWER,
        "a client Produce to the witness is refused"
    );
    let expected_endpoint = ProduceNodeEndpoint {
        node_id: leader_id,
        port: i32::from(advertised.port),
        host: advertised.host.clone(),
        rack: registration.rack.clone(),
        ..Default::default()
    };
    check!(
        produced.node_endpoints == [expected_endpoint],
        "the refused Produce carries the leader's endpoint (KIP-951): {:?}",
        produced.node_endpoints
    );

    let fetched = witness_topic
        .fetched
        .as_ref()
        .expect("witness Fetch response");
    check!(
        fetched.responses[0].partitions[0].error_code == codes::NOT_LEADER_OR_FOLLOWER,
        "a client Fetch to the witness is refused"
    );
    check!(
        fetched.responses[0].partitions[0].current_leader.leader_id == leader_id,
        "the refused Fetch names the leader (KIP-951): {:?}",
        fetched.responses[0].partitions[0].current_leader
    );
    check!(
        fetched.node_endpoints
            == vec![fetch_response::NodeEndpoint {
                node_id: leader_id,
                host: advertised.host.clone(),
                port: i32::from(advertised.port),
                rack: registration.rack.clone(),
                ..Default::default()
            }],
        "the refused Fetch carries the leader's endpoint (KIP-951): {:?}",
        fetched.node_endpoints
    );

    // The refusals are client-facing only. The witness is still a full ISR
    // member, which is what keeps `min.insync.replicas=2` satisfiable when a
    // data site is lost.
    let isr: BTreeSet<u64> = witness_topic.cluster[0]
        .0
        .partition_isr_for_test(TOPIC, 0)
        .expect("the leader knows the partition")
        .into_iter()
        .collect();
    check!(
        isr == maplit::btreeset! {1, 2, 3},
        "the witness stays in the ISR after refusing client traffic"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/witness_role/witness_client_traffic.feature",
    name = "witness refuses client traffic while replication advances"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_refuses_client_traffic_while_replication_advances(
    mut witness_topic: WitnessTopic,
) {
    witness_topic.shutdown().await;
    drop(witness_topic);
}
