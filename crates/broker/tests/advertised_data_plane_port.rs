//! A broker configured with port zero advertises the endpoint it actually bound.
#![cfg(test)]

mod support;

use {
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_protocol::owned::metadata_response::MetadataResponse,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::{client::connect_client, discovery::topic_metadata_request, standalone_broker},
    tempfile::TempDir,
};

struct DiscoveryScenario {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    response: Option<MetadataResponse>,
}

#[fixture]
fn discovery() -> DiscoveryScenario {
    DiscoveryScenario {
        broker: None,
        directory: None,
        response: None,
    }
}

#[given("a standalone broker configured with port zero")]
async fn start_broker(discovery: &mut DiscoveryScenario) {
    let (directory, broker) = standalone_broker().await;
    discovery.directory = Some(directory);
    discovery.broker = Some(broker);
}

#[when("a client requests cluster metadata")]
async fn request_metadata(discovery: &mut DiscoveryScenario) {
    let broker = discovery.broker.as_ref().expect("a broker was started");
    let client = connect_client(broker.listen_addr().to_string(), None).await;
    discovery.response = Some(
        client
            .send(topic_metadata_request(Some(vec![])))
            .await
            .unwrap(),
    );
}

#[then("metadata advertises the broker's actual bound port")]
fn advertised_port(discovery: &DiscoveryScenario) {
    let broker = discovery.broker.as_ref().expect("a broker was started");
    let response = discovery.response.as_ref().expect("metadata was requested");
    let bound_port = i32::from(broker.listen_addr().port());
    assert!(bound_port != 0, "the broker bound an OS-assigned port");
    assert!(response.brokers.len() == 1);
    assert!(
        response.brokers[0].port == bound_port,
        "Metadata advertised port {}, the broker actually bound {bound_port}: {:?}",
        response.brokers[0].port,
        response.brokers[0]
    );
}

#[scenario(
    path = "tests/features/advertised_data_plane_port.feature",
    name = "An OS assigned port is advertised in metadata"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_port_zero_broker_registers_its_bound_port(discovery: DiscoveryScenario) {
    discovery
        .broker
        .expect("a broker was started")
        .shutdown()
        .await;
}
