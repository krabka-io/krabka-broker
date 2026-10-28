#![cfg(test)]

//! KIP-919 `UnregisterBroker` admin RPC (`api_key` 64). It drops a broker
//! registration record, so later `Metadata` responses no longer advertise the
//! broker's endpoints.

use assert2::assert;
mod support;

use {
    krabka_broker::NodeId,
    krabka_protocol::owned::{
        metadata_request::MetadataRequest, unregister_broker_request::UnregisterBrokerRequest,
        unregister_broker_response::UnregisterBrokerResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct BrokerRegistration {
    process: Option<support::InProcess>,
    response: Option<UnregisterBrokerResponse>,
    requested_id: Option<i32>,
}

#[fixture]
fn broker_registration() -> BrokerRegistration {
    // Each unregister flow observes a fresh registration and real admin responses.
    BrokerRegistration::default()
}

#[given("a running broker registered as node 1")]
async fn registered_broker(broker_registration: &mut BrokerRegistration) {
    let process = support::start().await;
    let metadata = process
        .client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata before unregister");
    assert!(metadata.brokers.len() == 1);
    assert!(metadata.brokers[0].node_id == 1);
    broker_registration.process = Some(process);
}

#[when("an administrator unregisters broker {broker_id}")]
async fn unregister_node(broker_registration: &mut BrokerRegistration, broker_id: i32) {
    broker_registration.response = Some(
        broker_registration
            .process
            .as_ref()
            .expect("registered broker")
            .client
            .send(UnregisterBrokerRequest {
                broker_id,
                ..Default::default()
            })
            .await
            .expect("UnregisterBroker"),
    );
    broker_registration.requested_id = Some(broker_id);
}

#[then("the unregister request succeeds without an error message")]
fn unregister_succeeded(broker_registration: &BrokerRegistration) {
    let response = broker_registration
        .response
        .as_ref()
        .expect("UnregisterBroker answered");
    assert!(response.error_code == 0, "{response:?}");
    assert!(response.error_message.is_none() || response.error_message.as_deref() == Some(""));
}

#[then("the committed controller image no longer contains broker 1")]
async fn registration_removed(broker_registration: &BrokerRegistration) {
    broker_registration
        .process
        .as_ref()
        .expect("broker")
        .broker
        .wait_for_image(|image| image.broker(NodeId(1)).is_none())
        .await;
}

#[then("the complete unregister response reports the requested broker ID is not registered")]
fn unknown_registration(broker_registration: &BrokerRegistration) {
    let broker_id = broker_registration
        .requested_id
        .expect("unregister requested");
    assert!(
        broker_registration
            .response
            .as_ref()
            .expect("UnregisterBroker answered")
            == &UnregisterBrokerResponse {
                throttle_time_ms: 0,
                error_code: 102,
                error_message: Some(format!("Broker ID {broker_id} is not currently registered")),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }
    );
}

#[then("the repeated unregister returns BROKER_ID_NOT_REGISTERED")]
fn repeated_unregister(broker_registration: &BrokerRegistration) {
    let response = broker_registration
        .response
        .as_ref()
        .expect("repeated UnregisterBroker answered");
    assert!(response.error_code == 102, "{response:?}");
}

#[scenario(
    path = "tests/features/migrated/tests/unregister_broker.feature",
    name = "unregister known broker drops it from metadata"
)]
#[tokio::test]
async fn unregister_known_broker_drops_it_from_metadata(broker_registration: BrokerRegistration) {
    broker_registration
        .process
        .take()
        .expect("broker")
        .broker
        .shutdown()
        .await;
    drop(broker_registration);
}

#[scenario(
    path = "tests/features/migrated/tests/unregister_broker.feature",
    name = "unregister unknown broker returns broker id not registered"
)]
#[tokio::test]
async fn unregister_unknown_broker_returns_broker_id_not_registered(
    broker_registration: BrokerRegistration,
    broker_id: i32,
) {
    let _ = broker_id;
    broker_registration
        .process
        .take()
        .expect("broker")
        .broker
        .shutdown()
        .await;
    drop(broker_registration);
}

#[scenario(
    path = "tests/features/migrated/tests/unregister_broker.feature",
    name = "unregister is idempotent on repeat call"
)]
#[tokio::test]
async fn unregister_is_idempotent_on_repeat_call(broker_registration: BrokerRegistration) {
    broker_registration
        .process
        .take()
        .expect("broker")
        .broker
        .shutdown()
        .await;
    drop(broker_registration);
}
