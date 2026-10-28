//! A fresh client's version, metadata, and group-coordinator discovery.

use {
    crate::support::{
        self, InProcess,
        discovery::{
            api_versions, expected_coordinator, expected_coordinator_response,
            expected_local_coordinator, topic_metadata_request,
        },
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    krabka_protocol::owned::{
        api_versions_response::ApiVersionsResponse,
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::{Coordinator, FindCoordinatorResponse},
        metadata_response::MetadataResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct DiscoveryScenario {
    broker: Option<InProcess>,
    versions: Option<ApiVersionsResponse>,
    metadata: Option<MetadataResponse>,
    first_coordinator: Option<FindCoordinatorResponse>,
    retried_coordinator: Option<Coordinator>,
}

impl DiscoveryScenario {
    fn broker(&self) -> &InProcess {
        self.broker
            .as_ref()
            .expect("the discovery broker is running")
    }

    async fn shutdown(self) {
        self.broker
            .expect("the discovery broker is running")
            .broker
            .shutdown()
            .await;
    }
}

#[fixture]
fn discovery() -> DiscoveryScenario {
    // Each scenario owns fresh services and observations.
    DiscoveryScenario {
        broker: None,
        ..Default::default()
    }
}

#[given("a single broker for client discovery")]
async fn discovery_broker(discovery: &mut DiscoveryScenario) {
    discovery.broker = Some(support::start().await);
}

#[given("topic beta has three partitions")]
async fn beta_topic(discovery: &DiscoveryScenario) {
    let create = create_topic_request(creatable_topic().name("beta".into()).partitions(3).call());
    let _ = discovery.broker().client.send(create).await.unwrap();
}

#[when("the client requests supported API versions")]
async fn request_versions(discovery: &mut DiscoveryScenario) {
    discovery.versions = Some(api_versions(&discovery.broker().client).await);
}

#[then("API version discovery succeeds and advertises ApiVersions itself")]
fn supported_versions(discovery: &DiscoveryScenario) {
    let response = discovery
        .versions
        .as_ref()
        .expect("ApiVersions was requested");
    assert!(response.error_code == 0);
    assert!(response.api_keys.iter().any(|key| key.api_key == 18));
}

#[when("the client requests metadata for all topics")]
async fn request_all_metadata(discovery: &mut DiscoveryScenario) {
    discovery.metadata = Some(
        discovery
            .broker()
            .client
            .send(topic_metadata_request(None))
            .await
            .expect("Metadata"),
    );
}

#[then("metadata names this broker and all three beta partition leaders")]
fn beta_metadata(discovery: &DiscoveryScenario) {
    let response = discovery.metadata.as_ref().expect("Metadata was requested");
    assert!(response.brokers.len() == 1);
    let topic = response
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some("beta"))
        .unwrap();
    assert!(topic.partitions.len() == 3);
    for (index, partition) in topic.partitions.iter().take(3).enumerate() {
        let expected_index = i32::try_from(index).expect("partition index fits in i32");
        check!(
            partition.error_code == 0
                && partition.partition_index == expected_index
                && partition.leader_id == 1
        );
    }
}

#[when("the client first looks up coordinator for any-group")]
async fn first_group_lookup(discovery: &mut DiscoveryScenario) {
    discovery.first_coordinator = Some(
        discovery
            .broker()
            .client
            .send(FindCoordinatorRequest {
                coordinator_keys: vec!["any-group".into()],
                ..Default::default()
            })
            .await
            .expect("FindCoordinator"),
    );
}

#[then("the group coordinator is initially unavailable with no broker address")]
fn coordinator_initially_unavailable(discovery: &DiscoveryScenario) {
    check!(
        discovery
            .first_coordinator
            .as_ref()
            .expect("the first lookup completed")
            == &expected_coordinator_response(expected_coordinator(
                "any-group",
                -1,
                String::new(),
                -1,
                15,
                None
            ))
    );
}

#[when("the client retries group coordinator discovery until it is ready")]
async fn retry_group_lookup(discovery: &mut DiscoveryScenario) {
    discovery.retried_coordinator = Some(
        support::find_coordinator(
            &discovery.broker().client,
            support::KEY_TYPE_GROUP,
            "any-group",
        )
        .await,
    );
}

#[then("the group coordinator is this broker at its bound listener")]
fn coordinator_is_local(discovery: &DiscoveryScenario) {
    check!(
        discovery
            .retried_coordinator
            .as_ref()
            .expect("the retried lookup completed")
            == &expected_local_coordinator("any-group", discovery.broker().broker.listen_addr())
    );
}

#[scenario(
    path = "tests/features/migrated/tests/unit/discovery.feature",
    name = "api versions round trip"
)]
#[tokio::test]
async fn api_versions_round_trip(discovery: DiscoveryScenario) { discovery.shutdown().await; }

#[scenario(
    path = "tests/features/migrated/tests/unit/discovery.feature",
    name = "metadata returns this broker and listed topics"
)]
#[tokio::test]
async fn metadata_returns_this_broker_and_listed_topics(discovery: DiscoveryScenario) {
    discovery.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/discovery.feature",
    name = "find coordinator creates the offsets topic then returns self"
)]
#[tokio::test]
async fn find_coordinator_creates_the_offsets_topic_then_returns_self(
    discovery: DiscoveryScenario,
) {
    discovery.shutdown().await;
}
