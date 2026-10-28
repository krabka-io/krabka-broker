//! Topic Read and Write grants also authorize named Metadata through Describe.
use {
    crate::{polling::named_metadata_as_alice, sasl_cluster::start_alice_topic_grant},
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_metadata::AclOperation,
    krabka_protocol::owned::metadata_response::MetadataResponse,
    std::net::SocketAddr,
    tempfile::TempDir,
};
struct TopicVisibility {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    address: Option<SocketAddr>,
    grant: Option<AclOperation>,
    metadata: Option<MetadataResponse>,
}
#[rstest::fixture]
fn visibility() -> TopicVisibility {
    // Start this scenario with fresh inputs and observations.

    TopicVisibility {
        broker: None,
        directory: None,
        address: None,
        grant: None,
        metadata: None,
    }
}
#[rstest_bdd_macros::given("alice has only a topic {operation} grant for foo")]
async fn alice_topic_grant(visibility: &mut TopicVisibility, operation: String) {
    let grant = match operation.as_str() {
        "Read" => AclOperation::Read,
        "Write" => AclOperation::Write,
        other => panic!("unsupported topic operation {other}"),
    };
    let (broker, directory, address) = start_alice_topic_grant(grant).await;
    visibility.broker = Some(broker);
    visibility.directory = Some(directory);
    visibility.address = Some(address);
    visibility.grant = Some(grant);
}
#[rstest_bdd_macros::when("alice requests Metadata for foo by name")]
async fn alice_named_metadata(visibility: &mut TopicVisibility) {
    visibility.metadata =
        Some(named_metadata_as_alice(visibility.address.expect("broker address"), "foo").await);
}
#[rstest_bdd_macros::then("Metadata contains exactly foo without a topic authorization error")]
fn alice_topic_described(visibility: &TopicVisibility) {
    let response = visibility
        .metadata
        .as_ref()
        .expect("named Metadata requested");
    assert!(response.topics.len() == 1, "one topic row in response");
    let [row] = &response.topics[..] else {
        unreachable!()
    };
    assert!(row.name.as_deref() == Some("foo"));
    let operation = visibility.grant.expect("topic grant");
    assert!(
        row.error_code == 0,
        "{operation:?} implies Describe, foo must be visible to alice with error_code=0, got {row:?}"
    );
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/implications.feature",
    name = "Read and Write imply Describe for named Metadata"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_operations_imply_describe(visibility: TopicVisibility, operation: String) {
    let _ = operation;
    visibility.broker.expect("broker started").shutdown().await;
}
