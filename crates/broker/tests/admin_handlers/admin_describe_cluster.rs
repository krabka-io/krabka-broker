//! `DescribeCluster` (`api_key` 60): the broker listing a client gets from a
//! broker endpoint, and the KIP-919 rejection of a controller projection asked
//! for on that same endpoint.

use {
    crate::admin_harness::{AdminCluster, admin},
    assert2::check,
    krabka_broker::codes,
    krabka_protocol::owned::{
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::DescribeClusterResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{scenario, then, when},
};

#[derive(Default)]
struct ClusterDiscovery {
    request: DescribeClusterRequest,
    response: Option<DescribeClusterResponse>,
}

#[fixture]
fn discovery() -> ClusterDiscovery {
    // Start this scenario with fresh inputs and observations.

    ClusterDiscovery {
        request: DescribeClusterRequest::default(),
        response: None,
    }
}

#[when("the admin requests the broker cluster projection")]
async fn broker_projection(admin: &AdminCluster, discovery: &mut ClusterDiscovery) {
    discovery.response = Some(
        admin
            .client()
            .send(discovery.request.clone())
            .await
            .expect("describe_cluster"),
    );
}

#[when("the admin requests a controller projection from the broker endpoint")]
async fn wrong_projection(admin: &AdminCluster, discovery: &mut ClusterDiscovery) {
    discovery.response = Some(
        admin
            .client()
            .send(DescribeClusterRequest {
                endpoint_type: 2,
                ..Default::default()
            })
            .await
            .expect("describe_cluster controllers"),
    );
}

#[then("DescribeCluster succeeds with broker 1 as the only broker and controller")]
fn single_broker_projection(discovery: &ClusterDiscovery) {
    let response = discovery
        .response
        .as_ref()
        .expect("a cluster projection was requested");
    check!(response.error_code == 0, "describe_cluster error_code");
    check!(response.brokers.len() == 1, "expected exactly 1 broker");
    check!(response.controller_id == 1, "expected controller_id == 1");
}

#[then("DescribeCluster reports MISMATCHED_ENDPOINT_TYPE with an empty broker projection")]
fn projection_rejected(discovery: &ClusterDiscovery) {
    let response = discovery
        .response
        .as_ref()
        .expect("a cluster projection was requested");
    check!(
        response.error_code == codes::MISMATCHED_ENDPOINT_TYPE,
        "describe_cluster error_code"
    );
    check!(
        response.endpoint_type == 1,
        "error response leaves endpoint_type at the schema default; got {}",
        response.endpoint_type
    );
    check!(response.brokers.is_empty());
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_describe_cluster.feature",
    name = "describe cluster lists brokers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_lists_brokers(admin: AdminCluster, discovery: ClusterDiscovery) {
    admin.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_describe_cluster.feature",
    name = "describe cluster endpoint type controllers is rejected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_endpoint_type_controllers_is_rejected(
    admin: AdminCluster,
    discovery: ClusterDiscovery,
) {
    admin.shutdown().await;
}
