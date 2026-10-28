//! `DescribeCluster` (`api_key` 60): the broker listing a client gets from a
//! broker endpoint, and the KIP-919 rejection of a controller projection asked
//! for on that same endpoint.

use {
    crate::support::start_n_node_client, assert2::check, krabka_broker::codes,
    krabka_protocol::owned::describe_cluster_request::DescribeClusterRequest,
};

#[cfg(test)]
#[rstest_bdd_macros::then("admin handlers admin describe cluster: describe cluster lists brokers")]
async fn bdd_verify_describe_cluster_lists_brokers() {
    let (_cluster, client) = start_n_node_client(1, "admin-handlers-test").await;

    let resp = client
        .send(DescribeClusterRequest::default())
        .await
        .expect("describe_cluster");
    check!(resp.error_code == 0, "describe_cluster error_code");
    check!(resp.brokers.len() == 1, "expected exactly 1 broker");
    check!(resp.controller_id == 1, "expected controller_id == 1");
}

/// `DescribeCluster` on a 1-broker cluster returns `error_code == 0`, exactly
/// one broker entry, and `controller_id == 1`.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_describe_cluster.feature",
    name = "describe cluster lists brokers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_lists_brokers() {}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "admin handlers admin describe cluster: describe cluster endpoint type controllers is rejected"
)]
async fn bdd_verify_describe_cluster_endpoint_type_controllers_is_rejected() {
    const ENDPOINT_TYPE_CONTROLLERS: i8 = 2;
    let (_cluster, client) = start_n_node_client(1, "admin-handlers-test").await;

    let resp = client
        .send(DescribeClusterRequest {
            endpoint_type: ENDPOINT_TYPE_CONTROLLERS,
            ..Default::default()
        })
        .await
        .expect("describe_cluster controllers");
    check!(
        resp.error_code == codes::MISMATCHED_ENDPOINT_TYPE,
        "describe_cluster error_code"
    );
    // Kafka's error response does not echo the requested endpoint_type back;
    // it stays at the schema default (1 = BROKER), matching real Kafka's
    // `AuthHelper.computeDescribeClusterResponse`.
    check!(
        resp.endpoint_type == 1,
        "error response leaves endpoint_type at the schema default; got {}",
        resp.endpoint_type
    );
    check!(resp.brokers.is_empty());
}

/// KIP-919: a broker endpoint rejects a controller projection request. Clients
/// must send `endpoint_type = 2` to a controller listener instead.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_describe_cluster.feature",
    name = "describe cluster endpoint type controllers is rejected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_endpoint_type_controllers_is_rejected() {}
