//! `DescribeCluster` (`api_key` 60): the broker listing a client gets from a
//! broker endpoint, and the KIP-919 rejection of a controller projection asked
//! for on that same endpoint.

use assert2::check;
use krabka_protocol::owned::describe_cluster_request::DescribeClusterRequest;

use crate::{admin_harness::build_client, support::start_n_node};

/// `DescribeCluster` on a 1-broker cluster returns `error_code == 0`, exactly
/// one broker entry, and a `controller_id` naming that entry. A client
/// resolves the controller against the broker list in the same response, so
/// the id has to be one of the rows the response carries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_lists_brokers() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    let resp = client
        .send(DescribeClusterRequest::default())
        .await
        .expect("describe_cluster");
    check!(resp.error_code == 0, "describe_cluster error_code");
    check!(resp.brokers.len() == 1, "expected exactly 1 broker");
    check!(
        resp.brokers
            .iter()
            .any(|b| b.broker_id == resp.controller_id),
        "controller_id {} names no broker in {:?}",
        resp.controller_id,
        resp.brokers
    );
}

/// KIP-919: a broker endpoint rejects a controller projection request. Clients
/// must send `endpoint_type = 2` to a controller listener instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_endpoint_type_controllers_is_rejected() {
    const ENDPOINT_TYPE_CONTROLLERS: i8 = 2;
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    let resp = client
        .send(DescribeClusterRequest {
            endpoint_type: ENDPOINT_TYPE_CONTROLLERS,
            ..Default::default()
        })
        .await
        .expect("describe_cluster controllers");
    check!(
        resp.error_code == krabka_broker::codes::MISMATCHED_ENDPOINT_TYPE,
        "describe_cluster error_code"
    );
    check!(
        resp.endpoint_type == ENDPOINT_TYPE_CONTROLLERS,
        "response echoes endpoint_type=2; got {}",
        resp.endpoint_type
    );
    check!(resp.brokers.is_empty());
}
