//! Tests for the `ApiVersions` entry point in [`super`]: the api-key table it
//! advertises, the KIP-511 client-information rejection, and the KIP-1242
//! routing checks.
//!
//! Most of them drive a live broker, so they are kept out of the module root.
//! The feature-row and name-validation tests live beside the code they cover,
//! in [`super::feature_keys`] and [`super::client_info`].

use assert2::{assert, check};
use bytes::{Bytes, BytesMut};
use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
use krabka_protocol::{
    Encode,
    owned::{api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse},
};

use super::*;
use crate::{broker::Broker, codes};

const API_VERSIONS_V3: i16 = 3;
const API_VERSIONS_V5: i16 = 5;

fn request(name: &str, version: &str) -> Bytes {
    let req = ApiVersionsRequest {
        client_software_name: name.into(),
        client_software_version: version.into(),
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(req.encoded_len(API_VERSIONS_V3));
    req.encode(&mut buf, API_VERSIONS_V3)
        .expect("encode ApiVersionsRequest");
    buf.freeze()
}

fn routing_request(cluster_id: Option<String>, node_id: i32) -> Bytes {
    let req = ApiVersionsRequest {
        client_software_name: "krabka-test".into(),
        client_software_version: "1.0.0".into(),
        cluster_id,
        node_id,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(req.encoded_len(API_VERSIONS_V5));
    req.encode(&mut buf, API_VERSIONS_V5)
        .expect("encode ApiVersionsRequest v5");
    buf.freeze()
}

fn decode_response(version: i16, bytes: &Bytes) -> ApiVersionsResponse {
    crate::test_support::decode_response(bytes, version)
}

async fn start_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|_cfg| {}).await
}

async fn wait_for_leader(broker: &Broker) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if broker
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == broker.config.node_id)
        {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "broker did not become controller leader"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

#[test]
fn api_versions_advertises_legacy_produce_and_fetch_min() {
    let table = crate::api_catalog::supported_apis();
    let produce = table.iter().find(|v| v.api_key == 0).expect("produce");
    let fetch = table.iter().find(|v| v.api_key == 1).expect("fetch");
    assert!(
        produce.min_version == 0,
        "Produce min must be 0 to advertise the legacy v0-2 support"
    );
    assert!(
        fetch.min_version == 0,
        "Fetch min must be 0 to advertise the legacy v0-3 support"
    );
}

#[test]
fn api_versions_advertises_kip853_rpcs_and_describe_quorum_v2() {
    use krabka_protocol::owned;
    let table = crate::api_catalog::supported_apis();
    let by_key = |k: i16| table.iter().find(|v| v.api_key == k);

    for (key, max) in [
        (80i16, owned::add_raft_voter_request::MAX_VERSION),
        (81, owned::remove_raft_voter_request::MAX_VERSION),
        (82, owned::update_raft_voter_request::MAX_VERSION),
    ] {
        let v = by_key(key).unwrap_or_else(|| panic!("api_key {key} advertised"));
        assert!(v.min_version == 0);
        assert!(v.max_version == max, "api_key {key} max matches codegen");
    }

    // DescribeQuorum (55) max follows its schema const — now v2 (KIP-853
    // adds VoterDirectoryId + Nodes).
    let dq = by_key(55).expect("describe_quorum advertised");
    assert!(
        dq.max_version == owned::describe_quorum_request::MAX_VERSION,
        "DescribeQuorum max tracks the codegen const"
    );
    assert!(dq.max_version == 2, "DescribeQuorum is v2 after KIP-853");
}

#[tokio::test]
async fn handle_rejects_each_invalid_v3_client_info_field() {
    let (broker_handle, _dir) = start_broker().await;
    let broker = broker_handle.broker_arc_for_test();

    for (name, version) in [("", "1.0.0"), ("krabka-test", "")] {
        let req = request(name, version);
        let bytes = handle(&broker, API_VERSIONS_V3, 7, &req)
            .await
            .expect("ApiVersions handler");
        let resp = decode_response(API_VERSIONS_V3, &bytes);
        assert!(resp.error_code == codes::INVALID_REQUEST, "{resp:?}");
        assert!(resp.api_keys.is_empty(), "{resp:?}");
    }

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_accepts_legacy_request_without_client_info() {
    let (broker_handle, _dir) = start_broker().await;
    let broker = broker_handle.broker_arc_for_test();
    let req = ApiVersionsRequest::default();
    let mut req_bytes = BytesMut::with_capacity(req.encoded_len(0));
    req.encode(&mut req_bytes, 0)
        .expect("encode legacy ApiVersionsRequest");

    let bytes = handle(&broker, 0, 7, &req_bytes)
        .await
        .expect("ApiVersions handler");
    let resp = decode_response(0, &bytes);

    assert!(resp.error_code == codes::NONE, "{resp:?}");
    assert!(!resp.api_keys.is_empty(), "{resp:?}");

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_accepts_valid_v3_and_surfaces_catalog_and_features() {
    let (broker_handle, _dir) = start_broker().await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 24,
        })])
        .await
        .expect("submit finalized feature");
    let image = broker.controller.current_image();
    assert!(image.finalized_features_epoch() > 0);

    let req = request("krabka-test", "1.0.0");
    let bytes = handle(&broker, API_VERSIONS_V3, 7, &req)
        .await
        .expect("ApiVersions handler");
    let resp = decode_response(API_VERSIONS_V3, &bytes);

    check!(resp.error_code == codes::NONE, "{resp:?}");
    check!(
        resp.api_keys == crate::api_catalog::supported_apis(),
        "{resp:?}"
    );
    check!(!resp.supported_features.is_empty(), "{resp:?}");
    let mv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version supported");
    check!(mv.min_version == crate::features::METADATA_VERSION_MIN);
    check!(mv.max_version == crate::features::METADATA_VERSION_MAX);
    check!(resp.finalized_features_epoch == image.finalized_features_epoch());
    let finalized_mv = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version finalized");
    assert!(finalized_mv.max_version_level == 24, "{resp:?}");
    assert!(finalized_mv.min_version_level == 24, "{resp:?}");

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_applies_kip1242_routing_checks() {
    let (broker_handle, _dir) =
        crate::test_support::start_broker_with(|cfg| cfg.broker_id = 42).await;
    let broker = broker_handle.broker_arc_for_test();
    let cluster_id = broker.controller.current_image().cluster_id().to_string();
    let node_id = i32::try_from(broker.config.node_id.0).expect("node id fits Kafka wire");
    assert!(node_id != broker.config.broker_id);

    for (request_cluster_id, request_node_id, expected_error) in [
        (None, -1, codes::NONE),
        (Some(cluster_id.clone()), -1, codes::INVALID_REQUEST),
        (None, node_id, codes::INVALID_REQUEST),
        (Some(cluster_id.clone()), node_id, codes::NONE),
        (
            Some("wrong-cluster".into()),
            node_id,
            codes::REBOOTSTRAP_REQUIRED,
        ),
        (
            Some(cluster_id.clone()),
            node_id + 1,
            codes::REBOOTSTRAP_REQUIRED,
        ),
    ] {
        let request = routing_request(request_cluster_id, request_node_id);
        let bytes = handle(&broker, API_VERSIONS_V5, 7, &request)
            .await
            .expect("ApiVersions v5 handler");
        let response = decode_response(API_VERSIONS_V5, &bytes);

        assert!(response.error_code == expected_error, "{response:?}");
        assert!(
            response.api_keys.is_empty() == (expected_error != codes::NONE),
            "{response:?}"
        );
    }

    broker_handle.shutdown().await;
}
