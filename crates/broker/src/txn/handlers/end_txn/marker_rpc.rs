//! The remote leg of the marker fan-out: the `WriteTxnMarkers` request that the
//! transaction coordinator sends to a partition leader on another broker, the
//! request it builds from the transaction's partitions, and the per-partition
//! result check it applies to the response.

use std::collections::HashMap;

use krabka_metadata::NodeId;
use krabka_protocol::owned::{
    write_txn_markers_request::{
        WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
    },
    write_txn_markers_response::WriteTxnMarkersResponse,
};

use super::markers::MarkerDispatchContext;
use crate::{
    codes,
    error::BrokerError,
    txn::{
        marker::MarkerType,
        state::{TopicPartition, TxnEntry},
    },
};

/// Send a `WriteTxnMarkersRequest` to a remote broker that leads one or more
/// of the transaction's partitions.
///
/// Dials through the shared
/// [`InterBrokerClient`](crate::network::client::InterBrokerClient) so the connection
/// terminates TLS and runs the SASL client handshake whenever the
/// inter-broker listener demands them. A one-shot
/// `krabka_client_core::Client` per call would carry no TLS
/// connector and no inter-broker credentials. Marker fan-out would then
/// succeed only against a PLAINTEXT inter-broker listener, and it would
/// silently break transactions that span remote-led partitions on any
/// secured cluster.
///
/// ## Coordinator epoch
///
/// The caller resolves the current `__transaction_state` partition leader epoch
/// from the metadata image and stamps it on every marker.
// cargo-mutants: an I/O-only wrapper with no in-process signal. It dials a remote
// broker through the shared `InterBrokerClient`, sends one
// `WriteTxnMarkersRequest` and maps the reply; no test in this process can build
// the connection, so every mutant of the dial-and-send sequence survives
// unobserved. The marker batch it sends is built by `marker::build`, which is
// mutation-tested.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn send_write_txn_markers(
    context: MarkerDispatchContext<'_>,
    leader_node: NodeId,
    entry: &TxnEntry,
    marker_type: MarkerType,
    tps: &[TopicPartition],
) -> Result<(), BrokerError> {
    let MarkerDispatchContext {
        node_id: my_node_id,
        coordinator_epoch,
        image,
        inter_broker_client,
        inter_broker_protocol,
        inter_broker_listener_name,
        inter_broker_server_name,
        ..
    } = context;
    let Some(broker_info) = image.broker(leader_node) else {
        return Err(BrokerError::Txn(format!(
            "EndTxn: leader node {leader_node} not found in metadata image"
        )));
    };

    // Prefer the leader's inter-broker listener endpoint when it has projected
    // one onto its registration record; fall back to the legacy top-level
    // host/port. Mirrors the resolution in the replicator supervisor and
    // heartbeat client — the marker RPC must target the same listener whose
    // protocol we dial with.
    let (host, port) = broker_info
        .endpoints
        .iter()
        .find(|e| e.name == inter_broker_listener_name)
        .map_or_else(
            || (broker_info.host.clone(), broker_info.port),
            |e| (e.host.clone(), e.port),
        );

    let req = build_write_txn_markers_request(entry, marker_type, tps, coordinator_epoch);

    let opts = krabka_client_core::ConnectionOptions {
        client_id: format!("krabka-broker-txn-{my_node_id}"),
        ..krabka_client_core::ConnectionOptions::default()
    };
    let conn = inter_broker_client
        .connect_as_connection(
            &host,
            port,
            inter_broker_protocol,
            inter_broker_server_name,
            opts,
        )
        .await
        .map_err(|e| BrokerError::Txn(format!("EndTxn: connect to {host}:{port}: {e}")))?;

    // `Connection::send` negotiates the wire version from the broker-advertised
    // ApiVersions table established during connect.
    let resp = conn
        .send(req)
        .await
        .map_err(|e| BrokerError::Txn(format!("EndTxn: WriteTxnMarkers to {host}:{port}: {e}")))?;

    conn.close();
    validate_marker_response(entry, tps, &resp)
}

fn build_write_txn_markers_request(
    entry: &TxnEntry,
    marker_type: MarkerType,
    tps: &[TopicPartition],
    coordinator_epoch: i32,
) -> WriteTxnMarkersRequest {
    // Group tps by topic for the nested WritableTxnMarkerTopic structure.
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for tp in tps {
        by_topic
            .entry(tp.topic.clone())
            .or_default()
            .push(tp.partition.get());
    }

    let topics: Vec<WritableTxnMarkerTopic> = by_topic
        .into_iter()
        .map(|(name, partition_indexes)| WritableTxnMarkerTopic {
            name,
            partition_indexes,
            ..Default::default()
        })
        .collect();

    WriteTxnMarkersRequest {
        markers: vec![WritableTxnMarker {
            // Unwrap into the raw-`i64` wire field.
            producer_id: entry.producer_id.get(),
            producer_epoch: entry.producer_epoch,
            transaction_result: marker_type == MarkerType::Commit,
            topics,
            coordinator_epoch,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn validate_marker_response(
    entry: &TxnEntry,
    tps: &[TopicPartition],
    response: &WriteTxnMarkersResponse,
) -> Result<(), BrokerError> {
    let marker = response
        .markers
        .iter()
        .find(|marker| marker.producer_id == entry.producer_id.get())
        .ok_or_else(|| {
            BrokerError::Txn(format!(
                "WriteTxnMarkers response omitted producer {}",
                entry.producer_id.get()
            ))
        })?;
    for tp in tps {
        let result = marker
            .topics
            .iter()
            .find(|topic| topic.name == tp.topic)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|partition| partition.partition_index == tp.partition.get())
            })
            .ok_or_else(|| {
                BrokerError::Txn(format!(
                    "WriteTxnMarkers response omitted {}-{}",
                    tp.topic,
                    tp.partition.get()
                ))
            })?;
        if result.error_code != codes::NONE {
            return Err(BrokerError::Txn(format!(
                "WriteTxnMarkers failed for {}-{} with error code {}",
                tp.topic,
                tp.partition.get(),
                result.error_code
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{
        BrokerEndpoint, BrokerRegistrationRecord, MetadataImage, MetadataRecord,
    };
    use krabka_protocol::owned::write_txn_markers_response::{
        WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
    };
    use krabka_security::ListenerProtocol;

    use super::*;
    use crate::{
        network::client::InterBrokerClient,
        txn::handlers::end_txn::test_support::{marker_entry, plaintext_client, tps},
    };

    fn marker_response(error_code: i16) -> WriteTxnMarkersResponse {
        WriteTxnMarkersResponse {
            markers: vec![WritableTxnMarkerResult {
                producer_id: 7,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "t".to_string(),
                    partitions: vec![WritableTxnMarkerPartitionResult {
                        partition_index: 0,
                        error_code,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn marker_response_requires_every_partition_to_succeed() {
        let entry = marker_entry();
        let partitions = tps();
        assert!(
            validate_marker_response(&entry, &partitions, &marker_response(codes::NONE)).is_ok()
        );
        assert!(
            validate_marker_response(
                &entry,
                &partitions,
                &marker_response(codes::NOT_LEADER_OR_FOLLOWER)
            )
            .is_err()
        );
        assert!(
            validate_marker_response(&entry, &partitions, &WriteTxnMarkersResponse::default())
                .is_err()
        );
    }

    #[test]
    fn marker_request_uses_current_coordinator_epoch() {
        let request =
            build_write_txn_markers_request(&marker_entry(), MarkerType::Abort, &tps(), 42);

        assert!(request.markers.len() == 1);
        assert!(request.markers[0].coordinator_epoch == 42);
    }

    async fn send_test_markers(
        image: &MetadataImage,
        leader: NodeId,
        listener_name: &str,
    ) -> Result<(), BrokerError> {
        let client = plaintext_client();
        let entry = marker_entry();
        let partitions = tps();
        send_write_txn_markers(
            MarkerDispatchContext {
                node_id: NodeId(1),
                coordinator_epoch: 0,
                image,
                inter_broker_client: &client,
                inter_broker_protocol: ListenerProtocol::Plaintext,
                inter_broker_listener_name: listener_name,
                inter_broker_server_name: "localhost",
                group_coordinator: None,
            },
            leader,
            &entry,
            MarkerType::Commit,
            &partitions,
        )
        .await
    }

    /// Leader node absent from the metadata image → descriptive `Txn` error,
    /// and no dial.
    #[tokio::test]
    async fn errors_when_leader_node_missing_from_image() {
        let image = MetadataImage::default();
        let err = send_test_markers(&image, NodeId(99), "PLAINTEXT")
            .await
            .expect_err("missing leader must error");
        assert!(
            matches!(&err, BrokerError::Txn(m) if m.contains("not found")),
            "unexpected error: {err:?}"
        );
    }

    /// Leader resolves to its inter-broker endpoint, but the address is
    /// unreachable → the dial fails and the error names the resolved
    /// `host:port` (the endpoint, not the top-level fallback).
    #[tokio::test]
    async fn errors_when_inter_broker_endpoint_unreachable() {
        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(2),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".to_string(),
                port: 9,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![BrokerEndpoint {
                    name: "INTERNAL".to_string(),
                    host: "127.0.0.1".to_string(),
                    // Discard port: refuses connections immediately.
                    port: 9,
                    protocol: ListenerProtocol::Plaintext,
                }],
                features: std::collections::BTreeMap::new(),
            },
        ));
        let err = send_test_markers(&image, NodeId(2), "INTERNAL")
            .await
            .expect_err("unreachable endpoint must error");
        assert!(
            matches!(&err, BrokerError::Txn(m) if m.contains("connect to 127.0.0.1:9")),
            "unexpected error: {err:?}"
        );
    }

    /// No endpoint matches the inter-broker listener name → fall back to the
    /// record's top-level `host`/`port`. Still unreachable, so the dial fails
    /// against the fallback address.
    #[tokio::test]
    async fn falls_back_to_top_level_host_port_when_no_matching_endpoint() {
        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(2),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".to_string(),
                port: 9,
                rack: None,
                log_dirs: vec![],
                // Endpoint exists but under a different listener name, so the
                // `find(name == inter_broker_listener_name)` misses.
                endpoints: vec![BrokerEndpoint {
                    name: "SOMETHING_ELSE".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 65000,
                    protocol: ListenerProtocol::Plaintext,
                }],
                features: std::collections::BTreeMap::new(),
            },
        ));
        let err = send_test_markers(&image, NodeId(2), "INTERNAL")
            .await
            .expect_err("unreachable fallback must error");
        assert!(
            matches!(&err, BrokerError::Txn(m) if m.contains("connect to 127.0.0.1:9")),
            "expected fallback to top-level 127.0.0.1:9, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn remote_marker_dispatch_dials_with_configured_server_name() {
        use std::sync::Arc;

        use tokio::net::TcpListener;
        use tokio_rustls::{
            LazyConfigAcceptor,
            rustls::{ClientConfig, RootCertStore, server::Acceptor},
        };

        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();

        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind TLS ClientHello capture listener");
        let port = listener
            .local_addr()
            .expect("capture listener address")
            .port();
        let capture = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept marker dial");
            let handshake = LazyConfigAcceptor::new(Acceptor::default(), stream)
                .await
                .expect("parse marker dial ClientHello");
            handshake.client_hello().server_name().map(str::to_owned)
        });

        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(2),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".to_string(),
                port,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![BrokerEndpoint {
                    name: "INTERNAL".to_string(),
                    host: "127.0.0.1".to_string(),
                    port,
                    protocol: ListenerProtocol::Ssl,
                }],
                features: std::collections::BTreeMap::new(),
            },
        ));
        let tls = ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        let client =
            InterBrokerClient::new(Some(tokio_rustls::TlsConnector::from(Arc::new(tls))), None);
        let entry = marker_entry();
        let partitions = tps();
        let result = send_write_txn_markers(
            MarkerDispatchContext {
                node_id: NodeId(1),
                coordinator_epoch: 0,
                image: &image,
                inter_broker_client: &client,
                inter_broker_protocol: ListenerProtocol::Ssl,
                inter_broker_listener_name: "INTERNAL",
                inter_broker_server_name: "broker.internal",
                group_coordinator: None,
            },
            NodeId(2),
            &entry,
            MarkerType::Commit,
            &partitions,
        )
        .await;

        assert!(
            result.is_err(),
            "capture server intentionally stops after ClientHello"
        );
        assert!(
            capture.await.expect("join ClientHello capture").as_deref() == Some("broker.internal")
        );
    }
}
