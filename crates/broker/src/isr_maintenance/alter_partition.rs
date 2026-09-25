//! Delivery of an ISR proposal to the controller quorum: it picks the
//! candidate targets from the metadata image, sends the request over a
//! short-lived client, and classifies the response. Kept apart from the scan
//! loop because every step here is about reaching a controller, not about
//! deciding what to propose.

use std::sync::Arc;

use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        alter_partition_request::{self, AlterPartitionRequest},
        alter_partition_response::AlterPartitionResponse,
    },
};
use krabka_raft::NodeId;
use tracing::{debug, warn};

use super::request_builder::build_alter_partition_request;

#[tracing::instrument(
    name = "isr_send_alter_partition",
    level = "info",
    skip_all,
    fields(topic = %topic, partition, leader_epoch, new_isr_len = new_isr.len()),
    err,
)]
#[allow(clippy::too_many_arguments)] // Keeps controller identity and transport inputs explicit.
pub(super) async fn send_alter_partition(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    broker_id: i32,
    topic: &str,
    partition: i32,
    new_isr: Vec<NodeId>,
    leader_epoch: i32,
    outbound_client: &crate::network::client::InterBrokerClient,
    listener_protocol: krabka_security::ListenerProtocol,
    server_name: &str,
) -> Result<(), String> {
    let image = controller.current_image();
    let leader_id = *controller.watch_leader().borrow();
    let targets = alter_partition_targets(&image, leader_id);
    if targets.is_empty() {
        return match leader_id {
            Some(_) => Err("controller leader not in image".into()),
            None => Err("no controller leader".into()),
        };
    }

    let req =
        build_alter_partition_request(&image, broker_id, topic, partition, &new_isr, leader_epoch);
    let mut last_err = String::new();
    for (target_id, addr) in targets {
        let Some((host, port)) = crate::host_port::parse_host_port(&addr) else {
            last_err = format!("invalid target address {addr}");
            continue;
        };
        match send_alter_partition_to(
            broker_id,
            &host,
            port,
            req.clone(),
            outbound_client,
            listener_protocol,
            server_name,
        )
        .await
        {
            Ok(()) => {
                debug!(controller_target = target_id.0, "AlterPartition proposed");
                return Ok(());
            }
            Err(AlterPartitionSendError::NotController) => {
                last_err = format!("target {target_id} is not controller");
            }
            Err(AlterPartitionSendError::Rejected {
                global_err,
                part_err,
            }) => {
                warn!(
                    controller_target = target_id.0,
                    global_error_code = global_err,
                    partition_error_code = part_err,
                    "AlterPartition rejected by controller"
                );
                return Err(format!(
                    "AlterPartition rejected: global={global_err} partition={part_err}"
                ));
            }
            Err(AlterPartitionSendError::Transport(error)) => {
                last_err = format!("target {target_id} ({addr}): {error}");
            }
        }
    }
    Err(last_err)
}

fn alter_partition_targets(
    image: &krabka_metadata::MetadataImage,
    leader_id: Option<NodeId>,
) -> Vec<(NodeId, String)> {
    let mut out = Vec::new();
    if let Some(id) = leader_id
        && let Some(broker) = image.broker(id)
    {
        out.push((id, format!("{}:{}", broker.host, broker.port)));
    }
    let mut others: Vec<_> = image
        .brokers()
        .filter(|broker| Some(broker.node_id) != leader_id)
        .map(|broker| (broker.node_id, format!("{}:{}", broker.host, broker.port)))
        .collect();
    others.sort_by_key(|(id, _)| *id);
    out.extend(others);
    out
}

#[derive(Debug, PartialEq, Eq)]
enum AlterPartitionSendError {
    NotController,
    Rejected { global_err: i16, part_err: i16 },
    Transport(String),
}

/// Send one `AlterPartition` to `host:port` and decode its response.
///
/// `Connection::send` negotiates its version from the peer's advertised
/// `ApiVersions` table, and since #843 that table withholds every
/// [`crate::api_catalog::INTER_BROKER_ONLY_APIS`] key -- `AlterPartition`
/// included -- from any listener a client can reach, which on the default
/// single-listener broker is the only listener there is
/// (`ListenerKind::ClientAndInterBroker`). Negotiating off that table always
/// fails with `IncompatibleVersion`, even though the peer's dispatch registry
/// accepts and answers the request there.
///
/// When the peer's `ApiVersions` response does carry `AlterPartition` --
/// a peer that predates #843's scoping, or a future dedicated
/// `ListenerKind::InterBroker` listener that keeps advertising it -- this
/// still negotiates normally through `send`, so a mixed-version fleet keeps
/// working through the rolling upgrade `docs/operations/deploy.md` promises:
/// `request_builder::build_alter_partition_request` populates both the v2 and
/// v3 ISR fields precisely so whichever version the peer supports carries the
/// right one. Only when the peer withholds it entirely --
/// [`krabka_client_core::Connection::advertised_api_range`] returns `None` --
/// does this fall back to [`krabka_client_core::Connection::raw_request`] at
/// `MIN_VERSION`, the floor every krabka build has dispatched and ever will:
/// a peer new enough to withhold the key from `ApiVersions` (#843 shipped
/// alongside dispatch continuing to accept the full `MIN_VERSION..=MAX_VERSION`
/// range on that same listener) is guaranteed to still accept it, without
/// this guessing the peer's exact `MAX_VERSION` the way a hard-coded highest
/// version would.
async fn send_alter_partition_to(
    broker_id: i32,
    host: &str,
    port: u16,
    req: AlterPartitionRequest,
    outbound_client: &crate::network::client::InterBrokerClient,
    listener_protocol: krabka_security::ListenerProtocol,
    server_name: &str,
) -> Result<(), AlterPartitionSendError> {
    let client = outbound_client
        .connect_as_connection(
            host,
            port,
            listener_protocol,
            server_name,
            krabka_client_core::ConnectionOptions {
                client_id: format!("krabka-broker-{broker_id}-isr"),
                ..krabka_client_core::ConnectionOptions::default()
            },
        )
        .await
        .map_err(|e| AlterPartitionSendError::Transport(format!("connect: {e}")))?;

    let (global_err, part_err) = if client
        .advertised_api_range(alter_partition_request::API_KEY)
        .is_some()
    {
        let resp = client
            .send(req)
            .await
            .map_err(|e| AlterPartitionSendError::Transport(format!("send: {e}")))?;
        alter_partition_response_errors(&resp)
    } else {
        let version = alter_partition_request::MIN_VERSION;
        let mut body = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut body, version)
            .map_err(|e| AlterPartitionSendError::Transport(format!("encode: {e}")))?;
        let resp_body = client
            .raw_request(alter_partition_request::API_KEY, version, body.freeze())
            .await
            .map_err(|e| AlterPartitionSendError::Transport(format!("send: {e}")))?;
        let resp = AlterPartitionResponse::decode(&mut resp_body.as_ref(), version)
            .map_err(|e| AlterPartitionSendError::Transport(format!("decode: {e}")))?;
        alter_partition_response_errors(&resp)
    };
    classify_alter_partition_response(global_err, part_err)
}

fn alter_partition_response_errors(resp: &AlterPartitionResponse) -> (i16, i16) {
    let part_err = resp
        .topics
        .first()
        .and_then(|t| t.partitions.first())
        .map_or(0, |p| p.error_code);
    (resp.error_code, part_err)
}

fn classify_alter_partition_response(
    global_err: i16,
    part_err: i16,
) -> Result<(), AlterPartitionSendError> {
    if is_not_controller_response(global_err, part_err) {
        return Err(AlterPartitionSendError::NotController);
    }
    if global_err != 0 || part_err != 0 {
        return Err(AlterPartitionSendError::Rejected {
            global_err,
            part_err,
        });
    }
    Ok(())
}

fn is_not_controller_response(global_err: i16, part_err: i16) -> bool {
    global_err == crate::codes::NOT_CONTROLLER || part_err == crate::codes::NOT_CONTROLLER
}

#[cfg(test)]
mod tests {
    use krabka_metadata::MetadataImage;

    use super::*;
    use crate::{broker::Broker, isr_maintenance::test_support::fake_source};

    fn plaintext_client() -> Arc<crate::network::client::InterBrokerClient> {
        Arc::new(crate::network::client::InterBrokerClient::new(None, None))
    }

    #[tokio::test]
    async fn send_alter_partition_errors_without_controller_target() {
        let controller: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(fake_source(MetadataImage::new(uuid::Uuid::nil()), None));

        let err = send_alter_partition(
            &controller,
            1,
            "orders",
            0,
            vec![NodeId(1)],
            3,
            &plaintext_client(),
            krabka_security::ListenerProtocol::Plaintext,
            "localhost",
        )
        .await
        .expect_err("missing controller leader should reject the send");

        assert2::assert!((err) == ("no controller leader"));
    }

    #[tokio::test]
    async fn send_alter_partition_to_reports_transport_error_for_closed_port() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let client = plaintext_client();
        let err = send_alter_partition_to(
            1,
            &addr.ip().to_string(),
            addr.port(),
            AlterPartitionRequest::default(),
            &client,
            krabka_security::ListenerProtocol::Plaintext,
            "localhost",
        )
        .await
        .expect_err("closed local port should fail as transport");

        assert2::assert!(matches!(err, AlterPartitionSendError::Transport(_)));
    }

    /// #843/#1098 regression: a default single-listener broker withholds
    /// `AlterPartition` from `ApiVersions` on its one listener
    /// (`ListenerKind::ClientAndInterBroker`), because that listener is also
    /// what a client reaches. Before `send_alter_partition_to` moved off
    /// negotiated `send` and onto `raw_request`, that withholding broke ISR
    /// shrink/expand outright: `Connection::send::<AlterPartitionRequest>`
    /// negotiates its version against the advertised table, finds no entry,
    /// and fails every proposal with `IncompatibleVersion` before the peer's
    /// dispatch registry -- which still accepts and answers the request --
    /// ever sees it. This starts a real broker with the production default
    /// (no declared `listeners`) and drives `send_alter_partition_to` at its
    /// bound address, asserting the RPC actually completes: whatever the
    /// single-node broker answers, it must not be
    /// `AlterPartitionSendError::Transport`, which is what a negotiation
    /// failure (or any other connect/codec fault) surfaces as.
    #[tokio::test]
    async fn send_alter_partition_to_completes_on_the_default_single_listener_broker() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        assert2::assert!(config.effective_listeners().len() == 1);
        assert2::assert!(config.inter_broker_listener_name == "PLAINTEXT");
        assert2::assert!(
            config.listener_kind("PLAINTEXT")
                == crate::api_catalog::ListenerKind::ClientAndInterBroker
        );

        let handle = Broker::start(config).await.expect("start broker");
        let addr = handle.listen_addr();

        let client = plaintext_client();
        let req = AlterPartitionRequest::default();
        let result = send_alter_partition_to(
            1,
            &addr.ip().to_string(),
            addr.port(),
            req,
            &client,
            krabka_security::ListenerProtocol::Plaintext,
            "localhost",
        )
        .await;

        assert2::assert!(
            !matches!(result, Err(AlterPartitionSendError::Transport(_))),
            "AlterPartition must negotiate and dispatch on the default listener: {result:?}"
        );

        handle.shutdown().await;
    }

    /// Rolling-upgrade guard for the P1 Codex raised on this fallback
    /// (#1101 review): a split-listener broker's dedicated
    /// `ListenerKind::InterBroker` listener still advertises `AlterPartition`
    /// (unlike the default combined listener above), so
    /// `send_alter_partition_to` must still negotiate through
    /// `Connection::send` there rather than fall back to the `MIN_VERSION`
    /// `raw_request` path -- the whole point of checking
    /// `advertised_api_range` first is to keep negotiating whenever the peer
    /// still offers a table to negotiate against.
    #[tokio::test]
    async fn send_alter_partition_to_negotiates_when_the_peer_advertises_the_key() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        let client_listener = crate::config::ListenerSpec {
            name: "CLIENT".to_string(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            advertised: "127.0.0.1:0".to_string(),
            protocol: krabka_security::ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: crate::SslPrincipalMapper::default(),
        };
        let inter_broker_listener = crate::config::ListenerSpec {
            name: "INTERNAL".to_string(),
            // A distinct loopback address, not just a distinct port: both
            // listeners bind port 0, and `BrokerConfig::validate` rejects two
            // listeners with an identical `bind_addr` outright, before the OS
            // ever assigns either a real port.
            bind_addr: "127.0.0.2:0".parse().unwrap(),
            advertised: "127.0.0.2:0".to_string(),
            ..client_listener.clone()
        };
        config.listeners = vec![client_listener, inter_broker_listener];
        config.inter_broker_listener_name = "INTERNAL".to_string();
        assert2::assert!(
            config.listener_kind("INTERNAL") == crate::api_catalog::ListenerKind::InterBroker
        );

        let handle = Broker::start(config).await.expect("start broker");
        // `handle.listen_addr()` resolves to the bound address of
        // `inter_broker_listener_name` on a multi-listener broker.
        let addr = handle.listen_addr();

        let client = plaintext_client();
        let negotiated = client
            .connect_as_connection(
                &addr.ip().to_string(),
                addr.port(),
                krabka_security::ListenerProtocol::Plaintext,
                "localhost",
                krabka_client_core::ConnectionOptions::default(),
            )
            .await
            .expect("connect to the dedicated inter-broker listener");
        assert2::assert!(
            negotiated
                .advertised_api_range(alter_partition_request::API_KEY)
                .is_some(),
            "the dedicated inter-broker listener must still advertise AlterPartition"
        );
        negotiated.close();

        let result = send_alter_partition_to(
            1,
            &addr.ip().to_string(),
            addr.port(),
            AlterPartitionRequest::default(),
            &client,
            krabka_security::ListenerProtocol::Plaintext,
            "localhost",
        )
        .await;

        assert2::assert!(
            !matches!(result, Err(AlterPartitionSendError::Transport(_))),
            "AlterPartition must still negotiate on a listener that advertises it: {result:?}"
        );

        handle.shutdown().await;
    }

    #[test]
    fn not_controller_classification_covers_global_and_partition_codes() {
        let cases = [
            (crate::codes::NOT_CONTROLLER, 0, true),
            (0, crate::codes::NOT_CONTROLLER, true),
            (0, 0, false),
            (crate::codes::UNKNOWN_SERVER_ERROR, 0, false),
        ];
        for (global_err, part_err, want) in cases {
            assert2::assert!(
                (is_not_controller_response(global_err, part_err)) == (want),
                "global_err={global_err} part_err={part_err}"
            );
        }
    }

    #[test]
    fn alter_partition_response_classifies_all_error_surfaces() {
        let cases = [
            (0, 0, Ok(())),
            (
                crate::codes::NOT_CONTROLLER,
                0,
                Err(AlterPartitionSendError::NotController),
            ),
            (
                0,
                crate::codes::NOT_CONTROLLER,
                Err(AlterPartitionSendError::NotController),
            ),
            (
                crate::codes::UNKNOWN_SERVER_ERROR,
                0,
                Err(AlterPartitionSendError::Rejected {
                    global_err: crate::codes::UNKNOWN_SERVER_ERROR,
                    part_err: 0,
                }),
            ),
            (
                0,
                crate::codes::UNKNOWN_SERVER_ERROR,
                Err(AlterPartitionSendError::Rejected {
                    global_err: 0,
                    part_err: crate::codes::UNKNOWN_SERVER_ERROR,
                }),
            ),
            (
                crate::codes::UNKNOWN_SERVER_ERROR,
                crate::codes::UNKNOWN_TOPIC_OR_PARTITION,
                Err(AlterPartitionSendError::Rejected {
                    global_err: crate::codes::UNKNOWN_SERVER_ERROR,
                    part_err: crate::codes::UNKNOWN_TOPIC_OR_PARTITION,
                }),
            ),
        ];
        for (global_err, part_err, want) in cases {
            assert2::assert!(
                (classify_alter_partition_response(global_err, part_err)) == (want),
                "global_err={global_err} part_err={part_err}"
            );
        }
    }
}
